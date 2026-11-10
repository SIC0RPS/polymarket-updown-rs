use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use bbwick_engine::OracleEngine;
use polymarket_hft::ctf_inventory::CtfInventoryRuntime;
use polymarket_hft::polymarket::discovery::build_gamma_discovery_http_client;
use polymarket_hft::polymarket::engine_bridge::EnginePolymarketBridge;
use polymarket_hft::types::MarketTick;
use tokio::task::JoinHandle;

#[path = "bbwick_runtime_loop/args.rs"]
mod args;
#[path = "bbwick_runtime_loop/chainlink.rs"]
mod chainlink;
#[path = "bbwick_runtime_loop/engine_bridge.rs"]
mod engine_bridge;
#[path = "bbwick_runtime_loop/engine_config.rs"]
mod engine_config;
#[path = "bbwick_runtime_loop/runtime_connections.rs"]
mod runtime_connections;
#[path = "bbwick_runtime_loop/strategy_cycles.rs"]
mod strategy_cycles;
#[path = "bbwick_runtime_loop/timing.rs"]
mod timing;
#[path = "bbwick_runtime_loop/top_of_book.rs"]
mod top_of_book;
#[path = "bbwick_runtime_loop/ui_ticks.rs"]
mod ui_ticks;

pub(crate) use args::RunBbwickModeArgs;
use chainlink::ChainlinkCaptureRuntime;
use engine_bridge::{spawn_engine_state_forwarding, start_polymarket_bridge};
use engine_config::oracle_engine_config;
use runtime_connections::{
    contain_runtime_fault, contain_runtime_fault_and_backoff, log_empty_step_symbol,
    log_oracle_connecting, log_oracle_lagged, publish_connected_oracle,
    restart_finished_oracle_runtime, shutdown_runtime_connections,
    top_of_book_receive_error_message,
};
use strategy_cycles::{contain_odds_watch_closed, dispatch_strategy_cycle, StrategyCycleKind};
use timing::{
    branch_timestamps, wait_for_strategy_deadline, CachedWallClock, BBWICK_STATUS_INTERVAL_MS,
};
use top_of_book::handle_top_of_book;
use ui_ticks::UiTickProjection;

use super::bbwick_runtime_status::BbwickOracleRuntimeState;
use super::bbwick_runtime_streams::record_oracle_step;
use super::bbwick_runtime_symbols::{
    cached_canonical_bbwick_symbol, required_bbwick_symbols_for_strategy_cfg,
};
use super::bbwick_runtime_tick::StrategyStatusEmitState;
use crate::app::ctf_inventory::risk_reservation::CentralCtfRiskViews;

pub(crate) async fn run_bbwick_mode(
    args: RunBbwickModeArgs<'_>,
    ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
    ctf_risk_views: CentralCtfRiskViews,
) -> Result<()> {
    let RunBbwickModeArgs {
        run_id,
        cfg,
        oracle_history,
        shared_strategy_cfg,
        actors,
        exec,
        intents_tx,
        split_tx,
        terminal_merge_tx,
        cancel_requests_tx,
        runtime_config_state,
        engine_state_tx,
        oracle_runtime_tx,
        market_projection_tx,
        mut portfolio_rx,
        obs,
        capture,
        shutdown,
    } = args;
    let pause_rx = runtime_config_state.subscribe_pause();
    let mut wall_clock = CachedWallClock::new();
    let startup_ts = branch_timestamps(&mut wall_clock, &None);
    let engine_cfg = oracle_engine_config(cfg, capture);
    let active_symbols: std::collections::HashSet<String> =
        engine_cfg.active_symbols.iter().cloned().collect();
    let mut status_tick = tokio::time::interval(Duration::from_millis(BBWICK_STATUS_INTERVAL_MS));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut runtime = BbwickOracleRuntimeState::new(cfg);
    let capture_underlyings = polymarket_hft::capture::scope::configured_capture_underlyings(cfg);
    let mut oracle: Option<OracleEngine> = None;
    if let Some(oracle_runtime_tx) = oracle_runtime_tx.as_ref() {
        oracle_runtime_tx.send_replace(runtime.supervisor_status());
    }
    runtime.emit_status(obs, startup_ts.wall_ms, startup_ts.ingest_mono_ns, true);
    let mut top_of_book_rx = None;
    let mut step_rx = None;
    let mut polymarket_bridge: Option<EnginePolymarketBridge> = None;
    let mut engine_state_forward_task: Option<JoinHandle<()>> = None;
    let mut ui_tick_projection: Option<UiTickProjection> = None;
    let mut last_ticks = std::collections::BTreeMap::<String, MarketTick>::new();
    let mut portfolio_open = true;
    let mut odds_watch_open = true;
    let mut strategy_status_emit_by_symbol =
        std::collections::BTreeMap::<String, StrategyStatusEmitState>::new();
    let mut managed_tokens_by_symbol = std::collections::BTreeMap::new();
    let mut chainlink_runtime = ChainlinkCaptureRuntime::start(shutdown.clone());
    let mut next_execution_intent_id = 1u64;
    let mut consumption_sequence = 0u64;
    let mut start_pending = true;
    let mut reconnect_attempt = 0_usize;
    let mut reconnect_backoff_ms = 1_000_u64;
    let mut retained_bids = Default::default();

    'runtime: loop {
        if shutdown.is_cancelled() {
            break;
        }
        let loop_ts = branch_timestamps(&mut wall_clock, &oracle);
        if let Some(bids) = restart_finished_oracle_runtime(
            obs,
            &mut runtime,
            oracle_runtime_tx.as_ref(),
            loop_ts.wall_ms,
            loop_ts.ingest_mono_ns,
            &mut polymarket_bridge,
            &mut engine_state_forward_task,
            &mut oracle,
            &mut top_of_book_rx,
            &mut step_rx,
            &mut ui_tick_projection,
            &mut reconnect_attempt,
            &mut reconnect_backoff_ms,
        )
        .await
        {
            retained_bids = bids;
            start_pending = true;
            continue 'runtime;
        }
        if start_pending {
            start_pending = false;
            log_oracle_connecting(loop_ts.wall_ms, &runtime.required_symbols);
            let discovery_http = match build_gamma_discovery_http_client() {
                Ok(client) => client,
                Err(err) => {
                    contain_runtime_fault_and_backoff(
                        obs,
                        &mut runtime,
                        oracle_runtime_tx.as_ref(),
                        loop_ts.wall_ms,
                        loop_ts.ingest_mono_ns,
                        format!("gamma_http_build_failed {err:#}"),
                        &mut reconnect_attempt,
                        &mut reconnect_backoff_ms,
                    )
                    .await;
                    start_pending = true;
                    continue 'runtime;
                }
            };
            retained_bids = shutdown_runtime_connections(
                &mut polymarket_bridge,
                &mut engine_state_forward_task,
                &mut oracle,
                &mut top_of_book_rx,
                &mut step_rx,
                &mut ui_tick_projection,
            )
            .await
            .unwrap_or(retained_bids);
            match OracleEngine::start(
                engine_cfg.clone(),
                retained_bids.clone(),
                oracle_history.clone(),
            ) {
                Ok(mut started) => {
                    let started_top_of_book_rx = match started.take_top_of_book_receiver() {
                        Ok(receiver) => receiver,
                        Err(err) => {
                            let msg = format!("bbwick_oracle_receiver_start_failed error={err:#}");
                            contain_runtime_fault_and_backoff(
                                obs,
                                &mut runtime,
                                oracle_runtime_tx.as_ref(),
                                loop_ts.wall_ms,
                                loop_ts.ingest_mono_ns,
                                msg,
                                &mut reconnect_attempt,
                                &mut reconnect_backoff_ms,
                            )
                            .await;
                            retained_bids = started.shutdown().await;
                            start_pending = true;
                            continue 'runtime;
                        }
                    };
                    runtime.reset_oracle_epoch_state();
                    actors.reset_oracle_epoch_state();
                    last_ticks.clear();
                    engine_state_forward_task = spawn_engine_state_forwarding(
                        &started,
                        engine_state_tx.clone(),
                        market_projection_tx.clone(),
                        obs,
                        capture,
                        shutdown.clone(),
                    );
                    let bridge = match start_polymarket_bridge(
                        cfg,
                        &capture_underlyings,
                        shared_strategy_cfg.clone(),
                        obs.clone(),
                        shutdown.clone(),
                        &started,
                        portfolio_rx.clone(),
                        discovery_http.clone(),
                        capture.clone(),
                    ) {
                        Ok(bridge) => bridge,
                        Err(err) => {
                            let msg =
                                format!("bbwick_polymarket_bridge_start_failed error={err:#}");
                            contain_runtime_fault_and_backoff(
                                obs,
                                &mut runtime,
                                oracle_runtime_tx.as_ref(),
                                loop_ts.wall_ms,
                                loop_ts.ingest_mono_ns,
                                msg,
                                &mut reconnect_attempt,
                                &mut reconnect_backoff_ms,
                            )
                            .await;
                            shutdown_runtime_connections(
                                &mut polymarket_bridge,
                                &mut engine_state_forward_task,
                                &mut oracle,
                                &mut top_of_book_rx,
                                &mut step_rx,
                                &mut ui_tick_projection,
                            )
                            .await;
                            retained_bids = started.shutdown().await;
                            start_pending = true;
                            continue 'runtime;
                        }
                    };
                    polymarket_bridge = Some(bridge);
                    top_of_book_rx = Some(started_top_of_book_rx);
                    step_rx = Some(started.subscribe_steps());
                    ui_tick_projection = Some(UiTickProjection::start(obs.clone(), cfg));
                    reconnect_attempt = 0;
                    reconnect_backoff_ms = 1_000;
                    let started = oracle.insert(started);
                    publish_connected_oracle(
                        &mut runtime,
                        oracle_runtime_tx.as_ref(),
                        &shared_strategy_cfg,
                        obs,
                        &started,
                        loop_ts.wall_ms,
                    );
                    retained_bids.clear();
                }
                Err(err) => {
                    let msg = format!("bbwick_oracle_start_failed error={err:#}");
                    contain_runtime_fault_and_backoff(
                        obs,
                        &mut runtime,
                        oracle_runtime_tx.as_ref(),
                        loop_ts.wall_ms,
                        loop_ts.ingest_mono_ns,
                        msg,
                        &mut reconnect_attempt,
                        &mut reconnect_backoff_ms,
                    )
                    .await;
                    start_pending = true;
                    continue 'runtime;
                }
            }
        }

        let next_strategy_deadline_ms = actors.next_strategy_deadline_ms(&last_ticks);
        let scheduler_ts = branch_timestamps(&mut wall_clock, &oracle);
        let strategy_deadline_overdue =
            next_strategy_deadline_ms.is_some_and(|deadline_ms| deadline_ms < scheduler_ts.wall_ms);

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            received = chainlink_runtime.recv(), if chainlink_runtime.is_open() && !strategy_deadline_overdue => {
                let result = chainlink_runtime.observe(
                    run_id,
                    received,
                    actors,
                    cancel_requests_tx,
                    capture,
                    &mut consumption_sequence,
                );
                let fault = result
                    .err()
                    .map(|err| format!("bbwick_chainlink_cycle_failed error={err:#}"))
                    .or_else(|| (!chainlink_runtime.is_open()).then(|| "bbwick_chainlink_feed_closed".to_string()));
                if let Some(msg) = fault {
                    let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                    contain_runtime_fault(
                        obs, &mut runtime, oracle_runtime_tx.as_ref(),
                        event_ts.wall_ms, event_ts.ingest_mono_ns, msg,
                    ).await;
                }
            }
            _ = status_tick.tick(), if !strategy_deadline_overdue => {
                let heartbeat_ts = branch_timestamps(&mut wall_clock, &oracle);
                let strategy_cfg = shared_strategy_cfg.borrow().clone();
                let desired_symbols = required_bbwick_symbols_for_strategy_cfg(cfg, &strategy_cfg);
                if desired_symbols != runtime.required_symbols {
                    let msg = format!(
                        "live oracle symbol set changed; restart required previous={} next={}",
                        runtime.required_symbols.join(","),
                        desired_symbols.join(",")
                    );
                    contain_runtime_fault(
                        obs, &mut runtime, oracle_runtime_tx.as_ref(),
                        heartbeat_ts.wall_ms, heartbeat_ts.ingest_mono_ns, msg,
                    ).await;
                    continue;
                }
                let changed = runtime.refresh_readiness(
                    strategy_cfg.max_oracle_staleness_ms,
                    heartbeat_ts.ingest_mono_ns,
                );
                if changed {
                    if let Some(oracle_runtime_tx) = oracle_runtime_tx.as_ref() {
                        oracle_runtime_tx.send_replace(runtime.supervisor_status());
                    }
                }
                runtime.emit_status(obs, heartbeat_ts.wall_ms, heartbeat_ts.ingest_mono_ns, changed);
            }
            changed = portfolio_rx.changed(), if portfolio_open && !strategy_deadline_overdue => {
                if changed.is_err() {
                    portfolio_open = false;
                    let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                    contain_runtime_fault(
                        obs, &mut runtime, oracle_runtime_tx.as_ref(),
                        event_ts.wall_ms, event_ts.ingest_mono_ns,
                        "bbwick_portfolio_feed_closed".to_string(),
                    ).await;
                    continue;
                }
                let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                dispatch_strategy_cycle(
                    StrategyCycleKind::Portfolio,
                    cfg, run_id, actors, exec, intents_tx, split_tx, &ctf_inventory_runtime,
                    &ctf_risk_views, terminal_merge_tx, cancel_requests_tx, *pause_rx.borrow(),
                    &last_ticks, obs, capture, &mut strategy_status_emit_by_symbol,
                    &mut managed_tokens_by_symbol, &mut next_execution_intent_id,
                    &mut consumption_sequence,
                    event_ts, &mut runtime, oracle_runtime_tx.as_ref(),
                ).await;
            }
            changed = actors.odds_changed(), if odds_watch_open && !strategy_deadline_overdue => {
                let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                if changed.is_err() {
                    contain_odds_watch_closed(
                        &mut odds_watch_open, obs, &mut runtime,
                        oracle_runtime_tx.as_ref(), event_ts,
                    ).await;
                    continue;
                }
                dispatch_strategy_cycle(
                    StrategyCycleKind::Polymarket,
                    cfg, run_id, actors, exec, intents_tx, split_tx, &ctf_inventory_runtime,
                    &ctf_risk_views, terminal_merge_tx, cancel_requests_tx, *pause_rx.borrow(),
                    &last_ticks, obs, capture, &mut strategy_status_emit_by_symbol,
                    &mut managed_tokens_by_symbol, &mut next_execution_intent_id,
                    &mut consumption_sequence,
                    event_ts, &mut runtime, oracle_runtime_tx.as_ref(),
                ).await;
            }
            _changed = async {
                match polymarket_bridge.as_mut() {
                    Some(bridge) => bridge.exit_changed().await,
                    None => std::future::pending().await,
                }
            }, if !strategy_deadline_overdue => {
                let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                let exit_error = polymarket_bridge
                    .as_ref()
                    .and_then(EnginePolymarketBridge::exit_error)
                    .unwrap_or_else(|| "bbwick_polymarket_bridge_closed".to_string());
                let msg = format!("bbwick_polymarket_bridge_exited error={exit_error}");
                if !shutdown.is_cancelled() {
                    contain_runtime_fault(
                        obs, &mut runtime, oracle_runtime_tx.as_ref(),
                        event_ts.wall_ms, event_ts.ingest_mono_ns, msg,
                    ).await;
                    if let Some(bridge) = polymarket_bridge.take() {
                        bridge.shutdown().await;
                    }
                }
            }
            received = async {
                match top_of_book_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            }, if !strategy_deadline_overdue => {
                let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                match received {
                    Ok(tick) => {
                        let handle_result = handle_top_of_book(
                            tick,
                            event_ts,
                            cfg,
                            &shared_strategy_cfg,
                            &active_symbols,
                            &mut runtime,
                            oracle_runtime_tx.as_ref(),
                            obs,
                            capture,
                            actors,
                            run_id,
                            exec,
                            intents_tx,
                            split_tx,
                            &ctf_inventory_runtime,
                            &ctf_risk_views,
                            terminal_merge_tx,
                            cancel_requests_tx,
                            *pause_rx.borrow(),
                            &mut last_ticks,
                            &mut strategy_status_emit_by_symbol,
                            &mut managed_tokens_by_symbol,
                            &mut next_execution_intent_id,
                            &mut consumption_sequence,
                            ui_tick_projection.as_ref(),
                        ).await;
                        let fault = match handle_result {
                            Ok(None) => None,
                            Ok(Some(msg)) => Some(msg),
                            Err(err) => Some(format!("bbwick_top_of_book_cycle_failed error={err:#}")),
                        };
                        if let Some(msg) = fault {
                            contain_runtime_fault(
                                obs, &mut runtime, oracle_runtime_tx.as_ref(),
                                event_ts.wall_ms,
                                event_ts.ingest_mono_ns, msg,
                            ).await;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        log_oracle_lagged(event_ts.wall_ms, skipped);
                        last_ticks.clear();
                        runtime.reset_oracle_epoch_state();
                        actors.reset_oracle_epoch_state();
                        let max_oracle_staleness_ms =
                            shared_strategy_cfg.borrow().max_oracle_staleness_ms;
                        let changed = runtime
                            .refresh_readiness(max_oracle_staleness_ms, event_ts.ingest_mono_ns);
                        if let Some(oracle_runtime_tx) = oracle_runtime_tx.as_ref() {
                            oracle_runtime_tx.send_replace(runtime.supervisor_status());
                        }
                        runtime.emit_status(
                            obs,
                            event_ts.wall_ms,
                            event_ts.ingest_mono_ns,
                            changed,
                        );
                        if let Some(rx) = top_of_book_rx.as_mut() {
                            *rx = rx.resubscribe();
                        }
                    }
                    Err(err) => {
                        let msg = top_of_book_receive_error_message(&err);
                        contain_runtime_fault(
                            obs, &mut runtime, oracle_runtime_tx.as_ref(),
                            event_ts.wall_ms, event_ts.ingest_mono_ns, msg,
                        ).await;
                        top_of_book_rx = None;
                    }
                }
            }
            received = async {
                match step_rx.as_mut() {
                    Some(rx) => rx.changed().await,
                    None => std::future::pending().await,
                }
            }, if !strategy_deadline_overdue => {
                let event_ts = branch_timestamps(&mut wall_clock, &oracle);
                match received {
                    Ok(()) => {
                        let Some(rx) = step_rx.as_mut() else {
                            continue;
                        };
                        let step = rx.borrow_and_update().clone();
                        let Some(symbol) = cached_canonical_bbwick_symbol(
                            &mut runtime.canonical_symbol_cache,
                            &step.symbol,
                        ) else {
                            let msg = format!(
                                "bbwick_oracle_drop_step_empty_symbol timeframe={}",
                                step.timeframe
                            );
                            log_empty_step_symbol(event_ts.wall_ms, &step.timeframe);
                            runtime.last_error = Some(msg);
                            if let Some(oracle_runtime_tx) = oracle_runtime_tx.as_ref() {
                                oracle_runtime_tx.send_replace(runtime.supervisor_status());
                            }
                            runtime.emit_status(obs, event_ts.wall_ms, event_ts.ingest_mono_ns, true);
                            continue;
                        };
                        if !active_symbols.is_empty() && !active_symbols.contains(symbol) {
                            continue;
                        }
                        let mut normalized_step = step.clone();
                        normalized_step.symbol = symbol.to_string();
                        record_oracle_step(cfg, obs, &normalized_step);
                    }
                    Err(_) => {
                        let msg = "bbwick_oracle_step_feed_closed".to_string();
                        contain_runtime_fault(
                            obs, &mut runtime, oracle_runtime_tx.as_ref(),
                            event_ts.wall_ms, event_ts.ingest_mono_ns, msg,
                        ).await;
                        step_rx = None;
                    }
                }
            }
            _ = wait_for_strategy_deadline(next_strategy_deadline_ms, scheduler_ts.wall_ms) => {
                let deadline_ts = branch_timestamps(&mut wall_clock, &oracle);
                dispatch_strategy_cycle(
                    StrategyCycleKind::Deadline,
                    cfg, run_id, actors, exec, intents_tx, split_tx, &ctf_inventory_runtime,
                    &ctf_risk_views, terminal_merge_tx, cancel_requests_tx, *pause_rx.borrow(),
                    &last_ticks, obs, capture, &mut strategy_status_emit_by_symbol,
                    &mut managed_tokens_by_symbol, &mut next_execution_intent_id,
                    &mut consumption_sequence,
                    deadline_ts, &mut runtime, oracle_runtime_tx.as_ref(),
                ).await;
            }
        }
    }

    shutdown_runtime_connections(
        &mut polymarket_bridge,
        &mut engine_state_forward_task,
        &mut oracle,
        &mut top_of_book_rx,
        &mut step_rx,
        &mut ui_tick_projection,
    )
    .await;
    chainlink_runtime.join().await;
    if oracle_history.persistence_failed() {
        tracing::error!(target: "app.bbwick_runtime", "bbwick_history_persistence_failed");
        anyhow::bail!("bbwick canonical history persistence failed");
    }
    Ok(())
}

use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures_util::FutureExt as _;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_hft::build_info::current_build_info;
use polymarket_hft::capture::events::{ReplayRiskSnapshotCapture, ReplaySeedCapture};
use polymarket_hft::capture::session::CaptureMsg;
use polymarket_hft::config::{AppConfig, SharedStrategyConfig};
use polymarket_hft::ctf_inventory::CtfInventoryRuntime;
use polymarket_hft::event_ledger::{
    append_event, runtime_start_event, snapshot_config_path, RunIdProvider, RuntimeStartEventInput,
};
use polymarket_hft::execution::supervisor::{
    ExecutionSupervisorClock, ExecutionSupervisorRuntime, ExecutionSupervisorStart,
    OracleRuntimeStatus,
};
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::observability::{ObservabilityOperatorSurfaceStart, ObservabilityRuntime};
use polymarket_hft::polymarket::order_context::OrderFillContextRegistry;
use polymarket_hft::polymarket::state::MarketProjection;
use polymarket_hft::polymarket::user_ws::{PolymarketUserWsRuntime, PolymarketUserWsStart};
use polymarket_hft::portfolio::PortfolioRuntime;
use polymarket_hft::reclaim_merge::{spawn_reclaim_task, CtfTerminalMergeCommand};
use polymarket_hft::risk::recon::build_recon_http_client;
use polymarket_hft::risk::RiskHandle;
use polymarket_hft::runtime_control::{RuntimeConfigProjection, SharedRuntimeConfigState};
use polymarket_hft::runtime_files::{RuntimeFileRuntime, RuntimeFileStart};
use polymarket_hft::trade_session_ledger::initialize_run_trade_ledger;
use tokio::sync::{mpsc, watch};

use crate::app::bbwick_runtime::{run_bbwick_mode, RunBbwickModeArgs};
use crate::app::bootstrap::{now_ms, MAIN_CONFIG_PATH};
use crate::app::config_reload::spawn_config_reload_task;
use crate::app::ctf_inventory::spawn_ctf_inventory_task;
use crate::app::maker_rebate_receipts::{spawn_maker_rebate_receipt_task, MakerRebateReceiptStart};
use crate::app::shutdown::{spawn_freeze_cancel_task, spawn_signal_task};

mod forwarding;
pub(crate) mod startup;
use forwarding::spawn_capture_forwarding_tasks;
use startup::{
    append_shutdown_failure, ensure_optional_engine_history_ready,
    latest_operator_pause_file_enabled, load_live_oracle_learning, publish_terminal_result,
    spawn_execution_heartbeat_monitor_task, start_live_capture_session, SessionCancellationGuard,
};

pub(crate) async fn run_main_runtime(
    cfg: AppConfig,
    signal_incident_rx: watch::Receiver<u64>,
) -> Result<()> {
    let process_start_ms = now_ms();
    let run_id = RunIdProvider::start_new(process_start_ms)?;
    initialize_run_trade_ledger(&run_id)?;

    let mode = if cfg.bbwick_oracle.enabled {
        "bbwick_oracle"
    } else if cfg.polymarket.market_data_enabled {
        "polymarket_ws"
    } else {
        "market_data_disabled"
    };
    let mut observability_rt = ObservabilityRuntime::start(&cfg.observability, mode).await?;
    let obs = observability_rt.handle();
    let global_shutdown = obs.shutdown_token();
    let _cancellation_guard = SessionCancellationGuard(global_shutdown.clone());
    let shutdown = global_shutdown.child_token();
    let capture_shutdown = global_shutdown.child_token();
    let delivery_shutdown = global_shutdown.child_token();
    let financial_shutdown = global_shutdown.child_token();
    let portfolio_shutdown = global_shutdown.child_token();
    let supervisor_shutdown = global_shutdown.child_token();
    let forwarding_shutdown = global_shutdown.child_token();
    let runtime_files_shutdown = global_shutdown.child_token();

    let mut current_config_hash = None;
    macro_rules! collect_shutdown {
        ($result:ident, $owner:literal, $completion:expr) => {
            if let Err(error) = $completion {
                append_shutdown_failure(&mut $result, $owner, error.into());
            }
        };
    }
    let (capture, mut capture_task) =
        match start_live_capture_session(&cfg, capture_shutdown.clone()).await {
            Ok(started) => started,
            Err(error) => {
                let mut result = Err(error);
                global_shutdown.cancel();
                collect_shutdown!(
                    result,
                    "observability",
                    observability_rt.shutdown_with_result().await
                );
                return publish_terminal_result(
                    &run_id,
                    process_start_ms,
                    current_config_hash.as_deref(),
                    result,
                );
            }
        };
    macro_rules! startup_try {
        ($expr:expr) => {
            match $expr {
                Ok(value) => value,
                Err(err) => {
                    let mut result: Result<()> = Err(err.into());
                    shutdown.cancel();
                    collect_shutdown!(
                        result,
                        "dashboard",
                        observability_rt.shutdown_operator_surface().await
                    );
                    capture_shutdown.cancel();
                    if let Some(task) = capture_task.take() {
                        collect_shutdown!(
                            result,
                            "capture",
                            task.await
                                .map_err(anyhow::Error::from)
                                .and_then(|result| result)
                        );
                    }
                    global_shutdown.cancel();
                    collect_shutdown!(
                        result,
                        "observability",
                        observability_rt.shutdown_with_result().await
                    );
                    return publish_terminal_result(
                        &run_id,
                        process_start_ms,
                        current_config_hash.as_deref(),
                        result,
                    );
                }
            }
        };
    }
    let (
        selected_oracle_learning_to_validate,
        initial_oracle_learning,
        oracle_learning_continuation_paths,
    ) = startup_try!(load_live_oracle_learning(&cfg).await);
    let live_learning_identity = capture
        .session_dir()
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("live:{run_id}"));
    let oracle_learning_capture_identities: std::collections::BTreeMap<String, Vec<String>> =
        initial_oracle_learning
            .iter()
            .map(|(symbol, snapshot)| {
                let mut identities = snapshot.capture_identities().to_vec();
                if !identities.contains(&live_learning_identity) {
                    identities.push(live_learning_identity.clone());
                }
                (symbol.clone(), identities)
            })
            .collect();
    let initial_oracle_learning_bytes =
        startup_try!(rmp_serde::to_vec_named(&initial_oracle_learning));
    startup_try!(capture.write_initial_oracle_learning(&initial_oracle_learning_bytes));

    startup::publish_startup_status(&cfg, &obs, mode);

    let oracle_history = startup_try!(ensure_optional_engine_history_ready(&cfg).await);

    let (risk, risk_rx) = RiskHandle::new(cfg.risk.clone(), cfg.max_order_qty);
    obs.set_risk(risk_rx.borrow().clone());

    let exec = startup_try!(ExecutionClient::from_config(&cfg).await);
    obs.set_execution_backend(exec.backend_name());
    tracing::info!(
        target: "app.live_runtime",
        backend = %exec.backend_name(),
        "execution_backend_ready"
    );
    let vendor_truth_http = startup_try!(build_recon_http_client(
        "failed to build vendor truth HTTP client"
    ));
    let maker_rebate_data =
        startup_try!(DataClient::new(&cfg.polymarket.data_http_url).map_err(anyhow::Error::from));
    let reclaim_data =
        startup_try!(DataClient::new(&cfg.polymarket.data_http_url).map_err(anyhow::Error::from));
    let shared_strategy_cfg: SharedStrategyConfig =
        watch::channel(cfg.strategy.maker_rebate_mm.clone()).0;
    let operator_pause = startup_try!(latest_operator_pause_file_enabled().await).unwrap_or(false);
    let requested_pause = cfg.polymarket.execution.pause || operator_pause;
    let current_config_snapshot = snapshot_config_path(Path::new(MAIN_CONFIG_PATH)).ok();
    let runtime_config_state = SharedRuntimeConfigState::from_initial_pause(
        &cfg,
        requested_pause,
        current_config_snapshot
            .as_ref()
            .map(|snapshot| snapshot.hash.clone()),
    );
    let pause_reload_rx = runtime_config_state.subscribe_pause();
    let initial_pause = *pause_reload_rx.borrow();
    let build_info = current_build_info();
    let hot_reload_cfg = RuntimeConfigProjection::from_config_with_pause(&cfg, initial_pause);
    let runtime_cfg_fingerprint = hot_reload_cfg.fingerprint().to_string();
    current_config_hash = current_config_snapshot
        .as_ref()
        .map(|snapshot| snapshot.hash.clone());
    let argv_summary = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let runtime_start_record = runtime_start_event(RuntimeStartEventInput {
        run_id: &run_id,
        ts_ms: process_start_ms,
        pid: std::process::id(),
        commit_sha: &build_info.commit_sha,
        commit_subject: &build_info.commit_subject,
        dirty_worktree: build_info.dirty_worktree,
        config_hash: current_config_hash.as_deref(),
        argv_summary: &argv_summary,
        runtime_config_fingerprint: &runtime_cfg_fingerprint,
        changed_paths: &build_info.changed_paths,
        mode,
    });
    startup_try!(append_event(&runtime_start_record));

    let (
        odds_rx,
        engine_state_tx,
        oracle_runtime_tx,
        oracle_runtime_rx,
        market_projection_rx,
        market_projection_tx,
    ) = if cfg.bbwick_oracle.enabled && cfg.polymarket.market_data_enabled {
        let (engine_tx, engine_rx) =
            watch::channel(Arc::new(bbwick_engine::PolymarketOddsState::default()));
        let (oracle_runtime_tx, oracle_runtime_rx) = watch::channel(OracleRuntimeStatus::default());
        let (projection_tx, projection_rx) = watch::channel(MarketProjection::default());
        (
            Some(engine_rx.clone()),
            Some(engine_tx),
            Some(oracle_runtime_tx),
            Some(oracle_runtime_rx),
            Some(projection_rx),
            Some(projection_tx),
        )
    } else {
        (None, None, None, None, None, None)
    };
    let required_market_projection_rx = startup_try!(market_projection_rx
        .clone()
        .ok_or_else(|| anyhow::anyhow!("bbwick-owned market projection runtime missing")));
    if let Err(err) = exec.reclaim_precheck() {
        exec.reclaim_ops_handle()
            .record_redeem_disabled(format!("{err}"));
        startup_try!(Err::<(), _>(anyhow::anyhow!(
            "terminal settlement owner precheck failed: {err:#}"
        )));
    }
    startup_try!(
        observability_rt
            .start_operator_surface(ObservabilityOperatorSurfaceStart {
                cfg: cfg.observability.dashboard.clone(),
                obs: obs.clone(),
                exec: exec.clone(),
                shutdown: shutdown.clone(),
                runtime_config_state: runtime_config_state.clone(),
                risk: Some(risk.clone()),
                price_history_root: cfg.bbwick_oracle.history.resolved_data_root(),
                price_symbols: cfg.bbwick_oracle.resolved_underlying_symbols(),
            })
            .await
    );
    let active = exec.heartbeats_active();
    exec.live_ops().record_heartbeat_sample(Some(active));
    let level = if active {
        "heartbeat_active"
    } else {
        "heartbeat_inactive"
    };
    tracing::info!(target: "app.live_runtime", heartbeat = level, "execution_heartbeat_state");
    if !active {
        tracing::warn!(
            target: "app.live_runtime",
            "live_execution_started_without_active_heartbeats"
        );
    }

    let ctf_inventory_runtime = Arc::new(Mutex::new(CtfInventoryRuntime::default()));
    let (fills_tx, fills_rx) = mpsc::channel(4096);
    let (orders_tx, orders_rx) = mpsc::channel(4096);
    let order_fill_ctx = OrderFillContextRegistry::default();
    let portfolio_rt = PortfolioRuntime::start(
        obs.clone(),
        fills_rx,
        odds_rx.clone(),
        portfolio_shutdown.clone(),
    );
    let portfolio_close = portfolio_rt.close_handle();
    let portfolio_hydration = portfolio_rt.hydration_handle();
    let portfolio_rx = portfolio_rt.state();
    let startup_replay_seed = risk.current_snapshot();
    capture.emit(CaptureMsg::ReplaySeed(ReplaySeedCapture {
        session_ts_ms: now_ms(),
        usdc_balance: startup_replay_seed.usdc_balance,
        trading_collateral_balance: startup_replay_seed.trading_collateral_balance,
        current_wallet_equity: startup_replay_seed.current_wallet_equity,
        current_equity: startup_replay_seed.current_equity,
        max_order_qty: startup_replay_seed.max_order_qty,
        max_market_equity_pct: startup_replay_seed.max_market_equity_pct,
    }));
    let startup_baseline = ReplayRiskSnapshotCapture::from_snapshot(now_ms(), &startup_replay_seed);
    if capture.emit_replay_risk_snapshot(startup_baseline).is_err() {
        tracing::error!(
            target: "app.live_runtime",
            "startup_replay_risk_baseline_enqueue_failed"
        );
    }

    let (risk_fwd_task, portfolio_fwd_task, settlement_refresh_tx) =
        match spawn_capture_forwarding_tasks(
            capture.clone(),
            obs.clone(),
            risk.clone(),
            risk_rx,
            portfolio_rx.clone(),
            exec.clone(),
            vendor_truth_http.clone(),
            cfg.polymarket.resolved_account_rpc_url(),
            odds_rx.clone(),
            forwarding_shutdown.clone(),
        )
        .await
        {
            Ok(tasks) => tasks,
            Err(error) => {
                let mut result = Err(error);
                shutdown.cancel();
                collect_shutdown!(
                    result,
                    "dashboard",
                    observability_rt.shutdown_operator_surface().await
                );
                drop(fills_tx);
                portfolio_shutdown.cancel();
                collect_shutdown!(result, "portfolio", portfolio_rt.shutdown().await);
                capture_shutdown.cancel();
                if let Some(task) = capture_task.take() {
                    collect_shutdown!(
                        result,
                        "capture",
                        task.await
                            .map_err(anyhow::Error::from)
                            .and_then(|result| result)
                    );
                }
                global_shutdown.cancel();
                collect_shutdown!(
                    result,
                    "observability",
                    observability_rt.shutdown_with_result().await
                );
                return publish_terminal_result(
                    &run_id,
                    process_start_ms,
                    current_config_hash.as_deref(),
                    result,
                );
            }
        };

    let signal_task = spawn_signal_task(signal_incident_rx, shutdown.clone());
    let config_reload_task = spawn_config_reload_task(
        obs.clone(),
        risk.clone(),
        shared_strategy_cfg.clone(),
        runtime_config_state.clone(),
        shutdown.clone(),
        run_id.clone(),
        capture.clone(),
    );
    let runtime_file_rt = RuntimeFileRuntime::start(RuntimeFileStart {
        obs: obs.clone(),
        live_ops: exec.live_ops().clone(),
        risk: risk.clone(),
        runtime_config_state: runtime_config_state.clone(),
        capture: capture.clone(),
        shutdown: runtime_files_shutdown.clone(),
        odds_rx: odds_rx.clone(),
    });
    let heartbeat_monitor_task =
        spawn_execution_heartbeat_monitor_task(exec.clone(), shutdown.clone());

    let freeze_cancel_task =
        spawn_freeze_cancel_task(exec.clone(), risk.snapshot(), shutdown.clone());
    let (terminal_merge_tx, terminal_merge_rx) =
        mpsc::channel::<Vec<CtfTerminalMergeCommand>>(1024);
    let reclaim_task = spawn_reclaim_task(
        &cfg,
        exec.clone(),
        obs.clone(),
        reclaim_data,
        portfolio_close,
        ctf_inventory_runtime.clone(),
        terminal_merge_rx,
        settlement_refresh_tx,
        financial_shutdown.clone(),
    );

    let equity_baseline = {
        let snap_rx = risk.snapshot();
        let snap = snap_rx.borrow();
        snap.current_wallet_equity
            .or(Some(snap.current_equity))
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0)
    };

    let polymarket_user_ws_rt = odds_rx.as_ref().map(|odds_rx| PolymarketUserWsStart {
        ws_url: cfg.polymarket.clob_ws_url.clone(),
        auth: exec.polymarket_user_ws_auth(),
        exec: exec.clone(),
        fills_tx,
        orders_tx,
        order_fill_ctx: order_fill_ctx.clone(),
        odds_rx: odds_rx.clone(),
        obs: obs.clone(),
        capture: capture.clone(),
        shutdown: delivery_shutdown.clone(),
    });
    let (polymarket_user_ws_rt, user_ws_start_error) = match polymarket_user_ws_rt {
        Some(start) => match PolymarketUserWsRuntime::start(start).await {
            Ok(runtime) => (Some(runtime), None),
            Err(err) => (None, Some(err)),
        },
        None => (None, None),
    };

    let maker_rebate_receipt_task = spawn_maker_rebate_receipt_task(MakerRebateReceiptStart {
        cfg: cfg.clone(),
        shutdown: financial_shutdown.clone(),
        http: vendor_truth_http.clone(),
        data: maker_rebate_data,
        tracked_wallet: exec.polymarket_tracked_wallet(),
    });

    let (blocked_markets_tx, blocked_markets_rx) =
        watch::channel::<HashSet<String>>(HashSet::new());

    let mut exec_supervisor_rt = ExecutionSupervisorRuntime::start(ExecutionSupervisorStart {
        cfg: cfg.clone(),
        exec: exec.clone(),
        risk: risk.clone(),
        ctf_inventory_runtime: ctf_inventory_runtime.clone(),
        shared_strategy_cfg: shared_strategy_cfg.clone(),
        order_fill_ctx: order_fill_ctx.clone(),
        pause_rx: pause_reload_rx,
        runtime_control_rx: runtime_config_state.subscribe(),
        blocked_markets_tx,
        portfolio_rx: portfolio_rx.clone(),
        odds_rx: odds_rx.clone(),
        oracle_runtime_rx: oracle_runtime_rx.clone(),
        market_projection_rx: market_projection_rx.clone(),
        order_updates_rx: orders_rx,
        obs: obs.clone(),
        capture: capture.clone(),
        shutdown: supervisor_shutdown.clone(),
        clock: ExecutionSupervisorClock::Live,
    });
    let intents_tx = exec_supervisor_rt.intents();
    let open_orders_rx = exec_supervisor_rt.open_orders();
    let (split_tx, split_rx) = mpsc::channel(1024);
    let ctf_risk_views = crate::app::ctf_inventory::risk_reservation::CentralCtfRiskViews {
        market_projection_rx: required_market_projection_rx.clone(),
        risk: risk.clone(),
    };
    let (ctf_inventory_task, ctf_inventory_start_error) = match spawn_ctf_inventory_task(
        exec.clone(),
        portfolio_hydration.clone(),
        ctf_inventory_runtime.clone(),
        runtime_config_state.clone(),
        ctf_risk_views.market_projection_rx.clone(),
        shared_strategy_cfg.clone(),
        split_rx,
        capture.clone(),
        shutdown.clone(),
    ) {
        Ok(task) => (Some(task), None),
        Err(err) => (None, Some(err)),
    };
    let cancel_requests_tx = exec_supervisor_rt.cancel_requests();

    let startup_error = user_ws_start_error.or(ctf_inventory_start_error);
    let mut run_result = match startup_error {
        Some(err) => Err(err),
        None => match AssertUnwindSafe(async {
            let (odds_rx, oracle_history) =
                odds_rx.clone().zip(oracle_history).ok_or_else(|| {
                    anyhow::anyhow!("maker_rebate_mm requires bbwick-owned polymarket odds runtime")
                })?;
            let mut actors = polymarket_hft::strategy_runtime::actors::MakerRebateMmActorSet::new(
                polymarket_hft::strategy_runtime::actors::MakerRebateMmActorSetInit {
                    cfg: cfg.strategy.maker_rebate_mm.clone(),
                    equity_baseline_usdt: equity_baseline,
                    risk_rx: risk.snapshot(),
                    risk: risk.clone(),
                    blocked_markets_rx: blocked_markets_rx.clone(),
                    open_orders_rx: open_orders_rx.clone(),
                    bbwick_oracle_cfg: cfg.bbwick_oracle.clone(),
                    oracle_history: oracle_history.clone(),
                    odds_rx,
                    market_projection_rx: required_market_projection_rx,
                    portfolio_rx: portfolio_rx.clone(),
                    shared_cfg: shared_strategy_cfg.clone(),
                    ctf_inventory_runtime: ctf_inventory_runtime.clone(),
                    selected_oracle_learning_to_validate: selected_oracle_learning_to_validate
                        .clone(),
                    initial_oracle_learning: initial_oracle_learning.clone(),
                    oracle_learning_continuation_paths: oracle_learning_continuation_paths.clone(),
                    oracle_learning_capture_identities: oracle_learning_capture_identities.clone(),
                },
            )?;
            run_bbwick_mode(
                RunBbwickModeArgs {
                    run_id: &run_id,
                    cfg: &cfg,
                    oracle_history,
                    shared_strategy_cfg: shared_strategy_cfg.clone(),
                    actors: &mut actors,
                    exec: &exec,
                    intents_tx: &intents_tx,
                    split_tx: &split_tx,
                    terminal_merge_tx: &terminal_merge_tx,
                    cancel_requests_tx: &cancel_requests_tx,
                    runtime_config_state: runtime_config_state.clone(),
                    engine_state_tx: engine_state_tx.clone(),
                    oracle_runtime_tx: oracle_runtime_tx.clone(),
                    market_projection_tx: market_projection_tx.clone(),
                    portfolio_rx: portfolio_rx.clone(),
                    obs: &obs,
                    capture: &capture,
                    shutdown: shutdown.clone(),
                },
                ctf_inventory_runtime,
                ctf_risk_views,
            )
            .await
        })
        .catch_unwind()
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("live operational panic contained")),
        },
    };

    if let Err(err) = &run_result {
        tracing::error!(
            target: "app.live_runtime",
            error = %err,
            "trader_runtime_error"
        );
    }

    tracing::info!(target: "app.live_runtime", "runtime_shutdown");

    shutdown.cancel();
    collect_shutdown!(
        run_result,
        "dashboard",
        observability_rt.shutdown_operator_surface().await
    );
    collect_shutdown!(run_result, "signal observer", signal_task.await);
    collect_shutdown!(run_result, "config reload", config_reload_task.await);
    collect_shutdown!(
        run_result,
        "heartbeat monitor",
        heartbeat_monitor_task.await
    );
    collect_shutdown!(run_result, "freeze cancellation", freeze_cancel_task.await);
    drop(intents_tx);
    drop(cancel_requests_tx);
    collect_shutdown!(
        run_result,
        "supervisor upstream",
        exec_supervisor_rt.finish_upstream().await
    );

    delivery_shutdown.cancel();
    if let Some(rt) = polymarket_user_ws_rt {
        collect_shutdown!(run_result, "authenticated delivery", rt.shutdown().await);
    }
    drop(split_tx);
    drop(terminal_merge_tx);
    financial_shutdown.cancel();
    if let Some(task) = ctf_inventory_task {
        collect_shutdown!(
            run_result,
            "CTF inventory",
            task.await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
        );
    }
    collect_shutdown!(
        run_result,
        "reclaim",
        reclaim_task
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
    );
    collect_shutdown!(
        run_result,
        "maker rebate",
        maker_rebate_receipt_task
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
    );
    portfolio_shutdown.cancel();
    collect_shutdown!(run_result, "portfolio", portfolio_rt.shutdown().await);
    supervisor_shutdown.cancel();
    collect_shutdown!(
        run_result,
        "supervisor",
        exec_supervisor_rt.shutdown().await
    );
    forwarding_shutdown.cancel();
    collect_shutdown!(
        run_result,
        "portfolio forwarding",
        portfolio_fwd_task
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
    );
    collect_shutdown!(run_result, "risk forwarding", risk_fwd_task.await);
    collect_shutdown!(
        run_result,
        "runtime files",
        runtime_file_rt.shutdown().await
    );
    capture_shutdown.cancel();
    if let Some(task) = capture_task.take() {
        collect_shutdown!(
            run_result,
            "capture",
            task.await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
        );
    }
    global_shutdown.cancel();
    collect_shutdown!(
        run_result,
        "observability",
        observability_rt.shutdown_with_result().await
    );

    publish_terminal_result(
        &run_id,
        process_start_ms,
        current_config_hash.as_deref(),
        run_result,
    )
}

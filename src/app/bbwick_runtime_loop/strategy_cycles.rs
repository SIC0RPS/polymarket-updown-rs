use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::AppConfig;
use polymarket_hft::ctf_inventory::CtfInventoryRuntime;
use polymarket_hft::execution::supervisor::{ExecutionIntentBatch, OracleRuntimeStatus};
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::observability::Observability;
use polymarket_hft::reclaim_merge::CtfTerminalMergeCommand;
use polymarket_hft::strategy_runtime::actors::MakerRebateMmActorSet;
use polymarket_hft::strategy_runtime::runtime_driver::RuntimeInput;
use polymarket_hft::types::MarketTick;
use tokio::sync::{mpsc, watch};

use super::super::bbwick_runtime_status::BbwickOracleRuntimeState;
use super::super::{handle_tick, StrategyStatusEmitState};
use super::runtime_connections::contain_runtime_fault;
use super::timing::BranchTimestamps;
use crate::app::ctf_inventory::risk_reservation::CentralCtfRiskViews;
use crate::app::ctf_inventory::CtfSplitCommand;

#[derive(Clone, Copy)]
pub(super) enum StrategyCycleKind {
    Portfolio,
    Deadline,
    Polymarket,
}

impl StrategyCycleKind {
    fn error_context(self) -> &'static str {
        match self {
            Self::Portfolio => "bbwick_portfolio_cycle_failed",
            Self::Deadline => "bbwick_strategy_deadline_cycle_failed",
            Self::Polymarket => "bbwick_polymarket_cycle_failed",
        }
    }
}

pub(super) async fn contain_odds_watch_closed(
    odds_watch_open: &mut bool,
    obs: &Observability,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    event_ts: BranchTimestamps,
) {
    *odds_watch_open = false;
    contain_runtime_fault(
        obs,
        runtime,
        oracle_runtime_tx,
        event_ts.wall_ms,
        event_ts.ingest_mono_ns,
        "bbwick_polymarket_odds_feed_closed".to_string(),
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn dispatch_strategy_cycle(
    kind: StrategyCycleKind,
    cfg: &AppConfig,
    run_id: &str,
    actors: &mut MakerRebateMmActorSet,
    exec: &ExecutionClient,
    intents_tx: &mpsc::UnboundedSender<ExecutionIntentBatch>,
    split_tx: &mpsc::Sender<Vec<CtfSplitCommand>>,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    ctf_risk_views: &CentralCtfRiskViews,
    terminal_merge_tx: &mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    pause: bool,
    last_ticks: &BTreeMap<String, MarketTick>,
    obs: &Observability,
    capture: &MaybeCapture,
    strategy_status_emit_by_symbol: &mut BTreeMap<String, StrategyStatusEmitState>,
    managed_tokens_by_symbol: &mut BTreeMap<String, HashSet<String>>,
    next_execution_intent_id: &mut u64,
    consumption_sequence: &mut u64,
    event_ts: BranchTimestamps,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
) {
    let result = drive_strategy_cycle(
        kind,
        cfg,
        run_id,
        actors,
        exec,
        intents_tx,
        split_tx,
        ctf_inventory_runtime,
        ctf_risk_views,
        terminal_merge_tx,
        cancel_requests_tx,
        pause,
        last_ticks,
        obs,
        capture,
        strategy_status_emit_by_symbol,
        managed_tokens_by_symbol,
        next_execution_intent_id,
        consumption_sequence,
        event_ts.wall_ms,
        event_ts.ingest_mono_ns,
    )
    .await;
    let fault = match result {
        Ok(None) => None,
        Ok(Some(msg)) => Some(msg),
        Err(err) => Some(format!("{} error={err:#}", kind.error_context())),
    };
    if let Some(msg) = fault {
        contain_runtime_fault(
            obs,
            runtime,
            oracle_runtime_tx,
            event_ts.wall_ms,
            event_ts.ingest_mono_ns,
            msg,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_strategy_cycle(
    kind: StrategyCycleKind,
    cfg: &AppConfig,
    run_id: &str,
    actors: &mut MakerRebateMmActorSet,
    exec: &ExecutionClient,
    intents_tx: &mpsc::UnboundedSender<ExecutionIntentBatch>,
    split_tx: &mpsc::Sender<Vec<CtfSplitCommand>>,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    ctf_risk_views: &CentralCtfRiskViews,
    terminal_merge_tx: &mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    pause: bool,
    last_ticks: &BTreeMap<String, MarketTick>,
    obs: &Observability,
    capture: &MaybeCapture,
    strategy_status_emit_by_symbol: &mut BTreeMap<String, StrategyStatusEmitState>,
    managed_tokens_by_symbol: &mut BTreeMap<String, HashSet<String>>,
    next_execution_intent_id: &mut u64,
    consumption_sequence: &mut u64,
    wall_ms: u64,
    ingest_mono_ns: u64,
) -> Result<Option<String>> {
    actors.refresh_views(wall_ms)?;
    let mut cycle_admission = actors.cycle_admission();
    let central_command_ownership = actors.command_ownership();
    let mut drove_actor = false;
    for symbol in actors.actor_symbols() {
        let Some(retained_tick) = last_ticks.get(symbol).cloned() else {
            continue;
        };
        let should_drive = {
            let actor = actors.actor_for_underlying_mut(symbol).ok_or_else(|| {
                anyhow::anyhow!("strategy actor disappeared during cycle symbol={symbol}")
            })?;
            match kind {
                StrategyCycleKind::Portfolio => true,
                StrategyCycleKind::Deadline => actor
                    .next_strategy_deadline_ms()
                    .is_some_and(|deadline_ms| deadline_ms <= wall_ms),
                StrategyCycleKind::Polymarket => actor.has_unconsumed_odds(),
            }
        };
        if !should_drive {
            continue;
        }
        if drove_actor {
            actors.refresh_symbol_risk_view(symbol, wall_ms)?;
        }
        let actor = actors.actor_for_underlying_mut(symbol).ok_or_else(|| {
            anyhow::anyhow!("strategy actor disappeared during cycle symbol={symbol}")
        })?;
        let input = match kind {
            StrategyCycleKind::Portfolio => RuntimeInput::PortfolioChanged {
                last_tick: retained_tick,
                now_ms: wall_ms,
                now_ingest_mono_ns: ingest_mono_ns,
            },
            StrategyCycleKind::Deadline => RuntimeInput::Heartbeat {
                tick: retained_tick,
                now_ms: wall_ms,
                now_ingest_mono_ns: ingest_mono_ns,
            },
            StrategyCycleKind::Polymarket => RuntimeInput::PolymarketDataChanged {
                last_tick: retained_tick,
                now_ms: wall_ms,
                now_ingest_mono_ns: ingest_mono_ns,
            },
        };
        let status_emit = strategy_status_emit_by_symbol
            .entry(actor.symbol.to_string())
            .or_default();
        let error = handle_tick(
            cfg,
            run_id,
            actor,
            &central_command_ownership,
            exec,
            intents_tx,
            split_tx,
            ctf_inventory_runtime,
            ctf_risk_views,
            terminal_merge_tx,
            cancel_requests_tx,
            pause,
            input,
            obs,
            capture,
            status_emit,
            managed_tokens_by_symbol,
            next_execution_intent_id,
            consumption_sequence,
            &mut cycle_admission,
            wall_ms,
        )
        .await?;
        drove_actor = true;
        if let Some(error) = error {
            return Ok(Some(error));
        }
    }
    Ok(None)
}

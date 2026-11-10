use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use bbwick_engine::CanonicalTopOfBook;
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::{AppConfig, SharedStrategyConfig};
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
use super::super::bbwick_runtime_streams::to_market_tick;
use super::super::bbwick_runtime_symbols::cached_canonical_bbwick_symbol;
use super::super::{handle_tick, StrategyStatusEmitState};
use super::timing::BranchTimestamps;
use super::ui_ticks::UiTickProjection;
use crate::app::ctf_inventory::risk_reservation::CentralCtfRiskViews;
use crate::app::ctf_inventory::CtfSplitCommand;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_top_of_book(
    tick: CanonicalTopOfBook,
    event_ts: BranchTimestamps,
    cfg: &AppConfig,
    shared_strategy_cfg: &SharedStrategyConfig,
    active_symbols: &HashSet<String>,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    obs: &Observability,
    capture: &MaybeCapture,
    actors: &mut MakerRebateMmActorSet,
    run_id: &str,
    exec: &ExecutionClient,
    intents_tx: &mpsc::UnboundedSender<ExecutionIntentBatch>,
    split_tx: &mpsc::Sender<Vec<CtfSplitCommand>>,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    ctf_risk_views: &CentralCtfRiskViews,
    terminal_merge_tx: &mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    pause: bool,
    last_ticks: &mut BTreeMap<String, MarketTick>,
    strategy_status_emit_by_symbol: &mut BTreeMap<String, StrategyStatusEmitState>,
    managed_tokens_by_symbol: &mut BTreeMap<String, HashSet<String>>,
    next_execution_intent_id: &mut u64,
    consumption_sequence: &mut u64,
    ui_tick_projection: Option<&UiTickProjection>,
) -> Result<Option<String>> {
    let Some(symbol) =
        cached_canonical_bbwick_symbol(&mut runtime.canonical_symbol_cache, &tick.symbol)
    else {
        let msg = format!("bbwick_oracle_drop_empty_symbol seq_no={}", tick.seq_no);
        tracing::warn!(
            target: "app.bbwick_runtime",
            ts_ms = event_ts.wall_ms,
            seq_no = tick.seq_no,
            "bbwick_oracle_drop_empty_symbol"
        );
        runtime.last_error = Some(msg);
        if let Some(oracle_runtime_tx) = oracle_runtime_tx {
            oracle_runtime_tx.send_replace(runtime.supervisor_status());
        }
        runtime.emit_status(obs, event_ts.wall_ms, event_ts.ingest_mono_ns, true);
        return Ok(None);
    };
    if !active_symbols.is_empty() && !active_symbols.contains(symbol) {
        let msg = format!("bbwick_oracle_drop_disabled_symbol symbol={symbol}");
        tracing::debug!(
            target: "app.bbwick_runtime",
            ts_ms = event_ts.wall_ms,
            symbol = %symbol,
            "bbwick_oracle_drop_disabled_symbol"
        );
        runtime.last_error = Some(msg);
        if let Some(oracle_runtime_tx) = oracle_runtime_tx {
            oracle_runtime_tx.send_replace(runtime.supervisor_status());
        }
        runtime.emit_status(obs, event_ts.wall_ms, event_ts.ingest_mono_ns, true);
        return Ok(None);
    }
    if !(tick.bid_px.is_finite()
        && tick.ask_px.is_finite()
        && tick.bid_sz.is_finite()
        && tick.ask_sz.is_finite()
        && tick.bid_px > 0.0
        && tick.ask_px >= tick.bid_px
        && tick.bid_sz > 0.0
        && tick.ask_sz > 0.0)
    {
        let msg = format!(
            "bbwick_oracle_drop_incomplete_top_of_book symbol={symbol} bid={} ask={} bid_sz={} ask_sz={}",
            tick.bid_px, tick.ask_px, tick.bid_sz, tick.ask_sz
        );
        tracing::warn!(
            target: "app.bbwick_runtime",
            ts_ms = event_ts.wall_ms,
            symbol = %symbol,
            bid = tick.bid_px,
            ask = tick.ask_px,
            bid_sz = tick.bid_sz,
            ask_sz = tick.ask_sz,
            "bbwick_oracle_drop_incomplete_top_of_book"
        );
        runtime.last_error = Some(msg);
        if let Some(oracle_runtime_tx) = oracle_runtime_tx {
            oracle_runtime_tx.send_replace(runtime.supervisor_status());
        }
        runtime.emit_status(obs, event_ts.wall_ms, event_ts.ingest_mono_ns, true);
        return Ok(None);
    }
    let mut normalized_tick = tick;
    let normalized_symbol = symbol.to_string();
    normalized_tick.symbol = normalized_symbol.clone();
    runtime.note_tick(
        normalized_symbol.as_str(),
        normalized_tick.source_ts_ns / 1_000_000,
        normalized_tick.ingest_mono_ns,
    );
    let status_ingest_now = event_ts.ingest_mono_ns.max(normalized_tick.ingest_mono_ns);
    let max_oracle_staleness_ms = shared_strategy_cfg.borrow().max_oracle_staleness_ms;
    let changed = runtime.refresh_readiness(max_oracle_staleness_ms, status_ingest_now);
    if changed {
        if let Some(oracle_runtime_tx) = oracle_runtime_tx {
            oracle_runtime_tx.send_replace(runtime.supervisor_status());
        }
    }
    runtime.emit_status(obs, event_ts.wall_ms, status_ingest_now, changed);
    let ui_values = (
        ui_tick_projection.and_then(|projection| projection.slot_index(&normalized_symbol)),
        normalized_tick.bid_px,
        normalized_tick.ask_px,
        normalized_tick.bid_sz,
        normalized_tick.ask_sz,
    );
    let tick = to_market_tick(normalized_tick, normalized_symbol, event_ts.wall_ms);
    actors.refresh_views(event_ts.wall_ms)?;
    let mut cycle_admission = actors.cycle_admission();
    let central_command_ownership = actors.command_ownership();
    let actor = actors
        .actor_for_oracle_symbol_mut(&tick.instrument)
        .ok_or_else(|| anyhow::anyhow!("bbwick tick has no symbol actor: {}", tick.instrument))?;
    last_ticks.insert(actor.symbol.to_string(), tick.clone());
    let status_emit = strategy_status_emit_by_symbol
        .entry(actor.symbol.to_string())
        .or_default();
    let handle_result = handle_tick(
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
        RuntimeInput::AcceptedTick { tick },
        obs,
        capture,
        status_emit,
        managed_tokens_by_symbol,
        next_execution_intent_id,
        consumption_sequence,
        &mut cycle_admission,
        event_ts.wall_ms,
    )
    .await?;
    if let (Some(projection), Some(index)) = (ui_tick_projection, ui_values.0) {
        projection.observe(
            index,
            event_ts.wall_ms,
            ui_values.1,
            ui_values.2,
            ui_values.3,
            ui_values.4,
        );
    }
    Ok(handle_result)
}

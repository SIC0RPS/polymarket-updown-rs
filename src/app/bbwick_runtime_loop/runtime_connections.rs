use bbwick_engine::state::LastCanonicalBid;
use bbwick_engine::{CanonicalTopOfBook, OracleEngine};
use polymarket_client_sdk::types::U256;
use polymarket_hft::execution::http_support::advance_capped_full_jitter_backoff_ms;
use polymarket_hft::execution::supervisor::OracleRuntimeStatus;
use polymarket_hft::polymarket::engine_bridge::EnginePolymarketBridge;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use super::super::bbwick_runtime_status::BbwickOracleRuntimeState;
use super::ui_ticks::UiTickProjection;

type RetainedBids = HashMap<U256, LastCanonicalBid>;

pub(super) fn log_oracle_connecting(ts_ms: u64, required_symbols: &[String]) {
    tracing::info!(target: "app.bbwick_runtime", ts_ms, "mode_selected_bbwick_oracle");
    tracing::info!(
        target: "app.bbwick_runtime",
        ts_ms,
        required_symbols = %required_symbols.join(","),
        "bbwick_oracle_connecting"
    );
}

pub(super) fn log_oracle_connected(ts_ms: u64) {
    tracing::info!(target: "app.bbwick_runtime", ts_ms, "bbwick_oracle_connected");
}

pub(super) fn publish_connected_oracle(
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    shared_strategy_cfg: &polymarket_hft::config::SharedStrategyConfig,
    obs: &polymarket_hft::observability::Observability,
    started: &OracleEngine,
    wall_ms: u64,
) {
    let ingest_now_ns = started.current_ingest_mono_ns();
    let mut changed = runtime.mark_connected(true);
    runtime.last_error = None;
    let max_oracle_staleness_ms = shared_strategy_cfg.borrow().max_oracle_staleness_ms;
    changed |= runtime.refresh_readiness(max_oracle_staleness_ms, ingest_now_ns);
    if let Some(oracle_runtime_tx) = oracle_runtime_tx {
        oracle_runtime_tx.send_replace(runtime.supervisor_status());
    }
    runtime.emit_status(obs, wall_ms, ingest_now_ns, changed);
    log_oracle_connected(wall_ms);
}

pub(super) fn log_oracle_lagged(ts_ms: u64, skipped: u64) {
    tracing::warn!(
        target: "app.bbwick_runtime",
        ts_ms,
        skipped,
        "bbwick_oracle_tick_feed_lagged_resubscribed"
    );
}

pub(super) fn log_empty_step_symbol(ts_ms: u64, timeframe: &str) {
    tracing::warn!(
        target: "app.bbwick_runtime",
        ts_ms,
        timeframe,
        "bbwick_oracle_drop_step_empty_symbol"
    );
}

pub(super) fn top_of_book_receive_error_message(err: &broadcast::error::RecvError) -> String {
    match err {
        broadcast::error::RecvError::Lagged(skipped) => {
            format!("bbwick_oracle_tick_feed_lagged skipped={skipped}")
        }
        broadcast::error::RecvError::Closed => "bbwick_oracle_tick_feed_closed".to_string(),
    }
}

pub(super) async fn shutdown_runtime_connections(
    polymarket_bridge: &mut Option<EnginePolymarketBridge>,
    engine_state_forward_task: &mut Option<JoinHandle<()>>,
    oracle: &mut Option<OracleEngine>,
    top_of_book_rx: &mut Option<broadcast::Receiver<CanonicalTopOfBook>>,
    step_rx: &mut Option<watch::Receiver<bbwick_engine::StepFrame>>,
    ui_tick_projection: &mut Option<UiTickProjection>,
) -> Option<RetainedBids> {
    if let Some(projection) = ui_tick_projection.take() {
        projection.shutdown().await;
    }
    if let Some(bridge) = polymarket_bridge.take() {
        bridge.shutdown().await;
    }
    if let Some(task) = engine_state_forward_task.take() {
        task.abort();
        let _ = task.await;
    }
    let retained_bids = if let Some(engine) = oracle.take() {
        Some(engine.shutdown().await)
    } else {
        None
    };
    *top_of_book_rx = None;
    *step_rx = None;
    retained_bids
}

pub(super) async fn contain_runtime_fault(
    obs: &polymarket_hft::observability::Observability,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    ts_ms: u64,
    ingest_now_ns: u64,
    msg: String,
) {
    tracing::error!(
        target: "app.bbwick_runtime",
        ts_ms,
        error = %msg,
        "bbwick_runtime_fault_contained"
    );
    runtime.note_runtime_error(ts_ms, msg.clone());
    if let Some(oracle_runtime_tx) = oracle_runtime_tx {
        oracle_runtime_tx.send_replace(runtime.supervisor_status());
    }
    runtime.emit_status(obs, ts_ms, ingest_now_ns, true);
}

pub(super) async fn contain_runtime_fault_and_backoff(
    obs: &polymarket_hft::observability::Observability,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    ts_ms: u64,
    ingest_now_ns: u64,
    msg: String,
    attempt: &mut usize,
    backoff_ms: &mut u64,
) {
    contain_runtime_fault(obs, runtime, oracle_runtime_tx, ts_ms, ingest_now_ns, msg).await;
    *attempt = attempt.saturating_add(1);
    let sleep_ms = advance_capped_full_jitter_backoff_ms(*attempt, backoff_ms, 30_000).max(1_000);
    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
}

#[allow(
    clippy::too_many_arguments,
    reason = "existing BBWICK connection owner state"
)]
pub(super) async fn restart_finished_oracle_runtime(
    obs: &polymarket_hft::observability::Observability,
    runtime: &mut BbwickOracleRuntimeState,
    oracle_runtime_tx: Option<&watch::Sender<OracleRuntimeStatus>>,
    ts_ms: u64,
    ingest_now_ns: u64,
    polymarket_bridge: &mut Option<EnginePolymarketBridge>,
    engine_state_forward_task: &mut Option<JoinHandle<()>>,
    oracle: &mut Option<OracleEngine>,
    top_of_book_rx: &mut Option<broadcast::Receiver<CanonicalTopOfBook>>,
    step_rx: &mut Option<watch::Receiver<bbwick_engine::StepFrame>>,
    ui_tick_projection: &mut Option<UiTickProjection>,
    attempt: &mut usize,
    backoff_ms: &mut u64,
) -> Option<RetainedBids> {
    let Some(task) = oracle
        .as_mut()
        .and_then(OracleEngine::take_finished_runtime_task)
    else {
        return None;
    };
    let retained_bids = shutdown_runtime_connections(
        polymarket_bridge,
        engine_state_forward_task,
        oracle,
        top_of_book_rx,
        step_rx,
        ui_tick_projection,
    )
    .await;
    contain_runtime_fault_and_backoff(
        obs,
        runtime,
        oracle_runtime_tx,
        ts_ms,
        ingest_now_ns,
        format!("bbwick_oracle_root_task_exited task={task}"),
        attempt,
        backoff_ms,
    )
    .await;
    retained_bids
}

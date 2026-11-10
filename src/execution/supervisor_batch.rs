use super::supervisor_loop::SubmissionRecovery;
use super::*;
use crate::capture::events::{LocalOrderBlockCapture, RiskBlockCapture};
use crate::capture::session::CaptureMsg;
use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::execution::order_type::apply_configured_resting_order_type;
use crate::execution::RejectKind;
#[path = "supervisor_batch/admission_blocks.rs"]
mod admission_blocks;
#[path = "supervisor_batch/audit_records.rs"]
mod audit_records;
#[path = "supervisor_batch/cancel_replace.rs"]
mod cancel_replace;
use cancel_replace::cancel_then_submit_replacements;
#[path = "supervisor_batch/latency.rs"]
pub(super) mod latency;
#[path = "supervisor_batch/submit.rs"]
mod submit;
pub(super) use crate::engine_trade_journal::build_order_terminal_record;
pub(super) use admission_blocks::current_post_only_reject_status;
use admission_blocks::*;
pub(in crate::execution::supervisor) use admission_blocks::{
    record_batch_pre_submission_abandoned, record_pre_submission_abandoned,
};
pub(super) use audit_records::{persist_execution_audit_record, ExecutionAuditFailure};
use audit_records::{
    promote_accepted_order as promote_accepted_order_inner,
    record_accepted_order_submission as record_accepted_order_submission_inner,
};
use latency::*;
use std::collections::BTreeMap;
pub(super) use submit::apply_submission_completion;
pub(super) use submit::submit_ready_orders;

pub(in crate::execution::supervisor) use submit::{ActiveSubmissionCompletion, ActiveSubmissions};

pub(super) fn record_accepted_order_submission(
    order_fill_ctx: &OrderFillContextRegistry,
    intent: &OrderIntent,
    order_id: &str,
    accepted_at_ms: u64,
) -> Result<EngineTradeJournalRecord> {
    record_accepted_order_submission_inner(
        order_fill_ctx,
        intent,
        order_id,
        accepted_at_ms,
        Some(order_execution_timing_from_intent(intent)),
    )
}

pub(super) fn record_accepted_order_submission_with_timing(
    order_fill_ctx: &OrderFillContextRegistry,
    intent: &OrderIntent,
    order_id: &str,
    accepted_at_ms: u64,
    timing: OrderExecutionTiming,
) -> Result<EngineTradeJournalRecord> {
    record_accepted_order_submission_inner(
        order_fill_ctx,
        intent,
        order_id,
        accepted_at_ms,
        Some(timing),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "accepted order state owner plumbing"
)]
pub(super) fn promote_accepted_order(
    now_ms: u64,
    open_orders: &mut OpenOrdersState,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    intent: &OrderIntent,
    order_id: &str,
    submit_status: Option<&str>,
    pre_accept_terminal: bool,
) {
    promote_accepted_order_inner(
        now_ms,
        open_orders,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        intent,
        order_id,
        submit_status,
        pre_accept_terminal,
    );
}

pub(super) fn emit_execution_owner_block(
    obs: &Observability,
    capture: &MaybeCapture,
    ts_ms: u64,
    raw: String,
) {
    emit_local_order_block(obs, capture, ts_ms, raw);
}

#[allow(clippy::too_many_arguments, reason = "execution/risk/context plumbing")]
pub(super) async fn handle_intents_batch(
    now_ms: u64,
    decision_ts_ms: u64,
    decision_instant: Option<Instant>,
    handoff_instant: Option<Instant>,
    clock: &ExecutionSupervisorClock,
    pause: bool,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
    market_projection: Option<&crate::polymarket::state::MarketProjection>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    execution_order_type: PolymarketOrderType,
    resting_order_timeout_ms: u64,
    obs: &Observability,
    capture: &MaybeCapture,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    hard_submit_block: &mut Option<String>,
    intents: Vec<OrderIntent>,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
    mut active_submissions: Option<&mut ActiveSubmissions>,
    mut active_cancellations: Option<&mut ActiveCancellations>,
    submission_recovery: &mut SubmissionRecovery,
) -> Result<()> {
    if intents.is_empty() {
        return Ok(());
    }
    anyhow::ensure!(
        intents
            .iter()
            .all(|intent| intent.execution_intent_id != Some(0)),
        "execution intent identity is zero"
    );
    if let Some(reason) = hard_submit_block.as_deref() {
        emit_local_order_block(
            obs,
            capture,
            now_ms,
            format!(
                "local_order_block hard_submit_blocked reason={} dropped_intents={}",
                reason,
                intents.len()
            ),
        );
        tracing::error!(
            target: "execution.supervisor_batch",
            reason,
            dropped_intents = intents.len(),
            "hard_submit_block_active"
        );
        record_batch_pre_submission_abandoned(
            risk,
            ctf_inventory_runtime,
            submission_recovery,
            &intents,
            "hard_submit_blocked",
            now_ms,
            execution_audit_records,
        )?;
        return Ok(());
    }
    let portfolio = portfolio_rx.borrow().clone();
    let odds_state = odds_rx.map(|rx| rx.borrow().clone());
    let (passed, mut preserved_quote_keys, mut cancel_quote_keys) = prepare_admitted_intents(
        now_ms,
        pause,
        risk,
        ctf_inventory_runtime,
        order_lifecycle,
        post_only_rejects,
        strategy_cfg,
        &portfolio,
        odds_state.as_deref(),
        market_projection,
        open_orders,
        fenced_scopes,
        execution_order_type,
        resting_order_timeout_ms,
        obs,
        capture,
        intents,
        execution_audit_records,
        submission_recovery,
    )?;
    if passed.is_empty() && preserved_quote_keys.is_empty() && cancel_quote_keys.is_empty() {
        return Ok(());
    }
    let mut desired_by_key: HashMap<QuoteKey, OrderIntent> = HashMap::new();
    for intent in passed {
        let Some(key) = quote_key_from_intent(&intent) else {
            tracing::error!(
                target: "execution.supervisor_batch",
                token = %intent.instrument,
                side = ?intent.side(),
                "missing_quote_metadata"
            );
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "missing_quote_metadata",
                now_ms,
                execution_audit_records,
            )?;
            continue;
        };
        desired_by_key.insert(key, intent);
    }
    if desired_by_key.is_empty() && preserved_quote_keys.is_empty() && cancel_quote_keys.is_empty()
    {
        return Ok(());
    }
    let mut to_place_now: Vec<OrderIntent> = Vec::new();
    let mut to_place_after_cancel: Vec<OrderIntent> = Vec::new();
    let mut to_cancel: HashSet<String> = HashSet::new();
    let mut cancel_pending_scopes: HashSet<ScopedOrderFenceKey> = HashSet::new();
    let mut current_orders: Vec<&OpenOrderState> = open_orders.orders.values().collect();
    current_orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));
    for ord in current_orders {
        let scope = ScopedOrderFenceKey {
            market: ord.market.clone(),
            token: ord.token.clone(),
        };
        if fenced_scopes.contains(&scope) {
            continue;
        }
        let Some(key) = quote_key_from_order(ord) else {
            continue;
        };
        if preserved_quote_keys.remove(&key) {
            if let Some(intent) = desired_by_key.remove(&key) {
                if intent.execution_intent_id != ord.execution_intent_id {
                    record_pre_submission_abandoned(
                        risk,
                        ctf_inventory_runtime,
                        submission_recovery,
                        &intent,
                        "existing_order_preserved",
                        now_ms,
                        execution_audit_records,
                    )?;
                }
            }
            continue;
        }
        if cancel_quote_keys.remove(&key) {
            to_cancel.insert(ord.order_id.clone());
            cancel_pending_scopes.insert(scope);
            continue;
        }
        match desired_by_key.remove(&key) {
            Some(intent) => {
                let extra_buffer_ticks = active_post_only_extra_buffer_ticks(
                    post_only_rejects,
                    &intent,
                    strategy_cfg,
                    now_ms,
                );
                if should_keep_existing_quote(
                    now_ms,
                    ord,
                    &intent,
                    odds_state.as_deref(),
                    strategy_cfg,
                    effective_admission_buffer_ticks(strategy_cfg, extra_buffer_ticks),
                ) {
                    if intent.execution_intent_id != ord.execution_intent_id {
                        record_pre_submission_abandoned(
                            risk,
                            ctf_inventory_runtime,
                            submission_recovery,
                            &intent,
                            "existing_order_preserved",
                            now_ms,
                            execution_audit_records,
                        )?;
                    }
                    continue;
                }
                to_cancel.insert(ord.order_id.clone());
                cancel_pending_scopes.insert(scope);
                to_place_after_cancel.push(intent);
            }
            None => {}
        }
    }
    for intent in desired_by_key.into_values() {
        let Some(market) = intent_market_id(&intent) else {
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block missing_market_context token={} side={:?}",
                    intent.instrument,
                    intent.side()
                ),
            );
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "missing_market_context",
                now_ms,
                execution_audit_records,
            )?;
            continue;
        };
        let scope = ScopedOrderFenceKey {
            market: market.to_string(),
            token: intent.instrument.clone(),
        };
        if cancel_pending_scopes.contains(&scope) {
            to_place_after_cancel.push(intent);
        } else {
            to_place_now.push(intent);
        }
    }
    submit_ready_orders(
        now_ms,
        decision_ts_ms,
        decision_instant,
        handoff_instant,
        clock,
        exec,
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        strategy_cfg,
        odds_rx,
        market_projection,
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        fenced_scopes,
        post_only_rejects,
        obs,
        capture,
        open_orders_tx,
        hard_submit_block,
        execution_order_type,
        resting_order_timeout_ms,
        to_place_now,
        execution_audit_records,
        active_submissions.as_deref_mut(),
        submission_recovery,
        pause,
        runtime_control_rx,
        None,
        active_cancellations.as_deref_mut(),
    )
    .await?;
    cancel_then_submit_replacements(
        decision_ts_ms,
        decision_instant,
        handoff_instant,
        clock,
        pause,
        runtime_control_rx,
        exec,
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        strategy_cfg,
        portfolio_rx,
        odds_rx,
        market_projection,
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        fenced_scopes,
        scoped_heal_inflight,
        scoped_heal_tx,
        post_only_rejects,
        obs,
        capture,
        open_orders_tx,
        hard_submit_block,
        execution_order_type,
        resting_order_timeout_ms,
        to_cancel,
        to_place_after_cancel,
        execution_audit_records,
        active_submissions,
        active_cancellations,
        submission_recovery,
    )
    .await
}

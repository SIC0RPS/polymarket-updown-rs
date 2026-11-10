use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::Result;
use tokio::sync::oneshot;

use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::portfolio::FillSide;
use crate::types::{OrderIntent, Side};

use super::*;

const AUDIT_RETRY_INITIAL_MS: u64 = 250;
const AUDIT_RETRY_MAX_MS: u64 = 5_000;

#[derive(Debug)]
struct PendingAuditRecord {
    record: EngineTradeJournalRecord,
    accepted_order_id: Option<String>,
    _owned_context: Option<OrderFillContext>,
}

#[derive(Debug, Default)]
pub(super) struct SubmissionAuditQueue {
    records: VecDeque<PendingAuditRecord>,
    retry_at_ms: u64,
    retry_delay_ms: u64,
}

impl SubmissionAuditQueue {
    pub(super) fn new() -> Self {
        Self {
            records: VecDeque::new(),
            retry_at_ms: 0,
            retry_delay_ms: AUDIT_RETRY_INITIAL_MS,
        }
    }

    pub(super) fn persist_shutdown(
        &mut self,
        now_ms: u64,
        recovery: &mut SubmissionRecovery,
        order_fill_ctx: &OrderFillContextRegistry,
    ) -> std::result::Result<(), String> {
        self.retry_at_ms = 0;
        self.persist_due(now_ms, self.records.len(), recovery, order_fill_ctx)?;
        if self.records.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "execution audit records unresolved at shutdown count={}",
                self.records.len()
            ))
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(super) fn is_due(&self, now_ms: u64) -> bool {
        !self.records.is_empty() && now_ms >= self.retry_at_ms
    }

    pub(super) fn next_due_ms(&self) -> u64 {
        self.retry_at_ms
    }

    pub(super) fn push(&mut self, record: EngineTradeJournalRecord) {
        self.push_with_context(record, None);
    }

    pub(super) fn push_with_context(
        &mut self,
        record: EngineTradeJournalRecord,
        owned_context: Option<OrderFillContext>,
    ) {
        let accepted_order_id = record.ctf_event.as_ref().and_then(|event| match event {
            crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact) => {
                Some(fact.order_id.clone())
            }
            _ => None,
        });
        if accepted_order_id.is_some() {
            self.retry_at_ms = 0;
        }
        self.records.push_back(PendingAuditRecord {
            record,
            accepted_order_id,
            _owned_context: owned_context,
        });
    }

    pub(super) fn push_from_registry(
        &mut self,
        record: EngineTradeJournalRecord,
        order_fill_ctx: &OrderFillContextRegistry,
    ) -> Result<()> {
        match terminal_audit_context(&record, order_fill_ctx) {
            Ok(owned_context) => self.push_with_context(record, owned_context),
            Err(error) => {
                tracing::error!(
                    target: "execution.supervisor_loop",
                    error = %format!("{error:#}"),
                    "execution_terminal_audit_context_retry_pending"
                );
                self.push_with_context(record, None);
            }
        }
        Ok(())
    }

    pub(super) fn extend(
        &mut self,
        records: impl IntoIterator<Item = EngineTradeJournalRecord>,
        order_fill_ctx: &OrderFillContextRegistry,
        order_lifecycle: &mut OrderLifecycleSupervisor,
    ) -> Result<()> {
        for record in records {
            self.push_from_registry(record, order_fill_ctx)?;
        }
        order_lifecycle.finish_execution_completions(order_fill_ctx);
        Ok(())
    }

    pub(super) fn persist_due(
        &mut self,
        now_ms: u64,
        limit: usize,
        recovery: &mut SubmissionRecovery,
        order_fill_ctx: &OrderFillContextRegistry,
    ) -> std::result::Result<bool, String> {
        if now_ms < self.retry_at_ms {
            return Ok(false);
        }
        let mut did_work = false;
        let mut terminal_retry = false;
        let terminal_retry_delay_ms = self.retry_delay_ms;
        let attempts = limit.min(self.records.len());
        for _ in 0..attempts {
            let Some(record) = self.records.front() else {
                break;
            };
            let is_terminal = record.record.order_terminal_fact.is_some();
            match persist_execution_audit_record(&record.record) {
                Ok(()) => {
                    if let Some(order_id) = record.accepted_order_id.as_deref() {
                        recovery.note_accepted_audit_durable(order_id, order_fill_ctx);
                    }
                    self.records.pop_front();
                    if !terminal_retry {
                        self.retry_delay_ms = AUDIT_RETRY_INITIAL_MS;
                        self.retry_at_ms = 0;
                    }
                    did_work = true;
                }
                Err(error) if is_terminal => {
                    let text = format!("{error:#}");
                    let record = self
                        .records
                        .pop_front()
                        .expect("terminal audit queue front exists");
                    self.records.push_back(record);
                    terminal_retry = true;
                    tracing::error!(
                        target: "execution.supervisor_loop",
                        error = %text,
                        "execution_terminal_audit_retry_pending"
                    );
                }
                Err(error) => {
                    let text = format!("{error:#}");
                    if error
                        .downcast_ref::<ExecutionAuditFailure>()
                        .is_some_and(ExecutionAuditFailure::is_retriable)
                    {
                        let retry_delay_ms = if terminal_retry {
                            terminal_retry_delay_ms
                        } else {
                            self.retry_delay_ms
                        };
                        self.retry_at_ms = now_ms.saturating_add(retry_delay_ms);
                        self.retry_delay_ms =
                            retry_delay_ms.saturating_mul(2).min(AUDIT_RETRY_MAX_MS);
                        tracing::error!(
                            target: "execution.supervisor_loop",
                            error = %text,
                            "execution_audit_retry_pending"
                        );
                        return Ok(did_work);
                    }
                    if let Some(order_id) = record.accepted_order_id.as_deref() {
                        recovery.note_accepted_audit_failed(order_id, &text);
                    }
                    return Err(text);
                }
            }
        }
        if terminal_retry {
            self.retry_at_ms = now_ms.saturating_add(terminal_retry_delay_ms);
            self.retry_delay_ms = terminal_retry_delay_ms
                .saturating_mul(2)
                .min(AUDIT_RETRY_MAX_MS);
        }
        Ok(did_work)
    }
}

fn terminal_audit_context(
    record: &EngineTradeJournalRecord,
    order_fill_ctx: &OrderFillContextRegistry,
) -> Result<Option<OrderFillContext>> {
    let Some(fact) = record.order_terminal_fact.as_ref() else {
        return Ok(None);
    };
    let Some(order_id) = fact.order_id.as_deref() else {
        return Ok(None);
    };
    match order_fill_ctx.owned_context(order_id) {
        Ok(context) => Ok(Some(context)),
        Err(_) if fact.provenance == OrderTerminalProvenance::SubmissionRejected => Ok(None),
        Err(error) => Err(error.context(format!(
            "accepted terminal audit is missing owned order context order_id={order_id}"
        ))),
    }
}

#[derive(Debug, Clone)]
struct SubmissionCandidate {
    intent: OrderIntent,
    reservation_id: String,
    recovered_order_id: Option<String>,
}

#[derive(Debug)]
struct DeferredAuthenticatedTrade {
    observation: UnboundCtfTradeObservation,
    ack: oneshot::Sender<std::result::Result<OrderFillContext, String>>,
}

#[derive(Debug)]
pub(super) struct RecoveredSubmission {
    pub intent: OrderIntent,
    pub reservation_id: String,
    pub order_id: String,
}

enum SubmissionRecoveryMatch {
    Recovered(RecoveredSubmission),
    NoMatch,
    Ambiguous,
}

#[derive(Debug, Default)]
pub(in crate::execution::supervisor) struct SubmissionRecovery {
    candidates: HashMap<u64, SubmissionCandidate>,
    durable_accepted_orders: HashSet<String>,
    pending_accepted_orders: HashSet<String>,
    waiters: HashMap<
        String,
        Vec<(
            oneshot::Sender<std::result::Result<OrderFillContext, String>>,
            OrderFillContext,
        )>,
    >,
    deferred_authenticated_trades: VecDeque<DeferredAuthenticatedTrade>,
}

impl SubmissionRecovery {
    fn unresolved_candidates(&self) -> impl Iterator<Item = &SubmissionCandidate> {
        self.candidates.values().filter(|candidate| {
            !candidate
                .recovered_order_id
                .as_ref()
                .is_some_and(|order_id| self.durable_accepted_orders.contains(order_id))
        })
    }

    pub(super) fn unresolved_submissions(&self) -> usize {
        self.unresolved_candidates().count()
    }

    pub(super) fn has_unresolved_submission(&self, market: &str, token: Option<&str>) -> bool {
        self.unresolved_candidates().any(|candidate| {
            candidate.intent.market_id.as_deref() == Some(market)
                && token.is_none_or(|token| candidate.intent.instrument == token)
        })
    }

    pub(super) fn fail_unresolved_authorizations(&mut self) -> usize {
        let mut failed = 0;
        for (_, waiters) in self.waiters.drain() {
            for (waiter, _context) in waiters {
                failed += 1;
                let _ = waiter.send(Err(
                    "submission authorization audit unresolved at shutdown".to_string()
                ));
            }
        }
        for deferred in self.deferred_authenticated_trades.drain(..) {
            failed += 1;
            let _ = deferred.ack.send(Err(
                "submission authorization remains ambiguous after upstream completion".to_string(),
            ));
        }
        failed
    }

    pub(in crate::execution::supervisor) fn retains_execution_intent(
        &self,
        intent: &OrderIntent,
    ) -> bool {
        intent
            .execution_intent_id
            .is_some_and(|id| self.candidates.contains_key(&id))
    }

    pub(in crate::execution::supervisor) fn register_inflight(
        &mut self,
        intents: &[OrderIntent],
        reservation_ids: &[String],
    ) -> Result<()> {
        anyhow::ensure!(
            intents.len() == reservation_ids.len(),
            "submission recovery identity count mismatch"
        );
        for (index, intent) in intents.iter().enumerate() {
            let intent_id = intent
                .execution_intent_id
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow::anyhow!("inflight submission has no intent identity"))?;
            anyhow::ensure!(
                !self.candidates.contains_key(&intent_id)
                    && !intents[..index]
                        .iter()
                        .any(|previous| { previous.execution_intent_id == Some(intent_id) }),
                "duplicate inflight submission intent identity {intent_id}"
            );
        }
        for (intent, reservation_id) in intents.iter().zip(reservation_ids) {
            self.candidates.insert(
                intent
                    .execution_intent_id
                    .expect("validated submission identity"),
                SubmissionCandidate {
                    intent: intent.clone(),
                    reservation_id: reservation_id.clone(),
                    recovered_order_id: None,
                },
            );
        }
        Ok(())
    }

    pub(in crate::execution::supervisor) fn mark_unknown(&mut self, intents: &[OrderIntent]) {
        for intent in intents {
            let _preserved = intent
                .execution_intent_id
                .and_then(|intent_id| self.candidates.get(&intent_id));
        }
    }

    pub(in crate::execution::supervisor) fn remove_intent(&mut self, intent: &OrderIntent) {
        if let Some(intent_id) = intent.execution_intent_id {
            self.candidates.remove(&intent_id);
        }
    }

    pub(in crate::execution::supervisor) fn recovered_order_id(
        &self,
        intent: &OrderIntent,
    ) -> Option<&str> {
        intent
            .execution_intent_id
            .and_then(|intent_id| self.candidates.get(&intent_id))
            .and_then(|candidate| candidate.recovered_order_id.as_deref())
    }

    pub(in crate::execution::supervisor) fn known_ack_already_recovered(
        &mut self,
        intent: &OrderIntent,
        order_id: &str,
    ) -> Result<bool> {
        let Some(intent_id) = intent.execution_intent_id else {
            return Ok(false);
        };
        let Some(candidate) = self.candidates.remove(&intent_id) else {
            return Ok(false);
        };
        if let Some(recovered_order_id) = candidate.recovered_order_id {
            anyhow::ensure!(
                recovered_order_id == order_id,
                "authenticated trade order identity differs from HTTP acknowledgment intent_id={intent_id} recovered={recovered_order_id} acknowledged={order_id}"
            );
            return Ok(true);
        }
        Ok(false)
    }

    fn recover_authenticated_trade(
        &mut self,
        observation: &UnboundCtfTradeObservation,
    ) -> Result<SubmissionRecoveryMatch> {
        if self
            .durable_accepted_orders
            .contains(observation.order_id())
            || self
                .pending_accepted_orders
                .contains(observation.order_id())
        {
            return Ok(SubmissionRecoveryMatch::NoMatch);
        }
        let mut matches = self
            .candidates
            .iter()
            .filter(|(_, candidate)| {
                candidate.recovered_order_id.is_none() && compatible(&candidate.intent, observation)
            })
            .map(|(intent_id, _)| *intent_id);
        let Some(intent_id) = matches.next() else {
            return Ok(SubmissionRecoveryMatch::NoMatch);
        };
        if matches.next().is_some() {
            return Ok(SubmissionRecoveryMatch::Ambiguous);
        }
        let candidate = self
            .candidates
            .get_mut(&intent_id)
            .expect("matched recovery candidate exists");
        candidate.recovered_order_id = Some(observation.order_id().to_string());
        Ok(SubmissionRecoveryMatch::Recovered(RecoveredSubmission {
            intent: candidate.intent.clone(),
            reservation_id: candidate.reservation_id.clone(),
            order_id: observation.order_id().to_string(),
        }))
    }

    fn defer_authenticated_trade(
        &mut self,
        observation: UnboundCtfTradeObservation,
        ack: oneshot::Sender<std::result::Result<OrderFillContext, String>>,
    ) {
        self.deferred_authenticated_trades
            .push_back(DeferredAuthenticatedTrade { observation, ack });
    }

    pub(super) fn wait_for_accepted_audit(
        &mut self,
        order_id: &str,
        order_fill_ctx: &OrderFillContextRegistry,
        waiter: oneshot::Sender<std::result::Result<OrderFillContext, String>>,
    ) {
        let context = match order_fill_ctx.owned_context(order_id) {
            Ok(context) => context,
            Err(error) => {
                let _ = waiter.send(Err(format!("{error:#}")));
                return;
            }
        };
        if self.durable_accepted_orders.contains(order_id) {
            let _ = waiter.send(Ok(context));
            return;
        }
        self.waiters
            .entry(order_id.to_string())
            .or_default()
            .push((waiter, context));
    }

    pub(in crate::execution::supervisor) fn note_accepted_audit_pending(&mut self, order_id: &str) {
        self.pending_accepted_orders.insert(order_id.to_string());
    }

    pub(super) fn note_accepted_audit_durable(
        &mut self,
        order_id: &str,
        order_fill_ctx: &OrderFillContextRegistry,
    ) {
        self.pending_accepted_orders.remove(order_id);
        self.durable_accepted_orders.insert(order_id.to_string());
        order_fill_ctx.mark_audit_complete(order_id);
        if let Some(waiters) = self.waiters.remove(order_id) {
            for (waiter, context) in waiters {
                let _ = waiter.send(Ok(context));
            }
        }
    }

    pub(super) fn note_accepted_audit_failed(&mut self, order_id: &str, error: &str) {
        self.pending_accepted_orders.remove(order_id);
        if let Some(waiters) = self.waiters.remove(order_id) {
            for (waiter, _context) in waiters {
                let _ = waiter.send(Err(error.to_string()));
            }
        }
    }

    pub(super) fn accepted_audit_is_tracked(&self, order_id: &str) -> bool {
        self.durable_accepted_orders.contains(order_id)
            || self.pending_accepted_orders.contains(order_id)
    }
}

fn compatible(intent: &OrderIntent, observation: &UnboundCtfTradeObservation) -> bool {
    let Some(market_id) = intent.market_id.as_deref() else {
        return false;
    };
    if market_id != observation.market_id().as_str()
        || intent.instrument != observation.asset_id()
        || match (intent.side(), observation.side()) {
            (Side::Buy, FillSide::Buy) | (Side::Sell, FillSide::Sell) => false,
            _ => true,
        }
    {
        return false;
    }
    let intent_qty_units = crate::ctf_accounting::qty_to_units_6dp(intent.qty).ok();
    let intent_price_units = crate::polymarket::fixed_math::price_to_units_8dp(intent.limit_px);
    let Some((intent_qty_units, intent_price_units)) = intent_qty_units.zip(intent_price_units)
    else {
        return false;
    };
    if observation.owned_size_units_6dp() == 0
        || observation.owned_size_units_6dp() > intent_qty_units
    {
        return false;
    }
    let Some(observed_price_units) = observation.owned_price_units_6dp().checked_mul(100) else {
        return false;
    };
    match intent.side() {
        Side::Buy => observed_price_units <= u128::from(intent_price_units),
        Side::Sell => observed_price_units >= u128::from(intent_price_units),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "authenticated recovery owner plumbing"
)]
pub(super) fn apply_authenticated_trade_event(
    now_ms: u64,
    observation: UnboundCtfTradeObservation,
    ack: oneshot::Sender<std::result::Result<OrderFillContext, String>>,
    risk: &RiskHandle,
    order_fill_ctx: &OrderFillContextRegistry,
    portfolio: &PortfolioState,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    recovery: &mut SubmissionRecovery,
    audit_queue: &mut SubmissionAuditQueue,
) -> Result<bool> {
    let order_id = observation.order_id().to_string();
    match recovery.recover_authenticated_trade(&observation) {
        Ok(SubmissionRecoveryMatch::Recovered(recovered)) => {
            risk.bind_inflight_order(&recovered.reservation_id, recovered.order_id.clone());
            let record = record_accepted_order_submission(
                order_fill_ctx,
                &recovered.intent,
                recovered.order_id.as_str(),
                now_ms,
            );
            match record {
                Ok(record) => {
                    order_lifecycle.record_submitted(
                        open_orders,
                        &recovered.intent,
                        recovered.order_id.as_str(),
                        now_ms,
                    );
                    let (pre_accept_terminal, _, pre_accept_fact) = order_lifecycle
                        .bind_pre_accept_update(
                            open_orders,
                            &recovered.intent,
                            recovered.order_id.as_str(),
                            now_ms,
                        )?;
                    let pre_accept_terminal_record = pre_accept_fact
                        .map(build_order_terminal_record)
                        .transpose()?;
                    promote_accepted_order(
                        now_ms,
                        open_orders,
                        live_quote_cancel_index,
                        seen_quote_cancel_keys,
                        &recovered.intent,
                        recovered.order_id.as_str(),
                        Some("AUTHENTICATED_TRADE"),
                        pre_accept_terminal,
                    );
                    let metadata = order_fill_ctx.order_metadata(&order_id);
                    order_lifecycle.consume_matched_trade(
                        &observation,
                        metadata.as_ref(),
                        portfolio,
                        open_orders,
                        now_ms,
                        order_fill_ctx.direct_buy_authorization(&order_id),
                        risk,
                    )?;
                    if recovered.intent.buy_authorization().is_none()
                        && open_orders.orders.contains_key(recovered.order_id.as_str())
                    {
                        risk.release_inflight_order(&recovered.reservation_id);
                    }
                    recovery.note_accepted_audit_pending(&order_id);
                    recovery.wait_for_accepted_audit(&order_id, order_fill_ctx, ack);
                    audit_queue.push(record);
                    if let Some(record) = pre_accept_terminal_record {
                        audit_queue.push_from_registry(record, order_fill_ctx)?;
                    }
                    Ok(true)
                }
                Err(error) => {
                    let text = format!("{error:#}");
                    let _ = ack.send(Err(text.clone()));
                    Err(error)
                }
            }
        }
        Ok(SubmissionRecoveryMatch::Ambiguous) => {
            recovery.defer_authenticated_trade(observation, ack);
            Ok(false)
        }
        Ok(SubmissionRecoveryMatch::NoMatch) if recovery.accepted_audit_is_tracked(&order_id) => {
            let metadata = order_fill_ctx.order_metadata(&order_id);
            let applied = order_lifecycle.consume_matched_trade(
                &observation,
                metadata.as_ref(),
                portfolio,
                open_orders,
                now_ms,
                order_fill_ctx.direct_buy_authorization(&order_id),
                risk,
            )?;
            recovery.wait_for_accepted_audit(&order_id, order_fill_ctx, ack);
            Ok(applied)
        }
        Ok(SubmissionRecoveryMatch::NoMatch) => {
            let _ = ack.send(Err(format!(
                "authenticated trade has no unique original submission order_id={order_id}"
            )));
            Ok(false)
        }
        Err(error) => {
            let text = format!("{error:#}");
            let _ = ack.send(Err(text));
            Err(error)
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "authenticated recovery owner plumbing"
)]
pub(super) fn retry_deferred_authenticated_trade_events(
    now_ms: u64,
    risk: &RiskHandle,
    order_fill_ctx: &OrderFillContextRegistry,
    portfolio: &PortfolioState,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    recovery: &mut SubmissionRecovery,
    audit_queue: &mut SubmissionAuditQueue,
) -> Result<bool> {
    let pending = recovery.deferred_authenticated_trades.len();
    let mut applied = false;
    for _ in 0..pending {
        let Some(deferred) = recovery.deferred_authenticated_trades.pop_front() else {
            break;
        };
        applied |= apply_authenticated_trade_event(
            now_ms,
            deferred.observation,
            deferred.ack,
            risk,
            order_fill_ctx,
            portfolio,
            open_orders,
            order_lifecycle,
            live_quote_cancel_index,
            seen_quote_cancel_keys,
            recovery,
            audit_queue,
        )?;
    }
    Ok(applied)
}

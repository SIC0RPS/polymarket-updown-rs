use super::*;
use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::trade_session_ledger::ConfirmedOrderSubmissionFact;

#[derive(Debug)]
pub(in crate::execution) struct ExecutionAuditFailure {
    message: String,
    retriable: bool,
}

impl ExecutionAuditFailure {
    fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retriable: false,
        }
    }

    fn retriable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retriable: true,
        }
    }

    pub(in crate::execution) fn is_retriable(&self) -> bool {
        self.retriable
    }
}

impl std::fmt::Display for ExecutionAuditFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ExecutionAuditFailure {}

fn persist_accepted_order_record(record: &EngineTradeJournalRecord) -> Result<()> {
    let order_id = record
        .ctf_event
        .as_ref()
        .and_then(|event| match event {
            crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact) => {
                Some(fact.order_id.clone())
            }
            _ => None,
        })
        .ok_or_else(|| {
            ExecutionAuditFailure::permanent("accepted order audit record has no typed fact")
        })?;
    let inserted = crate::engine_trade_journal::append_once(
        format!("order_submitted:{order_id}"),
        record.clone(),
    )
    .map_err(|error| ExecutionAuditFailure::retriable(format!("{error:#}")))?;
    if !inserted {
        return Err(ExecutionAuditFailure::permanent(format!(
            "duplicate accepted order audit identity: {order_id}"
        ))
        .into());
    }
    Ok(())
}

fn persist_order_terminal_record(record: &EngineTradeJournalRecord) -> Result<()> {
    let fact = record.order_terminal_fact.as_ref().ok_or_else(|| {
        ExecutionAuditFailure::permanent("order terminal audit record has no typed fact")
    })?;
    fact.validate()
        .map_err(|error| ExecutionAuditFailure::permanent(format!("{error:#}")))?;
    let disposition = crate::engine_trade_journal::append_with_disposition(record.clone())
        .map_err(|error| ExecutionAuditFailure::retriable(format!("{error:#}")))?;
    if disposition
        == crate::trade_session_ledger::FinancialAdmissionDisposition::DefinitivelyRejected
    {
        return Err(ExecutionAuditFailure::permanent(format!(
            "conflicting order terminal audit identity execution_intent_id={}",
            fact.execution_intent_id
        ))
        .into());
    }
    Ok(())
}

pub(in crate::execution) fn persist_execution_audit_record(
    record: &EngineTradeJournalRecord,
) -> Result<()> {
    if record.order_terminal_fact.is_some() {
        persist_order_terminal_record(record)
    } else if record.ctf_event.is_some() {
        persist_accepted_order_record(record)
    } else {
        Err(
            ExecutionAuditFailure::permanent("execution audit record has no supported typed fact")
                .into(),
        )
    }
}

pub(super) fn record_accepted_order_submission(
    order_fill_ctx: &OrderFillContextRegistry,
    intent: &OrderIntent,
    order_id: &str,
    accepted_at_ms: u64,
    execution_timing: Option<OrderExecutionTiming>,
) -> Result<EngineTradeJournalRecord> {
    let Some(market) = intent_market_id(intent) else {
        tracing::error!(
            target: "execution.supervisor_batch",
            order_id,
            token = %intent.instrument,
            "accepted_order_missing_market_context"
        );
        anyhow::bail!("accepted order is missing market context");
    };
    let submitted_order_id = order_id.to_string();
    let intent_id = intent
        .execution_intent_id
        .filter(|intent_id| *intent_id > 0)
        .ok_or_else(|| anyhow::anyhow!("accepted order is missing execution intent identity"))?;
    let cohort_id = intent
        .lifecycle_cohort
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("accepted order is missing lifecycle cohort"))?
        .to_string();
    let underlying = intent
        .lifecycle_symbol
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("accepted order is missing lifecycle symbol"))?
        .to_ascii_uppercase();
    let fact = ConfirmedOrderSubmissionFact {
        order_id: submitted_order_id.clone(),
        buy_authorization: intent.buy_authorization().copied(),
        intent_id,
        market_id: market.clone(),
        cohort_id: cohort_id.clone(),
        underlying,
        asset_id: intent.instrument.clone(),
        instruction_qty_units_6dp: intent.execution_instruction_qty_units_6dp,
        instruction_limit_px_units_8dp: intent.execution_instruction_limit_px_units_8dp,
        time_in_force: intent.time_in_force.clone(),
        post_only: intent.post_only,
        accepted_at_ms,
    };
    fact.validate()?;
    order_fill_ctx.record_order_submission_with_timing(
        intent.execution_intent_id,
        submitted_order_id.as_str(),
        market.as_str(),
        intent.instrument.as_str(),
        intent.side(),
        &intent.time_in_force,
        intent.post_only,
        intent.quote_level,
        intent.quote_group.as_deref(),
        intent.lifecycle_cohort.as_deref(),
        intent.lifecycle_symbol.as_deref(),
        Some(intent.qty),
        intent.order_close_reason_str(),
        execution_timing,
        intent.buy_authorization().copied(),
    )?;
    let ctf_event = crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact);
    let mut record =
        EngineTradeJournalRecord::new(cohort_id.clone(), ctf_event.event_kind(), accepted_at_ms);
    record.ctf_event = Some(ctf_event);
    record.market_id = Some(market);
    record.tokens = vec![intent.instrument.clone()];
    record.paired_cohort_key = Some(cohort_id);
    record.local_order_ids = vec![submitted_order_id];
    record.reconciliation_status = Some("accepted_order_submission".to_string());
    Ok(record)
}

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
    let updated_at_ms = now_ms;
    let Some(market) = intent_market_id(intent) else {
        tracing::error!(
            target: "execution.supervisor_batch",
            order_id,
            token = %intent.instrument,
            "promoted_order_missing_market_context"
        );
        return;
    };
    let submitted_order_id = order_id.to_string();
    let status = submit_status
        .map(|status| status.trim().to_ascii_uppercase())
        .filter(|status| !status.is_empty())
        .unwrap_or_else(|| "SUBMITTED".to_string());
    if pre_accept_terminal || intent.time_in_force.is_immediate() {
        return;
    }
    if matches!(status.as_str(), "MATCHED" | "CANCELED" | "CANCELLED") {
        if open_orders.orders.remove(&submitted_order_id).is_some() {
            open_orders.note_runtime_mutation(updated_at_ms);
        }
        return;
    }
    match open_orders.orders.entry(submitted_order_id.clone()) {
        std::collections::hash_map::Entry::Occupied(mut occupied) => {
            let order = occupied.get_mut();
            order.execution_intent_id = intent.execution_intent_id;
            order.market = market.to_string();
            order.token = intent.instrument.clone();
            order.side = intent.side();
            order.time_in_force = intent.time_in_force.clone();
            order.px = intent.limit_px;
            order.original_size = intent.qty;
            order.placed_at_ms = order.placed_at_ms.min(updated_at_ms);
            order.last_update_ms = order.last_update_ms.max(updated_at_ms);
            order.post_only = intent.post_only;
            order.quote_level = intent.quote_level;
            order.quote_group = intent.quote_group.clone();
            order.lifecycle_cohort = intent.lifecycle_cohort.clone();
            order.lifecycle_symbol = intent.lifecycle_symbol.clone();
            order.client_tag = intent
                .quote_group
                .as_ref()
                .map(|g| format!("{g}:{}", intent.quote_level.unwrap_or(255)));
            order.order_close_reason = intent.order_close_reason();
        }
        std::collections::hash_map::Entry::Vacant(vacant) => {
            vacant.insert(OpenOrderState {
                execution_intent_id: intent.execution_intent_id,
                order_id: submitted_order_id.clone(),
                market: market.to_string(),
                token: intent.instrument.clone(),
                side: intent.side(),
                time_in_force: intent.time_in_force.clone(),
                px: intent.limit_px,
                original_size: intent.qty,
                matched_size: 0.0,
                placed_at_ms: updated_at_ms,
                last_update_ms: updated_at_ms,
                status,
                post_only: intent.post_only,
                quote_level: intent.quote_level,
                quote_group: intent.quote_group.clone(),
                lifecycle_cohort: intent.lifecycle_cohort.clone(),
                lifecycle_symbol: intent.lifecycle_symbol.clone(),
                client_tag: intent
                    .quote_group
                    .as_ref()
                    .map(|g| format!("{g}:{}", intent.quote_level.unwrap_or(255))),
                order_close_reason: intent.order_close_reason(),
            });
        }
    }
    if let Some(order) = open_orders.orders.get(&submitted_order_id) {
        register_quote_cancel_key(live_quote_cancel_index, seen_quote_cancel_keys, order);
    }
    open_orders.note_runtime_mutation(updated_at_ms);
}

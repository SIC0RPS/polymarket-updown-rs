use std::sync::MutexGuard;

use anyhow::{anyhow, Result};

use super::*;

pub fn append_engine_trade_record(record: &EngineTradeJournalRecord) -> Result<TradeLedgerRecord> {
    let (record, disposition) = append_engine_trade_record_with_disposition(record)?;
    anyhow::ensure!(
        disposition == FinancialAdmissionDisposition::Admitted,
        "engine trade record was definitively rejected"
    );
    Ok(record)
}

pub(crate) fn append_engine_trade_record_with_disposition(
    record: &EngineTradeJournalRecord,
) -> Result<(TradeLedgerRecord, FinancialAdmissionDisposition)> {
    let run_id = record.run_id.trim();
    anyhow::ensure!(
        is_live_run_id(run_id),
        "trade session ledger append requires canonical live run identity"
    );
    let run_id = run_id.to_string();
    let guard = canonical_ledger_lock()?;
    let mut ledger_record = TradeLedgerRecord::from_engine_trade_journal(record, run_id);
    ledger_record.ledger_seq = allocate_trade_ledger_seq()?;
    if let Some(CanonicalCtfEvent::ResolutionObserved(resolution)) =
        ledger_record.ctf_event.as_mut()
    {
        anyhow::ensure!(
            resolution.ledger_seq == 0,
            "new resolution observation already has a durable ledger sequence"
        );
        resolution.ledger_seq = ledger_record.ledger_seq;
    }
    if validate_ctf_event_record(&ledger_record).is_err()
        || validate_order_terminal_record(&ledger_record).is_err()
    {
        drop(guard);
        return Ok((
            ledger_record,
            FinancialAdmissionDisposition::DefinitivelyRejected,
        ));
    }
    if ledger_record.ctf_event.is_some() || ledger_record.order_terminal_fact.is_some() {
        if !record_belongs_to_run_window(&ledger_record) {
            drop(guard);
            return Ok((
                ledger_record,
                FinancialAdmissionDisposition::DefinitivelyRejected,
            ));
        }
    }
    if let Some(existing) = find_existing_ctf_accounting_record(&ledger_record)? {
        if !same_ctf_accounting_record(&existing, &ledger_record) {
            drop(guard);
            return Ok((
                ledger_record,
                FinancialAdmissionDisposition::DefinitivelyRejected,
            ));
        }
        let existing = append_trade_ledger_record_with_disposition(&existing)?;
        drop(guard);
        return Ok(existing);
    }
    let appended = should_append_trade_record_to_canonical_ledger(&ledger_record);
    if appended {
        let (record, disposition) = append_trade_ledger_record_with_disposition(&ledger_record)?;
        ledger_record = record;
        drop(guard);
        return Ok((ledger_record, disposition));
    }
    drop(guard);
    Ok((
        ledger_record,
        FinancialAdmissionDisposition::DefinitivelyRejected,
    ))
}

pub fn append_owned_leg_outcome_facts(
    run_id: &str,
    symbol: &str,
    facts: &[StrategyFact],
    execution_intent_id: Option<u64>,
) -> Result<usize> {
    anyhow::ensure!(
        is_live_run_id(run_id),
        "owned-leg outcome append requires canonical live run identity"
    );
    anyhow::ensure!(
        matches!(symbol, "BTC" | "ETH" | "SOL" | "XRP"),
        "owned-leg outcome append has invalid symbol={symbol}"
    );
    if facts.is_empty() {
        return Ok(0);
    }
    anyhow::ensure!(
        execution_intent_id.is_none() || facts.len() == 1,
        "live execution intent identity requires exactly one owned-leg outcome fact"
    );
    let guard = canonical_ledger_lock()?;
    for fact in facts {
        let StrategyFact::CtfOwnedLegOutcome(fact) = fact;
        let ledger_seq = allocate_trade_ledger_seq()?;
        let record = super::live_accounting::inspect_resident_records(
            run_id,
            |records| -> Result<Option<TradeLedgerRecord>> {
                let outcome = owned_leg_outcomes::join_live_owned_leg_outcome(
                    symbol,
                    fact,
                    execution_intent_id,
                    ledger_seq,
                    records.iter(),
                )?;
                if owned_leg_outcomes::repeats_latest_hold_decision(
                    &outcome,
                    records.iter(),
                    std::iter::empty(),
                ) {
                    return Ok(None);
                }
                let record = TradeLedgerRecord::from_owned_leg_outcome(outcome, run_id.to_string());
                validate_owned_leg_outcome_record(&record, records.iter())?;
                Ok(Some(record))
            },
        )?;
        let Some(record) = record else {
            continue;
        };
        let _ = append_trade_ledger_record(&record)?;
    }
    drop(guard);
    Ok(facts.len())
}

pub(super) fn canonical_ledger_lock() -> Result<MutexGuard<'static, ()>> {
    CTF_DURABLE_EVENT_LOCK
        .lock()
        .map_err(|_| anyhow!("CTF durable event lock poisoned"))
}

fn find_existing_ctf_accounting_record(
    candidate: &TradeLedgerRecord,
) -> Result<Option<TradeLedgerRecord>> {
    let mut existing = if let Some(terminal) = candidate.order_terminal_fact.as_ref() {
        super::live_accounting::resident_terminal_by_intent(
            &candidate.run_id,
            &terminal.execution_intent_id,
        )?
    } else {
        None
    };
    let Some(candidate_fact) = candidate.ctf_event.as_ref() else {
        return Ok(existing);
    };
    let durable_identity = candidate_fact.durable_identity();
    let candidate_order = match candidate_fact {
        CanonicalCtfEvent::OrderAccepted(order) => Some(order),
        _ => None,
    };
    if durable_identity.is_none() && candidate_order.is_none() {
        return Ok(None);
    }
    let candidate_fill_id = match candidate_fact {
        CanonicalCtfEvent::TradeConfirmed(fact) => Some(fact.fill_id.as_str()),
        _ => None,
    };
    let ctf_existing = durable_identity.as_ref().map_or(Ok(None), |identity| {
        super::live_accounting::resident_ctf_event_by_identity(&candidate.run_id, identity)
    })?;
    if ctf_existing.is_some() {
        anyhow::ensure!(
            existing.is_none(),
            "record has competing canonical identities"
        );
        existing = ctf_existing;
    }
    if let (Some(fill_id), Some(identity)) = (candidate_fill_id, durable_identity.as_deref()) {
        if let Some(record) = super::live_accounting::resident_managed_sell_by_fill(
            &candidate.run_id,
            &fill_id.to_string(),
        )? {
            let record_fact = record
                .ctf_event
                .as_ref()
                .context("indexed fill has no CTF fact")?;
            if record_fact.durable_identity().as_deref() != Some(identity) {
                return Ok(Some(record));
            }
        }
    }
    if let Some(candidate_order) = candidate_order {
        let by_order = super::live_accounting::resident_order_by_id(
            &candidate.run_id,
            &candidate_order.order_id,
        )?;
        let by_intent = super::live_accounting::resident_order_by_intent(
            &candidate.run_id,
            &candidate_order.intent_id,
        )?;
        for record in [by_order, by_intent].into_iter().flatten() {
            let Some(CanonicalCtfEvent::OrderAccepted(order)) = record.ctf_event.as_ref() else {
                anyhow::bail!("indexed accepted order has wrong CTF fact");
            };
            anyhow::ensure!(
                *order == *candidate_order && same_ctf_accounting_record(&record, candidate),
                "accepted order or intent identity already consumed order_id={} intent_id={}",
                candidate_order.order_id,
                candidate_order.intent_id
            );
            if existing
                .as_ref()
                .is_none_or(|prior| prior.ledger_seq != record.ledger_seq)
            {
                anyhow::ensure!(
                    existing.is_none(),
                    "duplicate durable accepted order identity order_id={} intent_id={}",
                    candidate_order.order_id,
                    candidate_order.intent_id
                );
                existing = Some(record);
            }
        }
    }
    Ok(existing)
}

pub(super) fn same_ctf_accounting_record(
    left: &TradeLedgerRecord,
    right: &TradeLedgerRecord,
) -> bool {
    if let (Some(left_terminal), Some(right_terminal)) = (
        left.order_terminal_fact.as_ref(),
        right.order_terminal_fact.as_ref(),
    ) {
        return left.run_id == right.run_id
            && left.trade_id == right.trade_id
            && left.event_kind == right.event_kind
            && left_terminal.semantically_matches(right_terminal);
    }
    if let (
        Some(CanonicalCtfEvent::ResolutionObserved(left_resolution)),
        Some(CanonicalCtfEvent::ResolutionObserved(right_resolution)),
    ) = (left.ctf_event.as_ref(), right.ctf_event.as_ref())
    {
        return left.run_id == right.run_id
            && left.trade_id == right.trade_id
            && left.event_kind == right.event_kind
            && left.market_id == right.market_id
            && left.tx_hashes == right.tx_hashes
            && left_resolution.run_id == right_resolution.run_id
            && left_resolution.condition_id == right_resolution.condition_id
            && left_resolution.resolution_tx_hash == right_resolution.resolution_tx_hash
            && left_resolution.resolution_log_index == right_resolution.resolution_log_index
            && left_resolution.finalized_block_number == right_resolution.finalized_block_number
            && left_resolution.finalized_block_hash == right_resolution.finalized_block_hash
            && left_resolution.finalized_block_timestamp_ms
                == right_resolution.finalized_block_timestamp_ms
            && left_resolution.outcome_slot_0_numerator
                == right_resolution.outcome_slot_0_numerator
            && left_resolution.outcome_slot_1_numerator
                == right_resolution.outcome_slot_1_numerator
            && left_resolution.denominator == right_resolution.denominator;
    }
    left.run_id == right.run_id
        && left.ts_ms == right.ts_ms
        && left.trade_id == right.trade_id
        && left.event_kind == right.event_kind
        && left.ctf_event == right.ctf_event
        && left.market_id == right.market_id
        && left.paired_cohort_key == right.paired_cohort_key
        && left.tokens == right.tokens
}

pub(super) fn should_append_trade_record_to_canonical_ledger(record: &TradeLedgerRecord) -> bool {
    if record.ctf_event.is_some()
        || record.order_terminal_fact.is_some()
        || record.oracle_resolved_label.is_some()
        || record.owned_leg_outcome.is_some()
    {
        return true;
    }
    matches!(
        record.event_kind.as_str(),
        "entry_intent"
            | "winner_selected"
            | "protected_side_late_exit_authorized"
            | "protected_winner_hold"
            | "loser_cleanup_requested"
            | "loser_cleanup_confirmed"
            | "terminal_release_ready"
            | "post_proceeds_sell_expired_or_unfilled"
            | "exit_intent"
            | "reconciled"
    )
}

pub(super) fn validate_order_terminal_record(record: &TradeLedgerRecord) -> Result<()> {
    let Some(fact) = record.order_terminal_fact.as_ref() else {
        return Ok(());
    };
    fact.validate()?;
    anyhow::ensure!(
        record.ctf_event.is_none()
            && record.owned_leg_outcome.is_none()
            && record.event_kind == "order_terminal"
            && record.trade_id == fact.cohort
            && record
                .market_id
                .as_ref()
                .is_some_and(|market| market.as_str() == fact.market)
            && record.paired_cohort_key.as_deref() == Some(fact.cohort.as_str())
            && record.tokens.as_slice() == [fact.token.as_str()]
            && match fact.order_id.as_deref() {
                Some(order_id) => record.local_order_ids.as_slice() == [order_id],
                None => record.local_order_ids.is_empty(),
            },
        "order terminal fact does not match its durable audit record"
    );
    Ok(())
}

pub(super) fn append_trade_ledger_record(record: &TradeLedgerRecord) -> Result<TradeLedgerRecord> {
    append_trade_ledger_record_with_disposition(record).map(|(record, _)| record)
}

fn append_trade_ledger_record_with_disposition(
    record: &TradeLedgerRecord,
) -> Result<(TradeLedgerRecord, FinancialAdmissionDisposition)> {
    if !record_belongs_to_run_window(record) {
        tracing::warn!(
            target: "trade_session_ledger",
            run_id = %record.run_id,
            ts_ms = record.ts_ms,
            event_kind = %record.event_kind,
            trade_id = %record.trade_id,
            "trade_session_ledger_rejected_out_of_window_record"
        );
        return Ok((
            record.clone(),
            FinancialAdmissionDisposition::DefinitivelyRejected,
        ));
    }
    if let Some(existing) =
        super::live_accounting::resident_trade_by_sequence(&record.run_id, record.ledger_seq)?
    {
        anyhow::ensure!(
            serde_json::to_value(&existing)? == serde_json::to_value(record)?,
            "trade session ledger sequence conflicts ledger_seq={}",
            record.ledger_seq
        );
        let disposition = super::live_accounting::resident_trade_disposition_by_sequence(
            &record.run_id,
            record.ledger_seq,
        )?
        .context("resident trade has no financial admission disposition")?;
        return Ok((existing, disposition));
    }
    ensure_run_trade_ledger_layout(&record.run_id)?;
    let (record, retry) = super::live_accounting::begin_durable_trade_append(
        record,
        same_trade_record_without_allocated_sequence,
    )?;
    let session_path = trade_session_ledger_path_for_run(&record.run_id);
    append_and_sync_ledger_row(&session_path, &record, record.ledger_seq, retry)?;

    let trade_path = trade_session_ledger_run_dir(&record.run_id)
        .join(TRADE_LEDGER_DIR_NAME)
        .join(format!(
            "{}.jsonl",
            sanitize_path_component(&record.trade_id)
        ));
    append_and_sync_ledger_row(&trade_path, &record, record.ledger_seq, retry)?;

    update_run_index(&record)?;
    let disposition = match super::live_accounting::ingest_durable_trade_record(record.clone()) {
        Ok(disposition) => disposition,
        Err(error) => {
            tracing::error!(
                target: "trade_session_ledger.live_accounting",
                run_id = %record.run_id,
                ledger_seq = record.ledger_seq,
                error = %error,
                "durable_trade_live_accounting_projection_failed"
            );
            return Err(error);
        }
    };
    Ok((record, disposition))
}

fn same_trade_record_without_allocated_sequence(
    left: &TradeLedgerRecord,
    right: &TradeLedgerRecord,
) -> Result<bool> {
    let mut left = left.clone();
    let mut right = right.clone();
    for record in [&mut left, &mut right] {
        record.ledger_seq = 0;
        if let Some(CanonicalCtfEvent::ResolutionObserved(resolution)) = record.ctf_event.as_mut() {
            resolution.ledger_seq = 0;
        }
        if let Some(outcome) = record.owned_leg_outcome.as_mut() {
            outcome.ledger_seq = 0;
        }
    }
    Ok(serde_json::to_value(left)? == serde_json::to_value(right)?)
}

fn sanitize_path_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| match ch {
            '/' | '\\' | '\0' => '_',
            _ => ch,
        })
        .collect()
}

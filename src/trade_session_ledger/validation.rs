use super::*;

pub(crate) fn validate_owned_leg_outcome_record<'a>(
    record: &TradeLedgerRecord,
    records: impl IntoIterator<Item = &'a TradeLedgerRecord>,
) -> Result<()> {
    let validation = owned_leg_outcomes::OwnedLegValidationProjection::from_records(records);
    validate_owned_leg_outcome_record_against(record, &validation)
}

pub(crate) fn validate_owned_leg_outcome_record_against(
    record: &TradeLedgerRecord,
    validation: &owned_leg_outcomes::OwnedLegValidationProjection,
) -> Result<()> {
    anyhow::ensure!(
        (record.event_kind == "owned_leg_outcome") == record.owned_leg_outcome.is_some(),
        "owned-leg outcome event label and typed payload presence disagree"
    );
    let Some(outcome) = record.owned_leg_outcome.as_ref() else {
        return Ok(());
    };
    anyhow::ensure!(
        record.ctf_event.is_none()
            && record.event_kind == "owned_leg_outcome"
            && record.ledger_seq == outcome.ledger_seq
            && record.ts_ms == outcome.ts_ms
            && record.trade_id == outcome.cohort_id
            && record.paired_cohort_key.as_deref() == Some(outcome.cohort_id.as_str())
            && record.market_id.as_ref() == Some(&outcome.market_id)
            && record.tokens.as_slice() == [outcome.token.as_str()],
        "owned-leg outcome does not match its durable audit record"
    );
    validation.validate(outcome)
}

pub(crate) fn validate_ctf_event_record(record: &TradeLedgerRecord) -> Result<()> {
    let Some(event) = record.ctf_event.as_ref() else {
        return Ok(());
    };
    event.validate()?;
    anyhow::ensure!(
        record.event_kind == event.event_kind(),
        "CTF event label does not match typed event: expected={} actual={}",
        event.event_kind(),
        record.event_kind
    );
    if let Some(cohort_id) = event.cohort_id() {
        anyhow::ensure!(
            record.trade_id == cohort_id && record.paired_cohort_key.as_deref() == Some(cohort_id),
            "CTF event cohort does not match durable audit identity"
        );
    }
    match event {
        CanonicalCtfEvent::SplitConfirmed(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.tokens.len() == 2
                    && record.tokens.contains(&fact.up_token)
                    && record.tokens.contains(&fact.down_token)
                    && record.split_tx_hash.as_deref() == Some(fact.transaction_id.as_str()),
                "confirmed split fact does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::BuyConfirmed(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.tokens.as_slice() == [fact.token.as_str()]
                    && record.fill_ids.as_slice() == [fact.fill_id.as_str()]
                    && record.local_order_ids.as_slice() == [fact.order_id.as_str()]
                    && record.split_tx_hash.is_none()
                    && record.exit_fill_id.is_none()
                    && record.ts_ms == fact.confirmed_at_ms,
                "confirmed BUY fact does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::TradeConfirmed(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.tokens.as_slice() == [fact.token.as_str()]
                    && record.exit_fill_id.as_deref() == Some(fact.fill_id.as_str())
                    && record.local_order_ids.iter().any(|id| id == &fact.order_id),
                "confirmed managed-sell fact does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::MergeConfirmed(fact) | CanonicalCtfEvent::RedeemConfirmed(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.tokens.len() == 2
                    && record.tokens.contains(&fact.up_token)
                    && record.tokens.contains(&fact.down_token)
                    && record.settlement_tx_hash.as_deref()
                        == Some(fact.terminal_operation_identity()),
                "confirmed terminal fact does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::OrderAccepted(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.ts_ms == fact.accepted_at_ms
                    && record.tokens.as_slice() == [fact.asset_id.as_str()]
                    && record.local_order_ids.as_slice() == [fact.order_id.as_str()],
                "accepted order event does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::TradeMatched(fact) | CanonicalCtfEvent::TradeFailed(fact) => {
            anyhow::ensure!(
                record.market_id.as_ref() == Some(&fact.market_id)
                    && record.tokens.as_slice() == [fact.asset_id.as_str()]
                    && record.local_order_ids.iter().any(|id| id == &fact.order_id),
                "trade lifecycle event does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::FeeApplied(fact) | CanonicalCtfEvent::CostApplied(fact) => {
            anyhow::ensure!(
                record.trade_id == fact.cohort_id,
                "charge event does not match its durable audit record"
            );
        }
        CanonicalCtfEvent::ResolutionObserved(fact) => {
            anyhow::ensure!(
                record.ts_ms == fact.observed_at_ms
                    && record.trade_id == fact.condition_id
                    && record
                        .market_id
                        .as_ref()
                        .is_some_and(|market| market.as_str() == fact.condition_id)
                    && record.tx_hashes.as_slice() == [fact.resolution_tx_hash.as_str()],
                "resolution observation does not match its durable audit record"
            );
        }
    }
    Ok(())
}

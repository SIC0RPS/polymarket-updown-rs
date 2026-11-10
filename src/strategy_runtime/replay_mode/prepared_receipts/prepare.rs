use super::*;
use crate::trade_session_ledger::DirectEoaOperation;

pub(super) fn select_symbol_receipts(
    receipts: &[SessionReceipt],
    records: &[TradeLedgerRecord],
    run_id: &str,
    catalog: &MarketCatalog,
    symbol: &str,
) -> Result<(
    Vec<FinalizedConditionResolutionReceipt>,
    Vec<PreparedSettlementReceipt>,
)> {
    let mut resolutions = Vec::new();
    let mut settlements = Vec::new();
    let mut resolution_ids = HashSet::new();
    let mut settlement_ids = HashSet::new();
    let mut ledger_sequences = HashSet::new();
    let mut source_acquisitions = BTreeMap::new();
    let mut source_buy_receipt_ids = HashSet::new();
    for record in records {
        match record.ctf_event.as_ref() {
            Some(CanonicalCtfEvent::ResolutionObserved(receipt))
                if condition_belongs_to_catalog(&receipt.condition_id, catalog) =>
            {
                validate_ctf_event_record(record)?;
                anyhow::ensure!(
                    record.run_id == run_id
                        && receipt.run_id == run_id
                        && receipt.ledger_seq == record.ledger_seq,
                    "prepared resolution observation run or ledger identity mismatch"
                );
                anyhow::ensure!(
                    resolution_ids.insert(receipt.condition_id.clone()),
                    "duplicate finalized condition resolution condition_id={}",
                    receipt.condition_id
                );
                anyhow::ensure!(
                    ledger_sequences.insert(record.ledger_seq),
                    "duplicate central receipt ledger_seq={}",
                    record.ledger_seq
                );
                resolutions.push(receipt.clone());
            }
            Some(event @ CanonicalCtfEvent::SplitConfirmed(split))
                if condition_belongs_to_catalog(&split.condition_id, catalog) =>
            {
                validate_ctf_event_record(record)?;
                anyhow::ensure!(
                    record.run_id == run_id,
                    "prepared source split run identity mismatch"
                );
                anyhow::ensure!(
                    source_acquisitions
                        .insert(
                            split.cohort_id.clone(),
                            (event, record.ledger_seq, record.ts_ms),
                        )
                        .is_none(),
                    "duplicate prepared source split cohort_id={}",
                    split.cohort_id
                );
                anyhow::ensure!(
                    ledger_sequences.insert(record.ledger_seq),
                    "duplicate central receipt ledger_seq={}",
                    record.ledger_seq
                );
            }
            Some(event @ CanonicalCtfEvent::BuyConfirmed(buy))
                if condition_belongs_to_catalog(&buy.condition_id, catalog) =>
            {
                validate_ctf_event_record(record)?;
                anyhow::ensure!(
                    record.run_id == run_id && buy.authorization.symbol.as_str() == symbol,
                    "prepared source BUY run or symbol identity mismatch"
                );
                anyhow::ensure!(
                    source_buy_receipt_ids.insert(buy.receipt_id.as_str()),
                    "duplicate prepared source BUY receipt_id={}",
                    buy.receipt_id
                );
                anyhow::ensure!(
                    ledger_sequences.insert(record.ledger_seq),
                    "duplicate central receipt ledger_seq={}",
                    record.ledger_seq
                );
                if let Some((source, source_ledger_seq, source_ts_ms)) =
                    source_acquisitions.get_mut(&buy.cohort_id)
                {
                    let CanonicalCtfEvent::BuyConfirmed(original) = *source else {
                        anyhow::bail!("prepared cohort has conflicting acquisition operations");
                    };
                    anyhow::ensure!(
                        original.authorization == buy.authorization
                            && original.market_id == buy.market_id
                            && original.condition_id == buy.condition_id
                            && original.token == buy.token,
                        "prepared partial BUY differs from its original acquisition"
                    );
                    *source_ledger_seq = (*source_ledger_seq).max(record.ledger_seq);
                    *source_ts_ms = (*source_ts_ms).max(record.ts_ms);
                } else {
                    source_acquisitions.insert(
                        buy.cohort_id.clone(),
                        (event, record.ledger_seq, record.ts_ms),
                    );
                }
            }
            _ => {}
        }
    }
    let mut terminal_by_tx = BTreeMap::new();
    for record in records {
        let (operation, terminal) = match record.ctf_event.as_ref() {
            Some(CanonicalCtfEvent::MergeConfirmed(terminal)) => {
                (DirectEoaOperation::Merge, terminal)
            }
            Some(CanonicalCtfEvent::RedeemConfirmed(terminal)) => {
                (DirectEoaOperation::Redeem, terminal)
            }
            _ => continue,
        };
        if !condition_belongs_to_catalog(&terminal.condition_id, catalog) {
            continue;
        }
        validate_ctf_event_record(record)?;
        let Some((receipt_id, transaction_id)) = terminal.confirmed_receipt_identity() else {
            continue;
        };
        anyhow::ensure!(
            record.run_id == run_id
                && receipt_id == format!("settlement_collateral:{transaction_id}")
                && record.settlement_tx_hash.as_deref() == Some(transaction_id),
            "prepared terminal record run or settlement identity mismatch"
        );
        let (source, source_ledger_seq, source_ts_ms) = source_acquisitions
            .get(&terminal.cohort_id)
            .context("prepared terminal record has no source acquisition cohort")?;
        anyhow::ensure!(
            *source_ledger_seq < record.ledger_seq && *source_ts_ms <= record.ts_ms,
            "prepared terminal record precedes its source acquisition"
        );
        match source {
            CanonicalCtfEvent::SplitConfirmed(split) => {
                anyhow::ensure!(
                    split.market_id == terminal.market_id
                        && split.condition_id == terminal.condition_id
                        && split.up_token == terminal.up_token
                        && split.down_token == terminal.down_token,
                    "prepared terminal record differs from its causal source split"
                );
            }
            CanonicalCtfEvent::BuyConfirmed(buy) => {
                anyhow::ensure!(
                    operation == DirectEoaOperation::Redeem
                        && buy.market_id == terminal.market_id
                        && buy.condition_id == terminal.condition_id
                        && buy.authorization.up_token.to_string() == terminal.up_token
                        && buy.authorization.down_token.to_string() == terminal.down_token
                        && if buy.authorization.selected_up {
                            terminal.down_terminal_qty_units_6dp == 0
                        } else {
                            terminal.up_terminal_qty_units_6dp == 0
                        },
                    "prepared terminal record differs from its causal source BUY"
                );
            }
            _ => anyhow::bail!("prepared terminal source is not a confirmed acquisition"),
        }
        anyhow::ensure!(
            terminal_by_tx
                .insert(
                    transaction_id.to_string(),
                    (operation, terminal.clone(), record.ledger_seq, record.ts_ms),
                )
                .is_none(),
            "duplicate prepared terminal record tx_hash={transaction_id}"
        );
    }
    for receipt in receipts {
        match receipt {
            SessionReceipt::SettlementCollateral(receipt)
                if matches!(
                    &receipt.outcome,
                    SettlementCollateralOutcome::Confirmed { .. }
                ) && condition_belongs_to_catalog(&receipt.condition_id, catalog) =>
            {
                let validated = validate_confirmed_settlement_receipt_lineage(
                    receipts,
                    run_id,
                    &receipt.tx_hash,
                )?
                .context("confirmed settlement receipt disappeared during lineage validation")?;
                anyhow::ensure!(
                    validated.collateral == *receipt,
                    "prepared settlement differs from canonical validated lineage"
                );
                anyhow::ensure!(
                    receipt.run_id == run_id,
                    "prepared settlement receipt run identity mismatch"
                );
                let (terminal_operation, terminal, terminal_ledger_seq, terminal_ts_ms) =
                    terminal_by_tx.remove(&receipt.tx_hash).with_context(|| {
                        format!(
                            "prepared settlement has no canonical terminal record tx_hash={}",
                            receipt.tx_hash
                        )
                    })?;
                anyhow::ensure!(
                    terminal_operation == receipt.operation
                        && terminal.condition_id == receipt.condition_id
                        && receipt.ledger_seq < terminal_ledger_seq
                        && receipt.ts_ms <= terminal_ts_ms,
                    "prepared settlement/terminal operation, condition, or ordering mismatch"
                );
                anyhow::ensure!(
                    settlement_ids.insert(receipt.tx_hash.clone()),
                    "duplicate confirmed settlement collateral tx_hash={}",
                    receipt.tx_hash
                );
                for ledger_seq in [
                    validated.submission.ledger_seq,
                    validated.gas.ledger_seq,
                    validated.collateral.ledger_seq,
                ] {
                    anyhow::ensure!(
                        ledger_sequences.insert(ledger_seq),
                        "duplicate central receipt ledger_seq={ledger_seq}"
                    );
                }
                anyhow::ensure!(
                    ledger_sequences.insert(terminal_ledger_seq),
                    "duplicate central receipt ledger_seq={terminal_ledger_seq}"
                );
                settlements.push(PreparedSettlementReceipt {
                    lineage: validated,
                    terminal,
                    terminal_ledger_seq,
                    terminal_ts_ms,
                });
            }
            SessionReceipt::ReceiptConflict(_) => {}
            SessionReceipt::DirectEoaSubmission(_)
            | SessionReceipt::DirectEoaGas(_)
            | SessionReceipt::MakerRebate(_)
            | SessionReceipt::SettlementCollateral(_) => {}
        }
    }
    let selected_settlement_ids = receipts
        .iter()
        .filter_map(|receipt| match receipt {
            SessionReceipt::SettlementCollateral(receipt)
                if condition_belongs_to_catalog(&receipt.condition_id, catalog) =>
            {
                Some(receipt.tx_hash.as_str())
            }
            _ => None,
        })
        .collect::<HashSet<_>>();
    for receipt in receipts {
        let SessionReceipt::ReceiptConflict(conflict) = receipt else {
            continue;
        };
        let settlement_conflict = conflict
            .receipt_identity
            .strip_prefix("settlement_collateral:")
            .is_some_and(|tx_hash| selected_settlement_ids.contains(tx_hash));
        anyhow::ensure!(
            !settlement_conflict
                && !source_buy_receipt_ids.contains(conflict.receipt_identity.as_str()),
            "captured finalized receipt identity is conflicted symbol={} identity={}",
            symbol,
            conflict.receipt_identity
        );
    }
    anyhow::ensure!(
        terminal_by_tx.is_empty(),
        "prepared terminal record has no confirmed settlement lineage"
    );
    validate_symbol_scope(&resolutions, &settlements, catalog, symbol)?;
    Ok((resolutions, settlements))
}

use std::collections::BTreeMap;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::polymarket::identity::MarketId;
use crate::types::TimeInForce;

use super::receipt_validation::validate_terminal_receipts;
use super::{
    deserialize_i128_string, deserialize_u128_string, deserialize_u64_string,
    serialize_i128_string, serialize_u128_string, serialize_u64_string, StampedFact,
};
#[path = "cohort_projection/financial.rs"]
mod financial;
pub(in crate::trade_session_ledger) use financial::{
    confirmed_event_cashflow, confirmed_split_cashflow, financial_event_order_key,
    project_retained_financial_cohort, validate_retained_financial_stamped_event,
    CohortFinancialState, FinancialCohortProjection, FinancialProjectionContribution,
    FinancialStampedEvent,
};
#[path = "cohort_projection/types.rs"]
mod types;
pub(in crate::trade_session_ledger) use types::CohortEntryIdentity;
pub use types::{
    CanonicalClosedCohortStats, CanonicalCohortCashflow, CanonicalCohortEvent,
    CanonicalCohortOrderIdentity, CanonicalCohortSellReceipt, CanonicalCohortTerminalReceipt,
    CanonicalCohortTerminalReceiptEvidence, CanonicalCtfLegRealization,
    CanonicalUnresolvedCohortState, CanonicalUnresolvedCohortStats,
};

use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCtfManagedSellFact, ConfirmedCtfSplitFact,
    ConfirmedCtfTerminalFact, ConfirmedOrderSubmissionFact, CtfTerminalOperation,
    CtfTerminalSettlementEvidence, FinalizedConditionResolutionReceipt, ManagedSellReceiptIdentity,
    SettlementCollateralOutcome, SettlementCollateralReceipt,
};

pub(super) enum ReducedCohort {
    Closed(CanonicalClosedCohortStats),
    Unresolved(CanonicalUnresolvedCohortStats),
}

pub(super) fn unresolved_cohort_from_error(
    cohort_id: &str,
    facts: &[StampedFact],
    detail: String,
) -> anyhow::Result<CanonicalUnresolvedCohortStats> {
    let entry_stamp = acquisition_stamp(facts)?;
    let entry = CohortEntryIdentity::from_event(&entry_stamp.fact)?;
    Ok(CanonicalUnresolvedCohortStats {
        symbol: entry.underlying.clone(),
        market_id: entry.market_id.clone(),
        market_name: entry.market_name.clone(),
        paired_cohort_key: cohort_id.to_string(),
        started_at_ms: entry_stamp.ts_ms,
        last_ts_ms: facts
            .iter()
            .map(|fact| fact.ts_ms)
            .max()
            .unwrap_or(entry_stamp.ts_ms),
        event_count: facts.len(),
        state: CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { detail },
    })
}

pub(super) fn reduce_cohort(
    cohort_id: &str,
    facts: &[StampedFact],
    order_submissions: Vec<(u64, ConfirmedOrderSubmissionFact)>,
    resolution_by_condition: &BTreeMap<String, &FinalizedConditionResolutionReceipt>,
    settlement_by_tx: &BTreeMap<String, &SettlementCollateralReceipt>,
) -> anyhow::Result<ReducedCohort> {
    let entry_stamp = acquisition_stamp(facts)?;
    let mut financial = CohortFinancialState::new(cohort_id, &entry_stamp.fact)?;
    for stamped in facts {
        if stamped.ledger_seq == entry_stamp.ledger_seq {
            continue;
        }
        if let CanonicalCtfEvent::MergeConfirmed(terminal)
        | CanonicalCtfEvent::RedeemConfirmed(terminal) = &stamped.fact
        {
            let operation = if matches!(&stamped.fact, CanonicalCtfEvent::MergeConfirmed(_)) {
                CtfTerminalOperation::Merge
            } else {
                CtfTerminalOperation::Redeem
            };
            validate_terminal_identity(financial.entry(), terminal)?;
            validate_terminal_receipts(
                stamped,
                operation,
                terminal,
                resolution_by_condition,
                settlement_by_tx,
            )?;
        }
        financial.apply_validated_event(&stamped.fact)?;
    }
    let entry = financial.entry();
    let realized_pnl_units_6dp = financial.realized_pnl_units()?;
    let summary = financial.summary()?;
    let cashflow = canonical_cashflow(entry, facts, order_submissions, &summary, settlement_by_tx)?;
    let last_ts_ms = facts
        .iter()
        .map(|fact| fact.ts_ms)
        .max()
        .unwrap_or(entry_stamp.ts_ms);
    let Some(realized_pnl_units_6dp) = realized_pnl_units_6dp else {
        return Ok(ReducedCohort::Unresolved(CanonicalUnresolvedCohortStats {
            symbol: entry.underlying.clone(),
            market_id: entry.market_id.clone(),
            market_name: entry.market_name.clone(),
            paired_cohort_key: cohort_id.to_string(),
            started_at_ms: entry_stamp.ts_ms,
            last_ts_ms,
            event_count: facts.len(),
            state: CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow },
        }));
    };
    Ok(ReducedCohort::Closed(CanonicalClosedCohortStats {
        symbol: entry.underlying.clone(),
        market_id: Some(entry.market_id.clone()),
        market_name: entry.market_name.clone(),
        trade_id: cohort_id.to_string(),
        paired_cohort_key: cohort_id.to_string(),
        authority_source: Some("trade_session_ledger".to_string()),
        owner: None,
        durability_state: Some("confirmed_immutable_receipts".to_string()),
        close_hint: None,
        reconciliation_status: Some("exact_share_conservation".to_string()),
        started_at_ms: entry_stamp.ts_ms,
        last_ts_ms,
        event_count: facts.len(),
        closed_outcome_share_quantity_units_6dp: cashflow
            .up_created_qty_units_6dp
            .checked_add(cashflow.down_created_qty_units_6dp)
            .context("closed outcome share quantity overflow")?,
        realized_pnl_units_6dp,
        cashflow,
    }))
}

pub(in crate::trade_session_ledger) fn order_submission_matches_entry(
    entry: &CohortEntryIdentity,
    submission: &ConfirmedOrderSubmissionFact,
) -> bool {
    (submission.buy_authorization.is_none()
        || submission.buy_authorization == entry.buy_authorization)
        && submission.market_id == entry.market_id
        && submission.cohort_id == entry.cohort_id
        && submission.underlying == entry.underlying
        && match entry.buy_authorization {
            Some(authorization) if authorization.selected_up => {
                submission.asset_id == entry.up_token
            }
            Some(_) => submission.asset_id == entry.down_token,
            None => {
                submission.asset_id == entry.up_token || submission.asset_id == entry.down_token
            }
        }
}

fn canonical_cashflow(
    entry: &CohortEntryIdentity,
    facts: &[StampedFact],
    order_submissions: Vec<(u64, ConfirmedOrderSubmissionFact)>,
    summary: &crate::ctf_accounting::CtfMarketEntryCashflowSummary,
    settlement_by_tx: &BTreeMap<String, &SettlementCollateralReceipt>,
) -> anyhow::Result<CanonicalCohortCashflow> {
    for (_, submission) in &order_submissions {
        anyhow::ensure!(
            order_submission_matches_entry(entry, submission),
            "order_submission_split_identity_mismatch:order_id={}",
            submission.order_id
        );
    }
    let quantity = |quantities: &BTreeMap<String, u128>, token: &str| {
        quantities.get(token).copied().unwrap_or(0)
    };
    let entry_stamp = acquisition_stamp(facts)?;
    let split = match &entry_stamp.fact {
        CanonicalCtfEvent::SplitConfirmed(split) => Some(split),
        _ => None,
    };
    anyhow::ensure!(
        facts
            .iter()
            .all(|stamped| stamped.run_id == entry_stamp.run_id),
        "cohort projection crosses run identity"
    );
    let cashflow = CanonicalCohortCashflow {
        buy_authorization: entry.buy_authorization,
        buy_principal_units_6dp: if entry.buy_authorization.is_some() {
            summary.principal_units
        } else {
            0
        },
        confirmed_collateral_refund_units_6dp: summary.confirmed_collateral_refund_units,
        up_remaining_basis_units_6dp: entry
            .buy_authorization
            .map(|_| quantity(&summary.remaining_basis_units_by_token, &entry.up_token)),
        down_remaining_basis_units_6dp: entry
            .buy_authorization
            .map(|_| quantity(&summary.remaining_basis_units_by_token, &entry.down_token)),
        run_id: entry_stamp.run_id.clone(),
        split_receipt_id: split
            .map(|value| value.receipt_id.clone())
            .unwrap_or_default(),
        split_transaction_id: split
            .map(|value| value.transaction_id.clone())
            .unwrap_or_default(),
        split_ledger_seq: split.map(|_| entry_stamp.ledger_seq).unwrap_or(0),
        split_ts_ms: split.map(|_| entry_stamp.ts_ms).unwrap_or(0),
        condition_id: entry.condition_id.clone(),
        up_token: entry.up_token.clone(),
        down_token: entry.down_token.clone(),
        up_created_qty_units_6dp: quantity(&summary.created_qty_by_token, &entry.up_token),
        down_created_qty_units_6dp: quantity(&summary.created_qty_by_token, &entry.down_token),
        up_confirmed_sold_qty_units_6dp: quantity(
            &summary.confirmed_sold_qty_by_token,
            &entry.up_token,
        ),
        down_confirmed_sold_qty_units_6dp: quantity(
            &summary.confirmed_sold_qty_by_token,
            &entry.down_token,
        ),
        up_confirmed_merged_qty_units_6dp: quantity(
            &summary.confirmed_merged_qty_by_token,
            &entry.up_token,
        ),
        down_confirmed_merged_qty_units_6dp: quantity(
            &summary.confirmed_merged_qty_by_token,
            &entry.down_token,
        ),
        up_confirmed_redeemed_qty_units_6dp: quantity(
            &summary.confirmed_redeemed_qty_by_token,
            &entry.up_token,
        ),
        down_confirmed_redeemed_qty_units_6dp: quantity(
            &summary.confirmed_redeemed_qty_by_token,
            &entry.down_token,
        ),
        up_confirmed_disposed_qty_units_6dp: quantity(
            &summary.confirmed_disposed_qty_by_token,
            &entry.up_token,
        ),
        down_confirmed_disposed_qty_units_6dp: quantity(
            &summary.confirmed_disposed_qty_by_token,
            &entry.down_token,
        ),
        up_remaining_qty_units_6dp: quantity(&summary.remaining_qty_by_token, &entry.up_token),
        down_remaining_qty_units_6dp: quantity(&summary.remaining_qty_by_token, &entry.down_token),
        split_principal_units_6dp: split
            .map(|value| value.collateral_debit_units_6dp)
            .unwrap_or(0),
        split_fee_units_6dp: split.map(|value| value.fee_units_6dp).unwrap_or(0),
        split_approved_direct_cost_units_6dp: split
            .map(|value| value.approved_direct_cost_units_6dp)
            .unwrap_or(0),
        confirmed_sell_proceeds_units_6dp: summary.confirmed_sell_proceeds_units,
        confirmed_terminal_receipts_units_6dp: summary.confirmed_terminal_receipts_units,
        confirmed_fee_units_6dp: summary.confirmed_fee_units,
        confirmed_approved_direct_cost_units_6dp: summary.confirmed_approved_direct_cost_units,
        events: canonical_events(facts),
        order_submissions: canonical_order_submissions(order_submissions),
        managed_sell_receipts: facts
            .iter()
            .filter_map(|stamped| match &stamped.fact {
                CanonicalCtfEvent::TradeConfirmed(sell) => Some((stamped, sell)),
                _ => None,
            })
            .map(|(stamped, sell)| {
                let realization = summary
                    .sell_realizations
                    .get(&sell.receipt_id)
                    .context("confirmed sell receipt has no accounting realization")?;
                Ok(CanonicalCohortSellReceipt {
                    ledger_seq: stamped.ledger_seq,
                    receipt_id: sell.receipt_id.clone(),
                    fill_id: sell.fill_id.clone(),
                    receipt_identity: sell.receipt_identity.clone(),
                    order_id: sell.order_id.clone(),
                    intent_id: sell.intent_id,
                    token: sell.token.clone(),
                    qty_units_6dp: sell.qty_units_6dp,
                    gross_proceeds_units_6dp: sell.gross_proceeds_units_6dp,
                    fee_units_6dp: sell.fee_units_6dp,
                    approved_direct_cost_units_6dp: sell.approved_direct_cost_units_6dp,
                    confirmed_at_ms: sell.confirmed_at_ms,
                    close_reason: sell.close_reason.clone(),
                    realization: canonical_realization(*realization),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        terminal_receipts: canonical_terminal_receipts(facts, settlement_by_tx, summary)?,
    };
    let realization = cashflow
        .total_realization()
        .context("canonical cashflow realization projection overflow")?;
    anyhow::ensure!(
        summary.remaining_qty_by_token.values().any(|qty| *qty > 0)
            || (realization.confirmed_proceeds_units_6dp
                == summary
                    .confirmed_sell_proceeds_units
                    .checked_add(summary.confirmed_terminal_receipts_units)
                    .context("canonical receipt total overflow")?
                && realization.allocated_principal_units_6dp == summary.principal_units
                && realization.allocated_collateral_refund_units_6dp
                    == summary.confirmed_collateral_refund_units
                && realization.attributable_fee_units_6dp == summary.confirmed_fee_units
                && realization.attributable_approved_direct_cost_units_6dp
                    == summary.confirmed_approved_direct_cost_units),
        "canonical leg realizations do not reconcile to cohort cashflow"
    );
    Ok(cashflow)
}

fn canonical_terminal_receipts(
    facts: &[StampedFact],
    settlement_by_tx: &BTreeMap<String, &SettlementCollateralReceipt>,
    summary: &crate::ctf_accounting::CtfMarketEntryCashflowSummary,
) -> anyhow::Result<Vec<CanonicalCohortTerminalReceipt>> {
    facts
        .iter()
        .filter_map(|stamped| match &stamped.fact {
            CanonicalCtfEvent::MergeConfirmed(terminal) => {
                Some((stamped, terminal, CtfTerminalOperation::Merge))
            }
            CanonicalCtfEvent::RedeemConfirmed(terminal) => {
                Some((stamped, terminal, CtfTerminalOperation::Redeem))
            }
            _ => None,
        })
        .map(|(stamped, terminal, operation)| {
            let evidence = match &terminal.settlement_evidence {
                CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt {
                    receipt_id,
                    transaction_id,
                } => {
                    let settlement = settlement_by_tx
                        .get(transaction_id)
                        .context("terminal audit has no finalized collateral receipt")?;
                    anyhow::ensure!(
                        settlement.accounting_receipt_id() == *receipt_id,
                        "terminal audit receipt identity does not equal finalized collateral receipt"
                    );
                    let SettlementCollateralOutcome::Confirmed { wrapped, .. } =
                        &settlement.outcome
                    else {
                        anyhow::bail!("terminal audit collateral receipt is not confirmed");
                    };
                    CanonicalCohortTerminalReceiptEvidence::ConfirmedCollateralReceipt {
                        transaction_id: transaction_id.clone(),
                        settlement_run_id: settlement.run_id.clone(),
                        settlement_ledger_seq: settlement.ledger_seq,
                        settlement_ts_ms: settlement.ts_ms,
                        finalized_log_index: wrapped.log.log_index,
                        finalized_log_emitter: wrapped.log.emitter.clone(),
                        finalized_block_number: wrapped.log.block_number,
                        finalized_block_hash: wrapped.log.block_hash.clone(),
                    }
                }
                CtfTerminalSettlementEvidence::SimulatedMergeOperation { operation_id } => {
                    anyhow::ensure!(
                        operation == CtfTerminalOperation::Merge
                            && operation_id == terminal.terminal_operation_identity(),
                        "simulated terminal evidence is not an exact merge operation"
                    );
                    CanonicalCohortTerminalReceiptEvidence::SimulatedMergeOperation {
                        operation_id: operation_id.clone(),
                    }
                }
                CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
                    operation_id,
                    condition_id,
                    up_token,
                    down_token,
                    winning_token,
                    resolution_observed_at_ms,
                    raw_response_sha256,
                } => {
                    anyhow::ensure!(
                        operation == CtfTerminalOperation::Redeem
                            && operation_id == terminal.terminal_operation_identity()
                            && condition_id == &terminal.condition_id
                            && up_token == &terminal.up_token
                            && down_token == &terminal.down_token,
                        "simulated terminal evidence is not an exact resolution redemption"
                    );
                    CanonicalCohortTerminalReceiptEvidence::SimulatedResolutionRedemption {
                        operation_id: operation_id.clone(),
                        condition_id: condition_id.clone(),
                        up_token: up_token.clone(),
                        down_token: down_token.clone(),
                        winning_token: winning_token.clone(),
                        resolution_observed_at_ms: *resolution_observed_at_ms,
                        raw_response_sha256: raw_response_sha256.clone(),
                    }
                }
            };
            let receipt_id = terminal.settlement_id();
            let realization = |token: &str, qty: u128| -> anyhow::Result<_> {
                (qty > 0)
                    .then(|| {
                        summary
                            .terminal_realizations
                            .get(&(receipt_id.clone(), token.to_string()))
                            .copied()
                            .map(canonical_realization)
                            .context("terminal token has no accounting realization")
                    })
                    .transpose()
            };
            Ok(CanonicalCohortTerminalReceipt {
                accounting_ledger_seq: stamped.ledger_seq,
                receipt_id: receipt_id.clone(),
                operation,
                evidence,
                up_terminal_qty_units_6dp: terminal.up_terminal_qty_units_6dp,
                down_terminal_qty_units_6dp: terminal.down_terminal_qty_units_6dp,
                collateral_receipt_units_6dp: terminal.collateral_proceeds_units_6dp,
                fee_units_6dp: terminal.fee_units_6dp,
                approved_direct_cost_units_6dp: terminal.approved_direct_cost_units_6dp,
                up_realization: realization(
                    &terminal.up_token,
                    terminal.up_terminal_qty_units_6dp,
                )?,
                down_realization: realization(
                    &terminal.down_token,
                    terminal.down_terminal_qty_units_6dp,
                )?,
            })
        })
        .collect()
}

fn canonical_realization(
    value: crate::ctf_accounting::CtfDisposalRealization,
) -> CanonicalCtfLegRealization {
    CanonicalCtfLegRealization {
        confirmed_proceeds_units_6dp: value.proceeds,
        allocated_principal_units_6dp: value.principal,
        allocated_collateral_refund_units_6dp: value.refunds,
        attributable_fee_units_6dp: value.fees,
        attributable_approved_direct_cost_units_6dp: value.costs,
        realized_pnl_units_6dp: value.pnl,
    }
}

fn canonical_events(facts: &[StampedFact]) -> Vec<CanonicalCohortEvent> {
    facts
        .iter()
        .map(|stamped| CanonicalCohortEvent {
            ledger_seq: stamped.ledger_seq,
            ts_ms: stamped.ts_ms,
            event: stamped.fact.clone(),
        })
        .collect()
}

pub(in crate::trade_session_ledger) fn canonical_order_submissions(
    order_submissions: Vec<(u64, ConfirmedOrderSubmissionFact)>,
) -> Vec<CanonicalCohortOrderIdentity> {
    order_submissions
        .into_iter()
        .map(|(ledger_seq, submission)| CanonicalCohortOrderIdentity {
            ledger_seq,
            order_id: submission.order_id,
            intent_id: submission.intent_id,
            asset_id: submission.asset_id,
            instruction_qty_units_6dp: submission.instruction_qty_units_6dp,
            instruction_limit_px_units_8dp: submission.instruction_limit_px_units_8dp,
            time_in_force: submission.time_in_force,
            post_only: submission.post_only,
            accepted_at_ms: submission.accepted_at_ms,
        })
        .collect()
}

pub(in crate::trade_session_ledger) fn validate_sell_identity(
    entry: &CohortEntryIdentity,
    sell: &ConfirmedCtfManagedSellFact,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        sell.market_id == entry.market_id
            && sell.condition_id == entry.condition_id
            && sell.cohort_id == entry.cohort_id
            && (sell.token == entry.up_token || sell.token == entry.down_token)
            && entry.buy_authorization.is_none_or(|authorization| {
                sell.close_reason == crate::types::OrderCloseReason::PairedPostProceedsSell.as_str()
                    && if authorization.selected_up {
                        sell.token == entry.up_token
                    } else {
                        sell.token == entry.down_token
                    }
            }),
        "managed_sell_identity_mismatch:receipt_id={}",
        sell.receipt_id
    );
    Ok(())
}

fn validate_terminal_identity(
    entry: &CohortEntryIdentity,
    terminal: &ConfirmedCtfTerminalFact,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        terminal.market_id == entry.market_id
            && terminal.condition_id == entry.condition_id
            && terminal.cohort_id == entry.cohort_id
            && terminal.up_token == entry.up_token
            && terminal.down_token == entry.down_token,
        "terminal_identity_mismatch:settlement_id={}",
        terminal.settlement_id()
    );
    Ok(())
}

pub(super) fn acquisition_stamp(facts: &[StampedFact]) -> anyhow::Result<&StampedFact> {
    facts
        .iter()
        .find(|stamped| {
            matches!(
                &stamped.fact,
                CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_)
            )
        })
        .context("cohort has no confirmed acquisition ledger fact")
}

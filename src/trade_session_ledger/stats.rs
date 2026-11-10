use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::strategy::facts::CtfOwnedLegCloseReason;

pub(super) use super::exact_serde::{
    deserialize_i128_string, deserialize_u128_string, deserialize_u64_string,
    serialize_i128_string, serialize_u128_string, serialize_u64_string,
};
use super::{
    CanonicalCtfEvent, CanonicalOwnedLegOutcome, ConfirmedOrderSubmissionFact,
    CtfTerminalOperation, FinalizedConditionResolutionReceipt, SettlementCollateralReceipt,
    TradeLedgerRecord, TradeLedgerRunIndex,
};

#[path = "stats/exit_projection.rs"]
mod exit_projection;
pub use exit_projection::{
    CanonicalCtfExitBucket, CanonicalCtfExitClass, CanonicalCtfExitCohortRow,
    CanonicalCtfExitSummaryRow,
};
#[path = "stats/schema.rs"]
mod schema;
pub(in crate::trade_session_ledger) use schema::{finish_financial_rows, FinancialRowDelta};
pub use schema::{
    CanonicalCtfSummaryRow, CanonicalFinancialProjection, CanonicalFinancialView,
    CanonicalSessionStats, CanonicalStatsProof, ExactOpenUnrecoveredCash,
};

#[path = "stats/audit_facts.rs"]
mod audit_facts;
#[path = "stats/causal_metrics.rs"]
mod causal_metrics;
#[path = "stats/cohort_projection.rs"]
mod cohort_projection;
#[path = "stats/coverage.rs"]
mod coverage;
#[path = "stats/ctf_summary.rs"]
mod ctf_summary;
#[path = "stats/order_terminal_summary.rs"]
mod order_terminal_summary;
#[path = "stats/receipt_validation.rs"]
mod receipt_validation;
#[path = "stats/submission_projection.rs"]
mod submission_projection;
use audit_facts::StampedFact;
pub(in crate::trade_session_ledger) use audit_facts::{
    FinancialCashflowKey, FinancialIdentityIndex, IdentityRejection,
};
use causal_metrics::populate_causal_metrics;
pub(in crate::trade_session_ledger) use cohort_projection::{
    confirmed_event_cashflow, confirmed_split_cashflow, financial_event_order_key,
    order_submission_matches_entry, project_retained_financial_cohort,
    validate_retained_financial_stamped_event, validate_sell_identity, CohortEntryIdentity,
    CohortFinancialState, FinancialCohortProjection, FinancialProjectionContribution,
    FinancialStampedEvent,
};
use cohort_projection::{reduce_cohort, unresolved_cohort_from_error, ReducedCohort};

pub(in crate::trade_session_ledger) fn validate_financial_submission(
    entry: &CohortEntryIdentity,
    submission: &ConfirmedOrderSubmissionFact,
    outcome: Option<&CanonicalOwnedLegOutcome>,
    summary: &mut CanonicalCtfSummaryRow,
    violations: &mut Vec<String>,
) -> bool {
    submission_projection::validate_and_count(entry, submission, outcome, summary, violations)
}
pub use cohort_projection::{
    CanonicalClosedCohortStats, CanonicalCohortCashflow, CanonicalCohortEvent,
    CanonicalCohortOrderIdentity, CanonicalCohortSellReceipt, CanonicalCohortTerminalReceipt,
    CanonicalCohortTerminalReceiptEvidence, CanonicalCtfLegRealization,
    CanonicalUnresolvedCohortState, CanonicalUnresolvedCohortStats,
};
pub use coverage::{CanonicalCoverageGap, CanonicalCoverageGapKind, CanonicalCoverageProof};
pub use ctf_summary::ExactRealizedPnl;
use ctf_summary::{checked_add_amount, checked_add_quantity, checked_increment};
pub use order_terminal_summary::{
    CanonicalOrderExecutionLineageRow, CanonicalOrderExecutionTerminalFact,
    CanonicalOrderTerminalSummaryRow,
};

impl CanonicalSessionStats {
    pub(crate) fn project_venue_outcomes(
        &mut self,
        resolutions: Option<&BTreeMap<String, &FinalizedConditionResolutionReceipt>>,
        captured: Option<&crate::polymarket::state::PolymarketOddsState>,
    ) {
        let cashflows = self
            .closed_cohorts
            .iter()
            .map(|cohort| (cohort.paired_cohort_key.as_str(), &cohort.cashflow))
            .chain(
                self.unresolved_cohorts
                    .iter()
                    .filter_map(|cohort| match &cohort.state {
                        CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                            cashflow,
                        } => Some((cohort.paired_cohort_key.as_str(), cashflow)),
                        _ => None,
                    }),
            )
            .collect::<BTreeMap<_, _>>();
        for row in &mut self.canonical.ctf_exit_cohorts {
            let outcome = cashflows
                .get(row.paired_cohort_key.as_str())
                .filter(|cashflow| cashflow.condition_id == row.market_id)
                .and_then(|cashflow| match captured {
                    Some(odds) => exit_projection::captured_venue_outcome(cashflow, odds),
                    None => exit_projection::finalized_venue_outcome(
                        cashflow,
                        resolutions?.get(&cashflow.condition_id)?,
                    ),
                });
            let (side, observed_at_ms, provenance) = match outcome {
                Some((side, observed_at_ms, provenance)) => (
                    Some(side.to_string()),
                    Some(observed_at_ms),
                    Some(provenance),
                ),
                None if captured.is_some() => continue,
                None => (None, None, None),
            };
            row.venue_winner_side = Some(side);
            row.venue_resolution_observed_at_ms = Some(observed_at_ms);
            row.venue_resolution_provenance = Some(provenance);
        }
    }

    pub(super) fn from_resident_audit(
        index: &TradeLedgerRunIndex,
        records: &[TradeLedgerRecord],
        expected_symbols: &[String],
        financial: &CanonicalFinancialProjection,
    ) -> Self {
        audit_facts::project_resident(index, records, expected_symbols, financial)
    }

    pub(super) fn from_audit(
        index: &TradeLedgerRunIndex,
        records: &[TradeLedgerRecord],
        settlement_receipts: &[SettlementCollateralReceipt],
        expected_symbols: &[String],
    ) -> Self {
        let normalized_symbols = expected_symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let revision = records
            .iter()
            .map(|record| record.ledger_seq)
            .chain(settlement_receipts.iter().map(|receipt| receipt.ledger_seq))
            .max()
            .unwrap_or_default();
        let mut stats = Self {
            canonical: CanonicalStatsProof::unavailable(revision, &normalized_symbols),
            market_count: index.market_count,
            trade_count: index.trade_count,
            event_count: index.event_count,
            ..Self::unavailable(&normalized_symbols)
        };
        if normalized_symbols.len() != expected_symbols.len()
            || normalized_symbols.is_empty()
            || normalized_symbols
                .iter()
                .any(|symbol| !matches!(symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP"))
        {
            stats
                .violations
                .push("invalid_canonical_expected_symbol_scope".to_string());
        }
        let (owned_leg_decision_history, owned_leg_outcomes) =
            super::owned_leg_outcomes::project_owned_leg_outcomes(records, &mut stats.violations);
        let (mut symbol_summary, mut confirmed_entry_markets, sell_intents) =
            ctf_summary::initialize(
                &stats.canonical.ctf_summary,
                &normalized_symbols,
                &owned_leg_outcomes,
                &owned_leg_decision_history,
                &mut stats.violations,
            );
        let audit_facts = audit_facts::project(records, settlement_receipts, &mut stats.violations);
        let audit_facts::AuditFacts {
            cohorts,
            mut submissions,
            invalid_cohort_receipt_errors,
            resolution_by_condition,
            settlement_by_tx,
        } = audit_facts;
        let (order_terminal_summary, order_execution_lineage) = order_terminal_summary::project(
            records,
            &normalized_symbols,
            &sell_intents,
            &submissions,
            &mut stats.violations,
        );
        stats.canonical.order_terminal_summary = order_terminal_summary;
        stats.canonical.order_execution_lineage = order_execution_lineage;
        for (cohort_id, mut cohort) in cohorts {
            let buy_origin = cohort
                .facts
                .iter()
                .filter(|fact| {
                    matches!(
                        &fact.fact,
                        CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_)
                    )
                })
                .min_by_key(|fact| fact.ledger_seq)
                .is_some_and(|fact| matches!(&fact.fact, CanonicalCtfEvent::BuyConfirmed(_)));
            cohort.facts.sort_by_key(|fact| {
                financial_event_order_key(buy_origin, fact.ts_ms, fact.ledger_seq)
            });
            let entry_stamp = match cohort_projection::acquisition_stamp(&cohort.facts) {
                Ok(stamp) => stamp,
                Err(error) => {
                    stats
                        .violations
                        .push(format!("invalid_ctf_cohort:cohort={cohort_id}:{error}"));
                    continue;
                }
            };
            let entry = match CohortEntryIdentity::from_event(&entry_stamp.fact) {
                Ok(entry) => entry,
                Err(error) => {
                    stats
                        .violations
                        .push(format!("invalid_ctf_cohort:cohort={cohort_id}:{error}"));
                    continue;
                }
            };
            let Some(summary) = symbol_summary.get_mut(&entry.underlying) else {
                stats.violations.push(format!(
                    "ctf_cohort_unknown_symbol:cohort={cohort_id}:symbol={}",
                    entry.underlying
                ));
                continue;
            };
            let symbol = &entry.underlying;
            if let CanonicalCtfEvent::SplitConfirmed(split) = &entry_stamp.fact {
                checked_increment(
                    &mut summary.split_entries,
                    &mut stats.violations,
                    format!("ctf_symbol_split_entry_count_overflow:symbol={symbol}"),
                );
                checked_add_quantity(
                    &mut summary.split_principal_units_6dp,
                    split.collateral_debit_units_6dp,
                    &mut stats.violations,
                    format!("ctf_symbol_split_principal_overflow:symbol={symbol}"),
                );
                match split.up_qty_units_6dp.checked_add(split.down_qty_units_6dp) {
                    Some(quantity) => checked_add_quantity(
                        &mut summary.created_share_quantity_units_6dp,
                        quantity,
                        &mut stats.violations,
                        format!("ctf_symbol_created_share_quantity_overflow:symbol={symbol}"),
                    ),
                    None => stats.violations.push(format!(
                        "ctf_symbol_created_share_quantity_overflow:symbol={symbol}"
                    )),
                }
            } else {
                for fact in &cohort.facts {
                    if let CanonicalCtfEvent::BuyConfirmed(buy) = &fact.fact {
                        checked_add_quantity(
                            &mut summary.created_share_quantity_units_6dp,
                            buy.qty_units_6dp,
                            &mut stats.violations,
                            format!("ctf_symbol_created_share_quantity_overflow:symbol={symbol}"),
                        );
                    }
                }
            }
            if let Some(markets) = confirmed_entry_markets.get_mut(symbol) {
                markets.insert(entry.market_id.to_string());
            }
            let mut cohort_submissions = BTreeMap::new();
            for (ledger_seq, submission) in submissions.remove(&cohort_id).unwrap_or_default() {
                if !submission_projection::validate_and_count(
                    &entry,
                    &submission,
                    sell_intents.get(&submission.intent_id).copied(),
                    summary,
                    &mut stats.violations,
                ) {
                    continue;
                }
                cohort_submissions.insert(submission.order_id.clone(), (ledger_seq, submission));
            }
            let mut cohort_receipt_error = invalid_cohort_receipt_errors.get(&cohort_id).cloned();
            for stamped in &cohort.facts {
                match &stamped.fact {
                    CanonicalCtfEvent::SplitConfirmed(_)
                    | CanonicalCtfEvent::BuyConfirmed(_)
                    | CanonicalCtfEvent::MergeConfirmed(_)
                    | CanonicalCtfEvent::RedeemConfirmed(_) => {}
                    CanonicalCtfEvent::TradeConfirmed(sell) => {
                        if let Err(error) = validate_sell_identity(&entry, sell) {
                            cohort_receipt_error.get_or_insert_with(|| error.to_string());
                            stats
                                .violations
                                .push(format!("invalid_ctf_cohort:cohort={cohort_id}:{error}"));
                            continue;
                        }
                        let Some((_, submission)) = cohort_submissions.get(&sell.order_id) else {
                            let error = format!(
                                "managed_sell_without_accepted_submission:fill_id={}:order_id={}",
                                sell.fill_id, sell.order_id
                            );
                            cohort_receipt_error.get_or_insert_with(|| error.clone());
                            stats.violations.push(error);
                            continue;
                        };
                        if submission.intent_id != sell.intent_id
                            || submission.market_id != sell.market_id
                            || submission.cohort_id != sell.cohort_id
                            || submission.asset_id != sell.token
                            || submission.accepted_at_ms > sell.confirmed_at_ms
                        {
                            let error = format!(
                                "managed_sell_submission_identity_mismatch:fill_id={}:order_id={}",
                                sell.fill_id, sell.order_id
                            );
                            cohort_receipt_error.get_or_insert_with(|| error.clone());
                            stats.violations.push(error);
                            continue;
                        }
                        let Some(outcome) = sell_intents.get(&sell.intent_id) else {
                            let error = format!(
                                "managed_sell_without_sell_now_owner_intent:fill_id={}:intent_id={}",
                                sell.fill_id, sell.intent_id
                            );
                            cohort_receipt_error.get_or_insert_with(|| error.clone());
                            stats.violations.push(error);
                            continue;
                        };
                        let expected_close_reason = match outcome.close_reason {
                            Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) => {
                                "paired_first_managed_sell"
                            }
                            Some(CtfOwnedLegCloseReason::PairedPostProceedsSell) => {
                                "paired_post_proceeds_sell"
                            }
                            None => continue,
                        };
                        if outcome.symbol != entry.underlying
                            || outcome.market_id != entry.market_id
                            || outcome.cohort_id != entry.cohort_id
                            || outcome.token != sell.token
                            || sell.close_reason != expected_close_reason
                        {
                            let error = format!(
                                "managed_sell_owner_intent_identity_mismatch:fill_id={}:intent_id={}",
                                sell.fill_id, sell.intent_id
                            );
                            cohort_receipt_error.get_or_insert_with(|| error.clone());
                            stats.violations.push(error);
                            continue;
                        }
                        checked_increment(
                            &mut summary.confirmed_managed_sell_fill_count,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_managed_sell_fill_count_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                        checked_add_quantity(
                            &mut summary.confirmed_managed_sell_quantity_units_6dp,
                            sell.qty_units_6dp,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_managed_sell_quantity_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                        checked_add_amount(
                            &mut summary.confirmed_managed_sell_proceeds_units_6dp,
                            sell.gross_proceeds_units_6dp,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_managed_sell_proceeds_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                        let (fill_count, fill_quantity, fill_proceeds) = match outcome.close_reason
                        {
                            Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) => (
                                &mut summary.paired_first_managed_sell_confirmed_fill_count,
                                &mut summary
                                    .paired_first_managed_sell_confirmed_fill_quantity_units_6dp,
                                &mut summary.paired_first_managed_sell_confirmed_proceeds_units_6dp,
                            ),
                            Some(CtfOwnedLegCloseReason::PairedPostProceedsSell) => (
                                &mut summary.paired_post_proceeds_sell_confirmed_fill_count,
                                &mut summary
                                    .paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp,
                                &mut summary.paired_post_proceeds_sell_confirmed_proceeds_units_6dp,
                            ),
                            None => continue,
                        };
                        checked_increment(
                            fill_count,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_close_reason_fill_count_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                        checked_add_amount(
                            fill_proceeds,
                            sell.gross_proceeds_units_6dp,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_close_reason_fill_proceeds_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                        checked_add_quantity(
                            fill_quantity,
                            sell.qty_units_6dp,
                            &mut stats.violations,
                            format!(
                                "ctf_symbol_close_reason_fill_quantity_overflow:symbol={}",
                                entry.underlying
                            ),
                        );
                    }
                    CanonicalCtfEvent::OrderAccepted(_)
                    | CanonicalCtfEvent::TradeMatched(_)
                    | CanonicalCtfEvent::TradeFailed(_)
                    | CanonicalCtfEvent::ResolutionObserved(_) => {}
                    CanonicalCtfEvent::FeeApplied(charge)
                    | CanonicalCtfEvent::CostApplied(charge) => {
                        let error = format!(
                            "standalone_charge_not_bound_to_cashflow:receipt_id={}",
                            charge.receipt_id
                        );
                        cohort_receipt_error.get_or_insert_with(|| error.clone());
                        stats.violations.push(error);
                    }
                }
            }
            if let Some(error) = cohort_receipt_error {
                match unresolved_cohort_from_error(&cohort_id, &cohort.facts, error) {
                    Ok(unresolved) => stats.unresolved_cohorts.push(unresolved),
                    Err(projection_error) => stats.violations.push(format!(
                        "invalid_ctf_cohort_projection:cohort={cohort_id}:{projection_error}"
                    )),
                }
                continue;
            }
            let cohort_submissions = cohort_submissions.into_values().collect::<Vec<_>>();
            match reduce_cohort(
                &cohort_id,
                &cohort.facts,
                cohort_submissions.clone(),
                &resolution_by_condition,
                &settlement_by_tx,
            ) {
                Ok(ReducedCohort::Closed(closed)) => {
                    stats.closed_cohorts.push(closed);
                }
                Ok(ReducedCohort::Unresolved(unresolved)) => {
                    stats.unresolved_cohorts.push(unresolved);
                }
                Err(error) => {
                    stats
                        .violations
                        .push(format!("invalid_ctf_cohort:cohort={cohort_id}:{error}"));
                    match unresolved_cohort_from_error(&cohort_id, &cohort.facts, error.to_string())
                    {
                        Ok(unresolved) => stats.unresolved_cohorts.push(unresolved),
                        Err(projection_error) => stats.violations.push(format!(
                            "invalid_ctf_cohort_projection:cohort={cohort_id}:{projection_error}"
                        )),
                    }
                }
            }
        }
        for (cohort_id, orphaned) in submissions {
            for (_, submission) in orphaned {
                if let Some(authorization) = submission.buy_authorization {
                    match (
                        CohortEntryIdentity::from_buy_authorization(authorization),
                        symbol_summary.get_mut(&submission.underlying),
                    ) {
                        (Ok(entry), Some(summary)) => {
                            submission_projection::validate_and_count(
                                &entry,
                                &submission,
                                None,
                                summary,
                                &mut stats.violations,
                            );
                        }
                        _ => stats.violations.push(format!(
                            "buy_submission_without_valid_authorization:order_id={}",
                            submission.order_id
                        )),
                    }
                    continue;
                }
                stats.violations.push(format!(
                    "order_submission_without_confirmed_split:order_id={}:cohort={cohort_id}",
                    submission.order_id
                ));
            }
        }
        stats.closed_cohorts.sort_by(|left, right| {
            left.started_at_ms
                .cmp(&right.started_at_ms)
                .then_with(|| left.paired_cohort_key.cmp(&right.paired_cohort_key))
        });
        stats.unresolved_cohorts.sort_by(|left, right| {
            left.started_at_ms
                .cmp(&right.started_at_ms)
                .then_with(|| left.paired_cohort_key.cmp(&right.paired_cohort_key))
        });
        let ctf_summary = ctf_summary::finish(
            symbol_summary,
            confirmed_entry_markets,
            &normalized_symbols,
            &stats.closed_cohorts,
            &stats.unresolved_cohorts,
            &mut stats.violations,
        );
        stats.canonical.ctf_summary = ctf_summary;
        let oracle_label_by_condition =
            populate_causal_metrics(&mut stats, records, &owned_leg_decision_history);
        let (ctf_exit_summary, ctf_exit_cohorts) = exit_projection::project(
            &normalized_symbols,
            &stats.closed_cohorts,
            &stats.unresolved_cohorts,
            &owned_leg_decision_history,
            &stats.canonical.order_execution_lineage,
            &oracle_label_by_condition,
            &stats.canonical.ctf_summary,
            &mut stats.violations,
        );
        stats.canonical.ctf_exit_summary = ctf_exit_summary;
        stats.canonical.ctf_exit_cohorts = ctf_exit_cohorts;
        stats.project_venue_outcomes(Some(&resolution_by_condition), None);
        stats.canonical.owned_leg_transition_history = owned_leg_decision_history;
        stats.owned_leg_outcomes = owned_leg_outcomes;
        stats
    }
}

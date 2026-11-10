use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::strategy::facts::{CtfLegEconomicOutcome, CtfOwnedLegCloseReason};
use crate::trade_session_ledger::CtfTerminalOperation;

use super::{
    deserialize_i128_string, serialize_i128_string, CanonicalClosedCohortStats,
    CanonicalCohortCashflow, CanonicalCtfSummaryRow, CanonicalOwnedLegOutcome,
    CanonicalUnresolvedCohortState, CanonicalUnresolvedCohortStats, ExactOpenUnrecoveredCash,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExactRealizedPnl {
    Available {
        #[serde(
            serialize_with = "serialize_i128_string",
            deserialize_with = "deserialize_i128_string"
        )]
        units_6dp: i128,
    },
    UnavailableCanonicalReceiptFact,
}

impl ExactRealizedPnl {
    pub(crate) fn unavailable() -> Self {
        Self::UnavailableCanonicalReceiptFact
    }

    pub fn status(&self) -> &'static str {
        match self {
            Self::Available { .. } => "available",
            Self::UnavailableCanonicalReceiptFact => "unavailable_canonical_receipt_fact",
        }
    }

    pub fn units_6dp(&self) -> Option<i128> {
        match self {
            Self::Available { units_6dp } => Some(*units_6dp),
            Self::UnavailableCanonicalReceiptFact => None,
        }
    }
}

impl super::CanonicalSessionStats {
    pub(crate) fn closed_realized_pnl_by_symbol(&self) -> BTreeMap<String, ExactRealizedPnl> {
        let mut totals = self
            .canonical
            .ctf_summary
            .iter()
            .filter(|row| row.symbol != "TOTAL")
            .map(|row| (row.symbol.clone(), Some(0i128)))
            .collect::<BTreeMap<_, _>>();
        let mut unknown_symbol = false;

        for cohort in &self.closed_cohorts {
            let Some(symbol_total) = totals.get_mut(&cohort.symbol) else {
                unknown_symbol = true;
                continue;
            };
            *symbol_total =
                symbol_total.and_then(|total| total.checked_add(cohort.realized_pnl_units_6dp));
        }

        let total = if unknown_symbol {
            None
        } else {
            totals
                .values()
                .try_fold(0i128, |sum, value| sum.checked_add((*value)?))
        };
        let mut result = totals
            .into_iter()
            .map(|(symbol, total)| {
                (
                    symbol,
                    total
                        .map(|units_6dp| ExactRealizedPnl::Available { units_6dp })
                        .unwrap_or_else(ExactRealizedPnl::unavailable),
                )
            })
            .collect::<BTreeMap<_, _>>();
        result.insert(
            "TOTAL".to_string(),
            total
                .map(|units_6dp| ExactRealizedPnl::Available { units_6dp })
                .unwrap_or_else(ExactRealizedPnl::unavailable),
        );
        result
    }
}

pub(super) type SummaryRows = BTreeMap<String, CanonicalCtfSummaryRow>;
pub(super) type EntryMarkets = BTreeMap<String, BTreeSet<String>>;
pub(super) type SellIntents<'a> = BTreeMap<u64, &'a CanonicalOwnedLegOutcome>;

pub(super) fn initialize<'a>(
    initial_rows: &[CanonicalCtfSummaryRow],
    symbols: &[String],
    dispositions: &[CanonicalOwnedLegOutcome],
    decision_history: &'a [CanonicalOwnedLegOutcome],
    violations: &mut Vec<String>,
) -> (SummaryRows, EntryMarkets, SellIntents<'a>) {
    let mut rows = initial_rows
        .iter()
        .filter(|row| row.symbol != "TOTAL")
        .cloned()
        .map(|row| (row.symbol.clone(), row))
        .collect::<SummaryRows>();
    let markets = symbols
        .iter()
        .map(|symbol| (symbol.clone(), BTreeSet::new()))
        .collect::<EntryMarkets>();
    let mut sell_intents = BTreeMap::new();
    let mut symbol_sell_intents = symbols
        .iter()
        .map(|symbol| (symbol.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, BTreeSet<u64>>>();

    for outcome in dispositions {
        let Some(summary) = rows.get_mut(&outcome.symbol) else {
            violations.push(format!(
                "owned_leg_outcome_unknown_symbol:symbol={}",
                outcome.symbol
            ));
            continue;
        };
        let outcome_count = match outcome.outcome {
            CtfLegEconomicOutcome::SellNow => &mut summary.sell_now_outcome_count,
            CtfLegEconomicOutcome::HoldForSettlement => {
                &mut summary.hold_for_settlement_outcome_count
            }
        };
        checked_increment(
            outcome_count,
            violations,
            format!(
                "ctf_symbol_owned_leg_outcome_count_overflow:symbol={}",
                outcome.symbol
            ),
        );
    }
    for outcome in decision_history {
        let Some(summary) = rows.get_mut(&outcome.symbol) else {
            violations.push(format!(
                "owned_leg_decision_history_unknown_symbol:symbol={}",
                outcome.symbol
            ));
            continue;
        };
        if outcome.outcome != CtfLegEconomicOutcome::SellNow {
            continue;
        }
        let Some(intent_id) = outcome.execution_intent_id else {
            continue;
        };
        let Some(intent_ids) = symbol_sell_intents.get_mut(&outcome.symbol) else {
            continue;
        };
        if !intent_ids.insert(intent_id) {
            violations.push(format!(
                "ctf_sell_now_intent_identity_reused:symbol={}:intent_id={intent_id}",
                outcome.symbol
            ));
            continue;
        }
        sell_intents.insert(intent_id, outcome);
        checked_increment(
            &mut summary.sell_now_intent_count,
            violations,
            format!(
                "ctf_symbol_sell_now_intent_count_overflow:symbol={}",
                outcome.symbol
            ),
        );
        let intent_count = match outcome.close_reason {
            Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) => {
                &mut summary.paired_first_managed_sell_intent_count
            }
            Some(CtfOwnedLegCloseReason::PairedPostProceedsSell) => {
                &mut summary.paired_post_proceeds_sell_intent_count
            }
            None => continue,
        };
        checked_increment(
            intent_count,
            violations,
            format!(
                "ctf_symbol_sell_now_close_reason_intent_count_overflow:symbol={}",
                outcome.symbol
            ),
        );
    }
    (rows, markets, sell_intents)
}

pub(super) fn finish(
    mut rows: SummaryRows,
    markets: EntryMarkets,
    symbols: &[String],
    closed_cohorts: &[CanonicalClosedCohortStats],
    unresolved_cohorts: &[CanonicalUnresolvedCohortStats],
    violations: &mut Vec<String>,
) -> Vec<CanonicalCtfSummaryRow> {
    let mut invalid_receipt_symbols = BTreeSet::new();
    for summary in rows.values_mut() {
        summary.open_unrecovered_cash = ExactOpenUnrecoveredCash::available(0);
        summary.realized_pnl = ExactRealizedPnl::Available { units_6dp: 0 };
    }
    for cohort in closed_cohorts {
        let Some(summary) = rows.get_mut(&cohort.symbol) else {
            violations.push(format!(
                "ctf_closed_cohort_unknown_symbol:symbol={}",
                cohort.symbol
            ));
            continue;
        };
        checked_increment(
            &mut summary.closed_cohort_count,
            violations,
            format!(
                "ctf_symbol_closed_cohort_count_overflow:symbol={}",
                cohort.symbol
            ),
        );
        add_realized_pnl(
            summary,
            cohort.realized_pnl_units_6dp,
            &cohort.symbol,
            violations,
        );
        add_acquisition(summary, &cohort.cashflow, true, violations, &cohort.symbol);
        checked_add_quantity(
            &mut summary.closed_outcome_share_quantity_units_6dp,
            cohort.closed_outcome_share_quantity_units_6dp,
            violations,
            format!(
                "ctf_symbol_closed_outcome_share_quantity_overflow:symbol={}",
                cohort.symbol
            ),
        );
        add_cashflow_quantities(summary, &cohort.cashflow, violations, &cohort.symbol);
        count_terminal_receipts(summary, &cohort.cashflow, violations, &cohort.symbol);
    }
    for cohort in unresolved_cohorts {
        let Some(summary) = rows.get_mut(&cohort.symbol) else {
            violations.push(format!(
                "ctf_open_cohort_unknown_symbol:symbol={}",
                cohort.symbol
            ));
            continue;
        };
        checked_increment(
            &mut summary.open_cohort_count,
            violations,
            format!(
                "ctf_symbol_open_cohort_count_overflow:symbol={}",
                cohort.symbol
            ),
        );
        let cashflow = match &cohort.state {
            CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => {
                cashflow
            }
            CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => {
                summary.realized_pnl = ExactRealizedPnl::unavailable();
                invalid_receipt_symbols.insert(cohort.symbol.clone());
                continue;
            }
        };
        let realization = cashflow.total_realization();
        if realization.is_none() {
            summary.realized_pnl = ExactRealizedPnl::unavailable();
            violations.push(format!(
                "ctf_symbol_open_realized_pnl_overflow:symbol={}",
                cohort.symbol
            ));
        }
        if let Some(realization) = realization {
            add_realized_pnl(
                summary,
                realization.realized_pnl_units_6dp,
                &cohort.symbol,
                violations,
            );
        }
        add_acquisition(summary, cashflow, false, violations, &cohort.symbol);
        add_open_cashflow(summary, cashflow, violations, &cohort.symbol);
        add_cashflow_quantities(summary, cashflow, violations, &cohort.symbol);
        count_terminal_receipts(summary, cashflow, violations, &cohort.symbol);
    }
    for (symbol, markets) in markets {
        let Some(summary) = rows.get_mut(&symbol) else {
            continue;
        };
        summary.confirmed_entry_market_count = match u64::try_from(markets.len()) {
            Ok(count) => count,
            Err(_) => {
                violations.push(format!(
                    "ctf_symbol_confirmed_entry_market_count_overflow:symbol={symbol}"
                ));
                0
            }
        };
        let disposition_count = summary
            .sell_now_outcome_count
            .checked_add(summary.hold_for_settlement_outcome_count);
        let expected_dispositions = summary
            .confirmed_entry_market_count
            .checked_sub(summary.split_entries)
            .and_then(|buys| summary.split_entries.checked_mul(2)?.checked_add(buys));
        if disposition_count != expected_dispositions {
            violations.push(format!(
                "ctf_symbol_owned_leg_disposition_count_mismatch:symbol={symbol}:expected={}:actual={}",
                expected_dispositions.unwrap_or(0),
                disposition_count.unwrap_or(0)
            ));
        }
        summary.share_conservation_proven = !invalid_receipt_symbols.contains(&symbol)
            && summary
                .confirmed_disposed_share_quantity_units_6dp
                .checked_add(summary.open_share_quantity_units_6dp)
                .is_some_and(|accounted| accounted == summary.created_share_quantity_units_6dp);
        if !summary.share_conservation_proven {
            violations.push(format!(
                "ctf_symbol_share_conservation_mismatch:symbol={symbol}:created={}:disposed={}:open={}",
                summary.created_share_quantity_units_6dp,
                summary.confirmed_disposed_share_quantity_units_6dp,
                summary.open_share_quantity_units_6dp
            ));
        }
    }

    let mut summary_rows = Vec::with_capacity(symbols.len() + 1);
    let mut total = CanonicalCtfSummaryRow::unavailable("TOTAL");
    total.share_conservation_proven = true;
    total.open_unrecovered_cash = ExactOpenUnrecoveredCash::available(0);
    total.realized_pnl = ExactRealizedPnl::Available { units_6dp: 0 };
    let mut total_valid = true;
    let mut total_realized_pnl_units_6dp = Some(0_i128);
    for symbol in symbols {
        let Some(summary) = rows.remove(symbol) else {
            violations.push(format!(
                "ctf_summary_missing_expected_symbol:symbol={symbol}"
            ));
            total_valid = false;
            continue;
        };
        if total.checked_add_from(&summary).is_none() {
            total_valid = false;
        }
        total.share_conservation_proven &= summary.share_conservation_proven;
        total_realized_pnl_units_6dp = total_realized_pnl_units_6dp.and_then(|total_units| {
            summary
                .realized_pnl
                .units_6dp()
                .and_then(|units| total_units.checked_add(units))
        });
        summary_rows.push(summary);
    }
    if !total_valid {
        violations.push("ctf_total_summary_checked_add_overflow".to_string());
        total = CanonicalCtfSummaryRow::unavailable("TOTAL");
    } else {
        total.realized_pnl = match total_realized_pnl_units_6dp {
            Some(units_6dp) => ExactRealizedPnl::Available { units_6dp },
            None => ExactRealizedPnl::unavailable(),
        };
    }
    total.share_conservation_proven &= total
        .confirmed_disposed_share_quantity_units_6dp
        .checked_add(total.open_share_quantity_units_6dp)
        .is_some_and(|accounted| accounted == total.created_share_quantity_units_6dp);
    summary_rows.push(total);
    summary_rows
}

fn add_realized_pnl(
    summary: &mut CanonicalCtfSummaryRow,
    addition: i128,
    symbol: &str,
    violations: &mut Vec<String>,
) {
    let Some(current) = summary.realized_pnl.units_6dp() else {
        return;
    };
    summary.realized_pnl = match current.checked_add(addition) {
        Some(units_6dp) => ExactRealizedPnl::Available { units_6dp },
        None => {
            violations.push(format!("ctf_symbol_realized_pnl_overflow:symbol={symbol}"));
            ExactRealizedPnl::unavailable()
        }
    };
}

fn add_cashflow_quantities(
    summary: &mut CanonicalCtfSummaryRow,
    cashflow: &CanonicalCohortCashflow,
    violations: &mut Vec<String>,
    symbol: &str,
) {
    for (field, up, down, label) in [
        (
            &mut summary.confirmed_disposed_share_quantity_units_6dp,
            cashflow.up_confirmed_disposed_qty_units_6dp,
            cashflow.down_confirmed_disposed_qty_units_6dp,
            "confirmed_disposed",
        ),
        (
            &mut summary.open_share_quantity_units_6dp,
            cashflow.up_remaining_qty_units_6dp,
            cashflow.down_remaining_qty_units_6dp,
            "open",
        ),
    ] {
        let Some(quantity) = up.checked_add(down) else {
            violations.push(format!(
                "ctf_symbol_{label}_share_quantity_overflow:symbol={symbol}"
            ));
            continue;
        };
        checked_add_quantity(
            field,
            quantity,
            violations,
            format!("ctf_symbol_{label}_share_quantity_overflow:symbol={symbol}"),
        );
    }
}

fn add_open_cashflow(
    summary: &mut CanonicalCtfSummaryRow,
    cashflow: &CanonicalCohortCashflow,
    violations: &mut Vec<String>,
    symbol: &str,
) {
    checked_add_amount(
        &mut summary.open_confirmed_sell_proceeds_units_6dp,
        cashflow.confirmed_sell_proceeds_units_6dp,
        violations,
        format!("ctf_symbol_open_sell_proceeds_overflow:symbol={symbol}"),
    );
    checked_add_amount(
        &mut summary.open_confirmed_terminal_receipts_units_6dp,
        cashflow.confirmed_terminal_receipts_units_6dp,
        violations,
        format!("ctf_symbol_open_terminal_receipts_overflow:symbol={symbol}"),
    );
    checked_add_quantity(
        &mut summary.open_confirmed_fee_units_6dp,
        cashflow.confirmed_fee_units_6dp,
        violations,
        format!("ctf_symbol_open_fee_overflow:symbol={symbol}"),
    );
    checked_add_quantity(
        &mut summary.open_confirmed_approved_direct_cost_units_6dp,
        cashflow.confirmed_approved_direct_cost_units_6dp,
        violations,
        format!("ctf_symbol_open_direct_cost_overflow:symbol={symbol}"),
    );
    let Some(cohort_units) = cashflow.unrecovered_cash_units_6dp() else {
        violations.push(format!(
            "ctf_symbol_open_unrecovered_cash_overflow:symbol={symbol}"
        ));
        summary.open_unrecovered_cash = ExactOpenUnrecoveredCash::unavailable();
        return;
    };
    summary.open_unrecovered_cash = match summary.open_unrecovered_cash.units_6dp() {
        Some(units) => match units.checked_add(cohort_units) {
            Some(units) => ExactOpenUnrecoveredCash::available(units),
            None => {
                violations.push(format!(
                    "ctf_symbol_open_unrecovered_cash_overflow:symbol={symbol}"
                ));
                ExactOpenUnrecoveredCash::unavailable()
            }
        },
        None => ExactOpenUnrecoveredCash::unavailable(),
    };
}

pub(super) fn checked_add_amount(
    value: &mut i128,
    increment: i128,
    violations: &mut Vec<String>,
    violation: String,
) {
    match value.checked_add(increment) {
        Some(next) => *value = next,
        None => violations.push(violation),
    }
}

pub(super) fn checked_increment(value: &mut u64, violations: &mut Vec<String>, violation: String) {
    match value.checked_add(1) {
        Some(next) => *value = next,
        None => violations.push(violation),
    }
}

pub(super) fn checked_add_quantity(
    value: &mut u128,
    quantity: u128,
    violations: &mut Vec<String>,
    violation: String,
) {
    match value.checked_add(quantity) {
        Some(next) => *value = next,
        None => violations.push(violation),
    }
}

fn count_terminal_receipts(
    summary: &mut CanonicalCtfSummaryRow,
    cashflow: &CanonicalCohortCashflow,
    violations: &mut Vec<String>,
    symbol: &str,
) {
    for receipt in &cashflow.terminal_receipts {
        checked_increment(
            &mut summary.settlement_receipt_count,
            violations,
            format!("ctf_symbol_settlement_receipt_count_overflow:symbol={symbol}"),
        );
        let operation_count = match receipt.operation {
            CtfTerminalOperation::Merge => &mut summary.merge_confirmed_count,
            CtfTerminalOperation::Redeem => &mut summary.redeem_confirmed_count,
        };
        checked_increment(
            operation_count,
            violations,
            format!("ctf_symbol_terminal_operation_count_overflow:symbol={symbol}"),
        );
    }
}

fn add_acquisition(
    summary: &mut CanonicalCtfSummaryRow,
    cashflow: &CanonicalCohortCashflow,
    closed: bool,
    violations: &mut Vec<String>,
    symbol: &str,
) {
    let target = if closed {
        &mut summary.closed_cohort_principal_units_6dp
    } else {
        &mut summary.open_cohort_principal_units_6dp
    };
    match cashflow.entry_principal_units_6dp() {
        Some(principal) => checked_add_quantity(
            target,
            principal,
            violations,
            format!("ctf_symbol_cohort_principal_overflow:symbol={symbol}"),
        ),
        None => violations.push(format!(
            "ctf_symbol_cohort_principal_overflow:symbol={symbol}"
        )),
    }
}

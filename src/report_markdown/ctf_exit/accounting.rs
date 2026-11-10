use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::strategy::facts::CtfLegDecisionReason;
use crate::trade_session_ledger::{
    CanonicalCtfExitClass, CanonicalCtfExitCohortRow, CanonicalCtfExitSummaryRow,
    CanonicalCtfLegRealization, CanonicalSessionStats, CanonicalUnresolvedCohortState,
};

use super::{comparison, display_money_units_6dp, display_unsigned_money_units_6dp};

pub(super) fn comparison_fields_present(row: &CanonicalCtfExitCohortRow) -> bool {
    row.first_authorized_at_ms.is_some()
        || row.venue_winner_side.is_some()
        || row.venue_resolution_observed_at_ms.is_some()
        || row.venue_resolution_provenance.is_some()
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
struct CellAgg {
    activity: u64,
    fills: u64,
    #[serde(
        rename = "realized_pnl_units_6dp",
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_i128_string"
    )]
    realized_pnl: i128,
    #[serde(skip)]
    recovered_pusd: u128,
    error: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct SymbolExitAgg {
    entries: u64,
    predictions: u64,
    outcomes: u64,
    comparable: u64,
    correct: u64,
    dead_cat_cohorts: u64,
    dead_cat_winning: u64,
    dead_cat_losing: u64,
    dead_cat_unknown: u64,
    dead_cat_saved: i128,
    dead_cat_forgone: i128,
    dead_cat_comparison_covered: u64,
    cells: [CellAgg; 6],
    buy_entries: u64,
    buy_predictions: u64,
    buy_comparable: u64,
    buy_correct: u64,
    realized_pnl: Option<i128>,
    error: bool,
}

pub(super) fn rows(
    summary: &[CanonicalCtfExitSummaryRow],
    cohorts: &[CanonicalCtfExitCohortRow],
    stats: Option<&CanonicalSessionStats>,
    comparisons_available: bool,
    buys_available: bool,
) -> Vec<Vec<String>> {
    aggregate(summary, cohorts, stats)
        .into_iter()
        .map(|(symbol, agg)| {
            let split_entries = agg.entries - agg.buy_entries;
            let mut cells = vec![
                symbol,
                agg.entries.to_string(),
                agg.cells[0].display(true),
                format!(
                    "{}; saved={} / forgone={} / covered={} / unknown={}",
                    agg.cells[1].display(true),
                    agg.display_dead_cat_comparison(agg.dead_cat_saved),
                    agg.display_dead_cat_comparison(agg.dead_cat_forgone),
                    agg.dead_cat_comparison_covered,
                    agg.dead_cat_cohorts - agg.dead_cat_comparison_covered,
                ),
                agg.cells[2].display(true),
                agg.cells[3].display(false),
                agg.cells[4].display_recovered(),
                agg.display_total(),
                agg.display_average(),
            ];
            if comparisons_available {
                cells.extend([
                    format!(
                        "{} / {} / {}",
                        agg.predictions,
                        split_entries,
                        split_entries - agg.predictions
                    ),
                    format!(
                        "{} / {} / {}",
                        agg.outcomes,
                        agg.entries,
                        agg.entries - agg.outcomes
                    ),
                    format!(
                        "{} / {} / {}",
                        agg.correct,
                        agg.comparable,
                        split_entries - agg.comparable
                    ),
                    format!(
                        "{} / {} / {} / {}",
                        agg.dead_cat_winning,
                        agg.dead_cat_losing,
                        agg.dead_cat_unknown,
                        agg.dead_cat_cohorts,
                    ),
                ]);
            }
            if buys_available {
                cells.extend([
                    split_entries.to_string(),
                    agg.cells[5].display(false),
                    format!(
                        "{} / {} / {}",
                        agg.buy_predictions,
                        agg.buy_entries,
                        agg.buy_entries - agg.buy_predictions
                    ),
                    format!(
                        "{} / {} / {}",
                        agg.buy_correct,
                        agg.buy_comparable,
                        agg.buy_entries - agg.buy_comparable
                    ),
                ]);
            }
            cells
        })
        .collect()
}

pub(crate) fn aggregate(
    summary: &[CanonicalCtfExitSummaryRow],
    cohorts: &[CanonicalCtfExitCohortRow],
    stats: Option<&CanonicalSessionStats>,
) -> Vec<(String, SymbolExitAgg)> {
    let cashflows = stats.map(|stats| {
        stats
            .closed_cohorts
            .iter()
            .map(|cohort| (cohort.paired_cohort_key.as_str(), &cohort.cashflow))
            .chain(
                stats
                    .unresolved_cohorts
                    .iter()
                    .filter_map(|cohort| match &cohort.state {
                        CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                            cashflow,
                        } => Some((cohort.paired_cohort_key.as_str(), cashflow)),
                        _ => None,
                    }),
            )
            .collect::<std::collections::BTreeMap<_, _>>()
    });
    let dead_cat = format!("{:?}", CtfLegDecisionReason::DeadCat);
    let mut map = std::collections::BTreeMap::<String, SymbolExitAgg>::new();
    let mut total_row_pnl: Option<i128> = None;
    for row in summary {
        if row.symbol == "TOTAL" {
            total_row_pnl = Some(row.total_realized_pnl_units_6dp);
            continue;
        }
        map.entry(row.symbol.clone()).or_default().realized_pnl =
            Some(row.total_realized_pnl_units_6dp);
    }
    let mut total = SymbolExitAgg::default();
    for cohort in cohorts {
        debug_assert_ne!(cohort.symbol, "TOTAL");
        let projected = project_cells(cohort, &dead_cat);
        let comparison = comparison(cohort);
        let dead_cat_value = comparison.dead_cat_winner.and_then(|winner| {
            let cashflow = cashflows.as_ref()?.get(cohort.paired_cohort_key.as_str())?;
            super::dead_cat_receipt_value(cohort, cashflow, &dead_cat, winner, comparison.outcome?)
                .map(|value| (winner, value))
        });
        for agg in [map.entry(cohort.symbol.clone()).or_default(), &mut total] {
            let Some(entries) = agg.entries.checked_add(1) else {
                agg.error = true;
                continue;
            };
            agg.entries = entries;
            if cohort.buy_authorization.is_some() {
                agg.buy_entries += 1;
                let prediction = cohort.buy_authorization.map(|authorization| {
                    if authorization.selected_up {
                        "UP"
                    } else {
                        "DOWN"
                    }
                });
                agg.buy_predictions += u64::from(prediction.is_some());
                let correct = prediction
                    .zip(comparison.outcome)
                    .map(|(prediction, outcome)| prediction == outcome);
                agg.buy_comparable += u64::from(correct.is_some());
                agg.buy_correct += u64::from(correct == Some(true));
            }
            agg.predictions += u64::from(comparison.prediction.is_some());
            agg.outcomes += u64::from(comparison.outcome.is_some());
            agg.comparable += u64::from(comparison.first_correct.is_some());
            agg.correct += u64::from(comparison.first_correct == Some(true));
            if comparison.dead_cat_sold {
                agg.dead_cat_cohorts += 1;
                if let Some((winner, value)) = dead_cat_value {
                    let target = if winner {
                        &mut agg.dead_cat_forgone
                    } else {
                        &mut agg.dead_cat_saved
                    };
                    if let Some(sum) = target.checked_add(value) {
                        *target = sum;
                        agg.dead_cat_comparison_covered += 1;
                    }
                }
                match comparison.dead_cat_winner {
                    Some(true) => agg.dead_cat_winning += 1,
                    Some(false) => agg.dead_cat_losing += 1,
                    None => agg.dead_cat_unknown += 1,
                }
            }
            if let Ok(cells) = &projected {
                for (target, source) in agg.cells.iter_mut().zip(cells) {
                    target.add(source);
                }
            } else {
                agg.error = true;
                if cohort.first_instructions.is_some() || cohort.post_instructions.is_some() {
                    for cell in &mut agg.cells {
                        cell.error = true;
                    }
                } else {
                    agg.cells[0].error = true;
                    agg.cells[terminal_target(cohort, &dead_cat)].error = true;
                }
            }
        }
    }

    let mut out = Vec::with_capacity(summary.len());
    for row in summary {
        if row.symbol == "TOTAL" {
            continue;
        }
        let mut agg = map.remove(&row.symbol).unwrap_or_default();
        agg.realized_pnl = Some(row.total_realized_pnl_units_6dp);
        agg.verify_total();
        out.push((row.symbol.clone(), agg));
    }
    for (symbol, agg) in map {
        debug_assert_ne!(symbol.as_str(), "TOTAL");
        out.push((symbol, agg));
    }
    total.realized_pnl = total_row_pnl;
    total.verify_total();
    out.push(("TOTAL".to_string(), total));
    out
}

fn terminal_target(row: &CanonicalCtfExitCohortRow, dead_cat: &str) -> usize {
    match row.exit_class {
        CanonicalCtfExitClass::FirstToPostProceeds if row.first_instructions.is_none() => {
            if row.sibling_reason.as_deref() == Some(dead_cat) {
                1
            } else {
                2
            }
        }
        CanonicalCtfExitClass::Merge => 4,
        CanonicalCtfExitClass::BuyAndHold | CanonicalCtfExitClass::BuyManagedExit => 5,
        _ => 3,
    }
}

fn project_cells(row: &CanonicalCtfExitCohortRow, dead_cat: &str) -> Result<[CellAgg; 6], ()> {
    let (principal, up_created, down_created) = match (row.exit_class, row.buy_authorization) {
        (
            CanonicalCtfExitClass::BuyAndHold | CanonicalCtfExitClass::BuyManagedExit,
            Some(authorization),
        ) => {
            let principal = row.buy_principal_units_6dp.ok_or(())?;
            let quantity = row.buy_created_quantity_units_6dp.ok_or(())?;
            let refunds = row.buy_collateral_refund_units_6dp.ok_or(())?;
            let realization = row.buy_realization.ok_or(())?;
            let complete = row.up_remaining_quantity_units_6dp == 0
                && row.down_remaining_quantity_units_6dp == 0;
            if quantity == 0
                || quantity > authorization.target_qty_units_6dp
                || row.oracle_winner_side.is_some()
                || row.first_sell_token.is_some()
                || row.first_submitted_order_count != 0
                || row.first_confirmed_fill_count != 0
                || row.first_instructions.is_some()
                || row.first_confirmed_fill_quantity_units_6dp != 0
                || row.first_confirmed_proceeds_units_6dp != 0
                || row.first_realization.is_some()
                || row.first_terminal_realization.is_some()
                || row.remaining_realization.is_some()
                || (row.exit_class == CanonicalCtfExitClass::BuyManagedExit)
                    != (row.post_confirmed_fill_count > 0)
                || (row.post_instructions.is_none()
                    && (row.post_submitted_order_count != 0 || row.post_confirmed_fill_count != 0))
                || row.split_principal_units_6dp != 0
                || row.unassigned_up_realization.is_some()
                || row.unassigned_down_realization.is_some()
                || realization.allocated_collateral_refund_units_6dp > refunds
                || realization.attributable_fee_units_6dp > row.confirmed_fee_units_6dp
                || realization.attributable_approved_direct_cost_units_6dp
                    > row.confirmed_approved_direct_cost_units_6dp
                || (complete
                    && (realization.allocated_collateral_refund_units_6dp != refunds
                        || realization.attributable_fee_units_6dp != row.confirmed_fee_units_6dp
                        || realization.attributable_approved_direct_cost_units_6dp
                            != row.confirmed_approved_direct_cost_units_6dp))
            {
                return Err(());
            }
            if authorization.selected_up {
                (principal, quantity, 0)
            } else {
                (principal, 0, quantity)
            }
        }
        (CanonicalCtfExitClass::BuyAndHold | CanonicalCtfExitClass::BuyManagedExit, None) => {
            return Err(());
        }
        (_, Some(_)) => return Err(()),
        (_, None) => (
            row.split_principal_units_6dp,
            row.split_principal_units_6dp,
            row.split_principal_units_6dp,
        ),
    };
    let instruction_evidence = row.first_instructions.is_some()
        || (row.buy_authorization.is_some() && row.post_instructions.is_some());
    if row
        .first_instructions
        .iter()
        .flatten()
        .chain(row.post_instructions.iter().flatten())
        .any(|instruction| !instruction.integrity_valid)
    {
        return Err(());
    }
    let first_q = row.first_confirmed_fill_quantity_units_6dp;
    let post_q = row.post_confirmed_fill_quantity_units_6dp;
    let (first_up, first_down, post_up, post_down) =
        if let Some(authorization) = row.buy_authorization {
            if authorization.selected_up {
                (0, 0, post_q, 0)
            } else {
                (0, 0, 0, post_q)
            }
        } else {
            match row.oracle_winner_side.as_deref() {
                Some("UP") => (0, first_q, post_q, 0),
                Some("DOWN") => (first_q, 0, 0, post_q),
                None if first_q == 0 && post_q == 0 => (0, 0, 0, 0),
                _ => return Err(()),
            }
        };
    let (first_value, first_basis, first_pnl) = realization_totals([
        if instruction_evidence || first_q > 0 {
            row.first_realization
        } else {
            None
        },
        None,
        None,
        None,
    ])?;
    let mut cells: [CellAgg; 6] = std::array::from_fn(|_| CellAgg::default());
    cells[0] = CellAgg {
        activity: row.first_submitted_order_count,
        fills: row.first_confirmed_fill_count,
        realized_pnl: first_pnl,
        ..CellAgg::default()
    };
    let profit_now = format!("{:?}", CtfLegDecisionReason::ProfitNow);
    let mut post_realization = CanonicalCtfLegRealization::ZERO;
    for instruction in row.post_instructions.iter().flatten() {
        let target = if instruction.decision_reason == dead_cat {
            1
        } else if instruction.decision_reason == profit_now {
            2
        } else {
            return Err(());
        };
        let realization = instruction.confirmed_realization;
        post_realization = post_realization.checked_add(realization).ok_or(())?;
        cells[target].activity = cells[target]
            .activity
            .checked_add(if instruction.accepted { 1 } else { 0 })
            .ok_or(())?;
        cells[target].fills = cells[target]
            .fills
            .checked_add(u64::try_from(instruction.confirmed_fills.len()).map_err(|_| ())?)
            .ok_or(())?;
        cells[target].realized_pnl = cells[target]
            .realized_pnl
            .checked_add(realization.realized_pnl_units_6dp)
            .ok_or(())?;
    }
    let post_value = post_realization.confirmed_proceeds_units_6dp;
    let post_basis = post_realization.allocated_principal_units_6dp;
    let post_pnl = post_realization.realized_pnl_units_6dp;
    if row.buy_authorization.is_some() {
        let residual = if instruction_evidence {
            row.remaining_terminal_realization.flatten().ok_or(())?
        } else {
            row.buy_realization.ok_or(())?
        };
        if post_realization.checked_add(residual) != row.buy_realization {
            return Err(());
        }
    }
    let terminal_target = terminal_target(row, dead_cat);
    let (terminal_value, terminal_basis, terminal_pnl) = realization_totals([
        if instruction_evidence {
            row.first_terminal_realization.flatten()
        } else if first_q == 0 {
            row.first_realization
        } else {
            None
        },
        if instruction_evidence {
            row.remaining_terminal_realization.flatten()
        } else {
            row.remaining_realization
        },
        if row.buy_authorization.is_some() && !instruction_evidence {
            row.buy_realization
        } else {
            row.unassigned_up_realization
        },
        row.unassigned_down_realization,
    ])?;
    cells[terminal_target].activity = if row.buy_authorization.is_some() {
        1
    } else if !instruction_evidence {
        if row.exit_class == CanonicalCtfExitClass::FirstToPostProceeds {
            row.post_submitted_order_count
        } else {
            1
        }
    } else if row.terminal_receipt_count > 0
        || matches!(
            row.exit_class,
            CanonicalCtfExitClass::FirstToSettlement
                | CanonicalCtfExitClass::SettlementOnlyNoCanonicalOracle
                | CanonicalCtfExitClass::SettlementOnlyFailedMandatoryExecution
                | CanonicalCtfExitClass::Merge
        )
    {
        1
    } else {
        0
    };
    if !instruction_evidence {
        cells[terminal_target].fills = row.post_confirmed_fill_count;
    }
    cells[terminal_target].realized_pnl = terminal_pnl;
    let expected_value = row
        .first_confirmed_proceeds_units_6dp
        .checked_add(row.post_confirmed_proceeds_units_6dp)
        .ok_or(())?
        .checked_add(row.terminal_receipt_value_units_6dp)
        .ok_or(())?;
    if first_value
        .checked_add(post_value)
        .and_then(|value| value.checked_add(terminal_value))
        .ok_or(())?
        != expected_value
    {
        return Err(());
    }
    let allocated = first_basis
        .checked_add(post_basis)
        .and_then(|value| value.checked_add(terminal_basis))
        .ok_or(())?;
    let open_basis = principal.checked_sub(allocated).ok_or(())?;
    if first_pnl
        .checked_add(post_pnl)
        .and_then(|value| value.checked_add(terminal_pnl))
        .ok_or(())?
        != row.realized_pnl_units_6dp
        || first_up
            .checked_add(post_up)
            .and_then(|value| value.checked_add(row.terminal_up_quantity_units_6dp))
            .and_then(|value| value.checked_add(row.up_remaining_quantity_units_6dp))
            != Some(up_created)
        || first_down
            .checked_add(post_down)
            .and_then(|value| value.checked_add(row.terminal_down_quantity_units_6dp))
            .and_then(|value| value.checked_add(row.down_remaining_quantity_units_6dp))
            != Some(down_created)
        || (row.up_remaining_quantity_units_6dp == 0
            && row.down_remaining_quantity_units_6dp == 0
            && open_basis != 0)
    {
        return Err(());
    }
    match (row.terminal_receipt_count, row.terminal_operation.as_str()) {
        (0, "NONE")
            if row.terminal_up_quantity_units_6dp == 0
                && row.terminal_down_quantity_units_6dp == 0
                && row.terminal_receipt_value_units_6dp == 0 =>
        {
            Ok(())
        }
        (count, "MERGE") if count > 0 && row.exit_class == CanonicalCtfExitClass::Merge => Ok(()),
        (count, "REDEEM") if count > 0 => Ok(()),
        _ => Err(()),
    }?;
    cells[4].recovered_pusd = if row.exit_class == CanonicalCtfExitClass::Merge {
        u128::try_from(row.terminal_receipt_value_units_6dp).map_err(|_| ())?
    } else {
        0
    };
    if row.post_instruction_evidence_complete == Some(true)
        && (cells[1].activity.checked_add(cells[2].activity).ok_or(())?
            != row.post_submitted_order_count
            || cells[1].fills.checked_add(cells[2].fills).ok_or(())?
                != row.post_confirmed_fill_count)
    {
        return Err(());
    }
    Ok(cells)
}

fn realization_totals(
    values: [Option<CanonicalCtfLegRealization>; 4],
) -> Result<(i128, u128, i128), ()> {
    values.into_iter().flatten().try_fold(
        (0_i128, 0_u128, 0_i128),
        |(proceeds, basis, pnl), value| {
            Ok((
                proceeds
                    .checked_add(value.confirmed_proceeds_units_6dp)
                    .ok_or(())?,
                basis
                    .checked_add(value.allocated_principal_units_6dp)
                    .ok_or(())?,
                pnl.checked_add(value.realized_pnl_units_6dp).ok_or(())?,
            ))
        },
    )
}

impl CellAgg {
    fn add(&mut self, other: &Self) {
        macro_rules! add {
            ($($field:ident),+) => {
                $(if let Some(value) = self.$field.checked_add(other.$field) {
                    self.$field = value;
                } else {
                    self.error = true;
                })+
            };
        }
        add!(activity, fills, realized_pnl, recovered_pusd);
        self.error |= other.error;
    }

    fn display(&self, execution: bool) -> String {
        if self.error {
            return "ACCOUNTING_ERROR".to_string();
        }
        let activity = if execution {
            format!("{}/{}", self.activity, self.fills)
        } else {
            self.activity.to_string()
        };
        format!(
            "{activity} / {}",
            display_money_units_6dp(self.realized_pnl)
        )
    }

    fn display_recovered(&self) -> String {
        if self.error {
            return "ACCOUNTING_ERROR".to_string();
        }
        format!(
            "{} / {}",
            self.activity,
            display_unsigned_money_units_6dp(self.recovered_pusd)
        )
    }
}

impl SymbolExitAgg {
    fn display_dead_cat_comparison(&self, value: i128) -> String {
        if self.dead_cat_comparison_covered == 0 {
            "UNAVAILABLE".to_string()
        } else {
            display_money_units_6dp(value)
        }
    }

    fn verify_total(&mut self) {
        let computed = self
            .cells
            .iter()
            .try_fold(0_i128, |sum, cell| sum.checked_add(cell.realized_pnl));
        self.error |= computed != self.realized_pnl;
    }

    fn display_total(&self) -> String {
        match (self.error, self.realized_pnl) {
            (false, Some(value)) => display_money_units_6dp(value),
            _ => "ACCOUNTING_ERROR".to_string(),
        }
    }

    fn display_average(&self) -> String {
        match (self.error, self.realized_pnl, self.entries) {
            (false, Some(_), 0) => display_money_units_6dp(0),
            (false, Some(value), entries) => display_money_units_6dp(value / i128::from(entries)),
            _ => "ACCOUNTING_ERROR".to_string(),
        }
    }
}

impl Serialize for SymbolExitAgg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut hold_to_settlement = self.cells[3].clone();
        hold_to_settlement.add(&self.cells[5]);
        let mut state = serializer.serialize_struct("SymbolExitAgg", 9)?;
        state.serialize_field("first", &self.cells[0])?;
        state.serialize_field("dead_cat", &self.cells[1])?;
        state.serialize_field("profit_now", &self.cells[2])?;
        state.serialize_field("hold_to_settlement", &hold_to_settlement)?;
        let covered = self.dead_cat_comparison_covered;
        state.serialize_field(
            "dead_cat_saved_units_6dp",
            &(covered > 0).then(|| self.dead_cat_saved.to_string()),
        )?;
        state.serialize_field(
            "dead_cat_forgone_units_6dp",
            &(covered > 0).then(|| self.dead_cat_forgone.to_string()),
        )?;
        state.serialize_field("dead_cat_comparison_covered", &covered)?;
        state.serialize_field(
            "dead_cat_comparison_unknown",
            &(self.dead_cat_cohorts - covered),
        )?;
        state.serialize_field("error", &self.error)?;
        state.end()
    }
}

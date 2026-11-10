use std::collections::{BTreeMap, BTreeSet};

use crate::oracle_resolution::{canonical_chainlink_symbol, OracleResolvedLabel};
use crate::strategy::facts::{CtfLegEconomicOutcome, CtfOwnedLegCloseReason};

use super::*;

#[path = "exit_projection/execution.rs"]
mod execution;
#[path = "exit_projection/schema.rs"]
mod schema;
#[path = "exit_projection/unassigned.rs"]
mod unassigned;

use execution::{
    classify, count_orders, first_deadline_status, intent_ids, project_instructions,
    receipt_totals, sell_realization, terminal_operation, terminal_realization, terminal_totals,
};
pub use schema::{
    CanonicalCtfExitBucket, CanonicalCtfExitClass, CanonicalCtfExitCohortRow,
    CanonicalCtfExitConfirmedFill, CanonicalCtfExitInstructionRow, CanonicalCtfExitSummaryRow,
};
use unassigned::{project_buy, project_unassigned};

const FIRST_CLOSE_REASON: &str = "paired_first_managed_sell";
const POST_CLOSE_REASON: &str = "paired_post_proceeds_sell";

pub(super) fn project(
    expected_symbols: &[String],
    closed_cohorts: &[CanonicalClosedCohortStats],
    unresolved_cohorts: &[CanonicalUnresolvedCohortStats],
    owned_leg_history: &[CanonicalOwnedLegOutcome],
    order_execution_lineage: &[CanonicalOrderExecutionLineageRow],
    oracle_label_by_condition: &BTreeMap<String, OracleResolvedLabel>,
    ctf_summary: &[CanonicalCtfSummaryRow],
    violations: &mut Vec<String>,
) -> (
    Vec<CanonicalCtfExitSummaryRow>,
    Vec<CanonicalCtfExitCohortRow>,
) {
    let mut history_by_cohort = BTreeMap::<&str, Vec<&CanonicalOwnedLegOutcome>>::new();
    for outcome in owned_leg_history {
        history_by_cohort
            .entry(&outcome.cohort_id)
            .or_default()
            .push(outcome);
    }
    let mut lineage_by_cohort = BTreeMap::<&str, Vec<&CanonicalOrderExecutionLineageRow>>::new();
    for row in order_execution_lineage {
        lineage_by_cohort
            .entry(&row.reducer_owned_intent.cohort_id)
            .or_default()
            .push(row);
    }

    let mut cohort_rows = Vec::with_capacity(closed_cohorts.len() + unresolved_cohorts.len());
    let mut pending_by_symbol = expected_symbols
        .iter()
        .map(|symbol| (symbol.clone(), 0u64))
        .collect::<BTreeMap<_, _>>();
    let mut projection_errors = Vec::new();
    let mut project_one = |symbol: &str,
                           cohort_key: &str,
                           market_name: Option<&String>,
                           cashflow: &CanonicalCohortCashflow,
                           realized_pnl: i128,
                           complete: bool| {
        let history = history_by_cohort
            .get(cohort_key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let cohort_lineage = lineage_by_cohort
            .get(cohort_key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        match project_cohort(
            symbol,
            cohort_key,
            market_name,
            cashflow,
            realized_pnl,
            complete,
            history,
            cohort_lineage,
            oracle_label_by_condition.get(&cashflow.condition_id),
        ) {
            Ok(Some(row)) => cohort_rows.push(row),
            Ok(None) => match pending_by_symbol.get_mut(symbol) {
                Some(count) => match count.checked_add(1) {
                    Some(next) => *count = next,
                    None => projection_errors.push(format!(
                        "canonical_ctf_exit_pending_count_overflow:symbol={symbol}"
                    )),
                },
                None => projection_errors.push(format!(
                    "canonical_ctf_exit_projection_unknown_symbol:cohort={cohort_key}:symbol={symbol}"
                )),
            },
            Err(error) => projection_errors.push(format!(
                "invalid_ctf_exit_projection:cohort={cohort_key}:{error}"
            )),
        }
    };
    for cohort in closed_cohorts {
        project_one(
            &cohort.symbol,
            &cohort.paired_cohort_key,
            cohort.market_name.as_ref(),
            &cohort.cashflow,
            cohort.realized_pnl_units_6dp,
            true,
        );
    }
    for cohort in unresolved_cohorts {
        let CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } =
            &cohort.state
        else {
            continue;
        };
        let Some(realization) = cashflow.total_realization() else {
            continue;
        };
        project_one(
            &cohort.symbol,
            &cohort.paired_cohort_key,
            cohort.market_name.as_ref(),
            cashflow,
            realization.realized_pnl_units_6dp,
            false,
        );
    }
    drop(project_one);
    violations.append(&mut projection_errors);

    let mut by_symbol = expected_symbols
        .iter()
        .map(|symbol| {
            (
                symbol.clone(),
                CanonicalCtfExitSummaryRow {
                    symbol: symbol.clone(),
                    ..CanonicalCtfExitSummaryRow::default()
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for cohort in &cohort_rows {
        let Some(summary) = by_symbol.get_mut(&cohort.symbol) else {
            violations.push(format!(
                "canonical_ctf_exit_projection_unknown_symbol:cohort={}:symbol={}",
                cohort.paired_cohort_key, cohort.symbol
            ));
            continue;
        };
        add_cohort(summary, cohort, violations);
    }

    let mut rows = expected_symbols
        .iter()
        .filter_map(|symbol| by_symbol.remove(symbol))
        .collect::<Vec<_>>();
    rows.retain(|row| {
        validate_against_ctf_summary(
            row,
            pending_by_symbol.get(&row.symbol).copied().unwrap_or(0),
            ctf_summary,
            violations,
        )
    });
    let mut total = CanonicalCtfExitSummaryRow {
        symbol: "TOTAL".to_string(),
        ..CanonicalCtfExitSummaryRow::default()
    };
    for row in &rows {
        add_summary(&mut total, row, violations);
    }
    let total_pending = pending_by_symbol
        .values()
        .try_fold(0u64, |total, count| total.checked_add(*count));
    if total_pending.is_none() {
        violations.push("canonical_ctf_exit_pending_count_overflow:symbol=TOTAL".to_string());
    }
    if rows.len() == expected_symbols.len()
        && total_pending.is_some_and(|pending| {
            validate_against_ctf_summary(&total, pending, ctf_summary, violations)
        })
    {
        rows.push(total);
    }
    (rows, cohort_rows)
}

fn project_cohort(
    symbol: &str,
    cohort_key: &str,
    market_name: Option<&String>,
    cashflow: &CanonicalCohortCashflow,
    realized_pnl: i128,
    complete: bool,
    history: &[&CanonicalOwnedLegOutcome],
    order_execution_lineage: &[&CanonicalOrderExecutionLineageRow],
    oracle_label: Option<&OracleResolvedLabel>,
) -> Result<Option<CanonicalCtfExitCohortRow>, String> {
    if cashflow.buy_authorization.is_some() {
        return project_buy(
            symbol,
            cohort_key,
            market_name,
            cashflow,
            realized_pnl,
            complete,
            history,
            order_execution_lineage,
        );
    }
    let first_history = history
        .iter()
        .copied()
        .filter(|outcome| {
            outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
        })
        .collect::<Vec<_>>();
    let first_auth = first_history
        .iter()
        .copied()
        .filter(|outcome| outcome.action_qty_units_6dp > 0)
        .min_by_key(|outcome| (outcome.ts_ms, outcome.ledger_seq));
    if first_auth.is_none() {
        return project_unassigned(
            symbol,
            cohort_key,
            market_name,
            cashflow,
            realized_pnl,
            complete,
            history,
        );
    }
    let first_auth = first_auth.ok_or_else(|| "missing_first_authorization".to_string())?;
    let evidence_revision = first_auth
        .execution_evidence_revision
        .filter(|revision| *revision > 0)
        .ok_or_else(|| "missing_first_evidence_revision".to_string())?;
    let winner = first_auth
        .causal_winner_side
        .ok_or_else(|| "missing_first_oracle_winner".to_string())?;
    let expected_first_token = match winner {
        crate::strategy::hold_to_resolution::PairTokenSide::Up => &cashflow.down_token,
        crate::strategy::hold_to_resolution::PairTokenSide::Down => &cashflow.up_token,
    };
    if first_auth.token != *expected_first_token {
        return Err("first_token_not_opposite_winner".to_string());
    }
    let sibling_token = if first_auth.token == cashflow.up_token {
        &cashflow.down_token
    } else if first_auth.token == cashflow.down_token {
        &cashflow.up_token
    } else {
        return Err("first_token_not_in_split".to_string());
    };
    let first_intents = intent_ids(&first_history);
    let post_history = history
        .iter()
        .copied()
        .filter(|outcome| {
            outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
        })
        .collect::<Vec<_>>();
    let post_intents = intent_ids(&post_history);
    let first_order_count = count_orders(cashflow, &first_intents)?;
    let post_order_count = count_orders(cashflow, &post_intents)?;
    let (first_instructions, first_instruction_evidence_complete) = project_instructions(
        &first_history,
        order_execution_lineage,
        cashflow,
        FIRST_CLOSE_REASON,
    )?;
    let (post_instructions, post_instruction_evidence_complete) = project_instructions(
        &post_history,
        order_execution_lineage,
        cashflow,
        POST_CLOSE_REASON,
    )?;

    let first_receipts = cashflow
        .managed_sell_receipts
        .iter()
        .filter(|receipt| receipt.close_reason == FIRST_CLOSE_REASON)
        .collect::<Vec<_>>();
    let post_receipts = cashflow
        .managed_sell_receipts
        .iter()
        .filter(|receipt| receipt.close_reason == POST_CLOSE_REASON)
        .collect::<Vec<_>>();
    if first_receipts
        .iter()
        .any(|receipt| receipt.token != first_auth.token)
    {
        return Err("first_receipt_token_mismatch".to_string());
    }
    if post_receipts
        .iter()
        .any(|receipt| receipt.token != *sibling_token)
    {
        return Err("post_receipt_token_mismatch".to_string());
    }
    if first_receipts.len() + post_receipts.len() != cashflow.managed_sell_receipts.len() {
        return Err("unknown_sell_receipt_close_reason".to_string());
    }
    let (
        first_fill_count,
        first_fill_qty,
        first_proceeds,
        first_confirmed_at_ms,
        first_latest_confirmed_at_ms,
    ) = receipt_totals(&first_receipts)?;
    let (post_fill_count, post_fill_qty, post_proceeds, _, _) = receipt_totals(&post_receipts)?;
    let total_sell_proceeds = first_proceeds
        .checked_add(post_proceeds)
        .ok_or_else(|| "sell_proceeds_overflow".to_string())?;
    if total_sell_proceeds != cashflow.confirmed_sell_proceeds_units_6dp {
        return Err("managed_sell_proceeds_mismatch".to_string());
    }
    let total_sell_qty = first_fill_qty
        .checked_add(post_fill_qty)
        .ok_or_else(|| "sell_quantity_overflow".to_string())?;
    let canonical_sell_qty = cashflow
        .up_confirmed_sold_qty_units_6dp
        .checked_add(cashflow.down_confirmed_sold_qty_units_6dp)
        .ok_or_else(|| "canonical_sell_quantity_overflow".to_string())?;
    if total_sell_qty != canonical_sell_qty {
        return Err("managed_sell_quantity_mismatch".to_string());
    }

    let terminal_operation = terminal_operation(cashflow)?;
    let exit_class = classify(first_fill_count, post_fill_count, terminal_operation)?;
    let (terminal_receipt_count, terminal_up_qty, terminal_down_qty, terminal_value) =
        terminal_totals(cashflow)?;
    if terminal_value != cashflow.confirmed_terminal_receipts_units_6dp {
        return Err("terminal_receipt_mismatch".to_string());
    }
    let first_realization = sell_realization(&first_receipts)?;
    let first_terminal_realization = terminal_realization(cashflow, &first_auth.token)?;
    let remaining_realization = cashflow
        .realization_for_token(sibling_token)
        .ok_or_else(|| "remaining_leg_realization_failed".to_string())?;
    let remaining_terminal_realization = terminal_realization(cashflow, sibling_token)?;
    let market_window = oracle_label.and_then(|label| {
        let expected_symbol = canonical_chainlink_symbol(symbol).ok()?;
        (label.condition_id == cashflow.condition_id
            && label.market_id.as_str() == cashflow.condition_id
            && label.symbol == expected_symbol)
            .then_some((label.window_start_ms, label.window_end_ms))
    });
    let market_window_start_ms = market_window.map(|window| window.0);
    let deadline_ms = market_window
        .map(|window| crate::strategy::pair_lifecycle::ctf_first_deadline_ms(window.1));
    let (full_target_matched_at_ms, pre_deadline_status) = first_deadline_status(
        &first_instructions,
        first_instruction_evidence_complete,
        first_auth.owned_qty_units_6dp,
        deadline_ms,
    )?;
    let executed_reasons = post_instructions
        .iter()
        .filter(|instruction| !instruction.confirmed_fills.is_empty())
        .map(|instruction| instruction.decision_reason.clone())
        .collect::<BTreeSet<_>>();
    let sibling = history
        .iter()
        .copied()
        .filter(|outcome| outcome.token == *sibling_token)
        .max_by_key(|outcome| (outcome.ts_ms, outcome.ledger_seq))
        .ok_or_else(|| "missing_sibling_disposition".to_string())?;

    let mut row = CanonicalCtfExitCohortRow::base(
        symbol,
        cohort_key,
        market_name,
        cashflow,
        exit_class,
        terminal_operation
            .map(|operation| format!("{operation:?}").to_ascii_uppercase())
            .unwrap_or_else(|| "NONE".to_string()),
        (
            terminal_receipt_count,
            terminal_up_qty,
            terminal_down_qty,
            terminal_value,
        ),
        realized_pnl,
    );
    row.oracle_winner_side = Some(format!("{winner:?}").to_ascii_uppercase());
    row.first_authorized_at_ms = Some(Some(first_auth.ts_ms));
    row.first_sell_token = Some(first_auth.token.clone());
    row.market_window_start_ms = Some(market_window_start_ms);
    row.first_authorized_quantity_units_6dp = Some(first_auth.owned_qty_units_6dp);
    row.first_evidence_revision = Some(evidence_revision);
    row.first_confirmed_at_ms = Some(first_confirmed_at_ms);
    row.first_full_target_confirmed_at_ms = Some(
        (first_fill_qty == first_auth.owned_qty_units_6dp)
            .then_some(first_latest_confirmed_at_ms)
            .flatten(),
    );
    row.first_deadline_ms = Some(deadline_ms);
    row.first_full_target_matched_at_ms = Some(full_target_matched_at_ms);
    row.first_pre_deadline_status = Some(pre_deadline_status.to_string());
    row.first_instruction_evidence_complete = Some(first_instruction_evidence_complete);
    row.first_instructions = Some(first_instructions);
    row.first_submitted_order_count = first_order_count;
    row.first_confirmed_fill_count = first_fill_count;
    row.first_confirmed_fill_quantity_units_6dp = first_fill_qty;
    row.first_confirmed_proceeds_units_6dp = first_proceeds;
    row.first_realization = Some(first_realization);
    row.first_terminal_realization = Some(Some(first_terminal_realization));
    row.sibling_token = Some(sibling.token.clone());
    row.sibling_disposition = Some(if executed_reasons.is_empty() {
        format!("{:?}", sibling.outcome)
    } else {
        format!("{:?}", CtfLegEconomicOutcome::SellNow)
    });
    row.sibling_reason = Some(match executed_reasons.len() {
        0 => format!("{:?}", sibling.reason),
        1 => executed_reasons.into_iter().next().unwrap_or_default(),
        _ => format!(
            "MIXED[{}]",
            executed_reasons.into_iter().collect::<Vec<_>>().join(",")
        ),
    });
    row.post_instruction_evidence_complete = Some(post_instruction_evidence_complete);
    row.post_instructions = Some(post_instructions);
    row.post_submitted_order_count = post_order_count;
    row.post_confirmed_fill_count = post_fill_count;
    row.post_confirmed_fill_quantity_units_6dp = post_fill_qty;
    row.post_confirmed_proceeds_units_6dp = post_proceeds;
    row.remaining_realization = Some(remaining_realization);
    row.remaining_terminal_realization = Some(Some(remaining_terminal_realization));
    Ok(Some(row))
}

pub(super) fn captured_venue_outcome(
    cashflow: &CanonicalCohortCashflow,
    odds: &crate::polymarket::state::PolymarketOddsState,
) -> Option<(&'static str, u64, String)> {
    use polymarket_client_sdk::types::{B256, U256};

    let condition = cashflow.condition_id.parse::<B256>().ok()?;
    let up = cashflow.up_token.parse::<U256>().ok()?;
    let down = cashflow.down_token.parse::<U256>().ok()?;
    let context = odds.resolved_market_contexts.get(&condition)?;
    let winner = odds.resolved_market_winners.get(&condition)?;
    if up == down
        || context.up_token != up
        || context.down_token != down
        || context.observed_at_ms == 0
    {
        return None;
    }
    let side = if *winner == up {
        "UP"
    } else if *winner == down {
        "DOWN"
    } else {
        return None;
    };
    Some((
        side,
        context.observed_at_ms,
        format!(
            "captured_provider:sha256={}",
            alloy::hex::encode(context.raw_response_sha256)
        ),
    ))
}

pub(super) fn finalized_venue_outcome(
    cashflow: &CanonicalCohortCashflow,
    resolution: &FinalizedConditionResolutionReceipt,
) -> Option<(&'static str, u64, String)> {
    if resolution.condition_id != cashflow.condition_id || resolution.denominator == 0 {
        return None;
    }
    let winner_index = match (
        resolution.outcome_slot_0_numerator,
        resolution.outcome_slot_1_numerator,
    ) {
        (value, 0) if value == resolution.denominator => 0,
        (0, value) if value == resolution.denominator => 1,
        _ => return None,
    };
    let mut indices = [None, None];
    for event in &cashflow.events {
        let CanonicalCtfEvent::RedeemConfirmed(terminal) = &event.event else {
            continue;
        };
        if terminal.confirmed_receipt_identity().is_none() {
            continue;
        }
        if terminal.condition_id != cashflow.condition_id
            || terminal.up_token != cashflow.up_token
            || terminal.down_token != cashflow.down_token
            || resolution.observed_at_ms > event.ts_ms
        {
            return None;
        }
        for (retained, observed) in indices
            .iter_mut()
            .zip([terminal.up_outcome_index, terminal.down_outcome_index])
        {
            if let Some(index) = observed {
                if index > 1 || retained.is_some_and(|previous| previous != index) {
                    return None;
                }
                *retained = Some(index);
            }
        }
    }
    let [Some(up), Some(down)] = indices else {
        return None;
    };
    if up == down {
        return None;
    }
    Some((
        if up == winner_index { "UP" } else { "DOWN" },
        resolution.observed_at_ms,
        format!(
            "finalized_chain:tx={}:log={}:block={}",
            resolution.resolution_tx_hash,
            resolution.resolution_log_index,
            resolution.finalized_block_hash,
        ),
    ))
}

fn add_cohort(
    summary: &mut CanonicalCtfExitSummaryRow,
    cohort: &CanonicalCtfExitCohortRow,
    violations: &mut Vec<String>,
) {
    let bucket = match cohort.exit_class {
        CanonicalCtfExitClass::FirstToPostProceeds => &mut summary.first_to_post_proceeds,
        CanonicalCtfExitClass::FirstToSettlement => &mut summary.first_to_settlement,
        CanonicalCtfExitClass::SettlementOnlyNoCanonicalOracle => {
            &mut summary.settlement_only_no_canonical_oracle
        }
        CanonicalCtfExitClass::SettlementOnlyFailedMandatoryExecution => {
            &mut summary.settlement_only_failed_mandatory_execution
        }
        CanonicalCtfExitClass::Merge => &mut summary.merge,
        CanonicalCtfExitClass::BuyAndHold => &mut summary.buy_and_hold,
        CanonicalCtfExitClass::BuyManagedExit => &mut summary.buy_managed_exit,
    };
    let Some(cohort_count) = bucket.cohort_count.checked_add(1) else {
        violations.push(format!(
            "canonical_ctf_exit_count_overflow:symbol={}",
            summary.symbol
        ));
        return;
    };
    let Some(bucket_pnl) = bucket
        .realized_pnl_units_6dp
        .checked_add(cohort.realized_pnl_units_6dp)
    else {
        violations.push(format!(
            "canonical_ctf_exit_pnl_overflow:symbol={}",
            summary.symbol
        ));
        return;
    };
    let Some(total_pnl) = summary
        .total_realized_pnl_units_6dp
        .checked_add(cohort.realized_pnl_units_6dp)
    else {
        violations.push(format!(
            "canonical_ctf_exit_total_pnl_overflow:symbol={}",
            summary.symbol
        ));
        return;
    };
    bucket.cohort_count = cohort_count;
    bucket.realized_pnl_units_6dp = bucket_pnl;
    summary.total_realized_pnl_units_6dp = total_pnl;
}

fn add_summary(
    total: &mut CanonicalCtfExitSummaryRow,
    row: &CanonicalCtfExitSummaryRow,
    violations: &mut Vec<String>,
) {
    for (target, source, label) in [
        (
            &mut total.first_to_post_proceeds,
            &row.first_to_post_proceeds,
            "first_to_post_proceeds",
        ),
        (
            &mut total.first_to_settlement,
            &row.first_to_settlement,
            "first_to_settlement",
        ),
        (
            &mut total.settlement_only_no_canonical_oracle,
            &row.settlement_only_no_canonical_oracle,
            "settlement_only_no_canonical_oracle",
        ),
        (
            &mut total.settlement_only_failed_mandatory_execution,
            &row.settlement_only_failed_mandatory_execution,
            "settlement_only_failed_mandatory_execution",
        ),
        (&mut total.merge, &row.merge, "merge"),
        (&mut total.buy_and_hold, &row.buy_and_hold, "buy_and_hold"),
        (
            &mut total.buy_managed_exit,
            &row.buy_managed_exit,
            "buy_managed_exit",
        ),
    ] {
        let Some(count) = target.cohort_count.checked_add(source.cohort_count) else {
            violations.push(format!(
                "canonical_ctf_exit_total_count_overflow:class={label}"
            ));
            continue;
        };
        let Some(pnl) = target
            .realized_pnl_units_6dp
            .checked_add(source.realized_pnl_units_6dp)
        else {
            violations.push(format!(
                "canonical_ctf_exit_total_pnl_overflow:class={label}"
            ));
            continue;
        };
        target.cohort_count = count;
        target.realized_pnl_units_6dp = pnl;
    }
    match total
        .total_realized_pnl_units_6dp
        .checked_add(row.total_realized_pnl_units_6dp)
    {
        Some(value) => total.total_realized_pnl_units_6dp = value,
        None => violations.push("canonical_ctf_exit_total_realized_pnl_overflow".to_string()),
    }
}

fn validate_against_ctf_summary(
    row: &CanonicalCtfExitSummaryRow,
    pending_cohort_count: u64,
    ctf_summary: &[CanonicalCtfSummaryRow],
    violations: &mut Vec<String>,
) -> bool {
    let Some(canonical) = ctf_summary
        .iter()
        .find(|summary| summary.symbol == row.symbol)
    else {
        violations.push(format!(
            "canonical_ctf_exit_missing_accounting_summary:symbol={}",
            row.symbol
        ));
        return false;
    };
    let prior_violation_count = violations.len();
    // Every cohort is named by its own decision, so the exit classes account for the whole
    // canonical population — closed and open alike — against the same canonical counts.
    let projected_count = row
        .first_to_post_proceeds
        .cohort_count
        .checked_add(row.first_to_settlement.cohort_count)
        .and_then(|count| count.checked_add(row.settlement_only_no_canonical_oracle.cohort_count))
        .and_then(|count| {
            count.checked_add(row.settlement_only_failed_mandatory_execution.cohort_count)
        })
        .and_then(|count| count.checked_add(row.merge.cohort_count))
        .and_then(|count| count.checked_add(row.buy_and_hold.cohort_count))
        .and_then(|count| count.checked_add(row.buy_managed_exit.cohort_count))
        .and_then(|count| count.checked_add(pending_cohort_count));
    let canonical_count = canonical
        .closed_cohort_count
        .checked_add(canonical.open_cohort_count);
    if canonical_count.is_none() || projected_count != canonical_count {
        violations.push(format!(
            "canonical_ctf_exit_cohort_count_mismatch:symbol={}",
            row.symbol
        ));
    }
    let Some(canonical_pnl) = canonical.realized_pnl.units_6dp() else {
        return false;
    };
    if canonical_pnl != row.total_realized_pnl_units_6dp {
        violations.push(format!(
            "canonical_ctf_exit_realized_pnl_mismatch:symbol={}",
            row.symbol
        ));
    }
    violations.len() == prior_violation_count
}

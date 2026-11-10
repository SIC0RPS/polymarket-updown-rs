use crate::strategy::facts::CtfLegDecisionReason;

use super::*;

pub(super) fn project_unassigned(
    symbol: &str,
    cohort_key: &str,
    market_name: Option<&String>,
    cashflow: &CanonicalCohortCashflow,
    realized_pnl: i128,
    complete: bool,
    history: &[&CanonicalOwnedLegOutcome],
) -> Result<Option<CanonicalCtfExitCohortRow>, String> {
    let tokens = BTreeSet::from([cashflow.up_token.as_str(), cashflow.down_token.as_str()]);
    let seen = history
        .iter()
        .map(|outcome| outcome.token.as_str())
        .collect::<BTreeSet<_>>();
    if complete && seen != tokens {
        return Err("unassigned_leg_identity_mismatch".to_string());
    }
    let nonexecuting = |o: &&CanonicalOwnedLegOutcome| {
        tokens.contains(o.token.as_str())
            && o.action_qty_units_6dp == 0
            && o.execution_intent_id.is_none()
            && o.execution_evidence_revision.is_none()
            && o.close_reason.is_none()
    };
    if !complete && !history.iter().all(|o| nonexecuting(o)) {
        return Err("invalid_unassigned_open_history".into());
    }
    if complete
        && !history.iter().all(|o| {
            let expected_qty = if o.token == cashflow.up_token {
                Some(cashflow.up_created_qty_units_6dp)
            } else if o.token == cashflow.down_token {
                Some(cashflow.down_created_qty_units_6dp)
            } else {
                None
            };
            nonexecuting(o)
                && expected_qty == Some(o.owned_qty_units_6dp)
                && o.post_sell_full_entry_pnl_bits.is_none()
                && o.post_hold_full_entry_pnl_bits.is_none()
                && o.post_fillable_qty_bits.is_none()
                && o.outcome == CtfLegEconomicOutcome::HoldForSettlement
        })
    {
        return Err("invalid_unassigned_leg_history".to_string());
    }
    let final_winner_side = if complete {
        let latest_up = history
            .iter()
            .copied()
            .filter(|outcome| outcome.token == cashflow.up_token)
            .max_by_key(|outcome| (outcome.ts_ms, outcome.ledger_seq))
            .ok_or_else(|| "unassigned_missing_final_UP_state".to_string())?;
        let latest_down = history
            .iter()
            .copied()
            .filter(|outcome| outcome.token == cashflow.down_token)
            .max_by_key(|outcome| (outcome.ts_ms, outcome.ledger_seq))
            .ok_or_else(|| "unassigned_missing_final_DOWN_state".to_string())?;
        if latest_up.ts_ms != latest_down.ts_ms {
            return Err("unassigned_final_state_timestamp_mismatch".to_string());
        }
        match (latest_up.causal_winner_side, latest_down.causal_winner_side) {
            (Some(up_side), Some(down_side))
                if up_side == down_side
                    && latest_up.reason != CtfLegDecisionReason::CtfFirstSellNoSide
                    && latest_down.reason != CtfLegDecisionReason::CtfFirstSellNoSide =>
            {
                Some(up_side)
            }
            (None, None)
                if latest_up.reason == CtfLegDecisionReason::CtfFirstSellNoSide
                    && latest_down.reason == CtfLegDecisionReason::CtfFirstSellNoSide =>
            {
                None
            }
            _ => return Err("invalid_unassigned_final_state".to_string()),
        }
    } else {
        None
    };
    if !cashflow.order_submissions.is_empty()
        || !cashflow.managed_sell_receipts.is_empty()
        || cashflow.up_confirmed_sold_qty_units_6dp != 0
        || cashflow.down_confirmed_sold_qty_units_6dp != 0
        || cashflow.confirmed_sell_proceeds_units_6dp != 0
    {
        return Err("unassigned_has_sell_execution".to_string());
    }
    if complete && cashflow.terminal_receipts.is_empty() {
        return Err("unassigned_missing_terminal_receipt".to_string());
    }
    let (terminal_receipt_count, terminal_up_qty, terminal_down_qty, terminal_value) =
        terminal_totals(cashflow)?;
    let up_terminal_qty = cashflow
        .up_confirmed_merged_qty_units_6dp
        .checked_add(cashflow.up_confirmed_redeemed_qty_units_6dp)
        .ok_or_else(|| "unassigned_up_terminal_quantity_overflow".to_string())?;
    let down_terminal_qty = cashflow
        .down_confirmed_merged_qty_units_6dp
        .checked_add(cashflow.down_confirmed_redeemed_qty_units_6dp)
        .ok_or_else(|| "unassigned_down_terminal_quantity_overflow".to_string())?;
    if complete
        && (up_terminal_qty != cashflow.up_created_qty_units_6dp
            || down_terminal_qty != cashflow.down_created_qty_units_6dp
            || cashflow.up_confirmed_disposed_qty_units_6dp != cashflow.up_created_qty_units_6dp
            || cashflow.down_confirmed_disposed_qty_units_6dp
                != cashflow.down_created_qty_units_6dp
            || terminal_up_qty != cashflow.up_created_qty_units_6dp
            || terminal_down_qty != cashflow.down_created_qty_units_6dp
            || cashflow.up_remaining_qty_units_6dp != 0
            || cashflow.down_remaining_qty_units_6dp != 0)
    {
        return Err("unassigned_share_conservation_unproven".to_string());
    }
    if terminal_value != cashflow.confirmed_terminal_receipts_units_6dp {
        return Err("terminal_receipt_mismatch".to_string());
    }
    let up_realization = cashflow
        .realization_for_token(&cashflow.up_token)
        .ok_or_else(|| "UP_realization_failed".to_string())?;
    let down_realization = cashflow
        .realization_for_token(&cashflow.down_token)
        .ok_or_else(|| "DOWN_realization_failed".to_string())?;
    let terminal_operation = terminal_operation(cashflow)?;

    if !complete && terminal_operation.is_none() {
        let receipt_realized = up_realization
            .realized_pnl_units_6dp
            .checked_add(down_realization.realized_pnl_units_6dp);
        let valid_pending = cashflow.terminal_receipts.is_empty()
            && terminal_receipt_count == 0
            && terminal_up_qty == 0
            && terminal_down_qty == 0
            && terminal_value == 0
            && cashflow.confirmed_terminal_receipts_units_6dp == 0
            && cashflow.up_confirmed_merged_qty_units_6dp == 0
            && cashflow.down_confirmed_merged_qty_units_6dp == 0
            && cashflow.up_confirmed_redeemed_qty_units_6dp == 0
            && cashflow.down_confirmed_redeemed_qty_units_6dp == 0
            && cashflow.up_confirmed_disposed_qty_units_6dp == 0
            && cashflow.down_confirmed_disposed_qty_units_6dp == 0
            && cashflow.up_remaining_qty_units_6dp == cashflow.up_created_qty_units_6dp
            && cashflow.down_remaining_qty_units_6dp == cashflow.down_created_qty_units_6dp
            && cashflow.up_created_qty_units_6dp == cashflow.down_created_qty_units_6dp
            && cashflow.up_created_qty_units_6dp > 0
            && receipt_realized == Some(0)
            && realized_pnl == 0
            && cashflow.confirmed_fee_units_6dp == cashflow.split_fee_units_6dp
            && cashflow.confirmed_approved_direct_cost_units_6dp
                == cashflow.split_approved_direct_cost_units_6dp;
        return if valid_pending {
            Ok(None)
        } else {
            Err("invalid_unassigned_pending_receipt_state".to_string())
        };
    }

    let exit_class = match terminal_operation {
        Some(CtfTerminalOperation::Redeem) => {
            if final_winner_side.is_some() {
                CanonicalCtfExitClass::SettlementOnlyFailedMandatoryExecution
            } else {
                CanonicalCtfExitClass::SettlementOnlyNoCanonicalOracle
            }
        }
        Some(CtfTerminalOperation::Merge) => CanonicalCtfExitClass::Merge,
        None => return Err("unassigned_has_no_canonical_exit_class".to_string()),
    };

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
    row.unassigned_up_realization = Some(up_realization);
    row.unassigned_down_realization = Some(down_realization);
    Ok(Some(row))
}

pub(super) fn project_buy(
    symbol: &str,
    cohort_key: &str,
    market_name: Option<&String>,
    cashflow: &CanonicalCohortCashflow,
    realized_pnl: i128,
    complete: bool,
    history: &[&CanonicalOwnedLegOutcome],
    order_execution_lineage: &[&CanonicalOrderExecutionLineageRow],
) -> Result<Option<CanonicalCtfExitCohortRow>, String> {
    let authorization = cashflow
        .buy_authorization
        .ok_or_else(|| "BUY acquisition authorization missing".to_string())?;
    let held_token = authorization.selected_token().to_string();
    if cashflow.up_confirmed_merged_qty_units_6dp != 0
        || cashflow.down_confirmed_merged_qty_units_6dp != 0
        || cashflow.split_principal_units_6dp != 0
        || !cashflow.split_receipt_id.is_empty()
        || !cashflow.split_transaction_id.is_empty()
    {
        return Err("BUY acquisition has split or merge facts".to_string());
    }
    if history.iter().any(|outcome| {
        outcome.token != held_token
            || outcome.first_instruction_economics.is_some()
            || match outcome.outcome {
                CtfLegEconomicOutcome::HoldForSettlement => {
                    outcome.close_reason.is_some()
                        || outcome.reason == CtfLegDecisionReason::CtfFirstSellNoSide
                }
                CtfLegEconomicOutcome::SellNow => {
                    outcome.close_reason != Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
                        || !matches!(
                            outcome.reason,
                            CtfLegDecisionReason::DeadCat | CtfLegDecisionReason::ProfitNow
                        )
                }
            }
    }) {
        return Err("BUY acquisition has invalid owned-leg history".to_string());
    }
    let post_history = history
        .iter()
        .copied()
        .filter(|outcome| {
            outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
        })
        .collect::<Vec<_>>();
    let post_intents = intent_ids(&post_history);
    if authorization.symbol.as_str() != symbol
        || authorization.condition_id.to_string() != cashflow.condition_id
        || authorization.up_token.to_string() != cashflow.up_token
        || authorization.down_token.to_string() != cashflow.down_token
        || cashflow.order_submissions.iter().any(|order| {
            (order.intent_id != authorization.intent_id && !post_intents.contains(&order.intent_id))
                || order.asset_id != held_token
        })
    {
        return Err("BUY acquisition authorization identity mismatch".to_string());
    }
    let (created, opposite) = if authorization.selected_up {
        (
            cashflow.up_created_qty_units_6dp,
            cashflow.down_created_qty_units_6dp,
        )
    } else {
        (
            cashflow.down_created_qty_units_6dp,
            cashflow.up_created_qty_units_6dp,
        )
    };
    if created == 0 || created > authorization.target_qty_units_6dp || opposite != 0 {
        return Err("BUY acquisition quantity differs from authorized side or target".to_string());
    }
    let post_receipts = cashflow.managed_sell_receipts.iter().collect::<Vec<_>>();
    if post_receipts.iter().any(|receipt| {
        receipt.token != held_token
            || receipt.close_reason != POST_CLOSE_REASON
            || !post_intents.contains(&receipt.intent_id)
    }) {
        return Err("BUY exit receipt identity mismatch".to_string());
    }
    let (post_fill_count, post_fill_qty, post_proceeds, _, _) = receipt_totals(&post_receipts)?;
    if cashflow
        .up_confirmed_sold_qty_units_6dp
        .checked_add(cashflow.down_confirmed_sold_qty_units_6dp)
        != Some(post_fill_qty)
        || post_proceeds != cashflow.confirmed_sell_proceeds_units_6dp
    {
        return Err("BUY exit receipt totals mismatch".to_string());
    }
    let post_order_count = count_orders(cashflow, &post_intents)?;
    let (post_instructions, post_instruction_evidence_complete) = project_instructions(
        &post_history,
        order_execution_lineage,
        cashflow,
        POST_CLOSE_REASON,
    )?;
    let operation = terminal_operation(cashflow)?;
    if operation == Some(CtfTerminalOperation::Merge) {
        return Err("BUY holding has invalid terminal operation".to_string());
    }
    let terminal = terminal_totals(cashflow)?;
    let up_disposed = terminal
        .1
        .checked_add(cashflow.up_confirmed_sold_qty_units_6dp);
    let down_disposed = terminal
        .2
        .checked_add(cashflow.down_confirmed_sold_qty_units_6dp);
    if up_disposed.and_then(|quantity| quantity.checked_add(cashflow.up_remaining_qty_units_6dp))
        != Some(cashflow.up_created_qty_units_6dp)
        || down_disposed
            .and_then(|quantity| quantity.checked_add(cashflow.down_remaining_qty_units_6dp))
            != Some(cashflow.down_created_qty_units_6dp)
        || terminal.1 != cashflow.up_confirmed_redeemed_qty_units_6dp
        || terminal.2 != cashflow.down_confirmed_redeemed_qty_units_6dp
        || up_disposed != Some(cashflow.up_confirmed_disposed_qty_units_6dp)
        || down_disposed != Some(cashflow.down_confirmed_disposed_qty_units_6dp)
        || terminal.3 != cashflow.confirmed_terminal_receipts_units_6dp
        || (complete
            && (cashflow.up_remaining_qty_units_6dp != 0
                || cashflow.down_remaining_qty_units_6dp != 0))
    {
        return Err("BUY acquisition terminal share conservation unproven".to_string());
    }
    let realization = cashflow
        .total_realization()
        .ok_or_else(|| "BUY realization overflow".to_string())?;
    let residual_realization = terminal_realization(cashflow, &held_token)?;
    if realization.realized_pnl_units_6dp != realized_pnl
        || sell_realization(&post_receipts)?.checked_add(residual_realization) != Some(realization)
    {
        return Err("BUY canonical realization mismatch".to_string());
    }
    let executed_reasons = post_instructions
        .iter()
        .filter(|instruction| !instruction.confirmed_fills.is_empty())
        .map(|instruction| instruction.decision_reason.clone())
        .collect::<BTreeSet<_>>();
    let latest = history
        .iter()
        .copied()
        .max_by_key(|outcome| outcome.ledger_seq);
    let mut row = CanonicalCtfExitCohortRow::base(
        symbol,
        cohort_key,
        market_name,
        cashflow,
        if post_fill_count > 0 {
            CanonicalCtfExitClass::BuyManagedExit
        } else {
            CanonicalCtfExitClass::BuyAndHold
        },
        operation
            .map(|operation| format!("{operation:?}").to_ascii_uppercase())
            .unwrap_or_else(|| "NONE".to_string()),
        terminal,
        realized_pnl,
    );
    row.buy_realization = Some(realization);
    row.first_authorized_at_ms = None;
    row.first_confirmed_at_ms = None;
    row.first_full_target_confirmed_at_ms = None;
    row.first_deadline_ms = None;
    row.first_full_target_matched_at_ms = None;
    row.first_pre_deadline_status = None;
    row.first_instruction_evidence_complete = None;
    row.first_instructions = None;
    row.first_terminal_realization = None;
    row.post_instruction_evidence_complete = None;
    row.post_instructions = None;
    row.remaining_terminal_realization = None;
    if let Some(latest) = latest {
        row.sibling_token = Some(held_token);
        row.sibling_disposition = Some(if executed_reasons.is_empty() {
            format!("{:?}", latest.outcome)
        } else {
            format!("{:?}", CtfLegEconomicOutcome::SellNow)
        });
        row.sibling_reason = Some(match executed_reasons.len() {
            0 => format!("{:?}", latest.reason),
            1 => executed_reasons.into_iter().next().unwrap_or_default(),
            _ => format!(
                "MIXED[{}]",
                executed_reasons.into_iter().collect::<Vec<_>>().join(",")
            ),
        });
        row.post_instruction_evidence_complete = Some(post_instruction_evidence_complete);
        row.post_instructions = Some(post_instructions);
        row.remaining_terminal_realization = Some(Some(residual_realization));
    }
    row.post_submitted_order_count = post_order_count;
    row.post_confirmed_fill_count = post_fill_count;
    row.post_confirmed_fill_quantity_units_6dp = post_fill_qty;
    row.post_confirmed_proceeds_units_6dp = post_proceeds;
    Ok(Some(row))
}

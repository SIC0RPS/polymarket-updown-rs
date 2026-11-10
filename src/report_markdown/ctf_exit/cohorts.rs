use super::{
    comparison, display_instructions, display_leg_realization, display_money_units_6dp,
    display_optional_money_units_6dp, display_optional_quantity, display_optional_timestamp,
    display_quantity_units_6dp, display_timing, display_unsigned_money_units_6dp, section, table,
    CanonicalCtfExitCohortRow,
};

pub(super) fn include_cohort_column(index: usize, instructions: bool, timing: bool) -> bool {
    match index {
        6 | 10 | 11 => timing,
        12 | 13 | 17 | 19 => instructions,
        _ => true,
    }
}

pub(super) fn cohort_row(
    row: &CanonicalCtfExitCohortRow,
    instructions_available: bool,
    timing_available: bool,
) -> Vec<String> {
    let oracle_to_first_sell = match (&row.oracle_winner_side, &row.first_sell_token) {
        (Some(winner), Some(token)) => format!("{winner} → {token}"),
        _ => "NA".to_string(),
    };
    let first_authorization = match (
        row.first_authorized_quantity_units_6dp,
        row.first_evidence_revision,
    ) {
        (Some(quantity), Some(revision)) => {
            format!("{} / {revision}", display_quantity_units_6dp(quantity))
        }
        _ => "NA".to_string(),
    };
    let sibling = match (
        &row.sibling_token,
        &row.sibling_disposition,
        &row.sibling_reason,
    ) {
        (Some(token), Some(disposition), Some(reason)) => {
            format!("{token} / {disposition} / {reason}")
        }
        _ => "NA".to_string(),
    };
    vec![
        row.symbol.clone(),
        row.paired_cohort_key.clone(),
        row.exit_class.label().to_string(),
        row.market_id.clone(),
        row.market_name.clone().unwrap_or_else(|| "NA".to_string()),
        row.split_ts_ms.to_string(),
        display_timing(
            row.market_window_start_ms.flatten(),
            row.market_window_start_ms.flatten(),
            row.split_ts_ms,
        ),
        display_unsigned_money_units_6dp(row.split_principal_units_6dp),
        oracle_to_first_sell,
        first_authorization,
        display_timing(
            row.first_confirmed_at_ms.flatten(),
            row.market_window_start_ms.flatten(),
            row.split_ts_ms,
        ),
        display_timing(
            row.first_full_target_confirmed_at_ms.flatten(),
            row.market_window_start_ms.flatten(),
            row.split_ts_ms,
        ),
        if timing_available {
            format!(
                "deadline={} / full_match={} / strict={}",
                display_timing(
                    row.first_deadline_ms.flatten(),
                    row.market_window_start_ms.flatten(),
                    row.split_ts_ms,
                ),
                display_timing(
                    row.first_full_target_matched_at_ms.flatten(),
                    row.market_window_start_ms.flatten(),
                    row.split_ts_ms,
                ),
                row.first_pre_deadline_status
                    .as_deref()
                    .unwrap_or("UNKNOWN")
            )
        } else {
            format!(
                "{} / {} / {}",
                display_optional_timestamp(row.first_deadline_ms.flatten()),
                display_optional_timestamp(row.first_full_target_matched_at_ms.flatten()),
                row.first_pre_deadline_status
                    .as_deref()
                    .unwrap_or("UNKNOWN")
            )
        },
        display_instructions(row, true),
        row.first_submitted_order_count.to_string(),
        format!(
            "{} / {} / {}",
            row.first_confirmed_fill_count,
            display_quantity_units_6dp(row.first_confirmed_fill_quantity_units_6dp),
            display_money_units_6dp(row.first_confirmed_proceeds_units_6dp)
        ),
        display_leg_realization(row.first_realization),
        display_leg_realization(row.first_terminal_realization.flatten()),
        sibling,
        display_instructions(row, false),
        row.post_submitted_order_count.to_string(),
        format!(
            "{} / {} / {}",
            row.post_confirmed_fill_count,
            display_quantity_units_6dp(row.post_confirmed_fill_quantity_units_6dp),
            display_money_units_6dp(row.post_confirmed_proceeds_units_6dp)
        ),
        display_leg_realization(row.remaining_realization),
        format!(
            "{} / {} / {} / {} / {}",
            row.terminal_operation,
            row.terminal_receipt_count,
            display_quantity_units_6dp(row.terminal_up_quantity_units_6dp),
            display_quantity_units_6dp(row.terminal_down_quantity_units_6dp),
            display_money_units_6dp(row.terminal_receipt_value_units_6dp)
        ),
        display_leg_realization(row.unassigned_up_realization),
        display_leg_realization(row.unassigned_down_realization),
        format!(
            "{} / {}",
            display_quantity_units_6dp(row.up_remaining_quantity_units_6dp),
            display_quantity_units_6dp(row.down_remaining_quantity_units_6dp)
        ),
        display_optional_money_units_6dp(Some(row.realized_pnl_units_6dp)),
    ]
    .into_iter()
    .enumerate()
    .filter(|(index, _)| include_cohort_column(*index, instructions_available, timing_available))
    .map(|(_, cell)| cell)
    .collect()
}

pub(super) fn render_buy_cohorts(body: &mut String, cohorts: &[CanonicalCtfExitCohortRow]) {
    section(body, "Canonical BUY Acquisition Cohorts");
    table(
        body,
        &[
            "symbol",
            "cohort",
            "market_id",
            "exit",
            "original BUY prediction / token",
            "prediction / authorization / first confirmed timestamp",
            "authorized target / confirmed acquired qty",
            "confirmed principal / refunds / fees / costs",
            "owned token / disposition / reason",
            "exit instructions: intent / order / reason / qty / terminal matched / unfilled / confirmed fills / evidence",
            "exit orders / fills / qty / sell proceeds",
            "residual terminal proceeds / principal / fees / costs / PnL",
            "terminal operation / receipts / UP qty / DOWN qty / value",
            "remaining UP / DOWN",
            "realized proceeds / principal / refunds / fees / costs / PnL",
            "cohort realized PnL",
            "venue outcome / observation timestamp / provenance",
            "BUY prediction vs venue",
            "confirmed DeadCat sold token vs venue",
        ],
        cohorts
            .iter()
            .filter(|row| row.buy_authorization.is_some())
            .map(|row| {
                let authorization = row.buy_authorization;
                let prediction = authorization.map(|authorization| {
                    if authorization.selected_up {
                        "UP"
                    } else {
                        "DOWN"
                    }
                });
                let comparison = comparison(row);
                let outcome = comparison.outcome;
                let realized = row.buy_realization.map_or_else(
                    || "UNKNOWN".to_string(),
                    |value| {
                        format!(
                            "{} / {} / {} / {} / {} / {}",
                            display_money_units_6dp(value.confirmed_proceeds_units_6dp),
                            display_unsigned_money_units_6dp(value.allocated_principal_units_6dp),
                            display_unsigned_money_units_6dp(
                                value.allocated_collateral_refund_units_6dp
                            ),
                            display_unsigned_money_units_6dp(value.attributable_fee_units_6dp),
                            display_unsigned_money_units_6dp(
                                value.attributable_approved_direct_cost_units_6dp
                            ),
                            display_money_units_6dp(value.realized_pnl_units_6dp),
                        )
                    },
                );
                vec![
                    row.symbol.clone(),
                    row.paired_cohort_key.clone(),
                    row.market_id.clone(),
                    row.exit_class.label().to_string(),
                    format!(
                        "{} / {}",
                        prediction.unwrap_or("UNKNOWN"),
                        authorization
                            .map(|authorization| authorization.selected_token().to_string())
                            .unwrap_or_else(|| "UNKNOWN".to_string())
                    ),
                    format!(
                        "{} / {} / {}",
                        display_optional_timestamp(
                            authorization.map(|authorization| authorization.prediction_at_ms)
                        ),
                        display_optional_timestamp(
                            authorization.map(|authorization| authorization.authorized_at_ms)
                        ),
                        display_optional_timestamp(row.buy_first_confirmed_at_ms)
                    ),
                    format!(
                        "{} / {}",
                        display_optional_quantity(
                            authorization.map(|authorization| authorization.target_qty_units_6dp)
                        ),
                        display_optional_quantity(row.buy_created_quantity_units_6dp)
                    ),
                    format!(
                        "{} / {} / {} / {}",
                        row.buy_principal_units_6dp
                            .map(display_unsigned_money_units_6dp)
                            .unwrap_or_else(|| "UNKNOWN".to_string()),
                        row.buy_collateral_refund_units_6dp
                            .map(display_unsigned_money_units_6dp)
                            .unwrap_or_else(|| "UNKNOWN".to_string()),
                        display_unsigned_money_units_6dp(row.confirmed_fee_units_6dp),
                        display_unsigned_money_units_6dp(
                            row.confirmed_approved_direct_cost_units_6dp
                        )
                    ),
                    format!(
                        "{} / {} / {}",
                        row.sibling_token.as_deref().unwrap_or("NA"),
                        row.sibling_disposition.as_deref().unwrap_or("NA"),
                        row.sibling_reason.as_deref().unwrap_or("NA")
                    ),
                    if row.post_instructions.is_some() {
                        display_instructions(row, false)
                    } else {
                        "NA".to_string()
                    },
                    format!(
                        "{} / {} / {} / {}",
                        row.post_submitted_order_count,
                        row.post_confirmed_fill_count,
                        display_quantity_units_6dp(row.post_confirmed_fill_quantity_units_6dp),
                        display_money_units_6dp(row.post_confirmed_proceeds_units_6dp)
                    ),
                    display_leg_realization(if row.post_instructions.is_some() {
                        row.remaining_terminal_realization.flatten()
                    } else {
                        row.buy_realization
                    }),
                    format!(
                        "{} / {} / {} / {} / {}",
                        row.terminal_operation,
                        row.terminal_receipt_count,
                        display_quantity_units_6dp(row.terminal_up_quantity_units_6dp),
                        display_quantity_units_6dp(row.terminal_down_quantity_units_6dp),
                        display_money_units_6dp(row.terminal_receipt_value_units_6dp)
                    ),
                    format!(
                        "{} / {}",
                        display_quantity_units_6dp(row.up_remaining_quantity_units_6dp),
                        display_quantity_units_6dp(row.down_remaining_quantity_units_6dp)
                    ),
                    realized,
                    display_money_units_6dp(row.realized_pnl_units_6dp),
                    format!(
                        "{} / {} / {}",
                        outcome.unwrap_or("UNKNOWN"),
                        display_optional_timestamp(row.venue_resolution_observed_at_ms.flatten()),
                        row.venue_resolution_provenance
                            .as_ref()
                            .and_then(Option::as_deref)
                            .unwrap_or("UNKNOWN")
                    ),
                    match prediction.zip(outcome) {
                        Some((prediction, outcome)) if prediction == outcome => "CORRECT",
                        Some(_) => "INCORRECT",
                        None => "UNKNOWN",
                    }
                    .to_string(),
                    if !comparison.dead_cat_sold {
                        "NONE"
                    } else {
                        match comparison.dead_cat_winner {
                            Some(true) => "WINNING_TOKEN",
                            Some(false) => "LOSING_TOKEN",
                            None => "UNKNOWN",
                        }
                    }
                    .to_string(),
                ]
            })
            .collect(),
    );
}

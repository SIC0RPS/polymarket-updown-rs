mod accounting;
mod cohorts;

pub(crate) use accounting::{aggregate, SymbolExitAgg};

use chrono::{SecondsFormat, TimeZone, Utc};

use crate::strategy::facts::CtfLegDecisionReason;
use crate::strategy_runtime::replay_mode::RecordedMarketCoverageReport;
use crate::trade_session_ledger::{
    CanonicalCohortCashflow, CanonicalCtfExitCohortRow, CanonicalCtfExitSummaryRow,
    CanonicalCtfLegRealization, CanonicalSessionStats,
};

use super::table::{display_quantity_units_6dp, display_units_6dp, section, table};

pub(super) fn render(
    body: &mut String,
    summary: &[CanonicalCtfExitSummaryRow],
    cohorts: &[CanonicalCtfExitCohortRow],
    stats: Option<&CanonicalSessionStats>,
    recorded_market_coverage: Option<&RecordedMarketCoverageReport>,
) {
    let buys_available = cohorts.iter().any(|row| row.buy_authorization.is_some());
    let comparisons_available = cohorts.iter().any(accounting::comparison_fields_present);
    let instructions_available =
        cohorts.is_empty() || cohorts.iter().any(|row| row.first_instructions.is_some());
    let timing_available = cohorts.is_empty()
        || cohorts
            .iter()
            .any(|row| row.market_window_start_ms.is_some());
    let first_economics_available = cohorts.iter().any(|cohort| {
        cohort
            .first_instructions
            .iter()
            .flatten()
            .any(|instruction| instruction.first_economics.is_some())
    });
    section(
        body,
        if buys_available {
            "Canonical Entry Outcomes"
        } else {
            "Canonical CTF Exit Outcomes"
        },
    );

    let mut summary_columns = vec![
        "Symbol",
        if comparisons_available {
            "Cohorts"
        } else {
            "Entries"
        },
        "First (orders/fills / PnL)",
        "DeadCat (orders/fills / PnL; saved / forgone / coverage)",
        "ProfitNow (orders/fills / PnL)",
        "Hold→settlement (cohorts / PnL)",
        "Merge (cohorts / recovered pUSD)",
        "Realized PnL",
        "Avg PnL / entry",
    ];
    if comparisons_available {
        summary_columns.extend([
            "First prediction known / total / unknown",
            "Venue outcome known / total / unknown",
            "First correct / comparable / unknown",
            "Confirmed DeadCat cohorts: winning / losing / unknown / total",
        ]);
    }
    if buys_available {
        summary_columns.extend([
            "Split cohorts",
            "BUY cohorts / residual PnL",
            "BUY prediction known / total / unknown",
            "BUY correct / comparable / unknown",
        ]);
    }
    table(
        body,
        &summary_columns,
        accounting::rows(
            summary,
            cohorts,
            stats,
            comparisons_available,
            buys_available,
        ),
    );
    body.push_str("\nDeadCat saved on eventual losing tokens is confirmed net sale proceeds; payout forgone on eventual winning tokens is the sold quantity less net sale proceeds. Both use sale-only fees and direct costs, exclude sunk acquisition allocations, and leave realized PnL unchanged. Coverage counts cohorts with recorded venue outcomes and exact instruction-linked canonical sale receipts; unavailable cohorts contribute no comparison amount.\n");
    if comparisons_available {
        body.push_str("\nFirst compares the original authorized prediction with independently retained venue resolution. DeadCat classifies the token actually sold by confirmed instruction-linked fills; each cohort counts once. Unknown First comparisons include missing predictions or outcomes. Numerical forecasts, probability intervals and contemporaneous exit signals are UNAVAILABLE; retained First direction and Chainlink training labels do not supply them.\n");
    }

    if buys_available {
        body.push_str("\nBUY predictions retain the original authorized direction and are compared with independently retained venue resolution. First coverage counts only split cohorts. BUY principal, refunds, fees, costs and realized PnL come from confirmed canonical receipts; unconfirmed acquisition quantities are excluded.\n");
    }

    render_recorded_market_coverage(body, recorded_market_coverage);

    if !buys_available || cohorts.iter().any(|row| row.buy_authorization.is_none()) {
        section(body, "Canonical CTF Exit Cohorts");
        let mut columns = [
                "symbol",
                "cohort",
                "exit",
                "market_id",
                "market_name",
                "split_ts_ms",
                "market open: timestamp / market age / elapsed after split",
                "principal",
                if comparisons_available {
                    "First prediction → authorized First sell token"
                } else {
                    "oracle winner → first-sell token"
                },
                if instructions_available {
                    "first original target qty / evidence revision"
                } else {
                    "first authorization qty / evidence revision"
                },
                "first confirmation: timestamp / market age / elapsed after split",
                "first full-target confirmation: timestamp / market age / elapsed after split",
                if timing_available {
                    "first deadline / full match: timestamp / market age / elapsed after split / strict status"
                } else {
                    "first deadline / full match / strict status"
                },
                if first_economics_available {
                    "first instructions: intent / order / reason / qty / terminal matched / unfilled / confirmed fills / evidence / economics"
                } else {
                    "first instructions: intent / order / reason / qty / terminal matched / unfilled / confirmed fills / evidence"
                },
                "first orders",
                "first fills / qty / sell proceeds",
                if instructions_available {
                    "first managed-sell proceeds / basis / fees / costs / PnL"
                } else {
                    "first proceeds / basis / fees / costs / PnL"
                },
                "unsold First terminal proceeds / basis / fees / costs / PnL",
                if comparisons_available {
                    "remaining token / disposition / reason"
                } else {
                    "sibling token / disposition / reason"
                },
                "post instructions: intent / order / reason / qty / terminal matched / unfilled / confirmed fills / evidence",
                "post orders",
                "post fills / qty / sell proceeds",
                "remaining proceeds / basis / fees / costs / PnL",
                "terminal operation / receipts / UP qty / DOWN qty / value",
                "unassigned UP proceeds / basis / fees / costs / PnL",
                "unassigned DOWN proceeds / basis / fees / costs / PnL",
                "remaining UP / DOWN",
                "cohort realized PnL",
            ]
            .into_iter()
            .enumerate()
            .filter(|(index, _)| cohorts::include_cohort_column(*index, instructions_available, timing_available))
            .map(|(_, column)| column)
            .collect::<Vec<_>>();
        if comparisons_available {
            columns.extend([
                "First authorization: timestamp / market age / elapsed after split",
                "confirmed First sale: side / token",
                "venue outcome / observation timestamp / provenance",
                "First prediction vs venue",
                "confirmed DeadCat sold token vs venue",
            ]);
        }
        table(
            body,
            &columns,
            cohorts
                .iter()
                .filter(|row| row.buy_authorization.is_none())
                .map(|row| {
                    let mut cells =
                        cohorts::cohort_row(row, instructions_available, timing_available);
                    if comparisons_available {
                        cells.extend(comparison_cells(row));
                    }
                    cells
                })
                .collect(),
        );
    }
    if buys_available {
        cohorts::render_buy_cohorts(body, cohorts);
    }
}

pub(super) struct CohortComparison<'a> {
    pub prediction: Option<&'a str>,
    pub outcome: Option<&'a str>,
    pub first_correct: Option<bool>,
    pub dead_cat_sold: bool,
    pub dead_cat_winner: Option<bool>,
}

pub(super) fn comparison(row: &CanonicalCtfExitCohortRow) -> CohortComparison<'_> {
    let prediction = row
        .oracle_winner_side
        .as_deref()
        .filter(|side| matches!(*side, "UP" | "DOWN"));
    let outcome = row
        .venue_winner_side
        .as_ref()
        .and_then(Option::as_deref)
        .filter(|side| matches!(*side, "UP" | "DOWN"));
    let first_correct = prediction.zip(outcome).map(|(first, venue)| first == venue);
    let dead_cat = format!("{:?}", CtfLegDecisionReason::DeadCat);
    let mut dead_cat_sold = false;
    let mut dead_cat_integrity = true;
    for instruction in row.post_instructions.iter().flatten() {
        if instruction.decision_reason == dead_cat
            && instruction
                .confirmed_fills
                .iter()
                .any(|fill| fill.quantity_units_6dp > 0)
        {
            dead_cat_sold = true;
            dead_cat_integrity &= instruction.integrity_valid;
        }
    }
    let sibling_identified = row.buy_authorization.map_or_else(
        || {
            row.first_sell_token
                .as_ref()
                .zip(row.sibling_token.as_ref())
                .is_some_and(|(first, sibling)| first != sibling)
        },
        |authorization| {
            row.sibling_token.as_deref()
                == Some(authorization.selected_token().to_string().as_str())
        },
    );
    let sold_correct = row
        .buy_authorization
        .map_or(first_correct, |authorization| {
            outcome.map(|side| (side == "UP") == authorization.selected_up)
        });
    CohortComparison {
        prediction,
        outcome,
        first_correct,
        dead_cat_sold,
        dead_cat_winner: if dead_cat_sold && dead_cat_integrity && sibling_identified {
            sold_correct
        } else {
            None
        },
    }
}

pub(super) fn dead_cat_receipt_value(
    row: &CanonicalCtfExitCohortRow,
    cashflow: &CanonicalCohortCashflow,
    dead_cat: &str,
    winner: bool,
    outcome: &str,
) -> Option<i128> {
    if cashflow.condition_id != row.market_id {
        return None;
    }
    let winning_token = match outcome {
        "UP" => &cashflow.up_token,
        "DOWN" => &cashflow.down_token,
        _ => return None,
    };
    let sold_token = row.sibling_token.as_ref()?;
    if (sold_token == winning_token) != winner
        || (sold_token != &cashflow.up_token && sold_token != &cashflow.down_token)
    {
        return None;
    }
    let receipts = cashflow
        .managed_sell_receipts
        .iter()
        .map(|receipt| (receipt.receipt_id.as_str(), receipt))
        .collect::<std::collections::BTreeMap<_, _>>();
    if receipts.len() != cashflow.managed_sell_receipts.len() {
        return None;
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut value = 0_i128;
    for instruction in row
        .post_instructions
        .iter()
        .flatten()
        .filter(|instruction| instruction.decision_reason == dead_cat)
    {
        if !instruction.integrity_valid {
            return None;
        }
        for fill in &instruction.confirmed_fills {
            let receipt = receipts.get(fill.receipt_id.as_str())?;
            if !seen.insert(fill.receipt_id.as_str())
                || receipt.fill_id != fill.fill_id
                || receipt.intent_id != instruction.execution_intent_id
                || instruction.order_id.as_deref() != Some(receipt.order_id.as_str())
                || receipt.token != *sold_token
                || receipt.qty_units_6dp != fill.quantity_units_6dp
                || receipt.qty_units_6dp == 0
            {
                return None;
            }
            let net = receipt
                .gross_proceeds_units_6dp
                .checked_sub(i128::try_from(receipt.fee_units_6dp).ok()?)?
                .checked_sub(i128::try_from(receipt.approved_direct_cost_units_6dp).ok()?)?;
            let amount = if winner {
                i128::try_from(receipt.qty_units_6dp)
                    .ok()?
                    .checked_sub(net)?
            } else {
                net
            };
            value = value.checked_add(amount)?;
        }
    }
    (!seen.is_empty()).then_some(value)
}

fn comparison_cells(row: &CanonicalCtfExitCohortRow) -> [String; 5] {
    let comparison = comparison(row);
    let first_sale = if row.first_confirmed_fill_quantity_units_6dp > 0 {
        let side = match comparison.prediction {
            Some("UP") => "DOWN",
            Some("DOWN") => "UP",
            _ => "UNKNOWN",
        };
        format!(
            "{side} / {}",
            row.first_sell_token.as_deref().unwrap_or("UNKNOWN")
        )
    } else {
        "NONE".to_string()
    };
    [
        display_timing(
            row.first_authorized_at_ms.flatten(),
            row.market_window_start_ms.flatten(),
            row.split_ts_ms,
        ),
        first_sale,
        format!(
            "{} / {} / {}",
            comparison.outcome.unwrap_or("UNKNOWN"),
            display_optional_timestamp(row.venue_resolution_observed_at_ms.flatten()),
            row.venue_resolution_provenance
                .as_ref()
                .and_then(Option::as_deref)
                .unwrap_or("UNKNOWN"),
        ),
        match comparison.first_correct {
            Some(true) => "CORRECT",
            Some(false) => "INCORRECT",
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
}

fn display_optional_money_units_6dp(value: Option<i128>) -> String {
    value.map_or_else(|| "UNAVAILABLE".to_string(), display_money_units_6dp)
}

fn display_money_units_6dp(value: i128) -> String {
    match value.cmp(&0) {
        std::cmp::Ordering::Greater => {
            format!("+${}", display_units_6dp(value).trim_start_matches('+'))
        }
        std::cmp::Ordering::Less => {
            format!("-${}", display_units_6dp(value).trim_start_matches('-'))
        }
        std::cmp::Ordering::Equal => "$0.000000".to_string(),
    }
}

fn display_unsigned_money_units_6dp(value: u128) -> String {
    format!("${}", display_quantity_units_6dp(value))
}

fn display_timing(value: Option<u64>, market_open_ms: Option<u64>, split_ts_ms: u64) -> String {
    let Some(value) = value else {
        return "UNKNOWN".to_string();
    };
    let market_age = market_open_ms.map_or_else(
        || "UNKNOWN".to_string(),
        |market_open_ms| (i128::from(value) - i128::from(market_open_ms)).to_string(),
    );
    let elapsed_after_split = i128::from(value) - i128::from(split_ts_ms);
    format!("{value} / market_age_ms={market_age} / elapsed_after_split_ms={elapsed_after_split}")
}

fn display_optional_timestamp(value: Option<u64>) -> String {
    value.map_or_else(|| "UNKNOWN".to_string(), |value| value.to_string())
}

fn display_instructions(row: &CanonicalCtfExitCohortRow, first: bool) -> String {
    let (instructions, coverage_complete) = if first {
        (
            row.first_instructions.as_deref().unwrap_or_default(),
            row.first_instruction_evidence_complete.unwrap_or(false),
        )
    } else {
        (
            row.post_instructions.as_deref().unwrap_or_default(),
            row.post_instruction_evidence_complete.unwrap_or(false),
        )
    };
    let mut rows = instructions
        .iter()
        .map(|instruction| {
            let fills = if instruction.confirmed_fills.is_empty() {
                "none".to_string()
            } else {
                instruction
                    .confirmed_fills
                    .iter()
                    .map(|fill| {
                        format!(
                            "{} @ {} qty={} matched={}",
                            fill.receipt_id,
                            fill.fill_id,
                            display_quantity_units_6dp(fill.quantity_units_6dp),
                            if row.buy_authorization.is_none()
                                && row.market_window_start_ms.is_some()
                            {
                                display_timing(
                                    fill.matched_at_ms,
                                    row.market_window_start_ms.flatten(),
                                    row.split_ts_ms,
                                )
                            } else {
                                display_optional_timestamp(fill.matched_at_ms)
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let economics = if first {
                instruction.first_economics.as_ref().map_or_else(
                    String::new,
                    |economics| {
                        format!(
                            " first_economics=[limit={} book_revision={} depth_gross={} depth_fee={} depth_net={} authorization_fee={} authorization_net={} cushion={} allocated_basis={} upper_valuation={} reservation={} mandatory_urgency={}]",
                            display_f64_bits(economics.instruction_limit_px_bits),
                            economics.execution_evidence_revision,
                            display_f64_bits(economics.depth_gross_proceeds_bits),
                            display_f64_bits(economics.depth_fee_bits),
                            display_f64_bits(economics.depth_net_proceeds_bits),
                            display_f64_bits(economics.authorization_fee_bits),
                            display_f64_bits(economics.authorization_net_proceeds_bits),
                            display_f64_bits(economics.delay_cushion_bits),
                            display_unsigned_money_units_6dp(economics.allocated_basis_units_6dp),
                            economics
                                .upper_valuation_bits
                                .map(display_f64_bits)
                                .unwrap_or_else(|| "UNAVAILABLE".to_string()),
                            display_f64_bits(economics.reservation_bits),
                            economics.mandatory_urgency,
                        )
                    },
                )
            } else {
                String::new()
            };
            let realization = if accounting::comparison_fields_present(row) {
                format!(
                    " realization=[{}]",
                    display_leg_realization(Some(instruction.confirmed_realization)),
                )
            } else {
                String::new()
            };
            format!(
                "intent={} order={} accepted={} reason={} qty={} terminal={}/{} fills=[{}] evidence={}{}{}",
                instruction.execution_intent_id,
                instruction.order_id.as_deref().unwrap_or("NONE"),
                instruction.accepted,
                instruction.decision_reason,
                display_quantity_units_6dp(instruction.instruction_quantity_units_6dp),
                display_optional_quantity(instruction.terminal_matched_quantity_units_6dp),
                display_optional_quantity(instruction.terminal_unfilled_quantity_units_6dp),
                fills,
                if !instruction.integrity_valid {
                    "INTEGRITY_ERROR"
                } else if instruction.evidence_complete {
                    "COMPLETE"
                } else {
                    "UNKNOWN"
                },
                economics,
                realization,
            )
        })
        .collect::<Vec<_>>();
    if !coverage_complete {
        rows.push("coverage=UNKNOWN".to_string());
    }
    if rows.is_empty() {
        "NA".to_string()
    } else {
        rows.join("; ")
    }
}

fn render_recorded_market_coverage(
    body: &mut String,
    coverage: Option<&RecordedMarketCoverageReport>,
) {
    let Some(coverage) = coverage else {
        return;
    };
    section(body, "Recorded Market Coverage");
    let mut summary_rows = coverage
        .by_symbol
        .iter()
        .map(|row| {
            vec![
                row.symbol.clone(),
                format!("{} / {}", row.entered_markets, row.expected_markets),
                row.resolution_backed_eligible_markets.to_string(),
                row.missing_resolution_markets.to_string(),
                row.missing_entry_markets.to_string(),
                row.boundary_excluded_markets.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    summary_rows.push(vec![
        "TOTAL".to_string(),
        format!(
            "{} / {}",
            coverage.entered_markets, coverage.expected_markets
        ),
        coverage.resolution_backed_eligible_markets.to_string(),
        coverage.missing_resolution_markets.to_string(),
        coverage.missing_entry_markets.to_string(),
        coverage.boundary_excluded_market_count.to_string(),
    ]);
    table(
        body,
        &[
            "symbol",
            "entered / expected",
            "resolution-backed eligible",
            "missing resolution",
            "missing entry",
            "boundary excluded",
        ],
        summary_rows,
    );

    section(body, "Missing Recorded Markets");
    table(
        body,
        &[
            "symbol",
            "market_id",
            "window_start_utc",
            "window_end_utc",
            "recorded disposition",
            "report disposition",
        ],
        coverage
            .missing_markets
            .iter()
            .map(|record| {
                vec![
                    record.market.symbol.clone(),
                    record.market.market_id.clone(),
                    display_utc_ms(record.market.window_start_ms),
                    display_utc_ms(record.market.window_end_ms),
                    record.market.disposition.label().to_string(),
                    record.report_disposition.label().to_string(),
                ]
            })
            .collect(),
    );

    section(body, "Boundary-Excluded Recorded Markets");
    table(
        body,
        &[
            "symbol",
            "market_id",
            "window_start_utc",
            "window_end_utc",
            "recorded disposition",
        ],
        coverage
            .boundary_excluded_markets
            .iter()
            .map(|record| {
                vec![
                    record.symbol.clone(),
                    record.market_id.clone(),
                    display_utc_ms(record.window_start_ms),
                    display_utc_ms(record.window_end_ms),
                    record.disposition.label().to_string(),
                ]
            })
            .collect(),
    );
}

fn display_utc_ms(timestamp_ms: u64) -> String {
    i64::try_from(timestamp_ms)
        .ok()
        .and_then(|timestamp_ms| Utc.timestamp_millis_opt(timestamp_ms).single())
        .map(|timestamp| timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
        .unwrap_or_else(|| format!("INVALID_TIMESTAMP_MS({timestamp_ms})"))
}

fn display_f64_bits(bits: u64) -> String {
    let value = f64::from_bits(bits);
    if value.is_finite() {
        format!("{value:.8}")
    } else {
        format!("NON_FINITE(0x{bits:016x})")
    }
}

fn display_optional_quantity(value: Option<u128>) -> String {
    value.map_or_else(|| "UNKNOWN".to_string(), display_quantity_units_6dp)
}

fn display_leg_realization(value: Option<CanonicalCtfLegRealization>) -> String {
    let Some(value) = value else {
        return "NA".to_string();
    };
    format!(
        "{} / {} / {} / {} / {}",
        display_money_units_6dp(value.confirmed_proceeds_units_6dp),
        display_unsigned_money_units_6dp(value.allocated_principal_units_6dp),
        display_unsigned_money_units_6dp(value.attributable_fee_units_6dp),
        display_unsigned_money_units_6dp(value.attributable_approved_direct_cost_units_6dp,),
        display_money_units_6dp(value.realized_pnl_units_6dp),
    )
}

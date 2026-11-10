use super::*;

pub(super) fn intent_ids(history: &[&CanonicalOwnedLegOutcome]) -> BTreeSet<u64> {
    history
        .iter()
        .filter_map(|outcome| outcome.execution_intent_id)
        .collect()
}

pub(super) fn terminal_operation(
    cashflow: &CanonicalCohortCashflow,
) -> Result<Option<CtfTerminalOperation>, String> {
    let operation = cashflow
        .terminal_receipts
        .first()
        .map(|item| item.operation);
    if cashflow
        .terminal_receipts
        .iter()
        .any(|item| Some(item.operation) != operation)
    {
        return Err("mixed_terminal_operations".to_string());
    }
    Ok(operation)
}

pub(super) fn count_orders(
    cashflow: &CanonicalCohortCashflow,
    intent_ids: &BTreeSet<u64>,
) -> Result<u64, String> {
    u64::try_from(
        cashflow
            .order_submissions
            .iter()
            .filter(|order| intent_ids.contains(&order.intent_id))
            .count(),
    )
    .map_err(|_| "submitted_order_count_overflow".to_string())
}

pub(super) fn receipt_totals(
    receipts: &[&CanonicalCohortSellReceipt],
) -> Result<(u64, u128, i128, Option<u64>, Option<u64>), String> {
    let count = u64::try_from(receipts.len())
        .map_err(|_| "managed_sell_receipt_count_overflow".to_string())?;
    receipts.iter().try_fold(
        (count, 0_u128, 0_i128, None, None),
        |(count, quantity, proceeds, earliest, latest), receipt| {
            let confirmed_at_ms = receipt.confirmed_at_ms;
            Ok((
                count,
                quantity
                    .checked_add(receipt.qty_units_6dp)
                    .ok_or_else(|| "sell_quantity_overflow".to_string())?,
                proceeds
                    .checked_add(receipt.gross_proceeds_units_6dp)
                    .ok_or_else(|| "sell_proceeds_overflow".to_string())?,
                Some(earliest.map_or(confirmed_at_ms, |value: u64| value.min(confirmed_at_ms))),
                Some(latest.map_or(confirmed_at_ms, |value: u64| value.max(confirmed_at_ms))),
            ))
        },
    )
}

pub(super) fn sell_realization(
    receipts: &[&CanonicalCohortSellReceipt],
) -> Result<CanonicalCtfLegRealization, String> {
    receipts
        .iter()
        .try_fold(CanonicalCtfLegRealization::ZERO, |sum, receipt| {
            sum.checked_add(receipt.realization)
                .ok_or_else(|| "sell_realization_overflow".to_string())
        })
}

pub(super) fn terminal_realization(
    cashflow: &CanonicalCohortCashflow,
    token: &str,
) -> Result<CanonicalCtfLegRealization, String> {
    cashflow
        .terminal_receipts
        .iter()
        .filter_map(|receipt| {
            if token == cashflow.up_token {
                receipt.up_realization
            } else if token == cashflow.down_token {
                receipt.down_realization
            } else {
                None
            }
        })
        .try_fold(CanonicalCtfLegRealization::ZERO, |sum, realization| {
            sum.checked_add(realization)
                .ok_or_else(|| "terminal_realization_overflow".to_string())
        })
}
pub(super) fn terminal_totals(
    cashflow: &CanonicalCohortCashflow,
) -> Result<(u64, u128, u128, i128), String> {
    let count = u64::try_from(cashflow.terminal_receipts.len())
        .map_err(|_| "terminal_receipt_count_overflow".to_string())?;
    cashflow.terminal_receipts.iter().try_fold(
        (count, 0_u128, 0_u128, 0_i128),
        |(count, up, down, value), receipt| {
            Ok((
                count,
                up.checked_add(receipt.up_terminal_qty_units_6dp)
                    .ok_or_else(|| "terminal_up_quantity_overflow".to_string())?,
                down.checked_add(receipt.down_terminal_qty_units_6dp)
                    .ok_or_else(|| "terminal_down_quantity_overflow".to_string())?,
                value
                    .checked_add(receipt.collateral_receipt_units_6dp)
                    .ok_or_else(|| "terminal_receipt_value_overflow".to_string())?,
            ))
        },
    )
}

/// Exit class is owned by the strategy decision, never by settlement completion:
/// the first managed-sell leg says whether the mandatory execution happened, and the
/// sticky sibling disposition says how the remaining leg was decided. Terminal receipts
/// stay an accounting fact and only guard invalid combinations.
pub(super) fn classify(
    first_fill_count: u64,
    post_fill_count: u64,
    terminal_operation: Option<CtfTerminalOperation>,
) -> Result<CanonicalCtfExitClass, String> {
    if first_fill_count > 0 && terminal_operation == Some(CtfTerminalOperation::Merge) {
        return Err("managed_sell_fill_followed_by_merge".to_string());
    }
    Ok(match (first_fill_count > 0, post_fill_count > 0) {
        (false, _) => CanonicalCtfExitClass::SettlementOnlyFailedMandatoryExecution,
        (true, true) => CanonicalCtfExitClass::FirstToPostProceeds,
        (true, false) => CanonicalCtfExitClass::FirstToSettlement,
    })
}

pub(super) fn project_instructions(
    history: &[&CanonicalOwnedLegOutcome],
    lineage: &[&CanonicalOrderExecutionLineageRow],
    cashflow: &CanonicalCohortCashflow,
    receipt_close_reason: &str,
) -> Result<(Vec<CanonicalCtfExitInstructionRow>, bool), String> {
    let mut expected = BTreeMap::new();
    let mut duplicate_intents = BTreeSet::new();
    let mut coverage_complete = true;
    for outcome in history {
        if outcome.action_qty_units_6dp == 0 {
            continue;
        }
        let Some(intent_id) = outcome.execution_intent_id else {
            coverage_complete = false;
            continue;
        };
        if expected.insert(intent_id, *outcome).is_some() {
            duplicate_intents.insert(intent_id);
        }
    }

    let mut lineage_by_intent = BTreeMap::new();
    for &row in lineage {
        if expected.contains_key(&row.terminal.execution_intent_id) {
            lineage_by_intent
                .entry(row.terminal.execution_intent_id)
                .or_insert_with(Vec::new)
                .push(row);
        }
    }
    let mut submissions_by_intent = BTreeMap::new();
    for submission in &cashflow.order_submissions {
        if expected.contains_key(&submission.intent_id) {
            submissions_by_intent
                .entry(submission.intent_id)
                .or_insert_with(Vec::new)
                .push(submission);
        }
    }
    let mut receipts_by_intent = BTreeMap::new();
    for receipt in &cashflow.managed_sell_receipts {
        if expected.contains_key(&receipt.intent_id) {
            receipts_by_intent
                .entry(receipt.intent_id)
                .or_insert_with(Vec::new)
                .push(receipt);
        }
    }
    let mut confirmed_events_by_receipt = BTreeMap::new();
    for event in &cashflow.events {
        if let CanonicalCtfEvent::TradeConfirmed(sell) = &event.event {
            confirmed_events_by_receipt
                .entry(sell.receipt_id.as_str())
                .or_insert_with(Vec::new)
                .push(sell);
        }
    }

    let mut rows = Vec::with_capacity(expected.len());
    for (intent_id, outcome) in &expected {
        let lineages = lineage_by_intent.remove(intent_id).unwrap_or_default();
        let submissions = submissions_by_intent.remove(intent_id).unwrap_or_default();
        let receipts = receipts_by_intent.remove(intent_id).unwrap_or_default();
        let terminal_lineage = (lineages.len() == 1).then(|| lineages[0]);
        let accepted_submission = submissions
            .first()
            .copied()
            .or_else(|| terminal_lineage.and_then(|row| row.accepted_submission.as_ref()));
        let mut integrity_valid =
            !duplicate_intents.contains(intent_id) && lineages.len() <= 1 && submissions.len() <= 1;
        let mut receipt_evidence_complete = true;
        let mut confirmed_quantity = 0_u128;
        let mut confirmed_realization = CanonicalCtfLegRealization::ZERO;
        let mut confirmed_fills = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            confirmed_quantity = confirmed_quantity
                .checked_add(receipt.qty_units_6dp)
                .ok_or_else(|| "instruction_confirmed_quantity_overflow".to_string())?;
            confirmed_realization = confirmed_realization
                .checked_add(receipt.realization)
                .ok_or_else(|| "instruction_realization_overflow".to_string())?;
            integrity_valid &= receipt.close_reason == receipt_close_reason;
            if let Some(accepted) = accepted_submission {
                integrity_valid &= accepted.order_id == receipt.order_id;
            } else {
                receipt_evidence_complete = false;
            }
            if let Some(row) = terminal_lineage {
                integrity_valid &=
                    row.terminal.order_id.as_deref() == Some(receipt.order_id.as_str());
            }
            let event = match confirmed_events_by_receipt.get(receipt.receipt_id.as_str()) {
                Some(events) if events.len() == 1 => Some(events[0]),
                Some(_) => {
                    integrity_valid = false;
                    None
                }
                None => {
                    receipt_evidence_complete = false;
                    None
                }
            };
            if let Some(sell) = event {
                integrity_valid &= sell.fill_id == receipt.fill_id
                    && sell.intent_id == *intent_id
                    && sell.order_id == receipt.order_id
                    && sell.qty_units_6dp == receipt.qty_units_6dp;
                receipt_evidence_complete &= sell.matched_trade.is_some();
            }
            confirmed_fills.push(CanonicalCtfExitConfirmedFill {
                receipt_id: receipt.receipt_id.clone(),
                fill_id: receipt.fill_id.clone(),
                quantity_units_6dp: receipt.qty_units_6dp,
                matched_at_ms: event.and_then(|sell| {
                    sell.matched_trade
                        .as_ref()
                        .map(|matched| matched.matched_at_ms)
                }),
            });
        }

        if let Some(row) = terminal_lineage {
            integrity_valid &= row.reducer_owned_intent.ledger_seq == outcome.ledger_seq
                && row.terminal.instruction_qty_units_6dp == outcome.action_qty_units_6dp
                && row
                    .terminal
                    .matched_qty_units_6dp
                    .checked_add(row.terminal.unfilled_qty_units_6dp)
                    == Some(row.terminal.instruction_qty_units_6dp)
                && confirmed_quantity <= row.terminal.matched_qty_units_6dp;
            if let Some(accepted) = accepted_submission {
                integrity_valid &= accepted.instruction_qty_units_6dp
                    == outcome.action_qty_units_6dp
                    && row.terminal.order_id.as_deref() == Some(accepted.order_id.as_str());
                if let Some(economics) = outcome.first_instruction_economics.as_ref() {
                    integrity_valid &= crate::polymarket::fixed_math::price_to_units_8dp(
                        f64::from_bits(economics.instruction_limit_px_bits),
                    ) == Some(accepted.instruction_limit_px_units_8dp);
                }
            }
            if let (Some(cashflow_submission), Some(lineage_submission)) =
                (submissions.first(), row.accepted_submission.as_ref())
            {
                integrity_valid &= *cashflow_submission == lineage_submission;
            }
        } else if let Some(accepted) = accepted_submission {
            integrity_valid &= accepted.instruction_qty_units_6dp == outcome.action_qty_units_6dp;
            if let Some(economics) = outcome.first_instruction_economics.as_ref() {
                integrity_valid &= crate::polymarket::fixed_math::price_to_units_8dp(
                    f64::from_bits(economics.instruction_limit_px_bits),
                ) == Some(accepted.instruction_limit_px_units_8dp);
            }
        }
        if let Some(economics) = outcome.first_instruction_economics.as_ref() {
            let scalars = [
                economics.depth_gross_proceeds_bits,
                economics.depth_fee_bits,
                economics.depth_net_proceeds_bits,
                economics.authorization_fee_bits,
                economics.authorization_net_proceeds_bits,
                economics.delay_cushion_bits,
                economics.reservation_bits,
            ];
            let instruction_limit_px = f64::from_bits(economics.instruction_limit_px_bits);
            integrity_valid &= receipt_close_reason == "paired_first_managed_sell"
                && outcome.execution_evidence_revision
                    == Some(economics.execution_evidence_revision)
                && instruction_limit_px.is_finite()
                && instruction_limit_px > 0.0
                && scalars.into_iter().all(|bits| {
                    let value = f64::from_bits(bits);
                    value.is_finite() && value >= 0.0
                })
                && economics.upper_valuation_bits.is_none_or(|bits| {
                    let value = f64::from_bits(bits);
                    value.is_finite() && value >= 0.0
                });
        }
        let evidence_complete = integrity_valid
            && terminal_lineage.is_some()
            && receipt_evidence_complete
            && terminal_lineage
                .is_some_and(|row| row.terminal.matched_qty_units_6dp == confirmed_quantity);
        coverage_complete &= evidence_complete;
        rows.push(CanonicalCtfExitInstructionRow {
            execution_intent_id: *intent_id,
            order_id: accepted_submission
                .map(|accepted| accepted.order_id.clone())
                .or_else(|| terminal_lineage.and_then(|row| row.terminal.order_id.clone())),
            accepted: accepted_submission.is_some(),
            decision_reason: format!("{:?}", outcome.reason),
            instruction_quantity_units_6dp: outcome.action_qty_units_6dp,
            first_economics: outcome.first_instruction_economics.clone(),
            terminal_matched_quantity_units_6dp: terminal_lineage
                .map(|row| row.terminal.matched_qty_units_6dp),
            terminal_unfilled_quantity_units_6dp: terminal_lineage
                .map(|row| row.terminal.unfilled_qty_units_6dp),
            confirmed_fills,
            confirmed_realization,
            evidence_complete,
            integrity_valid,
        });
    }
    Ok((rows, coverage_complete))
}

pub(super) fn first_deadline_status(
    instructions: &[CanonicalCtfExitInstructionRow],
    coverage_complete: bool,
    target: u128,
    deadline_ms: Option<u64>,
) -> Result<(Option<u64>, &'static str), String> {
    if !coverage_complete || instructions.iter().any(|row| !row.evidence_complete) {
        return Ok((None, "UNKNOWN"));
    }
    let mut fills = Vec::new();
    let mut total = 0_u128;
    for fill in instructions.iter().flat_map(|row| &row.confirmed_fills) {
        total = total
            .checked_add(fill.quantity_units_6dp)
            .ok_or_else(|| "first_confirmed_quantity_overflow".to_string())?;
        let Some(matched_at_ms) = fill.matched_at_ms else {
            return Ok((None, "UNKNOWN"));
        };
        fills.push((
            matched_at_ms,
            fill.fill_id.as_str(),
            fill.quantity_units_6dp,
        ));
    }
    let Some(deadline_ms) = deadline_ms else {
        return Ok((None, "UNKNOWN"));
    };
    if total < target {
        return Ok((None, "FAILED"));
    }
    if total != target {
        return Ok((None, "UNKNOWN"));
    }
    fills.sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    let mut cumulative = 0_u128;
    for (matched_at_ms, _, quantity) in fills {
        cumulative = cumulative
            .checked_add(quantity)
            .ok_or_else(|| "first_match_quantity_overflow".to_string())?;
        if cumulative == target {
            return Ok((
                Some(matched_at_ms),
                if matched_at_ms < deadline_ms {
                    "SUCCESS"
                } else {
                    "FAILED"
                },
            ));
        }
    }
    Ok((None, "UNKNOWN"))
}

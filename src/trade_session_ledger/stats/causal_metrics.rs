use std::collections::BTreeMap;

use super::{CanonicalOwnedLegOutcome, CanonicalSessionStats};
use crate::oracle_resolution::{canonical_chainlink_symbol, OracleResolvedLabel};
use crate::strategy::facts::{CtfLegDecisionReason, CtfLegEconomicOutcome, CtfOwnedLegCloseReason};
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::trade_session_ledger::{CanonicalCtfEvent, TradeLedgerRecord};

pub(super) fn populate_causal_metrics(
    stats: &mut CanonicalSessionStats,
    records: &[TradeLedgerRecord],
    outcomes: &[CanonicalOwnedLegOutcome],
) -> BTreeMap<String, OracleResolvedLabel> {
    let mut oracle_label_by_condition = BTreeMap::new();
    for record in records {
        let Some(canonical) = record.oracle_resolved_label.as_ref() else {
            continue;
        };
        if let Err(error) = super::super::oracle_labels::validate_record(record) {
            stats.violations.push(format!(
                "invalid_oracle_resolved_label:condition_id={}:{}",
                canonical.condition_id, error
            ));
            continue;
        }
        let label = match canonical.validated_runtime() {
            Ok(label) => label,
            Err(error) => {
                stats.violations.push(format!(
                    "invalid_oracle_resolved_label:condition_id={}:{}",
                    canonical.condition_id, error
                ));
                continue;
            }
        };
        if oracle_label_by_condition
            .insert(label.condition_id.clone(), label)
            .is_some()
        {
            stats.violations.push(format!(
                "duplicate_oracle_resolved_label:condition_id={}",
                canonical.condition_id
            ));
        }
    }
    let splits = records
        .iter()
        .filter_map(|record| match record.ctf_event.as_ref() {
            Some(CanonicalCtfEvent::SplitConfirmed(split)) => Some((record, split)),
            _ => None,
        });
    let mut resolved = 0_u64;
    let mut emitted = 0_u64;
    let mut correct = 0_u64;
    let mut abstained = 0_u64;
    let mut first_sells_on_eventual_oracle_winner = 0_u64;
    for (split_record, split) in splits {
        let Some(label) = oracle_label_by_condition.get(&split.condition_id) else {
            continue;
        };
        let expected_oracle_symbol = match canonical_chainlink_symbol(&split.underlying) {
            Ok(symbol) => symbol,
            Err(error) => {
                stats.violations.push(format!(
                    "invalid_causal_split_symbol:cohort={}:{}",
                    split.cohort_id, error
                ));
                continue;
            }
        };
        if label.market_id != split.market_id
            || label.condition_id != split.condition_id
            || label.symbol != expected_oracle_symbol
        {
            stats.violations.push(format!(
                "oracle_label_split_identity_mismatch:cohort={}",
                split.cohort_id
            ));
            continue;
        }
        let winner = label.winner;
        if split_record.ts_ms >= label.release_ts_ms {
            stats.violations.push(format!(
                "confirmed_split_is_not_causal:cohort={}",
                split.cohort_id
            ));
            continue;
        }
        if !increment(&mut resolved) {
            stats
                .violations
                .push("resolved_decision_count_overflow".to_string());
            return oracle_label_by_condition;
        }

        let mut first_authorization = None;
        let mut latest_up = None;
        let mut latest_down = None;
        for outcome in outcomes.iter().filter(|outcome| {
            outcome.cohort_id == split.cohort_id
                && outcome.symbol == split.underlying
                && outcome.market_id == split.market_id
                && outcome.ts_ms >= split_record.ts_ms
                && outcome.ts_ms < label.release_ts_ms
        }) {
            if outcome.token != split.up_token && outcome.token != split.down_token {
                stats.violations.push(format!(
                    "causal_outcome_token_is_foreign:cohort={}:token={}",
                    split.cohort_id, outcome.token
                ));
                continue;
            }
            let latest = if outcome.token == split.up_token {
                &mut latest_up
            } else {
                &mut latest_down
            };
            if latest.is_none_or(|prior: &CanonicalOwnedLegOutcome| {
                (outcome.ts_ms, outcome.ledger_seq) > (prior.ts_ms, prior.ledger_seq)
            }) {
                *latest = Some(outcome);
            }
            if outcome.outcome == CtfLegEconomicOutcome::SellNow
                && outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
                && outcome.action_qty_units_6dp > 0
                && first_authorization.is_none_or(|prior: &CanonicalOwnedLegOutcome| {
                    (outcome.ts_ms, outcome.ledger_seq) < (prior.ts_ms, prior.ledger_seq)
                })
            {
                first_authorization = Some(outcome);
            }
        }

        let causal_side = if let Some(first_authorization) = first_authorization {
            let authoritative_side = first_authorization.causal_winner_side;
            let changed_after_authorization = authoritative_side.is_some_and(|side| {
                outcomes.iter().any(|outcome| {
                    outcome.cohort_id == split.cohort_id
                        && outcome.symbol == split.underlying
                        && outcome.market_id == split.market_id
                        && outcome.ts_ms >= split_record.ts_ms
                        && outcome.ts_ms < label.release_ts_ms
                        && (outcome.token == split.up_token || outcome.token == split.down_token)
                        && (outcome.ts_ms, outcome.ledger_seq)
                            >= (first_authorization.ts_ms, first_authorization.ledger_seq)
                        && outcome
                            .causal_winner_side
                            .is_some_and(|next_side| next_side != side)
                })
            });
            if authoritative_side.is_none() || changed_after_authorization {
                stats.violations.push(format!(
                    "conflicting_causal_side:cohort={}",
                    split.cohort_id
                ));
                None
            } else {
                authoritative_side
            }
        } else {
            let exact_waiting_leg = |outcome: &CanonicalOwnedLegOutcome, expected_qty| {
                outcome.owned_qty_units_6dp == expected_qty
                    && outcome.action_qty_units_6dp == 0
                    && outcome.execution_intent_id.is_none()
                    && outcome.execution_evidence_revision.is_none()
                    && outcome.post_sell_full_entry_pnl_bits.is_none()
                    && outcome.post_hold_full_entry_pnl_bits.is_none()
                    && outcome.post_fillable_qty_bits.is_none()
                    && outcome.outcome == CtfLegEconomicOutcome::HoldForSettlement
                    && outcome.close_reason.is_none()
            };
            match (latest_up, latest_down) {
                (Some(up), Some(down))
                    if up.ts_ms == down.ts_ms
                        && exact_waiting_leg(up, split.up_qty_units_6dp)
                        && exact_waiting_leg(down, split.down_qty_units_6dp) =>
                {
                    match (up.causal_winner_side, down.causal_winner_side) {
                        (Some(up_side), Some(down_side))
                            if up_side == down_side
                                && up.reason != CtfLegDecisionReason::CtfFirstSellNoSide
                                && down.reason != CtfLegDecisionReason::CtfFirstSellNoSide =>
                        {
                            Some(up_side)
                        }
                        (None, None)
                            if up.reason == CtfLegDecisionReason::CtfFirstSellNoSide
                                && down.reason == CtfLegDecisionReason::CtfFirstSellNoSide =>
                        {
                            if !increment(&mut abstained) {
                                stats
                                    .violations
                                    .push("abstained_decision_count_overflow".to_string());
                                return oracle_label_by_condition;
                            }
                            None
                        }
                        _ => {
                            stats.violations.push(format!(
                                "invalid_causal_waiting_state:cohort={}",
                                split.cohort_id
                            ));
                            None
                        }
                    }
                }
                _ => {
                    stats.violations.push(format!(
                        "invalid_causal_waiting_state:cohort={}",
                        split.cohort_id
                    ));
                    None
                }
            }
        };
        if let Some(side) = causal_side {
            if !increment(&mut emitted) {
                stats
                    .violations
                    .push("emitted_side_decision_count_overflow".to_string());
                return oracle_label_by_condition;
            }
            if side == winner && !increment(&mut correct) {
                stats
                    .violations
                    .push("correct_side_decision_count_overflow".to_string());
                return oracle_label_by_condition;
            }
        }

        let first_sell = records
            .iter()
            .filter_map(|record| match record.ctf_event.as_ref() {
                Some(CanonicalCtfEvent::TradeConfirmed(sell))
                    if sell.cohort_id == split.cohort_id
                        && sell.close_reason == "paired_first_managed_sell" =>
                {
                    Some((record.ledger_seq, sell))
                }
                _ => None,
            })
            .min_by(|(left_seq, left), (right_seq, right)| {
                (left.confirmed_at_ms, *left_seq, left.fill_id.as_str()).cmp(&(
                    right.confirmed_at_ms,
                    *right_seq,
                    right.fill_id.as_str(),
                ))
            });
        if first_sell.is_some_and(|(_, sell)| {
            (winner == PairTokenSide::Up && sell.token == split.up_token)
                || (winner == PairTokenSide::Down && sell.token == split.down_token)
        }) && !increment(&mut first_sells_on_eventual_oracle_winner)
        {
            stats
                .violations
                .push("wrong_side_first_sell_count_overflow".to_string());
            return oracle_label_by_condition;
        }
    }
    if resolved == 0 {
        return oracle_label_by_condition;
    }
    stats.resolved_decisions = Some(resolved);
    stats.emitted_side_decisions = Some(emitted);
    stats.emitted_side_correct = Some(correct);
    stats.abstained_decisions = Some(abstained);
    stats.wrong_side_first_sells = Some(first_sells_on_eventual_oracle_winner);
    oracle_label_by_condition
}

fn increment(value: &mut u64) -> bool {
    let Some(next) = value.checked_add(1) else {
        return false;
    };
    *value = next;
    true
}

use std::collections::{BTreeMap, BTreeSet};

use crate::strategy::facts::{CtfLegDecisionReason, CtfLegEconomicOutcome, CtfOwnedLegCloseReason};

use super::{CanonicalOwnedLegOutcome, ManagedSellReduction};

pub(super) fn validate(
    reductions: &BTreeMap<String, ManagedSellReduction>,
    invalid_reductions: &BTreeSet<String>,
    transitions: &[CanonicalOwnedLegOutcome],
    current: &BTreeMap<(String, String), (u64, CanonicalOwnedLegOutcome)>,
    violations: &mut Vec<String>,
) {
    for (cohort_id, reduction) in reductions {
        if invalid_reductions.contains(cohort_id) {
            continue;
        }
        let first_sells = transitions
            .iter()
            .filter(|outcome| {
                outcome.cohort_id == *cohort_id
                    && outcome.outcome == CtfLegEconomicOutcome::SellNow
                    && outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
            })
            .map(|outcome| outcome.token.as_str())
            .collect::<BTreeSet<_>>();
        if first_sells.len() > 1 {
            violations.push(format!(
                "ctf_cohort_multiple_first_managed_sell_tokens:cohort={cohort_id}:count={}",
                first_sells.len()
            ));
            continue;
        }
        let Ok(summary) = reduction.cashflow.summary() else {
            violations.push(format!(
                "current_owned_leg_receipt_summary_failed:cohort={cohort_id}"
            ));
            continue;
        };
        let remaining = summary
            .remaining_qty_by_token
            .iter()
            .filter(|(_, qty_units_6dp)| **qty_units_6dp > 0)
            .collect::<Vec<_>>();
        if let Some(authorization) = reduction.buy_authorization {
            let held_token = authorization.selected_token().to_string();
            let valid = first_sells.is_empty()
                && remaining
                    .iter()
                    .all(|(token, _)| token.as_str() == held_token)
                && match current.get(&(cohort_id.clone(), held_token.clone())) {
                    None => summary
                        .confirmed_sold_qty_by_token
                        .values()
                        .all(|qty| *qty == 0),
                    Some((_, outcome)) => {
                        outcome.token == held_token
                            && outcome.first_instruction_economics.is_none()
                            && match outcome.outcome {
                                CtfLegEconomicOutcome::HoldForSettlement => {
                                    outcome.close_reason.is_none()
                                        && outcome.reason
                                            != CtfLegDecisionReason::CtfFirstSellNoSide
                                }
                                CtfLegEconomicOutcome::SellNow => {
                                    outcome.close_reason
                                        == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
                                        && matches!(
                                            outcome.reason,
                                            CtfLegDecisionReason::DeadCat
                                                | CtfLegDecisionReason::ProfitNow
                                        )
                                }
                            }
                    }
                };
            if !valid {
                violations.push(format!(
                    "buy_cohort_invalid_owned_leg:cohort={cohort_id}:token={held_token}"
                ));
            }
            continue;
        }
        let valid = match first_sells.first() {
            None => {
                let no_side = remaining.len() == 2
                    && remaining.iter().all(|(token, qty)| {
                        current
                            .get(&(cohort_id.clone(), (*token).clone()))
                            .is_some_and(|(_, outcome)| {
                                outcome.owned_qty_units_6dp == **qty
                                    && outcome.action_qty_units_6dp == 0
                                    && outcome.outcome == CtfLegEconomicOutcome::HoldForSettlement
                                    && outcome.reason == CtfLegDecisionReason::CtfFirstSellNoSide
                                    && outcome.causal_winner_side.is_none()
                                    && outcome.close_reason.is_none()
                            })
                    });
                let floor_wait = if remaining.len() == 2 {
                    let shared_winner = remaining
                        .first()
                        .and_then(|(token, _)| current.get(&(cohort_id.clone(), (*token).clone())))
                        .and_then(|(_, outcome)| outcome.causal_winner_side);
                    let common = shared_winner.is_some()
                        && remaining.iter().all(|(token, qty)| {
                            current
                                .get(&(cohort_id.clone(), (*token).clone()))
                                .is_some_and(|(_, outcome)| {
                                    outcome.owned_qty_units_6dp == **qty
                                        && outcome.action_qty_units_6dp == 0
                                        && outcome.outcome
                                            == CtfLegEconomicOutcome::HoldForSettlement
                                        && outcome.causal_winner_side == shared_winner
                                        && outcome.reason
                                            != CtfLegDecisionReason::CtfFirstSellNoSide
                                        && outcome.close_reason.is_none()
                                })
                        });
                    common
                } else {
                    false
                };
                remaining.is_empty() || no_side || floor_wait
            }
            Some(first_token) => {
                let sell = |token: &str| {
                    transitions.iter().find(|outcome| {
                        outcome.cohort_id == *cohort_id
                            && outcome.token == token
                            && outcome.outcome == CtfLegEconomicOutcome::SellNow
                    })
                };
                let auth = transitions.iter().find(|outcome| {
                    outcome.cohort_id == *cohort_id
                        && outcome.token == **first_token
                        && outcome.close_reason
                            == Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
                });
                let winner = auth.and_then(|outcome| outcome.causal_winner_side);
                let stable = summary.remaining_qty_by_token.keys().all(|token| {
                    let Some(prior) = sell(token) else {
                        return token != *first_token;
                    };
                    let Some(created_qty_units_6dp) = summary.created_qty_by_token.get(token)
                    else {
                        return false;
                    };
                    let close = if token == *first_token {
                        CtfOwnedLegCloseReason::PairedFirstManagedSell
                    } else {
                        CtfOwnedLegCloseReason::PairedPostProceedsSell
                    };
                    prior.owned_qty_units_6dp == *created_qty_units_6dp
                        && transitions
                            .iter()
                            .filter(|next| {
                                next.cohort_id == *cohort_id
                                    && next.token == *token
                                    && next.ledger_seq >= prior.ledger_seq
                            })
                            .all(|next| {
                                next.outcome == CtfLegEconomicOutcome::SellNow
                                    && next.owned_qty_units_6dp == prior.owned_qty_units_6dp
                                    && next.close_reason == Some(close)
                                    && next.reason == prior.reason
                                    && next.causal_winner_side == winner
                                    && next.post_sell_full_entry_pnl_bits
                                        == prior.post_sell_full_entry_pnl_bits
                                    && next.post_hold_full_entry_pnl_bits
                                        == prior.post_hold_full_entry_pnl_bits
                                    && next.post_fillable_qty_bits == prior.post_fillable_qty_bits
                            })
                });
                winner.is_some()
                    && stable
                    && remaining.iter().all(|(token, qty)| {
                        let Some((_, outcome)) =
                            current.get(&(cohort_id.clone(), (*token).clone()))
                        else {
                            return false;
                        };
                        outcome.causal_winner_side == winner
                            && match sell(token) {
                                Some(prior) => {
                                    outcome.outcome == CtfLegEconomicOutcome::SellNow
                                        && outcome.owned_qty_units_6dp == prior.owned_qty_units_6dp
                                        && (outcome.action_qty_units_6dp == 0
                                            || if token.as_str() == *first_token {
                                                outcome.action_qty_units_6dp <= **qty
                                            } else {
                                                outcome.action_qty_units_6dp == **qty
                                            })
                                }
                                None => {
                                    token.as_str() != *first_token
                                        && outcome.owned_qty_units_6dp == **qty
                                        && outcome.outcome
                                            == CtfLegEconomicOutcome::HoldForSettlement
                                        && outcome.reason
                                            != CtfLegDecisionReason::CtfFirstSellNoSide
                                        && outcome.close_reason.is_none()
                                }
                            }
                    })
            }
        };
        if !valid {
            violations.push(format!(
                "ctf_cohort_invalid_unresolved_owned_legs:cohort={cohort_id}:first_sell_authorized={}:remaining_tokens={}",
                !first_sells.is_empty(),
                remaining.len(),
            ));
        }
    }
}

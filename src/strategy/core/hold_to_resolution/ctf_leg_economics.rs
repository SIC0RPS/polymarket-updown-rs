use super::{PairTokenSide, INVENTORY_EPS};
use crate::strategy::facts::{CtfLegDecisionReason, CtfPostSellEconomicsEvidence};
use crate::strategy::pair_lifecycle::CtfOwnedLegDisposition;
use crate::types::OrderCloseReason;
#[path = "ctf_leg_economics/candidates.rs"]
mod candidates;
#[path = "ctf_leg_economics/joint_hold.rs"]
mod joint_hold;
#[path = "ctf_leg_economics/sell_selection.rs"]
mod sell_selection;
#[path = "ctf_leg_economics/types.rs"]
mod types;
#[path = "ctf_leg_economics/valuation.rs"]
mod valuation;
use joint_hold::{ctf_joint_entry_pnl_for_sells, ctf_joint_terminal_zero_pnl};
use sell_selection::{
    ctf_best_sibling_market_entry_sell, ctf_joint_sell_reason, ctf_leg_sell_surface_executable,
};
use types::CtfLegMarketEvaluation;
pub(super) use types::{
    CtfDeadCatInstruction, CtfEntryLedgerProjection, CtfMarketEntryLegDecision,
    CtfMarketEntryLegSnapshot,
};
use valuation::ctf_leg_market_evaluation;

pub(super) fn ctf_market_entry_decisions(
    legs: Vec<CtfMarketEntryLegSnapshot>,
    canonical_winner_side: Option<PairTokenSide>,
    first_execution_complete: bool,
    direct_buy_side: Option<PairTokenSide>,
) -> Vec<(PairTokenSide, CtfMarketEntryLegDecision)> {
    let quantum = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
    let mut evaluations = legs
        .iter()
        .filter_map(|leg| {
            leg.projection.map(|projection| {
                (
                    leg.token_side,
                    ctf_leg_market_evaluation(projection, quantum),
                )
            })
        })
        .collect::<Vec<_>>();
    let Some(exit_side) = direct_buy_side.or(canonical_winner_side) else {
        return legs
            .into_iter()
            .map(|leg| {
                let side = leg.token_side;
                (
                    side,
                    CtfMarketEntryLegDecision {
                        disposition: CtfOwnedLegDisposition::HoldForSettlement {
                            reason: CtfLegDecisionReason::CtfFirstSellNoSide,
                        },
                        fresh_first_instruction_authorized: false,
                        selected_dead_cat_instruction: None,
                    },
                )
            })
            .collect();
    };
    let first_side = canonical_winner_side
        .filter(|_| direct_buy_side.is_none())
        .map(PairTokenSide::opposite);
    let sibling_sell = (direct_buy_side.is_some() || first_execution_complete)
        .then(|| ctf_best_sibling_market_entry_sell(&legs, &mut evaluations, exit_side, quantum))
        .flatten();
    legs.into_iter()
        .map(|leg| {
            let side = leg.token_side;
            let decision = if Some(side) == first_side
                && leg.route_block_reason.is_none()
                && evaluations
                    .iter()
                    .find(|(candidate, _)| *candidate == side)
                    .is_some_and(|(_, evaluation)| {
                        evaluation.projection.first_sell_allowed
                            && ctf_leg_sell_surface_executable(evaluation, quantum)
                    }) {
                CtfMarketEntryLegDecision {
                    disposition: CtfOwnedLegDisposition::SellNow {
                        authorized_qty_units_6dp: leg.owned_qty_units_6dp,
                        close_reason: OrderCloseReason::PairedFirstManagedSell,
                        // First-leg: side identity only — not DeadCat / ProfitNow.
                        reason: ctf_joint_sell_reason(side),
                        post_sell_economics: None,
                    },
                    fresh_first_instruction_authorized: true,
                    selected_dead_cat_instruction: None,
                }
            } else if let Some((_, evidence, post_reason, selected_dead_cat_instruction)) =
                sibling_sell.filter(|(sell_side, _, _, _)| *sell_side == side)
            {
                CtfMarketEntryLegDecision {
                    disposition: CtfOwnedLegDisposition::SellNow {
                        authorized_qty_units_6dp: leg.owned_qty_units_6dp,
                        close_reason: OrderCloseReason::PairedPostProceedsSell,
                        reason: post_reason,
                        post_sell_economics: Some(evidence),
                    },
                    fresh_first_instruction_authorized: false,
                    selected_dead_cat_instruction,
                }
            } else {
                CtfMarketEntryLegDecision {
                    disposition: CtfOwnedLegDisposition::HoldForSettlement {
                        reason: leg
                            .route_block_reason
                            .unwrap_or(CtfLegDecisionReason::JointEntryHoldForSettlementSelected),
                    },
                    fresh_first_instruction_authorized: false,
                    selected_dead_cat_instruction: None,
                }
            };
            (side, decision)
        })
        .collect()
}

fn ctf_remaining_owned_value(
    up_qty: f64,
    down_qty: f64,
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
) -> f64 {
    if !(up_qty.is_finite() && down_qty.is_finite()) {
        return f64::NEG_INFINITY;
    }
    let complete_qty = up_qty.min(down_qty).max(0.0);
    let unmatched_up_qty = (up_qty - complete_qty).max(0.0);
    let unmatched_down_qty = (down_qty - complete_qty).max(0.0);
    if unmatched_up_qty <= INVENTORY_EPS && unmatched_down_qty <= INVENTORY_EPS {
        return complete_qty;
    }
    let Some(p_up) = ctf_joint_valuation_probability_up(evaluations) else {
        return f64::NEG_INFINITY;
    };
    complete_qty + unmatched_up_qty * p_up + unmatched_down_qty * (1.0 - p_up)
}

fn ctf_joint_valuation_probability_up(
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
) -> Option<f64> {
    let quantum = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
    let mut selected = None;
    for (_, evaluation) in evaluations {
        let p_up = evaluation
            .projection
            .p_fair
            .filter(|p| p.is_finite() && (0.0..=1.0).contains(p))?;
        if !(evaluation.projection.current_fair_value.is_finite()
            && evaluation.projection.current_fair_value >= 0.0
            && evaluation.projection.exit_fair_value.is_finite()
            && evaluation.projection.exit_fair_value >= 0.0)
        {
            return None;
        }
        if selected.is_some_and(|selected_p_up: f64| (selected_p_up - p_up).abs() > quantum) {
            return None;
        }
        selected = Some(p_up);
    }
    selected
}

fn ctf_owned_qty_for_side(legs: &[CtfMarketEntryLegSnapshot], side: PairTokenSide) -> f64 {
    legs.iter()
        .filter(|leg| leg.token_side == side)
        .map(|leg| leg.economic_owned_qty.max(0.0))
        .filter(|qty| qty.is_finite())
        .sum()
}

fn ctf_sell_qty_for_side(
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
    sell_sides: &[PairTokenSide],
    side: PairTokenSide,
) -> f64 {
    if !sell_sides.contains(&side) {
        return 0.0;
    }
    evaluations
        .iter()
        .find_map(|(candidate_side, evaluation)| {
            (*candidate_side == side).then_some(evaluation.projection.current_fillable_qty.max(0.0))
        })
        .unwrap_or(0.0)
}

fn ctf_sell_proceeds_for_side(
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
    sell_sides: &[PairTokenSide],
    side: PairTokenSide,
) -> f64 {
    if !sell_sides.contains(&side) {
        return 0.0;
    }
    evaluations
        .iter()
        .find_map(|(candidate_side, evaluation)| {
            (*candidate_side == side).then_some(
                evaluation
                    .projection
                    .current_authorization_net_proceeds
                    .max(0.0),
            )
        })
        .unwrap_or(0.0)
}

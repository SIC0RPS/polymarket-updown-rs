use super::*;

/// Owned BUY leg or post-First sibling: DeadCat, ProfitNow, or no sell (Hold).
/// Full-clear of remaining inventory is required for either sell class.
pub(super) fn ctf_best_sibling_market_entry_sell(
    legs: &[CtfMarketEntryLegSnapshot],
    evaluations: &mut [(PairTokenSide, CtfLegMarketEvaluation)],
    sibling_side: PairTokenSide,
    quantum: f64,
) -> Option<(
    PairTokenSide,
    CtfPostSellEconomicsEvidence,
    CtfLegDecisionReason,
    Option<CtfDeadCatInstruction>,
)> {
    let index = evaluations
        .iter()
        .position(|(side, _)| *side == sibling_side)?;
    let base = evaluations[index].1.projection;
    let owned_qty = ctf_owned_qty_for_side(legs, sibling_side);
    let sell_sides = [sibling_side];
    let instruction = base.dead_cat_instruction;
    let dead_cat_projection = instruction.map_or(base, |instruction| {
        base.with_dead_cat_instruction(instruction)
    });
    let mut base_sell_ev = None;

    // BUY protection is already checked against the current cash requirement.
    // CTF retains its existing opposing-oracle and reference qualification.
    let dead_cat = if dead_cat_projection
        .oracle_signal_side
        .is_some_and(|winner| winner != sibling_side)
        && dead_cat_projection.dead_cat_price_below_reference
    {
        evaluations[index].1.projection = dead_cat_projection;
        let feasible =
            ctf_post_proceeds_sell_surface_feasible(&evaluations[index].1, owned_qty, quantum);
        let sell_ev =
            feasible.then(|| ctf_joint_entry_pnl_for_sells(legs, evaluations, &sell_sides));
        let terminal_zero_ev = feasible
            .then(|| ctf_joint_terminal_zero_pnl(legs, evaluations, &[]))
            .flatten();
        evaluations[index].1.projection = base;
        if dead_cat_projection.limit_px.to_bits() == base.limit_px.to_bits() {
            base_sell_ev = sell_ev;
        }
        sell_ev
            .zip(terminal_zero_ev)
            .filter(|(sell_ev, terminal_zero_ev)| {
                sell_ev.is_finite()
                    && terminal_zero_ev.is_finite()
                    && *sell_ev > *terminal_zero_ev + quantum
            })
    } else {
        None
    };

    let (sell_ev, hold_baseline, reason, selected_instruction, fillable_qty) =
        if let Some((sell_ev, terminal_zero_ev)) = dead_cat {
            (
                sell_ev,
                terminal_zero_ev,
                CtfLegDecisionReason::DeadCat,
                instruction,
                dead_cat_projection.current_fillable_qty,
            )
        } else {
            if !ctf_post_proceeds_sell_surface_feasible(&evaluations[index].1, owned_qty, quantum) {
                return None;
            }
            let sell_ev = base_sell_ev
                .unwrap_or_else(|| ctf_joint_entry_pnl_for_sells(legs, evaluations, &sell_sides));
            if !sell_ev.is_finite() {
                return None;
            }
            // ProfitNow compares the original instruction's whole-cohort net profit
            // with unadjusted expected settlement profit at monetary precision.
            let settlement_ev = ctf_joint_entry_pnl_for_sells(legs, evaluations, &[]);
            if !(settlement_ev.is_finite()
                && sell_ev >= quantum
                && sell_ev + quantum >= settlement_ev)
            {
                return None;
            }
            (
                sell_ev,
                settlement_ev,
                CtfLegDecisionReason::ProfitNow,
                None,
                base.current_fillable_qty,
            )
        };

    Some((
        sibling_side,
        CtfPostSellEconomicsEvidence {
            sell_full_entry_pnl_bits: sell_ev.to_bits(),
            hold_full_entry_pnl_bits: hold_baseline.to_bits(),
            fillable_qty_bits: fillable_qty.to_bits(),
        },
        reason,
        selected_instruction,
    ))
}

/// Sibling surface only: executable and fully fillable for the owned quantity.
/// Economics (DeadCat / ProfitNow / hold) live in ctf_best_sibling_market_entry_sell.
fn ctf_post_proceeds_sell_surface_feasible(
    evaluation: &CtfLegMarketEvaluation,
    owned_qty: f64,
    quantum: f64,
) -> bool {
    if !ctf_leg_sell_surface_executable(evaluation, quantum) {
        return false;
    }
    owned_qty.is_finite()
        && owned_qty > INVENTORY_EPS
        && evaluation.projection.current_fillable_qty + quantum >= owned_qty
}

pub(super) fn ctf_leg_sell_surface_executable(
    evaluation: &CtfLegMarketEvaluation,
    quantum: f64,
) -> bool {
    super::super::ctf_sell_surface_executable(
        evaluation.projection.current_fillable_qty,
        evaluation.projection.current_net_proceeds,
        evaluation.projection.current_authorization_net_proceeds,
        quantum,
    )
}

/// First-leg only: side identity for mandatory loser sell (not DeadCat / ProfitNow).
pub(super) fn ctf_joint_sell_reason(side: PairTokenSide) -> CtfLegDecisionReason {
    match side {
        PairTokenSide::Up => CtfLegDecisionReason::JointEntrySellUpSelected,
        PairTokenSide::Down => CtfLegDecisionReason::JointEntrySellDownSelected,
    }
}

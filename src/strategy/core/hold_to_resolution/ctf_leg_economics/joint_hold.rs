use super::*;

pub(super) fn ctf_joint_entry_pnl_for_sells(
    legs: &[CtfMarketEntryLegSnapshot],
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
    sell_sides: &[PairTokenSide],
) -> f64 {
    let Some(first) = evaluations
        .first()
        .map(|(_, evaluation)| evaluation.projection)
    else {
        return f64::NEG_INFINITY;
    };
    let up_owned = ctf_owned_qty_for_side(legs, PairTokenSide::Up);
    let down_owned = ctf_owned_qty_for_side(legs, PairTokenSide::Down);
    let up_sold = ctf_sell_qty_for_side(evaluations, sell_sides, PairTokenSide::Up);
    let down_sold = ctf_sell_qty_for_side(evaluations, sell_sides, PairTokenSide::Down);
    let sell_proceeds = ctf_sell_proceeds_for_side(evaluations, sell_sides, PairTokenSide::Up)
        + ctf_sell_proceeds_for_side(evaluations, sell_sides, PairTokenSide::Down);
    let remaining_up = (up_owned - up_sold).max(0.0);
    let remaining_down = (down_owned - down_sold).max(0.0);
    if remaining_up <= INVENTORY_EPS && remaining_down <= INVENTORY_EPS {
        return first.prior_net_proceeds + sell_proceeds - first.entry_cost;
    }
    let remaining_value = ctf_remaining_owned_value(remaining_up, remaining_down, evaluations);
    if !remaining_value.is_finite() {
        return f64::NEG_INFINITY;
    }
    first.prior_net_proceeds + sell_proceeds + remaining_value - first.entry_cost
}

pub(super) fn ctf_joint_terminal_zero_pnl(
    legs: &[CtfMarketEntryLegSnapshot],
    evaluations: &[(PairTokenSide, CtfLegMarketEvaluation)],
    sell_sides: &[PairTokenSide],
) -> Option<f64> {
    let first = evaluations
        .first()
        .map(|(_, evaluation)| evaluation.projection)?;
    let up_owned = ctf_owned_qty_for_side(legs, PairTokenSide::Up);
    let down_owned = ctf_owned_qty_for_side(legs, PairTokenSide::Down);
    let up_sold = ctf_sell_qty_for_side(evaluations, sell_sides, PairTokenSide::Up);
    let down_sold = ctf_sell_qty_for_side(evaluations, sell_sides, PairTokenSide::Down);
    let sell_proceeds = ctf_sell_proceeds_for_side(evaluations, sell_sides, PairTokenSide::Up)
        + ctf_sell_proceeds_for_side(evaluations, sell_sides, PairTokenSide::Down);
    let complete_set_value = (up_owned - up_sold)
        .max(0.0)
        .min((down_owned - down_sold).max(0.0));
    Some(first.prior_net_proceeds + sell_proceeds + complete_set_value - first.entry_cost)
}

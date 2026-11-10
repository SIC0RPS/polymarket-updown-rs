use crate::portfolio::PortfolioState;
use crate::strategy::pair_lifecycle::AuthoritativePairedCohortView;
use crate::strategy::support::oracle_authority::{OracleAuthorityMode, OracleAuthoritySnapshot};

use super::super::ctf_leg_economics::CtfDeadCatInstruction;
use super::super::FakSellDepthValuation;
use super::{
    ctf_sell_surface_executable, fak_sell_depth_valuation, protected_fak_sell_limit_px,
    CtfEntryLedgerProjection, CtfFirstExecutionSummary, CtfLegExecutableSell, PairTokenSide,
    INVENTORY_EPS,
};

pub(super) fn reference(
    portfolio: &PortfolioState,
    cohort: &AuthoritativePairedCohortView,
    token: &str,
    token_side: PairTokenSide,
    first_execution: Option<CtfFirstExecutionSummary>,
) -> Option<(u128, u128)> {
    if let Some(authorization) = cohort.direct_buy_authorization() {
        let selected_token = if authorization.selected_up {
            cohort.up_token.as_str()
        } else {
            cohort.down_token.as_str()
        };
        portfolio
            .position(token)
            .filter(|position| {
                token == selected_token
                    && position.qty_units_6dp == cohort.remaining_owned_qty_units_6dp
                    && position.qty_units_6dp > 0
            })
            .and_then(|position| {
                position
                    .remaining_basis_units_6dp
                    .map(|basis| (position.qty_units_6dp, basis))
            })
    } else {
        first_execution
            .filter(|summary| {
                summary.complete
                    && summary.pending_qty_units_6dp == 0
                    && summary.token_side != token_side
            })
            .and_then(|summary| {
                let qty = summary.confirmed_qty_units_6dp.filter(|qty| *qty > 0)?;
                let net = u128::try_from(summary.confirmed_net_units_6dp?).ok()?;
                Some((qty, net))
            })
    }
}

pub(super) fn price_below_reference(
    current: FakSellDepthValuation,
    reference: Option<(u128, u128)>,
) -> bool {
    current.fillable_qty > INVENTORY_EPS
        && reference
            .and_then(|(reference_qty, reference_net)| {
                let current_net =
                    crate::ctf_accounting::checked_qty_to_units_6dp(current.authorization_net)
                        .ok()?;
                let current_qty =
                    crate::ctf_accounting::checked_qty_to_units_6dp(current.fillable_qty).ok()?;
                Some((
                    current_net.checked_mul(reference_qty)?,
                    reference_net.checked_mul(current_qty)?,
                ))
            })
            .is_some_and(|(current, reference)| current < reference)
}

pub(super) fn protected_buy_instruction(
    executable: &CtfLegExecutableSell,
    projection: &CtfEntryLedgerProjection,
    base_current: FakSellDepthValuation,
    oracle_snapshot: Option<OracleAuthoritySnapshot>,
    remaining_qty_units_6dp: u128,
    entry_outlay_units_6dp: u128,
    confirmed_prior_net_units_6dp: i128,
    max_supported_limit_px: Option<f64>,
    taker_base_fee_bps: f64,
    reference: Option<(u128, u128)>,
) -> Option<CtfDeadCatInstruction> {
    let snapshot = oracle_snapshot?;
    if snapshot.controller_mode != OracleAuthorityMode::Market
        || snapshot.oracle_signal_side? == executable.token_side
        || remaining_qty_units_6dp == 0
        || projection.instruction_qty_units_6dp != remaining_qty_units_6dp
    {
        return None;
    }
    let confirmed_prior = u128::try_from(confirmed_prior_net_units_6dp).ok()?;
    let capital_units = entry_outlay_units_6dp
        .div_ceil(10)
        .saturating_sub(confirmed_prior);
    let required_units = match (snapshot.p_up_lower_bound, snapshot.p_up_upper_bound) {
        (Some(lower), Some(upper))
            if lower.is_finite()
                && upper.is_finite()
                && (0.0..=1.0).contains(&lower)
                && (lower..=1.0).contains(&upper) =>
        {
            let held_lower = match executable.token_side {
                PairTokenSide::Up => lower,
                PairTokenSide::Down => 1.0 - upper,
            };
            let dynamic_units = (remaining_qty_units_6dp as f64 * held_lower).ceil();
            if !(dynamic_units.is_finite()
                && dynamic_units >= 0.0
                && dynamic_units < u128::MAX as f64)
            {
                return None;
            }
            capital_units.max(dynamic_units as u128)
        }
        _ => capital_units,
    };
    let reservation = crate::strategy::full_set_units_to_qty(required_units);
    let qty = crate::strategy::full_set_units_to_qty(projection.instruction_qty_units_6dp);
    let depth = executable.depth.as_ref()?;
    let limit_px = protected_fak_sell_limit_px(
        qty,
        projection.limit_px,
        max_supported_limit_px?,
        executable.tick_size,
        taker_base_fee_bps,
        depth.bids.len(),
        reservation,
    )?;
    let current = if limit_px.to_bits() == projection.limit_px.to_bits() {
        base_current
    } else {
        fak_sell_depth_valuation(Some(depth), qty, limit_px, taker_base_fee_bps)?
    };
    let quantum = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
    if !(current.fillable_qty.is_finite()
        && current.net.is_finite()
        && current.authorization_net.is_finite()
        && current.fillable_qty + quantum >= qty
        && current.net >= reservation
        && current.authorization_net >= reservation
        && ctf_sell_surface_executable(
            current.fillable_qty,
            current.net,
            current.authorization_net,
            quantum,
        )
        && price_below_reference(current, reference))
    {
        return None;
    }
    Some(CtfDeadCatInstruction { limit_px, current })
}

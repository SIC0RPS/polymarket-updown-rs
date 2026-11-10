use crate::polymarket::state::{book_level_price_is_l1_coherent, BookDepth, BookTop};
use crate::types::Side;

use super::{FakSellDepthValuation, INVENTORY_EPS};

pub(in crate::strategy) fn size_aware_fak_sell_surface(
    top: &BookTop,
    depth: Option<&BookDepth>,
    qty: f64,
    min_executable_qty: f64,
    max_route_limit_px: f64,
    tick_size: f64,
    delay_cushion_ticks: f64,
) -> Option<(f64, f64, f64)> {
    if !(qty.is_finite()
        && qty > INVENTORY_EPS
        && min_executable_qty.is_finite()
        && min_executable_qty >= 0.0
        && max_route_limit_px.is_finite()
        && max_route_limit_px > 0.0
        && crate::strategy::support::shared::common::is_valid_tick_size(tick_size)
        && delay_cushion_ticks.is_finite()
        && delay_cushion_ticks >= 1.0)
    {
        return None;
    }
    let Some(depth) = depth else {
        return None;
    };
    if depth.token_id != top.token_id {
        return None;
    }
    let required_qty = min_executable_qty.max(INVENTORY_EPS).min(qty);
    let mut cumulative = 0.0;
    let mut marginal_px = None;
    for level in &depth.bids {
        if !(level.price.is_finite()
            && level.price > 0.0
            && level.size.is_finite()
            && level.size > 0.0
            && book_level_price_is_l1_coherent(top, Side::Sell, level.price) == Some(true))
        {
            continue;
        }
        cumulative += level.size;
        marginal_px = Some(level.price);
        if cumulative + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM >= qty {
            break;
        }
    }
    let supported_qty = crate::polymarket::order_rules::truncate_to_vendor_qty(cumulative.min(qty));
    if !(supported_qty.is_finite() && supported_qty > INVENTORY_EPS)
        || supported_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < required_qty
    {
        return None;
    }
    let Some(marginal_px) = marginal_px else {
        return None;
    };
    let max_supported_limit_px = crate::strategy::support::shared::common::round_to_tick(
        marginal_px.min(1.0 - tick_size),
        tick_size,
        false,
    );
    let raw = (marginal_px - delay_cushion_ticks * tick_size)
        .max(tick_size)
        .min(max_route_limit_px);
    let aggressive_limit_px =
        crate::strategy::support::shared::common::round_to_tick(raw, tick_size, false);
    let positive_crossing_liquidity = depth.bids.iter().any(|level| {
        level.price.is_finite()
            && level.price > 0.0
            && level.size.is_finite()
            && level.size > 0.0
            && book_level_price_is_l1_coherent(top, Side::Sell, level.price) == Some(true)
            && level.price + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM >= aggressive_limit_px
    });
    if aggressive_limit_px.is_finite()
        && aggressive_limit_px > 0.0
        && max_supported_limit_px.is_finite()
        && max_supported_limit_px >= aggressive_limit_px
        && positive_crossing_liquidity
    {
        Some((supported_qty, aggressive_limit_px, max_supported_limit_px))
    } else {
        None
    }
}

pub(super) fn protected_fak_sell_limit_px(
    qty: f64,
    aggressive_limit_px: f64,
    max_supported_limit_px: f64,
    tick_size: f64,
    taker_base_fee_bps: f64,
    fee_partition_upper_bound: usize,
    reservation: f64,
) -> Option<f64> {
    if !(qty.is_finite()
        && qty > INVENTORY_EPS
        && aggressive_limit_px.is_finite()
        && aggressive_limit_px > 0.0
        && max_supported_limit_px.is_finite()
        && max_supported_limit_px >= aggressive_limit_px
        && crate::strategy::support::shared::common::is_valid_tick_size(tick_size)
        && taker_base_fee_bps.is_finite()
        && taker_base_fee_bps >= 0.0
        && reservation.is_finite()
        && reservation >= 0.0)
    {
        return None;
    }
    let tick_units = crate::polymarket::fixed_math::price_to_units_8dp(tick_size)?;
    let max_supported_units =
        crate::polymarket::fixed_math::price_to_units_8dp(max_supported_limit_px)?;
    // The existing fee owner rounds each fee call to five decimals. This bounds only the
    // scalar-selection error versus the final per-depth-partition authorization valuation.
    let rounding_selection_error_bound = if taker_base_fee_bps == 0.0 {
        0.0
    } else {
        (fee_partition_upper_bound as f64 + 1.0)
            * 5.0
            * crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
    };
    let gross_floor_px = crate::strategy::support::shared::common::round_to_tick(
        (reservation / qty).max(aggressive_limit_px),
        tick_size,
        true,
    );
    let candidate_units = crate::polymarket::fixed_math::price_to_units_8dp(gross_floor_px)?;
    if tick_units == 0 || candidate_units % tick_units != 0 || candidate_units > max_supported_units
    {
        return None;
    }
    let evaluate = |price_units| {
        let price = price_units as f64 / crate::polymarket::fixed_math::FIXED_8DP_SCALE as f64;
        let fee = crate::polymarket::fills::documented_fee_usdc_for_taker_base_fee_bps(
            price,
            qty,
            taker_base_fee_bps,
        )?;
        Some((price, fee, qty * price - fee))
    };
    let (candidate_px, candidate_fee, candidate_net) = evaluate(candidate_units)?;
    if candidate_net - rounding_selection_error_bound >= reservation {
        return Some(candidate_px);
    }
    if candidate_units == max_supported_units {
        return Some(max_supported_limit_px);
    }
    let fee_floor_px = crate::strategy::support::shared::common::round_to_tick(
        (reservation + candidate_fee) / qty,
        tick_size,
        true,
    );
    let fee_floor_units = crate::polymarket::fixed_math::price_to_units_8dp(
        fee_floor_px.min(max_supported_limit_px),
    )?;
    let adjusted_units = candidate_units
        .checked_add(tick_units)?
        .max(fee_floor_units)
        .min(max_supported_units);
    let (adjusted_px, _, adjusted_net) = evaluate(adjusted_units)?;
    if adjusted_net - rounding_selection_error_bound >= reservation {
        return Some(adjusted_px);
    }
    Some(max_supported_limit_px)
}

pub(super) fn fak_sell_depth_valuation(
    depth: Option<&BookDepth>,
    qty: f64,
    worst_limit_px: f64,
    taker_base_fee_bps: f64,
) -> Option<FakSellDepthValuation> {
    if !(qty.is_finite()
        && qty > INVENTORY_EPS
        && worst_limit_px.is_finite()
        && worst_limit_px > 0.0)
    {
        return Some(FakSellDepthValuation::default());
    }
    let Some(depth) = depth else {
        return Some(FakSellDepthValuation::default());
    };
    let mut remaining = qty;
    let mut valuation = FakSellDepthValuation::default();
    let mut authorization_fees = 0.0;
    for level in &depth.bids {
        if remaining <= INVENTORY_EPS {
            break;
        }
        if !(level.price.is_finite() && level.size.is_finite() && level.size > 0.0) {
            continue;
        }
        if level.price + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < worst_limit_px {
            break;
        }
        let take = remaining.min(level.size);
        let fee = crate::polymarket::fills::documented_fee_usdc_for_taker_base_fee_bps(
            level.price,
            take,
            taker_base_fee_bps,
        )?;
        let authorization_fee =
            crate::polymarket::fills::documented_fee_usdc_for_taker_base_fee_bps(
                worst_limit_px,
                take,
                taker_base_fee_bps,
            )?;
        valuation.fillable_qty += take;
        valuation.gross += take * level.price;
        valuation.fees += fee;
        authorization_fees += authorization_fee;
        valuation.slippage_reserve += take * (level.price - worst_limit_px).max(0.0);
        remaining -= take;
    }
    valuation.net = (valuation.gross - valuation.fees).max(0.0);
    valuation.authorization_fees = authorization_fees;
    valuation.authorization_net =
        (valuation.gross - valuation.slippage_reserve - authorization_fees).max(0.0);
    Some(valuation)
}

use super::*;

pub(super) fn round_to_tick(px: f64, tick: f64, round_up: bool) -> f64 {
    if !(px.is_finite() && tick.is_finite() && tick > 0.0) {
        return px;
    }
    let scaled = px / tick;
    let ticks = if round_up {
        scaled.ceil()
    } else {
        scaled.floor()
    };
    (ticks * tick).max(0.0)
}

pub(super) fn normalize_order_qty_for_vendor(intent: &mut OrderIntent, risk: &RiskHandle) {
    if is_ctf_market_entry_sell_intent(intent) || intent.buy_authorization().is_some() {
        return;
    }
    // Vendor SDK enforces lot size precision at 2 decimal places.
    let truncated = truncate_to_decimals(intent.qty, VENDOR_LOT_SIZE_DECIMALS);
    let capped = risk.cap_order_qty(truncated);
    intent.qty = capped;
}

pub(super) fn normalize_order_price_for_vendor(
    intent: &mut OrderIntent,
    odds_state: Option<&PolymarketOddsState>,
) -> Option<f64> {
    if !(intent.limit_px.is_finite() && intent.limit_px > 0.0) {
        return None;
    }
    let tick = resolve_tick_size(intent, odds_state).filter(|t| is_valid_tick_size(*t))?;
    if is_ctf_market_entry_sell_intent(intent) || intent.buy_authorization().is_some() {
        let tick_units_8dp = crate::polymarket::fixed_math::price_to_units_8dp(tick)?;
        let price_units_8dp = crate::polymarket::fixed_math::price_to_units_8dp(intent.limit_px)?;
        let max_price_units_8dp =
            crate::polymarket::fixed_math::FIXED_8DP_SCALE.checked_sub(tick_units_8dp)?;
        return (tick_units_8dp > 0
            && price_units_8dp >= tick_units_8dp
            && price_units_8dp <= max_price_units_8dp
            && price_units_8dp % tick_units_8dp == 0)
            .then_some(tick);
    }
    let round_up = matches!(intent.side(), Side::Sell);
    let rounded = round_to_tick(intent.limit_px, tick, round_up);
    let clamped = rounded.clamp(tick, (1.0 - tick).max(tick));
    let decimals = tick_decimal_places(tick).unwrap_or(VENDOR_LOT_SIZE_DECIMALS);
    intent.limit_px = truncate_to_decimals(clamped, decimals.min(VENDOR_MAX_PRICE_DECIMALS));
    Some(tick)
}

pub(super) fn order_book_max_age_ms(
    intent: &OrderIntent,
    strategy_cfg: &MakerRebateMmStrategyConfig,
) -> u64 {
    let _ = intent;
    strategy_cfg.paired_first_managed_sell_book_max_age_ms
}

pub(super) fn local_venue_order_block_reason(
    intent: &OrderIntent,
    odds_state: Option<&PolymarketOddsState>,
    max_book_staleness_ms: u64,
    admission_buffer_ticks: u8,
    now_ms: u64,
) -> Option<String> {
    let top = U256::from_str(intent.instrument.trim())
        .ok()
        .and_then(|token_id| {
            odds_state.and_then(|odds| crate::polymarket::state::book_for_token(odds, token_id))
        });
    let Some(book) = top.as_ref() else {
        return Some(format!(
            "local_order_block missing_book token={} side={:?}",
            intent.instrument,
            intent.side()
        ));
    };
    if !is_valid_tick_size(book.tick_size) {
        return Some(format!(
            "local_order_block invalid_tick_size token={} side={:?} tick={:.8}",
            intent.instrument,
            intent.side(),
            book.tick_size
        ));
    }
    let Some(age_ms) = (book.observed_at_ms > 0)
        .then(|| now_ms.checked_sub(book.observed_at_ms))
        .flatten()
    else {
        return Some(format!(
            "local_order_block invalid_book_observation_time token={} side={:?}",
            intent.instrument,
            intent.side()
        ));
    };
    let max_staleness = max_book_staleness_ms.max(1);
    if age_ms > max_staleness {
        return Some(format!(
            "local_order_block stale_book token={} side={:?} age_ms={age_ms} max_age_ms={max_staleness}",
            intent.instrument, intent.side()
        ));
    }

    if !intent.post_only && intent.side() == Side::Sell && intent.time_in_force.is_immediate() {
        let Some(bid) = book.bid else {
            return Some(format!(
                "local_order_block missing_book_bid token={} side={:?}",
                intent.instrument,
                intent.side()
            ));
        };
        if intent.limit_px > bid + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM {
            return Some(format!(
                "local_order_block fak_sell_not_marketable token={} px={:.6} bid={:.6}",
                intent.instrument, intent.limit_px, bid
            ));
        }
        let bid_sz = book.bid_sz.unwrap_or(0.0);
        if !(bid_sz.is_finite() && bid_sz > crate::polymarket::fixed_math::FIXED_6DP_QUANTUM) {
            return Some(format!(
                "local_order_block fak_sell_no_bid_size token={} px={:.6} bid={:.6} bid_sz={:?}",
                intent.instrument, intent.limit_px, bid, book.bid_sz
            ));
        }
    }

    if intent.post_only {
        let tick = book.tick_size;
        match intent.side() {
            Side::Buy => {
                let Some(ask) = book.ask else {
                    return Some(format!(
                        "local_order_block missing_book_ask token={} side=Buy",
                        intent.instrument
                    ));
                };
                let Some(max_maker_px) = max_maker_buy_px(ask, tick, admission_buffer_ticks) else {
                    return Some(format!(
                        "local_order_block post_only_no_ask_room token={} ask={:.6} tick={tick:.6} buffer_ticks={admission_buffer_ticks}",
                        intent.instrument, ask
                    ));
                };
                if intent.limit_px > max_maker_px + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
                {
                    return Some(format!(
                        "local_order_block post_only_would_cross token={} side=Buy px={:.6} max_maker_px={max_maker_px:.6} ask={:.6} tick={tick:.6} buffer_ticks={admission_buffer_ticks}",
                        intent.instrument, intent.limit_px, ask
                    ));
                }
            }
            Side::Sell => {
                let Some(bid) = book.bid else {
                    return Some(format!(
                        "local_order_block missing_book_bid token={} side=Sell",
                        intent.instrument
                    ));
                };
                let Some(min_maker_px) = min_maker_sell_px(bid, tick, admission_buffer_ticks)
                else {
                    return Some(format!(
                        "local_order_block post_only_no_bid_room token={} bid={:.6} tick={tick:.6} buffer_ticks={admission_buffer_ticks}",
                        intent.instrument, bid
                    ));
                };
                if intent.limit_px + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < min_maker_px
                {
                    return Some(format!(
                        "local_order_block post_only_would_cross token={} side=Sell px={:.6} min_maker_px={min_maker_px:.6} bid={:.6} tick={tick:.6} buffer_ticks={admission_buffer_ticks}",
                        intent.instrument, intent.limit_px, bid
                    ));
                }
            }
        }
    }

    let normalized_qty = truncate_to_vendor_qty(intent.qty);
    let normalized_min_qty = normalized_min_order_size(book.min_order_size);
    if !(normalized_min_qty.is_finite() && normalized_min_qty > 0.0) {
        return Some(format!(
            "local_order_block invalid_min_order_size token={} side={:?} min_qty={:.8}",
            intent.instrument,
            intent.side(),
            book.min_order_size
        ));
    }
    if normalized_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < normalized_min_qty {
        return Some(format!(
            "local_order_block qty_below_venue_min token={} side={:?} qty={normalized_qty:.2} min_qty={normalized_min_qty:.2} px={:.4}",
            intent.instrument, intent.side(), intent.limit_px
        ));
    }

    None
}

use std::collections::HashSet;

use crate::polymarket::state::{book_level_price_is_l1_coherent, BookDepth, BookTop};
use crate::types::Side;

use super::{
    checked_round_div, exact_level_gross_units_6dp, fixed_units_6dp, LadderSource, MatchedLevel,
    SimFill, VenueOrder,
};

#[derive(Debug, Clone, Copy)]
struct ExecutableLevel {
    source: LadderSource,
    price_units_6dp: u128,
    available_units_6dp: u128,
}

#[derive(Debug, Clone)]
pub(super) struct CapturedLadderQuote {
    levels: Vec<ExecutableLevel>,
    pub(super) available_units_6dp: u128,
    pub(super) depth_source: &'static str,
}

pub(super) fn captured_ladder_quote(
    book: &BookTop,
    depth: Option<&BookDepth>,
    order: &VenueOrder,
    consumed_at_level: impl Fn(LadderSource, u128) -> u128,
    source_invalidated: impl Fn(LadderSource) -> bool,
) -> Result<CapturedLadderQuote, &'static str> {
    let mut levels = Vec::new();
    if let Some(depth) = depth {
        if source_invalidated(LadderSource::L2) {
            return Err("captured_l2_invalidated_by_trade");
        }
        let mut seen_prices = HashSet::new();
        let mut coherent_crossing_level_seen = false;
        let mut positive_crossing_liquidity_seen = false;
        let depth_levels = match order.side {
            Side::Buy => depth.asks.as_slice(),
            Side::Sell => depth.bids.as_slice(),
        };
        for level in depth_levels {
            let price_units_6dp =
                fixed_units_6dp(level.price, false).ok_or("depth_price_not_exact_fixed_6dp")?;
            let size_units_6dp =
                fixed_units_6dp(level.size, true).ok_or("depth_size_not_exact_fixed_6dp")?;
            if !seen_prices.insert(price_units_6dp) {
                return Err("duplicate_depth_price");
            }
            let l1_price = match order.side {
                Side::Buy => book.ask.ok_or("missing_book_ask")?,
                Side::Sell => book.bid.ok_or("missing_book_bid")?,
            };
            fixed_units_6dp(l1_price, false).ok_or("book_price_not_exact_fixed_6dp")?;
            if !book_level_price_is_l1_coherent(book, order.side, level.price)
                .ok_or("book_price_not_exact_fixed_6dp")?
            {
                continue;
            }
            let crosses = match order.side {
                Side::Buy => price_units_6dp <= order.px_units_6dp,
                Side::Sell => price_units_6dp >= order.px_units_6dp,
            };
            if crosses {
                coherent_crossing_level_seen = true;
                positive_crossing_liquidity_seen |= size_units_6dp > 0;
                let available_units_6dp = size_units_6dp
                    .saturating_sub(consumed_at_level(LadderSource::L2, price_units_6dp));
                if available_units_6dp > 0 {
                    levels.push(ExecutableLevel {
                        source: LadderSource::L2,
                        price_units_6dp,
                        available_units_6dp,
                    });
                }
            }
        }
        match order.side {
            Side::Buy => levels.sort_by_key(|level| level.price_units_6dp),
            Side::Sell => {
                levels.sort_by(|left, right| right.price_units_6dp.cmp(&left.price_units_6dp))
            }
        }
        let available_units_6dp = levels
            .iter()
            .try_fold(0u128, |total, level| {
                total.checked_add(level.available_units_6dp)
            })
            .ok_or("captured_ladder_quantity_overflow")?;
        if available_units_6dp == 0 {
            return Err(if positive_crossing_liquidity_seen {
                "captured_l2_fully_consumed"
            } else if coherent_crossing_level_seen {
                "captured_l2_crossing_displayed_size_zero"
            } else {
                "captured_l2_no_coherent_crossing"
            });
        }
        return Ok(CapturedLadderQuote {
            levels,
            available_units_6dp,
            depth_source: "captured_l2",
        });
    }

    if source_invalidated(LadderSource::L1) {
        return Err("captured_l1_invalidated_by_trade");
    }

    let (price, size) = match order.side {
        Side::Buy => (
            book.ask.ok_or("missing_book_ask")?,
            book.ask_sz.unwrap_or(0.0),
        ),
        Side::Sell => (
            book.bid.ok_or("missing_book_bid")?,
            book.bid_sz.unwrap_or(0.0),
        ),
    };
    let price_units_6dp = fixed_units_6dp(price, false).ok_or("book_price_not_exact_fixed_6dp")?;
    let size_units_6dp = fixed_units_6dp(size, true).ok_or("book_size_not_exact_fixed_6dp")?;
    let crosses = match order.side {
        Side::Buy => price_units_6dp <= order.px_units_6dp,
        Side::Sell => price_units_6dp >= order.px_units_6dp,
    };
    if !crosses {
        return Err("not_crossing_captured_ladder");
    }
    let available_units_6dp =
        size_units_6dp.saturating_sub(consumed_at_level(LadderSource::L1, price_units_6dp));
    if available_units_6dp == 0 {
        return Err(if size_units_6dp > 0 {
            "captured_l1_fully_consumed"
        } else {
            "captured_l1_crossing_displayed_size_zero"
        });
    }
    let levels = (available_units_6dp > 0)
        .then_some(ExecutableLevel {
            source: LadderSource::L1,
            price_units_6dp,
            available_units_6dp,
        })
        .into_iter()
        .collect();
    Ok(CapturedLadderQuote {
        levels,
        available_units_6dp,
        depth_source: "captured_l1",
    })
}

pub(super) fn fill_from_ladder(
    quote: &CapturedLadderQuote,
    requested_units_6dp: u128,
    book_age_ms: u64,
    order_rest_ms: u64,
) -> Option<SimFill> {
    if requested_units_6dp == 0 {
        return None;
    }
    let mut remaining_units_6dp = requested_units_6dp;
    let mut filled_units_6dp = 0u128;
    let mut gross_units_6dp = 0u128;
    let mut matched_levels = Vec::new();
    for level in &quote.levels {
        if remaining_units_6dp == 0 {
            break;
        }
        let take_units_6dp = level.available_units_6dp.min(remaining_units_6dp);
        if take_units_6dp == 0 {
            continue;
        }
        filled_units_6dp = filled_units_6dp.checked_add(take_units_6dp)?;
        gross_units_6dp = gross_units_6dp.checked_add(exact_level_gross_units_6dp(
            take_units_6dp,
            level.price_units_6dp,
        )?)?;
        remaining_units_6dp -= take_units_6dp;
        matched_levels.push(MatchedLevel {
            source: Some(level.source),
            price_units_6dp: level.price_units_6dp,
            qty_units_6dp: take_units_6dp,
        });
    }
    if filled_units_6dp == 0 {
        return None;
    }
    let px_units_6dp = checked_round_div(
        gross_units_6dp.checked_mul(super::FIXED_SCALE)?,
        filled_units_6dp,
    )?;
    Some(SimFill {
        qty_units_6dp: filled_units_6dp,
        px_units_6dp,
        book_age_ms,
        order_rest_ms,
        depth_source: quote.depth_source,
        available_qty: super::units_6dp_to_f64(quote.available_units_6dp),
        levels_crossed: matched_levels.len().min(u16::MAX as usize) as u16,
        matched_levels,
        gross_units_6dp,
        fee_units_6dp: None,
        receipt_identity: None,
    })
}

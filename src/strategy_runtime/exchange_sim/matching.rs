use crate::types::{Side, TimeInForce};

use super::{captured_ladder, ExchangeSim, SimFillResult, VenueOrder};

pub(super) fn order_crosses_book(
    exchange: &ExchangeSim,
    order: &VenueOrder,
    ts_ms: u64,
) -> Result<bool, &'static str> {
    let (book, _) = exchange.validated_book_state(order.token_id, ts_ms)?;
    let crossing_price = match order.side {
        Side::Buy => book.ask,
        Side::Sell => book.bid,
    };
    let Some(crossing_price) = crossing_price else {
        return Ok(false);
    };
    let crossing_price_units =
        super::fixed_units_6dp(crossing_price, false).ok_or("book_price_not_exact_fixed_6dp")?;
    Ok(match order.side {
        Side::Buy => crossing_price_units <= order.px_units_6dp,
        Side::Sell => crossing_price_units >= order.px_units_6dp,
    })
}

pub(super) fn immediate_fill(
    exchange: &mut ExchangeSim,
    order: &VenueOrder,
    ts_ms: u64,
) -> SimFillResult {
    let (quote, book_age_ms) = executable_quote(exchange, order, ts_ms)?;
    let requested_units_6dp = match order.time_in_force {
        TimeInForce::Fok if quote.available_units_6dp >= order.original_size_units_6dp => {
            order.original_size_units_6dp
        }
        TimeInForce::Fok => return Err("fok_insufficient_captured_ladder_size"),
        TimeInForce::Fak if quote.available_units_6dp > 0 => {
            quote.available_units_6dp.min(order.original_size_units_6dp)
        }
        TimeInForce::Fak => return Err("fak_insufficient_captured_ladder_size"),
        _ => return Err("unsupported_immediate_tif"),
    };
    let mut fill = captured_ladder::fill_from_ladder(&quote, requested_units_6dp, book_age_ms, 0)
        .ok_or("zero_fill_qty")?;
    fill.receipt_identity = Some(exchange.ladder_receipt_identity(order.token_id, &fill)?);
    if matches!(order.time_in_force, TimeInForce::Fok)
        && fill.qty_units_6dp != order.original_size_units_6dp
    {
        return Err("fok_non_exact_fill");
    }
    exchange.finalize_fill_economics(order, &mut fill)?;
    exchange
        .consume_ladder(order.token_id, order.side, &fill)
        .ok_or("captured_ladder_consumption_overflow")?;
    Ok(fill)
}

pub(super) fn marketable_limit_fill(
    exchange: &mut ExchangeSim,
    order: &VenueOrder,
    ts_ms: u64,
) -> SimFillResult {
    let (quote, book_age_ms) = executable_quote(exchange, order, ts_ms)?;
    let requested_units_6dp = quote.available_units_6dp.min(order.remaining_units_6dp());
    let mut fill = captured_ladder::fill_from_ladder(&quote, requested_units_6dp, book_age_ms, 0)
        .ok_or("zero_fill_qty")?;
    fill.receipt_identity = Some(exchange.ladder_receipt_identity(order.token_id, &fill)?);
    exchange.finalize_fill_economics(order, &mut fill)?;
    exchange
        .consume_ladder(order.token_id, order.side, &fill)
        .ok_or("captured_ladder_consumption_overflow")?;
    Ok(fill)
}

fn executable_quote(
    exchange: &ExchangeSim,
    order: &VenueOrder,
    ts_ms: u64,
) -> Result<(captured_ladder::CapturedLadderQuote, u64), &'static str> {
    let (book, depth) = exchange.validated_book_state(order.token_id, ts_ms)?;
    let quote = captured_ladder::captured_ladder_quote(
        book,
        depth,
        order,
        |source, price_units| {
            exchange.consumed_at_level(order.token_id, source, order.side, price_units)
        },
        |source| exchange.ladder_invalidated(order.token_id, source, order.side),
    )?;
    Ok((quote, ts_ms - book.observed_at_ms))
}

use std::str::FromStr;

use polymarket_client_sdk::clob::types::TickSize;
use polymarket_client_sdk::clob::utilities::price_valid;
use polymarket_client_sdk::types::Decimal;

use super::book_state::polymarket_tick_size_from_f64;

pub(crate) fn strict_ladder_is_valid<Bids, Asks>(bids: Bids, asks: Asks) -> bool
where
    Bids: Iterator<Item = (f64, f64)> + Clone,
    Asks: Iterator<Item = (f64, f64)> + Clone,
{
    if !bids.clone().chain(asks.clone()).all(|(price, size)| {
        price.is_finite() && price > 0.0 && price <= 1.0 && size.is_finite() && size > 0.0
    }) {
        return false;
    }
    if !strictly_ordered(bids.clone(), |previous, next| previous > next)
        || !strictly_ordered(asks.clone(), |previous, next| previous < next)
    {
        return false;
    }
    !matches!(
        (bids.map(|(price, _)| price).next(), asks.map(|(price, _)| price).next()),
        (Some(best_bid), Some(best_ask)) if best_bid > best_ask
    )
}

pub(crate) fn ws_ladder_conforms_to_tick<Bids, Asks>(bids: Bids, asks: Asks, tick_size: f64) -> bool
where
    Bids: Iterator<Item = (f64, f64)> + Clone,
    Asks: Iterator<Item = (f64, f64)> + Clone,
{
    let Some(tick_size) = polymarket_tick_size_from_f64(tick_size) else {
        return false;
    };
    strict_ladder_is_valid(bids.clone(), asks.clone())
        && bids
            .chain(asks)
            .all(|(price, _)| ws_price_conforms_to_tick(price, tick_size))
}

pub(crate) fn decimal_price_conforms_to_tick(price: Decimal, tick_size: TickSize) -> bool {
    price_valid(price, tick_size) && (price % tick_size.as_decimal()).is_zero()
}

pub(crate) fn ws_price_conforms_to_tick(price: f64, tick_size: TickSize) -> bool {
    if !price.is_finite() {
        return false;
    }
    Decimal::from_str(&price.to_string())
        .ok()
        .is_some_and(|price| decimal_price_conforms_to_tick(price, tick_size))
}

fn strictly_ordered<Levels>(mut levels: Levels, ordered: impl Fn(f64, f64) -> bool) -> bool
where
    Levels: Iterator<Item = (f64, f64)>,
{
    let Some((mut previous, _)) = levels.next() else {
        return true;
    };
    levels.all(|(next, _)| {
        let valid = ordered(previous, next);
        previous = next;
        valid
    })
}

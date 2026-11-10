use num_traits::cast::ToPrimitive;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::ws::types::response::{
    BookUpdate, LastTradePrice, PriceChangeBatchEntry, TickSizeChange,
};
use polymarket_client_sdk::types::Decimal;

use crate::wire::{
    PolymarketBookFrame, PolymarketBookLevelFrame, PolymarketLastTradeFrame,
    PolymarketPriceChangeEntryFrame, PolymarketTickSizeFrame, TradeSide,
};

pub(super) fn book_frame(book: BookUpdate) -> Option<PolymarketBookFrame> {
    let mut bids = Vec::with_capacity(book.bids.len());
    for level in &book.bids {
        bids.push(PolymarketBookLevelFrame {
            price: decimal_to_f64(level.price)?,
            size: decimal_to_f64(level.size)?,
        });
    }
    let mut asks = Vec::with_capacity(book.asks.len());
    for level in &book.asks {
        asks.push(PolymarketBookLevelFrame {
            price: decimal_to_f64(level.price)?,
            size: decimal_to_f64(level.size)?,
        });
    }
    bids.sort_by(|a, b| b.price.total_cmp(&a.price));
    asks.sort_by(|a, b| a.price.total_cmp(&b.price));
    Some(PolymarketBookFrame {
        asset_id: book.asset_id,
        bids,
        asks,
        timestamp: book.timestamp,
    })
}

pub(super) fn price_change_entry_frame(
    entry: PriceChangeBatchEntry,
) -> Option<PolymarketPriceChangeEntryFrame> {
    Some(PolymarketPriceChangeEntryFrame {
        asset_id: entry.asset_id,
        price: decimal_to_f64(entry.price)?,
        size: optional_decimal_to_f64(entry.size)?,
        side: match entry.side {
            polymarket_client_sdk::clob::types::Side::Buy => TradeSide::Buy,
            polymarket_client_sdk::clob::types::Side::Sell => TradeSide::Sell,
            polymarket_client_sdk::clob::types::Side::Unknown => return None,
            _ => return None,
        },
        hash: entry.hash,
        best_bid: optional_decimal_to_f64(entry.best_bid)?,
        best_ask: optional_decimal_to_f64(entry.best_ask)?,
    })
}

pub(super) fn tick_size_frame(change: TickSizeChange) -> PolymarketTickSizeFrame {
    PolymarketTickSizeFrame {
        asset_id: change.asset_id,
        old_tick_size: decimal_to_f64(change.old_tick_size).unwrap_or(f64::NAN),
        new_tick_size: decimal_to_f64(change.new_tick_size).unwrap_or(f64::NAN),
        timestamp: change.timestamp,
    }
}

pub(super) fn last_trade_frame(last_trade: LastTradePrice) -> Option<PolymarketLastTradeFrame> {
    Some(PolymarketLastTradeFrame {
        asset_id: last_trade.asset_id,
        price: decimal_to_f64(last_trade.price)?,
        size: optional_decimal_to_f64(last_trade.size)?.unwrap_or(0.0),
        side: last_trade.side.and_then(map_last_trade_side),
        timestamp: last_trade.timestamp,
    })
}

fn map_last_trade_side(side: ClobSide) -> Option<TradeSide> {
    match side {
        ClobSide::Buy => Some(TradeSide::Buy),
        ClobSide::Sell => Some(TradeSide::Sell),
        ClobSide::Unknown => None,
        _ => None,
    }
}

pub(super) fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|value| value.is_finite())
}

fn optional_decimal_to_f64(value: Option<Decimal>) -> Option<Option<f64>> {
    match value {
        Some(value) => Some(Some(decimal_to_f64(value)?)),
        None => Some(None),
    }
}

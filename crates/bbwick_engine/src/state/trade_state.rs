use polymarket_client_sdk::types::U256;
use serde::{Deserialize, Serialize};

use crate::state::{normalize_ws_ts_ms, PolymarketOddsState};
use crate::wire::{PolymarketLastTradeFrame, TradeSide};

const MAX_RECENT_LAST_TRADES: usize = 512;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LastTrade {
    pub seq: u64,
    pub token_id: U256,
    pub price: f64,
    pub size: f64,
    pub side: Option<TradeSide>,
    pub ts_ms: u64,
}

pub fn apply_last_trade_to_state(
    state: &mut PolymarketOddsState,
    frame: &PolymarketLastTradeFrame,
    now_ms: u64,
) -> bool {
    if !(frame.price.is_finite() && frame.price > 0.0) {
        return false;
    }
    state.next_last_trade_seq = state.next_last_trade_seq.saturating_add(1);
    state.last_trades.push_back(LastTrade {
        seq: state.next_last_trade_seq,
        token_id: frame.asset_id,
        price: frame.price,
        size: frame.size.max(0.0),
        side: frame.side,
        ts_ms: normalize_ws_ts_ms(frame.timestamp),
    });
    while state.last_trades.len() > MAX_RECENT_LAST_TRADES {
        let _ = state.last_trades.pop_front();
    }
    state.updated_at_ms = now_ms;
    true
}

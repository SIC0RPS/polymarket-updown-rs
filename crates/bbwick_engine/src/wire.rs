use std::str::FromStr;

use polymarket_client_sdk::types::U256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct TickFrame {
    pub symbol: String,
    pub ts_ms: u64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub struct HeartbeatFrame {
    pub symbol: String,
    pub ts_ms: u64,
    pub seq: u32,
}

#[derive(Debug, Clone, Default)]
pub struct StepFrame {
    pub symbol: String,
    pub timeframe: String,
    pub received_mono_ns: u64,
    pub bar: BarPayload,
    pub overlays: Vec<OverlayPayload>,
    pub trades: Vec<StepTradePayload>,
    pub seq: u32,
    pub ledger_epoch: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StepTradePayload {
    pub entry_ts_ms: u64,
    pub exit_ts_ms: u64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub side: String,
    pub entry_reason: Option<String>,
    pub exit_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct OverlayPayload {
    pub ts_ms: u64,
    pub ema_fast_plot: f64,
    pub ema_slow_plot: f64,
    pub ema_fast_raw: f64,
    pub ema_slow_raw: f64,
    pub cross_over_confirmed: bool,
    pub cross_under_confirmed: bool,
    pub bb_upper: f64,
    pub bb_lower: f64,
    pub bbw_pct: f64,
}

#[derive(Debug, Clone, Default)]
pub struct BarPayload {
    pub ts_ms: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketBookLevelFrame {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketBookFrame {
    #[serde(with = "u256_decimal_string")]
    pub asset_id: U256,
    pub bids: Vec<PolymarketBookLevelFrame>,
    pub asks: Vec<PolymarketBookLevelFrame>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketPriceChangeEntryFrame {
    #[serde(with = "u256_decimal_string")]
    pub asset_id: U256,
    pub price: f64,
    pub size: Option<f64>,
    pub side: TradeSide,
    pub hash: Option<String>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketPriceChangeFrame {
    pub timestamp: i64,
    pub price_changes: Vec<PolymarketPriceChangeEntryFrame>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketTickSizeFrame {
    #[serde(with = "u256_decimal_string")]
    pub asset_id: U256,
    pub old_tick_size: f64,
    pub new_tick_size: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    #[serde(rename = "BUY", alias = "buy")]
    Buy,
    #[serde(rename = "SELL", alias = "sell")]
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketLastTradeFrame {
    #[serde(with = "u256_decimal_string")]
    pub asset_id: U256,
    pub price: f64,
    pub size: f64,
    pub side: Option<TradeSide>,
    pub timestamp: i64,
}

mod u256_decimal_string {
    use super::*;
    use serde::{de::Error as _, Deserializer, Serializer};

    pub fn serialize<S>(value: &U256, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        U256::from_str(raw.trim()).map_err(D::Error::custom)
    }
}

use anyhow::bail;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TransportSettings {
    pub market_data_ws_url: Option<String>,
    pub market_data_rest_base_url: Option<String>,
    pub polymarket_ws_url: Option<String>,
    pub polymarket_clob_http_url: Option<String>,
    pub polymarket_use_server_time: Option<bool>,
    pub provider_subscribe_trades: Option<bool>,
    pub provider_subscribe_top_of_book: Option<bool>,
    pub provider_subscribe_klines: Option<bool>,
    pub polymarket_subscribe_orderbook: Option<bool>,
    pub polymarket_subscribe_prices: Option<bool>,
    pub polymarket_subscribe_tick_size: Option<bool>,
    pub polymarket_subscribe_last_trade: Option<bool>,
    pub step_timeframes: Option<Vec<String>>,
}

impl TransportSettings {
    pub fn validate(&self) -> anyhow::Result<()> {
        let step_timeframes = self.step_timeframes();
        if step_timeframes.is_empty() {
            bail!("transport.step_timeframes must not be empty");
        }
        if !step_timeframes
            .iter()
            .all(|value| matches!(value.as_str(), "1m" | "5m"))
        {
            bail!("transport.step_timeframes must only contain 1m or 5m");
        }
        Ok(())
    }

    pub fn market_data_ws_url(&self) -> Option<&str> {
        self.market_data_ws_url.as_deref()
    }

    pub fn market_data_rest_base_url(&self) -> Option<&str> {
        self.market_data_rest_base_url.as_deref()
    }

    pub fn polymarket_ws_url(&self) -> Option<&str> {
        self.polymarket_ws_url.as_deref()
    }

    pub fn polymarket_clob_http_url(&self) -> Option<&str> {
        self.polymarket_clob_http_url.as_deref()
    }

    pub fn polymarket_use_server_time(&self) -> bool {
        self.polymarket_use_server_time.unwrap_or(false)
    }

    pub fn provider_subscribe_trades(&self) -> bool {
        self.provider_subscribe_trades.unwrap_or(true)
    }

    pub fn provider_subscribe_top_of_book(&self) -> bool {
        self.provider_subscribe_top_of_book.unwrap_or(true)
    }

    pub fn provider_subscribe_klines(&self) -> bool {
        self.provider_subscribe_klines.unwrap_or(true)
    }

    pub fn polymarket_subscribe_orderbook(&self) -> bool {
        self.polymarket_subscribe_orderbook.unwrap_or(true)
    }

    pub fn polymarket_subscribe_prices(&self) -> bool {
        self.polymarket_subscribe_prices.unwrap_or(true)
    }

    pub fn polymarket_subscribe_tick_size(&self) -> bool {
        self.polymarket_subscribe_tick_size.unwrap_or(true)
    }

    pub fn polymarket_subscribe_last_trade(&self) -> bool {
        self.polymarket_subscribe_last_trade.unwrap_or(true)
    }

    pub fn step_timeframes(&self) -> Vec<String> {
        let requested = self
            .step_timeframes
            .clone()
            .unwrap_or_else(|| {
                DEFAULT_STEP_TIMEFRAMES
                    .iter()
                    .map(|v| (*v).to_string())
                    .collect()
            })
            .into_iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
            .collect::<std::collections::HashSet<_>>();
        if requested.is_empty() {
            return DEFAULT_STEP_TIMEFRAMES
                .iter()
                .map(|value| (*value).to_string())
                .collect();
        }
        DEFAULT_STEP_TIMEFRAMES
            .iter()
            .filter(|value| requested.contains(**value))
            .map(|value| (*value).to_string())
            .collect()
    }
}

const DEFAULT_STEP_TIMEFRAMES: &[&str] = &["1m", "5m"];

use bbwick_engine::config::TransportSettings;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BbwickOracleConfig {
    pub enabled: bool,
    pub quote_asset: String,
    pub symbols: HashMap<String, bool>,
    pub underlying_symbol_map: HashMap<String, String>,
    #[serde(skip)]
    pub(crate) normalized_underlying_symbol_map: Arc<OnceLock<HashMap<String, String>>>,
    pub transport: BbwickOracleTransportConfig,
    pub runtime: BbwickOracleRuntimeConfig,
    pub history: BbwickOracleHistoryConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BbwickOracleTransportConfig {
    pub ws_url: String,
    pub rest_base_url: String,
    pub polymarket_ws_url: String,
    pub polymarket_clob_http_url: String,
    pub polymarket_use_server_time: bool,
    pub provider_subscribe_trades: bool,
    pub provider_subscribe_top_of_book: bool,
    pub provider_subscribe_klines: bool,
    pub polymarket_subscribe_orderbook: bool,
    pub polymarket_subscribe_prices: bool,
    pub polymarket_subscribe_tick_size: bool,
    pub polymarket_subscribe_last_trade: bool,
    pub step_timeframes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BbwickOracleRuntimeConfig {
    pub ingest_channel_capacity: usize,
    pub tick_channel_capacity: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BbwickOracleHistoryConfig {
    pub enabled: bool,
    pub data_root: PathBuf,
    pub max_bars: usize,
}

impl BbwickOracleConfig {
    pub fn resolved_underlying_symbols(&self) -> BTreeMap<String, String> {
        super::strategy::VALID_ASSETS
            .iter()
            .map(|asset| {
                (
                    (*asset).to_string(),
                    self.resolve_symbol_for_underlying(asset),
                )
            })
            .collect()
    }

    pub(crate) fn isolated_for_underlying(&self, underlying: &str) -> Self {
        let canonical_underlying = canonical_underlying_key(underlying)
            .unwrap_or_else(|| underlying.trim().to_ascii_lowercase());
        let oracle_symbol = self.resolve_symbol_for_underlying(underlying);
        let mut isolated = self.clone();
        isolated.symbols.clear();
        isolated.symbols.insert(oracle_symbol.clone(), true);
        isolated.underlying_symbol_map.retain(|candidate, _| {
            canonical_underlying_key(candidate).as_deref() == Some(canonical_underlying.as_str())
        });
        if isolated.underlying_symbol_map.is_empty() {
            isolated
                .underlying_symbol_map
                .insert(canonical_underlying, oracle_symbol.clone());
        }
        isolated.normalized_underlying_symbol_map = Arc::new(OnceLock::new());
        isolated
    }

    pub fn active_symbols(&self) -> Vec<String> {
        let mut symbols: Vec<String> = self
            .symbols
            .iter()
            .filter_map(|(symbol, enabled)| {
                if *enabled {
                    canonical_oracle_symbol(symbol)
                } else {
                    None
                }
            })
            .collect();
        symbols.sort();
        symbols.dedup();
        symbols
    }

    pub fn resolve_symbol_for_underlying(&self, underlying: &str) -> String {
        let key = canonical_underlying_key(underlying)
            .unwrap_or_else(|| underlying.trim().to_ascii_lowercase());
        if let Some(mapped) = self
            .normalized_underlying_symbol_map()
            .get(key.as_str())
            .and_then(|value| canonical_oracle_symbol(value))
        {
            return mapped;
        }

        let base = key
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_uppercase();
        let quote = canonical_quote_asset(&self.quote_asset);
        if base.ends_with(&quote) {
            base
        } else {
            format!("{base}{quote}")
        }
    }

    fn normalized_underlying_symbol_map(&self) -> &HashMap<String, String> {
        self.normalized_underlying_symbol_map.get_or_init(|| {
            self.underlying_symbol_map
                .iter()
                .filter_map(|(underlying, symbol)| {
                    canonical_underlying_key(underlying).map(|key| (key, symbol.clone()))
                })
                .collect()
        })
    }

    pub fn to_transport_settings(&self) -> TransportSettings {
        TransportSettings {
            market_data_ws_url: Some(self.transport.ws_url.clone()),
            market_data_rest_base_url: Some(self.transport.rest_base_url.clone()),
            polymarket_ws_url: Some(self.transport.polymarket_ws_url.clone()),
            polymarket_clob_http_url: Some(self.transport.polymarket_clob_http_url.clone()),
            polymarket_use_server_time: Some(self.transport.polymarket_use_server_time),
            provider_subscribe_trades: Some(self.transport.provider_subscribe_trades),
            provider_subscribe_top_of_book: Some(self.transport.provider_subscribe_top_of_book),
            provider_subscribe_klines: Some(self.transport.provider_subscribe_klines),
            polymarket_subscribe_orderbook: Some(self.transport.polymarket_subscribe_orderbook),
            polymarket_subscribe_prices: Some(self.transport.polymarket_subscribe_prices),
            polymarket_subscribe_tick_size: Some(self.transport.polymarket_subscribe_tick_size),
            polymarket_subscribe_last_trade: Some(self.transport.polymarket_subscribe_last_trade),
            step_timeframes: Some(self.transport.step_timeframes.clone()),
            ..Default::default()
        }
    }
}

impl BbwickOracleHistoryConfig {
    pub fn resolved_data_root(&self) -> PathBuf {
        self.data_root.clone()
    }
}

pub(crate) fn canonical_underlying_key(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.eq_ignore_ascii_case("btc") || trimmed.eq_ignore_ascii_case("bitcoin") {
        return Some("btc".to_string());
    }
    if trimmed.eq_ignore_ascii_case("eth") || trimmed.eq_ignore_ascii_case("ethereum") {
        return Some("eth".to_string());
    }
    if trimmed.eq_ignore_ascii_case("sol") || trimmed.eq_ignore_ascii_case("solana") {
        return Some("sol".to_string());
    }
    if trimmed.eq_ignore_ascii_case("xrp") || trimmed.eq_ignore_ascii_case("ripple") {
        return Some("xrp".to_string());
    }
    if trimmed.eq_ignore_ascii_case("bnb") || trimmed.eq_ignore_ascii_case("binancecoin") {
        return Some("bnb".to_string());
    }

    let mut cleaned = String::with_capacity(trimmed.len());
    for byte in trimmed.bytes() {
        if byte.is_ascii_alphanumeric() {
            cleaned.push((byte as char).to_ascii_lowercase());
        }
    }
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

pub(crate) fn canonical_quote_asset(raw: &str) -> String {
    let quote = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    if quote.is_empty() {
        "USDT".to_string()
    } else {
        quote
    }
}

pub(crate) fn canonical_oracle_symbol(raw: &str) -> Option<String> {
    let symbol = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    if symbol.len() < 3 {
        return None;
    }
    Some(symbol)
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PolymarketConfig {
    pub market_data_enabled: bool,
    pub market_data_mode: PolymarketMarketDataMode,
    pub execution: PolymarketExecutionConfig,
    pub account_rpc_url: String,
    pub clob_http_url: String,
    pub clob_ws_url: String,
    pub gamma_http_url: String,
    pub data_http_url: String,
    pub use_server_time: bool,
    pub token_id: Option<String>,
    pub chain_id: u64,
    pub private_key_env: String,
    pub wallet_address: String,
    pub discovery: PolymarketDiscoveryConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketMarketDataMode {
    Ws,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketExecutionConfig {
    pub mode: Option<PolymarketExecutionMode>,
    #[serde(rename = "PAUSE")]
    pub pause: bool,
    pub order_type: PolymarketOrderType,
    pub reclaim_retry_cooldown_secs: u64,
    pub order_submit_pia_region: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketExecutionMode {
    Live,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketOrderType {
    Gtc,
    Fok,
    Gtd,
    Fak,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketDiscoveryConfig {
    pub enabled: bool,
    pub query: String,
    pub poll_interval_ms: u64,
    pub timeout_ms: u64,
    pub window_ms: u64,
    pub window_tolerance_ms: u64,
    pub slug_must_contain: Vec<String>,
    pub underlyings_allowlist: Vec<String>,
    pub min_time_left_ms: u64,
    pub max_time_left_ms: u64,
    pub require_order_book: bool,
    pub require_accepting_orders: bool,
}

impl PolymarketConfig {
    pub fn resolved_account_rpc_url(&self) -> String {
        self.account_rpc_url.trim().to_string()
    }
}

use serde::{Deserialize, Serialize};

pub(crate) const POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS: u64 = 120_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RiskConfig {
    pub enabled: bool,
    pub startup_cooldown_ms: u64,
    pub max_open_orders: usize,
    pub max_window_cost: f64,
    pub max_market_equity_pct: f64,
    pub order_timeout_ms: u64,
    pub min_usdc_balance: f64,
    pub max_drawdown_usd: f64,
}

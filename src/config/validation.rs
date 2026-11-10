use anyhow::{ensure, Result};

use super::{AppConfig, PolymarketMarketDataMode, POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS};

mod maker_rebate_core;
mod maker_rebate_extended;

impl AppConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_maker_rebate_core()?;
        self.validate_maker_rebate_extended()?;

        ensure!(
            self.polymarket.market_data_enabled,
            "maker_rebate_mm requires polymarket.market_data_enabled = true"
        );
        ensure!(
            self.polymarket.market_data_mode == PolymarketMarketDataMode::Ws,
            "maker_rebate_mm requires polymarket.market_data_mode = \"ws\""
        );
        ensure!(
            self.polymarket.discovery.enabled,
            "maker_rebate_mm requires polymarket.discovery.enabled = true"
        );
        ensure!(
            matches!(
                self.polymarket.execution.order_type,
                crate::config::PolymarketOrderType::Gtc | crate::config::PolymarketOrderType::Gtd
            ),
            "maker_rebate_mm requires polymarket.execution.order_type = \"gtc\" or \"gtd\""
        );
        if matches!(
            self.polymarket.execution.order_type,
            crate::config::PolymarketOrderType::Gtd
        ) {
            ensure!(
                self.risk.order_timeout_ms >= POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS,
                "polymarket.execution.order_type = \"gtd\" requires risk.order_timeout_ms >= {}",
                POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS
            );
        }
        super::validation_bbwick::validate_bbwick_oracle_config(self)?;
        super::validation_runtime::validate_runtime_config(self)?;
        Ok(())
    }
}

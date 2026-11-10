use anyhow::{ensure, Result};

use crate::config::AppConfig;

impl AppConfig {
    pub(super) fn validate_maker_rebate_core(&self) -> Result<()> {
        ensure!(!self.symbols.is_empty(), "symbols must not be empty");
        ensure!(self.max_order_qty >= 0.0, "max_order_qty must be >= 0");
        ensure!(
            self.bbwick_oracle.enabled,
            "maker_rebate_mm requires bbwick_oracle.enabled = true"
        );
        ensure!(
            self.strategy.maker_rebate_mm.max_orders_per_cycle > 0,
            "strategy.maker_rebate_mm.max_orders_per_cycle must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.normal_rebalance_interval_ms > 0,
            "strategy.maker_rebate_mm.normal_rebalance_interval_ms must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.fast_rebalance_interval_ms > 0,
            "strategy.maker_rebate_mm.fast_rebalance_interval_ms must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.base_half_spread >= 0.0,
            "strategy.maker_rebate_mm.base_half_spread must be >= 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.ranking_book_max_age_ms > 0,
            "strategy.maker_rebate_mm.ranking_book_max_age_ms must be > 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .paired_first_managed_sell_book_max_age_ms
                > 0,
            "strategy.maker_rebate_mm.paired_first_managed_sell_book_max_age_ms must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.max_oracle_staleness_ms > 0,
            "strategy.maker_rebate_mm.max_oracle_staleness_ms must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.ewma_half_life_ms > 0,
            "strategy.maker_rebate_mm.ewma_half_life_ms must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.oracle_shock_bps >= 0.0,
            "strategy.maker_rebate_mm.oracle_shock_bps must be >= 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.max_inventory_per_token >= 0.0,
            "strategy.maker_rebate_mm.max_inventory_per_token must be >= 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.max_inventory_total >= 0.0,
            "strategy.maker_rebate_mm.max_inventory_total must be >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .markout_windows_ms
                .iter()
                .all(|v| *v > 0),
            "strategy.maker_rebate_mm.markout_windows_ms entries must be > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.markout_windows_ms.len() >= 3,
            "strategy.maker_rebate_mm.markout_windows_ms must include at least 3 windows"
        );
        ensure!(
            self.strategy.maker_rebate_mm.admission_buffer_ticks <= 2,
            "strategy.maker_rebate_mm.admission_buffer_ticks must be 0, 1, or 2"
        );
        self.strategy
            .maker_rebate_mm
            .live_fair_value_model
            .validate("strategy.maker_rebate_mm.live_fair_value_model")?;
        ensure!(
            self.strategy
                .maker_rebate_mm
                .post_only_reject_size_multiplier
                .is_finite()
                && (0.0..=1.0)
                    .contains(&self.strategy.maker_rebate_mm.post_only_reject_size_multiplier),
            "strategy.maker_rebate_mm.post_only_reject_size_multiplier must be finite and in [0, 1]"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .paired_first_managed_sell_slippage_tolerance
                >= 0.0
                && self
                    .strategy
                    .maker_rebate_mm
                    .paired_first_managed_sell_slippage_tolerance
                    .is_finite(),
            "strategy.maker_rebate_mm.paired_first_managed_sell_slippage_tolerance must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .paired_first_managed_sell_aggressiveness
                >= 0.0
                && self
                    .strategy
                    .maker_rebate_mm
                    .paired_first_managed_sell_aggressiveness
                    .is_finite(),
            "strategy.maker_rebate_mm.paired_first_managed_sell_aggressiveness must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_side_min_advantage
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_side_min_advantage >= 0.0,
            "strategy.maker_rebate_mm.oracle_side_min_advantage must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_side_flip_hysteresis
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_side_flip_hysteresis >= 0.0,
            "strategy.maker_rebate_mm.oracle_side_flip_hysteresis must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_side_noise_weight
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_side_noise_weight >= 0.0,
            "strategy.maker_rebate_mm.oracle_side_noise_weight must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_side_flip_noise_weight
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_side_flip_noise_weight >= 0.0,
            "strategy.maker_rebate_mm.oracle_side_flip_noise_weight must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_side_cleanup_flip_bump
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_side_cleanup_flip_bump >= 0.0,
            "strategy.maker_rebate_mm.oracle_side_cleanup_flip_bump must be finite and >= 0"
        );
        Ok(())
    }
}

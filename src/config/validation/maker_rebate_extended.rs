use anyhow::{ensure, Result};

use crate::config::strategy::VALID_ASSETS;
use crate::config::AppConfig;

impl AppConfig {
    pub(super) fn validate_maker_rebate_extended(&self) -> Result<()> {
        ensure!(
            self.strategy
                .maker_rebate_mm
                .min_hold_probability_edge
                .is_finite()
                && (0.0..=1.0).contains(&self.strategy.maker_rebate_mm.min_hold_probability_edge,),
            "strategy.maker_rebate_mm.min_hold_probability_edge must be finite and in [0, 1]"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_remaining_leg_liquidity_drawdown_weight
                .is_finite()
                && (0.0..=1.0).contains(
                    &self
                        .strategy
                        .maker_rebate_mm
                        .ctf_remaining_leg_liquidity_drawdown_weight,
                ),
            "strategy.maker_rebate_mm.ctf_remaining_leg_liquidity_drawdown_weight must be finite and in [0, 1]"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_remaining_leg_time_risk_weight
                .is_finite()
                && (0.0..=1.0).contains(
                    &self
                        .strategy
                        .maker_rebate_mm
                        .ctf_remaining_leg_time_risk_weight,
            ),
            "strategy.maker_rebate_mm.ctf_remaining_leg_time_risk_weight must be finite and in [0, 1]"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_sell_limit_delay_cushion_ticks
                .is_finite()
                && self
                    .strategy
                    .maker_rebate_mm
                    .ctf_sell_limit_delay_cushion_ticks
                    >= 1.0,
            "strategy.maker_rebate_mm.ctf_sell_limit_delay_cushion_ticks must be finite and >= 1"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_first_managed_sell_min_market_confirmation
                .is_finite()
                && (0.0..=1.0).contains(
                    &self
                        .strategy
                        .maker_rebate_mm
                        .ctf_first_managed_sell_min_market_confirmation,
                ),
            "strategy.maker_rebate_mm.ctf_first_managed_sell_min_market_confirmation must be finite and in [0, 1]"
        );
        ensure!(
            [
                self.strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_probability_weight,
                self.strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_spread_weight,
                self.strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_depth_weight,
                self.strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_book_weight,
                self.strategy.maker_rebate_mm.oracle_ta_boundary_weight,
                self.strategy
                    .maker_rebate_mm
                    .oracle_ta_token_agreement_weight,
                self.strategy
                    .maker_rebate_mm
                    .oracle_ta_component_alignment_weight,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0),
            "strategy.maker_rebate_mm oracle market-confirmation and TA weights must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_market_confirmation_probability_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_spread_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_depth_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .oracle_market_confirmation_book_weight
                > 0.0,
            "strategy.maker_rebate_mm oracle market-confirmation weights must sum to > 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.oracle_ta_boundary_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .oracle_ta_token_agreement_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .oracle_ta_component_alignment_weight
                > 0.0,
            "strategy.maker_rebate_mm oracle TA weights must sum to > 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .oracle_ta_boundary_scale
                .is_finite()
                && self.strategy.maker_rebate_mm.oracle_ta_boundary_scale > 0.0,
            "strategy.maker_rebate_mm.oracle_ta_boundary_scale must be finite and > 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_probability_calibration_min_confidence
                .is_finite()
                && (0.0..=1.0).contains(
                    &self
                        .strategy
                        .maker_rebate_mm
                        .ctf_probability_calibration_min_confidence,
                ),
            "strategy.maker_rebate_mm.ctf_probability_calibration_min_confidence must be finite and in [0, 1]"
        );
        ensure!(
            [
                self.strategy
                    .maker_rebate_mm
                    .ctf_probability_confidence_market_weight,
                self.strategy
                    .maker_rebate_mm
                    .ctf_probability_confidence_spread_weight,
                self.strategy
                    .maker_rebate_mm
                    .ctf_probability_confidence_depth_weight,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0),
            "strategy.maker_rebate_mm CTF probability confidence weights must be finite and >= 0"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .ctf_probability_confidence_market_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .ctf_probability_confidence_spread_weight
                + self
                    .strategy
                    .maker_rebate_mm
                    .ctf_probability_confidence_depth_weight
                > 0.0,
            "strategy.maker_rebate_mm CTF probability confidence weights must sum to > 0"
        );
        ensure!(
            !self
                .strategy
                .maker_rebate_mm
                .markout_window_weights
                .is_empty(),
            "strategy.maker_rebate_mm.markout_window_weights must not be empty"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .markout_window_weights
                .iter()
                .all(|w| w.is_finite() && *w >= 0.0),
            "strategy.maker_rebate_mm.markout_window_weights entries must be finite and >= 0"
        );
        ensure!(
            self.strategy.maker_rebate_mm.markout_window_weights.len()
                == self.strategy.maker_rebate_mm.markout_windows_ms.len(),
            "strategy.maker_rebate_mm.markout_window_weights length must match markout_windows_ms"
        );
        ensure!(
            self.strategy
                .maker_rebate_mm
                .enabled_assets
                .iter()
                .all(|a| VALID_ASSETS.iter().any(|v| v.eq_ignore_ascii_case(a))),
            "strategy.maker_rebate_mm.enabled_assets entries must be one of {VALID_ASSETS:?}"
        );
        Ok(())
    }
}

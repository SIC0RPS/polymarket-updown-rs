use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, BTreeSet};
mod fair_value_config;
mod wire;
pub use fair_value_config::CurrentBlendedFairValueConfig;
pub(crate) use wire::MakerRebateMmStrategyConfigWire;

pub const VALID_ASSETS: &[&str] = &["BTC", "ETH", "SOL", "XRP"];

#[derive(Debug, Clone, Serialize)]
pub struct MakerRebateMmStrategyConfig {
    pub oracle_buy_and_hold: bool,
    pub max_orders_per_cycle: usize,
    pub normal_rebalance_interval_ms: u64,
    pub fast_rebalance_interval_ms: u64,
    pub base_half_spread: f64,
    pub ranking_book_max_age_ms: u64,
    pub paired_first_managed_sell_book_max_age_ms: u64,
    pub max_oracle_staleness_ms: u64,
    pub entry_cutoff_before_expiry_ms: u64,
    pub ewma_half_life_ms: u64,
    pub oracle_shock_bps: f64,
    pub max_inventory_per_token: f64,
    pub max_inventory_total: f64,
    pub markout_windows_ms: Vec<u64>,
    pub admission_buffer_ticks: u8,
    /// Minimum rest window for passive quotes before normal reconsideration.
    pub passive_min_rest_ms: u64,
    /// Cooldown after a post-only cross reject during which extra maker buffer applies.
    pub post_only_reject_cooldown_ms: u64,
    /// Max extra maker-buffer ticks applied while a post-only cross-reject streak is active.
    pub post_only_reject_max_extra_buffer_ticks: u8,
    /// Size multiplier applied while a post-only cross-reject streak is active.
    pub post_only_reject_size_multiplier: f64,
    /// Fraction below the current bid used for the route-price floor (0.05 = 5%),
    /// subject to price bounds and tick rounding.
    pub paired_first_managed_sell_slippage_tolerance: f64,
    /// Price-unit offset below the current bid (0.03 = 3 cents), at least one tick
    /// and subject to the route tolerance. Depth-based execution pricing applies
    /// its separate delay cushion.
    pub paired_first_managed_sell_aggressiveness: f64,
    /// Minimum positive carry advantage required before selecting an oracle side.
    pub oracle_side_min_advantage: f64,
    /// Extra advantage required to flip oracle side away from the current side.
    pub oracle_side_flip_hysteresis: f64,
    /// Weight applied to rolling advantage noise when building entry thresholds.
    pub oracle_side_noise_weight: f64,
    /// Extra rolling-noise weight applied when building flip thresholds.
    pub oracle_side_flip_noise_weight: f64,
    /// Extra threshold added while asymmetric cleanup is active.
    pub oracle_side_cleanup_flip_bump: f64,
    /// Probability-agreement weight in oracle market-confirmation scoring.
    pub oracle_market_confirmation_probability_weight: f64,
    /// Spread-quality weight in oracle market-confirmation scoring.
    pub oracle_market_confirmation_spread_weight: f64,
    /// Depth-quality weight in oracle market-confirmation scoring.
    pub oracle_market_confirmation_depth_weight: f64,
    /// Book-completeness weight in oracle market-confirmation scoring.
    pub oracle_market_confirmation_book_weight: f64,
    /// Volatility-normalized boundary move scale in oracle TA alignment.
    pub oracle_ta_boundary_scale: f64,
    /// Boundary-move weight in oracle TA alignment.
    pub oracle_ta_boundary_weight: f64,
    /// Model/market token-agreement weight in oracle TA alignment.
    pub oracle_ta_token_agreement_weight: f64,
    /// Closed TA component consensus weight in oracle TA alignment.
    pub oracle_ta_component_alignment_weight: f64,
    /// Minimum continuous confidence for the CTF economics owner to treat the likely-winner
    /// side as identified.
    pub ctf_likely_winner_min_confidence: f64,
    /// Minimum same-side model and market probability edge required to prove a hold.
    pub min_hold_probability_edge: f64,
    /// Weight applied to remaining-leg liquidity drawdown in CTF owner economics.
    pub ctf_remaining_leg_liquidity_drawdown_weight: f64,
    /// Weight applied to remaining-leg time-to-window-end risk in CTF owner economics.
    pub ctf_remaining_leg_time_risk_weight: f64,
    /// Tick cushion below marginal executable bid used for CTF FOK/FAK sell worst-limit durability.
    pub ctf_sell_limit_delay_cushion_ticks: f64,
    /// Minimum market confirmation required before paired first managed sell release or loss-cut.
    pub ctf_first_managed_sell_min_market_confirmation: f64,
    /// Minimum confidence floor used by CTF redeem probability calibration.
    pub ctf_probability_calibration_min_confidence: f64,
    /// Market-confirmation weight in CTF probability confidence.
    pub ctf_probability_confidence_market_weight: f64,
    /// Spread-quality weight in CTF probability confidence.
    pub ctf_probability_confidence_spread_weight: f64,
    /// Depth-quality weight in CTF probability confidence.
    pub ctf_probability_confidence_depth_weight: f64,
    /// Weight applied when market price supports the selected side more than the model while the loser leg is expensive.
    pub ctf_probability_model_market_loser_bid_ambiguity_weight: f64,
    /// Live short-horizon probability model used by side selection and CTF exit economics.
    pub live_fair_value_model: CurrentBlendedFairValueConfig,
    /// Minimum time winner protection must remain eligible before promotion.
    pub protection_dwell_ms: u64,
    /// Weights for markout window aggregation (short, mid, long). Must match markout_windows_ms length.
    pub markout_window_weights: Vec<f64>,
    /// Which underlyings to quote. Empty = all. Valid: "BTC", "ETH", "SOL", "XRP".
    pub enabled_assets: Vec<String>,
    /// Per-symbol optimizer knobs for the one strategy implementation.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub optimizer_symbol_configs: BTreeMap<String, Box<MakerRebateMmStrategyConfig>>,
}

impl MakerRebateMmStrategyConfig {
    /// Returns true if the given underlying (e.g. "btc") is enabled for quoting.
    /// An empty `enabled_assets` list means all assets are enabled.
    pub fn is_asset_enabled(&self, underlying: &str) -> bool {
        let upper = underlying.to_ascii_uppercase();
        if !self.optimizer_symbol_configs.is_empty()
            && !self.optimizer_symbol_configs.contains_key(&upper)
        {
            return false;
        }
        if self.enabled_assets.is_empty() {
            return true;
        }
        self.enabled_assets
            .iter()
            .any(|a| a.eq_ignore_ascii_case(&upper))
    }

    pub fn config_for_symbol(&self, symbol: &str) -> &MakerRebateMmStrategyConfig {
        if self.optimizer_symbol_configs.is_empty() {
            return self;
        }
        self.optimizer_symbol_configs
            .get(&symbol.to_ascii_uppercase())
            .map(Box::as_ref)
            .expect("missing required optimizer symbol config")
    }

    /// Actual cutoff for new entry quoting before market expiry.
    pub fn hard_entry_gate_ms(&self) -> u64 {
        self.entry_cutoff_before_expiry_ms
    }

    /// Validate fields that are safe to change at runtime (subset of the full
    /// AppConfig::validate). Returns an error string for the first invalid field.
    pub fn validate_runtime(&self) -> Result<()> {
        self.validate_runtime_fields()?;
        self.validate_live_asset_scope()
    }

    pub(crate) fn validate_symbol_runtime(&self, expected_symbol: &str) -> Result<()> {
        self.validate_runtime_fields()?;
        ensure!(
            VALID_ASSETS.contains(&expected_symbol),
            "symbol runtime expected asset '{expected_symbol}' is not canonical, valid: {VALID_ASSETS:?}"
        );
        ensure!(
            self.optimizer_symbol_configs.is_empty(),
            "symbol runtime config must not contain nested optimizer symbol configs"
        );
        ensure!(
            self.enabled_assets.len() == 1 && self.enabled_assets[0] == expected_symbol,
            "symbol runtime enabled_assets must contain exactly expected asset '{expected_symbol}'"
        );
        Ok(())
    }

    fn validate_runtime_fields(&self) -> Result<()> {
        ensure!(
            self.max_orders_per_cycle > 0,
            "max_orders_per_cycle must be > 0"
        );
        ensure!(
            self.normal_rebalance_interval_ms > 0,
            "normal_rebalance_interval_ms must be > 0"
        );
        ensure!(
            self.fast_rebalance_interval_ms > 0,
            "fast_rebalance_interval_ms must be > 0"
        );
        ensure!(
            self.base_half_spread >= 0.0,
            "base_half_spread must be >= 0"
        );
        ensure!(
            self.ranking_book_max_age_ms > 0,
            "ranking_book_max_age_ms must be > 0"
        );
        ensure!(
            self.paired_first_managed_sell_book_max_age_ms > 0,
            "paired_first_managed_sell_book_max_age_ms must be > 0"
        );
        ensure!(
            self.max_oracle_staleness_ms > 0,
            "max_oracle_staleness_ms must be > 0"
        );
        ensure!(self.ewma_half_life_ms > 0, "ewma_half_life_ms must be > 0");
        ensure!(
            self.oracle_shock_bps >= 0.0,
            "oracle_shock_bps must be >= 0"
        );
        ensure!(
            self.max_inventory_per_token >= 0.0,
            "max_inventory_per_token must be >= 0"
        );
        ensure!(
            self.max_inventory_total >= 0.0,
            "max_inventory_total must be >= 0"
        );
        ensure!(
            self.post_only_reject_size_multiplier.is_finite()
                && (0.0..=1.0).contains(&self.post_only_reject_size_multiplier),
            "post_only_reject_size_multiplier must be finite and in [0, 1]"
        );
        ensure!(
            self.paired_first_managed_sell_slippage_tolerance >= 0.0
                && self
                    .paired_first_managed_sell_slippage_tolerance
                    .is_finite(),
            "paired_first_managed_sell_slippage_tolerance must be finite and >= 0"
        );
        ensure!(
            self.paired_first_managed_sell_aggressiveness >= 0.0
                && self.paired_first_managed_sell_aggressiveness.is_finite(),
            "paired_first_managed_sell_aggressiveness must be finite and >= 0"
        );
        ensure!(
            self.oracle_side_min_advantage >= 0.0 && self.oracle_side_min_advantage.is_finite(),
            "oracle_side_min_advantage must be finite and >= 0"
        );
        ensure!(
            self.oracle_side_flip_hysteresis >= 0.0 && self.oracle_side_flip_hysteresis.is_finite(),
            "oracle_side_flip_hysteresis must be finite and >= 0"
        );
        ensure!(
            self.oracle_side_noise_weight >= 0.0 && self.oracle_side_noise_weight.is_finite(),
            "oracle_side_noise_weight must be finite and >= 0"
        );
        ensure!(
            self.oracle_side_flip_noise_weight >= 0.0
                && self.oracle_side_flip_noise_weight.is_finite(),
            "oracle_side_flip_noise_weight must be finite and >= 0"
        );
        ensure!(
            self.oracle_side_cleanup_flip_bump >= 0.0
                && self.oracle_side_cleanup_flip_bump.is_finite(),
            "oracle_side_cleanup_flip_bump must be finite and >= 0"
        );
        ensure!(
            [
                self.oracle_market_confirmation_probability_weight,
                self.oracle_market_confirmation_spread_weight,
                self.oracle_market_confirmation_depth_weight,
                self.oracle_market_confirmation_book_weight,
                self.oracle_ta_boundary_weight,
                self.oracle_ta_token_agreement_weight,
                self.oracle_ta_component_alignment_weight,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0),
            "oracle market-confirmation and TA weights must be finite and >= 0"
        );
        ensure!(
            self.oracle_market_confirmation_probability_weight
                + self.oracle_market_confirmation_spread_weight
                + self.oracle_market_confirmation_depth_weight
                + self.oracle_market_confirmation_book_weight
                > 0.0,
            "oracle market-confirmation weights must sum to > 0"
        );
        ensure!(
            self.oracle_ta_boundary_weight
                + self.oracle_ta_token_agreement_weight
                + self.oracle_ta_component_alignment_weight
                > 0.0,
            "oracle TA weights must sum to > 0"
        );
        ensure!(
            self.oracle_ta_boundary_scale.is_finite() && self.oracle_ta_boundary_scale > 0.0,
            "oracle_ta_boundary_scale must be finite and > 0"
        );
        ensure!(
            self.ctf_likely_winner_min_confidence.is_finite()
                && (0.0..=1.0).contains(&self.ctf_likely_winner_min_confidence),
            "ctf_likely_winner_min_confidence must be finite and in [0, 1]"
        );
        ensure!(
            self.min_hold_probability_edge.is_finite()
                && (0.0..=1.0).contains(&self.min_hold_probability_edge),
            "min_hold_probability_edge must be finite and in [0, 1]"
        );
        ensure!(
            self.ctf_remaining_leg_liquidity_drawdown_weight.is_finite()
                && (0.0..=1.0).contains(&self.ctf_remaining_leg_liquidity_drawdown_weight),
            "ctf_remaining_leg_liquidity_drawdown_weight must be finite and in [0, 1]"
        );
        ensure!(
            self.ctf_remaining_leg_time_risk_weight.is_finite()
                && (0.0..=1.0).contains(&self.ctf_remaining_leg_time_risk_weight),
            "ctf_remaining_leg_time_risk_weight must be finite and in [0, 1]"
        );
        ensure!(
            self.ctf_sell_limit_delay_cushion_ticks.is_finite()
                && self.ctf_sell_limit_delay_cushion_ticks >= 1.0,
            "ctf_sell_limit_delay_cushion_ticks must be finite and >= 1"
        );
        ensure!(
            self.ctf_first_managed_sell_min_market_confirmation
                .is_finite()
                && (0.0..=1.0).contains(&self.ctf_first_managed_sell_min_market_confirmation),
            "ctf_first_managed_sell_min_market_confirmation must be finite and in [0, 1]"
        );
        ensure!(
            self.ctf_probability_calibration_min_confidence.is_finite()
                && (0.0..=1.0).contains(&self.ctf_probability_calibration_min_confidence),
            "ctf_probability_calibration_min_confidence must be finite and in [0, 1]"
        );
        ensure!(
            [
                self.ctf_probability_confidence_market_weight,
                self.ctf_probability_confidence_spread_weight,
                self.ctf_probability_confidence_depth_weight,
                self.ctf_probability_model_market_loser_bid_ambiguity_weight,
            ]
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0),
            "CTF probability confidence weights must be finite and >= 0"
        );
        ensure!(
            self.ctf_probability_confidence_market_weight
                + self.ctf_probability_confidence_spread_weight
                + self.ctf_probability_confidence_depth_weight
                > 0.0,
            "CTF probability confidence weights must sum to > 0"
        );
        ensure!(
            self.markout_windows_ms.iter().all(|v| *v > 0),
            "markout_windows_ms entries must be > 0"
        );
        ensure!(
            self.markout_windows_ms.len() >= 3,
            "markout_windows_ms must include at least 3 windows"
        );
        self.live_fair_value_model
            .validate("live_fair_value_model")
            .context("fair-value current config invalid")?;
        ensure!(
            !self.markout_window_weights.is_empty(),
            "markout_window_weights must not be empty"
        );
        ensure!(
            self.markout_window_weights
                .iter()
                .all(|w| w.is_finite() && *w >= 0.0),
            "markout_window_weights entries must be finite and >= 0"
        );
        Ok(())
    }

    fn validate_live_asset_scope(&self) -> Result<()> {
        for a in &self.enabled_assets {
            ensure!(
                VALID_ASSETS.iter().any(|v| v.eq_ignore_ascii_case(a)),
                "enabled_assets: unknown asset '{a}', valid: {VALID_ASSETS:?}"
            );
        }
        if !self.optimizer_symbol_configs.is_empty() {
            let configured_assets = self
                .optimizer_symbol_configs
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let required_assets = VALID_ASSETS
                .iter()
                .map(|symbol| (*symbol).to_string())
                .collect::<BTreeSet<_>>();
            ensure!(
                configured_assets == required_assets,
                "optimizer_symbol_configs must contain exactly {VALID_ASSETS:?}, got {configured_assets:?}"
            );
        }
        for (symbol, cfg) in &self.optimizer_symbol_configs {
            ensure!(
                VALID_ASSETS
                    .iter()
                    .any(|valid| valid.eq_ignore_ascii_case(symbol)),
                "optimizer_symbol_configs: unknown asset '{symbol}', valid: {VALID_ASSETS:?}"
            );
            ensure!(
                cfg.optimizer_symbol_configs.is_empty(),
                "optimizer_symbol_configs.{symbol} must not contain nested symbol configs"
            );
            cfg.validate_runtime()
                .with_context(|| format!("optimizer_symbol_configs.{symbol} invalid"))?;
        }
        Ok(())
    }
}

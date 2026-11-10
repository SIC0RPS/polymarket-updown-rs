use super::*;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct MakerRebateMmStrategyConfigWire {
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
    pub passive_min_rest_ms: u64,
    pub post_only_reject_cooldown_ms: u64,
    pub post_only_reject_max_extra_buffer_ticks: u8,
    pub post_only_reject_size_multiplier: f64,
    pub paired_first_managed_sell_slippage_tolerance: f64,
    pub paired_first_managed_sell_aggressiveness: f64,
    pub oracle_side_min_advantage: f64,
    pub oracle_side_flip_hysteresis: f64,
    pub oracle_side_noise_weight: f64,
    pub oracle_side_flip_noise_weight: f64,
    pub oracle_side_cleanup_flip_bump: f64,
    pub oracle_market_confirmation_probability_weight: f64,
    pub oracle_market_confirmation_spread_weight: f64,
    pub oracle_market_confirmation_depth_weight: f64,
    pub oracle_market_confirmation_book_weight: f64,
    pub oracle_ta_boundary_scale: f64,
    pub oracle_ta_boundary_weight: f64,
    pub oracle_ta_token_agreement_weight: f64,
    pub oracle_ta_component_alignment_weight: f64,
    pub ctf_likely_winner_min_confidence: f64,
    pub min_hold_probability_edge: f64,
    pub ctf_remaining_leg_liquidity_drawdown_weight: f64,
    pub ctf_remaining_leg_time_risk_weight: f64,
    pub ctf_sell_limit_delay_cushion_ticks: f64,
    pub ctf_first_managed_sell_min_market_confirmation: f64,
    pub ctf_probability_calibration_min_confidence: f64,
    pub ctf_probability_confidence_market_weight: f64,
    pub ctf_probability_confidence_spread_weight: f64,
    pub ctf_probability_confidence_depth_weight: f64,
    pub ctf_probability_model_market_loser_bid_ambiguity_weight: f64,
    pub live_fair_value_model: CurrentBlendedFairValueConfig,
    pub protection_dwell_ms: u64,
    pub markout_window_weights: Vec<f64>,
    pub enabled_assets: Vec<String>,
}

impl<'de> Deserialize<'de> for MakerRebateMmStrategyConfig {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        serde_json::from_value::<MakerRebateMmStrategyConfigWire>(value)
            .map(MakerRebateMmStrategyConfig::from)
            .map_err(serde::de::Error::custom)
    }
}

impl From<MakerRebateMmStrategyConfigWire> for MakerRebateMmStrategyConfig {
    fn from(wire: MakerRebateMmStrategyConfigWire) -> Self {
        Self {
            oracle_buy_and_hold: wire.oracle_buy_and_hold,
            max_orders_per_cycle: wire.max_orders_per_cycle,
            normal_rebalance_interval_ms: wire.normal_rebalance_interval_ms,
            fast_rebalance_interval_ms: wire.fast_rebalance_interval_ms,
            base_half_spread: wire.base_half_spread,
            ranking_book_max_age_ms: wire.ranking_book_max_age_ms,
            paired_first_managed_sell_book_max_age_ms: wire
                .paired_first_managed_sell_book_max_age_ms,
            max_oracle_staleness_ms: wire.max_oracle_staleness_ms,
            entry_cutoff_before_expiry_ms: wire.entry_cutoff_before_expiry_ms,
            ewma_half_life_ms: wire.ewma_half_life_ms,
            oracle_shock_bps: wire.oracle_shock_bps,
            max_inventory_per_token: wire.max_inventory_per_token,
            max_inventory_total: wire.max_inventory_total,
            markout_windows_ms: wire.markout_windows_ms,
            admission_buffer_ticks: wire.admission_buffer_ticks,
            passive_min_rest_ms: wire.passive_min_rest_ms,
            post_only_reject_cooldown_ms: wire.post_only_reject_cooldown_ms,
            post_only_reject_max_extra_buffer_ticks: wire.post_only_reject_max_extra_buffer_ticks,
            post_only_reject_size_multiplier: wire.post_only_reject_size_multiplier,
            paired_first_managed_sell_slippage_tolerance: wire
                .paired_first_managed_sell_slippage_tolerance,
            paired_first_managed_sell_aggressiveness: wire.paired_first_managed_sell_aggressiveness,
            oracle_side_min_advantage: wire.oracle_side_min_advantage,
            oracle_side_flip_hysteresis: wire.oracle_side_flip_hysteresis,
            oracle_side_noise_weight: wire.oracle_side_noise_weight,
            oracle_side_flip_noise_weight: wire.oracle_side_flip_noise_weight,
            oracle_side_cleanup_flip_bump: wire.oracle_side_cleanup_flip_bump,
            oracle_market_confirmation_probability_weight: wire
                .oracle_market_confirmation_probability_weight,
            oracle_market_confirmation_spread_weight: wire.oracle_market_confirmation_spread_weight,
            oracle_market_confirmation_depth_weight: wire.oracle_market_confirmation_depth_weight,
            oracle_market_confirmation_book_weight: wire.oracle_market_confirmation_book_weight,
            oracle_ta_boundary_scale: wire.oracle_ta_boundary_scale,
            oracle_ta_boundary_weight: wire.oracle_ta_boundary_weight,
            oracle_ta_token_agreement_weight: wire.oracle_ta_token_agreement_weight,
            oracle_ta_component_alignment_weight: wire.oracle_ta_component_alignment_weight,
            ctf_likely_winner_min_confidence: wire.ctf_likely_winner_min_confidence,
            min_hold_probability_edge: wire.min_hold_probability_edge,
            ctf_remaining_leg_liquidity_drawdown_weight: wire
                .ctf_remaining_leg_liquidity_drawdown_weight,
            ctf_remaining_leg_time_risk_weight: wire.ctf_remaining_leg_time_risk_weight,
            ctf_sell_limit_delay_cushion_ticks: wire.ctf_sell_limit_delay_cushion_ticks,
            ctf_first_managed_sell_min_market_confirmation: wire
                .ctf_first_managed_sell_min_market_confirmation,
            ctf_probability_calibration_min_confidence: wire
                .ctf_probability_calibration_min_confidence,
            ctf_probability_confidence_market_weight: wire.ctf_probability_confidence_market_weight,
            ctf_probability_confidence_spread_weight: wire.ctf_probability_confidence_spread_weight,
            ctf_probability_confidence_depth_weight: wire.ctf_probability_confidence_depth_weight,
            ctf_probability_model_market_loser_bid_ambiguity_weight: wire
                .ctf_probability_model_market_loser_bid_ambiguity_weight,
            live_fair_value_model: wire.live_fair_value_model,
            protection_dwell_ms: wire.protection_dwell_ms,
            markout_window_weights: wire.markout_window_weights,
            enabled_assets: wire.enabled_assets,
            optimizer_symbol_configs: BTreeMap::new(),
        }
    }
}

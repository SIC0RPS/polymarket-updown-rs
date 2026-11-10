mod calibration;
pub(crate) mod features;
mod final_probability;
mod learning;
mod logistic;
mod market_calibration;
mod microstructure;
mod probability_calibration;
mod probability_evidence;
pub(crate) mod probability_interval;
mod selection;
mod snapshot_build;
pub(crate) mod ta;
use crate::oracle_resolution::{ChainlinkResolutionPrice, OracleResolvedLabel};
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::polymarket::state::BookTop;
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::paired_maker::{OracleTaFeatureCacheEntry, OracleTaFeatureCacheKey};
use crate::strategy::support::oracle_authority::features::{
    OracleDecisionInput, OracleDecisionScoreConfig,
};
use crate::strategy::support::oracle_authority::logistic::OracleMicrostructureLogisticScore;
pub(crate) use crate::strategy::support::oracle_authority::logistic::OracleMicrostructureLogisticTrainer;
use crate::strategy::support::oracle_authority::market_calibration::OracleMarketCalibrationScore;
pub(crate) use crate::strategy::support::oracle_authority::market_calibration::OracleMarketCalibrationTrainer;
use crate::strategy::support::oracle_authority::microstructure::OracleMicrostructureFeatures;
use crate::strategy::support::oracle_authority::selection::{
    advantage_noise_step, ewma_alpha, ewma_step, oracle_emission_thresholds,
    oracle_side_confidence, select_oracle_side, OracleEmissionEvidence,
    OracleEmissionThresholdInput, OracleSelectionInput,
};
use crate::strategy::support::oracle_authority::ta::OracleTaFeatures;
use calibration::OracleBetaCalibrationScore;
pub(crate) use calibration::OracleBetaCalibrationTrainer;
pub(crate) use features::decision_ta_timeframe_for_market_window_ms;
pub use learning::{OracleLearningMetrics, OracleLearningSnapshot};
pub(crate) use ta::{OracleTaBar, OracleTaFeatureCalculator};
const ORACLE_CANDLE_TA_MAX_BARS: usize = 1_000;
const ORACLE_TA_FEATURE_CACHE_MAX_ENTRIES: usize = 4_096;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OracleSideReason {
    NoMarketData,
    NoActionableEdge,
    PreserveUp,
    PreserveDown,
    SelectUp,
    SelectDown,
    FlipToUp,
    FlipToDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OracleAuthorityMode {
    #[default]
    Market,
    Cleanup {
        remaining_owned_side: PairTokenSide,
    },
}
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleAuthoritySnapshot {
    pub(crate) controller_mode: OracleAuthorityMode,
    pub(crate) cleanup_anchor_side: Option<PairTokenSide>,
    pub(crate) last_eval_ts_ms: Option<u64>,
    pub(crate) p_up: Option<f64>,
    pub(crate) p_up_lower_bound: Option<f64>,
    pub(crate) p_up_upper_bound: Option<f64>,
    pub(crate) model_p_up: Option<f64>,
    pub(crate) market_p_up: Option<f64>,
    pub(crate) market_raw_p_up: Option<f64>,
    pub(crate) base_p_fair: Option<f64>,
    pub(crate) model_weight: Option<f64>,
    pub(crate) unwind_up: Option<f64>,
    pub(crate) unwind_down: Option<f64>,
    pub(crate) adv_up: Option<f64>,
    pub(crate) adv_down: Option<f64>,
    pub(crate) adv_up_ewma: Option<f64>,
    pub(crate) adv_down_ewma: Option<f64>,
    pub(crate) adv_up_lcb_ewma: Option<f64>,
    pub(crate) adv_up_ucb_ewma: Option<f64>,
    pub(crate) adv_down_lcb_ewma: Option<f64>,
    pub(crate) adv_down_ucb_ewma: Option<f64>,
    pub(crate) adv_noise: Option<f64>,
    pub(crate) uncertainty_edge_threshold: Option<f64>,
    pub(crate) uncertainty_edge_weight: Option<f64>,
    pub(crate) remaining_owned_adv: Option<f64>,
    pub(crate) remaining_owned_adv_ewma: Option<f64>,
    pub(crate) remaining_owned_noise_ewma: Option<f64>,
    pub(crate) theta_enter_up: Option<f64>,
    pub(crate) theta_enter_down: Option<f64>,
    pub(crate) theta_release_cleanup: Option<f64>,
    pub(crate) theta_flip: Option<f64>,
    pub(crate) theta_escape: Option<f64>,
    pub(crate) ta_signal_alignment: Option<f64>,
    pub(crate) oracle_signal_side: Option<PairTokenSide>,
    pub(crate) oracle_side_confidence: Option<f64>,
    pub(crate) oracle_side_reason: Option<OracleSideReason>,
    pub(crate) oracle_data_issue: Option<&'static str>,
    pub(crate) p_fair: Option<f64>,
    pub(crate) calibration_confidence: Option<f64>,
    pub(crate) calibration_center: Option<f64>,
    pub(crate) calibration_market_center_weight: Option<f64>,
    pub(crate) calibration_spread_quality: Option<f64>,
    pub(crate) calibration_depth_quality: Option<f64>,
    pub(crate) beta_calibration_enabled: bool,
    pub(crate) beta_calibration_input_p: Option<f64>,
    pub(crate) beta_calibration_p_fair: Option<f64>,
    pub(crate) beta_calibration_a: Option<f64>,
    pub(crate) beta_calibration_b: Option<f64>,
    pub(crate) beta_calibration_c: Option<f64>,
    pub(crate) beta_calibration_sample_count: Option<u32>,
    pub(crate) beta_calibration_positive_count: Option<u32>,
    pub(crate) beta_calibration_brier_score: Option<f64>,
    pub(crate) beta_calibration_log_loss: Option<f64>,
    pub(crate) beta_calibration_uncertainty: Option<f64>,
    pub(crate) market_calibration_enabled: bool,
    pub(crate) market_calibration_a: Option<f64>,
    pub(crate) market_calibration_b: Option<f64>,
    pub(crate) market_calibration_c: Option<f64>,
    pub(crate) market_calibration_sample_count: Option<u32>,
    pub(crate) market_calibration_positive_count: Option<u32>,
    pub(crate) market_calibration_brier_score: Option<f64>,
    pub(crate) market_calibration_log_loss: Option<f64>,
    pub(crate) market_calibration_uncertainty: Option<f64>,
    pub(crate) depth_pressure: Option<f64>,
    pub(crate) micro_queue_imbalance_l1: Option<f64>,
    pub(crate) micro_queue_imbalance_l5: Option<f64>,
    pub(crate) micro_ofi_depth: Option<f64>,
    pub(crate) micro_microprice_gap_ticks: Option<f64>,
    pub(crate) micro_signed_return_1s: Option<f64>,
    pub(crate) micro_signed_return_5s: Option<f64>,
    pub(crate) micro_volatility_state: Option<f64>,
    pub(crate) micro_jump_proxy: Option<f64>,
    pub(crate) micro_periodic_hour_sin: Option<f64>,
    pub(crate) micro_periodic_hour_cos: Option<f64>,
    pub(crate) micro_periodic_minute_sin: Option<f64>,
    pub(crate) micro_periodic_minute_cos: Option<f64>,
    pub(crate) micro_feature_availability: Option<f64>,
    pub(crate) micro_logistic_enabled: bool,
    pub(crate) micro_logistic_p_up: Option<f64>,
    pub(crate) micro_logistic_logit: Option<f64>,
    pub(crate) micro_logistic_feature_count: Option<u32>,
    pub(crate) micro_logistic_l2_penalty: Option<f64>,
    pub(crate) micro_logistic_fit_sample_count: Option<u32>,
    pub(crate) micro_logistic_fit_positive_count: Option<u32>,
    pub(crate) micro_logistic_fit_residual_trust_alpha: Option<f64>,
    pub(crate) micro_logistic_fit_brier_score: Option<f64>,
    pub(crate) micro_logistic_fit_log_loss: Option<f64>,
    pub(crate) micro_logistic_fit_prior_brier_score: Option<f64>,
    pub(crate) micro_logistic_fit_prior_log_loss: Option<f64>,
    pub(crate) micro_logistic_fit_candidate_brier_score: Option<f64>,
    pub(crate) micro_logistic_fit_candidate_log_loss: Option<f64>,
    pub(crate) micro_logistic_fit_prequentially_trusted: Option<bool>,
    pub(crate) micro_logistic_fit_weight_norm: Option<f64>,
    pub(crate) micro_logistic_fit_weights: Option<[f64; 8]>,
    pub(crate) micro_logistic_residual_logit_std: Option<f64>,
    pub(crate) micro_logistic_residual_logit_std_unavailable_reason: Option<&'static str>,
    pub(crate) micro_logistic_market_calibration_logit_std: Option<f64>,
    pub(crate) top_up_bid: Option<f64>,
    pub(crate) top_down_bid: Option<f64>,
    pub(crate) top_up_bid_delta: Option<f64>,
    pub(crate) top_down_bid_delta: Option<f64>,
    pub(crate) market_confirmation: Option<f64>,
    pub(crate) market_confirmation_probability_agreement: Option<f64>,
    pub(crate) market_confirmation_spread_quality: Option<f64>,
    pub(crate) market_confirmation_depth_support: Option<f64>,
    pub(crate) market_confirmation_book_complete: Option<f64>,
    pub(crate) ta_alignment: Option<f64>,
    pub(crate) ta_ema_trend: Option<f64>,
    pub(crate) ta_macd_trend: Option<f64>,
    pub(crate) ta_rsi_regime: Option<f64>,
    pub(crate) ta_bollinger_position: Option<f64>,
    pub(crate) ta_recent_return_sign: Option<f64>,
    pub(crate) ta_drift_score: Option<f64>,
    pub(crate) ta_bar_ts_ms: Option<u64>,
    pub(crate) ta_closed_bars_in_market: Option<u32>,
    pub(crate) pnl_risk_multiplier: Option<f64>,
    pub(crate) predictive_first_managed_sell_allowed: bool,
    pub(crate) risk_pause_recommended: bool,
}
impl OracleAuthoritySnapshot {
    pub(crate) fn apply_microstructure(&mut self, features: OracleMicrostructureFeatures) {
        self.micro_queue_imbalance_l1 = features.queue_imbalance_l1;
        self.micro_queue_imbalance_l5 = features.queue_imbalance_l5;
        self.micro_ofi_depth = features.ofi_depth;
        self.micro_microprice_gap_ticks = features.microprice_gap_ticks;
        self.micro_signed_return_1s = features.signed_return_1s;
        self.micro_signed_return_5s = features.signed_return_5s;
        self.micro_volatility_state = features.volatility_state;
        self.micro_jump_proxy = features.jump_proxy;
        self.micro_periodic_hour_sin = features.periodic_hour_sin;
        self.micro_periodic_hour_cos = features.periodic_hour_cos;
        self.micro_periodic_minute_sin = features.periodic_minute_sin;
        self.micro_periodic_minute_cos = features.periodic_minute_cos;
        self.micro_feature_availability = Some(features.availability);
    }

    pub(crate) fn apply_micro_logistic(&mut self, score: OracleMicrostructureLogisticScore) {
        self.micro_logistic_enabled = score.enabled;
        self.micro_logistic_p_up = score.p_up;
        self.micro_logistic_logit = score.logit;
        self.micro_logistic_feature_count = score.feature_count;
        self.micro_logistic_l2_penalty = score.l2_penalty;
        self.micro_logistic_fit_sample_count = score.fit_sample_count;
        self.micro_logistic_fit_positive_count = score.fit_positive_count;
        self.micro_logistic_fit_residual_trust_alpha = score.fit_residual_trust_alpha;
        self.micro_logistic_fit_brier_score = score.fit_brier_score;
        self.micro_logistic_fit_log_loss = score.fit_log_loss;
        self.micro_logistic_fit_prior_brier_score = score.fit_prior_brier_score;
        self.micro_logistic_fit_prior_log_loss = score.fit_prior_log_loss;
        self.micro_logistic_fit_candidate_brier_score = score.fit_candidate_brier_score;
        self.micro_logistic_fit_candidate_log_loss = score.fit_candidate_log_loss;
        self.micro_logistic_fit_prequentially_trusted = score.fit_prequentially_trusted;
        self.micro_logistic_fit_weight_norm = score.fit_weight_norm;
        self.micro_logistic_fit_weights = score.fit_weights;
        self.micro_logistic_residual_logit_std = score.residual_logit_std;
        self.micro_logistic_residual_logit_std_unavailable_reason =
            score.residual_logit_std_unavailable_reason;
        self.micro_logistic_market_calibration_logit_std = score.market_calibration_logit_std;
    }

    pub(crate) fn apply_beta_calibration(&mut self, score: OracleBetaCalibrationScore) {
        self.beta_calibration_enabled = score.enabled;
        self.beta_calibration_input_p = score.input_p;
        self.beta_calibration_p_fair = score.p_fair;
        self.beta_calibration_a = score.param_a;
        self.beta_calibration_b = score.param_b;
        self.beta_calibration_c = score.param_c;
        self.beta_calibration_sample_count = score.sample_count;
        self.beta_calibration_positive_count = score.positive_count;
        self.beta_calibration_brier_score = score.brier_score;
        self.beta_calibration_log_loss = score.log_loss;
        self.beta_calibration_uncertainty = score.uncertainty;
    }

    pub(crate) fn apply_market_calibration(&mut self, score: OracleMarketCalibrationScore) {
        self.market_raw_p_up = score.raw_p;
        self.market_p_up = score.p_market;
        self.market_calibration_enabled = score.enabled;
        self.market_calibration_a = score.param_a;
        self.market_calibration_b = score.param_b;
        self.market_calibration_c = score.param_c;
        self.market_calibration_sample_count = score.sample_count;
        self.market_calibration_positive_count = score.positive_count;
        self.market_calibration_brier_score = score.brier_score;
        self.market_calibration_log_loss = score.log_loss;
        self.market_calibration_uncertainty = score.uncertainty;
    }
}

impl MakerRebateMmStrategy {
    pub fn observe_chainlink_resolution_price(
        &mut self,
        price: &ChainlinkResolutionPrice,
        learning_applied_ts_ms: u64,
    ) -> anyhow::Result<Vec<OracleResolvedLabel>> {
        self.pin_oracle_candles()?;
        let markets = self
            .market_projection
            .markets()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut newly_resolved = Vec::new();
        for market in &markets {
            for label in self.resolution_label_reducer.register_market(market)? {
                self.micro_logistic_trainer
                    .observe_resolved_label(&label, learning_applied_ts_ms);
                self.market_calibration_trainer
                    .observe_resolved_label(&label, learning_applied_ts_ms);
                self.beta_calibration_trainer
                    .observe_resolved_label(&label, learning_applied_ts_ms);
                newly_resolved.push(label);
            }
        }
        for label in self.resolution_label_reducer.observe(price)? {
            self.micro_logistic_trainer
                .observe_resolved_label(&label, learning_applied_ts_ms);
            self.market_calibration_trainer
                .observe_resolved_label(&label, learning_applied_ts_ms);
            self.beta_calibration_trainer
                .observe_resolved_label(&label, learning_applied_ts_ms);
            newly_resolved.push(label);
        }
        Ok(newly_resolved)
    }

    fn beta_calibration_score_for_market(
        &mut self,
        market: &MarketSelection,
        p_base: f64,
        now_ms: u64,
    ) -> OracleBetaCalibrationScore {
        self.beta_calibration_trainer.observe_market(
            market,
            p_base,
            now_ms,
            &self.cfg.live_fair_value_model,
        )
    }
}

fn cleanup_anchor_side(mode: OracleAuthorityMode) -> Option<PairTokenSide> {
    match mode {
        OracleAuthorityMode::Market => None,
        OracleAuthorityMode::Cleanup {
            remaining_owned_side,
        } => Some(remaining_owned_side),
    }
}

pub(crate) fn ta_timeframe_duration_ms(timeframe: &str) -> Option<u64> {
    let normalized = timeframe.trim().to_ascii_lowercase();
    let value = normalized
        .get(..normalized.len().saturating_sub(1))?
        .parse::<u64>()
        .ok()?;
    match normalized.chars().last()? {
        's' => Some(value.saturating_mul(1_000)),
        'm' => Some(value.saturating_mul(60_000)),
        'h' => Some(value.saturating_mul(60 * 60_000)),
        'd' => Some(value.saturating_mul(24 * 60 * 60_000)),
        _ => None,
    }
}

impl MakerRebateMmStrategy {
    fn oracle_candle_ta_features(
        &self,
        market: &MarketSelection,
        oracle_symbol: &str,
        now_ms: u64,
    ) -> Option<(OracleTaFeatures, u64, u32)> {
        let window_ms = market.window_end_ms.saturating_sub(market.window_start_ms);
        let timeframe = decision_ta_timeframe_for_market_window_ms(window_ms).ok()?;
        let timeframe_ms = ta_timeframe_duration_ms(&timeframe)?;
        let window = self.oracle_candle_view.window(oracle_symbol, &timeframe)?;
        let lookback_ms = timeframe_ms.saturating_mul(ORACLE_CANDLE_TA_MAX_BARS as u64);
        let start_ts = now_ms
            .saturating_sub(lookback_ms)
            .min(market.window_start_ms)
            .min(i64::MAX as u64) as i64;
        let end_ts = now_ms.min(i64::MAX as u64) as i64;
        let first = window.bars.partition_point(|bar| bar.ts_ms < start_ts);
        let end = window.bars.partition_point(|bar| bar.ts_ms <= end_ts);
        self.oracle_ta_features_from_bars(
            &window.bars[first..end],
            window.revision,
            now_ms,
            timeframe_ms,
            market.window_start_ms,
            market.window_end_ms,
        )
    }

    fn oracle_ta_features_from_bars(
        &self,
        bars: &[canonical_ohlc_arrow::OhlcBar],
        candle_revision: u64,
        now_ms: u64,
        timeframe_ms: u64,
        window_start_ms: u64,
        window_end_ms: u64,
    ) -> Option<(OracleTaFeatures, u64, u32)> {
        let first = bars.partition_point(|bar| bar.ts_ms < 0);
        let bars = &bars[first..];
        let closed_end = bars.partition_point(|bar| {
            (bar.ts_ms as u64).saturating_add(timeframe_ms) <= now_ms
        });
        let closed_bars = &bars[..closed_end];
        let latest_window_idx = closed_bars
            .partition_point(|bar| (bar.ts_ms as u64) < window_end_ms)
            .checked_sub(1)
            .filter(|&index| closed_bars[index].ts_ms as u64 >= window_start_ms);
        let latest_idx = latest_window_idx.or_else(|| {
            (now_ms >= window_start_ms)
                .then(|| closed_bars.len().checked_sub(1))
                .flatten()
        })?;
        let bars = &closed_bars[..=latest_idx];
        let latest_ts_ms = bars.last()?.ts_ms as u64;
        if now_ms.saturating_sub(latest_ts_ms) > timeframe_ms.saturating_mul(2) {
            return None;
        }
        let cache_key = OracleTaFeatureCacheKey {
            candle_revision,
            timeframe_ms,
            closed_bar_bucket: now_ms.saturating_sub(timeframe_ms) / timeframe_ms.max(1),
            window_start_ms,
            window_end_ms,
        };
        if let Some(entry) = self.oracle_ta_feature_cache.borrow().get(&cache_key) {
            return entry.features;
        }
        let computed = (|| {
            canonical_ohlc_resync::validate_continuity(
                bars,
                i64::try_from(timeframe_ms).ok()?,
            ).ok()?;
            let market_first = bars
                .partition_point(|bar| (bar.ts_ms as u64) < window_start_ms);
            let market_end = bars
                .partition_point(|bar| (bar.ts_ms as u64) < window_end_ms);
            let closed_bars_in_market = market_end
                .saturating_sub(market_first)
                .try_into()
                .unwrap_or(u32::MAX);
            let ta_bars = bars
                .iter()
                .map(|bar| OracleTaBar {
                    ts_ms: bar.ts_ms,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    volume: bar.volume,
                })
                .collect::<Vec<_>>();
            OracleTaFeatureCalculator::features_from_bars(&ta_bars)
                .ok()
                .flatten()
                .map(|features| (features, latest_ts_ms, closed_bars_in_market))
        })();
        let mut cache = self.oracle_ta_feature_cache.borrow_mut();
        if cache.len() >= ORACLE_TA_FEATURE_CACHE_MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(cache_key, OracleTaFeatureCacheEntry { features: computed });
        computed
    }
}

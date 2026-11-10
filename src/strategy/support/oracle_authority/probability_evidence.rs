use crate::oracle_resolution::ChainlinkReferenceDistance;
use crate::polymarket::identity::MarketId;
use crate::strategy::fair_value::FairValueResult;

use super::learning::OracleLogisticFeatureLayout;
use super::logistic::OracleMicrostructureLogisticScore;
use super::market_calibration::OracleMarketCalibrationScore;
use super::probability_interval::ProbabilityInterval;
use super::{OracleAuthorityMode, PairTokenSide};

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(super) struct OracleChainlinkProbabilityEvidence {
    symbol: Option<&'static str>,
    topic: Option<&'static str>,
    window_s: Option<u64>,
    open_price_e18: Option<i128>,
    open_source_ts_ms: Option<u64>,
    open_observed_ts_ms: Option<u64>,
    open_capture_sequence: Option<u64>,
    current_price_e18: Option<i128>,
    current_source_ts_ms: Option<u64>,
    current_observed_ts_ms: Option<u64>,
    current_capture_sequence: Option<u64>,
    log_return: Option<f64>,
}

impl OracleChainlinkProbabilityEvidence {
    pub(super) fn capture(
        reference: Option<ChainlinkReferenceDistance<'_>>,
        log_return: Option<f64>,
    ) -> Self {
        let mut evidence = Self {
            log_return,
            ..Self::default()
        };
        let Some(reference) = reference else {
            return evidence;
        };
        evidence.symbol = match reference.symbol {
            "btc/usd" => Some("btc/usd"),
            "eth/usd" => Some("eth/usd"),
            "sol/usd" => Some("sol/usd"),
            "xrp/usd" => Some("xrp/usd"),
            _ => None,
        };
        evidence.topic = Some(reference.topic);
        evidence.window_s = Some(reference.window_s);
        evidence.open_price_e18 = Some(reference.open.price_e18);
        evidence.open_source_ts_ms = Some(reference.open.source_ts_ms);
        evidence.open_observed_ts_ms = Some(reference.open.observed_ts_ms);
        evidence.open_capture_sequence = Some(reference.open.capture_sequence);
        evidence.current_price_e18 = Some(reference.current.value_e18);
        evidence.current_source_ts_ms = Some(reference.current.payload_ts_ms);
        evidence.current_observed_ts_ms = Some(reference.current.observed_ts_ms);
        evidence.current_capture_sequence = Some(reference.current.capture_sequence);
        evidence
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleProbabilityEvidence {
    oracle_mode: &'static str,
    oracle_learning_enabled: bool,
    layout: OracleLogisticFeatureLayout,
    snapshot_last_eval_ts_ms: Option<u64>,
    unavailable_reason: Option<&'static str>,
    market_calibration: OracleMarketCalibrationScore,
    base_fair_result: FairValueResult,
    micro_logistic: OracleMicrostructureLogisticScore,
    micro_logistic_prediction_valid: bool,
    post_logistic_fair_result: FairValueResult,
    probability_calibration_attempted_p_up: Option<f64>,
    probability_calibration_effective_p_up: Option<f64>,
    beta_calibration_attempted_p_up: Option<f64>,
    beta_observation_p_up: Option<f64>,
    beta_calibration_applied: bool,
    beta_calibration_effective_p_up: Option<f64>,
    final_p_up: Option<f64>,
    final_interval: Option<ProbabilityInterval>,
    direct_market_selected: bool,
    confidence_transform_applied: bool,
    oracle_signal_side: Option<PairTokenSide>,
}

impl OracleProbabilityEvidence {
    pub(super) fn after_model(
        mode: OracleAuthorityMode,
        layout: OracleLogisticFeatureLayout,
        market_calibration: OracleMarketCalibrationScore,
        base_fair_result: FairValueResult,
        micro_logistic: OracleMicrostructureLogisticScore,
        micro_logistic_prediction_valid: bool,
        post_logistic_fair_result: FairValueResult,
        unavailable_reason: Option<&'static str>,
    ) -> Self {
        Self {
            oracle_mode: oracle_mode_label(mode),
            oracle_learning_enabled: micro_logistic.enabled,
            layout,
            snapshot_last_eval_ts_ms: None,
            unavailable_reason,
            market_calibration,
            base_fair_result,
            micro_logistic,
            micro_logistic_prediction_valid,
            post_logistic_fair_result,
            probability_calibration_attempted_p_up: None,
            probability_calibration_effective_p_up: None,
            beta_calibration_attempted_p_up: None,
            beta_observation_p_up: None,
            beta_calibration_applied: false,
            beta_calibration_effective_p_up: None,
            final_p_up: None,
            final_interval: None,
            direct_market_selected: false,
            confidence_transform_applied: false,
            oracle_signal_side: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn with_composition(
        mut self,
        snapshot_last_eval_ts_ms: Option<u64>,
        probability_calibration_attempted_p_up: Option<f64>,
        probability_calibration_effective_p_up: Option<f64>,
        beta_calibration_attempted_p_up: Option<f64>,
        beta_observation_p_up: Option<f64>,
        beta_calibration_applied: bool,
        beta_calibration_effective_p_up: f64,
        final_p_up: f64,
        final_interval: Option<ProbabilityInterval>,
        direct_market_selected: bool,
        confidence_transform_applied: bool,
    ) -> Self {
        self.snapshot_last_eval_ts_ms = snapshot_last_eval_ts_ms;
        self.probability_calibration_attempted_p_up = probability_calibration_attempted_p_up;
        self.probability_calibration_effective_p_up = probability_calibration_effective_p_up;
        self.beta_calibration_attempted_p_up = beta_calibration_attempted_p_up;
        self.beta_observation_p_up = beta_observation_p_up;
        self.beta_calibration_applied = beta_calibration_applied;
        self.beta_calibration_effective_p_up = Some(beta_calibration_effective_p_up);
        self.final_p_up = Some(final_p_up);
        self.final_interval = final_interval;
        self.direct_market_selected = direct_market_selected;
        self.confidence_transform_applied = confidence_transform_applied;
        self
    }

    pub(super) fn with_oracle_signal_side(mut self, side: Option<PairTokenSide>) -> Self {
        self.oracle_signal_side = side;
        self
    }

    pub(super) fn emit(self, market_id: &MarketId, decision_eval_ts_ms: u64) {
        let chainlink = self.micro_logistic.probability_evidence.unwrap_or_default();
        let micro_logistic_applied = self.micro_logistic.applied();
        let micro_logistic_effective_contribution = micro_logistic_applied
            && self
                .post_logistic_fair_result
                .model_weight
                .is_some_and(|weight| weight > 0.0);
        tracing::trace!(
            target: "strategy.oracle_probability",
            event = "oracle_probability_evaluation",
            market_id = %market_id,
            decision_eval_ts_ms,
            snapshot_last_eval_ts_ms = ?self.snapshot_last_eval_ts_ms,
            micro_logistic_observation_ts_ms = ?self
                .micro_logistic
                .final_output_observation_ts_ms,
            oracle_signal_side = ?self.oracle_signal_side.map(oracle_signal_side_label),
            probability_available = self.final_p_up.is_some(),
            unavailable_reason = ?self.unavailable_reason,
            oracle_mode = self.oracle_mode,
            oracle_learning_enabled = self.oracle_learning_enabled,
            logistic_feature_layout = self.layout.as_str(),
            trade_flow_imbalance_5s = ?self.micro_logistic.trade_flow_imbalance_5s,
            trade_flow_available = self.micro_logistic.trade_flow_imbalance_5s.is_some(),
            micro_logistic_trade_flow_enabled = self.micro_logistic.trade_flow_enabled,
            micro_logistic_forecast_feature_identity = self.micro_logistic.forecast_feature_identity.as_str(),
            micro_logistic_trade_flow_qualification_count = ?self.micro_logistic.trade_flow_qualification_count,
            micro_logistic_trade_flow_weight = ?self.micro_logistic.trade_flow_weight,
            spot_z = ?self.base_fair_result.spot_z,
            chainlink_symbol = ?chainlink.symbol,
            chainlink_topic = ?chainlink.topic,
            chainlink_window_s = ?chainlink.window_s,
            chainlink_open_price_e18 = ?chainlink.open_price_e18,
            chainlink_open_source_ts_ms = ?chainlink.open_source_ts_ms,
            chainlink_open_observed_ts_ms = ?chainlink.open_observed_ts_ms,
            chainlink_open_capture_sequence = ?chainlink.open_capture_sequence,
            chainlink_current_price_e18 = ?chainlink.current_price_e18,
            chainlink_current_source_ts_ms = ?chainlink.current_source_ts_ms,
            chainlink_current_observed_ts_ms = ?chainlink.current_observed_ts_ms,
            chainlink_current_capture_sequence = ?chainlink.current_capture_sequence,
            chainlink_reference_log_return = ?chainlink.log_return,
            base_model_raw_p_up = self.base_fair_result.p_model_raw,
            base_model_p_up = self.base_fair_result.p_model,
            base_blended_p_up = self.base_fair_result.p_fair,
            micro_logistic_p_up = ?self.micro_logistic.p_up,
            micro_logistic_prior_p_up = ?self.micro_logistic.market_prior_p,
            micro_logistic_interval_low = ?self
                .micro_logistic
                .p_up_interval
                .map(|interval| interval.lo()),
            micro_logistic_interval_high = ?self
                .micro_logistic
                .p_up_interval
                .map(|interval| interval.hi()),
            micro_logistic_unavailable_reason = ?self.micro_logistic.unavailable_reason,
            micro_logistic_fit_available = self.micro_logistic.fit_available,
            micro_logistic_prediction_valid = self.micro_logistic_prediction_valid,
            micro_logistic_fit_sample_count = ?self.micro_logistic.fit_sample_count,
            micro_logistic_prequentially_trusted = ?self.micro_logistic.fit_prequentially_trusted,
            micro_logistic_trust_alpha = ?self.micro_logistic.fit_residual_trust_alpha,
            micro_logistic_application_policy = ?self.micro_logistic.application_policy_label(),
            micro_logistic_phase = ?self.micro_logistic.phase_label(),
            micro_logistic_phase_sample_count = ?self.micro_logistic.phase_sample_count,
            micro_logistic_phase_trusted = ?self.micro_logistic.phase_trusted,
            micro_logistic_trust_source = ?self.micro_logistic.trust_source_label(),
            micro_logistic_trust_sample_count = ?self.micro_logistic.trust_sample_count,
            micro_logistic_trust_trusted = ?self.micro_logistic.trust_trusted,
            micro_logistic_first_sample_count = ?self.micro_logistic.first_sample_count,
            micro_logistic_phase_p0 = ?self.micro_logistic.phase_p0,
            micro_logistic_phase_p1 = ?self.micro_logistic.phase_p1,
            micro_logistic_application_attempted = self.micro_logistic.application_attempted(),
            micro_logistic_applied,
            micro_logistic_effective_contribution,
            micro_logistic_effective_delta_p_up = ?micro_logistic_effective_contribution.then_some(
                self.post_logistic_fair_result.p_fair - self.base_fair_result.p_fair
            ),
            post_logistic_blend_p_up = self.post_logistic_fair_result.p_fair,
            post_logistic_model_weight = ?self.post_logistic_fair_result.model_weight,
            probability_calibration_attempted_p_up = ?self.probability_calibration_attempted_p_up,
            probability_calibration_effective_p_up = ?self.probability_calibration_effective_p_up,
            beta_calibration_attempted_p_up = ?self.beta_calibration_attempted_p_up,
            final_beta_observation_p_up = ?self.beta_observation_p_up,
            beta_calibration_applied = self.beta_calibration_applied,
            beta_calibration_effective_p_up = ?self.beta_calibration_effective_p_up,
            final_p_up = ?self.final_p_up,
            final_interval_low = ?self.final_interval.map(|interval| interval.lo()),
            final_interval_high = ?self.final_interval.map(|interval| interval.hi()),
            direct_market_selected = self.direct_market_selected,
            confidence_transform_applied = self.confidence_transform_applied,
            market_raw_p_up = ?self.market_calibration.raw_p,
            market_calibrated_p_up = ?self.market_calibration.p_market,
            effective_market_p_up = ?self.market_calibration.p_market,
            effective_market_interval_low = ?self
                .market_calibration
                .calibrated_interval
                .map(|interval| interval.lo()),
            effective_market_interval_high = ?self
                .market_calibration
                .calibrated_interval
                .map(|interval| interval.hi()),
            "oracle_probability_evaluation"
        );
    }
}

pub(super) fn emit_unavailable(
    market_id: &MarketId,
    decision_eval_ts_ms: u64,
    mode: OracleAuthorityMode,
    oracle_learning_enabled: bool,
    trade_flow_enabled: bool,
    trade_flow_imbalance_5s: Option<f64>,
    layout: OracleLogisticFeatureLayout,
    unavailable_reason: &'static str,
) {
    tracing::trace!(
        target: "strategy.oracle_probability",
        event = "oracle_probability_evaluation",
        market_id = %market_id,
        decision_eval_ts_ms,
        snapshot_last_eval_ts_ms = ?Option::<u64>::None,
        micro_logistic_observation_ts_ms = ?Option::<u64>::None,
        oracle_signal_side = ?Option::<&str>::None,
        probability_available = false,
        unavailable_reason,
        oracle_mode = oracle_mode_label(mode),
        oracle_learning_enabled,
        micro_logistic_trade_flow_enabled = trade_flow_enabled,
        trade_flow_imbalance_5s = ?trade_flow_imbalance_5s,
        trade_flow_available = trade_flow_imbalance_5s.is_some(),
        micro_logistic_forecast_feature_identity = ?Option::<&str>::None,
        micro_logistic_trade_flow_qualification_count = ?Option::<u32>::None,
        micro_logistic_trade_flow_weight = ?Option::<f64>::None,
        micro_logistic_fit_available = false,
        micro_logistic_prediction_valid = false,
        micro_logistic_prequentially_trusted = ?Option::<bool>::None,
        micro_logistic_prior_p_up = ?Option::<f64>::None,
        micro_logistic_interval_low = ?Option::<f64>::None,
        micro_logistic_interval_high = ?Option::<f64>::None,
        micro_logistic_application_policy = ?Option::<&str>::None,
        micro_logistic_phase = ?Option::<&str>::None,
        micro_logistic_phase_sample_count = ?Option::<u32>::None,
        micro_logistic_phase_trusted = ?Option::<bool>::None,
        micro_logistic_trust_source = ?Option::<&str>::None,
        micro_logistic_trust_sample_count = ?Option::<u32>::None,
        micro_logistic_trust_trusted = ?Option::<bool>::None,
        micro_logistic_first_sample_count = ?Option::<u32>::None,
        micro_logistic_phase_p0 = ?Option::<f64>::None,
        micro_logistic_phase_p1 = ?Option::<f64>::None,
        direct_market_selected = false,
        effective_market_p_up = ?Option::<f64>::None,
        effective_market_interval_low = ?Option::<f64>::None,
        effective_market_interval_high = ?Option::<f64>::None,
        final_p_up = ?Option::<f64>::None,
        final_interval_low = ?Option::<f64>::None,
        final_interval_high = ?Option::<f64>::None,
        final_beta_observation_p_up = ?Option::<f64>::None,
        confidence_transform_applied = false,
        beta_calibration_applied = false,
        logistic_feature_layout = layout.as_str(),
        "oracle_probability_evaluation"
    );
}

const fn oracle_signal_side_label(side: PairTokenSide) -> &'static str {
    match side {
        PairTokenSide::Up => "up",
        PairTokenSide::Down => "down",
    }
}

fn oracle_mode_label(mode: OracleAuthorityMode) -> &'static str {
    match mode {
        OracleAuthorityMode::Market => "market",
        OracleAuthorityMode::Cleanup { .. } => "cleanup",
    }
}

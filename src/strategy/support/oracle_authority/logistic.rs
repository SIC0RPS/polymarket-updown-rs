use self::posterior::Posterior;
pub(in crate::strategy::support::oracle_authority) use self::posterior::PosteriorVector;
use crate::strategy::fair_value::sigmoid;
use crate::strategy::support::oracle_authority::learning::{
    OracleLogisticFeatureLayout, OracleLogisticForecastFeatureIdentity,
};
use crate::strategy::support::oracle_authority::microstructure::OracleMicrostructureFeatures;
use crate::strategy::support::oracle_authority::probability_evidence::OracleChainlinkProbabilityEvidence;
use crate::strategy::support::oracle_authority::probability_interval::ProbabilityInterval;
use std::collections::{BTreeMap, VecDeque};

mod application;
mod contribution;
mod market_offset;
mod posterior;
mod trainer;

pub(in crate::strategy::support::oracle_authority) use application::FinalOutputIntervalProjectionInput;
pub(in crate::strategy::support::oracle_authority) use contribution::{
    attach_final_output_context, FinalOutputMissingInterval, FinalOutputObservationContext,
    OracleLogisticApplicationPolicy, RetainedObservationHandle,
};
pub(crate) use trainer::{
    OracleLogisticFinalOutputPairRef, OracleMicrostructureLogisticLearning,
    OracleMicrostructureLogisticTrainer,
};

pub(crate) const MICRO_LOGISTIC_FITTED_FEATURE_COUNT: usize = 8;
pub(super) const MICRO_LOGISTIC_OBSERVATION_BIN_COUNT: usize = 5;
pub(super) const MICRO_LOGISTIC_HORIZON_INDEX: usize = 1;
pub(super) const MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX: usize = 2;
pub(super) const MICRO_LOGISTIC_FEATURE_START: usize = 3;
pub(super) const MICRO_LOGISTIC_REFERENCE_INDEX: usize =
    MICRO_LOGISTIC_FEATURE_START + MICRO_LOGISTIC_FITTED_FEATURE_COUNT;
pub(super) const MICRO_LOGISTIC_TRADE_FLOW_INDEX: usize = MICRO_LOGISTIC_REFERENCE_INDEX + 1;
pub(super) const MICRO_LOGISTIC_PARAMETER_COUNT: usize = MICRO_LOGISTIC_TRADE_FLOW_INDEX + 1;
const MICRO_LOGISTIC_MAX_SAMPLES: usize = 512;
const MICRO_LOGISTIC_MIN_FIT_SAMPLES: usize = 1;
const MICRO_LOGISTIC_MIN_PREQUENTIAL_SAMPLES: usize = 8;
const MICRO_LOGISTIC_COLD_START_N0: f64 = 64.0;
const MICRO_LOGISTIC_RETURN_SCALE: f64 = 0.001;
const MICRO_LOGISTIC_VOLATILITY_SCALE: f64 = 0.001;
const MICRO_LOGISTIC_JUMP_SCALE: f64 = 10.0;
const MICRO_LOGISTIC_PROBABILITY_EPSILON: f64 = 1e-6;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleMicrostructureLogisticScore {
    pub(crate) enabled: bool,
    pub(crate) trade_flow_enabled: bool,
    pub(crate) trade_flow_imbalance_5s: Option<f64>,
    pub(crate) trade_flow_qualification_count: Option<u32>,
    pub(crate) trade_flow_weight: Option<f64>,
    pub(super) forecast_feature_identity: OracleLogisticForecastFeatureIdentity,
    pub(crate) p_up: Option<f64>,
    pub(crate) logit: Option<f64>,
    pub(crate) feature_count: Option<u32>,
    pub(crate) l2_penalty: Option<f64>,
    pub(crate) fit_available: bool,
    pub(crate) fit_sample_count: Option<u32>,
    pub(crate) fit_positive_count: Option<u32>,
    pub(crate) fit_residual_trust_alpha: Option<f64>,
    pub(crate) fit_interval_trust_alpha: Option<f64>,
    pub(crate) fit_brier_score: Option<f64>,
    pub(crate) fit_log_loss: Option<f64>,
    pub(crate) fit_prior_brier_score: Option<f64>,
    pub(crate) fit_prior_log_loss: Option<f64>,
    pub(crate) fit_candidate_brier_score: Option<f64>,
    pub(crate) fit_candidate_log_loss: Option<f64>,
    pub(crate) fit_prequentially_trusted: Option<bool>,
    pub(crate) fit_weight_norm: Option<f64>,
    pub(crate) fit_weights: Option<[f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT]>,
    pub(crate) residual_logit_std: Option<f64>,
    pub(crate) residual_logit_std_unavailable_reason: Option<&'static str>,
    pub(crate) market_calibration_logit_std: Option<f64>,
    pub(super) application_policy: Option<OracleLogisticApplicationPolicy>,
    pub(crate) phase: Option<u8>,
    pub(crate) phase_sample_count: Option<u32>,
    pub(crate) phase_trusted: Option<bool>,
    trust_source: Option<contribution::TrustSource>,
    pub(crate) trust_sample_count: Option<u32>,
    pub(crate) trust_trusted: Option<bool>,
    pub(crate) first_sample_count: Option<u32>,
    pub(crate) phase_p0: Option<f64>,
    pub(crate) phase_p1: Option<f64>,
    pub(crate) phase_model_weight: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) final_output_retained_handle:
        Option<RetainedObservationHandle>,
    pub(in crate::strategy::support::oracle_authority) final_output_observation_ts_ms: Option<u64>,
    pub(in crate::strategy::support::oracle_authority) final_output_interval_projection:
        Option<FinalOutputIntervalProjectionInput>,
    pub(crate) unavailable_reason: Option<&'static str>,
    pub(super) feature_layout: OracleLogisticFeatureLayout,
    pub(super) market_prior_p: Option<f64>,
    market_offset_correction_logit: Option<f64>,
    market_offset_correction_interval: Option<[f64; 2]>,
    pub(super) probability_evidence: Option<OracleChainlinkProbabilityEvidence>,
    pub(super) p_up_interval: Option<ProbabilityInterval>,
}

impl OracleMicrostructureLogisticScore {
    fn unavailable(mut self, reason: &'static str) -> Self {
        self.unavailable_reason = Some(reason);
        self
    }

    pub(crate) fn with_fit_score_enabled(mut self, enabled: bool) -> Self {
        if !enabled {
            self.p_up = None;
            self.logit = None;
            self.p_up_interval = None;
            self.unavailable_reason = Some("market_closed");
        }
        self
    }

    pub(super) fn with_probability_evidence(
        mut self,
        evidence: OracleChainlinkProbabilityEvidence,
    ) -> Self {
        self.probability_evidence = Some(evidence);
        self
    }

    fn with_phase_application(
        mut self,
        trust: Option<contribution::TrustSelection>,
        application: Option<contribution::PhaseTrustApplication>,
        retained_handle: Option<RetainedObservationHandle>,
        observation_ts_ms: u64,
    ) -> Self {
        self.trust_source = trust.map(|value| value.source);
        self.trust_sample_count = trust.map(|value| value.sample_count);
        self.trust_trusted = trust.map(|value| value.trusted);
        self.first_sample_count = trust.map(|value| value.first_sample_count);
        self.phase = application.map(|value| value.phase);
        self.phase_sample_count = application.and(trust).map(|value| value.phase_sample_count);
        self.phase_trusted = application.and(trust).map(|value| value.phase_trusted);
        self.phase_p0 = application.map(|value| value.p0);
        self.phase_p1 = application.map(|value| value.p1);
        self.phase_model_weight = application.map(|value| value.model_weight);
        self.final_output_retained_handle = application.and(retained_handle);
        self.final_output_observation_ts_ms =
            self.final_output_retained_handle.map(|_| observation_ts_ms);
        self
    }

    pub(super) fn with_final_output_interval_projection(
        mut self,
        projection: Option<FinalOutputIntervalProjectionInput>,
    ) -> Self {
        self.final_output_interval_projection = projection;
        self
    }

    pub(super) fn application_policy_label(self) -> Option<&'static str> {
        self.application_policy.map(|policy| policy.as_str())
    }

    pub(super) fn phase_label(self) -> Option<&'static str> {
        self.phase.and_then(contribution::phase_label)
    }

    pub(super) fn trust_source_label(self) -> Option<&'static str> {
        self.trust_source.map(contribution::TrustSource::as_str)
    }

    pub(crate) fn application_attempted(self) -> bool {
        self.enabled && self.p_up.is_some()
    }

    pub(crate) fn applied(self) -> bool {
        self.application_attempted()
            && (!self.trade_flow_enabled || self.fit_prequentially_trusted == Some(true))
            && if self.application_policy.is_some() {
                self.trust_trusted == Some(true) && self.phase_p1.is_some()
            } else {
                self.fit_prequentially_trusted != Some(false)
            }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleMicrostructureLogisticFit {
    feature_layout: OracleLogisticFeatureLayout,
    trade_flow_fitted: bool,
    trade_flow_weight: f64,
    qualification_window_count: u32,
    pub(crate) l2_penalty: f64,
    pub(crate) intercept: f64,
    pub(crate) horizon_weight: f64,
    pub(crate) boundary_return_weight: f64,
    pub(crate) reference_weight: f64,
    pub(crate) weights: [f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT],
    pub(crate) residual_trust_alpha: f64,
    pub(crate) sample_count: u32,
    pub(crate) independent_window_count: u32,
    pub(crate) positive_count: u32,
    pub(crate) brier_score: Option<f64>,
    pub(crate) log_loss: Option<f64>,
    pub(crate) prior_brier_score: Option<f64>,
    pub(crate) prior_log_loss: Option<f64>,
    pub(crate) candidate_brier_score: Option<f64>,
    pub(crate) candidate_log_loss: Option<f64>,
    pub(crate) prequentially_trusted: bool,
    pub(crate) weight_norm: Option<f64>,
}

pub(super) fn normalized_boundary_log_return(spot_px: f64, open_px: f64) -> Option<f64> {
    if !(spot_px.is_finite() && open_px.is_finite() && spot_px > 0.0 && open_px > 0.0) {
        return None;
    }
    let value = (spot_px / open_px).ln() / MICRO_LOGISTIC_RETURN_SCALE;
    value.is_finite().then_some(value.clamp(-10.0, 10.0))
}

fn fitted_feature_values(
    features: &OracleMicrostructureFeatures,
) -> [Option<f64>; MICRO_LOGISTIC_FITTED_FEATURE_COUNT] {
    [
        features
            .signed_return_1s
            .and_then(|value| normalize_feature_value(0, value)),
        features
            .signed_return_5s
            .and_then(|value| normalize_feature_value(1, value)),
        features
            .ofi_depth
            .and_then(|value| normalize_feature_value(2, value)),
        features
            .queue_imbalance_l1
            .and_then(|value| normalize_feature_value(3, value)),
        features
            .queue_imbalance_l5
            .and_then(|value| normalize_feature_value(4, value)),
        features
            .microprice_gap_ticks
            .and_then(|value| normalize_feature_value(5, value)),
        features
            .volatility_state
            .and_then(|value| normalize_feature_value(6, value)),
        features
            .jump_proxy
            .and_then(|value| normalize_feature_value(7, value)),
    ]
}

pub(super) fn normalized_feature_vector(
    features: &OracleMicrostructureFeatures,
) -> ([f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT], u32) {
    let values = fitted_feature_values(features);
    let present = values
        .iter()
        .filter(|value| value.is_some_and(f64::is_finite))
        .count() as u32;
    (
        values.map(|value| value.filter(|value| value.is_finite()).unwrap_or(0.0)),
        present,
    )
}

pub(super) fn feature_design(
    layout: OracleLogisticFeatureLayout,
    normalized_forecast_horizon: Option<f64>,
    boundary_return: Option<f64>,
    spot_z: Option<f64>,
    chainlink_reference_return: Option<f64>,
    features: &[f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT],
) -> std::result::Result<PosteriorVector, &'static str> {
    let mut design = PosteriorVector::zeros();
    design[0] = 1.0;
    match layout {
        OracleLogisticFeatureLayout::Baseline
        | OracleLogisticFeatureLayout::MarketOffset
        | OracleLogisticFeatureLayout::ReferenceOnly => {
            design[MICRO_LOGISTIC_HORIZON_INDEX] = normalized_forecast_horizon
                .filter(|value| value.is_finite() && *value >= 0.0)
                .ok_or("invalid_forecast_horizon")?;
            design[MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX] = boundary_return
                .filter(|value| value.is_finite())
                .ok_or("invalid_boundary_return")?;
        }
        OracleLogisticFeatureLayout::RepresentationOnly | OracleLogisticFeatureLayout::Combined => {
            design[MICRO_LOGISTIC_HORIZON_INDEX] = spot_z
                .filter(|value| value.is_finite())
                .ok_or("missing_spot_z")?;
        }
    }
    if layout.uses_chainlink_reference() {
        let reference_return = chainlink_reference_return
            .filter(|value| value.is_finite())
            .ok_or("missing_chainlink_reference_return")?;
        let scaled = reference_return / MICRO_LOGISTIC_RETURN_SCALE;
        if !scaled.is_finite() {
            return Err("invalid_chainlink_reference_return");
        }
        design[MICRO_LOGISTIC_REFERENCE_INDEX] = scaled;
    }
    for (index, feature) in features.iter().copied().enumerate() {
        design[MICRO_LOGISTIC_FEATURE_START + index] = feature;
    }
    Ok(design)
}

fn observation_design(
    layout: OracleLogisticFeatureLayout,
    observation: &trainer::FrozenMicrostructureObservation,
) -> Option<PosteriorVector> {
    let mut design = feature_design(
        layout,
        Some(observation.normalized_forecast_horizon),
        Some(observation.boundary_return),
        observation.spot_z,
        observation
            .chainlink_reference
            .as_ref()
            .map(|reference| reference.log_return),
        &observation.features,
    )
    .ok()?;
    // Missing flow contributes the original base likelihood and no flow derivative.
    design[MICRO_LOGISTIC_TRADE_FLOW_INDEX] = observation.trade_flow_imbalance_5s.unwrap_or(0.0);
    Some(design)
}

fn parameters_from_fit(fit: OracleMicrostructureLogisticFit) -> PosteriorVector {
    let mut parameters = PosteriorVector::zeros();
    parameters[0] = fit.intercept;
    parameters[MICRO_LOGISTIC_HORIZON_INDEX] = fit.horizon_weight;
    parameters[MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX] = fit.boundary_return_weight;
    for (index, weight) in fit.weights.into_iter().enumerate() {
        parameters[MICRO_LOGISTIC_FEATURE_START + index] = weight;
    }
    parameters[MICRO_LOGISTIC_REFERENCE_INDEX] = fit.reference_weight;
    parameters[MICRO_LOGISTIC_TRADE_FLOW_INDEX] = fit.trade_flow_weight;
    parameters
}

fn model_logit(parameters: &PosteriorVector, design: &PosteriorVector) -> f64 {
    let mut logit = parameters[0]
        + parameters[MICRO_LOGISTIC_HORIZON_INDEX] * design[MICRO_LOGISTIC_HORIZON_INDEX]
        + parameters[MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX]
            * design[MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX]
        + (0..MICRO_LOGISTIC_FITTED_FEATURE_COUNT)
            .map(|index| {
                parameters[MICRO_LOGISTIC_FEATURE_START + index]
                    * design[MICRO_LOGISTIC_FEATURE_START + index]
            })
            .sum::<f64>();
    if design[MICRO_LOGISTIC_REFERENCE_INDEX] != 0.0 {
        logit +=
            parameters[MICRO_LOGISTIC_REFERENCE_INDEX] * design[MICRO_LOGISTIC_REFERENCE_INDEX];
    }
    if design[MICRO_LOGISTIC_TRADE_FLOW_INDEX] != 0.0 {
        logit +=
            parameters[MICRO_LOGISTIC_TRADE_FLOW_INDEX] * design[MICRO_LOGISTIC_TRADE_FLOW_INDEX];
    }
    logit
}

fn normalize_feature_value(index: usize, value: f64) -> Option<f64> {
    let value = match index {
        0 | 1 => value / MICRO_LOGISTIC_RETURN_SCALE,
        6 => value / MICRO_LOGISTIC_VOLATILITY_SCALE,
        7 => value / MICRO_LOGISTIC_JUMP_SCALE,
        _ => value,
    };
    value.is_finite().then_some(value.clamp(-1.0, 1.0))
}

fn residual_trust_alpha(sample_count: u32) -> f64 {
    let n = sample_count as f64;
    if n <= 0.0 {
        0.0
    } else {
        (n / (n + MICRO_LOGISTIC_COLD_START_N0)).clamp(0.0, 1.0)
    }
}

fn logit_from_probability(p: f64) -> Option<f64> {
    (p.is_finite() && p > 0.0 && p < 1.0).then(|| (p / (1.0 - p)).ln())
}

fn prequential_fit_metrics<'a>(
    resolved_windows: impl Iterator<Item = &'a trainer::ResolvedMicrostructureWindow>,
    trade_flow_enabled: bool,
) -> (usize, [f64; 4]) {
    let eligible = |sample: &&trainer::LabeledMicrostructureExample| {
        !trade_flow_enabled
            || (sample.observation.forecast_feature_identity
                == Some(OracleLogisticForecastFeatureIdentity::TradeFlowV1)
                && sample.observation.trade_flow_imbalance_5s.is_some())
    };
    let (window_count, totals) =
        resolved_windows.fold((0_usize, [0.0; 4]), |(count, mut totals), window| {
            let sample_count = window.observations.iter().filter(eligible).count();
            if sample_count == 0 {
                return (count, totals);
            }
            let observation_weight = 1.0 / sample_count as f64;
            for observation in window.observations.iter().filter(eligible) {
                totals[0] += observation_weight * observation.candidate_brier_loss;
                totals[1] += observation_weight * observation.candidate_log_loss;
                totals[2] += observation_weight * observation.prior_brier_loss;
                totals[3] += observation_weight * observation.prior_log_loss;
            }
            (count + 1, totals)
        });
    let metrics = if window_count == 0 {
        totals
    } else {
        totals.map(|total| total / window_count as f64)
    };
    (window_count, metrics)
}

fn prequential_probabilities(
    fit: OracleMicrostructureLogisticFit,
    market_logit: f64,
    design: &PosteriorVector,
) -> (f64, f64) {
    let prior_p = market_offset::bounded_probability_from_logit(market_logit);
    if fit.independent_window_count < MICRO_LOGISTIC_MIN_FIT_SAMPLES as u32 {
        return (prior_p, prior_p);
    }
    let correction_logit = model_logit(&parameters_from_fit(fit), design);
    let eta = if fit.feature_layout.uses_market_offset() {
        market_offset::recorded_prior_logit(fit.feature_layout, prior_p)
            .expect("bounded market prior")
            + correction_logit
    } else {
        correction_logit
    };
    let candidate_p = sigmoid(eta).clamp(0.0, 1.0).clamp(
        MICRO_LOGISTIC_PROBABILITY_EPSILON,
        1.0 - MICRO_LOGISTIC_PROBABILITY_EPSILON,
    );
    (candidate_p, prior_p)
}

fn fit_logistic_model(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<trainer::ResolvedMicrostructureWindow>,
    l2_penalty: f64,
    trade_flow_enabled: bool,
) -> (OracleMicrostructureLogisticFit, Option<Box<Posterior>>) {
    let l2_penalty = if l2_penalty.is_finite() {
        l2_penalty.max(0.0)
    } else {
        0.0
    };
    let window_outcomes = resolved_windows
        .iter()
        .filter_map(|window| {
            window.observations.first().map(|observation| {
                (
                    (observation.window_start_ms, observation.window_end_ms),
                    observation.y_up,
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    if window_outcomes.is_empty() {
        return (
            OracleMicrostructureLogisticFit {
                feature_layout: layout,
                l2_penalty,
                ..Default::default()
            },
            None,
        );
    }
    let positive_count = window_outcomes
        .values()
        .filter(|y_up| **y_up >= 0.5)
        .count() as u32;
    let independent_window_count = window_outcomes.len() as u32;
    let (
        qualification_window_count,
        [candidate_brier, candidate_log_loss, prior_brier, prior_log_loss],
    ) = prequential_fit_metrics(resolved_windows.iter(), trade_flow_enabled);
    let qualification_window_count = if trade_flow_enabled {
        qualification_window_count as u32
    } else {
        independent_window_count
    };
    let candidate_alpha = residual_trust_alpha(qualification_window_count);
    let fit_trade_flow = trade_flow_enabled
        && resolved_windows.iter().any(|window| {
            window
                .observations
                .iter()
                .any(|sample| sample.observation.trade_flow_imbalance_5s.is_some())
        });
    let prequentially_trusted = prior_brier - candidate_brier > MICRO_LOGISTIC_PROBABILITY_EPSILON
        && candidate_log_loss <= prior_log_loss
        && qualification_window_count as usize >= MICRO_LOGISTIC_MIN_PREQUENTIAL_SAMPLES;
    let map = (l2_penalty.is_finite()
        && l2_penalty > 0.0
        && independent_window_count as usize >= MICRO_LOGISTIC_MIN_FIT_SAMPLES)
        .then(|| posterior::fit_map(layout, resolved_windows, l2_penalty, fit_trade_flow))
        .flatten();
    let trade_flow_fitted = fit_trade_flow && map.is_some();
    let residual_trust_alpha = map
        .as_ref()
        .filter(|_| prequentially_trusted)
        .map(|_| candidate_alpha)
        .unwrap_or(0.0);
    let (brier_score, log_loss) = if residual_trust_alpha > 0.0 {
        (candidate_brier, candidate_log_loss)
    } else {
        (prior_brier, prior_log_loss)
    };
    let (parameters, posterior) = map
        .map(|(parameters, posterior)| (parameters, Some(Box::new(posterior))))
        .unwrap_or_else(|| (PosteriorVector::zeros(), None));
    let mut weights = [0.0; MICRO_LOGISTIC_FITTED_FEATURE_COUNT];
    weights.copy_from_slice(
        parameters
            .fixed_rows::<MICRO_LOGISTIC_FITTED_FEATURE_COUNT>(MICRO_LOGISTIC_FEATURE_START)
            .as_slice(),
    );
    let fit = OracleMicrostructureLogisticFit {
        feature_layout: layout,
        l2_penalty,
        intercept: parameters[0],
        horizon_weight: parameters[MICRO_LOGISTIC_HORIZON_INDEX],
        boundary_return_weight: parameters[MICRO_LOGISTIC_BOUNDARY_RETURN_INDEX],
        reference_weight: parameters[MICRO_LOGISTIC_REFERENCE_INDEX],
        trade_flow_fitted,
        trade_flow_weight: parameters[MICRO_LOGISTIC_TRADE_FLOW_INDEX],
        qualification_window_count,
        weights,
        residual_trust_alpha,
        sample_count: independent_window_count,
        independent_window_count,
        positive_count,
        brier_score: Some(brier_score),
        log_loss: Some(log_loss),
        prior_brier_score: Some(prior_brier),
        prior_log_loss: Some(prior_log_loss),
        candidate_brier_score: Some(candidate_brier),
        candidate_log_loss: Some(candidate_log_loss),
        prequentially_trusted,
        weight_norm: array_weight_norm(&weights),
    };
    (fit, posterior)
}

fn binary_log_loss(y_up: f64, p_up: f64) -> f64 {
    let p_up = p_up.clamp(
        MICRO_LOGISTIC_PROBABILITY_EPSILON,
        1.0 - MICRO_LOGISTIC_PROBABILITY_EPSILON,
    );
    -(y_up * p_up.ln() + (1.0 - y_up) * (1.0 - p_up).ln())
}

fn array_weight_norm(weights: &[f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT]) -> Option<f64> {
    weights.iter().all(|weight| weight.is_finite()).then(|| {
        weights
            .iter()
            .map(|weight| weight * weight)
            .sum::<f64>()
            .sqrt()
    })
}

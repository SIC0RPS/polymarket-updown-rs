use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::oracle_resolution::{ChainlinkReferenceDistance, ChainlinkReferenceEvidence};
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::strategy::support::oracle_authority::learning::{
    OracleLogisticFeatureLayout, OracleLogisticForecastFeatureIdentity,
};

use super::super::contribution::{
    self, FinalOutputObservationContext, MarketOffsetContribution,
    MarketOffsetFirstDecisionContribution, OracleLogisticApplicationPolicy,
};
use super::super::market_offset;
use super::super::{
    binary_log_loss, logit_from_probability, prequential_probabilities, PosteriorVector,
    MICRO_LOGISTIC_FITTED_FEATURE_COUNT, MICRO_LOGISTIC_OBSERVATION_BIN_COUNT,
};
use super::{emit_retained, OracleMicrostructureLogisticTrainer, MICRO_LOGISTIC_MAX_PENDING};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(in crate::strategy::support::oracle_authority::logistic) enum FirstDecisionSampleUnavailableReason
{
    MissingSnapshotTimestamp,
    ExactObservationUnavailable,
    ProspectiveContributionUnavailable,
}

impl FirstDecisionSampleUnavailableReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MissingSnapshotTimestamp => "missing_snapshot_timestamp",
            Self::ExactObservationUnavailable => "exact_observation_unavailable",
            Self::ProspectiveContributionUnavailable => "prospective_contribution_unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(in crate::strategy::support::oracle_authority::logistic) struct ResolvedFirstAuthorization {
    pub(in crate::strategy::support::oracle_authority::logistic) authorization_ts_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) snapshot_last_eval_ts_ms:
        Option<u64>,
    pub(in crate::strategy::support::oracle_authority::logistic) window_start_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) window_end_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) sample_unavailable_reason:
        Option<FirstDecisionSampleUnavailableReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(in crate::strategy::support::oracle_authority::logistic) struct FrozenMicrostructureObservation
{
    pub(in crate::strategy::support::oracle_authority::logistic) observation_ts_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) decision_ts_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) normalized_forecast_horizon: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) boundary_return: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) features:
        [f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) trade_flow_imbalance_5s:
        Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) forecast_feature_identity:
        Option<OracleLogisticForecastFeatureIdentity>,
    pub(in crate::strategy::support::oracle_authority::logistic) candidate_p: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) prior_p: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) spot_z: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) chainlink_reference:
        Option<ChainlinkReferenceEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) market_offset_contribution:
        Option<MarketOffsetContribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) first_decision_contribution:
        Option<MarketOffsetFirstDecisionContribution>,
    #[serde(skip)]
    pub(in crate::strategy::support::oracle_authority::logistic) final_output_context:
        Option<FinalOutputObservationContext>,
}

#[derive(Debug, Clone)]
pub(in crate::strategy::support::oracle_authority::logistic) struct PendingMicrostructureWindow {
    pub(in crate::strategy::support::oracle_authority::logistic) anchor:
        Option<FrozenMicrostructureObservation>,
    pub(in crate::strategy::support::oracle_authority::logistic) bins:
        [Option<FrozenMicrostructureObservation>; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    pub(in crate::strategy::support::oracle_authority::logistic) first_authorization:
        Option<ResolvedFirstAuthorization>,
    pub(in crate::strategy::support::oracle_authority::logistic) first_sample:
        Option<FrozenMicrostructureObservation>,
}

impl PendingMicrostructureWindow {
    fn with_observation(
        observation: FrozenMicrostructureObservation,
        bins: [Option<FrozenMicrostructureObservation>; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    ) -> Self {
        Self {
            anchor: Some(observation),
            bins,
            first_authorization: None,
            first_sample: None,
        }
    }

    fn empty() -> Self {
        Self {
            anchor: None,
            bins: std::array::from_fn(|_| None),
            first_authorization: None,
            first_sample: None,
        }
    }

    fn exact_observation(&self, decision_ts_ms: u64) -> Option<&FrozenMicrostructureObservation> {
        self.bins
            .iter()
            .flatten()
            .chain(self.anchor.iter())
            .find(|observation| observation.decision_ts_ms == decision_ts_ms)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(in crate::strategy::support::oracle_authority::logistic) struct LabeledMicrostructureExample {
    pub(in crate::strategy::support::oracle_authority::logistic) y_up: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) window_start_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) window_end_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) observation:
        FrozenMicrostructureObservation,
    pub(in crate::strategy::support::oracle_authority::logistic) candidate_brier_loss: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) candidate_log_loss: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) prior_brier_loss: f64,
    pub(in crate::strategy::support::oracle_authority::logistic) prior_log_loss: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(in crate::strategy::support::oracle_authority::logistic) struct ResolvedMicrostructureWindow {
    pub(in crate::strategy::support::oracle_authority::logistic) market_id: MarketId,
    pub(in crate::strategy::support::oracle_authority::logistic) label_release_ts_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) learning_applied_ts_ms: u64,
    pub(in crate::strategy::support::oracle_authority::logistic) observations:
        Vec<LabeledMicrostructureExample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(in crate::strategy::support::oracle_authority::logistic) first_authorization:
        Option<ResolvedFirstAuthorization>,
}

impl OracleMicrostructureLogisticTrainer {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::strategy::support::oracle_authority::logistic) fn observe_market(
        &mut self,
        market: &MarketSelection,
        observation_ts_ms: u64,
        decision_ts_ms: u64,
        feature_vector: &[f64; MICRO_LOGISTIC_FITTED_FEATURE_COUNT],
        trade_flow_imbalance_5s: Option<f64>,
        design: std::result::Result<&PosteriorVector, &'static str>,
        p_market: Option<f64>,
        normalized_forecast_horizon: Option<f64>,
        boundary_return: Option<f64>,
        spot_z: Option<f64>,
        chainlink_reference: Option<ChainlinkReferenceDistance<'_>>,
        chainlink_reference_return: Option<f64>,
    ) -> contribution::ObservedMicrostructureMarket {
        let window = (market.window_start_ms, market.window_end_ms);
        let retain_first_decision_samples = self.retains_first_decision_samples();
        let phase =
            contribution::phase_index(market.window_start_ms, market.window_end_ms, decision_ts_ms);
        let mut observed = contribution::ObservedMicrostructureMarket::new(self.fit, window, phase);
        let market_logit = p_market.and_then(logit_from_probability);
        let normalized_forecast_horizon =
            normalized_forecast_horizon.filter(|value| value.is_finite() && *value >= 0.0);
        let boundary_return = boundary_return.filter(|value| value.is_finite());
        if let (Some(market_logit), Some(normalized_forecast_horizon), Some(boundary_return)) =
            (market_logit, normalized_forecast_horizon, boundary_return)
        {
            let Ok(design) = design else {
                if retain_first_decision_samples {
                    observed.mark_first_authorization(
                        self.pending_by_window
                            .get(&window)
                            .is_some_and(|pending| pending.first_authorization.is_some()),
                    );
                }
                return observed;
            };
            let (candidate_p, prior_p) = prequential_probabilities(self.fit, market_logit, design);
            let layout = self.layout;
            let forecast_feature_identity = if self.trade_flow_enabled && self.fit.trade_flow_fitted
            {
                OracleLogisticForecastFeatureIdentity::TradeFlowV1
            } else {
                OracleLogisticForecastFeatureIdentity::EightInputV1
            };
            let make_observation = || FrozenMicrostructureObservation {
                observation_ts_ms,
                decision_ts_ms,
                normalized_forecast_horizon,
                boundary_return,
                features: *feature_vector,
                trade_flow_imbalance_5s,
                forecast_feature_identity: Some(forecast_feature_identity),
                candidate_p,
                prior_p,
                spot_z: layout.uses_spot_z().then_some(spot_z).flatten(),
                chainlink_reference: layout
                    .uses_chainlink_reference()
                    .then(|| {
                        chainlink_reference
                            .zip(chainlink_reference_return)
                            .map(|(reference, value)| reference.evidence(value))
                    })
                    .flatten(),
                market_offset_contribution: None,
                first_decision_contribution: None,
                final_output_context: None,
            };
            if let Some(pending) = self.pending_by_window.get_mut(&window) {
                if retain_first_decision_samples {
                    observed.mark_first_authorization(pending.first_authorization.is_some());
                }
                if pending.anchor.is_none() {
                    let observation = make_observation();
                    pending.anchor = Some(observation.clone());
                    if let Some(phase) = phase {
                        pending.bins[phase] = Some(observation);
                    }
                    observed.retain_anchor();
                } else if let Some(phase) = phase {
                    let should_replace = pending.bins[phase]
                        .as_ref()
                        .map(|existing| decision_ts_ms >= existing.decision_ts_ms)
                        .unwrap_or(true);
                    if should_replace {
                        pending.bins[phase] = Some(make_observation());
                        observed.retain_bin(phase);
                    }
                }
            } else if self.pending_count < MICRO_LOGISTIC_MAX_PENDING {
                let observation = make_observation();
                let mut bins = std::array::from_fn(|_| None);
                if let Some(phase) = phase {
                    bins[phase] = Some(observation.clone());
                    observed.retain_anchor();
                }
                self.pending_by_window.insert(
                    window,
                    PendingMicrostructureWindow::with_observation(observation, bins),
                );
                self.pending_count += 1;
                tracing::debug!(
                    target: "strategy.oracle_logistic",
                    market_id = %market.market_id,
                    sample_count = self.fit.sample_count,
                    window_start_ms = market.window_start_ms,
                    window_end_ms = market.window_end_ms,
                    "micro_logistic_pending_sample_created"
                );
            }
        } else if retain_first_decision_samples {
            observed.mark_first_authorization(
                self.pending_by_window
                    .get(&window)
                    .is_some_and(|pending| pending.first_authorization.is_some()),
            );
        }
        observed
    }

    pub(crate) fn retain_first_authorization(
        &mut self,
        market: &MarketSelection,
        authorization_ts_ms: u64,
        snapshot_last_eval_ts_ms: Option<u64>,
    ) {
        if !self
            .application_policy
            .is_some_and(OracleLogisticApplicationPolicy::uses_first_decision_trust)
        {
            return;
        }
        let window = (market.window_start_ms, market.window_end_ms);
        let pending = match self.pending_by_window.entry(window) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if self.pending_count >= MICRO_LOGISTIC_MAX_PENDING {
                    emit_retained(
                        market,
                        authorization_ts_ms,
                        snapshot_last_eval_ts_ms,
                        false,
                        None,
                        Some("pending_capacity"),
                    );
                    return;
                }
                self.pending_count += 1;
                entry.insert(PendingMicrostructureWindow::empty())
            }
        };
        if pending.first_authorization.is_some() {
            return;
        }
        let (first_sample, unavailable_reason) = match snapshot_last_eval_ts_ms {
            None => (
                None,
                Some(FirstDecisionSampleUnavailableReason::MissingSnapshotTimestamp),
            ),
            Some(snapshot_ts_ms) => match pending.exact_observation(snapshot_ts_ms).cloned() {
                None => (
                    None,
                    Some(FirstDecisionSampleUnavailableReason::ExactObservationUnavailable),
                ),
                Some(mut observation) => match observation.market_offset_contribution {
                    None => (
                        None,
                        Some(
                            FirstDecisionSampleUnavailableReason::ProspectiveContributionUnavailable,
                        ),
                    ),
                    Some(contribution) => {
                        observation.market_offset_contribution = None;
                        observation.first_decision_contribution = Some(
                            MarketOffsetFirstDecisionContribution::from_phase(contribution),
                        );
                        (Some(observation), None)
                    }
                },
            },
        };
        pending.first_authorization = Some(ResolvedFirstAuthorization {
            authorization_ts_ms,
            snapshot_last_eval_ts_ms,
            window_start_ms: market.window_start_ms,
            window_end_ms: market.window_end_ms,
            sample_unavailable_reason: unavailable_reason,
        });
        emit_retained(
            market,
            authorization_ts_ms,
            snapshot_last_eval_ts_ms,
            true,
            first_sample.as_ref(),
            unavailable_reason.map(FirstDecisionSampleUnavailableReason::as_str),
        );
        pending.first_sample = first_sample;
    }
}

pub(super) fn push_distinct_observation(
    observations: &mut Vec<FrozenMicrostructureObservation>,
    mut observation: FrozenMicrostructureObservation,
) -> bool {
    if let Some(existing) = observations
        .iter_mut()
        .find(|existing| observations_identical(existing, &observation))
    {
        if existing.first_decision_contribution.is_none() {
            existing.first_decision_contribution = observation.first_decision_contribution.take();
        }
        return true;
    }
    observations.push(observation);
    false
}

fn observations_identical(
    left: &FrozenMicrostructureObservation,
    right: &FrozenMicrostructureObservation,
) -> bool {
    let flow_forecast = Some(OracleLogisticForecastFeatureIdentity::TradeFlowV1);
    let same_flow = if left.forecast_feature_identity == flow_forecast
        || right.forecast_feature_identity == flow_forecast
    {
        left.forecast_feature_identity == right.forecast_feature_identity
            && left.trade_flow_imbalance_5s.map(f64::to_bits)
                == right.trade_flow_imbalance_5s.map(f64::to_bits)
    } else {
        true
    };
    same_flow
        && left.observation_ts_ms == right.observation_ts_ms
        && left.decision_ts_ms == right.decision_ts_ms
        && left.normalized_forecast_horizon.to_bits() == right.normalized_forecast_horizon.to_bits()
        && left.boundary_return.to_bits() == right.boundary_return.to_bits()
        && left
            .features
            .iter()
            .zip(right.features.iter())
            .all(|(left, right)| left.to_bits() == right.to_bits())
        && left.candidate_p.to_bits() == right.candidate_p.to_bits()
        && left.prior_p.to_bits() == right.prior_p.to_bits()
        && left.spot_z.map(f64::to_bits) == right.spot_z.map(f64::to_bits)
        && left.chainlink_reference == right.chainlink_reference
}

pub(super) fn validate_resolved_window(
    window: &ResolvedMicrostructureWindow,
    policy: Option<OracleLogisticApplicationPolicy>,
    layout: OracleLogisticFeatureLayout,
    expected_symbol: &str,
    knowledge_cutoff_ms: u64,
) -> Result<()> {
    ensure!(
        !window.market_id.as_str().is_empty()
            && window.label_release_ts_ms > 0
            && window.learning_applied_ts_ms >= window.label_release_ts_ms
            && window.learning_applied_ts_ms <= knowledge_cutoff_ms
            && window.observations.len()
                <= 1 + MICRO_LOGISTIC_OBSERVATION_BIN_COUNT
                    + usize::from(
                        policy.is_some_and(
                            OracleLogisticApplicationPolicy::uses_first_decision_trust,
                        ) && window
                            .first_authorization
                            .is_some_and(|marker| { marker.sample_unavailable_reason.is_none() }),
                    ),
        "invalid micro-logistic resolved-window identity"
    );
    let sample_bounds = window
        .observations
        .first()
        .map(|sample| (sample.window_start_ms, sample.window_end_ms));
    let marker_bounds = window
        .first_authorization
        .map(|marker| (marker.window_start_ms, marker.window_end_ms));
    let (window_start_ms, window_end_ms) = sample_bounds
        .or(marker_bounds)
        .ok_or_else(|| anyhow::anyhow!("micro-logistic resolved window has no evidence"))?;
    ensure!(
        window_start_ms > 0
            && window_start_ms < window_end_ms
            && sample_bounds.is_none_or(|bounds| bounds == (window_start_ms, window_end_ms))
            && marker_bounds.is_none_or(|bounds| bounds == (window_start_ms, window_end_ms)),
        "invalid micro-logistic market window"
    );
    let first_policy =
        policy.is_some_and(OracleLogisticApplicationPolicy::uses_first_decision_trust);
    ensure!(
        first_policy || window.first_authorization.is_none(),
        "micro-logistic First marker has incompatible application policy"
    );
    if let Some(marker) = window.first_authorization {
        ensure!(
            marker.authorization_ts_ms >= window_start_ms
                && marker.authorization_ts_ms < window_end_ms
                && marker.authorization_ts_ms <= knowledge_cutoff_ms
                && marker.snapshot_last_eval_ts_ms.is_none_or(|timestamp| {
                    timestamp >= window_start_ms
                        && timestamp < window_end_ms
                        && timestamp <= marker.authorization_ts_ms
                }),
            "invalid micro-logistic First marker timing"
        );
    }
    let mut first_sample_count = 0_u8;
    for (observation_index, sample) in window.observations.iter().enumerate() {
        let observation = &sample.observation;
        ensure!(
            sample.window_start_ms == window_start_ms
                && sample.window_end_ms == window_end_ms
                && (sample.y_up == 0.0 || sample.y_up == 1.0)
                && observation.observation_ts_ms > 0
                && observation.observation_ts_ms < sample.window_end_ms
                && observation.observation_ts_ms <= window.label_release_ts_ms
                && observation.decision_ts_ms >= sample.window_start_ms
                && observation.decision_ts_ms < sample.window_end_ms
                && observation.decision_ts_ms <= window.label_release_ts_ms
                && observation.normalized_forecast_horizon.is_finite()
                && observation.boundary_return.is_finite()
                && observation.features.iter().all(|value| value.is_finite()),
            "invalid micro-logistic learning sample identity or timing"
        );
        if let Some(first) = window.observations.first() {
            ensure!(
                sample.y_up.to_bits() == first.y_up.to_bits(),
                "inconsistent micro-logistic resolved label"
            );
        }
        ensure!(
            layout.accepts_inputs(
                observation.spot_z,
                observation.chainlink_reference.is_some(),
            ) && observation
                .chainlink_reference
                .as_ref()
                .is_none_or(|reference| {
                    reference.valid_for_window(expected_symbol, sample.window_start_ms)
                }),
            "invalid micro-logistic learning sample layout inputs"
        );
        ensure!(
            observation
                .trade_flow_imbalance_5s
                .is_none_or(|value| value.is_finite() && (-1.0..=1.0).contains(&value))
                && (observation.forecast_feature_identity
                    != Some(OracleLogisticForecastFeatureIdentity::TradeFlowV1)
                    || observation.trade_flow_imbalance_5s.is_some()),
            "invalid micro-logistic trade-flow evidence"
        );
        ensure!(
            (0.0..=1.0).contains(&observation.candidate_p)
                && (0.0..=1.0).contains(&observation.prior_p)
                && market_offset::recorded_prior_logit(layout, observation.prior_p).is_some(),
            "invalid micro-logistic learning sample probability"
        );
        ensure!(
            contribution::validate_retained_contribution(
                policy,
                layout,
                observation.market_offset_contribution,
                sample.window_start_ms,
                sample.window_end_ms,
                observation.decision_ts_ms,
                observation_index == 0,
            ) && contribution::validate_first_decision_contribution(
                policy,
                observation.first_decision_contribution,
            ),
            "invalid micro-logistic retained contribution"
        );
        if observation.first_decision_contribution.is_some() {
            first_sample_count = first_sample_count.saturating_add(1);
            ensure!(
                window.first_authorization.is_some_and(|marker| {
                    marker.snapshot_last_eval_ts_ms == Some(observation.decision_ts_ms)
                        && marker.sample_unavailable_reason.is_none()
                }),
                "invalid micro-logistic First sample identity"
            );
        }
        ensure!(
            sample.candidate_brier_loss.to_bits()
                == (observation.candidate_p - sample.y_up).powi(2).to_bits()
                && sample.candidate_log_loss.to_bits()
                    == binary_log_loss(sample.y_up, observation.candidate_p).to_bits()
                && sample.prior_brier_loss.to_bits()
                    == (observation.prior_p - sample.y_up).powi(2).to_bits()
                && sample.prior_log_loss.to_bits()
                    == binary_log_loss(sample.y_up, observation.prior_p).to_bits(),
            "invalid micro-logistic learning sample loss"
        );
    }
    ensure!(
        first_sample_count <= 1
            && window.first_authorization.is_none_or(|marker| {
                (first_sample_count == 1) == marker.sample_unavailable_reason.is_none()
            }),
        "invalid micro-logistic First evidence availability"
    );
    for (index, sample) in window.observations.iter().enumerate() {
        ensure!(
            !window.observations[index + 1..]
                .iter()
                .any(|other| observations_identical(&sample.observation, &other.observation)),
            "duplicate micro-logistic learning observation"
        );
    }
    Ok(())
}

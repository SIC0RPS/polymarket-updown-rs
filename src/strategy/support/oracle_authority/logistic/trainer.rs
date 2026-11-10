use std::collections::{HashMap, HashSet, VecDeque};

use crate::oracle_resolution::OracleResolvedLabel;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use super::contribution::{self, OracleLogisticApplicationPolicy, PhaseTrustSummary};
use super::posterior::Posterior;
use super::*;
use crate::strategy::support::oracle_authority::learning::OracleLogisticFeatureLayout;

mod observations;

use observations::{push_distinct_observation, validate_resolved_window};
pub(super) use observations::{
    FrozenMicrostructureObservation, LabeledMicrostructureExample, PendingMicrostructureWindow,
    ResolvedMicrostructureWindow,
};

const MICRO_LOGISTIC_MAX_PENDING: usize = MICRO_LOGISTIC_MAX_SAMPLES;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AppliedLogitStd {
    pub(crate) value: Option<f64>,
    pub(crate) unavailable_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OracleLogisticFinalOutputPairRef<'a> {
    pub(crate) market_id: &'a MarketId,
    pub(crate) window_start_ms: u64,
    pub(crate) window_end_ms: u64,
    pub(crate) observation_ts_ms: u64,
    pub(crate) decision_ts_ms: u64,
    pub(crate) label_release_ts_ms: u64,
    pub(crate) learning_applied_ts_ms: u64,
    pub(crate) phase: u8,
    pub(crate) winner_up: bool,
    pub(crate) f0_p_up: f64,
    pub(crate) f1_p_up: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OracleMicrostructureLogisticLearning {
    l2_penalty: f64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    trade_flow_enabled: bool,
    resolved_windows: Vec<ResolvedMicrostructureWindow>,
    knowledge_cutoff_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    feature_layout: Option<OracleLogisticFeatureLayout>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    application_policy: Option<OracleLogisticApplicationPolicy>,
}

impl OracleMicrostructureLogisticLearning {
    pub(in crate::strategy::support::oracle_authority) fn feature_layout(
        &self,
    ) -> OracleLogisticFeatureLayout {
        self.feature_layout.unwrap_or_default()
    }

    pub(in crate::strategy::support::oracle_authority) fn application_policy(
        &self,
    ) -> Option<OracleLogisticApplicationPolicy> {
        self.application_policy
    }

    pub(in crate::strategy::support::oracle_authority) fn fresh_for_layout(
        &self,
        layout: OracleLogisticFeatureLayout,
    ) -> Self {
        Self {
            l2_penalty: self.l2_penalty,
            trade_flow_enabled: self.trade_flow_enabled,
            resolved_windows: Vec::new(),
            knowledge_cutoff_ms: 0,
            feature_layout: layout.serialized(),
            application_policy: None,
        }
    }

    pub(in crate::strategy::support::oracle_authority) fn fresh_for_layout_with_policy(
        &self,
        layout: OracleLogisticFeatureLayout,
        application_policy: OracleLogisticApplicationPolicy,
    ) -> Result<Self> {
        ensure!(
            application_policy.accepts_layout(layout),
            "oracle logistic application policy is incompatible with feature layout"
        );
        Ok(Self {
            l2_penalty: self.l2_penalty,
            trade_flow_enabled: self.trade_flow_enabled,
            resolved_windows: Vec::new(),
            knowledge_cutoff_ms: 0,
            feature_layout: layout.serialized(),
            application_policy: Some(application_policy),
        })
    }

    pub(crate) fn retained_prequential_metrics(&self) -> (usize, Option<[f64; 4]>) {
        let (window_count, metrics) =
            prequential_fit_metrics(self.resolved_windows.iter(), self.trade_flow_enabled);
        (window_count, (window_count > 0).then_some(metrics))
    }

    pub(crate) fn final_output_pairs(
        &self,
    ) -> impl Iterator<Item = OracleLogisticFinalOutputPairRef<'_>> + '_ {
        let enabled = self
            .application_policy
            .is_some_and(OracleLogisticApplicationPolicy::uses_final_output_trust);
        self.resolved_windows.iter().flat_map(move |window| {
            window.observations.iter().filter_map(move |example| {
                let contribution = enabled
                    .then_some(example.observation.market_offset_contribution)
                    .flatten()?;
                Some(OracleLogisticFinalOutputPairRef {
                    market_id: &window.market_id,
                    window_start_ms: example.window_start_ms,
                    window_end_ms: example.window_end_ms,
                    observation_ts_ms: example.observation.observation_ts_ms,
                    decision_ts_ms: example.observation.decision_ts_ms,
                    label_release_ts_ms: window.label_release_ts_ms,
                    learning_applied_ts_ms: window.learning_applied_ts_ms,
                    phase: contribution.phase,
                    winner_up: example.y_up == 1.0,
                    f0_p_up: contribution.p0,
                    f1_p_up: contribution.p1,
                })
            })
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OracleMicrostructureLogisticTrainer {
    layout: OracleLogisticFeatureLayout,
    pub(super) trade_flow_enabled: bool,
    pub(super) application_policy: Option<OracleLogisticApplicationPolicy>,
    l2_penalty: f64,
    pub(super) pending_by_window: HashMap<(u64, u64), PendingMicrostructureWindow>,
    pending_count: usize,
    resolved_windows: VecDeque<ResolvedMicrostructureWindow>,
    pub(super) phase_summaries: [PhaseTrustSummary; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    pub(super) first_decision_summary: PhaseTrustSummary,
    pub(super) fit: OracleMicrostructureLogisticFit,
    pub(super) posterior: Option<Box<Posterior>>,
    label_generation: u64,
    fitted_label_generation: u64,
    knowledge_cutoff_ms: u64,
}

impl OracleMicrostructureLogisticTrainer {
    pub(crate) fn new(l2_penalty: f64) -> Self {
        let l2_penalty = if l2_penalty.is_finite() {
            l2_penalty.max(0.0)
        } else {
            0.0
        };
        Self {
            layout: OracleLogisticFeatureLayout::Baseline,
            trade_flow_enabled: false,
            application_policy: None,
            l2_penalty,
            pending_by_window: HashMap::new(),
            pending_count: 0,
            resolved_windows: VecDeque::new(),
            phase_summaries: [PhaseTrustSummary::default(); MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
            first_decision_summary: PhaseTrustSummary::default(),
            fit: OracleMicrostructureLogisticFit {
                l2_penalty,
                ..Default::default()
            },
            posterior: None,
            label_generation: 0,
            fitted_label_generation: 0,
            knowledge_cutoff_ms: 0,
        }
    }

    pub(crate) fn set_trade_flow_enabled(&mut self, enabled: bool) {
        if self.trade_flow_enabled == enabled {
            return;
        }
        self.trade_flow_enabled = enabled;
        (self.fit, self.posterior) = fit_logistic_model(
            self.layout,
            &self.resolved_windows,
            self.l2_penalty,
            enabled,
        );
        self.fitted_label_generation = self.label_generation;
    }

    pub(crate) fn set_l2_penalty(&mut self, l2_penalty: f64) {
        let l2_penalty = l2_penalty.max(0.0);
        if self.l2_penalty.to_bits() == l2_penalty.to_bits() {
            return;
        }
        self.l2_penalty = l2_penalty;
        (self.fit, self.posterior) = fit_logistic_model(
            self.layout,
            &self.resolved_windows,
            self.l2_penalty,
            self.trade_flow_enabled,
        );
        self.fitted_label_generation = self.label_generation;
    }

    pub(crate) fn applied_logit_std(
        &self,
        p_market: Option<f64>,
        design: std::result::Result<&PosteriorVector, &'static str>,
    ) -> AppliedLogitStd {
        if p_market.and_then(logit_from_probability).is_none() {
            return AppliedLogitStd {
                value: None,
                unavailable_reason: Some("invalid_market_probability"),
            };
        }
        let design = match design {
            Ok(design) => design,
            Err(reason) => {
                return AppliedLogitStd {
                    value: None,
                    unavailable_reason: Some(reason),
                }
            }
        };
        let Some(posterior) = self.posterior.as_deref() else {
            return AppliedLogitStd {
                value: None,
                unavailable_reason: Some("no_posterior"),
            };
        };
        let Some(variance) = posterior.applied_variance(design) else {
            return AppliedLogitStd {
                value: None,
                unavailable_reason: Some("invalid_posterior_variance"),
            };
        };
        let value = (variance.is_finite() && variance >= 0.0)
            .then_some(variance.sqrt())
            .filter(|std| std.is_finite());
        AppliedLogitStd {
            value,
            unavailable_reason: value.is_none().then_some("invalid_posterior_std"),
        }
    }

    pub(crate) fn observe_resolved_label(
        &mut self,
        label: &OracleResolvedLabel,
        learning_applied_ts_ms: u64,
    ) {
        let Some(pending) = self
            .pending_by_window
            .remove(&(label.window_start_ms, label.window_end_ms))
        else {
            return;
        };
        self.pending_count = self.pending_count.saturating_sub(1);
        let y_up = label.y_up();
        let first_authorization = pending.first_authorization;
        let mut pending_observations = Vec::with_capacity(2 + MICRO_LOGISTIC_OBSERVATION_BIN_COUNT);
        if let Some(anchor) = pending.anchor {
            push_distinct_observation(&mut pending_observations, anchor);
        }
        for observation in pending.bins.into_iter().flatten() {
            push_distinct_observation(&mut pending_observations, observation);
        }
        let ordinary_fit_row_count = pending_observations.len() as u32;
        let first_contribution = pending
            .first_sample
            .as_ref()
            .and_then(|observation| observation.first_decision_contribution);
        let sample_available = pending.first_sample.is_some();
        let sample_merged = pending
            .first_sample
            .map(|observation| push_distinct_observation(&mut pending_observations, observation))
            .unwrap_or(false);
        if self
            .application_policy
            .is_some_and(OracleLogisticApplicationPolicy::uses_final_output_trust)
        {
            contribution::finalize_final_output_contributions(&mut pending_observations);
        }
        let resolved_fit_row_count = pending_observations.len() as u32;
        let observations = pending_observations
            .into_iter()
            .map(|observation| {
                let candidate_brier_loss = (observation.candidate_p - y_up).powi(2);
                let candidate_log_loss = binary_log_loss(y_up, observation.candidate_p);
                let prior_brier_loss = (observation.prior_p - y_up).powi(2);
                let prior_log_loss = binary_log_loss(y_up, observation.prior_p);
                LabeledMicrostructureExample {
                    y_up,
                    window_start_ms: label.window_start_ms,
                    window_end_ms: label.window_end_ms,
                    observation,
                    candidate_brier_loss,
                    candidate_log_loss,
                    prior_brier_loss,
                    prior_log_loss,
                }
            })
            .collect();
        let resolved_window = ResolvedMicrostructureWindow {
            market_id: label.market_id.clone(),
            label_release_ts_ms: label.release_ts_ms,
            learning_applied_ts_ms,
            observations,
            first_authorization,
        };
        if let Some(policy) = self.application_policy {
            contribution::add_window(
                &mut self.phase_summaries,
                &mut self.first_decision_summary,
                policy,
                &resolved_window,
            );
        }
        self.resolved_windows.push_back(resolved_window);
        self.knowledge_cutoff_ms = self
            .knowledge_cutoff_ms
            .max(label.release_ts_ms)
            .max(learning_applied_ts_ms)
            .max(
                self.resolved_windows
                    .back()
                    .and_then(|window| {
                        window
                            .observations
                            .iter()
                            .map(|sample| {
                                sample
                                    .observation
                                    .observation_ts_ms
                                    .max(sample.observation.decision_ts_ms)
                            })
                            .max()
                    })
                    .unwrap_or_default(),
            );
        if let Some(first_authorization) = first_authorization {
            self.knowledge_cutoff_ms = self
                .knowledge_cutoff_ms
                .max(first_authorization.authorization_ts_ms)
                .max(
                    first_authorization
                        .snapshot_last_eval_ts_ms
                        .unwrap_or_default(),
                );
        }
        while self.resolved_windows.len() > MICRO_LOGISTIC_MAX_SAMPLES {
            if let Some(removed) = self.resolved_windows.pop_front() {
                if let Some(policy) = self.application_policy {
                    contribution::remove_window(
                        &mut self.phase_summaries,
                        &mut self.first_decision_summary,
                        policy,
                        &removed,
                    );
                }
            }
        }
        self.label_generation = self.label_generation.saturating_add(1);
        self.refit_if_needed();
        if let Some(first_authorization) = first_authorization {
            tracing::trace!(
                target: "strategy.oracle_probability",
                event = "oracle_first_decision_sample_resolved",
                market_id = %label.market_id,
                symbol = %label.symbol,
                window_start_ms = label.window_start_ms,
                window_end_ms = label.window_end_ms,
                label_release_ts_ms = label.release_ts_ms,
                learning_applied_ts_ms,
                marker_available = true,
                authorization_ts_ms = ?Some(first_authorization.authorization_ts_ms),
                snapshot_last_eval_ts_ms = ?first_authorization.snapshot_last_eval_ts_ms,
                sample_available,
                sample_merged,
                first_fit_row_added = sample_available && !sample_merged,
                ordinary_fit_row_count,
                resolved_fit_row_count,
                first_p0 = ?first_contribution.map(|contribution| contribution.p0),
                first_p1 = ?first_contribution.map(|contribution| contribution.p1),
                first_support_count = self.first_decision_summary.market_count(),
                fit_sample_count = self.fit.sample_count,
                "oracle_first_decision_sample_resolved"
            );
        }
        tracing::debug!(
            target: "strategy.oracle_logistic",
            market_id = %label.market_id,
            symbol = %label.symbol,
            sample_count = self.fit.sample_count,
            residual_trust_alpha = self.fit.residual_trust_alpha,
            candidate_brier_score = ?self.fit.candidate_brier_score,
            prior_brier_score = ?self.fit.prior_brier_score,
            candidate_log_loss = ?self.fit.candidate_log_loss,
            prior_log_loss = ?self.fit.prior_log_loss,
            prequentially_trusted = self.fit.prequentially_trusted,
            posterior_available = self.posterior.is_some(),
            "micro_logistic_label_refit"
        );
    }

    fn refit_if_needed(&mut self) {
        if self.fitted_label_generation == self.label_generation {
            return;
        }
        self.fitted_label_generation = self.label_generation;
        let (fit, posterior) = fit_logistic_model(
            self.layout,
            &self.resolved_windows,
            self.l2_penalty,
            self.trade_flow_enabled,
        );
        self.fit = fit;
        self.posterior = posterior;
    }

    pub(crate) fn learning(&self) -> OracleMicrostructureLogisticLearning {
        OracleMicrostructureLogisticLearning {
            l2_penalty: self.l2_penalty,
            trade_flow_enabled: self.trade_flow_enabled,
            resolved_windows: self.resolved_windows.iter().cloned().collect(),
            knowledge_cutoff_ms: self.knowledge_cutoff_ms,
            feature_layout: self.layout.serialized(),
            application_policy: self.application_policy,
        }
    }

    pub(crate) fn knowledge_cutoff_ms(&self) -> u64 {
        self.knowledge_cutoff_ms
    }

    pub(in crate::strategy::support::oracle_authority) fn feature_layout(
        &self,
    ) -> OracleLogisticFeatureLayout {
        self.layout
    }

    pub(crate) fn fit_available(&self) -> bool {
        self.posterior.is_some()
    }

    pub(super) fn application_policy(&self) -> Option<OracleLogisticApplicationPolicy> {
        self.application_policy
    }

    pub(crate) fn retains_first_decision_samples(&self) -> bool {
        self.application_policy
            .is_some_and(OracleLogisticApplicationPolicy::uses_first_decision_trust)
    }

    pub(crate) fn requires_chainlink_reference(&self) -> bool {
        self.layout.uses_chainlink_reference()
    }

    pub(crate) fn from_learning(
        learning: OracleMicrostructureLogisticLearning,
        expected_symbol: &str,
    ) -> Result<Self> {
        let layout = learning.feature_layout();
        let application_policy = learning.application_policy();
        ensure!(
            application_policy.is_none_or(|policy| policy.accepts_layout(layout)),
            "oracle logistic application policy is incompatible with feature layout"
        );
        ensure!(
            learning.l2_penalty.is_finite() && learning.l2_penalty >= 0.0,
            "invalid micro-logistic fit configuration"
        );
        ensure!(
            learning.resolved_windows.len() <= MICRO_LOGISTIC_MAX_SAMPLES,
            "micro-logistic learning exceeds retained history"
        );
        let mut market_ids = HashSet::new();
        for window in &learning.resolved_windows {
            ensure!(
                market_ids.insert(window.market_id.clone()),
                "duplicate micro-logistic resolved-window market identity"
            );
            validate_resolved_window(
                window,
                application_policy,
                layout,
                expected_symbol,
                learning.knowledge_cutoff_ms,
            )?;
        }
        let resolved_windows = learning
            .resolved_windows
            .into_iter()
            .collect::<VecDeque<_>>();
        let (fit, posterior) = fit_logistic_model(
            layout,
            &resolved_windows,
            learning.l2_penalty,
            learning.trade_flow_enabled,
        );
        let (phase_summaries, first_decision_summary) = if let Some(policy) = application_policy {
            contribution::summaries_from_windows(policy, resolved_windows.iter())
        } else {
            (
                [PhaseTrustSummary::default(); MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
                PhaseTrustSummary::default(),
            )
        };
        let generation = resolved_windows.len() as u64;
        Ok(Self {
            layout,
            trade_flow_enabled: learning.trade_flow_enabled,
            application_policy,
            l2_penalty: learning.l2_penalty,
            pending_by_window: HashMap::new(),
            pending_count: 0,
            resolved_windows,
            phase_summaries,
            first_decision_summary,
            fit,
            posterior,
            label_generation: generation,
            fitted_label_generation: generation,
            knowledge_cutoff_ms: learning.knowledge_cutoff_ms,
        })
    }
}

fn emit_retained(
    market: &MarketSelection,
    authorization_ts_ms: u64,
    snapshot_last_eval_ts_ms: Option<u64>,
    marker_retained: bool,
    sample: Option<&FrozenMicrostructureObservation>,
    unavailable_reason: Option<&'static str>,
) {
    let contribution = sample.and_then(|sample| sample.first_decision_contribution);
    tracing::trace!(
        target: "strategy.oracle_probability",
        event = "oracle_first_decision_sample_retained",
        market_id = %market.market_id,
        symbol = %market.underlying,
        authorization_ts_ms,
        window_start_ms = market.window_start_ms,
        window_end_ms = market.window_end_ms,
        snapshot_last_eval_ts_ms = ?snapshot_last_eval_ts_ms,
        marker_retained,
        sample_available = sample.is_some(),
        unavailable_reason = ?unavailable_reason,
        observation_ts_ms = ?sample.map(|sample| sample.observation_ts_ms),
        sample_decision_ts_ms = ?sample.map(|sample| sample.decision_ts_ms),
        candidate_p_up = ?sample.map(|sample| sample.candidate_p),
        prior_p_up = ?sample.map(|sample| sample.prior_p),
        first_p0 = ?contribution.map(|contribution| contribution.p0),
        first_p1 = ?contribution.map(|contribution| contribution.p1),
        "oracle_first_decision_sample_retained"
    );
}

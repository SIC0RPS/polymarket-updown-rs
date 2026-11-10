use serde::{Deserialize, Serialize};

use crate::config::CurrentBlendedFairValueConfig;
use crate::strategy::fair_value::CurrentBlended;
use crate::strategy::support::oracle_authority::calibration::OracleBetaCalibrationScore;
use crate::strategy::support::oracle_authority::features::OracleDecisionScoreConfig;
use crate::strategy::support::oracle_authority::probability_interval::ProbabilityInterval;
use crate::strategy::support::oracle_authority::snapshot_build::{
    final_output_model_agreement, project_final_output_arm, FinalOutputArmProjectionInput,
};

use super::application::FinalOutputIntervalProjectionInput;
use super::trainer::{
    FrozenMicrostructureObservation, OracleMicrostructureLogisticTrainer,
    ResolvedMicrostructureWindow,
};
use super::{
    binary_log_loss, OracleLogisticFeatureLayout, OracleMicrostructureLogisticFit,
    OracleMicrostructureLogisticScore, MICRO_LOGISTIC_MIN_FIT_SAMPLES,
    MICRO_LOGISTIC_MIN_PREQUENTIAL_SAMPLES, MICRO_LOGISTIC_OBSERVATION_BIN_COUNT,
    MICRO_LOGISTIC_PROBABILITY_EPSILON,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(in crate::strategy::support::oracle_authority) enum OracleLogisticApplicationPolicy {
    MarketOffsetPhaseTrust,
    MarketOffsetFirstDecisionTrust,
    #[serde(rename = "market_offset_final_output_phase_trust_v2")]
    MarketOffsetFinalOutputPhaseTrust,
}

impl OracleLogisticApplicationPolicy {
    pub(in crate::strategy::support::oracle_authority) fn parse(
        value: &str,
    ) -> anyhow::Result<Self> {
        match value {
            "market_offset_phase_trust" => Ok(Self::MarketOffsetPhaseTrust),
            "market_offset_first_decision_trust" => Ok(Self::MarketOffsetFirstDecisionTrust),
            "market_offset_final_output_phase_trust_v2" => {
                Ok(Self::MarketOffsetFinalOutputPhaseTrust)
            }
            _ => anyhow::bail!("unsupported oracle logistic application policy {value}"),
        }
    }

    pub(in crate::strategy::support::oracle_authority) const fn as_str(self) -> &'static str {
        match self {
            Self::MarketOffsetPhaseTrust => "market_offset_phase_trust",
            Self::MarketOffsetFirstDecisionTrust => "market_offset_first_decision_trust",
            Self::MarketOffsetFinalOutputPhaseTrust => "market_offset_final_output_phase_trust_v2",
        }
    }

    pub(super) const fn accepts_layout(self, layout: OracleLogisticFeatureLayout) -> bool {
        matches!(
            (self, layout),
            (
                Self::MarketOffsetPhaseTrust
                    | Self::MarketOffsetFirstDecisionTrust
                    | Self::MarketOffsetFinalOutputPhaseTrust,
                OracleLogisticFeatureLayout::MarketOffset
            )
        )
    }

    pub(super) const fn uses_first_decision_trust(self) -> bool {
        matches!(self, Self::MarketOffsetFirstDecisionTrust)
    }

    pub(in crate::strategy::support::oracle_authority) const fn uses_final_output_trust(
        self,
    ) -> bool {
        matches!(self, Self::MarketOffsetFinalOutputPhaseTrust)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(super) struct MarketOffsetContribution {
    pub(super) phase: u8,
    pub(super) anchor: bool,
    pub(super) p0: f64,
    pub(super) p1: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::strategy::support::oracle_authority) enum FinalOutputMissingInterval {
    Incoming(Option<ProbabilityInterval>),
    DirectMarket(Option<ProbabilityInterval>),
    Learner(FinalOutputIntervalProjectionInput),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::strategy::support::oracle_authority) struct FinalOutputObservationContext {
    pub(in crate::strategy::support::oracle_authority) phase: u8,
    pub(in crate::strategy::support::oracle_authority) anchor: bool,
    pub(in crate::strategy::support::oracle_authority) selected_arm: bool,
    pub(in crate::strategy::support::oracle_authority) published_p: f64,
    pub(in crate::strategy::support::oracle_authority) missing_p: f64,
    pub(in crate::strategy::support::oracle_authority) missing_model_p: f64,
    pub(in crate::strategy::support::oracle_authority) missing_interval: FinalOutputMissingInterval,
    pub(in crate::strategy::support::oracle_authority) market_p: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) spread_up: f64,
    pub(in crate::strategy::support::oracle_authority) spread_down: f64,
    pub(in crate::strategy::support::oracle_authority) depth_up: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) depth_down: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) min_confidence: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_market_weight: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_spread_weight: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_depth_weight: f64,
    pub(in crate::strategy::support::oracle_authority) score_cfg: OracleDecisionScoreConfig,
    pub(in crate::strategy::support::oracle_authority) raw_beta: OracleBetaCalibrationScore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(super) struct MarketOffsetFirstDecisionContribution {
    pub(super) p0: f64,
    pub(super) p1: f64,
}

impl MarketOffsetFirstDecisionContribution {
    pub(super) const fn from_phase(contribution: MarketOffsetContribution) -> Self {
        Self {
            p0: contribution.p0,
            p1: contribution.p1,
        }
    }

    fn valid(self) -> bool {
        self.p0.is_finite()
            && (0.0..=1.0).contains(&self.p0)
            && self.p1.is_finite()
            && (0.0..=1.0).contains(&self.p1)
    }
}

pub(super) fn validate_first_decision_contribution(
    policy: Option<OracleLogisticApplicationPolicy>,
    contribution: Option<MarketOffsetFirstDecisionContribution>,
) -> bool {
    match (policy, contribution) {
        (Some(policy), Some(contribution)) => {
            policy.uses_first_decision_trust() && contribution.valid()
        }
        (_, None) => true,
        _ => false,
    }
}

impl MarketOffsetContribution {
    fn valid_for(
        self,
        window_start_ms: u64,
        window_end_ms: u64,
        decision_ts_ms: u64,
        expected_anchor: bool,
    ) -> bool {
        self.anchor == expected_anchor
            && phase_index(window_start_ms, window_end_ms, decision_ts_ms)
                == Some(self.phase as usize)
            && self.p0.is_finite()
            && (0.0..=1.0).contains(&self.p0)
            && self.p1.is_finite()
            && (0.0..=1.0).contains(&self.p1)
    }
}

pub(super) fn validate_retained_contribution(
    policy: Option<OracleLogisticApplicationPolicy>,
    layout: OracleLogisticFeatureLayout,
    contribution: Option<MarketOffsetContribution>,
    window_start_ms: u64,
    window_end_ms: u64,
    decision_ts_ms: u64,
    expected_anchor: bool,
) -> bool {
    match (policy, contribution) {
        (None, None) => true,
        (Some(policy), Some(contribution)) => {
            policy.accepts_layout(layout)
                && contribution.valid_for(
                    window_start_ms,
                    window_end_ms,
                    decision_ts_ms,
                    expected_anchor,
                )
        }
        (Some(_), None) => true,
        (None, Some(_)) => false,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PhaseTrustSummary {
    market_count: u32,
    p0_brier_sum: f64,
    p1_brier_sum: f64,
    p0_log_sum: f64,
    p1_log_sum: f64,
}

impl PhaseTrustSummary {
    pub(super) const fn market_count(self) -> u32 {
        self.market_count
    }

    pub(super) fn trusted(self) -> bool {
        if self.market_count < MICRO_LOGISTIC_MIN_PREQUENTIAL_SAMPLES as u32 {
            return false;
        }
        let divisor = self.market_count as f64;
        let p0_brier = self.p0_brier_sum / divisor;
        let p1_brier = self.p1_brier_sum / divisor;
        let p0_log = self.p0_log_sum / divisor;
        let p1_log = self.p1_log_sum / divisor;
        p0_brier - p1_brier > MICRO_LOGISTIC_PROBABILITY_EPSILON && p1_log <= p0_log
    }

    fn add(&mut self, loss: MarketPhaseLoss) {
        self.market_count = self.market_count.saturating_add(1);
        self.p0_brier_sum += loss.p0_brier;
        self.p1_brier_sum += loss.p1_brier;
        self.p0_log_sum += loss.p0_log;
        self.p1_log_sum += loss.p1_log;
    }

    fn remove(&mut self, loss: MarketPhaseLoss) {
        if self.market_count <= 1 {
            *self = Self::default();
            return;
        }
        self.market_count -= 1;
        self.p0_brier_sum -= loss.p0_brier;
        self.p1_brier_sum -= loss.p1_brier;
        self.p0_log_sum -= loss.p0_log;
        self.p1_log_sum -= loss.p1_log;
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PhaseTrustApplication {
    pub(super) phase: u8,
    pub(super) p0: f64,
    pub(super) p1: f64,
    pub(super) model_weight: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TrustSource {
    First,
    Phase,
}

impl TrustSource {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Phase => "phase",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TrustSelection {
    pub(super) source: TrustSource,
    pub(super) phase: u8,
    pub(super) sample_count: u32,
    pub(super) trusted: bool,
    pub(super) first_sample_count: u32,
    pub(super) phase_sample_count: u32,
    pub(super) phase_trusted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RetainedObservationTarget {
    Anchor,
    Bin(usize),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::strategy::support::oracle_authority) struct RetainedObservationHandle {
    window: (u64, u64),
    target: RetainedObservationTarget,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedMicrostructureMarket {
    pub(super) fit: Option<OracleMicrostructureLogisticFit>,
    window: (u64, u64),
    phase: Option<usize>,
    first_authorization_marked: bool,
    target: Option<RetainedObservationTarget>,
}

impl ObservedMicrostructureMarket {
    pub(super) const fn new(
        fit: OracleMicrostructureLogisticFit,
        window: (u64, u64),
        phase: Option<usize>,
    ) -> Self {
        Self {
            fit: Some(fit),
            window,
            phase,
            first_authorization_marked: false,
            target: None,
        }
    }

    pub(super) fn retain_anchor(&mut self) {
        self.target = Some(RetainedObservationTarget::Anchor);
    }

    pub(super) fn retain_bin(&mut self, phase: usize) {
        self.target = Some(RetainedObservationTarget::Bin(phase));
    }

    pub(super) fn mark_first_authorization(&mut self, marked: bool) {
        self.first_authorization_marked = marked;
    }

    const fn retained_handle(&self) -> Option<RetainedObservationHandle> {
        match self.target {
            Some(target) => Some(RetainedObservationHandle {
                window: self.window,
                target,
            }),
            None => None,
        }
    }
}

fn trust_selection(
    policy: OracleLogisticApplicationPolicy,
    phase: Option<usize>,
    first_authorization_marked: bool,
    phase_summaries: &[PhaseTrustSummary; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    first_summary: PhaseTrustSummary,
) -> Option<TrustSelection> {
    let phase = phase.filter(|phase| *phase < MICRO_LOGISTIC_OBSERVATION_BIN_COUNT)?;
    let phase_summary = phase_summaries[phase];
    let source = if policy.uses_first_decision_trust() && !first_authorization_marked {
        TrustSource::First
    } else {
        TrustSource::Phase
    };
    let summary = match source {
        TrustSource::First => first_summary,
        TrustSource::Phase => phase_summary,
    };
    Some(TrustSelection {
        source,
        phase: phase as u8,
        sample_count: summary.market_count(),
        trusted: summary.trusted(),
        first_sample_count: first_summary.market_count(),
        phase_sample_count: phase_summary.market_count(),
        phase_trusted: phase_summary.trusted(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prospective_application(
    policy: Option<OracleLogisticApplicationPolicy>,
    fit: OracleMicrostructureLogisticFit,
    fit_available: bool,
    selection: TrustSelection,
    p0: f64,
    score: OracleMicrostructureLogisticScore,
    shock_toxicity: f64,
    markout_toxicity: f64,
    cfg: &CurrentBlendedFairValueConfig,
) -> Option<PhaseTrustApplication> {
    let policy = policy?;
    let q = score.p_up?;
    let market_prior = score.market_prior_p?;
    if !score.application_attempted()
        || score.unavailable_reason.is_some()
        || !policy.accepts_layout(fit.feature_layout)
        || !fit_available
        || fit.independent_window_count < MICRO_LOGISTIC_MIN_FIT_SAMPLES as u32
        || !(p0.is_finite()
            && q.is_finite()
            && market_prior.is_finite()
            && (0.0..=1.0).contains(&p0)
            && (0.0..=1.0).contains(&q)
            && (0.0..=1.0).contains(&market_prior))
    {
        return None;
    }
    let model_trust = score.fit_interval_trust_alpha?;
    let blend = CurrentBlended::new(cfg).blend_probability(
        q,
        Some(market_prior),
        shock_toxicity,
        markout_toxicity,
        model_trust,
    );
    let model_weight = blend.model_weight?;
    Some(PhaseTrustApplication {
        phase: selection.phase,
        p0,
        p1: blend.p_fair,
        model_weight,
    })
}

pub(super) fn retained_contribution(
    application: Option<PhaseTrustApplication>,
    anchor: bool,
) -> Option<MarketOffsetContribution> {
    application.map(|application| MarketOffsetContribution {
        phase: application.phase,
        anchor,
        p0: application.p0,
        p1: application.p1,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn attach_retained_contribution(
    trainer: &mut OracleMicrostructureLogisticTrainer,
    observed: ObservedMicrostructureMarket,
    p0: f64,
    score: OracleMicrostructureLogisticScore,
    shock_toxicity: f64,
    markout_toxicity: f64,
    cfg: &CurrentBlendedFairValueConfig,
) -> (
    Option<TrustSelection>,
    Option<PhaseTrustApplication>,
    Option<RetainedObservationHandle>,
) {
    let Some(policy) = trainer.application_policy else {
        return (None, None, None);
    };
    let Some(selection) = trust_selection(
        policy,
        observed.phase,
        observed.first_authorization_marked,
        &trainer.phase_summaries,
        trainer.first_decision_summary,
    ) else {
        return (None, None, None);
    };
    let application = prospective_application(
        Some(policy),
        trainer.fit,
        trainer.posterior.is_some(),
        selection,
        p0,
        score,
        shock_toxicity,
        markout_toxicity,
        cfg,
    );
    let retained_handle = observed.retained_handle();
    if policy.uses_final_output_trust() {
        return (Some(selection), application, retained_handle);
    }
    if let Some(target) = observed.target {
        if let Some(pending) = trainer.pending_by_window.get_mut(&observed.window) {
            let contribution = retained_contribution(
                application,
                matches!(target, RetainedObservationTarget::Anchor),
            );
            match target {
                RetainedObservationTarget::Anchor => {
                    if let Some(observation) = pending.anchor.as_mut() {
                        observation.market_offset_contribution = contribution;
                    }
                    if let Some(observation) = observed
                        .phase
                        .and_then(|phase| pending.bins.get_mut(phase))
                        .and_then(Option::as_mut)
                    {
                        observation.market_offset_contribution = contribution;
                    }
                }
                RetainedObservationTarget::Bin(phase) => {
                    if let Some(observation) = pending.bins.get_mut(phase).and_then(Option::as_mut)
                    {
                        observation.market_offset_contribution = contribution;
                    }
                }
            }
        }
    }
    (Some(selection), application, None)
}

pub(in crate::strategy::support::oracle_authority) fn attach_final_output_context(
    trainer: &mut OracleMicrostructureLogisticTrainer,
    handle: RetainedObservationHandle,
    mut context: FinalOutputObservationContext,
) {
    let Some(pending) = trainer.pending_by_window.get_mut(&handle.window) else {
        return;
    };
    context.anchor = matches!(handle.target, RetainedObservationTarget::Anchor);
    match handle.target {
        RetainedObservationTarget::Anchor => {
            if let Some(observation) = pending.anchor.as_mut() {
                observation.final_output_context = Some(context);
            }
            if let Some(observation) = pending
                .bins
                .get_mut(context.phase as usize)
                .and_then(Option::as_mut)
            {
                observation.final_output_context = Some(context);
            }
        }
        RetainedObservationTarget::Bin(phase) => {
            if let Some(observation) = pending.bins.get_mut(phase).and_then(Option::as_mut) {
                observation.final_output_context = Some(context);
            }
        }
    }
}

pub(super) fn finalize_final_output_contributions(
    observations: &mut [FrozenMicrostructureObservation],
) {
    for observation in observations {
        let Some(context) = observation.final_output_context.take() else {
            continue;
        };
        let market_confirmation = final_output_model_agreement(
            context.missing_model_p,
            context.market_p,
            context.spread_up,
            context.spread_down,
            context.depth_up,
            context.depth_down,
            context.score_cfg,
        );
        let (base_interval, direct_market_selected) = match context.missing_interval {
            FinalOutputMissingInterval::Incoming(interval) => (interval, false),
            FinalOutputMissingInterval::DirectMarket(interval) => (interval, true),
            FinalOutputMissingInterval::Learner(input) => (input.project(context.missing_p), false),
        };
        let Some(alternative) = project_final_output_arm(
            FinalOutputArmProjectionInput {
                direct_market_selected,
                base_p_fair: context.missing_p,
                base_interval,
                market_p_up: context.market_p,
                market_confirmation,
                spread_up: context.spread_up,
                spread_down: context.spread_down,
                depth_up_at_size: context.depth_up,
                depth_down_at_size: context.depth_down,
                min_confidence: context.min_confidence,
                confidence_market_weight: context.confidence_market_weight,
                confidence_spread_weight: context.confidence_spread_weight,
                confidence_depth_weight: context.confidence_depth_weight,
            },
            context.raw_beta,
        ) else {
            continue;
        };
        let (p0, p1) = if context.selected_arm {
            (alternative.mean, context.published_p)
        } else {
            (context.published_p, alternative.mean)
        };
        observation.market_offset_contribution = Some(MarketOffsetContribution {
            phase: context.phase,
            anchor: context.anchor,
            p0,
            p1,
        });
    }
}

pub(super) const fn phase_label(phase: u8) -> Option<&'static str> {
    match phase {
        0 => Some("phase_0"),
        1 => Some("phase_1"),
        2 => Some("phase_2"),
        3 => Some("phase_3"),
        4 => Some("phase_4"),
        _ => None,
    }
}

pub(super) fn phase_index(
    window_start_ms: u64,
    window_end_ms: u64,
    decision_ts_ms: u64,
) -> Option<usize> {
    let duration_ms = window_end_ms.checked_sub(window_start_ms)?;
    if duration_ms == 0 || decision_ts_ms < window_start_ms || decision_ts_ms >= window_end_ms {
        return None;
    }
    let elapsed_ms = decision_ts_ms - window_start_ms;
    Some(
        ((elapsed_ms as u128 * MICRO_LOGISTIC_OBSERVATION_BIN_COUNT as u128) / duration_ms as u128)
            as usize,
    )
}

pub(super) fn add_window(
    summaries: &mut [PhaseTrustSummary; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    first_summary: &mut PhaseTrustSummary,
    policy: OracleLogisticApplicationPolicy,
    window: &ResolvedMicrostructureWindow,
) {
    for (phase, loss) in market_phase_losses(window).into_iter().enumerate() {
        if let Some(loss) = loss {
            summaries[phase].add(loss);
        }
    }
    if policy.uses_first_decision_trust() {
        if let Some(loss) = market_first_decision_loss(window) {
            first_summary.add(loss);
        }
    }
}

pub(super) fn remove_window(
    summaries: &mut [PhaseTrustSummary; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    first_summary: &mut PhaseTrustSummary,
    policy: OracleLogisticApplicationPolicy,
    window: &ResolvedMicrostructureWindow,
) {
    for (phase, loss) in market_phase_losses(window).into_iter().enumerate() {
        if let Some(loss) = loss {
            summaries[phase].remove(loss);
        }
    }
    if policy.uses_first_decision_trust() {
        if let Some(loss) = market_first_decision_loss(window) {
            first_summary.remove(loss);
        }
    }
}

pub(super) fn summaries_from_windows<'a>(
    policy: OracleLogisticApplicationPolicy,
    windows: impl IntoIterator<Item = &'a ResolvedMicrostructureWindow>,
) -> (
    [PhaseTrustSummary; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT],
    PhaseTrustSummary,
) {
    let mut summaries = [PhaseTrustSummary::default(); MICRO_LOGISTIC_OBSERVATION_BIN_COUNT];
    let mut first_summary = PhaseTrustSummary::default();
    for window in windows {
        add_window(&mut summaries, &mut first_summary, policy, window);
    }
    (summaries, first_summary)
}

fn market_first_decision_loss(window: &ResolvedMicrostructureWindow) -> Option<MarketPhaseLoss> {
    let example = window
        .observations
        .iter()
        .find(|example| example.observation.first_decision_contribution.is_some())?;
    let contribution = example.observation.first_decision_contribution?;
    Some(MarketPhaseLoss {
        p0_brier: (contribution.p0 - example.y_up).powi(2),
        p1_brier: (contribution.p1 - example.y_up).powi(2),
        p0_log: binary_log_loss(example.y_up, contribution.p0),
        p1_log: binary_log_loss(example.y_up, contribution.p1),
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct MarketPhaseLoss {
    p0_brier: f64,
    p1_brier: f64,
    p0_log: f64,
    p1_log: f64,
}

fn market_phase_losses(
    window: &ResolvedMicrostructureWindow,
) -> [Option<MarketPhaseLoss>; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT] {
    let mut totals = [MarketPhaseLoss::default(); MICRO_LOGISTIC_OBSERVATION_BIN_COUNT];
    let mut counts = [0_u32; MICRO_LOGISTIC_OBSERVATION_BIN_COUNT];
    for example in &window.observations {
        let Some(contribution) = example.observation.market_offset_contribution else {
            continue;
        };
        let phase = contribution.phase as usize;
        if phase >= MICRO_LOGISTIC_OBSERVATION_BIN_COUNT {
            continue;
        }
        let y_up = example.y_up;
        totals[phase].p0_brier += (contribution.p0 - y_up).powi(2);
        totals[phase].p1_brier += (contribution.p1 - y_up).powi(2);
        totals[phase].p0_log += binary_log_loss(y_up, contribution.p0);
        totals[phase].p1_log += binary_log_loss(y_up, contribution.p1);
        counts[phase] = counts[phase].saturating_add(1);
    }
    std::array::from_fn(|phase| {
        let count = counts[phase];
        (count > 0).then(|| {
            let divisor = count as f64;
            MarketPhaseLoss {
                p0_brier: totals[phase].p0_brier / divisor,
                p1_brier: totals[phase].p1_brier / divisor,
                p0_log: totals[phase].p0_log / divisor,
                p1_log: totals[phase].p1_log / divisor,
            }
        })
    })
}

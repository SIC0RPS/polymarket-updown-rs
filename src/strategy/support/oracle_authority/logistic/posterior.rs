use std::collections::{BTreeMap, VecDeque};

use nalgebra::{SMatrix, SVector};

use crate::strategy::fair_value::sigmoid;

use super::trainer::{LabeledMicrostructureExample, ResolvedMicrostructureWindow};
use super::{
    market_offset::recorded_prior_logit, observation_design, OracleLogisticFeatureLayout,
    MICRO_LOGISTIC_PARAMETER_COUNT,
};

const NEWTON_MAX_ITERATIONS: usize = 24;
const NEWTON_MAX_BACKTRACKS: usize = 8;
const NEWTON_DECREMENT_TOLERANCE: f64 = 1e-8;
const ARMIJO_COEFFICIENT: f64 = 1e-4;
const BACKTRACK_SHRINK: f64 = 0.5;
const BASELINE_PARAMETER_COUNT: usize = MICRO_LOGISTIC_PARAMETER_COUNT - 2;
const EXPERIMENTAL_PARAMETER_COUNT: usize = MICRO_LOGISTIC_PARAMETER_COUNT - 1;

pub(in crate::strategy::support::oracle_authority) type PosteriorVector =
    SVector<f64, MICRO_LOGISTIC_PARAMETER_COUNT>;
type BaselinePosteriorVector = SVector<f64, BASELINE_PARAMETER_COUNT>;
type BaselinePosteriorMatrix = SMatrix<f64, BASELINE_PARAMETER_COUNT, BASELINE_PARAMETER_COUNT>;
type ExperimentalPosteriorMatrix =
    SMatrix<f64, EXPERIMENTAL_PARAMETER_COUNT, EXPERIMENTAL_PARAMETER_COUNT>;
type TradeFlowPosteriorMatrix =
    SMatrix<f64, MICRO_LOGISTIC_PARAMETER_COUNT, MICRO_LOGISTIC_PARAMETER_COUNT>;

#[derive(Debug, Clone)]
pub(super) enum Posterior {
    Baseline(BaselinePosteriorMatrix),
    Experimental(ExperimentalPosteriorMatrix),
    TradeFlow(TradeFlowPosteriorMatrix),
}

impl Posterior {
    pub(super) fn applied_variance(&self, design: &PosteriorVector) -> Option<f64> {
        let variance = match self {
            Self::Baseline(covariance) => {
                let design = BaselinePosteriorVector::from_fn(|row, _| design[row]);
                (design.transpose() * covariance * design)[(0, 0)]
            }
            Self::Experimental(covariance) => {
                let design =
                    SVector::<f64, EXPERIMENTAL_PARAMETER_COUNT>::from_fn(|row, _| design[row]);
                (design.transpose() * covariance * design)[(0, 0)]
            }
            Self::TradeFlow(covariance) => (design.transpose() * covariance * design)[(0, 0)],
        };
        (variance.is_finite() && variance >= 0.0).then_some(variance)
    }
}

pub(super) fn fit_map(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<ResolvedMicrostructureWindow>,
    l2_penalty: f64,
    trade_flow_enabled: bool,
) -> Option<(PosteriorVector, Posterior)> {
    if trade_flow_enabled {
        let (parameters, covariance) =
            fit_map_fixed::<MICRO_LOGISTIC_PARAMETER_COUNT>(layout, resolved_windows, l2_penalty)?;
        return Some((parameters, Posterior::TradeFlow(covariance)));
    }
    match layout {
        OracleLogisticFeatureLayout::Baseline | OracleLogisticFeatureLayout::MarketOffset => {
            let (parameters, covariance) =
                fit_map_fixed::<BASELINE_PARAMETER_COUNT>(layout, resolved_windows, l2_penalty)?;
            Some((
                expand_parameters(&parameters),
                Posterior::Baseline(covariance),
            ))
        }
        OracleLogisticFeatureLayout::ReferenceOnly
        | OracleLogisticFeatureLayout::RepresentationOnly
        | OracleLogisticFeatureLayout::Combined => {
            let (parameters, covariance) = fit_map_fixed::<EXPERIMENTAL_PARAMETER_COUNT>(
                layout,
                resolved_windows,
                l2_penalty,
            )?;
            Some((
                expand_parameters(&parameters),
                Posterior::Experimental(covariance),
            ))
        }
    }
}

fn fit_map_fixed<const N: usize>(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<ResolvedMicrostructureWindow>,
    l2_penalty: f64,
) -> Option<(SVector<f64, N>, SMatrix<f64, N, N>)> {
    let mut parameters = SVector::<f64, N>::zeros();
    let mut converged = false;
    for _ in 0..NEWTON_MAX_ITERATIONS {
        let (objective, gradient, hessian) =
            posterior_terms(layout, resolved_windows, &parameters, l2_penalty)?;
        let cholesky = hessian.cholesky()?;
        let step = cholesky.solve(&gradient);
        let decrement = gradient.dot(&step) * 0.5;
        if !(decrement.is_finite() && decrement >= 0.0) {
            return None;
        }
        if decrement <= NEWTON_DECREMENT_TOLERANCE {
            converged = true;
            break;
        }
        let directional_derivative = -gradient.dot(&step);
        let mut step_scale = 1.0;
        let mut accepted = false;
        for _ in 0..NEWTON_MAX_BACKTRACKS {
            let candidate = &parameters - step_scale * &step;
            let candidate_objective =
                posterior_objective(layout, resolved_windows, &candidate, l2_penalty)?;
            if candidate_objective
                <= objective + ARMIJO_COEFFICIENT * step_scale * directional_derivative
            {
                parameters = candidate;
                accepted = true;
                break;
            }
            step_scale *= BACKTRACK_SHRINK;
        }
        if !accepted {
            return None;
        }
    }
    if !converged || !parameters.iter().all(|value| value.is_finite()) {
        return None;
    }
    let (_, _, hessian) = posterior_terms(layout, resolved_windows, &parameters, l2_penalty)?;
    let bread = hessian.cholesky()?.inverse();
    let covariance = clustered_sandwich_covariance(layout, resolved_windows, &parameters, &bread)?;
    covariance
        .iter()
        .all(|value| value.is_finite())
        .then_some((parameters, covariance))
}

fn expand_parameters<const N: usize>(parameters: &SVector<f64, N>) -> PosteriorVector {
    let mut expanded = PosteriorVector::zeros();
    for (target, value) in expanded.iter_mut().zip(parameters.iter()) {
        *target = *value;
    }
    expanded
}

fn clustered_sandwich_covariance<const N: usize>(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<ResolvedMicrostructureWindow>,
    parameters: &SVector<f64, N>,
    bread: &SMatrix<f64, N, N>,
) -> Option<SMatrix<f64, N, N>> {
    let mut cluster_scores = BTreeMap::<(u64, u64), SVector<f64, N>>::new();
    for window in resolved_windows {
        let Some(first_sample) = window.observations.first() else {
            continue;
        };
        let observation_weight = 1.0 / window.observations.len() as f64;
        let mut window_score = SVector::<f64, N>::zeros();
        for sample in &window.observations {
            let design = sample_design::<N>(layout, sample)?;
            let eta =
                design.dot(parameters) + recorded_prior_logit(layout, sample.observation.prior_p)?;
            let probability = sigmoid(eta).clamp(0.0, 1.0);
            window_score += design * (probability - sample.y_up) * observation_weight;
        }
        *cluster_scores
            .entry((first_sample.window_start_ms, first_sample.window_end_ms))
            .or_insert_with(SVector::<f64, N>::zeros) += window_score;
    }
    let cluster_count = cluster_scores.len();
    if cluster_count == 0 {
        return None;
    }
    if cluster_count == 1 {
        return Some(*bread);
    }
    let meat = cluster_scores
        .values()
        .fold(SMatrix::<f64, N, N>::zeros(), |meat, score| {
            meat + score * score.transpose()
        });
    let finite_cluster_correction = cluster_count as f64 / (cluster_count - 1) as f64;
    let covariance = bread * meat * bread * finite_cluster_correction;
    covariance
        .iter()
        .all(|value| value.is_finite())
        .then_some(covariance)
}

fn posterior_objective<const N: usize>(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<ResolvedMicrostructureWindow>,
    parameters: &SVector<f64, N>,
    l2_penalty: f64,
) -> Option<f64> {
    let mut objective = 0.5 * l2_penalty * parameters.dot(parameters);
    for window in resolved_windows {
        if window.observations.is_empty() {
            continue;
        }
        let observation_weight = 1.0 / window.observations.len() as f64;
        for sample in &window.observations {
            let design = sample_design::<N>(layout, sample)?;
            let eta =
                design.dot(parameters) + recorded_prior_logit(layout, sample.observation.prior_p)?;
            if !eta.is_finite() {
                return None;
            }
            objective += observation_weight * (softplus(eta) - sample.y_up * eta);
        }
    }
    objective.is_finite().then_some(objective)
}

fn posterior_terms<const N: usize>(
    layout: OracleLogisticFeatureLayout,
    resolved_windows: &VecDeque<ResolvedMicrostructureWindow>,
    parameters: &SVector<f64, N>,
    l2_penalty: f64,
) -> Option<(f64, SVector<f64, N>, SMatrix<f64, N, N>)> {
    let mut objective = 0.5 * l2_penalty * parameters.dot(parameters);
    let mut gradient = l2_penalty * parameters;
    let mut hessian = SMatrix::<f64, N, N>::identity() * l2_penalty;
    for window in resolved_windows {
        if window.observations.is_empty() {
            continue;
        }
        let observation_weight = 1.0 / window.observations.len() as f64;
        for sample in &window.observations {
            let design = sample_design::<N>(layout, sample)?;
            let eta =
                design.dot(parameters) + recorded_prior_logit(layout, sample.observation.prior_p)?;
            if !eta.is_finite() {
                return None;
            }
            let probability = sigmoid(eta).clamp(0.0, 1.0);
            objective += observation_weight * (softplus(eta) - sample.y_up * eta);
            gradient += observation_weight * design * (probability - sample.y_up);
            hessian += observation_weight
                * design
                * design.transpose()
                * (probability * (1.0 - probability));
        }
    }
    (objective.is_finite()
        && gradient.iter().all(|value| value.is_finite())
        && hessian.iter().all(|value| value.is_finite()))
    .then_some((objective, gradient, hessian))
}

fn sample_design<const N: usize>(
    layout: OracleLogisticFeatureLayout,
    sample: &LabeledMicrostructureExample,
) -> Option<SVector<f64, N>> {
    let design = observation_design(layout, &sample.observation)?;
    Some(SVector::<f64, N>::from_fn(|row, _| design[row]))
}

fn softplus(value: f64) -> f64 {
    if value > 0.0 {
        value + (-value).exp().ln_1p()
    } else {
        value.exp().ln_1p()
    }
}

use std::collections::{BTreeMap, VecDeque};

use nalgebra::{SMatrix, SVector};

use crate::strategy::fair_value::sigmoid;

use super::beta_features;

const PARAMETER_COUNT: usize = 3;
const NEWTON_MAX_ITERATIONS: usize = 24;
const NEWTON_DECREMENT_TOLERANCE: f64 = 1e-8;
const ARMIJO_COEFFICIENT: f64 = 1e-4;
const BACKTRACK_SHRINK: f64 = 0.5;
const MIN_BACKTRACK_STEP: f64 = 1.490_116_119_384_765_6e-8;

pub(in crate::strategy::support::oracle_authority) type BetaCalibrationParameters =
    SVector<f64, PARAMETER_COUNT>;
type BetaCalibrationMatrix = SMatrix<f64, PARAMETER_COUNT, PARAMETER_COUNT>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::strategy::support::oracle_authority) struct BetaCalibrationPosterior {
    covariance: BetaCalibrationMatrix,
}

impl BetaCalibrationPosterior {
    pub(in crate::strategy::support::oracle_authority) fn logit_std_at(
        self,
        probability: f64,
    ) -> Option<f64> {
        let design = sample_design(probability);
        let variance = outward_up(design.dot(&(self.covariance * design)));
        (variance.is_finite() && variance >= 0.0)
            .then(|| outward_up(variance.sqrt()))
            .filter(|value| value.is_finite())
    }

    pub(super) fn logit_std_upper(self, lo: f64, hi: f64) -> Option<f64> {
        let (lo_a, lo_b) = beta_features(lo);
        let (hi_a, hi_b) = beta_features(hi);
        let feature_bounds = BetaCalibrationParameters::new(
            lo_a.abs().max(hi_a.abs()),
            lo_b.abs().max(hi_b.abs()),
            1.0,
        );
        let mut variance_bound = 0.0;
        for row in 0..PARAMETER_COUNT {
            for column in 0..PARAMETER_COUNT {
                let first_product =
                    outward_up(self.covariance[(row, column)].abs() * feature_bounds[row]);
                let term = outward_up(first_product * feature_bounds[column]);
                variance_bound = outward_up(variance_bound + term);
            }
        }
        (variance_bound.is_finite() && variance_bound >= 0.0)
            .then(|| outward_up(variance_bound.sqrt()))
            .filter(|value| value.is_finite())
    }
}

pub(in crate::strategy::support::oracle_authority) trait BetaCalibrationSample {
    fn probability(&self) -> f64;
    fn label(&self) -> f64;
    fn window_identity(&self) -> (u64, u64);
}

pub(in crate::strategy::support::oracle_authority) fn fit_map<S: BetaCalibrationSample>(
    samples: &VecDeque<S>,
    l2_penalty: f64,
) -> Option<(BetaCalibrationParameters, BetaCalibrationPosterior)> {
    let mut parameters = BetaCalibrationParameters::new(1.0, 1.0, 0.0);
    let mut converged = false;
    for _ in 0..NEWTON_MAX_ITERATIONS {
        let (objective, gradient, hessian) = posterior_terms(samples, &parameters, l2_penalty)?;
        let cholesky = hessian.cholesky()?;
        let step = cholesky.solve(&gradient);
        let decrement = 0.5 * gradient.dot(&step);
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
        while step_scale >= MIN_BACKTRACK_STEP {
            let candidate = parameters - step_scale * step;
            let candidate_objective = posterior_objective(samples, &candidate, l2_penalty)?;
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
    if !converged {
        let (_, gradient, hessian) = posterior_terms(samples, &parameters, l2_penalty)?;
        let step = hessian.cholesky()?.solve(&gradient);
        let decrement = 0.5 * gradient.dot(&step);
        converged =
            decrement.is_finite() && decrement >= 0.0 && decrement <= NEWTON_DECREMENT_TOLERANCE;
    }
    if !converged || !parameters.iter().all(|value| value.is_finite()) {
        return None;
    }
    let (_, _, hessian) = posterior_terms(samples, &parameters, l2_penalty)?;
    let bread = hessian.cholesky()?.inverse();
    let covariance = clustered_sandwich_covariance(samples, &parameters, &bread)?;
    covariance
        .iter()
        .all(|value| value.is_finite())
        .then_some((parameters, BetaCalibrationPosterior { covariance }))
}

fn clustered_sandwich_covariance<S: BetaCalibrationSample>(
    samples: &VecDeque<S>,
    parameters: &BetaCalibrationParameters,
    bread: &BetaCalibrationMatrix,
) -> Option<BetaCalibrationMatrix> {
    let mut cluster_scores = BTreeMap::<(u64, u64), BetaCalibrationParameters>::new();
    for sample in samples {
        let design = sample_design(sample.probability());
        let probability = sigmoid(design.dot(parameters)).clamp(0.0, 1.0);
        let score = design * (probability - sample.label());
        *cluster_scores
            .entry(sample.window_identity())
            .or_insert_with(BetaCalibrationParameters::zeros) += score;
    }
    let cluster_count = cluster_scores.len();
    if cluster_count < 2 {
        return None;
    }
    let meat = cluster_scores
        .values()
        .fold(BetaCalibrationMatrix::zeros(), |meat, score| {
            meat + score * score.transpose()
        });
    let finite_cluster_correction = cluster_count as f64 / (cluster_count - 1) as f64;
    let covariance = bread * meat * bread * finite_cluster_correction;
    covariance
        .iter()
        .all(|value| value.is_finite())
        .then_some(covariance)
}

fn posterior_objective<S: BetaCalibrationSample>(
    samples: &VecDeque<S>,
    parameters: &BetaCalibrationParameters,
    l2_penalty: f64,
) -> Option<f64> {
    let centered_a = parameters[0] - 1.0;
    let centered_b = parameters[1] - 1.0;
    let centered_c = parameters[2];
    let mut objective = 0.5
        * l2_penalty
        * (centered_a * centered_a + centered_b * centered_b + centered_c * centered_c);
    for sample in samples {
        let design = sample_design(sample.probability());
        let eta = design.dot(parameters);
        if !eta.is_finite() {
            return None;
        }
        objective += softplus(eta) - sample.label() * eta;
    }
    objective.is_finite().then_some(objective)
}

fn posterior_terms<S: BetaCalibrationSample>(
    samples: &VecDeque<S>,
    parameters: &BetaCalibrationParameters,
    l2_penalty: f64,
) -> Option<(f64, BetaCalibrationParameters, BetaCalibrationMatrix)> {
    let centered_a = parameters[0] - 1.0;
    let centered_b = parameters[1] - 1.0;
    let centered_c = parameters[2];
    let mut objective = 0.5
        * l2_penalty
        * (centered_a * centered_a + centered_b * centered_b + centered_c * centered_c);
    let mut gradient = BetaCalibrationParameters::new(
        l2_penalty * centered_a,
        l2_penalty * centered_b,
        l2_penalty * centered_c,
    );
    let mut hessian = BetaCalibrationMatrix::zeros();
    hessian[(0, 0)] = l2_penalty;
    hessian[(1, 1)] = l2_penalty;
    hessian[(2, 2)] = l2_penalty;
    for sample in samples {
        let design = sample_design(sample.probability());
        let eta = design.dot(parameters);
        if !eta.is_finite() {
            return None;
        }
        let probability = sigmoid(eta).clamp(0.0, 1.0);
        objective += softplus(eta) - sample.label() * eta;
        gradient += design * (probability - sample.label());
        hessian += design * design.transpose() * (probability * (1.0 - probability));
    }
    (objective.is_finite()
        && gradient.iter().all(|value| value.is_finite())
        && hessian.iter().all(|value| value.is_finite()))
    .then_some((objective, gradient, hessian))
}

fn sample_design(probability: f64) -> BetaCalibrationParameters {
    let (a, b) = beta_features(probability);
    BetaCalibrationParameters::new(a, b, 1.0)
}

fn softplus(value: f64) -> f64 {
    if value > 0.0 {
        value + (-value).exp().ln_1p()
    } else {
        value.exp().ln_1p()
    }
}

fn outward_up(value: f64) -> f64 {
    if !value.is_finite() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value > 0.0 { bits + 1 } else { bits - 1 })
}

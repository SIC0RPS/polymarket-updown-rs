use super::{
    logit_from_probability, OracleLogisticFeatureLayout, MICRO_LOGISTIC_PROBABILITY_EPSILON,
};
use crate::strategy::fair_value::sigmoid;
use crate::strategy::support::oracle_authority::probability_interval::{
    probability_hull, ProbabilityInterval,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct BoundedMarketPrior {
    pub(super) probability: f64,
    pub(super) logit: f64,
}

pub(super) fn bounded_prior(p_market: Option<f64>) -> Option<BoundedMarketPrior> {
    p_market
        .and_then(logit_from_probability)
        .map(bounded_prior_from_logit)
}

pub(super) fn bounded_probability_from_logit(market_logit: f64) -> f64 {
    sigmoid(market_logit).clamp(0.0, 1.0).clamp(
        MICRO_LOGISTIC_PROBABILITY_EPSILON,
        1.0 - MICRO_LOGISTIC_PROBABILITY_EPSILON,
    )
}

fn bounded_prior_from_logit(market_logit: f64) -> BoundedMarketPrior {
    let probability = bounded_probability_from_logit(market_logit);
    BoundedMarketPrior {
        probability,
        logit: (probability / (1.0 - probability)).ln(),
    }
}

pub(super) fn recorded_prior_logit(
    layout: OracleLogisticFeatureLayout,
    prior_p: f64,
) -> Option<f64> {
    if !layout.uses_market_offset() {
        return Some(0.0);
    }
    (prior_p.is_finite()
        && (MICRO_LOGISTIC_PROBABILITY_EPSILON..=1.0 - MICRO_LOGISTIC_PROBABILITY_EPSILON)
            .contains(&prior_p))
    .then(|| (prior_p / (1.0 - prior_p)).ln())
    .filter(|logit| logit.is_finite())
}

pub(super) fn correction_interval(correction_logit: f64, std: f64, z: f64) -> Option<[f64; 2]> {
    if !(correction_logit.is_finite() && std.is_finite() && std >= 0.0 && z.is_finite() && z > 0.0)
    {
        return None;
    }
    let radius = (std * z).next_up();
    Some([
        (correction_logit - radius).next_down(),
        (correction_logit + radius).next_up(),
    ])
}

pub(super) fn bounded_interval(interval: ProbabilityInterval) -> Option<ProbabilityInterval> {
    ProbabilityInterval::new(
        bounded_interval_endpoint(interval.lo())?,
        bounded_interval_endpoint(interval.hi())?,
    )
}

pub(super) fn model_interval(
    point_probability: f64,
    correction_logit: f64,
    correction_interval: Option<[f64; 2]>,
    conditional_interval: Option<ProbabilityInterval>,
    market_interval: Option<ProbabilityInterval>,
) -> Option<ProbabilityInterval> {
    let Some(market_interval) = market_interval else {
        return correction_interval.and(conditional_interval);
    };
    let correction = correction_interval.unwrap_or([correction_logit, correction_logit]);
    let lo = sigmoid(logit_from_probability(market_interval.lo())? + correction[0]).clamp(0.0, 1.0);
    let hi = sigmoid(logit_from_probability(market_interval.hi())? + correction[1]).clamp(0.0, 1.0);
    probability_hull(point_probability, [lo, hi])
}

fn bounded_interval_endpoint(probability: f64) -> Option<f64> {
    if probability == 0.0 {
        Some(MICRO_LOGISTIC_PROBABILITY_EPSILON)
    } else if probability == 1.0 {
        Some(1.0 - MICRO_LOGISTIC_PROBABILITY_EPSILON)
    } else {
        bounded_prior(Some(probability)).map(|prior| prior.probability)
    }
}

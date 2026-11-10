use super::calibration::OracleBetaCalibrationScore;
use super::probability_calibration::{
    apply_model_market_loser_bid_ambiguity, apply_model_market_loser_bid_ambiguity_interval,
    OracleProbabilityCalibration,
};
use super::probability_interval::{monotone_probability_hull, ProbabilityInterval};
use super::selection::ewma_step;
use crate::config::CurrentBlendedFairValueConfig;
use crate::strategy::fair_value::CurrentBlended;

#[derive(Debug, Clone, Copy)]
pub(super) struct EdgeProbability {
    pub(super) mean: f64,
    pub(super) interval: Option<ProbabilityInterval>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct EdgeBoundEwmas {
    pub(super) up_lcb: Option<f64>,
    pub(super) up_ucb: Option<f64>,
    pub(super) down_lcb: Option<f64>,
    pub(super) down_ucb: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BetaCalibrationApplication {
    pub(super) score: OracleBetaCalibrationScore,
    pub(super) mean: f64,
    pub(super) interval: Option<ProbabilityInterval>,
}

pub(super) fn compose_calibration_intervals(
    base_interval: Option<ProbabilityInterval>,
    calibration: Option<OracleProbabilityCalibration>,
    calibration_mean: f64,
    beta_calibration: OracleBetaCalibrationScore,
    candidate_beta_mean: f64,
) -> BetaCalibrationApplication {
    let pre_beta_interval = base_interval.and_then(|base| {
        let calibrated = match calibration {
            Some(calibration) => calibration.probability_interval(base)?,
            None => base,
        };
        if !calibrated.contains(calibration_mean) {
            return None;
        }
        Some(calibrated)
    });
    let interval = pre_beta_interval
        .and_then(|interval| beta_calibration.apply_interval(interval, candidate_beta_mean));
    if interval.is_some() {
        BetaCalibrationApplication {
            score: beta_calibration,
            mean: candidate_beta_mean,
            interval,
        }
    } else {
        BetaCalibrationApplication {
            score: beta_calibration.identity(calibration_mean),
            mean: calibration_mean,
            interval: pre_beta_interval,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn edge_bound_ewmas(
    interval: Option<ProbabilityInterval>,
    unwind_up: f64,
    unwind_down: f64,
    previous_up_lcb: Option<f64>,
    previous_up_ucb: Option<f64>,
    previous_down_lcb: Option<f64>,
    previous_down_ucb: Option<f64>,
    alpha: f64,
) -> EdgeBoundEwmas {
    let Some(interval) = interval else {
        return EdgeBoundEwmas::default();
    };
    EdgeBoundEwmas {
        up_lcb: Some(ewma_step(previous_up_lcb, interval.lo() - unwind_up, alpha)),
        up_ucb: Some(ewma_step(previous_up_ucb, interval.hi() - unwind_up, alpha)),
        down_lcb: Some(ewma_step(
            previous_down_lcb,
            (1.0 - interval.hi()) - unwind_down,
            alpha,
        )),
        down_ucb: Some(ewma_step(
            previous_down_ucb,
            (1.0 - interval.lo()) - unwind_down,
            alpha,
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compose_cleanup_edge_probability(
    cfg: &CurrentBlendedFairValueConfig,
    beta_mean: f64,
    beta_interval: Option<ProbabilityInterval>,
    has_executable_ladder: bool,
    unwind_up: f64,
    unwind_down: f64,
    top_bid_up: f64,
    top_bid_down: f64,
    model_p_up: f64,
    market_p_up: Option<f64>,
    loser_bid_ambiguity_weight: f64,
) -> EdgeProbability {
    let fair_model = CurrentBlended::new(cfg);
    let clamped_mean = beta_mean.clamp(0.0, 1.0);
    let rounded_interval = beta_interval.and_then(|interval| {
        monotone_probability_hull(interval, clamped_mean, |p| p.clamp(0.0, 1.0))
    });

    let adverse_mean = if has_executable_ladder {
        fair_model.apply_adverse_selection_haircut(clamped_mean, unwind_up, unwind_down)
    } else {
        clamped_mean
    };
    let adverse_interval = rounded_interval.and_then(|interval| {
        if has_executable_ladder {
            fair_model.apply_adverse_selection_haircut_interval(interval, adverse_mean)
        } else {
            interval.contains(adverse_mean).then_some(interval)
        }
    });

    let top_loser_mean =
        fair_model.apply_top_loser_adverse_selection(adverse_mean, top_bid_up, top_bid_down);
    let top_loser_interval = adverse_interval.and_then(|interval| {
        fair_model.apply_top_loser_adverse_selection_interval(interval, top_loser_mean)
    });

    let mean = apply_model_market_loser_bid_ambiguity(
        top_loser_mean,
        model_p_up,
        market_p_up,
        top_bid_up,
        top_bid_down,
        cfg.executable_ev_scale,
        loser_bid_ambiguity_weight,
    );
    let interval = top_loser_interval
        .and_then(|interval| apply_model_market_loser_bid_ambiguity_interval(interval, mean));
    EdgeProbability { mean, interval }
}

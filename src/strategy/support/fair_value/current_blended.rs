use crate::config::CurrentBlendedFairValueConfig;
use crate::strategy::support::oracle_authority::probability_interval::{
    probability_hull, toward_midpoint_hull, ProbabilityInterval,
};

use super::{normal_cdf, sigmoid, FairValueContext, FairValueModel, FairValueResult};

const MICROSTRUCTURE_BARRIER_SIGMA_BAND: f64 = 3.0;
const PROBABILITY_EPSILON: f64 = 1e-6;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbabilityBlendResult {
    pub(crate) p_fair: f64,
    pub(crate) model_weight: Option<f64>,
}

pub struct CurrentBlended<'a> {
    cfg: &'a CurrentBlendedFairValueConfig,
}

impl<'a> CurrentBlended<'a> {
    pub fn new(cfg: &'a CurrentBlendedFairValueConfig) -> Self {
        Self { cfg }
    }

    pub(crate) fn forecast_p_up(
        &self,
        horizon_s: f64,
        spot_px: f64,
        barrier_px: f64,
        sigma2: f64,
        spot_l1_imbalance: f64,
    ) -> (f64, Option<f64>) {
        if !horizon_s.is_finite() || horizon_s <= 0.0 {
            return (0.5, None);
        }
        if !spot_px.is_finite() || !barrier_px.is_finite() || spot_px <= 0.0 || barrier_px <= 0.0 {
            return (0.5, None);
        }

        let var = sigma2.max(self.cfg.min_forecast_sigma2) * horizon_s;
        let sigma = var.sqrt();
        if !sigma.is_finite() || sigma <= 0.0 {
            return (0.5, None);
        }

        let log_ratio = (spot_px / barrier_px).ln();
        if !log_ratio.is_finite() {
            return (0.5, None);
        }

        let z = (log_ratio - 0.5 * var) / sigma;
        let p = normal_cdf(z).clamp(0.0, 1.0);
        (
            self.apply_microstructure_logit(p, log_ratio, sigma, spot_l1_imbalance),
            Some(z),
        )
    }

    pub(crate) fn apply_microstructure_logit(
        &self,
        p: f64,
        log_ratio: f64,
        sigma: f64,
        spot_l1_imbalance: f64,
    ) -> f64 {
        let adjustment = self.microstructure_logit_adjustment(log_ratio, sigma, spot_l1_imbalance);
        if adjustment == 0.0 {
            return p.clamp(0.0, 1.0);
        }
        let p = p.clamp(1e-6, 1.0 - 1e-6);
        let logit = (p / (1.0 - p)).ln();
        sigmoid(logit + adjustment).clamp(0.0, 1.0)
    }

    pub(crate) fn microstructure_logit_adjustment(
        &self,
        log_ratio: f64,
        sigma: f64,
        spot_l1_imbalance: f64,
    ) -> f64 {
        let pressure = spot_l1_imbalance.clamp(-1.0, 1.0);
        if self.cfg.depth_pressure_logit_weight <= 0.0
            || pressure == 0.0
            || !sigma.is_finite()
            || sigma <= 0.0
            || !log_ratio.is_finite()
        {
            return 0.0;
        }

        let barrier_band = MICROSTRUCTURE_BARRIER_SIGMA_BAND * sigma;
        let barrier_proximity = (1.0 - (log_ratio.abs() / barrier_band.max(1e-12))).clamp(0.0, 1.0);
        self.cfg.depth_pressure_logit_weight * pressure * barrier_proximity
    }

    pub(crate) fn calibrate_probability(&self, p: f64) -> f64 {
        let logit = probability_logit(p);
        let z =
            self.cfg.prob_calibration_logit_bias + self.cfg.prob_calibration_logit_scale * logit;
        sigmoid(z).clamp(0.0, 1.0)
    }

    pub(crate) fn apply_adverse_selection_haircut(
        &self,
        p_up: f64,
        unwind_up: f64,
        unwind_down: f64,
    ) -> f64 {
        if self.cfg.adverse_selection_k <= 0.0
            || !self.cfg.adverse_selection_k.is_finite()
            || !self.cfg.executable_ev_scale.is_finite()
            || self.cfg.executable_ev_scale <= 0.0
            || !unwind_up.is_finite()
            || !unwind_down.is_finite()
        {
            return p_up.clamp(0.0, 1.0);
        }
        let p_up = p_up.clamp(0.0, 1.0);
        let p_down = 1.0 - p_up;
        let keep_up_ev = p_up + unwind_down;
        let keep_down_ev = p_down + unwind_up;
        let k = self.cfg.adverse_selection_k.clamp(0.0, 1.0);
        let band = self.cfg.executable_ev_scale.max(1e-9);
        if keep_up_ev > keep_down_ev {
            let adverse_factor = ((unwind_down - p_down) / band).clamp(0.0, 1.0);
            clamp_toward_midpoint(p_up, p_up * (1.0 - k * adverse_factor))
        } else if keep_down_ev > keep_up_ev {
            let adverse_factor = ((unwind_up - p_up) / band).clamp(0.0, 1.0);
            let adjusted_p_down = p_down * (1.0 - k * adverse_factor);
            clamp_toward_midpoint(p_up, 1.0 - adjusted_p_down)
        } else {
            p_up
        }
    }

    pub(crate) fn apply_adverse_selection_haircut_interval(
        &self,
        input: ProbabilityInterval,
        mean: f64,
    ) -> Option<ProbabilityInterval> {
        toward_midpoint_hull(input, mean)
    }

    pub(crate) fn apply_top_loser_adverse_selection(
        &self,
        p_up: f64,
        top_unwind_up: f64,
        top_unwind_down: f64,
    ) -> f64 {
        if self.cfg.top_loser_adverse_selection_k <= 0.0
            || !self.cfg.top_loser_adverse_selection_k.is_finite()
            || !self.cfg.executable_ev_scale.is_finite()
            || self.cfg.executable_ev_scale <= 0.0
            || !top_unwind_up.is_finite()
            || !top_unwind_down.is_finite()
        {
            return p_up.clamp(0.0, 1.0);
        }
        let p_up = p_up.clamp(1e-6, 1.0 - 1e-6);
        let p_down = 1.0 - p_up;
        let band = self.cfg.executable_ev_scale.max(1e-9);
        let up_adverse = ((top_unwind_down - p_down) / band).clamp(0.0, 1.0);
        let down_adverse = ((top_unwind_up - p_up) / band).clamp(0.0, 1.0);
        let pressure = (down_adverse - up_adverse).clamp(-1.0, 1.0);
        if pressure == 0.0 {
            return p_up;
        }
        let logit = (p_up / (1.0 - p_up)).ln();
        let adjusted =
            sigmoid(logit + self.cfg.top_loser_adverse_selection_k * pressure).clamp(0.0, 1.0);
        clamp_toward_midpoint(p_up, adjusted)
    }

    pub(crate) fn apply_top_loser_adverse_selection_interval(
        &self,
        input: ProbabilityInterval,
        mean: f64,
    ) -> Option<ProbabilityInterval> {
        toward_midpoint_hull(input, mean)
    }

    pub(crate) fn blend_probability(
        &self,
        p_model: f64,
        p_market: Option<f64>,
        shock_toxicity: f64,
        markout_toxicity: f64,
        model_trust: f64,
    ) -> ProbabilityBlendResult {
        let model_trust = model_trust.clamp(0.0, 1.0);
        let Some(p_market) = p_market else {
            return ProbabilityBlendResult {
                p_fair: p_model.clamp(0.0, 1.0),
                model_weight: None,
            };
        };

        let edge_abs = (p_model - p_market).abs();
        let edge_score = (edge_abs / self.cfg.model_blend_edge_full).clamp(0.0, 1.0);
        let toxicity = (shock_toxicity + markout_toxicity).clamp(0.0, 4.0);
        let tox_penalty = (self.cfg.model_blend_toxicity_k * toxicity).clamp(0.0, 1.0);

        let range = (self.cfg.model_blend_max_weight - self.cfg.model_blend_min_weight).max(0.0);
        let model_weight = model_trust
            * (self.cfg.model_blend_min_weight + range * edge_score * (1.0 - tox_penalty)).clamp(
                self.cfg.model_blend_min_weight,
                self.cfg.model_blend_max_weight,
            );
        let p_fair = (model_weight * p_model + (1.0 - model_weight) * p_market).clamp(0.0, 1.0);
        ProbabilityBlendResult {
            p_fair,
            model_weight: Some(model_weight),
        }
    }

    pub(crate) fn blend_probability_interval(
        &self,
        p_model: ProbabilityInterval,
        mean: f64,
        p_market: Option<f64>,
        market_interval: Option<ProbabilityInterval>,
        shock_toxicity: f64,
        markout_toxicity: f64,
        model_trust: f64,
    ) -> Option<ProbabilityInterval> {
        if !self.cfg.model_blend_edge_full.is_finite()
            || self.cfg.model_blend_edge_full <= 0.0
            || !self.cfg.model_blend_min_weight.is_finite()
            || !self.cfg.model_blend_max_weight.is_finite()
            || self.cfg.model_blend_max_weight < self.cfg.model_blend_min_weight
            || self.cfg.model_blend_min_weight < 0.0
            || self.cfg.model_blend_max_weight > 1.0
            || !model_trust.is_finite()
        {
            return None;
        }
        let Some(p_market) = p_market else {
            return probability_hull(mean, [p_model.lo(), p_model.hi(), mean]);
        };
        let market_interval =
            market_interval.or_else(|| ProbabilityInterval::new(p_market, p_market))?;
        if !market_interval.contains(p_market) {
            return None;
        }
        let trust = model_trust.clamp(0.0, 1.0);
        let toxicity = (shock_toxicity + markout_toxicity).clamp(0.0, 4.0);
        let tox_penalty = (self.cfg.model_blend_toxicity_k * toxicity).clamp(0.0, 1.0);
        if !tox_penalty.is_finite() {
            return None;
        }
        let range = self.cfg.model_blend_max_weight - self.cfg.model_blend_min_weight;
        let slope = 2.0 * trust * range * (1.0 - tox_penalty);
        let stationary_distance = if slope > 0.0 {
            (1.0 - trust * self.cfg.model_blend_min_weight) * self.cfg.model_blend_edge_full / slope
        } else {
            f64::INFINITY
        };
        let edge_full = self.cfg.model_blend_edge_full;
        let values = [p_model.lo(), p_model.hi()].into_iter().flat_map(|model| {
            [
                market_interval.lo(),
                market_interval.hi(),
                model,
                model - edge_full,
                model + edge_full,
                model - stationary_distance,
                model + stationary_distance,
            ]
            .into_iter()
            .filter(move |market| market_interval.contains(*market))
            .map(move |market| {
                self.blend_probability(
                    model,
                    Some(market),
                    shock_toxicity,
                    markout_toxicity,
                    model_trust,
                )
                .p_fair
            })
        });
        probability_hull(mean, std::iter::once(mean).chain(values))
    }
}

fn probability_logit(p: f64) -> f64 {
    let p = p.clamp(PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON);
    (p / (1.0 - p)).ln()
}

fn clamp_toward_midpoint(original: f64, adjusted: f64) -> f64 {
    let original = original.clamp(0.0, 1.0);
    let adjusted = adjusted.clamp(0.0, 1.0);
    if original > 0.5 {
        adjusted.clamp(0.5, original)
    } else if original < 0.5 {
        adjusted.clamp(original, 0.5)
    } else {
        0.5
    }
}

impl FairValueModel for CurrentBlended<'_> {
    fn compute(&self, ctx: &FairValueContext) -> FairValueResult {
        let (p_model_raw, spot_z) = self.forecast_p_up(
            ctx.horizon_s,
            ctx.spot_px,
            ctx.open_px,
            ctx.sigma2,
            ctx.spot_l1_imbalance.unwrap_or(0.0),
        );
        let calibrated = self.calibrate_probability(p_model_raw);
        let confidence_scale = ctx.confidence_scale.clamp(0.0, 1.0);
        let p_model = (0.5 + (calibrated - 0.5) * confidence_scale).clamp(0.0, 1.0);
        let blend = self.blend_probability(
            p_model,
            ctx.p_market,
            ctx.shock_toxicity,
            ctx.markout_toxicity,
            1.0,
        );
        FairValueResult {
            p_model_raw,
            p_model,
            p_fair: blend.p_fair,
            model_weight: blend.model_weight,
            depth_pressure: ctx.depth_pressure,
            spot_z,
        }
    }
}

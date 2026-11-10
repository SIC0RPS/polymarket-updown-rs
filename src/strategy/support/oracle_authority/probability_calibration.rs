use super::probability_interval::{toward_midpoint_hull, ProbabilityInterval};

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleProbabilityCalibrationInput {
    pub(super) market_p_up: Option<f64>,
    pub(super) base_p_fair: f64,
    pub(super) market_confirmation: Option<f64>,
    pub(super) spread_up: Option<f64>,
    pub(super) spread_down: Option<f64>,
    pub(super) depth_up_at_size: Option<f64>,
    pub(super) depth_down_at_size: Option<f64>,
    pub(super) min_confidence: f64,
    pub(super) confidence_market_weight: f64,
    pub(super) confidence_spread_weight: f64,
    pub(super) confidence_depth_weight: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleProbabilityCalibration {
    pub(super) p_fair: f64,
    pub(super) confidence: f64,
    pub(super) center: f64,
    pub(super) market_center_weight: f64,
    pub(super) spread_quality: f64,
    pub(super) depth_quality: f64,
}

impl OracleProbabilityCalibration {
    pub(super) fn probability_interval(
        self,
        base: ProbabilityInterval,
    ) -> Option<ProbabilityInterval> {
        let affine = base.affine(self.confidence, self.center * (1.0 - self.confidence))?;
        affine.contains(self.p_fair).then_some(affine)
    }
}

pub(super) fn calibrated_probability_detail(
    input: OracleProbabilityCalibrationInput,
) -> Option<OracleProbabilityCalibration> {
    let base_p_fair = finite_probability_value(input.base_p_fair)?;
    let market_p_up = finite_probability_value(input.market_p_up?)?;
    let market_confirmation = finite_probability_value(input.market_confirmation?)?;
    let min_confidence = finite_probability_value(input.min_confidence)?;
    let spread_quality = spread_quality(input.spread_up, input.spread_down)?;
    let depth_quality = depth_quality(input.depth_up_at_size, input.depth_down_at_size)?;
    let confidence_weights = [
        input.confidence_market_weight,
        input.confidence_spread_weight,
        input.confidence_depth_weight,
    ];
    if !confidence_weights
        .into_iter()
        .all(|value| value.is_finite() && value >= 0.0)
    {
        return None;
    }
    let confidence_weight = input.confidence_market_weight
        + input.confidence_spread_weight
        + input.confidence_depth_weight;
    if !(confidence_weight.is_finite() && confidence_weight > 0.0) {
        return None;
    }
    let confidence = ((input.confidence_market_weight * market_confirmation
        + input.confidence_spread_weight * spread_quality
        + input.confidence_depth_weight * depth_quality)
        / confidence_weight)
        .clamp(min_confidence, 1.0);
    let market_center_weight =
        (market_confirmation * spread_quality * depth_quality).clamp(0.0, 1.0);
    let center = 0.5 + (market_p_up - 0.5) * market_center_weight;
    let calibrated = center + (base_p_fair - center) * confidence;
    Some(OracleProbabilityCalibration {
        p_fair: calibrated.clamp(0.0, 1.0),
        confidence,
        center,
        market_center_weight,
        spread_quality,
        depth_quality,
    })
}

pub(super) fn apply_model_market_loser_bid_ambiguity(
    p_fair: f64,
    model_p_up: f64,
    market_p_up: Option<f64>,
    top_bid_up: f64,
    top_bid_down: f64,
    executable_ev_scale: f64,
    weight: f64,
) -> f64 {
    if !(p_fair.is_finite()
        && model_p_up.is_finite()
        && top_bid_up.is_finite()
        && top_bid_down.is_finite()
        && executable_ev_scale.is_finite()
        && weight.is_finite())
    {
        return p_fair.clamp(0.0, 1.0);
    }
    let Some(market_p_up) = market_p_up.filter(|value| value.is_finite()) else {
        return p_fair.clamp(0.0, 1.0);
    };
    let weight = weight.clamp(0.0, 1.0);
    if weight <= 0.0 || executable_ev_scale <= 0.0 {
        return p_fair.clamp(0.0, 1.0);
    }

    let p_fair = p_fair.clamp(1e-6, 1.0 - 1e-6);
    let fair_edge = p_fair - 0.5;
    if fair_edge == 0.0 {
        return p_fair;
    }

    let protected_is_up = fair_edge > 0.0;
    let model_keep = if protected_is_up {
        model_p_up
    } else {
        1.0 - model_p_up
    }
    .clamp(0.0, 1.0);
    let market_keep = if protected_is_up {
        market_p_up
    } else {
        1.0 - market_p_up
    }
    .clamp(0.0, 1.0);
    let loser_bid = if protected_is_up {
        top_bid_down
    } else {
        top_bid_up
    }
    .clamp(0.0, 1.0);

    let band = executable_ev_scale.max(1e-9);
    let market_over_model = ((market_keep - model_keep) / band).clamp(0.0, 1.0);
    if market_over_model <= 0.0 || loser_bid <= 0.0 {
        return p_fair;
    }

    let ambiguity = (market_over_model * loser_bid).clamp(0.0, 1.0);
    (0.5 + fair_edge * (1.0 - weight * ambiguity)).clamp(0.0, 1.0)
}

pub(super) fn apply_model_market_loser_bid_ambiguity_interval(
    input: ProbabilityInterval,
    mean: f64,
) -> Option<ProbabilityInterval> {
    toward_midpoint_hull(input, mean)
}

fn finite_probability_value(value: f64) -> Option<f64> {
    (value.is_finite() && (0.0..=1.0).contains(&value)).then_some(value)
}

fn spread_quality(spread_up: Option<f64>, spread_down: Option<f64>) -> Option<f64> {
    let spread_up = spread_up.filter(|value| value.is_finite() && (0.0..=1.0).contains(value))?;
    let spread_down =
        spread_down.filter(|value| value.is_finite() && (0.0..=1.0).contains(value))?;
    Some((1.0 - spread_up.max(spread_down) * 2.0).clamp(0.0, 1.0))
}

fn depth_quality(depth_up: Option<f64>, depth_down: Option<f64>) -> Option<f64> {
    let depth_up = depth_up.filter(|value| value.is_finite() && *value >= 0.0)?;
    let depth_down = depth_down.filter(|value| value.is_finite() && *value >= 0.0)?;
    Some(depth_up.min(depth_down).clamp(0.0, 1.0))
}

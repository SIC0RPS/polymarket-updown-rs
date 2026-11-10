use anyhow::{bail, Result};

use super::ta::OracleTaFeatures;
use crate::config::MakerRebateMmStrategyConfig;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct OracleDecisionInput {
    pub(crate) oracle_spot: Option<f64>,
    pub(crate) window_open: Option<f64>,
    pub(crate) model_p_up: Option<f64>,
    pub(crate) market_p_up: Option<f64>,

    pub(crate) best_bid_up: Option<f64>,
    pub(crate) best_ask_up: Option<f64>,
    pub(crate) best_bid_down: Option<f64>,
    pub(crate) best_ask_down: Option<f64>,
    pub(crate) spread_up: Option<f64>,
    pub(crate) spread_down: Option<f64>,
    pub(crate) depth_up_at_size: Option<f64>,
    pub(crate) depth_down_at_size: Option<f64>,

    pub(crate) volatility: Option<f64>,

    pub(crate) ta_ema_trend: Option<f64>,
    pub(crate) ta_macd_trend: Option<f64>,
    pub(crate) ta_rsi_regime: Option<f64>,
    pub(crate) ta_bollinger_position: Option<f64>,
    pub(crate) ta_recent_return_sign: Option<f64>,
    pub(crate) ta_drift_score: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleDecisionScores {
    pub(crate) market_confirmation: Option<f64>,
    pub(crate) market_confirmation_probability_agreement: Option<f64>,
    pub(crate) market_confirmation_spread_quality: Option<f64>,
    pub(crate) market_confirmation_depth_support: Option<f64>,
    pub(crate) market_confirmation_book_complete: Option<f64>,
    pub(crate) ta_alignment: Option<f64>,
    pub(crate) pnl_risk_multiplier: Option<f64>,
    pub(crate) risk_pause_recommended: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OracleDecisionScoreConfig {
    market_confirmation_probability_weight: f64,
    market_confirmation_spread_weight: f64,
    market_confirmation_depth_weight: f64,
    market_confirmation_book_weight: f64,
    ta_boundary_scale: f64,
    ta_boundary_weight: f64,
    ta_token_agreement_weight: f64,
    ta_component_alignment_weight: f64,
}

impl OracleDecisionScoreConfig {
    pub(crate) fn from_strategy_config(cfg: &MakerRebateMmStrategyConfig) -> Self {
        Self {
            market_confirmation_probability_weight: cfg
                .oracle_market_confirmation_probability_weight,
            market_confirmation_spread_weight: cfg.oracle_market_confirmation_spread_weight,
            market_confirmation_depth_weight: cfg.oracle_market_confirmation_depth_weight,
            market_confirmation_book_weight: cfg.oracle_market_confirmation_book_weight,
            ta_boundary_scale: cfg.oracle_ta_boundary_scale,
            ta_boundary_weight: cfg.oracle_ta_boundary_weight,
            ta_token_agreement_weight: cfg.oracle_ta_token_agreement_weight,
            ta_component_alignment_weight: cfg.oracle_ta_component_alignment_weight,
        }
    }
}

impl OracleDecisionInput {
    pub(crate) fn with_ta_features(mut self, ta: OracleTaFeatures) -> Self {
        self.ta_ema_trend = Some(ta.ta_ema_trend);
        self.ta_macd_trend = Some(ta.ta_macd_trend);
        self.ta_rsi_regime = Some(ta.ta_rsi_regime);
        self.ta_bollinger_position = Some(ta.ta_bollinger_position);
        self.ta_recent_return_sign = Some(ta.ta_recent_return_sign);
        self.ta_drift_score = Some(ta.ta_drift_score);
        self
    }

    pub(crate) fn scores(&self, cfg: OracleDecisionScoreConfig) -> OracleDecisionScores {
        let market_confirmation = market_confirmation_score(self, cfg);
        let ta_alignment = ta_alignment_score(self, cfg);
        OracleDecisionScores {
            market_confirmation: market_confirmation.map(|score| score.composite),
            market_confirmation_probability_agreement: market_confirmation
                .map(|score| score.probability_agreement),
            market_confirmation_spread_quality: market_confirmation
                .map(|score| score.spread_quality),
            market_confirmation_depth_support: market_confirmation.map(|score| score.depth_support),
            market_confirmation_book_complete: market_confirmation.map(|score| score.book_complete),
            ta_alignment,
            pnl_risk_multiplier: Some(1.0),
            risk_pause_recommended: false,
        }
    }
}

pub(crate) fn decision_ta_timeframe_for_market_window_ms(window_ms: u64) -> Result<String> {
    if window_ms <= 5 * 60 * 1_000 {
        return Ok("1m".to_string());
    }
    if window_ms == 15 * 60 * 1_000 {
        return Ok("5m".to_string());
    }

    const SECOND_MS: u64 = 1_000;
    const MINUTE_MS: u64 = 60 * SECOND_MS;
    const HOUR_MS: u64 = 60 * MINUTE_MS;
    const DAY_MS: u64 = 24 * HOUR_MS;

    if window_ms == 0 {
        bail!("window_ms must be > 0");
    }
    if window_ms.is_multiple_of(DAY_MS) {
        return Ok(format!("{}d", window_ms / DAY_MS));
    }
    if window_ms.is_multiple_of(HOUR_MS) {
        return Ok(format!("{}h", window_ms / HOUR_MS));
    }
    if window_ms.is_multiple_of(MINUTE_MS) {
        return Ok(format!("{}m", window_ms / MINUTE_MS));
    }
    if window_ms.is_multiple_of(SECOND_MS) {
        return Ok(format!("{}s", window_ms / SECOND_MS));
    }
    bail!("unsupported discovery.window_ms={window_ms} for oracle authority TA timeframe mapping")
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct MarketConfirmationScore {
    composite: f64,
    probability_agreement: f64,
    spread_quality: f64,
    depth_support: f64,
    book_complete: f64,
}

fn market_confirmation_score(
    input: &OracleDecisionInput,
    cfg: OracleDecisionScoreConfig,
) -> Option<MarketConfirmationScore> {
    let p_model = input.model_p_up?;
    let p_market = input.market_p_up.filter(|value| value.is_finite())?;
    let probability_agreement = (1.0 - (p_model - p_market).abs() * 2.0).clamp(0.0, 1.0);
    let spread_penalty = input
        .spread_up
        .into_iter()
        .chain(input.spread_down)
        .filter(|value| value.is_finite())
        .fold(0.0_f64, f64::max)
        .clamp(0.0, 1.0);
    let spread_quality = 1.0 - spread_penalty;
    let depth_support = input
        .depth_up_at_size
        .zip(input.depth_down_at_size)
        .map(|(up, down)| up.min(down).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    let book_complete = [
        input.best_bid_up,
        input.best_ask_up,
        input.best_bid_down,
        input.best_ask_down,
    ]
    .into_iter()
    .flatten()
    .filter(|value| value.is_finite())
    .count() as f64
        / 4.0;
    let total_weight = cfg.market_confirmation_probability_weight
        + cfg.market_confirmation_spread_weight
        + cfg.market_confirmation_depth_weight
        + cfg.market_confirmation_book_weight;
    if !(total_weight.is_finite() && total_weight > 0.0) {
        return None;
    }
    Some(MarketConfirmationScore {
        composite: ((cfg.market_confirmation_probability_weight * probability_agreement
            + cfg.market_confirmation_spread_weight * spread_quality
            + cfg.market_confirmation_depth_weight * depth_support
            + cfg.market_confirmation_book_weight * book_complete)
            / total_weight)
            .clamp(0.0, 1.0),
        probability_agreement,
        spread_quality,
        depth_support,
        book_complete,
    })
}

fn ta_alignment_score(input: &OracleDecisionInput, cfg: OracleDecisionScoreConfig) -> Option<f64> {
    let spot = input
        .oracle_spot
        .filter(|value| value.is_finite() && *value > 0.0)?;
    let open = input
        .window_open
        .filter(|value| value.is_finite() && *value > 0.0)?;
    let volatility = input
        .volatility
        .filter(|value| value.is_finite() && *value > 0.0)?;
    let model_p_up = input.model_p_up.filter(|value| value.is_finite())?;
    let market_p_up = input.market_p_up.filter(|value| value.is_finite())?;

    let boundary_move = (spot / open - 1.0) / volatility.max(1e-9);
    let boundary_score = (boundary_move / cfg.ta_boundary_scale.max(1e-9)).clamp(-1.0, 1.0);
    let model_score = ((model_p_up.clamp(0.0, 1.0) - 0.5) * 2.0).clamp(-1.0, 1.0);
    let market_score = ((market_p_up.clamp(0.0, 1.0) - 0.5) * 2.0).clamp(-1.0, 1.0);
    let token_agreement = if model_score.signum() == market_score.signum() {
        model_score.abs().min(market_score.abs()) * model_score.signum()
    } else {
        0.0
    };

    let closed_ta_alignment = closed_ta_component_alignment(input).unwrap_or(0.0);
    let component_weight = if closed_ta_alignment == 0.0 {
        0.0
    } else {
        cfg.ta_component_alignment_weight
    };
    let total_weight = cfg.ta_boundary_weight + cfg.ta_token_agreement_weight + component_weight;
    if !(total_weight.is_finite() && total_weight > 0.0) {
        return None;
    }
    let alignment = ((cfg.ta_boundary_weight * boundary_score
        + cfg.ta_token_agreement_weight * token_agreement
        + component_weight * closed_ta_alignment)
        / total_weight)
        .clamp(-1.0, 1.0);
    let has_closed_ta_features = [
        input.ta_ema_trend,
        input.ta_macd_trend,
        input.ta_rsi_regime,
        input.ta_bollinger_position,
        input.ta_recent_return_sign,
        input.ta_drift_score,
    ]
    .into_iter()
    .flatten()
    .any(|value| value.is_finite());
    has_closed_ta_features.then_some(alignment)
}

fn closed_ta_component_alignment(input: &OracleDecisionInput) -> Option<f64> {
    let mut sum = 0.0;
    let mut count = 0.0;
    for value in [
        input.ta_ema_trend,
        input.ta_macd_trend,
        input.ta_rsi_regime,
        input.ta_bollinger_position,
        input.ta_recent_return_sign,
        input.ta_drift_score,
    ]
    .into_iter()
    .flatten()
    .filter(|value| value.is_finite())
    {
        sum += value.clamp(-1.0, 1.0);
        count += 1.0;
    }
    (count > 0.0).then_some((sum / count).clamp(-1.0, 1.0))
}

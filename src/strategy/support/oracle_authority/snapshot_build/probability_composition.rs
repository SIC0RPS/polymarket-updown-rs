use crate::polymarket::discovery::MarketSelection;
use crate::strategy::fair_value::FairValueResult;
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::oracle_authority::calibration::OracleBetaCalibrationScore;
use crate::strategy::support::oracle_authority::features::{
    OracleDecisionScoreConfig, OracleDecisionScores,
};
use crate::strategy::support::oracle_authority::final_probability::{self, EdgeProbability};
use crate::strategy::support::oracle_authority::logistic::OracleMicrostructureLogisticScore;
use crate::strategy::support::oracle_authority::probability_calibration::{
    self, OracleProbabilityCalibration,
};
use crate::strategy::support::oracle_authority::probability_interval::ProbabilityInterval;
use crate::strategy::support::oracle_authority::ta::OracleTaFeatures;
use crate::strategy::support::oracle_authority::OracleAuthorityMode;

use super::super::OracleDecisionInput;

pub(super) struct ProbabilityCompositionInput<'a> {
    pub(super) market: &'a MarketSelection,
    pub(super) mode: OracleAuthorityMode,
    pub(super) now_ms: u64,
    pub(super) spot_px: f64,
    pub(super) sigma2: f64,
    pub(super) p_market: Option<f64>,
    pub(super) market_interval: Option<ProbabilityInterval>,
    pub(super) fair_result: FairValueResult,
    pub(super) micro_logistic: OracleMicrostructureLogisticScore,
    pub(super) probability_intervals: Option<ProbabilityInterval>,
    pub(super) up_bid: f64,
    pub(super) up_ask: f64,
    pub(super) down_bid: f64,
    pub(super) down_ask: f64,
    pub(super) up_depth_at_size: Option<f64>,
    pub(super) down_depth_at_size: Option<f64>,
    pub(super) cleanup_unwind: Option<(f64, f64)>,
    pub(super) ta_values: Option<OracleTaFeatures>,
    pub(super) score_cfg: OracleDecisionScoreConfig,
}

pub(super) struct ProbabilityComposition {
    pub(super) decision_scores: OracleDecisionScores,
    pub(super) probability_calibration: Option<OracleProbabilityCalibration>,
    pub(super) raw_beta_calibration: OracleBetaCalibrationScore,
    pub(super) beta_calibration: OracleBetaCalibrationScore,
    pub(super) beta_calibration_attempted_p_up: Option<f64>,
    pub(super) beta_observation_p_up: Option<f64>,
    pub(super) p_fair: f64,
    pub(super) edge_probability: EdgeProbability,
    pub(super) up_edge_reference: f64,
    pub(super) down_edge_reference: f64,
    pub(super) direct_market_selected: bool,
}

pub(in crate::strategy::support::oracle_authority) struct ProbabilityCore {
    probability_calibration: Option<OracleProbabilityCalibration>,
    beta_calibration: OracleBetaCalibrationScore,
    beta_calibration_attempted_p_up: Option<f64>,
    beta_observation_p_up: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) mean: f64,
    pub(in crate::strategy::support::oracle_authority) interval: Option<ProbabilityInterval>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::strategy::support::oracle_authority) struct FinalOutputArmProjectionInput {
    pub(in crate::strategy::support::oracle_authority) direct_market_selected: bool,
    pub(in crate::strategy::support::oracle_authority) base_p_fair: f64,
    pub(in crate::strategy::support::oracle_authority) base_interval: Option<ProbabilityInterval>,
    pub(in crate::strategy::support::oracle_authority) market_p_up: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) market_confirmation: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) spread_up: f64,
    pub(in crate::strategy::support::oracle_authority) spread_down: f64,
    pub(in crate::strategy::support::oracle_authority) depth_up_at_size: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) depth_down_at_size: Option<f64>,
    pub(in crate::strategy::support::oracle_authority) min_confidence: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_market_weight: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_spread_weight: f64,
    pub(in crate::strategy::support::oracle_authority) confidence_depth_weight: f64,
}

pub(in crate::strategy::support::oracle_authority) fn final_output_model_agreement(
    model_p_up: f64,
    market_p_up: Option<f64>,
    spread_up: f64,
    spread_down: f64,
    depth_up_at_size: Option<f64>,
    depth_down_at_size: Option<f64>,
    score_cfg: OracleDecisionScoreConfig,
) -> Option<f64> {
    OracleDecisionInput {
        model_p_up: Some(model_p_up),
        market_p_up,
        spread_up: Some(spread_up),
        spread_down: Some(spread_down),
        depth_up_at_size,
        depth_down_at_size,
        ..OracleDecisionInput::default()
    }
    .scores(score_cfg)
    .market_confirmation_probability_agreement
}

pub(in crate::strategy::support::oracle_authority) fn project_final_output_arm(
    input: FinalOutputArmProjectionInput,
    raw_beta: OracleBetaCalibrationScore,
) -> Option<ProbabilityCore> {
    if input.direct_market_selected {
        return Some(direct_market_core(
            input.market_p_up?,
            input.base_interval,
            raw_beta,
        ));
    }
    let calibration = calibrate_final_output_arm(input)?;
    let beta_score = raw_beta.rescore(calibration.p_fair);
    Some(finish_final_output_arm(
        input.base_interval,
        calibration,
        beta_score,
    ))
}

fn calibrate_final_output_arm(
    input: FinalOutputArmProjectionInput,
) -> Option<OracleProbabilityCalibration> {
    probability_calibration::calibrated_probability_detail(
        probability_calibration::OracleProbabilityCalibrationInput {
            market_p_up: input.market_p_up,
            base_p_fair: input.base_p_fair,
            market_confirmation: input.market_confirmation,
            spread_up: Some(input.spread_up),
            spread_down: Some(input.spread_down),
            depth_up_at_size: input.depth_up_at_size,
            depth_down_at_size: input.depth_down_at_size,
            min_confidence: input.min_confidence,
            confidence_market_weight: input.confidence_market_weight,
            confidence_spread_weight: input.confidence_spread_weight,
            confidence_depth_weight: input.confidence_depth_weight,
        },
    )
}

fn direct_market_core(
    market_p_up: f64,
    market_interval: Option<ProbabilityInterval>,
    raw_beta: OracleBetaCalibrationScore,
) -> ProbabilityCore {
    ProbabilityCore {
        probability_calibration: None,
        beta_calibration: raw_beta.identity(market_p_up),
        beta_calibration_attempted_p_up: raw_beta.p_fair.filter(|_| raw_beta.enabled),
        beta_observation_p_up: Some(market_p_up),
        mean: market_p_up,
        interval: market_interval,
    }
}

fn finish_final_output_arm(
    base_interval: Option<ProbabilityInterval>,
    calibration: OracleProbabilityCalibration,
    beta_score: OracleBetaCalibrationScore,
) -> ProbabilityCore {
    let calibration_mean = calibration.p_fair;
    let beta_calibration_attempted_p_up = beta_score.p_fair.filter(|_| beta_score.enabled);
    let candidate_beta_mean = beta_score
        .p_fair
        .filter(|_| beta_score.enabled)
        .unwrap_or(calibration_mean);
    let beta_application = final_probability::compose_calibration_intervals(
        base_interval,
        Some(calibration),
        calibration_mean,
        beta_score,
        candidate_beta_mean,
    );
    ProbabilityCore {
        probability_calibration: Some(calibration),
        beta_calibration: beta_application.score,
        beta_calibration_attempted_p_up,
        beta_observation_p_up: Some(calibration_mean),
        mean: beta_application.mean,
        interval: beta_application.interval,
    }
}

pub(super) fn probability_is_valid(value: Option<f64>) -> bool {
    value.is_some_and(|value| value.is_finite() && (0.0..=1.0).contains(&value))
}

pub(super) fn ordinary_direct_market_selected(
    score: OracleMicrostructureLogisticScore,
    mode: OracleAuthorityMode,
    now_ms: u64,
    window_end_ms: u64,
    p_market: Option<f64>,
) -> bool {
    mode == OracleAuthorityMode::Market
        && now_ms < window_end_ms
        && score.enabled
        && score.fit_available
        && probability_is_valid(score.p_up)
        && score.fit_prequentially_trusted == Some(false)
        && probability_is_valid(p_market)
}

impl MakerRebateMmStrategy {
    pub(super) fn compose_oracle_probability(
        &mut self,
        input: ProbabilityCompositionInput<'_>,
    ) -> Option<ProbabilityComposition> {
        let model_p_up = input.fair_result.p_model;
        let base_p_fair = input.fair_result.p_fair;
        let mut decision_input = OracleDecisionInput {
            oracle_spot: Some(input.spot_px),
            window_open: self
                .open_px_by_market
                .get(&input.market.market_id)
                .map(|state| state.px),
            model_p_up: Some(model_p_up),
            market_p_up: input.p_market,
            best_bid_up: Some(input.up_bid),
            best_ask_up: Some(input.up_ask),
            best_bid_down: Some(input.down_bid),
            best_ask_down: Some(input.down_ask),
            spread_up: Some((input.up_ask - input.up_bid).max(0.0)),
            spread_down: Some((input.down_ask - input.down_bid).max(0.0)),
            depth_up_at_size: input.up_depth_at_size,
            depth_down_at_size: input.down_depth_at_size,
            volatility: Some(input.sigma2.max(0.0).sqrt()),
            ..OracleDecisionInput::default()
        };
        if let Some(features) = input.ta_values {
            decision_input = decision_input.with_ta_features(features);
        }
        let decision_scores = decision_input.scores(input.score_cfg);

        let ordinary_policy = input.micro_logistic.application_policy.is_none()
            || input
                .micro_logistic
                .application_policy
                .is_some_and(|policy| policy.uses_final_output_trust())
                && !input.micro_logistic.applied();
        let direct_market_selected = (ordinary_policy || self.oracle_direct_market_research)
            && ordinary_direct_market_selected(
                input.micro_logistic,
                input.mode,
                input.now_ms,
                input.market.window_end_ms,
                input.p_market,
            );

        let (core, raw_beta_calibration) = if direct_market_selected {
            let market_p_up = input.p_market.expect("direct market probability is valid");
            let raw_beta =
                self.beta_calibration_score_for_market(input.market, market_p_up, input.now_ms);
            (
                direct_market_core(market_p_up, input.market_interval, raw_beta),
                raw_beta,
            )
        } else {
            let calibration = calibrate_final_output_arm(FinalOutputArmProjectionInput {
                direct_market_selected: false,
                base_p_fair,
                base_interval: input.probability_intervals,
                market_p_up: input.p_market,
                market_confirmation: decision_scores.market_confirmation_probability_agreement,
                spread_up: (input.up_ask - input.up_bid).max(0.0),
                spread_down: (input.down_ask - input.down_bid).max(0.0),
                depth_up_at_size: input.up_depth_at_size,
                depth_down_at_size: input.down_depth_at_size,
                min_confidence: self.cfg.ctf_probability_calibration_min_confidence,
                confidence_market_weight: self.cfg.ctf_probability_confidence_market_weight,
                confidence_spread_weight: self.cfg.ctf_probability_confidence_spread_weight,
                confidence_depth_weight: self.cfg.ctf_probability_confidence_depth_weight,
            })?;
            let raw_beta = self.beta_calibration_score_for_market(
                input.market,
                calibration.p_fair,
                input.now_ms,
            );
            (
                finish_final_output_arm(input.probability_intervals, calibration, raw_beta),
                raw_beta,
            )
        };

        let (edge_probability, up_edge_reference, down_edge_reference) = match input.mode {
            OracleAuthorityMode::Market => (
                EdgeProbability {
                    mean: core.mean,
                    interval: core.interval,
                },
                0.5,
                0.5,
            ),
            OracleAuthorityMode::Cleanup { .. } => {
                let (unwind_up, unwind_down) = input
                    .cleanup_unwind
                    .expect("Cleanup validates top unwind before selection");
                (
                    final_probability::compose_cleanup_edge_probability(
                        &self.cfg.live_fair_value_model,
                        core.mean,
                        core.interval,
                        false,
                        unwind_up,
                        unwind_down,
                        input.up_bid,
                        input.down_bid,
                        model_p_up,
                        input.p_market,
                        self.cfg
                            .ctf_probability_model_market_loser_bid_ambiguity_weight,
                    ),
                    unwind_up,
                    unwind_down,
                )
            }
        };

        Some(ProbabilityComposition {
            decision_scores,
            probability_calibration: core.probability_calibration,
            raw_beta_calibration,
            beta_calibration: core.beta_calibration,
            beta_calibration_attempted_p_up: core.beta_calibration_attempted_p_up,
            beta_observation_p_up: core.beta_observation_p_up,
            p_fair: core.mean,
            edge_probability,
            up_edge_reference,
            down_edge_reference,
            direct_market_selected,
        })
    }
}

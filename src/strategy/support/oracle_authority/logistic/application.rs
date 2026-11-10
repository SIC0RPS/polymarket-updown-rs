use crate::config::CurrentBlendedFairValueConfig;
use crate::polymarket::discovery::MarketSelection;
use crate::strategy::fair_value::{sigmoid, CurrentBlended, FairValueResult};
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::oracle_authority::microstructure::OracleMicrostructureFeatures;
use crate::strategy::support::oracle_authority::probability_evidence::OracleChainlinkProbabilityEvidence;
use crate::strategy::support::oracle_authority::probability_interval::{
    probability_hull, ProbabilityInterval, ONE_SIDED_95_Z,
};

use super::contribution::OracleLogisticApplicationPolicy;
use super::{
    feature_design, market_offset, model_logit, normalized_boundary_log_return,
    normalized_feature_vector, parameters_from_fit, OracleMicrostructureLogisticFit,
    OracleMicrostructureLogisticScore, PosteriorVector, MICRO_LOGISTIC_MIN_FIT_SAMPLES,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::strategy::support::oracle_authority) struct FinalOutputIntervalProjectionInput {
    p_up: f64,
    p_up_interval: Option<ProbabilityInterval>,
    correction_logit: f64,
    correction_interval: Option<[f64; 2]>,
    market_prior_p: Option<f64>,
    market_interval: Option<ProbabilityInterval>,
    shock_toxicity: f64,
    markout_toxicity: f64,
    model_trust: f64,
    blend_edge_full: f64,
    blend_min_weight: f64,
    blend_max_weight: f64,
    blend_toxicity_k: f64,
}

impl FinalOutputIntervalProjectionInput {
    pub(in crate::strategy::support::oracle_authority) fn capture(
        cfg: &CurrentBlendedFairValueConfig,
        score: OracleMicrostructureLogisticScore,
        market_interval: Option<ProbabilityInterval>,
        shock_toxicity: f64,
        markout_toxicity: f64,
    ) -> Option<Self> {
        score.feature_layout.uses_market_offset().then_some(Self {
            p_up: score.p_up?,
            p_up_interval: score.p_up_interval,
            correction_logit: score.market_offset_correction_logit?,
            correction_interval: score.market_offset_correction_interval,
            market_prior_p: score.market_prior_p,
            market_interval: market_interval.and_then(market_offset::bounded_interval),
            shock_toxicity,
            markout_toxicity,
            model_trust: score.fit_interval_trust_alpha.unwrap_or(0.0),
            blend_edge_full: cfg.model_blend_edge_full,
            blend_min_weight: cfg.model_blend_min_weight,
            blend_max_weight: cfg.model_blend_max_weight,
            blend_toxicity_k: cfg.model_blend_toxicity_k,
        })
    }

    pub(in crate::strategy::support::oracle_authority) const fn model_p_up(self) -> f64 {
        self.p_up
    }

    fn project_with_config(
        self,
        p_fair: f64,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> Option<ProbabilityInterval> {
        let model_interval = market_offset::model_interval(
            self.p_up,
            self.correction_logit,
            self.correction_interval,
            self.p_up_interval,
            self.market_interval,
        )?;
        CurrentBlended::new(cfg).blend_probability_interval(
            model_interval,
            p_fair,
            self.market_prior_p,
            self.market_interval,
            self.shock_toxicity,
            self.markout_toxicity,
            self.model_trust,
        )
    }

    pub(in crate::strategy::support::oracle_authority) fn project(
        self,
        p_fair: f64,
    ) -> Option<ProbabilityInterval> {
        let cfg = CurrentBlendedFairValueConfig {
            model_blend_edge_full: self.blend_edge_full,
            model_blend_min_weight: self.blend_min_weight,
            model_blend_max_weight: self.blend_max_weight,
            model_blend_toxicity_k: self.blend_toxicity_k,
            ..CurrentBlendedFairValueConfig::default()
        };
        self.project_with_config(p_fair, &cfg)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn score(
    cfg: &CurrentBlendedFairValueConfig,
    p_market: Option<f64>,
    feature_count: u32,
    feature_availability: f64,
    design: Result<&PosteriorVector, &'static str>,
    fit_available: bool,
    fit: Option<OracleMicrostructureLogisticFit>,
    residual_logit_std: Option<f64>,
    residual_logit_std_unavailable_reason: Option<&'static str>,
    market_calibration_logit_std: Option<f64>,
    application_policy: Option<OracleLogisticApplicationPolicy>,
) -> OracleMicrostructureLogisticScore {
    let feature_layout = fit.map(|fit| fit.feature_layout).unwrap_or_default();
    let configured_l2_penalty = cfg.micro_logistic_l2_penalty.max(0.0);
    let l2_penalty = fit
        .map(|fit| fit.l2_penalty)
        .unwrap_or(configured_l2_penalty);
    let fit_residual_trust_alpha = fit.map(|fit| fit.residual_trust_alpha * feature_availability);
    let fit_interval_trust_alpha = fit.map(|fit| {
        super::residual_trust_alpha(fit.qualification_window_count) * feature_availability
    });
    let base = OracleMicrostructureLogisticScore {
        enabled: cfg.micro_logistic_enabled,
        trade_flow_enabled: cfg.micro_logistic_trade_flow_enabled,
        forecast_feature_identity: if cfg.micro_logistic_trade_flow_enabled
            && fit.is_some_and(|fit| fit.trade_flow_fitted)
        {
            super::OracleLogisticForecastFeatureIdentity::TradeFlowV1
        } else {
            super::OracleLogisticForecastFeatureIdentity::EightInputV1
        },
        trade_flow_qualification_count: cfg
            .micro_logistic_trade_flow_enabled
            .then(|| fit.map_or(0, |fit| fit.qualification_window_count)),
        trade_flow_weight: fit
            .filter(|fit| fit.trade_flow_fitted)
            .map(|fit| fit.trade_flow_weight),
        fit_available,
        feature_count: Some(feature_count),
        l2_penalty: Some(l2_penalty),
        fit_sample_count: fit.map(|fit| fit.sample_count),
        fit_positive_count: fit.map(|fit| fit.positive_count),
        fit_residual_trust_alpha,
        fit_interval_trust_alpha,
        fit_brier_score: fit.and_then(|fit| fit.brier_score),
        fit_log_loss: fit.and_then(|fit| fit.log_loss),
        fit_prior_brier_score: fit.and_then(|fit| fit.prior_brier_score),
        fit_prior_log_loss: fit.and_then(|fit| fit.prior_log_loss),
        fit_candidate_brier_score: fit.and_then(|fit| fit.candidate_brier_score),
        fit_candidate_log_loss: fit.and_then(|fit| fit.candidate_log_loss),
        fit_prequentially_trusted: fit.map(|fit| fit.prequentially_trusted),
        fit_weight_norm: fit.and_then(|fit| {
            (fit.independent_window_count as usize >= MICRO_LOGISTIC_MIN_FIT_SAMPLES)
                .then_some(fit)
                .and_then(|fit| fit.weight_norm)
        }),
        fit_weights: fit.and_then(|fit| {
            (fit.independent_window_count as usize >= MICRO_LOGISTIC_MIN_FIT_SAMPLES)
                .then_some(fit.weights)
        }),
        residual_logit_std,
        residual_logit_std_unavailable_reason,
        market_calibration_logit_std,
        feature_layout,
        application_policy,
        ..Default::default()
    };
    if !cfg.micro_logistic_enabled {
        return base.unavailable("micro_logistic_disabled");
    }

    let Some(fit) =
        fit.filter(|fit| fit.independent_window_count as usize >= MICRO_LOGISTIC_MIN_FIT_SAMPLES)
    else {
        return base.unavailable("insufficient_fit");
    };
    if feature_layout.uses_market_offset() && !fit_available {
        return base.unavailable("fit_unavailable");
    }
    let market_prior = feature_layout
        .uses_market_offset()
        .then(|| market_offset::bounded_prior(p_market))
        .flatten();
    if if feature_layout.uses_market_offset() {
        market_prior.is_none()
    } else {
        p_market.and_then(super::logit_from_probability).is_none()
    } {
        return base.unavailable("invalid_market_probability");
    }
    let design = match design {
        Ok(design) => design,
        Err(reason) => return base.unavailable(reason),
    };
    let correction_logit = model_logit(&parameters_from_fit(fit), design);
    let (logit, market_prior_p) = if let Some(prior) = market_prior {
        (prior.logit + correction_logit, Some(prior.probability))
    } else {
        (correction_logit, None)
    };
    if !logit.is_finite() {
        return base.unavailable("invalid_model_logit");
    }
    let p_up = sigmoid(logit).clamp(0.0, 1.0);
    let p_up_interval = residual_logit_std
        .and_then(|std| ProbabilityInterval::from_logit(logit, std, ONE_SIDED_95_Z))
        .filter(|interval| interval.contains(p_up));
    let market_offset_correction_interval = feature_layout
        .uses_market_offset()
        .then(|| {
            residual_logit_std.and_then(|std| {
                market_offset::correction_interval(correction_logit, std, ONE_SIDED_95_Z)
            })
        })
        .flatten();
    OracleMicrostructureLogisticScore {
        p_up: Some(p_up),
        logit: Some(logit),
        p_up_interval,
        market_prior_p,
        market_offset_correction_logit: feature_layout
            .uses_market_offset()
            .then_some(correction_logit),
        market_offset_correction_interval,
        ..base
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply(
    fair_result: FairValueResult,
    score: OracleMicrostructureLogisticScore,
    cfg: &CurrentBlendedFairValueConfig,
    p_market: Option<f64>,
    market_interval: Option<ProbabilityInterval>,
    base_interval: Option<ProbabilityInterval>,
    shock_toxicity: f64,
    markout_toxicity: f64,
) -> (FairValueResult, Option<ProbabilityInterval>) {
    if !score.applied() {
        return (fair_result, base_interval);
    }
    let p_up = score.p_up.expect("applied logistic score has a prediction");
    if score.application_policy.is_some() {
        let p_fair = score
            .phase_p1
            .expect("trusted phase application has a prospective probability");
        let result = FairValueResult {
            p_model_raw: p_up,
            p_model: p_up,
            p_fair,
            model_weight: score.phase_model_weight,
            depth_pressure: fair_result.depth_pressure,
            spot_z: fair_result.spot_z,
        };
        let interval = score
            .final_output_interval_projection
            .or_else(|| {
                FinalOutputIntervalProjectionInput::capture(
                    cfg,
                    score,
                    market_interval,
                    shock_toxicity,
                    markout_toxicity,
                )
            })
            .and_then(|input| input.project_with_config(result.p_fair, cfg));
        return (result, interval);
    }

    let fair_model = CurrentBlended::new(cfg);
    let bounded_market_interval = score
        .feature_layout
        .uses_market_offset()
        .then(|| market_interval.and_then(market_offset::bounded_interval))
        .flatten();
    let model_interval = if score.feature_layout.uses_market_offset() {
        score.market_offset_correction_logit.and_then(|correction| {
            market_offset::model_interval(
                p_up,
                correction,
                score.market_offset_correction_interval,
                score.p_up_interval,
                bounded_market_interval,
            )
        })
    } else {
        (score.p_up_interval.is_some() || market_interval.is_some())
            .then(|| {
                score
                    .p_up_interval
                    .or_else(|| ProbabilityInterval::new(p_up, p_up))
            })
            .flatten()
    };
    let model_trust = score.fit_residual_trust_alpha.unwrap_or(0.0);
    let interval_trust = score.fit_interval_trust_alpha.unwrap_or(0.0);
    let (blend_market, blend_market_interval) = if score.feature_layout.uses_market_offset() {
        (score.market_prior_p, bounded_market_interval)
    } else {
        (p_market, market_interval)
    };
    let blend = fair_model.blend_probability(
        p_up,
        blend_market,
        shock_toxicity,
        markout_toxicity,
        model_trust,
    );
    let result = FairValueResult {
        p_model_raw: p_up,
        p_model: p_up,
        p_fair: blend.p_fair,
        model_weight: blend.model_weight,
        depth_pressure: fair_result.depth_pressure,
        spot_z: fair_result.spot_z,
    };
    let interval = model_interval.and_then(|model_interval| {
        let point_trust_interval = fair_model.blend_probability_interval(
            model_interval,
            result.p_fair,
            blend_market,
            blend_market_interval,
            shock_toxicity,
            markout_toxicity,
            model_trust,
        )?;
        let interval_trust_interval = fair_model.blend_probability_interval(
            model_interval,
            result.p_fair,
            blend_market,
            blend_market_interval,
            shock_toxicity,
            markout_toxicity,
            interval_trust,
        )?;
        probability_hull(
            result.p_fair,
            [
                point_trust_interval.lo(),
                point_trust_interval.hi(),
                interval_trust_interval.lo(),
                interval_trust_interval.hi(),
            ],
        )
    });
    (result, interval)
}

impl MakerRebateMmStrategy {
    #[allow(clippy::too_many_arguments)]
    fn micro_logistic_score_for_market(
        &mut self,
        market: &MarketSelection,
        spot_px: f64,
        spot_z: Option<f64>,
        microstructure: &OracleMicrostructureFeatures,
        incoming_p: f64,
        p_market: Option<f64>,
        market_calibration_logit_std: Option<f64>,
        shock_toxicity: f64,
        markout_toxicity: f64,
        observation_ts_ms: u64,
        now_ms: u64,
    ) -> OracleMicrostructureLogisticScore {
        let normalized_forecast_horizon = (market.window_end_ms > market.window_start_ms
            && now_ms < market.window_end_ms)
            .then(|| {
                (market.window_end_ms - now_ms) as f64
                    / (market.window_end_ms - market.window_start_ms) as f64
            });
        let boundary_return = if observation_ts_ms <= market.window_start_ms {
            Some(0.0)
        } else if observation_ts_ms < market.window_end_ms {
            self.open_px_by_market
                .get(&market.market_id)
                .map(|state| state.px)
                .and_then(|open_px| normalized_boundary_log_return(spot_px, open_px))
        } else {
            None
        };
        let probability_trace_enabled =
            tracing::enabled!(target: "strategy.oracle_probability", tracing::Level::TRACE);
        let capture_chainlink_reference =
            self.micro_logistic_trainer.requires_chainlink_reference() || probability_trace_enabled;
        let chainlink_reference = capture_chainlink_reference
            .then(|| {
                self.resolution_label_reducer
                    .reference_distance(&market.market_id)
            })
            .flatten();
        let chainlink_reference_return =
            chainlink_reference.and_then(|reference| reference.log_return());
        let trade_flow_enabled = self
            .cfg
            .live_fair_value_model
            .micro_logistic_trade_flow_enabled;
        self.micro_logistic_trainer
            .set_trade_flow_enabled(trade_flow_enabled);
        let layout = self.micro_logistic_trainer.feature_layout();
        let (feature_vector, mut feature_count) = normalized_feature_vector(microstructure);
        let design = feature_design(
            layout,
            normalized_forecast_horizon,
            boundary_return,
            spot_z,
            chainlink_reference_return,
            &feature_vector,
        );
        let design = design.and_then(|mut design| {
            if trade_flow_enabled {
                design[super::MICRO_LOGISTIC_TRADE_FLOW_INDEX] = microstructure
                    .trade_flow_imbalance_5s
                    .ok_or("missing_trade_flow_5s")?;
                feature_count += 1;
            }
            Ok(design)
        });
        let design = design.as_ref().map_err(|reason| *reason);
        let fit_available = self.micro_logistic_trainer.fit_available();
        let observation = self.micro_logistic_trainer.observe_market(
            market,
            observation_ts_ms,
            now_ms,
            &feature_vector,
            microstructure.trade_flow_imbalance_5s,
            design,
            p_market,
            normalized_forecast_horizon,
            boundary_return,
            spot_z,
            chainlink_reference,
            chainlink_reference_return,
        );
        let residual_logit_std = self
            .micro_logistic_trainer
            .applied_logit_std(p_market, design);
        let mut logistic_score = score(
            &self.cfg.live_fair_value_model,
            p_market,
            feature_count,
            microstructure.availability.clamp(0.0, 1.0),
            design,
            fit_available,
            observation.fit,
            residual_logit_std.value,
            residual_logit_std.unavailable_reason,
            market_calibration_logit_std,
            self.micro_logistic_trainer.application_policy(),
        );
        logistic_score.trade_flow_imbalance_5s = microstructure.trade_flow_imbalance_5s;
        let (trust_selection, phase_application, retained_handle) =
            super::contribution::attach_retained_contribution(
                &mut self.micro_logistic_trainer,
                observation,
                incoming_p,
                logistic_score,
                shock_toxicity,
                markout_toxicity,
                &self.cfg.live_fair_value_model,
            );
        let logistic_score = logistic_score.with_phase_application(
            trust_selection,
            phase_application,
            retained_handle,
            observation_ts_ms,
        );
        if probability_trace_enabled {
            logistic_score.with_probability_evidence(OracleChainlinkProbabilityEvidence::capture(
                chainlink_reference,
                chainlink_reference_return,
            ))
        } else {
            logistic_score
        }
    }

    pub(in crate::strategy::support::oracle_authority) fn micro_logistic_hop_one_for_market(
        &mut self,
        market: &MarketSelection,
        spot_px: f64,
        microstructure: &OracleMicrostructureFeatures,
        fair_result: FairValueResult,
        p_market: Option<f64>,
        market_interval: Option<ProbabilityInterval>,
        base_interval: Option<ProbabilityInterval>,
        market_calibration_logit_std: Option<f64>,
        last_return_bps: f64,
        observation_ts_ms: u64,
        now_ms: u64,
    ) -> (
        OracleMicrostructureLogisticScore,
        FairValueResult,
        Option<ProbabilityInterval>,
        Option<(FairValueResult, Option<ProbabilityInterval>)>,
    ) {
        let shock_toxicity = Self::shock_toxicity(last_return_bps, self.cfg.oracle_shock_bps);
        let markout_toxicity = self
            .markout_toxicity_at(market.up_token_id(), now_ms)
            .max(self.markout_toxicity_at(market.down_token_id(), now_ms));
        let score = self.micro_logistic_score_for_market(
            market,
            spot_px,
            fair_result.spot_z,
            microstructure,
            fair_result.p_fair,
            p_market,
            market_calibration_logit_std,
            shock_toxicity,
            markout_toxicity,
            observation_ts_ms,
            now_ms,
        );
        let score_for_application = score.with_fit_score_enabled(now_ms < market.window_end_ms);
        let final_output_policy = score
            .application_policy
            .is_some_and(OracleLogisticApplicationPolicy::uses_final_output_trust);
        let ordinary_hop_one = if final_output_policy {
            let mut ordinary_score = score_for_application;
            ordinary_score.application_policy = None;
            Some(apply(
                fair_result,
                ordinary_score,
                &self.cfg.live_fair_value_model,
                p_market,
                market_interval,
                base_interval,
                shock_toxicity,
                markout_toxicity,
            ))
        } else {
            None
        };
        let retain_deferred_interval = final_output_policy
            && score.final_output_retained_handle.is_some()
            && score.phase_p1.is_some()
            && !score_for_application.applied();
        let final_output_interval_projection = retain_deferred_interval
            .then(|| {
                FinalOutputIntervalProjectionInput::capture(
                    &self.cfg.live_fair_value_model,
                    score,
                    market_interval,
                    shock_toxicity,
                    markout_toxicity,
                )
            })
            .flatten();
        let score = score.with_final_output_interval_projection(final_output_interval_projection);
        let score_for_application = score_for_application
            .with_final_output_interval_projection(final_output_interval_projection);
        let (fair_result, probability_interval) = ordinary_hop_one
            .filter(|_| !score_for_application.applied())
            .unwrap_or_else(|| {
                apply(
                    fair_result,
                    score_for_application,
                    &self.cfg.live_fair_value_model,
                    p_market,
                    market_interval,
                    base_interval,
                    shock_toxicity,
                    markout_toxicity,
                )
            });
        let score_for_trace = if score.application_policy.is_some() {
            score_for_application
        } else {
            score
        };
        (
            score_for_trace,
            fair_result,
            probability_interval,
            ordinary_hop_one,
        )
    }
}

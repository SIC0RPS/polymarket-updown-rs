use crate::polymarket::state::BookDepth;

pub(in crate::strategy::support::oracle_authority) mod probability_composition;

use self::probability_composition::ProbabilityCompositionInput;
pub(in crate::strategy::support::oracle_authority) use self::probability_composition::{
    final_output_model_agreement, project_final_output_arm, FinalOutputArmProjectionInput,
};
use super::logistic::{
    attach_final_output_context, FinalOutputMissingInterval, FinalOutputObservationContext,
};
use super::probability_evidence::OracleProbabilityEvidence;
use super::selection::OracleEmissionEvidenceBasis;
use super::*;

impl MakerRebateMmStrategy {
    pub(crate) fn build_oracle_authority_snapshot(
        &mut self,
        market: &MarketSelection,
        market_id: &MarketId,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        previous: Option<OracleAuthoritySnapshot>,
        previous_side: Option<PairTokenSide>,
        mode: OracleAuthorityMode,
        now_ms: u64,
    ) -> OracleAuthoritySnapshot {
        let previous = previous.unwrap_or_default();
        let probability_trace_enabled =
            tracing::enabled!(target: "strategy.oracle_probability", tracing::Level::TRACE);
        macro_rules! trace_unavailable {
            ($reason:literal) => {
                if probability_trace_enabled {
                    probability_evidence::emit_unavailable(
                        market_id,
                        now_ms,
                        mode,
                        self.cfg.live_fair_value_model.micro_logistic_enabled,
                        self.cfg
                            .live_fair_value_model
                            .micro_logistic_trade_flow_enabled,
                        self.consumed_trade_flow_snapshot()
                            .and_then(|flow| flow.imbalance()),
                        self.micro_logistic_trainer.feature_layout(),
                        $reason,
                    );
                }
            };
        }
        let Some((oracle_symbol, ts_ms, spot_px, sigma2, last_return_bps)) =
            self.market_oracle_input(market, now_ms)
        else {
            trace_unavailable!("missing_market_oracle_input");
            return OracleAuthoritySnapshot {
                controller_mode: mode,
                cleanup_anchor_side: cleanup_anchor_side(mode),
                oracle_side_reason: Some(OracleSideReason::NoMarketData),
                oracle_data_issue: Some("missing_market_oracle_input"),
                ..Default::default()
            };
        };
        let (Some(up_bid), Some(up_ask), Some(down_bid), Some(down_ask)) =
            (up.bid, up.ask, down.bid, down.ask)
        else {
            trace_unavailable!("missing_live_fair_result");
            return OracleAuthoritySnapshot {
                controller_mode: mode,
                cleanup_anchor_side: cleanup_anchor_side(mode),
                oracle_side_reason: Some(OracleSideReason::NoMarketData),
                oracle_data_issue: Some("missing_live_fair_result"),
                ..Default::default()
            };
        };
        let Some((market_calibration, fair_result, p_market)) =
            self.market_calibrated_live_fair_result(market, up, down, up_depth, down_depth, now_ms)
        else {
            trace_unavailable!("missing_live_fair_result");
            return OracleAuthoritySnapshot {
                controller_mode: mode,
                cleanup_anchor_side: cleanup_anchor_side(mode),
                oracle_side_reason: Some(OracleSideReason::NoMarketData),
                oracle_data_issue: Some("missing_live_fair_result"),
                ..Default::default()
            };
        };
        let microstructure = self.oracle_microstructure_features(sigma2, now_ms);
        let base_fair_result = fair_result;
        let (micro_logistic, fair_result, probability_intervals, ordinary_hop_one) = self
            .micro_logistic_hop_one_for_market(
                market,
                spot_px,
                &microstructure,
                fair_result,
                p_market,
                market_calibration.calibrated_interval,
                market_calibration.base_blend_interval,
                market_calibration.calibrated_logit_std,
                last_return_bps,
                ts_ms,
                now_ms,
            );
        macro_rules! trace_model_unavailable {
            ($reason:literal) => {
                if probability_trace_enabled {
                    OracleProbabilityEvidence::after_model(
                        mode,
                        self.micro_logistic_trainer.feature_layout(),
                        market_calibration,
                        base_fair_result,
                        micro_logistic,
                        probability_composition::probability_is_valid(micro_logistic.p_up),
                        fair_result,
                        Some($reason),
                    )
                    .emit(market_id, now_ms);
                }
            };
        }
        let raw_p_up = fair_result.p_model_raw;
        let model_p_up = fair_result.p_model;
        let base_p_fair = fair_result.p_fair;
        let cleanup_unwind = if matches!(mode, OracleAuthorityMode::Cleanup { .. }) {
            let Some(unwind_up) = self.executable_unwind_value(up) else {
                trace_model_unavailable!("missing_top_unwind_up");
                return OracleAuthoritySnapshot {
                    controller_mode: mode,
                    cleanup_anchor_side: cleanup_anchor_side(mode),
                    p_up: Some(raw_p_up),
                    oracle_side_reason: Some(OracleSideReason::NoMarketData),
                    oracle_data_issue: Some("missing_top_unwind_up"),
                    ..Default::default()
                };
            };
            let Some(unwind_down) = self.executable_unwind_value(down) else {
                trace_model_unavailable!("missing_top_unwind_down");
                return OracleAuthoritySnapshot {
                    controller_mode: mode,
                    cleanup_anchor_side: cleanup_anchor_side(mode),
                    p_up: Some(raw_p_up),
                    unwind_up: Some(unwind_up),
                    oracle_side_reason: Some(OracleSideReason::NoMarketData),
                    oracle_data_issue: Some("missing_top_unwind_down"),
                    ..Default::default()
                };
            };
            Some((unwind_up, unwind_down))
        } else {
            None
        };
        let alpha = ewma_alpha(now_ms, previous.last_eval_ts_ms, self.cfg.ewma_half_life_ms);
        let mut evidence_previous = previous;
        if previous.controller_mode != mode {
            evidence_previous.adv_up_ewma = None;
            evidence_previous.adv_down_ewma = None;
            evidence_previous.adv_up_lcb_ewma = None;
            evidence_previous.adv_up_ucb_ewma = None;
            evidence_previous.adv_down_lcb_ewma = None;
            evidence_previous.adv_down_ucb_ewma = None;
            evidence_previous.adv_noise = None;
            evidence_previous.remaining_owned_adv = None;
            evidence_previous.remaining_owned_adv_ewma = None;
            evidence_previous.remaining_owned_noise_ewma = None;
        }
        let top_up_bid_delta = previous
            .top_up_bid
            .filter(|value| value.is_finite())
            .and_then(|prev| up_bid.is_finite().then_some(up_bid - prev));
        let top_down_bid_delta = previous
            .top_down_bid
            .filter(|value| value.is_finite())
            .and_then(|prev| down_bid.is_finite().then_some(down_bid - prev));
        let ta_features = self.oracle_candle_ta_features(market, &oracle_symbol, now_ms);
        let ta_bar_ts_ms = ta_features.as_ref().map(|(_, ts_ms, _)| *ts_ms);
        let ta_closed_bars_in_market = ta_features.as_ref().map(|(_, _, count)| *count);
        let ta_values = ta_features.map(|(features, _, _)| features);
        let score_cfg = OracleDecisionScoreConfig::from_strategy_config(&self.cfg);
        let Some(composition) = self.compose_oracle_probability(ProbabilityCompositionInput {
            market,
            mode,
            now_ms,
            spot_px,
            sigma2,
            p_market,
            market_interval: market_calibration.calibrated_interval,
            fair_result,
            micro_logistic,
            probability_intervals,
            up_bid,
            up_ask,
            down_bid,
            down_ask,
            up_depth_at_size: up.bid_sz,
            down_depth_at_size: down.bid_sz,
            cleanup_unwind,
            ta_values,
            score_cfg,
        }) else {
            trace_model_unavailable!("invalid_probability_calibration_inputs");
            return OracleAuthoritySnapshot {
                controller_mode: mode,
                cleanup_anchor_side: cleanup_anchor_side(mode),
                p_up: Some(raw_p_up),
                model_p_up: Some(model_p_up),
                market_p_up: p_market,
                base_p_fair: Some(base_p_fair),
                unwind_up: cleanup_unwind.map(|(unwind_up, _)| unwind_up),
                unwind_down: cleanup_unwind.map(|(_, unwind_down)| unwind_down),
                oracle_side_reason: Some(OracleSideReason::NoMarketData),
                oracle_data_issue: Some("invalid_probability_calibration_inputs"),
                ..Default::default()
            };
        };
        if let (Some(handle), Some(phase)) = (
            micro_logistic.final_output_retained_handle,
            micro_logistic.phase,
        ) {
            let selected_arm = micro_logistic.applied();
            let missing = if selected_arm {
                ordinary_hop_one.map(|(fair, interval)| {
                    let interval = if probability_composition::ordinary_direct_market_selected(
                        micro_logistic,
                        mode,
                        now_ms,
                        market.window_end_ms,
                        p_market,
                    ) {
                        FinalOutputMissingInterval::DirectMarket(
                            market_calibration.calibrated_interval,
                        )
                    } else {
                        FinalOutputMissingInterval::Incoming(interval)
                    };
                    (fair.p_fair, fair.p_model, interval)
                })
            } else {
                micro_logistic
                    .phase_p1
                    .zip(micro_logistic.final_output_interval_projection)
                    .map(|(p1, projection)| {
                        (
                            p1,
                            projection.model_p_up(),
                            FinalOutputMissingInterval::Learner(projection),
                        )
                    })
            };
            if let Some((missing_p, missing_model_p, missing_interval)) = missing {
                attach_final_output_context(
                    &mut self.micro_logistic_trainer,
                    handle,
                    FinalOutputObservationContext {
                        phase,
                        anchor: false,
                        selected_arm,
                        published_p: composition.p_fair,
                        missing_p,
                        missing_model_p,
                        missing_interval,
                        market_p: p_market,
                        spread_up: (up_ask - up_bid).max(0.0),
                        spread_down: (down_ask - down_bid).max(0.0),
                        depth_up: up.bid_sz,
                        depth_down: down.bid_sz,
                        min_confidence: self.cfg.ctf_probability_calibration_min_confidence,
                        confidence_market_weight: self.cfg.ctf_probability_confidence_market_weight,
                        confidence_spread_weight: self.cfg.ctf_probability_confidence_spread_weight,
                        confidence_depth_weight: self.cfg.ctf_probability_confidence_depth_weight,
                        score_cfg,
                        raw_beta: composition.raw_beta_calibration,
                    },
                );
            }
        }
        let p_fair = composition.p_fair;
        let edge_probability = composition.edge_probability;
        let up_edge_reference = composition.up_edge_reference;
        let down_edge_reference = composition.down_edge_reference;
        let decision_scores = composition.decision_scores;
        // Market prediction is centered at 0.5; Cleanup retains top unwind references.
        let adv_up = edge_probability.mean - up_edge_reference;
        let adv_down = (1.0 - edge_probability.mean) - down_edge_reference;
        let adv_up_ewma = ewma_step(evidence_previous.adv_up_ewma, adv_up, alpha);
        let adv_down_ewma = ewma_step(evidence_previous.adv_down_ewma, adv_down, alpha);
        let bound_ewmas = final_probability::edge_bound_ewmas(
            edge_probability.interval,
            up_edge_reference,
            down_edge_reference,
            evidence_previous.adv_up_lcb_ewma,
            evidence_previous.adv_up_ucb_ewma,
            evidence_previous.adv_down_lcb_ewma,
            evidence_previous.adv_down_ucb_ewma,
            alpha,
        );
        let noise_up =
            advantage_noise_step(evidence_previous.adv_noise, adv_up_ewma, adv_up, alpha);
        let noise_down =
            advantage_noise_step(evidence_previous.adv_noise, adv_down_ewma, adv_down, alpha);
        let adv_noise = noise_up.max(noise_down);
        let posterior_uncertainty = edge_probability.interval.map(|interval| {
            (edge_probability.mean - interval.lo()).max(interval.hi() - edge_probability.mean)
        });
        let uncertainty_edge_threshold = posterior_uncertainty;
        let weighted_posterior_uncertainty = edge_probability
            .interval
            .map(|uncertainty| {
                (uncertainty.hi() - uncertainty.lo()).max(0.0)
                    * self
                        .cfg
                        .live_fair_value_model
                        .uncertainty_edge_weight
                        .max(0.0)
            })
            .unwrap_or(0.0);
        let fast_favored_side = if adv_up_ewma > adv_down_ewma {
            Some(PairTokenSide::Up)
        } else if adv_down_ewma > adv_up_ewma {
            Some(PairTokenSide::Down)
        } else {
            None
        };
        let feature_alignment = fast_favored_side
            .and_then(|side| {
                decision_scores.ta_alignment.map(|alignment| match side {
                    PairTokenSide::Up => alignment,
                    PairTokenSide::Down => -alignment,
                })
            })
            .unwrap_or(0.0);
        let alignment = feature_alignment.clamp(-1.0, 1.0);
        let emission_evidence = OracleEmissionEvidence::new(
            adv_up_ewma,
            adv_down_ewma,
            bound_ewmas.up_lcb,
            bound_ewmas.up_ucb,
            bound_ewmas.down_lcb,
            bound_ewmas.down_ucb,
        );
        let threshold_uncertainty = if mode == OracleAuthorityMode::Market
            && emission_evidence.basis() == OracleEmissionEvidenceBasis::Bounded
        {
            0.0
        } else {
            weighted_posterior_uncertainty
        };
        let thresholds = oracle_emission_thresholds(OracleEmissionThresholdInput {
            base: self.cfg.oracle_side_min_advantage,
            adv_noise,
            uncertainty: threshold_uncertainty,
            threshold_multiplier: 1.0,
            noise_weight: self.cfg.oracle_side_noise_weight,
            flip_hysteresis: self.cfg.oracle_side_flip_hysteresis,
            flip_noise_weight: self.cfg.oracle_side_flip_noise_weight,
            cleanup_flip_bump: self.cfg.oracle_side_cleanup_flip_bump,
            cleanup_mode: matches!(mode, OracleAuthorityMode::Cleanup { .. }),
        });
        let theta_enter_up = thresholds.enter_up;
        let theta_enter_down = thresholds.enter_down;
        let theta_flip = thresholds.flip;
        let theta_release_up = thresholds.release_up;
        let theta_release_down = thresholds.release_down;
        let theta_escape = thresholds.escape;
        let selection = select_oracle_side(OracleSelectionInput {
            mode,
            previous: &evidence_previous,
            previous_side,
            evidence: emission_evidence,
            adv_up,
            adv_down,
            adv_up_ewma,
            adv_down_ewma,
            theta_enter_up,
            theta_enter_down,
            theta_release_up,
            theta_release_down,
            theta_flip,
            theta_escape,
            alpha,
        });
        let oracle_signal_side = selection.signal;
        let oracle_side_reason = selection.reason;
        let remaining_owned_adv = selection.remaining_owned_adv;
        let remaining_owned_adv_ewma = selection.remaining_owned_adv_ewma;
        let remaining_owned_noise_ewma = selection.remaining_owned_noise_ewma;
        let side_confidence = |side: PairTokenSide| match mode {
            OracleAuthorityMode::Market => {
                emission_evidence.market_confidence_edge(side).map(|edge| {
                    oracle_side_confidence(
                        edge,
                        match side {
                            PairTokenSide::Up => theta_enter_up,
                            PairTokenSide::Down => theta_enter_down,
                        },
                        adv_noise,
                    )
                })
            }
            OracleAuthorityMode::Cleanup { .. } => {
                let filtered = emission_evidence.mean_confidence_edge(side);
                let threshold = match side {
                    PairTokenSide::Up => theta_enter_up,
                    PairTokenSide::Down => theta_enter_down,
                };
                Some(oracle_side_confidence(
                    filtered,
                    threshold,
                    remaining_owned_noise_ewma.unwrap_or(adv_noise),
                ))
            }
        };
        let oracle_data_issue = selection.data_issue;
        let oracle_side_confidence = oracle_signal_side.and_then(side_confidence);
        if mode == OracleAuthorityMode::Market {
            tracing::trace!(
                market_id = ?market_id,
                now_ms,
                previous_side = ?previous_side,
                evidence_basis = ?emission_evidence.basis(),
                adv_up_lcb_ewma = ?bound_ewmas.up_lcb,
                adv_up_ucb_ewma = ?bound_ewmas.up_ucb,
                adv_down_lcb_ewma = ?bound_ewmas.down_lcb,
                adv_down_ucb_ewma = ?bound_ewmas.down_ucb,
                theta_enter_up,
                theta_enter_down,
                theta_release_up,
                theta_release_down,
                theta_flip,
                theta_escape,
                signal = ?oracle_signal_side,
                reason = ?oracle_side_reason,
                data_issue = ?oracle_data_issue,
                "oracle Market selection"
            );
        }
        let has_live_prediction = oracle_signal_side.is_some();
        let ta_confirms_or_neutral = match (oracle_signal_side, decision_scores.ta_alignment) {
            (Some(PairTokenSide::Up), Some(alignment)) => alignment >= 0.0,
            (Some(PairTokenSide::Down), Some(alignment)) => alignment <= 0.0,
            (Some(_), None) => false,
            (None, _) => false,
        };
        let predictive_first_managed_sell_allowed = has_live_prediction
            && decision_scores
                .market_confirmation
                .is_some_and(|confirmation| {
                    confirmation >= self.cfg.ctf_first_managed_sell_min_market_confirmation
                })
            && ta_confirms_or_neutral;
        let mut snapshot = OracleAuthoritySnapshot {
            controller_mode: mode,
            cleanup_anchor_side: cleanup_anchor_side(mode),
            last_eval_ts_ms: Some(now_ms),
            p_up: Some(raw_p_up),
            p_up_lower_bound: edge_probability.interval.map(|interval| interval.lo()),
            p_up_upper_bound: edge_probability.interval.map(|interval| interval.hi()),
            model_p_up: Some(model_p_up),
            market_p_up: p_market,
            base_p_fair: Some(base_p_fair),
            model_weight: fair_result.model_weight,
            unwind_up: cleanup_unwind.map(|(unwind_up, _)| unwind_up),
            unwind_down: cleanup_unwind.map(|(_, unwind_down)| unwind_down),
            adv_up: Some(adv_up),
            adv_down: Some(adv_down),
            adv_up_ewma: Some(adv_up_ewma),
            adv_down_ewma: Some(adv_down_ewma),
            adv_up_lcb_ewma: bound_ewmas.up_lcb,
            adv_up_ucb_ewma: bound_ewmas.up_ucb,
            adv_down_lcb_ewma: bound_ewmas.down_lcb,
            adv_down_ucb_ewma: bound_ewmas.down_ucb,
            adv_noise: Some(adv_noise),
            uncertainty_edge_threshold,
            uncertainty_edge_weight: Some(self.cfg.live_fair_value_model.uncertainty_edge_weight),
            remaining_owned_adv,
            remaining_owned_adv_ewma,
            remaining_owned_noise_ewma,
            theta_enter_up: Some(theta_enter_up),
            theta_enter_down: Some(theta_enter_down),
            theta_release_cleanup: match mode {
                OracleAuthorityMode::Market => None,
                OracleAuthorityMode::Cleanup {
                    remaining_owned_side,
                } => Some(match remaining_owned_side {
                    PairTokenSide::Up => theta_release_up,
                    PairTokenSide::Down => theta_release_down,
                }),
            },
            theta_flip: Some(theta_flip),
            theta_escape: Some(theta_escape),
            ta_signal_alignment: Some(alignment),
            oracle_signal_side,
            oracle_side_confidence,
            oracle_side_reason,
            oracle_data_issue,
            p_fair: Some(p_fair),
            calibration_confidence: composition
                .probability_calibration
                .map(|calibration| calibration.confidence),
            calibration_center: composition
                .probability_calibration
                .map(|calibration| calibration.center),
            calibration_market_center_weight: composition
                .probability_calibration
                .map(|calibration| calibration.market_center_weight),
            calibration_spread_quality: composition
                .probability_calibration
                .map(|calibration| calibration.spread_quality),
            calibration_depth_quality: composition
                .probability_calibration
                .map(|calibration| calibration.depth_quality),
            depth_pressure: Some(fair_result.depth_pressure),
            top_up_bid: up_bid.is_finite().then_some(up_bid),
            top_down_bid: down_bid.is_finite().then_some(down_bid),
            top_up_bid_delta,
            top_down_bid_delta,
            market_confirmation: decision_scores.market_confirmation,
            market_confirmation_probability_agreement: decision_scores
                .market_confirmation_probability_agreement,
            market_confirmation_spread_quality: decision_scores.market_confirmation_spread_quality,
            market_confirmation_depth_support: decision_scores.market_confirmation_depth_support,
            market_confirmation_book_complete: decision_scores.market_confirmation_book_complete,
            ta_alignment: decision_scores.ta_alignment,
            ta_ema_trend: ta_values.map(|features| features.ta_ema_trend),
            ta_macd_trend: ta_values.map(|features| features.ta_macd_trend),
            ta_rsi_regime: ta_values.map(|features| features.ta_rsi_regime),
            ta_bollinger_position: ta_values.map(|features| features.ta_bollinger_position),
            ta_recent_return_sign: ta_values.map(|features| features.ta_recent_return_sign),
            ta_drift_score: ta_values.map(|features| features.ta_drift_score),
            ta_bar_ts_ms,
            ta_closed_bars_in_market,
            pnl_risk_multiplier: decision_scores.pnl_risk_multiplier,
            predictive_first_managed_sell_allowed,
            risk_pause_recommended: decision_scores.risk_pause_recommended,
            ..Default::default()
        };
        snapshot.apply_market_calibration(market_calibration);
        snapshot.apply_beta_calibration(composition.beta_calibration);
        snapshot.apply_microstructure(microstructure);
        snapshot.apply_micro_logistic(micro_logistic);
        if probability_trace_enabled {
            OracleProbabilityEvidence::after_model(
                mode,
                self.micro_logistic_trainer.feature_layout(),
                market_calibration,
                base_fair_result,
                micro_logistic,
                probability_composition::probability_is_valid(micro_logistic.p_up),
                fair_result,
                None,
            )
            .with_composition(
                snapshot.last_eval_ts_ms,
                composition
                    .probability_calibration
                    .map(|calibration| calibration.p_fair),
                composition
                    .probability_calibration
                    .map(|calibration| calibration.p_fair),
                composition.beta_calibration_attempted_p_up,
                composition.beta_observation_p_up,
                composition.beta_calibration.enabled,
                p_fair,
                p_fair,
                edge_probability.interval,
                composition.direct_market_selected,
                composition.probability_calibration.is_some(),
            )
            .with_oracle_signal_side(oracle_signal_side)
            .emit(market_id, now_ms);
        }
        self.oracle_authority_by_market
            .insert(market_id.clone(), snapshot);
        snapshot
    }
}

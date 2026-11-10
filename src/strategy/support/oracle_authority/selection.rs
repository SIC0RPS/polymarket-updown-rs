use crate::strategy::hold_to_resolution::PairTokenSide;

use super::{OracleAuthorityMode, OracleAuthoritySnapshot, OracleSideReason};

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleEmissionEvidence {
    mean_up: f64,
    mean_down: f64,
    up_lcb: Option<f64>,
    up_ucb: Option<f64>,
    down_lcb: Option<f64>,
    down_ucb: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OracleEmissionEvidenceBasis {
    MeanOnly,
    Bounded,
    Invalid,
}

impl OracleEmissionEvidence {
    pub(super) fn new(
        mean_up: f64,
        mean_down: f64,
        up_lcb: Option<f64>,
        up_ucb: Option<f64>,
        down_lcb: Option<f64>,
        down_ucb: Option<f64>,
    ) -> Self {
        Self {
            mean_up,
            mean_down,
            up_lcb,
            up_ucb,
            down_lcb,
            down_ucb,
        }
    }

    pub(super) fn basis(self) -> OracleEmissionEvidenceBasis {
        match (self.up_lcb, self.up_ucb, self.down_lcb, self.down_ucb) {
            (None, None, None, None) => OracleEmissionEvidenceBasis::MeanOnly,
            (Some(up_lcb), Some(up_ucb), Some(down_lcb), Some(down_ucb))
                if up_lcb.is_finite()
                    && up_ucb.is_finite()
                    && down_lcb.is_finite()
                    && down_ucb.is_finite() =>
            {
                OracleEmissionEvidenceBasis::Bounded
            }
            _ => OracleEmissionEvidenceBasis::Invalid,
        }
    }

    pub(super) fn allows_market(self, side: PairTokenSide, margin: f64, dominance: f64) -> bool {
        let Some((selected, opposing)) = self.market_edges(side) else {
            return false;
        };
        selected > margin.max(0.0) && selected - opposing > dominance
    }

    fn allows_mean(self, side: PairTokenSide, threshold: f64, dominance: f64) -> bool {
        let (selected, other) = match side {
            PairTokenSide::Up => (self.mean_up, self.mean_down),
            PairTokenSide::Down => (self.mean_down, self.mean_up),
        };
        selected > threshold.max(0.0) && selected - other > dominance
    }

    pub(super) fn mean_confidence_edge(self, side: PairTokenSide) -> f64 {
        match side {
            PairTokenSide::Up => self.mean_up,
            PairTokenSide::Down => self.mean_down,
        }
    }

    pub(super) fn market_confidence_edge(self, side: PairTokenSide) -> Option<f64> {
        self.market_edges(side).map(|(selected, _)| selected)
    }

    fn market_edges(self, side: PairTokenSide) -> Option<(f64, f64)> {
        match self.basis() {
            OracleEmissionEvidenceBasis::MeanOnly => Some(match side {
                PairTokenSide::Up => (self.mean_up, self.mean_down),
                PairTokenSide::Down => (self.mean_down, self.mean_up),
            }),
            OracleEmissionEvidenceBasis::Bounded => Some(match side {
                PairTokenSide::Up => (self.up_lcb?, self.down_ucb?),
                PairTokenSide::Down => (self.down_lcb?, self.up_ucb?),
            }),
            OracleEmissionEvidenceBasis::Invalid => None,
        }
    }

    fn market_abstention_issue(self, input: &OracleSelectionInput<'_>) -> &'static str {
        let Some((up_selected, up_opposing)) = self.market_edges(PairTokenSide::Up) else {
            return "oracle_emission_selection_inconsistent";
        };
        let Some((down_selected, down_opposing)) = self.market_edges(PairTokenSide::Down) else {
            return "oracle_emission_selection_inconsistent";
        };
        if up_selected <= 0.0 && down_selected <= 0.0 {
            return "oracle_emission_advantage_nonpositive";
        }
        let (up_threshold, up_dominance, down_threshold, down_dominance) = match input.previous_side
        {
            Some(PairTokenSide::Up) => (
                input.theta_release_up,
                input.theta_flip,
                input.theta_enter_down,
                input.theta_escape,
            ),
            Some(PairTokenSide::Down) => (
                input.theta_enter_up,
                input.theta_escape,
                input.theta_release_down,
                input.theta_flip,
            ),
            None => (
                input.theta_enter_up,
                input.theta_flip,
                input.theta_enter_down,
                input.theta_flip,
            ),
        };
        let up_threshold_failed = !(up_selected > up_threshold.max(0.0));
        let down_threshold_failed = !(down_selected > down_threshold.max(0.0));
        let up_dominance_failed = !(up_selected - up_opposing > up_dominance);
        let down_dominance_failed = !(down_selected - down_opposing > down_dominance);
        match (
            up_threshold_failed,
            up_dominance_failed,
            down_threshold_failed,
            down_dominance_failed,
        ) {
            (true, _, true, _) => "oracle_emission_up_threshold_down_threshold",
            (true, _, false, true) => "oracle_emission_up_threshold_down_dominance",
            (false, true, true, _) => "oracle_emission_up_dominance_down_threshold",
            (false, true, false, true) => "oracle_emission_dominance_insufficient",
            _ => "oracle_emission_selection_inconsistent",
        }
    }

    fn cleanup_abstention_issue(self, side: PairTokenSide, release: f64) -> &'static str {
        let selected = self.mean_confidence_edge(side);
        if selected <= release.max(0.0) {
            return "oracle_cleanup_advantage_below_release";
        }
        "oracle_emission_dominance_insufficient"
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleSelectionInput<'a> {
    pub(super) mode: OracleAuthorityMode,
    pub(super) previous: &'a OracleAuthoritySnapshot,
    pub(super) previous_side: Option<PairTokenSide>,
    pub(super) evidence: OracleEmissionEvidence,
    pub(super) adv_up: f64,
    pub(super) adv_down: f64,
    pub(super) adv_up_ewma: f64,
    pub(super) adv_down_ewma: f64,
    pub(super) theta_enter_up: f64,
    pub(super) theta_enter_down: f64,
    pub(super) theta_release_up: f64,
    pub(super) theta_release_down: f64,
    pub(super) theta_flip: f64,
    pub(super) theta_escape: f64,
    pub(super) alpha: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleSelectionDecision {
    pub(super) signal: Option<PairTokenSide>,
    pub(super) reason: Option<OracleSideReason>,
    pub(super) data_issue: Option<&'static str>,
    pub(super) remaining_owned_adv: Option<f64>,
    pub(super) remaining_owned_adv_ewma: Option<f64>,
    pub(super) remaining_owned_noise_ewma: Option<f64>,
}

pub(super) fn select_oracle_side(input: OracleSelectionInput<'_>) -> OracleSelectionDecision {
    let decision = match input.mode {
        OracleAuthorityMode::Market => match input.previous_side {
            Some(PairTokenSide::Up) => {
                if input.evidence.allows_market(
                    PairTokenSide::Down,
                    input.theta_enter_down,
                    input.theta_escape,
                ) {
                    (
                        Some(PairTokenSide::Down),
                        Some(OracleSideReason::FlipToDown),
                    )
                } else if input.evidence.allows_market(
                    PairTokenSide::Up,
                    input.theta_release_up,
                    input.theta_flip,
                ) {
                    (Some(PairTokenSide::Up), Some(OracleSideReason::PreserveUp))
                } else {
                    (None, Some(OracleSideReason::NoActionableEdge))
                }
            }
            Some(PairTokenSide::Down) => {
                if input.evidence.allows_market(
                    PairTokenSide::Up,
                    input.theta_enter_up,
                    input.theta_escape,
                ) {
                    (Some(PairTokenSide::Up), Some(OracleSideReason::FlipToUp))
                } else if input.evidence.allows_market(
                    PairTokenSide::Down,
                    input.theta_release_down,
                    input.theta_flip,
                ) {
                    (
                        Some(PairTokenSide::Down),
                        Some(OracleSideReason::PreserveDown),
                    )
                } else {
                    (None, Some(OracleSideReason::NoActionableEdge))
                }
            }
            None => {
                if input.evidence.allows_market(
                    PairTokenSide::Up,
                    input.theta_enter_up,
                    input.theta_flip,
                ) {
                    (Some(PairTokenSide::Up), Some(OracleSideReason::SelectUp))
                } else if input.evidence.allows_market(
                    PairTokenSide::Down,
                    input.theta_enter_down,
                    input.theta_flip,
                ) {
                    (
                        Some(PairTokenSide::Down),
                        Some(OracleSideReason::SelectDown),
                    )
                } else {
                    (None, Some(OracleSideReason::NoActionableEdge))
                }
            }
        },
        OracleAuthorityMode::Cleanup {
            remaining_owned_side,
        } => {
            let (adv, filtered, release) = match remaining_owned_side {
                PairTokenSide::Up => (input.adv_up, input.adv_up_ewma, input.theta_release_up),
                PairTokenSide::Down => (
                    input.adv_down,
                    input.adv_down_ewma,
                    input.theta_release_down,
                ),
            };
            let noise = advantage_noise_step(
                input
                    .previous
                    .remaining_owned_noise_ewma
                    .or(input.previous.adv_noise),
                filtered,
                adv,
                input.alpha,
            );
            let previously_selected = input.previous_side == Some(remaining_owned_side)
                && (input.previous.cleanup_anchor_side == Some(remaining_owned_side)
                    || input.previous.controller_mode == OracleAuthorityMode::Market);
            let previous_market_bias_matches = input.previous.controller_mode
                == OracleAuthorityMode::Market
                && match remaining_owned_side {
                    PairTokenSide::Up => input.adv_up_ewma > input.adv_down_ewma,
                    PairTokenSide::Down => input.adv_down_ewma > input.adv_up_ewma,
                };
            let signal =
                input
                    .evidence
                    .allows_mean(remaining_owned_side, release, input.theta_flip);
            return OracleSelectionDecision {
                signal: signal.then_some(remaining_owned_side),
                reason: Some(if signal {
                    match remaining_owned_side {
                        PairTokenSide::Up
                            if previously_selected || previous_market_bias_matches =>
                        {
                            OracleSideReason::PreserveUp
                        }
                        PairTokenSide::Down
                            if previously_selected || previous_market_bias_matches =>
                        {
                            OracleSideReason::PreserveDown
                        }
                        PairTokenSide::Up => OracleSideReason::SelectUp,
                        PairTokenSide::Down => OracleSideReason::SelectDown,
                    }
                } else {
                    OracleSideReason::NoActionableEdge
                }),
                data_issue: (!signal).then(|| {
                    input
                        .evidence
                        .cleanup_abstention_issue(remaining_owned_side, release)
                }),
                remaining_owned_adv: Some(adv),
                remaining_owned_adv_ewma: Some(filtered),
                remaining_owned_noise_ewma: Some(noise),
            };
        }
    };
    OracleSelectionDecision {
        signal: decision.0,
        reason: decision.1,
        data_issue: decision
            .0
            .is_none()
            .then(|| input.evidence.market_abstention_issue(&input)),
        remaining_owned_adv: None,
        remaining_owned_adv_ewma: None,
        remaining_owned_noise_ewma: None,
    }
}

pub(super) fn ewma_step(previous: Option<f64>, sample: f64, alpha: f64) -> f64 {
    match previous {
        Some(prev) => prev + alpha * (sample - prev),
        None => sample,
    }
}

pub(super) fn ewma_alpha(now_ms: u64, previous_eval_ts_ms: Option<u64>, half_life_ms: u64) -> f64 {
    let half_life_ms = half_life_ms.max(1) as f64;
    let dt_ms = previous_eval_ts_ms
        .map(|previous_ms| now_ms.saturating_sub(previous_ms) as f64)
        .unwrap_or(half_life_ms);
    1.0 - (-std::f64::consts::LN_2 * dt_ms / half_life_ms).exp()
}

pub(super) fn advantage_noise_step(
    previous: Option<f64>,
    filtered: f64,
    raw: f64,
    alpha: f64,
) -> f64 {
    let sample = (raw - filtered).abs();
    ewma_step(previous, sample, alpha)
}

pub(super) fn dynamic_threshold(base: f64, adv_noise: f64, noise_weight: f64) -> f64 {
    // Transaction cost and spread are already embedded in the executable-edge samples.
    (base + noise_weight * adv_noise).max(0.0)
}

pub(super) fn oracle_side_confidence(filtered_adv: f64, threshold: f64, adv_noise: f64) -> f64 {
    let denom = (threshold + adv_noise).max(1e-6);
    ((filtered_adv - threshold) / denom).clamp(0.0, 1.0)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleEmissionThresholdInput {
    pub(super) base: f64,
    pub(super) adv_noise: f64,
    pub(super) uncertainty: f64,
    pub(super) threshold_multiplier: f64,
    pub(super) noise_weight: f64,
    pub(super) flip_hysteresis: f64,
    pub(super) flip_noise_weight: f64,
    pub(super) cleanup_flip_bump: f64,
    pub(super) cleanup_mode: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OracleEmissionThresholds {
    pub(super) enter_up: f64,
    pub(super) enter_down: f64,
    pub(super) release_up: f64,
    pub(super) release_down: f64,
    pub(super) flip: f64,
    pub(super) escape: f64,
}

pub(super) fn oracle_emission_thresholds(
    input: OracleEmissionThresholdInput,
) -> OracleEmissionThresholds {
    let enter = dynamic_threshold(input.base, input.adv_noise, input.noise_weight)
        .max(input.uncertainty)
        * input.threshold_multiplier;
    let enter_up = enter;
    let enter_down = enter;
    let flip = (input.flip_hysteresis + input.flip_noise_weight * input.adv_noise)
        * input.threshold_multiplier;
    let release_offset = input.flip_hysteresis * input.threshold_multiplier;
    let cleanup_bump = if input.cleanup_mode {
        input.cleanup_flip_bump * input.threshold_multiplier
    } else {
        0.0
    };
    OracleEmissionThresholds {
        enter_up,
        enter_down,
        release_up: (enter_up - release_offset).max(0.0),
        release_down: (enter_down - release_offset).max(0.0),
        flip,
        escape: flip + cleanup_bump,
    }
}

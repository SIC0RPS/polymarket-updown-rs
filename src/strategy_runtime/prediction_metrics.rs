use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const PREDICTION_PHASES: usize = 5;
pub const PREDICTION_METRIC_DEFINITION: &str = "published_final_up_equal_market_brier_v1";

/// Binary probability scores, with each represented market receiving equal weight.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ProbabilityScores {
    pub observations: u64,
    pub markets: u64,
    pub brier: Option<f64>,
    pub log_loss: Option<f64>,
    pub direction_accuracy: Option<f64>,
    pub direction_observations: u64,
    pub direction_ties: u64,
}

/// Sufficient statistics for both possible labels, collected before the label arrives.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProbabilityScoreSums {
    observations: u64,
    brier: [f64; 2],
    log_loss: [f64; 2],
    direction_up: u64,
    direction_down: u64,
    direction_ties: u64,
}

impl ProbabilityScoreSums {
    pub(crate) fn observe(&mut self, probability: f64) -> bool {
        if !valid_probability(probability) {
            return false;
        }
        self.observations += 1;
        self.brier[0] += probability.powi(2);
        self.brier[1] += (probability - 1.0).powi(2);
        let capped = probability.clamp(1e-6, 1.0 - 1e-6);
        self.log_loss[0] += -(1.0 - capped).ln();
        self.log_loss[1] += -capped.ln();
        match probability_direction(probability) {
            Some(true) => self.direction_up += 1,
            Some(false) => self.direction_down += 1,
            None => self.direction_ties += 1,
        }
        true
    }

    pub(crate) fn scores(&self, winner_up: bool) -> ProbabilityScores {
        let count = self.observations;
        let direction_observations = self.direction_up + self.direction_down;
        let correct = if winner_up {
            self.direction_up
        } else {
            self.direction_down
        };
        ProbabilityScores {
            observations: count,
            markets: u64::from(count != 0),
            brier: (count != 0).then(|| self.brier[usize::from(winner_up)] / count as f64),
            log_loss: (count != 0).then(|| self.log_loss[usize::from(winner_up)] / count as f64),
            direction_accuracy: (direction_observations != 0)
                .then(|| correct as f64 / direction_observations as f64),
            direction_observations,
            direction_ties: self.direction_ties,
        }
    }
}

pub(crate) fn aggregate_probability_scores<'a>(
    rows: impl Iterator<Item = (Option<f64>, &'a str, bool)>,
) -> ProbabilityScores {
    let mut markets = BTreeMap::<&str, (ProbabilityScoreSums, bool)>::new();
    for (probability, market, winner_up) in rows {
        let Some(probability) = probability.filter(|value| valid_probability(*value)) else {
            continue;
        };
        let row = markets
            .entry(market)
            .or_insert_with(|| (ProbabilityScoreSums::default(), winner_up));
        row.0.observe(probability);
    }
    equal_market_scores(markets.values().map(|(sums, winner)| sums.scores(*winner)))
}

pub fn equal_market_scores(rows: impl Iterator<Item = ProbabilityScores>) -> ProbabilityScores {
    let mut result = ProbabilityScores::default();
    let mut brier_sum = 0.0;
    let mut log_loss_sum = 0.0;
    let mut direction_sum = 0.0;
    let mut direction_markets = 0_u64;
    for row in rows {
        result.observations += row.observations;
        result.direction_observations += row.direction_observations;
        result.direction_ties += row.direction_ties;
        if let (Some(brier), Some(log_loss)) = (row.brier, row.log_loss) {
            result.markets += 1;
            brier_sum += brier;
            log_loss_sum += log_loss;
        }
        if let Some(accuracy) = row.direction_accuracy {
            direction_sum += accuracy;
            direction_markets += 1;
        }
    }
    result.brier = (result.markets != 0).then(|| brier_sum / result.markets as f64);
    result.log_loss = (result.markets != 0).then(|| log_loss_sum / result.markets as f64);
    result.direction_accuracy =
        (direction_markets != 0).then(|| direction_sum / direction_markets as f64);
    result
}

pub(crate) fn valid_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

pub(crate) fn probability_direction(probability: f64) -> Option<bool> {
    if probability > 0.5 {
        Some(true)
    } else if probability < 0.5 {
        Some(false)
    } else {
        None
    }
}

pub(crate) fn phase_index(start_ms: u64, end_ms: u64, decision_ms: u64) -> Option<usize> {
    let duration = end_ms.checked_sub(start_ms)?;
    let elapsed = decision_ms.checked_sub(start_ms)?;
    if duration == 0 || elapsed >= duration {
        return None;
    }
    Some(((u128::from(elapsed) * PREDICTION_PHASES as u128) / u128::from(duration)) as usize)
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct PredictionPhaseMetrics {
    pub expected_observations: u64,
    pub valid_predictions: u64,
    pub unavailable_predictions: u64,
    pub invalid_predictions: u64,
    pub future_dated_forecasts: u64,
    pub expected_observation_identity: String,
    pub valid_prediction_identity: String,
    pub direction_up: u64,
    pub direction_down: u64,
    pub direction_ties: u64,
    pub emitted_up: u64,
    pub emitted_down: u64,
    pub abstained: u64,
    pub snapshot_reuses: u64,
    pub scores: ProbabilityScores,
    pub emitted_direction_accuracy: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct PredictionMarketMetrics {
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub label_identity: String,
    pub winner_up: Option<bool>,
    pub scores: ProbabilityScores,
    pub emitted_direction_accuracy: Option<f64>,
    pub phases: [PredictionPhaseMetrics; PREDICTION_PHASES],
}

/// Evidence from original recorded runtime consumptions, never synthetic drives.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct PredictionMetrics {
    pub metric_definition: String,
    pub brier: Option<f64>,
    pub log_loss: Option<f64>,
    pub direction_accuracy: Option<f64>,
    pub emitted_direction_accuracy: Option<f64>,
    pub expected_observations: u64,
    pub valid_predictions: u64,
    pub unavailable_predictions: u64,
    pub invalid_predictions: u64,
    pub future_dated_forecasts: u64,
    pub missing_labels: u64,
    pub direction_up: u64,
    pub direction_down: u64,
    pub direction_ties: u64,
    pub emitted_up: u64,
    pub emitted_down: u64,
    pub abstained: u64,
    pub snapshot_reuses: u64,
    pub markets: BTreeMap<String, PredictionMarketMetrics>,
    pub phases: [PredictionPhaseMetrics; PREDICTION_PHASES],
}

impl PredictionMetrics {
    /// Structural validity is separate from eligibility, so rejected evidence is retained.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.metric_definition == PREDICTION_METRIC_DEFINITION,
            "prediction metric definition mismatch"
        );
        validate_optional_probability(self.brier)?;
        validate_optional_loss(self.log_loss)?;
        validate_optional_probability(self.direction_accuracy)?;
        validate_optional_probability(self.emitted_direction_accuracy)?;
        let mut totals = [0_u64; 12];
        for phase in &self.phases {
            phase.validate()?;
            add_counts(&mut totals, phase)?;
        }
        ensure!(
            totals == self.counts(),
            "prediction aggregate count mismatch"
        );
        let mut market_totals = [0_u64; 12];
        let mut missing_labels = 0_u64;
        let mut by_phase = [[0_u64; 12]; PREDICTION_PHASES];
        for (identity, market) in &self.markets {
            ensure!(!identity.is_empty(), "prediction market identity is empty");
            ensure!(
                market.window_start_ms < market.window_end_ms,
                "prediction market window is invalid"
            );
            ensure!(
                market.winner_up.is_some() == valid_digest(&market.label_identity),
                "prediction label identity mismatch"
            );
            missing_labels += u64::from(market.winner_up.is_none());
            validate_scores(&market.scores)?;
            validate_optional_probability(market.emitted_direction_accuracy)?;
            let mut market_count = 0_u64;
            for (index, phase) in market.phases.iter().enumerate() {
                phase.validate()?;
                add_counts(&mut market_totals, phase)?;
                add_counts(&mut by_phase[index], phase)?;
                market_count += phase.valid_predictions;
                if market.winner_up.is_some() {
                    ensure!(
                        phase.scores.observations == phase.valid_predictions
                            && phase.scores.markets == u64::from(phase.valid_predictions != 0)
                            && phase.scores.direction_observations
                                == phase.direction_up + phase.direction_down
                            && phase.scores.direction_ties == phase.direction_ties,
                        "prediction market phase score population mismatch"
                    );
                } else {
                    ensure!(
                        phase.scores == ProbabilityScores::default(),
                        "prediction phase scored without a recorded label"
                    );
                }
            }
            ensure!(
                market.winner_up.is_none()
                    || (market.scores.observations == market_count
                        && market.scores.markets == u64::from(market_count != 0)),
                "prediction market score population mismatch"
            );
            ensure!(
                market.winner_up.is_some() || market.scores == ProbabilityScores::default(),
                "prediction market scored without a recorded label"
            );
        }
        let scores = equal_market_scores(self.markets.values().map(|market| market.scores.clone()));
        ensure!(
            self.brier == scores.brier
                && self.log_loss == scores.log_loss
                && self.direction_accuracy == scores.direction_accuracy,
            "prediction aggregate score mismatch"
        );
        ensure!(market_totals == totals, "prediction market count mismatch");
        ensure!(
            missing_labels == self.missing_labels,
            "prediction missing-label count mismatch"
        );
        for (index, counts) in by_phase.iter().enumerate() {
            ensure!(
                *counts == self.phases[index].counts(),
                "prediction phase count mismatch"
            );
            let scores = equal_market_scores(
                self.markets
                    .values()
                    .map(|market| market.phases[index].scores.clone()),
            );
            ensure!(
                self.phases[index].scores == scores,
                "prediction phase score mismatch"
            );
        }
        Ok(())
    }

    pub fn is_admissible(&self) -> bool {
        self.validate().is_ok()
            && self.expected_observations != 0
            && self.valid_predictions != 0
            && self.invalid_predictions == 0
            && self.future_dated_forecasts == 0
            && self.missing_labels == 0
            && self.brier.is_some_and(valid_probability)
            && self
                .log_loss
                .is_some_and(|value| value.is_finite() && value >= 0.0)
    }

    pub fn comparable_coverage(&self, reference: &Self) -> bool {
        self.is_admissible()
            && reference.is_admissible()
            && self.markets.len() == reference.markets.len()
            && self.markets.iter().all(|(identity, market)| {
                reference.markets.get(identity).is_some_and(|other| {
                    market.window_start_ms == other.window_start_ms
                        && market.window_end_ms == other.window_end_ms
                        && market.label_identity == other.label_identity
                        && market.winner_up == other.winner_up
                        && market
                            .phases
                            .iter()
                            .zip(&other.phases)
                            .all(|(left, right)| {
                                left.expected_observations == right.expected_observations
                                    && left.valid_predictions == right.valid_predictions
                                    && left.expected_observation_identity
                                        == right.expected_observation_identity
                                    && left.valid_prediction_identity
                                        == right.valid_prediction_identity
                            })
                })
            })
    }

    fn counts(&self) -> [u64; 12] {
        [
            self.expected_observations,
            self.valid_predictions,
            self.unavailable_predictions,
            self.invalid_predictions,
            self.future_dated_forecasts,
            self.direction_up,
            self.direction_down,
            self.direction_ties,
            self.emitted_up,
            self.emitted_down,
            self.abstained,
            self.snapshot_reuses,
        ]
    }
}

impl PredictionPhaseMetrics {
    fn counts(&self) -> [u64; 12] {
        [
            self.expected_observations,
            self.valid_predictions,
            self.unavailable_predictions,
            self.invalid_predictions,
            self.future_dated_forecasts,
            self.direction_up,
            self.direction_down,
            self.direction_ties,
            self.emitted_up,
            self.emitted_down,
            self.abstained,
            self.snapshot_reuses,
        ]
    }

    fn validate(&self) -> Result<()> {
        let classified = [
            self.valid_predictions,
            self.unavailable_predictions,
            self.invalid_predictions,
            self.future_dated_forecasts,
        ]
        .into_iter()
        .try_fold(0_u64, |sum, count| sum.checked_add(count));
        ensure!(
            classified == Some(self.expected_observations),
            "prediction availability count mismatch"
        );
        ensure!(
            self.direction_up
                .checked_add(self.direction_down)
                .and_then(|sum| sum.checked_add(self.direction_ties))
                == Some(self.valid_predictions),
            "prediction direction count mismatch"
        );
        ensure!(
            self.emitted_up
                .checked_add(self.emitted_down)
                .and_then(|sum| sum.checked_add(self.abstained))
                == Some(self.expected_observations),
            "prediction emitted direction count mismatch"
        );
        ensure!(
            self.snapshot_reuses <= self.expected_observations,
            "prediction snapshot reuse count mismatch"
        );
        ensure!(
            (self.expected_observations == 0 && self.expected_observation_identity.is_empty())
                || valid_digest(&self.expected_observation_identity),
            "prediction expected-observation identity is invalid"
        );
        ensure!(
            (self.valid_predictions == 0 && self.valid_prediction_identity.is_empty())
                || valid_digest(&self.valid_prediction_identity),
            "prediction valid-observation identity is invalid"
        );
        validate_scores(&self.scores)?;
        validate_optional_probability(self.emitted_direction_accuracy)
    }
}

fn add_counts(totals: &mut [u64; 12], phase: &PredictionPhaseMetrics) -> Result<()> {
    for (total, count) in totals.iter_mut().zip(phase.counts()) {
        *total = total
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("prediction count overflow"))?;
    }
    Ok(())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_optional_probability(value: Option<f64>) -> Result<()> {
    ensure!(
        value.is_none_or(valid_probability),
        "prediction probability metric is invalid"
    );
    Ok(())
}

fn validate_optional_loss(value: Option<f64>) -> Result<()> {
    ensure!(
        value.is_none_or(|value| value.is_finite() && value >= 0.0),
        "prediction loss metric is invalid"
    );
    Ok(())
}

fn validate_scores(scores: &ProbabilityScores) -> Result<()> {
    validate_optional_probability(scores.brier)?;
    validate_optional_loss(scores.log_loss)?;
    validate_optional_probability(scores.direction_accuracy)?;
    ensure!(
        scores
            .direction_observations
            .checked_add(scores.direction_ties)
            == Some(scores.observations),
        "prediction scored direction count mismatch"
    );
    ensure!(
        scores.markets <= scores.observations
            && scores.brier.is_some() == (scores.markets != 0)
            && scores.log_loss.is_some() == (scores.markets != 0)
            && scores.direction_accuracy.is_some() == (scores.direction_observations != 0),
        "prediction score availability mismatch"
    );
    Ok(())
}

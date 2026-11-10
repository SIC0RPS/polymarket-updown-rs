use alloy::hex;
use anyhow::{ensure, Context, Result};
use polymarket_client_sdk::types::B256;
use sha2::{Digest, Sha256};

use crate::polymarket::identity::MarketId;
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy::support::oracle_authority::OracleAuthorityMode;
use crate::strategy::MakerRebateMmStrategy;
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::strategy_runtime::prediction_metrics::{
    equal_market_scores, phase_index, probability_direction, valid_probability,
    PredictionMarketMetrics, PredictionMetrics, PredictionPhaseMetrics, ProbabilityScoreSums,
    PREDICTION_METRIC_DEFINITION, PREDICTION_PHASES,
};
use crate::strategy_runtime::symbol_process::protocol::OracleResolvedLabelRecord;

/// Passive, bounded evidence owned by one replay candidate's existing worker.
pub(super) struct PredictionCollector {
    markets: Vec<MarketAccumulator>,
}

struct MarketAccumulator {
    record: MarketCatalogRecord,
    identity: MarketId,
    phases: [PhaseAccumulator; PREDICTION_PHASES],
    scores: ProbabilityScoreSums,
    label: Option<OracleResolvedLabelRecord>,
    previous_publication: Option<(u64, Option<u64>, Option<PairTokenSide>, OracleAuthorityMode)>,
}

#[derive(Default)]
struct PhaseAccumulator {
    metrics: PredictionPhaseMetrics,
    scores: ProbabilityScoreSums,
    expected: Sha256,
    valid: Sha256,
}

impl PredictionCollector {
    pub(super) fn new(catalog: &MarketCatalog) -> Self {
        let markets = catalog
            .records()
            .iter()
            .map(|record| {
                let phases = std::array::from_fn(|phase| {
                    let mut identity = Sha256::new();
                    identity.update(b"PM-PREDICTION-OBSERVATIONS-1");
                    identity.update(catalog.sha256());
                    identity.update(record.condition_id);
                    identity.update(record.window_start_ms.to_le_bytes());
                    identity.update(record.window_end_ms.to_le_bytes());
                    identity.update((phase as u64).to_le_bytes());
                    PhaseAccumulator {
                        expected: identity.clone(),
                        valid: identity,
                        ..PhaseAccumulator::default()
                    }
                });
                MarketAccumulator {
                    record: *record,
                    identity: MarketId::new(B256::from(record.condition_id).to_string()),
                    phases,
                    scores: ProbabilityScoreSums::default(),
                    label: None,
                    previous_publication: None,
                }
            })
            .collect();
        Self { markets }
    }

    /// Called only after an original RuntimeInput, including an undriven heartbeat.
    pub(super) fn observe_runtime(
        &mut self,
        strategy: &MakerRebateMmStrategy,
        sequence: u64,
        decision_ms: u64,
    ) -> Result<()> {
        ensure!(sequence != 0, "prediction observation sequence is zero");
        for market in &mut self.markets {
            let Some(index) = phase_index(
                market.record.window_start_ms,
                market.record.window_end_ms,
                decision_ms,
            ) else {
                continue;
            };
            let phase = &mut market.phases[index];
            let metrics = &mut phase.metrics;
            metrics.expected_observations += 1;
            observation_key(&mut phase.expected, sequence, decision_ms);
            let snapshot = strategy.oracle_authority_by_market.get(&market.identity);
            let eval_ts = snapshot.and_then(|snapshot| snapshot.last_eval_ts_ms);
            let publication = snapshot.and_then(|snapshot| {
                snapshot.last_eval_ts_ms.map(|timestamp| {
                    (
                        timestamp,
                        snapshot.p_fair.map(f64::to_bits),
                        snapshot.oracle_signal_side,
                        snapshot.controller_mode,
                    )
                })
            });
            metrics.snapshot_reuses +=
                u64::from(publication.is_some() && publication == market.previous_publication);
            market.previous_publication = publication;
            match snapshot.and_then(|snapshot| snapshot.oracle_signal_side) {
                Some(PairTokenSide::Up) => metrics.emitted_up += 1,
                Some(PairTokenSide::Down) => metrics.emitted_down += 1,
                None => metrics.abstained += 1,
            }
            if eval_ts.is_some_and(|timestamp| timestamp > decision_ms) {
                metrics.future_dated_forecasts += 1;
                continue;
            }
            let probability = snapshot.and_then(|snapshot| snapshot.p_fair);
            let Some(probability) = probability else {
                metrics.unavailable_predictions += 1;
                continue;
            };
            if !valid_probability(probability) {
                metrics.invalid_predictions += 1;
                continue;
            }
            // A probability without its original publication clock is unavailable.
            if eval_ts.is_none() {
                metrics.unavailable_predictions += 1;
                continue;
            }
            metrics.valid_predictions += 1;
            observation_key(&mut phase.valid, sequence, decision_ms);
            phase.scores.observe(probability);
            market.scores.observe(probability);
            match probability_direction(probability) {
                Some(true) => metrics.direction_up += 1,
                Some(false) => metrics.direction_down += 1,
                None => metrics.direction_ties += 1,
            }
        }
        Ok(())
    }

    /// Uses only labels emitted while consuming the original recorded Chainlink lane.
    pub(super) fn observe_labels(
        &mut self,
        labels: &[OracleResolvedLabelRecord],
        catalog: &MarketCatalog,
        consumption_ms: u64,
    ) -> Result<()> {
        for label in labels {
            let label = label.validate()?.validate_catalog(catalog)?;
            ensure!(
                label.release_ts_ms <= consumption_ms,
                "prediction label release exceeds original consumption clock"
            );
            let market = self
                .markets
                .get_mut((label.market_id - 1) as usize)
                .context("prediction label market is absent from frozen population")?;
            ensure!(
                market.record.market_handle == label.market_id,
                "prediction label market handle mismatch"
            );
            ensure!(
                market.label.is_none_or(|previous| previous == label),
                "prediction label changed for a frozen market"
            );
            market.label = Some(label);
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<PredictionMetrics> {
        let mut result = PredictionMetrics {
            metric_definition: PREDICTION_METRIC_DEFINITION.to_string(),
            ..PredictionMetrics::default()
        };
        for market in self.markets {
            if market
                .phases
                .iter()
                .all(|phase| phase.metrics.expected_observations == 0)
            {
                continue;
            }
            let winner = market.label.map(|label| label.winner == 1);
            let mut row = PredictionMarketMetrics {
                window_start_ms: market.record.window_start_ms,
                window_end_ms: market.record.window_end_ms,
                label_identity: market
                    .label
                    .map(|label| label_identity(&market.record, label))
                    .unwrap_or_default(),
                winner_up: winner,
                scores: winner
                    .map(|winner| market.scores.scores(winner))
                    .unwrap_or_default(),
                ..PredictionMarketMetrics::default()
            };
            for (index, phase) in market.phases.into_iter().enumerate() {
                let mut metrics = phase.metrics;
                metrics.expected_observation_identity = hex::encode(phase.expected.finalize());
                metrics.valid_prediction_identity = hex::encode(phase.valid.finalize());
                metrics.scores = winner
                    .map(|winner| phase.scores.scores(winner))
                    .unwrap_or_default();
                metrics.emitted_direction_accuracy =
                    winner.and_then(|winner| emitted_accuracy(&metrics, winner));
                row.phases[index] = metrics;
            }
            row.emitted_direction_accuracy = winner.and_then(|winner| {
                let up = row.phases.iter().map(|phase| phase.emitted_up).sum::<u64>();
                let down = row
                    .phases
                    .iter()
                    .map(|phase| phase.emitted_down)
                    .sum::<u64>();
                (up + down != 0).then(|| if winner { up } else { down } as f64 / (up + down) as f64)
            });
            result.missing_labels += u64::from(winner.is_none());
            result.markets.insert(market.identity.to_string(), row);
        }
        let scores =
            equal_market_scores(result.markets.values().map(|market| market.scores.clone()));
        result.brier = scores.brier;
        result.log_loss = scores.log_loss;
        result.direction_accuracy = scores.direction_accuracy;
        result.emitted_direction_accuracy = mean(
            result
                .markets
                .values()
                .filter_map(|market| market.emitted_direction_accuracy),
        );
        for index in 0..PREDICTION_PHASES {
            let phase = &mut result.phases[index];
            let mut expected = Sha256::new();
            let mut valid = Sha256::new();
            expected.update(b"PM-PREDICTION-PHASE-1");
            valid.update(b"PM-PREDICTION-PHASE-1");
            for (identity, market) in &result.markets {
                let row = &market.phases[index];
                add_counts(phase, row);
                expected.update(identity.as_bytes());
                expected.update(row.expected_observation_identity.as_bytes());
                valid.update(identity.as_bytes());
                valid.update(row.valid_prediction_identity.as_bytes());
            }
            phase.expected_observation_identity = hex::encode(expected.finalize());
            phase.valid_prediction_identity = hex::encode(valid.finalize());
            phase.scores = equal_market_scores(
                result
                    .markets
                    .values()
                    .map(|market| market.phases[index].scores.clone()),
            );
            phase.emitted_direction_accuracy = mean(
                result
                    .markets
                    .values()
                    .filter_map(|market| market.phases[index].emitted_direction_accuracy),
            );
            result.expected_observations += phase.expected_observations;
            result.valid_predictions += phase.valid_predictions;
            result.unavailable_predictions += phase.unavailable_predictions;
            result.invalid_predictions += phase.invalid_predictions;
            result.future_dated_forecasts += phase.future_dated_forecasts;
            result.direction_up += phase.direction_up;
            result.direction_down += phase.direction_down;
            result.direction_ties += phase.direction_ties;
            result.emitted_up += phase.emitted_up;
            result.emitted_down += phase.emitted_down;
            result.abstained += phase.abstained;
            result.snapshot_reuses += phase.snapshot_reuses;
        }
        result.validate()?;
        Ok(result)
    }
}

fn observation_key(hasher: &mut Sha256, sequence: u64, decision_ms: u64) {
    hasher.update(sequence.to_le_bytes());
    hasher.update(decision_ms.to_le_bytes());
}

fn label_identity(market: &MarketCatalogRecord, label: OracleResolvedLabelRecord) -> String {
    let mut identity = Sha256::new();
    identity.update(b"PM-PREDICTION-LABEL-1");
    identity.update(market.condition_id);
    identity.update([label.winner, label.flags]);
    for value in [
        label.window_start_ms,
        label.window_end_ms,
        label.release_ts_ms,
        label.open_source_ts_ms,
        label.open_observed_ts_ms,
        label.open_capture_sequence,
        label.close_source_ts_ms,
        label.close_observed_ts_ms,
        label.close_capture_sequence,
    ] {
        identity.update(value.to_le_bytes());
    }
    identity.update(label.open_price_e18.to_le_bytes());
    identity.update(label.close_price_e18.to_le_bytes());
    hex::encode(identity.finalize())
}

fn emitted_accuracy(phase: &PredictionPhaseMetrics, winner_up: bool) -> Option<f64> {
    let count = phase.emitted_up + phase.emitted_down;
    (count != 0).then(|| {
        let correct = if winner_up {
            phase.emitted_up
        } else {
            phase.emitted_down
        };
        correct as f64 / count as f64
    })
}

fn add_counts(total: &mut PredictionPhaseMetrics, row: &PredictionPhaseMetrics) {
    total.expected_observations += row.expected_observations;
    total.valid_predictions += row.valid_predictions;
    total.unavailable_predictions += row.unavailable_predictions;
    total.invalid_predictions += row.invalid_predictions;
    total.future_dated_forecasts += row.future_dated_forecasts;
    total.direction_up += row.direction_up;
    total.direction_down += row.direction_down;
    total.direction_ties += row.direction_ties;
    total.emitted_up += row.emitted_up;
    total.emitted_down += row.emitted_down;
    total.abstained += row.abstained;
    total.snapshot_reuses += row.snapshot_reuses;
}

fn mean(values: impl Iterator<Item = f64>) -> Option<f64> {
    let (sum, count) = values.fold((0.0, 0_u64), |(sum, count), value| (sum + value, count + 1));
    (count != 0).then(|| sum / count as f64)
}

use std::collections::{BTreeMap, BTreeSet};

use super::trace::{Key, Label, Observation, Trace};
use super::types::{
    OracleResearchAccuracy, OracleResearchFinalOutputDiagnostics,
    OracleResearchFinalOutputPhaseMetrics, OracleResearchModelScores,
    OracleResearchPhaseTrustDiagnostics, OracleResearchPhaseTrustMetrics,
    OracleResearchProbabilityScores, OracleResearchPublishedPhaseMetrics,
    OracleResearchSignalScores, OracleResearchSymbolMetrics, OracleResearchUncertaintyAvailability,
};

use crate::strategy_runtime::prediction_metrics::{
    aggregate_probability_scores, probability_direction as direction,
};
pub(super) use crate::strategy_runtime::prediction_metrics::{
    phase_index as final_output_phase_index, valid_probability,
};

mod first_decision;
pub(in crate::strategy_runtime::backtest_matrix_report) use first_decision::first_decision_incomplete;
pub(super) use first_decision::{
    first_decision_metrics, first_evidence, first_join_conflicts, latched_missing,
};

pub(super) const PHASES: [&str; 5] = ["phase_0", "phase_1", "phase_2", "phase_3", "phase_4"];

pub(super) type FinalOutputProbabilityRow<'a> = (Option<f64>, &'a str, bool);
pub(super) type FinalOutputSignalRow<'a> = (bool, Option<&'a str>, &'a str, bool);

const FINAL_OUTPUT_POPULATIONS: [&str; 6] = [
    "common_market_nonmaterialization",
    "common_cleanup_nonmaterialization",
    "common_mode_unavailable_nonmaterialization",
    "own_market_materialization",
    "own_cleanup_materialization",
    "own_mode_unavailable_materialization",
];

pub(super) struct FinalOutputScores<'a> {
    retained_f0: [Vec<FinalOutputProbabilityRow<'a>>; 5],
    retained_f1: [Vec<FinalOutputProbabilityRow<'a>>; 5],
    published_probability: [Vec<FinalOutputProbabilityRow<'a>>; 30],
    published_signal: [Vec<FinalOutputSignalRow<'a>>; 30],
}

impl Default for FinalOutputScores<'_> {
    fn default() -> Self {
        Self {
            retained_f0: std::array::from_fn(|_| Vec::new()),
            retained_f1: std::array::from_fn(|_| Vec::new()),
            published_probability: std::array::from_fn(|_| Vec::new()),
            published_signal: std::array::from_fn(|_| Vec::new()),
        }
    }
}

impl<'a> FinalOutputScores<'a> {
    pub(super) fn record_retained(
        &mut self,
        phase: usize,
        market: &'a str,
        winner_up: bool,
        f0_p_up: f64,
        f1_p_up: f64,
    ) {
        self.retained_f0[phase].push((Some(f0_p_up), market, winner_up));
        self.retained_f1[phase].push((Some(f1_p_up), market, winner_up));
    }

    pub(super) fn record_published(
        &mut self,
        materialization: bool,
        mode: Option<&str>,
        phase: usize,
        probability: Option<f64>,
        signal_field_present: bool,
        signal: Option<&'a str>,
        market: &'a str,
        winner_up: bool,
    ) {
        let mode = match mode {
            Some("market") => 0,
            Some("cleanup") => 1,
            _ => 2,
        };
        let population = usize::from(materialization) * 3 + mode;
        let index = population * PHASES.len() + phase;
        self.published_probability[index].push((probability, market, winner_up));
        self.published_signal[index].push((signal_field_present, signal, market, winner_up));
    }

    pub(super) fn finish(mut self, diagnostics: &mut OracleResearchFinalOutputDiagnostics) {
        for (index, phase) in diagnostics.phases.iter_mut().enumerate() {
            phase.f0 = final_output_probability_scores(&self.retained_f0[index]);
            phase.f1 = final_output_probability_scores(&self.retained_f1[index]);
        }
        diagnostics
            .published
            .reserve(FINAL_OUTPUT_POPULATIONS.len() * PHASES.len() - diagnostics.published.len());
        for (population_index, population) in FINAL_OUTPUT_POPULATIONS.iter().enumerate() {
            for (phase_index, phase) in PHASES.iter().enumerate() {
                let index = population_index * PHASES.len() + phase_index;
                let probability = std::mem::take(&mut self.published_probability[index]);
                let signal = std::mem::take(&mut self.published_signal[index]);
                diagnostics
                    .published
                    .push(OracleResearchPublishedPhaseMetrics {
                        population: (*population).to_string(),
                        phase: (*phase).to_string(),
                        observations: probability.len() as u64,
                        final_probability: final_output_probability_scores(&probability),
                        emitted_signal: final_output_signal_scores(&signal),
                    });
            }
        }
    }
}

pub(super) fn final_output_diagnostics(
    application_policy: &str,
) -> OracleResearchFinalOutputDiagnostics {
    OracleResearchFinalOutputDiagnostics {
        application_policy: application_policy.to_string(),
        phases: PHASES
            .iter()
            .map(|phase| OracleResearchFinalOutputPhaseMetrics {
                phase: (*phase).to_string(),
                ..OracleResearchFinalOutputPhaseMetrics::default()
            })
            .collect(),
        ..OracleResearchFinalOutputDiagnostics::default()
    }
}

pub(super) fn scores<F>(
    rows: &[(&Key, &Observation, &Label, u8)],
    stage: u8,
    probability: F,
) -> OracleResearchModelScores
where
    F: Fn(&Observation) -> Option<f64>,
{
    aggregate_scores(rows.iter().filter(|row| row.3 & stage == stage).map(
        |(key, observation, label, _)| {
            (
                probability(observation),
                key.market.as_str(),
                label.winner_up,
            )
        },
    ))
}

pub(super) fn probability_scores(
    rows: &[(Option<f64>, &str, &Label)],
) -> OracleResearchModelScores {
    aggregate_scores(
        rows.iter()
            .map(|(probability, market, label)| (*probability, *market, label.winner_up)),
    )
}

pub(super) fn accuracy(rows: &[(Option<f64>, &str, &Label)]) -> OracleResearchAccuracy {
    let mut markets = BTreeMap::<&str, (u64, u64)>::new();
    for (probability, market, label) in rows {
        let Some(prediction) = probability
            .filter(|value| valid_probability(*value))
            .and_then(direction)
        else {
            continue;
        };
        let row = markets.entry(*market).or_default();
        row.0 += u64::from(prediction == label.winner_up);
        row.1 += 1;
    }
    OracleResearchAccuracy {
        observations: markets.values().map(|row| row.1).sum(),
        markets: markets.len() as u64,
        equal_market_weight_accuracy: mean(markets.values().map(|row| row.0 as f64 / row.1 as f64)),
    }
}

pub(super) fn final_output_probability_scores(
    rows: &[FinalOutputProbabilityRow<'_>],
) -> OracleResearchProbabilityScores {
    let mut result = OracleResearchProbabilityScores {
        scores: aggregate_scores(rows.iter().copied()),
        ..OracleResearchProbabilityScores::default()
    };
    for (probability, _, _) in rows {
        match probability {
            None => result.unavailable += 1,
            Some(value) if !valid_probability(*value) => result.invalid += 1,
            Some(value) if *value > 0.5 => result.direction_up += 1,
            Some(value) if *value < 0.5 => result.direction_down += 1,
            Some(_) => result.direction_ties += 1,
        }
    }
    result
}

pub(super) fn final_output_signal_scores(
    rows: &[FinalOutputSignalRow<'_>],
) -> OracleResearchSignalScores {
    let mut result = OracleResearchSignalScores {
        observations: rows.len() as u64,
        ..OracleResearchSignalScores::default()
    };
    let mut markets = BTreeMap::<&str, (u64, u64)>::new();
    for (field_present, side, market, winner_up) in rows {
        if !field_present {
            result.field_absent += 1;
            continue;
        }
        let prediction = match side {
            None => {
                result.abstained += 1;
                continue;
            }
            Some(side) if side.eq_ignore_ascii_case("up") => {
                result.direction_up += 1;
                true
            }
            Some(side) if side.eq_ignore_ascii_case("down") => {
                result.direction_down += 1;
                false
            }
            Some(_) => {
                result.unrecognized += 1;
                continue;
            }
        };
        let market = markets.entry(*market).or_default();
        market.0 += u64::from(prediction == *winner_up);
        market.1 += 1;
    }
    result.accuracy = OracleResearchAccuracy {
        observations: markets.values().map(|row| row.1).sum(),
        markets: markets.len() as u64,
        equal_market_weight_accuracy: mean(
            markets
                .values()
                .filter(|row| row.1 != 0)
                .map(|row| row.0 as f64 / row.1 as f64),
        ),
    };
    result
}

pub(super) fn record_uncertainty(
    observation: &Observation,
    availability: Option<&mut OracleResearchUncertaintyAvailability>,
) {
    let Some(availability) = availability else {
        return;
    };
    if !observation.learner_interval_fields_present
        || !observation.effective_market_interval_fields_present
    {
        availability.omitted_fields += 1;
        return;
    }
    match (
        interval_available(observation.learner_interval),
        interval_available(observation.effective_market_interval),
    ) {
        (Some(true), Some(true)) => availability.learner_and_market += 1,
        (Some(true), Some(false)) => availability.learner_only += 1,
        (Some(false), Some(true)) => availability.market_only += 1,
        (Some(false), Some(false)) => availability.neither += 1,
        _ => availability.invalid_fields += 1,
    }
}

pub(super) fn phase_trust_metrics(
    trace: &Trace,
    symbol: &str,
    conflicted_labels: &BTreeSet<(String, String)>,
    application_policy: &str,
) -> OracleResearchPhaseTrustDiagnostics {
    let mut rows = Vec::new();
    let mut source_observations = 0_u64;
    let mut label_unavailable_observations = 0_u64;
    let mut label_conflict_observations = 0_u64;
    for (key, observation) in trace
        .observations
        .iter()
        .filter(|(key, _)| key.symbol == symbol)
    {
        source_observations += 1;
        let label_key = (key.symbol.clone(), key.market.clone());
        let label = if conflicted_labels.contains(&label_key) {
            label_conflict_observations += 1;
            None
        } else if let Some(label) = trace.labels.get(&label_key) {
            Some(label)
        } else {
            label_unavailable_observations += 1;
            None
        };
        rows.push((key, observation, label));
    }
    let mut result = OracleResearchPhaseTrustDiagnostics {
        application_policy: application_policy.to_string(),
        population: "candidate_own_all_drives".to_string(),
        source_observations,
        observations: rows.len() as u64,
        label_unavailable_observations,
        label_conflict_observations,
        phases: PHASES
            .iter()
            .map(|phase| OracleResearchPhaseTrustMetrics {
                phase: (*phase).to_string(),
                ..OracleResearchPhaseTrustMetrics::default()
            })
            .collect(),
        ..OracleResearchPhaseTrustDiagnostics::default()
    };
    let mut incoming =
        std::array::from_fn::<Vec<(Option<f64>, &str, &Label)>, 5, _>(|_| Vec::new());
    let mut prospective =
        std::array::from_fn::<Vec<(Option<f64>, &str, &Label)>, 5, _>(|_| Vec::new());
    for (key, observation, label) in rows.iter().copied() {
        let expected_phase = label.and_then(|label| expected_phase_index(key, label));
        let eligible = observation.fit_available == Some(true)
            && observation.prediction_valid == Some(true)
            && observation.attempted
            && (label.is_none() || expected_phase.is_some());
        if !observation.application_policy_field_present {
            result.policy_field_absent += 1;
            result.eligible_policy_missing += u64::from(eligible);
        } else {
            match observation.application_policy.as_deref() {
                Some(policy) if policy == application_policy => result.policy_available += 1,
                Some(_) => result.policy_mismatch += 1,
                None => {
                    result.policy_missing += 1;
                    result.eligible_policy_missing += u64::from(eligible);
                }
            }
        }
        let phase_index = observation
            .phase
            .as_deref()
            .and_then(|phase| PHASES.iter().position(|expected| *expected == phase));
        if !observation.phase_field_present {
            result.phase_field_absent += 1;
        } else {
            match (&observation.phase, phase_index) {
                (Some(_), Some(_)) => result.phase_available += 1,
                (Some(_), None) => result.phase_unrecognized += 1,
                (None, _) => result.phase_missing += 1,
            }
        }
        count_probability(
            observation.phase_p0,
            observation.phase_p0_field_present,
            &mut result.incoming_valid,
            &mut result.incoming_missing,
            &mut result.incoming_invalid,
            &mut result.incoming_field_absent,
        );
        count_probability(
            observation.phase_p1,
            observation.phase_p1_field_present,
            &mut result.prospective_valid,
            &mut result.prospective_missing,
            &mut result.prospective_invalid,
            &mut result.prospective_field_absent,
        );
        if observation.phase_sample_count_field_present {
            result.support_count_available += u64::from(observation.phase_sample_count.is_some());
            result.support_count_missing += u64::from(observation.phase_sample_count.is_none());
        } else {
            result.support_count_field_absent += 1;
        }
        if observation.phase_trusted_field_present {
            result.trusted += u64::from(observation.phase_trusted == Some(true));
            result.untrusted += u64::from(observation.phase_trusted == Some(false));
            result.trust_missing += u64::from(observation.phase_trusted.is_none());
        } else {
            result.trust_field_absent += 1;
        }
        result.actual_applied += u64::from(observation.applied);
        result.actually_contributing += u64::from(observation.contributing);
        if !eligible {
            result.unavailable_observations += 1;
            result.ineligible_with_phase_facts += u64::from(
                observation.phase.is_some()
                    || observation.phase_sample_count.is_some()
                    || observation.phase_trusted.is_some()
                    || observation.phase_p0.is_some()
                    || observation.phase_p1.is_some(),
            );
            continue;
        }
        result.eligible_observations += 1;
        result.phase_identity_unverifiable += u64::from(label.is_none());
        result.phase_identity_mismatch +=
            u64::from(expected_phase.is_some_and(|expected| phase_index != Some(expected)));
        let Some(index) = phase_index else {
            continue;
        };
        if expected_phase.is_some_and(|expected| index != expected) {
            continue;
        }
        let phase = &mut result.phases[index];
        phase.observations += 1;
        phase.incoming_missing += u64::from(observation.phase_p0.is_none());
        phase.incoming_invalid += u64::from(
            observation
                .phase_p0
                .is_some_and(|value| !valid_probability(value)),
        );
        phase.prospective_missing += u64::from(observation.phase_p1.is_none());
        phase.prospective_invalid += u64::from(
            observation
                .phase_p1
                .is_some_and(|value| !valid_probability(value)),
        );
        phase.support_count_available += u64::from(observation.phase_sample_count.is_some());
        phase.support_count_missing += u64::from(observation.phase_sample_count.is_none());
        if let Some(count) = observation.phase_sample_count {
            phase.support_count_min =
                Some(phase.support_count_min.map_or(count, |old| old.min(count)));
            phase.support_count_max =
                Some(phase.support_count_max.map_or(count, |old| old.max(count)));
        }
        phase.trusted += u64::from(observation.phase_trusted == Some(true));
        phase.untrusted += u64::from(observation.phase_trusted == Some(false));
        phase.trust_missing += u64::from(observation.phase_trusted.is_none());
        phase.actual_applied += u64::from(observation.applied);
        phase.actually_contributing += u64::from(observation.contributing);
        if observation.phase_p0.is_some_and(valid_probability)
            && observation.phase_p1.is_some_and(valid_probability)
        {
            phase.valid_pairs += 1;
            if let Some(label) = label {
                phase.scored_pairs += 1;
                incoming[index].push((observation.phase_p0, key.market.as_str(), label));
                prospective[index].push((observation.phase_p1, key.market.as_str(), label));
                stage(
                    observation.phase_p0,
                    observation.phase_p1,
                    label.winner_up,
                    &mut phase.prospective_corrected,
                    &mut phase.prospective_damaged,
                );
            }
        }
    }
    for (index, phase) in result.phases.iter_mut().enumerate() {
        phase.incoming = probability_scores(&incoming[index]);
        phase.prospective = probability_scores(&prospective[index]);
    }
    result
}

pub(in crate::strategy_runtime::backtest_matrix_report) fn phase_trust_incomplete(
    row: &OracleResearchSymbolMetrics,
) -> bool {
    let Some(diagnostics) = &row.phase_trust else {
        return true;
    };
    diagnostics.population != "candidate_own_all_drives"
        || diagnostics.observations != diagnostics.source_observations
        || diagnostics.label_unavailable_observations + diagnostics.label_conflict_observations
            > diagnostics.observations
        || diagnostics.eligible_observations + diagnostics.unavailable_observations
            != diagnostics.observations
        || diagnostics.ineligible_with_phase_facts != 0
        || diagnostics.policy_available + diagnostics.policy_missing != diagnostics.observations
        || diagnostics.policy_mismatch != 0
        || diagnostics.policy_field_absent != 0
        || diagnostics.eligible_policy_missing != 0
        || diagnostics.phase_available != diagnostics.eligible_observations
        || diagnostics.phase_missing != diagnostics.unavailable_observations
        || diagnostics.phase_unrecognized != 0
        || diagnostics.phase_field_absent != 0
        || diagnostics.phase_identity_mismatch != 0
        || diagnostics.phase_identity_unverifiable > diagnostics.eligible_observations
        || diagnostics.phase_identity_unverifiable
            > diagnostics.label_unavailable_observations + diagnostics.label_conflict_observations
        || diagnostics.incoming_valid != diagnostics.eligible_observations
        || diagnostics.incoming_missing != diagnostics.unavailable_observations
        || diagnostics.incoming_invalid != 0
        || diagnostics.incoming_field_absent != 0
        || diagnostics.prospective_valid != diagnostics.eligible_observations
        || diagnostics.prospective_missing != diagnostics.unavailable_observations
        || diagnostics.prospective_invalid != 0
        || diagnostics.prospective_field_absent != 0
        || diagnostics.support_count_available != diagnostics.eligible_observations
        || diagnostics.support_count_missing != diagnostics.unavailable_observations
        || diagnostics.support_count_field_absent != 0
        || diagnostics.trusted + diagnostics.untrusted != diagnostics.eligible_observations
        || diagnostics.trust_missing != diagnostics.unavailable_observations
        || diagnostics.trust_field_absent != 0
        || diagnostics.phases.len() != PHASES.len()
        || diagnostics
            .phases
            .iter()
            .zip(PHASES)
            .any(|(phase, expected)| {
                phase.phase != expected
                    || phase.valid_pairs != phase.observations
                    || phase.scored_pairs > phase.valid_pairs
                    || phase.incoming.observations != phase.scored_pairs
                    || phase.prospective.observations != phase.scored_pairs
                    || phase.incoming_missing != 0
                    || phase.incoming_invalid != 0
                    || phase.prospective_missing != 0
                    || phase.prospective_invalid != 0
                    || phase.support_count_available != phase.observations
                    || phase.support_count_missing != 0
                    || phase.trusted + phase.untrusted != phase.observations
                    || phase.trust_missing != 0
            })
}

fn expected_phase_index(key: &Key, label: &Label) -> Option<usize> {
    final_output_phase_index(label.start_ms, label.end_ms, key.eval_ms)
}

fn count_probability(
    probability: Option<f64>,
    field_present: bool,
    valid: &mut u64,
    missing: &mut u64,
    invalid: &mut u64,
    field_absent: &mut u64,
) {
    if !field_present {
        *field_absent += 1;
        return;
    }
    match probability {
        Some(value) if valid_probability(value) => *valid += 1,
        Some(_) => *invalid += 1,
        None => *missing += 1,
    }
}

pub(super) fn stage(
    before: Option<f64>,
    after: Option<f64>,
    winner_up: bool,
    corrected: &mut u64,
    damaged: &mut u64,
) {
    let (Some(before), Some(after)) = (before.and_then(direction), after.and_then(direction))
    else {
        return;
    };
    *corrected += u64::from(before != winner_up && after == winner_up);
    *damaged += u64::from(before == winner_up && after != winner_up);
}

fn interval_available(interval: (Option<f64>, Option<f64>)) -> Option<bool> {
    match interval {
        (None, None) => Some(false),
        (Some(lo), Some(hi)) if valid_probability(lo) && valid_probability(hi) && lo <= hi => {
            Some(true)
        }
        _ => None,
    }
}

fn aggregate_scores<'a>(
    rows: impl Iterator<Item = (Option<f64>, &'a str, bool)>,
) -> OracleResearchModelScores {
    let scores = aggregate_probability_scores(rows);
    OracleResearchModelScores {
        observations: scores.observations,
        markets: scores.markets,
        brier: scores.brier,
        log_loss: scores.log_loss,
        direction_accuracy: scores.direction_accuracy,
        direction_observations: Some(scores.direction_observations),
        direction_ties: Some(scores.direction_ties),
    }
}

fn mean(values: impl Iterator<Item = f64>) -> Option<f64> {
    let (sum, count) = values.fold((0.0, 0_u64), |(sum, count), value| (sum + value, count + 1));
    (count != 0).then(|| sum / count as f64)
}

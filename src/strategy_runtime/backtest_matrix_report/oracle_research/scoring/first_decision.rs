use std::collections::{BTreeMap, BTreeSet};

use super::{final_output_signal_scores, probability_scores, valid_probability};
use crate::strategy_runtime::backtest_matrix_report::oracle_research::trace::{
    First, FirstDecisionResolved, FirstDecisionRetained, Key, Observation, Trace,
};
use crate::strategy_runtime::backtest_matrix_report::oracle_research::types::{
    OracleResearchAccuracy, OracleResearchFirstDecisionDiagnostics, OracleResearchModelScores,
    OracleResearchSymbolMetrics,
};

pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) struct FirstEvidence {
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) scores:
        OracleResearchModelScores,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) accuracy:
        OracleResearchAccuracy,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) authorizations: u64,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) attributed: u64,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) missing_snapshot: u64,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) label_available:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) label_unavailable:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) label_conflict:
        Option<u64>,
}

pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fn first_evidence(
    trace: &Trace,
    symbol: &str,
    conflicted_labels: &BTreeSet<(String, String)>,
    emit_label_evidence: bool,
) -> FirstEvidence {
    let events = trace
        .first_authorizations
        .iter()
        .filter(|((event_symbol, _, _, _, _), _)| event_symbol == symbol)
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut direction_rows = Vec::new();
    let mut attributed = BTreeSet::new();
    let mut missing_snapshot = 0_u64;
    let mut label_available = 0_u64;
    let mut label_unavailable = 0_u64;
    let mut label_conflict = 0_u64;
    for ((_, market, _, _, _), authorization) in &events {
        let Some(direction) = joined_direction(trace, symbol, market, authorization) else {
            missing_snapshot += 1;
            continue;
        };
        if !attributed.insert(market.as_str()) {
            continue;
        }
        let label_key = (symbol.to_string(), market.to_string());
        if conflicted_labels.contains(&label_key) {
            label_conflict += 1;
        } else if let Some(label) = trace.labels.get(&label_key) {
            label_available += 1;
            rows.push((direction.actual_p, market.as_str(), label));
            let winner =
                direction
                    .selected_side
                    .as_deref()
                    .map(|side| if side == "up" { "down" } else { "up" });
            direction_rows.push((true, winner, market.as_str(), label.winner_up));
        } else {
            label_unavailable += 1;
        }
    }
    let accuracy = final_output_signal_scores(&direction_rows).accuracy;
    FirstEvidence {
        scores: OracleResearchModelScores {
            direction_accuracy: accuracy.equal_market_weight_accuracy,
            direction_observations: Some(accuracy.observations),
            ..probability_scores(&rows)
        },
        accuracy,
        authorizations: events.len() as u64,
        attributed: attributed.len() as u64,
        missing_snapshot,
        label_available: emit_label_evidence.then_some(label_available),
        label_unavailable: emit_label_evidence.then_some(label_unavailable),
        label_conflict: emit_label_evidence.then_some(label_conflict),
    }
}

pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fn first_decision_metrics(
    trace: &Trace,
    symbol: &str,
    application_policy: &str,
) -> OracleResearchFirstDecisionDiagnostics {
    let retained = trace
        .first_decision_retained
        .iter()
        .filter(|row| row.symbol == symbol)
        .collect::<Vec<_>>();
    let resolved = trace
        .first_decision_resolved
        .iter()
        .filter(|row| row.symbol == symbol)
        .collect::<Vec<_>>();
    let mut retained_by_market = BTreeMap::<&str, Vec<&FirstDecisionRetained>>::new();
    for row in &retained {
        retained_by_market
            .entry(row.market.as_str())
            .or_default()
            .push(row);
    }
    let mut resolved_by_market = BTreeMap::<&str, Vec<&FirstDecisionResolved>>::new();
    for row in &resolved {
        resolved_by_market
            .entry(row.market.as_str())
            .or_default()
            .push(row);
    }
    let original_first_markers = trace
        .first_directions
        .keys()
        .filter(|(event_symbol, _)| event_symbol == symbol)
        .count() as u64;
    let repeated_authorization_attempts = trace
        .first_authorizations
        .iter()
        .filter(|((event_symbol, _, _, _, _), row)| {
            event_symbol == symbol && row.latched == Some(true) && row.fresh == Some(false)
        })
        .count() as u64;
    let expected_resolution_events = retained
        .iter()
        .filter(|row| {
            trace
                .labels
                .contains_key(&(row.symbol.clone(), row.market.clone()))
        })
        .count() as u64;
    let mut result = OracleResearchFirstDecisionDiagnostics {
        application_policy: application_policy.to_string(),
        original_first_markers,
        retention_events: retained.len() as u64,
        retained_windows: retained_by_market.len() as u64,
        repeated_authorization_attempts,
        expected_resolution_events,
        resolution_events: resolved.len() as u64,
        resolved_windows: resolved_by_market.len() as u64,
        ..OracleResearchFirstDecisionDiagnostics::default()
    };
    for row in &retained {
        result.marker_retained += u64::from(row.marker_retained);
        result.retention_sample_available += u64::from(row.sample_available);
        result.retention_sample_unavailable += u64::from(!row.sample_available);
        result.retention_field_absent += u64::from(!row.optional_fields_present);
        match row.unavailable_reason.as_deref() {
            None => {}
            Some("missing_snapshot_timestamp") => {
                result.unavailable_missing_snapshot_timestamp += 1
            }
            Some("exact_observation_unavailable") => result.unavailable_exact_observation += 1,
            Some("prospective_contribution_unavailable") => {
                result.unavailable_prospective_contribution += 1
            }
            Some("pending_capacity") => result.unavailable_pending_capacity += 1,
            Some(_) => result.unavailable_reason_unrecognized += 1,
        }
        result.retention_identity_failures += u64::from(!retention_identity_valid(trace, row));
        let paired = resolved_by_market
            .get(row.market.as_str())
            .is_some_and(|rows| {
                rows.iter()
                    .any(|resolved| retained_resolution_match(row, resolved))
            });
        result.retained_without_resolution += u64::from(!paired);
    }
    for row in &resolved {
        result.resolution_marker_available += u64::from(row.marker_available);
        result.resolution_sample_available += u64::from(row.sample_available);
        result.resolution_sample_unavailable += u64::from(!row.sample_available);
        result.resolution_field_absent += u64::from(!row.optional_fields_present);
        result.samples_merged += u64::from(row.sample_merged);
        result.first_fit_rows_added += u64::from(row.first_fit_row_added);
        update_range(
            row.first_support_count,
            &mut result.first_support_count_min,
            &mut result.first_support_count_max,
        );
        update_range(
            row.fit_sample_count,
            &mut result.fit_sample_count_min,
            &mut result.fit_sample_count_max,
        );
        let pair = retained_by_market
            .get(row.market.as_str())
            .filter(|rows| rows.len() == 1)
            .and_then(|rows| rows.first())
            .is_some_and(|retained| retained_resolution_match(retained, row));
        result.retained_resolution_pairs += u64::from(pair);
        result.retained_resolution_mismatches += u64::from(!pair);
        result.fitting_identity_failures += u64::from(!fitting_identity_valid(row));
        result.resolution_chronology_failures +=
            u64::from(!resolution_chronology_valid(trace, row));
    }
    for (key, observation) in trace
        .observations
        .iter()
        .filter(|(key, _)| key.symbol == symbol)
    {
        result.evaluation_observations += 1;
        let eligible = observation.phase_p0.is_some_and(valid_probability)
            && observation.phase_p1.is_some_and(valid_probability);
        result.eligible_trust_observations += u64::from(eligible);
        result.trust_field_absent += u64::from(
            !observation.trust_source_field_present
                || !observation.trust_sample_count_field_present
                || !observation.trust_trusted_field_present
                || !observation.first_sample_count_field_present,
        );
        result.trust_sample_count_available += u64::from(observation.trust_sample_count.is_some());
        result.trust_sample_count_missing += u64::from(observation.trust_sample_count.is_none());
        result.first_sample_count_available += u64::from(observation.first_sample_count.is_some());
        result.first_sample_count_missing += u64::from(observation.first_sample_count.is_none());
        result.evaluation_fit_sample_count_available +=
            u64::from(observation.fit_sample_count.is_some());
        result.evaluation_fit_sample_count_missing +=
            u64::from(observation.fit_sample_count.is_none());
        match observation.trust_source.as_deref() {
            Some("first") => result.trust_source_first += 1,
            Some("phase") => result.trust_source_phase += 1,
            None => result.trust_source_missing += 1,
            Some(_) => result.trust_source_unrecognized += 1,
        }
        result.trust_identity_failures += u64::from(!trust_identity_valid(
            retained_by_market
                .get(key.market.as_str())
                .filter(|rows| rows.len() == 1)
                .and_then(|rows| rows.first())
                .filter(|row| row.marker_retained)
                .and_then(|_| {
                    trace
                        .first_directions
                        .get(&(key.symbol.clone(), key.market.clone()))
                }),
            key,
            observation,
            eligible,
        ));
    }
    result
}

pub(in crate::strategy_runtime::backtest_matrix_report) fn first_decision_incomplete(
    row: &OracleResearchSymbolMetrics,
) -> bool {
    let Some(diagnostics) = &row.first_decision else {
        return true;
    };
    diagnostics.application_policy != "market_offset_first_decision_trust"
        || diagnostics.original_first_markers == 0
        || diagnostics.retention_events != diagnostics.original_first_markers
        || diagnostics.retained_windows != diagnostics.retention_events
        || diagnostics.marker_retained != diagnostics.retention_events
        || diagnostics.retention_sample_available + diagnostics.retention_sample_unavailable
            != diagnostics.retention_events
        || diagnostics.unavailable_missing_snapshot_timestamp
            + diagnostics.unavailable_exact_observation
            + diagnostics.unavailable_prospective_contribution
            + diagnostics.unavailable_pending_capacity
            + diagnostics.unavailable_reason_unrecognized
            != diagnostics.retention_sample_unavailable
        || diagnostics.unavailable_reason_unrecognized != 0
        || diagnostics.retention_field_absent != 0
        || diagnostics.retention_identity_failures != 0
        || diagnostics.resolution_events != diagnostics.expected_resolution_events
        || diagnostics.resolved_windows != diagnostics.resolution_events
        || diagnostics.resolution_marker_available != diagnostics.resolution_events
        || diagnostics.resolution_sample_available + diagnostics.resolution_sample_unavailable
            != diagnostics.resolution_events
        || diagnostics.resolution_field_absent != 0
        || diagnostics.retained_resolution_pairs != diagnostics.resolution_events
        || diagnostics.retained_resolution_mismatches != 0
        || diagnostics.retained_resolution_pairs + diagnostics.retained_without_resolution
            != diagnostics.retention_events
        || diagnostics.samples_merged + diagnostics.first_fit_rows_added
            != diagnostics.resolution_sample_available
        || diagnostics.fitting_identity_failures != 0
        || diagnostics.resolution_chronology_failures != 0
        || (diagnostics.resolution_events != 0
            && (diagnostics.first_support_count_min.is_none()
                || diagnostics.first_support_count_max.is_none()
                || diagnostics.fit_sample_count_min.is_none()
                || diagnostics.fit_sample_count_max.is_none()))
        || diagnostics.trust_source_first
            + diagnostics.trust_source_phase
            + diagnostics.trust_source_missing
            + diagnostics.trust_source_unrecognized
            != diagnostics.evaluation_observations
        || diagnostics.trust_source_unrecognized != 0
        || diagnostics.trust_field_absent != 0
        || diagnostics.trust_identity_failures != 0
        || diagnostics.trust_sample_count_available + diagnostics.trust_sample_count_missing
            != diagnostics.evaluation_observations
        || diagnostics.first_sample_count_available + diagnostics.first_sample_count_missing
            != diagnostics.evaluation_observations
        || diagnostics.evaluation_fit_sample_count_available
            + diagnostics.evaluation_fit_sample_count_missing
            != diagnostics.evaluation_observations
        || diagnostics.eligible_trust_observations
            > diagnostics.trust_source_first + diagnostics.trust_source_phase
}

pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fn first_join_conflicts(
    trace: &Trace,
) -> u64 {
    trace
        .first_authorizations
        .iter()
        .filter(|((symbol, market, _, _, _), _)| {
            trace
                .first_directions
                .get(&(symbol.clone(), market.clone()))
                .is_some_and(|direction| matching_forecasts(trace, symbol, market, direction) > 1)
        })
        .count() as u64
}

pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fn latched_missing(
    trace: &Trace,
) -> u64 {
    trace
        .first_authorizations
        .iter()
        .filter(|((symbol, market, _, _, _), authorization)| {
            authorization.latched == Some(true)
                && joined_direction(trace, symbol, market, authorization).is_none()
        })
        .count() as u64
}

fn joined_direction<'a>(
    trace: &'a Trace,
    symbol: &str,
    market: &str,
    authorization: &First,
) -> Option<&'a First> {
    let direction = trace
        .first_directions
        .get(&(symbol.to_string(), market.to_string()))?;
    let fields_valid = match (authorization.latched, authorization.fresh) {
        (Some(false), Some(true)) => {
            authorization.selected_side == direction.selected_side
                && authorization.snapshot_ms == direction.snapshot_ms
                && authorization.actual_p == direction.actual_p
        }
        (Some(true), Some(false)) => {
            authorization.selected_side == direction.selected_side
                && authorization.snapshot_ms.is_none()
                && authorization.actual_p.is_none()
        }
        _ => false,
    };
    let side_valid = direction.selected_side.as_deref().is_some_and(valid_side)
        && direction.causal_side.as_deref().is_none_or(valid_side);
    let probability_valid = direction.actual_p.is_some_and(valid_probability);
    let chronology_valid = causally_precedes(
        direction.sequence,
        direction.materialization,
        direction.event_ms,
        authorization.sequence,
        authorization.materialization,
        authorization.event_ms,
    );
    let unique_forecast = matching_forecasts(trace, symbol, market, direction) == 1;
    (fields_valid && side_valid && probability_valid && chronology_valid && unique_forecast)
        .then_some(direction)
}

fn matching_forecasts(trace: &Trace, symbol: &str, market: &str, direction: &First) -> usize {
    trace
        .observations
        .iter()
        .filter(|(key, observation)| forecast_matches(key, observation, symbol, market, direction))
        .take(2)
        .count()
}

fn matching_forecast<'a>(
    trace: &'a Trace,
    symbol: &str,
    market: &str,
    direction: &First,
) -> Option<&'a Observation> {
    let mut matches = trace
        .observations
        .iter()
        .filter(|(key, observation)| forecast_matches(key, observation, symbol, market, direction));
    let (_, observation) = matches.next()?;
    matches.next().is_none().then_some(observation)
}

fn forecast_matches(
    key: &Key,
    observation: &Observation,
    symbol: &str,
    market: &str,
    direction: &First,
) -> bool {
    key.symbol == symbol
        && key.market == market
        && observation.snapshot_ms == direction.snapshot_ms
        && same_probability(observation.final_p, direction.actual_p)
        && direction.snapshot_ms.is_some()
        && direction.actual_p.is_some()
        && causally_precedes(
            key.sequence,
            key.materialization,
            key.eval_ms,
            direction.sequence,
            direction.materialization,
            direction.event_ms,
        )
}

fn retention_identity_valid(trace: &Trace, row: &FirstDecisionRetained) -> bool {
    let marker = trace
        .first_directions
        .get(&(row.symbol.clone(), row.market.clone()));
    let marker_valid = marker.is_some_and(|marker| {
        marker.event_ms == row.authorization_ms
            && marker.snapshot_ms == row.snapshot_ms
            && marker.selected_side.as_deref().is_some_and(valid_side)
            && marker.causal_side.as_deref().is_none_or(valid_side)
    });
    let matching_observation =
        marker.and_then(|marker| matching_forecast(trace, &row.symbol, &row.market, marker));
    let chronology_valid = row.window_start_ms < row.window_end_ms
        && row.authorization_ms >= row.window_start_ms
        && row.authorization_ms < row.window_end_ms
        && row.snapshot_ms.is_none_or(|timestamp| {
            timestamp >= row.window_start_ms && timestamp <= row.authorization_ms
        });
    let available_fields_valid = row.sample_available
        && row.marker_retained
        && row.unavailable_reason.is_none()
        && row.observation_ms.is_some_and(|timestamp| {
            timestamp > 0 && timestamp < row.window_end_ms && timestamp <= row.authorization_ms
        })
        && row.sample_decision_ms == row.snapshot_ms
        && row.candidate_p.is_some_and(valid_probability)
        && row.prior_p.is_some_and(valid_probability)
        && row.first_p0.is_some_and(valid_probability)
        && row.first_p1.is_some_and(valid_probability)
        && matching_observation.is_some_and(|observation| {
            same_probability(row.candidate_p, observation.learner_p)
                && same_probability(row.prior_p, observation.learner_prior_p)
                && same_probability(row.first_p0, observation.phase_p0)
                && same_probability(row.first_p1, observation.phase_p1)
        });
    let unavailable_fields_empty = !row.sample_available
        && row.observation_ms.is_none()
        && row.sample_decision_ms.is_none()
        && row.candidate_p.is_none()
        && row.prior_p.is_none()
        && row.first_p0.is_none()
        && row.first_p1.is_none();
    let unavailable_identity_valid = unavailable_fields_empty
        && match row.unavailable_reason.as_deref() {
            Some("missing_snapshot_timestamp") => row.marker_retained && row.snapshot_ms.is_none(),
            Some("exact_observation_unavailable")
            | Some("prospective_contribution_unavailable") => {
                row.marker_retained && row.snapshot_ms.is_some()
            }
            Some("pending_capacity") => !row.marker_retained,
            _ => false,
        };
    marker_valid && chronology_valid && (available_fields_valid || unavailable_identity_valid)
}

fn retained_resolution_match(
    retained: &FirstDecisionRetained,
    resolved: &FirstDecisionResolved,
) -> bool {
    retained.marker_retained
        && retained.market == resolved.market
        && retained.symbol == resolved.symbol
        && retained.authorization_ms == resolved.authorization_ms.unwrap_or_default()
        && retained.snapshot_ms == resolved.snapshot_ms
        && retained.window_start_ms == resolved.window_start_ms
        && retained.window_end_ms == resolved.window_end_ms
        && retained.sample_available == resolved.sample_available
        && retained.first_p0 == resolved.first_p0
        && retained.first_p1 == resolved.first_p1
}

fn fitting_identity_valid(row: &FirstDecisionResolved) -> bool {
    let sample_identity = if row.sample_available {
        row.first_p0.is_some_and(valid_probability)
            && row.first_p1.is_some_and(valid_probability)
            && row.sample_merged != row.first_fit_row_added
    } else {
        row.first_p0.is_none()
            && row.first_p1.is_none()
            && !row.sample_merged
            && !row.first_fit_row_added
    };
    let expected_rows = row
        .ordinary_fit_row_count
        .saturating_add(u64::from(row.first_fit_row_added));
    sample_identity
        && row.resolved_fit_row_count == expected_rows
        && (row.fit_sample_count > 0 || row.resolved_fit_row_count == 0)
        && row.first_support_count <= row.fit_sample_count
}

fn resolution_chronology_valid(trace: &Trace, row: &FirstDecisionResolved) -> bool {
    let label_key = (row.symbol.clone(), row.market.clone());
    let label_valid = trace.labels.get(&label_key).is_some_and(|label| {
        (label.start_ms, label.end_ms) == (row.window_start_ms, row.window_end_ms)
    });
    row.marker_available
        && label_valid
        && row.window_start_ms < row.window_end_ms
        && row.window_end_ms <= row.label_release_ms
        && row.label_release_ms <= row.learning_applied_ms
        && row.authorization_ms.is_some_and(|timestamp| {
            timestamp >= row.window_start_ms && timestamp < row.window_end_ms
        })
        && row.snapshot_ms.is_none_or(|snapshot| {
            row.authorization_ms
                .is_some_and(|authorization| snapshot <= authorization)
        })
}

fn trust_identity_valid(
    marker: Option<&First>,
    key: &Key,
    observation: &Observation,
    eligible: bool,
) -> bool {
    let recognized_source = matches!(observation.trust_source.as_deref(), Some("first" | "phase"));
    let expected_source = marker.map_or("first", |marker| {
        if causally_precedes(
            key.sequence,
            key.materialization,
            key.eval_ms,
            marker.sequence,
            marker.materialization,
            marker.event_ms,
        ) {
            "first"
        } else {
            "phase"
        }
    });
    let source_identity = match observation.trust_source.as_deref() {
        Some("first") => {
            expected_source == "first"
                && observation.trust_sample_count == observation.first_sample_count
                && observation.trust_sample_count.is_some()
                && observation.trust_trusted.is_some()
        }
        Some("phase") => {
            expected_source == "phase"
                && observation.trust_sample_count.is_some()
                && observation.trust_trusted.is_some()
        }
        None => {
            observation.trust_sample_count.is_none()
                && observation.trust_trusted.is_none()
                && observation.first_sample_count.is_none()
        }
        Some(_) => false,
    };
    let application_identity = if eligible {
        observation.phase.is_some()
            && observation.phase_sample_count.is_some()
            && observation.phase_trusted.is_some()
            && observation.phase_p0.is_some_and(valid_probability)
            && observation.phase_p1.is_some_and(valid_probability)
            && (observation.trust_source.as_deref() != Some("phase")
                || (observation.trust_sample_count == observation.phase_sample_count
                    && observation.trust_trusted == observation.phase_trusted))
    } else {
        observation.phase.is_none()
            && observation.phase_sample_count.is_none()
            && observation.phase_trusted.is_none()
            && observation.phase_p0.is_none()
            && observation.phase_p1.is_none()
    };
    source_identity
        && application_identity
        && (!eligible || recognized_source)
        && observation.applied == (eligible && observation.trust_trusted == Some(true))
        && (!observation.contributing || observation.applied)
}

fn causally_precedes(
    earlier_sequence: u64,
    earlier_materialization: bool,
    earlier_ms: u64,
    later_sequence: u64,
    later_materialization: bool,
    later_ms: u64,
) -> bool {
    earlier_ms <= later_ms
        && (earlier_sequence < later_sequence
            || (earlier_sequence == later_sequence
                && (!earlier_materialization || later_materialization)))
}

fn update_range(value: u64, min: &mut Option<u64>, max: &mut Option<u64>) {
    *min = Some(min.map_or(value, |old| old.min(value)));
    *max = Some(max.map_or(value, |old| old.max(value)));
}

fn valid_side(side: &str) -> bool {
    matches!(side, "up" | "down")
}

fn same_probability(left: Option<f64>, right: Option<f64>) -> bool {
    left.zip(right)
        .is_some_and(|(left, right)| left.to_bits() == right.to_bits())
}

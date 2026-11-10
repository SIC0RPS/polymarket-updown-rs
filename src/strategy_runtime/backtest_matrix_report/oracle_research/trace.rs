use std::collections::{btree_map::Entry, BTreeMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

use super::types::OracleResearchDirectMarketCoverage;
use super::{
    STAGE_BASE_BLEND, STAGE_FINAL, STAGE_LEARNER, STAGE_MARKET_PRIOR, STAGE_MARKET_RAW,
    STAGE_ORIGINAL_BASE, STAGE_POST_LOGISTIC,
};
use crate::strategy_runtime::backtest_matrix::oracle_research::{
    BacktestOracleResearchVariant, OracleResearchRun,
};

mod observation;
pub(super) use observation::Observation;
use observation::{
    bool_field, field, option_f64, option_string, option_u64, parse_u64, string, u64_field,
};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Label {
    pub(super) winner_up: bool,
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
}

#[derive(Debug, PartialEq)]
pub(super) struct First {
    pub(super) event_ms: u64,
    pub(super) sequence: u64,
    pub(super) materialization: bool,
    pub(super) selected_side: Option<String>,
    pub(super) causal_side: Option<String>,
    pub(super) snapshot_ms: Option<u64>,
    pub(super) actual_p: Option<f64>,
    pub(super) latched: Option<bool>,
    pub(super) fresh: Option<bool>,
}

#[derive(Debug)]
pub(super) struct FirstDecisionRetained {
    pub(super) symbol: String,
    pub(super) market: String,
    pub(super) authorization_ms: u64,
    pub(super) window_start_ms: u64,
    pub(super) window_end_ms: u64,
    pub(super) snapshot_ms: Option<u64>,
    pub(super) marker_retained: bool,
    pub(super) sample_available: bool,
    pub(super) unavailable_reason: Option<String>,
    pub(super) observation_ms: Option<u64>,
    pub(super) sample_decision_ms: Option<u64>,
    pub(super) candidate_p: Option<f64>,
    pub(super) prior_p: Option<f64>,
    pub(super) first_p0: Option<f64>,
    pub(super) first_p1: Option<f64>,
    pub(super) optional_fields_present: bool,
}

#[derive(Debug)]
pub(super) struct FirstDecisionResolved {
    pub(super) symbol: String,
    pub(super) market: String,
    pub(super) window_start_ms: u64,
    pub(super) window_end_ms: u64,
    pub(super) label_release_ms: u64,
    pub(super) learning_applied_ms: u64,
    pub(super) marker_available: bool,
    pub(super) authorization_ms: Option<u64>,
    pub(super) snapshot_ms: Option<u64>,
    pub(super) sample_available: bool,
    pub(super) sample_merged: bool,
    pub(super) first_fit_row_added: bool,
    pub(super) ordinary_fit_row_count: u64,
    pub(super) resolved_fit_row_count: u64,
    pub(super) first_p0: Option<f64>,
    pub(super) first_p1: Option<f64>,
    pub(super) first_support_count: u64,
    pub(super) fit_sample_count: u64,
    pub(super) optional_fields_present: bool,
}

#[derive(Debug, Default)]
pub(super) struct Trace {
    pub(super) runtime_log_bytes: u64,
    pub(super) parsed_lines: u64,
    pub(super) malformed_lines: u64,
    pub(super) starts: u64,
    pub(super) ends: u64,
    pub(super) timings: u64,
    pub(super) probability_events: u64,
    pub(super) unavailable: u64,
    pub(super) missing_scope: u64,
    pub(super) duplicate_conflicts: u64,
    pub(super) identity_failures: u64,
    pub(super) observations: BTreeMap<Key, Observation>,
    pub(super) labels: BTreeMap<(String, String), Label>,
    pub(super) first_directions: BTreeMap<(String, String), First>,
    pub(super) first_authorizations: BTreeMap<(String, String, u64, u64, bool), First>,
    pub(super) first_decision_retained: Vec<FirstDecisionRetained>,
    pub(super) first_decision_resolved: Vec<FirstDecisionResolved>,
    pub(super) direct_market_trace: bool,
    pub(super) direct_market: OracleResearchDirectMarketCoverage,
    pub(super) direct_market_by_symbol: BTreeMap<String, OracleResearchDirectMarketCoverage>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    pub(super) symbol: String,
    pub(super) market: String,
    pub(super) eval_ms: u64,
    pub(super) materialization: bool,
    pub(super) sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FinalOutputTimestampKey<'a> {
    pub(super) market: &'a str,
    pub(super) decision_ms: u64,
    pub(super) observation_ms: Option<u64>,
}

impl Key {
    pub(super) fn final_output_timestamp_key<'a>(
        &'a self,
        observation: &Observation,
    ) -> FinalOutputTimestampKey<'a> {
        FinalOutputTimestampKey {
            market: self.market.as_str(),
            decision_ms: self.eval_ms,
            observation_ms: observation.observation_ms,
        }
    }
}

pub(super) fn parse(
    path: &Path,
    run: &OracleResearchRun,
    expected_variant: BacktestOracleResearchVariant,
    timing_sha: &str,
) -> Result<Trace> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut trace = Trace {
        runtime_log_bytes: fs::metadata(path)
            .with_context(|| format!("inspect {}", path.display()))?
            .len(),
        ..Trace::default()
    };
    for line in BufReader::new(file).lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => {
                trace.malformed_lines += 1;
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        trace.parsed_lines += 1;
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                trace.malformed_lines += 1;
                continue;
            }
        };
        match field(&value, "event").and_then(Value::as_str) {
            Some("oracle_research_run_start") => {
                trace.starts += 1;
                let valid = string(&value, "variant").as_deref() == Some(expected_variant.as_str())
                    && string(&value, "capture").as_deref() == Some(run.capture_name())
                    && string(&value, "capture_run_id").as_deref() == Some(run.capture_run_id())
                    && string(&value, "common_input_identity_sha256").as_deref()
                        == Some(run.common_input_identity_sha256());
                trace.identity_failures += u64::from(!valid);
            }
            Some("oracle_research_run_end") => {
                trace.ends += 1;
                let valid = string(&value, "variant").as_deref() == Some(expected_variant.as_str())
                    && string(&value, "capture").as_deref() == Some(run.capture_name())
                    && string(&value, "common_input_identity_sha256").as_deref()
                        == Some(run.common_input_identity_sha256());
                trace.identity_failures += u64::from(!valid);
            }
            Some("oracle_research_execution_timing") => {
                trace.timings += 1;
                trace.identity_failures += u64::from(
                    string(&value, "execution_timing_sha256").as_deref() != Some(timing_sha),
                );
            }
            Some("oracle_probability_evaluation") => {
                probability(&value, &mut trace, expected_variant)
            }
            Some("oracle_resolved_label") => label(&value, &mut trace),
            Some("oracle_first_direction_selected") => first(&value, &mut trace, true),
            Some("oracle_first_authorization") => first(&value, &mut trace, false),
            Some("oracle_first_decision_sample_retained") => {
                first_decision_retained(&value, &mut trace)
            }
            Some("oracle_first_decision_sample_resolved") => {
                first_decision_resolved(&value, &mut trace)
            }
            _ => {}
        }
    }
    if expected_variant == BacktestOracleResearchVariant::DirectMarket {
        trace.direct_market_trace = true;
        validate_direct_market(&mut trace);
    }
    Ok(trace)
}

fn forecast_scope(value: &Value, trace: &mut Trace) -> Option<(String, u64, u64, bool)> {
    let (Some(materialization), Some(symbol), Some(sequence), Some(eval_ms)) = (
        scope_bool(value, "materialization_drive"),
        scope_string(value, "symbol"),
        scope_u64(value, "global_sequence"),
        scope_u64(value, "decision_eval_ts_ms"),
    ) else {
        trace.missing_scope += 1;
        return None;
    };
    let span = value.get("span");
    if span.and_then(|span| span.get("candidate_slot")).is_none()
        || span.and_then(|span| span.get("candidate_sha256")).is_none()
    {
        trace.missing_scope += 1;
        return None;
    }
    Some((
        symbol.to_ascii_uppercase(),
        sequence,
        eval_ms,
        materialization,
    ))
}

fn probability(value: &Value, trace: &mut Trace, expected_variant: BacktestOracleResearchVariant) {
    let Some((symbol, sequence, scoped_eval_ms, materialization)) = forecast_scope(value, trace)
    else {
        return;
    };
    let (Some(market), Some(eval_ms)) = (
        string(value, "market_id"),
        u64_field(value, "decision_eval_ts_ms"),
    ) else {
        trace.missing_scope += 1;
        return;
    };
    trace.identity_failures += u64::from(
        eval_ms != scoped_eval_ms
            || string(value, "logistic_feature_layout").as_deref()
                != Some(expected_variant.feature_layout()),
    );
    trace.probability_events += 1;
    trace.unavailable += u64::from(!bool_field(value, "probability_available").unwrap_or(false));
    let observation = observation::parse(value);
    let application_policy_mismatch = match expected_variant.application_policy() {
        Some(expected) => {
            !observation.application_policy_field_present
                || match observation.application_policy.as_deref() {
                    Some(actual) => actual != expected,
                    None => observation.prediction_valid == Some(true) && observation.attempted,
                }
        }
        None => observation.application_policy.is_some(),
    };
    trace.identity_failures += u64::from(application_policy_mismatch);
    let key = Key {
        symbol,
        market,
        eval_ms,
        materialization,
        sequence,
    };
    trace.duplicate_conflicts +=
        u64::from(insert_unique(&mut trace.observations, key, observation));
}

fn validate_direct_market(trace: &mut Trace) {
    for (key, observation) in &trace.observations {
        let mut event = OracleResearchDirectMarketCoverage::default();
        let selected = observation.direct_market_selected == Some(true);
        let open = trace
            .labels
            .get(&(key.symbol.clone(), key.market.clone()))
            .map(|label| key.eval_ms < label.end_ms);
        count_direct_scope(&mut event, observation, open);
        let market_valid = observation.effective_market_p.map(valid_probability);
        let recorded_prediction_valid = observation.learner_p.map(valid_probability);
        event.prediction_validity_disagreements += u64::from(
            observation
                .prediction_valid
                .zip(recorded_prediction_valid)
                .is_some_and(|(emitted, recorded)| emitted != recorded),
        );
        let predicate = all_required([
            observation
                .oracle_mode
                .as_deref()
                .map(|mode| mode == "market"),
            open,
            observation.learning_enabled,
            observation.fit_available,
            observation.prediction_valid,
            recorded_prediction_valid,
            observation.trusted.map(|trusted| !trusted),
            market_valid,
        ]);
        event.unknown_predicate_events += u64::from(predicate.is_none());
        event.selected_unknown_predicate_events += u64::from(selected && predicate.is_none());
        event.predicate_violations +=
            u64::from(predicate.is_some_and(|expected| expected != selected));
        if selected {
            event.selected_events = 1;
            event.materialization_selected_events = u64::from(key.materialization);
            let mean_failed = !matches!(
                (observation.final_p, observation.effective_market_p),
                (Some(final_p), Some(market_p))
                    if final_p == market_p && valid_probability(market_p)
            );
            let interval_failed = !observation.effective_market_interval_fields_present
                || !observation.final_interval_fields_present
                || observation.final_interval != observation.effective_market_interval;
            let beta_failed = !matches!(
                (observation.beta_observation_p, observation.effective_market_p),
                (Some(beta_p), Some(market_p))
                    if beta_p == market_p && valid_probability(market_p)
            );
            let confidence_failed = observation.confidence_transform_applied != Some(false);
            let beta_transform_failed = observation.beta_calibration_applied != Some(false);
            event.mean_identity_failures = u64::from(mean_failed);
            event.interval_identity_failures = u64::from(interval_failed);
            event.beta_observation_identity_failures = u64::from(beta_failed);
            event.confidence_transform_violations = u64::from(confidence_failed);
            event.beta_transform_violations = u64::from(beta_transform_failed);
            event.materialization_identity_failures = u64::from(
                key.materialization
                    && (mean_failed
                        || interval_failed
                        || beta_failed
                        || confidence_failed
                        || beta_transform_failed),
            );
        }
        trace.direct_market.add(&event);
        trace
            .direct_market_by_symbol
            .entry(key.symbol.clone())
            .or_default()
            .add(&event);
    }
}

fn count_direct_scope(
    coverage: &mut OracleResearchDirectMarketCoverage,
    observation: &Observation,
    open: Option<bool>,
) {
    coverage.market_scope_events += u64::from(observation.oracle_mode.as_deref() == Some("market"));
    coverage.cleanup_scope_events +=
        u64::from(observation.oracle_mode.as_deref() == Some("cleanup"));
    coverage.mode_unavailable_events += u64::from(observation.oracle_mode.is_none());
    coverage.open_scope_events += u64::from(open == Some(true));
    coverage.closed_scope_events += u64::from(open == Some(false));
    coverage.close_scope_unavailable_events += u64::from(open.is_none());
    coverage.learning_enabled_events += u64::from(observation.learning_enabled == Some(true));
    coverage.learning_disabled_events += u64::from(observation.learning_enabled == Some(false));
    coverage.learning_unavailable_events += u64::from(observation.learning_enabled.is_none());
    coverage.fit_available_events += u64::from(observation.fit_available == Some(true));
    coverage.fit_missing_events += u64::from(observation.fit_available == Some(false));
    coverage.fit_unavailable_events += u64::from(observation.fit_available.is_none());
    coverage.prediction_valid_events += u64::from(observation.prediction_valid == Some(true));
    coverage.prediction_invalid_events += u64::from(observation.prediction_valid == Some(false));
    coverage.prediction_validity_unavailable_events +=
        u64::from(observation.prediction_valid.is_none());
    coverage.trust_false_events += u64::from(observation.trusted == Some(false));
    coverage.trust_true_events += u64::from(observation.trusted == Some(true));
    coverage.trust_unavailable_events += u64::from(observation.trusted.is_none());
    match observation.effective_market_p {
        Some(value) if valid_probability(value) => coverage.market_probability_valid_events += 1,
        Some(_) => coverage.market_probability_invalid_events += 1,
        None => coverage.market_probability_unavailable_events += 1,
    }
    coverage.interval_field_absent_events +=
        u64::from(!observation.effective_market_interval_fields_present);
    coverage.interval_unavailable_events += u64::from(
        observation.effective_market_interval_fields_present
            && observation.effective_market_interval == (None, None),
    );
}

fn all_required(values: [Option<bool>; 8]) -> Option<bool> {
    if values.iter().any(|value| *value == Some(false)) {
        Some(false)
    } else {
        values.iter().all(Option::is_some).then_some(true)
    }
}

fn valid_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

fn label(value: &Value, trace: &mut Trace) {
    let Some((symbol, _, _, _)) = forecast_scope(value, trace) else {
        return;
    };
    let (Some(market), Some(winner_up), Some(start_ms), Some(end_ms)) = (
        string(value, "market_id"),
        bool_field(value, "winner_up"),
        u64_field(value, "window_start_ms"),
        u64_field(value, "window_end_ms"),
    ) else {
        trace.missing_scope += 1;
        return;
    };
    trace.duplicate_conflicts += u64::from(insert_unique(
        &mut trace.labels,
        (symbol, market),
        Label {
            winner_up,
            start_ms,
            end_ms,
        },
    ));
}

fn first(value: &Value, trace: &mut Trace, direction: bool) {
    let Some((symbol, sequence, scoped_eval_ms, materialization)) = forecast_scope(value, trace)
    else {
        return;
    };
    let timestamp = if direction {
        "selection_ts_ms"
    } else {
        "authorization_ts_ms"
    };
    let (Some(market), Some(event_ms)) = (string(value, "market_id"), u64_field(value, timestamp))
    else {
        trace.missing_scope += 1;
        return;
    };
    trace.identity_failures += u64::from(event_ms != scoped_eval_ms);
    let first = First {
        event_ms,
        sequence,
        materialization,
        selected_side: option_string(value, "selected_first_side"),
        causal_side: direction
            .then(|| option_string(value, "causal_winner_side"))
            .flatten(),
        snapshot_ms: option_u64(value, "snapshot_last_eval_ts_ms"),
        actual_p: option_f64(value, "actual_first_p_up"),
        latched: (!direction)
            .then(|| bool_field(value, "first_side_latched"))
            .flatten(),
        fresh: (!direction)
            .then(|| bool_field(value, "fresh_direction_chosen"))
            .flatten(),
    };
    let conflict = if direction {
        insert_unique(&mut trace.first_directions, (symbol, market), first)
    } else {
        match trace.first_authorizations.entry((
            symbol,
            market,
            event_ms,
            sequence,
            materialization,
        )) {
            Entry::Vacant(entry) => {
                entry.insert(first);
                false
            }
            Entry::Occupied(entry) => {
                let original = entry.get();
                let fresh_to_latched = original.latched == Some(false)
                    && original.fresh == Some(true)
                    && first.latched == Some(true)
                    && first.fresh == Some(false)
                    && original.selected_side.is_some()
                    && original.selected_side == first.selected_side
                    && first.snapshot_ms.is_none()
                    && first.actual_p.is_none();
                original != &first && !fresh_to_latched
            }
        }
    };
    trace.duplicate_conflicts += u64::from(conflict);
}

fn first_decision_retained(value: &Value, trace: &mut Trace) {
    let (
        Some(symbol),
        Some(market),
        Some(authorization_ms),
        Some(window_start_ms),
        Some(window_end_ms),
        Some(marker_retained),
        Some(sample_available),
    ) = (
        string(value, "symbol"),
        string(value, "market_id"),
        u64_field(value, "authorization_ts_ms"),
        u64_field(value, "window_start_ms"),
        u64_field(value, "window_end_ms"),
        bool_field(value, "marker_retained"),
        bool_field(value, "sample_available"),
    )
    else {
        trace.missing_scope += 1;
        return;
    };
    let symbol = symbol.to_ascii_uppercase();
    trace.identity_failures += u64::from(
        scope_string(value, "symbol").is_some_and(|scope| scope.to_ascii_uppercase() != symbol),
    );
    let optional_fields_present = [
        "snapshot_last_eval_ts_ms",
        "unavailable_reason",
        "observation_ts_ms",
        "sample_decision_ts_ms",
        "candidate_p_up",
        "prior_p_up",
        "first_p0",
        "first_p1",
    ]
    .iter()
    .all(|name| field(value, name).is_some());
    trace.first_decision_retained.push(FirstDecisionRetained {
        symbol,
        market,
        authorization_ms,
        window_start_ms,
        window_end_ms,
        snapshot_ms: option_u64(value, "snapshot_last_eval_ts_ms"),
        marker_retained,
        sample_available,
        unavailable_reason: option_string(value, "unavailable_reason"),
        observation_ms: option_u64(value, "observation_ts_ms"),
        sample_decision_ms: option_u64(value, "sample_decision_ts_ms"),
        candidate_p: option_f64(value, "candidate_p_up"),
        prior_p: option_f64(value, "prior_p_up"),
        first_p0: option_f64(value, "first_p0"),
        first_p1: option_f64(value, "first_p1"),
        optional_fields_present,
    });
}

fn first_decision_resolved(value: &Value, trace: &mut Trace) {
    let (
        Some(label_symbol),
        Some(market),
        Some(window_start_ms),
        Some(window_end_ms),
        Some(label_release_ms),
        Some(learning_applied_ms),
        Some(marker_available),
        Some(sample_available),
        Some(sample_merged),
        Some(first_fit_row_added),
        Some(ordinary_fit_row_count),
        Some(resolved_fit_row_count),
        Some(first_support_count),
        Some(fit_sample_count),
    ) = (
        string(value, "symbol"),
        string(value, "market_id"),
        u64_field(value, "window_start_ms"),
        u64_field(value, "window_end_ms"),
        u64_field(value, "label_release_ts_ms"),
        u64_field(value, "learning_applied_ts_ms"),
        bool_field(value, "marker_available"),
        bool_field(value, "sample_available"),
        bool_field(value, "sample_merged"),
        bool_field(value, "first_fit_row_added"),
        u64_field(value, "ordinary_fit_row_count"),
        u64_field(value, "resolved_fit_row_count"),
        u64_field(value, "first_support_count"),
        u64_field(value, "fit_sample_count"),
    )
    else {
        trace.missing_scope += 1;
        return;
    };
    let Some(symbol) = scope_string(value, "symbol") else {
        trace.missing_scope += 1;
        return;
    };
    let symbol = symbol.to_ascii_uppercase();
    let expected_label_symbol = format!("{}/usd", symbol.to_ascii_lowercase());
    trace.identity_failures += u64::from(label_symbol != expected_label_symbol);
    let optional_fields_present = [
        "authorization_ts_ms",
        "snapshot_last_eval_ts_ms",
        "first_p0",
        "first_p1",
    ]
    .iter()
    .all(|name| field(value, name).is_some());
    trace.first_decision_resolved.push(FirstDecisionResolved {
        symbol,
        market,
        window_start_ms,
        window_end_ms,
        label_release_ms,
        learning_applied_ms,
        marker_available,
        authorization_ms: option_u64(value, "authorization_ts_ms"),
        snapshot_ms: option_u64(value, "snapshot_last_eval_ts_ms"),
        sample_available,
        sample_merged,
        first_fit_row_added,
        ordinary_fit_row_count,
        resolved_fit_row_count,
        first_p0: option_f64(value, "first_p0"),
        first_p1: option_f64(value, "first_p1"),
        first_support_count,
        fit_sample_count,
        optional_fields_present,
    });
}

fn scope_string(value: &Value, name: &str) -> Option<String> {
    value
        .get("span")?
        .get(name)?
        .as_str()
        .map(ToString::to_string)
}

fn scope_u64(value: &Value, name: &str) -> Option<u64> {
    parse_u64(value.get("span")?.get(name)?)
}

fn scope_bool(value: &Value, name: &str) -> Option<bool> {
    let value = value.get("span")?.get(name)?;
    value.as_bool().or_else(|| value.as_str()?.parse().ok())
}

fn insert_unique<K: Ord, V: PartialEq>(map: &mut BTreeMap<K, V>, key: K, value: V) -> bool {
    match map.entry(key) {
        Entry::Vacant(entry) => {
            entry.insert(value);
            false
        }
        Entry::Occupied(entry) => entry.get() != &value,
    }
}

pub(super) fn has_label(trace: &Trace, key: &Key) -> bool {
    trace
        .labels
        .contains_key(&(key.symbol.clone(), key.market.clone()))
}

pub(super) fn missing_features(observation: &Observation, layout: &str) -> bool {
    (matches!(layout, "representation_only" | "combined") && observation.spot_z.is_none())
        || (matches!(layout, "reference_only" | "combined") && observation.reference.is_none())
        || (observation.trade_flow_enabled == Some(true)
            && observation.trade_flow_imbalance_5s.is_none())
}

pub(super) fn reference_consistent(key: &Key, observation: &Observation, label: &Label) -> bool {
    !observation.partial_reference
        && observation
            .reference
            .as_ref()
            .is_none_or(|reference| reference.valid_for_window(&key.symbol, label.start_ms))
}

pub(super) fn observation_stages(observation: &Observation, trade_flow: bool) -> u8 {
    let mut stages = 0;
    stages |= u8::from(observation.learner_p.is_some_and(valid_probability)) * STAGE_LEARNER;
    stages |=
        u8::from(observation.market_prior_p.is_some_and(valid_probability)) * STAGE_MARKET_PRIOR;
    stages |= u8::from(observation.market_raw_p.is_some_and(valid_probability)) * STAGE_MARKET_RAW;
    stages |=
        u8::from(observation.original_base_p.is_some_and(valid_probability)) * STAGE_ORIGINAL_BASE;
    stages |= u8::from(observation.base_blend_p.is_some_and(valid_probability)) * STAGE_BASE_BLEND;
    stages |=
        u8::from(observation.post_logistic_p.is_some_and(valid_probability)) * STAGE_POST_LOGISTIC;
    stages |= u8::from(observation.final_p.is_some_and(valid_probability)) * STAGE_FINAL;
    let paired = STAGE_LEARNER | STAGE_FINAL | STAGE_MARKET_RAW;
    if trade_flow && (stages & paired != paired || missing_features(observation, "baseline")) {
        stages &= !paired;
    }
    stages
}

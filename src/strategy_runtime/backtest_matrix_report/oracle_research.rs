use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;

use self::identity::ValidatedOracleResearchTrace;
use self::scoring::{
    accuracy, final_output_diagnostics, final_output_phase_index, first_decision_incomplete,
    first_decision_metrics, first_evidence, first_join_conflicts, latched_missing,
    phase_trust_incomplete, phase_trust_metrics, probability_scores, record_uncertainty, scores,
    stage, valid_probability, FinalOutputScores,
};
use super::{
    CompletedOracleResearchMeasurement, OracleResearchCoverage, OracleResearchDirectMarketScores,
    OracleResearchMeasurement, OracleResearchSymbolMetrics, OracleResearchUncertaintyAvailability,
};
use crate::strategy::OracleLearningSnapshot;
use crate::strategy_runtime::backtest_matrix::oracle_research::{
    BacktestOracleResearchVariant, OracleResearchRun,
};

pub(super) mod identity;
mod scoring;
mod trace;
pub(super) mod types;

use trace::{
    has_label, missing_features, observation_stages, reference_consistent, Key, Observation, Trace,
};

const STAGE_LEARNER: u8 = 1 << 0;
const STAGE_MARKET_PRIOR: u8 = 1 << 1;
const STAGE_MARKET_RAW: u8 = 1 << 2;
const STAGE_ORIGINAL_BASE: u8 = 1 << 3;
const STAGE_BASE_BLEND: u8 = 1 << 4;
const STAGE_POST_LOGISTIC: u8 = 1 << 5;
const STAGE_FINAL: u8 = 1 << 6;
const ALL_STAGES: u8 = (1 << 7) - 1;

struct CommonKey {
    key: Key,
    stages: u8,
    affected: bool,
}

pub(in crate::strategy_runtime::backtest_matrix_report) fn incomplete_evidence_reason(
    variant: BacktestOracleResearchVariant,
    rows: &[OracleResearchSymbolMetrics],
) -> Option<&'static str> {
    match variant {
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust
            if rows.iter().any(phase_trust_incomplete) =>
        {
            Some("phase_trust_evidence_incomplete")
        }
        variant
            if variant.uses_final_output_trust()
                && rows.iter().any(|row| {
                    row.final_output
                        .as_ref()
                        .is_none_or(types::OracleResearchFinalOutputDiagnostics::incomplete)
                }) =>
        {
            Some("final_output_evidence_incomplete")
        }
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust
            if rows.iter().any(first_decision_incomplete) =>
        {
            Some("first_decision_evidence_incomplete")
        }
        _ => None,
    }
}

pub(super) fn measure_completed_variants(
    run: &OracleResearchRun,
    execution_timing_sha256: &str,
    completed: &[ValidatedOracleResearchTrace],
    final_learning: &BTreeMap<String, OracleLearningSnapshot>,
) -> Result<Vec<CompletedOracleResearchMeasurement>> {
    let mut parsed = completed
        .iter()
        .map(|entry| {
            Ok((
                entry.variant,
                trace::parse(
                    &entry.runtime_log_path,
                    run,
                    entry.variant,
                    execution_timing_sha256,
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    anyhow::ensure!(!parsed.is_empty(), "no completed oracle research traces");
    let mut conflicted_labels = BTreeSet::new();
    for (_, trace) in &parsed {
        for (key, label) in &trace.labels {
            if parsed
                .iter()
                .filter_map(|(_, other)| other.labels.get(key))
                .any(|other| other != label)
            {
                conflicted_labels.insert(key.clone());
            }
        }
    }
    for (_, trace) in &mut parsed {
        trace.duplicate_conflicts += conflicted_labels
            .iter()
            .filter(|key| trace.labels.contains_key(*key))
            .count() as u64;
    }
    let direct_experiment = run.variant() == BacktestOracleResearchVariant::DirectMarket;
    let trade_flow = run.variant() == BacktestOracleResearchVariant::TradeFlow;
    let affected = parsed
        .iter()
        .find(|(variant, _)| *variant == BacktestOracleResearchVariant::DirectMarket)
        .map(|(_, trace)| {
            trace
                .observations
                .iter()
                .filter(|(_, observation)| observation.direct_market_selected == Some(true))
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();

    let mut common = parsed[0]
        .1
        .observations
        .keys()
        .filter(|key| {
            !key.materialization
                && has_label(&parsed[0].1, key)
                && !conflicted_labels.contains(&(key.symbol.clone(), key.market.clone()))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    for (_, trace) in parsed.iter().skip(1) {
        common.retain(|key| trace.observations.contains_key(key) && has_label(trace, key));
    }
    let mut by_symbol = BTreeMap::<String, Vec<CommonKey>>::new();
    for key in common {
        let stages = parsed.iter().fold(ALL_STAGES, |stages, (_, trace)| {
            stages
                & trace
                    .observations
                    .get(&key)
                    .map(|row| observation_stages(row, trade_flow))
                    .unwrap_or_default()
        });
        by_symbol
            .entry(key.symbol.clone())
            .or_default()
            .push(CommonKey {
                affected: affected.contains(&key),
                key,
                stages,
            });
    }
    let variant_count = parsed.len();
    let final_output_experiment = run.variant().uses_final_output_trust();
    Ok(parsed
        .into_iter()
        .map(|(variant, trace)| CompletedOracleResearchMeasurement {
            variant,
            measurement: measurement(
                &trace,
                run.prepared_payload_sha256(),
                &by_symbol,
                &conflicted_labels,
                variant_count,
                variant,
                variant.feature_layout(),
                variant.application_policy(),
                direct_experiment,
                final_output_experiment,
                (variant == run.variant()).then_some(final_learning),
                run.capture_window(),
            ),
        })
        .collect())
}

fn measurement(
    trace: &Trace,
    scope: &BTreeMap<String, String>,
    common: &BTreeMap<String, Vec<CommonKey>>,
    conflicted_labels: &BTreeSet<(String, String)>,
    variant_count: usize,
    variant: BacktestOracleResearchVariant,
    layout: &str,
    application_policy: Option<&str>,
    direct_experiment: bool,
    final_output_experiment: bool,
    final_learning: Option<&BTreeMap<String, OracleLearningSnapshot>>,
    capture_window: (u64, u64),
) -> OracleResearchMeasurement {
    let symbols = scope
        .keys()
        .map(|symbol| {
            symbol_metrics(
                symbol,
                trace,
                common.get(symbol),
                conflicted_labels,
                variant,
                layout,
                application_policy,
                direct_experiment,
                final_output_experiment,
                final_learning.and_then(|learning| learning.get(symbol)),
                capture_window,
            )
        })
        .collect::<Vec<_>>();
    let reference_provenance_failures = trace
        .observations
        .iter()
        .filter(|(key, observation)| {
            trace
                .labels
                .get(&(key.symbol.clone(), key.market.clone()))
                .is_some_and(|label| !reference_consistent(key, observation, label))
        })
        .count() as u64;
    let first_direction_markets = trace.first_directions.keys().fold(
        BTreeMap::<String, BTreeSet<String>>::new(),
        |mut markets, (symbol, market)| {
            markets
                .entry(symbol.clone())
                .or_default()
                .insert(market.clone());
            markets
        },
    );
    OracleResearchMeasurement {
        coverage: OracleResearchCoverage {
            runtime_log_bytes: trace.runtime_log_bytes,
            parsed_lines: trace.parsed_lines,
            malformed_lines: trace.malformed_lines,
            run_start_markers: trace.starts,
            run_end_markers: trace.ends,
            timing_identity_markers: trace.timings,
            probability_events: trace.probability_events,
            unavailable_probability_events: trace.unavailable,
            resolved_label_events: trace.labels.len() as u64,
            first_authorization_events: trace.first_authorizations.len() as u64,
            missing_scope_events: trace.missing_scope,
            duplicate_conflicts: trace
                .duplicate_conflicts
                .saturating_add(first_join_conflicts(trace)),
            identity_failures: trace.identity_failures,
            missing_first_snapshot_joins: symbols
                .iter()
                .map(|row| row.actual_first_missing_original_snapshot)
                .sum(),
            latched_first_without_original_snapshot: latched_missing(trace),
            missing_required_features: trace
                .observations
                .iter()
                .filter(|(key, observation)| {
                    has_label(trace, key) && missing_features(observation, layout)
                })
                .count() as u64,
            reference_provenance_failures,
            common_variant_count: variant_count,
            common_paired_observations: symbols
                .iter()
                .map(|row| row.common_paired_observations)
                .sum(),
            direct_market: (trace.direct_market_trace).then(|| trace.direct_market.clone()),
            ..OracleResearchCoverage::default()
        },
        symbols,
        first_direction_markets,
    }
}

fn symbol_metrics(
    symbol: &str,
    trace: &Trace,
    common: Option<&Vec<CommonKey>>,
    conflicted_labels: &BTreeSet<(String, String)>,
    variant: BacktestOracleResearchVariant,
    layout: &str,
    application_policy: Option<&str>,
    direct_experiment: bool,
    final_output_experiment: bool,
    final_learning: Option<&OracleLearningSnapshot>,
    capture_window: (u64, u64),
) -> OracleResearchSymbolMetrics {
    let market_offset = layout == "market_offset";
    let mut rows = common
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter_map(|common| {
            Some((
                &common.key,
                trace.observations.get(&common.key)?,
                trace
                    .labels
                    .get(&(common.key.symbol.clone(), common.key.market.clone()))?,
                common.stages,
            ))
        })
        .collect::<Vec<_>>();
    if market_offset {
        for (_, observation, _, stages) in &mut rows {
            if !observation.learner_prior_p.is_some_and(valid_probability) {
                *stages &= !STAGE_MARKET_PRIOR;
            }
        }
    }
    let offset_prior_evidence_complete = !market_offset
        || rows
            .iter()
            .all(|(_, observation, _, _)| observation.learner_prior_field_present);
    let markets = rows
        .iter()
        .map(|(key, _, _, _)| key.market.as_str())
        .collect::<BTreeSet<_>>()
        .len() as u64;
    let mut first_minute = Vec::new();
    let mut later = Vec::new();
    for (key, observation, label, stages) in &rows {
        if stages & STAGE_FINAL == 0 {
            continue;
        }
        if key.eval_ms >= label.start_ms
            && key.eval_ms < label.end_ms
            && key.eval_ms < label.start_ms.saturating_add(60_000)
        {
            first_minute.push((observation.final_p, key.market.as_str(), *label));
        } else if key.eval_ms >= label.start_ms.saturating_add(60_000) && key.eval_ms < label.end_ms
        {
            later.push((observation.final_p, key.market.as_str(), *label));
        }
    }

    let first = first_evidence(
        trace,
        symbol,
        conflicted_labels,
        variant == BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust,
    );
    let paired_population = scores(&rows, STAGE_FINAL, |row| row.final_p);
    let affected_keys = common
        .into_iter()
        .flatten()
        .filter(|entry| entry.affected)
        .map(|entry| entry.key.clone())
        .collect::<BTreeSet<_>>();
    let affected_rows = rows
        .iter()
        .filter(|(key, _, _, _)| affected_keys.contains(*key))
        .copied()
        .collect::<Vec<_>>();
    let first_minute_scores = probability_scores(&first_minute);
    let actual_first_scores = first.scores.clone();
    let affected_first_minute = rows
        .iter()
        .filter(|(key, _, label, stages)| {
            affected_keys.contains(*key)
                && *stages & STAGE_FINAL != 0
                && key.eval_ms >= label.start_ms
                && key.eval_ms < label.end_ms
                && key.eval_ms < label.start_ms.saturating_add(60_000)
        })
        .map(|(key, observation, label, _)| (observation.final_p, key.market.as_str(), *label))
        .collect::<Vec<_>>();

    let mut metrics = OracleResearchSymbolMetrics {
        symbol: symbol.to_string(),
        labeled_markets: markets,
        labeled_observations: rows.len() as u64,
        common_paired_observations: rows.len() as u64,
        learner: scores(&rows, STAGE_LEARNER, |row| row.learner_p),
        market_prior: scores(&rows, STAGE_MARKET_PRIOR, |row| {
            if market_offset {
                row.learner_prior_p
            } else {
                row.market_prior_p
            }
        }),
        paired_learner: offset_prior_evidence_complete.then(|| {
            scores(&rows, STAGE_LEARNER | STAGE_MARKET_PRIOR, |row| {
                row.learner_p
            })
        }),
        paired_market_prior: offset_prior_evidence_complete.then(|| {
            scores(&rows, STAGE_LEARNER | STAGE_MARKET_PRIOR, |row| {
                if market_offset {
                    row.learner_prior_p
                } else {
                    row.market_prior_p
                }
            })
        }),
        market_raw: scores(&rows, STAGE_MARKET_RAW, |row| row.market_raw_p),
        original_base: scores(&rows, STAGE_ORIGINAL_BASE, |row| row.original_base_p),
        final_probability: paired_population.clone(),
        first_minute_final_scores: Some(first_minute_scores.clone()),
        actual_first_scores: Some(actual_first_scores.clone()),
        first_minute_final_accuracy: accuracy(&first_minute),
        actual_first_accuracy: first.accuracy,
        later_horizon_final_accuracy: accuracy(&later),
        actual_first_authorizations: first.authorizations,
        actual_first_attributed: first.attributed,
        actual_first_missing_original_snapshot: first.missing_snapshot,
        actual_first_label_available: first.label_available,
        actual_first_label_unavailable: first.label_unavailable,
        actual_first_label_conflict: first.label_conflict,
        prediction_unavailable: Some(0),
        prequentially_untrusted: Some(0),
        prequential_trust_unavailable: Some(0),
        uncertainty_availability: market_offset
            .then(OracleResearchUncertaintyAvailability::default),
        direct_market: trace.direct_market_trace.then(|| {
            trace
                .direct_market_by_symbol
                .get(symbol)
                .cloned()
                .unwrap_or_default()
        }),
        direct_market_scores: direct_experiment.then(|| OracleResearchDirectMarketScores {
            paired_population,
            affected_population: scores(&affected_rows, STAGE_FINAL, |row| row.final_p),
            first_minute: first_minute_scores,
            affected_first_minute: probability_scores(&affected_first_minute),
            actual_first: actual_first_scores,
        }),
        phase_trust: (variant == BacktestOracleResearchVariant::MarketOffsetPhaseTrust).then(
            || {
                phase_trust_metrics(
                    trace,
                    symbol,
                    conflicted_labels,
                    application_policy.expect("phase-trust policy"),
                )
            },
        ),
        final_output: final_output_experiment.then(|| {
            final_output_metrics(
                trace,
                symbol,
                conflicted_labels,
                application_policy.unwrap_or("none"),
                final_learning,
                capture_window,
                common,
            )
        }),
        first_decision: (variant == BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust)
            .then(|| {
                first_decision_metrics(
                    trace,
                    symbol,
                    application_policy.expect("First-decision policy"),
                )
            }),
        ..OracleResearchSymbolMetrics::default()
    };
    for (_, observation, label, stages) in rows {
        metrics.missing_features += u64::from(missing_features(observation, layout));
        metrics.ties += u64::from(observation.final_p.is_some_and(|p| p == 0.5));
        metrics.fit_available += u64::from(observation.fit_available == Some(true));
        metrics.prediction_available += u64::from(observation.learner_p.is_some());
        if let Some(count) = &mut metrics.prediction_unavailable {
            *count += u64::from(observation.learner_p.is_none());
        }
        record_uncertainty(observation, metrics.uncertainty_availability.as_mut());
        metrics.prequentially_trusted += u64::from(observation.trusted == Some(true));
        if let Some(count) = &mut metrics.prequentially_untrusted {
            *count += u64::from(observation.trusted == Some(false));
        }
        if let Some(count) = &mut metrics.prequential_trust_unavailable {
            *count += u64::from(observation.trusted.is_none());
        }
        metrics.application_attempted += u64::from(observation.attempted);
        metrics.branch_applied += u64::from(observation.applied);
        metrics.actually_contributing += u64::from(observation.contributing);
        metrics.probability_calibration_attempted +=
            u64::from(observation.probability_calibration_attempted);
        metrics.probability_calibration_effective +=
            u64::from(observation.probability_calibration_effective);
        metrics.beta_calibration_attempted += u64::from(observation.beta_calibration_attempted);
        metrics.beta_calibration_applied +=
            u64::from(observation.beta_calibration_applied == Some(true));
        if stages & (STAGE_BASE_BLEND | STAGE_POST_LOGISTIC)
            == STAGE_BASE_BLEND | STAGE_POST_LOGISTIC
        {
            stage(
                observation.base_blend_p,
                observation.post_logistic_p,
                label.winner_up,
                &mut metrics.learner_corrected,
                &mut metrics.learner_damaged,
            );
        }
        if stages & (STAGE_POST_LOGISTIC | STAGE_FINAL) == STAGE_POST_LOGISTIC | STAGE_FINAL {
            stage(
                observation.post_logistic_p,
                observation.final_p,
                label.winner_up,
                &mut metrics.calibration_corrected,
                &mut metrics.calibration_damaged,
            );
        }
        if stages & (STAGE_ORIGINAL_BASE | STAGE_FINAL) == STAGE_ORIGINAL_BASE | STAGE_FINAL {
            stage(
                observation.original_base_p,
                observation.final_p,
                label.winner_up,
                &mut metrics.final_corrected,
                &mut metrics.final_damaged,
            );
        }
    }
    metrics
}

fn final_output_metrics<'a>(
    trace: &'a Trace,
    symbol: &str,
    conflicted_labels: &BTreeSet<(String, String)>,
    application_policy: &str,
    learning: Option<&'a OracleLearningSnapshot>,
    capture_window: (u64, u64),
    common: Option<&Vec<CommonKey>>,
) -> types::OracleResearchFinalOutputDiagnostics {
    let mut diagnostics = final_output_diagnostics(application_policy);
    let mut scores = FinalOutputScores::default();
    let common_final = common
        .into_iter()
        .flatten()
        .filter(|row| row.stages & STAGE_FINAL != 0)
        .map(|row| &row.key)
        .collect::<BTreeSet<_>>();
    let mut joins = BTreeMap::<(&str, u64, Option<u64>), Option<(&Key, &Observation)>>::new();
    for (key, observation) in trace
        .observations
        .iter()
        .filter(|(key, _)| key.symbol == symbol)
    {
        let timestamp = key.final_output_timestamp_key(observation);
        joins
            .entry((
                timestamp.market,
                timestamp.decision_ms,
                timestamp.observation_ms,
            ))
            .and_modify(|entry| *entry = None)
            .or_insert(Some((key, observation)));
        diagnostics.published_source_observations += 1;
        if !key.materialization && !common_final.contains(key) {
            diagnostics.published_noncommon_excluded += 1;
            continue;
        }
        let label_key = (key.symbol.clone(), key.market.clone());
        if conflicted_labels.contains(&label_key) {
            diagnostics.published_label_conflict += 1;
            continue;
        }
        let Some(label) = trace.labels.get(&label_key) else {
            diagnostics.published_label_unavailable += 1;
            continue;
        };
        let Some(phase) = final_output_phase_index(label.start_ms, label.end_ms, key.eval_ms)
        else {
            diagnostics.published_phase_unavailable += 1;
            continue;
        };
        scores.record_published(
            key.materialization,
            observation.oracle_mode.as_deref(),
            phase,
            observation.final_p,
            observation.oracle_signal_side_field_present,
            observation.oracle_signal_side.as_deref(),
            key.market.as_str(),
            label.winner_up,
        );
    }
    if let Some(learning) = learning {
        for row in learning.logistic_final_output_pairs() {
            diagnostics.retained_source_rows += 1;
            let market = row.market_id.as_str();
            let label_key = (symbol.to_string(), market.to_string());
            let Some(label) = trace.labels.get(&label_key) else {
                diagnostics.retained_outside_replay_scope += 1;
                continue;
            };
            if row.decision_ts_ms < capture_window.0 || row.decision_ts_ms >= capture_window.1 {
                diagnostics.retained_outside_replay_scope += 1;
                continue;
            }
            if conflicted_labels.contains(&label_key) {
                diagnostics.retained_label_conflict += 1;
                continue;
            }
            if label.start_ms != row.window_start_ms
                || label.end_ms != row.window_end_ms
                || label.winner_up != row.winner_up
            {
                diagnostics.retained_label_mismatch += 1;
                continue;
            }
            if row.window_start_ms >= row.window_end_ms
                || row.observation_ts_ms > row.decision_ts_ms
                || row.decision_ts_ms < row.window_start_ms
                || row.decision_ts_ms >= row.window_end_ms
                || row.label_release_ts_ms < row.window_end_ms
                || row.learning_applied_ts_ms < row.label_release_ts_ms
            {
                diagnostics.retained_chronology_failures += 1;
                continue;
            }
            let phase = usize::from(row.phase);
            if phase >= scoring::PHASES.len() {
                diagnostics.retained_phase_unrecognized += 1;
                continue;
            }
            if final_output_phase_index(row.window_start_ms, row.window_end_ms, row.decision_ts_ms)
                != Some(phase)
            {
                diagnostics.retained_phase_identity_mismatch += 1;
                continue;
            }
            let phase_metrics = &mut diagnostics.phases[phase];
            phase_metrics.observations += 1;
            scores.record_retained(phase, market, row.winner_up, row.f0_p_up, row.f1_p_up);
            match joins.get(&(market, row.decision_ts_ms, Some(row.observation_ts_ms))) {
                None => {
                    phase_metrics.runtime_join_missing += 1;
                    phase_metrics.selected_unavailable += 1;
                }
                Some(None) => {
                    phase_metrics.runtime_join_ambiguous += 1;
                    phase_metrics.selected_unavailable += 1;
                }
                Some(Some((key, observation))) => {
                    phase_metrics.runtime_join_unique += 1;
                    phase_metrics.selected_f1 += u64::from(observation.applied);
                    phase_metrics.selected_f0 += u64::from(!observation.applied);
                    phase_metrics.mode_market +=
                        u64::from(observation.oracle_mode.as_deref() == Some("market"));
                    phase_metrics.mode_cleanup +=
                        u64::from(observation.oracle_mode.as_deref() == Some("cleanup"));
                    phase_metrics.mode_unavailable += u64::from(!matches!(
                        observation.oracle_mode.as_deref(),
                        Some("market" | "cleanup")
                    ));
                    phase_metrics.materialization += u64::from(key.materialization);
                    phase_metrics.nonmaterialization += u64::from(!key.materialization);
                    phase_metrics.application_attempted += u64::from(observation.attempted);
                    phase_metrics.branch_applied += u64::from(observation.applied);
                    phase_metrics.actually_contributing += u64::from(observation.contributing);
                    let selected = if observation.applied {
                        row.f1_p_up
                    } else {
                        row.f0_p_up
                    };
                    match observation.final_p {
                        Some(published) if published == selected => {
                            phase_metrics.selected_published_equal += 1
                        }
                        Some(_) => phase_metrics.selected_published_mismatch += 1,
                        None => phase_metrics.selected_unavailable += 1,
                    }
                }
            }
        }
    }
    scores.finish(&mut diagnostics);
    diagnostics
}

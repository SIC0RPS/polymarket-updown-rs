use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::backtest_live_latency::ReplayExecutionTimingModel;
use super::backtest_live_latency::ReplayLiveLatencySummary;
use super::backtest_matrix::{
    oracle_research::{
        BacktestOracleResearchVariant, OracleResearchRun, PreparedOracleResearchSeed,
    },
    BacktestMatrixDiagnostics, SymbolRow,
};
use super::backtest_ohlc_manifest::ReplayOhlcManifestSummary;
use super::backtest_run_identity::BacktestRunIdentity;
use super::replay_mode::RecordedMarketCoverageReport;
use super::replay_mode::ReplayCtfSplitLatencySummary;
use crate::portfolio::PortfolioAccountingProjectionProof;
use crate::telemetry::FloodControlSnapshot;
use crate::trade_session_ledger::CanonicalSessionStats;

mod oracle_research;
mod proof_metadata_artifact;
mod replay_assumptions_artifact;
mod replay_runtime_artifact;

use oracle_research::types::{CompletedOracleResearchMeasurement, OracleResearchMeasurement};
pub use oracle_research::types::{
    OracleResearchAccuracy, OracleResearchArtifact, OracleResearchComparisonIdentity,
    OracleResearchCoverage, OracleResearchDirectMarketCoverage, OracleResearchDirectMarketScores,
    OracleResearchFirstDecisionDiagnostics, OracleResearchModelScores,
    OracleResearchPhaseTrustDiagnostics, OracleResearchPhaseTrustMetrics,
    OracleResearchSignalScores, OracleResearchSymbolMetrics, OracleResearchUncertaintyAvailability,
    OracleResearchVariantMeasurement,
};
pub use proof_metadata_artifact::BacktestProofMetadata;
use replay_assumptions_artifact::{
    replay_assumptions_artifact, BacktestMatrixReplayAssumptionsArtifact,
};
pub use replay_runtime_artifact::{
    BacktestClobTimingDiagnostic, BacktestCtfTimingDiagnostic, BacktestMatrixReplayRuntimeArtifact,
};

#[derive(Clone, Debug, Serialize)]
pub struct BacktestMatrixReportArtifact {
    pub schema_version: u32,
    pub candidate_id: String,
    pub capture_session: String,
    pub output_root: String,
    pub failed: bool,
    pub proof_metadata: BacktestProofMetadata,
    pub run_identity: BacktestRunIdentity,
    pub ohlc_manifest: ReplayOhlcManifestSummary,
    pub replay_assumptions: BacktestMatrixReplayAssumptionsArtifact,
    pub replay_runtime: BacktestMatrixReplayRuntimeArtifact,
    pub diagnostics: BacktestMatrixDiagnostics,
    pub canonical_session_stats: CanonicalSessionStats,
    pub portfolio_accounting: PortfolioAccountingProjectionProof,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_market_coverage: Option<RecordedMarketCoverageReport>,
    pub oracle_learning_initial_source: String,
    pub oracle_learning_initial_sha256: Option<String>,
    pub oracle_learning_initial_cutoffs_ms: BTreeMap<String, u64>,
    pub oracle_learning_initial_metrics: BTreeMap<String, crate::strategy::OracleLearningMetrics>,
    pub oracle_learning_final_path: String,
    pub oracle_learning_final_sha256: String,
    pub oracle_learning_final_cutoffs_ms: BTreeMap<String, u64>,
    pub oracle_learning_final_metrics: BTreeMap<String, crate::strategy::OracleLearningMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oracle_research: Option<OracleResearchArtifact>,
    pub symbols: Vec<BacktestMatrixSymbolArtifact>,
}

pub(super) fn finalize_oracle_research_artifact(
    report: &mut BacktestMatrixReportArtifact,
    run: &OracleResearchRun,
    seed: &PreparedOracleResearchSeed,
    final_seed_sha256: &str,
    final_learning: &BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
    execution_timing_sha256: &str,
    flood: &FloodControlSnapshot,
) -> Result<()> {
    let comparison_identity =
        oracle_research::identity::comparison_identity(run, &report.run_identity, seed)?;
    let traces = oracle_research::identity::validated_completed_oracle_research_traces(
        run,
        execution_timing_sha256,
    )?;
    let measurements = oracle_research::measure_completed_variants(
        run,
        execution_timing_sha256,
        &traces,
        final_learning,
    )?;
    let current_pnl = canonical_pnl(report);
    let first_markets = canonical_first_markets(report);
    let mut prior = Vec::with_capacity(traces.len().saturating_sub(1));
    for trace in &traces {
        if trace.variant != run.variant() {
            let value =
                run.completed_report(run.capture_name(), run.capture_run_id(), trace.variant)?;
            prior.push((
                trace.variant,
                serde_json::from_value::<OracleResearchArtifact>(
                    value
                        .get("oracle_research")
                        .cloned()
                        .context("completed report omits oracle research artifact")?,
                )?,
            ));
        }
    }
    let pnl = |variant, symbol: &str| {
        if variant == run.variant() {
            current_pnl.get(symbol).copied().flatten()
        } else {
            prior_pnl(&prior, variant, symbol)
        }
    };
    let mut current = None;
    let mut comparison = Vec::with_capacity(measurements.len());
    for completed in measurements {
        let variant = completed.variant;
        let saved = prior
            .iter()
            .find(|(candidate, _)| *candidate == variant)
            .map(|(_, artifact)| artifact);
        let mut measurement = completed.measurement;
        apply_run_coverage(
            &mut measurement,
            (variant == run.variant()).then_some(&first_markets),
            saved,
            run.runtime_log_capacity_bytes(),
            (variant == run.variant()).then_some(flood),
        );
        attach_financial(&mut measurement.symbols, variant, &pnl);
        finish_coverage(&mut measurement, variant, run.variant());
        if variant == run.variant() {
            current = Some((measurement.coverage.clone(), measurement.symbols.clone()));
        }
        comparison.push(OracleResearchVariantMeasurement {
            variant: variant.as_str().to_string(),
            coverage: measurement.coverage,
            symbols: measurement.symbols,
        });
    }
    let (coverage, symbols) = current.context("current oracle research measurement is absent")?;
    let (capture_start_ts_ms, capture_end_ts_ms) = run.capture_window();
    report.schema_version = match run.variant() {
        variant if variant.uses_final_output_trust() => 62,
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust => 61,
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust => 60,
        _ => 59,
    };
    report.oracle_research = Some(OracleResearchArtifact {
        schema_version: match run.variant() {
            variant if variant.uses_final_output_trust() => 7,
            BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust => 6,
            BacktestOracleResearchVariant::MarketOffsetPhaseTrust => 5,
            _ => 4,
        },
        variant: run.variant().as_str().to_string(),
        capture: run.capture_name().to_string(),
        capture_run_id: run.capture_run_id().to_string(),
        capture_start_ts_ms,
        capture_end_ts_ms,
        frozen_manifest_sha256: run.frozen_manifest_sha256().to_string(),
        common_input_identity_sha256: run.common_input_identity_sha256().to_string(),
        execution_timing_sha256: execution_timing_sha256.to_string(),
        frozen_input_sha256: run.input_sha256().clone(),
        prepared_payload_sha256: run.prepared_payload_sha256().clone(),
        seed_source: seed.source.to_string(),
        seed_initial_sha256: seed.sha256.clone(),
        seed_final_sha256: final_seed_sha256.to_string(),
        seed_transformed_once: seed.transformed_once,
        application_policy: seed.application_policy.map(str::to_string),
        runtime_log_path: run.runtime_log_path().display().to_string(),
        comparison_identity,
        coverage,
        symbols,
        completed_variant_comparison: comparison,
    });
    Ok(())
}

fn prior_pnl(
    prior: &[(BacktestOracleResearchVariant, OracleResearchArtifact)],
    variant: BacktestOracleResearchVariant,
    symbol: &str,
) -> Option<i128> {
    for (saved_variant, artifact) in prior {
        let Some(row) = artifact.symbols.iter().find(|row| row.symbol == symbol) else {
            continue;
        };
        if *saved_variant == variant {
            return row.canonical_cohort_pnl_units_6dp;
        }
        if variant == BacktestOracleResearchVariant::OperationalBaseline {
            if let Some(value) = row.operational_baseline_pnl_units_6dp {
                return Some(value);
            }
        }
        if row.comparison_baseline == variant.as_str() {
            if let Some(value) = row.comparison_baseline_pnl_units_6dp {
                return Some(value);
            }
        }
    }
    None
}

fn apply_run_coverage(
    measurement: &mut OracleResearchMeasurement,
    canonical_first: Option<&BTreeMap<String, BTreeSet<String>>>,
    saved: Option<&OracleResearchArtifact>,
    log_capacity: u64,
    flood: Option<&FloodControlSnapshot>,
) {
    measurement.coverage.runtime_log_capacity_bytes = log_capacity;
    if let Some(canonical) = canonical_first {
        for row in &mut measurement.symbols {
            let expected = canonical.get(&row.symbol);
            let observed = measurement.first_direction_markets.get(&row.symbol);
            row.unobserved_initial_first_boundaries = set_difference(expected, observed);
            row.unexpected_initial_first_boundaries = set_difference(observed, expected);
        }
        measurement.coverage.unobserved_initial_first_boundaries = measurement
            .symbols
            .iter()
            .map(|row| row.unobserved_initial_first_boundaries)
            .sum();
        measurement.coverage.unexpected_initial_first_boundaries = measurement
            .symbols
            .iter()
            .map(|row| row.unexpected_initial_first_boundaries)
            .sum();
    } else if let Some(saved) = saved {
        measurement.coverage.unobserved_initial_first_boundaries =
            saved.coverage.unobserved_initial_first_boundaries;
        measurement.coverage.unexpected_initial_first_boundaries =
            saved.coverage.unexpected_initial_first_boundaries;
        for row in &mut measurement.symbols {
            if let Some(old) = saved.symbols.iter().find(|old| old.symbol == row.symbol) {
                row.unobserved_initial_first_boundaries = old.unobserved_initial_first_boundaries;
                row.unexpected_initial_first_boundaries = old.unexpected_initial_first_boundaries;
            }
        }
    }
    if let Some(flood) = flood {
        measurement.coverage.flood_control_enabled = flood.enabled;
        measurement.coverage.flood_suppressed_events = flood.total_events_suppressed;
        measurement.coverage.sink_dropped_lines = flood.sink_dropped_lines;
    } else if let Some(saved) = saved {
        measurement.coverage.flood_control_enabled = saved.coverage.flood_control_enabled;
        measurement.coverage.flood_suppressed_events = saved.coverage.flood_suppressed_events;
        measurement.coverage.sink_dropped_lines = saved.coverage.sink_dropped_lines;
    }
}

fn finish_coverage(
    measurement: &mut OracleResearchMeasurement,
    variant: BacktestOracleResearchVariant,
    experiment: BacktestOracleResearchVariant,
) {
    let missing_pnl = measurement.symbols.iter().any(|row| {
        row.canonical_cohort_pnl_units_6dp.is_none()
            || row.operational_baseline_pnl_units_6dp.is_none()
            || row.comparison_baseline_pnl_units_6dp.is_none()
            || row.canonical_pnl_delta_units_6dp.is_none()
    });
    let offset_metrics_missing = measurement.symbols.iter().any(|row| {
        row.paired_learner.is_none()
            || row.paired_market_prior.is_none()
            || row.first_minute_final_scores.is_none()
            || row.actual_first_scores.is_none()
            || row.prediction_unavailable.is_none()
            || row.prequentially_untrusted.is_none()
            || row.prequential_trust_unavailable.is_none()
            || row.uncertainty_availability.is_none()
    });
    let offset_uncertainty_omitted = measurement.symbols.iter().any(|row| {
        row.uncertainty_availability
            .as_ref()
            .is_some_and(|availability| availability.omitted_fields != 0)
    });
    let offset_uncertainty_invalid = measurement.symbols.iter().any(|row| {
        row.uncertainty_availability
            .as_ref()
            .is_some_and(|availability| availability.invalid_fields != 0)
    });
    let coverage = &mut measurement.coverage;
    coverage.incomplete_reasons.extend(
        [
            (coverage.run_start_markers != 1, "run_start_marker_count"),
            (coverage.run_end_markers != 1, "run_end_marker_count"),
            (
                coverage.timing_identity_markers != 1,
                "execution_timing_marker_count",
            ),
            (coverage.identity_failures != 0, "trace_identity_mismatch"),
            (coverage.malformed_lines != 0, "malformed_trace_lines"),
            (coverage.missing_scope_events != 0, "missing_worker_scope"),
            (
                coverage.duplicate_conflicts != 0,
                "conflicting_duplicate_observations",
            ),
            (
                coverage.probability_events == 0,
                "missing_probability_observations",
            ),
            (
                coverage.resolved_label_events == 0,
                "missing_resolved_labels",
            ),
            (
                coverage.common_paired_observations == 0,
                "missing_common_paired_observations",
            ),
            (
                coverage.reference_provenance_failures != 0,
                "reference_provenance_mismatch",
            ),
            (
                coverage.missing_first_snapshot_joins != 0,
                "missing_actual_first_snapshot_join",
            ),
            (
                coverage.latched_first_without_original_snapshot != 0,
                "latched_first_without_original_snapshot",
            ),
            (
                coverage.unobserved_initial_first_boundaries != 0,
                "unobserved_initial_first_boundary",
            ),
            (
                coverage.unexpected_initial_first_boundaries != 0,
                "unexpected_initial_first_boundary",
            ),
            (
                coverage.flood_control_enabled,
                "trace_flood_control_enabled",
            ),
            (
                coverage.flood_suppressed_events != 0,
                "trace_events_suppressed",
            ),
            (coverage.sink_dropped_lines != 0, "trace_sink_dropped_lines"),
            (
                coverage.runtime_log_capacity_bytes != 0
                    && coverage.runtime_log_bytes >= coverage.runtime_log_capacity_bytes,
                "runtime_log_capacity_reached",
            ),
            (missing_pnl, "canonical_pnl_comparison_unavailable"),
        ]
        .into_iter()
        .filter(|(failed, _)| *failed)
        .map(|(_, reason)| reason.to_string()),
    );
    if variant == BacktestOracleResearchVariant::Combined && coverage.common_variant_count != 5 {
        coverage
            .incomplete_reasons
            .push("combined_comparison_variants_missing".to_string());
    }
    if variant.uses_final_output_trust()
        || matches!(
            variant,
            BacktestOracleResearchVariant::MarketOffset
                | BacktestOracleResearchVariant::MarketOffsetPhaseTrust
                | BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust
        )
    {
        let expected_variants = if experiment.uses_final_output_trust()
            || matches!(
                experiment,
                BacktestOracleResearchVariant::MarketOffsetPhaseTrust
                    | BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust
            ) {
            2
        } else {
            3
        };
        for (failed, reason) in [
            (
                coverage.common_variant_count != expected_variants,
                "market_offset_comparison_variants_missing",
            ),
            (offset_metrics_missing, "market_offset_metrics_missing"),
            (
                offset_uncertainty_omitted,
                "market_offset_uncertainty_evidence_omitted",
            ),
            (
                offset_uncertainty_invalid,
                "market_offset_uncertainty_evidence_invalid",
            ),
        ] {
            if failed {
                coverage.incomplete_reasons.push(reason.to_string());
            }
        }
    }
    if let Some(reason) = oracle_research::incomplete_evidence_reason(variant, &measurement.symbols)
    {
        coverage.incomplete_reasons.push(reason.to_string());
    }
    if variant == BacktestOracleResearchVariant::DirectMarket {
        if coverage.common_variant_count != 2 {
            coverage
                .incomplete_reasons
                .push("direct_market_comparison_variants_missing".to_string());
        }
        if let Some(direct) = &coverage.direct_market {
            for (failed, reason) in [
                (
                    direct.selected_events == 0,
                    "direct_market_branch_unexercised",
                ),
                (
                    direct.predicate_violations != 0,
                    "direct_market_predicate_violation",
                ),
                (
                    direct.mean_identity_failures != 0,
                    "direct_market_mean_identity",
                ),
                (
                    direct.interval_identity_failures != 0,
                    "direct_market_interval_identity",
                ),
                (
                    direct.beta_observation_identity_failures != 0,
                    "direct_market_beta_observation_identity",
                ),
                (
                    direct.confidence_transform_violations != 0,
                    "direct_market_confidence_transform",
                ),
                (
                    direct.beta_transform_violations != 0,
                    "direct_market_beta_transform",
                ),
                (
                    direct.materialization_identity_failures != 0,
                    "direct_market_materialization_identity",
                ),
                (
                    direct.selected_unknown_predicate_events != 0,
                    "direct_market_selected_predicate_unknown",
                ),
                (
                    direct.unknown_predicate_events != 0,
                    "direct_market_predicate_unknown",
                ),
                (
                    direct.prediction_validity_disagreements != 0,
                    "direct_market_prediction_validity_disagreement",
                ),
            ] {
                if failed {
                    coverage.incomplete_reasons.push(reason.to_string());
                }
            }
        }
    }
    coverage.complete = coverage.incomplete_reasons.is_empty();
}

fn attach_financial(
    rows: &mut [OracleResearchSymbolMetrics],
    variant: BacktestOracleResearchVariant,
    pnl: &impl Fn(BacktestOracleResearchVariant, &str) -> Option<i128>,
) {
    let baseline = match variant {
        BacktestOracleResearchVariant::OperationalBaseline
        | BacktestOracleResearchVariant::TradeFlow
        | BacktestOracleResearchVariant::EqualStartBaseline
        | BacktestOracleResearchVariant::DirectMarket
        | BacktestOracleResearchVariant::MarketOffsetFinalOutputPhaseTrust => {
            BacktestOracleResearchVariant::OperationalBaseline
        }
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust => {
            BacktestOracleResearchVariant::MarketOffset
        }
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust => {
            BacktestOracleResearchVariant::MarketOffsetPhaseTrust
        }
        BacktestOracleResearchVariant::MarketOffset
        | BacktestOracleResearchVariant::ReferenceOnly
        | BacktestOracleResearchVariant::RepresentationOnly
        | BacktestOracleResearchVariant::Combined => {
            BacktestOracleResearchVariant::EqualStartBaseline
        }
    };
    for row in rows {
        row.canonical_cohort_pnl_units_6dp = pnl(variant, &row.symbol);
        row.operational_baseline_pnl_units_6dp = pnl(
            BacktestOracleResearchVariant::OperationalBaseline,
            &row.symbol,
        );
        row.comparison_baseline = baseline.as_str().to_string();
        row.comparison_baseline_pnl_units_6dp = pnl(baseline, &row.symbol);
        row.canonical_pnl_delta_units_6dp = row
            .canonical_cohort_pnl_units_6dp
            .zip(row.comparison_baseline_pnl_units_6dp)
            .and_then(|(current, baseline)| current.checked_sub(baseline));
    }
}

fn set_difference(left: Option<&BTreeSet<String>>, right: Option<&BTreeSet<String>>) -> u64 {
    left.map_or(0, |left| {
        left.iter()
            .filter(|market| right.is_none_or(|right| !right.contains(*market)))
            .count() as u64
    })
}

fn canonical_first_markets(
    report: &BacktestMatrixReportArtifact,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut markets = BTreeMap::<String, BTreeSet<String>>::new();
    for outcome in &report
        .canonical_session_stats
        .canonical
        .owned_leg_transition_history
    {
        if outcome.close_reason
            == Some(crate::strategy::CtfOwnedLegCloseReason::PairedFirstManagedSell)
        {
            markets
                .entry(outcome.symbol.clone())
                .or_default()
                .insert(outcome.market_id.to_string());
        }
    }
    markets
}

fn canonical_pnl(report: &BacktestMatrixReportArtifact) -> BTreeMap<String, Option<i128>> {
    report
        .canonical_session_stats
        .canonical
        .ctf_summary
        .iter()
        .filter(|row| row.symbol != "TOTAL")
        .map(|row| (row.symbol.clone(), row.realized_pnl.units_6dp()))
        .collect()
}

#[derive(Clone, Debug, Serialize)]
pub struct BacktestMatrixSymbolArtifact {
    pub symbol: String,
    pub status: String,
    pub error: Option<String>,
}

pub(super) fn build_report_artifact(
    candidate_id: &str,
    capture: &Path,
    root: &Path,
    failed: bool,
    run_identity: BacktestRunIdentity,
    ohlc_manifest: ReplayOhlcManifestSummary,
    live_latency: &ReplayLiveLatencySummary,
    ctf_split_latency: &ReplayCtfSplitLatencySummary,
    execution_timing: &ReplayExecutionTimingModel,
    replay_runtime: BacktestMatrixReplayRuntimeArtifact,
    diagnostics: BacktestMatrixDiagnostics,
    canonical_session_stats: CanonicalSessionStats,
    portfolio_accounting: PortfolioAccountingProjectionProof,
    recorded_market_coverage: Option<RecordedMarketCoverageReport>,
    rows: &[SymbolRow],
) -> BacktestMatrixReportArtifact {
    BacktestMatrixReportArtifact {
        schema_version: 56,
        candidate_id: candidate_id.to_string(),
        capture_session: capture
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("capture")
            .to_string(),
        output_root: root.display().to_string(),
        failed,
        proof_metadata: BacktestProofMetadata::normal_backtest(),
        run_identity,
        ohlc_manifest,
        replay_assumptions: replay_assumptions_artifact(
            live_latency,
            ctf_split_latency,
            execution_timing,
        ),
        replay_runtime,
        diagnostics,
        canonical_session_stats,
        portfolio_accounting,
        recorded_market_coverage,
        oracle_learning_initial_source: String::new(),
        oracle_learning_initial_sha256: None,
        oracle_learning_initial_cutoffs_ms: BTreeMap::new(),
        oracle_learning_initial_metrics: BTreeMap::new(),
        oracle_learning_final_path: String::new(),
        oracle_learning_final_sha256: String::new(),
        oracle_learning_final_cutoffs_ms: BTreeMap::new(),
        oracle_learning_final_metrics: BTreeMap::new(),
        oracle_research: None,
        symbols: rows
            .iter()
            .map(|row| BacktestMatrixSymbolArtifact {
                symbol: row.symbol.clone(),
                status: row.status.clone(),
                error: row.error.clone(),
            })
            .collect(),
    }
}

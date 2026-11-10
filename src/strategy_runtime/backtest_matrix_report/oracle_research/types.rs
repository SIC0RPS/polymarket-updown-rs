use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::strategy::OracleLearningSnapshot;
use crate::strategy_runtime::backtest_matrix::oracle_research::BacktestOracleResearchVariant;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchArtifact {
    pub schema_version: u32,
    pub variant: String,
    pub capture: String,
    pub capture_run_id: String,
    pub capture_start_ts_ms: u64,
    pub capture_end_ts_ms: u64,
    pub frozen_manifest_sha256: String,
    pub common_input_identity_sha256: String,
    pub execution_timing_sha256: String,
    pub frozen_input_sha256: BTreeMap<String, String>,
    pub prepared_payload_sha256: BTreeMap<String, String>,
    pub seed_source: String,
    pub seed_initial_sha256: String,
    pub seed_final_sha256: String,
    pub seed_transformed_once: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_policy: Option<String>,
    pub runtime_log_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison_identity: Option<OracleResearchComparisonIdentity>,
    pub coverage: OracleResearchCoverage,
    pub symbols: Vec<OracleResearchSymbolMetrics>,
    #[serde(default)]
    pub completed_variant_comparison: Vec<OracleResearchVariantMeasurement>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchComparisonIdentity {
    pub candidate_run_identity: Value,
    pub reference_run_identity: Value,
    pub candidate_seed_initial_sha256: String,
    pub reference_seed_initial_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_application_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_application_policy: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchCoverage {
    pub complete: bool,
    pub incomplete_reasons: Vec<String>,
    pub runtime_log_bytes: u64,
    pub runtime_log_capacity_bytes: u64,
    pub parsed_lines: u64,
    pub malformed_lines: u64,
    pub run_start_markers: u64,
    pub run_end_markers: u64,
    pub timing_identity_markers: u64,
    pub probability_events: u64,
    pub unavailable_probability_events: u64,
    pub resolved_label_events: u64,
    pub first_authorization_events: u64,
    pub missing_scope_events: u64,
    pub duplicate_conflicts: u64,
    pub identity_failures: u64,
    pub missing_first_snapshot_joins: u64,
    pub unobserved_initial_first_boundaries: u64,
    pub unexpected_initial_first_boundaries: u64,
    pub latched_first_without_original_snapshot: u64,
    pub missing_required_features: u64,
    pub reference_provenance_failures: u64,
    pub common_variant_count: usize,
    pub common_paired_observations: u64,
    pub flood_control_enabled: bool,
    pub flood_suppressed_events: u64,
    pub sink_dropped_lines: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_market: Option<OracleResearchDirectMarketCoverage>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchPhaseTrustDiagnostics {
    pub application_policy: String,
    pub population: String,
    pub source_observations: u64,
    pub observations: u64,
    pub label_unavailable_observations: u64,
    pub label_conflict_observations: u64,
    pub eligible_observations: u64,
    pub unavailable_observations: u64,
    pub ineligible_with_phase_facts: u64,
    pub policy_available: u64,
    pub policy_missing: u64,
    pub policy_mismatch: u64,
    pub policy_field_absent: u64,
    pub eligible_policy_missing: u64,
    pub phase_available: u64,
    pub phase_missing: u64,
    pub phase_unrecognized: u64,
    pub phase_field_absent: u64,
    pub phase_identity_mismatch: u64,
    pub phase_identity_unverifiable: u64,
    pub incoming_valid: u64,
    pub incoming_missing: u64,
    pub incoming_invalid: u64,
    pub incoming_field_absent: u64,
    pub prospective_valid: u64,
    pub prospective_missing: u64,
    pub prospective_invalid: u64,
    pub prospective_field_absent: u64,
    pub support_count_available: u64,
    pub support_count_missing: u64,
    pub support_count_field_absent: u64,
    pub trusted: u64,
    pub untrusted: u64,
    pub trust_missing: u64,
    pub trust_field_absent: u64,
    pub actual_applied: u64,
    pub actually_contributing: u64,
    pub phases: Vec<OracleResearchPhaseTrustMetrics>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchPhaseTrustMetrics {
    pub phase: String,
    pub observations: u64,
    pub valid_pairs: u64,
    pub scored_pairs: u64,
    pub incoming_missing: u64,
    pub incoming_invalid: u64,
    pub prospective_missing: u64,
    pub prospective_invalid: u64,
    pub support_count_available: u64,
    pub support_count_missing: u64,
    pub support_count_min: Option<u64>,
    pub support_count_max: Option<u64>,
    pub trusted: u64,
    pub untrusted: u64,
    pub trust_missing: u64,
    pub actual_applied: u64,
    pub actually_contributing: u64,
    pub incoming: OracleResearchModelScores,
    pub prospective: OracleResearchModelScores,
    pub prospective_corrected: u64,
    pub prospective_damaged: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchFinalOutputDiagnostics {
    pub application_policy: String,
    pub retained_source_rows: u64,
    pub retained_outside_replay_scope: u64,
    pub retained_label_conflict: u64,
    pub retained_label_mismatch: u64,
    pub retained_chronology_failures: u64,
    pub retained_phase_unrecognized: u64,
    pub retained_phase_identity_mismatch: u64,
    pub published_source_observations: u64,
    pub published_noncommon_excluded: u64,
    pub published_label_unavailable: u64,
    pub published_label_conflict: u64,
    pub published_phase_unavailable: u64,
    pub phases: Vec<OracleResearchFinalOutputPhaseMetrics>,
    pub published: Vec<OracleResearchPublishedPhaseMetrics>,
}

impl OracleResearchFinalOutputDiagnostics {
    pub(in crate::strategy_runtime::backtest_matrix_report) fn incomplete(&self) -> bool {
        let retained = self
            .phases
            .iter()
            .map(|phase| phase.observations)
            .sum::<u64>();
        let published = self
            .published
            .iter()
            .map(|row| row.observations)
            .sum::<u64>();
        self.application_policy
            != OracleLearningSnapshot::LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_FINAL_OUTPUT_PHASE_TRUST
            || self.retained_source_rows
                != retained
                    + self.retained_outside_replay_scope
                    + self.retained_label_conflict
                    + self.retained_label_mismatch
                    + self.retained_chronology_failures
                    + self.retained_phase_unrecognized
                    + self.retained_phase_identity_mismatch
            || self.published_source_observations
                != published
                    + self.published_noncommon_excluded
                    + self.published_label_unavailable
                    + self.published_label_conflict
                    + self.published_phase_unavailable
            || self.retained_label_conflict
                + self.retained_label_mismatch
                + self.retained_chronology_failures
                + self.retained_phase_unrecognized
                + self.retained_phase_identity_mismatch
                + self.published_label_conflict
                != 0
            || self.phases.len() != 5
            || self.published.len() != 30
            || self.phases.iter().any(|phase| {
                !phase.f0.complete_for(phase.observations)
                    || !phase.f1.complete_for(phase.observations)
                    || phase.f0.unavailable
                        + phase.f0.invalid
                        + phase.f1.unavailable
                        + phase.f1.invalid
                        != 0
                    || phase.runtime_join_unique
                        + phase.runtime_join_missing
                        + phase.runtime_join_ambiguous
                        != phase.observations
                    || phase.selected_f0 + phase.selected_f1 != phase.runtime_join_unique
                    || phase.selected_published_equal
                        + phase.selected_published_mismatch
                        + phase.selected_unavailable
                        != phase.observations
                    || phase.selected_published_equal + phase.selected_published_mismatch
                        > phase.runtime_join_unique
                    || phase.selected_unavailable
                        < phase.runtime_join_missing + phase.runtime_join_ambiguous
                    || phase.selected_published_mismatch != 0
                    || phase.mode_market + phase.mode_cleanup + phase.mode_unavailable
                        != phase.runtime_join_unique
                    || phase.materialization + phase.nonmaterialization
                        != phase.runtime_join_unique
                    || phase.branch_applied != phase.selected_f1
                    || phase.branch_applied > phase.application_attempted
                    || phase.actually_contributing > phase.branch_applied
            })
            || self.published.iter().any(|row| {
                !row.final_probability.complete_for(row.observations)
                    || !row.emitted_signal.complete_for(row.observations)
                    || row.emitted_signal.field_absent != 0
            })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchFinalOutputPhaseMetrics {
    pub phase: String,
    pub observations: u64,
    pub f0: OracleResearchProbabilityScores,
    pub f1: OracleResearchProbabilityScores,
    pub runtime_join_unique: u64,
    pub runtime_join_missing: u64,
    pub runtime_join_ambiguous: u64,
    pub selected_f0: u64,
    pub selected_f1: u64,
    pub selected_unavailable: u64,
    pub selected_published_equal: u64,
    pub selected_published_mismatch: u64,
    pub mode_market: u64,
    pub mode_cleanup: u64,
    pub mode_unavailable: u64,
    pub materialization: u64,
    pub nonmaterialization: u64,
    pub application_attempted: u64,
    pub branch_applied: u64,
    pub actually_contributing: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchPublishedPhaseMetrics {
    pub population: String,
    pub phase: String,
    pub observations: u64,
    pub final_probability: OracleResearchProbabilityScores,
    pub emitted_signal: OracleResearchSignalScores,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchProbabilityScores {
    pub scores: OracleResearchModelScores,
    pub unavailable: u64,
    pub invalid: u64,
    pub direction_up: u64,
    pub direction_down: u64,
    pub direction_ties: u64,
}

impl OracleResearchProbabilityScores {
    fn complete_for(&self, observations: u64) -> bool {
        self.scores.observations + self.unavailable + self.invalid == observations
            && self.direction_up + self.direction_down + self.direction_ties
                == self.scores.observations
            && self.scores.direction_observations.unwrap_or_default()
                + self.scores.direction_ties.unwrap_or_default()
                == self.scores.observations
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchSignalScores {
    pub observations: u64,
    pub field_absent: u64,
    pub abstained: u64,
    pub direction_up: u64,
    pub direction_down: u64,
    pub unrecognized: u64,
    pub accuracy: OracleResearchAccuracy,
}

impl OracleResearchSignalScores {
    fn complete_for(&self, observations: u64) -> bool {
        self.observations == observations
            && self.field_absent
                + self.abstained
                + self.direction_up
                + self.direction_down
                + self.unrecognized
                == observations
            && self.accuracy.observations == self.direction_up + self.direction_down
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchFirstDecisionDiagnostics {
    pub application_policy: String,
    pub original_first_markers: u64,
    pub retention_events: u64,
    pub retained_windows: u64,
    pub marker_retained: u64,
    pub retention_sample_available: u64,
    pub retention_sample_unavailable: u64,
    pub unavailable_missing_snapshot_timestamp: u64,
    pub unavailable_exact_observation: u64,
    pub unavailable_prospective_contribution: u64,
    pub unavailable_pending_capacity: u64,
    pub unavailable_reason_unrecognized: u64,
    pub retention_field_absent: u64,
    pub retention_identity_failures: u64,
    pub repeated_authorization_attempts: u64,
    pub expected_resolution_events: u64,
    pub resolution_events: u64,
    pub resolved_windows: u64,
    pub resolution_marker_available: u64,
    pub resolution_sample_available: u64,
    pub resolution_sample_unavailable: u64,
    pub resolution_field_absent: u64,
    pub retained_resolution_pairs: u64,
    pub retained_resolution_mismatches: u64,
    pub retained_without_resolution: u64,
    pub samples_merged: u64,
    pub first_fit_rows_added: u64,
    pub fitting_identity_failures: u64,
    pub resolution_chronology_failures: u64,
    pub first_support_count_min: Option<u64>,
    pub first_support_count_max: Option<u64>,
    pub fit_sample_count_min: Option<u64>,
    pub fit_sample_count_max: Option<u64>,
    pub evaluation_observations: u64,
    pub eligible_trust_observations: u64,
    pub trust_source_first: u64,
    pub trust_source_phase: u64,
    pub trust_source_missing: u64,
    pub trust_source_unrecognized: u64,
    pub trust_field_absent: u64,
    pub trust_identity_failures: u64,
    pub trust_sample_count_available: u64,
    pub trust_sample_count_missing: u64,
    pub first_sample_count_available: u64,
    pub first_sample_count_missing: u64,
    pub evaluation_fit_sample_count_available: u64,
    pub evaluation_fit_sample_count_missing: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct OracleResearchDirectMarketCoverage {
    pub selected_events: u64,
    pub materialization_selected_events: u64,
    pub predicate_violations: u64,
    pub unknown_predicate_events: u64,
    pub selected_unknown_predicate_events: u64,
    pub market_scope_events: u64,
    pub cleanup_scope_events: u64,
    pub mode_unavailable_events: u64,
    pub open_scope_events: u64,
    pub closed_scope_events: u64,
    pub close_scope_unavailable_events: u64,
    pub learning_enabled_events: u64,
    pub learning_disabled_events: u64,
    pub learning_unavailable_events: u64,
    pub fit_available_events: u64,
    pub fit_missing_events: u64,
    pub fit_unavailable_events: u64,
    pub prediction_valid_events: u64,
    pub prediction_invalid_events: u64,
    pub prediction_validity_unavailable_events: u64,
    pub prediction_validity_disagreements: u64,
    pub trust_false_events: u64,
    pub trust_true_events: u64,
    pub trust_unavailable_events: u64,
    pub market_probability_valid_events: u64,
    pub market_probability_invalid_events: u64,
    pub market_probability_unavailable_events: u64,
    pub interval_unavailable_events: u64,
    pub interval_field_absent_events: u64,
    pub mean_identity_failures: u64,
    pub interval_identity_failures: u64,
    pub beta_observation_identity_failures: u64,
    pub confidence_transform_violations: u64,
    pub beta_transform_violations: u64,
    pub materialization_identity_failures: u64,
}

impl OracleResearchDirectMarketCoverage {
    pub(super) fn add(&mut self, event: &Self) {
        macro_rules! add {
            ($($field:ident),+ $(,)?) => {
                $(self.$field += event.$field;)+
            };
        }
        add!(
            selected_events,
            materialization_selected_events,
            predicate_violations,
            unknown_predicate_events,
            selected_unknown_predicate_events,
            market_scope_events,
            cleanup_scope_events,
            mode_unavailable_events,
            open_scope_events,
            closed_scope_events,
            close_scope_unavailable_events,
            learning_enabled_events,
            learning_disabled_events,
            learning_unavailable_events,
            fit_available_events,
            fit_missing_events,
            fit_unavailable_events,
            prediction_valid_events,
            prediction_invalid_events,
            prediction_validity_unavailable_events,
            prediction_validity_disagreements,
            trust_false_events,
            trust_true_events,
            trust_unavailable_events,
            market_probability_valid_events,
            market_probability_invalid_events,
            market_probability_unavailable_events,
            interval_unavailable_events,
            interval_field_absent_events,
            mean_identity_failures,
            interval_identity_failures,
            beta_observation_identity_failures,
            confidence_transform_violations,
            beta_transform_violations,
            materialization_identity_failures,
        );
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchSymbolMetrics {
    pub symbol: String,
    pub labeled_markets: u64,
    pub labeled_observations: u64,
    pub common_paired_observations: u64,
    pub missing_features: u64,
    pub ties: u64,
    pub learner: OracleResearchModelScores,
    pub market_prior: OracleResearchModelScores,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_learner: Option<OracleResearchModelScores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_market_prior: Option<OracleResearchModelScores>,
    pub market_raw: OracleResearchModelScores,
    pub original_base: OracleResearchModelScores,
    pub final_probability: OracleResearchModelScores,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_minute_final_scores: Option<OracleResearchModelScores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_first_scores: Option<OracleResearchModelScores>,
    pub first_minute_final_accuracy: OracleResearchAccuracy,
    pub actual_first_accuracy: OracleResearchAccuracy,
    pub later_horizon_final_accuracy: OracleResearchAccuracy,
    pub actual_first_authorizations: u64,
    pub actual_first_attributed: u64,
    pub actual_first_missing_original_snapshot: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_first_label_available: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_first_label_unavailable: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_first_label_conflict: Option<u64>,
    pub unobserved_initial_first_boundaries: u64,
    #[serde(default)]
    pub unexpected_initial_first_boundaries: u64,
    pub fit_available: u64,
    pub prediction_available: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction_unavailable: Option<u64>,
    pub prequentially_trusted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prequentially_untrusted: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prequential_trust_unavailable: Option<u64>,
    pub application_attempted: u64,
    pub branch_applied: u64,
    pub actually_contributing: u64,
    pub learner_corrected: u64,
    pub learner_damaged: u64,
    pub calibration_corrected: u64,
    pub calibration_damaged: u64,
    pub final_corrected: u64,
    pub final_damaged: u64,
    pub probability_calibration_attempted: u64,
    pub probability_calibration_effective: u64,
    pub beta_calibration_attempted: u64,
    pub beta_calibration_applied: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty_availability: Option<OracleResearchUncertaintyAvailability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_market_scores: Option<OracleResearchDirectMarketScores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_market: Option<OracleResearchDirectMarketCoverage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_trust: Option<OracleResearchPhaseTrustDiagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_output: Option<OracleResearchFinalOutputDiagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_decision: Option<OracleResearchFirstDecisionDiagnostics>,
    pub canonical_cohort_pnl_units_6dp: Option<i128>,
    pub operational_baseline_pnl_units_6dp: Option<i128>,
    pub comparison_baseline: String,
    pub comparison_baseline_pnl_units_6dp: Option<i128>,
    pub canonical_pnl_delta_units_6dp: Option<i128>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchUncertaintyAvailability {
    pub learner_and_market: u64,
    pub learner_only: u64,
    pub market_only: u64,
    pub neither: u64,
    pub omitted_fields: u64,
    pub invalid_fields: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchDirectMarketScores {
    pub paired_population: OracleResearchModelScores,
    pub affected_population: OracleResearchModelScores,
    pub first_minute: OracleResearchModelScores,
    pub affected_first_minute: OracleResearchModelScores,
    pub actual_first: OracleResearchModelScores,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchVariantMeasurement {
    pub variant: String,
    pub coverage: OracleResearchCoverage,
    pub symbols: Vec<OracleResearchSymbolMetrics>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchModelScores {
    pub observations: u64,
    pub markets: u64,
    pub brier: Option<f64>,
    pub log_loss: Option<f64>,
    pub direction_accuracy: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction_observations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction_ties: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OracleResearchAccuracy {
    pub observations: u64,
    pub markets: u64,
    pub equal_market_weight_accuracy: Option<f64>,
}

pub(in crate::strategy_runtime::backtest_matrix_report) struct OracleResearchMeasurement {
    pub coverage: OracleResearchCoverage,
    pub symbols: Vec<OracleResearchSymbolMetrics>,
    pub first_direction_markets: BTreeMap<String, BTreeSet<String>>,
}

pub(in crate::strategy_runtime::backtest_matrix_report) struct CompletedOracleResearchMeasurement {
    pub variant: BacktestOracleResearchVariant,
    pub measurement: OracleResearchMeasurement,
}

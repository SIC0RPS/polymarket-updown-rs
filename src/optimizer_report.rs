use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::config::OptimizerParameterValue;
use crate::portfolio::PortfolioAccountingPnlRow;
use crate::strategy_runtime::prediction_metrics::PredictionMetrics;
use crate::strategy_runtime::replay_mode::{
    RecordedMarketCoverageReport, RecordedMarketPopulation,
};
use crate::trade_session_ledger::{
    CanonicalCoverageProof, CanonicalCtfExitCohortRow, CanonicalCtfExitSummaryRow,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OptimizerObjectiveScoreReport {
    pub numerator: i128,
    pub denominator: i128,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OptimizerObjectiveProfileReport {
    pub formula_type: String,
    pub expression: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerCaptureReport {
    pub capture: String,
    pub role: String,
    pub session_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_market_population: Option<RecordedMarketPopulation>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn profit_selection_mode() -> String {
    "profit".into()
}

fn is_profit_selection_mode(value: &str) -> bool {
    value == "profit"
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerCaptureLearnerReport {
    pub role: String,
    pub capture: String,
    pub initial_oracle_learning_sha256: String,
    pub initial_oracle_learning_sha256_by_symbol: BTreeMap<String, String>,
    pub initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerSelectedLearningBindingReport {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerRunProvenanceReport {
    #[serde(
        default = "profit_selection_mode",
        skip_serializing_if = "is_profit_selection_mode"
    )]
    pub selection_mode: String,
    pub optimizer_binary_sha256: String,
    pub app_config_sha256: String,
    pub root_spec_sha256: String,
    pub selection_final_oracle_learning_sha256: Option<String>,
    pub validation_final_oracle_learning_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_learning_bindings:
        Option<BTreeMap<String, OptimizerSelectedLearningBindingReport>>,
    pub capture_learners: Vec<OptimizerCaptureLearnerReport>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerCandidateReport {
    #[serde(
        default = "profit_selection_mode",
        skip_serializing_if = "is_profit_selection_mode"
    )]
    pub selection_mode: String,
    pub candidate_id: String,
    pub origin: String,
    pub origin_id: Option<String>,
    pub params_by_symbol: BTreeMap<String, BTreeMap<String, OptimizerParameterValue>>,
    pub exercised: bool,
    pub pnl_by_symbol: BTreeMap<String, PortfolioAccountingPnlRow>,
    pub objective_score: Option<OptimizerObjectiveScoreReport>,
    pub shortfall_units_6dp: u128,
    pub all_symbols_profitable: bool,
    pub entry_count: u64,
    pub profitable_market_count: u64,
    pub split_entries: u64,
    pub submitted_orders: u64,
    pub managed_sell_fills: u64,
    pub wrong_side_first_managed_sell: u64,
    pub evaluation_scope: String,
    pub accounting_coverage: Option<CanonicalCoverageProof>,
    pub canonical_ctf_exit_summary: Vec<CanonicalCtfExitSummaryRow>,
    pub canonical_ctf_exit_cohorts: Vec<CanonicalCtfExitCohortRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_market_coverage: Option<RecordedMarketCoverageReport>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub prediction_metrics: BTreeMap<String, PredictionMetrics>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerSymbolSearchReport {
    #[serde(
        default = "profit_selection_mode",
        skip_serializing_if = "is_profit_selection_mode"
    )]
    pub selection_mode: String,
    pub symbol: String,
    pub status: String,
    pub seed: u64,
    pub configured_max_function_evals: usize,
    pub configured_max_generations_per_run: usize,
    pub configured_max_no_improvement_evals: usize,
    pub search_evaluations: usize,
    pub no_improvement_evaluations: usize,
    pub search_runs: usize,
    pub exercised_trials: usize,
    pub default_candidate_id: String,
    pub selected_candidate_id: String,
    pub selected_params: BTreeMap<String, OptimizerParameterValue>,
    pub artifact_identity: String,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerSymbolEconomicReport {
    pub symbol: String,
    pub selection_candidate_pnl: Option<PortfolioAccountingPnlRow>,
    pub selection_incumbent_pnl: Option<PortfolioAccountingPnlRow>,
    pub validation_candidate_pnl: Option<PortfolioAccountingPnlRow>,
    pub validation_incumbent_pnl: Option<PortfolioAccountingPnlRow>,
    pub validation_delta_units_6dp: Option<i128>,
    pub selection_candidate_entries: Option<u64>,
    pub selection_candidate_cohorts: Option<u64>,
    pub validation_candidate_entries: Option<u64>,
    pub validation_candidate_cohorts: Option<u64>,
    pub selection_profitable: bool,
    pub validation_profitable: Option<bool>,
    pub beats_validation_incumbent: Option<bool>,
    pub economic_pass: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_candidate_brier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection_incumbent_brier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_candidate_brier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_incumbent_brier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction_comparable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction_improved: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerValidationReport {
    pub status: String,
    pub capture: Option<OptimizerCaptureReport>,
    pub candidate: Option<OptimizerCandidateReport>,
    pub incumbent: Option<OptimizerCandidateReport>,
    pub per_symbol: Vec<OptimizerSymbolEconomicReport>,
    pub all_symbols_pass: Option<bool>,
    pub error: Option<String>,
}

impl OptimizerValidationReport {
    pub fn economic_status(&self) -> String {
        ["BTC", "ETH", "SOL", "XRP"]
            .map(|symbol| {
                let result = self
                    .per_symbol
                    .iter()
                    .find(|row| row.symbol == symbol)
                    .and_then(|row| row.economic_pass);
                let status = match result {
                    Some(true) => "pass",
                    Some(false) => "fail",
                    None if self.status.starts_with("not_run") || self.status == "pending" => {
                        "pending"
                    }
                    None => "unavailable",
                };
                format!("{symbol}={status}")
            })
            .join(", ")
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerCaptureReplayReport {
    pub capture: OptimizerCaptureReport,
    pub candidate: Option<OptimizerCandidateReport>,
    pub structural_validation_status: String,
    pub structural_validation_issues: Vec<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OptimizerStructuralReport {
    pub validation_status: String,
    pub validation_issues: Vec<String>,
    pub capture_replays: Vec<OptimizerCaptureReplayReport>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OptimizerStatusArtifact {
    #[serde(
        default = "profit_selection_mode",
        skip_serializing_if = "is_profit_selection_mode"
    )]
    pub selection_mode: String,
    pub schema_version: u32,
    pub objective_profile: OptimizerObjectiveProfileReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<OptimizerRunProvenanceReport>,
    pub phase: String,
    pub run_root: String,
    pub captures: Vec<OptimizerCaptureReport>,
    #[serde(default)]
    pub symbol_searches: Vec<OptimizerSymbolSearchReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capture_replays: Vec<OptimizerCaptureReplayReport>,
    pub search_trials: usize,
    pub exercised_trials: usize,
    pub selection_status: String,
    pub validation_status: String,
    pub structural_status: String,
    pub economic_status: String,
    pub application_status: String,
    pub application_reason: String,
    pub incumbent_candidate: Option<OptimizerCandidateReport>,
    pub selected_candidate: Option<OptimizerCandidateReport>,
    pub rechecked_candidate: Option<OptimizerCandidateReport>,
    pub validation: Option<OptimizerValidationReport>,
    pub applied: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OptimizerSummaryArtifact {
    #[serde(
        default = "profit_selection_mode",
        skip_serializing_if = "is_profit_selection_mode"
    )]
    pub selection_mode: String,
    pub schema_version: u32,
    pub objective_profile: OptimizerObjectiveProfileReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<OptimizerRunProvenanceReport>,
    pub status: String,
    pub run_root: String,
    pub captures: Vec<OptimizerCaptureReport>,
    #[serde(default)]
    pub symbol_searches: Vec<OptimizerSymbolSearchReport>,
    pub search_trials: usize,
    pub exercised_trials: usize,
    pub selection_status: String,
    pub validation_status: String,
    pub structural_status: String,
    pub economic_status: String,
    pub application_status: String,
    pub application_reason: String,
    pub incumbent_candidate: Option<OptimizerCandidateReport>,
    pub selected_candidate: Option<OptimizerCandidateReport>,
    pub rechecked_candidate: Option<OptimizerCandidateReport>,
    pub validation: Option<OptimizerValidationReport>,
    pub structural_recheck: Option<OptimizerStructuralReport>,
    pub candidate_result_path: String,
    pub applied: bool,
}

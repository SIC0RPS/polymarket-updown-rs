use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestMatrixReplayRuntimeArtifact {
    pub clob_model_version: String,
    pub clob_model_sha256: String,
    pub clob_captured_rows: u64,
    pub clob_submit_latency_ms: u64,
    pub clob_itode_delay_ms: u64,
    pub pending_clob_submissions: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_clob_receipts: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clob_supported_admission_submissions: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clob_approximate_admission_submissions: Option<u64>,
    pub clob_source_capture: Option<String>,
    pub clob_source_config_sha256: Option<String>,
    pub clob_journal_path: Option<String>,
    pub clob_journal_sha256: Option<String>,
    pub clob_endpoint: String,
    pub clob_unit: String,
    pub clob_current_rule_source: String,
    pub clob_timing_diagnostics: Vec<BacktestClobTimingDiagnostic>,
    pub ctf_model_version: String,
    pub ctf_model_sha256: String,
    pub ctf_captured_rows: u64,
    pub ctf_materialization_latency_ms: u64,
    pub pending_ctf_split_receipts: usize,
    pub ctf_source_capture: Option<String>,
    pub ctf_source_config_sha256: Option<String>,
    pub ctf_journal_path: Option<String>,
    pub ctf_journal_sha256: Option<String>,
    pub ctf_endpoint: String,
    pub ctf_unit: String,
    pub ctf_timing_diagnostics: Vec<BacktestCtfTimingDiagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestClobTimingDiagnostic {
    pub order_id: String,
    pub sample_index: usize,
    pub accepted_ts_ms: u64,
    pub simulated_request_send_us: u64,
    pub sampled_response_duration_us: u64,
    pub qualifying_current_venue_hold_us: u64,
    pub modeled_delay_us: u64,
    pub absolute_due_us: u64,
    pub due_ts_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_timing: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_to_canonical_append_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_receipt_due_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_due_ts_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BacktestCtfTimingDiagnostic {
    pub tx_hash: String,
    pub intent_id: u64,
    pub pending_ts_ms: u64,
    pub sample_index: usize,
    pub sampled_pending_to_materialized_ms: u64,
    pub due_ts_ms: u64,
}

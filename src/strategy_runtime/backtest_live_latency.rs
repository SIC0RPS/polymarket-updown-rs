use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const CURRENT_MARKETABLE_TAKER_HOLD_US: u64 = 150_000;
pub(crate) const CURRENT_MARKETABLE_TAKER_HOLD_SOURCE: &str =
    "polymarket_predictions_changelog_current_crypto_taker_delay";
const CURRENT_EXECUTION_PROTOCOL: &str = "polymarket_clob_protocol_since_2026_09_04";
const CURRENT_EXECUTION_PROTOCOL_START_MS: u64 = 1_788_530_400_000;
const EXECUTION_TIMING_MODEL_VERSION_V1: &str = "capture_selected_execution_timing_v1";
const EXECUTION_TIMING_MODEL_VERSION_V2: &str = "capture_selected_execution_timing_v2";
const EXECUTION_TIMING_MODEL_VERSION_V3: &str = "capture_selected_execution_timing_v3";
pub(crate) const EXECUTION_TIMING_MODEL_VERSION_V4: &str = "capture_selected_execution_timing_v4";
pub(crate) const EXECUTION_TIMING_MODEL_VERSION_V5: &str = "capture_selected_execution_timing_v5";
const CLOB_JOURNAL: &str = "order_submit_latency.jsonl";
const CTF_JOURNAL: &str = "ctf_inventory_operation_latency.jsonl";

mod observations;
mod source;

pub use source::{select_replay_execution_timing, validate_frozen_replay_execution_timing};

pub(crate) fn sample_observation_index(sample_count: usize, seed: u64) -> Option<usize> {
    (sample_count > 0).then(|| (seed % sample_count as u64) as usize)
}

pub(crate) fn stable_latency_seed(parts: &[&str], ts_ms: u64) -> u64 {
    let mut hash = ts_ms ^ 0x9e37_79b9_7f4a_7c15;
    for part in parts {
        for byte in part.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayExecutionTimingModel {
    pub schema_version: u32,
    pub model_version: String,
    pub selected_capture: String,
    pub selected_config_sha256: Option<String>,
    pub current_execution_config_sha256: String,
    pub current_clob_protocol: String,
    pub current_clob_protocol_start_ms: u64,
    pub current_marketable_taker_hold_us: u64,
    pub current_marketable_taker_hold_source: String,
    pub clob: Option<ReplayClobTimingSource>,
    pub ctf: Option<ReplayCtfTimingSource>,
}

impl ReplayExecutionTimingModel {
    pub fn sha256(&self) -> Result<String> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).context("serialize execution timing model")?)
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayClobTimingSource {
    pub source_capture: String,
    pub source_run_id: String,
    pub source_config_sha256: Option<String>,
    pub journal_path: String,
    pub journal_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_journal_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_journal_sha256: Option<String>,
    pub endpoint: String,
    pub unit: String,
    pub observations: Vec<ReplayClobTimingObservation>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayClobTimingObservation {
    pub attempt: usize,
    pub request_ts_ms: u64,
    pub response_ts_ms: u64,
    pub decision_to_handoff_us: u64,
    pub handoff_to_send_invocation_us: u64,
    pub send_invocation_to_response_us: u64,
    pub response_to_owner_us: u64,
    pub invocation_elapsed_us: u64,
    pub response_headers_elapsed_us: Option<u64>,
    pub response_body_elapsed_us: Option<u64>,
    pub http_status: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_admission: Option<ReplayClobReceiptAdmissionObservation>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayClobReceiptAdmissionObservation {
    pub response_to_canonical_append_us: u64,
    pub order_id: String,
    pub canonical_receipt_id: String,
    pub execution_intent_id: u64,
    pub input_index: usize,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayCtfTimingSource {
    pub source_capture: String,
    pub source_run_id: String,
    pub source_config_sha256: Option<String>,
    pub journal_path: String,
    pub journal_sha256: String,
    pub endpoint: String,
    pub unit: String,
    pub observations: Vec<ReplayCtfTimingObservation>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayCtfTimingObservation {
    pub pending_ts_ms: u64,
    pub materialized_ts_ms: u64,
    pub latency_ms: u64,
    pub tx_hash: String,
}

pub(crate) fn profile_from_distribution(dist: &LatencyDistribution) -> Result<(u64, u64, u64)> {
    if let (Some(min), Some(max)) = (dist.min, dist.max) {
        let max = max.max(min);
        let avg = dist
            .avg
            .or(dist.p50)
            .unwrap_or_else(|| min + (max.saturating_sub(min) / 2))
            .clamp(min, max);
        return Ok((min, avg, max));
    }
    if let Some(point) = dist.avg.or(dist.p50) {
        return Ok((point, point, point));
    }
    bail!("latency distribution missing min/max/avg")
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayLiveLatencySummary {
    pub status: ReplayLiveLatencyStatus,
    pub source: String,
    pub model_version: Option<String>,
    pub model_path: Option<String>,
    pub cutoff_ts_ms: Option<u64>,
    pub cutoff_capture: Option<String>,
    pub marketable_taker_delay_ms: Option<u64>,
    pub marketable_taker_delay_source: Option<String>,
    pub source_captures: Vec<String>,
    pub provenance_hashes: Vec<String>,
    pub rows: u64,
    pub latency_ms: LatencyDistribution,
    pub issue: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayLiveLatencyStatus {
    Ok,
    Uncalibrated,
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LatencyDistribution {
    pub min: Option<u64>,
    pub avg: Option<u64>,
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
}

impl ReplayLiveLatencySummary {
    pub fn status_label(&self) -> &'static str {
        match self.status {
            ReplayLiveLatencyStatus::Ok => "ok",
            ReplayLiveLatencyStatus::Uncalibrated => "uncalibrated",
        }
    }

    pub fn report_line(&self) -> String {
        let issue = self.issue.as_deref().unwrap_or("none");
        let source_captures = if self.source_captures.is_empty() {
            "none".to_string()
        } else {
            self.source_captures.join(",")
        };
        let provenance_hashes = if self.provenance_hashes.is_empty() {
            "none".to_string()
        } else {
            self.provenance_hashes.join(",")
        };
        format!(
            "PROOF_QUALITY live_latency={} source={} model_version={} model_path={} cutoff_ts_ms={} cutoff_capture={} marketable_taker_delay_ms={} marketable_taker_delay_source={} source_captures={} provenance_hashes={} order_submit_latency_rows={} latency_ms_min={} latency_ms_avg={} latency_ms_p50={} latency_ms_p95={} latency_ms_max={} issue={}",
            self.status_label(),
            self.source,
            self.model_version.as_deref().unwrap_or("NA"),
            self.model_path.as_deref().unwrap_or("NA"),
            self.cutoff_ts_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NA".to_string()),
            self.cutoff_capture.as_deref().unwrap_or("NA"),
            self.marketable_taker_delay_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NA".to_string()),
            self.marketable_taker_delay_source
                .as_deref()
                .unwrap_or("NA"),
            source_captures,
            provenance_hashes,
            self.rows,
            fmt_opt_u64(self.latency_ms.min),
            fmt_opt_u64(self.latency_ms.avg),
            fmt_opt_u64(self.latency_ms.p50),
            fmt_opt_u64(self.latency_ms.p95),
            fmt_opt_u64(self.latency_ms.max),
            issue
        )
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_string_pretty(self).context("serialize replay live latency")?;
        fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }

    pub fn replay_latency_profile_ms(&self) -> Result<(u64, u64, u64)> {
        if self.issue.is_some() {
            bail!(
                "order submit latency capture invalid: {}",
                self.issue.as_deref().unwrap_or("unknown")
            );
        }
        profile_from_distribution(&self.latency_ms)
    }
}

pub fn frozen_replay_live_latency(
    model: &ReplayExecutionTimingModel,
) -> Result<ReplayLiveLatencySummary> {
    let (source, source_captures, provenance_hashes, rows, latency_ms, issue) = match &model.clob {
        Some(source) => {
            let values = source
                .observations
                .iter()
                .map(|observation| observation.invocation_elapsed_us.saturating_add(999) / 1_000)
                .collect::<Vec<_>>();
            (
                "capture_order_submit_transport".to_string(),
                vec![source.source_capture.clone()],
                vec![source.journal_sha256.clone()],
                source.observations.len() as u64,
                distribution(&values)?,
                None,
            )
        }
        None => (
            "uncalibrated".to_string(),
            Vec::new(),
            Vec::new(),
            0,
            LatencyDistribution::default(),
            Some("no compatible CLOB transport observations".to_string()),
        ),
    };
    Ok(ReplayLiveLatencySummary {
        status: if model.clob.is_some() {
            ReplayLiveLatencyStatus::Ok
        } else {
            ReplayLiveLatencyStatus::Uncalibrated
        },
        source,
        model_version: Some(model.model_version.clone()),
        model_path: None,
        cutoff_ts_ms: None,
        cutoff_capture: Some(model.selected_capture.clone()),
        marketable_taker_delay_ms: Some(model.current_marketable_taker_hold_us / 1_000),
        marketable_taker_delay_source: Some(model.current_marketable_taker_hold_source.clone()),
        source_captures,
        provenance_hashes,
        rows,
        latency_ms,
        issue,
    })
}

pub(crate) fn distribution(values: &[u64]) -> Result<LatencyDistribution> {
    anyhow::ensure!(!values.is_empty(), "latency distribution is empty");
    let mut values = values.to_vec();
    values.sort_unstable();
    let sum = values.iter().try_fold(0_u128, |sum, value| {
        sum.checked_add(u128::from(*value))
            .context("latency sum overflow")
    })?;
    let quantile = |percent: usize| values[(values.len() - 1) * percent / 100];
    Ok(LatencyDistribution {
        min: values.first().copied(),
        avg: Some(u64::try_from(sum / values.len() as u128).context("latency average overflow")?),
        p50: Some(quantile(50)),
        p95: Some(quantile(95)),
        p99: Some(quantile(99)),
        max: values.last().copied(),
    })
}

pub(crate) fn fmt_opt_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NA".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_seed_is_deterministic() {
        let a = stable_latency_seed(&["order-1", "sell"], 100);
        let b = stable_latency_seed(&["order-1", "sell"], 100);
        let c = stable_latency_seed(&["order-2", "sell"], 100);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}

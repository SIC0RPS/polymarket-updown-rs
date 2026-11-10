use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::VALID_ASSETS;

#[derive(Debug, Clone, Serialize)]
pub struct ReplayConfigTruthSummary {
    pub status: ReplayConfigTruthStatus,
    pub capture_config_snapshot_path: PathBuf,
    pub session_meta_path: PathBuf,
    pub current_config_path: PathBuf,
    pub capture_optimizer_snapshot_path: PathBuf,
    pub captured_config_hash: Option<String>,
    pub snapshot_config_hash: Option<String>,
    pub current_config_hash: Option<String>,
    pub captured_optimizer_config_hash: Option<String>,
    pub capture_optimizer_snapshot_hash: Option<String>,
    pub captured_final_config_hash: Option<String>,
    pub captured_config_timeline_hash: Option<String>,
    pub capture_config_timeline_hash: Option<String>,
    pub captured_config_change_count: Option<usize>,
    pub snapshot_matches_session_meta: Option<bool>,
    pub optimizer_snapshot_matches_session_meta: Option<bool>,
    pub config_timeline_matches_session_meta: Option<bool>,
    pub current_matches_capture: Option<bool>,
    pub issue: Option<String>,
    pub diagnostic_issue: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayConfigTruthStatus {
    Proof,
    NonProof,
}

impl ReplayConfigTruthSummary {
    pub fn proof_fails(&self) -> bool {
        matches!(self.status, ReplayConfigTruthStatus::NonProof)
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let body =
            serde_json::to_string_pretty(self).context("serialize replay config truth summary")?;
        fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }

    pub fn report_line(&self) -> String {
        format!(
            "PROOF_QUALITY config_truth={} captured_hash={} snapshot_hash={} current_hash={} captured_optimizer_hash={} optimizer_snapshot_hash={} final_config_hash={} config_timeline_hash={} config_timeline_snapshot_hash={} config_change_count={} snapshot_matches_meta={} optimizer_snapshot_matches_meta={} config_timeline_matches_meta={} current_matches_capture={} issue={} diagnostic_issue={}",
            self.status.label(),
            opt_str(self.captured_config_hash.as_deref()),
            opt_str(self.snapshot_config_hash.as_deref()),
            opt_str(self.current_config_hash.as_deref()),
            opt_str(self.captured_optimizer_config_hash.as_deref()),
            opt_str(self.capture_optimizer_snapshot_hash.as_deref()),
            opt_str(self.captured_final_config_hash.as_deref()),
            opt_str(self.captured_config_timeline_hash.as_deref()),
            opt_str(self.capture_config_timeline_hash.as_deref()),
            self.captured_config_change_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NA".to_string()),
            opt_bool(self.snapshot_matches_session_meta),
            opt_bool(self.optimizer_snapshot_matches_session_meta),
            opt_bool(self.config_timeline_matches_session_meta),
            opt_bool(self.current_matches_capture),
            self.issue.as_deref().unwrap_or("none"),
            self.diagnostic_issue.as_deref().unwrap_or("none")
        )
    }
}

impl ReplayConfigTruthStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Proof => "proof",
            Self::NonProof => "non_proof",
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct SessionMetaConfigHash {
    config_hash: Option<String>,
    #[serde(default)]
    optimizer_config_hash: Option<String>,
    #[serde(default)]
    final_config_hash: Option<String>,
    #[serde(default)]
    config_timeline_hash: Option<String>,
    #[serde(default)]
    config_change_count: Option<usize>,
}

pub fn summarize_replay_config_truth(capture: &Path) -> ReplayConfigTruthSummary {
    let capture_config_snapshot_path = capture.join("config_snapshot.toml");
    let session_meta_path = capture.join("session_meta.json");
    let current_config_path = PathBuf::from("config/main.toml");
    let capture_optimizer_snapshot_path = capture.join("optimizer_snapshot");

    let session_hashes = read_session_meta_config_hashes(&session_meta_path).unwrap_or_default();
    let captured_config_hash = session_hashes.config_hash;
    let captured_optimizer_config_hash = session_hashes.optimizer_config_hash;
    let captured_final_config_hash = session_hashes.final_config_hash;
    let captured_config_timeline_hash = session_hashes.config_timeline_hash;
    let captured_config_change_count = session_hashes.config_change_count;
    let snapshot_config_hash = read_config_hash(&capture_config_snapshot_path).ok();
    let current_config_hash = read_config_hash(&current_config_path).ok();
    let capture_optimizer_snapshot_hash = read_optimizer_snapshot_hash(capture).ok();
    let capture_config_timeline_hash = read_config_timeline_hash(capture).ok();

    let snapshot_matches_session_meta = captured_config_hash
        .as_deref()
        .zip(snapshot_config_hash.as_deref())
        .map(|(captured, snapshot)| captured == snapshot);
    let current_matches_capture = captured_config_hash
        .as_deref()
        .zip(current_config_hash.as_deref())
        .map(|(captured, current)| captured == current);
    let optimizer_snapshot_matches_session_meta = captured_optimizer_config_hash
        .as_deref()
        .zip(capture_optimizer_snapshot_hash.as_deref())
        .map(|(captured, snapshot)| captured == snapshot);
    let config_timeline_matches_session_meta = captured_config_timeline_hash
        .as_deref()
        .zip(capture_config_timeline_hash.as_deref())
        .map(|(captured, snapshot)| captured == snapshot);
    let diagnostic_issue = config_truth_diagnostic_issue(
        snapshot_matches_session_meta,
        optimizer_snapshot_matches_session_meta,
        current_matches_capture,
    );

    let issue = config_truth_issue(
        captured_config_hash.as_deref(),
        captured_config_timeline_hash.as_deref(),
        capture_config_timeline_hash.as_deref(),
        config_timeline_matches_session_meta,
    );
    let status = if issue.is_some() {
        ReplayConfigTruthStatus::NonProof
    } else {
        ReplayConfigTruthStatus::Proof
    };

    ReplayConfigTruthSummary {
        status,
        capture_config_snapshot_path,
        session_meta_path,
        current_config_path,
        capture_optimizer_snapshot_path,
        captured_config_hash,
        snapshot_config_hash,
        current_config_hash,
        captured_optimizer_config_hash,
        capture_optimizer_snapshot_hash,
        captured_final_config_hash,
        captured_config_timeline_hash,
        capture_config_timeline_hash,
        captured_config_change_count,
        snapshot_matches_session_meta,
        optimizer_snapshot_matches_session_meta,
        config_timeline_matches_session_meta,
        current_matches_capture,
        issue,
        diagnostic_issue,
    }
}

fn config_truth_issue(
    captured_config_hash: Option<&str>,
    captured_config_timeline_hash: Option<&str>,
    capture_config_timeline_hash: Option<&str>,
    config_timeline_matches_session_meta: Option<bool>,
) -> Option<String> {
    if captured_config_hash.is_none() {
        return Some("missing_captured_config_hash".to_string());
    }
    if captured_config_timeline_hash.is_some() && capture_config_timeline_hash.is_none() {
        return Some("missing_config_timeline_snapshot".to_string());
    }
    if captured_config_timeline_hash.is_some()
        && capture_config_timeline_hash.is_some()
        && config_timeline_matches_session_meta != Some(true)
    {
        return Some("config_timeline_hash_mismatch".to_string());
    }
    None
}

fn config_truth_diagnostic_issue(
    snapshot_matches_session_meta: Option<bool>,
    optimizer_snapshot_matches_session_meta: Option<bool>,
    current_matches_capture: Option<bool>,
) -> Option<String> {
    if snapshot_matches_session_meta == Some(false) {
        return Some("config_snapshot_hash_mismatch".to_string());
    }
    if snapshot_matches_session_meta.is_none() {
        return Some("missing_config_snapshot_diagnostic".to_string());
    }
    if optimizer_snapshot_matches_session_meta == Some(false) {
        return Some("captured_optimizer_hash_differs_from_symbol_only_snapshot".to_string());
    }
    if current_matches_capture == Some(false) {
        return Some("current_config_differs_from_capture".to_string());
    }
    None
}

fn read_session_meta_config_hashes(path: &Path) -> Result<SessionMetaConfigHash> {
    let body = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let meta: SessionMetaConfigHash =
        serde_json::from_str(&body).with_context(|| format!("parse {}", path.display()))?;
    Ok(meta)
}

fn read_config_hash(path: &Path) -> Result<String> {
    let body = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(hash_config_for_capture(&body))
}

fn read_optimizer_snapshot_hash(capture: &Path) -> Result<String> {
    let mut body = String::new();
    for file_name in optimizer_snapshot_files() {
        let path = capture.join("optimizer_snapshot").join(&file_name);
        let content =
            fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        body.push_str(&file_name);
        body.push('\n');
        body.push_str(&content);
        body.push('\n');
    }
    Ok(hash_config_for_capture(&body))
}

fn read_config_timeline_hash(capture: &Path) -> Result<String> {
    let body =
        fs::read_to_string(capture.join("config_change_timeline.json")).with_context(|| {
            format!(
                "read {}",
                capture.join("config_change_timeline.json").display()
            )
        })?;
    Ok(hash_config_for_capture(&body))
}

fn optimizer_snapshot_files() -> Vec<String> {
    VALID_ASSETS
        .iter()
        .map(|symbol| format!("{symbol}.optimization.yml"))
        .collect()
}

fn hash_config_for_capture(contents: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    contents.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn opt_str(value: Option<&str>) -> &str {
    value.unwrap_or("NA")
}

fn opt_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "NA",
    }
}

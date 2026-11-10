use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::AppConfig;
use crate::strategy_runtime::backtest_live_latency::{
    distribution, LatencyDistribution, ReplayExecutionTimingModel,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplayIntervalSummary {
    pub schema_version: u32,
    pub source: String,
    pub run_id: ReplayRunId,
    pub start_ts_ms: u64,
    pub end_ts_ms: u64,
    pub session_start_ts_ms: u64,
    pub session_end_ts_ms: u64,
    pub issue: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct ReplayRunId(String);

impl ReplayRunId {
    fn parse(value: String) -> Result<Self> {
        anyhow::ensure!(
            crate::trade_session_ledger::is_live_run_id(&value)
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "capture replay run_id is invalid: {value}"
        );
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ReplayRunId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CaptureSessionMeta {
    pub(super) session_id: String,
    run_id: ReplayRunId,
    pub(super) start_ts_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReplayCtfSplitLatencySummary {
    pub schema_version: u32,
    pub source: String,
    pub model_version: Option<String>,
    pub model_path: Option<String>,
    pub cutoff_ts_ms: Option<u64>,
    pub cutoff_capture: Option<String>,
    pub run_id: Option<String>,
    pub source_run_ids: Vec<String>,
    pub source_captures: Vec<String>,
    pub provenance_hashes: Vec<String>,
    pub rows: u64,
    pub latency_ms: LatencyDistribution,
    pub issue: Option<String>,
}

impl ReplayCtfSplitLatencySummary {
    pub(crate) fn replay_latency_profile_ms(&self) -> Result<(u64, u64, u64)> {
        if self.issue.is_some() {
            anyhow::bail!(
                "ctf split latency capture missing: {}",
                self.issue.as_deref().unwrap_or("unknown")
            );
        }
        if let (Some(min), Some(max)) = (self.latency_ms.min, self.latency_ms.max) {
            let max = max.max(min);
            let avg = self
                .latency_ms
                .avg
                .or(self.latency_ms.p50)
                .unwrap_or_else(|| min + (max.saturating_sub(min) / 2))
                .clamp(min, max);
            return Ok((min, avg, max));
        }
        if let Some(p50) = self.latency_ms.p50 {
            return Ok((p50, p50, p50));
        }
        anyhow::bail!("ctf split latency capture missing min/max/p50")
    }

    pub(crate) fn write_json(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }
}

impl ReplayIntervalSummary {
    pub(crate) fn window(&self) -> Option<(u64, u64)> {
        (self.start_ts_ms <= self.end_ts_ms).then_some((self.start_ts_ms, self.end_ts_ms))
    }

    pub(crate) fn write_json(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }
}

pub(crate) fn capture_replay_interval(session_dir: &Path) -> Result<ReplayIntervalSummary> {
    let meta = read_session_meta(&session_dir.join("session_meta.json"))?;
    let end_ts_ms = prepared_replay_end_ts_ms(session_dir)?;
    let summary = ReplayIntervalSummary {
        schema_version: 1,
        source: "prepared_replay_manifest".to_string(),
        run_id: meta.run_id,
        start_ts_ms: meta.start_ts_ms,
        end_ts_ms,
        session_start_ts_ms: meta.start_ts_ms,
        session_end_ts_ms: end_ts_ms,
        issue: None,
    };
    Ok(summary)
}

fn prepared_replay_end_ts_ms(session_dir: &Path) -> Result<u64> {
    let symbols = super::replay_symbols_for_capture(session_dir)?;
    anyhow::ensure!(
        !symbols.is_empty(),
        "prepared replay has no completed symbol manifests at {}",
        session_dir.display()
    );
    symbols
        .iter()
        .map(|symbol| {
            let manifest_path = super::prepared_manifest::prepared_symbol_dir(session_dir, symbol)
                .join(super::prepared_manifest::MANIFEST_FILE);
            let manifest = super::prepared_manifest::PreparedReplayManifest::read(&manifest_path)?;
            let start_ts_ms = manifest.start_ts_ms.with_context(|| {
                format!(
                    "prepared replay manifest missing start_ts_ms {}",
                    manifest_path.display()
                )
            })?;
            let end_ts_ms = manifest.end_ts_ms.with_context(|| {
                format!(
                    "prepared replay manifest missing end_ts_ms {}",
                    manifest_path.display()
                )
            })?;
            anyhow::ensure!(
                start_ts_ms <= end_ts_ms,
                "prepared replay manifest interval invalid start_ts_ms={} end_ts_ms={} path={}",
                start_ts_ms,
                end_ts_ms,
                manifest_path.display()
            );
            Ok(end_ts_ms)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("prepared replay end_ts_ms is unavailable")
}

pub(crate) fn frozen_ctf_split_latency(
    session_dir: &Path,
    model: &ReplayExecutionTimingModel,
) -> Result<ReplayCtfSplitLatencySummary> {
    let (source, source_run_ids, source_captures, provenance_hashes, rows, latency_ms, issue) =
        match &model.ctf {
            Some(source) => (
                "capture_confirmed_ctf_materialization".to_string(),
                vec![source.source_run_id.clone()],
                vec![source.source_capture.clone()],
                vec![source.journal_sha256.clone()],
                source.observations.len() as u64,
                distribution(
                    &source
                        .observations
                        .iter()
                        .map(|observation| observation.latency_ms)
                        .collect::<Vec<_>>(),
                )?,
                None,
            ),
            None => (
                "uncalibrated".to_string(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
                LatencyDistribution::default(),
                Some("no compatible CTF materialization observations".to_string()),
            ),
        };
    Ok(ReplayCtfSplitLatencySummary {
        schema_version: 1,
        source,
        model_version: Some(model.model_version.clone()),
        model_path: None,
        cutoff_ts_ms: None,
        cutoff_capture: Some(model.selected_capture.clone()),
        run_id: capture_run_id_from_session_dir(session_dir)?,
        source_run_ids,
        source_captures,
        provenance_hashes,
        rows,
        latency_ms,
        issue,
    })
}

pub(super) fn read_session_meta(path: &Path) -> Result<CaptureSessionMeta> {
    let file =
        File::open(path).with_context(|| format!("failed to open capture {}", path.display()))?;
    let meta: CaptureSessionMeta = serde_json::from_reader(file)
        .with_context(|| format!("failed to parse capture {}", path.display()))?;
    anyhow::ensure!(
        !meta.session_id.trim().is_empty()
            && meta.session_id == meta.session_id.trim()
            && meta.start_ts_ms > 0,
        "capture session metadata identity is incomplete"
    );
    Ok(meta)
}

fn capture_run_id_from_session_dir(session_dir: &Path) -> Result<Option<String>> {
    Ok(Some(
        capture_replay_interval(session_dir)?
            .run_id
            .as_str()
            .to_string(),
    ))
}

pub(super) fn resolve_capture_session(
    cfg: &AppConfig,
    requested: Option<&PathBuf>,
) -> Result<PathBuf> {
    let requested = requested.context("capture replay requires --capture-session <dir|latest>")?;
    if requested.as_os_str() != "latest" {
        return Ok(requested.clone());
    }
    latest_capture_session(Path::new(&cfg.capture.base_dir))
}

fn latest_capture_session(base_dir: &Path) -> Result<PathBuf> {
    let mut sessions = capture_session_dirs(base_dir)?
        .into_iter()
        .map(|path| {
            let interval = capture_replay_interval(&path)?;
            Ok((interval.end_ts_ms, path))
        })
        .collect::<Result<Vec<_>>>()?;
    sessions.sort();
    while let Some((_, path)) = sessions.pop() {
        if capture_session_replay_ready(&path)? {
            return Ok(path);
        }
    }
    Err(anyhow::anyhow!(
        "no prepared replay-ready capture sessions found under {}",
        base_dir.display()
    ))
}

fn capture_session_replay_ready(session_dir: &Path) -> Result<bool> {
    capture_replay_interval(session_dir)?;
    Ok(!super::replay_symbols_for_capture(session_dir)?.is_empty())
}

fn capture_session_dirs(base_dir: &Path) -> Result<Vec<PathBuf>> {
    if !base_dir.exists() {
        return Ok(Vec::new());
    }
    let mut sessions = Vec::new();
    for entry in std::fs::read_dir(base_dir)
        .with_context(|| format!("failed to read capture dir {}", base_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir()
            && path
                .join(super::prepared_manifest::PREPARED_REPLAY_DIR)
                .is_dir()
        {
            sessions.push(path);
        }
    }
    Ok(sessions)
}

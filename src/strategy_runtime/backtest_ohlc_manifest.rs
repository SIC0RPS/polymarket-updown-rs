use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use canonical_ohlc_arrow::read_all;
use canonical_ohlc_resync::parse_interval_ms;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::AppConfig;

#[derive(Clone, Debug, Serialize)]
pub struct ReplayOhlcManifestSummary {
    pub status: ReplayOhlcManifestStatus,
    pub capture_path: String,
    pub session_start_ts_ms: Option<u64>,
    pub session_end_ts_ms: Option<u64>,
    pub entries: Vec<ReplayOhlcManifestEntry>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayOhlcManifestStatus {
    Ok,
    Fail,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayOhlcManifestEntry {
    pub underlying: String,
    pub oracle_symbol: String,
    pub timeframe: String,
    pub path: String,
    pub sha256: Option<String>,
    pub row_count: usize,
    pub min_ts_ms: Option<i64>,
    pub max_ts_ms: Option<i64>,
    pub expected_interval_ms: Option<i64>,
    pub continuity_status: ReplayOhlcContinuityStatus,
    pub gap_count: u64,
    pub first_gap_after_ts_ms: Option<i64>,
    pub covers_session_start: Option<bool>,
    pub covers_session_end: Option<bool>,
    pub status: ReplayOhlcEntryStatus,
    pub issue: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayOhlcContinuityStatus {
    Continuous,
    Gap,
    NonMonotonic,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayOhlcEntryStatus {
    Ok,
    Missing,
    Empty,
    Corrupt,
    NonContinuous,
    CoverageGap,
}

impl ReplayOhlcManifestSummary {
    pub fn hash_by_key(&self) -> BTreeMap<String, String> {
        self.entries
            .iter()
            .filter_map(|entry| {
                entry.sha256.as_ref().map(|hash| {
                    (
                        format!("{}/{}", entry.oracle_symbol, entry.timeframe),
                        hash.clone(),
                    )
                })
            })
            .collect()
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let body = serde_json::to_string_pretty(self).context("serialize replay OHLC manifest")?;
        fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }

    pub fn report_line(&self) -> String {
        let missing = self
            .entries
            .iter()
            .filter(|entry| entry.status == ReplayOhlcEntryStatus::Missing)
            .count();
        let empty = self
            .entries
            .iter()
            .filter(|entry| entry.status == ReplayOhlcEntryStatus::Empty)
            .count();
        let corrupt = self
            .entries
            .iter()
            .filter(|entry| entry.status == ReplayOhlcEntryStatus::Corrupt)
            .count();
        let non_continuous = self
            .entries
            .iter()
            .filter(|entry| entry.status == ReplayOhlcEntryStatus::NonContinuous)
            .count();
        let coverage_gap = self
            .entries
            .iter()
            .filter(|entry| entry.status == ReplayOhlcEntryStatus::CoverageGap)
            .count();
        let total_rows = self
            .entries
            .iter()
            .map(|entry| entry.row_count)
            .sum::<usize>();
        format!(
            "PROOF_QUALITY ohlc_manifest={} entries={} rows={} missing={} empty={} corrupt={} non_continuous={} coverage_gap={}",
            self.status.label(),
            self.entries.len(),
            total_rows,
            missing,
            empty,
            corrupt,
            non_continuous,
            coverage_gap
        )
    }
}

impl ReplayOhlcManifestStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Fail => "fail",
        }
    }
}

pub fn summarize_capture_ohlc_manifest(
    capture: &Path,
    cfg: &AppConfig,
    underlyings: &[String],
) -> Result<ReplayOhlcManifestSummary> {
    let (session_start_ts_ms, session_end_ts_ms) = session_window(capture)?;
    let mut entries = Vec::new();
    let mut normalized_underlyings = underlyings
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    normalized_underlyings.sort();
    normalized_underlyings.dedup();
    for underlying in normalized_underlyings {
        let oracle_symbol = cfg.bbwick_oracle.resolve_symbol_for_underlying(&underlying);
        for timeframe in &cfg.bbwick_oracle.transport.step_timeframes {
            entries.push(summarize_entry(
                capture,
                &underlying,
                &oracle_symbol,
                timeframe,
                session_start_ts_ms,
                session_end_ts_ms,
            ));
        }
    }
    let status = if entries
        .iter()
        .all(|entry| entry.status == ReplayOhlcEntryStatus::Ok)
    {
        ReplayOhlcManifestStatus::Ok
    } else {
        ReplayOhlcManifestStatus::Fail
    };
    Ok(ReplayOhlcManifestSummary {
        status,
        capture_path: capture.display().to_string(),
        session_start_ts_ms,
        session_end_ts_ms,
        entries,
    })
}

fn summarize_entry(
    capture: &Path,
    underlying: &str,
    oracle_symbol: &str,
    timeframe: &str,
    session_start_ts_ms: Option<u64>,
    session_end_ts_ms: Option<u64>,
) -> ReplayOhlcManifestEntry {
    let timeframe = timeframe.trim().to_ascii_lowercase();
    let path = capture
        .join("bbwick_ohlc")
        .join(oracle_symbol)
        .join(&timeframe)
        .join("ohlc.arrow");
    let expected_interval_ms = parse_interval_ms(&timeframe).map(|value| value as i64);
    if !path.is_file() {
        return entry_issue(
            underlying,
            oracle_symbol,
            &timeframe,
            &path,
            expected_interval_ms,
            ReplayOhlcEntryStatus::Missing,
            "missing_ohlc_arrow",
        );
    }
    let sha256 = file_sha256(&path).ok();
    let bars = match read_all(&path) {
        Ok(bars) => bars,
        Err(err) => {
            let mut entry = entry_issue(
                underlying,
                oracle_symbol,
                &timeframe,
                &path,
                expected_interval_ms,
                ReplayOhlcEntryStatus::Corrupt,
                "read_ohlc_arrow_failed",
            );
            entry.sha256 = sha256;
            entry.issue = Some(format!("read_ohlc_arrow_failed:{err}"));
            return entry;
        }
    };
    if bars.is_empty() {
        let mut entry = entry_issue(
            underlying,
            oracle_symbol,
            &timeframe,
            &path,
            expected_interval_ms,
            ReplayOhlcEntryStatus::Empty,
            "empty_ohlc_arrow",
        );
        entry.sha256 = sha256;
        return entry;
    }

    let min_ts_ms = bars.first().map(|bar| bar.ts_ms);
    let max_ts_ms = bars.last().map(|bar| bar.ts_ms);
    let mut gap_count = 0_u64;
    let mut first_gap_after_ts_ms = None;
    let mut non_monotonic = false;
    for pair in bars.windows(2) {
        let previous = pair[0].ts_ms;
        let current = pair[1].ts_ms;
        if current <= previous {
            non_monotonic = true;
            first_gap_after_ts_ms.get_or_insert(previous);
            break;
        }
        if expected_interval_ms.is_some_and(|interval| current - previous != interval) {
            gap_count = gap_count.saturating_add(1);
            first_gap_after_ts_ms.get_or_insert(previous);
        }
    }
    let continuity_status = if non_monotonic {
        ReplayOhlcContinuityStatus::NonMonotonic
    } else if gap_count > 0 {
        ReplayOhlcContinuityStatus::Gap
    } else if expected_interval_ms.is_some() {
        ReplayOhlcContinuityStatus::Continuous
    } else {
        ReplayOhlcContinuityStatus::Unknown
    };
    let covers_session_start = session_start_ts_ms
        .zip(min_ts_ms)
        .map(|(start, min_ts)| min_ts <= start as i64);
    let covers_session_end = session_end_ts_ms
        .zip(max_ts_ms)
        .zip(expected_interval_ms)
        .map(|((end, max_ts), interval)| max_ts + interval >= end as i64);
    let (status, issue) = if non_monotonic || gap_count > 0 {
        (
            ReplayOhlcEntryStatus::NonContinuous,
            Some("ohlc_not_continuous".to_string()),
        )
    } else if covers_session_start == Some(false) {
        (
            ReplayOhlcEntryStatus::CoverageGap,
            Some("ohlc_does_not_cover_session_start".to_string()),
        )
    } else {
        (ReplayOhlcEntryStatus::Ok, None)
    };

    ReplayOhlcManifestEntry {
        underlying: underlying.to_string(),
        oracle_symbol: oracle_symbol.to_string(),
        timeframe,
        path: path.display().to_string(),
        sha256,
        row_count: bars.len(),
        min_ts_ms,
        max_ts_ms,
        expected_interval_ms,
        continuity_status,
        gap_count,
        first_gap_after_ts_ms,
        covers_session_start,
        covers_session_end,
        status,
        issue,
    }
}

fn entry_issue(
    underlying: &str,
    oracle_symbol: &str,
    timeframe: &str,
    path: &Path,
    expected_interval_ms: Option<i64>,
    status: ReplayOhlcEntryStatus,
    issue: &str,
) -> ReplayOhlcManifestEntry {
    ReplayOhlcManifestEntry {
        underlying: underlying.to_string(),
        oracle_symbol: oracle_symbol.to_string(),
        timeframe: timeframe.to_string(),
        path: path.display().to_string(),
        sha256: None,
        row_count: 0,
        min_ts_ms: None,
        max_ts_ms: None,
        expected_interval_ms,
        continuity_status: ReplayOhlcContinuityStatus::Unknown,
        gap_count: 0,
        first_gap_after_ts_ms: None,
        covers_session_start: None,
        covers_session_end: None,
        status,
        issue: Some(issue.to_string()),
    }
}

fn session_window(capture: &Path) -> Result<(Option<u64>, Option<u64>)> {
    let interval = crate::strategy_runtime::replay_mode::capture_replay_interval(capture)?;
    Ok((Some(interval.start_ts_ms), Some(interval.end_ts_ms)))
}

fn file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub fn default_ohlc_underlyings() -> Vec<String> {
    ["BTC", "ETH", "SOL", "XRP"]
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect()
}

pub fn capture_ohlc_manifest_for_default_symbols(
    capture: &Path,
) -> Result<ReplayOhlcManifestSummary> {
    let cfg = AppConfig::load().context(
        "load clean current backtest config; capture config_snapshot is diagnostic-only",
    )?;
    summarize_capture_ohlc_manifest(capture, &cfg, &default_ohlc_underlyings())
}

use std::path::{Path, PathBuf};

use crate::capture::arrow::EngineArrowReader;
use crate::capture::events::ReplayRiskSnapshotCapture;
use crate::config::AppConfig;
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use anyhow::{Context, Result};
use rayon::prelude::*;
#[path = "replay_mode/candidate_runner.rs"]
mod candidate_runner;
#[path = "replay_mode/central_projection.rs"]
mod central_projection;
#[path = "replay_mode/prepare.rs"]
mod prepare;
#[path = "replay_mode/prepared_arrow.rs"]
mod prepared_arrow;
#[path = "replay_mode/prepared_event_stream.rs"]
pub(crate) mod prepared_event_stream;
#[path = "replay_mode/prepared_manifest.rs"]
mod prepared_manifest;
#[path = "replay_mode/prepared_receipts.rs"]
mod prepared_receipts;
#[path = "replay_mode/prepared_tape.rs"]
mod prepared_tape;
#[path = "replay_mode/session_resolution.rs"]
mod session_resolution;

pub use crate::capture::{finalized_capture_identity, FinalizedCaptureIdentity};
pub use crate::strategy_runtime::market_catalog::{
    RecordedMarketCoverageReport, RecordedMarketCoverageSummary, RecordedMarketDisposition,
    RecordedMarketMissingRecord, RecordedMarketPopulation, RecordedMarketRecord,
    RecordedMarketReportDisposition,
};
pub(crate) use candidate_runner::{
    CandidateReplayResult, PersistentCandidatePool, SIMULATED_BACKTEST_EQUITY_SOURCE,
    SIMULATED_BACKTEST_EQUITY_USDT,
};
pub(crate) use central_projection::CentralSymbolProjection;
pub use prepare::{
    prepare_backtest_replay_for_capture, validate_prepared_replay_sources_for_optimizer,
};
use prepared_arrow::PreparedReplayWriter;
use prepared_manifest::{
    prepared_symbol_dir, PreparedReplayManifest, SourceFileManifest, MANIFEST_FILE,
    PREPARED_REPLAY_DIR, PREPARED_SCHEMA_VERSION,
};
pub(crate) use prepared_receipts::PreparedReceiptTape;
pub(crate) use prepared_tape::PackedReplayTape;
use session_resolution::resolve_capture_session;
pub(crate) use session_resolution::{
    capture_replay_interval, frozen_ctf_split_latency, ReplayCtfSplitLatencySummary,
};

pub fn resolve_capture_session_for_replay(
    cfg: &AppConfig,
    requested: Option<&PathBuf>,
) -> Result<PathBuf> {
    resolve_capture_session(cfg, requested)
}

pub fn prepared_market_lifecycles_for_capture(
    capture: &Path,
    symbol: &str,
) -> Result<Vec<(String, String, u64, u64)>> {
    PackedReplayTape::prepared_market_lifecycles(capture, symbol)
}

pub fn prepared_market_inputs_for_capture(
    capture: &Path,
    symbol: &str,
) -> Result<(Vec<(String, String, u64, u64)>, RecordedMarketPopulation)> {
    PackedReplayTape::prepared_market_inputs(capture, symbol)
}

pub fn replay_symbols_for_capture(session_dir: &Path) -> Result<Vec<String>> {
    let prepared_root = session_dir.join(PREPARED_REPLAY_DIR);
    let mut symbols = Vec::new();
    for entry in std::fs::read_dir(&prepared_root)
        .with_context(|| format!("read prepared replay dir {}", prepared_root.display()))?
    {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let Some(directory_symbol) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if directory_symbol == "central" || directory_symbol.starts_with('.') {
            continue;
        }
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest = PreparedReplayManifest::read(&manifest_path)?;
        anyhow::ensure!(
            manifest.is_complete(),
            "prepared replay manifest is incomplete at {}",
            manifest_path.display()
        );
        let symbol = crate::capture::arrow::normalize_capture_symbol(&manifest.symbol);
        anyhow::ensure!(
            directory_symbol == symbol,
            "prepared replay directory/manifest symbol mismatch directory={} manifest={}",
            directory_symbol,
            manifest.symbol
        );
        symbols.push(symbol);
    }
    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

const REPLAY_RISK_SNAPSHOT_FILE: &str = "replay_risk_snapshot.jsonl";

pub(crate) struct ReplayRiskTimelineSummary {
    pub status: &'static str,
    pub rows: usize,
    pub first_ts_ms: Option<u64>,
    pub last_ts_ms: Option<u64>,
    pub issue: Option<&'static str>,
}

impl ReplayRiskTimelineSummary {
    pub(crate) fn write_json(&self, path: &Path) -> Result<()> {
        let body = serde_json::json!({
            "status": self.status,
            "rows": self.rows,
            "first_ts_ms": self.first_ts_ms,
            "last_ts_ms": self.last_ts_ms,
            "issue": self.issue,
        });
        std::fs::write(path, serde_json::to_string_pretty(&body)?)
            .with_context(|| format!("write {}", path.display()))
    }
}

pub(crate) fn capture_replay_risk_timeline_summary(
    session_dir: &Path,
) -> Result<ReplayRiskTimelineSummary> {
    let path = session_dir.join(REPLAY_RISK_SNAPSHOT_FILE);
    if !path.exists() {
        return Ok(ReplayRiskTimelineSummary {
            status: "missing",
            rows: 0,
            first_ts_ms: None,
            last_ts_ms: None,
            issue: Some("replay_risk_snapshot_capture_missing"),
        });
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut snapshots = body
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| {
            let trimmed = line.trim();
            (!trimmed.is_empty()).then_some((idx, trimmed))
        })
        .map(|(idx, line)| {
            serde_json::from_str::<ReplayRiskSnapshotCapture>(line)
                .with_context(|| format!("parse {} line {}", path.display(), idx + 1))
        })
        .collect::<Result<Vec<_>>>()?;
    snapshots.sort_by_key(|snapshot| snapshot.session_ts_ms);
    Ok(ReplayRiskTimelineSummary {
        status: "ok",
        rows: snapshots.len(),
        first_ts_ms: snapshots.first().map(|snapshot| snapshot.session_ts_ms),
        last_ts_ms: snapshots.last().map(|snapshot| snapshot.session_ts_ms),
        issue: None,
    })
}

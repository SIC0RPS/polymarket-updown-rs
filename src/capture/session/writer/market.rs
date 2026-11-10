use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{atomic::Ordering, Arc};

use anyhow::{Context, Result};
use futures_util::future::join_all;
use tokio::sync::mpsc;

use crate::capture::arrow::{engine_arrow_path, EngineArrowWriter};
use crate::capture::events::PolymarketMarketWsIn;
use crate::capture::finalization::{CaptureJournalFile, CaptureJournalManifestEntry};
use crate::capture::polymarket_resolution::{
    polymarket_resolution_arrow_path, PolymarketResolutionArrowWriter, PolymarketResolutionCapture,
};
use crate::capture::scope::{capture_filtered_polymarket_state, normalize_capture_underlying};

use super::super::{
    journals::{hash_journal_file, BinanceProviderRecord},
    CaptureWriterConfig,
};

pub(in crate::capture::session) enum MarketJournalMsg {
    Snapshot(PolymarketMarketWsIn),
    Resolution(PolymarketResolutionCapture),
    Provider(Arc<BinanceProviderRecord>),
    Finish,
}

pub(in crate::capture::session) struct MarketJournalResult {
    pub(crate) underlying: String,
    pub(crate) snapshot_file: PathBuf,
    pub(crate) snapshot_written: u64,
    pub(crate) snapshot_sha256: Option<String>,
    pub(crate) resolution_file: PathBuf,
    pub(crate) resolution_written: u64,
    pub(crate) resolution_sha256: Option<String>,
    pub(crate) provider_file: PathBuf,
    pub(crate) provider_written: u64,
    pub(crate) provider_sha256: Option<String>,
    pub(crate) writer_error: Option<String>,
}

pub(super) fn start_market_journals(cfg: &mut CaptureWriterConfig) -> Result<()> {
    for (underlying, rx) in std::mem::take(&mut cfg.market_receivers) {
        let session_dir = cfg.session_dir.clone();
        let writer_underlying = underlying.clone();
        let thread = std::thread::Builder::new()
            .name(format!("capture-market-{underlying}"))
            .spawn(move || run_market_journal(rx, &session_dir, writer_underlying))
            .with_context(|| format!("spawn market journal writer for {underlying}"))?;
        cfg.market_threads.push((underlying, thread));
    }
    Ok(())
}

pub(super) async fn finish_market_journals(cfg: &mut CaptureWriterConfig) -> Result<bool> {
    let finish_sends = cfg
        .market_journals
        .iter()
        .map(|(underlying, lane)| async move {
            (
                underlying.clone(),
                lane.tx
                    .send(MarketJournalMsg::Finish)
                    .await
                    .err()
                    .map(|error| format!("send market journal finish: {error}")),
            )
        });
    let finish_errors: HashMap<String, String> = join_all(finish_sends)
        .await
        .into_iter()
        .filter_map(|(underlying, error)| error.map(|error| (underlying, error)))
        .collect();

    let threads = std::mem::take(&mut cfg.market_threads);
    let joined = tokio::task::spawn_blocking(move || {
        threads
            .into_iter()
            .map(|(underlying, thread)| (underlying, thread.join()))
            .collect::<HashMap<_, _>>()
    })
    .await;
    let join_error = joined.as_ref().err().map(ToString::to_string);
    let joined = joined.unwrap_or_default();

    let mut underlyings: Vec<_> = cfg.market_journals.keys().cloned().collect();
    underlyings.sort();
    let mut valid = true;
    for underlying in underlyings {
        let lane = &cfg.market_journals[&underlying];
        let attempted = lane.attempted.load(Ordering::Acquire);
        let full = lane.full.load(Ordering::Acquire);
        let closed = lane.closed.load(Ordering::Acquire);
        let accepted = attempted.saturating_sub(full.saturating_add(closed));
        let result = joined
            .get(&underlying)
            .and_then(|joined| joined.as_ref().ok());
        let snapshot_path = engine_arrow_path(&cfg.session_dir, &underlying);
        let resolution_path = polymarket_resolution_arrow_path(&cfg.session_dir, &underlying);
        let provider_path = cfg
            .session_dir
            .join(format!("binance_provider_{}.jsonl", underlying.to_ascii_lowercase()));
        let mut errors = Vec::new();
        if let Some(error) = finish_errors.get(&underlying) {
            errors.push(error.clone());
        }
        if let Some(error) = &join_error {
            errors.push(format!("join market journal writers: {error}"));
        }
        match joined.get(&underlying) {
            Some(Ok(result)) => {
                if result.underlying != underlying {
                    errors.push(format!(
                        "market journal writer identity mismatch: expected {underlying}, reported {}",
                        result.underlying
                    ));
                }
                if result.snapshot_file != snapshot_path {
                    errors.push(format!(
                        "market snapshot file mismatch: expected {}, reported {}",
                        snapshot_path.display(),
                        result.snapshot_file.display()
                    ));
                }
                if result.resolution_file != resolution_path {
                    errors.push(format!(
                        "market resolution file mismatch: expected {}, reported {}",
                        resolution_path.display(),
                        result.resolution_file.display()
                    ));
                }
                if result.provider_file != provider_path {
                    errors.push(format!(
                        "Binance provider file mismatch: expected {}, reported {}",
                        provider_path.display(),
                        result.provider_file.display()
                    ));
                }
                if let Some(error) = &result.writer_error {
                    errors.push(error.clone());
                }
            }
            Some(Err(_)) => errors.push("market journal writer panicked".to_string()),
            None => errors.push("market journal writer did not finish".to_string()),
        }
        let written = result.map_or(0, |result| {
            result
                .snapshot_written
                .saturating_add(result.resolution_written)
                .saturating_add(result.provider_written)
        });
        if result.is_some_and(|result| {
            result
                .snapshot_written
                .checked_add(result.resolution_written)
                .and_then(|written| written.checked_add(result.provider_written))
                .is_none()
        }) {
            errors.push("market journal written count overflow".to_string());
        }
        let writer_error = (!errors.is_empty()).then(|| errors.join("; "));
        if writer_error.is_some()
            || full != 0
            || closed != 0
            || accepted != written
            || accepted
                .checked_add(full)
                .and_then(|count| count.checked_add(closed))
                != Some(attempted)
        {
            valid = false;
        }
        let lower = underlying.to_ascii_lowercase();
        cfg.journal_entries.push(CaptureJournalManifestEntry {
            lane: "market".to_string(),
            provider_evidence_version: Some(1),
            causal_identity: underlying,
            attempted,
            accepted,
            full,
            closed,
            written,
            first_sequence: None,
            last_sequence: None,
            writer_error,
            files: vec![
                CaptureJournalFile {
                    path: format!("polymarket_engine_state_replayable_{lower}.arrow"),
                    written: result.map_or(0, |result| result.snapshot_written),
                    sha256: result.and_then(|result| result.snapshot_sha256.clone()),
                },
                CaptureJournalFile {
                    path: format!("polymarket_resolution_{lower}.arrow"),
                    written: result.map_or(0, |result| result.resolution_written),
                    sha256: result.and_then(|result| result.resolution_sha256.clone()),
                },
                CaptureJournalFile {
                    path: format!("binance_provider_{lower}.jsonl"),
                    written: result.map_or(0, |result| result.provider_written),
                    sha256: result.and_then(|result| result.provider_sha256.clone()),
                },
            ],
        });
    }
    Ok(valid)
}

pub(super) fn run_market_journal(
    mut rx: mpsc::Receiver<MarketJournalMsg>,
    session_dir: &Path,
    underlying: String,
) -> MarketJournalResult {
    let snapshot_file = engine_arrow_path(session_dir, &underlying);
    let resolution_file = polymarket_resolution_arrow_path(session_dir, &underlying);
    let provider_name = format!("binance_provider_{}.jsonl", underlying.to_ascii_lowercase());
    let provider_file = session_dir.join(&provider_name);
    let mut errors = Vec::new();
    // Create even an empty evidence file so capability never depends on traffic.
    let mut provider_writer = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&provider_file)
    {
        Ok(file) => Some(BufWriter::new(file)),
        Err(error) => {
            errors.push(format!("open Binance provider journal: {error}"));
            None
        }
    };
    let mut provider_failed = provider_writer.is_none();
    let mut provider_written = 0_u64;
    let mut snapshot_writer = match EngineArrowWriter::create(snapshot_file.clone()) {
        Ok(writer) => Some(writer),
        Err(error) => {
            errors.push(format!("create engine-state journal: {error:#}"));
            None
        }
    };
    let mut resolution_writer =
        match PolymarketResolutionArrowWriter::create(resolution_file.clone()) {
            Ok(writer) => Some(writer),
            Err(error) => {
                errors.push(format!("create Polymarket resolution journal: {error:#}"));
                None
            }
        };
    let mut snapshot_rows = 0_u64;
    let mut resolution_rows = 0_u64;
    let mut snapshot_failed = false;
    let mut resolution_failed = false;
    let mut finished = false;
    while let Some(message) = rx.blocking_recv() {
        match message {
            MarketJournalMsg::Finish => {
                finished = true;
                break;
            }
            MarketJournalMsg::Provider(record) => {
                if provider_failed {
                    continue;
                }
                match super::write_buffered_jsonl_capture(
                    session_dir,
                    &provider_name,
                    &mut provider_writer,
                    "Binance provider",
                    record.as_ref(),
                ) {
                    Ok(()) => provider_written += 1,
                    Err(error) => {
                        errors.push(format!("write Binance provider journal: {error:#}"));
                        provider_failed = true;
                    }
                }
            }
            MarketJournalMsg::Snapshot(snapshot) => {
                if snapshot_failed {
                    continue;
                }
                let Some(writer) = snapshot_writer.as_mut() else {
                    continue;
                };
                let payload = Arc::new(capture_filtered_polymarket_state(
                    snapshot.payload.as_ref(),
                    std::slice::from_ref(&underlying),
                    snapshot.session_ts_ms,
                ));
                let replayable = PolymarketMarketWsIn {
                    session_ts_ms: snapshot.session_ts_ms,
                    channel: "engine_state_by_underlying".to_string(),
                    asset_id: underlying.clone(),
                    event_ts_ms: snapshot.event_ts_ms,
                    recv_ts_ms: snapshot.recv_ts_ms,
                    ingest_delay_ms: snapshot.ingest_delay_ms,
                    payload,
                };
                if let Err(error) = writer.write(replayable) {
                    errors.push(format!("write engine-state journal: {error:#}"));
                    snapshot_failed = true;
                    continue;
                }
                snapshot_rows += 1;
            }
            MarketJournalMsg::Resolution(mut resolution) => {
                let Some(canonical) = normalize_capture_underlying(&resolution.underlying) else {
                    errors.push(format!(
                        "invalid resolution underlying for market journal {}: {}",
                        underlying, resolution.underlying
                    ));
                    continue;
                };
                if !canonical.eq_ignore_ascii_case(&underlying) {
                    errors.push(format!(
                        "resolution underlying does not match market journal {}: {}",
                        underlying, resolution.underlying
                    ));
                    continue;
                }
                if resolution_failed {
                    continue;
                }
                let Some(writer) = resolution_writer.as_mut() else {
                    continue;
                };
                resolution.underlying = canonical;
                if let Err(error) = writer.write(&resolution) {
                    errors.push(format!("write Polymarket resolution journal: {error:#}"));
                    resolution_failed = true;
                    continue;
                }
                resolution_rows += 1;
            }
        }
    }
    if !finished {
        errors.push("market journal closed without finish".to_string());
    }
    let snapshot_written = match snapshot_writer {
        Some(writer) => match writer.finish() {
            Ok(rows) => rows,
            Err(error) => {
                errors.push(format!("finish engine-state journal: {error:#}"));
                0
            }
        },
        None => 0,
    };
    let resolution_written = match resolution_writer {
        Some(writer) => match writer.finish() {
            Ok(rows) => rows,
            Err(error) => {
                errors.push(format!("finish Polymarket resolution journal: {error:#}"));
                0
            }
        },
        None => 0,
    };
    if snapshot_written != snapshot_rows {
        errors.push(format!(
            "engine-state journal row count changed: accepted {snapshot_rows}, written {snapshot_written}"
        ));
    }
    if resolution_written != resolution_rows {
        errors.push(format!(
            "Polymarket resolution journal row count changed: accepted {resolution_rows}, written {resolution_written}"
        ));
    }
    if let Some(writer) = provider_writer {
        if let Err(error) = writer.into_inner() {
            errors.push(format!("flush Binance provider journal: {error}"));
            provider_failed = true;
        }
    }
    let provider_sha256 = if provider_failed {
        None
    } else {
        match hash_journal_file(&provider_file) {
            Ok(hash) => Some(hash),
            Err(error) => {
                errors.push(format!("hash Binance provider journal: {error}"));
                None
            }
        }
    };
    let snapshot_sha256 = match hash_journal_file(&snapshot_file) {
        Ok(hash) => Some(hash),
        Err(error) => {
            errors.push(format!("hash engine-state journal: {error}"));
            None
        }
    };
    let resolution_sha256 = match hash_journal_file(&resolution_file) {
        Ok(hash) => Some(hash),
        Err(error) => {
            errors.push(format!("hash Polymarket resolution journal: {error}"));
            None
        }
    };
    MarketJournalResult {
        underlying,
        snapshot_file,
        snapshot_written,
        snapshot_sha256,
        resolution_file,
        resolution_written,
        resolution_sha256,
        provider_file,
        provider_written,
        provider_sha256,
        writer_error: (!errors.is_empty()).then(|| errors.join("; ")),
    }
}

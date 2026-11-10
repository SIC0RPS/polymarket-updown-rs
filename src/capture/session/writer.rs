use super::*;
use crate::capture::arrow::POLYMARKET_WS_INGRESS_ARROW_FILE;
use crate::capture::finalization::{
    finalize_capture_writer, write_capture_writer_failure_marker, CaptureJournalFile,
    CaptureJournalManifestEntry,
};
use bbwick_engine::polymarket_ws::{PolymarketBookAdmissionRecord, PolymarketWsIngressRecord};
use tokio::sync::oneshot;

#[path = "writer/strategy_consumption.rs"]
mod strategy_consumption;
pub(super) use strategy_consumption::{
    run_strategy_consumption_journal, strategy_consumption_journal_path,
    StrategyConsumptionJournalMsg, StrategyConsumptionJournalResult,
};
#[path = "writer/market.rs"]
mod market;
use market::{finish_market_journals, start_market_journals};
pub(super) use market::{MarketJournalMsg, MarketJournalResult};
#[path = "writer/raw_ingress.rs"]
mod raw_ingress;
use raw_ingress::run_raw_ingress_journal;
#[path = "writer/book_admission.rs"]
mod book_admission;
use book_admission::run_book_admission_journal;
#[path = "writer/priority.rs"]
mod priority;
use priority::run_priority_journal;

use super::journals::{finish_strategy_consumption_journals, start_strategy_consumption_journals};

pub(super) async fn run_capture_writer(
    priority_rx: mpsc::Receiver<CaptureMsg>,
    raw_rx: mpsc::Receiver<PolymarketWsIngressRecord>,
    book_rx: mpsc::Receiver<PolymarketBookAdmissionRecord>,
    mut cfg: CaptureWriterConfig,
) -> Result<()> {
    if let Err(error) = run_capture_writer_inner(priority_rx, raw_rx, book_rx, &mut cfg).await {
        if let Err(marker_error) = write_capture_writer_failure_marker(&cfg, &error).await {
            tracing::error!(
                target: "capture.writer",
                error = %marker_error,
                "capture_writer_failure_marker_failed"
            );
        }
        tracing::error!(target: "capture.writer", error = %error, "capture_writer_failed");
        return Err(error);
    }
    if tokio::fs::try_exists(cfg.session_dir.join("capture_artifact_manifest.json"))
        .await
        .context("inspect finalized capture before retention")?
    {
        let cap = cfg.retention_cap.lock().await;
        let base_dir = cfg
            .session_dir
            .parent()
            .and_then(Path::to_str)
            .context("capture retention base directory is invalid")?;
        crate::capture::retention::prune_capture_sessions(base_dir, *cap, cfg.min_save_duration_ms)
            .await
            .context("prune finalized capture retention")?;
    }
    Ok(())
}

async fn run_capture_writer_inner(
    priority_rx: mpsc::Receiver<CaptureMsg>,
    raw_rx: mpsc::Receiver<PolymarketWsIngressRecord>,
    book_rx: mpsc::Receiver<PolymarketBookAdmissionRecord>,
    cfg: &mut CaptureWriterConfig,
) -> Result<()> {
    let mut start_failed = false;
    if let Err(error) = start_strategy_consumption_journals(cfg) {
        tracing::error!(target: "capture.writer", error = %error, "strategy_journal_start_failed");
        start_failed = true;
    }
    if let Err(error) = start_market_journals(cfg) {
        tracing::error!(target: "capture.writer", error = %error, "market_journal_start_failed");
        start_failed = true;
    }

    let runtime = tokio::runtime::Handle::current();
    let (raw_finish_tx, raw_finish_rx) = oneshot::channel();
    let raw_thread = {
        let session_dir = cfg.session_dir.clone();
        let capture_lane = cfg.polymarket_ws_ingress_capture.clone();
        let runtime = runtime.clone();
        match std::thread::Builder::new()
            .name("capture-raw-ingress".to_string())
            .spawn(move || {
                run_raw_ingress_journal(raw_rx, raw_finish_rx, session_dir, capture_lane, runtime)
            }) {
            Ok(thread) => Some(thread),
            Err(error) => {
                tracing::error!(target: "capture.writer", error = %error, "raw_ingress_journal_start_failed");
                start_failed = true;
                None
            }
        }
    };
    let (book_finish_tx, book_finish_rx) = oneshot::channel();
    let book_thread = {
        let session_dir = cfg.session_dir.clone();
        let capture_lane = cfg.polymarket_book_admission_capture.clone();
        let runtime = runtime.clone();
        match std::thread::Builder::new()
            .name("capture-book-admission".to_string())
            .spawn(move || {
                run_book_admission_journal(
                    book_rx,
                    book_finish_rx,
                    session_dir,
                    capture_lane,
                    runtime,
                )
            }) {
            Ok(thread) => Some(thread),
            Err(error) => {
                tracing::error!(target: "capture.writer", error = %error, "book_admission_journal_start_failed");
                start_failed = true;
                None
            }
        }
    };
    let (priority_finish_tx, priority_finish_rx) = oneshot::channel();
    let priority_thread = {
        let session_dir = cfg.session_dir.clone();
        match std::thread::Builder::new()
            .name("capture-priority".to_string())
            .spawn(move || {
                run_priority_journal(priority_rx, priority_finish_rx, session_dir, runtime)
            }) {
            Ok(thread) => Some(thread),
            Err(error) => {
                tracing::error!(target: "capture.writer", error = %error, "priority_journal_start_failed");
                start_failed = true;
                None
            }
        }
    };

    let exit_reason = if start_failed {
        "writer_error"
    } else {
        tokio::select! {
            _ = cfg.shutdown.cancelled() => "shutdown",
            _ = tokio::time::sleep(Duration::from_millis(cfg.max_duration_ms)) => "max_duration",
        }
    };

    cfg.ohlc_shutdown.cancel();
    cfg.required_capture_admission
        .fetch_or(REQUIRED_CAPTURE_CLOSED, Ordering::AcqRel);
    while cfg.required_capture_admission.load(Ordering::Acquire) & REQUIRED_CAPTURE_IN_FLIGHT != 0 {
        tokio::task::yield_now().await;
    }
    cfg.polymarket_ws_ingress_capture.deactivate();
    cfg.polymarket_book_admission_capture.deactivate();
    let _ = raw_finish_tx.send(());
    let _ = book_finish_tx.send(());
    let _ = priority_finish_tx.send(());

    let raw_join = raw_thread.map(|thread| tokio::task::spawn_blocking(move || thread.join()));
    let book_join = book_thread.map(|thread| tokio::task::spawn_blocking(move || thread.join()));
    let priority_join =
        priority_thread.map(|thread| tokio::task::spawn_blocking(move || thread.join()));

    let strategy_contiguous = match finish_strategy_consumption_journals(cfg).await {
        Ok(complete) => complete,
        Err(error) => {
            tracing::error!(target: "capture.writer", error = %error, "strategy_journal_finish_failed");
            false
        }
    };
    let market_complete = match finish_market_journals(cfg).await {
        Ok(complete) => complete,
        Err(error) => {
            tracing::error!(target: "capture.writer", error = %error, "market_journal_finish_failed");
            false
        }
    };
    let ohlc_result = match cfg.ohlc_task.take() {
        Some(task) => match task.await {
            Ok(result) => result.context("finish capture OHLC watcher"),
            Err(error) => Err(error).context("join capture OHLC watcher"),
        },
        None => Err(anyhow::anyhow!("capture OHLC watcher task missing")),
    };
    if let Err(error) = &ohlc_result {
        tracing::error!(
            target: "capture.writer",
            error = %format_args!("{error:#}"),
            "capture_ohlc_finish_failed"
        );
    }
    if start_failed || !strategy_contiguous || !market_complete || ohlc_result.is_err() {
        cfg.required_capture_admission
            .fetch_or(REQUIRED_CAPTURE_LOST, Ordering::AcqRel);
    }

    let raw_result = match raw_join {
        Some(join) => match join.await {
            Ok(Ok(result)) => Some(result),
            _ => None,
        },
        None => None,
    };
    let book_result = match book_join {
        Some(join) => match join.await {
            Ok(Ok(result)) => Some(result),
            _ => None,
        },
        None => None,
    };
    let priority_result = match priority_join {
        Some(join) => match join.await {
            Ok(Ok(result)) => Some(result),
            _ => None,
        },
        None => None,
    };

    let raw_attempted = raw_result.as_ref().map_or_else(
        || cfg.polymarket_ws_ingress_capture.attempted_count(),
        |result| result.attempted,
    );
    let raw_accepted = raw_result.as_ref().map_or_else(
        || cfg.polymarket_ws_ingress_capture.accepted_count(),
        |result| result.accepted,
    );
    let raw_full = raw_result.as_ref().map_or_else(
        || cfg.polymarket_ws_ingress_capture.full_count(),
        |result| result.full,
    );
    let raw_closed = raw_result.as_ref().map_or_else(
        || cfg.polymarket_ws_ingress_capture.closed_count(),
        |result| result.closed,
    );
    let raw_lost = raw_result.as_ref().map_or_else(
        || cfg.polymarket_ws_ingress_capture.lost_count(),
        |result| result.lost,
    );
    let raw_written = raw_result.as_ref().map_or(0, |result| result.written);
    let raw_validation_failures = raw_result
        .as_ref()
        .map_or(0, |result| result.validation_failures);
    let mut raw_errors = Vec::new();
    match &raw_result {
        Some(result) => {
            let expected_file = cfg.session_dir.join(POLYMARKET_WS_INGRESS_ARROW_FILE);
            if result.file != expected_file {
                raw_errors.push(format!(
                    "raw ingress journal file mismatch: expected {}, reported {}",
                    expected_file.display(),
                    result.file.display()
                ));
            }
            if let Some(error) = &result.writer_error {
                raw_errors.push(error.clone());
            }
        }
        None => raw_errors.push("raw ingress journal did not finish".to_string()),
    }
    let raw_error = (!raw_errors.is_empty()).then(|| raw_errors.join("; "));
    cfg.journal_entries.push(CaptureJournalManifestEntry {
        lane: "raw_ingress".to_string(),
        provider_evidence_version: None,
        causal_identity: "raw_ingress".to_string(),
        attempted: raw_attempted,
        accepted: raw_accepted,
        full: raw_full,
        closed: raw_closed,
        written: raw_written,
        first_sequence: None,
        last_sequence: None,
        writer_error: raw_error.clone(),
        files: vec![CaptureJournalFile {
            path: POLYMARKET_WS_INGRESS_ARROW_FILE.to_string(),
            written: raw_written,
            sha256: raw_result.as_ref().and_then(|result| result.sha256.clone()),
        }],
    });

    let book_attempted = book_result.as_ref().map_or_else(
        || cfg.polymarket_book_admission_capture.attempted_count(),
        |result| result.attempted,
    );
    let book_accepted = book_result.as_ref().map_or_else(
        || cfg.polymarket_book_admission_capture.accepted_count(),
        |result| result.accepted,
    );
    let book_full = book_result.as_ref().map_or_else(
        || cfg.polymarket_book_admission_capture.full_count(),
        |result| result.full,
    );
    let book_closed = book_result.as_ref().map_or_else(
        || cfg.polymarket_book_admission_capture.closed_count(),
        |result| result.closed,
    );
    let book_written = book_result.as_ref().map_or(0, |result| result.written);
    let mut book_errors = Vec::new();
    match &book_result {
        Some(result) => {
            let expected_file = cfg.session_dir.join("polymarket_book_admission.jsonl");
            if result.file != expected_file {
                book_errors.push(format!(
                    "book admission journal file mismatch: expected {}, reported {}",
                    expected_file.display(),
                    result.file.display()
                ));
            }
            if let Some(error) = &result.writer_error {
                book_errors.push(error.clone());
            }
        }
        None => book_errors.push("book admission journal did not finish".to_string()),
    }
    let book_error = (!book_errors.is_empty()).then(|| book_errors.join("; "));
    cfg.journal_entries.push(CaptureJournalManifestEntry {
        lane: "book_admission".to_string(),
        provider_evidence_version: None,
        causal_identity: "book_admission".to_string(),
        attempted: book_attempted,
        accepted: book_accepted,
        full: book_full,
        closed: book_closed,
        written: book_written,
        first_sequence: None,
        last_sequence: None,
        writer_error: book_error,
        files: vec![CaptureJournalFile {
            path: "polymarket_book_admission.jsonl".to_string(),
            written: book_written,
            sha256: book_result
                .as_ref()
                .and_then(|result| result.sha256.clone()),
        }],
    });

    let priority_attempted = cfg.priority_attempted.load(Ordering::Acquire);
    let priority_full = cfg.priority_full.load(Ordering::Acquire);
    let priority_closed = cfg.priority_closed.load(Ordering::Acquire);
    let priority_accepted =
        priority_attempted.saturating_sub(priority_full.saturating_add(priority_closed));
    let priority_written = priority_result.as_ref().map_or(0, |result| result.written);
    let order_submit_latency_rows = priority_result
        .as_ref()
        .and_then(|result| {
            result
                .files
                .iter()
                .find(|file| file.path == "order_submit_latency.jsonl")
        })
        .map_or(0, |file| file.rows);
    cfg.journal_entries.push(CaptureJournalManifestEntry {
        lane: "priority".to_string(),
        provider_evidence_version: None,
        causal_identity: "priority".to_string(),
        attempted: priority_attempted,
        accepted: priority_accepted,
        full: priority_full,
        closed: priority_closed,
        written: priority_written,
        first_sequence: None,
        last_sequence: None,
        writer_error: priority_result
            .as_ref()
            .and_then(|result| result.writer_error.clone())
            .or_else(|| {
                priority_result
                    .is_none()
                    .then(|| "priority journal did not finish".to_string())
            }),
        files: priority_result.as_ref().map_or_else(Vec::new, |result| {
            result
                .files
                .iter()
                .map(|file| CaptureJournalFile {
                    path: file.path.clone(),
                    written: file.rows,
                    sha256: file.sha256.clone(),
                })
                .collect()
        }),
    });

    let strategy_written = cfg
        .journal_entries
        .iter()
        .filter(|entry| entry.lane == "strategy_consumption")
        .map(|entry| entry.written)
        .sum();
    let strategy_last_sequence = cfg
        .journal_entries
        .iter()
        .filter(|entry| entry.lane == "strategy_consumption")
        .filter_map(|entry| entry.last_sequence)
        .max()
        .unwrap_or(0);

    let finalization_result = finalize_capture_writer(
        cfg,
        exit_reason,
        cfg.strategy_consumption_attempted.load(Ordering::Acquire),
        strategy_written,
        cfg.strategy_consumption_lost.load(Ordering::Acquire),
        cfg.required_capture_admission.load(Ordering::Acquire) & REQUIRED_CAPTURE_LOST != 0,
        cfg.strategy_consumption_terminal_sequence
            .load(Ordering::Acquire),
        strategy_last_sequence,
        strategy_contiguous,
        order_submit_latency_rows,
        raw_written,
        raw_accepted,
        raw_lost,
        raw_validation_failures,
        raw_error,
    )
    .await;
    match (ohlc_result, finalization_result) {
        (Ok(()), result) => result,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(finalization_error)) => {
            Err(finalization_error).context(format!("capture OHLC watcher failed: {error:#}"))
        }
    }
}

pub(super) fn capture_symbol_filter(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect()
}

fn write_jsonl_capture<T: Serialize>(
    session_dir: &Path,
    file_name: &str,
    writer: &mut Option<std::io::BufWriter<fs::File>>,
    label: &str,
    event: &T,
) -> Result<()> {
    write_buffered_jsonl_capture(session_dir, file_name, writer, label, event)?;
    writer
        .as_mut()
        .with_context(|| format!("missing {label} capture writer"))?
        .flush()
        .with_context(|| format!("flush {label} capture"))
}

fn write_buffered_jsonl_capture<T: Serialize>(
    session_dir: &Path,
    file_name: &str,
    writer: &mut Option<std::io::BufWriter<fs::File>>,
    label: &str,
    event: &T,
) -> Result<()> {
    if writer.is_none() {
        let path = session_dir.join(file_name);
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        *writer = Some(std::io::BufWriter::new(file));
    }
    let writer = writer
        .as_mut()
        .with_context(|| format!("missing {label} capture writer"))?;
    writer
        .write_all(
            serde_json::to_string(event)
                .with_context(|| format!("serialize {label} capture"))?
                .as_bytes(),
        )
        .with_context(|| format!("write {label} capture"))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("write {label} newline"))?;
    Ok(())
}

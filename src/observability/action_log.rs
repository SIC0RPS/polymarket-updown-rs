use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use tokio::fs::File;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc::error::TryRecvError;

use crate::observability::action::ActionEvent;

#[derive(Serialize)]
struct ActionLogSummaryRow {
    #[serde(rename = "type")]
    type_name: &'static str,
    ts_ms: u64,
    event_type: &'static str,
    count: u64,
    first_ts_ms: u64,
    last_ts_ms: u64,
}

#[derive(Debug)]
struct CoalescedRepeat {
    signature: Vec<u8>,
    event_type: &'static str,
    first_ts_ms: u64,
    last_ts_ms: u64,
    count: u64,
}

impl CoalescedRepeat {
    fn new(signature: Vec<u8>, event_type: &'static str, ts_ms: u64) -> Self {
        Self {
            signature,
            event_type,
            first_ts_ms: ts_ms,
            last_ts_ms: ts_ms,
            count: 1,
        }
    }

    fn note_repeat(&mut self, ts_ms: u64) {
        self.last_ts_ms = ts_ms;
        self.count = self.count.saturating_add(1);
    }

    fn into_summary(self) -> Option<ActionLogSummaryRow> {
        (self.count > 1).then_some(ActionLogSummaryRow {
            type_name: "coalesced_repeats",
            ts_ms: self.last_ts_ms,
            event_type: self.event_type,
            count: self.count,
            first_ts_ms: self.first_ts_ms,
            last_ts_ms: self.last_ts_ms,
        })
    }
}

pub async fn run_action_logger(
    mut rx: tokio::sync::mpsc::Receiver<ActionEvent>,
    path: &Path,
    flush_every: usize,
    max_bytes: u64,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create action log dir {}", parent.display()))?;

    let flush_every = flush_every.max(1);
    anyhow::ensure!(max_bytes > 0, "action log max_bytes must be > 0");
    reset_action_log(path).await?;

    let (initial_writer, mut bytes_written) = open_action_log_writer(path).await?;
    let mut writer = Some(initial_writer);
    let mut pending = 0_usize;
    let mut repeats: Option<CoalescedRepeat> = None;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                // Best-effort drain so "last actions" are not lost on shutdown.
                for _ in 0..10_000 {
                    match rx.try_recv() {
                        Ok(event) => {
                            process_action_event(
                                &mut writer,
                                path,
                                &event,
                                &mut pending,
                                &mut bytes_written,
                                flush_every,
                                max_bytes,
                                &mut repeats,
                            ).await?;
                        }
                        Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                    }
                }
                break;
            },
            maybe = rx.recv() => {
                let Some(event) = maybe else { break };
                process_action_event(
                    &mut writer,
                    path,
                    &event,
                    &mut pending,
                    &mut bytes_written,
                    flush_every,
                    max_bytes,
                    &mut repeats,
                ).await?;
            }
        }
    }

    flush_repeat_summary(
        &mut writer,
        path,
        &mut pending,
        &mut bytes_written,
        flush_every,
        max_bytes,
        &mut repeats,
    )
    .await?;

    if let Some(mut w) = writer {
        w.flush().await.context("failed to flush action log")?;
    }
    Ok(())
}

async fn process_action_event(
    writer: &mut Option<tokio::io::BufWriter<File>>,
    path: &Path,
    event: &ActionEvent,
    pending: &mut usize,
    bytes_written: &mut u64,
    flush_every: usize,
    max_bytes: u64,
    repeats: &mut Option<CoalescedRepeat>,
) -> Result<()> {
    if let Some((signature, event_type, ts_ms)) = repeat_signature(event)? {
        if let Some(run) = repeats.as_mut() {
            if run.signature == signature {
                run.note_repeat(ts_ms);
                return Ok(());
            }
        }
        flush_repeat_summary(
            writer,
            path,
            pending,
            bytes_written,
            flush_every,
            max_bytes,
            repeats,
        )
        .await?;
        *repeats = Some(CoalescedRepeat::new(signature, event_type, ts_ms));
    } else {
        flush_repeat_summary(
            writer,
            path,
            pending,
            bytes_written,
            flush_every,
            max_bytes,
            repeats,
        )
        .await?;
    }

    write_action_event(
        writer,
        path,
        event,
        pending,
        bytes_written,
        flush_every,
        max_bytes,
    )
    .await
}

async fn flush_repeat_summary(
    writer: &mut Option<tokio::io::BufWriter<File>>,
    path: &Path,
    pending: &mut usize,
    bytes_written: &mut u64,
    flush_every: usize,
    max_bytes: u64,
    repeats: &mut Option<CoalescedRepeat>,
) -> Result<()> {
    let Some(run) = repeats.take() else {
        return Ok(());
    };
    let Some(summary) = run.into_summary() else {
        return Ok(());
    };
    write_json_line(
        writer,
        path,
        &summary,
        pending,
        bytes_written,
        flush_every,
        max_bytes,
    )
    .await
}

async fn write_action_event(
    writer: &mut Option<tokio::io::BufWriter<File>>,
    path: &Path,
    event: &ActionEvent,
    pending: &mut usize,
    bytes_written: &mut u64,
    flush_every: usize,
    max_bytes: u64,
) -> Result<()> {
    if writer.is_none() {
        let (opened, size_bytes) = open_action_log_writer(path).await?;
        *writer = Some(opened);
        *bytes_written = size_bytes;
    }
    write_json_line(
        writer,
        path,
        event,
        pending,
        bytes_written,
        flush_every,
        max_bytes,
    )
    .await
}

async fn write_json_line<T: Serialize + ?Sized>(
    writer: &mut Option<tokio::io::BufWriter<File>>,
    path: &Path,
    row: &T,
    pending: &mut usize,
    bytes_written: &mut u64,
    flush_every: usize,
    max_bytes: u64,
) -> Result<()> {
    let line = serde_json::to_vec(row).context("failed to serialize action log row")?;

    // Hard repo rule: actions.jsonl keeps one fixed filename and must never split into
    // timestamped/random files. When the cap is hit, truncate the same file in place.
    let next_record_bytes = (line.len() + 1) as u64;
    if *bytes_written > 0 && bytes_written.saturating_add(next_record_bytes) > max_bytes {
        if let Some(w) = writer.as_mut() {
            w.flush()
                .await
                .context("failed to flush action log before truncation")?;
        }
        let (truncated_writer, truncated_size) = open_truncated_action_log_writer(path).await?;
        *writer = Some(truncated_writer);
        *bytes_written = truncated_size;
        *pending = 0;
    }

    let w = writer.as_mut().context("action log writer missing")?;
    w.write_all(&line)
        .await
        .context("failed to write action event")?;
    w.write_all(b"\n")
        .await
        .context("failed to write action newline")?;

    *pending = pending.saturating_add(1);
    *bytes_written = bytes_written.saturating_add((line.len() + 1) as u64);

    if *pending >= flush_every {
        if let Some(w) = writer.as_mut() {
            w.flush().await.context("failed to flush action log")?;
        }
        *pending = 0;
    }

    Ok(())
}

fn repeat_signature(event: &ActionEvent) -> Result<Option<(Vec<u8>, &'static str, u64)>> {
    let Some(ts_ms) = action_event_ts_ms(event) else {
        return Ok(None);
    };
    if !is_repeat_coalescing_candidate(event) {
        return Ok(None);
    }
    let mut value =
        serde_json::to_value(event).context("failed to encode action event repeat signature")?;
    let Value::Object(map) = &mut value else {
        return Ok(None);
    };
    map.remove("ts_ms");
    let signature =
        serde_json::to_vec(&value).context("failed to serialize action event repeat signature")?;
    Ok(Some((signature, action_event_type(event), ts_ms)))
}

fn action_event_ts_ms(event: &ActionEvent) -> Option<u64> {
    match event {
        ActionEvent::OracleTick { ts_ms, .. }
        | ActionEvent::OracleStep { ts_ms, .. }
        | ActionEvent::OracleRuntimeStatus { ts_ms, .. }
        | ActionEvent::CexQuote { ts_ms, .. }
        | ActionEvent::MarketSelected { ts_ms, .. }
        | ActionEvent::NextMarketSelected { ts_ms, .. }
        | ActionEvent::NextMarketCleared { ts_ms, .. }
        | ActionEvent::Signal { ts_ms, .. }
        | ActionEvent::StrategyStatus { ts_ms, .. }
        | ActionEvent::OrderIntent { ts_ms, .. }
        | ActionEvent::OrderAck { ts_ms, .. }
        | ActionEvent::Fill { ts_ms, .. }
        | ActionEvent::RiskBlock { ts_ms, .. }
        | ActionEvent::RuntimeIssue { ts_ms, .. }
        | ActionEvent::LocalOrderBlock { ts_ms, .. }
        | ActionEvent::ReclaimResolved { ts_ms, .. } => Some(*ts_ms),
    }
}

fn action_event_type(event: &ActionEvent) -> &'static str {
    match event {
        ActionEvent::OracleTick { .. } => "oracle_tick",
        ActionEvent::OracleStep { .. } => "oracle_step",
        ActionEvent::OracleRuntimeStatus { .. } => "oracle_runtime_status",
        ActionEvent::CexQuote { .. } => "cex_quote",
        ActionEvent::MarketSelected { .. } => "market_selected",
        ActionEvent::NextMarketSelected { .. } => "next_market_selected",
        ActionEvent::NextMarketCleared { .. } => "next_market_cleared",
        ActionEvent::Signal { .. } => "signal",
        ActionEvent::StrategyStatus { .. } => "strategy_status",
        ActionEvent::OrderIntent { .. } => "order_intent",
        ActionEvent::OrderAck { .. } => "order_ack",
        ActionEvent::Fill { .. } => "fill",
        ActionEvent::RiskBlock { .. } => "risk_block",
        ActionEvent::RuntimeIssue { .. } => "runtime_issue",
        ActionEvent::LocalOrderBlock { .. } => "local_order_block",
        ActionEvent::ReclaimResolved { .. } => "reclaim_resolved",
    }
}

fn is_repeat_coalescing_candidate(event: &ActionEvent) -> bool {
    if matches!(event, ActionEvent::RuntimeIssue { .. }) {
        return false;
    }
    !matches!(
        event,
        ActionEvent::MarketSelected { .. }
            | ActionEvent::NextMarketSelected { .. }
            | ActionEvent::NextMarketCleared { .. }
            | ActionEvent::OrderAck { .. }
            | ActionEvent::Fill { .. }
    )
}

async fn open_action_log_writer(path: &Path) -> Result<(tokio::io::BufWriter<File>, u64)> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("failed to open action log {}", path.display()))?;
    let size_bytes = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    Ok((tokio::io::BufWriter::new(file), size_bytes))
}

async fn open_truncated_action_log_writer(
    path: &Path,
) -> Result<(tokio::io::BufWriter<File>, u64)> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .with_context(|| format!("failed to truncate action log {}", path.display()))?;
    Ok((tokio::io::BufWriter::new(file), 0))
}

async fn reset_action_log(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to reset action log {}", path.display()))
        }
    }
}

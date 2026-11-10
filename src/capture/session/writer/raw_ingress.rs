use std::path::PathBuf;

use bbwick_engine::polymarket_ws::{PolymarketWsIngressCaptureLane, PolymarketWsIngressRecord};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use crate::capture::arrow::{polymarket_ws_ingress_arrow_path, PolymarketWsIngressArrowWriter};

use super::super::journals::hash_journal_file;

pub(super) struct RawIngressJournalResult {
    pub(super) file: PathBuf,
    pub(super) attempted: u64,
    pub(super) accepted: u64,
    pub(super) full: u64,
    pub(super) closed: u64,
    pub(super) written: u64,
    pub(super) lost: u64,
    pub(super) validation_failures: u64,
    pub(super) writer_error: Option<String>,
    pub(super) sha256: Option<String>,
}

/// Runs on the raw ingress journal's dedicated thread. The finish signal ends
/// admission and the receiver drains records accepted before that boundary.
pub(super) fn run_raw_ingress_journal(
    mut rx: mpsc::Receiver<PolymarketWsIngressRecord>,
    mut finish: oneshot::Receiver<()>,
    session_dir: PathBuf,
    capture_lane: PolymarketWsIngressCaptureLane,
    runtime: Handle,
) -> RawIngressJournalResult {
    let file = polymarket_ws_ingress_arrow_path(&session_dir);
    let (mut writer, mut writer_error) = match PolymarketWsIngressArrowWriter::create(file.clone())
    {
        Ok(writer) => (Some(writer), None),
        Err(error) => {
            capture_lane.deactivate();
            (None, Some(format!("create raw ingress journal: {error:#}")))
        }
    };

    let finished = runtime.block_on(async {
        loop {
            tokio::select! {
                biased;
                signal = &mut finish => break signal.is_ok(),
                record = rx.recv() => {
                    let Some(record) = record else { break false };
                    write_record(&mut writer, &mut writer_error, &capture_lane, record);
                }
            }
        }
    });

    capture_lane.deactivate();
    rx.close();
    while let Some(record) = rx.blocking_recv() {
        write_record(&mut writer, &mut writer_error, &capture_lane, record);
    }
    if !finished && writer_error.is_none() {
        writer_error = Some("raw ingress journal closed without finish".to_string());
    }

    let (written, validation_failures) = match writer {
        Some(writer) => match writer.finish() {
            Ok(summary) => (summary.rows, summary.validation_failures),
            Err(error) => {
                writer_error
                    .get_or_insert_with(|| format!("finish raw ingress journal: {error:#}"));
                (0, 0)
            }
        },
        None => (0, 0),
    };
    let sha256 = if writer_error.is_none() {
        match hash_journal_file(&file) {
            Ok(hash) => Some(hash),
            Err(error) => {
                writer_error = Some(format!("hash raw ingress journal: {error}"));
                None
            }
        }
    } else {
        None
    };
    let attempted = capture_lane.attempted_count();
    let accepted = capture_lane.accepted_count();
    let full = capture_lane.full_count();
    let closed = capture_lane.closed_count();
    let lost = capture_lane.lost_count();
    RawIngressJournalResult {
        file,
        attempted,
        accepted,
        full,
        closed,
        written,
        lost,
        validation_failures,
        writer_error,
        sha256,
    }
}

fn write_record(
    writer: &mut Option<PolymarketWsIngressArrowWriter>,
    writer_error: &mut Option<String>,
    capture_lane: &PolymarketWsIngressCaptureLane,
    record: PolymarketWsIngressRecord,
) {
    if let Some(active_writer) = writer.as_mut() {
        if let Err(error) = active_writer.write(record) {
            *writer_error = Some(format!("write raw ingress journal: {error:#}"));
            capture_lane.deactivate();
            *writer = None;
        }
    }
}

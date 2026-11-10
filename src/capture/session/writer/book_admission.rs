use std::fs::{self, File};
use std::io::BufWriter;
use std::path::PathBuf;

use bbwick_engine::polymarket_ws::{
    PolymarketBookAdmissionCaptureLane, PolymarketBookAdmissionRecord,
};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use super::super::journals::hash_journal_file;

pub(super) struct BookAdmissionJournalResult {
    pub(super) file: PathBuf,
    pub(super) attempted: u64,
    pub(super) accepted: u64,
    pub(super) full: u64,
    pub(super) closed: u64,
    pub(super) written: u64,
    pub(super) writer_error: Option<String>,
    pub(super) sha256: Option<String>,
}

pub(super) fn run_book_admission_journal(
    mut rx: mpsc::Receiver<PolymarketBookAdmissionRecord>,
    mut finish: oneshot::Receiver<()>,
    session_dir: PathBuf,
    capture_lane: PolymarketBookAdmissionCaptureLane,
    runtime: Handle,
) -> BookAdmissionJournalResult {
    let file = session_dir.join("polymarket_book_admission.jsonl");
    let (mut writer, mut writer_error) =
        match fs::OpenOptions::new().create(true).append(true).open(&file) {
            Ok(file) => (Some(BufWriter::new(file)), None),
            Err(error) => {
                capture_lane.deactivate();
                (None, Some(format!("open book admission journal: {error}")))
            }
        };
    let mut written = 0_u64;
    let finished = runtime.block_on(async {
        loop {
            tokio::select! {
                biased;
                signal = &mut finish => break signal.is_ok(),
                record = rx.recv() => {
                    let Some(record) = record else { break false };
                    write_record(
                        &session_dir,
                        &mut writer,
                        &mut writer_error,
                        &capture_lane,
                        &mut written,
                        record,
                    );
                }
            }
        }
    });

    capture_lane.deactivate();
    rx.close();
    while let Some(record) = rx.blocking_recv() {
        write_record(
            &session_dir,
            &mut writer,
            &mut writer_error,
            &capture_lane,
            &mut written,
            record,
        );
    }
    if !finished && writer_error.is_none() {
        writer_error = Some("book admission journal closed without finish".to_string());
    }
    if let Some(writer) = writer {
        if let Err(error) = writer.into_inner() {
            writer_error.get_or_insert_with(|| format!("flush book admission journal: {error}"));
        }
    }
    let sha256 = if writer_error.is_none() {
        match hash_journal_file(&file) {
            Ok(hash) => Some(hash),
            Err(error) => {
                writer_error = Some(format!("hash book admission journal: {error}"));
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
    BookAdmissionJournalResult {
        file,
        attempted,
        accepted,
        full,
        closed,
        written,
        writer_error,
        sha256,
    }
}

fn write_record(
    session_dir: &PathBuf,
    writer: &mut Option<BufWriter<File>>,
    writer_error: &mut Option<String>,
    capture_lane: &PolymarketBookAdmissionCaptureLane,
    written: &mut u64,
    record: PolymarketBookAdmissionRecord,
) {
    if writer_error.is_some() {
        return;
    }
    match super::write_jsonl_capture(
        session_dir,
        "polymarket_book_admission.jsonl",
        writer,
        "Polymarket book admission",
        &record,
    ) {
        Ok(()) => *written = written.saturating_add(1),
        Err(error) => {
            *writer_error = Some(format!("write book admission journal: {error:#}"));
            capture_lane.deactivate();
        }
    }
}

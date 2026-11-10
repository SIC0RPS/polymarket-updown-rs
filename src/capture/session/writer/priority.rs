use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

use super::super::journals::hash_journal_file;
use super::super::CaptureMsg;

pub(super) struct PriorityJournalFileResult {
    pub(super) path: String,
    pub(super) rows: u64,
    pub(super) sha256: Option<String>,
}

pub(super) struct PriorityJournalResult {
    pub(super) written: u64,
    pub(super) writer_error: Option<String>,
    pub(super) files: Vec<PriorityJournalFileResult>,
}

#[derive(Default)]
struct PriorityFile {
    writer: Option<BufWriter<File>>,
    rows: u64,
}

pub(super) fn run_priority_journal(
    mut rx: mpsc::Receiver<CaptureMsg>,
    mut finish: oneshot::Receiver<()>,
    session_dir: PathBuf,
    runtime: Handle,
) -> PriorityJournalResult {
    let mut files = BTreeMap::<&'static str, PriorityFile>::new();
    let mut seed_written = false;
    let mut written = 0_u64;
    let mut writer_error = None;
    let finished = runtime.block_on(async {
        loop {
            tokio::select! {
                biased;
                signal = &mut finish => break signal.is_ok(),
                message = rx.recv() => {
                    let Some(message) = message else { break false };
                    process_record(
                        message,
                        &session_dir,
                        &mut files,
                        &mut seed_written,
                        &mut written,
                        &mut writer_error,
                    );
                }
            }
        }
    });

    rx.close();
    while let Some(message) = rx.blocking_recv() {
        process_record(
            message,
            &session_dir,
            &mut files,
            &mut seed_written,
            &mut written,
            &mut writer_error,
        );
    }
    if !finished && writer_error.is_none() {
        writer_error = Some("priority journal closed without finish".to_string());
    }

    for (name, file) in &mut files {
        if let Some(writer) = file.writer.take() {
            match writer.into_inner() {
                Ok(file) => {
                    if *name == "replay_risk_snapshot.jsonl" {
                        if let Err(error) = file.sync_data() {
                            append_error(
                                &mut writer_error,
                                format!("sync priority journal {name}: {error}"),
                            );
                        }
                    }
                }
                Err(error) => append_error(
                    &mut writer_error,
                    format!("flush priority journal {name}: {error}"),
                ),
            }
        }
    }

    let mut results = Vec::new();
    for (name, file) in files {
        let sha256 = hash_file(&session_dir, name, &mut writer_error);
        results.push(PriorityJournalFileResult {
            path: name.to_string(),
            rows: file.rows,
            sha256,
        });
    }
    if seed_written {
        results.push(PriorityJournalFileResult {
            path: "replay_seed.json".to_string(),
            rows: 1,
            sha256: hash_file(&session_dir, "replay_seed.json", &mut writer_error),
        });
    }
    PriorityJournalResult {
        written,
        writer_error,
        files: results,
    }
}

fn process_record(
    message: CaptureMsg,
    session_dir: &Path,
    files: &mut BTreeMap<&'static str, PriorityFile>,
    seed_written: &mut bool,
    written: &mut u64,
    writer_error: &mut Option<String>,
) {
    if writer_error.is_some() {
        return;
    }
    match write_record(message, session_dir, files, seed_written) {
        Ok(()) => *written = written.saturating_add(1),
        Err(error) => {
            *writer_error = Some(format!("write priority journal: {error:#}"));
        }
    }
}

fn write_record(
    message: CaptureMsg,
    session_dir: &Path,
    files: &mut BTreeMap<&'static str, PriorityFile>,
    seed_written: &mut bool,
) -> Result<()> {
    match message {
        CaptureMsg::OrderSubmitLatency(event) => write_jsonl(
            session_dir,
            files,
            "order_submit_latency.jsonl",
            "order submit latency",
            &event,
        ),
        CaptureMsg::OrderSubmitReject(event) => write_jsonl(
            session_dir,
            files,
            "order_submit_reject.jsonl",
            "order submit reject",
            &event,
        ),
        CaptureMsg::LocalOrderBlock(event) => write_jsonl(
            session_dir,
            files,
            "local_order_block.jsonl",
            "local order block",
            &event,
        ),
        CaptureMsg::RiskBlock(event) => {
            write_jsonl(session_dir, files, "risk_block.jsonl", "risk block", &event)
        }
        CaptureMsg::PolymarketUserOrder(event) => write_jsonl(
            session_dir,
            files,
            "polymarket_user_order.jsonl",
            "Polymarket user order",
            &event,
        ),
        CaptureMsg::PolymarketUserTrade(event) => write_jsonl(
            session_dir,
            files,
            "polymarket_user_trade.jsonl",
            "Polymarket user trade",
            &event,
        ),
        CaptureMsg::ReplaySeed(event) => {
            let body =
                serde_json::to_string_pretty(&event).context("serialize replay seed capture")?;
            std::fs::write(session_dir.join("replay_seed.json"), body)
                .context("write replay seed capture")?;
            *seed_written = true;
            Ok(())
        }
        CaptureMsg::ReplayRiskSnapshot(event) => {
            let file = files.entry("replay_risk_snapshot.jsonl").or_default();
            super::write_buffered_jsonl_capture(
                session_dir,
                "replay_risk_snapshot.jsonl",
                &mut file.writer,
                "replay risk snapshot",
                &event,
            )?;
            file.rows = file.rows.saturating_add(1);
            Ok(())
        }
        CaptureMsg::PolymarketDiscoverySnapshot(event) => write_jsonl(
            session_dir,
            files,
            "polymarket_discovery_snapshot.jsonl",
            "Polymarket discovery snapshot",
            &event,
        ),
        CaptureMsg::ControlActuation(event) => write_jsonl(
            session_dir,
            files,
            "control_timeline.jsonl",
            "control actuation",
            &event,
        ),
        CaptureMsg::CtfInventoryOperationLatency(event) => write_jsonl(
            session_dir,
            files,
            "ctf_inventory_operation_latency.jsonl",
            "CTF inventory operation latency",
            &event,
        ),
    }
}

fn write_jsonl<T: Serialize>(
    session_dir: &Path,
    files: &mut BTreeMap<&'static str, PriorityFile>,
    name: &'static str,
    label: &str,
    event: &T,
) -> Result<()> {
    let file = files.entry(name).or_default();
    super::write_jsonl_capture(session_dir, name, &mut file.writer, label, event)?;
    file.rows = file.rows.saturating_add(1);
    Ok(())
}

fn hash_file(session_dir: &Path, name: &str, writer_error: &mut Option<String>) -> Option<String> {
    match hash_journal_file(&session_dir.join(name)) {
        Ok(hash) => Some(hash),
        Err(error) => {
            append_error(
                writer_error,
                format!("hash priority journal {name}: {error}"),
            );
            None
        }
    }
}

fn append_error(writer_error: &mut Option<String>, error: String) {
    match writer_error {
        Some(existing) => {
            existing.push_str("; ");
            existing.push_str(&error);
        }
        None => *writer_error = Some(error),
    }
}

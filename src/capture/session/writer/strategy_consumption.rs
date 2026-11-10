use std::path::{Path, PathBuf};

use tokio::sync::mpsc;

use super::super::journals::hash_journal_file;
use crate::capture::arrow::StrategyConsumptionArrowWriter;
use crate::capture::events::{StrategyConsumptionKind, StrategyConsumptionRecord};
use crate::strategy_runtime::SymbolId;

pub(in crate::capture::session) enum StrategyConsumptionJournalMsg {
    Record(StrategyConsumptionRecord),
    Finish,
}

pub(in crate::capture::session) struct StrategyConsumptionJournalResult {
    pub(crate) actor: SymbolId,
    pub(crate) kind: StrategyConsumptionKind,
    pub(crate) file: PathBuf,
    pub(crate) written: u64,
    pub(crate) first_sequence: Option<u64>,
    pub(crate) last_sequence: Option<u64>,
    pub(crate) writer_error: Option<String>,
    pub(crate) sha256: Option<String>,
}

pub(in crate::capture::session) fn strategy_consumption_journal_path(
    session_dir: &Path,
    actor: SymbolId,
    kind: StrategyConsumptionKind,
) -> PathBuf {
    session_dir
        .join("strategy_consumption")
        .join(kind.as_str())
        .join(format!("{}.arrow", actor.as_str()))
}

/// Runs on the journal's dedicated thread. A failed journal drains its queue but
/// never asks the live strategy to stop consuming inputs.
pub(in crate::capture::session) fn run_strategy_consumption_journal(
    mut rx: mpsc::Receiver<StrategyConsumptionJournalMsg>,
    session_dir: &Path,
    actor: SymbolId,
    kind: StrategyConsumptionKind,
) -> StrategyConsumptionJournalResult {
    let file = strategy_consumption_journal_path(session_dir, actor, kind);
    let (mut writer, mut writer_error) = match StrategyConsumptionArrowWriter::create(file.clone())
    {
        Ok(writer) => (Some(writer), None),
        Err(error) => (None, Some(format!("create strategy journal: {error:#}"))),
    };
    let mut written = 0_u64;
    let mut first_sequence = None;
    let mut last_sequence = None;
    let mut finished = false;

    while let Some(message) = rx.blocking_recv() {
        match message {
            StrategyConsumptionJournalMsg::Finish => {
                finished = true;
                break;
            }
            StrategyConsumptionJournalMsg::Record(record) => {
                if writer_error.is_some() {
                    continue;
                }
                if record.actor_symbol != actor {
                    writer_error = Some(format!(
                        "strategy journal actor mismatch: expected {}, received {}",
                        actor.as_str(),
                        record.actor_symbol.as_str()
                    ));
                    continue;
                }
                if record.input.kind() != kind {
                    writer_error = Some(format!(
                        "strategy journal kind mismatch: actor={}, expected={}, received={}",
                        actor.as_str(),
                        kind.as_str(),
                        record.input.kind().as_str()
                    ));
                    continue;
                }
                if last_sequence.is_some_and(|last| record.consumption_sequence <= last) {
                    writer_error = Some(format!(
                        "strategy journal sequence is not increasing: actor={}, kind={}, sequence={}",
                        actor.as_str(),
                        kind.as_str(),
                        record.consumption_sequence
                    ));
                    continue;
                }
                let write_result = writer
                    .as_mut()
                    .expect("writer exists without error")
                    .write_record(&record);
                if let Err(error) = write_result {
                    writer_error = Some(format!("write strategy journal: {error:#}"));
                    continue;
                }
                first_sequence.get_or_insert(record.consumption_sequence);
                last_sequence = Some(record.consumption_sequence);
                written += 1;
            }
        }
    }

    if !finished && writer_error.is_none() {
        writer_error = Some("strategy journal closed without finish".to_string());
    }
    if let Some(writer) = writer {
        if let Err(error) = writer.finish() {
            writer_error.get_or_insert_with(|| format!("finish strategy journal: {error:#}"));
        }
    }
    let sha256 = if writer_error.is_none() {
        match hash_journal_file(&file) {
            Ok(hash) => Some(hash),
            Err(error) => {
                writer_error = Some(format!("hash strategy journal: {error}"));
                None
            }
        }
    } else {
        None
    };

    StrategyConsumptionJournalResult {
        actor,
        kind,
        file,
        written,
        first_sequence,
        last_sequence,
        writer_error,
        sha256,
    }
}

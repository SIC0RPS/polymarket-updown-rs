use std::collections::HashMap;

use crate::capture::arrow::{
    strategy_consumption_arrow_path, StrategyConsumptionArrowReader,
    StrategyConsumptionMergedArrowReader,
};
use crate::capture::events::StrategyConsumptionKind;
use crate::strategy_runtime::SymbolId;

use super::*;

#[derive(Clone)]
pub(super) struct StrategyConsumptionSource {
    pub(super) manifest: SourceFileManifest,
    pub(super) expected_actor: Option<SymbolId>,
    pub(super) expected_kind: Option<StrategyConsumptionKind>,
}

pub(super) struct StrategyConsumptionSourceSet {
    pub(super) sources: Vec<StrategyConsumptionSource>,
    pub(super) rows: u64,
}

pub(super) fn validate_strategy_consumption(
    capture: &Path,
    identity: &crate::capture::FinalizedCaptureIdentity,
) -> Result<HashMap<String, StrategyConsumptionSourceSet>> {
    let attempted = identity
        .strategy_consumption_attempted
        .context("finalized capture is missing strategy_consumption_attempted")?;
    let written = identity
        .strategy_consumption_written
        .context("finalized capture is missing strategy_consumption_written")?;
    let lost = identity
        .strategy_consumption_lost
        .context("finalized capture is missing strategy_consumption_lost")?;
    let terminal = identity
        .strategy_consumption_terminal_sequence
        .context("finalized capture is missing strategy_consumption_terminal_sequence")?;
    let last_written = identity
        .strategy_consumption_last_written_sequence
        .context("finalized capture is missing strategy_consumption_last_written_sequence")?;
    let contiguous = identity
        .strategy_consumption_contiguous
        .context("finalized capture is missing strategy_consumption_contiguous")?;
    anyhow::ensure!(
        attempted == written,
        "finalized strategy consumption count mismatch"
    );
    anyhow::ensure!(
        lost == 0,
        "finalized strategy consumption contains lost records"
    );
    anyhow::ensure!(
        contiguous,
        "finalized strategy consumption is not contiguous"
    );
    anyhow::ensure!(
        terminal == last_written,
        "finalized strategy consumption endpoint mismatch"
    );

    match identity.schema_version.as_str() {
        "capture_artifact_manifest.v1" => {
            validate_legacy_strategy_consumption(capture, written, terminal)
        }
        "capture_artifact_manifest.v2" => {
            validate_journal_strategy_consumption(capture, &identity, written, terminal, false)
        }
        "capture_artifact_manifest.v3" => {
            validate_journal_strategy_consumption(capture, &identity, written, terminal, true)
        }
        other => anyhow::bail!("unsupported capture artifact manifest schema={other}"),
    }
}

fn validate_legacy_strategy_consumption(
    capture: &Path,
    written: u64,
    terminal: u64,
) -> Result<HashMap<String, StrategyConsumptionSourceSet>> {
    let path = strategy_consumption_arrow_path(capture);
    let source = SourceFileManifest::capture(&path)?;
    let mut reader = StrategyConsumptionArrowReader::open(&path)?;
    let mut rows = 0_u64;
    let mut rows_by_symbol = HashMap::new();
    while let Some((record, _)) = reader.next_event()? {
        rows = rows
            .checked_add(1)
            .context("strategy consumption row count overflow")?;
        record.validate_schema()?;
        anyhow::ensure!(
            record.consumption_sequence == rows,
            "strategy consumption sequence mismatch row={rows} sequence={}",
            record.consumption_sequence
        );
        let symbol = crate::capture::arrow::normalize_capture_symbol(record.actor_symbol.as_str());
        let count = rows_by_symbol.entry(symbol).or_insert(0_u64);
        *count = count
            .checked_add(1)
            .context("strategy consumption symbol row count overflow")?;
    }
    anyhow::ensure!(
        rows == written,
        "strategy consumption file row count mismatch"
    );
    anyhow::ensure!(
        rows == terminal,
        "strategy consumption file endpoint mismatch"
    );
    Ok(rows_by_symbol
        .into_iter()
        .map(|(symbol, rows)| {
            (
                symbol,
                StrategyConsumptionSourceSet {
                    sources: vec![StrategyConsumptionSource {
                        manifest: source.clone(),
                        expected_actor: None,
                        expected_kind: None,
                    }],
                    rows,
                },
            )
        })
        .collect())
}

fn validate_journal_strategy_consumption(
    capture: &Path,
    identity: &crate::capture::FinalizedCaptureIdentity,
    written: u64,
    terminal: u64,
    split: bool,
) -> Result<HashMap<String, StrategyConsumptionSourceSet>> {
    const ACTORS: [SymbolId; 4] = [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp];
    let kinds = if split {
        vec![
            Some(StrategyConsumptionKind::Runtime),
            Some(StrategyConsumptionKind::Chainlink),
        ]
    } else {
        vec![None]
    };
    anyhow::ensure!(
        identity
            .journals
            .iter()
            .filter(|entry| entry.lane == "strategy_consumption")
            .count()
            == ACTORS.len() * kinds.len(),
        "finalized capture strategy journal count mismatch"
    );
    let mut reader_sources = Vec::with_capacity(ACTORS.len() * kinds.len());
    let mut source_sets = HashMap::with_capacity(ACTORS.len());
    let mut expected = HashMap::with_capacity(ACTORS.len() * kinds.len());
    for actor in ACTORS {
        let symbol = crate::capture::arrow::normalize_capture_symbol(actor.as_str());
        let mut source_set = StrategyConsumptionSourceSet {
            sources: Vec::with_capacity(kinds.len()),
            rows: 0,
        };
        for kind in &kinds {
            let causal_identity = kind.map_or_else(
                || actor.as_str().to_string(),
                |kind| format!("{}/{}", actor.as_str(), kind.as_str()),
            );
            let mut matches = identity.journals.iter().filter(|entry| {
                entry.lane == "strategy_consumption" && entry.causal_identity == causal_identity
            });
            let entry = matches
                .next()
                .with_context(|| format!("missing strategy journal identity={causal_identity}"))?;
            anyhow::ensure!(
                matches.next().is_none(),
                "duplicate strategy journal identity={causal_identity}"
            );
            anyhow::ensure!(
                entry.files.len() == 1,
                "strategy journal file count mismatch identity={causal_identity}"
            );
            let file = &entry.files[0];
            let expected_path = kind.map_or_else(
                || format!("strategy_consumption/{}.arrow", actor.as_str()),
                |kind| {
                    format!(
                        "strategy_consumption/{}/{}.arrow",
                        kind.as_str(),
                        actor.as_str()
                    )
                },
            );
            anyhow::ensure!(
                file.path == expected_path,
                "strategy journal path mismatch identity={causal_identity} actual={}",
                file.path
            );
            anyhow::ensure!(
                entry
                    .accepted
                    .checked_add(entry.full)
                    .and_then(|count| count.checked_add(entry.closed))
                    == Some(entry.attempted)
                    && entry.full == 0
                    && entry.closed == 0
                    && entry.accepted == entry.written
                    && entry.written == file.written
                    && entry.writer_error.is_none(),
                "strategy journal counts or writer error identity={causal_identity}"
            );
            let path = capture.join(&file.path);
            let source = if split {
                let sha256 = file
                    .sha256
                    .clone()
                    .filter(|hash| !hash.is_empty())
                    .with_context(|| {
                        format!("strategy journal hash missing identity={causal_identity}")
                    })?;
                let (size_bytes, mtime_unix_ms) =
                    super::super::prepared_manifest::source_file_identity(&path)?;
                SourceFileManifest {
                    path: path.display().to_string(),
                    size_bytes,
                    mtime_unix_ms,
                    sha256: Some(sha256),
                }
            } else {
                let source = SourceFileManifest::capture(&path)?;
                anyhow::ensure!(
                    file.sha256.as_deref().is_some_and(|hash| !hash.is_empty())
                        && file.sha256 == source.sha256,
                    "strategy journal hash mismatch identity={causal_identity}"
                );
                source
            };
            source_set.sources.push(StrategyConsumptionSource {
                manifest: source,
                expected_actor: Some(actor),
                expected_kind: *kind,
            });
            if split {
                source_set.rows = source_set
                    .rows
                    .checked_add(entry.written)
                    .context("strategy journal symbol row count overflow")?;
            } else {
                reader_sources.push((path, Some(actor), *kind));
                expected.insert(
                    (actor, *kind),
                    (entry.written, entry.first_sequence, entry.last_sequence),
                );
            }
        }
        source_sets.insert(symbol, source_set);
    }

    if split {
        let total_rows = source_sets.values().try_fold(0_u64, |total, source_set| {
            total
                .checked_add(source_set.rows)
                .context("strategy journal total row count overflow")
        })?;
        anyhow::ensure!(
            total_rows == written && total_rows == terminal,
            "global strategy journal count or endpoint mismatch"
        );
        return Ok(source_sets);
    }

    let mut reader = StrategyConsumptionMergedArrowReader::open(&reader_sources)?;
    let mut observed = HashMap::with_capacity(expected.len());
    let mut total_rows = 0_u64;
    while let Some((record, _)) = reader.next_event()? {
        let expected_sequence = total_rows
            .checked_add(1)
            .context("strategy consumption row count overflow")?;
        anyhow::ensure!(
            record.consumption_sequence == expected_sequence,
            "global strategy journal sequence mismatch expected={expected_sequence} actual={}",
            record.consumption_sequence
        );
        total_rows = expected_sequence;
        let key = (record.actor_symbol, split.then(|| record.input.kind()));
        anyhow::ensure!(
            expected.contains_key(&key),
            "strategy journal row has no required lane actor={} kind={}",
            record.actor_symbol.as_str(),
            record.input.kind().as_str()
        );
        let state = observed.entry(key).or_insert((0_u64, None, None));
        state.0 = state
            .0
            .checked_add(1)
            .context("strategy journal lane row count overflow")?;
        state.1.get_or_insert(record.consumption_sequence);
        state.2 = Some(record.consumption_sequence);
        let symbol = crate::capture::arrow::normalize_capture_symbol(record.actor_symbol.as_str());
        let source_set = source_sets
            .get_mut(&symbol)
            .context("strategy journal row has no symbol source set")?;
        source_set.rows = source_set
            .rows
            .checked_add(1)
            .context("strategy journal symbol row count overflow")?;
    }
    for (lane, expected_state) in expected {
        let observed_state = observed.get(&lane).copied().unwrap_or((0, None, None));
        anyhow::ensure!(
            observed_state == expected_state,
            "strategy journal count or sequence endpoint mismatch actor={} kind={}",
            lane.0.as_str(),
            lane.1.map_or("mixed", StrategyConsumptionKind::as_str)
        );
    }
    anyhow::ensure!(
        total_rows == written && total_rows == terminal,
        "global strategy journal sequence is incomplete"
    );
    Ok(source_sets)
}

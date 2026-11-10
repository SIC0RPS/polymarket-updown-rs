use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

#[path = "prepared_receipts/prepare.rs"]
mod captured_lineage;
use captured_lineage::select_symbol_receipts;

use super::prepared_event_stream::{
    EventRef, CENTRAL_EVENT_SYMBOL, EVENT_KIND_CONDITION_RESOLUTION,
    EVENT_KIND_SETTLEMENT_COLLATERAL,
};
use super::PackedReplayTape;
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::symbol_process::SymbolId;
use crate::trade_session_ledger::{
    load_records_from_path, load_session_receipts_from_path,
    validate_confirmed_settlement_receipt_lineage, validate_ctf_event_record, CanonicalCtfEvent,
    ConfirmedCtfTerminalFact, ConfirmedSettlementReceiptLineage,
    FinalizedConditionResolutionReceipt, SessionReceipt, SettlementCollateralOutcome,
    TradeLedgerRecord, CAPTURED_SESSION_LEDGER_FILE,
};

#[derive(Debug, Clone)]
pub(crate) struct PreparedSettlementReceipt {
    pub(crate) lineage: ConfirmedSettlementReceiptLineage,
    pub(crate) terminal: ConfirmedCtfTerminalFact,
    terminal_ledger_seq: u64,
    terminal_ts_ms: u64,
}

#[derive(Debug)]
pub(crate) struct PreparedReceiptTape {
    resolutions: Vec<FinalizedConditionResolutionReceipt>,
    settlements: Vec<PreparedSettlementReceipt>,
    events: Arc<[EventRef]>,
}

impl PreparedReceiptTape {
    pub(crate) fn load(
        capture: &Path,
        symbols: &[String],
        tapes: &BTreeMap<String, Arc<PackedReplayTape>>,
    ) -> Result<Arc<Self>> {
        let interval = super::session_resolution::capture_replay_interval(capture)?;
        let selected = canonical_symbols(symbols)?;
        anyhow::ensure!(
            selected.len() == tapes.len(),
            "captured central receipt symbol/tape coverage mismatch symbols={} tapes={}",
            selected.len(),
            tapes.len()
        );
        let source_path = capture.join(CAPTURED_SESSION_LEDGER_FILE);
        let source_metadata = std::fs::symlink_metadata(&source_path).with_context(|| {
            format!("inspect captured session ledger {}", source_path.display())
        })?;
        anyhow::ensure!(
            source_metadata.file_type().is_file(),
            "captured session ledger is not a regular file: {}",
            source_path.display()
        );
        let receipts = load_session_receipts_from_path(&source_path, interval.run_id.as_str())?;
        let records = load_records_from_path(&source_path)?;
        let mut resolutions = Vec::new();
        let mut settlements = Vec::new();
        let mut resolution_ids = HashSet::new();
        let mut settlement_ids = HashSet::new();
        let mut ledger_sequences = HashSet::new();
        for symbol in selected {
            let symbol_tape = tapes
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
                .map(|(_, tape)| tape)
                .with_context(|| {
                    format!("captured central receipt tape missing symbol={symbol}")
                })?;
            anyhow::ensure!(
                symbol_tape.symbol() == symbol,
                "captured central receipt tape is foreign expected={} actual={}",
                symbol,
                symbol_tape.symbol()
            );
            let (symbol_resolutions, symbol_settlements) = select_symbol_receipts(
                &receipts,
                &records,
                interval.run_id.as_str(),
                symbol_tape.market_catalog(),
                symbol,
            )?;
            for receipt in symbol_resolutions {
                anyhow::ensure!(
                    resolution_ids.insert(receipt.condition_id.clone()),
                    "captured receipt condition appears in multiple symbol catalogs condition_id={}",
                    receipt.condition_id
                );
                anyhow::ensure!(
                    ledger_sequences.insert(receipt.ledger_seq),
                    "captured receipt ledger_seq appears in multiple symbol catalogs ledger_seq={}",
                    receipt.ledger_seq
                );
                resolutions.push(receipt);
            }
            for settlement in symbol_settlements {
                let lineage = &settlement.lineage;
                anyhow::ensure!(
                    settlement_ids.insert(lineage.collateral.tx_hash.clone()),
                    "captured settlement appears in multiple symbol catalogs tx_hash={}",
                    lineage.collateral.tx_hash
                );
                for ledger_seq in [
                    lineage.submission.ledger_seq,
                    lineage.gas.ledger_seq,
                    lineage.collateral.ledger_seq,
                    settlement.terminal_ledger_seq,
                ] {
                    anyhow::ensure!(
                        ledger_sequences.insert(ledger_seq),
                        "captured receipt ledger_seq appears in multiple symbol catalogs ledger_seq={ledger_seq}"
                    );
                }
                settlements.push(settlement);
            }
        }
        let events = build_events(&resolutions, &settlements)?;
        Ok(Arc::new(Self {
            resolutions,
            settlements,
            events: Arc::from(events),
        }))
    }

    pub(crate) fn events(&self) -> &[EventRef] {
        &self.events
    }

    pub(crate) fn finalized_resolution(
        &self,
        event: EventRef,
    ) -> Result<FinalizedConditionResolutionReceipt> {
        anyhow::ensure!(
            event.symbol == CENTRAL_EVENT_SYMBOL
                && event.kind == EVENT_KIND_CONDITION_RESOLUTION
                && event.flags == 0
                && event.len == 1,
            "invalid finalized resolution EventRef"
        );
        let index = usize::try_from(event.start).context("central receipt index exceeds usize")?;
        self.resolutions
            .get(index)
            .cloned()
            .context("central resolution EventRef is out of range")
    }

    pub(crate) fn settlement_lineage(&self, event: EventRef) -> Result<PreparedSettlementReceipt> {
        anyhow::ensure!(
            event.symbol == CENTRAL_EVENT_SYMBOL
                && event.kind == EVENT_KIND_SETTLEMENT_COLLATERAL
                && event.flags == 0
                && event.len == 1,
            "invalid settlement collateral EventRef"
        );
        let index = usize::try_from(event.start).context("central receipt index exceeds usize")?;
        self.settlements
            .get(index)
            .cloned()
            .context("central settlement lineage EventRef is out of range")
    }
}

fn validate_symbol_scope(
    resolutions: &[FinalizedConditionResolutionReceipt],
    settlements: &[PreparedSettlementReceipt],
    catalog: &MarketCatalog,
    symbol: &str,
) -> Result<()> {
    for condition_id in resolutions
        .iter()
        .map(|receipt| receipt.condition_id.as_str())
        .chain(
            settlements
                .iter()
                .map(|settlement| settlement.lineage.collateral.condition_id.as_str()),
        )
    {
        anyhow::ensure!(
            condition_belongs_to_catalog(condition_id, catalog),
            "prepared receipt has foreign condition symbol={} condition_id={}",
            symbol,
            condition_id
        );
    }
    Ok(())
}

fn condition_belongs_to_catalog(condition_id: &str, catalog: &MarketCatalog) -> bool {
    condition_id
        .parse::<alloy::primitives::B256>()
        .ok()
        .and_then(|condition| catalog.market_handle_for_condition(condition.into()))
        .is_some()
}

fn canonical_symbols(symbols: &[String]) -> Result<Vec<&'static str>> {
    anyhow::ensure!(
        !symbols.is_empty(),
        "prepared central receipt symbols are empty"
    );
    let mut selected = BTreeSet::new();
    for symbol in symbols {
        anyhow::ensure!(
            selected.insert(SymbolId::parse(symbol)?.as_str()),
            "prepared central receipt symbols contain duplicate symbol={symbol}"
        );
    }
    const ORDER: [SymbolId; 4] = [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp];
    Ok(ORDER
        .into_iter()
        .map(SymbolId::as_str)
        .filter(|symbol| selected.contains(symbol))
        .collect())
}

fn build_events(
    resolutions: &[FinalizedConditionResolutionReceipt],
    settlements: &[PreparedSettlementReceipt],
) -> Result<Vec<EventRef>> {
    let mut rows = resolutions
        .iter()
        .enumerate()
        .map(|(index, receipt)| {
            (
                receipt.observed_at_ms,
                receipt.ledger_seq,
                0_u8,
                EVENT_KIND_CONDITION_RESOLUTION,
                index,
            )
        })
        .chain(settlements.iter().enumerate().map(|(index, settlement)| {
            (
                settlement.terminal_ts_ms,
                settlement.terminal_ledger_seq,
                1_u8,
                EVENT_KIND_SETTLEMENT_COLLATERAL,
                index,
            )
        }))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.0, row.1, row.2));
    rows.into_iter()
        .enumerate()
        .map(|(sequence, (timestamp_ms, _, _, kind, start))| {
            Ok(EventRef {
                sequence: u64::try_from(sequence)?
                    .checked_add(1)
                    .context("central receipt EventRef sequence overflow")?,
                timestamp_ms,
                symbol: CENTRAL_EVENT_SYMBOL,
                kind,
                flags: 0,
                start: u32::try_from(start).context("central receipt row exceeds u32")?,
                len: 1,
            })
        })
        .collect()
}

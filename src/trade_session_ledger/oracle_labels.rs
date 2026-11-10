use std::collections::BTreeMap;
use std::fs;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::oracle_resolution::{
    OracleResolvedLabel, ResolutionBoundaryValue, CHAINLINK_RESOLUTION_PROVENANCE,
};
use crate::polymarket::identity::MarketId;
use crate::strategy::hold_to_resolution::PairTokenSide;

use super::{
    allocate_trade_ledger_seq, append_trade_ledger_record, canonical_ledger_lock, is_live_run_id,
    load_records_from_path, record_belongs_to_run_window, trade_session_ledger_path_for_run,
    TradeLedgerRecord,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalOracleWinner {
    Up,
    Down,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalOracleBoundaryValue {
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_i128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_i128_string"
    )]
    pub price_e18: i128,
    pub source_ts_ms: u64,
    pub observed_ts_ms: u64,
    pub capture_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalOracleResolvedLabel {
    pub market_id: MarketId,
    pub condition_id: String,
    pub symbol: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub open: CanonicalOracleBoundaryValue,
    pub close: CanonicalOracleBoundaryValue,
    pub winner: CanonicalOracleWinner,
    pub release_ts_ms: u64,
    pub provenance: String,
}

impl CanonicalOracleResolvedLabel {
    pub(crate) fn from_runtime(label: &OracleResolvedLabel) -> Result<Self> {
        label.validate()?;
        Ok(Self {
            market_id: label.market_id.clone(),
            condition_id: label.condition_id.clone(),
            symbol: label.symbol.clone(),
            window_start_ms: label.window_start_ms,
            window_end_ms: label.window_end_ms,
            open: CanonicalOracleBoundaryValue::from_runtime(&label.open),
            close: CanonicalOracleBoundaryValue::from_runtime(&label.close),
            winner: CanonicalOracleWinner::from_runtime(label.winner),
            release_ts_ms: label.release_ts_ms,
            provenance: label.provenance.to_string(),
        })
    }

    pub(crate) fn validated_runtime(&self) -> Result<OracleResolvedLabel> {
        ensure!(
            self.provenance == CHAINLINK_RESOLUTION_PROVENANCE,
            "canonical oracle label provenance is invalid"
        );
        let label = OracleResolvedLabel {
            market_id: self.market_id.clone(),
            condition_id: self.condition_id.clone(),
            symbol: self.symbol.clone(),
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            open: self.open.to_runtime(),
            close: self.close.to_runtime(),
            winner: self.winner.to_runtime(),
            release_ts_ms: self.release_ts_ms,
            provenance: CHAINLINK_RESOLUTION_PROVENANCE,
        };
        label.validate()?;
        Ok(label)
    }
}

impl CanonicalOracleWinner {
    fn from_runtime(winner: PairTokenSide) -> Self {
        match winner {
            PairTokenSide::Up => Self::Up,
            PairTokenSide::Down => Self::Down,
        }
    }

    fn to_runtime(self) -> PairTokenSide {
        match self {
            Self::Up => PairTokenSide::Up,
            Self::Down => PairTokenSide::Down,
        }
    }
}

impl CanonicalOracleBoundaryValue {
    fn from_runtime(value: &ResolutionBoundaryValue) -> Self {
        Self {
            price_e18: value.price_e18,
            source_ts_ms: value.source_ts_ms,
            observed_ts_ms: value.observed_ts_ms,
            capture_sequence: value.capture_sequence,
        }
    }

    fn to_runtime(&self) -> ResolutionBoundaryValue {
        ResolutionBoundaryValue {
            price_e18: self.price_e18,
            source_ts_ms: self.source_ts_ms,
            observed_ts_ms: self.observed_ts_ms,
            capture_sequence: self.capture_sequence,
        }
    }
}

pub(super) fn record_from_runtime(
    run_id: String,
    ledger_seq: u64,
    label: &OracleResolvedLabel,
) -> Result<TradeLedgerRecord> {
    let canonical = CanonicalOracleResolvedLabel::from_runtime(label)?;
    let record = TradeLedgerRecord {
        run_id,
        ledger_seq,
        ts_ms: canonical.release_ts_ms,
        trade_id: canonical.condition_id.clone(),
        event_kind: "oracle_resolved_label".to_string(),
        oracle_resolved_label: Some(canonical.clone()),
        market_id: Some(canonical.market_id.clone()),
        ..TradeLedgerRecord::default()
    };
    validate_record(&record)?;
    Ok(record)
}

pub(super) fn canonical_from_runtime(
    label: &OracleResolvedLabel,
) -> Result<CanonicalOracleResolvedLabel> {
    CanonicalOracleResolvedLabel::from_runtime(label)
}

pub(super) fn existing_record_matches(
    records: &[TradeLedgerRecord],
    run_id: &str,
    canonical: &CanonicalOracleResolvedLabel,
) -> Result<bool> {
    let mut existing = None;
    for record in records {
        let Some(label) = record.oracle_resolved_label.as_ref() else {
            continue;
        };
        if label.condition_id != canonical.condition_id {
            continue;
        }
        validate_record(record)?;
        ensure!(
            record.run_id == run_id,
            "oracle resolved label wrapper has foreign run identity expected={run_id} actual={}",
            record.run_id
        );
        ensure!(
            existing.replace(label).is_none(),
            "duplicate canonical oracle resolved label condition_id={}",
            canonical.condition_id
        );
    }
    let Some(existing) = existing else {
        return Ok(false);
    };
    ensure!(
        existing == canonical,
        "oracle resolved label identity conflicts condition_id={}",
        canonical.condition_id
    );
    Ok(true)
}

pub(crate) fn validate_record(record: &TradeLedgerRecord) -> Result<()> {
    let Some(label) = record.oracle_resolved_label.as_ref() else {
        return Ok(());
    };
    label.validated_runtime()?;
    ensure!(
        record.ledger_seq > 0
            && record.ts_ms == label.release_ts_ms
            && record.trade_id == label.condition_id
            && record.event_kind == "oracle_resolved_label"
            && record.market_id.as_ref() == Some(&label.market_id)
            && record.ctf_event.is_none()
            && record.order_terminal_fact.is_none()
            && record.owned_leg_outcome.is_none()
            && record.paired_cohort_key.is_none()
            && record.tokens.is_empty()
            && record.entry_ts_ms.is_none()
            && record.exit_ts_ms.is_none()
            && record.strategy_entry_reason.is_none()
            && record.close_reason.is_none()
            && record.close_hint.is_none()
            && record.decision_context.is_none()
            && record.reconciliation_status.is_none()
            && record.local_order_ids.is_empty()
            && record.fill_ids.is_empty()
            && record.tx_hashes.is_empty()
            && record.split_tx_hash.is_none()
            && record.exit_fill_id.is_none()
            && record.settlement_tx_hash.is_none(),
        "oracle resolved label does not match its canonical audit record"
    );
    Ok(())
}

pub fn append_oracle_resolved_labels(
    run_id: &str,
    labels: &[OracleResolvedLabel],
) -> Result<usize> {
    ensure!(
        is_live_run_id(run_id),
        "oracle label append requires canonical live run identity"
    );
    if labels.is_empty() {
        return Ok(0);
    }
    let _guard = canonical_ledger_lock()?;
    let mut records = load_records_from_path(&trade_session_ledger_path_for_run(run_id))?;
    let mut staged =
        BTreeMap::<String, (&OracleResolvedLabel, CanonicalOracleResolvedLabel)>::new();
    for label in labels {
        let canonical = canonical_from_runtime(label)?;
        if existing_record_matches(&records, run_id, &canonical)? {
            continue;
        }
        match staged.entry(canonical.condition_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((label, canonical));
            }
            std::collections::btree_map::Entry::Occupied(entry) => ensure!(
                entry.get().1 == canonical,
                "oracle resolved label batch conflicts condition_id={}",
                canonical.condition_id
            ),
        }
    }
    let mut appended = 0_usize;
    for (_, (label, canonical)) in staged {
        let record = record_from_runtime(run_id.to_string(), allocate_trade_ledger_seq()?, label)?;
        ensure!(
            record_belongs_to_run_window(&record),
            "oracle resolved label is outside its canonical run window condition_id={}",
            canonical.condition_id
        );
        append_trade_ledger_record(&record)?;
        records.push(record);
        appended = appended
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("oracle resolved label append count overflow"))?;
    }
    if appended > 0 {
        fs::OpenOptions::new()
            .read(true)
            .open(trade_session_ledger_path_for_run(run_id))
            .with_context(|| {
                format!("open canonical oracle label ledger for sync run_id={run_id}")
            })?
            .sync_all()
            .with_context(|| format!("sync canonical oracle label ledger run_id={run_id}"))?;
    }
    drop(_guard);
    Ok(appended)
}

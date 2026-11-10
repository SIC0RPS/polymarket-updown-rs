use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::event_ledger::current_run_id;
use crate::orders::OrderTerminalFact;
use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{
    append_engine_trade_record as append_trade_session_ledger_record,
    append_engine_trade_record_with_disposition, canonical_hold_state, CanonicalCtfEvent,
    FinalizedConditionResolutionReceipt, FinancialAdmissionDisposition,
};

mod ctf_entry;
mod ctf_terminal;

pub use ctf_entry::{
    append_prepared_ctf_split_entry_accepted, build_confirmed_ctf_split_entry_record,
    ctf_split_entry_decision_context, prepare_ctf_split_entry_accepted,
    CtfSplitEntryDecisionContext, CtfSplitEntryJournal,
};
pub use ctf_terminal::{
    append_confirmed_ctf_terminal_record_once, build_confirmed_ctf_terminal_record,
    ConfirmedCtfTerminalClearance,
};
static ENGINE_TRADE_JOURNAL_ONCE_KEYS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct EngineTradeJournalRecord {
    pub run_id: String,
    pub trade_id: String,
    pub event_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctf_event: Option<CanonicalCtfEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_terminal_fact: Option<OrderTerminalFact>,
    pub ts_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_id: Option<MarketId>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tokens: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paired_cohort_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_entry_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_exit_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hold_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_hint: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub local_order_ids: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub fill_ids: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tx_hashes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_fill_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement_tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciliation_status: Option<String>,
}

impl EngineTradeJournalRecord {
    pub fn new(trade_id: impl Into<String>, event_kind: impl Into<String>, ts_ms: u64) -> Self {
        Self {
            trade_id: trade_id.into(),
            event_kind: event_kind.into(),
            ts_ms,
            ..Self::default()
        }
    }
}

pub fn build_order_terminal_record(fact: OrderTerminalFact) -> Result<EngineTradeJournalRecord> {
    fact.validate()?;
    let mut record =
        EngineTradeJournalRecord::new(fact.cohort.clone(), "order_terminal", fact.observed_at_ms);
    record.market_id = Some(MarketId::new(fact.market.clone()));
    record.tokens = vec![fact.token.clone()];
    record.paired_cohort_key = Some(fact.cohort.clone());
    record.local_order_ids = fact.order_id.iter().cloned().collect();
    record.reconciliation_status = Some("execution_terminal".to_string());
    record.order_terminal_fact = Some(fact);
    Ok(record)
}

pub fn build_resolution_observed_record(
    record_run_id: &str,
    receipt: FinalizedConditionResolutionReceipt,
) -> Result<EngineTradeJournalRecord> {
    anyhow::ensure!(
        !record_run_id.trim().is_empty(),
        "resolution observation record run identity is empty"
    );
    receipt.validate_exact()?;
    let event = CanonicalCtfEvent::ResolutionObserved(receipt.clone());
    let mut record = EngineTradeJournalRecord::new(
        receipt.condition_id.clone(),
        event.event_kind(),
        receipt.observed_at_ms,
    );
    record.run_id = record_run_id.to_string();
    record.market_id = Some(MarketId::new(receipt.condition_id));
    record.tx_hashes = vec![receipt.resolution_tx_hash];
    record.reconciliation_status = Some("finalized_condition_resolution".to_string());
    record.ctf_event = Some(event);
    Ok(record)
}

pub fn append(mut record: EngineTradeJournalRecord) -> Result<()> {
    normalize_record(&mut record)?;
    let _ = append_trade_session_ledger_record(&record)?;
    Ok(())
}

fn normalize_record(record: &mut EngineTradeJournalRecord) -> Result<()> {
    if record.run_id.trim().is_empty() {
        record.run_id =
            current_run_id().context("engine trade journal run identity is unavailable")?;
    }
    record.hold_state = record
        .hold_state
        .as_deref()
        .map(canonical_hold_state)
        .map(ToString::to_string);
    Ok(())
}

pub fn append_once(
    dedupe_key: impl Into<String>,
    record: EngineTradeJournalRecord,
) -> Result<bool> {
    let dedupe_key = dedupe_key.into();
    let once_keys = ENGINE_TRADE_JOURNAL_ONCE_KEYS.get_or_init(|| Mutex::new(HashSet::new()));
    {
        let mut guard = once_keys
            .lock()
            .expect("engine trade journal once keys poisoned");
        if !guard.insert(dedupe_key.clone()) {
            return Ok(false);
        }
    }
    if let Err(error) = append(record) {
        once_keys
            .lock()
            .expect("engine trade journal once keys poisoned")
            .remove(&dedupe_key);
        return Err(error);
    }
    Ok(true)
}

pub(crate) fn append_with_disposition(
    mut record: EngineTradeJournalRecord,
) -> Result<FinancialAdmissionDisposition> {
    normalize_record(&mut record)?;
    append_engine_trade_record_with_disposition(&record).map(|(_, disposition)| disposition)
}

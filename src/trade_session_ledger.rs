use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::event_ledger::TRADE_SESSION_LEDGER_DIR;
use crate::orders::OrderTerminalFact;
use crate::polymarket::identity::MarketId;
use crate::strategy::StrategyFact;

#[path = "trade_session_ledger/accounting_facts.rs"]
mod accounting_facts;
mod candidate_audit;
pub(crate) mod exact_serde;
mod ledger_append;
mod live_accounting;
mod narrative;
mod oracle_labels;
mod owned_leg_outcomes;
#[path = "trade_session_ledger/receipts.rs"]
mod receipts;
#[path = "trade_session_ledger/run_index.rs"]
mod run_index;
mod stats;
mod validation;
pub(crate) use validation::{validate_ctf_event_record, validate_owned_leg_outcome_record};

pub use accounting_facts::{
    CanonicalCtfEvent, ConfirmedCtfBuyFact, ConfirmedCtfChargeFact, ConfirmedCtfManagedSellFact,
    ConfirmedCtfSplitFact, ConfirmedCtfTerminalFact, ConfirmedOrderSubmissionFact,
    CtfTerminalOperation, CtfTerminalSettlementEvidence, CtfTradeStatus,
    ManagedSellReceiptIdentity, ObservedCtfTradeFact,
};
pub(crate) use candidate_audit::CanonicalAuditLedger;
pub(crate) use ledger_append::append_engine_trade_record_with_disposition;
pub use ledger_append::{append_engine_trade_record, append_owned_leg_outcome_facts};
use ledger_append::{
    append_trade_ledger_record, canonical_ledger_lock, same_ctf_accounting_record,
    should_append_trade_record_to_canonical_ledger, validate_order_terminal_record,
};
pub(crate) use live_accounting::{
    canonical_buy_by_venue_order, canonical_managed_sell_by_venue_order,
    FinancialAdmissionDisposition, LocalAccountingOwner,
};
pub use live_accounting::{
    live_accounting_receiver, reconcile_live_accounting, ConfirmedCashflowIdentity,
    ConfirmedCashflowKind, ConfirmedPortfolioReceiptIdentity, ConfirmedReceiptCashflow,
    LiveAccountingGeneration, LiveWalletAccounting, ReceiptBlockIdentity,
};
pub use narrative::{
    live_session_report_receiver, write_trade_ledger_stats_sidecars, LiveSessionReportArtifact,
};
pub(crate) use narrative::{snapshot_live_session_report, write_live_session_report_artifact};
pub use oracle_labels::{
    append_oracle_resolved_labels, CanonicalOracleBoundaryValue, CanonicalOracleResolvedLabel,
    CanonicalOracleWinner,
};
pub use owned_leg_outcomes::CanonicalOwnedLegOutcome;
pub(crate) use owned_leg_outcomes::UnjoinedOwnedLegOutcome;
pub use receipts::{
    append_session_receipt, load_confirmed_settlement_collateral_receipt_for_run,
    load_finalized_condition_resolution_receipt_for_run, load_records_from_path,
    load_session_receipts_for_run, load_session_receipts_from_path,
    validate_confirmed_settlement_collateral_receipt,
    validate_confirmed_settlement_receipt_lineage, ConfirmedSettlementReceiptLineage,
    DirectEoaGasReceipt, DirectEoaOperation, DirectEoaSplitIdentity, DirectEoaSubmissionReceipt,
    FinalizedConditionResolutionReceipt, MakerRebateReceipt, SessionReceipt,
    SessionReceiptAppendOutcome, SettlementCollateralOutcome, SettlementCollateralReceipt,
    SettlementCollateralReconciliationReason, SettlementFinalizedLogIdentity,
    SettlementPayoutRedemptionEventReceipt, SettlementWrappedEventReceipt,
    CAPTURED_SESSION_LEDGER_FILE,
};
use run_index::{ensure_run_trade_ledger_layout, update_run_index, write_run_index};
pub use stats::{
    CanonicalClosedCohortStats, CanonicalCohortCashflow, CanonicalCohortEvent,
    CanonicalCohortOrderIdentity, CanonicalCohortSellReceipt, CanonicalCohortTerminalReceipt,
    CanonicalCohortTerminalReceiptEvidence, CanonicalCoverageGap, CanonicalCoverageGapKind,
    CanonicalCoverageProof, CanonicalCtfExitBucket, CanonicalCtfExitClass,
    CanonicalCtfExitCohortRow, CanonicalCtfExitSummaryRow, CanonicalCtfLegRealization,
    CanonicalCtfSummaryRow, CanonicalFinancialProjection, CanonicalFinancialView,
    CanonicalOrderExecutionLineageRow, CanonicalOrderExecutionTerminalFact,
    CanonicalOrderTerminalSummaryRow, CanonicalSessionStats, CanonicalStatsProof,
    CanonicalUnresolvedCohortState, CanonicalUnresolvedCohortStats, ExactOpenUnrecoveredCash,
    ExactRealizedPnl,
};

pub const TRADE_LEDGER_PROJECTION_SOURCE: &str = "trade_session_ledger_projection";
pub const TRADE_LEDGER_PROJECTION_VERSION: u64 = 9;

static TRADE_LEDGER_SEQ: AtomicU64 = AtomicU64::new(1);
static TRADE_LEDGER_SEQ_INITIALIZED: AtomicBool = AtomicBool::new(false);
static TRADE_LEDGER_PROCESS_LOCK: Mutex<Option<fs::File>> = Mutex::new(None);
static RUN_INDEX_WRITE_SEQ: AtomicU64 = AtomicU64::new(1);
static RUN_INDEX_LOCK: Mutex<()> = Mutex::new(());
static CTF_DURABLE_EVENT_LOCK: Mutex<()> = Mutex::new(());

const SESSION_LEDGER_FILE_NAME: &str = "session.jsonl";
const TRADE_LEDGER_DIR_NAME: &str = "trades";
const TRADE_LEDGER_PROCESS_LOCK_FILE_NAME: &str = ".trade_ledger_process.lock";
const RUN_INDEX_FILE_NAME: &str = "index.json";

pub fn canonical_hold_state(value: &str) -> &str {
    match value {
        "accepted_pair"
        | "directional_remaining_owned_leg"
        | "loser_remaining_owned_leg"
        | "matched_hold"
        | "terminal_settlement"
        | "orphan_recovery"
        | "redeemable"
        | "merge_candidate"
        | "not_needed"
        | "pending"
        | "acked" => value,
        "winner_pending_protection"
        | "winner_protected"
        | "winner_late_exit"
        | "workingpair"
        | "pair_quoting" => "accepted_pair",
        "matched_pair_hold" => "matched_hold",
        other => other,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TradeLedgerRecord {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub trade_id: String,
    pub event_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctf_event: Option<CanonicalCtfEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_terminal_fact: Option<OrderTerminalFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oracle_resolved_label: Option<CanonicalOracleResolvedLabel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owned_leg_outcome: Option<CanonicalOwnedLegOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_id: Option<MarketId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paired_cohort_key: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tokens: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy_entry_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_context: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconciliation_status: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TradeLedgerRunIndex {
    pub run_id: String,
    pub projection_source: String,
    pub projection_version: u64,
    pub market_count: usize,
    pub trade_count: usize,
    pub event_count: usize,
    #[serde(default)]
    pub markets: Vec<TradeLedgerMarketIndexEntry>,
    #[serde(default)]
    pub trades: Vec<TradeLedgerTradeIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLedgerMarketIndexEntry {
    pub market_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLedgerTradeIndexEntry {
    pub trade_id: String,
}

pub fn trade_session_ledger_run_dir(run_id: &str) -> PathBuf {
    Path::new(TRADE_SESSION_LEDGER_DIR).join(run_id)
}

pub fn trade_session_ledger_path_for_run(run_id: &str) -> PathBuf {
    trade_session_ledger_run_dir(run_id).join(SESSION_LEDGER_FILE_NAME)
}

pub fn trade_session_run_index_path_for_run(run_id: &str) -> PathBuf {
    trade_session_ledger_run_dir(run_id).join(RUN_INDEX_FILE_NAME)
}

pub fn run_start_ts_ms(run_id: &str) -> Option<u64> {
    let mut components = run_id.split('_');
    if components.next()? != "run" {
        return None;
    }
    let start_ts_ms = components.next()?.parse::<u64>().ok()?;
    let process_id = components.next()?.parse::<u32>().ok()?;
    let run_sequence = components.next()?.parse::<u64>().ok()?;
    if components.next().is_some() || start_ts_ms == 0 || process_id == 0 || run_sequence == 0 {
        return None;
    }
    Some(start_ts_ms)
}

pub fn is_live_run_id(run_id: &str) -> bool {
    run_start_ts_ms(run_id).is_some()
}

pub fn acquire_trade_session_ledger_process_lock() -> Result<()> {
    acquire_trade_ledger_process_lock(Path::new(TRADE_SESSION_LEDGER_DIR))
}

pub fn direct_eoa_submission_receipt(tx_hash: &str) -> Result<DirectEoaSubmissionReceipt> {
    let tx_hash = tx_hash.trim();
    anyhow::ensure!(
        !tx_hash.is_empty(),
        "direct EOA submission transaction identity is empty"
    );
    let run_id = crate::event_ledger::current_run_id()?;
    let submission_identity = format!("direct_eoa_submission:{tx_hash}");
    if live_accounting::resident_receipt_by_identity(
        &run_id,
        &format!("receipt_conflict:{submission_identity}"),
    )?
    .is_some()
    {
        anyhow::bail!("direct EOA submission transaction identity is conflicted tx_hash={tx_hash}");
    }
    match live_accounting::resident_receipt_by_identity(&run_id, &submission_identity)? {
        Some(SessionReceipt::DirectEoaSubmission(receipt)) => Ok(receipt),
        Some(_) => {
            anyhow::bail!("direct EOA submission receipt has wrong variant tx_hash={tx_hash}")
        }
        None => {
            anyhow::bail!("current-session direct EOA submission is unavailable tx_hash={tx_hash}")
        }
    }
}

fn append_and_sync_ledger_row<T: Serialize>(
    path: &Path,
    row: &T,
    ledger_seq: u64,
    retry: bool,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(row)?;
    bytes.push(b'\n');
    let already_appended = retry && retry_tail_matches(path, &bytes, ledger_seq)?;
    if !already_appended {
        crate::event_ledger::append_jsonl_record(&path.to_string_lossy(), row)?;
    }
    receipts::sync_session_receipt_path(path)
}

fn retry_tail_matches(path: &Path, expected: &[u8], ledger_seq: u64) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let mut file = fs::File::open(path)
        .with_context(|| format!("open retry ledger tail {}", path.display()))?;
    let length = file
        .metadata()
        .with_context(|| format!("inspect retry ledger tail {}", path.display()))?
        .len();
    if length == 0 {
        return Ok(false);
    }
    let expected_len = u64::try_from(expected.len())?;
    if length >= expected_len {
        file.seek(SeekFrom::End(-i64::try_from(expected_len)?))
            .with_context(|| format!("seek retry ledger tail {}", path.display()))?;
        let mut tail = vec![0_u8; expected.len()];
        file.read_exact(&mut tail)
            .with_context(|| format!("read retry ledger tail {}", path.display()))?;
        if tail == expected {
            return Ok(true);
        }
    }
    file.seek(SeekFrom::End(-1))
        .with_context(|| format!("seek retry ledger terminator {}", path.display()))?;
    let mut terminator = [0_u8; 1];
    file.read_exact(&mut terminator)
        .with_context(|| format!("read retry ledger terminator {}", path.display()))?;
    anyhow::ensure!(
        terminator[0] == b'\n',
        "ambiguous partial durable ledger row path={} ledger_seq={ledger_seq}",
        path.display()
    );
    Ok(false)
}

fn record_belongs_to_run_window(record: &TradeLedgerRecord) -> bool {
    run_start_ts_ms(record.run_id.as_str()).is_none_or(|start_ts_ms| record.ts_ms >= start_ts_ms)
}

pub fn initialize_run_trade_ledger(run_id: &str) -> Result<()> {
    initialize_trade_ledger_sequence_from_durable_rows()?;
    ensure_run_trade_ledger_layout(run_id)?;
    let session_path = trade_session_ledger_path_for_run(run_id);
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&session_path)
        .with_context(|| format!("failed to create {}", session_path.display()))?;
    let index_path = trade_session_run_index_path_for_run(run_id);
    if !index_path.exists() {
        write_run_index(
            run_id,
            &TradeLedgerRunIndex {
                run_id: run_id.to_string(),
                projection_source: TRADE_LEDGER_PROJECTION_SOURCE.to_string(),
                projection_version: TRADE_LEDGER_PROJECTION_VERSION,
                ..TradeLedgerRunIndex::default()
            },
        )?;
    }
    let index: TradeLedgerRunIndex = serde_json::from_slice(
        &fs::read(&index_path).with_context(|| format!("read {}", index_path.display()))?,
    )
    .with_context(|| format!("parse {}", index_path.display()))?;
    live_accounting::initialize_live_accounting(run_id, &index)?;
    let _ = write_trade_ledger_stats_sidecars(Some(run_id));
    Ok(())
}

fn initialize_trade_ledger_sequence_from_durable_rows() -> Result<()> {
    acquire_trade_session_ledger_process_lock()?;
    let mut maximum_ledger_seq = 0_u64;
    for session_path in session_ledger_paths()? {
        maximum_ledger_seq =
            maximum_ledger_seq.max(maximum_ledger_seq_from_session_jsonl(&session_path)?);
    }
    let next_ledger_seq = maximum_ledger_seq
        .checked_add(1)
        .context("durable trade session ledger sequence overflow")?;
    anyhow::ensure!(
        next_ledger_seq < u64::MAX,
        "durable trade session ledger sequence is exhausted"
    );
    TRADE_LEDGER_SEQ.fetch_max(next_ledger_seq, Ordering::Relaxed);
    TRADE_LEDGER_SEQ_INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

fn maximum_ledger_seq_from_session_jsonl(session_path: &Path) -> Result<u64> {
    let file = fs::File::open(session_path)
        .with_context(|| format!("failed to open {}", session_path.display()))?;
    let reader = BufReader::new(file);
    let mut maximum_ledger_seq = 0_u64;
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "failed to read trade session ledger {} line {}",
                session_path.display(),
                line_index + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(&line).with_context(|| {
            format!(
                "failed to parse trade session ledger {} line {}",
                session_path.display(),
                line_index + 1
            )
        })?;
        let ledger_seq = value
            .get("ledger_seq")
            .and_then(Value::as_u64)
            .with_context(|| {
                format!(
                    "trade session ledger row has no numeric ledger_seq path={} line={}",
                    session_path.display(),
                    line_index + 1
                )
            })?;
        anyhow::ensure!(
            ledger_seq > 0,
            "durable trade session ledger sequence is zero path={} line={}",
            session_path.display(),
            line_index + 1
        );
        maximum_ledger_seq = maximum_ledger_seq.max(ledger_seq);
    }
    Ok(maximum_ledger_seq)
}

fn acquire_trade_ledger_process_lock(root: &Path) -> Result<()> {
    let mut guard = TRADE_LEDGER_PROCESS_LOCK
        .lock()
        .map_err(|_| anyhow!("trade session ledger process lock poisoned"))?;
    if guard.is_some() {
        return Ok(());
    }
    fs::create_dir_all(root).with_context(|| format!("failed to create {}", root.display()))?;
    let path = root.join(TRADE_LEDGER_PROCESS_LOCK_FILE_NAME);
    let file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| {
            format!(
                "failed to open trade session ledger lock {}",
                path.display()
            )
        })?;
    // SAFETY: `file` owns a valid descriptor for the entire call and is retained in the
    // process-lock guard for the lifetime of the process after `flock` succeeds.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "another central trader owns the trade session ledger lock {}",
                path.display()
            )
        });
    }
    *guard = Some(file);
    Ok(())
}

fn allocate_trade_ledger_seq() -> Result<u64> {
    anyhow::ensure!(
        TRADE_LEDGER_SEQ_INITIALIZED.load(Ordering::Acquire),
        "trade session ledger allocator is not initialized"
    );
    TRADE_LEDGER_SEQ
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            (current > 0).then_some(current)?.checked_add(1)
        })
        .map_err(|_| anyhow!("trade session ledger sequence is exhausted"))
}

pub fn session_ledger_paths() -> Result<Vec<PathBuf>> {
    let root = Path::new(TRADE_SESSION_LEDGER_DIR);
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", root.display()))
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to enumerate {}", root.display()))?;
        let path = entry.path();
        let run_id = entry.file_name().into_string().map_err(|_| {
            anyhow!(
                "trade session ledger entry name is not UTF-8 path={}",
                path.display()
            )
        })?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "failed to inspect trade session ledger entry {}",
                path.display()
            )
        })?;
        if !is_live_run_id(&run_id) {
            continue;
        }
        anyhow::ensure!(
            file_type.is_dir(),
            "canonical trade session ledger entry is not a directory path={}",
            path.display()
        );
        let session_path = path.join(SESSION_LEDGER_FILE_NAME);
        let metadata = fs::symlink_metadata(&session_path).with_context(|| {
            format!(
                "failed to inspect canonical trade session ledger {}",
                session_path.display()
            )
        })?;
        anyhow::ensure!(
            metadata.file_type().is_file(),
            "trade session ledger is not a regular file path={}",
            session_path.display()
        );
        paths.push(session_path);
    }
    paths.sort();
    Ok(paths)
}

impl TradeLedgerRecord {
    pub(crate) fn from_owned_leg_outcome(
        outcome: CanonicalOwnedLegOutcome,
        run_id: String,
    ) -> Self {
        Self {
            run_id,
            ledger_seq: outcome.ledger_seq,
            ts_ms: outcome.ts_ms,
            trade_id: outcome.cohort_id.clone(),
            event_kind: "owned_leg_outcome".to_string(),
            ctf_event: None,
            order_terminal_fact: None,
            oracle_resolved_label: None,
            market_id: Some(outcome.market_id.clone()),
            paired_cohort_key: Some(outcome.cohort_id.clone()),
            tokens: vec![outcome.token.clone()],
            owned_leg_outcome: Some(outcome),
            ..Self::default()
        }
    }

    pub fn from_engine_trade_journal(record: &EngineTradeJournalRecord, run_id: String) -> Self {
        let event_kind = record.ctf_event.as_ref().map_or_else(
            || record.event_kind.clone(),
            |event| event.event_kind().to_string(),
        );
        Self {
            run_id,
            ledger_seq: 0,
            ts_ms: record.ts_ms,
            trade_id: record.trade_id.clone(),
            event_kind,
            ctf_event: record.ctf_event.clone(),
            order_terminal_fact: record.order_terminal_fact.clone(),
            oracle_resolved_label: None,
            owned_leg_outcome: None,
            market_id: record.market_id.clone(),
            paired_cohort_key: record.paired_cohort_key.clone(),
            tokens: record.tokens.clone(),
            entry_ts_ms: record.entry_ts_ms,
            exit_ts_ms: record.exit_ts_ms,
            strategy_entry_reason: record.strategy_entry_reason.clone(),
            close_reason: record.strategy_exit_reason.clone(),
            close_hint: record.close_hint.clone(),
            decision_context: record.decision_context.clone(),
            reconciliation_status: record.reconciliation_status.clone(),
            local_order_ids: record.local_order_ids.clone(),
            fill_ids: record.fill_ids.clone(),
            tx_hashes: record.tx_hashes.clone(),
            split_tx_hash: record.split_tx_hash.clone(),
            exit_fill_id: record.exit_fill_id.clone(),
            settlement_tx_hash: record.settlement_tx_hash.clone(),
        }
    }
}

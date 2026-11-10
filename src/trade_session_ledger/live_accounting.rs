use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result};
use tokio::sync::watch;

use super::{SessionReceipt, TradeLedgerRecord, TradeLedgerRunIndex};

#[path = "live_accounting/financial.rs"]
mod financial;
#[path = "live_accounting/financial_summary.rs"]
mod financial_summary;
#[path = "live_accounting/history.rs"]
mod history;
#[path = "live_accounting/wallet.rs"]
mod wallet;

use super::stats::FinancialIdentityIndex;
use financial::LiveFinancialState;
pub(in crate::trade_session_ledger) use history::{
    begin_durable_receipt_append, begin_durable_trade_append, inspect_resident_records,
    project_live_accounting_snapshot, resident_ctf_event_by_identity,
    resident_managed_sell_by_fill, resident_order_by_id, resident_order_by_intent,
    resident_receipt_by_identity, resident_terminal_by_intent, resident_trade_by_sequence,
    resident_trade_disposition_by_sequence,
};
pub(crate) use history::{
    canonical_buy_by_venue_order, canonical_managed_sell_by_venue_order,
    FinancialAdmissionDisposition, LocalAccountingOwner,
};
use history::{ResidentHistory, ResidentHistorySnapshot};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConfirmedCashflowKind {
    Split,
    Buy,
    ManagedSale,
    Merge,
    Redemption,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConfirmedPortfolioReceiptIdentity {
    Split {
        cohort_id: String,
        transaction_id: String,
    },
    Buy {
        cohort_id: String,
        fill_id: String,
    },
    ManagedSell {
        cohort_id: String,
        fill_id: String,
    },
    Terminal {
        market_id: String,
        token: String,
        transaction_id: String,
        kind: ConfirmedCashflowKind,
    },
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConfirmedCashflowIdentity {
    pub ledger_sequence: u64,
    pub transaction_identity: String,
    pub kind: ConfirmedCashflowKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceiptBlockIdentity {
    pub number: u64,
    pub hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfirmedReceiptCashflow {
    pub identity: ConfirmedCashflowIdentity,
    pub block: ReceiptBlockIdentity,
    pub net_pusd_units_6dp: i128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveWalletAccounting {
    pub reconciled_block: ReceiptBlockIdentity,
    pub reconciled_usdc_units_6dp: u128,
    pub reconciled_pusd_units_6dp: u128,
    pub projected_pusd_units_6dp: u128,
    pub source_ts_ms: u64,
}

#[derive(Clone, Debug)]
pub struct LiveAccountingGeneration {
    pub run_id: String,
    pub projection_source: String,
    pub projection_version: u64,
    pub generation: u64,
    pub financial: Arc<super::CanonicalFinancialProjection>,
    pub confirmed_portfolio_receipts: Arc<[ConfirmedPortfolioReceiptIdentity]>,
    pub confirmed_cashflow_count: u64,
    pub confirmed_cashflow_net_pusd_units_6dp: i128,
    pub wallet: Option<LiveWalletAccounting>,
}

#[derive(Clone)]
struct ReconciledWallet {
    block: ReceiptBlockIdentity,
    usdc_units_6dp: u128,
    pusd_units_6dp: u128,
    source_ts_ms: u64,
}

struct LiveAccountingProjection {
    run_id: String,
    index: TradeLedgerRunIndex,
    records: ResidentHistory<TradeLedgerRecord>,
    receipts: ResidentHistory<SessionReceipt>,
    record_by_sequence: BTreeMap<u64, usize>,
    record_disposition_by_sequence: BTreeMap<u64, FinancialAdmissionDisposition>,
    receipt_by_sequence: BTreeMap<u64, usize>,
    receipt_by_identity: BTreeMap<String, usize>,
    ctf_event_by_identity: BTreeMap<String, usize>,
    managed_sell_by_fill: BTreeMap<String, usize>,
    managed_trade_by_venue_order:
        BTreeMap<(String, String), (usize, FinancialAdmissionDisposition)>,
    order_by_id: BTreeMap<String, usize>,
    order_by_intent: BTreeMap<u64, usize>,
    terminal_by_intent: BTreeMap<u64, usize>,
    financial_identities: FinancialIdentityIndex,
    financial_evidence_cohort: BTreeMap<String, BTreeSet<String>>,
    portfolio_receipts_by_cohort: BTreeMap<String, BTreeSet<ConfirmedPortfolioReceiptIdentity>>,
    portfolio_receipt_counts: BTreeMap<ConfirmedPortfolioReceiptIdentity, u64>,
    confirmed_portfolio_receipts: Arc<[ConfirmedPortfolioReceiptIdentity]>,
    financial: LiveFinancialState,
    generation: u64,
    latest: Arc<LiveAccountingGeneration>,
    reconciled_wallet: Option<ReconciledWallet>,
    wallet: Option<LiveWalletAccounting>,
    pending_row: Option<PendingSessionRow>,
    sender: watch::Sender<Arc<LiveAccountingGeneration>>,
}

#[derive(Clone)]
pub(super) enum PendingSessionRow {
    Trade(TradeLedgerRecord),
    Receipt(SessionReceipt),
}

pub(super) struct LiveAccountingSnapshot {
    run_id: String,
    projection_source: String,
    projection_version: u64,
    market_count: usize,
    trade_count: usize,
    event_count: usize,
    records: ResidentHistorySnapshot<TradeLedgerRecord>,
    receipts: ResidentHistorySnapshot<SessionReceipt>,
    financial: Arc<super::CanonicalFinancialProjection>,
    confirmed_portfolio_receipts: Arc<[ConfirmedPortfolioReceiptIdentity]>,
    confirmed_cashflow_count: u64,
    confirmed_cashflow_net_pusd_units_6dp: i128,
    wallet: Option<LiveWalletAccounting>,
    generation: u64,
}

static LIVE_ACCOUNTING: OnceLock<Mutex<Option<LiveAccountingProjection>>> = OnceLock::new();

fn live_accounting() -> &'static Mutex<Option<LiveAccountingProjection>> {
    LIVE_ACCOUNTING.get_or_init(|| Mutex::new(None))
}

pub(super) fn initialize_live_accounting(run_id: &str, index: &TradeLedgerRunIndex) -> Result<()> {
    anyhow::ensure!(
        index.run_id == run_id,
        "live accounting run index identity mismatch"
    );
    let mut owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    anyhow::ensure!(owner.is_none(), "live accounting owner already initialized");
    *owner = Some(history::new_accounting_projection(
        run_id,
        index.clone(),
        &["BTC", "ETH", "SOL", "XRP"].map(str::to_string),
    ));
    Ok(())
}

pub fn live_accounting_receiver() -> watch::Receiver<Arc<LiveAccountingGeneration>> {
    live_accounting()
        .lock()
        .expect("live accounting owner lock poisoned")
        .as_ref()
        .expect("live accounting owner is not initialized")
        .sender
        .subscribe()
}

pub(super) fn resident_run_index(run_id: &str) -> Result<TradeLedgerRunIndex> {
    let owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "live accounting resident index identity mismatch run_id={run_id}"
    );
    Ok(owner.index.clone())
}

pub(super) fn snapshot_live_accounting(run_id: &str) -> Result<LiveAccountingSnapshot> {
    let owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "live accounting snapshot identity mismatch run_id={run_id}"
    );
    Ok(LiveAccountingSnapshot {
        run_id: owner.run_id.clone(),
        projection_source: owner.index.projection_source.clone(),
        projection_version: owner.index.projection_version,
        market_count: owner.index.market_count,
        trade_count: owner.index.trade_count,
        event_count: owner.index.event_count,
        records: owner.records.snapshot(),
        receipts: owner.receipts.snapshot(),
        financial: owner.latest.financial.clone(),
        confirmed_portfolio_receipts: Arc::clone(&owner.confirmed_portfolio_receipts),
        confirmed_cashflow_count: owner.latest.confirmed_cashflow_count,
        confirmed_cashflow_net_pusd_units_6dp: owner.latest.confirmed_cashflow_net_pusd_units_6dp,
        wallet: owner.latest.wallet.clone(),
        generation: owner.generation,
    })
}

pub(super) fn ingest_durable_trade_record(
    record: TradeLedgerRecord,
) -> Result<FinancialAdmissionDisposition> {
    let mut owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_mut()
        .context("live accounting owner is not initialized")?;
    let result = ingest_trade_record(owner, record, true);
    if result.is_ok() {
        owner.pending_row = None;
    }
    result.map(|(_, disposition)| disposition)
}

fn ingest_trade_record(
    owner: &mut LiveAccountingProjection,
    record: TradeLedgerRecord,
    durable: bool,
) -> Result<(Arc<LiveAccountingGeneration>, FinancialAdmissionDisposition)> {
    anyhow::ensure!(
        record.ledger_seq > 0,
        "live accounting trade sequence is zero"
    );
    anyhow::ensure!(
        owner.run_id == record.run_id,
        "live accounting run identity changed"
    );
    if let Some(existing) = owner
        .record_by_sequence
        .get(&record.ledger_seq)
        .and_then(|index| owner.records.get(*index))
    {
        anyhow::ensure!(
            serde_json::to_value(existing)? == serde_json::to_value(&record)?,
            "live accounting trade identity conflicts ledger_seq={}",
            record.ledger_seq
        );
        let disposition = owner
            .record_disposition_by_sequence
            .get(&record.ledger_seq)
            .copied()
            .context("resident trade has no financial admission disposition")?;
        return Ok((Arc::clone(&owner.latest), disposition));
    }
    let generation = owner
        .generation
        .checked_add(1)
        .context("live accounting generation overflow")?;
    let identity_validation: Result<()> = (|| {
        if durable {
            let Some(PendingSessionRow::Trade(pending)) = owner.pending_row.as_ref() else {
                anyhow::bail!("durable trade admission has no pending identity")
            };
            anyhow::ensure!(
                serde_json::to_value(pending)? == serde_json::to_value(&record)?,
                "durable trade admission identity conflicts ledger_seq={}",
                record.ledger_seq
            );
            anyhow::ensure!(
                !owner.receipt_by_sequence.contains_key(&record.ledger_seq),
                "live accounting ledger sequence is already a receipt"
            );
        }
        Ok(())
    })();
    if let Err(error) = identity_validation {
        return Err(error);
    }
    let identity_check = owner.financial_identities.check_record(&record);
    let disposition = if identity_check.is_ok() {
        FinancialAdmissionDisposition::Admitted
    } else {
        FinancialAdmissionDisposition::DefinitivelyRejected
    };
    let identity_validation: Result<()> = (|| {
        if identity_check.is_err() {
            return Ok(());
        }
        if let Some(event) = record.ctf_event.as_ref() {
            if let Some(identity) = event.durable_identity() {
                anyhow::ensure!(
                    !owner.ctf_event_by_identity.contains_key(&identity),
                    "live accounting CTF identity reused"
                );
            }
            if let super::CanonicalCtfEvent::TradeConfirmed(sell) = event {
                anyhow::ensure!(
                    !owner.managed_sell_by_fill.contains_key(&sell.fill_id),
                    "live accounting managed-sell fill reused"
                );
            }
            if let super::CanonicalCtfEvent::OrderAccepted(order) = event {
                anyhow::ensure!(
                    !owner.order_by_id.contains_key(&order.order_id)
                        && !owner.order_by_intent.contains_key(&order.intent_id),
                    "live accounting accepted-order identity reused"
                );
            }
        }
        if let Some(fact) = record.order_terminal_fact.as_ref() {
            anyhow::ensure!(
                !owner
                    .terminal_by_intent
                    .contains_key(&fact.execution_intent_id),
                "live accounting terminal intent identity reused"
            );
        }
        Ok(())
    })();
    if let Err(error) = identity_validation {
        return Err(error);
    }
    let portfolio_updates = if let Err(rejection) = identity_check.as_ref() {
        LiveFinancialState::affected_rejection_cohorts(rejection, &owner.financial_evidence_cohort)
            .into_iter()
            .map(|cohort_id| (cohort_id, BTreeSet::new()))
            .collect()
    } else {
        financial_summary::prepare_portfolio_receipt(&owner.portfolio_receipts_by_cohort, &record)
    };
    let portfolio_counts = financial_summary::prepare_portfolio_receipt_counts(
        &owner.portfolio_receipts_by_cohort,
        &owner.portfolio_receipt_counts,
        &portfolio_updates,
    );
    let portfolio_counts = match portfolio_counts {
        Ok(counts) => counts,
        Err(error) => return Err(error),
    };
    let current_wallet = owner.wallet.clone();
    let financial_result = if let Err(rejection) = identity_check.as_ref() {
        owner.financial.apply_identity_rejection(
            rejection,
            &owner.financial_evidence_cohort,
            record.ledger_seq,
            current_wallet.as_ref(),
        )
    } else {
        owner
            .financial
            .apply_record(&record, current_wallet.as_ref())
    };
    let financial_apply = match financial_result {
        Ok(apply) => apply,
        Err(error) => return Err(error),
    };
    let identity_admission = owner.financial_identities.admit_record(&record);
    debug_assert_eq!(identity_admission.is_ok(), identity_check.is_ok());
    drop(identity_admission);
    commit_portfolio_updates(owner, portfolio_updates, portfolio_counts);
    owner.wallet = financial_apply.wallet;
    if identity_check.is_ok() {
        if let Some(
            super::CanonicalCtfEvent::MergeConfirmed(terminal)
            | super::CanonicalCtfEvent::RedeemConfirmed(terminal),
        ) = record.ctf_event.as_ref()
        {
            let cohort_id = terminal.cohort_id.clone();
            owner
                .financial_evidence_cohort
                .entry(format!("condition_resolution:{}", terminal.condition_id))
                .or_default()
                .insert(cohort_id.clone());
            if let Some((_, transaction_id)) = terminal.confirmed_receipt_identity() {
                owner
                    .financial_evidence_cohort
                    .entry(format!("settlement:{transaction_id}"))
                    .or_default()
                    .insert(cohort_id);
            }
        }
    }
    super::run_index::apply_record_to_run_index(&mut owner.index, &record);
    owner.index.market_count = owner.index.markets.len();
    let sequence = record.ledger_seq;
    let position = owner.records.push(record.clone());
    owner.record_by_sequence.insert(sequence, position);
    owner
        .record_disposition_by_sequence
        .insert(sequence, disposition);
    history::index_managed_trade_disposition(owner, &record, position, disposition);
    if identity_check.is_ok() {
        if let Some(fact) = record.order_terminal_fact.as_ref() {
            owner
                .terminal_by_intent
                .insert(fact.execution_intent_id, position);
        }
        if let Some(event) = record.ctf_event.as_ref() {
            if let Some(identity) = event.durable_identity() {
                owner.ctf_event_by_identity.insert(identity, position);
            }
            if let super::CanonicalCtfEvent::TradeConfirmed(sell) = event {
                owner
                    .managed_sell_by_fill
                    .insert(sell.fill_id.clone(), position);
            }
            if let super::CanonicalCtfEvent::OrderAccepted(order) = event {
                owner.order_by_id.insert(order.order_id.clone(), position);
                owner.order_by_intent.insert(order.intent_id, position);
            }
        }
    }
    publish_current(owner, generation);
    Ok((Arc::clone(&owner.latest), disposition))
}

pub(super) fn ingest_durable_session_receipt(receipt: SessionReceipt) -> Result<()> {
    let mut owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_mut()
        .context("live accounting owner is not initialized")?;
    let result = ingest_session_receipt(owner, receipt, true);
    if result.is_ok() {
        owner.pending_row = None;
    }
    result.map(|_| ())
}

fn ingest_session_receipt(
    owner: &mut LiveAccountingProjection,
    receipt: SessionReceipt,
    durable: bool,
) -> Result<Arc<LiveAccountingGeneration>> {
    anyhow::ensure!(
        receipt.ledger_seq() > 0,
        "live accounting receipt sequence is zero"
    );
    if durable {
        anyhow::ensure!(
            owner.run_id == receipt.run_id(),
            "live accounting run identity changed"
        );
    }
    let identity = receipt.identity();
    if let Some(existing) = owner
        .receipt_by_identity
        .get(&identity)
        .and_then(|index| owner.receipts.get(*index))
    {
        anyhow::ensure!(
            existing == &receipt,
            "live accounting receipt identity conflicts identity={identity}"
        );
        return Ok(Arc::clone(&owner.latest));
    }
    let generation = owner
        .generation
        .checked_add(1)
        .context("live accounting generation overflow")?;
    let sequence = receipt.ledger_seq();
    let identity_validation: Result<()> = (|| {
        if durable {
            let Some(PendingSessionRow::Receipt(pending)) = owner.pending_row.as_ref() else {
                anyhow::bail!("durable receipt admission has no pending identity")
            };
            anyhow::ensure!(
                pending == &receipt,
                "durable receipt admission identity conflicts identity={identity}"
            );
            anyhow::ensure!(
                !owner.record_by_sequence.contains_key(&sequence)
                    && !owner.receipt_by_sequence.contains_key(&sequence),
                "live accounting receipt sequence reused"
            );
        }
        Ok(())
    })();
    if let Err(error) = identity_validation {
        return Err(error);
    }
    let identity_check = owner.financial_identities.check_session_receipt(&receipt);
    let portfolio_updates: Vec<(String, BTreeSet<ConfirmedPortfolioReceiptIdentity>)> =
        identity_check
            .as_ref()
            .err()
            .map(|rejection| {
                LiveFinancialState::affected_rejection_cohorts(
                    rejection,
                    &owner.financial_evidence_cohort,
                )
                .into_iter()
                .map(|cohort_id| (cohort_id, BTreeSet::new()))
                .collect()
            })
            .unwrap_or_default();
    let portfolio_counts = financial_summary::prepare_portfolio_receipt_counts(
        &owner.portfolio_receipts_by_cohort,
        &owner.portfolio_receipt_counts,
        &portfolio_updates,
    );
    let portfolio_counts = match portfolio_counts {
        Ok(counts) => counts,
        Err(error) => return Err(error),
    };
    let current_wallet = owner.wallet.clone();
    let financial_result = if let Err(rejection) = identity_check.as_ref() {
        owner.financial.apply_identity_rejection(
            rejection,
            &owner.financial_evidence_cohort,
            receipt.ledger_seq(),
            current_wallet.as_ref(),
        )
    } else {
        owner
            .financial
            .apply_receipt(&receipt, current_wallet.as_ref())
    };
    let financial_apply = match financial_result {
        Ok(apply) => apply,
        Err(error) => return Err(error),
    };
    let identity_admission = owner.financial_identities.admit_session_receipt(&receipt);
    debug_assert_eq!(identity_admission.is_ok(), identity_check.is_ok());
    drop(identity_admission);
    commit_portfolio_updates(owner, portfolio_updates, portfolio_counts);
    owner.wallet = financial_apply.wallet;
    let position = owner.receipts.push(receipt);
    owner.receipt_by_sequence.insert(sequence, position);
    owner.receipt_by_identity.insert(identity, position);
    publish_current(owner, generation);
    Ok(Arc::clone(&owner.latest))
}

pub fn reconcile_live_accounting(
    block_number: u64,
    block_hash: String,
    usdc_units_6dp: u128,
    pusd_units_6dp: u128,
    source_ts_ms: u64,
) -> Result<()> {
    anyhow::ensure!(block_number > 0, "account reconciliation block is zero");
    anyhow::ensure!(
        !block_hash.trim().is_empty(),
        "account reconciliation block hash is empty"
    );
    let mut owner = live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_mut()
        .context("live accounting owner is not initialized")?;
    reconcile_accounting(
        owner,
        ReceiptBlockIdentity {
            number: block_number,
            hash: Some(block_hash),
        },
        usdc_units_6dp,
        pusd_units_6dp,
        source_ts_ms,
    )?;
    Ok(())
}

fn reconcile_accounting(
    owner: &mut LiveAccountingProjection,
    block: ReceiptBlockIdentity,
    usdc_units_6dp: u128,
    pusd_units_6dp: u128,
    source_ts_ms: u64,
) -> Result<Arc<LiveAccountingGeneration>> {
    let reconciled_wallet = ReconciledWallet {
        block,
        usdc_units_6dp,
        pusd_units_6dp,
        source_ts_ms,
    };
    let wallet = wallet::project_wallet(&reconciled_wallet, &owner.financial.cold_cashflows())?;
    let generation = owner
        .generation
        .checked_add(1)
        .context("live accounting generation overflow")?;
    owner.reconciled_wallet = Some(reconciled_wallet);
    owner.wallet = Some(wallet);
    publish_current(owner, generation);
    Ok(Arc::clone(&owner.latest))
}

fn publish_current(owner: &mut LiveAccountingProjection, generation_number: u64) {
    let wallet = owner.wallet.clone();
    publish_generation(owner, generation_number, wallet);
}

fn publish_generation(
    owner: &mut LiveAccountingProjection,
    generation_number: u64,
    wallet: Option<LiveWalletAccounting>,
) {
    owner.generation = generation_number;
    let generation = Arc::new(LiveAccountingGeneration {
        run_id: owner.run_id.clone(),
        projection_source: owner.index.projection_source.clone(),
        projection_version: owner.index.projection_version,
        generation: owner.generation,
        financial: owner.financial.projection(),
        confirmed_portfolio_receipts: Arc::clone(&owner.confirmed_portfolio_receipts),
        confirmed_cashflow_count: owner.financial.cashflow_count(),
        confirmed_cashflow_net_pusd_units_6dp: owner.financial.cashflow_net_pusd_units_6dp(),
        wallet,
    });
    owner.latest = generation.clone();
    owner.sender.send_replace(generation);
}

fn commit_portfolio_updates(
    owner: &mut LiveAccountingProjection,
    updates: Vec<(String, BTreeSet<ConfirmedPortfolioReceiptIdentity>)>,
    counts: BTreeMap<ConfirmedPortfolioReceiptIdentity, u64>,
) {
    if updates.is_empty() {
        return;
    }
    for (cohort_id, next) in updates {
        if next.is_empty() {
            owner.portfolio_receipts_by_cohort.remove(&cohort_id);
        } else {
            owner.portfolio_receipts_by_cohort.insert(cohort_id, next);
        }
    }
    for (identity, count) in counts {
        if count == 0 {
            owner.portfolio_receipt_counts.remove(&identity);
        } else {
            owner.portfolio_receipt_counts.insert(identity, count);
        }
    }
    owner.confirmed_portfolio_receipts = owner
        .portfolio_receipt_counts
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .into();
}

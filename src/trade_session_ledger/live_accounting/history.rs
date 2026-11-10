use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::watch;

use crate::trade_session_ledger::{
    CanonicalCtfEvent, CanonicalSessionStats, ConfirmedCtfBuyFact, ConfirmedCtfManagedSellFact,
    ManagedSellReceiptIdentity, ReceiptBlockIdentity, SessionReceipt, SettlementCollateralOutcome,
    TradeLedgerRecord, TradeLedgerRunIndex, TRADE_LEDGER_PROJECTION_SOURCE,
    TRADE_LEDGER_PROJECTION_VERSION,
};

use super::{
    financial::LiveFinancialState, ConfirmedPortfolioReceiptIdentity, LiveAccountingGeneration,
    LiveAccountingProjection, LiveAccountingSnapshot, PendingSessionRow,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinancialAdmissionDisposition {
    Admitted,
    DefinitivelyRejected,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedManagedSellDisposition {
    pub(crate) disposition: FinancialAdmissionDisposition,
    pub(crate) fact: ConfirmedCtfManagedSellFact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedBuyDisposition {
    pub(crate) disposition: FinancialAdmissionDisposition,
    pub(crate) fact: ConfirmedCtfBuyFact,
}

pub(crate) struct LocalAccountingOwner {
    projection: LiveAccountingProjection,
}

impl LocalAccountingOwner {
    pub(crate) fn new(run_id: &str, expected_symbols: &[String]) -> Result<Self> {
        anyhow::ensure!(
            !run_id.trim().is_empty(),
            "local accounting run identity is empty"
        );
        let index = TradeLedgerRunIndex {
            run_id: run_id.to_string(),
            projection_source: TRADE_LEDGER_PROJECTION_SOURCE.to_string(),
            projection_version: TRADE_LEDGER_PROJECTION_VERSION,
            ..TradeLedgerRunIndex::default()
        };
        Ok(Self {
            projection: new_accounting_projection(run_id, index, expected_symbols),
        })
    }

    pub(crate) fn admit_record(
        &mut self,
        record: TradeLedgerRecord,
    ) -> Result<FinancialAdmissionDisposition> {
        super::ingest_trade_record(&mut self.projection, record, false)
            .map(|(_, disposition)| disposition)
    }

    pub(crate) fn admit_receipt(
        &mut self,
        receipt: SessionReceipt,
    ) -> Result<Arc<LiveAccountingGeneration>> {
        super::ingest_session_receipt(&mut self.projection, receipt, false)
    }

    pub(crate) fn reconcile(
        &mut self,
        block: ReceiptBlockIdentity,
        usdc_units_6dp: u128,
        pusd_units_6dp: u128,
        source_ts_ms: u64,
    ) -> Result<Arc<LiveAccountingGeneration>> {
        super::reconcile_accounting(
            &mut self.projection,
            block,
            usdc_units_6dp,
            pusd_units_6dp,
            source_ts_ms,
        )
    }

    pub(crate) fn current(&self) -> Arc<LiveAccountingGeneration> {
        Arc::clone(&self.projection.latest)
    }
}

pub(super) fn new_accounting_projection(
    run_id: &str,
    mut index: TradeLedgerRunIndex,
    expected_symbols: &[String],
) -> LiveAccountingProjection {
    index.market_count = index.markets.len();
    let expected_symbols = expected_symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let financial = LiveFinancialState::new(run_id, &expected_symbols);
    let latest = Arc::new(LiveAccountingGeneration {
        run_id: run_id.to_string(),
        projection_source: index.projection_source.clone(),
        projection_version: index.projection_version,
        generation: 0,
        financial: financial.projection(),
        confirmed_portfolio_receipts: Arc::from([]),
        confirmed_cashflow_count: 0,
        confirmed_cashflow_net_pusd_units_6dp: 0,
        wallet: None,
    });
    let (sender, _) = watch::channel(Arc::clone(&latest));
    LiveAccountingProjection {
        run_id: run_id.to_string(),
        index,
        records: ResidentHistory::default(),
        receipts: ResidentHistory::default(),
        record_by_sequence: BTreeMap::new(),
        record_disposition_by_sequence: BTreeMap::new(),
        receipt_by_sequence: BTreeMap::new(),
        receipt_by_identity: BTreeMap::new(),
        ctf_event_by_identity: BTreeMap::new(),
        managed_sell_by_fill: BTreeMap::new(),
        managed_trade_by_venue_order: BTreeMap::new(),
        order_by_id: BTreeMap::new(),
        order_by_intent: BTreeMap::new(),
        terminal_by_intent: BTreeMap::new(),
        financial_identities: super::FinancialIdentityIndex::default(),
        financial_evidence_cohort: BTreeMap::new(),
        portfolio_receipts_by_cohort: BTreeMap::new(),
        portfolio_receipt_counts: BTreeMap::new(),
        confirmed_portfolio_receipts: Arc::<[ConfirmedPortfolioReceiptIdentity]>::from([]),
        financial,
        generation: 0,
        latest,
        reconciled_wallet: None,
        wallet: None,
        pending_row: None,
        sender,
    }
}

pub(in crate::trade_session_ledger) fn begin_durable_trade_append(
    candidate: &TradeLedgerRecord,
    same_fact: impl Fn(&TradeLedgerRecord, &TradeLedgerRecord) -> Result<bool>,
) -> Result<(TradeLedgerRecord, bool)> {
    let mut owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_mut()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == candidate.run_id,
        "pending trade run changed"
    );
    match owner.pending_row.as_ref() {
        Some(PendingSessionRow::Trade(pending)) => {
            anyhow::ensure!(
                same_fact(pending, candidate)?,
                "another durable trade append is pending ledger_seq={}",
                pending.ledger_seq
            );
            Ok((pending.clone(), true))
        }
        Some(PendingSessionRow::Receipt(pending)) => anyhow::bail!(
            "durable receipt append is pending ledger_seq={}",
            pending.ledger_seq()
        ),
        None => {
            owner.pending_row = Some(PendingSessionRow::Trade(candidate.clone()));
            Ok((candidate.clone(), false))
        }
    }
}

pub(in crate::trade_session_ledger) fn begin_durable_receipt_append(
    candidate: &SessionReceipt,
    same_fact: impl Fn(&SessionReceipt, &SessionReceipt) -> bool,
) -> Result<(SessionReceipt, bool)> {
    let mut owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_mut()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == candidate.run_id(),
        "pending receipt run changed"
    );
    match owner.pending_row.as_ref() {
        Some(PendingSessionRow::Receipt(pending)) => {
            anyhow::ensure!(
                same_fact(pending, candidate),
                "another durable receipt append is pending ledger_seq={}",
                pending.ledger_seq()
            );
            Ok((pending.clone(), true))
        }
        Some(PendingSessionRow::Trade(pending)) => anyhow::bail!(
            "durable trade append is pending ledger_seq={}",
            pending.ledger_seq
        ),
        None => {
            owner.pending_row = Some(PendingSessionRow::Receipt(candidate.clone()));
            Ok((candidate.clone(), false))
        }
    }
}

pub(super) fn index_managed_trade_disposition(
    owner: &mut LiveAccountingProjection,
    record: &TradeLedgerRecord,
    position: usize,
    disposition: FinancialAdmissionDisposition,
) {
    let (identity, order_id) = match record.ctf_event.as_ref() {
        Some(CanonicalCtfEvent::TradeConfirmed(sell)) => (&sell.receipt_identity, &sell.order_id),
        Some(CanonicalCtfEvent::BuyConfirmed(buy)) => (&buy.receipt_identity, &buy.order_id),
        _ => return,
    };
    let ManagedSellReceiptIdentity::ExchangeV2 { venue_trade_id, .. } = identity else {
        return;
    };
    owner
        .managed_trade_by_venue_order
        .entry((venue_trade_id.clone(), order_id.clone()))
        .or_insert((position, disposition));
}

pub(crate) fn canonical_managed_sell_by_venue_order(
    venue_trade_id: &str,
    order_id: &str,
) -> Result<Option<RecordedManagedSellDisposition>> {
    canonical_trade_by_venue_order(venue_trade_id, order_id)?
        .map(|(event, disposition)| {
            let CanonicalCtfEvent::TradeConfirmed(fact) = event else {
                anyhow::bail!("indexed venue/order receipt is not a confirmed SELL");
            };
            Ok(RecordedManagedSellDisposition { disposition, fact })
        })
        .transpose()
}

pub(crate) fn canonical_buy_by_venue_order(
    venue_trade_id: &str,
    order_id: &str,
) -> Result<Option<RecordedBuyDisposition>> {
    canonical_trade_by_venue_order(venue_trade_id, order_id)?
        .map(|(event, disposition)| {
            let CanonicalCtfEvent::BuyConfirmed(fact) = event else {
                anyhow::bail!("indexed venue/order receipt is not a confirmed BUY");
            };
            Ok(RecordedBuyDisposition { disposition, fact })
        })
        .transpose()
}

fn canonical_trade_by_venue_order(
    venue_trade_id: &str,
    order_id: &str,
) -> Result<Option<(CanonicalCtfEvent, FinancialAdmissionDisposition)>> {
    let venue_trade_id = venue_trade_id.trim();
    let order_id = order_id.trim();
    anyhow::ensure!(!venue_trade_id.is_empty(), "venue trade identity is empty");
    anyhow::ensure!(!order_id.is_empty(), "order identity is empty");
    let owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    let Some((position, disposition)) = owner
        .managed_trade_by_venue_order
        .get(&(venue_trade_id.to_string(), order_id.to_string()))
    else {
        return Ok(None);
    };
    let event = owner
        .records
        .get(*position)
        .and_then(|record| record.ctf_event.as_ref())
        .cloned()
        .context("indexed venue/order receipt has no confirmed fact")?;
    Ok(Some((event, *disposition)))
}

const RESIDENT_HISTORY_CHUNK_LEN: usize = 128;

pub(super) struct ResidentHistory<T> {
    chunks: Vec<Arc<ResidentHistoryChunk<T>>>,
    tail: Vec<T>,
    len: usize,
}

struct ResidentHistoryChunk<T> {
    prior: Option<Arc<ResidentHistoryChunk<T>>>,
    values: Vec<T>,
}

pub(super) struct ResidentHistorySnapshot<T> {
    head: Option<Arc<ResidentHistoryChunk<T>>>,
    tail: Vec<T>,
    len: usize,
}

impl<T> Default for ResidentHistory<T> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            tail: Vec::with_capacity(RESIDENT_HISTORY_CHUNK_LEN),
            len: 0,
        }
    }
}

impl<T> ResidentHistory<T> {
    pub(super) fn push(&mut self, value: T) -> usize {
        if self.tail.len() == RESIDENT_HISTORY_CHUNK_LEN {
            let values = std::mem::replace(
                &mut self.tail,
                Vec::with_capacity(RESIDENT_HISTORY_CHUNK_LEN),
            );
            self.chunks.push(Arc::new(ResidentHistoryChunk {
                prior: self.chunks.last().cloned(),
                values,
            }));
        }
        let index = self.len;
        self.tail.push(value);
        self.len += 1;
        index
    }

    pub(super) fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        let chunk_index = index / RESIDENT_HISTORY_CHUNK_LEN;
        let value_index = index % RESIDENT_HISTORY_CHUNK_LEN;
        self.chunks.get(chunk_index).map_or_else(
            || self.tail.get(value_index),
            |chunk| chunk.values.get(value_index),
        )
    }

    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.values.iter())
            .chain(self.tail.iter())
    }
}

impl<T: Clone> ResidentHistory<T> {
    pub(super) fn snapshot(&self) -> ResidentHistorySnapshot<T> {
        ResidentHistorySnapshot {
            head: self.chunks.last().cloned(),
            tail: self.tail.clone(),
            len: self.len,
        }
    }
}

impl<T: Clone> ResidentHistorySnapshot<T> {
    pub(super) fn into_vec(self) -> Vec<T> {
        let mut chunks = Vec::new();
        let mut next = self.head.as_deref();
        while let Some(chunk) = next {
            chunks.push(chunk);
            next = chunk.prior.as_deref();
        }
        let mut values = Vec::with_capacity(self.len);
        for chunk in chunks.into_iter().rev() {
            values.extend(chunk.values.iter().cloned());
        }
        values.extend(self.tail);
        values
    }
}

#[derive(Clone, Copy)]
pub(in crate::trade_session_ledger) struct ResidentHistoryView<'a, T> {
    pub(super) history: &'a ResidentHistory<T>,
}

impl<'a, T> ResidentHistoryView<'a, T> {
    pub(in crate::trade_session_ledger) fn iter(&self) -> impl DoubleEndedIterator<Item = &'a T> {
        self.history.iter()
    }
}

pub(in crate::trade_session_ledger) struct LiveAccountingAuditSnapshot {
    pub(in crate::trade_session_ledger) generation: Arc<LiveAccountingGeneration>,
    pub(in crate::trade_session_ledger) canonical: CanonicalSessionStats,
}

pub(in crate::trade_session_ledger) fn inspect_resident_records<T>(
    run_id: &str,
    inspect: impl FnOnce(ResidentHistoryView<'_, TradeLedgerRecord>) -> Result<T>,
) -> Result<T> {
    let owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "resident record run identity mismatch"
    );
    inspect(ResidentHistoryView {
        history: &owner.records,
    })
}

macro_rules! indexed_record_lookup {
    ($name:ident, $key:ty, $field:ident) => {
        pub(in crate::trade_session_ledger) fn $name(
            run_id: &str,
            key: &$key,
        ) -> Result<Option<TradeLedgerRecord>> {
            let owner = super::live_accounting()
                .lock()
                .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
            let owner = owner
                .as_ref()
                .context("live accounting owner is not initialized")?;
            anyhow::ensure!(
                owner.run_id == run_id,
                "resident record run identity mismatch"
            );
            Ok(owner
                .$field
                .get(key)
                .and_then(|index| owner.records.get(*index))
                .cloned())
        }
    };
}

indexed_record_lookup!(
    resident_ctf_event_by_identity,
    String,
    ctf_event_by_identity
);
indexed_record_lookup!(resident_managed_sell_by_fill, String, managed_sell_by_fill);
indexed_record_lookup!(resident_order_by_id, String, order_by_id);
indexed_record_lookup!(resident_order_by_intent, u64, order_by_intent);
indexed_record_lookup!(resident_terminal_by_intent, u64, terminal_by_intent);

pub(in crate::trade_session_ledger) fn resident_trade_by_sequence(
    run_id: &str,
    sequence: u64,
) -> Result<Option<TradeLedgerRecord>> {
    let owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "resident trade run identity mismatch"
    );
    Ok(owner
        .record_by_sequence
        .get(&sequence)
        .and_then(|index| owner.records.get(*index))
        .cloned())
}

pub(in crate::trade_session_ledger) fn resident_trade_disposition_by_sequence(
    run_id: &str,
    sequence: u64,
) -> Result<Option<FinancialAdmissionDisposition>> {
    let owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "resident trade disposition run identity mismatch"
    );
    Ok(owner.record_disposition_by_sequence.get(&sequence).copied())
}

pub(in crate::trade_session_ledger) fn resident_receipt_by_identity(
    run_id: &str,
    identity: &str,
) -> Result<Option<SessionReceipt>> {
    let owner = super::live_accounting()
        .lock()
        .map_err(|_| anyhow::anyhow!("live accounting owner lock poisoned"))?;
    let owner = owner
        .as_ref()
        .context("live accounting owner is not initialized")?;
    anyhow::ensure!(
        owner.run_id == run_id,
        "resident receipt run identity mismatch"
    );
    Ok(owner
        .receipt_by_identity
        .get(identity)
        .and_then(|index| owner.receipts.get(*index))
        .cloned())
}

pub(in crate::trade_session_ledger) fn project_live_accounting_snapshot(
    snapshot: LiveAccountingSnapshot,
) -> Result<LiveAccountingAuditSnapshot> {
    let records = snapshot.records.into_vec();
    let receipts = snapshot.receipts.into_vec();
    let audited_prefix_revision = records
        .iter()
        .map(|record| record.ledger_seq)
        .chain(receipts.iter().map(SessionReceipt::ledger_seq))
        .max()
        .unwrap_or_default();
    let settlement_receipts = receipts
        .iter()
        .filter_map(|receipt| match receipt {
            SessionReceipt::SettlementCollateral(receipt)
                if matches!(
                    receipt.outcome,
                    SettlementCollateralOutcome::Confirmed { .. }
                ) =>
            {
                Some(receipt.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let index = TradeLedgerRunIndex {
        run_id: snapshot.run_id.clone(),
        projection_source: snapshot.projection_source.clone(),
        projection_version: snapshot.projection_version,
        market_count: snapshot.market_count,
        trade_count: snapshot.trade_count,
        event_count: snapshot.event_count,
        markets: Vec::new(),
        trades: Vec::new(),
    };
    let mut canonical = CanonicalSessionStats::from_audit(
        &index,
        &records,
        &settlement_receipts,
        &["BTC", "ETH", "SOL", "XRP"].map(str::to_string),
    );
    canonical.canonical.revision = audited_prefix_revision;
    let generation = Arc::new(LiveAccountingGeneration {
        run_id: snapshot.run_id,
        projection_source: snapshot.projection_source,
        projection_version: snapshot.projection_version,
        generation: snapshot.generation,
        financial: snapshot.financial,
        confirmed_portfolio_receipts: snapshot.confirmed_portfolio_receipts,
        confirmed_cashflow_count: snapshot.confirmed_cashflow_count,
        confirmed_cashflow_net_pusd_units_6dp: snapshot.confirmed_cashflow_net_pusd_units_6dp,
        wallet: snapshot.wallet,
    });
    Ok(LiveAccountingAuditSnapshot {
        generation,
        canonical,
    })
}

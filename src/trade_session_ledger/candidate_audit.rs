use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, ensure, Context, Result};

use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::oracle_resolution::{canonical_chainlink_symbol, OracleResolvedLabel};

use super::run_index::apply_record_to_run_index;
use super::stats::{order_submission_matches_entry, validate_sell_identity, CohortEntryIdentity};
use super::{
    same_ctf_accounting_record, should_append_trade_record_to_canonical_ledger,
    validate_ctf_event_record, validate_order_terminal_record, CanonicalCtfEvent,
    CanonicalSessionStats, ConfirmedCtfSplitFact, FinalizedConditionResolutionReceipt,
    SettlementCollateralReceipt, TradeLedgerRecord, TradeLedgerRunIndex, UnjoinedOwnedLegOutcome,
    TRADE_LEDGER_PROJECTION_SOURCE, TRADE_LEDGER_PROJECTION_VERSION,
};

#[derive(Debug)]
pub(crate) struct CanonicalAuditLedger {
    run_id: String,
    last_source_sequence: Option<u64>,
    records: Vec<TradeLedgerRecord>,
    index: TradeLedgerRunIndex,
    identity_records: BTreeMap<String, TradeLedgerRecord>,
    trade_receipts_by_fill: BTreeMap<String, String>,
    submitted_orders: BTreeMap<String, u64>,
    submitted_intents: BTreeMap<u64, String>,
    terminal_intents: BTreeMap<u64, usize>,
    last_owner_audit_seq: BTreeMap<(String, u32), u64>,
    owned_leg_identities: BTreeMap<(String, u32, u64, u64, u64, String, String), u64>,
    expected_symbols: BTreeSet<String>,
    cohort_symbols: BTreeMap<String, String>,
    receipt_source_run_id: Option<String>,
    finalized_resolutions: BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlement_receipts: BTreeMap<String, SettlementCollateralReceipt>,
    candidate_slot: Option<u32>,
}

#[path = "candidate_audit/owned_leg_ingest.rs"]
mod owned_leg_ingest;

impl CanonicalAuditLedger {
    pub(crate) fn new(run_id: impl Into<String>, expected_symbols: &[String]) -> Result<Self> {
        let run_id = run_id.into();
        ensure!(
            !run_id.trim().is_empty(),
            "candidate canonical audit run_id must not be empty"
        );
        let expected_symbol_count = expected_symbols.len();
        let expected_symbols = expected_symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<BTreeSet<_>>();
        ensure!(
            !expected_symbols.is_empty()
                && expected_symbols.len() == expected_symbol_count
                && expected_symbols
                    .iter()
                    .all(|symbol| matches!(symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP")),
            "candidate canonical audit expected symbol scope is invalid"
        );
        Ok(Self {
            index: TradeLedgerRunIndex {
                run_id: run_id.clone(),
                projection_source: TRADE_LEDGER_PROJECTION_SOURCE.to_string(),
                projection_version: TRADE_LEDGER_PROJECTION_VERSION,
                ..TradeLedgerRunIndex::default()
            },
            run_id,
            last_source_sequence: None,
            records: Vec::new(),
            identity_records: BTreeMap::new(),
            trade_receipts_by_fill: BTreeMap::new(),
            submitted_orders: BTreeMap::new(),
            submitted_intents: BTreeMap::new(),
            terminal_intents: BTreeMap::new(),
            last_owner_audit_seq: BTreeMap::new(),
            owned_leg_identities: BTreeMap::new(),
            expected_symbols,
            cohort_symbols: BTreeMap::new(),
            receipt_source_run_id: None,
            finalized_resolutions: BTreeMap::new(),
            settlement_receipts: BTreeMap::new(),
            candidate_slot: None,
        })
    }

    pub(crate) fn run_id(&self) -> &str {
        &self.run_id
    }

    pub(crate) fn ingest(
        &mut self,
        source_sequence: u64,
        record: &EngineTradeJournalRecord,
        mut admit: impl FnMut(&TradeLedgerRecord) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            source_sequence > 0,
            "candidate canonical audit source_sequence must be nonzero"
        );
        if let Some(previous) = self.last_source_sequence {
            ensure!(
                source_sequence > previous,
                "candidate canonical audit source_sequence must be strictly increasing: previous={previous} next={source_sequence}"
            );
        }
        ensure!(
            record.run_id.trim().is_empty() || record.run_id == self.run_id,
            "candidate canonical audit foreign run identity: expected={} actual={}",
            self.run_id,
            record.run_id
        );

        let mut ledger_record =
            TradeLedgerRecord::from_engine_trade_journal(record, self.run_id.clone());
        ledger_record.ledger_seq = source_sequence;
        validate_ctf_event_record(&ledger_record)?;
        validate_order_terminal_record(&ledger_record)?;
        let mut repeated_terminal = false;
        let terminal_intent = if let Some(fact) = ledger_record.order_terminal_fact.as_ref() {
            ensure!(
                self.expected_symbols
                    .contains(&fact.symbol.to_ascii_uppercase()),
                "candidate canonical audit order terminal has foreign symbol={}",
                fact.symbol
            );
            ensure!(
                self.cohort_symbols.contains_key(&fact.cohort),
                "candidate canonical audit order terminal precedes scoped split cohort={}",
                fact.cohort
            );
            if let Some(position) = self.terminal_intents.get(&fact.execution_intent_id) {
                let existing = self
                    .records
                    .get(*position)
                    .and_then(|record| record.order_terminal_fact.as_ref())
                    .context("indexed candidate terminal intent has no terminal fact")?;
                ensure!(
                    existing.semantically_matches(fact),
                    "candidate canonical audit conflicting order terminal execution_intent_id={}",
                    fact.execution_intent_id
                );
                repeated_terminal = true;
            }
            Some(fact.execution_intent_id)
        } else {
            None
        };
        if let Some(submission) = accepted_order(&ledger_record) {
            ensure!(
                self.expected_symbols.contains(&submission.underlying),
                "candidate canonical audit order submission has foreign symbol={}",
                submission.underlying
            );
        }
        let mut finalized_resolution = None;
        if let Some(fact) = ledger_record.ctf_event.as_ref() {
            match fact {
                CanonicalCtfEvent::SplitConfirmed(split) => {
                    ensure!(
                        self.expected_symbols.contains(&split.underlying),
                        "candidate canonical audit split has foreign symbol={}",
                        split.underlying
                    );
                    if let Some(existing) = self.cohort_symbols.get(&split.cohort_id) {
                        ensure!(
                            existing == &split.underlying,
                            "candidate canonical audit cohort symbol changed cohort={} expected={} actual={}",
                            split.cohort_id,
                            existing,
                            split.underlying
                        );
                    }
                }
                CanonicalCtfEvent::BuyConfirmed(buy) => {
                    ensure!(
                        self.expected_symbols
                            .contains(buy.authorization.symbol.as_str()),
                        "candidate canonical audit BUY has foreign symbol"
                    );
                    if self.cohort_symbols.contains_key(&buy.cohort_id) {
                        self.scoped_entry(&buy.cohort_id, "confirmed BUY")?
                            .validate_buy(buy)?;
                    }
                }
                CanonicalCtfEvent::TradeConfirmed(sell) => {
                    let entry = self.scoped_entry(&sell.cohort_id, "confirmed trade")?;
                    validate_sell_identity(&entry, sell)?;
                }
                CanonicalCtfEvent::MergeConfirmed(terminal)
                | CanonicalCtfEvent::RedeemConfirmed(terminal) => {
                    let split = self.scoped_entry(&terminal.cohort_id, "terminal receipt")?;
                    ensure!(
                        terminal.market_id == split.market_id
                            && terminal.condition_id == split.condition_id
                            && terminal.up_token == split.up_token
                            && terminal.down_token == split.down_token,
                        "candidate canonical audit terminal receipt differs from scoped split cohort={}",
                        terminal.cohort_id
                    );
                }
                CanonicalCtfEvent::OrderAccepted(order) => {
                    let entry = match order.buy_authorization {
                        Some(authorization) => {
                            let entry = CohortEntryIdentity::from_buy_authorization(authorization)?;
                            if self.cohort_symbols.contains_key(&order.cohort_id) {
                                ensure!(
                                    self.scoped_entry(&order.cohort_id, "accepted BUY")? == entry,
                                    "candidate BUY authorization changed"
                                );
                            }
                            entry
                        }
                        None => self.scoped_entry(&order.cohort_id, "accepted order")?,
                    };
                    ensure!(
                        order_submission_matches_entry(&entry, order),
                        "candidate canonical audit accepted order differs from scoped split cohort={}",
                        order.cohort_id
                    );
                }
                CanonicalCtfEvent::TradeMatched(trade) | CanonicalCtfEvent::TradeFailed(trade) => {
                    let split = self.scoped_entry(&trade.cohort_id, "trade lifecycle event")?;
                    ensure!(
                        trade.market_id == split.market_id
                            && (trade.asset_id == split.up_token || trade.asset_id == split.down_token),
                        "candidate canonical audit trade lifecycle event differs from scoped split cohort={}",
                        trade.cohort_id
                    );
                }
                CanonicalCtfEvent::FeeApplied(charge) | CanonicalCtfEvent::CostApplied(charge) => {
                    self.scoped_entry(&charge.cohort_id, "confirmed charge")?;
                }
                CanonicalCtfEvent::ResolutionObserved(resolution) => {
                    self.validate_receipt_source_run(&resolution.run_id)?;
                    let matching_cohorts = self
                        .records
                        .iter()
                        .filter_map(|record| match record.ctf_event.as_ref()? {
                            CanonicalCtfEvent::SplitConfirmed(split)
                                if split.condition_id == resolution.condition_id =>
                            {
                                Some(&split.cohort_id)
                            }
                            CanonicalCtfEvent::BuyConfirmed(buy)
                                if buy.condition_id == resolution.condition_id =>
                            {
                                Some(&buy.cohort_id)
                            }
                            _ => None,
                        })
                        .collect::<BTreeSet<_>>();
                    ensure!(
                        matching_cohorts.len() <= 1,
                        "candidate canonical audit resolution has invalid acquisition cohort count"
                    );
                    ensure!(
                        !self
                            .finalized_resolutions
                            .contains_key(&resolution.condition_id),
                        "candidate canonical audit duplicate finalized condition resolution"
                    );
                    finalized_resolution = Some(resolution.clone());
                }
            }
        }
        let repeated_trade_update = self.validate_unique_accounting_identity(&ledger_record)?;
        self.validate_unique_order_submission_identity(&ledger_record)?;

        if !repeated_trade_update
            && !repeated_terminal
            && should_append_trade_record_to_canonical_ledger(&ledger_record)
        {
            admit(&ledger_record)?;
            if let Some(entry) = record_entry_identity(&ledger_record)? {
                self.cohort_symbols
                    .insert(entry.cohort_id, entry.underlying);
            }
            self.record_accounting_identity(&ledger_record);
            self.record_order_submission_identity(&ledger_record);
            apply_record_to_run_index(&mut self.index, &ledger_record);
            self.index.market_count = self.index.markets.len();
            let position = self.records.len();
            self.records.push(ledger_record);
            if let Some(intent_id) = terminal_intent {
                self.terminal_intents.insert(intent_id, position);
            }
        }
        if let Some(resolution) = finalized_resolution {
            let source_run_id = resolution.run_id.clone();
            self.finalized_resolutions
                .insert(resolution.condition_id.clone(), resolution);
            if self.receipt_source_run_id.is_none() {
                self.receipt_source_run_id = Some(source_run_id);
            }
        }
        self.last_source_sequence = Some(source_sequence);
        Ok(())
    }

    fn scoped_entry(&self, cohort_id: &str, event_label: &str) -> Result<CohortEntryIdentity> {
        for record in &self.records {
            if let Some(entry) = record_entry_identity(record)? {
                if entry.cohort_id == cohort_id {
                    return Ok(entry);
                }
            }
        }
        anyhow::bail!(
            "candidate canonical audit {event_label} precedes scoped acquisition cohort={cohort_id}"
        )
    }

    pub(crate) fn finish(
        self,
        financial: &super::CanonicalFinancialProjection,
    ) -> Result<CanonicalSessionStats> {
        let stats = CanonicalSessionStats::from_resident_audit(
            &self.index,
            &self.records,
            &self.expected_symbols.iter().cloned().collect::<Vec<_>>(),
            financial,
        );
        Ok(stats)
    }

    pub(crate) fn ingest_oracle_resolved_label(
        &mut self,
        source_sequence: u64,
        label: OracleResolvedLabel,
        mut admit: impl FnMut(&TradeLedgerRecord) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            source_sequence > 0
                && self
                    .last_source_sequence
                    .is_none_or(|previous| source_sequence > previous),
            "candidate canonical audit oracle label source sequence is not strictly increasing"
        );
        label.validate()?;
        ensure!(
            self.expected_symbols.iter().any(|symbol| {
                canonical_chainlink_symbol(symbol).is_ok_and(|expected| expected == label.symbol)
            }),
            "candidate canonical audit oracle label has foreign symbol={}",
            label.symbol
        );
        let canonical = super::oracle_labels::canonical_from_runtime(&label)?;
        if super::oracle_labels::existing_record_matches(
            &self.records,
            self.run_id.as_str(),
            &canonical,
        )? {
            return Ok(());
        }
        let record = super::oracle_labels::record_from_runtime(
            self.run_id.clone(),
            source_sequence,
            &label,
        )?;
        admit(&record)?;
        apply_record_to_run_index(&mut self.index, &record);
        self.records.push(record);
        self.last_source_sequence = Some(source_sequence);
        Ok(())
    }

    pub(crate) fn next_source_sequence(&self) -> Result<u64> {
        self.last_source_sequence
            .unwrap_or_default()
            .checked_add(1)
            .context("candidate canonical audit ledger sequence overflow")
    }

    pub(crate) fn ingest_settlement_receipt(
        &mut self,
        receipt: SettlementCollateralReceipt,
        mut admit: impl FnMut(&SettlementCollateralReceipt) -> Result<()>,
    ) -> Result<()> {
        receipt.confirmed_amount_units_6dp()?;
        self.validate_receipt_source_run(&receipt.run_id)?;
        ensure!(
            !self.settlement_receipts.contains_key(&receipt.tx_hash),
            "candidate canonical audit duplicate settlement receipt tx_hash={}",
            receipt.tx_hash
        );
        admit(&receipt)?;
        let source_run_id = receipt.run_id.clone();
        self.settlement_receipts
            .insert(receipt.tx_hash.clone(), receipt.clone());
        if self.receipt_source_run_id.is_none() {
            self.receipt_source_run_id = Some(source_run_id);
        }
        Ok(())
    }

    pub(crate) fn confirmed_split_for_terminal(
        &self,
        condition_id: &str,
    ) -> Result<Option<(ConfirmedCtfSplitFact, u64)>> {
        ensure!(
            !condition_id.trim().is_empty() && condition_id.trim() == condition_id,
            "candidate terminal split condition identity is missing or noncanonical"
        );
        let matching = self
            .records
            .iter()
            .filter_map(|record| match record.ctf_event.as_ref() {
                Some(CanonicalCtfEvent::SplitConfirmed(split))
                    if split.condition_id == condition_id =>
                {
                    Some((split.clone(), record.ts_ms))
                }
                Some(CanonicalCtfEvent::SplitConfirmed(_))
                | Some(CanonicalCtfEvent::BuyConfirmed(_))
                | Some(CanonicalCtfEvent::TradeConfirmed(_))
                | Some(CanonicalCtfEvent::MergeConfirmed(_))
                | Some(CanonicalCtfEvent::RedeemConfirmed(_))
                | Some(CanonicalCtfEvent::OrderAccepted(_))
                | Some(CanonicalCtfEvent::TradeMatched(_))
                | Some(CanonicalCtfEvent::TradeFailed(_))
                | Some(CanonicalCtfEvent::FeeApplied(_))
                | Some(CanonicalCtfEvent::CostApplied(_))
                | Some(CanonicalCtfEvent::ResolutionObserved(_))
                | None => None,
            })
            .collect::<Vec<_>>();
        ensure!(
            matching.len() <= 1,
            "candidate terminal condition has multiple confirmed split cohorts condition_id={condition_id} count={}",
            matching.len()
        );
        Ok(matching.into_iter().next())
    }

    pub(crate) fn finalized_resolution_for_terminal(
        &self,
        condition_id: &str,
    ) -> Option<&FinalizedConditionResolutionReceipt> {
        self.finalized_resolutions.get(condition_id)
    }

    fn validate_receipt_source_run(&self, source_run_id: &str) -> Result<()> {
        ensure!(
            !source_run_id.trim().is_empty() && source_run_id.trim() == source_run_id,
            "candidate receipt source run identity is missing or noncanonical"
        );
        if let Some(expected) = self.receipt_source_run_id.as_deref() {
            ensure!(
                expected == source_run_id,
                "candidate receipt source run mismatch expected={expected} actual={source_run_id}"
            );
        }
        Ok(())
    }

    fn validate_unique_accounting_identity(&self, candidate: &TradeLedgerRecord) -> Result<bool> {
        let Some(candidate_fact) = candidate.ctf_event.as_ref() else {
            return Ok(false);
        };
        let Some(identity) = candidate_fact.durable_identity() else {
            return Ok(false);
        };
        if let Some(existing) = self.identity_records.get(&identity) {
            let same_record = same_ctf_accounting_record(existing, candidate);
            if same_record
                && matches!(
                    candidate_fact,
                    CanonicalCtfEvent::TradeMatched(_)
                        | CanonicalCtfEvent::TradeFailed(_)
                        | CanonicalCtfEvent::BuyConfirmed(_)
                )
            {
                return Ok(true);
            }
            let failure = if same_record {
                "duplicate"
            } else {
                "conflicting"
            };
            return Err(anyhow!(
                "candidate canonical audit {failure} durable event identity: identity={identity}"
            ));
        }
        if let Some((fill_id, receipt_id)) = confirmed_fill(candidate_fact) {
            if let Some(existing_receipt_id) = self.trade_receipts_by_fill.get(fill_id) {
                return Err(anyhow!(
                    "candidate canonical audit confirmed fill identity reused: fill_id={fill_id} existing_receipt_id={existing_receipt_id} candidate_receipt_id={receipt_id}"
                ));
            }
        }
        Ok(false)
    }

    fn record_accounting_identity(&mut self, record: &TradeLedgerRecord) {
        let Some(fact) = record.ctf_event.as_ref() else {
            return;
        };
        if let Some(identity) = fact.durable_identity() {
            self.identity_records.insert(identity, record.clone());
        }
        if let Some((fill_id, receipt_id)) = confirmed_fill(fact) {
            self.trade_receipts_by_fill
                .insert(fill_id.to_string(), receipt_id.to_string());
        }
    }

    fn validate_unique_order_submission_identity(
        &self,
        candidate: &TradeLedgerRecord,
    ) -> Result<()> {
        let Some(fact) = accepted_order(candidate) else {
            return Ok(());
        };
        ensure!(
            !self.submitted_orders.contains_key(&fact.order_id),
            "candidate canonical audit duplicate submitted order identity: order_id={}",
            fact.order_id
        );
        ensure!(
            !self.submitted_intents.contains_key(&fact.intent_id),
            "candidate canonical audit duplicate submitted intent identity: intent_id={}",
            fact.intent_id
        );
        Ok(())
    }

    fn record_order_submission_identity(&mut self, record: &TradeLedgerRecord) {
        let Some(fact) = accepted_order(record) else {
            return;
        };
        self.submitted_orders
            .insert(fact.order_id.clone(), fact.intent_id);
        self.submitted_intents
            .insert(fact.intent_id, fact.order_id.clone());
    }
}

fn accepted_order(record: &TradeLedgerRecord) -> Option<&super::ConfirmedOrderSubmissionFact> {
    match record.ctf_event.as_ref()? {
        CanonicalCtfEvent::OrderAccepted(fact) => Some(fact),
        _ => None,
    }
}

fn record_entry_identity(record: &TradeLedgerRecord) -> Result<Option<CohortEntryIdentity>> {
    match record.ctf_event.as_ref() {
        Some(
            event @ (CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_)),
        ) => CohortEntryIdentity::from_event(event).map(Some),
        Some(CanonicalCtfEvent::OrderAccepted(order)) => order
            .buy_authorization
            .map(CohortEntryIdentity::from_buy_authorization)
            .transpose(),
        _ => Ok(None),
    }
}

fn confirmed_fill(event: &CanonicalCtfEvent) -> Option<(&str, &str)> {
    match event {
        CanonicalCtfEvent::TradeConfirmed(sell) => Some((&sell.fill_id, &sell.receipt_id)),
        CanonicalCtfEvent::BuyConfirmed(buy) => Some((&buy.fill_id, &buy.receipt_id)),
        _ => None,
    }
}

use std::collections::{BTreeMap, BTreeSet};

use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCashflowIdentity, ConfirmedCashflowKind,
    ConfirmedOrderSubmissionFact, FinalizedConditionResolutionReceipt, SessionReceipt,
    SettlementCollateralOutcome, SettlementCollateralReceipt, TradeLedgerRecord,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::trade_session_ledger) struct FinancialCashflowKey {
    pub(in crate::trade_session_ledger) ledger_sequence: u64,
    pub(in crate::trade_session_ledger) transaction_identity: String,
    pub(in crate::trade_session_ledger) kind: u8,
}

impl FinancialCashflowKey {
    pub(in crate::trade_session_ledger) fn from_identity(
        identity: &ConfirmedCashflowIdentity,
    ) -> Self {
        Self {
            ledger_sequence: identity.ledger_sequence,
            transaction_identity: identity.transaction_identity.clone(),
            kind: match identity.kind {
                ConfirmedCashflowKind::Split => 0,
                ConfirmedCashflowKind::ManagedSale => 1,
                ConfirmedCashflowKind::Merge => 2,
                ConfirmedCashflowKind::Redemption => 3,
                ConfirmedCashflowKind::Buy => 4,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::trade_session_ledger) struct IdentityRejection {
    pub(in crate::trade_session_ledger) error: String,
    pub(in crate::trade_session_ledger) affected_cohorts: Vec<String>,
    pub(in crate::trade_session_ledger) quarantine: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub(in crate::trade_session_ledger) struct FinancialIdentityIndex {
    receipts: BTreeMap<String, String>,
    trade_fills: BTreeMap<String, String>,
    order_ids: BTreeMap<String, (u64, String)>,
    intent_ids: BTreeMap<u64, (String, String)>,
    finalized_sequences: BTreeMap<u64, String>,
    receipt_source_run_id: Option<String>,
    resolution_conditions: BTreeSet<String>,
    settlement_transactions: BTreeSet<String>,
    duplicate_resolution_conditions: BTreeSet<String>,
    duplicate_settlement_transactions: BTreeSet<String>,
}

impl FinancialIdentityIndex {
    pub(in crate::trade_session_ledger) fn check_record(
        &self,
        record: &TradeLedgerRecord,
    ) -> Result<(), IdentityRejection> {
        let Some(event) = record.ctf_event.as_ref() else {
            return Ok(());
        };
        match event {
            CanonicalCtfEvent::ResolutionObserved(receipt) => {
                self.check_source(&receipt.run_id)?;
                self.check_finalized_identity(
                    receipt.ledger_seq,
                    &format!("condition_resolution:{}", receipt.condition_id),
                    self.duplicate_resolution_conditions
                        .contains(&receipt.condition_id)
                        || self.resolution_conditions.contains(&receipt.condition_id),
                    format!(
                        "duplicate_finalized_condition_resolution:condition_id={}",
                        receipt.condition_id
                    ),
                )?;
            }
            CanonicalCtfEvent::OrderAccepted(order) => {
                if let Some((first_sequence, first_cohort)) = self.order_ids.get(&order.order_id) {
                    return Err(IdentityRejection {
                        error: format!(
                            "order_submission_identity_reused:order_id={}:first_ledger_seq={first_sequence}:next_ledger_seq={}",
                            order.order_id, record.ledger_seq
                        ),
                        affected_cohorts: two_cohorts(first_cohort.clone(), order.cohort_id.clone()),
                        quarantine: Vec::new(),
                    });
                }
                if let Some((first_order_id, first_cohort)) = self.intent_ids.get(&order.intent_id)
                {
                    return Err(IdentityRejection {
                        error: format!(
                            "order_submission_intent_identity_reused:intent_id={}:first_order_id={first_order_id}:next_order_id={}",
                            order.intent_id, order.order_id
                        ),
                        affected_cohorts: two_cohorts(first_cohort.clone(), order.cohort_id.clone()),
                        quarantine: Vec::new(),
                    });
                }
            }
            _ => {
                if let (Some(receipt_id), Some(cohort_id)) = (event.receipt_id(), event.cohort_id())
                {
                    if let Some(previous_cohort) = self.receipts.get(&receipt_id) {
                        return Err(IdentityRejection {
                            error: format!(
                                "ctf_receipt_identity_reused:receipt_id={receipt_id}:first_cohort={previous_cohort}:next_cohort={cohort_id}"
                            ),
                            affected_cohorts: two_cohorts(previous_cohort.clone(), cohort_id.to_string()),
                            quarantine: Vec::new(),
                        });
                    }
                }
                if let Some((fill_id, receipt_id, cohort_id)) = confirmed_fill_identity(event) {
                    if let Some(previous_receipt) = self.trade_fills.get(fill_id) {
                        let mut affected_cohorts = Vec::new();
                        if let Some(previous_cohort) = self.receipts.get(previous_receipt) {
                            affected_cohorts.push(previous_cohort.clone());
                        }
                        if !affected_cohorts.iter().any(|value| value == cohort_id) {
                            affected_cohorts.push(cohort_id.to_string());
                        }
                        return Err(IdentityRejection {
                            error: format!(
                                "ctf_fill_identity_reused:fill_id={fill_id}:first_receipt={previous_receipt}:next_receipt={receipt_id}"
                            ),
                            affected_cohorts,
                            quarantine: Vec::new(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn check_session_receipt(
        &self,
        receipt: &SessionReceipt,
    ) -> Result<(), IdentityRejection> {
        let SessionReceipt::SettlementCollateral(settlement) = receipt else {
            return Ok(());
        };
        if !matches!(
            settlement.outcome,
            SettlementCollateralOutcome::Confirmed { .. }
        ) {
            return Ok(());
        }
        self.check_source(&settlement.run_id)?;
        self.check_finalized_identity(
            settlement.ledger_seq,
            &format!("settlement:{}", settlement.tx_hash),
            self.duplicate_settlement_transactions
                .contains(&settlement.tx_hash)
                || self.settlement_transactions.contains(&settlement.tx_hash),
            format!(
                "duplicate_settlement_collateral_receipt:tx_hash={}",
                settlement.tx_hash
            ),
        )
    }

    pub(in crate::trade_session_ledger) fn admit_record(
        &mut self,
        record: &TradeLedgerRecord,
    ) -> Result<(), IdentityRejection> {
        let Some(event) = record.ctf_event.as_ref() else {
            return Ok(());
        };
        if let CanonicalCtfEvent::ResolutionObserved(receipt) = event {
            return self.admit_resolution(receipt);
        }
        if let CanonicalCtfEvent::OrderAccepted(order) = event {
            return self.admit_order(record.ledger_seq, order);
        }
        if let (Some(receipt_id), Some(cohort_id)) = (event.receipt_id(), event.cohort_id()) {
            self.admit_receipt(&receipt_id, cohort_id)?;
        }
        self.admit_trade_fill(event)?;
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn admit_session_receipt(
        &mut self,
        receipt: &SessionReceipt,
    ) -> Result<(), IdentityRejection> {
        match receipt {
            SessionReceipt::SettlementCollateral(settlement)
                if matches!(
                    settlement.outcome,
                    SettlementCollateralOutcome::Confirmed { .. }
                ) =>
            {
                self.admit_settlement(settlement)
            }
            _ => Ok(()),
        }
    }

    pub(in crate::trade_session_ledger) fn admit_resolution(
        &mut self,
        receipt: &FinalizedConditionResolutionReceipt,
    ) -> Result<(), IdentityRejection> {
        self.admit_source(&receipt.run_id)?;
        let identity = format!("condition_resolution:{}", receipt.condition_id);
        if self
            .duplicate_resolution_conditions
            .contains(&receipt.condition_id)
            || self.resolution_conditions.contains(&receipt.condition_id)
        {
            self.resolution_conditions.remove(&receipt.condition_id);
            self.duplicate_resolution_conditions
                .insert(receipt.condition_id.clone());
            return Err(IdentityRejection {
                error: format!(
                    "duplicate_finalized_condition_resolution:condition_id={}",
                    receipt.condition_id
                ),
                affected_cohorts: Vec::new(),
                quarantine: vec![identity],
            });
        }
        if let Some(first_identity) = self
            .finalized_sequences
            .insert(receipt.ledger_seq, identity.clone())
        {
            self.quarantine_identity(&first_identity);
            self.duplicate_resolution_conditions
                .insert(receipt.condition_id.clone());
            return Err(IdentityRejection {
                error: format!(
                    "finalized_receipt_ledger_sequence_reused:ledger_seq={}:first_identity={}:next_identity={identity}",
                    receipt.ledger_seq, first_identity
                ),
                affected_cohorts: Vec::new(),
                quarantine: vec![first_identity, identity],
            });
        }
        self.resolution_conditions
            .insert(receipt.condition_id.clone());
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn admit_settlement(
        &mut self,
        receipt: &SettlementCollateralReceipt,
    ) -> Result<(), IdentityRejection> {
        self.admit_source(&receipt.run_id)?;
        let identity = format!("settlement:{}", receipt.tx_hash);
        if self
            .duplicate_settlement_transactions
            .contains(&receipt.tx_hash)
            || self.settlement_transactions.contains(&receipt.tx_hash)
        {
            self.settlement_transactions.remove(&receipt.tx_hash);
            self.duplicate_settlement_transactions
                .insert(receipt.tx_hash.clone());
            return Err(IdentityRejection {
                error: format!(
                    "duplicate_settlement_collateral_receipt:tx_hash={}",
                    receipt.tx_hash
                ),
                affected_cohorts: Vec::new(),
                quarantine: vec![identity],
            });
        }
        if let Some(first_identity) = self
            .finalized_sequences
            .insert(receipt.ledger_seq, identity.clone())
        {
            self.quarantine_identity(&first_identity);
            self.duplicate_settlement_transactions
                .insert(receipt.tx_hash.clone());
            return Err(IdentityRejection {
                error: format!(
                    "finalized_receipt_ledger_sequence_reused:ledger_seq={}:first_identity={}:next_identity={identity}",
                    receipt.ledger_seq, first_identity
                ),
                affected_cohorts: Vec::new(),
                quarantine: vec![first_identity, identity],
            });
        }
        self.settlement_transactions.insert(receipt.tx_hash.clone());
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn admit_order(
        &mut self,
        ledger_seq: u64,
        fact: &ConfirmedOrderSubmissionFact,
    ) -> Result<(), IdentityRejection> {
        if let Some((first_sequence, first_cohort)) = self
            .order_ids
            .insert(fact.order_id.clone(), (ledger_seq, fact.cohort_id.clone()))
        {
            let error = format!(
                "order_submission_identity_reused:order_id={}:first_ledger_seq={first_sequence}:next_ledger_seq={ledger_seq}",
                fact.order_id
            );
            return Err(IdentityRejection {
                error,
                affected_cohorts: two_cohorts(first_cohort, fact.cohort_id.clone()),
                quarantine: Vec::new(),
            });
        }
        if let Some((first_order_id, first_cohort)) = self.intent_ids.insert(
            fact.intent_id,
            (fact.order_id.clone(), fact.cohort_id.clone()),
        ) {
            let error = format!(
                "order_submission_intent_identity_reused:intent_id={}:first_order_id={first_order_id}:next_order_id={}",
                fact.intent_id, fact.order_id
            );
            return Err(IdentityRejection {
                error,
                affected_cohorts: two_cohorts(first_cohort, fact.cohort_id.clone()),
                quarantine: Vec::new(),
            });
        }
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn admit_receipt(
        &mut self,
        receipt_id: &str,
        cohort_id: &str,
    ) -> Result<(), IdentityRejection> {
        if let Some(previous_cohort) = self
            .receipts
            .insert(receipt_id.to_string(), cohort_id.to_string())
        {
            let error = format!(
                "ctf_receipt_identity_reused:receipt_id={receipt_id}:first_cohort={previous_cohort}:next_cohort={cohort_id}"
            );
            return Err(IdentityRejection {
                error,
                affected_cohorts: two_cohorts(previous_cohort, cohort_id.to_string()),
                quarantine: Vec::new(),
            });
        }
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn admit_trade_fill(
        &mut self,
        event: &CanonicalCtfEvent,
    ) -> Result<(), IdentityRejection> {
        let Some((fill_id, receipt_id, cohort_id)) = confirmed_fill_identity(event) else {
            return Ok(());
        };
        if let Some(previous_receipt) = self
            .trade_fills
            .insert(fill_id.to_string(), receipt_id.to_string())
        {
            let error = format!(
                "ctf_fill_identity_reused:fill_id={fill_id}:first_receipt={previous_receipt}:next_receipt={receipt_id}"
            );
            let mut affected_cohorts = Vec::new();
            if let Some(previous_cohort) = self.receipts.get(&previous_receipt) {
                affected_cohorts.push(previous_cohort.clone());
            }
            if !affected_cohorts.iter().any(|value| value == cohort_id) {
                affected_cohorts.push(cohort_id.to_string());
            }
            return Err(IdentityRejection {
                error,
                affected_cohorts,
                quarantine: Vec::new(),
            });
        }
        Ok(())
    }

    fn admit_source(&mut self, run_id: &str) -> Result<(), IdentityRejection> {
        if let Some(expected) = self.receipt_source_run_id.as_deref() {
            if expected != run_id {
                return Err(IdentityRejection {
                    error: format!(
                        "finalized_receipt_source_run_mismatch:expected={expected}:actual={run_id}"
                    ),
                    affected_cohorts: Vec::new(),
                    quarantine: Vec::new(),
                });
            }
        } else {
            self.receipt_source_run_id = Some(run_id.to_string());
        }
        Ok(())
    }

    fn check_source(&self, run_id: &str) -> Result<(), IdentityRejection> {
        if let Some(expected) = self.receipt_source_run_id.as_deref() {
            if expected != run_id {
                return Err(IdentityRejection {
                    error: format!(
                        "finalized_receipt_source_run_mismatch:expected={expected}:actual={run_id}"
                    ),
                    affected_cohorts: Vec::new(),
                    quarantine: Vec::new(),
                });
            }
        }
        Ok(())
    }

    fn check_finalized_identity(
        &self,
        ledger_seq: u64,
        identity: &str,
        duplicate: bool,
        duplicate_error: String,
    ) -> Result<(), IdentityRejection> {
        if duplicate {
            return Err(IdentityRejection {
                error: duplicate_error,
                affected_cohorts: Vec::new(),
                quarantine: vec![identity.to_string()],
            });
        }
        if let Some(first_identity) = self.finalized_sequences.get(&ledger_seq) {
            return Err(IdentityRejection {
                error: format!(
                    "finalized_receipt_ledger_sequence_reused:ledger_seq={ledger_seq}:first_identity={first_identity}:next_identity={identity}"
                ),
                affected_cohorts: Vec::new(),
                quarantine: vec![first_identity.clone(), identity.to_string()],
            });
        }
        Ok(())
    }

    fn quarantine_identity(&mut self, identity: &str) {
        if let Some(condition_id) = identity.strip_prefix("condition_resolution:") {
            self.resolution_conditions.remove(condition_id);
            self.duplicate_resolution_conditions
                .insert(condition_id.to_string());
        }
        if let Some(tx_hash) = identity.strip_prefix("settlement:") {
            self.settlement_transactions.remove(tx_hash);
            self.duplicate_settlement_transactions
                .insert(tx_hash.to_string());
        }
    }
}

fn two_cohorts(first: String, second: String) -> Vec<String> {
    if first == second {
        vec![first]
    } else {
        vec![first, second]
    }
}

fn confirmed_fill_identity(event: &CanonicalCtfEvent) -> Option<(&str, &str, &str)> {
    match event {
        CanonicalCtfEvent::TradeConfirmed(sell) => {
            Some((&sell.fill_id, &sell.receipt_id, &sell.cohort_id))
        }
        CanonicalCtfEvent::BuyConfirmed(buy) => {
            Some((&buy.fill_id, &buy.receipt_id, &buy.cohort_id))
        }
        _ => None,
    }
}

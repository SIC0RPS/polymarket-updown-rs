use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::trade_session_ledger::stats::{
    confirmed_event_cashflow, confirmed_split_cashflow, financial_event_order_key,
    project_retained_financial_cohort, validate_retained_financial_stamped_event,
    CanonicalFinancialProjection, CohortFinancialState, FinancialCashflowKey,
    FinancialCohortProjection, FinancialStampedEvent, IdentityRejection,
};
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedReceiptCashflow, DirectEoaGasReceipt, DirectEoaOperation,
    FinalizedConditionResolutionReceipt, LiveWalletAccounting, SessionReceipt,
    SettlementCollateralOutcome, SettlementCollateralReceipt, TradeLedgerRecord,
};

use super::{financial_summary::FinancialSummaryBook, wallet};

#[derive(Clone)]
pub(super) struct LiveFinancialState {
    run_id: String,
    revision: u64,
    summaries: FinancialSummaryBook,
    cohorts: BTreeMap<String, LiveFinancialCohort>,
    unresolved: BTreeMap<String, Arc<crate::trade_session_ledger::CanonicalUnresolvedCohortStats>>,
    split_cohort_by_transaction: BTreeMap<String, String>,
    resolutions: BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlements: BTreeMap<String, SettlementCollateralReceipt>,
    split_gas_by_transaction: BTreeMap<String, DirectEoaGasReceipt>,
    cashflows: BTreeMap<FinancialCashflowKey, (String, ConfirmedReceiptCashflow)>,
    cashflows_by_cohort: BTreeMap<String, BTreeSet<FinancialCashflowKey>>,
    cashflow_net_pusd_units_6dp: i128,
    invalid_cohorts: BTreeMap<String, String>,
}

#[derive(Clone)]
struct LiveFinancialCohort {
    reducer: CohortFinancialState,
    events: Vec<FinancialStampedEvent>,
    projection: FinancialCohortProjection,
}

#[derive(Default)]
pub(super) struct FinancialMutation {
    pub(super) removed_cashflows: Vec<ConfirmedReceiptCashflow>,
    pub(super) added_cashflows: Vec<ConfirmedReceiptCashflow>,
}

pub(super) struct FinancialApply {
    pub(super) wallet: Option<LiveWalletAccounting>,
}

struct PreparedCashflowReplacement {
    cohort_id: String,
    next: BTreeMap<FinancialCashflowKey, ConfirmedReceiptCashflow>,
    next_keys: BTreeSet<FinancialCashflowKey>,
    total: i128,
    mutation: FinancialMutation,
}

impl LiveFinancialState {
    pub(super) fn new(run_id: &str, expected_symbols: &[String]) -> Self {
        Self {
            run_id: run_id.to_string(),
            revision: 0,
            summaries: FinancialSummaryBook::new(expected_symbols),
            cohorts: BTreeMap::new(),
            unresolved: BTreeMap::new(),
            split_cohort_by_transaction: BTreeMap::new(),
            resolutions: BTreeMap::new(),
            settlements: BTreeMap::new(),
            split_gas_by_transaction: BTreeMap::new(),
            cashflows: BTreeMap::new(),
            cashflows_by_cohort: BTreeMap::new(),
            cashflow_net_pusd_units_6dp: 0,
            invalid_cohorts: BTreeMap::new(),
        }
    }

    pub(super) fn apply_record(
        &mut self,
        record: &TradeLedgerRecord,
        current_wallet: Option<&LiveWalletAccounting>,
    ) -> Result<FinancialApply> {
        let Some(event) = record.ctf_event.as_ref() else {
            self.summaries.apply_record(record, None)?;
            self.revision = self.revision.max(record.ledger_seq);
            return Ok(unchanged_wallet(current_wallet));
        };
        match event {
            CanonicalCtfEvent::ResolutionObserved(receipt) => {
                self.summaries.apply_record(record, None)?;
                self.resolutions
                    .insert(receipt.condition_id.clone(), receipt.clone());
                self.revision = self.revision.max(record.ledger_seq);
                return Ok(unchanged_wallet(current_wallet));
            }
            CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_) => {
                let cohort_id = event.cohort_id().context("acquisition has no cohort")?;
                if !self.cohorts.contains_key(cohort_id) {
                    let events = vec![FinancialStampedEvent::new(record, event.clone())];
                    let next = rebuild_cohort(
                        &record.run_id,
                        events,
                        self.invalid_cohorts.get(cohort_id).cloned(),
                        &self.resolutions,
                        &self.settlements,
                    )?;
                    let cashflows = self.project_cohort_cashflows(&next, None)?;
                    let prepared = self.prepare_cohort_cashflows(cohort_id, cashflows)?;
                    let next_wallet = project_wallet(current_wallet, &prepared.mutation)?;
                    self.summaries
                        .apply_record(record, Some((None, &next.projection)))?;
                    if let CanonicalCtfEvent::SplitConfirmed(split) = event {
                        self.split_cohort_by_transaction
                            .insert(split.transaction_id.clone(), split.cohort_id.clone());
                    }
                    self.commit_cohort(cohort_id, next, prepared);
                    self.revision = self.revision.max(record.ledger_seq);
                    return Ok(FinancialApply {
                        wallet: next_wallet,
                    });
                }
            }
            CanonicalCtfEvent::TradeConfirmed(_)
            | CanonicalCtfEvent::MergeConfirmed(_)
            | CanonicalCtfEvent::RedeemConfirmed(_) => {}
            CanonicalCtfEvent::FeeApplied(charge) | CanonicalCtfEvent::CostApplied(charge) => {
                let _ = charge;
            }
            CanonicalCtfEvent::OrderAccepted(_)
            | CanonicalCtfEvent::TradeMatched(_)
            | CanonicalCtfEvent::TradeFailed(_) => {
                self.summaries.apply_record(record, None)?;
                self.revision = self.revision.max(record.ledger_seq);
                return Ok(unchanged_wallet(current_wallet));
            }
        }
        let cohort_id = event
            .cohort_id()
            .context("retained financial event has no cohort identity")?;
        let current = self.cohorts.get(cohort_id).with_context(|| {
            format!("retained financial event precedes acquisition cohort_id={cohort_id}")
        })?;
        let prior = current.projection.clone();
        let invalid_detail = match event {
            CanonicalCtfEvent::FeeApplied(charge) | CanonicalCtfEvent::CostApplied(charge) => {
                Some(format!(
                    "standalone_charge_not_bound_to_cashflow:receipt_id={}",
                    charge.receipt_id
                ))
            }
            _ => self.invalid_cohorts.get(cohort_id).cloned(),
        };
        let next = advance_cohort(
            &record.run_id,
            current,
            FinancialStampedEvent::new(record, event.clone()),
            invalid_detail.clone(),
            &self.resolutions,
            &self.settlements,
        )?;
        let cashflows = self.project_cohort_cashflows(&next, None)?;
        let prepared = self.prepare_cohort_cashflows(cohort_id, cashflows)?;
        let next_wallet = project_wallet(current_wallet, &prepared.mutation)?;
        self.summaries
            .apply_record(record, Some((Some(&prior), &next.projection)))?;
        if let Some(detail) = invalid_detail {
            self.invalid_cohorts.insert(cohort_id.to_string(), detail);
        }
        self.commit_cohort(cohort_id, next, prepared);
        self.revision = self.revision.max(record.ledger_seq);
        Ok(FinancialApply {
            wallet: next_wallet,
        })
    }

    pub(super) fn apply_receipt(
        &mut self,
        receipt: &SessionReceipt,
        current_wallet: Option<&LiveWalletAccounting>,
    ) -> Result<FinancialApply> {
        let apply = match receipt {
            SessionReceipt::DirectEoaGas(gas)
                if gas.status
                    && gas.operation == DirectEoaOperation::Split
                    && gas.block_number.is_some() =>
            {
                let Some(cohort_id) = self.split_cohort_by_transaction.get(&gas.tx_hash).cloned()
                else {
                    self.split_gas_by_transaction
                        .insert(gas.tx_hash.clone(), gas.clone());
                    self.revision = self.revision.max(receipt.ledger_seq());
                    return Ok(unchanged_wallet(current_wallet));
                };
                let cohort = self
                    .cohorts
                    .get(&cohort_id)
                    .context("financial cashflow cohort missing")?;
                let cashflows = self.project_cohort_cashflows(cohort, Some(gas))?;
                let prepared = self.prepare_cohort_cashflows(&cohort_id, cashflows)?;
                let next_wallet = project_wallet(current_wallet, &prepared.mutation)?;
                self.split_gas_by_transaction
                    .insert(gas.tx_hash.clone(), gas.clone());
                self.commit_cohort_cashflows(prepared);
                FinancialApply {
                    wallet: next_wallet,
                }
            }
            SessionReceipt::SettlementCollateral(settlement)
                if matches!(
                    settlement.outcome,
                    SettlementCollateralOutcome::Confirmed { .. }
                ) =>
            {
                self.settlements
                    .insert(settlement.tx_hash.clone(), settlement.clone());
                unchanged_wallet(current_wallet)
            }
            SessionReceipt::DirectEoaSubmission(_)
            | SessionReceipt::DirectEoaGas(_)
            | SessionReceipt::MakerRebate(_)
            | SessionReceipt::SettlementCollateral(_)
            | SessionReceipt::ReceiptConflict(_) => unchanged_wallet(current_wallet),
        };
        self.revision = self.revision.max(receipt.ledger_seq());
        Ok(apply)
    }

    pub(super) fn projection(&self) -> Arc<CanonicalFinancialProjection> {
        Arc::new(CanonicalFinancialProjection {
            revision: self.revision,
            ctf_summary: self.summaries.rows(),
            closed_cohorts: self
                .cohorts
                .values()
                .filter_map(|cohort| cohort.projection.closed().cloned())
                .collect::<Vec<_>>()
                .into(),
            unresolved_cohorts: self.unresolved.values().cloned().collect::<Vec<_>>().into(),
        })
    }

    pub(super) fn cashflow_count(&self) -> u64 {
        u64::try_from(self.cashflows.len()).unwrap_or(u64::MAX)
    }

    pub(super) fn cashflow_net_pusd_units_6dp(&self) -> i128 {
        self.cashflow_net_pusd_units_6dp
    }

    pub(super) fn cold_cashflows(&self) -> Arc<[ConfirmedReceiptCashflow]> {
        self.cashflows
            .values()
            .map(|(_, cashflow)| cashflow.clone())
            .collect::<Vec<_>>()
            .into()
    }

    fn commit_cohort(
        &mut self,
        cohort_id: &str,
        next: LiveFinancialCohort,
        prepared: PreparedCashflowReplacement,
    ) -> FinancialMutation {
        let mutation = self.commit_cohort_cashflows(prepared);
        if let Some(cohort) = next.projection.unresolved() {
            self.unresolved
                .insert(cohort_id.to_string(), Arc::clone(cohort));
        } else {
            self.unresolved.remove(cohort_id);
        }
        self.cohorts.insert(cohort_id.to_string(), next);
        mutation
    }

    fn project_cohort_cashflows(
        &self,
        cohort: &LiveFinancialCohort,
        split_gas: Option<&DirectEoaGasReceipt>,
    ) -> Result<BTreeMap<FinancialCashflowKey, ConfirmedReceiptCashflow>> {
        if cohort.projection.cashflow().is_none() {
            return Ok(BTreeMap::new());
        }
        let mut cashflows = BTreeMap::new();
        let split_transaction = &cohort
            .projection
            .cashflow()
            .context("financial cohort cashflow missing")?
            .split_transaction_id;
        let gas = split_gas
            .filter(|gas| &gas.tx_hash == split_transaction)
            .or_else(|| self.split_gas_by_transaction.get(split_transaction));
        if let Some(cashflow) = confirmed_split_cashflow(&cohort.projection, gas)? {
            cashflows.insert(
                FinancialCashflowKey::from_identity(&cashflow.identity),
                cashflow,
            );
        }
        for event in &cohort.events {
            if let Some(cashflow) = confirmed_event_cashflow(event.ledger_seq, &cohort.projection)?
            {
                let key = FinancialCashflowKey::from_identity(&cashflow.identity);
                anyhow::ensure!(
                    cashflows.insert(key, cashflow).is_none(),
                    "cohort cashflow identity reused"
                );
            }
        }
        Ok(cashflows)
    }

    pub(super) fn apply_identity_rejection(
        &mut self,
        rejection: &IdentityRejection,
        evidence_cohort: &BTreeMap<String, BTreeSet<String>>,
        ledger_seq: u64,
        current_wallet: Option<&LiveWalletAccounting>,
    ) -> Result<FinancialApply> {
        let quarantined = rejection.quarantine.clone();
        let affected = Self::affected_rejection_cohorts(rejection, evidence_cohort);

        let mut replacements = Vec::new();
        let mut next_cohorts = Vec::new();
        let mut mutation = FinancialMutation::default();
        let mut total = self.cashflow_net_pusd_units_6dp;
        for cohort_id in affected {
            let Some(current) = self.cohorts.get(&cohort_id) else {
                next_cohorts.push((cohort_id, None));
                continue;
            };
            let prior = current.projection.clone();
            let next = rebuild_cohort(
                &self.run_id,
                current.events.clone(),
                Some(rejection.error.clone()),
                &self.resolutions,
                &self.settlements,
            )?;
            replacements.push((prior, next.projection.clone()));
            for key in self
                .cashflows_by_cohort
                .get(&cohort_id)
                .into_iter()
                .flat_map(|keys| keys.iter())
            {
                let (_, cashflow) = self.cashflows.get(key).context("cohort cashflow missing")?;
                total = total
                    .checked_sub(cashflow.net_pusd_units_6dp)
                    .context("cashflow withdrawal overflow")?;
                mutation.removed_cashflows.push(cashflow.clone());
            }
            next_cohorts.push((cohort_id, Some(next)));
        }

        let next_wallet = project_wallet(current_wallet, &mutation)?;
        self.summaries.replace_many(&replacements)?;
        for identity in quarantined {
            if let Some(condition_id) = identity.strip_prefix("condition_resolution:") {
                self.resolutions.remove(condition_id);
            }
            if let Some(tx_hash) = identity.strip_prefix("settlement:") {
                self.settlements.remove(tx_hash);
            }
        }
        for (cohort_id, next) in next_cohorts {
            self.invalid_cohorts
                .entry(cohort_id.clone())
                .or_insert_with(|| rejection.error.clone());
            let Some(next) = next else {
                continue;
            };
            if let Some(keys) = self.cashflows_by_cohort.remove(&cohort_id) {
                for key in keys {
                    self.cashflows.remove(&key);
                }
            }
            if let Some(unresolved) = next.projection.unresolved() {
                self.unresolved
                    .insert(cohort_id.clone(), Arc::clone(unresolved));
            } else {
                self.unresolved.remove(&cohort_id);
            }
            self.cohorts.insert(cohort_id, next);
        }
        self.cashflow_net_pusd_units_6dp = total;
        self.revision = self.revision.max(ledger_seq);
        Ok(FinancialApply {
            wallet: next_wallet,
        })
    }

    pub(super) fn affected_rejection_cohorts(
        rejection: &IdentityRejection,
        evidence_cohort: &BTreeMap<String, BTreeSet<String>>,
    ) -> BTreeSet<String> {
        let mut affected = rejection
            .affected_cohorts
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for identity in &rejection.quarantine {
            if let Some(cohorts) = evidence_cohort.get(identity) {
                affected.extend(cohorts.iter().cloned());
            }
        }
        affected
    }

    fn prepare_cohort_cashflows(
        &self,
        cohort_id: &str,
        next: BTreeMap<FinancialCashflowKey, ConfirmedReceiptCashflow>,
    ) -> Result<PreparedCashflowReplacement> {
        let prior_keys = self
            .cashflows_by_cohort
            .get(cohort_id)
            .cloned()
            .unwrap_or_default();
        let next_keys = next.keys().cloned().collect::<BTreeSet<_>>();
        let mut mutation = FinancialMutation::default();
        let mut total = self.cashflow_net_pusd_units_6dp;
        for key in prior_keys.difference(&next_keys) {
            let (_, cashflow) = self.cashflows.get(key).context("cohort cashflow missing")?;
            total = total
                .checked_sub(cashflow.net_pusd_units_6dp)
                .context("cashflow withdrawal overflow")?;
            mutation.removed_cashflows.push(cashflow.clone());
        }
        for key in next_keys.difference(&prior_keys) {
            let cashflow = &next[key];
            if let Some((owner, existing)) = self.cashflows.get(key) {
                anyhow::ensure!(
                    owner == cohort_id && existing == cashflow,
                    "retained financial cashflow identity conflicts"
                );
            }
            total = total
                .checked_add(cashflow.net_pusd_units_6dp)
                .context("cashflow admission overflow")?;
            mutation.added_cashflows.push(cashflow.clone());
        }
        for key in next_keys.intersection(&prior_keys) {
            let cashflow = &next[key];
            let (owner, existing) = self.cashflows.get(key).context("cohort cashflow missing")?;
            anyhow::ensure!(
                owner == cohort_id,
                "retained financial cashflow owner changed"
            );
            if existing != cashflow {
                total = total
                    .checked_sub(existing.net_pusd_units_6dp)
                    .and_then(|value| value.checked_add(cashflow.net_pusd_units_6dp))
                    .context("cashflow replacement overflow")?;
                mutation.removed_cashflows.push(existing.clone());
                mutation.added_cashflows.push(cashflow.clone());
            }
        }
        Ok(PreparedCashflowReplacement {
            cohort_id: cohort_id.to_string(),
            next,
            next_keys,
            total,
            mutation,
        })
    }

    fn commit_cohort_cashflows(
        &mut self,
        prepared: PreparedCashflowReplacement,
    ) -> FinancialMutation {
        let prior_keys = self
            .cashflows_by_cohort
            .get(&prepared.cohort_id)
            .cloned()
            .unwrap_or_default();
        for key in prior_keys.difference(&prepared.next_keys) {
            self.cashflows.remove(key);
        }
        for (key, cashflow) in prepared.next {
            self.cashflows
                .insert(key, (prepared.cohort_id.clone(), cashflow));
        }
        if prepared.next_keys.is_empty() {
            self.cashflows_by_cohort.remove(&prepared.cohort_id);
        } else {
            self.cashflows_by_cohort
                .insert(prepared.cohort_id, prepared.next_keys);
        }
        self.cashflow_net_pusd_units_6dp = prepared.total;
        prepared.mutation
    }
}

fn unchanged_wallet(current: Option<&LiveWalletAccounting>) -> FinancialApply {
    FinancialApply {
        wallet: current.cloned(),
    }
}

fn project_wallet(
    current: Option<&LiveWalletAccounting>,
    mutation: &FinancialMutation,
) -> Result<Option<LiveWalletAccounting>> {
    current
        .map(|wallet| wallet::project_cashflow_mutation(wallet, mutation))
        .transpose()
}

fn advance_cohort(
    run_id: &str,
    current: &LiveFinancialCohort,
    event: FinancialStampedEvent,
    mut invalid_detail: Option<String>,
    resolutions: &BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlements: &BTreeMap<String, SettlementCollateralReceipt>,
) -> Result<LiveFinancialCohort> {
    if invalid_detail.is_none() {
        invalid_detail = current
            .projection
            .unresolved()
            .and_then(|cohort| cohort.state.detail())
            .map(str::to_string);
    }
    let buy_origin = current.reducer.entry().buy_authorization.is_some();
    let is_tail = current
        .events
        .last()
        .map(|last| {
            financial_event_order_key(buy_origin, last.ts_ms, last.ledger_seq)
                < financial_event_order_key(buy_origin, event.ts_ms, event.ledger_seq)
        })
        .unwrap_or(true);
    if !is_tail {
        let mut events = current.events.clone();
        events.push(event);
        return rebuild_cohort(run_id, events, invalid_detail, resolutions, settlements);
    }
    let mut reducer = current.reducer.clone();
    if invalid_detail.is_none() {
        if let Err(error) = validate_retained_financial_stamped_event(
            reducer.entry(),
            &event,
            resolutions,
            settlements,
        )
        .and_then(|_| reducer.apply_validated_event(&event.event))
        {
            invalid_detail = Some(error.to_string());
        }
    }
    let mut events = current.events.clone();
    events.push(event);
    let projection = if let Some(detail) = invalid_detail.as_ref() {
        FinancialCohortProjection::invalid(reducer.entry(), &events, detail.clone())?
    } else {
        project_retained_financial_cohort(run_id, &reducer, &events, resolutions, settlements)?
    };
    Ok(LiveFinancialCohort {
        reducer,
        events,
        projection,
    })
}

fn rebuild_cohort(
    run_id: &str,
    mut events: Vec<FinancialStampedEvent>,
    mut invalid_detail: Option<String>,
    resolutions: &BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlements: &BTreeMap<String, SettlementCollateralReceipt>,
) -> Result<LiveFinancialCohort> {
    let buy_origin = events
        .iter()
        .filter(|event| {
            matches!(
                &event.event,
                CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_)
            )
        })
        .min_by_key(|event| event.ledger_seq)
        .is_some_and(|event| matches!(&event.event, CanonicalCtfEvent::BuyConfirmed(_)));
    events
        .sort_by_key(|event| financial_event_order_key(buy_origin, event.ts_ms, event.ledger_seq));
    let first = events
        .iter()
        .find(|event| {
            matches!(
                &event.event,
                CanonicalCtfEvent::SplitConfirmed(_) | CanonicalCtfEvent::BuyConfirmed(_)
            )
        })
        .context("retained financial cohort has no confirmed acquisition")?;
    let first_sequence = first.ledger_seq;
    let cohort_id = first
        .event
        .cohort_id()
        .context("acquisition has no cohort")?;
    let mut reducer = CohortFinancialState::new(cohort_id, &first.event)?;
    if invalid_detail.is_none() {
        for event in &events {
            if event.ledger_seq == first_sequence {
                continue;
            }
            if let Err(error) = validate_retained_financial_stamped_event(
                reducer.entry(),
                event,
                resolutions,
                settlements,
            )
            .and_then(|_| reducer.apply_validated_event(&event.event))
            {
                invalid_detail = Some(error.to_string());
                break;
            }
        }
    }
    let projection = if let Some(detail) = invalid_detail.as_ref() {
        FinancialCohortProjection::invalid(reducer.entry(), &events, detail.clone())?
    } else {
        project_retained_financial_cohort(run_id, &reducer, &events, resolutions, settlements)?
    };
    Ok(LiveFinancialCohort {
        reducer,
        events,
        projection,
    })
}

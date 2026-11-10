use super::*;

#[path = "audit_facts/identity.rs"]
mod identity;
pub(in crate::trade_session_ledger) use identity::{
    FinancialCashflowKey, FinancialIdentityIndex, IdentityRejection,
};

pub(super) struct AuditFacts<'a> {
    pub(super) cohorts: BTreeMap<String, CohortFacts>,
    pub(super) submissions: BTreeMap<String, Vec<(u64, ConfirmedOrderSubmissionFact)>>,
    pub(super) invalid_cohort_receipt_errors: BTreeMap<String, String>,
    pub(super) resolution_by_condition: BTreeMap<String, &'a FinalizedConditionResolutionReceipt>,
    pub(super) settlement_by_tx: BTreeMap<String, &'a SettlementCollateralReceipt>,
}

pub(super) fn project<'a>(
    records: &'a [TradeLedgerRecord],
    settlement_receipts: &'a [SettlementCollateralReceipt],
    violations: &mut Vec<String>,
) -> AuditFacts<'a> {
    let mut cohorts = BTreeMap::<String, CohortFacts>::new();
    let mut submissions = BTreeMap::<String, Vec<(u64, ConfirmedOrderSubmissionFact)>>::new();
    let mut identities = FinancialIdentityIndex::default();
    let mut invalid_cohort_receipt_errors = BTreeMap::<String, String>::new();
    let mut resolution_by_condition = BTreeMap::new();
    let mut settlement_by_tx = BTreeMap::new();
    for record in records {
        let Some(CanonicalCtfEvent::ResolutionObserved(receipt)) = record.ctf_event.as_ref() else {
            continue;
        };
        if let Err(error) = super::super::validate_ctf_event_record(record) {
            violations.push(format!(
                "invalid_resolution_observed:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        if let Err(rejection) = identities.admit_resolution(receipt) {
            quarantine_finalized_receipts(
                &rejection,
                &mut resolution_by_condition,
                &mut settlement_by_tx,
            );
            violations.push(rejection.error);
            continue;
        }
        resolution_by_condition.insert(receipt.condition_id.clone(), receipt);
    }
    for receipt in settlement_receipts {
        if let Err(error) = receipt.confirmed_amount_units_6dp() {
            violations.push(format!(
                "invalid_settlement_collateral_receipt:tx_hash={}:{}",
                receipt.tx_hash, error
            ));
            continue;
        }
        if let Err(rejection) = identities.admit_settlement(receipt) {
            quarantine_finalized_receipts(
                &rejection,
                &mut resolution_by_condition,
                &mut settlement_by_tx,
            );
            violations.push(rejection.error);
            continue;
        }
        settlement_by_tx.insert(receipt.tx_hash.clone(), receipt);
    }
    for record in records {
        let Some(event) = record.ctf_event.as_ref() else {
            continue;
        };
        if matches!(event, CanonicalCtfEvent::ResolutionObserved(_)) {
            continue;
        }
        if let Err(error) = super::super::validate_ctf_event_record(record) {
            violations.push(format!(
                "invalid_ctf_event:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        if let CanonicalCtfEvent::OrderAccepted(fact) = event {
            if let Err(rejection) = identities.admit_order(record.ledger_seq, fact) {
                record_identity_rejection(
                    rejection,
                    &mut invalid_cohort_receipt_errors,
                    violations,
                );
                continue;
            }
            submissions
                .entry(fact.cohort_id.clone())
                .or_default()
                .push((record.ledger_seq, fact.clone()));
            continue;
        }
        let (Some(receipt_id), Some(cohort_id)) = (event.receipt_id(), event.cohort_id()) else {
            continue;
        };
        if let Err(rejection) = identities.admit_receipt(&receipt_id, cohort_id) {
            record_identity_rejection(rejection, &mut invalid_cohort_receipt_errors, violations);
            continue;
        }
        if let Err(rejection) = identities.admit_trade_fill(event) {
            record_identity_rejection(rejection, &mut invalid_cohort_receipt_errors, violations);
            continue;
        }
        let stamped = StampedFact {
            run_id: record.run_id.clone(),
            ledger_seq: record.ledger_seq,
            ts_ms: record.ts_ms,
            fact: event.clone(),
        };
        let cohort = cohorts.entry(cohort_id.to_string()).or_default();
        cohort.facts.push(stamped);
    }

    AuditFacts {
        cohorts,
        submissions,
        invalid_cohort_receipt_errors,
        resolution_by_condition,
        settlement_by_tx,
    }
}

pub(super) fn project_resident(
    index: &TradeLedgerRunIndex,
    records: &[TradeLedgerRecord],
    expected_symbols: &[String],
    financial: &CanonicalFinancialProjection,
) -> CanonicalSessionStats {
    let normalized_symbols = expected_symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut stats = CanonicalSessionStats {
        canonical: CanonicalStatsProof::unavailable(financial.revision, &normalized_symbols),
        market_count: index.market_count,
        trade_count: index.trade_count,
        event_count: index.event_count,
        ..CanonicalSessionStats::unavailable(&normalized_symbols)
    };
    if normalized_symbols.len() != expected_symbols.len()
        || normalized_symbols.is_empty()
        || normalized_symbols
            .iter()
            .any(|symbol| !matches!(symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP"))
    {
        stats
            .violations
            .push("invalid_canonical_expected_symbol_scope".to_string());
    }
    stats.canonical.ctf_summary = financial.ctf_summary.iter().cloned().collect();
    stats.closed_cohorts = financial
        .closed_cohorts
        .iter()
        .map(|cohort| cohort.as_ref().clone())
        .collect();
    stats.unresolved_cohorts = financial
        .unresolved_cohorts
        .iter()
        .map(|cohort| cohort.as_ref().clone())
        .collect();
    let mut transition_identities = BTreeSet::new();
    let mut transitions = Vec::new();
    let mut current = BTreeMap::<(String, String), (u64, CanonicalOwnedLegOutcome)>::new();
    for record in records {
        let Some(outcome) = record.owned_leg_outcome.as_ref() else {
            continue;
        };
        if let Err(error) = super::super::validate_owned_leg_outcome_record(record, records.iter())
        {
            stats.violations.push(format!(
                "invalid_owned_leg_outcome:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        let identity = (
            outcome.cohort_id.clone(),
            outcome.token.clone(),
            outcome.ledger_seq,
        );
        if !transition_identities.insert(identity) {
            stats.violations.push(format!(
                "duplicate_owned_leg_outcome:cohort={}:token={}:ledger_seq={}",
                outcome.cohort_id, outcome.token, outcome.ledger_seq
            ));
            continue;
        }
        transitions.push(outcome.clone());
        let key = (outcome.cohort_id.clone(), outcome.token.clone());
        if current
            .get(&key)
            .is_none_or(|(sequence, _)| record.ledger_seq > *sequence)
        {
            current.insert(key, (record.ledger_seq, outcome.clone()));
        }
    }
    let outcomes = current
        .into_values()
        .map(|(_, outcome)| outcome)
        .collect::<Vec<_>>();
    let mut sell_intents = BTreeMap::new();
    for outcome in &transitions {
        if let Some(intent_id) = outcome.execution_intent_id {
            if sell_intents.insert(intent_id, outcome).is_some() {
                stats.violations.push(format!(
                    "duplicate_sell_now_owner_intent:execution_intent_id={intent_id}"
                ));
            }
        }
    }
    let mut submissions = BTreeMap::<String, Vec<(u64, ConfirmedOrderSubmissionFact)>>::new();
    for record in records {
        let Some(CanonicalCtfEvent::OrderAccepted(submission)) = record.ctf_event.as_ref() else {
            continue;
        };
        if let Err(error) = super::super::validate_ctf_event_record(record) {
            stats.violations.push(format!(
                "invalid_ctf_event:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        submissions
            .entry(submission.cohort_id.clone())
            .or_default()
            .push((record.ledger_seq, submission.clone()));
    }
    let (terminal_summary, terminal_lineage) = super::order_terminal_summary::project(
        records,
        &normalized_symbols,
        &sell_intents,
        &submissions,
        &mut stats.violations,
    );
    stats.canonical.order_terminal_summary = terminal_summary;
    stats.canonical.order_execution_lineage = terminal_lineage;
    for cohort in &mut stats.closed_cohorts {
        cohort.cashflow.order_submissions = super::cohort_projection::canonical_order_submissions(
            submissions
                .remove(&cohort.paired_cohort_key)
                .unwrap_or_default(),
        );
    }
    for cohort in &mut stats.unresolved_cohorts {
        match &mut cohort.state {
            CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => {
                cashflow.order_submissions = super::cohort_projection::canonical_order_submissions(
                    submissions
                        .remove(&cohort.paired_cohort_key)
                        .unwrap_or_default(),
                );
            }
            CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { detail } => {
                stats.violations.push(format!(
                    "invalid_ctf_cohort:cohort={}:{}",
                    cohort.paired_cohort_key, detail
                ));
            }
        }
    }
    for (cohort_id, orphaned) in submissions {
        for (_, submission) in orphaned {
            if submission.buy_authorization.is_some() {
                continue;
            }
            stats.violations.push(format!(
                "order_submission_without_confirmed_split:order_id={}:cohort={cohort_id}",
                submission.order_id
            ));
        }
    }
    stats.closed_cohorts.sort_by(|left, right| {
        left.started_at_ms
            .cmp(&right.started_at_ms)
            .then_with(|| left.paired_cohort_key.cmp(&right.paired_cohort_key))
    });
    stats.unresolved_cohorts.sort_by(|left, right| {
        left.started_at_ms
            .cmp(&right.started_at_ms)
            .then_with(|| left.paired_cohort_key.cmp(&right.paired_cohort_key))
    });
    let oracle_label_by_condition =
        super::populate_causal_metrics(&mut stats, records, &transitions);
    let (exit_summary, exit_cohorts) = super::exit_projection::project(
        &normalized_symbols,
        &stats.closed_cohorts,
        &stats.unresolved_cohorts,
        &transitions,
        &stats.canonical.order_execution_lineage,
        &oracle_label_by_condition,
        &stats.canonical.ctf_summary,
        &mut stats.violations,
    );
    stats.canonical.ctf_exit_summary = exit_summary;
    stats.canonical.ctf_exit_cohorts = exit_cohorts;
    stats.canonical.owned_leg_transition_history = transitions;
    stats.owned_leg_outcomes = outcomes;
    stats
}

#[derive(Debug, Clone)]
pub(super) struct StampedFact {
    pub(super) run_id: String,
    pub(super) ledger_seq: u64,
    pub(super) ts_ms: u64,
    pub(super) fact: CanonicalCtfEvent,
}

#[derive(Debug, Default)]
pub(super) struct CohortFacts {
    pub(super) facts: Vec<StampedFact>,
}

fn quarantine_finalized_receipts<'a>(
    rejection: &IdentityRejection,
    resolution_by_condition: &mut BTreeMap<String, &'a FinalizedConditionResolutionReceipt>,
    settlement_by_tx: &mut BTreeMap<String, &'a SettlementCollateralReceipt>,
) {
    for identity in &rejection.quarantine {
        if let Some(condition_id) = identity.strip_prefix("condition_resolution:") {
            resolution_by_condition.remove(condition_id);
        }
        if let Some(tx_hash) = identity.strip_prefix("settlement:") {
            settlement_by_tx.remove(tx_hash);
        }
    }
}

fn record_identity_rejection(
    rejection: IdentityRejection,
    invalid_cohort_receipt_errors: &mut BTreeMap<String, String>,
    violations: &mut Vec<String>,
) {
    for cohort_id in rejection.affected_cohorts {
        invalid_cohort_receipt_errors
            .entry(cohort_id)
            .or_insert_with(|| rejection.error.clone());
    }
    violations.push(rejection.error);
}

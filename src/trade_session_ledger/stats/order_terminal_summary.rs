use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::orders::{OrderTerminalFact, OrderTerminalProvenance};
use crate::strategy::facts::CtfOwnedLegCloseReason;
use crate::types::{OrderCloseReason, Side};

use super::super::{ConfirmedOrderSubmissionFact, TradeLedgerRecord};
use super::{CanonicalCohortOrderIdentity, CanonicalOwnedLegOutcome};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SummaryKey {
    symbol: String,
    close_reason: OrderCloseReason,
    provenance: OrderTerminalProvenance,
    normalized_reject_kind: Option<String>,
    raw_status: Option<String>,
    raw_message_type: Option<String>,
    raw_reason: Option<String>,
    raw_reason_provenance: Option<String>,
    partial_fill: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalOrderTerminalSummaryRow {
    pub symbol: String,
    pub close_reason: OrderCloseReason,
    pub provenance: OrderTerminalProvenance,
    pub normalized_reject_kind: Option<String>,
    pub raw_status: Option<String>,
    pub raw_message_type: Option<String>,
    pub raw_reason: Option<String>,
    pub raw_reason_provenance: Option<String>,
    pub partial_fill: bool,
    pub terminal_fact_count: u64,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub instruction_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub matched_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub unfilled_qty_units_6dp: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalOrderExecutionTerminalFact {
    #[serde(
        serialize_with = "super::serialize_u64_string",
        deserialize_with = "super::deserialize_u64_string"
    )]
    pub ledger_seq: u64,
    #[serde(
        serialize_with = "super::serialize_u64_string",
        deserialize_with = "super::deserialize_u64_string"
    )]
    pub execution_intent_id: u64,
    pub order_id: Option<String>,
    pub market: String,
    pub cohort: String,
    pub symbol: String,
    pub token: String,
    pub side: Side,
    pub close_reason: OrderCloseReason,
    pub provenance: OrderTerminalProvenance,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub instruction_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub matched_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::serialize_u128_string",
        deserialize_with = "super::deserialize_u128_string"
    )]
    pub unfilled_qty_units_6dp: u128,
    pub raw_status: Option<String>,
    pub raw_message_type: Option<String>,
    pub raw_reason: Option<String>,
    pub raw_reason_provenance: Option<String>,
    pub normalized_reject_kind: Option<String>,
    #[serde(
        serialize_with = "super::serialize_u64_string",
        deserialize_with = "super::deserialize_u64_string"
    )]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalOrderExecutionLineageRow {
    pub reducer_owned_intent: CanonicalOwnedLegOutcome,
    pub accepted_submission: Option<CanonicalCohortOrderIdentity>,
    pub terminal: CanonicalOrderExecutionTerminalFact,
}

pub(super) fn project(
    records: &[TradeLedgerRecord],
    expected_symbols: &[String],
    sell_intents: &BTreeMap<u64, &CanonicalOwnedLegOutcome>,
    submissions: &BTreeMap<String, Vec<(u64, ConfirmedOrderSubmissionFact)>>,
    violations: &mut Vec<String>,
) -> (
    Vec<CanonicalOrderTerminalSummaryRow>,
    Vec<CanonicalOrderExecutionLineageRow>,
) {
    let expected_symbols = expected_symbols.iter().cloned().collect::<BTreeSet<_>>();
    let accepted_by_intent = submissions
        .values()
        .flatten()
        .map(|(ledger_seq, fact)| (fact.intent_id, (*ledger_seq, fact)))
        .collect::<BTreeMap<_, _>>();
    let mut claimed_intents = BTreeSet::new();
    let mut rows = BTreeMap::<SummaryKey, CanonicalOrderTerminalSummaryRow>::new();
    let mut lineage = Vec::new();
    for record in records {
        let Some(fact) = record.order_terminal_fact.as_ref() else {
            continue;
        };
        if let Err(error) = super::super::validate_order_terminal_record(record) {
            violations.push(format!(
                "invalid_order_terminal_fact:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        if !expected_symbols.contains(&fact.symbol) {
            violations.push(format!(
                "order_terminal_fact_foreign_symbol:ledger_seq={}:symbol={}",
                record.ledger_seq, fact.symbol
            ));
            continue;
        }
        if !claimed_intents.insert(fact.execution_intent_id) {
            violations.push(format!(
                "order_terminal_intent_identity_reused:execution_intent_id={}",
                fact.execution_intent_id
            ));
            continue;
        }
        let key = summary_key(fact);
        let row = rows.entry(key).or_insert_with(|| summary_row(fact));
        if let Some(next) = row.terminal_fact_count.checked_add(1) {
            row.terminal_fact_count = next;
        } else {
            violations.push(format!(
                "order_terminal_fact_count_overflow:symbol={}",
                fact.symbol
            ));
        }
        for (target, amount, label) in [
            (
                &mut row.instruction_qty_units_6dp,
                fact.instruction_qty_units_6dp,
                "instruction",
            ),
            (
                &mut row.matched_qty_units_6dp,
                fact.matched_qty_units_6dp,
                "matched",
            ),
            (
                &mut row.unfilled_qty_units_6dp,
                fact.unfilled_qty_units_6dp,
                "unfilled",
            ),
        ] {
            if let Some(next) = target.checked_add(amount) {
                *target = next;
            } else {
                violations.push(format!(
                    "order_terminal_{label}_quantity_overflow:symbol={}",
                    fact.symbol
                ));
            }
        }
        let Some(owner) = sell_intents.get(&fact.execution_intent_id).copied() else {
            violations.push(format!(
                "order_terminal_without_sell_now_owner_intent:ledger_seq={}:execution_intent_id={}",
                record.ledger_seq, fact.execution_intent_id
            ));
            continue;
        };
        if !owner_matches_terminal(owner, fact) {
            violations.push(format!(
                "order_terminal_owner_intent_identity_mismatch:ledger_seq={}:execution_intent_id={}",
                record.ledger_seq, fact.execution_intent_id
            ));
            continue;
        }
        let accepted = accepted_by_intent.get(&fact.execution_intent_id).copied();
        let accepted_submission = match fact.provenance {
            OrderTerminalProvenance::PreSubmissionAbandoned
            | OrderTerminalProvenance::SubmissionRejected => {
                if accepted.is_some() {
                    violations.push(format!(
                        "rejected_order_terminal_has_accepted_submission:ledger_seq={}:execution_intent_id={}",
                        record.ledger_seq, fact.execution_intent_id
                    ));
                    continue;
                }
                None
            }
            OrderTerminalProvenance::SubmissionUnmatched | OrderTerminalProvenance::OrderUpdate => {
                match accepted {
                    None if fact.provenance == OrderTerminalProvenance::OrderUpdate
                        && fact.raw_status.as_deref().is_some_and(|status| {
                            status.trim().eq_ignore_ascii_case("rejected")
                        }) =>
                    {
                        None
                    }
                    None => {
                        violations.push(format!(
                            "accepted_order_terminal_without_submission:ledger_seq={}:execution_intent_id={}",
                            record.ledger_seq, fact.execution_intent_id
                        ));
                        continue;
                    }
                    Some((submission_ledger_seq, submission)) => {
                        if !submission_matches_terminal(owner, submission, fact) {
                            violations.push(format!(
                                "order_terminal_submission_identity_mismatch:ledger_seq={}:execution_intent_id={}",
                                record.ledger_seq, fact.execution_intent_id
                            ));
                            continue;
                        }
                        Some(canonical_submission(submission_ledger_seq, submission))
                    }
                }
            }
        };
        lineage.push(CanonicalOrderExecutionLineageRow {
            reducer_owned_intent: owner.clone(),
            accepted_submission,
            terminal: canonical_terminal(record.ledger_seq, fact),
        });
    }
    lineage.sort_by(|left, right| {
        left.reducer_owned_intent
            .symbol
            .cmp(&right.reducer_owned_intent.symbol)
            .then_with(|| {
                left.reducer_owned_intent
                    .cohort_id
                    .cmp(&right.reducer_owned_intent.cohort_id)
            })
            .then_with(|| {
                left.reducer_owned_intent
                    .ledger_seq
                    .cmp(&right.reducer_owned_intent.ledger_seq)
            })
            .then_with(|| {
                left.terminal
                    .execution_intent_id
                    .cmp(&right.terminal.execution_intent_id)
            })
    });
    (rows.into_values().collect(), lineage)
}

fn owner_matches_terminal(owner: &CanonicalOwnedLegOutcome, fact: &OrderTerminalFact) -> bool {
    let close_reason = CtfOwnedLegCloseReason::try_from(fact.close_reason).ok();
    owner.symbol == fact.symbol
        && owner.cohort_id == fact.cohort
        && owner.market_id.as_str() == fact.market
        && owner.token == fact.token
        && owner.close_reason == close_reason
        && fact.side == Side::Sell
}

fn submission_matches_terminal(
    owner: &CanonicalOwnedLegOutcome,
    submission: &ConfirmedOrderSubmissionFact,
    fact: &OrderTerminalFact,
) -> bool {
    submission.intent_id == fact.execution_intent_id
        && fact.order_id.as_deref() == Some(submission.order_id.as_str())
        && submission.underlying == owner.symbol
        && submission.market_id == owner.market_id
        && submission.cohort_id == owner.cohort_id
        && submission.asset_id == owner.token
}

fn canonical_submission(
    ledger_seq: u64,
    submission: &ConfirmedOrderSubmissionFact,
) -> CanonicalCohortOrderIdentity {
    CanonicalCohortOrderIdentity {
        ledger_seq,
        order_id: submission.order_id.clone(),
        intent_id: submission.intent_id,
        asset_id: submission.asset_id.clone(),
        instruction_qty_units_6dp: submission.instruction_qty_units_6dp,
        instruction_limit_px_units_8dp: submission.instruction_limit_px_units_8dp,
        time_in_force: submission.time_in_force.clone(),
        post_only: submission.post_only,
        accepted_at_ms: submission.accepted_at_ms,
    }
}

fn canonical_terminal(
    ledger_seq: u64,
    fact: &OrderTerminalFact,
) -> CanonicalOrderExecutionTerminalFact {
    CanonicalOrderExecutionTerminalFact {
        ledger_seq,
        execution_intent_id: fact.execution_intent_id,
        order_id: fact.order_id.clone(),
        market: fact.market.clone(),
        cohort: fact.cohort.clone(),
        symbol: fact.symbol.clone(),
        token: fact.token.clone(),
        side: fact.side,
        close_reason: fact.close_reason,
        provenance: fact.provenance,
        instruction_qty_units_6dp: fact.instruction_qty_units_6dp,
        matched_qty_units_6dp: fact.matched_qty_units_6dp,
        unfilled_qty_units_6dp: fact.unfilled_qty_units_6dp,
        raw_status: fact.raw_status.clone(),
        raw_message_type: fact.raw_message_type.clone(),
        raw_reason: fact.raw_reason.clone(),
        raw_reason_provenance: fact.raw_reason_provenance.clone(),
        normalized_reject_kind: fact.normalized_reject_kind.clone(),
        observed_at_ms: fact.observed_at_ms,
    }
}

fn summary_key(fact: &OrderTerminalFact) -> SummaryKey {
    SummaryKey {
        symbol: fact.symbol.clone(),
        close_reason: fact.close_reason,
        provenance: fact.provenance,
        normalized_reject_kind: fact.normalized_reject_kind.clone(),
        raw_status: fact.raw_status.clone(),
        raw_message_type: fact.raw_message_type.clone(),
        raw_reason: fact.raw_reason.clone(),
        raw_reason_provenance: fact.raw_reason_provenance.clone(),
        partial_fill: fact.matched_qty_units_6dp > 0 && fact.unfilled_qty_units_6dp > 0,
    }
}

fn summary_row(fact: &OrderTerminalFact) -> CanonicalOrderTerminalSummaryRow {
    CanonicalOrderTerminalSummaryRow {
        symbol: fact.symbol.clone(),
        close_reason: fact.close_reason,
        provenance: fact.provenance,
        normalized_reject_kind: fact.normalized_reject_kind.clone(),
        raw_status: fact.raw_status.clone(),
        raw_message_type: fact.raw_message_type.clone(),
        raw_reason: fact.raw_reason.clone(),
        raw_reason_provenance: fact.raw_reason_provenance.clone(),
        partial_fill: fact.matched_qty_units_6dp > 0 && fact.unfilled_qty_units_6dp > 0,
        terminal_fact_count: 0,
        instruction_qty_units_6dp: 0,
        matched_qty_units_6dp: 0,
        unfilled_qty_units_6dp: 0,
    }
}

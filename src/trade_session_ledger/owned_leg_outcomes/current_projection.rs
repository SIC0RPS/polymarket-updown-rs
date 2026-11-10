use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Result};

use crate::ctf_accounting::CtfMarketEntryCashflow;
use crate::strategy::facts::{CtfLegEconomicOutcome, CtfOwnedLegCloseReason};
use crate::strategy::hold_to_resolution::PairTokenSide;

use super::{CanonicalOwnedLegOutcome, TradeLedgerRecord};
use crate::trade_session_ledger::CanonicalCtfEvent;

#[path = "current_projection/unresolved.rs"]
mod unresolved;

struct EntryValidationEvidence {
    ts_ms: u64,
    buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    up_token: String,
    down_token: String,
}

struct AcceptedOrderValidationEvidence {
    underlying: String,
    market_id: crate::polymarket::identity::MarketId,
    cohort_id: String,
    tokens: Vec<String>,
}

#[derive(Default)]
pub(in crate::trade_session_ledger) struct OwnedLegValidationProjection {
    entries: BTreeMap<
        (String, crate::polymarket::identity::MarketId, String),
        Vec<EntryValidationEvidence>,
    >,
    outcome_intents: BTreeMap<u64, Vec<u64>>,
    accepted_orders: BTreeMap<u64, Vec<AcceptedOrderValidationEvidence>>,
}

impl OwnedLegValidationProjection {
    pub(in crate::trade_session_ledger) fn from_records<'a>(
        records: impl IntoIterator<Item = &'a TradeLedgerRecord>,
    ) -> Self {
        let mut projection = Self::default();
        for record in records {
            projection.insert(record);
        }
        projection
    }

    pub(in crate::trade_session_ledger) fn insert(&mut self, record: &TradeLedgerRecord) {
        if let Some(CanonicalCtfEvent::SplitConfirmed(split)) = record.ctf_event.as_ref() {
            self.entries
                .entry((
                    split.underlying.clone(),
                    split.market_id.clone(),
                    split.cohort_id.clone(),
                ))
                .or_default()
                .push(EntryValidationEvidence {
                    ts_ms: record.ts_ms,
                    buy_authorization: None,
                    up_token: split.up_token.clone(),
                    down_token: split.down_token.clone(),
                });
        }
        if let Some(CanonicalCtfEvent::BuyConfirmed(buy)) = record.ctf_event.as_ref() {
            let entries = self
                .entries
                .entry((
                    buy.authorization.symbol.as_str().to_string(),
                    buy.market_id.clone(),
                    buy.cohort_id.clone(),
                ))
                .or_default();
            if let Some(entry) = entries
                .iter_mut()
                .find(|entry| entry.buy_authorization == Some(buy.authorization))
            {
                entry.ts_ms = entry.ts_ms.min(record.ts_ms);
            } else {
                entries.push(EntryValidationEvidence {
                    ts_ms: record.ts_ms,
                    buy_authorization: Some(buy.authorization),
                    up_token: buy.authorization.up_token.to_string(),
                    down_token: buy.authorization.down_token.to_string(),
                });
            }
        }
        if let Some(prior) = record.owned_leg_outcome.as_ref() {
            if let Some(execution_intent_id) = prior.execution_intent_id {
                self.outcome_intents
                    .entry(execution_intent_id)
                    .or_default()
                    .push(record.ledger_seq);
            }
        }
        if let Some(CanonicalCtfEvent::OrderAccepted(order)) = record.ctf_event.as_ref() {
            self.accepted_orders
                .entry(order.intent_id)
                .or_default()
                .push(AcceptedOrderValidationEvidence {
                    underlying: order.underlying.clone(),
                    market_id: order.market_id.clone(),
                    cohort_id: order.cohort_id.clone(),
                    tokens: record.tokens.clone(),
                });
        }
    }

    pub(in crate::trade_session_ledger) fn validate(
        &self,
        outcome: &CanonicalOwnedLegOutcome,
    ) -> Result<()> {
        super::validate_owner_decision(
            outcome.owned_qty_units_6dp,
            outcome.action_qty_units_6dp,
            outcome.execution_intent_id,
            outcome.execution_evidence_revision,
            outcome.causal_winner_side,
            outcome.post_sell_full_entry_pnl_bits,
            outcome.post_hold_full_entry_pnl_bits,
            outcome.post_fillable_qty_bits,
            outcome.outcome,
            outcome.reason,
            outcome.close_reason,
        )?;
        ensure!(
            matches!(outcome.symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP")
                && !outcome.cohort_id.trim().is_empty()
                && outcome.ledger_seq > 0
                && outcome.ts_ms > 0
                && !outcome.market_id.as_str().trim().is_empty()
                && !outcome.token.trim().is_empty(),
            "canonical owned-leg outcome identity is incomplete"
        );
        let entry_key = (
            outcome.symbol.clone(),
            outcome.market_id.clone(),
            outcome.cohort_id.clone(),
        );
        let matching_entries = self.entries.get(&entry_key);
        let matching_entry_count = matching_entries.map_or(0, Vec::len);
        ensure!(
            matching_entry_count == 1,
            "canonical owned-leg outcome requires exactly one confirmed acquisition symbol={} market_id={} cohort_id={} count={}",
            outcome.symbol,
            outcome.market_id,
            outcome.cohort_id,
            matching_entry_count
        );
        let entry = &matching_entries.expect("one matching acquisition was counted")[0];
        ensure!(
            outcome.token == entry.up_token || outcome.token == entry.down_token,
            "canonical owned-leg outcome token conflicts with confirmed acquisition cohort={} token={}",
            outcome.cohort_id,
            outcome.token
        );
        if let Some(authorization) = entry.buy_authorization {
            let held_token = if authorization.selected_up {
                &entry.up_token
            } else {
                &entry.down_token
            };
            ensure!(
                outcome.token == *held_token
                    && outcome.close_reason != Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
                    && outcome.first_instruction_economics.is_none(),
                "BUY owned-leg outcome conflicts with confirmed acquisition cohort={}",
                outcome.cohort_id
            );
        }
        if outcome.close_reason == Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) {
            let causal_winner_side = outcome.causal_winner_side.ok_or_else(|| {
                anyhow::anyhow!(
                    "first-managed-sell owned-leg outcome omitted causal winner side cohort={}",
                    outcome.cohort_id
                )
            })?;
            let expected_loser_token = match causal_winner_side {
                PairTokenSide::Up => entry.down_token.as_str(),
                PairTokenSide::Down => entry.up_token.as_str(),
            };
            ensure!(
                outcome.token == expected_loser_token,
                "first-managed-sell token is not opposite the causal winner cohort={} token={} expected_loser_token={expected_loser_token}",
                outcome.cohort_id,
                outcome.token
            );
        }
        ensure!(
            outcome.ts_ms >= entry.ts_ms,
            "canonical owned-leg outcome precedes confirmed acquisition cohort={}",
            outcome.cohort_id
        );
        let competing_intent_outcome =
            outcome.execution_intent_id.and_then(|execution_intent_id| {
                self.outcome_intents
                    .get(&execution_intent_id)?
                    .iter()
                    .copied()
                    .find(|ledger_seq| *ledger_seq != outcome.ledger_seq)
            });
        ensure!(
            competing_intent_outcome.is_none(),
            "owned-leg execution intent identity reused intent_id={} first_ledger_seq={} next_ledger_seq={}",
            outcome.execution_intent_id.unwrap_or(0),
            competing_intent_outcome.unwrap_or(0),
            outcome.ledger_seq
        );
        if let Some(execution_intent_id) = outcome.execution_intent_id {
            let accepted = self.accepted_orders.get(&execution_intent_id);
            let accepted_order_count = accepted.map_or(0, Vec::len);
            ensure!(
                accepted_order_count <= 1,
                "owned-leg SELL_NOW execution intent has duplicate accepted orders intent_id={execution_intent_id} count={}",
                accepted_order_count
            );
            if let Some(order) = accepted.and_then(|orders| orders.first()) {
                ensure!(
                    order.underlying == outcome.symbol
                        && order.market_id == outcome.market_id
                        && order.cohort_id == outcome.cohort_id
                        && order.tokens.as_slice() == [outcome.token.as_str()],
                    "owned-leg SELL_NOW execution intent conflicts with accepted order intent_id={execution_intent_id}"
                );
            }
        }
        Ok(())
    }
}

pub(in crate::trade_session_ledger) fn validate_joined_owned_leg_outcome<'a>(
    outcome: &CanonicalOwnedLegOutcome,
    records: impl IntoIterator<Item = &'a TradeLedgerRecord>,
) -> Result<()> {
    OwnedLegValidationProjection::from_records(records).validate(outcome)
}

fn owned_leg_outcome_identity(outcome: &CanonicalOwnedLegOutcome) -> String {
    format!(
        "owned_leg:{}:{}:{}:{}:{}",
        outcome.symbol, outcome.cohort_id, outcome.market_id, outcome.token, outcome.ledger_seq
    )
}

struct ManagedSellReduction {
    buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    market_id: crate::polymarket::identity::MarketId,
    condition_id: String,
    cashflow: CtfMarketEntryCashflow,
}

pub(in crate::trade_session_ledger) fn project_owned_leg_outcomes(
    records: &[TradeLedgerRecord],
    violations: &mut Vec<String>,
) -> (Vec<CanonicalOwnedLegOutcome>, Vec<CanonicalOwnedLegOutcome>) {
    let mut identities = BTreeSet::new();
    let mut transitions = Vec::new();
    let mut current = BTreeMap::<(String, String), (u64, CanonicalOwnedLegOutcome)>::new();
    let validation = OwnedLegValidationProjection::from_records(records);
    for record in records {
        if record.owned_leg_outcome.is_none() && record.event_kind != "owned_leg_outcome" {
            continue;
        }
        if let Err(error) =
            super::super::validation::validate_owned_leg_outcome_record_against(record, &validation)
        {
            violations.push(format!(
                "invalid_owned_leg_outcome:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        let Some(outcome) = record.owned_leg_outcome.as_ref() else {
            continue;
        };
        let identity = owned_leg_outcome_identity(outcome);
        if !identities.insert(identity.clone()) {
            violations.push(format!("duplicate_owned_leg_outcome:{identity}"));
            continue;
        }
        transitions.push(outcome.clone());
        let key = (outcome.cohort_id.clone(), outcome.token.clone());
        match current.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((record.ledger_seq, outcome.clone()));
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if record.ledger_seq > entry.get().0 {
                    entry.insert((record.ledger_seq, outcome.clone()));
                }
            }
        }
    }

    let mut expected = BTreeSet::new();
    let mut reductions = BTreeMap::<String, ManagedSellReduction>::new();
    let mut invalid_reductions = BTreeSet::new();
    for record in records {
        let Some(CanonicalCtfEvent::SplitConfirmed(split)) = record.ctf_event.as_ref() else {
            continue;
        };
        if super::super::validate_ctf_event_record(record).is_err() {
            continue;
        }
        expected.insert((split.cohort_id.clone(), split.up_token.clone()));
        expected.insert((split.cohort_id.clone(), split.down_token.clone()));
        let (Some(up_weight), Some(down_weight)) = (
            split.up_cost_basis_weight_units_8dp,
            split.down_cost_basis_weight_units_8dp,
        ) else {
            continue;
        };
        let reduction = CtfMarketEntryCashflow::new(
            split.cohort_id.clone(),
            split.up_token.clone(),
            split.up_qty_units_6dp,
            split.down_token.clone(),
            split.down_qty_units_6dp,
            split.collateral_debit_units_6dp,
            split.fee_units_6dp,
            split.approved_direct_cost_units_6dp,
            up_weight,
            down_weight,
        );
        match reduction {
            Ok(cashflow) if !reductions.contains_key(&split.cohort_id) => {
                reductions.insert(
                    split.cohort_id.clone(),
                    ManagedSellReduction {
                        buy_authorization: None,
                        market_id: split.market_id.clone(),
                        condition_id: split.condition_id.clone(),
                        cashflow,
                    },
                );
            }
            Ok(_) => {
                invalid_reductions.insert(split.cohort_id.clone());
                violations.push(format!(
                    "current_owned_leg_receipt_join_duplicate_split:cohort={}",
                    split.cohort_id
                ));
            }
            Err(error) => {
                invalid_reductions.insert(split.cohort_id.clone());
                violations.push(format!(
                    "current_owned_leg_receipt_reduction_failed:cohort={}:{}",
                    split.cohort_id, error
                ));
            }
        }
    }

    let mut terminal_receipt_intents = BTreeMap::<(String, String), BTreeSet<u64>>::new();
    for record in records {
        if let Some(CanonicalCtfEvent::BuyConfirmed(buy)) = record.ctf_event.as_ref() {
            if invalid_reductions.contains(&buy.cohort_id) {
                continue;
            }
            let acquired = (|| -> Result<bool> {
                super::super::validate_ctf_event_record(record)?;
                match reductions.entry(buy.cohort_id.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(ManagedSellReduction {
                            buy_authorization: Some(buy.authorization),
                            market_id: buy.market_id.clone(),
                            condition_id: buy.condition_id.clone(),
                            cashflow: CtfMarketEntryCashflow::new_buy(buy)?,
                        });
                        Ok(true)
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let reduction = entry.get_mut();
                        ensure!(
                            reduction.buy_authorization == Some(buy.authorization)
                                && reduction.market_id == buy.market_id
                                && reduction.condition_id == buy.condition_id,
                            "BUY receipt conflicts with original acquisition"
                        );
                        let before = reduction.cashflow.summary()?.created_qty_by_token[&buy.token];
                        reduction.cashflow.confirm_buy(buy)?;
                        Ok(reduction.cashflow.summary()?.created_qty_by_token[&buy.token] > before)
                    }
                }
            })();
            match acquired {
                Ok(added) => {
                    let key = (buy.cohort_id.clone(), buy.token.clone());
                    if added {
                        terminal_receipt_intents.remove(&key);
                    }
                    if current.contains_key(&key) {
                        expected.insert(key);
                    }
                }
                Err(error) => {
                    invalid_reductions.insert(buy.cohort_id.clone());
                    violations.push(format!(
                        "current_owned_leg_receipt_reduction_failed:ledger_seq={}:cohort={}:{}",
                        record.ledger_seq, buy.cohort_id, error
                    ));
                }
            }
            continue;
        }
        let Some(CanonicalCtfEvent::TradeConfirmed(sell)) = record.ctf_event.as_ref() else {
            continue;
        };
        if invalid_reductions.contains(&sell.cohort_id) {
            continue;
        }
        if let Err(error) = super::super::validate_ctf_event_record(record) {
            invalid_reductions.insert(sell.cohort_id.clone());
            violations.push(format!(
                "current_owned_leg_receipt_invalid:ledger_seq={}:{}",
                record.ledger_seq, error
            ));
            continue;
        }
        let Some(reduction) = reductions.get_mut(&sell.cohort_id) else {
            continue;
        };
        if reduction.buy_authorization.is_some() {
            expected.insert((sell.cohort_id.clone(), sell.token.clone()));
        }
        if sell.market_id != reduction.market_id || sell.condition_id != reduction.condition_id {
            invalid_reductions.insert(sell.cohort_id.clone());
            violations.push(format!(
                "current_owned_leg_receipt_split_identity_mismatch:ledger_seq={}:cohort={}",
                record.ledger_seq, sell.cohort_id
            ));
            continue;
        }
        if let Err(error) = reduction.cashflow.confirm_sell(
            sell.receipt_id.clone(),
            sell.token.clone(),
            sell.qty_units_6dp,
            sell.gross_proceeds_units_6dp,
            sell.fee_units_6dp,
            sell.approved_direct_cost_units_6dp,
        ) {
            invalid_reductions.insert(sell.cohort_id.clone());
            violations.push(format!(
                "current_owned_leg_receipt_reduction_failed:ledger_seq={}:cohort={}:{}",
                record.ledger_seq, sell.cohort_id, error
            ));
            continue;
        }
        let Ok(summary) = reduction.cashflow.summary() else {
            invalid_reductions.insert(sell.cohort_id.clone());
            violations.push(format!(
                "current_owned_leg_receipt_summary_failed:cohort={}",
                sell.cohort_id
            ));
            continue;
        };
        if summary
            .remaining_qty_by_token
            .get(&sell.token)
            .copied()
            .unwrap_or(0)
            == 0
        {
            terminal_receipt_intents
                .entry((sell.cohort_id.clone(), sell.token.clone()))
                .or_default()
                .insert(sell.intent_id);
        }
    }

    for (cohort_id, token) in &expected {
        let key = (cohort_id.clone(), token.clone());
        if !current.contains_key(&key) {
            violations.push(format!(
                "missing_current_owned_leg_outcome:cohort={cohort_id}:token={token}"
            ));
            continue;
        }
        if invalid_reductions.contains(cohort_id) {
            continue;
        }
        let Some(reduction) = reductions.get(cohort_id) else {
            continue;
        };
        let Ok(summary) = reduction.cashflow.summary() else {
            violations.push(format!(
                "current_owned_leg_receipt_summary_failed:cohort={cohort_id}"
            ));
            continue;
        };
        if summary
            .remaining_qty_by_token
            .get(token)
            .copied()
            .unwrap_or(0)
            != 0
        {
            continue;
        }
        let intent_ids = terminal_receipt_intents
            .get(&key)
            .cloned()
            .unwrap_or_default();
        if intent_ids.len() != 1 {
            violations.push(format!(
                "current_owned_leg_receipt_intent_not_unique:cohort={cohort_id}:token={token}:count={}",
                intent_ids.len()
            ));
            continue;
        }
        let candidates = transitions
            .iter()
            .filter(|outcome| {
                outcome.cohort_id == *cohort_id
                    && outcome.token == *token
                    && outcome.outcome == CtfLegEconomicOutcome::SellNow
                    && outcome
                        .execution_intent_id
                        .is_some_and(|intent_id| intent_ids.contains(&intent_id))
            })
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            violations.push(format!(
                "current_owned_leg_receipt_owner_not_unique:cohort={cohort_id}:token={token}:count={}",
                candidates.len()
            ));
            continue;
        }
        let selected = candidates[0];
        current.insert(key, (selected.ledger_seq, selected.clone()));
    }

    unresolved::validate(
        &reductions,
        &invalid_reductions,
        &transitions,
        &current,
        violations,
    );

    (
        transitions,
        current.into_values().map(|(_, outcome)| outcome).collect(),
    )
}

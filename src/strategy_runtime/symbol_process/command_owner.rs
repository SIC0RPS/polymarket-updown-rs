use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::oracle_resolution::OracleResolvedLabel;
use crate::strategy::facts::CtfOwnedLegCloseReason;
use crate::strategy::StrategyAction;
use crate::trade_session_ledger::UnjoinedOwnedLegOutcome;
use crate::types::{OrderCloseReason, OrderIntent};

use super::{DecodedEntryContext, DecodedIntent, SymbolId};

#[must_use = "runtime commands, owner outcomes, and oracle labels must be accepted atomically"]
pub(crate) struct WorkerCommandBatch {
    source_symbol: SymbolId,
    worker_epoch: u32,
    decision_ts_ms: u64,
    intents: Vec<DecodedIntent>,
    owned_leg_outcomes: Vec<UnjoinedOwnedLegOutcome>,
    oracle_labels: Vec<OracleResolvedLabel>,
}

impl WorkerCommandBatch {
    pub(crate) fn new(
        source_symbol: SymbolId,
        worker_epoch: u32,
        decision_ts_ms: u64,
        intents: Vec<DecodedIntent>,
        owned_leg_outcomes: Vec<UnjoinedOwnedLegOutcome>,
        oracle_labels: Vec<OracleResolvedLabel>,
    ) -> Self {
        Self {
            source_symbol,
            worker_epoch,
            decision_ts_ms,
            intents,
            owned_leg_outcomes,
            oracle_labels,
        }
    }
}

struct AcceptedWorkerCommandBatch {
    intents: Vec<DecodedIntent>,
    owned_leg_outcomes: Vec<UnjoinedOwnedLegOutcome>,
    oracle_labels: Vec<OracleResolvedLabel>,
}

pub(crate) struct CentralWorkerCommandOwner {
    next_execution_intent_id: u64,
}

impl CentralWorkerCommandOwner {
    pub(crate) fn new(next_execution_intent_id: u64) -> Result<Self> {
        anyhow::ensure!(
            next_execution_intent_id > 0,
            "central execution intent identity starts at zero"
        );
        Ok(Self {
            next_execution_intent_id,
        })
    }

    pub(crate) fn accept_and_retain(
        &mut self,
        batch: WorkerCommandBatch,
        retained_owned_leg_outcomes: &mut Vec<UnjoinedOwnedLegOutcome>,
        retained_oracle_labels: &mut Vec<OracleResolvedLabel>,
    ) -> Result<Vec<DecodedIntent>> {
        let mut accepted = self.accept(batch)?;
        retained_owned_leg_outcomes.append(&mut accepted.owned_leg_outcomes);
        retained_oracle_labels.append(&mut accepted.oracle_labels);
        Ok(accepted.intents)
    }

    fn accept(&mut self, mut batch: WorkerCommandBatch) -> Result<AcceptedWorkerCommandBatch> {
        anyhow::ensure!(
            batch.worker_epoch > 0 && batch.decision_ts_ms > 0,
            "runtime command batch has zero epoch symbol={}",
            batch.source_symbol.as_str()
        );

        let mut sell_outcomes_by_local_intent = BTreeMap::new();
        for (index, outcome) in batch.owned_leg_outcomes.iter().enumerate() {
            let Some((symbol, worker_epoch, local_intent_id)) =
                outcome.execution_intent_source_identity()?
            else {
                continue;
            };
            anyhow::ensure!(
                symbol == batch.source_symbol.as_str() && worker_epoch == batch.worker_epoch,
                "SELL_NOW owner outcome is foreign to runtime command batch expected_symbol={} actual_symbol={} expected_epoch={} actual_epoch={worker_epoch}",
                batch.source_symbol.as_str(),
                symbol,
                batch.worker_epoch
            );
            anyhow::ensure!(
                sell_outcomes_by_local_intent
                    .insert(local_intent_id, index)
                    .is_none(),
                "runtime command batch reused SELL_NOW source intent identity symbol={} epoch={} local_intent_id={local_intent_id}",
                batch.source_symbol.as_str(),
                batch.worker_epoch
            );
        }

        let mut next_execution_intent_id = self.next_execution_intent_id;
        for decoded in &mut batch.intents {
            anyhow::ensure!(
                decoded.source_symbol == batch.source_symbol
                    && decoded.worker_epoch == batch.worker_epoch
                    && decoded.intent_id > 0,
                "decoded intent is foreign to runtime command batch symbol={} epoch={} local_intent_id={}",
                batch.source_symbol.as_str(),
                batch.worker_epoch,
                decoded.intent_id
            );
            let is_split = matches!(
                &decoded.action,
                StrategyAction::Inventory(intent)
                    if intent.operation == crate::strategy::InventoryOperation::SplitCompleteSet
            );
            anyhow::ensure!(
                is_split == matches!(&decoded.entry_context, Some(DecodedEntryContext::Split(_)))
                    && matches!(&decoded.action, StrategyAction::Order(OrderIntent::Buy(_)))
                        == matches!(&decoded.entry_context, Some(DecodedEntryContext::DirectBuy(_))),
                "central runtime command split context binding mismatch symbol={} epoch={} local_intent_id={}",
                batch.source_symbol.as_str(),
                batch.worker_epoch,
                decoded.intent_id
            );
            let StrategyAction::Order(order) = &mut decoded.action else {
                continue;
            };
            anyhow::ensure!(
                order.common().execution_intent_id == Some(decoded.intent_id),
                "decoded order lost symbol-local execution identity symbol={} epoch={} local_intent_id={}",
                batch.source_symbol.as_str(),
                batch.worker_epoch,
                decoded.intent_id
            );
            if let OrderIntent::Buy(buy) = order {
                let Some(DecodedEntryContext::DirectBuy(authorization)) = &decoded.entry_context
                else {
                    anyhow::bail!("central BUY lost same-batch entry context");
                };
                anyhow::ensure!(
                    authorization.intent_id == decoded.intent_id
                        && buy.authorization.is_none()
                        && !sell_outcomes_by_local_intent.contains_key(&decoded.intent_id),
                    "central BUY carries conflicting local ownership"
                );
                buy.authorization = Some(*authorization);
                let central_intent_id = next_execution_intent_id;
                next_execution_intent_id = next_execution_intent_id
                    .checked_add(1)
                    .context("central execution intent identity overflow")?;
                crate::ctf_inventory::CtfInventoryRuntime::bind_direct_buy_order(
                    order,
                    batch.source_symbol,
                    central_intent_id,
                    batch.decision_ts_ms,
                )?;
                decoded.entry_context = Some(DecodedEntryContext::DirectBuy(
                    *order.buy_authorization().expect("bound BUY authorization"),
                ));
                continue;
            }
            let outcome_index = sell_outcomes_by_local_intent
                .remove(&decoded.intent_id)
                .with_context(|| {
                    format!(
                        "central order has no same-batch SELL_NOW owner outcome symbol={} epoch={} local_intent_id={}",
                        batch.source_symbol.as_str(),
                        batch.worker_epoch,
                        decoded.intent_id
                    )
                })?;
            let outcome = &batch.owned_leg_outcomes[outcome_index];
            let OrderIntent::Sell(sell) = order else {
                anyhow::bail!("same-batch SELL_NOW owner outcome is bound to a BUY order");
            };
            anyhow::ensure!(
                sell.fields.market_id.as_ref() == Some(&outcome.market_id)
                    && sell.fields.lifecycle_cohort.as_deref() == Some(outcome.cohort_id.as_str())
                    && sell.fields.instrument == outcome.token
                    && CtfOwnedLegCloseReason::try_from(sell.close_reason)
                        .ok()
                        .is_some_and(|reason| outcome.close_reason == Some(reason))
                    && sell.fields.execution_instruction_qty_units_6dp == outcome.action_qty_units_6dp
                    && Some(sell.fields.execution_evidence_revision)
                        == outcome.execution_evidence_revision
                    && outcome.action_qty_units_6dp > 0
                    && outcome.action_qty_units_6dp <= outcome.owned_qty_units_6dp,
                "central order differs from same-batch SELL_NOW owner outcome symbol={} epoch={} local_intent_id={}",
                batch.source_symbol.as_str(),
                batch.worker_epoch,
                decoded.intent_id
            );
            if sell.close_reason == OrderCloseReason::PairedFirstManagedSell {
                anyhow::ensure!(
                    sell.first_target_qty_units_6dp
                        .is_none_or(|target| target == outcome.owned_qty_units_6dp),
                    "central First target differs from same-batch SELL_NOW owner outcome"
                );
                sell.first_target_qty_units_6dp = Some(outcome.owned_qty_units_6dp);
            }
            let central_intent_id = next_execution_intent_id;
            next_execution_intent_id = next_execution_intent_id
                .checked_add(1)
                .context("central execution intent identity overflow")?;
            order.common_mut().execution_intent_id = Some(central_intent_id);
            batch.owned_leg_outcomes[outcome_index].bind_central_execution_intent_id(
                batch.source_symbol.as_str(),
                batch.worker_epoch,
                decoded.intent_id,
                central_intent_id,
            )?;
        }
        anyhow::ensure!(
            sell_outcomes_by_local_intent.is_empty(),
            "SELL_NOW owner outcome has no same-batch central order symbol={} epoch={} count={}",
            batch.source_symbol.as_str(),
            batch.worker_epoch,
            sell_outcomes_by_local_intent.len()
        );

        self.next_execution_intent_id = next_execution_intent_id;
        Ok(AcceptedWorkerCommandBatch {
            intents: batch.intents,
            owned_leg_outcomes: batch.owned_leg_outcomes,
            oracle_labels: batch.oracle_labels,
        })
    }
}

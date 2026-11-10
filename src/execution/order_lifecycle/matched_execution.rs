use std::collections::{HashMap, HashSet};

use anyhow::Context as _;

use crate::orders::{OpenOrdersState, OrderLifecycleKey, OrderTerminalFact};
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::order_context::OrderRuntimeMetadata;
use crate::portfolio::{FillEvent, FillSide, PortfolioState};
use crate::trade_session_ledger::{CtfTradeStatus, ManagedSellReceiptIdentity};
use crate::types::{DirectBuyAuthorization, OrderCloseReason, OrderIntent};

use super::ctf_market_entry_sell::{key_and_intent_for_fill, key_and_intent_for_observation};
use super::direct_buy::published_buy_quantity;
use super::is_ctf_market_entry_sell_key;
use super::OrderLifecycleSupervisor;

type ExecutionIdentity = (String, String);
type ExecutionScope = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
enum MatchedExecutionScope {
    Sell(OrderLifecycleKey),
    Buy(DirectBuyAuthorization),
}

impl MatchedExecutionScope {
    fn sell_key(&self) -> Option<&OrderLifecycleKey> {
        match self {
            Self::Sell(key) => Some(key),
            Self::Buy(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatchedExecutionFact {
    scope: MatchedExecutionScope,
    execution_intent_id: u64,
    qty_units_6dp: u128,
    gross_units_6dp: Option<u128>,
    fee_units_6dp: Option<u128>,
}

#[derive(Debug)]
struct FirstExecutionTarget {
    key: OrderLifecycleKey,
    qty_units_6dp: u128,
    confirmed_qty_units_6dp: Option<u128>,
}

#[derive(Debug, Default)]
pub(super) struct MatchedExecutionState {
    facts: HashMap<ExecutionIdentity, MatchedExecutionFact>,
    settled: HashSet<ExecutionIdentity>,
    failed: HashSet<ExecutionIdentity>,
    first_targets: HashMap<ExecutionScope, FirstExecutionTarget>,
    portfolio_revision: Option<u64>,
}

impl OrderLifecycleSupervisor {
    pub(crate) fn reconcile_terminal_no_fills(
        &mut self,
        portfolio: &PortfolioState,
        risk: &crate::risk::RiskHandle,
        open_orders: &mut OpenOrdersState,
        terminal_audits: &mut Vec<OrderTerminalFact>,
        now_ms: u64,
    ) -> anyhow::Result<bool> {
        let mut changed = if open_orders.has_pending_confirmed_cancel() {
            self.consume_confirmed_cancels(open_orders, terminal_audits, now_ms)?
        } else {
            false
        };
        changed |= self
            .matched_execution
            .reconcile(portfolio, open_orders, now_ms);
        let completed = open_orders
            .active_order_lifecycles
            .iter()
            .filter_map(|(key, active)| {
                if !is_ctf_market_entry_sell_key(key) {
                    return None;
                }
                if active.terminal_no_fill {
                    return (!portfolio.has_matching_ctf_split_lot(&key.token, &key.cohort))
                        .then(|| (key.clone(), HashSet::new()));
                }
                if active.matched_execution_qty_units_6dp > 0 {
                    return None;
                }
                let intent_id = active.execution_intent_id.filter(|id| *id > 0)?;
                let expected = match active.order_id.as_deref() {
                    Some(_) => active.instruction_qty_units_6dp,
                    None => active.terminal_matched_qty_units_6dp?,
                };
                if expected == 0 || expected > active.instruction_qty_units_6dp {
                    return None;
                }
                let mut order_ids = HashSet::new();
                let confirmed = portfolio
                    .confirmed_lifecycle_sell_fills
                    .values()
                    .filter(|fill| {
                        fill.intent_id == intent_id
                            && fill.market_id.as_str() == key.market
                            && fill.lifecycle_cohort == key.cohort
                            && fill.token == key.token
                            && fill.close_reason == key.close_reason
                            && active
                                .order_id
                                .as_deref()
                                .is_none_or(|order_id| fill.order_id == order_id)
                    })
                    .try_fold(0_u128, |sum, fill| {
                        fill.receipt_identity.as_ref()?;
                        order_ids.insert(fill.order_id.clone());
                        sum.checked_add(fill.qty_units_6dp)
                    })?;
                (confirmed == expected).then(|| (key.clone(), order_ids))
            })
            .collect::<Vec<_>>();
        changed |= !completed.is_empty();
        for (key, order_ids) in completed {
            for order_id in order_ids {
                risk.release_inflight_order_by_order_id(&order_id);
                self.note_execution_complete(&order_id);
            }
            open_orders.remove_active_lifecycle(&key, now_ms);
        }
        self.matched_execution.prune_inactive_facts(open_orders);
        Ok(changed)
    }

    pub(crate) fn consume_matched_trade(
        &mut self,
        observation: &UnboundCtfTradeObservation,
        metadata: Option<&OrderRuntimeMetadata>,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
        authorization: Option<DirectBuyAuthorization>,
        risk: &crate::risk::RiskHandle,
    ) -> anyhow::Result<bool> {
        let changed = self.matched_execution.consume_trade(
            observation,
            metadata,
            portfolio,
            open_orders,
            now_ms,
            authorization,
        )?;
        if let Some(authorization) = authorization {
            risk.reconcile_direct_buy_order(
                authorization,
                None,
                self.matched_execution.pending_buy_quantity(authorization)?,
                published_buy_quantity(portfolio, authorization)?,
            )?;
        }
        Ok(changed)
    }

    pub(crate) fn consume_captured_matched_fill(
        &mut self,
        fill: &FillEvent,
        metadata: Option<&OrderRuntimeMetadata>,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
        authorization: Option<DirectBuyAuthorization>,
        risk: &crate::risk::RiskHandle,
    ) -> anyhow::Result<bool> {
        let changed = self.matched_execution.consume_captured_fill(
            fill,
            metadata,
            portfolio,
            open_orders,
            now_ms,
            authorization,
        )?;
        if let Some(authorization) = authorization {
            risk.reconcile_direct_buy_order(
                authorization,
                None,
                self.matched_execution.pending_buy_quantity(authorization)?,
                published_buy_quantity(portfolio, authorization)?,
            )?;
        }
        Ok(changed)
    }
}

impl MatchedExecutionState {
    pub(super) fn consume_trade(
        &mut self,
        observation: &UnboundCtfTradeObservation,
        metadata: Option<&OrderRuntimeMetadata>,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
        authorization: Option<DirectBuyAuthorization>,
    ) -> anyhow::Result<bool> {
        let mut changed = self.reconcile(portfolio, open_orders, now_ms);
        if observation.side() == FillSide::Buy {
            let authorization = authorization
                .ok_or_else(|| anyhow::anyhow!("BUY match has no original authorization"))?;
            changed |= self.remember_buy_match(
                authorization,
                observation.market_id(),
                observation.asset_id(),
                observation.trade_id(),
                observation.order_id(),
                observation.owned_size_units_6dp(),
                observation.status() == CtfTradeStatus::Failed,
            )?;
            return Ok(changed);
        }
        let identity = (
            observation.trade_id().trim().to_string(),
            observation.order_id().trim().to_string(),
        );
        anyhow::ensure!(
            !identity.0.is_empty() && !identity.1.is_empty(),
            "matched execution identity is empty"
        );
        if self.settled.contains(&identity) || self.failed.contains(&identity) {
            return Ok(changed);
        }
        let key_and_intent = key_and_intent_for_observation(observation, metadata)?;
        let Some((key, execution_intent_id)) = key_and_intent else {
            return Ok(changed);
        };
        if observation.status() == CtfTradeStatus::Failed {
            self.failed.insert(identity.clone());
            let removed_key = self
                .facts
                .remove(&identity)
                .and_then(|fact| fact.scope.sell_key().cloned());
            changed |=
                self.refresh_key(removed_key.as_ref().unwrap_or(&key), open_orders, now_ms)?;
            return Ok(changed);
        }
        let Some(gross_units_6dp) = observation.owned_gross_proceeds_units_6dp() else {
            return Ok(changed);
        };
        let fact = MatchedExecutionFact {
            scope: MatchedExecutionScope::Sell(key.clone()),
            execution_intent_id,
            qty_units_6dp: observation.owned_size_units_6dp(),
            gross_units_6dp: Some(gross_units_6dp),
            fee_units_6dp: observation.owned_fee_units_6dp(),
        };
        validate_fact(&fact)?;
        if let Some(existing) = self.facts.get_mut(&identity) {
            if merge_repeated_fact(existing, &fact)? {
                changed |= self.refresh_key(&key, open_orders, now_ms)?;
            }
            return Ok(changed);
        }
        self.facts.insert(identity, fact);
        changed |= self.refresh_key(&key, open_orders, now_ms)?;
        Ok(changed)
    }

    pub(super) fn consume_captured_fill(
        &mut self,
        fill: &FillEvent,
        metadata: Option<&OrderRuntimeMetadata>,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
        authorization: Option<DirectBuyAuthorization>,
    ) -> anyhow::Result<bool> {
        let mut changed = self.reconcile(portfolio, open_orders, now_ms);
        if fill.side == FillSide::Buy {
            let authorization = authorization
                .ok_or_else(|| anyhow::anyhow!("captured BUY match has no authorization"))?;
            changed |= self.remember_buy_match(
                authorization,
                fill.market_id
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("BUY match has no market"))?,
                &fill.token,
                fill.trade_id.as_deref().unwrap_or_default(),
                &fill.order_id,
                fill.receipt_qty_units_6dp
                    .ok_or_else(|| anyhow::anyhow!("BUY match has no quantity"))?,
                false,
            )?;
            return Ok(changed);
        }
        let Some((key, execution_intent_id)) = key_and_intent_for_fill(fill, metadata)? else {
            return Ok(changed);
        };
        let trade_id = fill
            .trade_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow::anyhow!("captured matched execution has no trade identity"))?;
        let identity = (trade_id.to_string(), fill.order_id.trim().to_string());
        let fact = MatchedExecutionFact {
            scope: MatchedExecutionScope::Sell(key.clone()),
            execution_intent_id,
            qty_units_6dp: fill
                .receipt_qty_units_6dp
                .ok_or_else(|| anyhow::anyhow!("captured matched execution has no quantity"))?,
            gross_units_6dp: Some(
                u128::try_from(fill.receipt_gross_proceeds_units_6dp.ok_or_else(|| {
                    anyhow::anyhow!("captured matched execution has no gross proceeds")
                })?)
                .map_err(|_| anyhow::anyhow!("captured matched execution gross is negative"))?,
            ),
            fee_units_6dp: fill.receipt_fee_units_6dp,
        };
        validate_fact(&fact)?;
        if let Some(existing) = self.facts.get_mut(&identity) {
            if merge_repeated_fact(existing, &fact)? {
                changed |= self.refresh_key(&key, open_orders, now_ms)?;
            }
        } else if !self.settled.contains(&identity) && !self.failed.contains(&identity) {
            self.facts.insert(identity, fact);
            changed |= self.refresh_key(&key, open_orders, now_ms)?;
        }
        Ok(changed)
    }

    fn remember_buy_match(
        &mut self,
        authorization: DirectBuyAuthorization,
        market: &crate::polymarket::identity::MarketId,
        token: &str,
        trade_id: &str,
        order_id: &str,
        qty_units_6dp: u128,
        failed: bool,
    ) -> anyhow::Result<bool> {
        authorization.validate()?;
        anyhow::ensure!(
            market.as_str() == authorization.condition_id.to_string()
                && token == authorization.selected_token().to_string()
                && !trade_id.trim().is_empty()
                && !order_id.trim().is_empty()
                && qty_units_6dp > 0
                && qty_units_6dp <= authorization.target_qty_units_6dp,
            "BUY matched identity differs from authorization"
        );
        let identity = (trade_id.to_string(), order_id.to_string());
        if self.settled.contains(&identity) {
            return Ok(false);
        }
        let incoming = MatchedExecutionFact {
            scope: MatchedExecutionScope::Buy(authorization),
            execution_intent_id: authorization.intent_id,
            qty_units_6dp,
            gross_units_6dp: None,
            fee_units_6dp: None,
        };
        let mut changed = false;
        if let Some(existing) = self.facts.get_mut(&identity) {
            merge_repeated_fact(existing, &incoming)?;
        } else {
            self.facts.insert(identity.clone(), incoming);
            changed = true;
        }
        if failed {
            changed |= self.failed.insert(identity);
        }
        Ok(changed)
    }

    pub(super) fn pending_buy_quantity(
        &self,
        authorization: DirectBuyAuthorization,
    ) -> anyhow::Result<u128> {
        self.facts
            .iter()
            .filter(|(identity, fact)| {
                fact.scope == MatchedExecutionScope::Buy(authorization)
                    && !self.failed.contains(*identity)
            })
            .try_fold(0u128, |qty, (_, fact)| {
                qty.checked_add(fact.qty_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("pending BUY matched quantity overflow"))
            })
    }

    pub(super) fn validate_first_target(
        &self,
        intent: &OrderIntent,
        key: Option<&OrderLifecycleKey>,
    ) -> anyhow::Result<Option<u128>> {
        let OrderIntent::Sell(sell) = intent else {
            return Ok(None);
        };
        if sell.close_reason != OrderCloseReason::PairedFirstManagedSell {
            return Ok(None);
        }
        let key = key.ok_or_else(|| anyhow::anyhow!("First target has no lifecycle identity"))?;
        anyhow::ensure!(
            is_ctf_market_entry_sell_key(key)
                && !key.market.trim().is_empty()
                && !key.token.trim().is_empty(),
            "First target has invalid lifecycle identity"
        );
        let qty_units_6dp = sell
            .first_target_qty_units_6dp
            .filter(|qty| *qty > 0)
            .ok_or_else(|| anyhow::anyhow!("First target quantity is missing or zero"))?;
        anyhow::ensure!(
            intent.execution_instruction_qty_units_6dp > 0
                && intent.execution_instruction_qty_units_6dp <= qty_units_6dp,
            "First instruction quantity exceeds target or is zero"
        );
        if let Some(target) = self.first_targets.get(&execution_scope(key)) {
            anyhow::ensure!(
                target.key == *key && target.qty_units_6dp == qty_units_6dp,
                "First target identity or quantity conflicts with original authorization"
            );
        }
        Ok(Some(qty_units_6dp))
    }

    pub(super) fn remember_first_target(
        &mut self,
        key: &OrderLifecycleKey,
        intent: &OrderIntent,
    ) -> bool {
        let qty_units_6dp = match self.validate_first_target(intent, Some(key)) {
            Ok(Some(qty)) => qty,
            Ok(None) => return true,
            Err(error) => {
                tracing::error!(
                    target: "execution.order_lifecycle",
                    key = %key.label(),
                    error = %format!("{error:#}"),
                    "matched_execution_first_identity_conflict"
                );
                return false;
            }
        };
        self.first_targets
            .entry(execution_scope(key))
            .or_insert_with(|| FirstExecutionTarget {
                key: key.clone(),
                qty_units_6dp,
                confirmed_qty_units_6dp: Some(0),
            });
        true
    }

    pub(super) fn sibling_ready(&self, key: &OrderLifecycleKey) -> bool {
        if key.close_reason != OrderCloseReason::PairedPostProceedsSell
            || !crate::ctf_inventory::is_confirmed_ctf_split_cohort_id(&key.cohort)
        {
            return true;
        }
        let Some(target) = self.first_targets.get(&execution_scope(key)) else {
            return false;
        };
        let Some(confirmed_qty_units_6dp) = target.confirmed_qty_units_6dp else {
            return false;
        };
        let pending = self
            .facts
            .values()
            .filter(|fact| fact.scope.sell_key() == Some(&target.key))
            .try_fold((0_u128, true), |(qty, fees_known), fact| {
                qty.checked_add(fact.qty_units_6dp)
                    .map(|total| (total, fees_known && fact.fee_units_6dp.is_some()))
            });
        let Some((pending_qty_units_6dp, pending_fees_known)) = pending else {
            return false;
        };
        pending_fees_known
            && confirmed_qty_units_6dp
                .checked_add(pending_qty_units_6dp)
                .is_some_and(|qty| qty == target.qty_units_6dp)
    }

    pub(super) fn reconcile(
        &mut self,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> bool {
        if self.portfolio_revision == Some(portfolio.revision) {
            return false;
        }
        for target in self.first_targets.values_mut() {
            target.confirmed_qty_units_6dp = Some(0);
        }
        for fill in portfolio.confirmed_direct_buy_receipts() {
            let trade_id = match &fill.receipt_identity {
                ManagedSellReceiptIdentity::ExchangeV2 { venue_trade_id, .. } => {
                    venue_trade_id.clone()
                }
                ManagedSellReceiptIdentity::CapturedSimulation { .. } => fill.fill_id.clone(),
            };
            let identity = (trade_id, fill.order_id.clone());
            self.settled.insert(identity.clone());
            self.failed.remove(&identity);
            self.facts.remove(&identity);
        }
        let mut affected = HashSet::new();
        for fill in portfolio.confirmed_lifecycle_sell_fills.values() {
            let Some(identity) = confirmed_identity(fill) else {
                continue;
            };
            self.settled.insert(identity.clone());
            self.failed.remove(&identity);
            if let Some(fact) = self.facts.remove(&identity) {
                affected.extend(fact.scope.sell_key().cloned());
            }
            if fill.close_reason == OrderCloseReason::PairedFirstManagedSell {
                let scope = (fill.market_id.to_string(), fill.lifecycle_cohort.clone());
                if let Some(target) = self.first_targets.get_mut(&scope) {
                    if fill.token == target.key.token {
                        target.confirmed_qty_units_6dp = target
                            .confirmed_qty_units_6dp
                            .and_then(|qty| qty.checked_add(fill.qty_units_6dp));
                    }
                }
            }
        }
        self.portfolio_revision = Some(portfolio.revision);
        affected.extend(
            self.facts
                .values()
                .filter_map(|fact| fact.scope.sell_key().cloned()),
        );
        let mut changed = false;
        for key in affected {
            changed |= match self.refresh_key(&key, open_orders, now_ms) {
                Ok(applied) => applied,
                Err(error) => {
                    tracing::error!(
                        target: "execution.order_lifecycle",
                        key = %key.label(),
                        error = %format!("{error:#}"),
                        "matched_execution_aggregate_invalid"
                    );
                    open_orders.clear_matched_execution(&key, portfolio.revision, now_ms)
                }
            };
        }
        changed
    }

    pub(super) fn prune_inactive_facts(&mut self, open_orders: &OpenOrdersState) {
        let active_keys = open_orders
            .active_order_lifecycles
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        self.facts.retain(|_, fact| {
            fact.scope
                .sell_key()
                .is_none_or(|key| active_keys.contains(key))
        });
    }

    fn refresh_key(
        &self,
        key: &OrderLifecycleKey,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> anyhow::Result<bool> {
        let mut qty = 0_u128;
        let mut gross = 0_u128;
        let mut fee = Some(0_u128);
        let mut found = false;
        for fact in self
            .facts
            .values()
            .filter(|fact| fact.scope.sell_key() == Some(key))
        {
            found = true;
            qty = qty
                .checked_add(fact.qty_units_6dp)
                .ok_or_else(|| anyhow::anyhow!("matched execution quantity overflow"))?;
            gross = gross
                .checked_add(
                    fact.gross_units_6dp
                        .context("SELL matched gross is missing")?,
                )
                .ok_or_else(|| anyhow::anyhow!("matched execution gross overflow"))?;
            fee = fee
                .zip(fact.fee_units_6dp)
                .and_then(|(total, value)| total.checked_add(value));
        }
        if !found {
            return Ok(open_orders.clear_matched_execution(
                key,
                self.portfolio_revision.unwrap_or_default(),
                now_ms,
            ));
        }
        Ok(open_orders.record_matched_execution(
            key,
            qty,
            gross,
            fee,
            self.portfolio_revision.unwrap_or_default(),
            now_ms,
        ))
    }
}

fn confirmed_identity(
    fill: &crate::portfolio::ConfirmedLifecycleSellFill,
) -> Option<ExecutionIdentity> {
    let trade_id = match fill.receipt_identity.as_ref()? {
        ManagedSellReceiptIdentity::ExchangeV2 { venue_trade_id, .. } => venue_trade_id.clone(),
        ManagedSellReceiptIdentity::CapturedSimulation { .. } => fill.fill_id.clone(),
    };
    Some((trade_id, fill.order_id.clone()))
}

fn validate_fact(fact: &MatchedExecutionFact) -> anyhow::Result<()> {
    anyhow::ensure!(
        fact.execution_intent_id > 0
            && fact.qty_units_6dp > 0
            && (fact.scope.sell_key().is_none()
                || fact.gross_units_6dp.is_some_and(|gross| gross > 0)),
        "matched execution economics are incomplete"
    );
    Ok(())
}

fn execution_scope(key: &OrderLifecycleKey) -> ExecutionScope {
    (key.market.clone(), key.cohort.clone())
}

fn merge_repeated_fact(
    existing: &mut MatchedExecutionFact,
    incoming: &MatchedExecutionFact,
) -> anyhow::Result<bool> {
    anyhow::ensure!(
        existing.scope == incoming.scope
            && existing.execution_intent_id == incoming.execution_intent_id
            && existing.qty_units_6dp == incoming.qty_units_6dp
            && existing.gross_units_6dp == incoming.gross_units_6dp,
        "matched execution economics changed across status updates"
    );
    match (existing.fee_units_6dp, incoming.fee_units_6dp) {
        (Some(previous), Some(current)) => {
            anyhow::ensure!(
                previous == current,
                "matched execution fee changed across status updates"
            );
            Ok(false)
        }
        (None, Some(fee)) => {
            existing.fee_units_6dp = Some(fee);
            Ok(true)
        }
        (Some(_), None) | (None, None) => Ok(false),
    }
}

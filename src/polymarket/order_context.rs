use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use crate::execution::supervisor::OrderExecutionTiming;
use crate::orders::OrderUpdateEvent;
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::identity::MarketId;
use crate::portfolio::{FillEvent, FillSide, NormalizedStrategyFill};
use crate::trade_session_ledger::ObservedCtfTradeFact;
use crate::types::{DirectBuyAuthorization, OrderCloseReason, Side, TimeInForce};

#[derive(Debug, Clone, Default)]
pub struct OrderFillContextRegistry {
    inner: Arc<Mutex<OrderFillContextState>>,
}

#[derive(Debug, Default)]
struct OrderFillContextState {
    entries: HashMap<String, RegisteredOrderFillContext>,
}

#[derive(Debug, Clone)]
struct OrderFillContextData {
    execution_intent_id: Option<u64>,
    direct_buy_authorization: Option<DirectBuyAuthorization>,
    token: String,
    side: FillSide,
    market_id: Option<MarketId>,
    time_in_force: Option<TimeInForce>,
    post_only: Option<bool>,
    quote_level: Option<u8>,
    quote_group: Option<String>,
    lifecycle_cohort: Option<String>,
    lifecycle_symbol: Option<String>,
    submitted_order_size: Option<f64>,
    order_close_reason: Option<OrderCloseReason>,
    execution_timing: Option<OrderExecutionTiming>,
}

#[derive(Debug)]
struct RegisteredOrderFillContext {
    context: Arc<OrderFillContextData>,
    audit_complete: bool,
    execution_complete: bool,
    owned_obligations: usize,
}

struct OrderFillContextLease {
    order_id: String,
    context: Arc<OrderFillContextData>,
    registry: Weak<Mutex<OrderFillContextState>>,
}

#[derive(Clone)]
pub struct OrderFillContext {
    lease: Arc<OrderFillContextLease>,
}

impl std::fmt::Debug for OrderFillContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OrderFillContext")
            .field("order_id", &self.lease.order_id)
            .finish_non_exhaustive()
    }
}

impl Drop for OrderFillContextLease {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let Ok(mut state) = registry.lock() else {
            tracing::error!(
                target: "polymarket.order_context",
                order_id = self.order_id,
                "order_context_owner_release_lock_failed"
            );
            return;
        };
        let should_retire = if let Some(entry) = state.entries.get_mut(&self.order_id) {
            if entry.owned_obligations == 0 {
                tracing::error!(
                    target: "polymarket.order_context",
                    order_id = self.order_id,
                    "order_context_owner_release_underflow"
                );
            } else {
                entry.owned_obligations -= 1;
            }
            entry.audit_complete && entry.execution_complete && entry.owned_obligations == 0
        } else {
            false
        };
        if should_retire {
            state.entries.remove(&self.order_id);
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderRuntimeMetadata {
    pub execution_intent_id: Option<u64>,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub quote_level: Option<u8>,
    pub quote_group: Option<String>,
    pub lifecycle_cohort: Option<String>,
    pub lifecycle_symbol: Option<String>,
    pub order_close_reason: Option<OrderCloseReason>,
}

#[derive(Debug, Clone)]
pub struct QuarantinedUnmanagedFill {
    pub reason: &'static str,
    pub fill: FillEvent,
}

#[derive(Debug, Clone)]
pub enum FillClassification {
    Normalized(NormalizedStrategyFill),
    Quarantined(QuarantinedUnmanagedFill),
}

impl OrderFillContextRegistry {
    pub(crate) fn mark_audit_complete(&self, order_id: &str) {
        self.mark_complete(order_id, true);
    }

    pub(crate) fn mark_execution_complete(&self, order_id: &str) {
        self.mark_complete(order_id, false);
    }

    fn mark_complete(&self, order_id: &str, audit: bool) {
        let order_id = order_id.trim();
        if order_id.is_empty() {
            return;
        }
        let mut state = self
            .inner
            .lock()
            .expect("order fill context mutex poisoned");
        let should_retire = if let Some(entry) = state.entries.get_mut(order_id) {
            if audit {
                entry.audit_complete = true;
            } else {
                entry.execution_complete = true;
            }
            entry.audit_complete && entry.execution_complete && entry.owned_obligations == 0
        } else {
            false
        };
        if should_retire {
            state.entries.remove(order_id);
        }
    }

    pub(crate) fn owned_context(&self, order_id: &str) -> anyhow::Result<OrderFillContext> {
        let order_id = order_id.trim();
        anyhow::ensure!(!order_id.is_empty(), "order context identity is empty");
        let mut state = self
            .inner
            .lock()
            .expect("order fill context mutex poisoned");
        let entry = state
            .entries
            .get_mut(order_id)
            .ok_or_else(|| anyhow::anyhow!("order context is unavailable"))?;
        ensure_complete_original_context(&entry.context, order_id)?;
        entry.owned_obligations = entry
            .owned_obligations
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("order context obligation count overflow"))?;
        Ok(OrderFillContext {
            lease: Arc::new(OrderFillContextLease {
                order_id: order_id.to_string(),
                context: entry.context.clone(),
                registry: Arc::downgrade(&self.inner),
            }),
        })
    }

    pub(crate) fn normalize_fill(&self, fill: FillEvent) -> FillClassification {
        if fill.order_id.trim().is_empty() {
            return quarantined_fill("missing_order_context", fill);
        }
        let entry = {
            let state = self
                .inner
                .lock()
                .expect("order fill context mutex poisoned");
            state
                .entries
                .get(fill.order_id.as_str())
                .map(|entry| entry.context.clone())
        };
        let Some(entry) = entry else {
            return quarantined_fill("missing_order_context", fill);
        };
        normalize_fill_with_context(fill, &entry, None)
    }

    fn complete_context_for_order(&self, order_id: &str) -> Option<Arc<OrderFillContextData>> {
        self.inner
            .lock()
            .expect("order fill context mutex poisoned")
            .entries
            .get(order_id)
            .map(|entry| entry.context.clone())
    }
}

impl OrderFillContext {
    pub(crate) fn direct_buy_authorization(&self) -> Option<DirectBuyAuthorization> {
        self.lease.context.direct_buy_authorization
    }

    pub(crate) fn runtime_metadata(&self) -> OrderRuntimeMetadata {
        runtime_metadata(&self.lease.context)
    }

    pub(crate) fn bind_trade_observation(
        &self,
        observation: &UnboundCtfTradeObservation,
    ) -> anyhow::Result<ObservedCtfTradeFact> {
        anyhow::ensure!(
            observation.order_id() == self.lease.order_id,
            "trade observation order differs from owned context"
        );
        bind_trade_observation(&self.lease.context, observation)
    }

    pub(crate) fn normalize_fill_with_matched_trade(
        &self,
        fill: FillEvent,
        matched_trade: ObservedCtfTradeFact,
    ) -> FillClassification {
        if fill.order_id != self.lease.order_id {
            return quarantined_fill("order_context_order_mismatch", fill);
        }
        normalize_fill_with_context(fill, &self.lease.context, Some(matched_trade))
    }
}

fn bind_trade_observation(
    entry: &OrderFillContextData,
    observation: &UnboundCtfTradeObservation,
) -> anyhow::Result<ObservedCtfTradeFact> {
    let market_id = entry
        .market_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("trade observation order has no market identity"))?;
    let cohort_id = entry
        .lifecycle_cohort
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("trade observation order has no cohort identity"))?;
    observation.bind(market_id, cohort_id, &entry.token)
}

impl OrderFillContextRegistry {
    pub fn record_order_submission(
        &self,
        execution_intent_id: Option<u64>,
        order_id: &str,
        market_id: &str,
        token: &str,
        side: Side,
        time_in_force: &TimeInForce,
        post_only: bool,
        quote_level: Option<u8>,
        quote_group: Option<&str>,
        lifecycle_cohort: Option<&str>,
        lifecycle_symbol: Option<&str>,
        submitted_order_size: Option<f64>,
        close_hint: Option<&str>,
    ) -> anyhow::Result<()> {
        self.record_order_submission_with_timing(
            execution_intent_id,
            order_id,
            market_id,
            token,
            side,
            time_in_force,
            post_only,
            quote_level,
            quote_group,
            lifecycle_cohort,
            lifecycle_symbol,
            submitted_order_size,
            close_hint,
            None,
            None,
        )
    }

    pub(crate) fn record_order_submission_with_timing(
        &self,
        execution_intent_id: Option<u64>,
        order_id: &str,
        market_id: &str,
        token: &str,
        side: Side,
        time_in_force: &TimeInForce,
        post_only: bool,
        quote_level: Option<u8>,
        quote_group: Option<&str>,
        lifecycle_cohort: Option<&str>,
        lifecycle_symbol: Option<&str>,
        submitted_order_size: Option<f64>,
        close_hint: Option<&str>,
        execution_timing: Option<OrderExecutionTiming>,
        direct_buy_authorization: Option<DirectBuyAuthorization>,
    ) -> anyhow::Result<()> {
        self.upsert(
            execution_intent_id,
            order_id,
            token,
            fill_side_from_order_side(side),
            Some(market_id),
            Some(time_in_force),
            Some(post_only),
            quote_level,
            quote_group,
            lifecycle_cohort,
            lifecycle_symbol,
            submitted_order_size,
            close_hint,
            execution_timing,
            direct_buy_authorization,
        )
    }

    pub(crate) fn refresh_order_execution_timing(
        &self,
        order_id: &str,
        execution_intent_id: Option<u64>,
        execution_timing: OrderExecutionTiming,
    ) {
        let order_id = order_id.trim();
        let Some(execution_intent_id) = execution_intent_id.filter(|intent_id| *intent_id > 0)
        else {
            return;
        };
        let Ok(mut state) = self.inner.lock() else {
            tracing::error!(
                target: "polymarket.order_context",
                order_id,
                "order_execution_timing_refresh_lock_failed"
            );
            return;
        };
        let Some(entry) = state.entries.get_mut(order_id) else {
            return;
        };
        if entry.context.execution_intent_id != Some(execution_intent_id) {
            tracing::warn!(
                target: "polymarket.order_context",
                order_id,
                expected_intent_id = execution_intent_id,
                actual_intent_id = ?entry.context.execution_intent_id,
                "order_execution_timing_refresh_identity_mismatch"
            );
            return;
        }
        Arc::make_mut(&mut entry.context).execution_timing = Some(execution_timing);
    }

    pub fn record_order_update(&self, update: &OrderUpdateEvent) {
        let Some(existing) = self.complete_context_for_order(&update.order_id) else {
            return;
        };
        if let Some(error) = order_update_identity_mismatch(
            Some(existing.as_ref()),
            update.token.as_str(),
            fill_side_from_order_side(update.side),
            clean_market_id(Some(update.market.as_str())).as_ref(),
        ) {
            tracing::error!(
                target: "polymarket.order_context",
                order_id = update.order_id,
                error,
                "order_update_context_rejected"
            );
        }
    }

    pub fn order_metadata(&self, order_id: &str) -> Option<OrderRuntimeMetadata> {
        let order_id = order_id.trim();
        if order_id.is_empty() {
            return None;
        }
        let state = self
            .inner
            .lock()
            .expect("order fill context mutex poisoned");
        state
            .entries
            .get(order_id)
            .map(|entry| runtime_metadata(&entry.context))
    }

    pub(crate) fn direct_buy_authorization(
        &self,
        order_id: &str,
    ) -> Option<DirectBuyAuthorization> {
        self.complete_context_for_order(order_id.trim())?
            .direct_buy_authorization
    }

    fn upsert(
        &self,
        execution_intent_id: Option<u64>,
        order_id: &str,
        token: &str,
        side: FillSide,
        market_id: Option<&str>,
        time_in_force: Option<&TimeInForce>,
        post_only: Option<bool>,
        quote_level: Option<u8>,
        quote_group: Option<&str>,
        lifecycle_cohort: Option<&str>,
        lifecycle_symbol: Option<&str>,
        submitted_order_size: Option<f64>,
        close_hint: Option<&str>,
        execution_timing: Option<OrderExecutionTiming>,
        direct_buy_authorization: Option<DirectBuyAuthorization>,
    ) -> anyhow::Result<()> {
        let order_id = order_id.trim();
        let token = token.trim();
        if order_id.is_empty() || token.is_empty() {
            anyhow::bail!("order fill context identity is empty");
        }
        anyhow::ensure!(
            execution_intent_id != Some(0),
            "execution intent identity is zero"
        );
        let market_id = clean_market_id(market_id);
        let quote_group = clean_optional(quote_group);
        let lifecycle_cohort = clean_optional(lifecycle_cohort);
        let lifecycle_symbol = clean_optional(lifecycle_symbol);
        let submitted_order_size =
            submitted_order_size.filter(|size| size.is_finite() && *size > 0.0);
        let order_close_reason = clean_close_reason(close_hint);
        if let Some(authorization) = direct_buy_authorization {
            authorization.validate()?;
            anyhow::ensure!(
                side == FillSide::Buy
                    && execution_intent_id == Some(authorization.intent_id)
                    && token == authorization.selected_token().to_string()
                    && market_id.as_ref().is_some_and(|market| {
                        market.as_str() == authorization.condition_id.to_string()
                    })
                    && lifecycle_cohort.as_deref()
                        == Some(
                            crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                                authorization.condition_id,
                                authorization.intent_id,
                            )?
                            .as_str()
                        ),
                "order context BUY authorization identity mismatch"
            );
        }

        let mut state = self
            .inner
            .lock()
            .expect("order fill context mutex poisoned");
        let existing = state
            .entries
            .get(order_id)
            .map(|entry| entry.context.clone());
        if let Some(reason) =
            order_update_identity_mismatch(existing.as_deref(), token, side, market_id.as_ref())
        {
            anyhow::bail!("order context identity mismatch reason={reason} order_id={order_id}");
        }
        let existing_intent_id = existing
            .as_ref()
            .and_then(|entry| entry.execution_intent_id);
        let existing_authorization = existing
            .as_ref()
            .and_then(|entry| entry.direct_buy_authorization);
        anyhow::ensure!(
            direct_buy_authorization.is_none()
                || existing_authorization.is_none()
                || direct_buy_authorization == existing_authorization,
            "order context BUY authorization changed order_id={order_id}"
        );
        let direct_buy_authorization = existing_authorization.or(direct_buy_authorization);
        anyhow::ensure!(
            execution_intent_id.is_none()
                || existing_intent_id.is_none()
                || execution_intent_id == existing_intent_id,
            "order context execution intent identity changed order_id={order_id} existing={existing_intent_id:?} actual={execution_intent_id:?}"
        );
        let market_id =
            market_id.or_else(|| existing.as_ref().and_then(|entry| entry.market_id.clone()));
        let execution_intent_id = existing_intent_id.or(execution_intent_id);
        let time_in_force = time_in_force.cloned().or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.time_in_force.clone())
        });
        let post_only = post_only.or_else(|| existing.as_ref().and_then(|entry| entry.post_only));
        let quote_level =
            quote_level.or_else(|| existing.as_ref().and_then(|entry| entry.quote_level));
        let quote_group = quote_group.or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.quote_group.clone())
        });
        let lifecycle_cohort = lifecycle_cohort.or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.lifecycle_cohort.clone())
        });
        let lifecycle_symbol = lifecycle_symbol.or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.lifecycle_symbol.clone())
        });
        let submitted_order_size = submitted_order_size.or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.submitted_order_size)
        });
        let order_close_reason = order_close_reason
            .or_else(|| existing.as_ref().and_then(|entry| entry.order_close_reason));
        let execution_timing = execution_timing.or_else(|| {
            existing
                .as_ref()
                .and_then(|entry| entry.execution_timing.clone())
        });
        let (audit_complete, execution_complete, owned_obligations) = state
            .entries
            .get(order_id)
            .map(|entry| {
                (
                    entry.audit_complete,
                    entry.execution_complete,
                    entry.owned_obligations,
                )
            })
            .unwrap_or((false, false, 0));

        state.entries.insert(
            order_id.to_string(),
            RegisteredOrderFillContext {
                context: Arc::new(OrderFillContextData {
                    execution_intent_id,
                    direct_buy_authorization,
                    token: token.to_string(),
                    side,
                    market_id,
                    time_in_force,
                    post_only,
                    quote_level,
                    quote_group,
                    lifecycle_cohort,
                    lifecycle_symbol,
                    submitted_order_size,
                    order_close_reason,
                    execution_timing,
                }),
                audit_complete,
                execution_complete,
                owned_obligations,
            },
        );
        Ok(())
    }
}

fn runtime_metadata(entry: &OrderFillContextData) -> OrderRuntimeMetadata {
    OrderRuntimeMetadata {
        execution_intent_id: entry.execution_intent_id,
        time_in_force: entry.time_in_force.clone().unwrap_or_default(),
        post_only: entry.post_only.unwrap_or(false),
        quote_level: entry.quote_level,
        quote_group: entry.quote_group.clone(),
        lifecycle_cohort: entry.lifecycle_cohort.clone(),
        lifecycle_symbol: entry.lifecycle_symbol.clone(),
        order_close_reason: entry.order_close_reason,
    }
}

fn ensure_complete_original_context(
    entry: &OrderFillContextData,
    order_id: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        entry.side != FillSide::Buy || entry.direct_buy_authorization.is_some(),
        "BUY order context has no original authorization"
    );
    anyhow::ensure!(
        entry.execution_intent_id.is_some(),
        "order context has no intent identity"
    );
    anyhow::ensure!(
        entry.market_id.is_some(),
        "order context has no market identity"
    );
    anyhow::ensure!(
        entry.time_in_force.is_some(),
        "order context has no time-in-force"
    );
    anyhow::ensure!(
        entry.post_only.is_some(),
        "order context has no post-only identity"
    );
    anyhow::ensure!(
        entry
            .lifecycle_cohort
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        "order context has no cohort identity order_id={order_id}"
    );
    Ok(())
}

fn normalize_fill_with_context(
    mut fill: FillEvent,
    entry: &OrderFillContextData,
    matched_trade: Option<ObservedCtfTradeFact>,
) -> FillClassification {
    if let Some(reason) = fill_identity_mismatch(&fill, entry) {
        return quarantined_fill(reason, fill);
    }
    if fill.market_id.is_none() {
        fill.market_id = entry.market_id.clone();
    }
    if fill.quote_group.is_none() {
        fill.quote_group = entry.quote_group.clone();
    }
    fill.lifecycle_cohort = entry.lifecycle_cohort.clone();
    fill.execution_intent_id = entry.execution_intent_id;
    if fill.direct_buy_authorization.is_some()
        && fill.direct_buy_authorization != entry.direct_buy_authorization
    {
        return quarantined_fill("order_context_buy_authorization_mismatch", fill);
    }
    if fill.side == FillSide::Buy && entry.direct_buy_authorization.is_none() {
        return quarantined_fill("missing_buy_authorization", fill);
    }
    fill.direct_buy_authorization = entry.direct_buy_authorization;
    if fill.submitted_order_size.is_none() {
        fill.submitted_order_size = entry.submitted_order_size;
    }
    let matched_trade = match matched_trade {
        Some(fact) => Some(fact),
        None => match fill.trade_observation.as_ref() {
            Some(observation) => match observation.bind(
                fill.market_id
                    .as_ref()
                    .expect("normalized fill has order market identity"),
                fill.lifecycle_cohort.as_deref().unwrap_or_default(),
                &fill.token,
            ) {
                Ok(fact) => Some(fact),
                Err(_) => return quarantined_fill("trade_observation_identity_mismatch", fill),
            },
            None => None,
        },
    };
    let normalized = match fill.side {
        FillSide::Buy => NormalizedStrategyFill::buy(fill),
        FillSide::Sell => {
            let Some(close_reason) = entry.order_close_reason else {
                return quarantined_fill("missing_close_reason", fill);
            };
            NormalizedStrategyFill::sell(fill, close_reason)
        }
    }
    .with_execution_timing(entry.execution_timing.clone());
    match matched_trade {
        Some(fact) => {
            let rejected_fill = normalized.fill().clone();
            normalized.with_matched_trade(fact).map_or_else(
                |_| quarantined_fill("trade_observation_identity_mismatch", rejected_fill),
                FillClassification::Normalized,
            )
        }
        None => FillClassification::Normalized(normalized),
    }
}

fn quarantined_fill(reason: &'static str, fill: FillEvent) -> FillClassification {
    FillClassification::Quarantined(QuarantinedUnmanagedFill { reason, fill })
}

fn fill_identity_mismatch(fill: &FillEvent, entry: &OrderFillContextData) -> Option<&'static str> {
    if fill.token != entry.token {
        return Some("order_context_token_mismatch");
    }
    if fill.side != entry.side {
        return Some("order_context_side_mismatch");
    }
    if fill
        .market_id
        .as_ref()
        .zip(entry.market_id.as_ref())
        .is_some_and(|(fill_market, entry_market)| fill_market != entry_market)
    {
        return Some("order_context_market_mismatch");
    }
    None
}

fn order_update_identity_mismatch(
    existing: Option<&OrderFillContextData>,
    token: &str,
    side: FillSide,
    market_id: Option<&MarketId>,
) -> Option<&'static str> {
    let existing = existing?;
    if existing.token != token {
        return Some("order_context_update_token_mismatch");
    }
    if existing.side != side {
        return Some("order_context_update_side_mismatch");
    }
    if existing
        .market_id
        .as_ref()
        .zip(market_id)
        .is_some_and(|(existing_market, update_market)| existing_market != update_market)
    {
        return Some("order_context_update_market_mismatch");
    }
    None
}

fn fill_side_from_order_side(side: Side) -> FillSide {
    match side {
        Side::Buy => FillSide::Buy,
        Side::Sell => FillSide::Sell,
    }
}

fn clean_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .map(ToString::to_string)
}

fn clean_close_reason(value: Option<&str>) -> Option<OrderCloseReason> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .and_then(OrderCloseReason::from_order_str)
}

fn clean_market_id(value: Option<&str>) -> Option<MarketId> {
    value
        .map(MarketId::new)
        .filter(|value| !value.is_empty() && value.as_str() != "-")
}

use std::collections::HashMap;

use crate::polymarket::order_context::OrderRuntimeMetadata;
use crate::types::{OrderCloseReason, Side, TimeInForce};
use bbwick_engine::state::{fingerprint_hash, Fingerprint};

mod active_lifecycle;
mod open_order;
mod operator_snapshot;
mod terminal_fact;
pub use active_lifecycle::ActiveOrderLifecycle;
pub(crate) use operator_snapshot::OpenOrderSnapshot;
pub use terminal_fact::{OrderTerminalFact, OrderTerminalProvenance};

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdateEvent {
    pub ts_ms: u64,
    pub order_id: String,
    pub market: String,
    pub token: String,
    pub side: Side,
    pub px: f64,
    pub original_size: Option<f64>,
    pub matched_size: Option<f64>,
    pub status: Option<String>,
    pub msg_type: Option<String>,
    pub terminal_reason: Option<String>,
    pub terminal_reason_provenance: Option<String>,
}

impl OrderUpdateEvent {
    pub(crate) fn is_closed(&self) -> bool {
        let status = self
            .status
            .as_deref()
            .unwrap_or("UNKNOWN")
            .trim()
            .to_ascii_uppercase();
        let msg_type = self
            .msg_type
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        matches!(
            status.as_str(),
            "CANCELED" | "CANCELLED" | "MATCHED" | "REJECTED"
        ) || matches!(msg_type.as_str(), "CANCELLATION" | "REJECTION")
    }
}

#[derive(Debug, Clone)]
pub struct OpenOrderState {
    pub execution_intent_id: Option<u64>,
    pub order_id: String,
    pub market: String,
    pub token: String,
    pub side: Side,
    pub time_in_force: TimeInForce,
    pub px: f64,
    pub original_size: f64,
    pub matched_size: f64,
    pub placed_at_ms: u64,
    pub last_update_ms: u64,
    pub status: String,
    pub post_only: bool,
    pub quote_level: Option<u8>,
    pub quote_group: Option<String>,
    pub lifecycle_cohort: Option<String>,
    pub lifecycle_symbol: Option<String>,
    pub client_tag: Option<String>,
    pub order_close_reason: Option<OrderCloseReason>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct OrderLifecycleKey {
    pub market: String,
    pub token: String,
    pub side: Side,
    pub close_reason: OrderCloseReason,
    pub cohort: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Default)]
pub struct OpenOrdersState {
    pub updated_at_ms: u64,
    pub revision: u64,
    pub orders_fingerprint: Fingerprint,
    pub orders: HashMap<String, OpenOrderState>,
    pub active_order_lifecycles: HashMap<OrderLifecycleKey, ActiveOrderLifecycle>,
    pre_accept_updates: HashMap<String, OrderUpdateEvent>,
    pub last_update: Option<OrderUpdateEvent>,
    last_update_metadata: Option<OrderRuntimeMetadata>,
}

impl OpenOrdersState {
    pub fn is_filtered_for_underlying(
        &self,
        underlying: &str,
        ownership: &crate::polymarket::symbol_ownership::SymbolOwnershipIndex,
    ) -> bool {
        let filtered = self.filtered_for_underlying(underlying, ownership);
        self.updated_at_ms == filtered.updated_at_ms
            && self.revision == filtered.revision
            && self.orders_fingerprint == filtered.orders_fingerprint
            && self.orders.len() == filtered.orders.len()
            && self.active_order_lifecycles.len() == filtered.active_order_lifecycles.len()
            && self.last_update == filtered.last_update
            && self.last_update_metadata.is_some() == filtered.last_update_metadata.is_some()
    }

    pub fn filtered_for_underlying(
        &self,
        underlying: &str,
        ownership: &crate::polymarket::symbol_ownership::SymbolOwnershipIndex,
    ) -> Self {
        let order_belongs = |order: &OpenOrderState| {
            ownership.facts_belong_to(
                underlying,
                order.lifecycle_symbol.as_deref(),
                Some(&order.market),
                &[&order.token],
            ) && order
                .lifecycle_cohort
                .as_deref()
                .is_none_or(|cohort| ownership.cohort_belongs(cohort, underlying))
        };
        let orders = self
            .orders
            .iter()
            .filter(|(_, order)| order_belongs(order))
            .map(|(order_id, order)| (order_id.clone(), order.clone()))
            .collect::<HashMap<_, _>>();
        let active_order_lifecycles = self
            .active_order_lifecycles
            .iter()
            .filter(|(key, _)| {
                ownership.facts_belong_to(
                    underlying,
                    Some(&key.symbol),
                    Some(&key.market),
                    &[&key.token],
                ) && ownership.cohort_belongs(&key.cohort, underlying)
            })
            .map(|(key, active)| (key.clone(), active.clone()))
            .collect();
        let last_update_belongs = self.last_update.as_ref().is_some_and(|update| {
            ownership.facts_belong_to(underlying, None, Some(&update.market), &[&update.token])
                && self.last_update_metadata.as_ref().is_none_or(|metadata| {
                    ownership.facts_belong_to(
                        underlying,
                        metadata.lifecycle_symbol.as_deref(),
                        Some(&update.market),
                        &[&update.token],
                    ) && metadata
                        .lifecycle_cohort
                        .as_deref()
                        .is_none_or(|cohort| ownership.cohort_belongs(cohort, underlying))
                })
        });
        let mut filtered = Self {
            updated_at_ms: self.updated_at_ms,
            revision: self.revision,
            orders,
            active_order_lifecycles,
            last_update: last_update_belongs
                .then(|| self.last_update.clone())
                .flatten(),
            last_update_metadata: last_update_belongs
                .then(|| self.last_update_metadata.clone())
                .flatten(),
            ..Default::default()
        };
        filtered.orders_fingerprint = filtered.compute_orders_fingerprint();
        filtered
    }

    pub(crate) fn note_runtime_mutation(&mut self, updated_at_ms: u64) {
        self.refresh_orders_metadata(updated_at_ms);
    }

    pub(crate) fn take_pre_accept_update(&mut self, order_id: &str) -> Option<OrderUpdateEvent> {
        self.pre_accept_updates.remove(order_id.trim())
    }

    fn remember_pre_accept_update(&mut self, update: &OrderUpdateEvent) {
        use std::collections::hash_map::Entry;

        match self.pre_accept_updates.entry(update.order_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(update.clone());
            }
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                let cumulative_matched = existing
                    .matched_size
                    .unwrap_or(0.0)
                    .max(update.matched_size.unwrap_or(0.0));
                if !existing.is_closed() || update.is_closed() {
                    *existing = update.clone();
                }
                if cumulative_matched > 0.0
                    || existing.matched_size.is_some()
                    || update.matched_size.is_some()
                {
                    existing.matched_size = Some(cumulative_matched);
                }
            }
        }
    }

    fn refresh_orders_metadata(&mut self, updated_at_ms: u64) {
        self.updated_at_ms = updated_at_ms;
        self.revision = self.revision.saturating_add(1);
        self.orders_fingerprint = self.compute_orders_fingerprint();
    }

    fn compute_orders_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = Fingerprint::default();
        for order in self.orders.values() {
            fingerprint.insert_hash(fingerprint_hash(&(
                (
                    order.order_id.as_str(),
                    order.market.as_str(),
                    order.token.as_str(),
                    order.side,
                    order.px.to_bits(),
                    order.original_size.to_bits(),
                    order.matched_size.to_bits(),
                ),
                (
                    order.execution_intent_id,
                    order.quote_level.unwrap_or(u8::MAX),
                    order.quote_group.as_deref(),
                    order.lifecycle_cohort.as_deref(),
                    order.lifecycle_symbol.as_deref(),
                    order.client_tag.as_deref(),
                    order.order_close_reason.as_deref(),
                    order.post_only,
                    order.status.as_str(),
                    order.last_update_ms,
                ),
            )));
        }
        for (key, active) in &self.active_order_lifecycles {
            fingerprint.insert_hash(fingerprint_hash(&(
                key,
                (
                    active.order_id.as_deref(),
                    active.remaining_qty_units_6dp,
                    active.instruction_qty_units_6dp,
                    active.instruction_limit_px_units_8dp,
                    &active.time_in_force,
                    active.post_only,
                    active.execution_evidence_revision,
                    active.execution_intent_id,
                ),
                (
                    active.terminal_matched_qty_units_6dp,
                    active.matched_execution_qty_units_6dp,
                    active.matched_execution_gross_units_6dp,
                    active.matched_execution_fee_units_6dp,
                    active.matched_execution_portfolio_revision,
                    active.terminal_no_fill,
                    active.active_until_ms,
                ),
            )));
        }
        if let Some(update) = self.last_update.as_ref() {
            fingerprint.insert_hash(fingerprint_hash(&(
                (
                    update.ts_ms,
                    update.order_id.as_str(),
                    update.market.as_str(),
                    update.token.as_str(),
                    update.side,
                    update.px.to_bits(),
                ),
                (
                    update.original_size.map(f64::to_bits),
                    update.matched_size.map(f64::to_bits),
                    update.status.as_deref(),
                    update.msg_type.as_deref(),
                ),
            )));
        }
        if let Some(metadata) = self.last_update_metadata.as_ref() {
            fingerprint.insert_hash(fingerprint_hash(&format!("{metadata:?}")));
        }
        fingerprint
    }

    pub fn open_count(&self) -> usize {
        self.orders.len()
    }

    pub(crate) fn last_update_metadata(&self, order_id: &str) -> Option<&OrderRuntimeMetadata> {
        self.last_update
            .as_ref()
            .is_some_and(|update| update.order_id == order_id)
            .then_some(self.last_update_metadata.as_ref())
            .flatten()
    }

    pub(crate) fn has_pending_confirmed_cancel(&self) -> bool {
        self.last_update.as_ref().is_some_and(|update| {
            update.terminal_reason_provenance.as_deref() == Some("confirmed_central_cancel")
                && self
                    .orders
                    .get(&update.order_id)
                    .is_some_and(|order| order.status.eq_ignore_ascii_case("CANCELED"))
        })
    }

    pub fn record_cancel_terminal(&mut self, order_id: &str, ts_ms: u64, reason: &str) -> bool {
        let Some(order) = self.orders.get_mut(order_id) else {
            return false;
        };
        order.status = "CANCELED".to_string();
        order.last_update_ms = order.last_update_ms.max(ts_ms);
        self.last_update_metadata = Some(OrderRuntimeMetadata {
            execution_intent_id: order.execution_intent_id,
            time_in_force: order.time_in_force.clone(),
            post_only: order.post_only,
            quote_level: order.quote_level,
            quote_group: order.quote_group.clone(),
            lifecycle_cohort: order.lifecycle_cohort.clone(),
            lifecycle_symbol: order.lifecycle_symbol.clone(),
            order_close_reason: order.order_close_reason,
        });
        self.last_update = Some(OrderUpdateEvent {
            ts_ms,
            order_id: order.order_id.clone(),
            market: order.market.clone(),
            token: order.token.clone(),
            side: order.side,
            px: order.px,
            original_size: Some(order.original_size),
            matched_size: Some(order.matched_size),
            status: Some("CANCELED".to_string()),
            msg_type: Some(reason.to_string()),
            terminal_reason: Some(reason.to_string()),
            terminal_reason_provenance: Some("confirmed_central_cancel".to_string()),
        });
        self.refresh_orders_metadata(ts_ms);
        true
    }

    pub fn apply_update_with_metadata_at(
        &mut self,
        update: OrderUpdateEvent,
        metadata: Option<&OrderRuntimeMetadata>,
        updated_at_ms: u64,
    ) {
        if metadata
            .and_then(|value| value.execution_intent_id)
            .is_none()
        {
            self.remember_pre_accept_update(&update);
        }
        self.last_update = Some(update.clone());
        self.last_update_metadata = metadata.cloned();

        let status = update
            .status
            .as_deref()
            .unwrap_or("UNKNOWN")
            .trim()
            .to_ascii_uppercase();
        if update.is_closed() {
            self.orders.remove(&update.order_id);
            self.remove_active_lifecycles_for_order_id(&update.order_id, updated_at_ms);
            self.refresh_orders_metadata(updated_at_ms);
            return;
        }

        let original = update.original_size.unwrap_or(0.0).max(0.0);
        let matched = update.matched_size.unwrap_or(0.0).max(0.0);

        let entry = self
            .orders
            .entry(update.order_id.clone())
            .or_insert_with(|| {
                let now = update.ts_ms;
                OpenOrderState {
                    execution_intent_id: metadata.and_then(|meta| meta.execution_intent_id),
                    order_id: update.order_id.clone(),
                    market: update.market.clone(),
                    token: update.token.clone(),
                    side: update.side,
                    time_in_force: metadata
                        .map(|meta| meta.time_in_force.clone())
                        .unwrap_or_default(),
                    px: update.px,
                    original_size: original,
                    matched_size: matched,
                    placed_at_ms: now,
                    last_update_ms: now,
                    status: status.clone(),
                    post_only: metadata.map(|meta| meta.post_only).unwrap_or(false),
                    quote_level: metadata.and_then(|meta| meta.quote_level),
                    quote_group: metadata.and_then(|meta| meta.quote_group.clone()),
                    lifecycle_cohort: metadata.and_then(|meta| meta.lifecycle_cohort.clone()),
                    lifecycle_symbol: metadata.and_then(|meta| meta.lifecycle_symbol.clone()),
                    client_tag: None,
                    order_close_reason: metadata.and_then(|meta| meta.order_close_reason),
                }
            });

        entry.market = update.market;
        entry.token = update.token;
        entry.side = update.side;
        entry.px = update.px;
        if original > 0.0 {
            entry.original_size = original;
        }
        if matched >= 0.0 {
            // Venue order updates own cumulative matched quantity. Never let a
            // stale update resurrect previously reported remaining size.
            entry.matched_size = entry.matched_size.max(matched);
        }
        entry.last_update_ms = update.ts_ms;
        entry.status = status;
        if let Some(meta) = metadata {
            if entry.execution_intent_id.is_none() {
                entry.execution_intent_id = meta.execution_intent_id;
            }
            entry.time_in_force = meta.time_in_force.clone();
            entry.post_only = meta.post_only;
            entry.quote_level = meta.quote_level;
            if meta.quote_group.is_some() {
                entry.quote_group = meta.quote_group.clone();
                entry.client_tag = Some(format!(
                    "{}:{}",
                    entry.quote_group.as_deref().unwrap_or("-"),
                    entry.quote_level.unwrap_or(255)
                ));
            }
            if meta.lifecycle_cohort.is_some() {
                entry.lifecycle_cohort = meta.lifecycle_cohort.clone();
            }
            if meta.lifecycle_symbol.is_some() {
                entry.lifecycle_symbol = meta.lifecycle_symbol.clone();
            }
            if let Some(order_close_reason) = meta.order_close_reason {
                entry.order_close_reason = Some(order_close_reason);
            }
        }

        // Immediate orders do not rest at the venue once updated; any surviving
        // remainder is already dead locally even if a closing update lags.
        let remove_id =
            if crate::polymarket::fixed_math::below_one_fixed_6dp_unit(entry.remaining_size())
                || entry.time_in_force.is_immediate()
            {
                Some(entry.order_id.clone())
            } else {
                None
            };
        if let Some(id) = remove_id {
            self.orders.remove(&id);
        }
        self.refresh_orders_metadata(updated_at_ms);
    }
}

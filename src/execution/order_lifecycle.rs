mod ctf_market_entry_sell;
mod direct_buy;
mod matched_execution;

pub(crate) use self::ctf_market_entry_sell::is_ctf_market_entry_sell_intent;
use self::ctf_market_entry_sell::{
    is_ctf_market_entry_sell_key, market_entry_sell_block_reason, retain_terminal_no_fill,
};
use self::matched_execution::MatchedExecutionState;
use crate::orders::{
    OpenOrderState, OpenOrdersState, OrderLifecycleKey, OrderTerminalFact, OrderTerminalProvenance,
};
use crate::types::{OrderIntent, Side, TimeInForce};

const MATERIAL_QTY_EPS: f64 = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
pub(crate) const NO_FILL_RELEASE_MS: u64 = 1_500;
const IMMEDIATE_ORDER_TERMINAL_WAIT_MS: u64 = 60_000;

impl OrderLifecycleKey {
    pub(crate) fn from_intent(intent: &OrderIntent) -> Option<Self> {
        let close_reason = intent.order_close_reason()?;
        Some(Self {
            market: intent.market_id.as_deref()?.to_string(),
            token: intent.instrument.clone(),
            side: intent.side(),
            close_reason,
            cohort: clean_key_component(intent.lifecycle_cohort.as_deref())?.to_string(),
            symbol: clean_key_component(intent.lifecycle_symbol.as_deref())?.to_string(),
        })
    }

    fn from_open_order(order: &OpenOrderState) -> Option<Self> {
        Some(Self {
            market: order.market.clone(),
            token: order.token.clone(),
            side: order.side,
            close_reason: order.order_close_reason?,
            cohort: clean_key_component(order.lifecycle_cohort.as_deref())?.to_string(),
            symbol: clean_key_component(order.lifecycle_symbol.as_deref())?.to_string(),
        })
    }

    pub(crate) fn label(&self) -> String {
        format!(
            "{}:{}:{:?}:{}:{}:{}",
            self.market, self.token, self.side, self.close_reason, self.cohort, self.symbol
        )
    }
}

fn clean_key_component(value: Option<&str>) -> Option<&str> {
    let value = value?.trim();
    (!value.is_empty()).then_some(value)
}

#[allow(clippy::too_many_arguments, reason = "terminal evidence fact boundary")]
pub fn build_order_terminal_fact(
    intent: &OrderIntent,
    order_id: Option<&str>,
    provenance: OrderTerminalProvenance,
    raw_status: Option<&str>,
    raw_message_type: Option<&str>,
    raw_reason: Option<&str>,
    raw_reason_provenance: Option<&str>,
    normalized_reject_kind: Option<&str>,
    observed_at_ms: u64,
) -> anyhow::Result<Option<OrderTerminalFact>> {
    let Some(key) = OrderLifecycleKey::from_intent(intent) else {
        return Ok(None);
    };
    if !is_ctf_market_entry_sell_key(&key) {
        return Ok(None);
    }
    let execution_intent_id = intent
        .execution_intent_id
        .filter(|intent_id| *intent_id > 0)
        .ok_or_else(|| {
            anyhow::anyhow!("managed-sell terminal submission has no execution intent identity")
        })?;
    let fact = OrderTerminalFact {
        execution_intent_id,
        order_id: order_id.map(str::to_string),
        market: key.market,
        cohort: key.cohort,
        symbol: key.symbol,
        token: key.token,
        side: key.side,
        close_reason: key.close_reason,
        provenance,
        instruction_qty_units_6dp: intent.execution_instruction_qty_units_6dp,
        matched_qty_units_6dp: 0,
        unfilled_qty_units_6dp: intent.execution_instruction_qty_units_6dp,
        raw_status: raw_status.map(str::to_string),
        raw_message_type: raw_message_type.map(str::to_string),
        raw_reason: raw_reason.map(str::to_string),
        raw_reason_provenance: raw_reason_provenance.map(str::to_string),
        normalized_reject_kind: normalized_reject_kind.map(str::to_string),
        observed_at_ms,
    };
    fact.validate()?;
    Ok(Some(fact))
}

#[derive(Debug, Default)]
pub(crate) struct OrderLifecycleSupervisor {
    matched_execution: MatchedExecutionState,
    execution_complete_order_ids: std::collections::HashSet<String>,
}

impl OrderLifecycleSupervisor {
    pub(crate) fn finish_execution_completions(
        &mut self,
        order_fill_ctx: &crate::polymarket::order_context::OrderFillContextRegistry,
    ) {
        for order_id in self.execution_complete_order_ids.drain() {
            order_fill_ctx.mark_execution_complete(&order_id);
        }
    }

    fn consume_confirmed_cancels(
        &mut self,
        open_orders: &mut OpenOrdersState,
        terminal_audits: &mut Vec<OrderTerminalFact>,
        now_ms: u64,
    ) -> anyhow::Result<bool> {
        let canceled = open_orders
            .orders
            .values()
            .filter(|order| order.status.eq_ignore_ascii_case("CANCELED"))
            .cloned()
            .collect::<Vec<_>>();
        let mut changed = false;
        for order in canceled {
            let metadata = crate::polymarket::order_context::OrderRuntimeMetadata {
                execution_intent_id: order.execution_intent_id,
                time_in_force: order.time_in_force.clone(),
                post_only: order.post_only,
                quote_level: order.quote_level,
                quote_group: order.quote_group.clone(),
                lifecycle_cohort: order.lifecycle_cohort.clone(),
                lifecycle_symbol: order.lifecycle_symbol.clone(),
                order_close_reason: order.order_close_reason,
            };
            let update = crate::orders::OrderUpdateEvent {
                ts_ms: order.last_update_ms,
                order_id: order.order_id,
                market: order.market,
                token: order.token,
                side: order.side,
                px: order.px,
                original_size: Some(order.original_size),
                matched_size: Some(order.matched_size),
                status: Some("CANCELED".to_string()),
                msg_type: Some("CONFIRMED_CANCEL".to_string()),
                terminal_reason: Some("CONFIRMED_CANCEL".to_string()),
                terminal_reason_provenance: Some("confirmed_central_cancel".to_string()),
            };
            match self.consume_closing_update(&update, Some(&metadata), open_orders, now_ms) {
                Ok(fact) => {
                    terminal_audits.extend(fact);
                    open_orders.apply_update_with_metadata_at(update, Some(&metadata), now_ms);
                    changed = true;
                }
                Err(error) => {
                    tracing::error!(
                        target: "execution.order_lifecycle",
                        order_id = %update.order_id,
                        error = %format!("{error:#}"),
                        "confirmed_cancel_lifecycle_reconciliation_failed"
                    );
                    open_orders.record_cancel_terminal(
                        update.order_id.as_str(),
                        now_ms,
                        "CONFIRMED_CANCEL",
                    );
                    return Err(error);
                }
            }
        }
        Ok(changed)
    }

    pub(crate) fn block_reason(
        &mut self,
        intent: &OrderIntent,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> Option<String> {
        if intent.side() != Side::Sell {
            return None;
        }
        self.refresh_from_open_orders(open_orders, now_ms);
        let key = OrderLifecycleKey::from_intent(intent);
        if let Err(error) = self
            .matched_execution
            .validate_first_target(intent, key.as_ref())
        {
            return Some(format!(
                "local_order_block ctf_first_target_invalid error={error:#}"
            ));
        }
        let key = key?;
        if is_ctf_market_entry_sell_key(&key) {
            if !self.matched_execution.sibling_ready(&key) {
                return Some(format!(
                    "local_order_block ctf_sibling_first_execution_incomplete requested_key={}",
                    key.label()
                ));
            }
            return market_entry_sell_block_reason(&key, intent, open_orders, now_ms);
        }
        let active_remaining = open_orders.active_lifecycle_remaining(&key, now_ms)?;
        if intent.qty > active_remaining + MATERIAL_QTY_EPS {
            return None;
        }
        Some(format!(
            "local_order_block order_lifecycle_active key={} active_remaining={:.6} requested_qty={:.6}",
            key.label(),
            active_remaining,
            intent.qty
        ))
    }

    pub(crate) fn retains_execution_intent(
        &self,
        intent: &OrderIntent,
        open_orders: &OpenOrdersState,
    ) -> bool {
        let Some(intent_id) = intent
            .execution_intent_id
            .filter(|intent_id| *intent_id > 0)
        else {
            return false;
        };
        OrderLifecycleKey::from_intent(intent)
            .and_then(|key| open_orders.active_order_lifecycles.get(&key))
            .is_some_and(|active| active.execution_intent_id == Some(intent_id))
    }

    pub(crate) fn normalized_instruction_block_reason(
        &self,
        intent: &OrderIntent,
    ) -> Option<String> {
        let key = OrderLifecycleKey::from_intent(intent)?;
        if !is_ctf_market_entry_sell_key(&key) {
            return None;
        }
        let actual_qty_units_6dp = match crate::ctf_accounting::qty_to_units_6dp(intent.qty) {
            Ok(units) => units,
            Err(error) => {
                return Some(format!(
                    "local_order_block ctf_market_entry_sell_normalized_qty_invalid requested_key={} error={error:#}",
                    key.label()
                ));
            }
        };
        let Some(actual_limit_px_units_8dp) =
            crate::polymarket::fixed_math::price_to_units_8dp(intent.limit_px)
        else {
            return Some(format!(
                "local_order_block ctf_market_entry_sell_normalized_price_invalid requested_key={}",
                key.label()
            ));
        };
        if actual_qty_units_6dp == intent.execution_instruction_qty_units_6dp
            && actual_limit_px_units_8dp == intent.execution_instruction_limit_px_units_8dp
        {
            return None;
        }
        Some(format!(
            "local_order_block ctf_market_entry_sell_instruction_drift requested_key={} owner_qty_units_6dp={} actual_qty_units_6dp={} owner_limit_px_units_8dp={} actual_limit_px_units_8dp={}",
            key.label(),
            intent.execution_instruction_qty_units_6dp,
            actual_qty_units_6dp,
            intent.execution_instruction_limit_px_units_8dp,
            actual_limit_px_units_8dp
        ))
    }

    pub(crate) fn consume_closing_update(
        &mut self,
        update: &crate::orders::OrderUpdateEvent,
        metadata: Option<&crate::polymarket::order_context::OrderRuntimeMetadata>,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> anyhow::Result<Option<OrderTerminalFact>> {
        if !update.is_closed() {
            return Ok(None);
        }
        let id = update.order_id.as_str();
        let mut matching = open_orders
            .active_order_lifecycles
            .iter()
            .filter(|(key, active)| {
                is_ctf_market_entry_sell_key(key) && active.order_id.as_deref() == Some(id.trim())
            })
            .map(|(key, active)| (key.clone(), active.clone()));
        let Some((key, active)) = matching.next() else {
            if update
                .matched_size
                .is_some_and(|matched| matched.is_finite() && matched == 0.0)
                && !open_orders
                    .orders
                    .get(id)
                    .is_some_and(|order| order.matched_size.is_finite() && order.matched_size > 0.0)
            {
                self.note_execution_complete(id);
            }
            return Ok(None);
        };
        anyhow::ensure!(
            matching.next().is_none(),
            "managed-sell closing update has ambiguous active lifecycle order_id={id}"
        );
        let metadata = metadata.ok_or_else(|| {
            anyhow::anyhow!("managed-sell closing update is missing runtime metadata order_id={id}")
        })?;
        anyhow::ensure!(
            matches!(&metadata.time_in_force, TimeInForce::Fak)
                && !metadata.post_only
                && metadata.order_close_reason == Some(key.close_reason)
                && metadata.lifecycle_cohort.as_deref() == Some(key.cohort.as_str())
                && metadata.lifecycle_symbol.as_deref() == Some(key.symbol.as_str()),
            "managed-sell closing update metadata differs from active lifecycle order_id={id}"
        );
        anyhow::ensure!(
            update.market == key.market && update.token == key.token && update.side == key.side,
            "managed-sell closing update identity differs from active lifecycle order_id={id}"
        );
        let reported_matched = update.matched_size.ok_or_else(|| {
            anyhow::anyhow!("managed-sell closing update is missing matched quantity order_id={id}")
        })?;
        anyhow::ensure!(
            reported_matched.is_finite() && reported_matched >= 0.0,
            "managed-sell closing update has invalid matched quantity order_id={id}"
        );
        let matched = open_orders
            .orders
            .get(id)
            .map(|order| order.matched_size)
            .filter(|matched| matched.is_finite() && *matched >= 0.0)
            .map_or(reported_matched, |known_matched| {
                known_matched.max(reported_matched)
            });
        let reported_matched_units_6dp = if matched == 0.0 {
            0
        } else {
            crate::ctf_accounting::qty_to_units_6dp(matched).map_err(|error| {
                anyhow::anyhow!(
                    "managed-sell closing update matched quantity is not exact order_id={id} error={error:#}"
                )
            })?
        };
        let matched_units_6dp = reported_matched_units_6dp
            .max(active.terminal_matched_qty_units_6dp.unwrap_or(0))
            .max(active.matched_execution_qty_units_6dp);
        let instruction_qty_units_6dp = active.instruction_qty_units_6dp;
        anyhow::ensure!(
            matched_units_6dp <= instruction_qty_units_6dp,
            "managed-sell closing update exceeds instruction quantity order_id={id} matched_units_6dp={matched_units_6dp} instruction_units_6dp={instruction_qty_units_6dp}"
        );
        let unfilled_qty_units_6dp = instruction_qty_units_6dp
            .checked_sub(matched_units_6dp)
            .ok_or_else(|| anyhow::anyhow!("managed-sell closing update quantity underflow"))?;
        let execution_intent_id = metadata
            .execution_intent_id
            .filter(|intent_id| *intent_id > 0)
            .ok_or_else(|| {
                anyhow::anyhow!("managed-sell terminal update has no intent identity")
            })?;
        if matched_units_6dp == 0 {
            let status = update.status.as_deref().unwrap_or_default().trim();
            anyhow::ensure!(
                !status.eq_ignore_ascii_case("MATCHED"),
                "managed-sell MATCHED terminal update has zero matched quantity order_id={id}"
            );
            retain_terminal_no_fill(key.clone(), &active, open_orders, now_ms);
        } else {
            open_orders.retain_terminal_match(&key, execution_intent_id, matched_units_6dp, now_ms);
        }
        let fact = OrderTerminalFact {
            execution_intent_id,
            order_id: Some(update.order_id.clone()),
            market: key.market,
            cohort: key.cohort,
            symbol: key.symbol,
            token: key.token,
            side: key.side,
            close_reason: key.close_reason,
            provenance: OrderTerminalProvenance::OrderUpdate,
            instruction_qty_units_6dp,
            matched_qty_units_6dp: matched_units_6dp,
            unfilled_qty_units_6dp,
            raw_status: update.status.clone(),
            raw_message_type: update.msg_type.clone(),
            raw_reason: update.terminal_reason.clone(),
            raw_reason_provenance: update.terminal_reason_provenance.clone(),
            normalized_reject_kind: None,
            observed_at_ms: update.ts_ms,
        };
        fact.validate()?;
        if matched_units_6dp == 0 {
            self.note_execution_complete(id);
        }
        Ok(Some(fact))
    }

    pub(crate) fn record_submitted(
        &mut self,
        open_orders: &mut OpenOrdersState,
        intent: &OrderIntent,
        order_id: &str,
        now_ms: u64,
    ) {
        let Some(key) = OrderLifecycleKey::from_intent(intent) else {
            return;
        };
        if !self.matched_execution.remember_first_target(&key, intent) {
            return;
        }
        let active_until_ms =
            if is_ctf_market_entry_sell_key(&key) && intent.time_in_force.is_immediate() {
                None
            } else {
                intent
                    .time_in_force
                    .is_immediate()
                    .then_some(now_ms.saturating_add(IMMEDIATE_ORDER_TERMINAL_WAIT_MS))
            };
        open_orders.record_active_lifecycle(
            key.clone(),
            Some(order_id.to_string()),
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_limit_px_units_8dp,
            intent.time_in_force.clone(),
            intent.post_only,
            intent.execution_evidence_revision,
            false,
            active_until_ms,
            now_ms,
        );
        if let Some(intent_id) = intent.execution_intent_id.filter(|value| *value > 0) {
            open_orders.bind_active_lifecycle_intent(&key, intent_id, now_ms);
        }
    }

    pub(crate) fn bind_pre_accept_update(
        &mut self,
        open_orders: &mut OpenOrdersState,
        intent: &OrderIntent,
        order_id: &str,
        now_ms: u64,
    ) -> anyhow::Result<(bool, bool, Option<OrderTerminalFact>)> {
        let Some(update) = open_orders.take_pre_accept_update(order_id) else {
            return Ok((false, false, None));
        };
        let terminal = update.is_closed();
        let matched = update
            .matched_size
            .is_some_and(|matched| matched.is_finite() && matched > 0.0);
        let metadata = crate::polymarket::order_context::OrderRuntimeMetadata {
            execution_intent_id: intent.execution_intent_id,
            time_in_force: intent.time_in_force.clone(),
            post_only: intent.post_only,
            quote_level: intent.quote_level,
            quote_group: intent.quote_group.clone(),
            lifecycle_cohort: intent.lifecycle_cohort.clone(),
            lifecycle_symbol: intent.lifecycle_symbol.clone(),
            order_close_reason: intent.order_close_reason(),
        };
        let terminal_fact =
            self.consume_closing_update(&update, Some(&metadata), open_orders, now_ms)?;
        open_orders.apply_update_with_metadata_at(update, Some(&metadata), now_ms);
        Ok((terminal, matched, terminal_fact))
    }

    pub(crate) fn note_execution_complete(&mut self, order_id: &str) {
        let order_id = order_id.trim();
        if !order_id.is_empty() {
            self.execution_complete_order_ids
                .insert(order_id.to_string());
        }
    }

    pub(crate) fn record_pending(
        &mut self,
        open_orders: &mut OpenOrdersState,
        intent: &OrderIntent,
        now_ms: u64,
    ) {
        let Some(key) = OrderLifecycleKey::from_intent(intent) else {
            return;
        };
        if !self.matched_execution.remember_first_target(&key, intent) {
            return;
        }
        open_orders.record_active_lifecycle(
            key.clone(),
            None,
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_limit_px_units_8dp,
            intent.time_in_force.clone(),
            intent.post_only,
            intent.execution_evidence_revision,
            false,
            None,
            now_ms,
        );
        if let Some(intent_id) = intent.execution_intent_id.filter(|value| *value > 0) {
            open_orders.bind_active_lifecycle_intent(&key, intent_id, now_ms);
        }
    }

    pub(crate) fn record_terminal(
        &mut self,
        open_orders: &mut OpenOrdersState,
        intent: &OrderIntent,
        now_ms: u64,
    ) {
        let Some(key) = OrderLifecycleKey::from_intent(intent) else {
            return;
        };
        open_orders.remove_active_attempt(&key, now_ms);
    }

    pub(crate) fn record_no_fill(
        &mut self,
        open_orders: &mut OpenOrdersState,
        intent: &OrderIntent,
        now_ms: u64,
    ) {
        let Some(key) = OrderLifecycleKey::from_intent(intent) else {
            return;
        };
        if intent.time_in_force.is_immediate() {
            if is_ctf_market_entry_sell_key(&key) {
                if let Some(active) = open_orders.active_order_lifecycles.get(&key).cloned() {
                    retain_terminal_no_fill(key, &active, open_orders, now_ms);
                }
            } else {
                open_orders.remove_active_lifecycle(&key, now_ms);
            }
            return;
        }
        open_orders.record_active_lifecycle(
            key,
            None,
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_qty_units_6dp,
            intent.execution_instruction_limit_px_units_8dp,
            intent.time_in_force.clone(),
            intent.post_only,
            intent.execution_evidence_revision,
            false,
            Some(now_ms.saturating_add(NO_FILL_RELEASE_MS)),
            now_ms,
        );
    }

    fn refresh_from_open_orders(&mut self, open_orders: &mut OpenOrdersState, now_ms: u64) {
        let active_orders = open_orders
            .orders
            .values()
            .filter_map(|order| {
                if order.side != Side::Sell {
                    return None;
                }
                let key = OrderLifecycleKey::from_open_order(order)?;
                let active = open_orders.active_order_lifecycles.get(&key)?;
                let remaining_qty_units_6dp =
                    crate::ctf_accounting::qty_to_units_6dp(order.remaining_size()).ok()?;
                Some((
                    key,
                    order.order_id.clone(),
                    remaining_qty_units_6dp,
                    active.instruction_qty_units_6dp,
                    active.instruction_limit_px_units_8dp,
                    order.time_in_force.clone(),
                    order.post_only,
                    active.execution_evidence_revision,
                ))
            })
            .collect::<Vec<_>>();
        for (
            key,
            order_id,
            remaining_qty_units_6dp,
            instruction_qty_units_6dp,
            instruction_limit_px_units_8dp,
            time_in_force,
            post_only,
            execution_evidence_revision,
        ) in active_orders
        {
            open_orders.record_active_lifecycle(
                key,
                Some(order_id),
                remaining_qty_units_6dp,
                instruction_qty_units_6dp,
                instruction_limit_px_units_8dp,
                time_in_force,
                post_only,
                execution_evidence_revision,
                false,
                None,
                now_ms,
            );
        }
    }
}

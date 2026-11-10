use super::{OpenOrdersState, OrderLifecycleKey};
use crate::types::TimeInForce;

#[derive(Debug, Clone, PartialEq)]
pub struct ActiveOrderLifecycle {
    pub order_id: Option<String>,
    pub remaining_qty_units_6dp: u128,
    pub instruction_qty_units_6dp: u128,
    pub instruction_limit_px_units_8dp: u64,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub execution_evidence_revision: u64,
    pub execution_intent_id: Option<u64>,
    pub terminal_matched_qty_units_6dp: Option<u128>,
    pub matched_execution_qty_units_6dp: u128,
    pub matched_execution_gross_units_6dp: u128,
    pub matched_execution_fee_units_6dp: Option<u128>,
    pub matched_execution_portfolio_revision: u64,
    pub terminal_no_fill: bool,
    pub active_until_ms: Option<u64>,
}

impl OpenOrdersState {
    pub fn active_lifecycle_remaining(
        &mut self,
        key: &OrderLifecycleKey,
        now_ms: u64,
    ) -> Option<f64> {
        self.refresh_active_lifecycles(now_ms);
        self.active_order_lifecycles
            .get(key)
            .map(|active| {
                active.remaining_qty_units_6dp as f64
                    / crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64
            })
            .filter(|qty| *qty > crate::polymarket::fixed_math::FIXED_6DP_QUANTUM)
    }

    pub fn active_lifecycle_remaining_readonly(
        &self,
        key: &OrderLifecycleKey,
        now_ms: u64,
    ) -> Option<f64> {
        let active = self.active_order_lifecycles.get(key)?;
        if active.matched_execution_qty_units_6dp == 0
            && active
                .active_until_ms
                .is_some_and(|active_until_ms| now_ms >= active_until_ms)
        {
            return None;
        }
        if let Some(matched_qty_units_6dp) = active
            .terminal_matched_qty_units_6dp
            .filter(|matched| *matched > 0)
        {
            return Some(
                matched_qty_units_6dp as f64
                    / crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64,
            );
        }
        if active.matched_execution_qty_units_6dp > 0 {
            return Some(
                active.matched_execution_qty_units_6dp as f64
                    / crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64,
            );
        }
        (active.remaining_qty_units_6dp > 1)
            .then_some(
                active.remaining_qty_units_6dp as f64
                    / crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64,
            )
            .filter(|qty| *qty > crate::polymarket::fixed_math::FIXED_6DP_QUANTUM)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_active_lifecycle(
        &mut self,
        key: OrderLifecycleKey,
        order_id: Option<String>,
        remaining_qty_units_6dp: u128,
        instruction_qty_units_6dp: u128,
        instruction_limit_px_units_8dp: u64,
        time_in_force: TimeInForce,
        post_only: bool,
        execution_evidence_revision: u64,
        terminal_no_fill: bool,
        active_until_ms: Option<u64>,
        now_ms: u64,
    ) {
        if instruction_qty_units_6dp <= 1
            || remaining_qty_units_6dp > instruction_qty_units_6dp
            || !(1..=crate::polymarket::fixed_math::FIXED_8DP_SCALE)
                .contains(&instruction_limit_px_units_8dp)
            || (post_only && time_in_force.is_immediate())
            || execution_evidence_revision == 0
            || (terminal_no_fill && (order_id.is_some() || active_until_ms.is_none()))
        {
            return;
        }
        let previous = self.active_order_lifecycles.get(&key).cloned();
        if remaining_qty_units_6dp <= 1 {
            self.active_order_lifecycles.remove(&key);
        } else {
            self.active_order_lifecycles.insert(
                key.clone(),
                ActiveOrderLifecycle {
                    order_id,
                    remaining_qty_units_6dp,
                    instruction_qty_units_6dp,
                    instruction_limit_px_units_8dp,
                    time_in_force,
                    post_only,
                    execution_evidence_revision,
                    execution_intent_id: previous
                        .as_ref()
                        .and_then(|active| active.execution_intent_id),
                    terminal_matched_qty_units_6dp: previous
                        .as_ref()
                        .and_then(|active| active.terminal_matched_qty_units_6dp),
                    matched_execution_qty_units_6dp: previous
                        .as_ref()
                        .map_or(0, |active| active.matched_execution_qty_units_6dp),
                    matched_execution_gross_units_6dp: previous
                        .as_ref()
                        .map_or(0, |active| active.matched_execution_gross_units_6dp),
                    matched_execution_fee_units_6dp: previous
                        .as_ref()
                        .and_then(|active| active.matched_execution_fee_units_6dp),
                    matched_execution_portfolio_revision: previous
                        .as_ref()
                        .map_or(0, |active| active.matched_execution_portfolio_revision),
                    terminal_no_fill,
                    active_until_ms,
                },
            );
        }
        if previous != self.active_order_lifecycles.get(&key).cloned() {
            self.refresh_orders_metadata(now_ms);
        }
    }

    pub(crate) fn bind_active_lifecycle_intent(
        &mut self,
        key: &OrderLifecycleKey,
        execution_intent_id: u64,
        now_ms: u64,
    ) {
        let Some(active) = self.active_order_lifecycles.get_mut(key) else {
            return;
        };
        if execution_intent_id == 0 || active.execution_intent_id == Some(execution_intent_id) {
            return;
        }
        active.execution_intent_id = Some(execution_intent_id);
        self.refresh_orders_metadata(now_ms);
    }

    pub(crate) fn record_matched_execution(
        &mut self,
        key: &OrderLifecycleKey,
        qty_units_6dp: u128,
        gross_units_6dp: u128,
        fee_units_6dp: Option<u128>,
        portfolio_revision: u64,
        now_ms: u64,
    ) -> bool {
        let Some(active) = self.active_order_lifecycles.get_mut(key) else {
            return false;
        };
        if qty_units_6dp == 0 || gross_units_6dp == 0 {
            return false;
        }
        let changed = active.matched_execution_qty_units_6dp != qty_units_6dp
            || active.matched_execution_gross_units_6dp != gross_units_6dp
            || active.matched_execution_fee_units_6dp != fee_units_6dp
            || active.matched_execution_portfolio_revision != portfolio_revision;
        if changed {
            active.matched_execution_qty_units_6dp = qty_units_6dp;
            active.matched_execution_gross_units_6dp = gross_units_6dp;
            active.matched_execution_fee_units_6dp = fee_units_6dp;
            active.matched_execution_portfolio_revision = portfolio_revision;
            self.refresh_orders_metadata(now_ms);
        }
        changed
    }

    pub(crate) fn clear_matched_execution(
        &mut self,
        key: &OrderLifecycleKey,
        portfolio_revision: u64,
        now_ms: u64,
    ) -> bool {
        let Some(active) = self.active_order_lifecycles.get_mut(key) else {
            return false;
        };
        let changed = active.matched_execution_qty_units_6dp != 0
            || active.matched_execution_gross_units_6dp != 0
            || active.matched_execution_fee_units_6dp.is_some()
            || active.matched_execution_portfolio_revision != portfolio_revision;
        if changed {
            active.matched_execution_qty_units_6dp = 0;
            active.matched_execution_gross_units_6dp = 0;
            active.matched_execution_fee_units_6dp = None;
            active.matched_execution_portfolio_revision = portfolio_revision;
            self.refresh_orders_metadata(now_ms);
        }
        changed
    }

    pub(crate) fn retain_terminal_match(
        &mut self,
        key: &OrderLifecycleKey,
        execution_intent_id: u64,
        matched_qty_units_6dp: u128,
        now_ms: u64,
    ) {
        let Some(active) = self.active_order_lifecycles.get_mut(key) else {
            return;
        };
        if execution_intent_id == 0
            || matched_qty_units_6dp == 0
            || matched_qty_units_6dp > active.instruction_qty_units_6dp
        {
            return;
        }
        active.execution_intent_id = Some(execution_intent_id);
        active.terminal_matched_qty_units_6dp = Some(matched_qty_units_6dp);
        active.remaining_qty_units_6dp = matched_qty_units_6dp;
        active.terminal_no_fill = false;
        active.active_until_ms = None;
        self.updated_at_ms = now_ms;
    }

    pub fn remove_active_lifecycle(&mut self, key: &OrderLifecycleKey, now_ms: u64) {
        if self.active_order_lifecycles.remove(key).is_some() {
            self.refresh_orders_metadata(now_ms);
        }
    }

    fn refresh_active_lifecycles(&mut self, now_ms: u64) {
        let orders = &self.orders;
        let mut changed = false;
        self.active_order_lifecycles.retain(|_, active| {
            if active.terminal_matched_qty_units_6dp.is_none()
                && active.matched_execution_qty_units_6dp == 0
                && active
                    .active_until_ms
                    .is_some_and(|active_until_ms| now_ms >= active_until_ms)
            {
                changed = true;
                return false;
            }
            if active.terminal_matched_qty_units_6dp.is_none() {
                if let Some(order_id) = active.order_id.as_deref() {
                    let Some(order) = orders.get(order_id) else {
                        return true;
                    };
                    let remaining_qty = order.remaining_size();
                    let remaining_qty_units_6dp =
                        if remaining_qty <= crate::polymarket::fixed_math::FIXED_6DP_QUANTUM {
                            0
                        } else {
                            let Ok(units) = crate::ctf_accounting::qty_to_units_6dp(remaining_qty)
                            else {
                                return true;
                            };
                            units
                        };
                    if active.remaining_qty_units_6dp != remaining_qty_units_6dp {
                        active.remaining_qty_units_6dp = remaining_qty_units_6dp;
                        changed = true;
                    }
                }
            }
            let keep = active
                .terminal_matched_qty_units_6dp
                .is_some_and(|matched| matched > 0)
                || active.matched_execution_qty_units_6dp > 0
                || active.remaining_qty_units_6dp > 1;
            if !keep {
                changed = true;
            }
            keep
        });
        if changed {
            self.refresh_orders_metadata(now_ms);
        }
    }

    pub fn has_active_lifecycle_order_id(&self, order_id: &str, now_ms: u64) -> bool {
        let order_id = order_id.trim();
        !order_id.is_empty()
            && self.active_order_lifecycles.values().any(|active| {
                active.order_id.as_deref() == Some(order_id)
                    && active
                        .active_until_ms
                        .is_none_or(|active_until_ms| now_ms < active_until_ms)
                    && active.remaining_qty_units_6dp > 1
            })
    }

    pub(super) fn remove_active_lifecycles_for_order_id(&mut self, order_id: &str, now_ms: u64) {
        let order_id = order_id.trim();
        if order_id.is_empty() {
            return;
        }
        let mut changed = false;
        self.active_order_lifecycles.retain(|_, active| {
            if active.order_id.as_deref() != Some(order_id) {
                return true;
            }
            changed = true;
            if active
                .terminal_matched_qty_units_6dp
                .is_some_and(|matched| matched > 0)
                || active.matched_execution_qty_units_6dp > 0
            {
                active.order_id = None;
                true
            } else {
                false
            }
        });
        if changed {
            self.updated_at_ms = now_ms;
        }
    }

    pub(crate) fn remove_active_attempt(&mut self, key: &OrderLifecycleKey, now_ms: u64) {
        let retain_matches = self
            .active_order_lifecycles
            .get(key)
            .is_some_and(|active| active.matched_execution_qty_units_6dp > 0);
        if retain_matches {
            if let Some(active) = self.active_order_lifecycles.get_mut(key) {
                active.order_id = None;
                active.active_until_ms = None;
            }
            self.refresh_orders_metadata(now_ms);
        } else {
            self.remove_active_lifecycle(key, now_ms);
        }
    }
}

use crate::orders::OrderLifecycleKey;
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::StrategyDecisionContext;
use crate::types::{OrderCloseReason, OrderIntent, OrderIntentFields, TimeInForce};

use super::super::super::{StrategyAction, StrategyDecision};
use super::PairedManagedSellRequest;

impl MakerRebateMmStrategy {
    pub(super) fn queue_paired_managed_sell(
        &mut self,
        request: PairedManagedSellRequest<'_>,
    ) -> bool {
        let PairedManagedSellRequest {
            fact_buffer,
            market,
            token,
            top,
            depth,
            fair,
            market_window_end_ms,
            permitted_inventory_units_6dp,
            permitted_inventory_qty,
            market_id,
            lifecycle_cohort,
            lifecycle_symbol,
            close_hint,
            require_positive_edge,
            authorization,
            now_ms,
        } = request;
        if !(permitted_inventory_qty.is_finite() && permitted_inventory_qty > 0.0) {
            return false;
        }
        let qty = permitted_inventory_qty;
        let Ok(execution_instruction_qty_units_6dp) = crate::ctf_accounting::qty_to_units_6dp(qty)
        else {
            return false;
        };
        if execution_instruction_qty_units_6dp != permitted_inventory_units_6dp
            || authorization.route.qty_units_6dp != permitted_inventory_units_6dp
            || authorization.route.qty.to_bits() != qty.to_bits()
        {
            return false;
        }
        let mut surface = self.evaluate_sell_surface(market, market_id, token, top, now_ms, false);
        let mode_label = close_hint;
        let mode_prefix = match close_hint {
            Self::PAIRED_POST_PROCEEDS_SELL_CLOSE_HINT => "paired_post_proceeds_sell",
            _ => "paired_first_managed_sell",
        };
        if !surface.book_current {
            self.note_sell_guard_block(
                fact_buffer,
                market_id,
                token,
                mode_label,
                Some(top),
                &surface.sell_state,
                now_ms,
            );
            return false;
        }
        let Some(tick) = self.validate_tick_size(token, top.tick_size, now_ms) else {
            self.note_sell_guard_block(
                fact_buffer,
                market_id,
                token,
                mode_label,
                Some(top),
                &surface.sell_state,
                now_ms,
            );
            return false;
        };

        let market_window_end_ms =
            market_window_end_ms.or_else(|| market.map(|market| market.window_end_ms));
        let in_market_window = if close_hint == Self::PAIRED_FIRST_MANAGED_SELL_CLOSE_HINT {
            market
                .zip(market_window_end_ms)
                .is_some_and(|(market, end_ms)| now_ms >= market.window_start_ms && now_ms < end_ms)
        } else {
            market_window_end_ms.is_some_and(|end_ms| now_ms < end_ms)
        };
        if !in_market_window {
            return false;
        }
        let final_limit_px = authorization.limit_px;
        if !(final_limit_px.is_finite() && final_limit_px > 0.0) {
            return false;
        }
        let Some(_depth) = depth.filter(|depth| {
            depth.revision == authorization.execution_evidence_revision
                && depth.observed_at_ms > 0
                && now_ms
                    .checked_sub(depth.observed_at_ms)
                    .is_some_and(|age_ms| {
                        age_ms <= self.cfg.paired_first_managed_sell_book_max_age_ms.max(1)
                    })
        }) else {
            return false;
        };
        let Some(tick_units_8dp) = crate::polymarket::fixed_math::price_to_units_8dp(tick) else {
            return false;
        };
        let Some(execution_instruction_limit_px_units_8dp) =
            crate::polymarket::fixed_math::price_to_units_8dp(final_limit_px)
        else {
            return false;
        };
        let Some(max_limit_px_units_8dp) =
            crate::polymarket::fixed_math::FIXED_8DP_SCALE.checked_sub(tick_units_8dp)
        else {
            return false;
        };
        if tick_units_8dp == 0
            || execution_instruction_limit_px_units_8dp < tick_units_8dp
            || execution_instruction_limit_px_units_8dp > max_limit_px_units_8dp
            || execution_instruction_limit_px_units_8dp % tick_units_8dp != 0
        {
            return false;
        }
        let ask_px = execution_instruction_limit_px_units_8dp as f64
            / crate::polymarket::fixed_math::FIXED_8DP_SCALE as f64;
        if crate::polymarket::fixed_math::price_to_units_8dp(ask_px)
            != Some(execution_instruction_limit_px_units_8dp)
        {
            return false;
        }
        surface.sell_state.sell_price_valid = ask_px.is_finite() && ask_px > 0.0;
        if super::super::super::market_quarantine::sell_block_reason(
            market_id,
            token,
            Some(top),
            &surface.sell_state,
        )
        .is_some()
        {
            self.note_sell_guard_block(
                fact_buffer,
                market_id,
                token,
                mode_label,
                Some(top),
                &surface.sell_state,
                now_ms,
            );
            return false;
        }
        if !(ask_px.is_finite() && ask_px > 0.0) {
            return false;
        }
        if require_positive_edge
            && ask_px <= fair + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
        {
            return false;
        }
        let close_reason = OrderCloseReason::from_order_str(mode_label)
            .expect("paired managed close hint is live close reason");
        let mut intent = OrderIntent::sell(
            OrderIntentFields {
                execution_intent_id: None,
                market_frame_received_at: None,
                market_state_published_at: None,
                market_connection_session_id: None,
                market_frame_sequence: None,
                market_state_revision: None,
                strategy_consumption_sequence: None,
                strategy_input_kind: None,
                decision_at: None,
                execution_evidence_revision: authorization.execution_evidence_revision,
                execution_instruction_qty_units_6dp,
                execution_instruction_limit_px_units_8dp,
                market_id: Some(market_id.to_string().into()),
                instrument: token.to_string(),
                qty,
                limit_px: ask_px,
                time_in_force: TimeInForce::Fak,
                post_only: false,
                quote_level: Some(0),
                quote_group: Some(format!("{mode_prefix}:{market_id}:{token}")),
                lifecycle_cohort: Some(lifecycle_cohort.to_string()),
                lifecycle_symbol: lifecycle_symbol.map(str::to_string),
            },
            close_reason,
        );
        if let OrderIntent::Sell(sell) = &mut intent {
            sell.first_target_qty_units_6dp = (close_reason
                == OrderCloseReason::PairedFirstManagedSell)
                .then_some(authorization.owner_outcome.fields().4);
        }
        let Some(lifecycle_key) = OrderLifecycleKey::from_intent(&intent) else {
            return false;
        };
        if self
            .open_orders_rx
            .as_ref()
            .and_then(|rx| {
                rx.borrow()
                    .active_lifecycle_remaining_readonly(&lifecycle_key, now_ms)
            })
            .is_some()
        {
            return false;
        }
        self.pending.push_back(StrategyDecision {
            action: StrategyAction::Order(intent),
            // Bind SELL_NOW owner fact to this order; remapped to execution_intent_id
            // when the worker assigns local intent and central rebinds it.
            context: Some(StrategyDecisionContext::CtfOwnedLegSell(
                authorization
                    .owner_outcome
                    .with_execution_action_qty_units_6dp(
                        execution_instruction_qty_units_6dp,
                    ),
            )),
            p_up: Some(fair),
            edge: Some(ask_px - fair),
            reason: format!(
                "{mode_prefix}_{mode_label}_fak token={token} inv={permitted_inventory_qty:.4} px={ask_px:.5} fair={fair:.5}"
            ),
        });
        let _ = fact_buffer;
        true
    }
}

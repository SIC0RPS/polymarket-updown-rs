// Strategy-owned market entry proposals. CTF complete-set operations remain
// separate from oracle-directed CLOB acquisitions.
#[path = "ctf_inventory/entry.rs"]
mod entry;

use super::market_ranking::CtfEntryCandidate;
use super::*;
use crate::polymarket::order_rules::truncate_to_vendor_qty;
use crate::strategy::facts::CtfLegDecisionReason;
use crate::strategy::hold_to_resolution::{CtfLegExecutableSell, PairTokenSide};
use crate::strategy::{full_set_qty_to_units_6dp, CtfSplitBookTop};
use polymarket_client_sdk::types::Decimal;
use std::str::FromStr as _;

fn finite_share_cap(value: f64) -> Option<f64> {
    value.is_finite().then_some(value.max(0.0))
}

fn ctf_clob_exitable_split_qty(qty: f64) -> f64 {
    truncate_to_vendor_qty(qty.max(0.0))
}

impl MakerRebateMmStrategy {
    pub(in crate::strategy) fn queue_oracle_buy_and_hold(
        &mut self,
        market: &CtfEntryCandidate,
        now_ms: u64,
    ) {
        let Some(condition_id) = market.selection.condition_id_b256() else {
            self.note_ctf_admission(&market.selection.market_id, "buy_condition_unavailable");
            self.note_quote_block("buy_condition_unavailable");
            return;
        };
        if !self
            .oracle_buy_entry_by_condition
            .contains_key(&condition_id)
        {
            let amount_units_6dp = match self.ctf_split_quantity_for_market(&market.selection) {
                Ok((amount_units_6dp, _, _, _)) => amount_units_6dp,
                Err(reason) => {
                    self.note_ctf_admission(&market.selection.market_id, reason);
                    self.note_quote_block(reason);
                    return;
                }
            };
            let basis = CtfSplitBookTop::from_book_top(&market.up)
                .and_then(|top| top.basis_weight_units_8dp())
                .zip(
                    CtfSplitBookTop::from_book_top(&market.down)
                        .and_then(|top| top.basis_weight_units_8dp()),
                )
                .and_then(|(up_weight, down_weight)| {
                    crate::ctf_accounting::allocate_units(amount_units_6dp, up_weight, down_weight)
                        .ok()
                });
            let Some((up_basis_units_6dp, down_basis_units_6dp)) = basis else {
                self.note_ctf_admission(&market.selection.market_id, "buy_entry_basis_unavailable");
                self.note_quote_block("buy_entry_basis_unavailable");
                return;
            };
            self.oracle_buy_entry_by_condition
                .retain(|_, reference| reference.market_end_ms > now_ms);
            self.oracle_buy_entry_by_condition.insert(
                condition_id,
                OracleBuyEntryReference {
                    admitted_at_ms: now_ms,
                    market_end_ms: market.selection.window_end_ms,
                    amount_units_6dp,
                    up_basis_units_6dp,
                    down_basis_units_6dp,
                    first_activation: None,
                },
            );
        }
        match self.oracle_buy_intent(market, now_ms) {
            Ok((order, p_up)) => {
                self.note_ctf_admission(&market.selection.market_id, "oracle_buy_intent");
                self.pending.push_back(crate::strategy::StrategyDecision {
                    action: crate::strategy::StrategyAction::Order(order),
                    context: None,
                    p_up,
                    edge: None,
                    reason: "oracle_buy_and_hold".to_string(),
                });
            }
            Err(reason) => {
                self.note_ctf_admission(&market.selection.market_id, reason);
                self.note_quote_block(reason);
            }
        }
    }

    fn oracle_buy_intent(
        &mut self,
        market: &CtfEntryCandidate,
        now_ms: u64,
    ) -> Result<(crate::types::OrderIntent, Option<f64>), &'static str> {
        use crate::types::{DirectBuyAuthorization, OrderIntent, OrderIntentFields, TimeInForce};

        let selection = &market.selection;
        let condition_id = selection
            .condition_id_b256()
            .ok_or("buy_condition_unavailable")?;
        let reference = self
            .oracle_buy_entry_by_condition
            .get(&condition_id)
            .copied()
            .ok_or("buy_entry_unavailable")?;
        let (winning_side, _activated_at_ms, prediction_at_ms, p_up) = if let Some(activation) =
            reference.first_activation
        {
            activation
        } else {
            let snapshot = self
                .oracle_authority_by_market
                .get(&selection.market_id)
                .copied()
                .ok_or("buy_prediction_unavailable")?;
            let winning_side = snapshot
                .oracle_signal_side
                .ok_or("buy_prediction_unavailable")?;
            let token_side = winning_side.opposite();
            let (top, token, basis_units_6dp) = match token_side {
                PairTokenSide::Up => (&market.up, selection.up_token, reference.up_basis_units_6dp),
                PairTokenSide::Down => (
                    &market.down,
                    selection.down_token,
                    reference.down_basis_units_6dp,
                ),
            };
            let limit_px = self
                .paired_managed_sell_limit_px(top)
                .ok_or("buy_first_sell_surface_unavailable")?;
            let bid = top.bid.ok_or("buy_first_sell_surface_unavailable")?;
            let ctf_signal_fair_px =
                crate::strategy::support::ctf_economics::side_fair_value(&snapshot, token_side);
            let executable = CtfLegExecutableSell {
                token_side,
                qty_units_6dp: reference.amount_units_6dp,
                qty: crate::strategy::full_set_units_to_qty(reference.amount_units_6dp),
                book_top: top.clone(),
                limit_px,
                tick_size: top.tick_size,
                fair_px: ctf_signal_fair_px.unwrap_or(bid).clamp(0.0, 1.0),
                ctf_signal_fair_px,
                depth: self.current_depth_for_token(self.odds.as_ref(), token),
            };
            let taker_base_fee_bps = selection
                .taker_base_fee_bps
                .filter(|fee_bps| *fee_bps >= 0)
                .map(f64::from)
                .ok_or("buy_fee_unavailable")?;
            let activation = self
                .ctf_first_activation(
                    &executable,
                    reference.amount_units_6dp,
                    reference.amount_units_6dp,
                    taker_base_fee_bps,
                    reference.admitted_at_ms,
                    reference.market_end_ms,
                    now_ms,
                    Some(snapshot),
                    0,
                    |instruction_qty_units_6dp| {
                        crate::ctf_accounting::allocation_increment(
                            basis_units_6dp,
                            0,
                            instruction_qty_units_6dp,
                            reference.amount_units_6dp,
                        )
                        .map_err(|_| CtfLegDecisionReason::ProjectionMissing)
                    },
                )
                .map_err(|_| "buy_first_activation_unavailable")?;
            if !activation.is_active() {
                return Err("buy_waiting_for_first_activation");
            }
            let activation = (
                winning_side,
                now_ms,
                snapshot.last_eval_ts_ms,
                snapshot.p_fair,
            );
            self.oracle_buy_entry_by_condition
                .get_mut(&condition_id)
                .expect("BUY entry reference disappeared during activation")
                .first_activation = Some(activation);
            activation
        };
        let selected_up = winning_side == PairTokenSide::Up;
        let prediction_at_ms = prediction_at_ms.ok_or("buy_prediction_unavailable")?;
        if !selection.standard_binary_ctf() {
            return Err("buy_market_not_standard_binary");
        }
        let time_left_ms = selection.window_end_ms.saturating_sub(now_ms);
        if time_left_ms
            < self
                .cfg
                .hard_entry_gate_ms()
                .max(crate::config::POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS)
        {
            return Err("buy_entry_time_cutoff");
        }
        if self.market_oracle_input(selection, now_ms).is_none() {
            return Err("buy_oracle_unavailable");
        }
        let top = if selected_up {
            &market.up
        } else {
            &market.down
        };
        let ask = top
            .ask
            .filter(|ask| ask.is_finite() && *ask > 0.0)
            .ok_or("buy_ask_unavailable")?;
        if !crate::strategy::support::shared::common::is_valid_tick_size(top.tick_size) {
            return Err("buy_tick_invalid");
        }
        let limit_px =
            crate::strategy::support::shared::common::round_to_tick(ask, top.tick_size, true);
        if !(limit_px > 0.0 && limit_px < 1.0) {
            return Err("buy_price_invalid");
        }
        let limit_px_units_8dp = crate::polymarket::fixed_math::price_to_units_8dp(limit_px)
            .ok_or("buy_price_invalid")?;
        let fee_rate_bps = selection
            .taker_base_fee_bps
            .map(f64::from)
            .filter(|rate| (0.0..=10_000.0).contains(rate))
            .ok_or("buy_fee_unavailable")?;
        let risk = self
            .risk_rx
            .as_ref()
            .ok_or("buy_risk_unavailable")?
            .borrow();
        if risk.enabled && !risk.trading_allowed {
            return Err("buy_risk_blocked");
        }
        let mut qty = self.cfg.max_inventory_per_token;
        if !(qty.is_finite() && qty > 0.0) {
            return Err("buy_inventory_disabled");
        }
        if risk.max_order_qty.units() > 0 {
            qty = qty.min(risk.max_order_qty.as_f64());
        }
        if risk.max_inventory_total.units() > 0 {
            qty = qty.min(risk.remaining_total_inventory_qty.as_f64());
        }
        let available_cash = u128::from(risk.available_buy_budget_usdt.units());
        let market_headroom = match risk.market_budgets.get(&selection.market_id) {
            Some(budget) => budget
                .effective_market_cap_notional
                .map(|_| u128::from(budget.remaining_market_headroom_usdt.units())),
            None if risk
                .market_budgets
                .values()
                .any(|budget| budget.effective_market_cap_notional.is_some()) =>
            {
                return Err("buy_market_budget_unavailable");
            }
            None => None,
        };
        let cash_budget =
            market_headroom.map_or(available_cash, |headroom| available_cash.min(headroom));
        let affordable_qty_units_6dp =
            crate::polymarket::fills::direct_buy_affordable_qty_units_6dp(
                cash_budget,
                limit_px_units_8dp,
                fee_rate_bps,
            )
            .map_err(|_| "buy_cash_requirement_invalid")?;
        let target_qty_units_6dp =
            crate::ctf_accounting::qty_to_units_6dp(truncate_to_vendor_qty(qty))
                .map_err(|_| "buy_quantity_invalid")?
                .min(affordable_qty_units_6dp);
        let qty = crate::strategy::full_set_units_to_qty(target_qty_units_6dp);
        if qty < self.venue_min_size(top).max(1.0) {
            return Err("buy_below_venue_min");
        }
        let principal_units_6dp = target_qty_units_6dp
            .checked_mul(u128::from(limit_px_units_8dp))
            .ok_or("buy_cash_requirement_invalid")?
            / u128::from(crate::polymarket::fixed_math::FIXED_8DP_SCALE);
        if principal_units_6dp < 5 * crate::polymarket::fixed_math::FIXED_6DP_SCALE {
            return Err("buy_below_principal_min");
        }
        let required_cash = crate::polymarket::fills::direct_buy_cash_requirement_units_6dp(
            target_qty_units_6dp,
            limit_px_units_8dp,
            fee_rate_bps,
        )
        .map_err(|_| "buy_cash_requirement_invalid")?;
        if required_cash > available_cash {
            return Err("buy_cash_budget_exceeded");
        }
        if market_headroom.is_some_and(|headroom| required_cash > headroom) {
            return Err("buy_market_budget_exceeded");
        }
        let authorization = DirectBuyAuthorization {
            intent_id: 0,
            condition_id,
            up_token: polymarket_client_sdk::types::U256::from_str(selection.up_token_id())
                .map_err(|_| "buy_token_invalid")?,
            down_token: polymarket_client_sdk::types::U256::from_str(selection.down_token_id())
                .map_err(|_| "buy_token_invalid")?,
            symbol: crate::strategy_runtime::SymbolId::parse(&selection.underlying)
                .map_err(|_| "buy_symbol_invalid")?,
            selected_up,
            fee_rate_bps_bits: fee_rate_bps.to_bits(),
            prediction_at_ms,
            authorized_at_ms: 0,
            target_qty_units_6dp,
            market_end_ms: selection.window_end_ms,
        };
        let instrument = authorization.selected_token().to_string();
        let quote_group = format!("oracle_buy_and_hold:{}:{instrument}", selection.market_id);
        Ok((
            OrderIntent::authorized_buy(
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
                    execution_evidence_revision: 0,
                    execution_instruction_qty_units_6dp: target_qty_units_6dp,
                    execution_instruction_limit_px_units_8dp: limit_px_units_8dp,
                    instrument,
                    market_id: Some(selection.market_id.clone()),
                    qty,
                    limit_px,
                    time_in_force: TimeInForce::Gtd {
                        expiration_ms: selection.window_end_ms,
                    },
                    post_only: false,
                    quote_level: Some(0),
                    quote_group: Some(quote_group),
                    lifecycle_cohort: None,
                    lifecycle_symbol: Some(selection.underlying.clone()),
                },
                authorization,
            ),
            p_up,
        ))
    }

    pub(in crate::strategy) fn ctf_split_quantity_for_market(
        &self,
        market: &MarketSelection,
    ) -> Result<(u128, f64, Option<f64>, f64), &'static str> {
        if self.cfg.max_inventory_per_token <= 0.0 {
            return Err("ctf_inventory_disabled");
        }
        let market_budget_share_cap = self
            .risk_rx
            .as_ref()
            .map(|risk_rx| {
                let risk = risk_rx.borrow();
                if !risk
                    .market_budgets
                    .values()
                    .any(|budget| budget.pct_cap_notional.is_some())
                {
                    return Ok(None);
                }
                let budget = risk
                    .market_budgets
                    .get(&market.market_id)
                    .ok_or("ctf_market_budget_missing")?;
                let Some(effective_cap) = budget.effective_market_cap_notional else {
                    return Err("ctf_market_budget_invalid");
                };
                if budget.market_id != market.market_id
                    || budget.pct_cap_notional.is_none()
                    || budget.remaining_market_headroom_usdt > effective_cap
                {
                    return Err("ctf_market_budget_invalid");
                }
                Ok(Some(budget.remaining_market_headroom_usdt.as_f64()))
            })
            .unwrap_or(Ok(None))?;
        let adverse_sized_qty = self
            .cfg
            .max_inventory_per_token
            .min(market_budget_share_cap.unwrap_or(f64::INFINITY));
        let split_qty = ctf_clob_exitable_split_qty(adverse_sized_qty);
        let split_qty_decimal = (split_qty.is_finite() && split_qty > 0.0)
            .then(|| Decimal::from_str(&format!("{split_qty:.6}")).ok())
            .flatten()
            .ok_or("ctf_split_amount_invalid")?;
        let amount_units_6dp =
            full_set_qty_to_units_6dp(split_qty_decimal).ok_or("ctf_split_amount_invalid")?;
        let share_qty = crate::strategy::full_set_units_to_qty(amount_units_6dp);
        Ok((
            amount_units_6dp,
            share_qty,
            market_budget_share_cap,
            adverse_sized_qty,
        ))
    }

    pub(in crate::strategy) fn queue_ctf_inventory_for_paired_market(
        &mut self,
        market: &CtfEntryCandidate,
        now_ms: u64,
    ) {
        self.queue_ctf_inventory_split(market, now_ms);
    }
}

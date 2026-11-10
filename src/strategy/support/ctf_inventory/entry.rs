use super::*;
use crate::strategy::{
    CtfSplitBookTop, CtfSplitEntryContext, InventoryIntent, InventoryOperation, StrategyAction,
    StrategyDecision, StrategyDecisionContext,
};

pub(super) struct CtfEntryTerms {
    pub(super) condition_id: polymarket_client_sdk::types::B256,
    pub(super) admitted_at_ms: u64,
    pub(super) amount_units_6dp: u128,
    pub(super) split_share_qty: f64,
    pub(super) market_budget_share_cap: Option<f64>,
    pub(super) adverse_sized_qty: f64,
    pub(super) min_quote_qty: f64,
    pub(super) up_book_top: CtfSplitBookTop,
    pub(super) down_book_top: CtfSplitBookTop,
}

impl MakerRebateMmStrategy {
    pub(super) fn ctf_entry_terms(
        &mut self,
        market: &CtfEntryCandidate,
        now_ms: u64,
    ) -> Option<CtfEntryTerms> {
        let Some(condition_id) = market.selection.condition_id_b256() else {
            self.note_ctf_admission(&market.selection.market_id, "missing_condition_id");
            self.note_quote_block_with_detail("ctf_split_blocked", "missing_condition_id".into());
            return None;
        };
        if !market.selection.standard_binary_ctf() {
            self.note_ctf_admission(
                &market.selection.market_id,
                "market_not_proven_standard_binary",
            );
            self.note_quote_block_with_detail(
                "ctf_split_blocked",
                "market_not_proven_standard_binary".into(),
            );
            return None;
        }
        // Hard entry cutoff: do not open a mandatory CTF complete set when the
        // market has less time left than configured. Soft ranking penalty alone
        // still allowed late splits that materialize after window end (CTF
        // latency ~5s), leaving inventory with no CLOB sell path (hold for
        // settlement only).
        let time_left_ms = market.selection.window_end_ms.saturating_sub(now_ms);
        if time_left_ms < self.cfg.hard_entry_gate_ms() {
            self.note_ctf_admission(
                &market.selection.market_id,
                format!(
                    "ctf_split_entry_time_cutoff time_left_ms={time_left_ms} hard_entry_gate_ms={}",
                    self.cfg.hard_entry_gate_ms()
                ),
            );
            self.note_quote_block("ctf_split_entry_time_cutoff");
            return None;
        }
        let has_redeemable_inventory = {
            let portfolio = self.portfolio_rx.borrow();
            let up = portfolio.position(market.selection.up_token_id());
            let down = portfolio.position(market.selection.down_token_id());
            up.is_some_and(|position| position.resolved_redeemable)
                || down.is_some_and(|position| position.resolved_redeemable)
        };
        if has_redeemable_inventory {
            self.note_ctf_admission(&market.selection.market_id, "ctf_redeemable_inventory");
            self.note_quote_block("ctf_redeemable_inventory");
            return None;
        }
        if self.cfg.max_inventory_per_token <= 0.0 {
            self.note_ctf_admission(&market.selection.market_id, "ctf_inventory_disabled");
            self.note_quote_block("ctf_inventory_disabled");
            return None;
        }
        let up_min = self.venue_min_size(&market.up);
        let down_min = self.venue_min_size(&market.down);
        let min_quote_qty = up_min.max(down_min).max(1.0);
        let complete_qty = {
            let portfolio = self.portfolio_rx.borrow();
            let up_inventory_qty = portfolio
                .position(market.selection.up_token_id())
                .map(|position| position.qty)
                .unwrap_or(0.0);
            let down_inventory_qty = portfolio
                .position(market.selection.down_token_id())
                .map(|position| position.qty)
                .unwrap_or(0.0);
            up_inventory_qty.min(down_inventory_qty).max(0.0)
        };
        if complete_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM >= min_quote_qty {
            self.note_ctf_admission(&market.selection.market_id, "ctf_complete_qty_merge_check");
            self.note_quote_block("ctf_complete_qty_merge_check");
            return None;
        }
        if self.pair_lifecycle_by_group.values().any(|state| {
            crate::strategy::pair_lifecycle::is_confirmed_ctf_split_cohort_id(&state.quote_group)
                && state.market_id == market.selection.market_id
                && state.up_token == market.selection.up_token_id()
                && state.down_token == market.selection.down_token_id()
        }) {
            self.note_ctf_admission(&market.selection.market_id, "ctf_lifecycle_already_exists");
            self.note_quote_block("ctf_lifecycle_already_exists");
            return None;
        }
        let (amount_units_6dp, split_share_qty, market_budget_share_cap, adverse_sized_qty) =
            match self.ctf_split_quantity_for_market(&market.selection) {
                Ok(quantity) => quantity,
                Err(reason) => {
                    self.note_ctf_admission(&market.selection.market_id, reason);
                    self.note_quote_block(reason);
                    return None;
                }
            };
        if adverse_sized_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < min_quote_qty {
            self.note_ctf_admission(
                &market.selection.market_id,
                format!(
                    "ctf_split_below_venue_min configured_split_qty={adverse_sized_qty:.6} min_quote_qty={min_quote_qty:.6}"
                ),
            );
            self.note_quote_block("ctf_split_below_venue_min");
            return None;
        }
        if split_share_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < min_quote_qty {
            self.note_ctf_admission(
                &market.selection.market_id,
                format!(
                    "ctf_split_below_venue_min split_share_qty={:.6} min_quote_qty={:.6}",
                    split_share_qty, min_quote_qty
                ),
            );
            self.note_quote_block("ctf_split_below_venue_min");
            return None;
        }
        let (Some(up_book_top), Some(down_book_top)) = (
            CtfSplitBookTop::from_book_top(&market.up),
            CtfSplitBookTop::from_book_top(&market.down),
        ) else {
            self.note_ctf_admission(&market.selection.market_id, "ctf_split_book_incomplete");
            self.note_quote_block("ctf_split_book_incomplete");
            return None;
        };
        Some(CtfEntryTerms {
            condition_id,
            admitted_at_ms: now_ms,
            amount_units_6dp,
            split_share_qty,
            market_budget_share_cap,
            adverse_sized_qty,
            min_quote_qty,
            up_book_top,
            down_book_top,
        })
    }

    pub(super) fn queue_ctf_inventory_split(&mut self, market: &CtfEntryCandidate, now_ms: u64) {
        let Some(CtfEntryTerms {
            condition_id,
            admitted_at_ms,
            amount_units_6dp,
            split_share_qty,
            market_budget_share_cap,
            adverse_sized_qty,
            min_quote_qty,
            up_book_top,
            down_book_top,
        }) = self.ctf_entry_terms(market, now_ms)
        else {
            return;
        };
        let oracle_authority = self
            .oracle_authority_by_market
            .get(&market.selection.market_id)
            .copied()
            .unwrap_or_default();
        // Contract: CTF split entry is not gated by strategy EV. Once venue,
        // inventory, and lifecycle constraints pass, the market-entry owner
        // must receive both legs and judge full lifecycle economics.
        let reason = format!(
            "ctf_split_complete_set market={} qty={:.6} target_per_side={:.6}",
            market.selection.market_id, split_share_qty, self.cfg.max_inventory_per_token,
        );
        self.note_ctf_admission(&market.selection.market_id, "split_intent");
        self.pending.push_back(StrategyDecision {
            action: StrategyAction::Inventory(InventoryIntent {
                operation: InventoryOperation::SplitCompleteSet,
                cohort_id: None,
                market_id: market.selection.market_id.clone(),
                gamma_market_id: Some(market.selection.gamma_market_id.clone()),
                condition_id,
                up_token: market.selection.up_token_id.clone(),
                down_token: market.selection.down_token_id.clone(),
                amount_units_6dp,
            }),
            context: Some(StrategyDecisionContext::CtfSplitEntry(
                CtfSplitEntryContext {
                    market_id: market.selection.market_id.to_string(),
                    condition_id: condition_id.to_string(),
                    gamma_market_id: market.selection.gamma_market_id.clone(),
                    market_underlying: market.selection.underlying.clone(),
                    entry_oracle_symbol: market.oracle_symbol.clone(),
                    up_token: market.selection.up_token_id.clone(),
                    down_token: market.selection.down_token_id.clone(),
                    qty: split_share_qty,
                    amount_units_6dp,
                    entry_raw_target_gap_qty: finite_share_cap(split_share_qty),
                    entry_min_quote_qty: finite_share_cap(min_quote_qty),
                    entry_market_budget_share_cap: market_budget_share_cap,
                    entry_total_inventory_share_cap: None,
                    entry_adverse_sized_qty: finite_share_cap(adverse_sized_qty),
                    entry_liquidity_manufacturer_base_qty: finite_share_cap(adverse_sized_qty),
                    entry_liquidity_manufacturer_manufactured_qty: finite_share_cap(
                        split_share_qty,
                    ),
                    entry_liquidity_manufacturer_cap_qty: None,
                    p_up: oracle_authority.p_fair,
                    model_p_up_raw: oracle_authority.p_up,
                    model_p_up: oracle_authority.model_p_up,
                    market_p_up: oracle_authority.market_p_up,
                    base_p_fair: oracle_authority.base_p_fair,
                    p_fair: oracle_authority.p_fair,
                    calibration_confidence: oracle_authority.calibration_confidence,
                    calibration_center: oracle_authority.calibration_center,
                    calibration_market_center_weight: oracle_authority
                        .calibration_market_center_weight,
                    calibration_spread_quality: oracle_authority.calibration_spread_quality,
                    calibration_depth_quality: oracle_authority.calibration_depth_quality,
                    beta_calibration_enabled: oracle_authority.beta_calibration_enabled,
                    beta_calibration_input_p: oracle_authority.beta_calibration_input_p,
                    beta_calibration_p_fair: oracle_authority.beta_calibration_p_fair,
                    beta_calibration_a: oracle_authority.beta_calibration_a,
                    beta_calibration_b: oracle_authority.beta_calibration_b,
                    beta_calibration_c: oracle_authority.beta_calibration_c,
                    beta_calibration_sample_count: oracle_authority.beta_calibration_sample_count,
                    beta_calibration_positive_count: oracle_authority
                        .beta_calibration_positive_count,
                    beta_calibration_brier_score: oracle_authority.beta_calibration_brier_score,
                    beta_calibration_log_loss: oracle_authority.beta_calibration_log_loss,
                    beta_calibration_uncertainty: oracle_authority.beta_calibration_uncertainty,
                    market_raw_p_up: oracle_authority.market_raw_p_up,
                    market_calibration_enabled: oracle_authority.market_calibration_enabled,
                    market_calibration_a: oracle_authority.market_calibration_a,
                    market_calibration_b: oracle_authority.market_calibration_b,
                    market_calibration_c: oracle_authority.market_calibration_c,
                    market_calibration_sample_count: oracle_authority
                        .market_calibration_sample_count,
                    market_calibration_positive_count: oracle_authority
                        .market_calibration_positive_count,
                    market_calibration_brier_score: oracle_authority.market_calibration_brier_score,
                    market_calibration_log_loss: oracle_authority.market_calibration_log_loss,
                    market_calibration_uncertainty: oracle_authority.market_calibration_uncertainty,
                    model_weight: oracle_authority.model_weight,
                    depth_pressure: oracle_authority.depth_pressure,
                    micro_queue_imbalance_l1: oracle_authority.micro_queue_imbalance_l1,
                    micro_queue_imbalance_l5: oracle_authority.micro_queue_imbalance_l5,
                    micro_ofi_depth: oracle_authority.micro_ofi_depth,
                    micro_microprice_gap_ticks: oracle_authority.micro_microprice_gap_ticks,
                    micro_signed_return_1s: oracle_authority.micro_signed_return_1s,
                    micro_signed_return_5s: oracle_authority.micro_signed_return_5s,
                    micro_volatility_state: oracle_authority.micro_volatility_state,
                    micro_jump_proxy: oracle_authority.micro_jump_proxy,
                    micro_periodic_hour_sin: oracle_authority.micro_periodic_hour_sin,
                    micro_periodic_hour_cos: oracle_authority.micro_periodic_hour_cos,
                    micro_periodic_minute_sin: oracle_authority.micro_periodic_minute_sin,
                    micro_periodic_minute_cos: oracle_authority.micro_periodic_minute_cos,
                    micro_feature_availability: oracle_authority.micro_feature_availability,
                    micro_logistic_enabled: oracle_authority.micro_logistic_enabled,
                    micro_logistic_p_up: oracle_authority.micro_logistic_p_up,
                    micro_logistic_logit: oracle_authority.micro_logistic_logit,
                    micro_logistic_feature_count: oracle_authority.micro_logistic_feature_count,
                    micro_logistic_l2_penalty: oracle_authority.micro_logistic_l2_penalty,
                    micro_logistic_fit_sample_count: oracle_authority
                        .micro_logistic_fit_sample_count,
                    micro_logistic_fit_positive_count: oracle_authority
                        .micro_logistic_fit_positive_count,
                    micro_logistic_fit_residual_trust_alpha: oracle_authority
                        .micro_logistic_fit_residual_trust_alpha,
                    micro_logistic_fit_brier_score: oracle_authority.micro_logistic_fit_brier_score,
                    micro_logistic_fit_log_loss: oracle_authority.micro_logistic_fit_log_loss,
                    micro_logistic_fit_prior_brier_score: oracle_authority
                        .micro_logistic_fit_prior_brier_score,
                    micro_logistic_fit_prior_log_loss: oracle_authority
                        .micro_logistic_fit_prior_log_loss,
                    micro_logistic_fit_weight_norm: oracle_authority.micro_logistic_fit_weight_norm,
                    micro_logistic_fit_weights: oracle_authority.micro_logistic_fit_weights,
                    unwind_up: oracle_authority.unwind_up,
                    unwind_down: oracle_authority.unwind_down,
                    adv_up: oracle_authority.adv_up,
                    adv_down: oracle_authority.adv_down,
                    adv_up_ewma: oracle_authority.adv_up_ewma,
                    adv_down_ewma: oracle_authority.adv_down_ewma,
                    uncertainty_edge_threshold: oracle_authority.uncertainty_edge_threshold,
                    uncertainty_edge_weight: oracle_authority.uncertainty_edge_weight,
                    theta_enter_up: oracle_authority.theta_enter_up,
                    theta_enter_down: oracle_authority.theta_enter_down,
                    oracle_side_confidence: oracle_authority.oracle_side_confidence,
                    market_confirmation: oracle_authority.market_confirmation,
                    market_confirmation_probability_agreement: oracle_authority
                        .market_confirmation_probability_agreement,
                    market_confirmation_spread_quality: oracle_authority
                        .market_confirmation_spread_quality,
                    market_confirmation_depth_support: oracle_authority
                        .market_confirmation_depth_support,
                    market_confirmation_book_complete: oracle_authority
                        .market_confirmation_book_complete,
                    shock_toxicity: None,
                    markout_toxicity: None,
                    ta_alignment: oracle_authority.ta_alignment,
                    ta_ema_trend: oracle_authority.ta_ema_trend,
                    ta_macd_trend: oracle_authority.ta_macd_trend,
                    ta_rsi_regime: oracle_authority.ta_rsi_regime,
                    ta_bollinger_position: oracle_authority.ta_bollinger_position,
                    ta_recent_return_sign: oracle_authority.ta_recent_return_sign,
                    ta_drift_score: oracle_authority.ta_drift_score,
                    oracle_side_reason: oracle_authority
                        .oracle_side_reason
                        .map(|reason| format!("{reason:?}")),
                    top_up_bid_delta: oracle_authority.top_up_bid_delta,
                    top_down_bid_delta: oracle_authority.top_down_bid_delta,
                    up_book_top,
                    down_book_top,
                    decision_ts_ms: admitted_at_ms,
                },
            )),
            p_up: oracle_authority.p_fair,
            edge: None,
            reason,
        });
    }
}

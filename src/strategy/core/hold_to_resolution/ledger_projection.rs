#[path = "ledger_projection/dead_cat.rs"]
mod dead_cat;

use crate::portfolio::PortfolioState;
use crate::strategy::facts::{CtfFirstInstructionEconomicsEvidence, CtfLegDecisionReason};
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::oracle_authority::OracleAuthoritySnapshot;

use super::{
    ctf_entry_closed_qty, ctf_entry_prior_net_proceeds, fak_sell_depth_valuation,
    protected_fak_sell_limit_px, size_aware_fak_sell_surface, CtfEntryLedgerProjection,
    CtfFirstExecutionSummary, CtfLegExecutableSell, PairTokenSide, INVENTORY_EPS,
};

pub(crate) struct CtfFirstActivation {
    instruction_qty_units_6dp: u128,
    execution_evidence_revision: u64,
    limit_px: f64,
    current: super::FakSellDepthValuation,
    economics: CtfFirstInstructionEconomicsEvidence,
    first_sell_allowed: bool,
}

impl CtfFirstActivation {
    pub(crate) fn is_active(&self) -> bool {
        self.first_sell_allowed
            && ctf_sell_surface_executable(
                self.current.fillable_qty,
                self.current.net,
                self.current.authorization_net,
                crate::polymarket::fixed_math::FIXED_6DP_QUANTUM,
            )
    }
}

pub(super) fn ctf_sell_surface_executable(
    fillable_qty: f64,
    net: f64,
    authorization_net: f64,
    quantum: f64,
) -> bool {
    fillable_qty > INVENTORY_EPS && net > quantum && authorization_net > quantum
}

impl MakerRebateMmStrategy {
    pub(crate) fn ctf_first_activation(
        &self,
        executable: &CtfLegExecutableSell,
        projected_qty_units_6dp: u128,
        first_remaining_qty_units_6dp: u128,
        taker_base_fee_bps: f64,
        entry_ms: u64,
        market_window_end_ms: u64,
        now_ms: u64,
        oracle_snapshot: Option<OracleAuthoritySnapshot>,
        active_release_remaining_ms: u64,
        basis_for_qty: impl FnOnce(u128) -> Result<u128, CtfLegDecisionReason>,
    ) -> Result<CtfFirstActivation, CtfLegDecisionReason> {
        let projected_qty = crate::strategy::full_set_units_to_qty(projected_qty_units_6dp);
        if !(projected_qty.is_finite() && projected_qty > INVENTORY_EPS) {
            return Err(CtfLegDecisionReason::NoLifecycleOwnedQty);
        }
        if !(taker_base_fee_bps.is_finite() && taker_base_fee_bps >= 0.0) {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let base_limit_px = executable.limit_px;
        if !(base_limit_px.is_finite() && base_limit_px > 0.0) {
            return Err(CtfLegDecisionReason::InvalidLimitPx);
        }
        let depth = executable
            .depth
            .as_ref()
            .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
        let execution_evidence_revision = (depth.revision > 0)
            .then_some(depth.revision)
            .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
        let Some((supported_qty, aggressive_limit_px, max_supported_limit_px)) =
            size_aware_fak_sell_surface(
                &executable.book_top,
                Some(depth),
                projected_qty,
                INVENTORY_EPS,
                base_limit_px,
                executable.tick_size,
                self.cfg.ctf_sell_limit_delay_cushion_ticks,
            )
        else {
            return Err(CtfLegDecisionReason::CtfSellLimitDepthNotProven);
        };
        let instruction_qty_units_6dp = crate::ctf_accounting::qty_to_units_6dp(supported_qty)
            .ok()
            .filter(|units| *units > 0)
            .map(|units| units.min(projected_qty_units_6dp))
            .ok_or(CtfLegDecisionReason::NoFillableQty)?;
        let instruction_qty = crate::strategy::full_set_units_to_qty(instruction_qty_units_6dp);
        let basis_units_6dp = basis_for_qty(instruction_qty_units_6dp)?;
        let mut first_upper_valuation = None;
        let basis = crate::strategy::full_set_units_to_qty(basis_units_6dp);
        if !(basis.is_finite() && basis >= 0.0) {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let deadline_ms =
            crate::strategy::pair_lifecycle::ctf_first_deadline_ms(market_window_end_ms);
        let span_ms = deadline_ms
            .checked_sub(entry_ms)
            .filter(|span_ms| *span_ms > 0);
        let post_timeout_ms =
            u64::try_from(crate::execution::http_support::ROUTED_POST_TIMEOUT.as_millis())
                .unwrap_or(u64::MAX);
        let attempt_allowance_ms = post_timeout_ms
            .saturating_add(crate::execution::order_lifecycle::NO_FILL_RELEASE_MS)
            .saturating_add(self.cfg.fast_rebalance_interval_ms);
        let attempts =
            u64::try_from(first_remaining_qty_units_6dp.div_ceil(instruction_qty_units_6dp))
                .unwrap_or(u64::MAX);
        let execution_budget_ms = active_release_remaining_ms
            .saturating_add(attempts.saturating_mul(attempt_allowance_ms));
        let mandatory_start_ms = deadline_ms
            .saturating_sub(execution_budget_ms)
            .saturating_sub(self.cfg.normal_rebalance_interval_ms);
        let mandatory_urgency = span_ms.is_none() || now_ms >= mandatory_start_ms;
        let upper_probability = match executable.token_side {
            PairTokenSide::Up => oracle_snapshot.and_then(|snapshot| snapshot.p_up_upper_bound),
            PairTokenSide::Down => oracle_snapshot
                .and_then(|snapshot| snapshot.p_up_lower_bound)
                .map(|p_up_lower| 1.0 - p_up_lower),
        }
        .filter(|probability| probability.is_finite() && (0.0..=1.0).contains(probability));
        let discretionary_reservation = match (span_ms, upper_probability) {
            (Some(span_ms), Some(probability)) => {
                let allocation =
                    (now_ms.saturating_sub(entry_ms) as f64 / span_ms as f64).clamp(0.0, 1.0);
                let upper_value = instruction_qty * probability;
                first_upper_valuation = Some(upper_value);
                let basis_with_epsilon = basis + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
                (1.0 - allocation) * basis_with_epsilon.max(upper_value)
                    + allocation * basis.min(upper_value)
            }
            _ => basis + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM,
        };
        let first_reservation = if mandatory_urgency {
            0.0
        } else {
            discretionary_reservation
        };
        let limit_px = if !mandatory_urgency {
            protected_fak_sell_limit_px(
                instruction_qty,
                aggressive_limit_px,
                max_supported_limit_px,
                executable.tick_size,
                taker_base_fee_bps,
                depth.bids.len(),
                first_reservation,
            )
            .unwrap_or(aggressive_limit_px)
        } else {
            aggressive_limit_px
        };
        if !(limit_px.is_finite() && limit_px > 0.0) {
            return Err(CtfLegDecisionReason::InvalidLimitPx);
        }
        let current =
            fak_sell_depth_valuation(Some(depth), instruction_qty, limit_px, taker_base_fee_bps)
                .filter(|valuation| {
                    valuation.fillable_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
                        >= instruction_qty
                })
                .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
        if !(current.fillable_qty.is_finite()
            && current.net.is_finite()
            && current.authorization_net.is_finite())
        {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let first_sell_allowed =
            mandatory_urgency || current.authorization_net >= first_reservation;
        let economics = CtfFirstInstructionEconomicsEvidence {
            instruction_limit_px_bits: limit_px.to_bits(),
            depth_gross_proceeds_bits: current.gross.to_bits(),
            depth_fee_bits: current.fees.to_bits(),
            depth_net_proceeds_bits: current.net.to_bits(),
            authorization_fee_bits: current.authorization_fees.to_bits(),
            authorization_net_proceeds_bits: current.authorization_net.to_bits(),
            delay_cushion_bits: current.slippage_reserve.to_bits(),
            allocated_basis_units_6dp: basis_units_6dp,
            upper_valuation_bits: first_upper_valuation.map(f64::to_bits),
            reservation_bits: first_reservation.to_bits(),
            mandatory_urgency,
        };
        Ok(CtfFirstActivation {
            instruction_qty_units_6dp,
            execution_evidence_revision,
            limit_px,
            current,
            economics,
            first_sell_allowed,
        })
    }

    pub(super) fn ctf_entry_ledger_projection(
        &self,
        portfolio: &PortfolioState,
        cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
        token: &str,
        executable: CtfLegExecutableSell,
        projected_qty_units_6dp: u128,
        first_remaining_qty_units_6dp: u128,
        taker_base_fee_bps: Option<i32>,
        first_execution: Option<CtfFirstExecutionSummary>,
        market_window_end_ms: u64,
        now_ms: u64,
        is_first_instruction: bool,
    ) -> Result<CtfEntryLedgerProjection, CtfLegDecisionReason> {
        let qty = crate::strategy::full_set_units_to_qty(projected_qty_units_6dp);
        if !(qty.is_finite() && qty > INVENTORY_EPS) {
            return Err(CtfLegDecisionReason::NoLifecycleOwnedQty);
        }
        let taker_base_fee_bps = taker_base_fee_bps
            .filter(|fee_bps| *fee_bps >= 0)
            .map(f64::from)
            .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
        let direct_buy = cohort.direct_buy_authorization();
        if is_first_instruction && direct_buy.is_some() {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let (confirmed_prior_net_units_6dp, prior_net_proceeds) =
            ctf_entry_prior_net_proceeds(portfolio, cohort, first_execution)
                .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
        let up_closed_qty = ctf_entry_closed_qty(portfolio, cohort, PairTokenSide::Up)
            .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
        let down_closed_qty = ctf_entry_closed_qty(portfolio, cohort, PairTokenSide::Down)
            .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
        let up_open_qty = portfolio
            .position(&cohort.up_token)
            .map(|position| position.qty.max(0.0))
            .unwrap_or(0.0);
        let down_open_qty = portfolio
            .position(&cohort.down_token)
            .map(|position| position.qty.max(0.0))
            .unwrap_or(0.0);
        let entry_qty = (up_closed_qty + up_open_qty)
            .max(down_closed_qty + down_open_qty)
            .max(cohort.matched_qty)
            .max(cohort.remaining_owned_qty);
        if !(entry_qty.is_finite() && entry_qty > INVENTORY_EPS) {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let entry_cost =
            crate::strategy::full_set_units_to_qty(cohort.ctf_entry_outlay_units_6dp());
        if !(entry_cost.is_finite() && (entry_cost > 0.0 || direct_buy.is_some())) {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let first_sell_side = self
            .pair_lifecycle_by_group
            .get(cohort.quote_group.as_str())
            .and_then(|state| {
                [PairTokenSide::Up, PairTokenSide::Down]
                    .into_iter()
                    .find(|side| {
                        matches!(
                            state.ctf_owned_leg_disposition(*side),
                            Some(
                                crate::strategy::pair_lifecycle::CtfOwnedLegDisposition::SellNow {
                                    close_reason:
                                        crate::types::OrderCloseReason::PairedFirstManagedSell,
                                    ..
                                }
                            )
                        )
                    })
            });
        let first_sell_authorized = first_sell_side.is_some();
        let base_limit_px = executable.limit_px;
        if !(base_limit_px.is_finite() && base_limit_px > 0.0) {
            return Err(CtfLegDecisionReason::InvalidLimitPx);
        }
        let projected_qty = qty;
        if !(projected_qty.is_finite() && projected_qty > INVENTORY_EPS) {
            return Err(CtfLegDecisionReason::NoLifecycleOwnedQty);
        }
        // First instructions may use the currently supported residual quantity.
        // Sibling instructions retain the existing full-clear depth requirement.
        let depth_pricing = first_sell_authorized || is_first_instruction || direct_buy.is_some();
        let oracle_snapshot = self
            .oracle_authority_by_market
            .get(&cohort.market_id)
            .copied();
        let mut confirmed_first_cohort_qty_units_6dp = None;
        let mut first_activation = None;
        let mut max_supported_limit_px = None;
        let (instruction_qty_units_6dp, execution_evidence_revision, limit_px, current) =
            if is_first_instruction {
                let activation = self.ctf_first_activation(
                    &executable,
                    projected_qty_units_6dp,
                    first_remaining_qty_units_6dp,
                    taker_base_fee_bps,
                    cohort
                        .ctf_split_entry_ts_ms()
                        .ok_or(CtfLegDecisionReason::ProjectionMissing)?,
                    market_window_end_ms,
                    now_ms,
                    oracle_snapshot,
                    first_execution
                        .filter(|summary| summary.token_side == executable.token_side)
                        .map_or(0, |summary| summary.active_release_remaining_ms),
                    |instruction_qty_units_6dp| {
                        let pending_qty_units_6dp = first_execution
                            .filter(|summary| summary.token_side == executable.token_side)
                            .map_or(0, |summary| summary.pending_qty_units_6dp);
                        let (basis_units_6dp, confirmed_cohort_qty_units_6dp) = portfolio
                            .confirmed_ctf_first_basis_units_6dp(
                                token,
                                cohort.quote_group.as_str(),
                                instruction_qty_units_6dp,
                                pending_qty_units_6dp,
                            )
                            .map_err(|_| CtfLegDecisionReason::ProjectionMissing)?;
                        confirmed_first_cohort_qty_units_6dp = Some(confirmed_cohort_qty_units_6dp);
                        Ok(basis_units_6dp)
                    },
                )?;
                let projection = (
                    activation.instruction_qty_units_6dp,
                    activation.execution_evidence_revision,
                    activation.limit_px,
                    activation.current,
                );
                first_activation = Some(activation);
                projection
            } else if depth_pricing {
                let depth = executable
                    .depth
                    .as_ref()
                    .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
                let execution_evidence_revision = (depth.revision > 0)
                    .then_some(depth.revision)
                    .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
                let Some((_, aggressive_limit_px, supported_limit_px)) =
                    size_aware_fak_sell_surface(
                        &executable.book_top,
                        Some(depth),
                        projected_qty,
                        INVENTORY_EPS,
                        base_limit_px,
                        executable.tick_size,
                        self.cfg.ctf_sell_limit_delay_cushion_ticks,
                    )
                else {
                    return Err(CtfLegDecisionReason::CtfSellLimitDepthNotProven);
                };
                max_supported_limit_px = Some(supported_limit_px);
                let instruction_qty_units_6dp = projected_qty_units_6dp;
                let instruction_qty =
                    crate::strategy::full_set_units_to_qty(instruction_qty_units_6dp);
                let limit_px = aggressive_limit_px;
                if !(limit_px.is_finite() && limit_px > 0.0) {
                    return Err(CtfLegDecisionReason::InvalidLimitPx);
                }
                let current = fak_sell_depth_valuation(
                    Some(depth),
                    instruction_qty,
                    limit_px,
                    taker_base_fee_bps,
                )
                .filter(|valuation| {
                    valuation.fillable_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
                        >= instruction_qty
                })
                .ok_or(CtfLegDecisionReason::CtfSellLimitDepthNotProven)?;
                (
                    instruction_qty_units_6dp,
                    execution_evidence_revision,
                    limit_px,
                    current,
                )
            } else {
                let top = &executable.book_top;
                let Some(bid) = top.bid else {
                    return Err(CtfLegDecisionReason::CtfEntryNoExecutableSellSurface);
                };
                let fillable_qty = top
                    .bid_sz
                    .filter(|size| size.is_finite() && *size > 0.0)
                    .map(|size| size.min(projected_qty))
                    .unwrap_or(0.0);
                let normalized_qty =
                    crate::polymarket::order_rules::truncate_to_vendor_qty(fillable_qty);
                let instruction_qty_units_6dp =
                    crate::ctf_accounting::qty_to_units_6dp(normalized_qty)
                        .ok()
                        .filter(|units| *units > 0)
                        .map(|units| units.min(projected_qty_units_6dp))
                        .ok_or(CtfLegDecisionReason::NoFillableQty)?;
                let instruction_qty =
                    crate::strategy::full_set_units_to_qty(instruction_qty_units_6dp);
                if !(bid.is_finite()
                    && bid > 0.0
                    && instruction_qty > INVENTORY_EPS
                    && base_limit_px.is_finite()
                    && base_limit_px > 0.0)
                {
                    return Err(CtfLegDecisionReason::CtfEntryNoExecutableSellSurface);
                }
                let gross = bid * instruction_qty;
                let fees = crate::polymarket::fills::documented_fee_usdc_for_taker_base_fee_bps(
                    bid,
                    instruction_qty,
                    taker_base_fee_bps,
                )
                .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
                let authorization_fees =
                    crate::polymarket::fills::documented_fee_usdc_for_taker_base_fee_bps(
                        base_limit_px,
                        instruction_qty,
                        taker_base_fee_bps,
                    )
                    .ok_or(CtfLegDecisionReason::ProjectionMissing)?;
                let net = (gross - fees).max(0.0);
                let slippage_reserve = instruction_qty * (bid - base_limit_px).max(0.0);
                let execution_evidence_revision = executable
                    .depth
                    .as_ref()
                    .map(|depth| depth.revision)
                    .filter(|revision| *revision > 0)
                    .ok_or(CtfLegDecisionReason::CtfEntryNoExecutableSellSurface)?;
                (
                    instruction_qty_units_6dp,
                    execution_evidence_revision,
                    base_limit_px,
                    super::FakSellDepthValuation {
                        fillable_qty: instruction_qty,
                        gross,
                        fees,
                        net,
                        slippage_reserve,
                        authorization_net: (gross - slippage_reserve - authorization_fees).max(0.0),
                        authorization_fees,
                    },
                )
            };
        let current_net_proceeds = current.net;
        let current_authorization_net_proceeds = current.authorization_net;
        let exit_fair_px = executable.fair_px.clamp(0.0, 1.0);
        let exit_fair_value = exit_fair_px * current.fillable_qty;
        let ctf_signal_fair_value = executable
            .ctf_signal_fair_px
            .filter(|px| px.is_finite())
            .map(|px| px.clamp(0.0, 1.0) * current.fillable_qty);
        let current_fair_value =
            if ctf_signal_fair_value.is_some_and(|value| value > exit_fair_value) {
                ctf_signal_fair_value.unwrap_or(exit_fair_value)
            } else {
                exit_fair_value
            };
        if !(prior_net_proceeds.is_finite()
            && current.fillable_qty.is_finite()
            && current_net_proceeds.is_finite()
            && current_authorization_net_proceeds.is_finite()
            && (!is_first_instruction || current_fair_value.is_finite()))
        {
            return Err(CtfLegDecisionReason::ProjectionMissing);
        }
        let dead_cat_reference = dead_cat::reference(
            portfolio,
            cohort,
            token,
            executable.token_side,
            first_execution,
        );
        let dead_cat_price_below_reference = !is_first_instruction
            && direct_buy.is_none()
            && dead_cat::price_below_reference(current, dead_cat_reference);
        let oracle_signal_side = oracle_snapshot.and_then(|snapshot| snapshot.oracle_signal_side);
        let p_fair = oracle_snapshot.and_then(|snapshot| snapshot.p_fair);
        let first_sell_allowed = direct_buy.is_none()
            && first_activation
                .as_ref()
                .map_or(true, |activation| activation.first_sell_allowed);
        let first_instruction_economics = first_activation.map(|activation| activation.economics);
        let mut projection = CtfEntryLedgerProjection {
            instruction_qty_units_6dp,
            confirmed_first_cohort_qty_units_6dp,
            execution_evidence_revision,
            prior_net_proceeds,
            current_fillable_qty: current.fillable_qty,
            current_net_proceeds,
            current_authorization_net_proceeds,
            current_fair_value,
            exit_fair_value,
            entry_cost,
            limit_px,
            first_instruction_economics,
            first_sell_allowed,
            dead_cat_price_below_reference,
            dead_cat_instruction: None,
            oracle_signal_side,
            p_fair,
        };
        if direct_buy.is_some() {
            projection.dead_cat_instruction = dead_cat::protected_buy_instruction(
                &executable,
                &projection,
                current,
                oracle_snapshot,
                cohort.remaining_owned_qty_units_6dp,
                cohort.ctf_entry_outlay_units_6dp(),
                confirmed_prior_net_units_6dp,
                max_supported_limit_px,
                taker_base_fee_bps,
                dead_cat_reference,
            );
        }
        Ok(projection)
    }
}

// Core paired-maker contract: classifies paired inventory and terminal
// settlement eligibility for protected winner hold, redeem, and merge.
use super::pair_lifecycle::{
    CtfOwnedLegDisposition, PairLifecycleEvent, PairLifecycleReducer, PairedCohortKey,
};
use crate::orders::OrderLifecycleKey;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::state::{BookDepth, BookTop};
use crate::portfolio::PortfolioState;
use crate::strategy::facts::CtfLegDecisionReason;
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::types::Side;

#[path = "hold_to_resolution/ctf_leg_economics.rs"]
mod ctf_leg_economics;
#[path = "hold_to_resolution/disposition_projection.rs"]
mod disposition_projection;
#[path = "hold_to_resolution/inventory_projection.rs"]
mod inventory_projection;
#[path = "hold_to_resolution/ledger_match.rs"]
mod ledger_match;
#[path = "hold_to_resolution/ledger_projection.rs"]
mod ledger_projection;
#[path = "hold_to_resolution/paired_action.rs"]
mod paired_action;
#[path = "hold_to_resolution/route_helpers.rs"]
mod route_helpers;
#[path = "hold_to_resolution/sell_surface.rs"]
mod sell_surface;

use ctf_leg_economics::{
    ctf_market_entry_decisions, CtfEntryLedgerProjection, CtfMarketEntryLegSnapshot,
};
pub(crate) use inventory_projection::{classify_inventory, InventoryPartition};
use ledger_match::{
    ctf_entry_closed_qty, ctf_entry_prior_net_proceeds, ctf_first_execution_summary,
    CtfFirstExecutionSummary,
};
use ledger_projection::ctf_sell_surface_executable;
pub(crate) use paired_action::{PairedAction, PairedSellRoute};
use route_helpers::paired_sell_route_from_action;
use sell_surface::{
    fak_sell_depth_valuation, protected_fak_sell_limit_px, size_aware_fak_sell_surface,
};

pub(super) const INVENTORY_EPS: f64 = 1e-6;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PairTokenSide {
    Up,
    Down,
}

impl PairTokenSide {
    pub(crate) const fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtectedInventoryTerminalState {
    MatchedPairMergeCandidate,
    Redeemable,
}

impl ProtectedInventoryTerminalState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MatchedPairMergeCandidate => "merge_candidate",
            Self::Redeemable => "redeemable",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CtfLegExecutableSell {
    pub(crate) token_side: PairTokenSide,
    pub(crate) qty_units_6dp: u128,
    pub(crate) qty: f64,
    pub(crate) book_top: BookTop,
    pub(crate) limit_px: f64,
    pub(crate) tick_size: f64,
    pub(crate) fair_px: f64,
    pub(crate) ctf_signal_fair_px: Option<f64>,
    pub(crate) depth: Option<BookDepth>,
}

#[derive(Debug, Clone)]
pub(crate) struct CtfLegDecisionInput {
    pub(crate) token: String,
    pub(crate) token_side: PairTokenSide,
    pub(crate) owned_qty_units_6dp: u128,
    pub(crate) owned_qty: f64,
    pub(crate) executable: Option<CtfLegExecutableSell>,
    pub(crate) route_block_reason: Option<CtfLegDecisionReason>,
}

#[derive(Debug, Clone)]
pub(crate) struct CtfLegSellAuthorization {
    pub(crate) token: String,
    pub(crate) route: PairedSellRoute,
    pub(crate) limit_px: f64,
    pub(crate) execution_evidence_revision: u64,
    pub(crate) owner_outcome: crate::strategy::facts::CtfOwnedLegOutcomeFact,
}

struct CtfMarketEntryDecisionLeg {
    token: String,
    token_side: PairTokenSide,
    causal_winner_side: Option<PairTokenSide>,
    owned_qty_units_6dp: u128,
    projection: Option<CtfEntryLedgerProjection>,
    fresh_first_instruction_authorized: bool,
}

struct CtfMarketEntryDecisionSnapshot {
    legs: Vec<(CtfMarketEntryDecisionLeg, CtfMarketEntryLegSnapshot)>,
    canonical_winner_side: Option<PairTokenSide>,
}

struct CtfMarketEntryDisposition {
    leg_decisions: Vec<(
        CtfMarketEntryDecisionLeg,
        ctf_leg_economics::CtfMarketEntryLegDecision,
    )>,
}

impl CtfMarketEntryDecisionSnapshot {
    fn from_candidates(
        candidates: Vec<(String, CtfMarketEntryLegSnapshot)>,
        causal_winner_side: Option<PairTokenSide>,
    ) -> Self {
        let legs = candidates
            .into_iter()
            .map(|(token, snapshot)| {
                (
                    CtfMarketEntryDecisionLeg {
                        token,
                        token_side: snapshot.token_side,
                        causal_winner_side,
                        owned_qty_units_6dp: snapshot.owned_qty_units_6dp,
                        projection: snapshot.projection,
                        fresh_first_instruction_authorized: false,
                    },
                    snapshot,
                )
            })
            .collect();
        Self {
            legs,
            canonical_winner_side: causal_winner_side,
        }
    }

    fn disposition(
        self,
        first_execution_complete: bool,
        direct_buy_side: Option<PairTokenSide>,
    ) -> CtfMarketEntryDisposition {
        let (legs, snapshots): (Vec<_>, Vec<_>) = self.legs.into_iter().unzip();
        let mut decisions = ctf_market_entry_decisions(
            snapshots,
            self.canonical_winner_side,
            first_execution_complete,
            direct_buy_side,
        );
        let leg_decisions = legs
            .into_iter()
            .map(|mut leg| {
                let decision_index = decisions
                    .iter()
                    .position(|(token_side, _)| *token_side == leg.token_side)
                    .expect("ctf market-entry owner omitted a lifecycle-owned leg decision");
                let (_, decision) = decisions.remove(decision_index);
                if let Some(instruction) = decision.selected_dead_cat_instruction {
                    leg.projection = leg
                        .projection
                        .map(|projection| projection.with_dead_cat_instruction(instruction));
                }
                leg.fresh_first_instruction_authorized =
                    decision.fresh_first_instruction_authorized;
                (leg, decision)
            })
            .collect();
        CtfMarketEntryDisposition { leg_decisions }
    }
}

impl MakerRebateMmStrategy {
    pub(in crate::strategy) fn ctf_owned_leg_authorizations_for_market(
        &mut self,
        fact_buffer: &mut crate::strategy::StrategyFactSink,
        portfolio: &PortfolioState,
        cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
        market_id: &str,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
        mut decision_inputs: Vec<CtfLegDecisionInput>,
    ) -> Vec<CtfLegSellAuthorization> {
        let direct_buy_side = cohort.direct_buy_authorization().map(|authorization| {
            if authorization.selected_up {
                PairTokenSide::Up
            } else {
                PairTokenSide::Down
            }
        });
        if let Some(side) = direct_buy_side {
            decision_inputs.retain(|input| input.token_side == side);
        }
        let first_authorization = self
            .pair_lifecycle_by_group
            .get(cohort.quote_group.as_str())
            .filter(|_| direct_buy_side.is_none())
            .and_then(|state| {
                [PairTokenSide::Up, PairTokenSide::Down]
                    .into_iter()
                    .find_map(|side| match state.ctf_owned_leg_disposition(side) {
                        Some(CtfOwnedLegDisposition::SellNow {
                            authorized_qty_units_6dp,
                            close_reason: crate::types::OrderCloseReason::PairedFirstManagedSell,
                            ..
                        }) => Some((side, authorized_qty_units_6dp)),
                        _ => None,
                    })
            });
        let open_orders = self.open_orders_rx.as_ref().map(|rx| rx.borrow().clone());
        let lifecycle_symbol = market_projection
            .market_by_id(market_id)
            .map(|market| market.underlying.as_str())
            .unwrap_or_default();
        let first_execution = first_authorization.map(|(side, target)| {
            ctf_first_execution_summary(
                portfolio,
                open_orders.as_ref(),
                cohort,
                lifecycle_symbol,
                side,
                target,
                now_ms,
            )
        });
        let first_execution_complete = first_execution.is_some_and(|summary| summary.complete);
        let causal_winner_side = first_authorization
            .map(|(side, _)| side.opposite())
            .or_else(|| {
                self.oracle_authority_by_market
                    .get(&cohort.market_id)
                    .and_then(|snapshot| snapshot.oracle_signal_side)
            });
        let first_side = causal_winner_side
            .filter(|_| direct_buy_side.is_none())
            .map(PairTokenSide::opposite);
        let market_window_end_ms = market_projection
            .market_by_id(market_id)
            .map_or(0, |market| market.window_end_ms);
        let candidates = self.ctf_market_entry_candidates(
            portfolio,
            cohort,
            market_id,
            market_projection,
            &decision_inputs,
            first_execution,
            first_side,
            market_window_end_ms,
            now_ms,
        );
        if candidates.is_empty() {
            return Vec::new();
        }
        let mut disposition =
            CtfMarketEntryDecisionSnapshot::from_candidates(candidates, causal_winner_side)
                .disposition(first_execution_complete, direct_buy_side);
        if let Some(state) = self
            .pair_lifecycle_by_group
            .get(cohort.quote_group.as_str())
            .filter(|_| direct_buy_side.is_none())
        {
            for (leg, decision) in &mut disposition.leg_decisions {
                if let Some(existing @ CtfOwnedLegDisposition::SellNow { close_reason, .. }) =
                    state.ctf_owned_leg_disposition(leg.token_side)
                {
                    if close_reason == crate::types::OrderCloseReason::PairedFirstManagedSell
                        || (close_reason == crate::types::OrderCloseReason::PairedPostProceedsSell
                            && ctf_sibling_submission_active(
                                open_orders.as_ref(),
                                cohort,
                                lifecycle_symbol,
                                leg.token_side,
                                now_ms,
                            ))
                    {
                        decision.disposition = existing;
                    }
                }
            }
        }
        if direct_buy_side.is_none() {
            let up = disposition
                .leg_decisions
                .iter()
                .find(|(leg, _)| leg.token_side == PairTokenSide::Up)
                .map(|(_, decision)| decision.disposition)
                .expect("CTF strategy omitted the UP owned-leg disposition");
            let down = disposition
                .leg_decisions
                .iter()
                .find(|(leg, _)| leg.token_side == PairTokenSide::Down)
                .map(|(_, decision)| decision.disposition)
                .expect("CTF strategy omitted the DOWN owned-leg disposition");
            let state = self
                .pair_lifecycle_by_group
                .get_mut(cohort.quote_group.as_str())
                .expect("authoritative CTF cohort omitted reducer state");
            PairLifecycleReducer::apply_state(
                state,
                PairLifecycleEvent::CtfOwnedLegDispositionsObserved { up, down },
            );
        }
        let first_side_latched = first_authorization.is_some();
        let retain_first_decision_samples =
            self.micro_logistic_trainer.retains_first_decision_samples();
        let first_authorization_trace_enabled = tracing::enabled!(
            target: "strategy.oracle_probability",
            tracing::Level::TRACE
        );
        let fresh_first_instruction_authorized = (retain_first_decision_samples
            || first_authorization_trace_enabled)
            && disposition
                .leg_decisions
                .iter()
                .any(|(leg, _)| leg.fresh_first_instruction_authorized);
        let retain_first_decision = fresh_first_instruction_authorized
            && !first_side_latched
            && retain_first_decision_samples;
        let first_snapshot = if (fresh_first_instruction_authorized && !first_side_latched)
            && (retain_first_decision || first_authorization_trace_enabled)
        {
            self.oracle_authority_by_market
                .get(&cohort.market_id)
                .map(|snapshot| (snapshot.last_eval_ts_ms, snapshot.p_fair))
        } else {
            None
        };
        if retain_first_decision {
            if let Some(market) = market_projection.market_by_id(market_id) {
                self.micro_logistic_trainer.retain_first_authorization(
                    market,
                    now_ms,
                    first_snapshot.and_then(|(last_eval_ts_ms, _)| last_eval_ts_ms),
                );
            }
        }

        let first_authorization_trace = first_authorization_trace_enabled.then_some((
            fresh_first_instruction_authorized,
            first_side_latched,
            first_snapshot,
        ));
        if let Some((true, false, first_snapshot)) = first_authorization_trace {
            let selected_first_side = first_side.map(|side| match side {
                PairTokenSide::Up => "up",
                PairTokenSide::Down => "down",
            });
            let causal_winner_side = causal_winner_side.map(|side| match side {
                PairTokenSide::Up => "up",
                PairTokenSide::Down => "down",
            });
            let snapshot_last_eval_ts_ms =
                first_snapshot.and_then(|(last_eval_ts_ms, _)| last_eval_ts_ms);
            let actual_first_p_up = first_snapshot.and_then(|(_, p_fair)| p_fair);
            tracing::trace!(
                target: "strategy.oracle_probability",
                event = "oracle_first_direction_selected",
                market_id = %market_id,
                selection_ts_ms = now_ms,
                selected_first_side = ?selected_first_side,
                causal_winner_side = ?causal_winner_side,
                snapshot_last_eval_ts_ms = ?snapshot_last_eval_ts_ms,
                actual_first_p_up = ?actual_first_p_up,
            );
        }
        let mut authorizations = self.paired_ctf_sell_routes_for_market(
            fact_buffer,
            portfolio,
            cohort,
            market_id,
            now_ms,
            disposition,
        );
        authorizations.retain(|authorization| {
            authorization.route.close_reason
                != crate::types::OrderCloseReason::PairedPostProceedsSell
                || direct_buy_side.is_some()
                || first_execution_complete
        });
        if let Some((true, first_side_latched, first_snapshot)) = first_authorization_trace {
            if authorizations.iter().any(|authorization| {
                authorization.route.close_reason
                    == crate::types::OrderCloseReason::PairedFirstManagedSell
            }) {
                let snapshot_last_eval_ts_ms =
                    first_snapshot.and_then(|(last_eval_ts_ms, _)| last_eval_ts_ms);
                let actual_first_p_up = first_snapshot.and_then(|(_, p_fair)| p_fair);
                let selected_first_side = first_side.map(|side| match side {
                    PairTokenSide::Up => "up",
                    PairTokenSide::Down => "down",
                });
                tracing::trace!(
                    target: "strategy.oracle_probability",
                    event = "oracle_first_authorization",
                    market_id = %market_id,
                    authorization_ts_ms = now_ms,
                    first_side_latched,
                    fresh_direction_chosen = !first_side_latched,
                    selected_first_side = ?selected_first_side,
                    snapshot_last_eval_ts_ms = ?snapshot_last_eval_ts_ms,
                    actual_first_p_up = ?actual_first_p_up,
                );
            }
        }
        authorizations
    }
}

fn ctf_sibling_submission_active(
    open_orders: Option<&crate::orders::OpenOrdersState>,
    cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
    symbol: &str,
    side: PairTokenSide,
    now_ms: u64,
) -> bool {
    let token = match side {
        PairTokenSide::Up => cohort.up_token.as_str(),
        PairTokenSide::Down => cohort.down_token.as_str(),
    };
    open_orders.is_some_and(|orders| {
        orders
            .active_lifecycle_remaining_readonly(
                &OrderLifecycleKey {
                    market: cohort.market_id.to_string(),
                    token: token.to_string(),
                    side: Side::Sell,
                    close_reason: crate::types::OrderCloseReason::PairedPostProceedsSell,
                    cohort: cohort.quote_group.clone(),
                    symbol: symbol.to_string(),
                },
                now_ms,
            )
            .is_some()
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct FakSellDepthValuation {
    fillable_qty: f64,
    gross: f64,
    fees: f64,
    net: f64,
    slippage_reserve: f64,
    authorization_fees: f64,
    authorization_net: f64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SellSurfaceEvaluation {
    pub book_current: bool,
    pub sell_state: super::market_quarantine::SellSurfaceState,
}

impl MakerRebateMmStrategy {
    pub(super) fn queue_unknown_inventory_cancel(
        &mut self,
        market_id: &str,
        token: &str,
        reason: &str,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) {
        let gamma_market_id =
            Self::market_selection_for_token_in_projection(market_projection, token, market_id)
                .map(|selection| selection.gamma_market_id().to_string());
        self.queue_cancel_market_request(
            crate::polymarket::identity::MarketId::new(market_id),
            gamma_market_id,
            now_ms,
            format!(
                "classification_unknown_cancel market={market_id} token={token} reason={reason}"
            ),
        );
    }

    pub(super) fn enter_paired_cohort_unknown(
        &mut self,
        key: &PairedCohortKey,
        market_id: &str,
        token: &str,
        reason: &str,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) {
        let mut emit_epoch = false;
        if let Some(cohort) = self.paired_cohorts.get_mut(key) {
            let reason_changed = cohort.last_unknown_reason.as_deref() != Some(reason);
            if reason_changed {
                cohort.reconciliation_epoch = cohort.reconciliation_epoch.saturating_add(1);
                cohort.last_unknown_reason = Some(reason.to_string());
            }
            if cohort.last_unknown_cancel_epoch != cohort.reconciliation_epoch {
                cohort.last_unknown_cancel_epoch = cohort.reconciliation_epoch;
                emit_epoch = true;
            }
        } else {
            self.queue_unknown_inventory_cancel(
                market_id,
                token,
                reason,
                now_ms,
                market_projection,
            );
            return;
        }
        if emit_epoch {
            self.queue_unknown_inventory_cancel(
                market_id,
                token,
                reason,
                now_ms,
                market_projection,
            );
        }
    }

    pub(super) fn evaluate_sell_surface(
        &mut self,
        market: Option<&MarketSelection>,
        _market_id: &str,
        token: &str,
        top: &BookTop,
        now_ms: u64,
        sell_price_valid: bool,
    ) -> SellSurfaceEvaluation {
        let valid_tick =
            crate::strategy::support::shared::common::is_valid_tick_size(top.tick_size);
        let book_valid = valid_tick && top.bid.is_some_and(|bid| bid.is_finite() && bid > 0.0);
        let book_current = book_valid && self.paired_managed_sell_book_is_current(top, now_ms);
        let sell_state = self.sell_surface_state_for_selection(
            market,
            token,
            Some(top),
            book_valid,
            book_current,
            valid_tick,
            sell_price_valid,
        );
        SellSurfaceEvaluation {
            book_current,
            sell_state,
        }
    }

    pub(super) fn paired_sell_route_for_token_market(
        &mut self,
        portfolio: &PortfolioState,
        token: &str,
        market_id: &str,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> Option<PairedSellRoute> {
        let context =
            self.resolve_paired_disposition_context(token, market_id, now_ms, market_projection);
        let market_disposition = self.build_paired_market_disposition(portfolio, context.as_ref());
        let disposition = self.derive_paired_token_disposition(
            portfolio,
            token,
            market_id,
            market_disposition,
            now_ms,
            market_projection,
        );
        let route = disposition.action.as_ref().and_then(|action| {
            let cohort = disposition.market.cohort.as_ref();
            if cohort.is_some_and(|cohort| cohort.strategy_closed()) {
                None
            } else {
                paired_sell_route_from_action(action, cohort)
            }
        });
        if route.is_some_and(|route| {
            route.close_reason == crate::types::OrderCloseReason::PairedPostProceedsSell
        }) {
            let cohort = disposition.market.cohort.as_ref()?;
            let (first_side, target_qty_units_6dp) = self
                .pair_lifecycle_by_group
                .get(cohort.quote_group.as_str())
                .and_then(|state| {
                    [PairTokenSide::Up, PairTokenSide::Down]
                        .into_iter()
                        .find_map(|side| match state.ctf_owned_leg_disposition(side) {
                            Some(CtfOwnedLegDisposition::SellNow {
                                authorized_qty_units_6dp,
                                close_reason:
                                    crate::types::OrderCloseReason::PairedFirstManagedSell,
                                ..
                            }) => Some((side, authorized_qty_units_6dp)),
                            _ => None,
                        })
                })?;
            let symbol = market_projection
                .market_by_id(market_id)
                .map(|market| market.underlying.as_str())?;
            let first_execution = if let Some(rx) = self.open_orders_rx.as_ref() {
                let open_orders = rx.borrow();
                ctf_first_execution_summary(
                    portfolio,
                    Some(&open_orders),
                    cohort,
                    symbol,
                    first_side,
                    target_qty_units_6dp,
                    now_ms,
                )
            } else {
                ctf_first_execution_summary(
                    portfolio,
                    None,
                    cohort,
                    symbol,
                    first_side,
                    target_qty_units_6dp,
                    now_ms,
                )
            };
            if !first_execution.complete {
                return None;
            }
        }
        if self
            .open_orders_rx
            .as_ref()
            .and_then(|rx| {
                let cohort = disposition.market.cohort.as_ref()?;
                let close_reason = route?.close_reason;
                let open_orders = rx.borrow();
                open_orders.active_lifecycle_remaining_readonly(
                    &OrderLifecycleKey {
                        market: market_id.to_string(),
                        token: token.to_string(),
                        side: Side::Sell,
                        close_reason,
                        cohort: cohort.quote_group.clone(),
                        symbol: market_projection
                            .market_by_id(market_id)
                            .map(|market| market.underlying.clone())?,
                    },
                    now_ms,
                )
            })
            .is_some()
        {
            return None;
        }
        route
    }
}

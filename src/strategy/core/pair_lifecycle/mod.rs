// Core paired-maker contract: authoritative paired cohort and lifecycle state.
mod cohort_sync;
mod ctf_authority;
mod helpers;
mod journal;
mod logic;
mod lookups;
mod materialization;
mod oracle_gate;
mod post_proceeds_sell_observation;
mod reducer;
pub(crate) mod types;

use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::portfolio::PortfolioState;
use crate::strategy::support::oracle_authority::OracleAuthorityMode;
use std::collections::HashSet;

use super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use crate::strategy::paired_maker::{MakerRebateMmStrategy, PAIR_LIFECYCLE_STALE_MS};

use logic::{inventory_eps, paired_release_qty_eps};
pub(crate) use logic::{resolve_non_ctf_inventory_owner, retain_pair_lifecycle_state};
pub(crate) use reducer::{CtfOwnedLegDisposition, PairLifecycleEvent, PairLifecycleReducer};
pub(crate) use types::{
    AuthoritativePairedCohortView, PairLifecycleDurabilityState, PairLifecycleState,
    PairLifecycleStateInit, PairedCohort, PairedCohortKey, PostProceedsSellState,
    ResolvedInventoryOwner, ResolvedInventoryOwnerKind, SiblingCancelState,
};

pub(crate) use crate::ctf_inventory::is_confirmed_ctf_split_cohort_id;

pub(crate) const fn ctf_first_deadline_ms(market_window_end_ms: u64) -> u64 {
    market_window_end_ms.saturating_sub(240_000)
}

fn ctf_unknown_reason(
    quote_group: &str,
    protected_side: Option<PairTokenSide>,
    owner_kind: ResolvedInventoryOwnerKind,
    effective_winner: Option<PairTokenSide>,
    remaining_owned_side: Option<PairTokenSide>,
    terminal_state: Option<ProtectedInventoryTerminalState>,
    sibling_cancel_state: SiblingCancelState,
    last_update_ms: u64,
    now_ms: u64,
) -> Option<String> {
    if !is_confirmed_ctf_split_cohort_id(quote_group) {
        return None;
    }
    if protected_side.is_some() || terminal_state.is_some() {
        return None;
    }
    if !is_confirmed_ctf_split_cohort_id(quote_group) && effective_winner.is_some() {
        return None;
    }
    if owner_kind == ResolvedInventoryOwnerKind::MatchedHold && remaining_owned_side.is_some() {
        return None;
    }
    let explicit_no_sell_authority_transient = now_ms.saturating_sub(last_update_ms)
        <= PAIR_LIFECYCLE_STALE_MS
        && matches!(
            owner_kind,
            ResolvedInventoryOwnerKind::AcceptedPair
                | ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg
        )
        && matches!(
            sibling_cancel_state,
            SiblingCancelState::Pending | SiblingCancelState::Acked
        );
    if explicit_no_sell_authority_transient {
        return None;
    }
    remaining_owned_side.map(|_| "ctf_inventory_classification_unknown".to_string())
}

fn remaining_owned_side_for_positions(
    portfolio: &PortfolioState,
    up_token: &str,
    down_token: &str,
) -> Option<PairTokenSide> {
    let up_qty = portfolio
        .position(up_token)
        .map(|position| position.qty.max(0.0))
        .unwrap_or(0.0);
    let down_qty = portfolio
        .position(down_token)
        .map(|position| position.qty.max(0.0))
        .unwrap_or(0.0);
    let matched_qty = up_qty.min(down_qty).max(0.0);
    let up_remaining_owned_qty = (up_qty - matched_qty).max(0.0);
    let down_remaining_owned_qty = (down_qty - matched_qty).max(0.0);
    if up_remaining_owned_qty > inventory_eps() {
        Some(PairTokenSide::Up)
    } else if down_remaining_owned_qty > inventory_eps() {
        Some(PairTokenSide::Down)
    } else {
        None
    }
}

fn ctf_strategy_lifecycle_should_close(
    state: &PairLifecycleState,
    up_qty_units_6dp: u128,
    down_qty_units_6dp: u128,
) -> bool {
    is_confirmed_ctf_split_cohort_id(&state.quote_group)
        && !state.strategy_closed()
        && up_qty_units_6dp == 0
        && down_qty_units_6dp == 0
}

fn trading_winner_for_cohort(cohort: &AuthoritativePairedCohortView) -> Option<PairTokenSide> {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        cohort.effective_winner().or(cohort.predictive_winner())
    } else {
        cohort
            .protected_side()
            .or(cohort.effective_winner())
            .or(cohort.predictive_winner())
    }
}

pub(crate) fn cohort_is_loser_remaining_owned_leg(cohort: &AuthoritativePairedCohortView) -> bool {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        cohort.owner_kind() == ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg
            && cohort.remaining_owned_side().is_some()
    } else {
        cohort.owner_kind() == ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg
            && trading_winner_for_cohort(cohort)
                .zip(cohort.remaining_owned_side())
                .is_some_and(|(winner, remaining_owned)| winner != remaining_owned)
    }
}

pub(crate) fn cohort_is_directional_remaining_owned_leg(
    cohort: &AuthoritativePairedCohortView,
) -> bool {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        cohort.owner_kind() == ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg
            && cohort.terminal_state().is_none()
            && cohort.remaining_owned_side().is_some()
    } else {
        cohort.owner_kind() == ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg
            && cohort.effective_winner().is_none()
            && cohort
                .predictive_winner()
                .zip(cohort.remaining_owned_side())
                .is_some_and(|(winner, remaining_owned)| winner == remaining_owned)
    }
}

pub(crate) fn cohort_is_winner_pending_protection(cohort: &AuthoritativePairedCohortView) -> bool {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        return false;
    }
    cohort.owner_kind() == ResolvedInventoryOwnerKind::AcceptedPair
        && !cohort.winner_exit_latched()
        && !cohort.protection_authorized()
        && cohort.terminal_state().is_none()
        && trading_winner_for_cohort(cohort).is_some()
        && cohort.remaining_owned_side() == trading_winner_for_cohort(cohort)
}

pub(crate) fn cohort_is_winner_protected(cohort: &AuthoritativePairedCohortView) -> bool {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        return false;
    }
    cohort.owner_kind() == ResolvedInventoryOwnerKind::AcceptedPair
        && !cohort.winner_exit_latched()
        && (cohort.protection_authorized()
            || cohort.terminal_state() == Some(ProtectedInventoryTerminalState::Redeemable))
        && trading_winner_for_cohort(cohort).is_some()
        && cohort.remaining_owned_side() == trading_winner_for_cohort(cohort)
}

pub(crate) fn cohort_is_winner_late_exit(cohort: &AuthoritativePairedCohortView) -> bool {
    if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
        return false;
    }
    cohort.winner_exit_latched()
        && trading_winner_for_cohort(cohort).is_some()
        && cohort.remaining_owned_side() == trading_winner_for_cohort(cohort)
}

fn latched_terminal_state_for_inventory(
    state: &PairLifecycleState,
    up_position: Option<&crate::portfolio::PositionState>,
    down_position: Option<&crate::portfolio::PositionState>,
    matched_qty: f64,
) -> Option<ProtectedInventoryTerminalState> {
    state
        .terminal_state()
        .filter(|terminal_state| match terminal_state {
            ProtectedInventoryTerminalState::Redeemable => {
                if is_confirmed_ctf_split_cohort_id(&state.quote_group)
                    && state.remaining_owned_side().is_none()
                    && matched_qty > inventory_eps()
                    && state.resolved_winner().is_some()
                {
                    return up_position.is_some_and(|position| position.qty > inventory_eps())
                        || down_position.is_some_and(|position| position.qty > inventory_eps());
                }
                let redeemable_side = if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
                    state.remaining_owned_side()
                } else {
                    state.effective_winner()
                };
                match redeemable_side {
                    Some(PairTokenSide::Up) => {
                        up_position.is_some_and(|position| position.qty > inventory_eps())
                    }
                    Some(PairTokenSide::Down) => {
                        down_position.is_some_and(|position| position.qty > inventory_eps())
                    }
                    None => false,
                }
            }
            ProtectedInventoryTerminalState::MatchedPairMergeCandidate => {
                matched_qty > inventory_eps() && state.resolved_winner().is_some()
            }
        })
}

#[derive(Clone, Copy)]
struct NormalizedLifecycleOwner {
    kind: ResolvedInventoryOwnerKind,
    side: Option<PairTokenSide>,
    qty_units_6dp: u128,
    qty: f64,
    protection_allowed: bool,
}

impl MakerRebateMmStrategy {
    pub fn runtime_managed_paired_tokens(&self) -> HashSet<String> {
        let portfolio_snapshot = self.portfolio_rx.borrow().clone();
        self.authoritative_paired_cohort_keys()
            .into_iter()
            .filter_map(|key| self.authoritative_paired_cohort_view_by_key(&key))
            .filter(|cohort| {
                if is_confirmed_ctf_split_cohort_id(&cohort.quote_group) {
                    return !cohort.strategy_closed();
                }
                cohort_is_loser_remaining_owned_leg(cohort)
                    || cohort_is_directional_remaining_owned_leg(cohort)
                    || cohort_is_winner_pending_protection(cohort)
                    || cohort_is_winner_late_exit(cohort)
                    || cohort_is_winner_protected(cohort)
                    || cohort.terminal_state().is_some()
            })
            .flat_map(|cohort| {
                [cohort.up_token, cohort.down_token]
                    .into_iter()
                    .filter(|token| {
                        portfolio_snapshot
                            .position(token)
                            .is_some_and(|position| position.qty > inventory_eps())
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn materialize_pair_authority(
        &mut self,
        state: &mut PairLifecycleState,
        portfolio: &PortfolioState,
        odds: &PolymarketOddsState,
        markets: &MarketProjection,
        now_ms: u64,
    ) {
        let previous_resolved_winner = state.effective_winner();
        let remaining_owned_side =
            remaining_owned_side_for_positions(portfolio, &state.up_token, &state.down_token);
        let ctf_pair_state = is_confirmed_ctf_split_cohort_id(&state.quote_group);
        if state.lifecycle_durability_state != PairLifecycleDurabilityState::Completed {
            if !ctf_pair_state {
                PairLifecycleReducer::refresh_materialization_oracle_signal_side(state, None);
            }
            PairLifecycleReducer::refresh_materialization_protection_eligible_since(state, None);
            PairLifecycleReducer::apply_state(state, PairLifecycleEvent::PredictionCleared);
            return;
        }
        PairLifecycleReducer::apply_state(
            state,
            PairLifecycleEvent::PredictionObserved {
                predictive_winner: state.predictive_winner(),
                resolved_winner: state.resolved_winner(),
                effective_winner: state.effective_winner(),
                predictive_first_managed_sell_allowed: false,
            },
        );
        let (predictive_winner, predictive_first_managed_sell_allowed) = if ctf_pair_state {
            (
                self.current_ctf_oracle_authority_snapshot(state)
                    .and_then(|snapshot| snapshot.oracle_signal_side),
                false,
            )
        } else {
            let snapshot = if let Some(remaining_owned_side) =
                remaining_owned_side.filter(|_| state.resolved_winner().is_none())
            {
                markets
                    .market_by_id(state.market_id.as_str())
                    .and_then(|market| {
                        let (up_book, down_book) =
                            self.pair_books_for_tokens(odds, &state.up_token, &state.down_token)?;
                        let (up_depth, down_depth) = self.current_depths_for_market(odds, market);
                        let previous_authority = self
                            .oracle_authority_by_market
                            .get(&state.market_id)
                            .copied();
                        let previous_side = state.oracle_signal_side().or_else(|| {
                            previous_authority.and_then(|snapshot| snapshot.oracle_signal_side)
                        });
                        Some(self.build_oracle_authority_snapshot(
                            market,
                            &state.market_id,
                            &up_book,
                            &down_book,
                            up_depth.as_ref(),
                            down_depth.as_ref(),
                            previous_authority,
                            previous_side,
                            OracleAuthorityMode::Cleanup {
                                remaining_owned_side,
                            },
                            now_ms,
                        ))
                    })
            } else {
                self.oracle_authority_by_market
                    .get(&state.market_id)
                    .copied()
            };
            let oracle_signal_side = snapshot.and_then(|snapshot| snapshot.oracle_signal_side);
            let predictive_first_managed_sell_allowed =
                snapshot.is_some_and(|snapshot| snapshot.predictive_first_managed_sell_allowed);
            PairLifecycleReducer::refresh_materialization_oracle_signal_side(
                state,
                oracle_signal_side,
            );
            (
                state
                    .oracle_signal_side()
                    .filter(|_| state.resolved_winner().is_none()),
                predictive_first_managed_sell_allowed,
            )
        };
        let effective_winner = state.resolved_winner();
        PairLifecycleReducer::apply_state(
            state,
            PairLifecycleEvent::PredictionObserved {
                predictive_winner,
                resolved_winner: state.resolved_winner(),
                effective_winner,
                predictive_first_managed_sell_allowed,
            },
        );
        let protection_side = if ctf_pair_state {
            None
        } else {
            state.effective_winner()
        };
        let protection_candidate = if ctf_pair_state {
            false
        } else {
            protection_side.is_some()
                && remaining_owned_side == protection_side
                && !state.winner_exit_latched()
        };
        PairLifecycleReducer::refresh_materialization_protection_window(
            state,
            protection_candidate,
            state.effective_winner() == previous_resolved_winner,
            now_ms,
        );
    }

    pub(in crate::strategy) fn authoritative_paired_cohort_keys(&self) -> Vec<PairedCohortKey> {
        let mut keys = std::collections::HashSet::new();
        keys.extend(
            self.paired_cohorts
                .values()
                .filter(|cohort| !is_confirmed_ctf_split_cohort_id(&cohort.quote_group))
                .map(|cohort| cohort.key.clone()),
        );
        keys.extend(
            self.pair_lifecycle_by_group
                .values()
                .map(|state| PairedCohortKey {
                    condition_id: state.market_id.clone(),
                    yes_token: state.up_token.clone(),
                    no_token: state.down_token.clone(),
                }),
        );
        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            left.condition_id
                .cmp(&right.condition_id)
                .then_with(|| left.yes_token.cmp(&right.yes_token))
                .then_with(|| left.no_token.cmp(&right.no_token))
        });
        keys
    }

    fn authoritative_pair_lifecycle_for_key(
        &self,
        key: &PairedCohortKey,
    ) -> Option<PairLifecycleState> {
        self.pair_lifecycle_by_group
            .values()
            .find(|state| {
                state.market_id == key.condition_id
                    && state.up_token == key.yes_token
                    && state.down_token == key.no_token
            })
            .cloned()
    }

    fn normalize_lifecycle_owner(
        &self,
        state: &PairLifecycleState,
        owner: &ResolvedInventoryOwner,
        terminal_state: Option<ProtectedInventoryTerminalState>,
    ) -> NormalizedLifecycleOwner {
        let mut normalized = NormalizedLifecycleOwner {
            kind: owner.kind,
            side: owner.remaining_owned_side(),
            qty_units_6dp: owner.remaining_owned_qty_units_6dp,
            qty: owner.remaining_owned_qty,
            protection_allowed: owner.protection_authorized(),
        };
        if state.strategy_closed() {
            normalized.kind = ResolvedInventoryOwnerKind::AcceptedPair;
            normalized.protection_allowed = false;
        } else if is_confirmed_ctf_split_cohort_id(&state.quote_group)
            && terminal_state.is_none()
            && matches!(
                owner.kind,
                ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg
            )
        {
            normalized.protection_allowed = true;
        }
        normalized
    }
}

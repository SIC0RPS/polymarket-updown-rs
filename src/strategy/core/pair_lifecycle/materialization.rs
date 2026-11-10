use crate::orders::OpenOrdersState;
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::portfolio::PortfolioState;
use crate::strategy::paired_maker::MakerRebateMmStrategy;

use super::journal::append_lifecycle_state_journal_events;
use super::post_proceeds_sell_observation::apply_post_proceeds_sell_observations;
use super::{
    ctf_strategy_lifecycle_should_close, is_confirmed_ctf_split_cohort_id,
    latched_terminal_state_for_inventory, resolve_non_ctf_inventory_owner,
    AuthoritativePairedCohortView, PairLifecycleEvent, PairLifecycleReducer, PairedCohortKey,
    ResolvedInventoryOwnerKind, SiblingCancelState,
};

impl MakerRebateMmStrategy {
    pub(in crate::strategy) fn authoritative_paired_cohort_view_by_key(
        &self,
        key: &PairedCohortKey,
    ) -> Option<AuthoritativePairedCohortView> {
        let state = self.authoritative_pair_lifecycle_for_key(key)?;
        let ctf_pair_state = is_confirmed_ctf_split_cohort_id(&state.quote_group);
        let last_unknown_reason = (!ctf_pair_state)
            .then(|| {
                self.paired_cohorts
                    .get(key)
                    .and_then(|cohort| cohort.last_unknown_reason.clone())
            })
            .flatten();
        let (release_winner_side, winner_exit_latched) = if ctf_pair_state {
            (None, false)
        } else {
            (state.release_winner_side(), state.winner_exit_latched())
        };
        Some(AuthoritativePairedCohortView::new(
            state.market_id.clone(),
            state.quote_group.clone(),
            state.up_token.clone(),
            state.down_token.clone(),
            state.ctf_entry_outlay_units_6dp(),
            state.ctf_split_entry_ts_ms(),
            state.protected_side(),
            release_winner_side,
            state.owner_kind(),
            state.predictive_winner(),
            state.effective_winner(),
            state.matched_qty_units_6dp,
            state.matched_qty,
            state.remaining_owned_side(),
            state.remaining_owned_qty_units_6dp,
            state.remaining_owned_qty,
            state.strategy_closed(),
            state.protection_authorized(),
            winner_exit_latched,
            state.terminal_state(),
            state.sibling_cancel_state,
            last_unknown_reason,
        ))
    }

    pub(super) fn recompute_pair_lifecycle_phase(
        &mut self,
        group_id: &str,
        _open_orders: &OpenOrdersState,
        odds: &PolymarketOddsState,
        now_ms: u64,
    ) {
        let Some((up_token, down_token)) = self
            .pair_lifecycle_by_group
            .get(group_id)
            .map(|state| (state.up_token.clone(), state.down_token.clone()))
        else {
            return;
        };
        let material_threshold = self.pair_material_asymmetry_qty(odds, &up_token, &down_token);
        if let Some(state) = self.pair_lifecycle_by_group.get_mut(group_id) {
            PairLifecycleReducer::refresh_phase(state, material_threshold, now_ms);
        }
        if let Some(state) = self
            .pair_lifecycle_by_group
            .get(group_id)
            .filter(|state| !is_confirmed_ctf_split_cohort_id(&state.quote_group))
            .cloned()
        {
            self.sync_paired_cohort_from_lifecycle(&state);
        }
    }

    pub(super) fn materialize_paired_authority(
        &mut self,
        now_ms: u64,
        portfolio: &PortfolioState,
        open_orders: Option<&OpenOrdersState>,
        odds: &PolymarketOddsState,
        markets: &MarketProjection,
    ) {
        let empty_open_orders = OpenOrdersState::default();
        let open_orders = open_orders.unwrap_or(&empty_open_orders);
        self.materialize_ctf_inventory_lifecycle_authority(portfolio, markets, now_ms);
        let mut group_ids: Vec<String> = self.pair_lifecycle_by_group.keys().cloned().collect();
        group_ids.sort();
        let base_cfg = self.cfg.clone();
        for group_id in group_ids {
            self.cfg = base_cfg.clone();
            let previous_lifecycle = self.pair_lifecycle_by_group.get(&group_id).cloned();
            self.recompute_pair_lifecycle_phase(&group_id, open_orders, odds, now_ms);
            let Some(mut state_snapshot) = self.pair_lifecycle_by_group.get(&group_id).cloned()
            else {
                continue;
            };
            if let Some(market) = markets.market_by_id(state_snapshot.market_id.as_str()) {
                self.cfg = base_cfg.config_for_symbol(&market.underlying).clone();
            }
            let paired_cohort_key = format!(
                "{}:{}:{}",
                state_snapshot.market_id, state_snapshot.up_token, state_snapshot.down_token
            );
            let up_position = portfolio.position(&state_snapshot.up_token);
            let down_position = portfolio.position(&state_snapshot.down_token);
            let up_qty = up_position
                .map(|position| position.qty.max(0.0))
                .unwrap_or(0.0);
            let down_qty = down_position
                .map(|position| position.qty.max(0.0))
                .unwrap_or(0.0);
            let up_qty_units_6dp = up_position
                .map(|position| position.qty_units_6dp)
                .unwrap_or(0);
            let down_qty_units_6dp = down_position
                .map(|position| position.qty_units_6dp)
                .unwrap_or(0);
            let ctf_pair_state = is_confirmed_ctf_split_cohort_id(&state_snapshot.quote_group);
            if ctf_pair_state {
                let ctf_inventory = portfolio
                    .confirmed_ctf_cohort_qty_units_6dp(
                        state_snapshot.up_token.as_str(),
                        state_snapshot.quote_group.as_str(),
                    )
                    .ok()
                    .zip(
                        portfolio
                            .confirmed_ctf_cohort_qty_units_6dp(
                                state_snapshot.down_token.as_str(),
                                state_snapshot.quote_group.as_str(),
                            )
                            .ok(),
                    )
                    .map(|(up_qty_units_6dp, down_qty_units_6dp)| {
                        let matched_qty_units_6dp = up_qty_units_6dp.min(down_qty_units_6dp);
                        let up_remaining_qty_units_6dp =
                            up_qty_units_6dp.saturating_sub(matched_qty_units_6dp);
                        let down_remaining_qty_units_6dp =
                            down_qty_units_6dp.saturating_sub(matched_qty_units_6dp);
                        let (remaining_owned_side, remaining_owned_qty_units_6dp) =
                            if up_remaining_qty_units_6dp > 0 {
                                (Some(super::PairTokenSide::Up), up_remaining_qty_units_6dp)
                            } else if down_remaining_qty_units_6dp > 0 {
                                (
                                    Some(super::PairTokenSide::Down),
                                    down_remaining_qty_units_6dp,
                                )
                            } else {
                                (None, 0)
                            };
                        (
                            up_qty_units_6dp,
                            down_qty_units_6dp,
                            matched_qty_units_6dp,
                            remaining_owned_side,
                            remaining_owned_qty_units_6dp,
                        )
                    });
                if let Some((_, _, matched_qty_units_6dp, _, remaining_qty_units_6dp)) =
                    ctf_inventory
                {
                    PairLifecycleReducer::refresh_materialization_projection(
                        &mut state_snapshot,
                        SiblingCancelState::Acked,
                        matched_qty_units_6dp,
                        crate::strategy::full_set_units_to_qty(matched_qty_units_6dp),
                        remaining_qty_units_6dp,
                        crate::strategy::full_set_units_to_qty(remaining_qty_units_6dp),
                        now_ms,
                    );
                }
                self.materialize_pair_authority(
                    &mut state_snapshot,
                    portfolio,
                    odds,
                    markets,
                    now_ms,
                );
                if let Some((
                    up_qty_units_6dp,
                    down_qty_units_6dp,
                    matched_qty_units_6dp,
                    remaining_owned_side,
                    _,
                )) = ctf_inventory
                {
                    let terminal_state = state_snapshot.terminal_state();
                    let owner_kind = if terminal_state.is_some() {
                        ResolvedInventoryOwnerKind::TerminalSettlement
                    } else if remaining_owned_side.is_some() {
                        ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg
                    } else if matched_qty_units_6dp > 0 {
                        ResolvedInventoryOwnerKind::MatchedHold
                    } else {
                        ResolvedInventoryOwnerKind::AcceptedPair
                    };
                    PairLifecycleReducer::apply_state(
                        &mut state_snapshot,
                        PairLifecycleEvent::AuthorityMaterialized {
                            owner_kind,
                            remaining_owned_side,
                            protection_authorized: false,
                            terminal_state,
                        },
                    );
                    if ctf_strategy_lifecycle_should_close(
                        &state_snapshot,
                        up_qty_units_6dp,
                        down_qty_units_6dp,
                    ) {
                        PairLifecycleReducer::apply_state(
                            &mut state_snapshot,
                            PairLifecycleEvent::StrategyClosed,
                        );
                    }
                }
                if let Some(state) = self.pair_lifecycle_by_group.get_mut(&group_id) {
                    PairLifecycleReducer::apply_materialization_snapshot(state, &state_snapshot);
                }
                append_lifecycle_state_journal_events(
                    previous_lifecycle.as_ref(),
                    &state_snapshot,
                    paired_cohort_key.as_str(),
                    now_ms,
                );
                continue;
            }
            let matched_qty = up_qty.min(down_qty).max(0.0);
            let next_cancel_state = if !state_snapshot.cancel_requested {
                SiblingCancelState::NotNeeded
            } else {
                SiblingCancelState::Acked
            };
            let current_inventory = resolve_non_ctf_inventory_owner(
                &state_snapshot,
                next_cancel_state,
                up_position,
                down_position,
                self.cfg.protection_dwell_ms,
                now_ms,
            );
            PairLifecycleReducer::refresh_materialization_projection(
                &mut state_snapshot,
                next_cancel_state,
                current_inventory.matched_qty_units_6dp,
                current_inventory.matched_qty,
                current_inventory.remaining_owned_qty_units_6dp,
                current_inventory.remaining_owned_qty,
                now_ms,
            );
            self.materialize_pair_authority(&mut state_snapshot, portfolio, odds, markets, now_ms);
            let owner = resolve_non_ctf_inventory_owner(
                &state_snapshot,
                next_cancel_state,
                up_position,
                down_position,
                self.cfg.protection_dwell_ms,
                now_ms,
            );
            if state_snapshot.terminal_state() != owner.terminal_state() {
                let _ = (matched_qty, up_qty, down_qty, owner.terminal_state());
            }
            let terminal_state = owner.terminal_state().or_else(|| {
                latched_terminal_state_for_inventory(
                    &state_snapshot,
                    up_position,
                    down_position,
                    matched_qty,
                )
            });
            let normalized_owner =
                self.normalize_lifecycle_owner(&state_snapshot, &owner, terminal_state);
            PairLifecycleReducer::refresh_materialization_projection(
                &mut state_snapshot,
                next_cancel_state,
                owner.matched_qty_units_6dp,
                owner.matched_qty,
                normalized_owner.qty_units_6dp,
                normalized_owner.qty,
                now_ms,
            );
            apply_post_proceeds_sell_observations(
                &mut state_snapshot,
                open_orders,
                paired_cohort_key.as_str(),
                now_ms,
            );
            PairLifecycleReducer::apply_state(
                &mut state_snapshot,
                PairLifecycleEvent::AuthorityMaterialized {
                    owner_kind: normalized_owner.kind,
                    remaining_owned_side: normalized_owner.side,
                    protection_authorized: normalized_owner.protection_allowed,
                    terminal_state,
                },
            );
            if ctf_strategy_lifecycle_should_close(
                &state_snapshot,
                up_qty_units_6dp,
                down_qty_units_6dp,
            ) {
                PairLifecycleReducer::apply_state(
                    &mut state_snapshot,
                    PairLifecycleEvent::StrategyClosed,
                );
            }
            if let Some(state) = self.pair_lifecycle_by_group.get_mut(&group_id) {
                PairLifecycleReducer::apply_materialization_snapshot(state, &state_snapshot);
            }
            append_lifecycle_state_journal_events(
                previous_lifecycle.as_ref(),
                &state_snapshot,
                paired_cohort_key.as_str(),
                now_ms,
            );
            self.apply_post_materialization_cohort_projection(
                &state_snapshot,
                previous_lifecycle.as_ref(),
                next_cancel_state,
                paired_cohort_key.as_str(),
                now_ms,
            );
        }

        self.cfg = base_cfg;
        self.retain_paired_lifecycle_state(portfolio, now_ms);
    }

    pub(crate) fn materialize_paired_authority_if_needed(
        &mut self,
        now_ms: u64,
        market_projection: &MarketProjection,
    ) {
        let portfolio_snapshot = self.portfolio_rx.borrow();
        let open_orders_snapshot = self.open_orders_rx.as_ref().map(|rx| rx.borrow());
        let live_confirmed_ctf_inventory = self.pair_lifecycle_by_group.values().any(|state| {
            is_confirmed_ctf_split_cohort_id(&state.quote_group)
                && [&state.up_token, &state.down_token]
                    .into_iter()
                    .any(|token| {
                        portfolio_snapshot
                            .position(token)
                            .is_some_and(|position| position.qty_units_6dp > 0)
                    })
        });
        let refresh_sig = self.paired_authority_refresh_signature(
            now_ms,
            &portfolio_snapshot,
            open_orders_snapshot.as_deref(),
            &self.odds,
        );
        if !live_confirmed_ctf_inventory && self.last_paired_authority_sig == Some(refresh_sig) {
            return;
        }
        // Clone the exact snapshots used above, then release both watch guards
        // before paired-lifecycle work can meet pending watch writers.
        let portfolio_owned_snapshot = portfolio_snapshot.clone();
        drop(portfolio_snapshot);
        let open_orders_owned_snapshot = open_orders_snapshot.as_deref().cloned();
        drop(open_orders_snapshot);
        let odds_snapshot = self.odds.clone();
        self.materialize_paired_authority(
            now_ms,
            &portfolio_owned_snapshot,
            open_orders_owned_snapshot.as_ref(),
            &odds_snapshot,
            market_projection,
        );
        if !live_confirmed_ctf_inventory {
            self.last_paired_authority_sig = Some(refresh_sig);
        }
    }
}

use crate::orders::OpenOrdersState;
use crate::polymarket::state::PolymarketOddsState;
use crate::portfolio::PortfolioState;
use crate::strategy::paired_maker::{MakerRebateMmStrategy, PAIR_LIFECYCLE_STALE_MS};

use super::is_confirmed_ctf_split_cohort_id;
use super::journal::{
    append_protected_winner_hold, append_sibling_cancel, append_terminal_release_ready,
    sibling_cancel_event_kind,
};
use super::logic::retain_pair_lifecycle_state;
use super::types::{
    PairLifecycleState, PairedCohortKey, ResolvedInventoryOwnerKind, SiblingCancelState,
};

impl MakerRebateMmStrategy {
    pub(crate) fn next_protection_dwell_deadline_ms(&self, after_ms: u64) -> Option<u64> {
        self.pair_lifecycle_by_group
            .values()
            .filter(|state| !is_confirmed_ctf_split_cohort_id(&state.quote_group))
            .filter_map(|state| {
                state
                    .protection_eligible_since_ms()
                    .and_then(|eligible_since_ms| {
                        (!state.loser_exit_latched()
                            && !state.protection_authorized()
                            && state.owner_kind() == ResolvedInventoryOwnerKind::AcceptedPair)
                            .then_some(
                                eligible_since_ms.saturating_add(self.cfg.protection_dwell_ms),
                            )
                    })
            })
            .filter(|deadline_ms| *deadline_ms > after_ms)
            .min()
    }

    pub(super) fn apply_post_materialization_cohort_projection(
        &mut self,
        state_snapshot: &PairLifecycleState,
        previous_lifecycle: Option<&PairLifecycleState>,
        next_cancel_state: SiblingCancelState,
        paired_cohort_key: &str,
        now_ms: u64,
    ) {
        if is_confirmed_ctf_split_cohort_id(&state_snapshot.quote_group) {
            self.paired_cohorts.remove(&PairedCohortKey {
                condition_id: state_snapshot.market_id.clone(),
                yes_token: state_snapshot.up_token.clone(),
                no_token: state_snapshot.down_token.clone(),
            });
            return;
        }
        self.sync_paired_cohort_from_lifecycle(state_snapshot);
        let key = PairedCohortKey {
            condition_id: state_snapshot.market_id.clone(),
            yes_token: state_snapshot.up_token.clone(),
            no_token: state_snapshot.down_token.clone(),
        };
        let mut state_event_msg: Option<String> = None;
        let mut cancel_event_msg: Option<String> = None;
        if let Some(cohort) = self.paired_cohorts.get_mut(&key) {
            let previous_state = cohort.owner_kind();
            let previous_cancel_state = cohort.sibling_cancel_state;
            cohort.sibling_cancel_state = next_cancel_state;
            let next_unknown_reason = super::ctf_unknown_reason(
                &cohort.quote_group,
                cohort.protected_side(),
                cohort.owner_kind(),
                cohort.effective_winner(),
                state_snapshot.remaining_owned_side(),
                state_snapshot.terminal_state(),
                cohort.sibling_cancel_state,
                state_snapshot.last_update_ms,
                now_ms,
            );
            if next_unknown_reason.is_some() {
                cohort.reconciliation_epoch = cohort.reconciliation_epoch.saturating_add(1);
            }
            cohort.last_unknown_reason = next_unknown_reason;
            if previous_state != cohort.owner_kind() {
                state_event_msg = Some(format!(
                    "paired_owner_kind market={} quote_group={} owner={} prev_owner={}",
                    cohort.market_id,
                    cohort.quote_group,
                    cohort.owner_kind().as_str(),
                    previous_state.as_str()
                ));
                cohort.reconciliation_epoch = 0;
                cohort.last_unknown_cancel_epoch = 0;
                cohort.last_unknown_reason = super::ctf_unknown_reason(
                    &cohort.quote_group,
                    cohort.protected_side(),
                    cohort.owner_kind(),
                    cohort.effective_winner(),
                    state_snapshot.remaining_owned_side(),
                    state_snapshot.terminal_state(),
                    cohort.sibling_cancel_state,
                    state_snapshot.last_update_ms,
                    now_ms,
                );
                if cohort.owner_kind() == ResolvedInventoryOwnerKind::AcceptedPair
                    && state_snapshot.protection_authorized()
                    && state_snapshot.effective_winner().is_some()
                {
                    append_protected_winner_hold(cohort, paired_cohort_key, now_ms);
                }
            }
            if previous_cancel_state != cohort.sibling_cancel_state {
                cancel_event_msg = Some(format!(
                    "paired_sibling_cancel market={} quote_group={} state={}",
                    cohort.market_id,
                    cohort.quote_group,
                    cohort.sibling_cancel_state.as_str()
                ));
                let event_kind =
                    sibling_cancel_event_kind(cohort.owner_kind(), cohort.sibling_cancel_state);
                if let Some(event_kind) = event_kind {
                    append_sibling_cancel(cohort, paired_cohort_key, event_kind, now_ms);
                }
            }
            if previous_lifecycle.and_then(PairLifecycleState::terminal_state)
                != state_snapshot.terminal_state()
            {
                if let Some(terminal_state) = state_snapshot.terminal_state() {
                    append_terminal_release_ready(
                        cohort,
                        paired_cohort_key,
                        terminal_state,
                        now_ms,
                    );
                }
            }
            cohort.last_update_ms = now_ms.max(cohort.last_update_ms);
        }
        let _ = (now_ms, state_event_msg, cancel_event_msg);
    }

    pub(super) fn paired_authority_refresh_signature(
        &self,
        now_ms: u64,
        portfolio_snapshot: &PortfolioState,
        open_orders_snapshot: Option<&OpenOrdersState>,
        odds_snapshot: &PolymarketOddsState,
    ) -> (u64, u64, u64, Option<u64>, u64) {
        (
            portfolio_snapshot.revision,
            open_orders_snapshot
                .map(|snapshot| snapshot.revision)
                .unwrap_or_default(),
            odds_snapshot.revision,
            self.next_protection_dwell_deadline_ms(0)
                .map(|deadline_ms| {
                    if now_ms >= deadline_ms {
                        0
                    } else {
                        deadline_ms
                    }
                }),
            0,
        )
    }

    pub(super) fn retain_paired_lifecycle_state(
        &mut self,
        portfolio: &PortfolioState,
        now_ms: u64,
    ) {
        self.pair_lifecycle_by_group.retain(|_, state| {
            retain_pair_lifecycle_state(state, portfolio, now_ms, PAIR_LIFECYCLE_STALE_MS)
        });
        let pair_lifecycle_by_group = &self.pair_lifecycle_by_group;
        self.paired_cohorts.retain(|_, state| {
            !is_confirmed_ctf_split_cohort_id(&state.quote_group)
                && Self::retain_paired_cohort_state(
                    state,
                    pair_lifecycle_by_group,
                    portfolio,
                    now_ms,
                    PAIR_LIFECYCLE_STALE_MS,
                )
        });
    }
}

use crate::polymarket::identity::MarketId;
use crate::portfolio::PortfolioState;

use super::types::{PairLifecycleState, PairedCohort, PairedCohortKey, SiblingCancelState};
use super::{is_confirmed_ctf_split_cohort_id, paired_release_qty_eps, PairLifecycleReducer};
use crate::strategy::paired_maker::MakerRebateMmStrategy;

impl MakerRebateMmStrategy {
    pub(crate) fn retain_paired_cohort_state(
        state: &PairedCohort,
        pair_lifecycle_by_group: &std::collections::HashMap<String, PairLifecycleState>,
        portfolio: &PortfolioState,
        now_ms: u64,
        stale_after_ms: u64,
    ) -> bool {
        let up_live = portfolio
            .position(&state.up_token)
            .is_some_and(|pos| pos.qty > paired_release_qty_eps());
        let down_live = portfolio
            .position(&state.down_token)
            .is_some_and(|pos| pos.qty > paired_release_qty_eps());
        let lifecycle_live = pair_lifecycle_by_group.values().any(|pair_state| {
            pair_state.market_id == state.market_id
                && pair_state.up_token == state.up_token
                && pair_state.down_token == state.down_token
                && super::retain_pair_lifecycle_state(pair_state, portfolio, now_ms, stale_after_ms)
        });
        if !up_live && !down_live && !lifecycle_live {
            return false;
        }
        let _ = (now_ms, stale_after_ms);
        up_live || down_live || lifecycle_live
    }

    fn authoritative_paired_cohort_entry_for_token_market(
        &self,
        token: &str,
        market_id: &str,
    ) -> Option<(PairedCohortKey, MarketId, String, String, String)> {
        self.paired_cohorts
            .values()
            .find(|cohort| {
                !is_confirmed_ctf_split_cohort_id(&cohort.quote_group)
                    && cohort.matches_market(market_id, token)
            })
            .map(|cohort| {
                (
                    cohort.key.clone(),
                    cohort.market_id.clone(),
                    cohort.up_token.clone(),
                    cohort.down_token.clone(),
                    cohort.quote_group.clone(),
                )
            })
    }

    fn authoritative_pair_lifecycle_for_token_market(
        &self,
        token: &str,
        market_id: &str,
    ) -> Option<PairLifecycleState> {
        self.pair_lifecycle_by_group
            .values()
            .find(|state| {
                state.market_id.as_str() == market_id
                    && (state.up_token == token || state.down_token == token)
            })
            .cloned()
    }

    pub(crate) fn resolve_paired_cohort_key_from_authority(
        &mut self,
        token: &str,
        market_id: &str,
    ) -> Option<PairedCohortKey> {
        if let Some(state) = self.authoritative_pair_lifecycle_for_token_market(token, market_id) {
            if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
                return Some(PairedCohortKey {
                    condition_id: state.market_id,
                    yes_token: state.up_token,
                    no_token: state.down_token,
                });
            }
            self.sync_paired_cohort_from_lifecycle(&state);
            return self
                .authoritative_paired_cohort_entry_for_token_market(token, market_id)
                .map(|(key, _, _, _, _)| key);
        }
        if let Some((key, _cohort_market_id, _up_token, _down_token, _quote_group)) =
            self.authoritative_paired_cohort_entry_for_token_market(token, market_id)
        {
            return Some(key);
        }
        None
    }

    pub(crate) fn sync_paired_cohort_from_lifecycle(&mut self, state: &PairLifecycleState) {
        if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
            self.paired_cohorts.remove(&PairedCohortKey {
                condition_id: state.market_id.clone(),
                yes_token: state.up_token.clone(),
                no_token: state.down_token.clone(),
            });
            return;
        }
        let next = PairedCohort::from_lifecycle(state);
        let cohort_key = next.key.clone();
        match self.paired_cohorts.get_mut(&next.key) {
            Some(current) => {
                if next.last_update_ms >= current.last_update_ms {
                    PairLifecycleReducer::apply_cohort_lifecycle_snapshot(current, &next);
                    current.market_id = next.market_id;
                    current.quote_group = next.quote_group;
                    current.up_token = next.up_token;
                    current.down_token = next.down_token;
                    current.lifecycle_durability_state = next.lifecycle_durability_state;
                    current.reconciliation_epoch = 0;
                    current.last_unknown_cancel_epoch = 0;
                    if state.cancel_requested {
                        if current.sibling_cancel_state == SiblingCancelState::NotNeeded {
                            current.sibling_cancel_state = SiblingCancelState::Pending;
                        }
                    } else {
                        current.sibling_cancel_state = SiblingCancelState::NotNeeded;
                    }
                    current.last_update_ms = next.last_update_ms.max(current.last_update_ms);
                    current.last_unknown_reason = super::ctf_unknown_reason(
                        &current.quote_group,
                        current.protected_side(),
                        current.owner_kind(),
                        current.effective_winner(),
                        state.remaining_owned_side(),
                        state.terminal_state(),
                        current.sibling_cancel_state,
                        current.last_update_ms,
                        current.last_update_ms,
                    );
                }
            }
            None => {
                self.paired_cohorts.insert(next.key.clone(), next);
            }
        }
        let _ = cohort_key;
    }
}

use super::super::{PairLifecycleDurabilityState, SiblingCancelState};
use super::{
    PairLifecycleReducer, PairLifecycleState, PairLifecycleStateInit, PairTokenSide,
    ResolvedInventoryOwnerKind,
};
use crate::polymarket::identity::MarketId;
use crate::strategy::full_set_units_to_qty;

impl PairLifecycleReducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_ctf_latched_entry_state(
        market_id: MarketId,
        quote_group: String,
        up_token: String,
        down_token: String,
        ctf_entry_outlay_units_6dp: u128,
        split_amount_units_6dp: u128,
        now_ms: u64,
    ) -> PairLifecycleState {
        let matched_qty = full_set_units_to_qty(split_amount_units_6dp);
        Self::new_state(PairLifecycleStateInit::new(
            market_id,
            quote_group,
            up_token,
            down_token,
            ctf_entry_outlay_units_6dp,
            matched_qty,
            matched_qty,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            false,
            ResolvedInventoryOwnerKind::MatchedHold,
            SiblingCancelState::Acked,
            split_amount_units_6dp,
            matched_qty,
            None,
            0,
            0.0,
            None,
            PairLifecycleDurabilityState::Completed,
            now_ms,
            now_ms,
            false,
        ))
    }

    pub(crate) fn refresh_phase(
        state: &mut PairLifecycleState,
        material_threshold: f64,
        now_ms: u64,
    ) {
        state.last_update_ms = now_ms;
        if super::super::is_confirmed_ctf_split_cohort_id(&state.quote_group) {
            state.lifecycle_durability_state = PairLifecycleDurabilityState::Completed;
            state.cancel_requested = false;
            return;
        }

        let matched_qty = state.up_filled_qty.min(state.down_filled_qty).max(0.0);
        let inventory_eps = super::super::logic::inventory_eps();
        let any_fill = state.up_filled_qty > inventory_eps || state.down_filled_qty > inventory_eps;
        state.lifecycle_durability_state =
            if matched_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM >= material_threshold
            {
                PairLifecycleDurabilityState::Completed
            } else if any_fill {
                PairLifecycleDurabilityState::AwaitingCompletion
            } else {
                PairLifecycleDurabilityState::Submitted
            };
        state.cancel_requested = false;
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn refresh_materialization_projection(
        state: &mut PairLifecycleState,
        sibling_cancel_state: SiblingCancelState,
        matched_qty_units_6dp: u128,
        matched_qty: f64,
        remaining_owned_qty_units_6dp: u128,
        remaining_owned_qty: f64,
        now_ms: u64,
    ) {
        state.last_update_ms = now_ms;
        state.sibling_cancel_state = sibling_cancel_state;
        state.matched_qty_units_6dp = matched_qty_units_6dp;
        state.matched_qty = matched_qty;
        state.remaining_owned_qty_units_6dp = remaining_owned_qty_units_6dp;
        state.remaining_owned_qty = remaining_owned_qty;
    }

    pub(crate) fn apply_materialization_snapshot(
        state: &mut PairLifecycleState,
        snapshot: &PairLifecycleState,
    ) {
        if !super::super::is_confirmed_ctf_split_cohort_id(&state.quote_group) {
            state.set_materialization_oracle_signal_side(
                Self::access(),
                snapshot.oracle_signal_side(),
            );
        }
        state.set_materialization_protection_eligible_since(
            Self::access(),
            snapshot.protection_eligible_since_ms(),
        );
        state.sibling_cancel_state = snapshot.sibling_cancel_state;
        state.matched_qty_units_6dp = snapshot.matched_qty_units_6dp;
        state.matched_qty = snapshot.matched_qty;
        state.remaining_owned_qty_units_6dp = snapshot.remaining_owned_qty_units_6dp;
        state.remaining_owned_qty = snapshot.remaining_owned_qty;
        state.lifecycle_durability_state = snapshot.lifecycle_durability_state;
        state.last_update_ms = snapshot.last_update_ms;
        Self::apply_state_snapshot(state, snapshot);
    }

    pub(in crate::strategy::pair_lifecycle) fn refresh_materialization_oracle_signal_side(
        state: &mut PairLifecycleState,
        oracle_signal_side: Option<PairTokenSide>,
    ) {
        state.set_materialization_oracle_signal_side(Self::access(), oracle_signal_side);
    }

    pub(in crate::strategy::pair_lifecycle) fn refresh_materialization_protection_eligible_since(
        state: &mut PairLifecycleState,
        protection_eligible_since_ms: Option<u64>,
    ) {
        state.set_materialization_protection_eligible_since(
            Self::access(),
            protection_eligible_since_ms,
        );
    }

    pub(in crate::strategy::pair_lifecycle) fn refresh_materialization_protection_window(
        state: &mut PairLifecycleState,
        protection_candidate: bool,
        effective_winner_unchanged: bool,
        now_ms: u64,
    ) {
        if !protection_candidate {
            state.set_materialization_protection_eligible_since(Self::access(), None);
        } else if !effective_winner_unchanged || state.protection_eligible_since_ms().is_none() {
            state.set_materialization_protection_eligible_since(Self::access(), Some(now_ms));
        }
    }
}

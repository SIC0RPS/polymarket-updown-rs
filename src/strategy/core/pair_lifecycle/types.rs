use crate::polymarket::identity::MarketId;

use super::super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use super::is_confirmed_ctf_split_cohort_id;
use super::reducer::{PairLifecycleReducerAccess, PairLifecycleTruth};

mod cohort;
mod dispositions;
pub(crate) use cohort::{
    AuthoritativePairedCohortView, PairedCohort, PairedCohortKey, ResolvedInventoryOwner,
    ResolvedInventoryOwnerKind, SiblingCancelState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairLifecycleDurabilityState {
    Submitted,
    AwaitingCompletion,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostProceedsSellState {
    Flat,
    Pending {
        token: String,
        order_id: String,
        reason: String,
    },
    ExpiredOrUnfilled {
        token: String,
        order_id: String,
        reason: String,
    },
}

pub(super) const POST_PROCEEDS_SELL_FINAL_ATTEMPT_UNFILLED_REASON: &str =
    "post_proceeds_sell_final_immediate_closed_without_fill";

impl PostProceedsSellState {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Pending { .. } => "pending",
            Self::ExpiredOrUnfilled { .. } => "expired_or_unfilled",
        }
    }

    pub(super) fn final_attempt_exhausted(&self) -> bool {
        matches!(
            self,
            Self::ExpiredOrUnfilled { reason, .. }
                if reason == POST_PROCEEDS_SELL_FINAL_ATTEMPT_UNFILLED_REASON
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PairLifecycleState {
    pub market_id: MarketId,
    pub quote_group: String,
    pub up_token: String,
    pub down_token: String,
    ctf_entry_outlay_units_6dp: u128,
    pub up_filled_qty: f64,
    pub down_filled_qty: f64,
    ctf_split_entry_ts_ms: u64,
    protected_side: Option<PairTokenSide>,
    release_winner_side: Option<PairTokenSide>,
    oracle_signal_side: Option<PairTokenSide>,
    truth: PairLifecycleTruth,
    predictive_first_managed_sell_allowed: bool,
    protection_eligible_since_ms: Option<u64>,
    strategy_closed: bool,
    pub sibling_cancel_state: SiblingCancelState,
    pub matched_qty_units_6dp: u128,
    pub matched_qty: f64,
    pub remaining_owned_qty_units_6dp: u128,
    pub remaining_owned_qty: f64,
    post_proceeds_sell_remaining_position_state: Option<PostProceedsSellState>,
    pub lifecycle_durability_state: PairLifecycleDurabilityState,
    pub last_update_ms: u64,
    pub cancel_requested: bool,
}

pub(crate) struct PairLifecycleStateInit {
    pub market_id: MarketId,
    pub quote_group: String,
    pub up_token: String,
    pub down_token: String,
    ctf_entry_outlay_units_6dp: u128,
    pub up_filled_qty: f64,
    pub down_filled_qty: f64,
    protected_side: Option<PairTokenSide>,
    pub oracle_signal_side: Option<PairTokenSide>,
    predictive_winner: Option<PairTokenSide>,
    predictive_first_managed_sell_allowed: bool,
    resolved_winner: Option<PairTokenSide>,
    effective_winner: Option<PairTokenSide>,
    pub protection_eligible_since_ms: Option<u64>,
    protection_authorized: bool,
    strategy_closed: bool,
    owner_kind: ResolvedInventoryOwnerKind,
    pub sibling_cancel_state: SiblingCancelState,
    pub matched_qty_units_6dp: u128,
    pub matched_qty: f64,
    remaining_owned_side: Option<PairTokenSide>,
    pub remaining_owned_qty_units_6dp: u128,
    pub remaining_owned_qty: f64,
    terminal_state: Option<ProtectedInventoryTerminalState>,
    pub lifecycle_durability_state: PairLifecycleDurabilityState,
    pub ctf_split_entry_ts_ms: u64,
    pub last_update_ms: u64,
    pub cancel_requested: bool,
}

impl PairLifecycleStateInit {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        market_id: MarketId,
        quote_group: String,
        up_token: String,
        down_token: String,
        ctf_entry_outlay_units_6dp: u128,
        up_filled_qty: f64,
        down_filled_qty: f64,
        protected_side: Option<PairTokenSide>,
        oracle_signal_side: Option<PairTokenSide>,
        predictive_winner: Option<PairTokenSide>,
        predictive_first_managed_sell_allowed: bool,
        resolved_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        protection_eligible_since_ms: Option<u64>,
        protection_authorized: bool,
        owner_kind: ResolvedInventoryOwnerKind,
        sibling_cancel_state: SiblingCancelState,
        matched_qty_units_6dp: u128,
        matched_qty: f64,
        remaining_owned_side: Option<PairTokenSide>,
        remaining_owned_qty_units_6dp: u128,
        remaining_owned_qty: f64,
        terminal_state: Option<ProtectedInventoryTerminalState>,
        lifecycle_durability_state: PairLifecycleDurabilityState,
        ctf_split_entry_ts_ms: u64,
        last_update_ms: u64,
        cancel_requested: bool,
    ) -> Self {
        Self {
            market_id,
            quote_group,
            up_token,
            down_token,
            ctf_entry_outlay_units_6dp,
            up_filled_qty,
            down_filled_qty,
            protected_side,
            oracle_signal_side,
            predictive_winner,
            predictive_first_managed_sell_allowed,
            resolved_winner,
            effective_winner,
            protection_eligible_since_ms,
            protection_authorized,
            strategy_closed: false,
            owner_kind,
            sibling_cancel_state,
            matched_qty_units_6dp,
            matched_qty,
            remaining_owned_side,
            remaining_owned_qty_units_6dp,
            remaining_owned_qty,
            terminal_state,
            lifecycle_durability_state,
            ctf_split_entry_ts_ms,
            last_update_ms,
            cancel_requested,
        }
    }
}

impl PairLifecycleState {
    fn merge_protected_side(
        current: Option<PairTokenSide>,
        incoming: Option<PairTokenSide>,
    ) -> Option<PairTokenSide> {
        if let (Some(current), Some(incoming)) = (current, incoming) {
            if current != incoming {
                return Some(current);
            }
        }
        current.or(incoming)
    }

    pub(super) fn from_reducer(
        access: PairLifecycleReducerAccess,
        init: PairLifecycleStateInit,
    ) -> Self {
        let ctf_pair_state = is_confirmed_ctf_split_cohort_id(&init.quote_group);
        Self {
            market_id: init.market_id,
            quote_group: init.quote_group,
            up_token: init.up_token,
            down_token: init.down_token,
            ctf_entry_outlay_units_6dp: init.ctf_entry_outlay_units_6dp,
            up_filled_qty: init.up_filled_qty,
            down_filled_qty: init.down_filled_qty,
            ctf_split_entry_ts_ms: init.ctf_split_entry_ts_ms,
            protected_side: init.protected_side,
            release_winner_side: None,
            oracle_signal_side: init.oracle_signal_side,
            truth: PairLifecycleTruth::new(
                access,
                init.predictive_winner
                    .filter(|_| init.resolved_winner.is_none()),
                init.resolved_winner,
                init.effective_winner,
                init.protection_authorized,
                init.owner_kind,
                init.remaining_owned_side,
                init.terminal_state,
            ),
            predictive_first_managed_sell_allowed: if ctf_pair_state {
                false
            } else {
                init.predictive_first_managed_sell_allowed
            },
            protection_eligible_since_ms: init.protection_eligible_since_ms,
            strategy_closed: init.strategy_closed,
            sibling_cancel_state: init.sibling_cancel_state,
            matched_qty_units_6dp: init.matched_qty_units_6dp,
            matched_qty: init.matched_qty,
            remaining_owned_qty_units_6dp: init.remaining_owned_qty_units_6dp,
            remaining_owned_qty: init.remaining_owned_qty,
            post_proceeds_sell_remaining_position_state: (!ctf_pair_state)
                .then_some(PostProceedsSellState::Flat),
            lifecycle_durability_state: init.lifecycle_durability_state,
            last_update_ms: init.last_update_ms,
            cancel_requested: init.cancel_requested,
        }
    }

    pub(crate) const fn predictive_winner(&self) -> Option<PairTokenSide> {
        self.truth.predictive_winner()
    }

    pub(crate) const fn protected_side(&self) -> Option<PairTokenSide> {
        self.protected_side
    }

    pub(crate) const fn release_winner_side(&self) -> Option<PairTokenSide> {
        self.release_winner_side
    }

    pub(crate) const fn ctf_entry_outlay_units_6dp(&self) -> u128 {
        self.ctf_entry_outlay_units_6dp
    }

    pub(crate) const fn ctf_split_entry_ts_ms(&self) -> u64 {
        self.ctf_split_entry_ts_ms
    }

    pub(crate) const fn resolved_winner(&self) -> Option<PairTokenSide> {
        self.truth.resolved_winner()
    }

    pub(crate) const fn effective_winner(&self) -> Option<PairTokenSide> {
        self.truth.effective_winner()
    }

    pub(crate) const fn oracle_signal_side(&self) -> Option<PairTokenSide> {
        self.oracle_signal_side
    }

    pub(crate) const fn protection_eligible_since_ms(&self) -> Option<u64> {
        self.protection_eligible_since_ms
    }

    pub(crate) const fn protection_authorized(&self) -> bool {
        self.truth.protection_authorized()
    }

    pub(crate) const fn loser_exit_latched(&self) -> bool {
        self.truth.loser_exit_latched()
    }

    pub(crate) const fn winner_exit_latched(&self) -> bool {
        self.truth.winner_exit_latched()
    }

    pub(crate) const fn strategy_closed(&self) -> bool {
        self.strategy_closed
    }

    pub(crate) const fn owner_kind(&self) -> ResolvedInventoryOwnerKind {
        self.truth.owner_kind()
    }

    pub(crate) const fn remaining_owned_side(&self) -> Option<PairTokenSide> {
        self.truth.remaining_owned_side()
    }

    pub(crate) const fn terminal_state(&self) -> Option<ProtectedInventoryTerminalState> {
        self.truth.terminal_state()
    }

    pub(super) const fn lifecycle_truth(&self) -> &PairLifecycleTruth {
        &self.truth
    }

    pub(crate) const fn post_proceeds_sell_remaining_position_state(
        &self,
    ) -> Option<&PostProceedsSellState> {
        self.post_proceeds_sell_remaining_position_state.as_ref()
    }

    pub(super) fn set_prediction(
        &mut self,
        access: PairLifecycleReducerAccess,
        predictive_winner: Option<PairTokenSide>,
        resolved_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        predictive_first_managed_sell_allowed: bool,
    ) {
        if is_confirmed_ctf_split_cohort_id(&self.quote_group) {
            self.truth.set_prediction(
                access,
                predictive_winner.filter(|_| resolved_winner.is_none()),
                resolved_winner,
                effective_winner,
            );
            self.predictive_first_managed_sell_allowed = false;
            return;
        }
        let previous_predictive_winner = self.truth.predictive_winner();
        let previously_allowed = self.predictive_first_managed_sell_allowed;
        let fresh_side_conflict = resolved_winner.is_none()
            && previous_predictive_winner.is_some()
            && predictive_winner.is_some()
            && predictive_winner != previous_predictive_winner;
        let next_predictive_winner = if resolved_winner.is_some() {
            None
        } else {
            self.truth.predictive_winner().or(predictive_winner)
        };
        let latched_permission_matches_lifecycle = previously_allowed
            && !fresh_side_conflict
            && previous_predictive_winner.is_some()
            && previous_predictive_winner == next_predictive_winner;
        let observed_permission_matches_lifecycle = predictive_first_managed_sell_allowed
            && !fresh_side_conflict
            && predictive_winner.is_some()
            && predictive_winner == next_predictive_winner;
        self.truth.set_prediction(
            access,
            next_predictive_winner,
            resolved_winner,
            effective_winner,
        );
        self.predictive_first_managed_sell_allowed = resolved_winner.is_none()
            && next_predictive_winner.is_some()
            && (latched_permission_matches_lifecycle || observed_permission_matches_lifecycle);
    }

    pub(super) fn clear_prediction(&mut self, access: PairLifecycleReducerAccess) {
        self.truth.clear_prediction(access);
        self.predictive_first_managed_sell_allowed = false;
    }

    pub(super) fn set_materialization_oracle_signal_side(
        &mut self,
        _access: PairLifecycleReducerAccess,
        oracle_signal_side: Option<PairTokenSide>,
    ) {
        self.oracle_signal_side = oracle_signal_side;
    }

    pub(super) fn set_materialization_protection_eligible_since(
        &mut self,
        _access: PairLifecycleReducerAccess,
        protection_eligible_since_ms: Option<u64>,
    ) {
        self.protection_eligible_since_ms = protection_eligible_since_ms;
    }

    pub(super) fn materialize_authority(
        &mut self,
        access: PairLifecycleReducerAccess,
        owner_kind: ResolvedInventoryOwnerKind,
        remaining_owned_side: Option<PairTokenSide>,
        protection_authorized: bool,
        terminal_state: Option<ProtectedInventoryTerminalState>,
    ) {
        let protection_authorized = if is_confirmed_ctf_split_cohort_id(&self.quote_group) {
            protection_authorized
        } else {
            protection_authorized
                && !self
                    .post_proceeds_sell_remaining_position_state
                    .as_ref()
                    .is_some_and(PostProceedsSellState::final_attempt_exhausted)
        };
        self.truth.materialize(
            access,
            owner_kind,
            remaining_owned_side,
            protection_authorized,
            terminal_state,
        );
    }

    pub(super) fn set_post_proceeds_sell_open_order(
        &mut self,
        _access: PairLifecycleReducerAccess,
        token: String,
        order_id: String,
        reason: String,
    ) {
        let Some(state) = self.post_proceeds_sell_remaining_position_state.as_mut() else {
            return;
        };
        *state = PostProceedsSellState::Pending {
            token,
            order_id,
            reason,
        };
    }

    pub(super) fn set_post_proceeds_sell_expired_or_unfilled(
        &mut self,
        _access: PairLifecycleReducerAccess,
        token: String,
        order_id: String,
        reason: String,
    ) {
        let Some(state) = self.post_proceeds_sell_remaining_position_state.as_mut() else {
            return;
        };
        *state = PostProceedsSellState::ExpiredOrUnfilled {
            token,
            order_id,
            reason,
        };
    }

    pub(super) fn close_strategy_lifecycle(&mut self, access: PairLifecycleReducerAccess) {
        self.strategy_closed = true;
        self.truth.close_strategy(access);
    }

    pub(super) fn apply_lifecycle_snapshot(
        &mut self,
        access: PairLifecycleReducerAccess,
        snapshot: &Self,
    ) {
        let ctf_pair_state = is_confirmed_ctf_split_cohort_id(&self.quote_group);
        if ctf_pair_state {
            self.predictive_first_managed_sell_allowed = false;
        } else {
            self.predictive_first_managed_sell_allowed =
                snapshot.predictive_first_managed_sell_allowed;
        }
        self.truth
            .apply_snapshot(access, &snapshot.truth, !ctf_pair_state);
        self.strategy_closed = snapshot.strategy_closed;
        if !ctf_pair_state {
            self.post_proceeds_sell_remaining_position_state =
                snapshot.post_proceeds_sell_remaining_position_state.clone();
            self.release_winner_side = snapshot.release_winner_side;
        }
        self.ctf_split_entry_ts_ms = snapshot.ctf_split_entry_ts_ms;
        self.ctf_entry_outlay_units_6dp = snapshot.ctf_entry_outlay_units_6dp;
        self.protected_side =
            Self::merge_protected_side(self.protected_side, snapshot.protected_side);
    }
}

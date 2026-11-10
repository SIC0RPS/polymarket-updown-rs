use super::super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use super::{
    is_confirmed_ctf_split_cohort_id, PairLifecycleState, PairLifecycleStateInit, PairedCohort,
    PostProceedsSellState, ResolvedInventoryOwnerKind,
};

#[path = "reducer/dispositions.rs"]
mod dispositions;
#[path = "reducer/materialization.rs"]
mod materialization;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CtfOwnedLegDisposition {
    HoldForSettlement {
        reason: crate::strategy::facts::CtfLegDecisionReason,
    },
    SellNow {
        authorized_qty_units_6dp: u128,
        close_reason: crate::types::OrderCloseReason,
        reason: crate::strategy::facts::CtfLegDecisionReason,
        post_sell_economics: Option<crate::strategy::facts::CtfPostSellEconomicsEvidence>,
    },
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PairLifecycleEvent<'a> {
    PredictionCleared,
    PredictionObserved {
        predictive_winner: Option<PairTokenSide>,
        resolved_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        predictive_first_managed_sell_allowed: bool,
    },
    AuthorityMaterialized {
        owner_kind: ResolvedInventoryOwnerKind,
        remaining_owned_side: Option<PairTokenSide>,
        protection_authorized: bool,
        terminal_state: Option<ProtectedInventoryTerminalState>,
    },
    CtfOwnedLegDispositionsObserved {
        up: CtfOwnedLegDisposition,
        down: CtfOwnedLegDisposition,
    },
    PostProceedsSellOpenOrderObserved {
        token: &'a str,
        order_id: &'a str,
        reason: &'a str,
    },
    PostProceedsSellExpiredOrUnfilled {
        token: &'a str,
        order_id: &'a str,
        reason: &'a str,
    },
    StrategyClosed,
}

pub(crate) struct PairLifecycleReducer;

#[derive(Clone, Copy)]
pub(super) struct PairLifecycleReducerAccess(());

#[derive(Debug, Clone)]
pub(super) struct PairLifecycleTruth {
    predictive_winner: Option<PairTokenSide>,
    resolved_winner: Option<PairTokenSide>,
    effective_winner: Option<PairTokenSide>,
    protection_authorized: bool,
    loser_exit_latched: bool,
    winner_exit_latched: bool,
    owner_kind: ResolvedInventoryOwnerKind,
    remaining_owned_side: Option<PairTokenSide>,
    terminal_state: Option<ProtectedInventoryTerminalState>,
    ctf_up_disposition: Option<CtfOwnedLegDisposition>,
    ctf_down_disposition: Option<CtfOwnedLegDisposition>,
}

impl PairLifecycleTruth {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        _access: PairLifecycleReducerAccess,
        predictive_winner: Option<PairTokenSide>,
        resolved_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        protection_authorized: bool,
        owner_kind: ResolvedInventoryOwnerKind,
        remaining_owned_side: Option<PairTokenSide>,
        terminal_state: Option<ProtectedInventoryTerminalState>,
    ) -> Self {
        Self {
            predictive_winner,
            resolved_winner,
            effective_winner,
            protection_authorized,
            loser_exit_latched: false,
            winner_exit_latched: false,
            owner_kind,
            remaining_owned_side,
            terminal_state,
            ctf_up_disposition: None,
            ctf_down_disposition: None,
        }
    }

    pub(super) const fn predictive_winner(&self) -> Option<PairTokenSide> {
        self.predictive_winner
    }

    pub(super) const fn resolved_winner(&self) -> Option<PairTokenSide> {
        self.resolved_winner
    }

    pub(super) const fn effective_winner(&self) -> Option<PairTokenSide> {
        self.effective_winner
    }

    pub(super) const fn protection_authorized(&self) -> bool {
        self.protection_authorized
    }

    pub(super) const fn loser_exit_latched(&self) -> bool {
        self.loser_exit_latched
    }

    pub(super) const fn winner_exit_latched(&self) -> bool {
        self.winner_exit_latched
    }

    pub(super) const fn owner_kind(&self) -> ResolvedInventoryOwnerKind {
        self.owner_kind
    }

    pub(super) const fn remaining_owned_side(&self) -> Option<PairTokenSide> {
        self.remaining_owned_side
    }

    pub(super) const fn terminal_state(&self) -> Option<ProtectedInventoryTerminalState> {
        self.terminal_state
    }

    pub(super) const fn ctf_owned_leg_disposition(
        &self,
        side: PairTokenSide,
    ) -> Option<CtfOwnedLegDisposition> {
        match side {
            PairTokenSide::Up => self.ctf_up_disposition,
            PairTokenSide::Down => self.ctf_down_disposition,
        }
    }

    pub(super) fn set_prediction(
        &mut self,
        _access: PairLifecycleReducerAccess,
        predictive_winner: Option<PairTokenSide>,
        resolved_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
    ) {
        self.predictive_winner = predictive_winner;
        self.resolved_winner = resolved_winner;
        self.effective_winner = effective_winner;
    }

    pub(super) fn clear_prediction(&mut self, _access: PairLifecycleReducerAccess) {
        self.predictive_winner = None;
        self.resolved_winner = None;
        self.effective_winner = None;
        self.terminal_state = None;
    }

    pub(super) fn materialize(
        &mut self,
        _access: PairLifecycleReducerAccess,
        owner_kind: ResolvedInventoryOwnerKind,
        remaining_owned_side: Option<PairTokenSide>,
        protection_authorized: bool,
        terminal_state: Option<ProtectedInventoryTerminalState>,
    ) {
        self.owner_kind = owner_kind;
        self.remaining_owned_side = remaining_owned_side;
        self.protection_authorized = protection_authorized;
        self.terminal_state = terminal_state;
    }

    pub(super) fn close_strategy(&mut self, _access: PairLifecycleReducerAccess) {
        self.protection_authorized = false;
    }

    pub(super) fn apply_snapshot(
        &mut self,
        _access: PairLifecycleReducerAccess,
        snapshot: &Self,
        include_non_ctf_state: bool,
    ) {
        self.predictive_winner = snapshot.predictive_winner;
        self.resolved_winner = snapshot.resolved_winner;
        self.effective_winner = snapshot.effective_winner;
        self.protection_authorized = snapshot.protection_authorized;
        if include_non_ctf_state {
            self.loser_exit_latched = snapshot.loser_exit_latched;
            self.winner_exit_latched = snapshot.winner_exit_latched;
        }
        self.owner_kind = snapshot.owner_kind;
        self.remaining_owned_side = snapshot.remaining_owned_side;
        self.terminal_state = snapshot.terminal_state;
        self.ctf_up_disposition = snapshot.ctf_up_disposition;
        self.ctf_down_disposition = snapshot.ctf_down_disposition;
    }

    pub(super) fn sync_cohort_projection(
        &mut self,
        _access: PairLifecycleReducerAccess,
        next: &Self,
    ) {
        self.predictive_winner = next.predictive_winner;
        self.resolved_winner = next.resolved_winner;
        self.effective_winner = next.effective_winner;
        self.owner_kind = next.owner_kind;
        self.loser_exit_latched = next.loser_exit_latched;
        self.winner_exit_latched = next.winner_exit_latched;
        self.ctf_up_disposition = next.ctf_up_disposition;
        self.ctf_down_disposition = next.ctf_down_disposition;
    }
}

impl PairLifecycleReducer {
    const fn access() -> PairLifecycleReducerAccess {
        PairLifecycleReducerAccess(())
    }

    pub(crate) fn new_state(init: PairLifecycleStateInit) -> PairLifecycleState {
        PairLifecycleState::from_reducer(Self::access(), init)
    }

    pub(crate) fn apply_state(state: &mut PairLifecycleState, event: PairLifecycleEvent<'_>) {
        match event {
            PairLifecycleEvent::PredictionCleared => {
                state.clear_prediction(Self::access());
            }
            PairLifecycleEvent::PredictionObserved {
                predictive_winner,
                resolved_winner,
                effective_winner,
                predictive_first_managed_sell_allowed,
            } => {
                state.set_prediction(
                    Self::access(),
                    predictive_winner,
                    resolved_winner,
                    effective_winner,
                    predictive_first_managed_sell_allowed,
                );
            }
            PairLifecycleEvent::AuthorityMaterialized {
                owner_kind,
                remaining_owned_side,
                protection_authorized,
                terminal_state,
            } => {
                state.materialize_authority(
                    Self::access(),
                    owner_kind,
                    remaining_owned_side,
                    protection_authorized,
                    terminal_state,
                );
            }
            PairLifecycleEvent::CtfOwnedLegDispositionsObserved { up, down } => {
                state.observe_ctf_owned_leg_dispositions(Self::access(), up, down);
            }
            PairLifecycleEvent::PostProceedsSellOpenOrderObserved {
                token,
                order_id,
                reason,
            } => {
                if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
                    return;
                }
                let should_observe = match state.post_proceeds_sell_remaining_position_state() {
                    Some(PostProceedsSellState::Flat | PostProceedsSellState::Pending { .. }) => {
                        true
                    }
                    Some(PostProceedsSellState::ExpiredOrUnfilled {
                        order_id: existing_order_id,
                        ..
                    }) => existing_order_id != order_id,
                    None => false,
                };
                if should_observe {
                    state.set_post_proceeds_sell_open_order(
                        Self::access(),
                        token.to_string(),
                        order_id.to_string(),
                        reason.to_string(),
                    );
                }
            }
            PairLifecycleEvent::PostProceedsSellExpiredOrUnfilled {
                token,
                order_id,
                reason,
            } => {
                if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
                    return;
                }
                let already_recorded = matches!(
                    state.post_proceeds_sell_remaining_position_state(),
                    Some(PostProceedsSellState::ExpiredOrUnfilled {
                        token: existing_token,
                        order_id: existing_order_id,
                        ..
                    }) if existing_token == token && existing_order_id == order_id
                );
                if !already_recorded {
                    state.set_post_proceeds_sell_expired_or_unfilled(
                        Self::access(),
                        token.to_string(),
                        order_id.to_string(),
                        reason.to_string(),
                    );
                }
            }
            PairLifecycleEvent::StrategyClosed => {
                state.close_strategy_lifecycle(Self::access());
            }
        }
    }

    pub(crate) fn apply_state_snapshot(
        state: &mut PairLifecycleState,
        snapshot: &PairLifecycleState,
    ) {
        state.apply_lifecycle_snapshot(Self::access(), snapshot);
    }

    pub(crate) fn apply_cohort_lifecycle_snapshot(current: &mut PairedCohort, next: &PairedCohort) {
        current.sync_lifecycle_from_cohort(Self::access(), next);
    }
}

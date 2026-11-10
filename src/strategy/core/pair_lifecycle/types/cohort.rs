use crate::polymarket::identity::MarketId;

use super::super::super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use super::super::is_confirmed_ctf_split_cohort_id;
use super::super::reducer::{PairLifecycleReducerAccess, PairLifecycleTruth};
use super::{PairLifecycleDurabilityState, PairLifecycleState};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PairedCohortKey {
    pub condition_id: MarketId,
    pub yes_token: String,
    pub no_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedInventoryOwnerKind {
    AcceptedPair,
    DirectionalRemainingOwnedLeg,
    LoserRemainingOwnedLeg,
    MatchedHold,
    TerminalSettlement,
}

impl ResolvedInventoryOwnerKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AcceptedPair => "accepted_pair",
            Self::DirectionalRemainingOwnedLeg => "directional_remaining_owned_leg",
            Self::LoserRemainingOwnedLeg => "loser_remaining_owned_leg",
            Self::MatchedHold => "matched_hold",
            Self::TerminalSettlement => "terminal_settlement",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SiblingCancelState {
    NotNeeded,
    Pending,
    Acked,
}

impl SiblingCancelState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NotNeeded => "not_needed",
            Self::Pending => "pending",
            Self::Acked => "acked",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PairedCohort {
    pub key: PairedCohortKey,
    pub market_id: MarketId,
    pub quote_group: String,
    pub up_token: String,
    pub down_token: String,
    ctf_entry_outlay_units_6dp: u128,
    protected_side: Option<PairTokenSide>,
    truth: PairLifecycleTruth,
    strategy_closed: bool,
    pub lifecycle_durability_state: PairLifecycleDurabilityState,
    pub sibling_cancel_state: SiblingCancelState,
    pub reconciliation_epoch: u64,
    pub last_unknown_cancel_epoch: u64,
    pub last_unknown_reason: Option<String>,
    pub last_update_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthoritativePairedCohortView {
    pub market_id: MarketId,
    pub quote_group: String,
    pub up_token: String,
    pub down_token: String,
    ctf_entry_outlay_units_6dp: u128,
    ctf_split_entry_ts_ms: Option<u64>,
    direct_buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    protected_side: Option<PairTokenSide>,
    release_winner_side: Option<PairTokenSide>,
    owner_kind: ResolvedInventoryOwnerKind,
    predictive_winner: Option<PairTokenSide>,
    effective_winner: Option<PairTokenSide>,
    pub matched_qty_units_6dp: u128,
    pub matched_qty: f64,
    remaining_owned_side: Option<PairTokenSide>,
    pub remaining_owned_qty_units_6dp: u128,
    pub remaining_owned_qty: f64,
    strategy_closed: bool,
    protection_authorized: bool,
    winner_exit_latched: bool,
    terminal_state: Option<ProtectedInventoryTerminalState>,
    pub sibling_cancel_state: SiblingCancelState,
    pub last_unknown_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedInventoryOwner {
    pub kind: ResolvedInventoryOwnerKind,
    ctf_pair_state: bool,
    protected_side: Option<PairTokenSide>,
    release_winner_side: Option<PairTokenSide>,
    predictive_winner: Option<PairTokenSide>,
    effective_winner: Option<PairTokenSide>,
    pub matched_qty: f64,
    remaining_owned_side: Option<PairTokenSide>,
    pub matched_qty_units_6dp: u128,
    pub remaining_owned_qty_units_6dp: u128,
    pub remaining_owned_qty: f64,
    terminal_state: Option<ProtectedInventoryTerminalState>,
    pub sibling_cancel_state: SiblingCancelState,
    winner_exit_latched: bool,
    protection_authorized: bool,
}

impl PairedCohort {
    pub(crate) fn from_lifecycle(state: &PairLifecycleState) -> Self {
        Self {
            key: PairedCohortKey {
                condition_id: state.market_id.clone(),
                yes_token: state.up_token.clone(),
                no_token: state.down_token.clone(),
            },
            market_id: state.market_id.clone(),
            quote_group: state.quote_group.clone(),
            up_token: state.up_token.clone(),
            down_token: state.down_token.clone(),
            ctf_entry_outlay_units_6dp: state.ctf_entry_outlay_units_6dp(),
            protected_side: state.protected_side,
            truth: state.lifecycle_truth().clone(),
            strategy_closed: state.strategy_closed,
            lifecycle_durability_state: state.lifecycle_durability_state,
            sibling_cancel_state: state.sibling_cancel_state,
            reconciliation_epoch: 0,
            last_unknown_cancel_epoch: 0,
            last_unknown_reason: None,
            last_update_ms: state.last_update_ms,
        }
    }

    pub(crate) fn matches_market(&self, market_id: &str, token: &str) -> bool {
        self.market_id.as_str() == market_id && (self.up_token == token || self.down_token == token)
    }

    pub(crate) const fn protected_side(&self) -> Option<PairTokenSide> {
        self.protected_side
    }

    pub(crate) const fn effective_winner(&self) -> Option<PairTokenSide> {
        self.truth.effective_winner()
    }

    pub(crate) const fn owner_kind(&self) -> ResolvedInventoryOwnerKind {
        self.truth.owner_kind()
    }

    pub(in crate::strategy::pair_lifecycle) fn sync_lifecycle_from_cohort(
        &mut self,
        access: PairLifecycleReducerAccess,
        next: &PairedCohort,
    ) {
        if is_confirmed_ctf_split_cohort_id(&self.quote_group)
            || is_confirmed_ctf_split_cohort_id(&next.quote_group)
        {
            return;
        }
        self.truth.sync_cohort_projection(access, &next.truth);
        self.protected_side =
            PairLifecycleState::merge_protected_side(self.protected_side, next.protected_side);
        self.ctf_entry_outlay_units_6dp = next.ctf_entry_outlay_units_6dp;
        self.strategy_closed = next.strategy_closed;
    }
}

impl AuthoritativePairedCohortView {
    pub(crate) fn from_confirmed_buy(
        entry: &crate::trade_session_ledger::ConfirmedCtfBuyFact,
        remaining_qty_units_6dp: u128,
        entry_outlay_units_6dp: u128,
    ) -> Self {
        let side = if entry.authorization.selected_up {
            PairTokenSide::Up
        } else {
            PairTokenSide::Down
        };
        Self {
            market_id: entry.market_id.clone(),
            quote_group: entry.cohort_id.clone(),
            up_token: entry.authorization.up_token.to_string(),
            down_token: entry.authorization.down_token.to_string(),
            ctf_entry_outlay_units_6dp: entry_outlay_units_6dp,
            ctf_split_entry_ts_ms: None,
            direct_buy_authorization: Some(entry.authorization),
            protected_side: None,
            release_winner_side: None,
            owner_kind: ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg,
            predictive_winner: None,
            effective_winner: None,
            matched_qty_units_6dp: 0,
            matched_qty: 0.0,
            remaining_owned_side: Some(side),
            remaining_owned_qty_units_6dp: remaining_qty_units_6dp,
            remaining_owned_qty: crate::strategy::full_set_units_to_qty(remaining_qty_units_6dp),
            strategy_closed: false,
            protection_authorized: false,
            winner_exit_latched: false,
            terminal_state: None,
            sibling_cancel_state: SiblingCancelState::NotNeeded,
            last_unknown_reason: None,
        }
    }

    pub(crate) const fn direct_buy_authorization(
        &self,
    ) -> Option<crate::types::DirectBuyAuthorization> {
        self.direct_buy_authorization
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        market_id: MarketId,
        quote_group: String,
        up_token: String,
        down_token: String,
        ctf_entry_outlay_units_6dp: u128,
        ctf_split_entry_ts_ms: u64,
        protected_side: Option<PairTokenSide>,
        release_winner_side: Option<PairTokenSide>,
        owner_kind: ResolvedInventoryOwnerKind,
        predictive_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        matched_qty_units_6dp: u128,
        matched_qty: f64,
        remaining_owned_side: Option<PairTokenSide>,
        remaining_owned_qty_units_6dp: u128,
        remaining_owned_qty: f64,
        strategy_closed: bool,
        protection_authorized: bool,
        winner_exit_latched: bool,
        terminal_state: Option<ProtectedInventoryTerminalState>,
        sibling_cancel_state: SiblingCancelState,
        last_unknown_reason: Option<String>,
    ) -> Self {
        Self {
            market_id,
            quote_group,
            up_token,
            down_token,
            ctf_entry_outlay_units_6dp,
            ctf_split_entry_ts_ms: Some(ctf_split_entry_ts_ms),
            direct_buy_authorization: None,
            protected_side,
            release_winner_side,
            owner_kind,
            predictive_winner,
            effective_winner,
            matched_qty_units_6dp,
            matched_qty,
            remaining_owned_side,
            remaining_owned_qty_units_6dp,
            remaining_owned_qty,
            strategy_closed,
            protection_authorized,
            winner_exit_latched,
            terminal_state,
            sibling_cancel_state,
            last_unknown_reason,
        }
    }

    pub(crate) const fn owner_kind(&self) -> ResolvedInventoryOwnerKind {
        self.owner_kind
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

    pub(crate) const fn ctf_split_entry_ts_ms(&self) -> Option<u64> {
        self.ctf_split_entry_ts_ms
    }

    pub(crate) const fn predictive_winner(&self) -> Option<PairTokenSide> {
        self.predictive_winner
    }

    pub(crate) const fn effective_winner(&self) -> Option<PairTokenSide> {
        self.effective_winner
    }

    pub(crate) const fn remaining_owned_side(&self) -> Option<PairTokenSide> {
        self.remaining_owned_side
    }

    pub(crate) const fn protection_authorized(&self) -> bool {
        self.protection_authorized
    }

    pub(crate) const fn winner_exit_latched(&self) -> bool {
        self.winner_exit_latched
    }

    pub(crate) const fn strategy_closed(&self) -> bool {
        self.strategy_closed
    }

    pub(crate) const fn terminal_state(&self) -> Option<ProtectedInventoryTerminalState> {
        self.terminal_state
    }
}

impl ResolvedInventoryOwner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: ResolvedInventoryOwnerKind,
        ctf_pair_state: bool,
        protected_side: Option<PairTokenSide>,
        release_winner_side: Option<PairTokenSide>,
        predictive_winner: Option<PairTokenSide>,
        effective_winner: Option<PairTokenSide>,
        matched_qty: f64,
        remaining_owned_side: Option<PairTokenSide>,
        matched_qty_units_6dp: u128,
        remaining_owned_qty_units_6dp: u128,
        remaining_owned_qty: f64,
        terminal_state: Option<ProtectedInventoryTerminalState>,
        sibling_cancel_state: SiblingCancelState,
        winner_exit_latched: bool,
        protection_authorized: bool,
    ) -> Self {
        Self {
            kind,
            ctf_pair_state,
            protected_side,
            release_winner_side,
            predictive_winner,
            effective_winner,
            matched_qty,
            remaining_owned_side,
            matched_qty_units_6dp,
            remaining_owned_qty_units_6dp,
            remaining_owned_qty,
            terminal_state,
            sibling_cancel_state,
            winner_exit_latched,
            protection_authorized,
        }
    }

    pub(crate) const fn predictive_winner(&self) -> Option<PairTokenSide> {
        self.predictive_winner
    }

    pub(crate) const fn ctf_pair_state(&self) -> bool {
        self.ctf_pair_state
    }

    pub(crate) const fn protected_side(&self) -> Option<PairTokenSide> {
        self.protected_side
    }

    pub(crate) const fn release_winner_side(&self) -> Option<PairTokenSide> {
        self.release_winner_side
    }

    pub(crate) const fn effective_winner(&self) -> Option<PairTokenSide> {
        self.effective_winner
    }

    pub(crate) const fn remaining_owned_side(&self) -> Option<PairTokenSide> {
        self.remaining_owned_side
    }

    pub(crate) const fn terminal_state(&self) -> Option<ProtectedInventoryTerminalState> {
        self.terminal_state
    }

    pub(crate) const fn winner_exit_latched(&self) -> bool {
        self.winner_exit_latched
    }

    pub(crate) const fn protection_authorized(&self) -> bool {
        self.protection_authorized
    }
}

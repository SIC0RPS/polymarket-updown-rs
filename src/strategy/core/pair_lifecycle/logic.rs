use crate::portfolio::{PortfolioState, PositionState};

use super::super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use super::types::{
    PairLifecycleState, ResolvedInventoryOwner, ResolvedInventoryOwnerKind, SiblingCancelState,
};
use crate::strategy::paired_maker::PAIRED_RELEASE_QTY_EPS;

const INVENTORY_EPS: f64 = 1e-6;

pub(crate) fn resolve_non_ctf_inventory_owner(
    state: &PairLifecycleState,
    sibling_cancel_state: SiblingCancelState,
    up_position: Option<&PositionState>,
    down_position: Option<&PositionState>,
    protection_dwell_ms: u64,
    now_ms: u64,
) -> ResolvedInventoryOwner {
    let up_qty_units_6dp = up_position
        .map(|position| position.qty_units_6dp)
        .unwrap_or(0);
    let down_qty_units_6dp = down_position
        .map(|position| position.qty_units_6dp)
        .unwrap_or(0);
    let matched_qty_units_6dp = up_qty_units_6dp.min(down_qty_units_6dp);
    let up_remaining_owned_qty_units_6dp = up_qty_units_6dp.saturating_sub(matched_qty_units_6dp);
    let down_remaining_owned_qty_units_6dp =
        down_qty_units_6dp.saturating_sub(matched_qty_units_6dp);
    let matched_qty = crate::strategy::full_set_units_to_qty(matched_qty_units_6dp);
    let up_remaining_owned_qty =
        crate::strategy::full_set_units_to_qty(up_remaining_owned_qty_units_6dp);
    let down_remaining_owned_qty =
        crate::strategy::full_set_units_to_qty(down_remaining_owned_qty_units_6dp);
    let up_redeemable = up_position.is_some_and(|position| position.resolved_redeemable);
    let down_redeemable = down_position.is_some_and(|position| position.resolved_redeemable);
    let remaining_owned_side = if up_remaining_owned_qty > INVENTORY_EPS {
        Some(PairTokenSide::Up)
    } else if down_remaining_owned_qty > INVENTORY_EPS {
        Some(PairTokenSide::Down)
    } else {
        None
    };
    let remaining_owned_qty = match remaining_owned_side {
        Some(PairTokenSide::Up) => up_remaining_owned_qty,
        Some(PairTokenSide::Down) => down_remaining_owned_qty,
        None => 0.0,
    };
    let remaining_owned_qty_units_6dp = match remaining_owned_side {
        Some(PairTokenSide::Up) => up_remaining_owned_qty_units_6dp,
        Some(PairTokenSide::Down) => down_remaining_owned_qty_units_6dp,
        None => 0,
    };

    let predictive_winner = state
        .predictive_winner()
        .filter(|_| state.resolved_winner().is_none());
    let entry_side = state.protected_side();
    let release_winner_side = state.release_winner_side();
    let effective_winner = state.effective_winner();
    let resolved_winner =
        effective_winner.filter(|winner| state.resolved_winner() == Some(*winner));
    let mut terminal_state = None;
    let mut protection_authorized = false;
    let mut protection_eligible_since_ms = None;

    let protected_inventory_side = resolved_winner;
    if let Some(protected_inventory_side) = protected_inventory_side {
        let protected_redeemable = match protected_inventory_side {
            PairTokenSide::Up => up_redeemable,
            PairTokenSide::Down => down_redeemable,
        };
        let remaining_owned_redeemable = match remaining_owned_side {
            Some(PairTokenSide::Up) => up_redeemable,
            Some(PairTokenSide::Down) => down_redeemable,
            None => false,
        };
        if remaining_owned_side == Some(protected_inventory_side) {
            let protection_eligible_now = sibling_cancel_state == SiblingCancelState::Acked;
            if protection_eligible_now {
                let eligible_since_ms = state.protection_eligible_since_ms().unwrap_or(now_ms);
                let stable_for_ms = now_ms.saturating_sub(eligible_since_ms);
                protection_eligible_since_ms = Some(eligible_since_ms);
                protection_authorized = stable_for_ms >= protection_dwell_ms;
            }
            if !state.winner_exit_latched() && protected_redeemable {
                terminal_state = Some(ProtectedInventoryTerminalState::Redeemable);
            }
        } else if remaining_owned_side.is_some() && remaining_owned_redeemable {
            terminal_state = Some(ProtectedInventoryTerminalState::Redeemable);
        }
    }

    let resolved_redeemable_complete_set =
        state.resolved_winner().is_some() && (up_redeemable || down_redeemable);
    if matched_qty > INVENTORY_EPS && resolved_redeemable_complete_set {
        terminal_state = Some(ProtectedInventoryTerminalState::Redeemable);
    }

    let resolved_mergeable_complete_set = state.resolved_winner().is_some()
        && (up_position.is_some_and(|position| position.mergeable)
            || down_position.is_some_and(|position| position.mergeable));
    if terminal_state.is_none() && matched_qty > INVENTORY_EPS && resolved_mergeable_complete_set {
        terminal_state = Some(ProtectedInventoryTerminalState::MatchedPairMergeCandidate);
    }
    let kind = if terminal_state.is_some() {
        ResolvedInventoryOwnerKind::TerminalSettlement
    } else if matched_qty > INVENTORY_EPS {
        ResolvedInventoryOwnerKind::MatchedHold
    } else if remaining_owned_side.is_some() {
        match resolved_winner {
            Some(winner_side) if remaining_owned_side == Some(winner_side) => {
                ResolvedInventoryOwnerKind::AcceptedPair
            }
            Some(_) => ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg,
            None => match predictive_winner {
                Some(winner_side) if remaining_owned_side == Some(winner_side) => {
                    ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg
                }
                Some(_) => ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg,
                None => ResolvedInventoryOwnerKind::AcceptedPair,
            },
        }
    } else {
        ResolvedInventoryOwnerKind::AcceptedPair
    };

    let _ = protection_eligible_since_ms;
    ResolvedInventoryOwner::new(
        kind,
        false,
        entry_side,
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
        state.winner_exit_latched(),
        protection_authorized,
    )
}

pub(crate) fn retain_pair_lifecycle_state(
    state: &PairLifecycleState,
    portfolio: &PortfolioState,
    _now_ms: u64,
    _stale_after_ms: u64,
) -> bool {
    let has_position = portfolio
        .position(&state.up_token)
        .is_some_and(|position| position.qty > INVENTORY_EPS)
        || portfolio
            .position(&state.down_token)
            .is_some_and(|position| position.qty > INVENTORY_EPS);
    if super::is_confirmed_ctf_split_cohort_id(&state.quote_group) {
        return has_position || state.strategy_closed() || state.terminal_state().is_some();
    }
    has_position
}

pub(super) fn inventory_eps() -> f64 {
    INVENTORY_EPS
}

pub(super) fn paired_release_qty_eps() -> f64 {
    PAIRED_RELEASE_QTY_EPS
}

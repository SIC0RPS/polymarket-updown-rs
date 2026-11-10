use crate::portfolio::PortfolioState;

use super::super::pair_lifecycle::{ResolvedInventoryOwner, ResolvedInventoryOwnerKind};
use super::{PairTokenSide, ProtectedInventoryTerminalState, INVENTORY_EPS};

#[derive(Debug, Clone, Default)]
pub(crate) struct InventoryPartition {
    pub partition_owner_kind: Option<ResolvedInventoryOwnerKind>,
    pub partition_terminal_state: Option<ProtectedInventoryTerminalState>,
    pub matched_pair_qty: f64,
    pub directional_remaining_owned_qty: f64,
    pub pending_winner_qty: f64,
    pub protected_winner_qty: f64,
    pub winner_exit_qty: f64,
    pub first_managed_sell_qty: f64,
    pub unprotected_yes_qty: f64,
    pub unprotected_no_qty: f64,
    pub directional_side: Option<PairTokenSide>,
    pub winner_side: Option<PairTokenSide>,
    pub confirmed_first_managed_sell_side: Option<PairTokenSide>,
    pub unknown_reason: Option<String>,
}

impl InventoryPartition {
    pub(crate) fn has_terminal_inventory(&self) -> bool {
        self.matched_pair_qty > INVENTORY_EPS || self.protected_winner_qty > INVENTORY_EPS
    }

    pub(crate) fn has_contract_managed_inventory(&self) -> bool {
        (self.partition_owner_kind.is_some() && self.has_terminal_inventory())
            || self.pending_winner_qty > INVENTORY_EPS
            || self.winner_exit_qty > INVENTORY_EPS
            || self.first_managed_sell_qty > INVENTORY_EPS
            || (self.partition_owner_kind.is_some()
                && (self.unprotected_yes_qty > INVENTORY_EPS
                    || self.unprotected_no_qty > INVENTORY_EPS))
            || self.unknown_reason.is_some()
    }

    pub(crate) fn token_unprotected_qty(
        &self,
        token: &str,
        up_token: &str,
        _down_token: &str,
    ) -> f64 {
        if token == up_token {
            self.unprotected_yes_qty.max(0.0)
        } else {
            self.unprotected_no_qty.max(0.0)
        }
    }

    pub(crate) fn token_first_managed_sell_qty(
        &self,
        token: &str,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        if self
            .confirmed_first_managed_sell_side
            .is_some_and(|confirmed_loser| {
                token_matches_side(token, up_token, down_token, confirmed_loser)
            })
        {
            self.first_managed_sell_qty.max(0.0)
        } else {
            0.0
        }
    }

    pub(crate) fn token_directional_remaining_owned_qty(
        &self,
        token: &str,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        if self
            .directional_side
            .is_some_and(|side| token_matches_side(token, up_token, down_token, side))
        {
            self.directional_remaining_owned_qty.max(0.0)
        } else {
            0.0
        }
    }

    pub(crate) fn token_protected_winner_qty(
        &self,
        token: &str,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        if self
            .winner_side
            .is_some_and(|winner| token_matches_side(token, up_token, down_token, winner))
        {
            self.protected_winner_qty.max(0.0)
        } else {
            0.0
        }
    }

    pub(crate) fn token_pending_winner_qty(
        &self,
        token: &str,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        if self
            .winner_side
            .is_some_and(|winner| token_matches_side(token, up_token, down_token, winner))
        {
            self.pending_winner_qty.max(0.0)
        } else {
            0.0
        }
    }

    pub(crate) fn token_winner_exit_qty(
        &self,
        token: &str,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        if self
            .winner_side
            .is_some_and(|winner| token_matches_side(token, up_token, down_token, winner))
        {
            self.winner_exit_qty.max(0.0)
        } else {
            0.0
        }
    }
}

fn token_matches_side(token: &str, up_token: &str, down_token: &str, side: PairTokenSide) -> bool {
    match side {
        PairTokenSide::Up => token == up_token,
        PairTokenSide::Down => token == down_token,
    }
}

pub(crate) fn classify_inventory(
    portfolio: &PortfolioState,
    up_token: &str,
    down_token: &str,
    authoritative_owner: Option<&ResolvedInventoryOwner>,
) -> InventoryPartition {
    let yes_position_qty = portfolio
        .position(up_token)
        .map(|position| position.qty.max(0.0))
        .unwrap_or(0.0);
    let no_position_qty = portfolio
        .position(down_token)
        .map(|position| position.qty.max(0.0))
        .unwrap_or(0.0);
    let matched_pair_qty = yes_position_qty.min(no_position_qty).max(0.0);
    let _net_yes = (yes_position_qty - matched_pair_qty).max(0.0);
    let _net_no = (no_position_qty - matched_pair_qty).max(0.0);

    let mut partition = InventoryPartition {
        matched_pair_qty,
        ..Default::default()
    };

    if let Some(owner) = authoritative_owner {
        let _sibling_cancel_state = owner.sibling_cancel_state;
        let protected_side = owner.protected_side();
        let release_winner_side = owner.release_winner_side();
        let ctf_pair_state = owner.ctf_pair_state();
        let lifecycle_keep_side = if ctf_pair_state {
            None
        } else {
            release_winner_side.or(protected_side)
        };
        let resolved_winner = owner.effective_winner();
        let predictive_winner = owner
            .predictive_winner()
            .filter(|_| owner.effective_winner().is_none());
        partition.partition_owner_kind = Some(owner.kind);
        partition.partition_terminal_state = owner.terminal_state();
        partition.winner_side = if ctf_pair_state {
            resolved_winner.or(predictive_winner)
        } else {
            lifecycle_keep_side.or(resolved_winner)
        };
        match owner.kind {
            ResolvedInventoryOwnerKind::DirectionalRemainingOwnedLeg => {
                if ctf_pair_state {
                    partition.directional_side = owner.remaining_owned_side();
                    partition.directional_remaining_owned_qty = owner.remaining_owned_qty;
                } else if predictive_winner
                    .zip(owner.remaining_owned_side())
                    .is_some_and(|(winner, remaining_owned)| winner == remaining_owned)
                {
                    partition.directional_side = owner.remaining_owned_side();
                    partition.directional_remaining_owned_qty = owner.remaining_owned_qty;
                }
            }
            ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg => {
                if !ctf_pair_state
                    && resolved_winner
                        .zip(owner.remaining_owned_side())
                        .is_some_and(|(winner, remaining_owned)| winner != remaining_owned)
                {
                    partition.confirmed_first_managed_sell_side = owner.remaining_owned_side();
                    partition.first_managed_sell_qty = owner.remaining_owned_qty;
                }
            }
            ResolvedInventoryOwnerKind::AcceptedPair
            | ResolvedInventoryOwnerKind::TerminalSettlement => {
                if ctf_pair_state && owner.terminal_state().is_none() {
                    partition.directional_side = owner.remaining_owned_side();
                    partition.directional_remaining_owned_qty = owner.remaining_owned_qty;
                } else if partition.winner_side.is_none()
                    && owner.remaining_owned_side().is_some()
                    && owner.terminal_state().is_none()
                {
                    partition.directional_side = owner.remaining_owned_side();
                    partition.directional_remaining_owned_qty = owner.remaining_owned_qty;
                } else if owner.winner_exit_latched() {
                    partition.winner_exit_qty = owner.remaining_owned_qty;
                } else if owner.protection_authorized() || owner.terminal_state().is_some() {
                    partition.protected_winner_qty = owner.remaining_owned_qty;
                } else {
                    partition.pending_winner_qty = owner.remaining_owned_qty;
                }
            }
            ResolvedInventoryOwnerKind::MatchedHold => {
                if owner.terminal_state().is_none() {
                    let dump_permit_side = if ctf_pair_state {
                        None
                    } else {
                        resolved_winner.map(|winner_side| match winner_side {
                            PairTokenSide::Up => PairTokenSide::Down,
                            PairTokenSide::Down => PairTokenSide::Up,
                        })
                    };
                    if let Some(dump_permit_side) = dump_permit_side {
                        partition.confirmed_first_managed_sell_side = Some(dump_permit_side);
                        partition.first_managed_sell_qty = matched_pair_qty;
                    }
                }
            }
        }
    }

    let protected_yes_component = match partition.winner_side {
        Some(PairTokenSide::Up) => partition.protected_winner_qty,
        _ => 0.0,
    };
    let pending_yes_component = match partition.winner_side {
        Some(PairTokenSide::Up) => partition.pending_winner_qty,
        _ => 0.0,
    };
    let winner_exit_yes_component = match partition.winner_side {
        Some(PairTokenSide::Up) => partition.winner_exit_qty,
        _ => 0.0,
    };
    let protected_no_component = match partition.winner_side {
        Some(PairTokenSide::Down) => partition.protected_winner_qty,
        _ => 0.0,
    };
    let pending_no_component = match partition.winner_side {
        Some(PairTokenSide::Down) => partition.pending_winner_qty,
        _ => 0.0,
    };
    let winner_exit_no_component = match partition.winner_side {
        Some(PairTokenSide::Down) => partition.winner_exit_qty,
        _ => 0.0,
    };
    let loser_yes_component = match partition.confirmed_first_managed_sell_side {
        Some(PairTokenSide::Up) => partition.first_managed_sell_qty,
        _ => 0.0,
    };
    let loser_no_component = match partition.confirmed_first_managed_sell_side {
        Some(PairTokenSide::Down) => partition.first_managed_sell_qty,
        _ => 0.0,
    };
    let directional_yes_component = match partition.directional_side {
        Some(PairTokenSide::Up) => partition.directional_remaining_owned_qty,
        _ => 0.0,
    };
    let directional_no_component = match partition.directional_side {
        Some(PairTokenSide::Down) => partition.directional_remaining_owned_qty,
        _ => 0.0,
    };
    partition.unprotected_yes_qty = (yes_position_qty
        - matched_pair_qty
        - directional_yes_component
        - protected_yes_component
        - pending_yes_component
        - winner_exit_yes_component
        - loser_yes_component)
        .max(0.0);
    partition.unprotected_no_qty = (no_position_qty
        - matched_pair_qty
        - directional_no_component
        - protected_no_component
        - pending_no_component
        - winner_exit_no_component
        - loser_no_component)
        .max(0.0);

    partition
}

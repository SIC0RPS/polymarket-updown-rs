use crate::portfolio::PortfolioState;
use crate::strategy::paired_maker::MakerRebateMmStrategy;

use super::super::pair_lifecycle::{
    AuthoritativePairedCohortView, PairedCohortKey, ResolvedInventoryOwner,
    ResolvedInventoryOwnerKind, SiblingCancelState,
};
use super::{
    classify_inventory, InventoryPartition, PairedAction, ProtectedInventoryTerminalState,
    INVENTORY_EPS,
};

#[derive(Debug, Clone)]
pub(in crate::strategy) struct PairedMarketDisposition {
    pub(in crate::strategy) cohort_key: Option<PairedCohortKey>,
    pub(in crate::strategy) cohort: Option<AuthoritativePairedCohortView>,
    pub(in crate::strategy) partition: InventoryPartition,
    pub(in crate::strategy) managed_as_paired: bool,
}

#[derive(Debug, Clone)]
pub(in crate::strategy) struct PairedDispositionContext {
    pub(in crate::strategy) cohort_key: Option<PairedCohortKey>,
    pub(in crate::strategy) up_token: String,
    pub(in crate::strategy) down_token: String,
    pub(in crate::strategy) managed_as_paired: bool,
}

#[derive(Debug, Clone)]
pub(super) struct PairedTokenDisposition {
    pub(super) market: PairedMarketDisposition,
    pub(super) action: Option<PairedAction>,
}

impl MakerRebateMmStrategy {
    pub(super) fn paired_action_from_market_disposition(
        &mut self,
        cohort_key: &PairedCohortKey,
        cohort: &AuthoritativePairedCohortView,
        partition: &InventoryPartition,
        token: &str,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> Option<PairedAction> {
        let ctf_pair_state =
            crate::strategy::pair_lifecycle::is_confirmed_ctf_split_cohort_id(&cohort.quote_group);
        if ctf_pair_state {
            return None;
        }
        let token_leftover =
            partition.token_unprotected_qty(token, &cohort.up_token, &cohort.down_token);
        let closed_lifecycle_qty = partition
            .token_first_managed_sell_qty(token, &cohort.up_token, &cohort.down_token)
            .max(partition.token_winner_exit_qty(token, &cohort.up_token, &cohort.down_token))
            .max(partition.token_protected_winner_qty(token, &cohort.up_token, &cohort.down_token))
            .max(partition.token_pending_winner_qty(token, &cohort.up_token, &cohort.down_token))
            .max(partition.token_directional_remaining_owned_qty(
                token,
                &cohort.up_token,
                &cohort.down_token,
            ))
            .max(token_leftover);

        if cohort.strategy_closed() && closed_lifecycle_qty > INVENTORY_EPS {
            self.enter_paired_cohort_unknown(
                cohort_key,
                cohort.market_id.as_str(),
                token,
                "ctf_closed_lifecycle_inventory_reappeared",
                now_ms,
                market_projection,
            );
            Some(PairedAction::FreezeUnknown)
        } else if partition.unknown_reason.is_some() {
            self.enter_paired_cohort_unknown(
                cohort_key,
                cohort.market_id.as_str(),
                token,
                "paired_cohort_classification_unknown",
                now_ms,
                market_projection,
            );
            Some(PairedAction::FreezeUnknown)
        } else if cohort.owner_kind() == ResolvedInventoryOwnerKind::LoserRemainingOwnedLeg
            && cohort.sibling_cancel_state == SiblingCancelState::Pending
        {
            Some(PairedAction::CancelSibling)
        } else if partition.token_first_managed_sell_qty(
            token,
            &cohort.up_token,
            &cohort.down_token,
        ) > INVENTORY_EPS
        {
            let qty =
                partition.token_first_managed_sell_qty(token, &cohort.up_token, &cohort.down_token);
            Some(PairedAction::FirstManagedSell { qty })
        } else if partition.token_winner_exit_qty(token, &cohort.up_token, &cohort.down_token)
            > INVENTORY_EPS
        {
            let qty = partition.token_winner_exit_qty(token, &cohort.up_token, &cohort.down_token);
            Some(PairedAction::PostProceedsSell { qty })
        } else if partition.token_directional_remaining_owned_qty(
            token,
            &cohort.up_token,
            &cohort.down_token,
        ) > INVENTORY_EPS
        {
            Some(PairedAction::HoldDirectionalRemainingOwnedLeg)
        } else if partition.token_protected_winner_qty(token, &cohort.up_token, &cohort.down_token)
            > INVENTORY_EPS
        {
            Some(match cohort.owner_kind() {
                ResolvedInventoryOwnerKind::TerminalSettlement
                    if cohort.terminal_state()
                        == Some(ProtectedInventoryTerminalState::Redeemable) =>
                {
                    PairedAction::HoldForRedeem
                }
                _ => PairedAction::HoldProtected,
            })
        } else if partition.token_pending_winner_qty(token, &cohort.up_token, &cohort.down_token)
            > INVENTORY_EPS
        {
            Some(match cohort.owner_kind() {
                ResolvedInventoryOwnerKind::TerminalSettlement
                    if cohort.terminal_state()
                        == Some(ProtectedInventoryTerminalState::Redeemable) =>
                {
                    PairedAction::HoldForRedeem
                }
                _ => PairedAction::HoldPendingProtection,
            })
        } else if partition.matched_pair_qty > INVENTORY_EPS
            && (token == cohort.up_token || token == cohort.down_token)
        {
            Some(match cohort.owner_kind() {
                ResolvedInventoryOwnerKind::TerminalSettlement
                    if cohort.terminal_state()
                        == Some(ProtectedInventoryTerminalState::MatchedPairMergeCandidate) =>
                {
                    PairedAction::HoldForMerge
                }
                _ => PairedAction::HoldMatchedPair,
            })
        } else if token_leftover > INVENTORY_EPS {
            self.enter_paired_cohort_unknown(
                cohort_key,
                cohort.market_id.as_str(),
                token,
                "paired_cohort_unreconciled_leftover",
                now_ms,
                market_projection,
            );
            Some(PairedAction::FreezeUnknown)
        } else {
            Some(PairedAction::QuoteBoth)
        }
    }

    pub(in crate::strategy) fn resolve_paired_disposition_context(
        &mut self,
        token: &str,
        market_id: &str,
        _now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> Option<PairedDispositionContext> {
        let (cohort_key, up_token, down_token) =
            self.resolve_paired_disposition_seed(token, market_id, market_projection)?;
        self.finalize_paired_disposition_context(
            market_id,
            cohort_key,
            Some(up_token),
            Some(down_token),
        )
    }

    fn resolve_paired_disposition_seed(
        &mut self,
        token: &str,
        market_id: &str,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> Option<(Option<PairedCohortKey>, String, String)> {
        if let Some(market) =
            Self::market_selection_for_token_in_projection(market_projection, token, market_id)
        {
            let up_token = market.up_token_id().to_owned();
            let down_token = market.down_token_id().to_owned();
            let cohort_key = self
                .resolve_paired_cohort_key_from_authority(&up_token, market_id)
                .or_else(|| self.resolve_paired_cohort_key_from_authority(&down_token, market_id));
            return Some((cohort_key, up_token, down_token));
        }

        let cohort_key = self.resolve_paired_cohort_key_from_authority(token, market_id);
        let cohort = cohort_key
            .as_ref()
            .and_then(|key| self.authoritative_paired_cohort_view_by_key(key))?;
        Some((cohort_key, cohort.up_token, cohort.down_token))
    }

    fn finalize_paired_disposition_context(
        &mut self,
        _market_id: &str,
        cohort_key: Option<PairedCohortKey>,
        up_token: Option<String>,
        down_token: Option<String>,
    ) -> Option<PairedDispositionContext> {
        let (up_token, down_token) = match (up_token, down_token) {
            (Some(up_token), Some(down_token)) => (up_token, down_token),
            _ => return None,
        };
        let managed_as_paired = cohort_key.is_some();

        Some(PairedDispositionContext {
            cohort_key,
            up_token,
            down_token,
            managed_as_paired,
        })
    }

    pub(in crate::strategy) fn build_paired_market_disposition(
        &self,
        portfolio: &PortfolioState,
        context: Option<&PairedDispositionContext>,
    ) -> PairedMarketDisposition {
        let Some(context) = context else {
            return PairedMarketDisposition {
                cohort_key: None,
                cohort: None,
                partition: InventoryPartition::default(),
                managed_as_paired: false,
            };
        };
        let cohort = context
            .cohort_key
            .as_ref()
            .and_then(|key| self.authoritative_paired_cohort_view_by_key(key));
        let ctf_pair_state = cohort.as_ref().is_some_and(|cohort| {
            crate::strategy::pair_lifecycle::is_confirmed_ctf_split_cohort_id(&cohort.quote_group)
        });
        let owner_snapshot = cohort
            .as_ref()
            .filter(|_| context.managed_as_paired && !ctf_pair_state)
            .map(|cohort| {
                ResolvedInventoryOwner::new(
                    cohort.owner_kind(),
                    false,
                    cohort.protected_side(),
                    cohort.release_winner_side(),
                    cohort.predictive_winner(),
                    cohort.effective_winner(),
                    cohort.matched_qty,
                    cohort.remaining_owned_side(),
                    cohort.matched_qty_units_6dp,
                    cohort.remaining_owned_qty_units_6dp,
                    cohort.remaining_owned_qty,
                    cohort.terminal_state(),
                    cohort.sibling_cancel_state,
                    cohort.winner_exit_latched(),
                    cohort.protection_authorized(),
                )
            });
        let mut partition = if ctf_pair_state {
            InventoryPartition::default()
        } else {
            classify_inventory(
                portfolio,
                &context.up_token,
                &context.down_token,
                owner_snapshot.as_ref(),
            )
        };

        if context.managed_as_paired && !ctf_pair_state {
            partition.partition_owner_kind = cohort.as_ref().map(|cohort| cohort.owner_kind());
            partition.partition_terminal_state =
                cohort.as_ref().and_then(|cohort| cohort.terminal_state());
            partition.unknown_reason = cohort
                .as_ref()
                .and_then(|cohort| cohort.last_unknown_reason.clone());
        }

        PairedMarketDisposition {
            cohort_key: context.cohort_key.clone(),
            cohort,
            partition,
            managed_as_paired: context.managed_as_paired,
        }
    }

    pub(super) fn derive_paired_token_disposition(
        &mut self,
        _portfolio: &PortfolioState,
        token: &str,
        _market_id: &str,
        market_disposition: PairedMarketDisposition,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> PairedTokenDisposition {
        let action = if market_disposition.managed_as_paired {
            if let (Some(key), Some(cohort)) = (
                market_disposition.cohort_key.as_ref(),
                market_disposition.cohort.as_ref(),
            ) {
                self.paired_action_from_market_disposition(
                    key,
                    cohort,
                    &market_disposition.partition,
                    token,
                    now_ms,
                    market_projection,
                )
            } else {
                None
            }
        } else {
            None
        };
        PairedTokenDisposition {
            market: market_disposition,
            action,
        }
    }
}

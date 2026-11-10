// Inventory/projection helpers only.
use std::sync::Arc;

use super::*;
use crate::strategy::pair_lifecycle::ResolvedInventoryOwnerKind;
use crate::strategy::{CancelMarketRequest, StrategyAction, StrategyDecision};
use crate::types::Side;

const RECENT_CANCEL_MARKET_GUARD_MS: u64 = 60_000;

impl MakerRebateMmStrategy {
    pub(super) fn inventory_partition(
        &mut self,
        portfolio: &PortfolioState,
        market: &MarketSelection,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> crate::strategy::hold_to_resolution::InventoryPartition {
        let context = self.resolve_paired_disposition_context(
            market.up_token_id(),
            market.market_id.as_str(),
            portfolio.updated_at_ms,
            market_projection,
        );
        self.build_paired_market_disposition(portfolio, context.as_ref())
            .partition
    }

    pub(in crate::strategy) fn tracked_markets_from_projection(
        market_projection: &MarketProjection,
    ) -> Arc<[MarketSelection]> {
        market_projection.market_rows()
    }

    pub(in crate::strategy) fn market_selection_for_token_in_projection(
        market_projection: &MarketProjection,
        token: &str,
        market_id: &str,
    ) -> Option<MarketSelection> {
        market_projection
            .market_for_token(token, market_id)
            .cloned()
    }

    pub(in crate::strategy) fn pending_cancel_market_request(&self, market_id: &str) -> bool {
        self.pending.iter().any(|decision| {
            matches!(
                &decision.action,
                StrategyAction::CancelMarket(request) if request.market_id.as_str() == market_id
            )
        })
    }

    fn cancel_market_guard_active(&mut self, market_id: &str, now_ms: u64) -> bool {
        let Some(emitted_at_ms) = self.cancel_market_guard_by_id.get(market_id).copied() else {
            return false;
        };
        if now_ms.saturating_sub(emitted_at_ms) >= RECENT_CANCEL_MARKET_GUARD_MS {
            self.cancel_market_guard_by_id.remove(market_id);
            return false;
        }
        true
    }

    pub(in crate::strategy) fn queue_cancel_market_request(
        &mut self,
        market_id: MarketId,
        gamma_market_id: Option<String>,
        now_ms: u64,
        reason: String,
    ) {
        if market_id.is_empty()
            || self.pending_cancel_market_request(market_id.as_str())
            || self.cancel_market_guard_active(market_id.as_str(), now_ms)
        {
            return;
        }
        self.cancel_market_guard_by_id
            .insert(market_id.to_string(), now_ms);
        self.pending.push_back(StrategyDecision {
            action: StrategyAction::CancelMarket(CancelMarketRequest {
                market_id,
                gamma_market_id,
            }),
            context: None,
            p_up: None,
            edge: None,
            reason,
        });
    }

    pub(crate) fn queue_protected_inventory_market_cancels(
        &mut self,
        market_projection: &MarketProjection,
        now_ms: u64,
    ) -> HashSet<String> {
        let portfolio = self.portfolio_rx.borrow().clone();
        let markets = Self::tracked_markets_from_projection(market_projection);
        let mut protected_market_ids = HashSet::new();

        for market in markets.iter() {
            let confirmed_ctf_inventory_owned =
                self.pair_lifecycle_by_group.values().any(|state| {
                    crate::strategy::pair_lifecycle::is_confirmed_ctf_split_cohort_id(
                        &state.quote_group,
                    ) && state.market_id == market.market_id
                        && state.up_token == market.up_token_id()
                        && state.down_token == market.down_token_id()
                        && [&state.up_token, &state.down_token]
                            .into_iter()
                            .any(|token| {
                                portfolio
                                    .position(token)
                                    .is_some_and(|position| position.qty_units_6dp > 0)
                            })
                });
            if confirmed_ctf_inventory_owned {
                for token in [market.up_token_id(), market.down_token_id()] {
                    self.queue_cancel_quotes_for_group(
                        &market.market_id,
                        &format!("ctf_inventory:{}:{token}", market.market_id),
                        Some(Side::Sell),
                        now_ms,
                        "ctf_inventory_lifecycle_took_authority",
                    );
                }
                protected_market_ids.insert(market.market_id.to_string());
                continue;
            }
            let partition = self.inventory_partition(&portfolio, market, market_projection);
            if !partition.has_contract_managed_inventory() {
                continue;
            }
            if partition.partition_owner_kind == Some(ResolvedInventoryOwnerKind::MatchedHold)
                && partition.winner_side.is_none()
                && partition.partition_terminal_state.is_none()
                && partition.first_managed_sell_qty <= 1e-6
                && partition.winner_exit_qty <= 1e-6
            {
                continue;
            }
            for token in [market.up_token_id(), market.down_token_id()] {
                self.queue_cancel_quotes_for_group(
                    &market.market_id,
                    &format!("ctf_inventory:{}:{token}", market.market_id),
                    Some(Side::Sell),
                    now_ms,
                    "ctf_inventory_lifecycle_took_authority",
                );
            }
            protected_market_ids.insert(market.market_id.to_string());
        }

        protected_market_ids
    }
}

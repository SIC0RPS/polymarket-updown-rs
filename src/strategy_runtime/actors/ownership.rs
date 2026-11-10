use crate::ctf_inventory::is_confirmed_ctf_split_cohort_id;
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::strategy::{InventoryOperation, StrategyAction};

use super::{CentralSymbolCommandOwnership, MakerRebateMmActor, MakerRebateMmActorScope};

impl MakerRebateMmActor {
    pub fn command_scope(&self) -> MakerRebateMmActorScope {
        MakerRebateMmActorScope {
            symbol: self.symbol,
            ownership: self.ownership.clone(),
        }
    }

    pub fn action_belongs(&self, action: &StrategyAction) -> bool {
        self.command_scope().action_belongs(action)
    }

    pub fn fault_cancel_requests(&self) -> Vec<crate::strategy::CancelRequest> {
        let mut markets = self
            .open_orders_tx
            .borrow()
            .orders
            .values()
            .map(|order| order.market.clone())
            .collect::<Vec<_>>();
        markets.sort();
        markets.dedup();
        markets
            .into_iter()
            .map(|market_id| {
                crate::strategy::CancelRequest::Market(crate::strategy::CancelMarketRequest {
                    market_id: market_id.into(),
                    gamma_market_id: None,
                })
            })
            .collect()
    }

    pub fn fault_cancel_decisions(&self) -> Vec<crate::strategy::StrategyDecision> {
        self.fault_cancel_requests()
            .into_iter()
            .map(|request| crate::strategy::StrategyDecision {
                action: match request {
                    crate::strategy::CancelRequest::Market(request) => {
                        StrategyAction::CancelMarket(request)
                    }
                    crate::strategy::CancelRequest::Quotes(request) => {
                        StrategyAction::CancelQuotes(request)
                    }
                },
                context: None,
                p_up: None,
                edge: None,
                reason: "symbol_actor_cancel_only".to_string(),
            })
            .collect()
    }
}

impl MakerRebateMmActorScope {
    pub fn action_belongs(&self, action: &StrategyAction) -> bool {
        action_belongs_to(self.symbol, &self.ownership, action)
    }

    pub fn allowed_while_cancel_only(&self, action: &StrategyAction) -> bool {
        matches!(
            action,
            StrategyAction::CancelMarket(_) | StrategyAction::CancelQuotes(_)
        )
    }
}

impl CentralSymbolCommandOwnership {
    pub fn action_belongs(&self, symbol: &str, action: &StrategyAction) -> bool {
        action_belongs_to(symbol, &self.ownership, action)
    }
}

fn action_belongs_to(
    symbol: &str,
    ownership: &SymbolOwnershipIndex,
    action: &StrategyAction,
) -> bool {
    match action {
        StrategyAction::Order(order) => {
            let Some(market_id) = order.market_id.as_deref() else {
                return false;
            };
            if !ownership.facts_belong_to(
                symbol,
                order.lifecycle_symbol.as_deref(),
                Some(market_id),
                &[&order.instrument],
            ) {
                return false;
            }
            order.lifecycle_cohort.as_deref().is_none_or(|cohort| {
                ownership.claim_cohort(cohort, symbol).is_ok()
                    && ownership.cohort_belongs(cohort, symbol)
            })
        }
        StrategyAction::Inventory(intent) => {
            if !ownership.facts_belong_to(
                symbol,
                None,
                Some(intent.market_id.as_str()),
                &[&intent.up_token, &intent.down_token],
            ) {
                return false;
            }
            match intent.operation {
                InventoryOperation::SplitCompleteSet => intent.cohort_id.is_none(),
                InventoryOperation::MergeCompleteSet => {
                    intent.cohort_id.as_deref().is_some_and(|cohort| {
                        is_confirmed_ctf_split_cohort_id(cohort)
                            && ownership.claim_cohort(cohort, symbol).is_ok()
                            && ownership.cohort_belongs(cohort, symbol)
                    })
                }
            }
        }
        StrategyAction::CancelMarket(request) => {
            ownership.market_belongs(request.market_id.as_str(), symbol)
        }
        StrategyAction::CancelQuotes(request) => request
            .market_id
            .as_ref()
            .is_some_and(|market| ownership.market_belongs(market.as_str(), symbol)),
    }
}

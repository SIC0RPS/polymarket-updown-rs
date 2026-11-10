use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::state::{BookDepth, BookTop};
use crate::strategy::facts::CtfLegDecisionReason;
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::{ctf_units_to_clob_qty, ctf_units_to_clob_units_6dp, full_set_units_to_qty};
use crate::strategy::{StrategyFact, StrategyFactSink};
use crate::types::{OrderCloseReason, Side};
use std::collections::HashSet;

use super::super::hold_to_resolution::{
    CtfLegDecisionInput, CtfLegExecutableSell, CtfLegSellAuthorization, PairTokenSide,
};

#[path = "paired_sell/ctf_decision_inputs.rs"]
mod ctf_decision_inputs;
#[path = "paired_sell/queue.rs"]
mod queue;
#[path = "paired_sell/support.rs"]
mod support;

struct PairedManagedSellRequest<'a> {
    fact_buffer: &'a mut StrategyFactSink,
    market: Option<&'a MarketSelection>,
    token: &'a str,
    top: &'a BookTop,
    depth: Option<&'a BookDepth>,
    fair: f64,
    market_window_end_ms: Option<u64>,
    permitted_inventory_units_6dp: u128,
    permitted_inventory_qty: f64,
    market_id: &'a str,
    lifecycle_cohort: &'a str,
    lifecycle_symbol: Option<&'a str>,
    close_hint: &'a str,
    require_positive_edge: bool,
    authorization: &'a CtfLegSellAuthorization,
    now_ms: u64,
}

impl MakerRebateMmStrategy {
    pub(crate) fn queue_paired_managed_sells(
        &mut self,
        fact_buffer: &mut StrategyFactSink,
        now_ms: u64,
        market_projection: &crate::polymarket::state::MarketProjection,
    ) -> HashSet<String> {
        let mut exit_market_ids = HashSet::new();
        let portfolio_snapshot = self.portfolio_rx.borrow().clone();
        let candidates = self.paired_exit_candidates(&portfolio_snapshot);
        let base_cfg = self.cfg.clone();

        for cohort in candidates {
            let direct_buy = cohort.direct_buy_authorization();
            let ctf_entry = direct_buy.is_some()
                || super::super::pair_lifecycle::is_confirmed_ctf_split_cohort_id(
                    &cohort.quote_group,
                );
            self.cfg = base_cfg.clone();
            let market = market_projection
                .market_by_id(cohort.market_id.as_str())
                .cloned();
            if let Some(market) = market.as_ref() {
                self.cfg = base_cfg.config_for_symbol(&market.underlying).clone();
            }
            if market
                .as_ref()
                .is_some_and(|market| self.market_quarantined_from_exit_routing(market))
            {
                if ctf_entry {
                    let decision_inputs = self.ctf_decision_inputs_for_blocked_market(
                        &portfolio_snapshot,
                        &cohort,
                        CtfLegDecisionReason::MarketQuarantinedBeforeSellRoute,
                    );
                    let _ = self.ctf_owned_leg_authorizations_for_market(
                        fact_buffer,
                        &portfolio_snapshot,
                        &cohort,
                        cohort.market_id.as_str(),
                        now_ms,
                        market_projection,
                        decision_inputs,
                    );
                }
                continue;
            }

            let (up_book, down_book, up_depth, down_depth) = {
                let odds = self.odds.as_ref();
                let (up_book, down_book) = self.paired_books_for_tokens(
                    &odds,
                    market.as_ref(),
                    &cohort.up_token,
                    &cohort.down_token,
                );
                let (up_depth, down_depth) = market
                    .as_ref()
                    .map(|market| self.current_depths_for_market(&odds, market))
                    .unwrap_or((None, None));
                (up_book, down_book, up_depth, down_depth)
            };
            if !ctf_entry {
                let (Some(up_book), Some(down_book)) = (up_book.as_ref(), down_book.as_ref())
                else {
                    continue;
                };
                let (Some(_up_bid), Some(_up_ask), Some(_down_bid), Some(_down_ask)) =
                    (up_book.bid, up_book.ask, down_book.bid, down_book.ask)
                else {
                    continue;
                };
            }
            let exit_fairs = match (market.as_ref(), up_book.as_ref(), down_book.as_ref()) {
                (Some(market), Some(up_book), Some(down_book)) => self.exit_fairs_for_market(
                    market,
                    up_book,
                    down_book,
                    up_depth.as_ref(),
                    down_depth.as_ref(),
                    now_ms,
                ),
                _ => None,
            };
            if exit_fairs.is_none() && !ctf_entry {
                continue;
            }
            let up_exit_fair = up_book.as_ref().and_then(|book| {
                book.bid.map(|bid| {
                    exit_fairs
                        .map(|fairs| fairs.up_fair)
                        .unwrap_or_else(|| bid.clamp(0.0, 1.0))
                })
            });
            let down_exit_fair = down_book.as_ref().and_then(|book| {
                book.bid.map(|bid| {
                    exit_fairs
                        .map(|fairs| fairs.down_fair)
                        .unwrap_or_else(|| bid.clamp(0.0, 1.0))
                })
            });
            let ctf_authorized_sells = if ctf_entry {
                let oracle_snapshot = self.oracle_authority_by_market.get(&cohort.market_id);
                let ctf_up_fair = oracle_snapshot.and_then(|snapshot| {
                    crate::strategy::support::ctf_economics::side_fair_value(
                        snapshot,
                        PairTokenSide::Up,
                    )
                });
                let ctf_down_fair = oracle_snapshot.and_then(|snapshot| {
                    crate::strategy::support::ctf_economics::side_fair_value(
                        snapshot,
                        PairTokenSide::Down,
                    )
                });
                let owned_units = |token: &str| {
                    if direct_buy.is_some() {
                        portfolio_snapshot
                            .confirmed_ctf_cohort_qty_units_6dp(token, &cohort.quote_group)
                            .unwrap_or(0)
                    } else {
                        portfolio_snapshot
                            .position(token)
                            .map_or(0, |position| position.qty_units_6dp)
                    }
                };
                let up_owned_units_6dp = owned_units(&cohort.up_token);
                let down_owned_units_6dp = owned_units(&cohort.down_token);
                let up_owned_qty = full_set_units_to_qty(up_owned_units_6dp);
                let down_owned_qty = full_set_units_to_qty(down_owned_units_6dp);
                let up_route_units_6dp = ctf_units_to_clob_units_6dp(up_owned_units_6dp);
                let down_route_units_6dp = ctf_units_to_clob_units_6dp(down_owned_units_6dp);
                let up_route_qty = ctf_units_to_clob_qty(up_route_units_6dp);
                let down_route_qty = ctf_units_to_clob_qty(down_route_units_6dp);
                let ctf_surfaces = [
                    (
                        cohort.up_token.as_str(),
                        PairTokenSide::Up,
                        up_book.as_ref(),
                        up_exit_fair,
                        ctf_up_fair,
                        up_depth.clone(),
                        up_owned_units_6dp,
                        up_owned_qty,
                        up_route_units_6dp,
                        up_route_qty,
                    ),
                    (
                        cohort.down_token.as_str(),
                        PairTokenSide::Down,
                        down_book.as_ref(),
                        down_exit_fair,
                        ctf_down_fair,
                        down_depth.clone(),
                        down_owned_units_6dp,
                        down_owned_qty,
                        down_route_units_6dp,
                        down_route_qty,
                    ),
                ];
                let mut decision_inputs = Vec::new();
                for (
                    token,
                    token_side,
                    book,
                    fair_px,
                    ctf_signal_fair_px,
                    depth,
                    owned_qty_units_6dp,
                    owned_qty,
                    qty_units_6dp,
                    qty,
                ) in ctf_surfaces
                {
                    if direct_buy.is_some() && cohort.remaining_owned_side() != Some(token_side) {
                        continue;
                    }
                    if qty_units_6dp == 0 {
                        decision_inputs.push(CtfLegDecisionInput {
                            token: token.to_string(),
                            token_side,
                            owned_qty_units_6dp,
                            owned_qty,
                            executable: None,
                            route_block_reason: Some(CtfLegDecisionReason::NoFillableQty),
                        });
                        continue;
                    }
                    let Some(book) = book else {
                        decision_inputs.push(CtfLegDecisionInput {
                            token: token.to_string(),
                            token_side,
                            owned_qty_units_6dp,
                            owned_qty,
                            executable: None,
                            route_block_reason: Some(CtfLegDecisionReason::PairedBooksUnavailable),
                        });
                        continue;
                    };
                    let Some(fair_px) = fair_px else {
                        decision_inputs.push(CtfLegDecisionInput {
                            token: token.to_string(),
                            token_side,
                            owned_qty_units_6dp,
                            owned_qty,
                            executable: None,
                            route_block_reason: Some(
                                CtfLegDecisionReason::CtfEntryNoExecutableSellSurface,
                            ),
                        });
                        continue;
                    };
                    let limit_px = self.paired_managed_sell_limit_px(book);
                    if let Some(limit_px) = limit_px {
                        decision_inputs.push(CtfLegDecisionInput {
                            token: token.to_string(),
                            token_side,
                            owned_qty_units_6dp,
                            owned_qty,
                            executable: Some(CtfLegExecutableSell {
                                token_side,
                                qty_units_6dp,
                                qty,
                                book_top: book.clone(),
                                limit_px,
                                tick_size: book.tick_size,
                                fair_px,
                                ctf_signal_fair_px,
                                depth,
                            }),
                            route_block_reason: None,
                        });
                        continue;
                    }
                    decision_inputs.push(CtfLegDecisionInput {
                        token: token.to_string(),
                        token_side,
                        owned_qty_units_6dp,
                        owned_qty,
                        executable: None,
                        route_block_reason: Some(
                            CtfLegDecisionReason::CtfEntryNoExecutableSellSurface,
                        ),
                    });
                }
                self.ctf_owned_leg_authorizations_for_market(
                    fact_buffer,
                    &portfolio_snapshot,
                    &cohort,
                    cohort.market_id.as_str(),
                    now_ms,
                    market_projection,
                    decision_inputs,
                )
            } else {
                Vec::new()
            };
            if let Some(entry_authorization) =
                direct_buy.filter(|_| !ctf_authorized_sells.is_empty())
            {
                let original_buy_group = self.open_orders_rx.as_ref().and_then(|rx| {
                    let orders = rx.borrow();
                    orders
                        .orders
                        .values()
                        .find(|order| {
                            order.side == Side::Buy
                                && order.execution_intent_id == Some(entry_authorization.intent_id)
                                && order.market == cohort.market_id.as_str()
                                && order.lifecycle_cohort.as_deref()
                                    == Some(cohort.quote_group.as_str())
                                && order.remaining_size() > 0.0
                                && portfolio_snapshot.confirmed_direct_buy_receipts().any(
                                    |receipt| {
                                        receipt.authorization == entry_authorization
                                            && receipt.order_id == order.order_id
                                            && receipt.token == order.token
                                    },
                                )
                        })
                        .map(|order| order.quote_group.clone())
                });
                if let Some(quote_group) = original_buy_group {
                    if let Some(quote_group) = quote_group {
                        self.queue_cancel_quotes_for_group(
                            &cohort.market_id,
                            &quote_group,
                            Some(Side::Buy),
                            now_ms,
                            "owned_buy_exit_cancel_entry_remainder",
                        );
                    }
                    for authorization in ctf_authorized_sells {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            authorization
                                .owner_outcome
                                .with_execution_action_qty_units_6dp(0),
                        ));
                    }
                    exit_market_ids.insert(cohort.market_id.to_string());
                    continue;
                }
            }
            let tokens = [
                (
                    cohort.up_token.as_str(),
                    PairTokenSide::Up,
                    up_book.as_ref(),
                    up_depth.as_ref(),
                    up_exit_fair,
                ),
                (
                    cohort.down_token.as_str(),
                    PairTokenSide::Down,
                    down_book.as_ref(),
                    down_depth.as_ref(),
                    down_exit_fair,
                ),
            ];
            let mut emitted_exit = false;
            for (token, _, top, depth, fair) in tokens {
                let (Some(top), Some(fair)) = (top, fair) else {
                    continue;
                };
                let mut route_authorizations = Vec::new();
                if ctf_entry {
                    route_authorizations.extend(
                        ctf_authorized_sells
                            .iter()
                            .filter(|authorization| authorization.token == token)
                            .map(|authorization| (authorization.route, Some(authorization))),
                    );
                } else {
                    if let Some(route) = self.paired_sell_route_for_token_market(
                        &portfolio_snapshot,
                        token,
                        cohort.market_id.as_str(),
                        now_ms,
                        market_projection,
                    ) {
                        route_authorizations.push((route, None));
                    }
                }
                for (route, authorization) in route_authorizations {
                    let Some(authorization) = authorization else {
                        continue;
                    };
                    let Some((qty, close_hint)) = (match route.close_reason {
                        OrderCloseReason::PairedFirstManagedSell => {
                            Some((route.qty, Self::PAIRED_FIRST_MANAGED_SELL_CLOSE_HINT))
                        }
                        OrderCloseReason::PairedPostProceedsSell => {
                            Some((route.qty, Self::PAIRED_POST_PROCEEDS_SELL_CLOSE_HINT))
                        }
                        OrderCloseReason::PredictivePostProceedsSell => {
                            unreachable!("live predictive post-proceeds sell production is deleted")
                        }
                    }) else {
                        continue;
                    };
                    if qty <= 0.0 {
                        continue;
                    }
                    let fair =
                        if close_hint == Self::PAIRED_FIRST_MANAGED_SELL_CLOSE_HINT && !ctf_entry {
                            0.0
                        } else {
                            fair
                        };
                    let market_window_end_ms = market
                        .as_ref()
                        .map(|market| market.window_end_ms)
                        .or_else(|| {
                            market_projection
                                .market_by_id(cohort.market_id.as_str())
                                .map(|market| market.window_end_ms)
                        });
                    let queued_sell = self.queue_paired_managed_sell(PairedManagedSellRequest {
                        fact_buffer,
                        market: market.as_ref(),
                        token,
                        top,
                        depth,
                        fair,
                        market_window_end_ms,
                        permitted_inventory_units_6dp: route.qty_units_6dp,
                        permitted_inventory_qty: qty,
                        market_id: cohort.market_id.as_str(),
                        lifecycle_cohort: cohort.quote_group.as_str(),
                        lifecycle_symbol: market.as_ref().map(|market| market.underlying.as_str()),
                        close_hint,
                        require_positive_edge: false,
                        authorization: &authorization,
                        now_ms,
                    });
                    if queued_sell {
                        emitted_exit = true;
                    } else {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            authorization
                                .owner_outcome
                                .with_execution_action_qty_units_6dp(0),
                        ));
                    }
                }
            }
            if emitted_exit {
                exit_market_ids.insert(cohort.market_id.to_string());
            }
        }

        self.cfg = base_cfg;
        exit_market_ids
    }
}

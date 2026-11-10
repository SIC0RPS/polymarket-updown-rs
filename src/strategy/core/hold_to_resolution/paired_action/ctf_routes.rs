use std::collections::BTreeMap;

use crate::portfolio::PortfolioState;
use crate::strategy::facts::{
    CtfLegEconomicOutcome, CtfOwnedLegCloseReason, CtfOwnedLegOutcomeFact,
};
use crate::strategy::pair_lifecycle::CtfOwnedLegDisposition;
use crate::strategy::{paired_maker::MakerRebateMmStrategy, StrategyFact, StrategyFactSink};

use super::super::{CtfLegSellAuthorization, CtfMarketEntryDisposition, PairedSellRoute};

impl MakerRebateMmStrategy {
    pub(in crate::strategy::hold_to_resolution) fn paired_ctf_sell_routes_for_market(
        &mut self,
        fact_buffer: &mut StrategyFactSink,
        portfolio: &PortfolioState,
        cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
        market_id: &str,
        now_ms: u64,
        disposition: CtfMarketEntryDisposition,
    ) -> Vec<CtfLegSellAuthorization> {
        let mut sell_candidates = Vec::new();
        for (leg, decision) in disposition.leg_decisions {
            let owned_leg_disposition = if cohort.direct_buy_authorization().is_some() {
                decision.disposition
            } else {
                self.pair_lifecycle_by_group
                    .get(cohort.quote_group.as_str())
                    .and_then(|state| state.ctf_owned_leg_disposition(leg.token_side))
                    .expect("CTF reducer omitted a strategy-owned leg disposition")
            };
            match owned_leg_disposition {
                CtfOwnedLegDisposition::SellNow {
                    authorized_qty_units_6dp,
                    close_reason,
                    reason,
                    post_sell_economics,
                } => {
                    if close_reason == crate::types::OrderCloseReason::PairedFirstManagedSell
                        && !leg.fresh_first_instruction_authorized
                    {
                        continue;
                    }
                    let exact_action_qty_units_6dp = match close_reason {
                        crate::types::OrderCloseReason::PairedFirstManagedSell => {
                            confirmed_first_disposed_qty_units_6dp(
                                portfolio,
                                cohort.quote_group.as_str(),
                                market_id,
                                leg.token.as_str(),
                            )
                            .and_then(|disposed| authorized_qty_units_6dp.checked_sub(disposed))
                        }
                        crate::types::OrderCloseReason::PairedPostProceedsSell => {
                            Some(authorized_qty_units_6dp)
                        }
                        crate::types::OrderCloseReason::PredictivePostProceedsSell => {
                            unreachable!("live predictive post-proceeds sell production is deleted")
                        }
                    };
                    let Some(exact_action_qty_units_6dp) = exact_action_qty_units_6dp else {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            CtfOwnedLegOutcomeFact::new(
                                now_ms,
                                market_id.to_string(),
                                cohort.quote_group.clone(),
                                leg.token,
                                authorized_qty_units_6dp,
                                0,
                                0,
                                leg.causal_winner_side,
                                None,
                                post_sell_economics,
                                CtfLegEconomicOutcome::SellNow,
                                reason,
                                Some(
                                    CtfOwnedLegCloseReason::try_from(close_reason)
                                        .expect("CTF disposition used a non-paired close reason"),
                                ),
                            ),
                        ));
                        continue;
                    };
                    if exact_action_qty_units_6dp == 0 {
                        continue;
                    }
                    let close_reason_fact = CtfOwnedLegCloseReason::try_from(close_reason)
                        .expect("CTF disposition used a non-paired close reason");
                    let confirmed_qty_matches = match close_reason {
                        crate::types::OrderCloseReason::PairedFirstManagedSell => leg
                            .projection
                            .and_then(|projection| projection.confirmed_first_cohort_qty_units_6dp)
                            .is_some_and(|owned| owned == exact_action_qty_units_6dp),
                        crate::types::OrderCloseReason::PairedPostProceedsSell => portfolio
                            .confirmed_ctf_cohort_qty_units_6dp(
                                leg.token.as_str(),
                                cohort.quote_group.as_str(),
                            )
                            .is_ok_and(|owned| owned == exact_action_qty_units_6dp),
                        crate::types::OrderCloseReason::PredictivePostProceedsSell => {
                            unreachable!("live predictive post-proceeds sell production is deleted")
                        }
                    };
                    if !confirmed_qty_matches {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            CtfOwnedLegOutcomeFact::new(
                                now_ms,
                                market_id.to_string(),
                                cohort.quote_group.clone(),
                                leg.token,
                                authorized_qty_units_6dp,
                                0,
                                0,
                                leg.causal_winner_side,
                                None,
                                post_sell_economics,
                                CtfLegEconomicOutcome::SellNow,
                                reason,
                                Some(close_reason_fact),
                            ),
                        ));
                        continue;
                    }
                    let Some(projection) = leg.projection else {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            CtfOwnedLegOutcomeFact::new(
                                now_ms,
                                market_id.to_string(),
                                cohort.quote_group.clone(),
                                leg.token,
                                authorized_qty_units_6dp,
                                0,
                                0,
                                leg.causal_winner_side,
                                None,
                                post_sell_economics,
                                CtfLegEconomicOutcome::SellNow,
                                reason,
                                Some(close_reason_fact),
                            ),
                        ));
                        continue;
                    };
                    let qty_units_6dp = projection.instruction_qty_units_6dp;
                    let qty = crate::strategy::full_set_units_to_qty(qty_units_6dp);
                    let instruction_matches_route = match close_reason {
                        crate::types::OrderCloseReason::PairedFirstManagedSell => {
                            qty_units_6dp <= exact_action_qty_units_6dp
                        }
                        crate::types::OrderCloseReason::PairedPostProceedsSell => {
                            qty_units_6dp == exact_action_qty_units_6dp
                        }
                        crate::types::OrderCloseReason::PredictivePostProceedsSell => {
                            unreachable!("live predictive post-proceeds sell production is deleted")
                        }
                    };
                    let limit_px = projection.limit_px;
                    let execution_evidence_revision = projection.execution_evidence_revision;
                    if !(instruction_matches_route
                        && qty.is_finite()
                        && qty > super::super::INVENTORY_EPS
                        && qty_units_6dp > 0
                        && limit_px.is_finite()
                        && limit_px > 0.0
                        && execution_evidence_revision > 0)
                    {
                        fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                            CtfOwnedLegOutcomeFact::new(
                                now_ms,
                                market_id.to_string(),
                                cohort.quote_group.clone(),
                                leg.token,
                                authorized_qty_units_6dp,
                                0,
                                0,
                                leg.causal_winner_side,
                                None,
                                post_sell_economics,
                                CtfLegEconomicOutcome::SellNow,
                                reason,
                                Some(close_reason_fact),
                            ),
                        ));
                        continue;
                    }
                    let route = PairedSellRoute {
                        qty_units_6dp,
                        qty,
                        close_reason,
                    };
                    let first_instruction_economics = match close_reason {
                        crate::types::OrderCloseReason::PairedFirstManagedSell => {
                            projection.first_instruction_economics
                        }
                        crate::types::OrderCloseReason::PairedPostProceedsSell => None,
                        crate::types::OrderCloseReason::PredictivePostProceedsSell => {
                            unreachable!("live predictive post-proceeds sell production is deleted")
                        }
                    };
                    let owner_outcome = CtfOwnedLegOutcomeFact::new(
                        now_ms,
                        market_id.to_string(),
                        cohort.quote_group.clone(),
                        leg.token.clone(),
                        authorized_qty_units_6dp,
                        route.qty_units_6dp,
                        execution_evidence_revision,
                        leg.causal_winner_side,
                        first_instruction_economics,
                        post_sell_economics,
                        CtfLegEconomicOutcome::SellNow,
                        reason,
                        Some(close_reason_fact),
                    );
                    sell_candidates.push((
                        leg.token,
                        route,
                        limit_px,
                        execution_evidence_revision,
                        owner_outcome,
                    ));
                }
                CtfOwnedLegDisposition::HoldForSettlement { reason } => {
                    if leg.owned_qty_units_6dp == 0 {
                        continue;
                    }
                    fact_buffer.push(StrategyFact::CtfOwnedLegOutcome(
                        CtfOwnedLegOutcomeFact::new(
                            now_ms,
                            market_id.to_string(),
                            cohort.quote_group.clone(),
                            leg.token,
                            leg.owned_qty_units_6dp,
                            0,
                            0,
                            leg.causal_winner_side,
                            None,
                            None,
                            CtfLegEconomicOutcome::HoldForSettlement,
                            reason,
                            None,
                        ),
                    ));
                }
            }
        }

        let mut selected = BTreeMap::new();
        for (token, route, limit_px, execution_evidence_revision, owner_outcome) in sell_candidates
        {
            selected.insert(
                token.clone(),
                CtfLegSellAuthorization {
                    token,
                    limit_px,
                    execution_evidence_revision,
                    route,
                    owner_outcome,
                },
            );
        }
        selected.into_values().collect()
    }
}

fn confirmed_first_disposed_qty_units_6dp(
    portfolio: &PortfolioState,
    lifecycle_cohort: &str,
    market_id: &str,
    token: &str,
) -> Option<u128> {
    portfolio
        .confirmed_lifecycle_sell_fills
        .values()
        .filter(|fill| {
            fill.lifecycle_cohort == lifecycle_cohort
                && fill.market_id.as_str() == market_id
                && fill.token == token
                && fill.close_reason == crate::types::OrderCloseReason::PairedFirstManagedSell
        })
        .try_fold(0_u128, |total, fill| total.checked_add(fill.qty_units_6dp))
}

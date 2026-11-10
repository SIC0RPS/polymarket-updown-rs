use crate::polymarket::state::MarketProjection;
use crate::portfolio::PortfolioState;
use crate::strategy::pair_lifecycle::AuthoritativePairedCohortView;
use crate::strategy::paired_maker::MakerRebateMmStrategy;

use super::super::{
    CtfFirstExecutionSummary, CtfLegDecisionInput, CtfMarketEntryLegSnapshot, PairTokenSide,
    INVENTORY_EPS,
};

impl MakerRebateMmStrategy {
    pub(in crate::strategy::hold_to_resolution) fn ctf_market_entry_candidates(
        &mut self,
        portfolio: &PortfolioState,
        cohort: &AuthoritativePairedCohortView,
        market_id: &str,
        market_projection: &MarketProjection,
        decision_inputs: &[CtfLegDecisionInput],
        first_execution: Option<CtfFirstExecutionSummary>,
        first_side: Option<PairTokenSide>,
        market_window_end_ms: u64,
        now_ms: u64,
    ) -> Vec<(String, CtfMarketEntryLegSnapshot)> {
        let mut candidates = Vec::new();
        let taker_base_fee_bps = market_projection
            .market_by_id(market_id)
            .and_then(|market| market.taker_base_fee_bps);

        for input in decision_inputs {
            let token = input.token.as_str();
            if input.owned_qty_units_6dp == 0 {
                candidates.push((
                    token.to_string(),
                    CtfMarketEntryLegSnapshot {
                        token_side: input.token_side,
                        owned_qty_units_6dp: input.owned_qty_units_6dp,
                        economic_owned_qty: economic_owned_qty(input, first_execution),
                        route_block_reason: Some(
                            crate::strategy::facts::CtfLegDecisionReason::NoLifecycleOwnedQty,
                        ),
                        projection: None,
                    },
                ));
                continue;
            }
            let Some(executable) = input.executable.as_ref() else {
                candidates.push((
                    token.to_string(),
                    CtfMarketEntryLegSnapshot {
                        token_side: input.token_side,
                        owned_qty_units_6dp: input.owned_qty_units_6dp,
                        economic_owned_qty: economic_owned_qty(input, first_execution),
                        route_block_reason: Some(
                            input
                                .route_block_reason
                                .unwrap_or(
                                    crate::strategy::facts::CtfLegDecisionReason::CtfEntryNoExecutableSellSurface,
                                ),
                        ),
                        projection: None,
                    },
                ));
                continue;
            };
            if !(executable.qty.is_finite()
                && executable.qty > INVENTORY_EPS
                && executable.qty_units_6dp > 0)
            {
                candidates.push((
                    token.to_string(),
                    CtfMarketEntryLegSnapshot {
                        token_side: executable.token_side,
                        owned_qty_units_6dp: input.owned_qty_units_6dp,
                        economic_owned_qty: economic_owned_qty(input, first_execution),
                        route_block_reason: Some(
                            crate::strategy::facts::CtfLegDecisionReason::NoFillableQty,
                        ),
                        projection: None,
                    },
                ));
                continue;
            }
            let is_first_instruction = first_side == Some(input.token_side);
            let pending_qty_units_6dp = first_execution
                .filter(|summary| summary.token_side == input.token_side)
                .map_or(0, |summary| summary.pending_qty_units_6dp);
            let first_remaining_qty_units_6dp = if is_first_instruction {
                input
                    .owned_qty_units_6dp
                    .checked_sub(pending_qty_units_6dp)
                    .unwrap_or(0)
            } else {
                executable.qty_units_6dp
            };
            let projected_qty_units_6dp = if is_first_instruction {
                first_remaining_qty_units_6dp.min(executable.qty_units_6dp)
            } else {
                executable.qty_units_6dp
            };
            if projected_qty_units_6dp == 0 {
                candidates.push((
                    token.to_string(),
                    CtfMarketEntryLegSnapshot {
                        token_side: input.token_side,
                        owned_qty_units_6dp: input.owned_qty_units_6dp,
                        economic_owned_qty: economic_owned_qty(input, first_execution),
                        route_block_reason: Some(
                            crate::strategy::facts::CtfLegDecisionReason::NoFillableQty,
                        ),
                        projection: None,
                    },
                ));
                continue;
            }
            match self.ctf_entry_ledger_projection(
                portfolio,
                cohort,
                token,
                executable.clone(),
                projected_qty_units_6dp,
                first_remaining_qty_units_6dp,
                taker_base_fee_bps,
                first_execution,
                market_window_end_ms,
                now_ms,
                is_first_instruction,
            ) {
                Ok(projection) => {
                    let route_block_reason =
                        (is_first_instruction && !projection.first_sell_allowed).then_some(
                            crate::strategy::facts::CtfLegDecisionReason::LossCutFloorNotMet,
                        );
                    candidates.push((
                        token.to_string(),
                        CtfMarketEntryLegSnapshot {
                            token_side: executable.token_side,
                            owned_qty_units_6dp: input.owned_qty_units_6dp,
                            economic_owned_qty: economic_owned_qty(input, first_execution),
                            route_block_reason,
                            projection: Some(projection),
                        },
                    ));
                }
                Err(reason) => {
                    candidates.push((
                        token.to_string(),
                        CtfMarketEntryLegSnapshot {
                            token_side: input.token_side,
                            owned_qty_units_6dp: input.owned_qty_units_6dp,
                            economic_owned_qty: economic_owned_qty(input, first_execution),
                            route_block_reason: Some(reason),
                            projection: None,
                        },
                    ));
                }
            }
        }

        candidates
    }
}

fn economic_owned_qty(
    input: &CtfLegDecisionInput,
    first_execution: Option<CtfFirstExecutionSummary>,
) -> f64 {
    let pending_units = first_execution
        .filter(|summary| summary.token_side == input.token_side)
        .map_or(0, |summary| summary.pending_qty_units_6dp);
    if !input.owned_qty.is_finite() || input.owned_qty_units_6dp < pending_units {
        return 0.0;
    }
    let pending_qty = crate::strategy::full_set_units_to_qty(pending_units);
    (input.owned_qty.max(0.0) - pending_qty).max(0.0)
}

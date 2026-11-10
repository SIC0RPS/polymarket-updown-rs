use crate::strategy::facts::CtfLegDecisionReason;
use crate::strategy::full_set_units_to_qty;
use crate::strategy::hold_to_resolution::{CtfLegDecisionInput, PairTokenSide};
use crate::strategy::pair_lifecycle::AuthoritativePairedCohortView;
use crate::strategy::paired_maker::MakerRebateMmStrategy;

impl MakerRebateMmStrategy {
    pub(super) fn ctf_decision_inputs_for_blocked_market(
        &self,
        portfolio: &crate::portfolio::PortfolioState,
        cohort: &AuthoritativePairedCohortView,
        route_block_reason: CtfLegDecisionReason,
    ) -> Vec<CtfLegDecisionInput> {
        [
            (cohort.up_token.as_str(), PairTokenSide::Up),
            (cohort.down_token.as_str(), PairTokenSide::Down),
        ]
        .into_iter()
        .map(|(token, token_side)| {
            let owned_qty_units_6dp = portfolio
                .position(token)
                .map(|position| position.qty_units_6dp)
                .unwrap_or(0);
            let owned_qty = full_set_units_to_qty(owned_qty_units_6dp);
            CtfLegDecisionInput {
                token: token.to_string(),
                token_side,
                owned_qty_units_6dp,
                owned_qty,
                executable: None,
                route_block_reason: Some(route_block_reason),
            }
        })
        .collect()
    }
}

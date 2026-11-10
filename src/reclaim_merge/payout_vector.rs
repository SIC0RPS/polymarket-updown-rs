use super::close_accounting::candidate_positions_map_to_payout;
use super::*;
use crate::execution::ConditionPayoutVector;
use crate::portfolio::PortfolioState;
use anyhow::Result;
use polymarket_client_sdk::data::types::response::Position as VendorPosition;

pub(super) async fn validated_payout_vector_for_condition(
    exec: &ExecutionClient,
    condition_id: B256,
    mergeable_positions: &[VendorPosition],
    redeemable_positions: &[VendorPosition],
    portfolio: &PortfolioState,
) -> Result<Option<ConditionPayoutVector>> {
    let Some(payout) = exec.condition_payout_vector(condition_id).await? else {
        return Ok(None);
    };
    if payout.condition_id != condition_id
        || payout.denominator == 0
        || payout
            .outcome_slot_0_numerator
            .checked_add(payout.outcome_slot_1_numerator)
            != Some(payout.denominator)
        || !((payout.outcome_slot_0_numerator == payout.denominator
            && payout.outcome_slot_1_numerator == 0)
            || (payout.outcome_slot_1_numerator == payout.denominator
                && payout.outcome_slot_0_numerator == 0))
        || !candidate_positions_map_to_payout(
            condition_id,
            mergeable_positions,
            redeemable_positions,
            portfolio,
            &payout,
        )
    {
        tracing::warn!(
            target: "app.reclaim_merge",
            condition_id = %condition_id,
            "reclaim_payout_vector_rejected"
        );
        return Ok(None);
    }
    Ok(Some(payout))
}

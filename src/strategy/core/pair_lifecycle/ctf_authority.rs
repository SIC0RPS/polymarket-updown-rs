use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::discovery::TokenLifecycleState;
use crate::polymarket::identity::MarketId;
use crate::polymarket::state::MarketProjection;
use crate::portfolio::{PortfolioState, PositionState};
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy::paired_maker::{ConfirmedCtfEntryFact, MakerRebateMmStrategy};

use super::{is_confirmed_ctf_split_cohort_id, PairLifecycleReducer, PairLifecycleState};

fn position_qty_units_6dp(position: Option<&PositionState>) -> u128 {
    position.map(|position| position.qty_units_6dp).unwrap_or(0)
}

fn terminal_position(position: Option<&PositionState>) -> bool {
    position.is_some_and(|position| position.resolved_redeemable || position.mergeable)
}

fn market_has_terminal_visibility(
    market: &MarketSelection,
    up_position: Option<&PositionState>,
    down_position: Option<&PositionState>,
) -> bool {
    terminal_position(up_position)
        || terminal_position(down_position)
        || market.up_lifecycle == TokenLifecycleState::Resolved
        || market.down_lifecycle == TokenLifecycleState::Resolved
}

fn ctf_remaining_owned_side(up_units_6dp: u128, down_units_6dp: u128) -> Option<PairTokenSide> {
    let matched_units_6dp = up_units_6dp.min(down_units_6dp);
    if up_units_6dp > matched_units_6dp {
        Some(PairTokenSide::Up)
    } else if down_units_6dp > matched_units_6dp {
        Some(PairTokenSide::Down)
    } else {
        None
    }
}

fn ctf_entry_units_6dp(up_units_6dp: u128, down_units_6dp: u128) -> u128 {
    up_units_6dp.max(down_units_6dp)
}

fn lifecycle_matches_market(state: &PairLifecycleState, market: &MarketSelection) -> bool {
    state.market_id == market.market_id
        && state.up_token == market.up_token_id()
        && state.down_token == market.down_token_id()
}

fn confirmed_ctf_entry_fact(
    portfolio: &PortfolioState,
    market: &MarketSelection,
) -> Option<ConfirmedCtfEntryFact> {
    let (cohort, up, down) = portfolio.confirmed_ctf_split_pair(
        &market.market_id,
        market.up_token_id(),
        market.down_token_id(),
    )?;
    if up.qty_units_6dp == 0
        || up.qty_units_6dp != down.qty_units_6dp
        || up.entry_tx_hash != down.entry_tx_hash
        || up.entry_outlay_units_6dp != down.entry_outlay_units_6dp
        || up.materialized_ts_ms != down.materialized_ts_ms
        || up.entry_outlay_units_6dp < up.qty_units_6dp
    {
        return None;
    }
    Some(ConfirmedCtfEntryFact::new(
        cohort,
        up.qty_units_6dp,
        up.entry_outlay_units_6dp,
        up.materialized_ts_ms,
    ))
}

impl MakerRebateMmStrategy {
    #[allow(clippy::too_many_arguments)]
    fn materialize_ctf_latched_entry_state(
        &mut self,
        market_id: MarketId,
        up_token: String,
        down_token: String,
        entry_latch: ConfirmedCtfEntryFact,
    ) {
        let quote_group = entry_latch.cohort_id().to_string();
        self.materialize_ctf_entry_state_from_authority(
            market_id,
            quote_group,
            up_token,
            down_token,
            entry_latch.entry_outlay_units_6dp(),
            entry_latch.amount_units_6dp(),
            entry_latch.materialized_ts_ms(),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn materialize_ctf_entry_state_from_authority(
        &mut self,
        market_id: MarketId,
        quote_group: String,
        up_token: String,
        down_token: String,
        entry_outlay_units_6dp: u128,
        split_amount_units_6dp: u128,
        materialized_ts_ms: u64,
    ) {
        let state = PairLifecycleReducer::new_ctf_latched_entry_state(
            market_id.clone(),
            quote_group.clone(),
            up_token,
            down_token,
            entry_outlay_units_6dp,
            split_amount_units_6dp,
            materialized_ts_ms,
        );
        self.pair_lifecycle_by_group.insert(quote_group, state);
    }

    pub(super) fn materialize_ctf_inventory_lifecycle_authority(
        &mut self,
        portfolio: &PortfolioState,
        markets: &MarketProjection,
        _now_ms: u64,
    ) {
        for market in markets.markets() {
            if !market.standard_binary_ctf() || market.condition_id_b256().is_none() {
                continue;
            }

            let up_position = portfolio.position(market.up_token_id());
            let down_position = portfolio.position(market.down_token_id());
            let up_units_6dp = position_qty_units_6dp(up_position);
            let down_units_6dp = position_qty_units_6dp(down_position);
            let remaining_owned_side = ctf_remaining_owned_side(up_units_6dp, down_units_6dp);
            let existing_group =
                self.pair_lifecycle_by_group
                    .iter()
                    .find_map(|(quote_group, state)| {
                        lifecycle_matches_market(state, market).then(|| quote_group.clone())
                    });
            if existing_group.as_ref().is_some_and(|quote_group| {
                self.pair_lifecycle_by_group
                    .get(quote_group)
                    .is_some_and(|state| is_confirmed_ctf_split_cohort_id(&state.quote_group))
            }) {
                continue;
            }
            if let Some(entry_fact) = confirmed_ctf_entry_fact(portfolio, market) {
                if let Some(existing_group) = existing_group {
                    self.pair_lifecycle_by_group.remove(&existing_group);
                }
                self.materialize_ctf_latched_entry_state(
                    market.market_id.clone(),
                    market.up_token_id().to_string(),
                    market.down_token_id().to_string(),
                    entry_fact,
                );
                continue;
            }
            let terminal_visibility =
                market_has_terminal_visibility(market, up_position, down_position);
            if remaining_owned_side.is_some() {
                let remaining_owned_token = match remaining_owned_side {
                    Some(PairTokenSide::Up) => market.up_token_id(),
                    Some(PairTokenSide::Down) => market.down_token_id(),
                    None => "",
                };
                let remaining_owned_attributed = portfolio
                    .token_market_hint
                    .get(remaining_owned_token)
                    .is_some_and(|market_id| market_id == &market.market_id);
                if !remaining_owned_attributed {
                    continue;
                }
                if !terminal_visibility {
                    continue;
                }
            }
            let observed_entry_units_6dp = ctf_entry_units_6dp(up_units_6dp, down_units_6dp);
            if observed_entry_units_6dp == 0 {
                continue;
            }
            let existing_group =
                self.pair_lifecycle_by_group
                    .iter()
                    .find_map(|(quote_group, state)| {
                        lifecycle_matches_market(state, market).then(|| quote_group.clone())
                    });
            if let Some(existing_group) = existing_group {
                let existing_is_ctf = self
                    .pair_lifecycle_by_group
                    .get(&existing_group)
                    .is_some_and(|state| is_confirmed_ctf_split_cohort_id(&state.quote_group));
                if existing_is_ctf {
                    continue;
                }
                let entry_latch = confirmed_ctf_entry_fact(portfolio, market);
                let Some(entry_latch) = entry_latch else {
                    continue;
                };
                self.pair_lifecycle_by_group.remove(&existing_group);
                self.materialize_ctf_latched_entry_state(
                    market.market_id.clone(),
                    market.up_token_id().to_string(),
                    market.down_token_id().to_string(),
                    entry_latch,
                );
                continue;
            }

            let entry_latch = confirmed_ctf_entry_fact(portfolio, market);
            let Some(entry_latch) = entry_latch else {
                continue;
            };
            self.materialize_ctf_latched_entry_state(
                market.market_id.clone(),
                market.up_token_id().to_string(),
                market.down_token_id().to_string(),
                entry_latch,
            );
        }
    }
}

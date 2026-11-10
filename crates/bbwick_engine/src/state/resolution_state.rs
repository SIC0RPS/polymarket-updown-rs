use anyhow::Result;
use polymarket_client_sdk::types::{B256, U256};
use serde::{Deserialize, Serialize};

use super::{PolymarketOddsState, PolymarketStateStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedMarketResolutionContext {
    pub up_token: U256,
    pub down_token: U256,
    pub observed_at_ms: u64,
    pub raw_response_sha256: [u8; 32],
}

impl PolymarketStateStore {
    pub fn preflight_market_resolution(
        &self,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
    ) -> Result<bool> {
        self.state
            .preflight_market_resolution(condition_id, up_token, down_token, winning_asset_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn commit_market_resolution(
        &mut self,
        state: &mut PolymarketOddsState,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
        observed_at_ms: u64,
        raw_response_sha256: [u8; 32],
    ) -> Result<bool> {
        if !self.state.retain_market_resolution(
            condition_id,
            up_token,
            down_token,
            winning_asset_id,
            observed_at_ms,
            raw_response_sha256,
        )? {
            return Ok(false);
        }
        state
            .resolved_market_winners
            .clone_from(&self.state.resolved_market_winners);
        state
            .resolved_market_contexts
            .clone_from(&self.state.resolved_market_contexts);
        state.updated_at_ms = self.state.updated_at_ms;
        self.refresh_trigger_metadata(state);
        Ok(true)
    }
}

impl PolymarketOddsState {
    #[allow(clippy::too_many_arguments)]
    pub fn preflight_market_resolution(
        &self,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
    ) -> Result<bool> {
        anyhow::ensure!(
            up_token != down_token
                && (winning_asset_id == up_token || winning_asset_id == down_token),
            "market resolution is not one exact tracked binary pair containing its winner"
        );
        match (
            self.resolved_market_winners.get(&condition_id),
            self.resolved_market_contexts.get(&condition_id),
        ) {
            (None, None) => Ok(true),
            (Some(existing_winner), Some(existing_context)) => {
                anyhow::ensure!(
                    *existing_winner == winning_asset_id
                        && existing_context.up_token == up_token
                        && existing_context.down_token == down_token,
                    "market resolution conflicts with retained terminal identity condition={condition_id}"
                );
                Ok(false)
            }
            _ => anyhow::bail!(
                "market resolution has partial retained identity condition={condition_id}"
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn retain_market_resolution(
        &mut self,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
        observed_at_ms: u64,
        raw_response_sha256: [u8; 32],
    ) -> Result<bool> {
        anyhow::ensure!(
            observed_at_ms > 0,
            "market resolution observation timestamp is zero"
        );
        if !self.preflight_market_resolution(
            condition_id,
            up_token,
            down_token,
            winning_asset_id,
        )? {
            return Ok(false);
        }
        self.resolved_market_winners
            .insert(condition_id, winning_asset_id);
        self.resolved_market_contexts.insert(
            condition_id,
            CapturedMarketResolutionContext {
                up_token,
                down_token,
                observed_at_ms,
                raw_response_sha256,
            },
        );
        self.updated_at_ms = self.updated_at_ms.max(observed_at_ms);
        Ok(true)
    }
}

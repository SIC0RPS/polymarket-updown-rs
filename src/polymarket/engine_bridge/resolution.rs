use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use polymarket_client_sdk::clob::types::response::MarketResponse;
use polymarket_client_sdk::types::{B256, U256};
use sha2::{Digest, Sha256};

use super::now_ms;
use crate::capture::polymarket_resolution::PolymarketResolutionCapture;
use crate::capture::session::MaybeCapture;
use crate::polymarket::discovery::MarketSelection;
use bbwick_engine::PolymarketDiscoveryController;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TrackedResolutionMarket {
    underlying: String,
    pub(super) condition_id: B256,
    up_token: U256,
    down_token: U256,
    pub(super) window_end_ms: u64,
}

pub(super) fn track_discovered_markets(
    pending: &mut BTreeMap<B256, TrackedResolutionMarket>,
    markets: &[MarketSelection],
) -> Result<()> {
    for market in markets {
        let condition_id = match market.condition_id_b256() {
            Some(condition_id) => condition_id,
            None => {
                tracing::error!(
                    target: "polymarket.engine_bridge",
                    market_id = %market.market_id,
                    "polymarket_resolution_invalid_discovery_identity_pending"
                );
                continue;
            }
        };
        if market.up_token == market.down_token {
            tracing::error!(
                target: "polymarket.engine_bridge",
                condition_id = %condition_id,
                "polymarket_resolution_duplicate_discovery_tokens_pending"
            );
            continue;
        }
        let tracked = TrackedResolutionMarket {
            underlying: market.underlying.clone(),
            condition_id,
            up_token: market.up_token,
            down_token: market.down_token,
            window_end_ms: market.window_end_ms,
        };
        if let Some(existing) = pending.get(&condition_id) {
            if existing != &tracked {
                tracing::error!(
                    target: "polymarket.engine_bridge",
                    condition_id = %condition_id,
                    "polymarket_resolution_discovery_identity_changed_pending"
                );
            }
        } else {
            pending.insert(condition_id, tracked);
        }
    }
    Ok(())
}

pub(super) async fn fetch_market_resolution(
    tracked: TrackedResolutionMarket,
    clob_http_url: &str,
    timeout_ms: u64,
    http: &reqwest::Client,
) -> (
    TrackedResolutionMarket,
    std::result::Result<(Vec<u8>, u64), reqwest::Error>,
) {
    let base_url = clob_http_url.trim_end_matches('/');
    let url = format!("{base_url}/markets/{}", tracked.condition_id);
    let timeout = Duration::from_millis(timeout_ms.max(1_000));
    let response = async {
        let response = http
            .get(&url)
            .timeout(timeout)
            .send()
            .await?
            .error_for_status()?;
        let raw_response = response.bytes().await?.to_vec();
        let observed_at_ms = now_ms();
        Ok((raw_response, observed_at_ms))
    }
    .await;
    (tracked, response)
}

pub(super) fn accept_market_resolution(
    pending: &mut BTreeMap<B256, TrackedResolutionMarket>,
    tracked: TrackedResolutionMarket,
    response: std::result::Result<(Vec<u8>, u64), reqwest::Error>,
    controller: &PolymarketDiscoveryController,
    capture: &MaybeCapture,
) -> Result<()> {
    if pending.get(&tracked.condition_id) != Some(&tracked) {
        return Ok(());
    }
    let (raw_response, observed_at_ms) = match response {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(
                target: "polymarket.engine_bridge",
                condition_id = %tracked.condition_id,
                error = %err,
                "polymarket_resolution_transport_pending"
            );
            return Ok(());
        }
    };
    let market = match serde_json::from_slice::<MarketResponse>(&raw_response) {
        Ok(market) => market,
        Err(err) => {
            tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, error = %err, "polymarket_resolution_decode_pending");
            return Ok(());
        }
    };
    let Some(response_condition_id) = market.condition_id else {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, "polymarket_resolution_missing_condition_pending");
        return Ok(());
    };
    if response_condition_id != tracked.condition_id {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, response_condition_id = %response_condition_id, "polymarket_resolution_condition_mismatch_pending");
        return Ok(());
    }
    if !(market.tokens.len() == 2
        && market.tokens[0].token_id != market.tokens[1].token_id
        && market
            .tokens
            .iter()
            .any(|token| token.token_id == tracked.up_token)
        && market
            .tokens
            .iter()
            .any(|token| token.token_id == tracked.down_token))
    {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, "polymarket_resolution_token_pair_pending");
        return Ok(());
    }
    if !market.closed {
        return Ok(());
    }
    let winners = market
        .tokens
        .iter()
        .filter(|token| token.winner)
        .collect::<Vec<_>>();
    if winners.is_empty() {
        return Ok(());
    }
    if winners.len() != 1 {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, winners = winners.len(), "polymarket_resolution_winner_count_pending");
        return Ok(());
    }
    let winning_asset_id = winners[0].token_id;
    if winning_asset_id != tracked.up_token && winning_asset_id != tracked.down_token {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, winning_asset_id = %winning_asset_id, "polymarket_resolution_winner_outside_pair_pending");
        return Ok(());
    }

    let is_new = match controller.preflight_market_resolution(
        tracked.condition_id,
        tracked.up_token,
        tracked.down_token,
        winning_asset_id,
    ) {
        Ok(is_new) => is_new,
        Err(err) => {
            tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, error = %err, "polymarket_resolution_preflight_pending");
            return Ok(());
        }
    };
    if !is_new {
        pending.remove(&tracked.condition_id);
        return Ok(());
    }

    let raw_response_sha256: [u8; 32] = Sha256::digest(&raw_response).into();
    let committed = match controller.commit_market_resolution(
        tracked.condition_id,
        tracked.up_token,
        tracked.down_token,
        winning_asset_id,
        observed_at_ms,
        raw_response_sha256,
    ) {
        Ok(committed) => committed,
        Err(err) => {
            tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, error = %err, "polymarket_resolution_commit_pending");
            return Ok(());
        }
    };
    if !committed {
        tracing::error!(target: "polymarket.engine_bridge", condition_id = %tracked.condition_id, "polymarket_resolution_commit_changed_pending");
        return Ok(());
    }
    capture.capture_committed_resolution(PolymarketResolutionCapture {
        observed_at_ms,
        underlying: tracked.underlying.clone(),
        condition_id: tracked.condition_id,
        up_token: tracked.up_token,
        down_token: tracked.down_token,
        winning_asset_id,
        raw_response,
    });
    pending.remove(&tracked.condition_id);
    tracing::info!(
        target: "polymarket.engine_bridge",
        condition_id = %tracked.condition_id,
        winning_asset_id = %winning_asset_id,
        observed_at_ms,
        "polymarket_resolution_accepted"
    );
    Ok(())
}

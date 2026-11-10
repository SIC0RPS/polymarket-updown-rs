use anyhow::Result;
use futures_util::SinkExt as _;
use polymarket_client_sdk::clob::ws::SubscriptionRequest;
use polymarket_client_sdk::types::U256;
use tokio_tungstenite::tungstenite::Message;

use super::{MarketWsStream, MarketWsWrite};
pub(super) async fn apply_market_ws_asset_update(
    write: &mut MarketWsWrite,
    asset_ids: &mut Vec<U256>,
    next_assets: Vec<U256>,
) -> Result<()> {
    let added = next_assets
        .iter()
        .copied()
        .filter(|asset_id| !asset_ids.contains(asset_id))
        .collect::<Vec<_>>();
    let removed = asset_ids
        .iter()
        .copied()
        .filter(|asset_id| !next_assets.contains(asset_id))
        .collect::<Vec<_>>();
    if added.is_empty() && removed.is_empty() {
        return Ok(());
    }
    if !removed.is_empty() {
        let request =
            serde_json::to_string(&SubscriptionRequest::market_unsubscribe(removed.clone()))?;
        write.send(Message::Text(request.into())).await?;
    }
    if !added.is_empty() {
        let request = market_subscription_payload(added.clone())?;
        write.send(Message::Text(request.into())).await?;
    }
    tracing::info!(
        target: "engine.polymarket_ws",
        added_count = added.len(),
        removed_count = removed.len(),
        asset_count = next_assets.len(),
        "bbwick_polymarket_ws_subscription_updated"
    );
    *asset_ids = next_assets;
    Ok(())
}

pub(super) async fn send_initial_market_subscription(
    ws: &mut MarketWsStream,
    asset_ids: &[U256],
) -> Result<()> {
    let request = market_subscription_payload(asset_ids.to_vec())?;
    ws.send(Message::Text(request.into())).await?;
    Ok(())
}

pub(super) fn market_subscription_payload(asset_ids: Vec<U256>) -> Result<String> {
    Ok(serde_json::to_string(&SubscriptionRequest::market(
        asset_ids,
    ))?)
}

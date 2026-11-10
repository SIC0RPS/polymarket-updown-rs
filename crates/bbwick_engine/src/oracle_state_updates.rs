use std::sync::{Arc, Mutex};

use anyhow::Result;
use polymarket_client_sdk::types::{B256, U256};
use tokio::sync::watch;

use crate::state::{DiscoverySnapshot, PolymarketOddsState, PolymarketStateStore};

pub fn sync_discovery_snapshot(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    state_tx: &watch::Sender<PolymarketOddsState>,
    snapshot: DiscoverySnapshot,
) -> Result<bool> {
    sync_state(state_store, state_tx, move |store, state, now| {
        store.sync_discovery_snapshot(state, &snapshot, now)
    })
}

pub(crate) fn sync_tombstone_no_orderbook(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    state_tx: &watch::Sender<PolymarketOddsState>,
    token_id: U256,
) -> Result<bool> {
    sync_state(state_store, state_tx, move |store, state, now| {
        store.sync_tombstone_no_orderbook(state, token_id, now)
    })
}

pub(crate) fn preflight_market_resolution(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    condition_id: B256,
    up_token: U256,
    down_token: U256,
    winning_asset_id: U256,
) -> Result<bool> {
    with_store(state_store, |store| {
        store.preflight_market_resolution(condition_id, up_token, down_token, winning_asset_id)
    })?
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_market_resolution(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    state_tx: &watch::Sender<PolymarketOddsState>,
    condition_id: B256,
    up_token: U256,
    down_token: U256,
    winning_asset_id: U256,
    observed_at_ms: u64,
    raw_response_sha256: [u8; 32],
) -> Result<bool> {
    let mut store = state_store
        .lock()
        .map_err(|_| anyhow::anyhow!("polymarket state store poisoned"))?;
    let mut next = state_tx.borrow().clone();
    let changed = store.commit_market_resolution(
        &mut next,
        condition_id,
        up_token,
        down_token,
        winning_asset_id,
        observed_at_ms,
        raw_response_sha256,
    )?;
    if changed {
        state_tx.send_replace(next);
    }
    Ok(changed)
}

fn with_store<T>(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    f: impl FnOnce(&PolymarketStateStore) -> T,
) -> Result<T> {
    let store = state_store
        .lock()
        .map_err(|_| anyhow::anyhow!("polymarket state store poisoned"))?;
    Ok(f(&store))
}

fn sync_state(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    state_tx: &watch::Sender<PolymarketOddsState>,
    apply: impl FnOnce(&mut PolymarketStateStore, &mut PolymarketOddsState, u64) -> bool,
) -> Result<bool> {
    update_polymarket_state(state_store, state_tx, apply)
        .ok_or_else(|| anyhow::anyhow!("polymarket state store poisoned"))
}

fn update_polymarket_state<F>(
    state_store: &Arc<Mutex<PolymarketStateStore>>,
    state_tx: &watch::Sender<PolymarketOddsState>,
    apply: F,
) -> Option<bool>
where
    F: FnOnce(&mut PolymarketStateStore, &mut PolymarketOddsState, u64) -> bool,
{
    let mut store = match state_store.lock() {
        Ok(store) => store,
        Err(_) => return None,
    };
    let now = now_ms();
    let mut changed = false;
    let _ = state_tx.send_if_modified(|state| {
        changed = apply(&mut store, state, now);
        changed
    });
    Some(changed)
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

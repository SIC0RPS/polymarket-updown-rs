use polymarket_client_sdk::types::U256;
use serde::{Deserialize, Serialize};

use crate::state::{MarketSelectionSnapshot, PolymarketOddsState};

pub(crate) const LIFECYCLE_ACTIVE: &str = "active";
pub(crate) const LIFECYCLE_PENDING: &str = "pending_revalidation";
pub(crate) const LIFECYCLE_PENDING_SUBSCRIPTION: &str = "pending_subscription";
pub(crate) const LIFECYCLE_INVALID_AFTER_404: &str = "invalid_after_404";
pub(crate) const LIFECYCLE_RESOLVED: &str = "resolved";
pub(crate) const LIFECYCLE_SYNC_SYNCED: &str = "synced";
pub(crate) const LIFECYCLE_SYNC_ROTATING: &str = "rotating";
pub(crate) const LIFECYCLE_SYNC_UNSYNCED: &str = "unsynced";
pub(crate) const LIFECYCLE_SYNC_RESYNCING_AFTER_ERROR: &str = "resyncing_after_error";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PolymarketLifecycleStatus {
    pub sync_state: String,
    pub sync_epoch: u64,
    pub rotation_epoch: u64,
    pub updated_at_ms: Option<u64>,
    pub rotation_started_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<u64>,
    pub active_tokens: usize,
    pub pending_revalidation_tokens: usize,
    pub pending_subscription_tokens: usize,
    pub invalid_after_404_tokens: usize,
    pub resolved_tokens: usize,
    pub quarantined_from_entry_tokens: usize,
    pub recent_book_404_total: u64,
    pub recent_book_404_tokens: Vec<String>,
}

pub(crate) fn apply_market_lifecycle_to_snapshot(
    state: &PolymarketOddsState,
    mut market: MarketSelectionSnapshot,
) -> MarketSelectionSnapshot {
    refresh_market_lifecycle(&state.token_lifecycle, &mut market);
    market
}

fn refresh_market_lifecycle(
    token_lifecycle: &std::collections::HashMap<U256, String>,
    market: &mut MarketSelectionSnapshot,
) -> bool {
    let up_lifecycle = token_lifecycle
        .get(&market.up_token)
        .map(String::as_str)
        .unwrap_or(LIFECYCLE_PENDING);
    let down_lifecycle = token_lifecycle
        .get(&market.down_token)
        .map(String::as_str)
        .unwrap_or(LIFECYCLE_PENDING);
    let mut changed = false;
    if market.up_lifecycle != up_lifecycle {
        market.up_lifecycle.clear();
        market.up_lifecycle.push_str(up_lifecycle);
        changed = true;
    }
    if market.down_lifecycle != down_lifecycle {
        market.down_lifecycle.clear();
        market.down_lifecycle.push_str(down_lifecycle);
        changed = true;
    }
    changed
}

pub(crate) fn refresh_market_lifecycles(state: &mut PolymarketOddsState) -> bool {
    let token_lifecycle = &state.token_lifecycle;
    let mut changed = false;
    if let Some(selection) = state.selection.as_mut() {
        changed |= refresh_market_lifecycle(token_lifecycle, selection);
    }
    if let Some(next_selection) = state.next_selection.as_mut() {
        changed |= refresh_market_lifecycle(token_lifecycle, next_selection);
    }
    for market in &mut state.markets {
        changed |= refresh_market_lifecycle(token_lifecycle, market);
    }
    changed
}

pub(crate) fn retain_tracked_tokens_in_state(state: &mut PolymarketOddsState, tracked: &[U256]) {
    state
        .token_lifecycle
        .retain(|token_id, _| tracked.contains(token_id));
}

pub(crate) fn mark_needs_snapshot_in_state(state: &mut PolymarketOddsState, token_id: U256) {
    if state.token_lifecycle.get(&token_id).map(String::as_str) != Some(LIFECYCLE_RESOLVED) {
        state
            .token_lifecycle
            .insert(token_id, LIFECYCLE_PENDING.to_string());
    }
}

pub(crate) fn apply_lifecycle_sync_to_state(
    state: &mut PolymarketOddsState,
    desired_assets: &[U256],
    sync_state: &str,
) {
    if matches!(
        sync_state,
        LIFECYCLE_SYNC_UNSYNCED | LIFECYCLE_SYNC_RESYNCING_AFTER_ERROR
    ) {
        for token_id in desired_assets {
            if !matches!(
                state
                    .token_lifecycle
                    .get(token_id)
                    .map(String::as_str)
                    .unwrap_or(LIFECYCLE_PENDING),
                LIFECYCLE_RESOLVED | LIFECYCLE_INVALID_AFTER_404
            ) {
                state
                    .token_lifecycle
                    .insert(*token_id, LIFECYCLE_PENDING.to_string());
            }
        }
    }
    if sync_state == LIFECYCLE_SYNC_SYNCED {
        state.polymarket_lifecycle.rotation_started_at_ms = None;
    }
}

pub(crate) fn note_rotation_delta_in_state(
    state: &mut PolymarketOddsState,
    desired_assets: &[U256],
    to_add: &[U256],
    rotation_epoch: u64,
    updated_at_ms: u64,
) {
    state.polymarket_lifecycle.sync_state = if desired_assets.is_empty() {
        LIFECYCLE_SYNC_SYNCED.to_string()
    } else {
        LIFECYCLE_SYNC_ROTATING.to_string()
    };
    state.polymarket_lifecycle.rotation_epoch = rotation_epoch;
    state.polymarket_lifecycle.updated_at_ms = Some(updated_at_ms);
    state.polymarket_lifecycle.rotation_started_at_ms = Some(updated_at_ms);
    state.polymarket_lifecycle.last_error = None;
    state.polymarket_lifecycle.last_error_at_ms = None;
    for token_id in to_add {
        if !matches!(
            state
                .token_lifecycle
                .get(token_id)
                .map(String::as_str)
                .unwrap_or(LIFECYCLE_PENDING),
            LIFECYCLE_ACTIVE | LIFECYCLE_RESOLVED | LIFECYCLE_INVALID_AFTER_404
        ) {
            state
                .token_lifecycle
                .insert(*token_id, LIFECYCLE_PENDING_SUBSCRIPTION.to_string());
        }
    }
}

pub(crate) fn mark_lifecycle_synced_in_state(state: &mut PolymarketOddsState, updated_at_ms: u64) {
    state.polymarket_lifecycle.sync_state = LIFECYCLE_SYNC_SYNCED.to_string();
    state.polymarket_lifecycle.updated_at_ms = Some(updated_at_ms);
    state.polymarket_lifecycle.rotation_started_at_ms = None;
    state.polymarket_lifecycle.last_error = None;
    state.polymarket_lifecycle.last_error_at_ms = None;
}

pub(crate) fn set_active_book_in_state(state: &mut PolymarketOddsState, token_id: U256) -> bool {
    let changed =
        state.token_lifecycle.get(&token_id).map(String::as_str) != Some(LIFECYCLE_ACTIVE);
    state
        .token_lifecycle
        .insert(token_id, LIFECYCLE_ACTIVE.to_string());
    changed
}

pub(crate) fn tombstone_no_orderbook_in_state(
    state: &mut PolymarketOddsState,
    token_id: U256,
) -> bool {
    let changed = state.token_lifecycle.get(&token_id).map(String::as_str)
        != Some(LIFECYCLE_INVALID_AFTER_404);
    state
        .token_lifecycle
        .insert(token_id, LIFECYCLE_INVALID_AFTER_404.to_string());
    changed
}

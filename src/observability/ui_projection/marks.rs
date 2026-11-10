use super::*;

pub(crate) fn prune_ui_market_cache(
    state: &mut UiState,
    active_market_ids: &HashSet<MarketId>,
    now_ms: u64,
) {
    let mut keep_market_ids: HashSet<MarketId> = active_market_ids.clone();

    if let Some(last) = state.last_market.as_ref() {
        keep_market_ids.insert(last.condition_id.clone());
    }
    if let Some(next) = state.next_market.as_ref() {
        keep_market_ids.insert(next.condition_id.clone());
    }

    for token in state.portfolio.positions.keys() {
        if let Some(condition_id) = state.token_to_condition_id.get(token) {
            keep_market_ids.insert(condition_id.clone());
        }
    }
    // Keep snapshots referenced by closed trades for asset name resolution.
    for trade in &state.portfolio.closed_trades {
        keep_market_ids.insert(trade.market_id.clone());
    }

    for (condition_id, snap) in &state.market_snapshots {
        if now_ms
            < snap
                .window_end_ms
                .saturating_add(UI_MARKET_SNAPSHOT_GRACE_MS)
        {
            keep_market_ids.insert(condition_id.clone());
        }
    }

    if state.market_snapshots.len() > UI_MARKET_SNAPSHOT_MAX {
        let mut evictable: Vec<(MarketId, u64)> = state
            .market_snapshots
            .iter()
            .filter(|(condition_id, _)| !keep_market_ids.contains(*condition_id))
            .map(|(condition_id, snap)| (condition_id.clone(), snap.window_end_ms.max(snap.ts_ms)))
            .collect();
        evictable.sort_by_key(|(_, ts)| *ts);

        let mut over = state
            .market_snapshots
            .len()
            .saturating_sub(UI_MARKET_SNAPSHOT_MAX);
        for (condition_id, _) in evictable {
            if over == 0 {
                break;
            }
            if state.market_snapshots.remove(&condition_id).is_some() {
                state.market_names.remove(&condition_id);
                keep_market_ids.remove(&condition_id);
                over -= 1;
            }
        }
    }

    state
        .market_snapshots
        .retain(|condition_id, _| keep_market_ids.contains(condition_id));
    state
        .market_names
        .retain(|condition_id, _| keep_market_ids.contains(condition_id));

    let mut rebuilt_token_map: HashMap<String, MarketId> = HashMap::new();
    for snap in state.market_snapshots.values() {
        rebuilt_token_map.insert(snap.up_token.clone(), snap.condition_id.clone());
        rebuilt_token_map.insert(snap.down_token.clone(), snap.condition_id.clone());
    }
    for token in state.portfolio.positions.keys() {
        if let Some(condition_id) = state.token_to_condition_id.get(token) {
            rebuilt_token_map.insert(token.clone(), condition_id.clone());
        }
    }
    // Preserve token→condition_id for closed trades so asset name resolution
    // continues to work after the market rotates out of discovery.
    for trade in &state.portfolio.closed_trades {
        if !rebuilt_token_map.contains_key(&trade.token) {
            if let Some(condition_id) = state.token_to_condition_id.get(&trade.token) {
                rebuilt_token_map.insert(trade.token.clone(), condition_id.clone());
            }
        }
    }
    state.token_to_condition_id = rebuilt_token_map;
}

pub(crate) fn prune_ui_cex_quotes(state: &mut UiState, now_ms: u64) {
    state
        .cex_quotes
        .retain(|_, quote| now_ms.saturating_sub(quote.ts_ms) <= UI_CEX_QUOTE_STALE_MS);
    if state.cex_quotes.len() <= UI_CEX_QUOTES_MAX {
        return;
    }

    let mut evictable: Vec<(String, u64)> = state
        .cex_quotes
        .iter()
        .map(|(key, quote)| (key.clone(), quote.ts_ms))
        .collect();
    evictable.sort_by_key(|(_, ts_ms)| *ts_ms);

    let mut over = evictable.len().saturating_sub(UI_CEX_QUOTES_MAX);
    for (key, _) in evictable {
        if over == 0 {
            break;
        }
        if state.cex_quotes.remove(&key).is_some() {
            over -= 1;
        }
    }
}

/// Extract a short asset label from a closed trade using current UiState context.
pub(crate) fn asset_label_for_closed_trade(
    state: &UiState,
    trade: &crate::portfolio::ClosedTrade,
) -> String {
    if let Some(cid) = state.token_to_condition_id.get(&trade.token) {
        if let Some(snap) = state.market_snapshots.get(cid) {
            if let Some(u) = snap.underlying.as_ref() {
                return u.clone();
            }
        }
    }
    "unknown".to_string()
}

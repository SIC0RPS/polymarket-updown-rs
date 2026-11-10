use std::time::Instant;

use polymarket_client_sdk::types::U256;
use serde::{Deserialize, Serialize};

use crate::state::PolymarketOddsState;

use super::book_state::refresh_selection_books;
use super::lifecycle_state::apply_market_lifecycle_to_snapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSelectionSnapshot {
    pub market_id: String,
    pub condition_id: String,
    pub gamma_market_id: String,
    pub condition_id_b256: Option<String>,
    pub underlying: String,
    pub up_token: U256,
    pub down_token: U256,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub maker_base_fee_bps: Option<i32>,
    pub taker_base_fee_bps: Option<i32>,
    pub minimum_tick_size: Option<f64>,
    pub minimum_order_size: Option<f64>,
    pub rewards_min_size: Option<f64>,
    pub rewards_max_spread: Option<f64>,
    pub resolution_source: Option<String>,
    pub neg_risk: Option<bool>,
    pub itode: Option<bool>,
    pub standard_binary_ctf: bool,
    pub question: Option<String>,
    pub slug: Option<String>,
    pub up_lifecycle: String,
    pub down_lifecycle: String,
    #[serde(skip)]
    pub market_frame_received_at: Option<Instant>,
    #[serde(skip)]
    pub market_state_published_at: Option<Instant>,
    #[serde(skip)]
    pub market_connection_session_id: Option<u64>,
    #[serde(skip)]
    pub market_frame_sequence: Option<u64>,
    #[serde(skip)]
    pub market_state_revision: Option<u64>,
}

impl PartialEq for MarketSelectionSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.market_id == other.market_id
            && self.condition_id == other.condition_id
            && self.gamma_market_id == other.gamma_market_id
            && self.condition_id_b256 == other.condition_id_b256
            && self.underlying == other.underlying
            && self.up_token == other.up_token
            && self.down_token == other.down_token
            && self.window_start_ms == other.window_start_ms
            && self.window_end_ms == other.window_end_ms
            && self.maker_base_fee_bps == other.maker_base_fee_bps
            && self.taker_base_fee_bps == other.taker_base_fee_bps
            && self.minimum_tick_size == other.minimum_tick_size
            && self.minimum_order_size == other.minimum_order_size
            && self.rewards_min_size == other.rewards_min_size
            && self.rewards_max_spread == other.rewards_max_spread
            && self.resolution_source == other.resolution_source
            && self.neg_risk == other.neg_risk
            && self.itode == other.itode
            && self.standard_binary_ctf == other.standard_binary_ctf
            && self.question == other.question
            && self.slug == other.slug
            && self.up_lifecycle == other.up_lifecycle
            && self.down_lifecycle == other.down_lifecycle
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscoverySnapshot {
    pub markets: Vec<MarketSelectionSnapshot>,
    pub selection: Option<MarketSelectionSnapshot>,
    pub next_selection: Option<MarketSelectionSnapshot>,
    pub asset_ids: Vec<U256>,
}

fn selection_changed(
    current: Option<&MarketSelectionSnapshot>,
    next: Option<&MarketSelectionSnapshot>,
) -> bool {
    match (current, next) {
        (Some(a), Some(b)) => a.market_id != b.market_id,
        (None, None) => false,
        _ => true,
    }
}

pub fn apply_discovery_snapshot_to_state(
    state: &mut PolymarketOddsState,
    snapshot: &DiscoverySnapshot,
    now_ms: u64,
) -> bool {
    let selection_changed_now =
        selection_changed(state.selection.as_ref(), snapshot.selection.as_ref());
    let next_changed = selection_changed(
        state.next_selection.as_ref(),
        snapshot.next_selection.as_ref(),
    );
    let mut changed = false;

    let next_selection = snapshot
        .selection
        .as_ref()
        .map(|selection| apply_market_lifecycle_to_snapshot(state, selection.clone()));
    if state.selection != next_selection {
        state.selection = next_selection;
        changed = true;
    }
    let next_next_selection = snapshot
        .next_selection
        .as_ref()
        .map(|selection| apply_market_lifecycle_to_snapshot(state, selection.clone()));
    if state.next_selection != next_next_selection {
        state.next_selection = next_next_selection;
        changed = true;
    }
    let next_markets: Vec<MarketSelectionSnapshot> = snapshot
        .markets
        .iter()
        .cloned()
        .map(|market| apply_market_lifecycle_to_snapshot(state, market))
        .collect();
    if state.markets != next_markets {
        state.markets = next_markets;
        changed = true;
    }
    if selection_changed_now {
        state.selection_updated_at_ms = now_ms;
        state.up = None;
        state.down = None;
        changed = true;
    }
    if next_changed {
        state.next_selection_updated_at_ms = now_ms;
        state.next_up = None;
        state.next_down = None;
        changed = true;
    }
    let book_len = state.books.len();
    state
        .books
        .retain(|token_id, _| snapshot.asset_ids.contains(token_id));
    changed |= state.books.len() != book_len;
    let depth_len = state.depths.len();
    state
        .depths
        .retain(|token_id, _| snapshot.asset_ids.contains(token_id));
    changed |= state.depths.len() != depth_len;
    let trade_len = state.last_trades.len();
    state
        .last_trades
        .retain(|trade| snapshot.asset_ids.contains(&trade.token_id));
    changed |= state.last_trades.len() != trade_len;
    let old_up = state.up.clone();
    let old_down = state.down.clone();
    let old_next_up = state.next_up.clone();
    let old_next_down = state.next_down.clone();
    refresh_selection_books(state);
    changed |= state.up != old_up
        || state.down != old_down
        || state.next_up != old_next_up
        || state.next_down != old_next_down;
    changed |= state.updated_at_ms != now_ms;
    state.updated_at_ms = now_ms;
    changed
}

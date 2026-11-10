use std::collections::hash_map::Entry;
use std::collections::HashMap;

use polymarket_client_sdk::clob::types::TickSize;
use polymarket_client_sdk::types::{Decimal, U256};
use serde::{Deserialize, Serialize};

use crate::state::normalize_ws_ts_ms;
use crate::wire::PolymarketBookFrame;

use super::book_validation::ws_ladder_conforms_to_tick;
use super::store::PolymarketOddsState;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BookDepth {
    pub token_id: U256,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub ts_ms: u64,
    #[serde(default)]
    pub revision: u64,
    #[serde(skip)]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct BookConstraints {
    pub tick_size: f64,
    pub min_order_size: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookTop {
    pub token_id: U256,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_sz: Option<f64>,
    pub ask_sz: Option<f64>,
    pub tick_size: f64,
    pub min_order_size: f64,
    pub ts_ms: u64,
    #[serde(skip)]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub size: f64,
}

fn replace_optional_book(slot: &mut Option<BookTop>, next: Option<&BookTop>) -> bool {
    match (slot.as_ref(), next) {
        (Some(current), Some(next)) if current == next => false,
        (_, Some(next)) => {
            *slot = Some(next.clone());
            true
        }
        (Some(_), None) => {
            *slot = None;
            true
        }
        (None, None) => false,
    }
}

fn upsert_book_top(books: &mut HashMap<U256, BookTop>, top: BookTop) -> bool {
    match books.entry(top.token_id) {
        Entry::Occupied(mut entry) => {
            if entry.get() == &top {
                return false;
            }
            entry.insert(top);
            true
        }
        Entry::Vacant(entry) => {
            entry.insert(top);
            true
        }
    }
}

fn upsert_book_depth(depths: &mut HashMap<U256, BookDepth>, mut depth: BookDepth) -> bool {
    match depths.entry(depth.token_id) {
        Entry::Occupied(mut entry) => {
            let current = entry.get();
            if current.token_id == depth.token_id
                && current.bids == depth.bids
                && current.asks == depth.asks
            {
                if current.ts_ms == depth.ts_ms && current.observed_at_ms == depth.observed_at_ms {
                    return false;
                }
                depth.revision = current.revision;
                entry.insert(depth);
                return true;
            }
            depth.revision = current.revision;
            entry.insert(depth);
            true
        }
        Entry::Vacant(entry) => {
            entry.insert(depth);
            true
        }
    }
}

pub(crate) fn update_selection_books(state: &mut PolymarketOddsState, top: &BookTop) -> bool {
    let selection_up_matches = state
        .selection
        .as_ref()
        .is_some_and(|sel| top.token_id == sel.up_token);
    let selection_down_matches = state
        .selection
        .as_ref()
        .is_some_and(|sel| top.token_id == sel.down_token);
    let next_up_matches = state
        .next_selection
        .as_ref()
        .is_some_and(|sel| top.token_id == sel.up_token);
    let next_down_matches = state
        .next_selection
        .as_ref()
        .is_some_and(|sel| top.token_id == sel.down_token);

    let mut changed = false;
    if let Some(sel) = state.selection.as_ref() {
        if top.token_id == sel.up_token {
            changed |= replace_optional_book(&mut state.up, Some(top));
        } else if top.token_id == sel.down_token {
            changed |= replace_optional_book(&mut state.down, Some(top));
        }
    } else {
        changed |= replace_optional_book(&mut state.up, Some(top));
    }
    if let Some(sel) = state.next_selection.as_ref() {
        if top.token_id == sel.up_token {
            changed |= replace_optional_book(&mut state.next_up, Some(top));
        } else if top.token_id == sel.down_token {
            changed |= replace_optional_book(&mut state.next_down, Some(top));
        }
    }

    // Clear stale mirrored slots if the same token no longer owns them.
    if selection_up_matches {
        changed |= replace_optional_book(&mut state.down, None);
    }
    if selection_down_matches {
        changed |= replace_optional_book(&mut state.up, None);
    }
    if next_up_matches {
        changed |= replace_optional_book(&mut state.next_down, None);
    }
    if next_down_matches {
        changed |= replace_optional_book(&mut state.next_up, None);
    }

    changed
}

pub(crate) fn refresh_selection_books(state: &mut PolymarketOddsState) {
    state.up = state
        .selection
        .as_ref()
        .and_then(|selection| state.books.get(&selection.up_token).cloned());
    state.down = state
        .selection
        .as_ref()
        .and_then(|selection| state.books.get(&selection.down_token).cloned());
    state.next_up = state
        .next_selection
        .as_ref()
        .and_then(|selection| state.books.get(&selection.up_token).cloned());
    state.next_down = state
        .next_selection
        .as_ref()
        .and_then(|selection| state.books.get(&selection.down_token).cloned());
}

pub(crate) fn clear_selection_book(state: &mut PolymarketOddsState, token_id: U256) -> bool {
    let mut changed = false;
    if state
        .selection
        .as_ref()
        .is_some_and(|sel| sel.up_token == token_id)
    {
        changed |= replace_optional_book(&mut state.up, None);
    }
    if state
        .selection
        .as_ref()
        .is_some_and(|sel| sel.down_token == token_id)
    {
        changed |= replace_optional_book(&mut state.down, None);
    }
    if state
        .next_selection
        .as_ref()
        .is_some_and(|sel| sel.up_token == token_id)
    {
        changed |= replace_optional_book(&mut state.next_up, None);
    }
    if state
        .next_selection
        .as_ref()
        .is_some_and(|sel| sel.down_token == token_id)
    {
        changed |= replace_optional_book(&mut state.next_down, None);
    }
    changed
}

pub(crate) fn remove_book_from_state(
    state: &mut PolymarketOddsState,
    token_id: U256,
    now_ms: u64,
) -> bool {
    let mut changed = state.books.remove(&token_id).is_some();
    changed |= state.depths.remove(&token_id).is_some();
    changed |= clear_selection_book(state, token_id);
    changed |= state.updated_at_ms != now_ms;
    state.updated_at_ms = now_ms;
    changed
}

pub fn polymarket_tick_size_from_f64(tick: f64) -> Option<TickSize> {
    if !tick.is_finite() {
        return None;
    }
    let tick = tick.to_string().parse::<Decimal>().ok()?;
    TickSize::try_from(tick).ok()
}

pub fn is_valid_polymarket_tick_size(tick: f64) -> bool {
    polymarket_tick_size_from_f64(tick).is_some()
}

pub fn apply_book_to_state(
    state: &mut PolymarketOddsState,
    frame: &PolymarketBookFrame,
    constraints: BookConstraints,
    now_ms: u64,
) -> bool {
    let asset_id = frame.asset_id;
    let ts_ms = normalize_ws_ts_ms(frame.timestamp);
    if ts_ms == 0
        || !constraints.min_order_size.is_finite()
        || constraints.min_order_size <= 0.0
        || !ws_ladder_conforms_to_tick(
            frame.bids.iter().map(|level| (level.price, level.size)),
            frame.asks.iter().map(|level| (level.price, level.size)),
            constraints.tick_size,
        )
    {
        return false;
    }
    let mut changed = false;

    let bid = frame.bids.first().map(|l| l.price);
    let bid_sz = frame.bids.first().map(|l| l.size);
    let ask = frame.asks.first().map(|l| l.price);
    let ask_sz = frame.asks.first().map(|l| l.size);

    let tick = constraints.tick_size;
    let min_order_size = constraints.min_order_size;

    let top = BookTop {
        token_id: asset_id,
        bid,
        ask,
        bid_sz,
        ask_sz,
        tick_size: tick,
        min_order_size,
        ts_ms,
        observed_at_ms: now_ms,
    };
    let mut bids = Vec::with_capacity(frame.bids.len());
    for level in &frame.bids {
        bids.push(PriceLevel {
            price: level.price,
            size: level.size,
        });
    }
    let mut asks = Vec::with_capacity(frame.asks.len());
    for level in &frame.asks {
        asks.push(PriceLevel {
            price: level.price,
            size: level.size,
        });
    }
    let depth = BookDepth {
        token_id: asset_id,
        bids,
        asks,
        ts_ms,
        revision: 0,
        observed_at_ms: now_ms,
    };
    changed |= upsert_book_top(&mut state.books, top.clone());
    changed |= upsert_book_depth(&mut state.depths, depth);
    changed |= update_selection_books(state, &top);
    changed |= state.updated_at_ms != now_ms;
    state.updated_at_ms = now_ms;
    changed
}

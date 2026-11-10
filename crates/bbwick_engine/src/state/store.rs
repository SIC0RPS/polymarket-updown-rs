use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use polymarket_client_sdk::types::{B256, U256};
use serde::{Deserialize, Serialize};

use crate::wire::PolymarketLastTradeFrame;

use super::book_state::{BookConstraints, BookDepth, BookTop};
use super::discovery_state::{
    apply_discovery_snapshot_to_state, DiscoverySnapshot, MarketSelectionSnapshot,
};
use super::fingerprint::Fingerprint;
use super::lifecycle_state::{
    apply_lifecycle_sync_to_state, mark_lifecycle_synced_in_state, mark_needs_snapshot_in_state,
    note_rotation_delta_in_state, refresh_market_lifecycles, retain_tracked_tokens_in_state,
    set_active_book_in_state, tombstone_no_orderbook_in_state, PolymarketLifecycleStatus,
};
use super::lifecycle_state::{
    LIFECYCLE_ACTIVE, LIFECYCLE_INVALID_AFTER_404, LIFECYCLE_PENDING,
    LIFECYCLE_PENDING_SUBSCRIPTION, LIFECYCLE_RESOLVED, LIFECYCLE_SYNC_RESYNCING_AFTER_ERROR,
    LIFECYCLE_SYNC_ROTATING, LIFECYCLE_SYNC_UNSYNCED,
};
use super::resolution_state::CapturedMarketResolutionContext;
use super::trade_state::{apply_last_trade_to_state, LastTrade};

const LIFECYCLE_RECENT_TOKEN_LIMIT: usize = 8;
const FIXED_8DP_SCALE: u64 = 100_000_000;

mod book_updates;
mod tick_updates;

pub(crate) use book_updates::PriceChangeApplyOutcome;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PolymarketOddsState {
    pub updated_at_ms: u64,
    pub revision: u64,
    pub trigger_fingerprint: Fingerprint,
    pub selection_updated_at_ms: u64,
    pub next_selection_updated_at_ms: u64,
    pub token_lifecycle: HashMap<U256, String>,
    pub resolved_market_winners: HashMap<B256, U256>,
    #[serde(default)]
    pub resolved_market_contexts: HashMap<B256, CapturedMarketResolutionContext>,
    pub polymarket_lifecycle: PolymarketLifecycleStatus,
    pub selection: Option<MarketSelectionSnapshot>,
    pub next_selection: Option<MarketSelectionSnapshot>,
    pub markets: Vec<MarketSelectionSnapshot>,
    pub up: Option<BookTop>,
    pub down: Option<BookTop>,
    pub next_up: Option<BookTop>,
    pub next_down: Option<BookTop>,
    pub books: HashMap<U256, BookTop>,
    pub depths: HashMap<U256, BookDepth>,
    #[serde(default)]
    pub last_canonical_bids: HashMap<U256, LastCanonicalBid>,
    pub last_trades: VecDeque<LastTrade>,
    pub next_last_trade_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LastCanonicalBid {
    pub last_canonical_bid_units_8dp: u64,
    pub book_ts_ms: u64,
    pub observed_at_ms: u64,
}

impl PolymarketOddsState {
    pub fn active_book(&self, token_id: U256) -> Option<&BookTop> {
        (self.token_lifecycle.get(&token_id).map(String::as_str) == Some(LIFECYCLE_ACTIVE))
            .then(|| self.books.get(&token_id))
            .flatten()
    }

    pub fn last_canonical_bid(&self, token_id: U256) -> Option<&LastCanonicalBid> {
        self.last_canonical_bids.get(&token_id)
    }

    pub fn retain_validated_canonical_bid(&mut self, book: &BookTop) {
        let Some(units) = book.bid.and_then(canonical_bid_units_8dp) else {
            return;
        };
        if book.ts_ms == 0 || book.observed_at_ms == 0 || book.ts_ms > book.observed_at_ms {
            return;
        }
        if let Some(existing) = self.last_canonical_bids.get(&book.token_id) {
            if book.ts_ms < existing.book_ts_ms
                || (book.ts_ms == existing.book_ts_ms
                    && book.observed_at_ms < existing.observed_at_ms)
            {
                return;
            }
        }
        self.last_canonical_bids.insert(
            book.token_id,
            LastCanonicalBid {
                last_canonical_bid_units_8dp: units,
                book_ts_ms: book.ts_ms,
                observed_at_ms: book.observed_at_ms,
            },
        );
    }
}

fn canonical_bid_units_8dp(value: f64) -> Option<u64> {
    if !(value.is_finite() && value > 0.0 && value <= 1.0) {
        return None;
    }
    let scaled = value * FIXED_8DP_SCALE as f64;
    let rounded = scaled.round();
    ((scaled - rounded).abs() <= 0.000_001).then_some(rounded as u64)
}

#[derive(Debug, Clone, Default)]
pub struct PolymarketStateStore {
    pub(super) state: PolymarketOddsState,
    book_constraints_by_token: HashMap<U256, BookConstraints>,
    depth_revision_by_token: HashMap<U256, u64>,
    token_book_state: HashMap<U256, TokenBookStatus>,
    next_book_authority_epoch: u64,
    validated_books: HashMap<U256, BTreeMap<(Instant, u64), ValidatedBookProvenance>>,
    recent_book_404_total: u64,
    recent_book_404_tokens: VecDeque<U256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenBookState {
    ActiveBook,
    NeedsSnapshot,
    NoOrderBook,
    BrokenWs,
}

#[derive(Debug, Clone, Copy)]
struct TokenBookStatus {
    state: TokenBookState,
    tick_authority_timestamp_ms: Option<u64>,
    authority_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
struct ValidatedBookProvenance {
    authority_epoch: u64,
    expires_at: Instant,
    payload: Arc<[u8]>,
}

pub struct LifecycleSyncRequest<'a> {
    pub sync_state: &'a str,
    pub sync_epoch: u64,
    pub rotation_epoch: u64,
    pub updated_at_ms: Option<u64>,
    pub rotation_started_at_ms: Option<u64>,
    pub last_error: Option<String>,
    pub last_error_at_ms: Option<u64>,
    pub desired_assets: &'a [U256],
}

impl PolymarketStateStore {
    pub(crate) fn record_market_frame_timing(
        &mut self,
        projected: &mut PolymarketOddsState,
        market: B256,
        received_at: Instant,
        published_at: Instant,
        connection_session_id: u64,
        sequence: u64,
    ) {
        let revision = projected.revision;
        Self::stamp_market_frame_timing(
            &mut self.state,
            market,
            received_at,
            published_at,
            connection_session_id,
            sequence,
            revision,
        );
        Self::stamp_market_frame_timing(
            projected,
            market,
            received_at,
            published_at,
            connection_session_id,
            sequence,
            revision,
        );
    }

    fn stamp_market_frame_timing(
        state: &mut PolymarketOddsState,
        market: B256,
        received_at: Instant,
        published_at: Instant,
        connection_session_id: u64,
        sequence: u64,
        revision: u64,
    ) {
        let mut stamp = |selection: &mut MarketSelectionSnapshot| {
            if selection
                .condition_id_b256
                .as_deref()
                .and_then(|condition_id| condition_id.parse::<B256>().ok())
                == Some(market)
            {
                selection.market_frame_received_at = Some(received_at);
                selection.market_state_published_at = Some(published_at);
                selection.market_connection_session_id = Some(connection_session_id);
                selection.market_frame_sequence = Some(sequence);
                selection.market_state_revision = Some(revision);
            }
        };
        state.markets.iter_mut().for_each(&mut stamp);
        if let Some(selection) = state.selection.as_mut() {
            stamp(selection);
        }
        if let Some(selection) = state.next_selection.as_mut() {
            stamp(selection);
        }
    }

    pub fn with_retained_canonical_bids(
        bids: HashMap<U256, LastCanonicalBid>,
    ) -> (Self, PolymarketOddsState) {
        let mut initial_state = PolymarketOddsState::default();
        initial_state.last_canonical_bids = bids
            .into_iter()
            .filter(|(_, bid)| {
                (1..=FIXED_8DP_SCALE).contains(&bid.last_canonical_bid_units_8dp)
                    && bid.book_ts_ms > 0
                    && bid.observed_at_ms >= bid.book_ts_ms
            })
            .collect();
        let store = Self {
            state: initial_state.clone(),
            ..Self::default()
        };
        (store, initial_state)
    }

    fn state_for(&self, token_id: U256) -> &str {
        self.state
            .token_lifecycle
            .get(&token_id)
            .map(String::as_str)
            .unwrap_or(LIFECYCLE_PENDING)
    }

    pub(crate) fn book_authority_missing(&self, token_id: U256) -> bool {
        !self.book_constraints_by_token.contains_key(&token_id)
            && self.state_for(token_id) != LIFECYCLE_RESOLVED
            && !self.no_orderbook_tombstoned(token_id)
    }

    fn known_market_for_token(&self, token_id: U256) -> Option<B256> {
        self.state
            .markets
            .iter()
            .chain(self.state.selection.iter())
            .chain(self.state.next_selection.iter())
            .filter(|market| market.up_token == token_id || market.down_token == token_id)
            .find_map(|market| {
                market
                    .condition_id_b256
                    .as_deref()
                    .and_then(|condition_id| condition_id.parse().ok())
            })
    }

    pub(super) fn reject_market_event(
        &mut self,
        state: &mut PolymarketOddsState,
        token_id: U256,
        now_ms: u64,
    ) -> bool {
        if self.known_market_for_token(token_id).is_some()
            || self.state.token_lifecycle.contains_key(&token_id)
        {
            return self.sync_needs_snapshot(state, token_id, now_ms);
        }
        let mut changed =
            super::book_state::remove_book_from_state(&mut self.state, token_id, now_ms);
        changed |= super::book_state::remove_book_from_state(state, token_id, now_ms);
        changed |= self.state.token_lifecycle.remove(&token_id).is_some();
        changed |= state.token_lifecycle.remove(&token_id).is_some();
        self.remove_token_book_status(token_id);
        self.book_constraints_by_token.remove(&token_id);
        self.depth_revision_by_token.remove(&token_id);
        changed
    }

    pub fn no_orderbook_tombstoned(&self, token_id: U256) -> bool {
        self.token_book_state
            .get(&token_id)
            .is_some_and(|status| status.state == TokenBookState::NoOrderBook)
    }

    fn retain_tracked_tokens(
        &mut self,
        state: &mut PolymarketOddsState,
        tracked: &[U256],
        now_ms: u64,
    ) -> bool {
        let removed = self
            .state
            .token_lifecycle
            .keys()
            .copied()
            .filter(|token_id| !tracked.contains(token_id))
            .collect::<Vec<_>>();
        let mut changed = false;
        for token_id in removed {
            changed |= super::book_state::remove_book_from_state(&mut self.state, token_id, now_ms);
            changed |= super::book_state::remove_book_from_state(state, token_id, now_ms);
        }
        retain_tracked_tokens_in_state(&mut self.state, tracked);
        retain_tracked_tokens_in_state(state, tracked);
        let untracked = self
            .token_book_state
            .keys()
            .copied()
            .filter(|token_id| !tracked.contains(token_id))
            .collect::<Vec<_>>();
        for token_id in untracked {
            self.remove_token_book_status(token_id);
        }
        self.book_constraints_by_token
            .retain(|token_id, _| tracked.contains(token_id));
        self.depth_revision_by_token
            .retain(|token_id, _| tracked.contains(token_id));
        self.recent_book_404_tokens
            .retain(|token_id| tracked.contains(token_id));
        self.recompute_lifecycle_status();
        changed
    }

    pub fn note_tracked_tokens(&mut self, asset_ids: &[U256]) {
        for token_id in asset_ids {
            if self.no_orderbook_tombstoned(*token_id) {
                continue;
            }
            match self.state.token_lifecycle.get(token_id).map(String::as_str) {
                Some(
                    LIFECYCLE_ACTIVE
                    | LIFECYCLE_PENDING
                    | LIFECYCLE_PENDING_SUBSCRIPTION
                    | LIFECYCLE_RESOLVED,
                ) => {}
                Some(LIFECYCLE_INVALID_AFTER_404) => self.mark_needs_snapshot(*token_id),
                _ => self.mark_needs_snapshot(*token_id),
            }
        }
        self.recompute_lifecycle_status();
    }

    pub fn mark_needs_snapshot(&mut self, token_id: U256) {
        mark_needs_snapshot_in_state(&mut self.state, token_id);
        self.replace_token_book_status(token_id, TokenBookState::NeedsSnapshot);
    }

    pub fn mark_broken_ws(&mut self, asset_ids: &[U256]) {
        for token_id in asset_ids {
            self.replace_token_book_status(*token_id, TokenBookState::BrokenWs);
        }
    }

    fn set_lifecycle_sync(&mut self, request: LifecycleSyncRequest<'_>) -> bool {
        let previous_lifecycle = self.state.polymarket_lifecycle.clone();
        let previous_token_lifecycle = self.state.token_lifecycle.clone();
        self.state.polymarket_lifecycle.sync_state = request.sync_state.to_string();
        self.state.polymarket_lifecycle.sync_epoch = request.sync_epoch;
        self.state.polymarket_lifecycle.rotation_epoch = request.rotation_epoch;
        self.state.polymarket_lifecycle.updated_at_ms = request.updated_at_ms;
        self.state.polymarket_lifecycle.rotation_started_at_ms = request.rotation_started_at_ms;
        self.state.polymarket_lifecycle.last_error = request.last_error;
        self.state.polymarket_lifecycle.last_error_at_ms = request.last_error_at_ms;
        let sync_state = self.state.polymarket_lifecycle.sync_state.clone();
        apply_lifecycle_sync_to_state(&mut self.state, request.desired_assets, sync_state.as_str());
        if matches!(
            self.state.polymarket_lifecycle.sync_state.as_str(),
            LIFECYCLE_SYNC_UNSYNCED | LIFECYCLE_SYNC_RESYNCING_AFTER_ERROR
        ) {
            self.mark_broken_ws(request.desired_assets);
        }
        self.recompute_lifecycle_status();
        self.state.polymarket_lifecycle != previous_lifecycle
            || self.state.token_lifecycle != previous_token_lifecycle
    }

    pub fn note_rotation_delta(
        &mut self,
        desired_assets: &[U256],
        to_add: &[U256],
        to_remove: &[U256],
        rotation_epoch: u64,
        updated_at_ms: u64,
    ) {
        note_rotation_delta_in_state(
            &mut self.state,
            desired_assets,
            to_add,
            rotation_epoch,
            updated_at_ms,
        );
        for token_id in to_remove {
            self.remove_token_book_status(*token_id);
            self.book_constraints_by_token.remove(token_id);
        }
        self.recompute_lifecycle_status();
    }

    pub fn maybe_mark_lifecycle_synced(&mut self, asset_ids: &[U256], updated_at_ms: u64) -> bool {
        if !matches!(
            self.state.polymarket_lifecycle.sync_state.as_str(),
            LIFECYCLE_SYNC_ROTATING | LIFECYCLE_SYNC_RESYNCING_AFTER_ERROR
        ) || asset_ids.is_empty()
        {
            return false;
        }
        let all_assets_rehydrated = asset_ids.iter().all(|token_id| {
            self.state_for(*token_id) == LIFECYCLE_RESOLVED
                || self.state_for(*token_id) == LIFECYCLE_ACTIVE
        });
        if !all_assets_rehydrated {
            return false;
        }
        mark_lifecycle_synced_in_state(&mut self.state, updated_at_ms);
        self.recompute_lifecycle_status();
        true
    }

    fn set_active_book(&mut self, token_id: U256) -> bool {
        let changed = set_active_book_in_state(&mut self.state, token_id);
        self.replace_token_book_status(token_id, TokenBookState::ActiveBook);
        self.recompute_lifecycle_status();
        changed
    }

    fn tombstone_no_orderbook(&mut self, token_id: U256) -> bool {
        let changed = tombstone_no_orderbook_in_state(&mut self.state, token_id);
        self.replace_token_book_status(token_id, TokenBookState::NoOrderBook);
        self.book_constraints_by_token.remove(&token_id);
        if changed {
            self.recent_book_404_total = self.recent_book_404_total.saturating_add(1);
            self.recent_book_404_tokens.push_front(token_id);
            while self.recent_book_404_tokens.len() > LIFECYCLE_RECENT_TOKEN_LIMIT {
                let _ = self.recent_book_404_tokens.pop_back();
            }
        }
        self.recompute_lifecycle_status();
        changed
    }

    pub fn sync_discovery_snapshot(
        &mut self,
        state: &mut PolymarketOddsState,
        snapshot: &DiscoverySnapshot,
        now_ms: u64,
    ) -> bool {
        self.note_tracked_tokens(&snapshot.asset_ids);
        let mut changed = self.retain_tracked_tokens(state, &snapshot.asset_ids, now_ms);
        changed |= apply_discovery_snapshot_to_state(&mut self.state, snapshot, now_ms);
        self.recompute_lifecycle_status();
        changed |= self.maybe_mark_lifecycle_synced(&snapshot.asset_ids, now_ms);
        changed |= self.project_discovery_snapshot(state, snapshot, now_ms);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_lifecycle_state(&mut self, state: &mut PolymarketOddsState) -> bool {
        let changed = self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_tombstone_no_orderbook(
        &mut self,
        state: &mut PolymarketOddsState,
        token_id: U256,
        now_ms: u64,
    ) -> bool {
        let mut changed = self.tombstone_no_orderbook(token_id);
        changed |= super::book_state::remove_book_from_state(&mut self.state, token_id, now_ms);
        changed |= super::book_state::remove_book_from_state(state, token_id, now_ms);
        changed |= self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_last_trade(
        &mut self,
        state: &mut PolymarketOddsState,
        frame: &PolymarketLastTradeFrame,
        now_ms: u64,
    ) -> bool {
        let changed = apply_last_trade_to_state(&mut self.state, frame, now_ms);
        let changed = apply_last_trade_to_state(state, frame, now_ms) || changed;
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_ws_last_trade(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketLastTradeFrame,
        now_ms: u64,
    ) -> bool {
        if self.known_market_for_token(frame.asset_id) != Some(market) {
            return self.reject_market_event(state, frame.asset_id, now_ms);
        }
        self.sync_last_trade(state, frame, now_ms)
    }

    pub fn sync_invalid_ws_event(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        token_id: U256,
        now_ms: u64,
    ) -> bool {
        if self.known_market_for_token(token_id) == Some(market) {
            self.sync_needs_snapshot(state, token_id, now_ms)
        } else {
            self.reject_market_event(state, token_id, now_ms)
        }
    }

    pub fn sync_transport_failure(&mut self, state: &mut PolymarketOddsState, now_ms: u64) -> bool {
        let mut token_ids = self
            .state
            .token_lifecycle
            .keys()
            .chain(self.state.books.keys())
            .chain(self.state.depths.keys())
            .copied()
            .collect::<Vec<_>>();
        token_ids.sort_unstable();
        token_ids.dedup();
        self.mark_broken_ws(&token_ids);
        let mut changed = false;
        for token_id in token_ids {
            changed |= super::book_state::remove_book_from_state(&mut self.state, token_id, now_ms);
            changed |= super::book_state::remove_book_from_state(state, token_id, now_ms);
        }
        changed |= self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_lifecycle_sync(
        &mut self,
        state: &mut PolymarketOddsState,
        request: LifecycleSyncRequest<'_>,
    ) -> bool {
        let changed = self.set_lifecycle_sync(request);
        let changed = self.project_lifecycle_state(state) || changed;
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub(super) fn project_lifecycle_state(&self, state: &mut PolymarketOddsState) -> bool {
        let mut changed = false;
        if state.token_lifecycle != self.state.token_lifecycle {
            state
                .token_lifecycle
                .clone_from(&self.state.token_lifecycle);
            refresh_market_lifecycles(state);
            changed = true;
        }
        if state.polymarket_lifecycle != self.state.polymarket_lifecycle {
            state.polymarket_lifecycle = self.state.polymarket_lifecycle.clone();
            changed = true;
        }
        if state.last_canonical_bids != self.state.last_canonical_bids {
            state
                .last_canonical_bids
                .clone_from(&self.state.last_canonical_bids);
            changed = true;
        }
        changed
    }

    fn project_discovery_snapshot(
        &self,
        state: &mut PolymarketOddsState,
        snapshot: &DiscoverySnapshot,
        now_ms: u64,
    ) -> bool {
        let mut changed = apply_discovery_snapshot_to_state(state, snapshot, now_ms);
        changed |= self.project_lifecycle_state(state);
        changed
    }

    fn recompute_lifecycle_status(&mut self) {
        let mut active_tokens = 0_usize;
        let mut pending_revalidation_tokens = 0_usize;
        let mut pending_subscription_tokens = 0_usize;
        let mut invalid_after_404_tokens = 0_usize;
        let mut resolved_tokens = 0_usize;
        for state in self.state.token_lifecycle.values().map(String::as_str) {
            match state {
                LIFECYCLE_ACTIVE => active_tokens += 1,
                LIFECYCLE_PENDING => pending_revalidation_tokens += 1,
                LIFECYCLE_PENDING_SUBSCRIPTION => pending_subscription_tokens += 1,
                LIFECYCLE_INVALID_AFTER_404 => invalid_after_404_tokens += 1,
                LIFECYCLE_RESOLVED => resolved_tokens += 1,
                _ => pending_revalidation_tokens += 1,
            }
        }
        self.state.polymarket_lifecycle.active_tokens = active_tokens;
        self.state.polymarket_lifecycle.pending_revalidation_tokens = pending_revalidation_tokens;
        self.state.polymarket_lifecycle.pending_subscription_tokens = pending_subscription_tokens;
        self.state.polymarket_lifecycle.invalid_after_404_tokens = invalid_after_404_tokens;
        self.state.polymarket_lifecycle.resolved_tokens = resolved_tokens;
        self.state
            .polymarket_lifecycle
            .quarantined_from_entry_tokens = invalid_after_404_tokens + resolved_tokens;
        self.state.polymarket_lifecycle.recent_book_404_total = self.recent_book_404_total;
        self.state.polymarket_lifecycle.recent_book_404_tokens = self
            .recent_book_404_tokens
            .iter()
            .map(ToString::to_string)
            .collect();
        if self.state.polymarket_lifecycle.sync_state.is_empty() {
            self.state.polymarket_lifecycle.sync_state = LIFECYCLE_SYNC_UNSYNCED.to_string();
        }
    }

    fn replace_token_book_status(&mut self, token_id: U256, state: TokenBookState) {
        let previous = self.token_book_state.get(&token_id).copied();
        let tick_authority_timestamp_ms =
            previous.and_then(|status| status.tick_authority_timestamp_ms);
        let authority_epoch = if state == TokenBookState::ActiveBook {
            previous
                .filter(|status| status.state == TokenBookState::ActiveBook)
                .and_then(|status| status.authority_epoch)
                .or_else(|| {
                    self.next_book_authority_epoch = self
                        .next_book_authority_epoch
                        .checked_add(1)
                        .expect("book authority epoch exhausted");
                    Some(self.next_book_authority_epoch)
                })
        } else {
            self.validated_books.remove(&token_id);
            None
        };
        self.token_book_state.insert(
            token_id,
            TokenBookStatus {
                state,
                tick_authority_timestamp_ms,
                authority_epoch,
            },
        );
    }

    fn remove_token_book_status(&mut self, token_id: U256) {
        self.token_book_state.remove(&token_id);
        self.validated_books.remove(&token_id);
    }

    pub(crate) fn book_authority_epoch(&self, token_id: U256) -> Option<u64> {
        self.token_book_state
            .get(&token_id)
            .filter(|status| status.state == TokenBookState::ActiveBook)
            .and_then(|status| status.authority_epoch)
            .filter(|_| self.state.books.contains_key(&token_id))
    }
}

pub fn normalize_ws_ts_ms(ts: i64) -> u64 {
    if ts <= 0 {
        return 0;
    }
    let raw = ts as u64;
    if raw < 1_000_000_000_000 {
        return raw.saturating_mul(1_000);
    }
    if raw > 10_000_000_000_000_000 {
        return raw / 1_000_000;
    }
    if raw > 10_000_000_000_000 {
        return raw / 1_000;
    }
    raw
}

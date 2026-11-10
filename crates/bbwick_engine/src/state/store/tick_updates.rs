use polymarket_client_sdk::types::{B256, U256};

use crate::state::book_state::{
    polymarket_tick_size_from_f64, remove_book_from_state, update_selection_books,
};
use crate::state::book_validation::ws_ladder_conforms_to_tick;
use crate::state::normalize_ws_ts_ms;
use crate::wire::PolymarketTickSizeFrame;

use super::{PolymarketOddsState, PolymarketStateStore, TokenBookState};

impl PolymarketStateStore {
    pub fn sync_tick_size(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketTickSizeFrame,
        now_ms: u64,
    ) -> bool {
        if self.known_market_for_token(frame.asset_id) != Some(market) {
            return self.reject_market_event(state, frame.asset_id, now_ms);
        }
        let changed = self.apply_tick_transition(state, frame, now_ms);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    fn apply_tick_transition(
        &mut self,
        state: &mut PolymarketOddsState,
        frame: &PolymarketTickSizeFrame,
        now_ms: u64,
    ) -> bool {
        let asset_id = frame.asset_id;
        if self.no_orderbook_tombstoned(asset_id) {
            return false;
        }
        let ts_ms = normalize_ws_ts_ms(frame.timestamp);
        let watermark = self
            .token_book_state
            .get(&asset_id)
            .and_then(|status| status.tick_authority_timestamp_ms);
        if ts_ms > 0 && watermark.is_some_and(|watermark| ts_ms < watermark) {
            return false;
        }
        let old_tick = polymarket_tick_size_from_f64(frame.old_tick_size);
        let new_tick = polymarket_tick_size_from_f64(frame.new_tick_size);
        let current = self.book_constraints_by_token.get(&asset_id).copied();
        let current_tick =
            current.and_then(|constraints| polymarket_tick_size_from_f64(constraints.tick_size));
        let same_timestamp = ts_ms > 0 && watermark == Some(ts_ms);

        tracing::trace!(
            target: "engine.polymarket_ws",
            asset_id = %asset_id,
            timestamp_ms = ts_ms,
            old_tick_size = frame.old_tick_size,
            new_tick_size = frame.new_tick_size,
            current_tick_size = current.map(|constraints| constraints.tick_size),
            "bbwick_polymarket_tick_transition"
        );

        if same_timestamp && new_tick.is_some() && new_tick == current_tick {
            return false;
        }
        if !same_timestamp
            && ts_ms > 0
            && new_tick.is_some()
            && new_tick == current_tick
            && current.is_some()
        {
            return self.advance_tick_watermark(asset_id, ts_ms);
        }

        let newer_than_watermark = watermark.is_none_or(|watermark| ts_ms > watermark);
        let retain_compatible_book = ts_ms > 0
            && newer_than_watermark
            && old_tick.is_some()
            && old_tick == current_tick
            && new_tick.is_some()
            && new_tick != current_tick
            && self.book_authority_epoch(asset_id).is_some()
            && self.state.books.contains_key(&asset_id)
            && self.state.depths.get(&asset_id).is_some_and(|depth| {
                ws_ladder_conforms_to_tick(
                    depth.bids.iter().map(|level| (level.price, level.size)),
                    depth.asks.iter().map(|level| (level.price, level.size)),
                    frame.new_tick_size,
                )
            });
        if retain_compatible_book {
            let mut changed = self.advance_tick_watermark(asset_id, ts_ms);
            if let Some(constraints) = self.book_constraints_by_token.get_mut(&asset_id) {
                changed |= constraints.tick_size != frame.new_tick_size;
                constraints.tick_size = frame.new_tick_size;
            }
            let top = self
                .state
                .books
                .get_mut(&asset_id)
                .expect("compatible book must retain its canonical top");
            changed |= top.tick_size != frame.new_tick_size;
            top.tick_size = frame.new_tick_size;
            let top = top.clone();
            changed |= update_selection_books(&mut self.state, &top);
            changed |= self.state.updated_at_ms != now_ms;
            self.state.updated_at_ms = now_ms;
            changed |= self.project_canonical_book(state, asset_id);
            return changed;
        }

        if ts_ms > 0 {
            let _ = self.advance_tick_watermark(asset_id, ts_ms);
        }
        let retain_supported_constraint =
            ts_ms > 0 && !same_timestamp && new_tick.is_some() && current.is_some();
        let mut changed = false;
        if retain_supported_constraint {
            if let Some(constraints) = self.book_constraints_by_token.get_mut(&asset_id) {
                constraints.tick_size = frame.new_tick_size;
                changed = true;
            }
        } else {
            changed |= self.book_constraints_by_token.remove(&asset_id).is_some();
        }
        self.mark_needs_snapshot(asset_id);
        changed |= remove_book_from_state(&mut self.state, asset_id, now_ms);
        changed |= remove_book_from_state(state, asset_id, now_ms);
        changed |= self.project_lifecycle_state(state);
        changed
    }

    fn advance_tick_watermark(&mut self, asset_id: U256, ts_ms: u64) -> bool {
        if !self.token_book_state.contains_key(&asset_id) {
            self.replace_token_book_status(asset_id, TokenBookState::NeedsSnapshot);
        }
        if let Some(status) = self.token_book_state.get_mut(&asset_id) {
            let next = Some(
                status
                    .tick_authority_timestamp_ms
                    .map_or(ts_ms, |watermark| watermark.max(ts_ms)),
            );
            let changed = status.tick_authority_timestamp_ms != next;
            status.tick_authority_timestamp_ms = next;
            return changed;
        }
        false
    }
}

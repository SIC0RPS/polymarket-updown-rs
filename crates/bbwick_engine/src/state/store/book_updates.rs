use std::sync::Arc;
use std::time::Instant;

use polymarket_client_sdk::clob::types::response::{OrderBookSummaryResponse, OrderSummary};
use polymarket_client_sdk::clob::types::TickSize;
use polymarket_client_sdk::types::{Decimal, B256, U256};

use crate::wire::{
    PolymarketBookFrame, PolymarketBookLevelFrame, PolymarketPriceChangeEntryFrame,
    PolymarketPriceChangeFrame, TradeSide,
};

use super::{PolymarketOddsState, PolymarketStateStore, ValidatedBookProvenance};
use crate::state::book_state::{
    apply_book_to_state, polymarket_tick_size_from_f64, remove_book_from_state,
    update_selection_books, BookConstraints, BookDepth, BookTop, PriceLevel,
};
use crate::state::book_validation::{
    decimal_price_conforms_to_tick, ws_ladder_conforms_to_tick, ws_price_conforms_to_tick,
};
use crate::state::{normalize_ws_ts_ms, strict_ladder_is_valid};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PriceChangeApplyOutcome {
    pub(crate) changed: bool,
    pub(crate) invalid_assets: Vec<U256>,
}

impl PolymarketStateStore {
    pub fn sync_book(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketBookFrame,
        now_ms: u64,
    ) -> bool {
        self.sync_book_with_outcome(state, market, frame, now_ms).0
    }

    pub(crate) fn sync_book_with_outcome(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketBookFrame,
        now_ms: u64,
    ) -> (bool, bool) {
        let asset_id = frame.asset_id;
        if self.known_market_for_token(asset_id) != Some(market) {
            return (self.reject_market_event(state, asset_id, now_ms), false);
        }
        if self.no_orderbook_tombstoned(asset_id) {
            return (false, false);
        }
        let ts_ms = normalize_ws_ts_ms(frame.timestamp);
        let older_than_canonical = self
            .state
            .depths
            .get(&asset_id)
            .is_some_and(|depth| ts_ms < depth.ts_ms);
        if older_than_canonical && ts_ms == 0 {
            return (self.sync_needs_snapshot(state, asset_id, now_ms), false);
        }
        let Some(constraints) = self.book_constraints_by_token.get(&asset_id).copied() else {
            return (false, false);
        };
        let valid_book = ts_ms > 0
            && !self
                .token_book_state
                .get(&asset_id)
                .and_then(|status| status.tick_authority_timestamp_ms)
                .is_some_and(|watermark| ts_ms < watermark)
            && ws_ladder_conforms_to_tick(
                frame.bids.iter().map(|level| (level.price, level.size)),
                frame.asks.iter().map(|level| (level.price, level.size)),
                constraints.tick_size,
            );
        if older_than_canonical {
            return (
                false,
                valid_book
                    && constraints.min_order_size.is_finite()
                    && constraints.min_order_size > 0.0,
            );
        }
        if !valid_book {
            return (self.sync_needs_snapshot(state, asset_id, now_ms), false);
        }
        let ladder_changed = self
            .state
            .depths
            .get(&asset_id)
            .map(|previous| {
                previous.bids.len() != frame.bids.len()
                    || previous
                        .bids
                        .iter()
                        .zip(&frame.bids)
                        .any(|(previous, incoming)| {
                            previous.price != incoming.price || previous.size != incoming.size
                        })
                    || previous.asks.len() != frame.asks.len()
                    || previous
                        .asks
                        .iter()
                        .zip(&frame.asks)
                        .any(|(previous, incoming)| {
                            previous.price != incoming.price || previous.size != incoming.size
                        })
            })
            .unwrap_or(true);
        let mut changed = apply_book_to_state(&mut self.state, frame, constraints, now_ms);
        self.refresh_depth_revision(asset_id, changed && ladder_changed);
        if let Some(top) = self.state.books.get(&asset_id).cloned() {
            self.state.retain_validated_canonical_bid(&top);
        }
        changed |= self.set_active_book(asset_id);
        changed |= self.project_canonical_book(state, asset_id);
        changed |= self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        (changed, true)
    }

    pub(crate) fn record_validated_book(
        &mut self,
        token_id: U256,
        digest: u64,
        first_seen: Instant,
        expires_at: Instant,
        payload: &Arc<[u8]>,
        next_expiry: Option<Instant>,
    ) -> Option<u64> {
        let now = Instant::now();
        if next_expiry.is_some_and(|expires_at| expires_at <= now) {
            self.prune_expired_provenance(now);
        }
        let epoch = self.book_authority_epoch(token_id)?;
        self.validated_books.entry(token_id).or_default().insert(
            (first_seen, digest),
            ValidatedBookProvenance {
                authority_epoch: epoch,
                expires_at,
                payload: Arc::clone(payload),
            },
        );
        Some(epoch)
    }

    pub(crate) fn validated_book_epochs(
        &mut self,
        token_ids: &[U256],
        digest: u64,
        first_seen: Instant,
        payload: &[u8],
    ) -> Vec<(U256, u64)> {
        self.prune_expired_provenance(Instant::now());
        token_ids
            .iter()
            .filter_map(|token_id| {
                let epoch = self.book_authority_epoch(*token_id)?;
                self.validated_books
                    .get(token_id)?
                    .get(&(first_seen, digest))
                    .filter(|validated| {
                        validated.expires_at > Instant::now()
                            && validated.authority_epoch == epoch
                            && validated.payload.as_ref() == payload
                    })
                    .map(|_| (*token_id, epoch))
            })
            .collect()
    }

    pub(crate) fn prune_expired_provenance(&mut self, now: Instant) -> Option<Instant> {
        let mut next_expiry = None;
        self.validated_books.retain(|_, entries| {
            while entries
                .first_key_value()
                .is_some_and(|(_, validated)| validated.expires_at <= now)
            {
                entries.pop_first();
            }
            if let Some((_, validated)) = entries.first_key_value() {
                next_expiry = Some(
                    next_expiry.map_or(validated.expires_at, |current: Instant| {
                        current.min(validated.expires_at)
                    }),
                );
            }
            !entries.is_empty()
        });
        next_expiry
    }

    pub fn sync_rest_book(
        &mut self,
        state: &mut PolymarketOddsState,
        requested_asset_id: U256,
        response: &OrderBookSummaryResponse,
        now_ms: u64,
    ) -> bool {
        if response.asset_id != requested_asset_id
            || self.known_market_for_token(requested_asset_id) != Some(response.market)
        {
            self.book_constraints_by_token.remove(&requested_asset_id);
            return self.reject_market_event(state, requested_asset_id, now_ms);
        }
        if !self.book_authority_missing(requested_asset_id) {
            return false;
        }
        let tick_size = response.tick_size;
        if TickSize::try_from(tick_size.as_decimal()).is_err() {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        }
        let vendor_timestamp_ms = response.timestamp.timestamp_millis();
        let Ok(vendor_timestamp_ms) = u64::try_from(vendor_timestamp_ms) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        if vendor_timestamp_ms == 0 {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        }
        let retained_watermark = self
            .token_book_state
            .get(&requested_asset_id)
            .and_then(|status| status.tick_authority_timestamp_ms);
        if retained_watermark.is_some_and(|watermark| vendor_timestamp_ms < watermark) {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        }
        let Some(min_order_size) = decimal_to_f64(response.min_order_size) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        if min_order_size <= 0.0 {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        }
        let Some(mut bids) = rest_levels(&response.bids, tick_size) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        let Some(mut asks) = rest_levels(&response.asks, tick_size) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        bids.sort_by(|left, right| right.price.total_cmp(&left.price));
        asks.sort_by(|left, right| left.price.total_cmp(&right.price));
        if !strict_ladder_is_valid(
            bids.iter().map(|level| (level.price, level.size)),
            asks.iter().map(|level| (level.price, level.size)),
        ) {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        }
        let Some(tick_size_f64) = decimal_to_f64(tick_size.as_decimal()) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        let Ok(timestamp) = i64::try_from(vendor_timestamp_ms) else {
            return self.sync_needs_snapshot(state, requested_asset_id, now_ms);
        };
        let frame = PolymarketBookFrame {
            asset_id: requested_asset_id,
            bids,
            asks,
            timestamp,
        };
        let constraints = BookConstraints {
            tick_size: tick_size_f64,
            min_order_size,
        };
        self.book_constraints_by_token
            .insert(requested_asset_id, constraints);
        self.mark_needs_snapshot(requested_asset_id);
        if let Some(status) = self.token_book_state.get_mut(&requested_asset_id) {
            status.tick_authority_timestamp_ms =
                Some(retained_watermark.map_or(vendor_timestamp_ms, |watermark| {
                    watermark.max(vendor_timestamp_ms)
                }));
        }
        let mut changed = apply_book_to_state(&mut self.state, &frame, constraints, now_ms);
        self.refresh_depth_revision(requested_asset_id, true);
        if let Some(top) = self.state.books.get(&requested_asset_id).cloned() {
            self.state.retain_validated_canonical_bid(&top);
        }
        changed |= self.set_active_book(requested_asset_id);
        changed |= self.project_canonical_book(state, requested_asset_id);
        changed |= self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_needs_snapshot(
        &mut self,
        state: &mut PolymarketOddsState,
        token_id: U256,
        now_ms: u64,
    ) -> bool {
        self.mark_needs_snapshot(token_id);
        self.recompute_lifecycle_status();
        let mut changed = remove_book_from_state(&mut self.state, token_id, now_ms);
        changed |= remove_book_from_state(state, token_id, now_ms);
        changed |= self.project_lifecycle_state(state);
        if changed {
            self.refresh_trigger_metadata(state);
        }
        changed
    }

    pub fn sync_price_change(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketPriceChangeFrame,
        now_ms: u64,
    ) -> bool {
        self.sync_price_change_with_outcome(state, market, frame, now_ms)
            .changed
    }

    pub(crate) fn sync_price_change_with_outcome(
        &mut self,
        state: &mut PolymarketOddsState,
        market: B256,
        frame: &PolymarketPriceChangeFrame,
        now_ms: u64,
    ) -> PriceChangeApplyOutcome {
        let mismatched_assets = frame
            .price_changes
            .iter()
            .map(|entry| entry.asset_id)
            .filter(|asset_id| self.known_market_for_token(*asset_id) != Some(market))
            .collect::<Vec<_>>();
        if !mismatched_assets.is_empty() {
            let mut outcome = PriceChangeApplyOutcome::default();
            for asset_id in mismatched_assets {
                outcome.changed |= self.reject_market_event(state, asset_id, now_ms);
                outcome.invalid_assets.push(asset_id);
            }
            return outcome;
        }
        let (mut outcome, committed_assets) = self.apply_price_change(frame, now_ms);
        for asset_id in outcome.invalid_assets.clone() {
            outcome.changed |= self.sync_needs_snapshot(state, asset_id, now_ms);
        }
        for asset_id in committed_assets {
            outcome.changed |= self.project_canonical_book(state, asset_id);
        }
        outcome.changed |= self.project_lifecycle_state(state);
        if outcome.changed {
            self.refresh_trigger_metadata(state);
        }
        outcome
    }

    fn apply_price_change(
        &mut self,
        frame: &PolymarketPriceChangeFrame,
        now_ms: u64,
    ) -> (PriceChangeApplyOutcome, Vec<U256>) {
        let ts_ms = normalize_ws_ts_ms(frame.timestamp);
        let mut outcome = PriceChangeApplyOutcome::default();
        let mut committed_assets = Vec::new();

        let single_asset = frame.price_changes.first().and_then(|first| {
            frame
                .price_changes
                .iter()
                .all(|entry| entry.asset_id == first.asset_id)
                .then_some(first.asset_id)
        });
        if let Some(asset_id) = single_asset {
            if self.apply_price_change_group(
                asset_id,
                frame.price_changes.iter(),
                ts_ms,
                now_ms,
                &mut outcome,
            ) {
                committed_assets.push(asset_id);
            }
        } else {
            for (asset_id, entries) in group_price_changes(frame) {
                if self.apply_price_change_group(
                    asset_id,
                    entries.iter().copied(),
                    ts_ms,
                    now_ms,
                    &mut outcome,
                ) {
                    committed_assets.push(asset_id);
                }
            }
        }

        if !committed_assets.is_empty() {
            outcome.changed |= self.state.updated_at_ms != now_ms;
            self.state.updated_at_ms = now_ms;
        }
        (outcome, committed_assets)
    }

    fn apply_price_change_group<'a>(
        &mut self,
        asset_id: U256,
        entries: impl Iterator<Item = &'a PolymarketPriceChangeEntryFrame> + Clone,
        ts_ms: u64,
        now_ms: u64,
        outcome: &mut PriceChangeApplyOutcome,
    ) -> bool {
        let Some(constraints) = self.book_constraints_by_token.get(&asset_id).copied() else {
            outcome.invalid_assets.push(asset_id);
            return false;
        };
        let Some(previous) = self.state.depths.get(&asset_id) else {
            outcome.invalid_assets.push(asset_id);
            return false;
        };
        if ts_ms > 0 && ts_ms < previous.ts_ms {
            return false;
        }
        let mut candidate = previous.clone();
        let previous_revision = candidate.revision;
        let Some(tick_size) = polymarket_tick_size_from_f64(constraints.tick_size) else {
            outcome.invalid_assets.push(asset_id);
            return false;
        };
        if ts_ms == 0
            || self
                .token_book_state
                .get(&asset_id)
                .and_then(|status| status.tick_authority_timestamp_ms)
                .is_some_and(|watermark| ts_ms < watermark)
            || entries
                .clone()
                .any(|entry| !ws_price_conforms_to_tick(entry.price, tick_size))
            || !apply_entries(&mut candidate, entries.clone())
        {
            outcome.invalid_assets.push(asset_id);
            return false;
        }
        let crossed = matches!(
            (candidate.bids.first(), candidate.asks.first()),
            (Some(bid), Some(ask)) if bid.price > ask.price
        );
        if (crossed && !reconcile_crossed_candidate(&mut candidate, &previous, entries, tick_size))
            || !strict_ladder_is_valid(
                candidate.bids.iter().map(|level| (level.price, level.size)),
                candidate.asks.iter().map(|level| (level.price, level.size)),
            )
        {
            outcome.invalid_assets.push(asset_id);
            return false;
        }
        let ladder_changed = self.state.depths.get(&asset_id).is_some_and(|previous| {
            previous.bids != candidate.bids || previous.asks != candidate.asks
        });
        let revision = self.depth_revision_by_token.entry(asset_id).or_default();
        if ladder_changed {
            *revision = revision.saturating_add(1).max(1);
        }
        candidate.revision = (*revision).max(previous_revision).max(1);
        candidate.ts_ms = ts_ms;
        candidate.observed_at_ms = now_ms;
        let top = BookTop {
            token_id: asset_id,
            bid: candidate.bids.first().map(|level| level.price),
            ask: candidate.asks.first().map(|level| level.price),
            bid_sz: candidate.bids.first().map(|level| level.size),
            ask_sz: candidate.asks.first().map(|level| level.size),
            tick_size: constraints.tick_size,
            min_order_size: constraints.min_order_size,
            ts_ms,
            observed_at_ms: now_ms,
        };
        if self.state.depths.get(&asset_id) != Some(&candidate) {
            self.state.depths.insert(asset_id, candidate);
            outcome.changed = true;
        }
        if self.state.books.get(&asset_id) != Some(&top) {
            self.state.books.insert(asset_id, top.clone());
            outcome.changed = true;
        }
        self.state.retain_validated_canonical_bid(&top);
        outcome.changed |= update_selection_books(&mut self.state, &top);
        true
    }

    pub(super) fn project_canonical_book(
        &self,
        state: &mut PolymarketOddsState,
        asset_id: U256,
    ) -> bool {
        let (Some(depth), Some(top)) = (
            self.state.depths.get(&asset_id),
            self.state.books.get(&asset_id),
        ) else {
            return false;
        };
        let mut changed = false;
        if state.depths.get(&asset_id) != Some(depth) {
            state.depths.insert(asset_id, depth.clone());
            changed = true;
        }
        if state.books.get(&asset_id) != Some(top) {
            state.books.insert(asset_id, top.clone());
            changed = true;
        }
        changed |= update_selection_books(state, top);
        if state.updated_at_ms != self.state.updated_at_ms {
            state.updated_at_ms = self.state.updated_at_ms;
            changed = true;
        }
        changed
    }

    fn refresh_depth_revision(&mut self, token_id: U256, ladder_changed: bool) {
        let Some(current) = self.state.depths.get_mut(&token_id) else {
            return;
        };
        let revision = self.depth_revision_by_token.entry(token_id).or_default();
        if ladder_changed {
            *revision = revision.saturating_add(1).max(1);
        }
        current.revision = (*revision).max(1);
    }
}

fn group_price_changes(
    frame: &PolymarketPriceChangeFrame,
) -> Vec<(U256, Vec<&PolymarketPriceChangeEntryFrame>)> {
    let mut grouped: Vec<(U256, Vec<&PolymarketPriceChangeEntryFrame>)> = Vec::new();
    for entry in &frame.price_changes {
        if let Some((_, entries)) = grouped
            .iter_mut()
            .find(|(asset_id, _)| *asset_id == entry.asset_id)
        {
            entries.push(entry);
        } else {
            grouped.push((entry.asset_id, vec![entry]));
        }
    }
    grouped
}

fn apply_entries<'a>(
    candidate: &mut BookDepth,
    entries: impl Iterator<Item = &'a PolymarketPriceChangeEntryFrame>,
) -> bool {
    let mut saw_entry = false;
    let mut bids_need_sort = false;
    let mut asks_need_sort = false;
    for entry in entries {
        saw_entry = true;
        let Some(size) = entry.size else {
            return false;
        };
        if !entry.price.is_finite()
            || entry.price <= 0.0
            || entry.price > 1.0
            || !size.is_finite()
            || size < 0.0
        {
            return false;
        }
        let levels = match entry.side {
            TradeSide::Buy => &mut candidate.bids,
            TradeSide::Sell => &mut candidate.asks,
        };
        if size == 0.0 {
            levels.retain(|level| !same_price(level.price, entry.price));
        } else if let Some(level) = levels
            .iter_mut()
            .find(|level| same_price(level.price, entry.price))
        {
            level.size = size;
        } else {
            levels.push(PriceLevel {
                price: entry.price,
                size,
            });
            match entry.side {
                TradeSide::Buy => bids_need_sort = true,
                TradeSide::Sell => asks_need_sort = true,
            }
        }
    }
    if bids_need_sort {
        candidate
            .bids
            .sort_by(|left, right| right.price.total_cmp(&left.price));
    }
    if asks_need_sort {
        candidate
            .asks
            .sort_by(|left, right| left.price.total_cmp(&right.price));
    }
    saw_entry
}

fn reconcile_crossed_candidate<'a>(
    candidate: &mut BookDepth,
    previous: &BookDepth,
    entries: impl Iterator<Item = &'a PolymarketPriceChangeEntryFrame> + Clone,
    tick_size: TickSize,
) -> bool {
    let Some(final_entry) = entries.clone().last() else {
        return false;
    };
    let (Some(best_bid), Some(best_ask)) = (final_entry.best_bid, final_entry.best_ask) else {
        return false;
    };
    if !post_change_tops_are_valid(best_bid, best_ask, tick_size) {
        return false;
    }

    let empty_bid = best_bid == 0.0
        && !candidate
            .bids
            .iter()
            .any(|level| same_price(level.price, best_bid));
    let empty_ask = best_ask == 1.0
        && !candidate
            .asks
            .iter()
            .any(|level| same_price(level.price, best_ask));
    if !remove_contradicted_levels(
        &mut candidate.bids,
        &previous.bids,
        entries.clone(),
        TradeSide::Buy,
        |price| empty_bid || (price > best_bid && !same_price(price, best_bid)),
    ) || !remove_contradicted_levels(
        &mut candidate.asks,
        &previous.asks,
        entries,
        TradeSide::Sell,
        |price| empty_ask || (price < best_ask && !same_price(price, best_ask)),
    ) {
        return false;
    }

    top_agrees(&candidate.bids, (!empty_bid).then_some(best_bid))
        && top_agrees(&candidate.asks, (!empty_ask).then_some(best_ask))
}

fn post_change_tops_are_valid(best_bid: f64, best_ask: f64, tick_size: TickSize) -> bool {
    best_bid.is_finite()
        && best_ask.is_finite()
        && (0.0..=1.0).contains(&best_bid)
        && best_ask > 0.0
        && best_ask <= 1.0
        && best_bid <= best_ask
        && (best_bid == 0.0 || ws_price_conforms_to_tick(best_bid, tick_size))
        && (best_ask == 1.0 || ws_price_conforms_to_tick(best_ask, tick_size))
}

fn remove_contradicted_levels<'a>(
    levels: &mut Vec<PriceLevel>,
    previous_levels: &[PriceLevel],
    entries: impl Iterator<Item = &'a PolymarketPriceChangeEntryFrame> + Clone,
    side: TradeSide,
    contradicted: impl Fn(f64) -> bool,
) -> bool {
    let mut conflict = false;
    levels.retain(|level| {
        if !contradicted(level.price) {
            return true;
        }
        let retained = previous_levels
            .iter()
            .any(|previous| same_price(previous.price, level.price));
        let explicitly_live = entries
            .clone()
            .filter(|entry| entry.side == side && same_price(entry.price, level.price))
            .last()
            .and_then(|entry| entry.size)
            .is_some_and(|size| size > 0.0);
        if !retained || explicitly_live {
            conflict = true;
            true
        } else {
            false
        }
    });
    !conflict
}

fn top_agrees(levels: &[PriceLevel], expected: Option<f64>) -> bool {
    match (levels.first(), expected) {
        (Some(level), Some(expected)) => same_price(level.price, expected),
        (None, None) => true,
        _ => false,
    }
}

fn same_price(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12
}

fn rest_levels(
    levels: &[OrderSummary],
    tick_size: TickSize,
) -> Option<Vec<PolymarketBookLevelFrame>> {
    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        if !decimal_price_conforms_to_tick(level.price, tick_size) {
            return None;
        }
        let price = decimal_to_f64(level.price)?;
        let size = decimal_to_f64(level.size)?;
        if size <= 0.0 {
            return None;
        }
        out.push(PolymarketBookLevelFrame { price, size });
    }
    Some(out)
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    value
        .normalize()
        .to_string()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

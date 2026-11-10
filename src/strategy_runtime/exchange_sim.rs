use std::collections::{HashMap, HashSet};

use polymarket_client_sdk::types::U256;

use crate::execution::SimSubmittedOrder;
use crate::orders::OrderUpdateEvent;
use crate::polymarket::identity::MarketId;
use crate::polymarket::state::{BookDepth, BookTop, PriceLevel, TradeSide};
use crate::trade_session_ledger::ManagedSellReceiptIdentity;
use crate::types::{Side, TimeInForce};

mod captured_ladder;
mod execution_events;
mod fill_event;
mod ingest;
mod matching;
mod resting_orders;

pub use execution_events::{
    CapturedVenueFact, ExchangeSimExecutionStats, VenueDiagnostic, VenueDiagnosticKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VenueSubmissionConsequence {
    Accepted,
    Rejected,
}

const FIXED_SCALE: u128 = crate::polymarket::fixed_math::FIXED_6DP_SCALE;
const DOCUMENTED_CRYPTO_TAKER_FEE_BPS: u16 = 700;

/// Captured external venue matcher.
///
/// Admission, order identity, lifecycle, inventory, portfolio, accounting, and
/// settlement remain central-runtime concerns. This owner keeps only captured
/// venue state and emits immutable venue consequences.
pub struct ExchangeSim {
    replay_books: HashMap<U256, BookTop>,
    replay_depths: HashMap<U256, BookDepth>,
    replay_book_receipt_identities: HashMap<U256, ManagedSellReceiptIdentity>,
    replay_depth_receipt_identities: HashMap<U256, ManagedSellReceiptIdentity>,
    seen_order_ids: HashSet<String>,
    consumed_ladder_units: HashMap<(U256, LadderSource, Side, u128), u128>,
    invalidated_ladders: HashSet<(U256, LadderSource, Side)>,
    resting_orders: HashMap<String, VenueOrder>,
    resting_queues: HashMap<String, RestingQueueState>,
    facts: Vec<CapturedVenueFact>,
    diagnostics: Vec<VenueDiagnostic>,
    execution_stats: ExchangeSimExecutionStats,
}

#[derive(Debug, Clone)]
struct VenueOrder {
    order_id: String,
    market_id: MarketId,
    token: String,
    token_id: U256,
    side: Side,
    time_in_force: TimeInForce,
    px_units_6dp: u128,
    original_size_units_6dp: u128,
    matched_size_units_6dp: u128,
    placed_at_ms: u64,
    post_only: bool,
    quote_group: Option<String>,
    lifecycle_cohort: Option<String>,
    execution_intent_id: Option<u64>,
}

impl VenueOrder {
    fn from_submission(submitted: SimSubmittedOrder, ts_ms: u64) -> Result<Self, &'static str> {
        let order_id = submitted.order_id.trim();
        if order_id.is_empty() {
            return Err("empty_order_id");
        }
        let intent = submitted.intent;
        let market_id = intent.market_id.clone().ok_or("missing_market_id")?;
        if market_id.as_str().trim().is_empty() {
            return Err("empty_market_id");
        }
        let token = intent.instrument.trim();
        let token_id = token.parse::<U256>().map_err(|_| "invalid_token")?;
        if token_id == U256::ZERO || token_id.to_string() != token {
            return Err("invalid_token");
        }
        let original_size_units_6dp =
            fixed_units_6dp(intent.qty, false).ok_or("quantity_not_exact_fixed_6dp")?;
        let px_units_6dp = fixed_units_6dp(intent.limit_px, false)
            .filter(|units| *units < FIXED_SCALE)
            .ok_or("price_not_exact_fixed_6dp")?;
        if px_units_6dp >= FIXED_SCALE {
            return Err("invalid_price");
        }
        Ok(Self {
            order_id: order_id.to_string(),
            market_id,
            token: token.to_string(),
            token_id,
            side: intent.side(),
            time_in_force: intent.time_in_force.clone(),
            px_units_6dp,
            original_size_units_6dp,
            matched_size_units_6dp: 0,
            placed_at_ms: ts_ms,
            post_only: intent.post_only,
            quote_group: intent.quote_group.clone(),
            lifecycle_cohort: intent.lifecycle_cohort.clone(),
            execution_intent_id: intent.execution_intent_id,
        })
    }

    fn remaining_units_6dp(&self) -> u128 {
        self.original_size_units_6dp
            .saturating_sub(self.matched_size_units_6dp)
    }

    fn px(&self) -> f64 {
        units_6dp_to_f64(self.px_units_6dp)
    }

    fn original_size(&self) -> f64 {
        units_6dp_to_f64(self.original_size_units_6dp)
    }

    fn matched_size(&self) -> f64 {
        units_6dp_to_f64(self.matched_size_units_6dp)
    }
}

#[derive(Debug, Clone, Copy)]
struct RestingQueueState {
    queue_ahead_units_6dp: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LadderSource {
    L1,
    L2,
}

#[derive(Debug, Clone, Copy)]
struct MatchedLevel {
    source: Option<LadderSource>,
    price_units_6dp: u128,
    qty_units_6dp: u128,
}

#[derive(Debug, Clone)]
struct SimFill {
    qty_units_6dp: u128,
    px_units_6dp: u128,
    book_age_ms: u64,
    order_rest_ms: u64,
    depth_source: &'static str,
    available_qty: f64,
    levels_crossed: u16,
    matched_levels: Vec<MatchedLevel>,
    gross_units_6dp: u128,
    fee_units_6dp: Option<u128>,
    receipt_identity: Option<ManagedSellReceiptIdentity>,
}

type SimFillResult = Result<SimFill, &'static str>;

impl Default for ExchangeSim {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeSim {
    pub fn new() -> Self {
        Self {
            replay_books: HashMap::new(),
            replay_depths: HashMap::new(),
            replay_book_receipt_identities: HashMap::new(),
            replay_depth_receipt_identities: HashMap::new(),
            seen_order_ids: HashSet::new(),
            consumed_ladder_units: HashMap::new(),
            invalidated_ladders: HashSet::new(),
            resting_orders: HashMap::new(),
            resting_queues: HashMap::new(),
            facts: Vec::new(),
            diagnostics: Vec::new(),
            execution_stats: ExchangeSimExecutionStats::default(),
        }
    }

    pub fn execution_stats(&self) -> &ExchangeSimExecutionStats {
        &self.execution_stats
    }

    pub fn ingest_replay_book(
        &mut self,
        book: BookTop,
        receipt_identity: ManagedSellReceiptIdentity,
    ) {
        let current = self.replay_books.get(&book.token_id);
        if current != Some(&book) {
            retain_unchanged_l1_consumption(
                &mut self.consumed_ladder_units,
                book.token_id,
                current,
                &book,
            );
            self.revise_ladder_source(book.token_id, LadderSource::L1, false);
        }
        self.replay_book_receipt_identities
            .insert(book.token_id, receipt_identity);
        self.replay_books.insert(book.token_id, book);
    }

    pub fn ingest_replay_depth(
        &mut self,
        depth: BookDepth,
        receipt_identity: ManagedSellReceiptIdentity,
    ) {
        let current = self.replay_depths.get(&depth.token_id);
        if current != Some(&depth) {
            retain_unchanged_l2_consumption(
                &mut self.consumed_ladder_units,
                depth.token_id,
                current,
                &depth,
            );
            self.revise_ladder_source(depth.token_id, LadderSource::L2, false);
        }
        self.replay_depth_receipt_identities
            .insert(depth.token_id, receipt_identity);
        self.replay_depths.insert(depth.token_id, depth);
    }

    pub fn remove_replay_book(&mut self, token_id: U256) {
        let removed_book = self.replay_books.remove(&token_id).is_some();
        let removed_depth = self.replay_depths.remove(&token_id).is_some();
        self.replay_book_receipt_identities.remove(&token_id);
        self.replay_depth_receipt_identities.remove(&token_id);
        if removed_book || removed_depth {
            self.revise_ladder_source(token_id, LadderSource::L1, removed_book);
            self.revise_ladder_source(token_id, LadderSource::L2, removed_depth);
        }
    }

    pub fn remove_replay_depth(&mut self, token_id: U256) {
        let removed = self.replay_depths.remove(&token_id).is_some();
        self.replay_depth_receipt_identities.remove(&token_id);
        if removed {
            self.revise_ladder_source(token_id, LadderSource::L2, true);
        }
    }

    pub fn process_captured_last_trade(
        &mut self,
        token_id: U256,
        price: f64,
        size: f64,
        side: Option<TradeSide>,
        ts_ms: u64,
        receipt_identity: ManagedSellReceiptIdentity,
    ) {
        resting_orders::process_captured_last_trade(
            self,
            token_id,
            price,
            size,
            side,
            ts_ms,
            receipt_identity,
        );
    }

    pub fn advance_time(&mut self, ts_ms: u64) {
        resting_orders::expire_orders(self, ts_ms);
    }

    pub fn drain_facts(&mut self) -> Vec<CapturedVenueFact> {
        self.facts.drain(..).collect()
    }

    pub fn drain_diagnostics(&mut self) -> Vec<VenueDiagnostic> {
        self.diagnostics.drain(..).collect()
    }

    fn book_for(&self, token_id: U256) -> Option<&BookTop> {
        self.replay_books.get(&token_id)
    }

    fn depth_for(&self, token_id: U256) -> Option<&BookDepth> {
        self.replay_depths.get(&token_id)
    }

    fn ladder_receipt_identity(
        &self,
        token_id: U256,
        fill: &SimFill,
    ) -> Result<ManagedSellReceiptIdentity, &'static str> {
        let source = fill
            .matched_levels
            .first()
            .and_then(|level| level.source)
            .ok_or("captured_ladder_fill_missing_source")?;
        if fill
            .matched_levels
            .iter()
            .any(|level| level.source != Some(source))
        {
            return Err("captured_ladder_fill_mixed_sources");
        }
        match source {
            LadderSource::L1 => self.replay_book_receipt_identities.get(&token_id),
            LadderSource::L2 => self.replay_depth_receipt_identities.get(&token_id),
        }
        .cloned()
        .ok_or("captured_ladder_receipt_identity_unavailable")
    }

    fn validated_book_state(
        &self,
        token_id: U256,
        action_ts_ms: u64,
    ) -> Result<(&BookTop, Option<&BookDepth>), &'static str> {
        let book = self.book_for(token_id).ok_or("missing_book")?;
        validate_observation_time(book.observed_at_ms, action_ts_ms, "book")?;
        if let Some(bid) = book.bid {
            validate_price(bid)?;
        }
        if let Some(ask) = book.ask {
            validate_price(ask)?;
        }
        if let Some(size) = book.bid_sz {
            fixed_units_6dp(size, true).ok_or("book_size_not_exact_fixed_6dp")?;
        }
        if let Some(size) = book.ask_sz {
            fixed_units_6dp(size, true).ok_or("book_size_not_exact_fixed_6dp")?;
        }
        let depth = self.depth_for(token_id);
        if let Some(depth) = depth {
            validate_observation_time(depth.observed_at_ms, action_ts_ms, "depth")?;
            for level in depth.bids.iter().chain(&depth.asks) {
                validate_price(level.price)?;
                fixed_units_6dp(level.size, true).ok_or("depth_size_not_exact_fixed_6dp")?;
            }
        }
        Ok((book, depth))
    }

    fn revise_ladder_source(
        &mut self,
        token_id: U256,
        source: LadderSource,
        liquidity_changed: bool,
    ) {
        if liquidity_changed {
            self.consumed_ladder_units
                .retain(|(consumed_token, consumed_source, _, _), _| {
                    *consumed_token != token_id || *consumed_source != source
                });
        }
        self.invalidated_ladders
            .remove(&(token_id, source, Side::Buy));
        self.invalidated_ladders
            .remove(&(token_id, source, Side::Sell));
    }

    fn consumed_at_level(
        &self,
        token_id: U256,
        source: LadderSource,
        side: Side,
        price_units_6dp: u128,
    ) -> u128 {
        self.consumed_ladder_units
            .get(&(token_id, source, side, price_units_6dp))
            .copied()
            .unwrap_or(0)
    }

    fn ladder_invalidated(&self, token_id: U256, source: LadderSource, side: Side) -> bool {
        self.invalidated_ladders.contains(&(token_id, source, side))
    }

    fn invalidate_trade_ladder(&mut self, token_id: U256, trade_side: TradeSide) {
        let side = match trade_side {
            TradeSide::Buy => Side::Buy,
            TradeSide::Sell => Side::Sell,
        };
        self.invalidated_ladders
            .insert((token_id, LadderSource::L1, side));
        self.invalidated_ladders
            .insert((token_id, LadderSource::L2, side));
    }

    fn consume_ladder(&mut self, token_id: U256, side: Side, fill: &SimFill) -> Option<()> {
        let mut next = Vec::with_capacity(fill.matched_levels.len());
        for level in &fill.matched_levels {
            let Some(source) = level.source else {
                continue;
            };
            let key = (token_id, source, side, level.price_units_6dp);
            let current = self.consumed_ladder_units.get(&key).copied().unwrap_or(0);
            next.push((key, current.checked_add(level.qty_units_6dp)?));
        }
        for (key, value) in next {
            self.consumed_ladder_units.insert(key, value);
        }
        Some(())
    }

    fn record_diagnostic(
        &mut self,
        ts_ms: u64,
        order: Option<&VenueOrder>,
        order_id: &str,
        kind: VenueDiagnosticKind,
        reason: &'static str,
    ) {
        self.diagnostics.push(VenueDiagnostic {
            ts_ms,
            order_id: order_id.to_string(),
            market_id: order.map(|order| order.market_id.to_string()),
            token: order.map(|order| order.token.clone()),
            kind,
            reason,
            book_age_ms: None,
            order_rest_ms: None,
            depth_source: None,
            available_qty: None,
            levels_crossed: None,
            gross_units_6dp: None,
            fee_units_6dp: None,
        });
    }

    fn push_order_update(
        &mut self,
        order: &VenueOrder,
        ts_ms: u64,
        status: &'static str,
        msg_type: &'static str,
        terminal_reason: Option<&'static str>,
    ) {
        self.facts
            .push(CapturedVenueFact::OrderUpdate(OrderUpdateEvent {
                ts_ms,
                order_id: order.order_id.clone(),
                market: order.market_id.to_string(),
                token: order.token.clone(),
                side: order.side,
                px: order.px(),
                original_size: Some(order.original_size()),
                matched_size: Some(order.matched_size()),
                status: Some(status.to_string()),
                msg_type: Some(msg_type.to_string()),
                terminal_reason: terminal_reason.map(str::to_string),
                terminal_reason_provenance: terminal_reason
                    .map(|_| "captured_venue_simulation".to_string()),
            }));
    }

    fn push_rejected_update(&mut self, order: &VenueOrder, ts_ms: u64, reason: &'static str) {
        self.execution_stats.note_rejected(&order.market_id);
        self.push_order_update(order, ts_ms, "REJECTED", "REJECTION", Some(reason));
    }

    fn is_taker(order: &VenueOrder, fill: &SimFill) -> bool {
        order.time_in_force.is_immediate() || (!order.post_only && fill.order_rest_ms == 0)
    }

    fn finalize_fill_economics(
        &self,
        order: &VenueOrder,
        fill: &mut SimFill,
    ) -> Result<(), &'static str> {
        if !Self::is_taker(order, fill) {
            fill.fee_units_6dp = Some(0);
            return Ok(());
        }
        let fee_units_6dp = fill
            .matched_levels
            .iter()
            .try_fold(0u128, |total, level| {
                total.checked_add(exact_level_fee_units_6dp(
                    level.qty_units_6dp,
                    level.price_units_6dp,
                    DOCUMENTED_CRYPTO_TAKER_FEE_BPS,
                )?)
            })
            .ok_or("fill_fee_overflow")?;
        fill.fee_units_6dp = Some(fee_units_6dp);
        Ok(())
    }
}

fn retain_unchanged_l1_consumption(
    consumed_ladder_units: &mut HashMap<(U256, LadderSource, Side, u128), u128>,
    token_id: U256,
    current: Option<&BookTop>,
    incoming: &BookTop,
) {
    consumed_ladder_units.retain(
        |(consumed_token, consumed_source, side, price_units_6dp), _| {
            *consumed_token != token_id
                || *consumed_source != LadderSource::L1
                || l1_level_unchanged(current, incoming, *side, *price_units_6dp)
        },
    );
}

fn retain_unchanged_l2_consumption(
    consumed_ladder_units: &mut HashMap<(U256, LadderSource, Side, u128), u128>,
    token_id: U256,
    current: Option<&BookDepth>,
    incoming: &BookDepth,
) {
    consumed_ladder_units.retain(
        |(consumed_token, consumed_source, side, price_units_6dp), _| {
            *consumed_token != token_id
                || *consumed_source != LadderSource::L2
                || l2_level_unchanged(current, incoming, *side, *price_units_6dp)
        },
    );
}

fn l1_level_unchanged(
    current: Option<&BookTop>,
    incoming: &BookTop,
    side: Side,
    price_units_6dp: u128,
) -> bool {
    let Some(current) = current else {
        return false;
    };
    let (current_price, current_size, incoming_price, incoming_size) = match side {
        Side::Buy => (current.ask, current.ask_sz, incoming.ask, incoming.ask_sz),
        Side::Sell => (current.bid, current.bid_sz, incoming.bid, incoming.bid_sz),
    };
    fixed_level_unchanged(
        current_price,
        current_size,
        incoming_price,
        incoming_size,
        price_units_6dp,
    )
}

fn fixed_level_unchanged(
    current_price: Option<f64>,
    current_size: Option<f64>,
    incoming_price: Option<f64>,
    incoming_size: Option<f64>,
    price_units_6dp: u128,
) -> bool {
    fixed_units_6dp(current_price.unwrap_or(f64::NAN), false) == Some(price_units_6dp)
        && fixed_units_6dp(incoming_price.unwrap_or(f64::NAN), false) == Some(price_units_6dp)
        && current_size.and_then(|size| fixed_units_6dp(size, true))
            == incoming_size.and_then(|size| fixed_units_6dp(size, true))
}

fn l2_level_unchanged(
    current: Option<&BookDepth>,
    incoming: &BookDepth,
    side: Side,
    price_units_6dp: u128,
) -> bool {
    let Some(current) = current else {
        return false;
    };
    let (current_levels, incoming_levels) = match side {
        Side::Buy => (&current.asks, &incoming.asks),
        Side::Sell => (&current.bids, &incoming.bids),
    };
    let size_at_price = |levels: &[PriceLevel]| {
        levels.iter().find_map(|level| {
            (fixed_units_6dp(level.price, false) == Some(price_units_6dp))
                .then(|| fixed_units_6dp(level.size, true))
                .flatten()
        })
    };
    let current_size = size_at_price(current_levels);
    current_size.is_some() && current_size == size_at_price(incoming_levels)
}

fn fixed_units_6dp(value: f64, allow_zero: bool) -> Option<u128> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let scaled = value * FIXED_SCALE as f64;
    let rounded = scaled.round();
    if rounded > u128::MAX as f64 || (!allow_zero && rounded < 1.0) {
        return None;
    }
    ((scaled - rounded).abs() <= 0.000_001).then_some(rounded as u128)
}

fn units_6dp_to_f64(units: u128) -> f64 {
    units as f64 / FIXED_SCALE as f64
}

fn checked_round_div(numerator: u128, denominator: u128) -> Option<u128> {
    (denominator > 0)
        .then(|| {
            numerator
                .checked_add(denominator / 2)?
                .checked_div(denominator)
        })
        .flatten()
}

fn exact_level_gross_units_6dp(qty_units_6dp: u128, price_units_6dp: u128) -> Option<u128> {
    checked_round_div(qty_units_6dp.checked_mul(price_units_6dp)?, FIXED_SCALE)
}

fn exact_level_fee_units_6dp(
    qty_units_6dp: u128,
    price_units_6dp: u128,
    fee_bps: u16,
) -> Option<u128> {
    let complement = FIXED_SCALE.checked_sub(price_units_6dp)?;
    let numerator = qty_units_6dp
        .checked_mul(price_units_6dp)?
        .checked_mul(complement)?
        .checked_mul(u128::from(fee_bps))?;
    let denominator = FIXED_SCALE.checked_mul(FIXED_SCALE)?.checked_mul(10_000)?;
    let fee_tens = checked_round_div(numerator, denominator.checked_mul(10)?)?;
    fee_tens.checked_mul(10)
}

fn validate_observation_time(
    observed_at_ms: u64,
    action_ts_ms: u64,
    source: &'static str,
) -> Result<(), &'static str> {
    if observed_at_ms == 0 {
        return Err(if source == "book" {
            "book_observation_time_zero"
        } else {
            "depth_observation_time_zero"
        });
    }
    if action_ts_ms.checked_sub(observed_at_ms).is_none() {
        return Err(if source == "book" {
            "book_observation_time_future"
        } else {
            "depth_observation_time_future"
        });
    }
    Ok(())
}

fn validate_price(value: f64) -> Result<u128, &'static str> {
    fixed_units_6dp(value, false)
        .filter(|units| *units < FIXED_SCALE)
        .ok_or("price_not_exact_fixed_6dp")
}

fn same_price(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && (left - right).abs() <= 1e-9
}

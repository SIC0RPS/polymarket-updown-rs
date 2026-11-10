use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use polymarket_client_sdk::types::U256;

use crate::capture::arrow::STRATEGY_CONSUMPTION_ARROW_FILE;
use crate::capture::events::StrategyConsumptionRecord;
use crate::polymarket::state::{
    BookDepth, BookTop, LastTrade, PolymarketLifecycleStatus, PolymarketOddsState, PriceLevel,
    TradeSide,
};

#[path = "prepared_arrow/apply.rs"]
mod apply;
#[path = "prepared_arrow/book_rows.rs"]
pub(super) mod book_rows;
#[path = "prepared_arrow/consumption.rs"]
mod consumption;
#[path = "prepared_arrow/io.rs"]
mod io;
#[path = "prepared_arrow/last_trade_io.rs"]
mod last_trade_io;
#[path = "prepared_arrow/mapped_ipc.rs"]
mod mapped_ipc;
#[path = "prepared_arrow/resolution.rs"]
mod resolution;
#[path = "prepared_arrow/validate.rs"]
mod validate;
pub(in crate::strategy_runtime::replay_mode) use apply::{
    apply_lifecycle_rows, apply_market_rows, refresh_projection_books, PreparedBookUpdate,
};
use book_rows::{BookRow, PreparedBookRows};
use consumption::PreparedConsumptionWriter;
pub(crate) use consumption::{PreparedConsumptionCursor, PreparedConsumptionSource};
pub(super) use io::read_prepared_replay_data;
use io::{
    book_batch, book_schema, clock_batch, clock_schema, last_trade_batch, last_trade_schema,
    lifecycle_batch, lifecycle_schema, market_batch, market_schema, open_writer, write_empty,
};
use last_trade_io::read_last_trade_rows;
use resolution::resolution_schema;
pub(in crate::strategy_runtime::replay_mode) use resolution::ResolutionRow;

const CLOCK_BATCH_SIZE: usize = 8192;
const MARKET_BATCH_SIZE: usize = 2048;
const BOOK_BATCH_SIZE: usize = 8192;
const LIFECYCLE_BATCH_SIZE: usize = 8192;
const RESOLUTION_BATCH_SIZE: usize = 256;
const LAST_TRADE_BATCH_SIZE: usize = 2048;
const STORED_DEPTH_LEVELS: usize = 5;
const ENGINE_CLOCK: &str = "engine_clock.arrow";
const MARKET_DELTA: &str = "market_delta.arrow";
const TOKEN_BOOK_DELTA: &str = "token_book_delta.arrow";
const LIFECYCLE_DELTA: &str = "lifecycle_delta.arrow";
const RESOLUTION_DELTA: &str = "resolution_delta.arrow";
const LAST_TRADE_DELTA: &str = "last_trade_delta.arrow";
const DICTIONARY: &str = "dictionary.arrow";
pub(super) const PREPARED_PAYLOAD_FILES: [&str; 7] = [
    ENGINE_CLOCK,
    MARKET_DELTA,
    TOKEN_BOOK_DELTA,
    STRATEGY_CONSUMPTION_ARROW_FILE,
    LIFECYCLE_DELTA,
    RESOLUTION_DELTA,
    LAST_TRADE_DELTA,
];

pub(super) struct PreparedWriteStats {
    pub engine_rows: u64,
    pub strategy_consumption_rows: u64,
    pub resolution_rows: u64,
    pub market_delta_rows: u64,
    pub token_book_delta_rows: u64,
    pub lifecycle_delta_rows: u64,
    pub resolution_delta_rows: u64,
    pub last_trade_delta_rows: u64,
    pub total_events: u64,
    pub start_ts_ms: Option<u64>,
    pub end_ts_ms: Option<u64>,
}

#[derive(Debug)]
pub(super) struct PreparedReplayData {
    pub clocks: Vec<ClockRow>,
    pub markets: Vec<MarketRow>,
    pub books: PreparedBookRows,
    pub consumptions: PreparedConsumptionSource,
    pub lifecycles: Vec<LifecycleRow>,
    pub resolutions: Vec<ResolutionRow>,
    pub last_trades: Vec<LastTradeRow>,
}

pub(super) fn slice<'a, T>(rows: &'a [T], start: i64, len: i64, name: &str) -> Result<&'a [T]> {
    if len <= 0 {
        return Ok(&[]);
    }
    anyhow::ensure!(start >= 0, "{name} negative start");
    let start = start as usize;
    let end = start.saturating_add(len as usize);
    rows.get(start..end)
        .ok_or_else(|| anyhow!("engine_clock references missing {name} rows"))
}

#[derive(Debug, Clone)]
pub(super) struct ClockRow {
    pub session_ts_ms: u64,
    pub source_priority: u8,
    pub sequence: u64,
    pub updated_at_ms: u64,
    pub revision: u64,
    pub market_start: i64,
    pub market_len: i64,
    pub book_start: i64,
    pub book_len: i64,
    pub lifecycle_start: i64,
    pub lifecycle_len: i64,
    pub consumption_start: i64,
    pub consumption_len: i64,
}

#[derive(Debug, Clone)]
pub(super) struct MarketRow {
    pub role: String,
    updated_at_ms: u64,
    revision: u64,
    selection_updated_at_ms: u64,
    next_selection_updated_at_ms: u64,
    pub market: bbwick_engine::state::MarketSelectionSnapshot,
}

#[derive(Debug, Clone)]
pub(super) struct LifecycleRow {
    kind: String,
    token_id: String,
    lifecycle: String,
    sync_state: String,
    sync_epoch: u64,
    rotation_epoch: u64,
    updated_at_ms: Option<u64>,
    rotation_started_at_ms: Option<u64>,
    last_error: Option<String>,
    last_error_at_ms: Option<u64>,
    active_tokens: usize,
    pending_revalidation_tokens: usize,
    pending_subscription_tokens: usize,
    invalid_after_404_tokens: usize,
    resolved_tokens: usize,
    quarantined_from_entry_tokens: usize,
    recent_book_404_total: u64,
    recent_book_404_tokens: String,
}

#[derive(Debug, Clone)]
pub(super) struct LastTradeRow {
    pub session_ts_ms: u64,
    pub seq: u64,
    pub token_id: U256,
    pub price: f64,
    pub size: f64,
    pub side: Option<TradeSide>,
    pub trade_ts_ms: u64,
}

#[derive(Default)]
struct PreviousState {
    markets: Vec<bbwick_engine::state::MarketSelectionSnapshot>,
    selection: Option<bbwick_engine::state::MarketSelectionSnapshot>,
    next_selection: Option<bbwick_engine::state::MarketSelectionSnapshot>,
    books: HashMap<U256, BookTop>,
    depths: HashMap<U256, BookDepth>,
    token_lifecycle: HashMap<U256, String>,
    lifecycle_status: PolymarketLifecycleStatus,
    last_trade_seq: u64,
}

pub(super) struct PreparedReplayWriter {
    clock_writer: FileWriter<File>,
    market_writer: FileWriter<File>,
    book_writer: FileWriter<File>,
    consumption_writer: PreparedConsumptionWriter,
    lifecycle_writer: FileWriter<File>,
    resolution_writer: FileWriter<File>,
    last_trade_writer: FileWriter<File>,
    clock_schema: SchemaRef,
    market_schema: SchemaRef,
    book_schema: SchemaRef,
    lifecycle_schema: SchemaRef,
    resolution_schema: SchemaRef,
    last_trade_schema: SchemaRef,
    clocks: Vec<ClockRow>,
    markets: Vec<MarketRow>,
    books: Vec<BookRow>,
    lifecycles: Vec<LifecycleRow>,
    resolutions: Vec<ResolutionRow>,
    last_trades: Vec<LastTradeRow>,
    stats: PreparedWriteStats,
    previous: PreviousState,
}

impl PreparedReplayWriter {
    pub(super) fn create(dir: &Path) -> Result<Self> {
        let clock_schema = clock_schema();
        let market_schema = market_schema();
        let book_schema = book_schema();
        let lifecycle_schema = lifecycle_schema();
        let resolution_schema = resolution_schema();
        let last_trade_schema = last_trade_schema();
        write_empty(&dir.join(DICTIONARY))?;
        Ok(Self {
            clock_writer: open_writer(&dir.join(ENGINE_CLOCK), &clock_schema)?,
            market_writer: open_writer(&dir.join(MARKET_DELTA), &market_schema)?,
            book_writer: open_writer(&dir.join(TOKEN_BOOK_DELTA), &book_schema)?,
            consumption_writer: PreparedConsumptionWriter::create(
                dir.join(STRATEGY_CONSUMPTION_ARROW_FILE),
            )?,
            lifecycle_writer: open_writer(&dir.join(LIFECYCLE_DELTA), &lifecycle_schema)?,
            resolution_writer: open_writer(&dir.join(RESOLUTION_DELTA), &resolution_schema)?,
            last_trade_writer: open_writer(&dir.join(LAST_TRADE_DELTA), &last_trade_schema)?,
            clock_schema,
            market_schema,
            book_schema,
            lifecycle_schema,
            resolution_schema,
            last_trade_schema,
            clocks: Vec::with_capacity(CLOCK_BATCH_SIZE),
            markets: Vec::with_capacity(MARKET_BATCH_SIZE),
            books: Vec::with_capacity(BOOK_BATCH_SIZE),
            lifecycles: Vec::with_capacity(LIFECYCLE_BATCH_SIZE),
            resolutions: Vec::with_capacity(RESOLUTION_BATCH_SIZE),
            last_trades: Vec::with_capacity(LAST_TRADE_BATCH_SIZE),
            stats: PreparedWriteStats {
                engine_rows: 0,
                strategy_consumption_rows: 0,
                resolution_rows: 0,
                market_delta_rows: 0,
                token_book_delta_rows: 0,
                lifecycle_delta_rows: 0,
                resolution_delta_rows: 0,
                last_trade_delta_rows: 0,
                total_events: 0,
                start_ts_ms: None,
                end_ts_ms: None,
            },
            previous: PreviousState::default(),
        })
    }

    pub(super) fn push_engine(
        &mut self,
        session_ts_ms: u64,
        payload: PolymarketOddsState,
    ) -> Result<()> {
        let market_start = self.stats.market_delta_rows as i64;
        let book_start = self.stats.token_book_delta_rows as i64;
        let lifecycle_start = self.stats.lifecycle_delta_rows as i64;

        if self.previous.markets != payload.markets
            || self.previous.selection != payload.selection
            || self.previous.next_selection != payload.next_selection
        {
            self.push_market_snapshot(&payload);
            self.previous.markets.clone_from(&payload.markets);
            self.previous.selection.clone_from(&payload.selection);
            self.previous
                .next_selection
                .clone_from(&payload.next_selection);
            self.previous.token_lifecycle.clear();
        }
        self.push_book_deltas(&payload);
        self.push_lifecycle_deltas(&payload);
        self.push_last_trade_deltas(session_ts_ms, &payload);

        let market_len = self.stats.market_delta_rows as i64 - market_start;
        let book_len = self.stats.token_book_delta_rows as i64 - book_start;
        let lifecycle_len = self.stats.lifecycle_delta_rows as i64 - lifecycle_start;
        self.stats.engine_rows = self.stats.engine_rows.saturating_add(1);
        if market_len == 0 && book_len == 0 && lifecycle_len == 0 {
            return self.flush_if_needed();
        }
        let clock = ClockRow {
            session_ts_ms,
            source_priority: 0,
            sequence: self
                .stats
                .total_events
                .checked_add(1)
                .context("prepared central event sequence overflow")?,
            updated_at_ms: payload.updated_at_ms,
            revision: payload.revision,
            market_start,
            market_len,
            book_start,
            book_len,
            lifecycle_start,
            lifecycle_len,
            consumption_start: -1,
            consumption_len: 0,
        };
        self.clocks.push(clock);
        self.record_event(session_ts_ms);
        self.flush_if_needed()
    }

    pub(super) fn push_strategy_consumption(
        &mut self,
        record: &StrategyConsumptionRecord,
    ) -> Result<()> {
        let consumption_start = self.stats.strategy_consumption_rows as i64;
        let session_ts_ms = record.consumed_at_ms;
        self.consumption_writer.write(record)?;
        self.clocks.push(ClockRow {
            session_ts_ms,
            source_priority: 1,
            sequence: record.consumption_sequence,
            updated_at_ms: 0,
            revision: 0,
            market_start: -1,
            market_len: 0,
            book_start: -1,
            book_len: 0,
            lifecycle_start: -1,
            lifecycle_len: 0,
            consumption_start,
            consumption_len: 1,
        });
        self.record_event(session_ts_ms);
        self.stats.strategy_consumption_rows =
            self.stats.strategy_consumption_rows.saturating_add(1);
        self.flush_if_needed()
    }

    pub(super) fn finish(mut self) -> Result<PreparedWriteStats> {
        self.flush_all()?;
        self.clock_writer.finish().context("finish engine_clock")?;
        self.market_writer.finish().context("finish market_delta")?;
        self.book_writer
            .finish()
            .context("finish token_book_delta")?;
        self.consumption_writer.finish()?;
        self.lifecycle_writer
            .finish()
            .context("finish lifecycle_delta")?;
        self.resolution_writer
            .finish()
            .context("finish resolution_delta")?;
        self.last_trade_writer
            .finish()
            .context("finish last_trade_delta")?;
        Ok(self.stats)
    }

    fn push_market_snapshot(&mut self, payload: &PolymarketOddsState) {
        for market in &payload.markets {
            self.markets.push(MarketRow {
                role: "market".to_string(),
                updated_at_ms: payload.updated_at_ms,
                revision: payload.revision,
                selection_updated_at_ms: payload.selection_updated_at_ms,
                next_selection_updated_at_ms: payload.next_selection_updated_at_ms,
                market: market.clone(),
            });
            self.stats.market_delta_rows = self.stats.market_delta_rows.saturating_add(1);
        }
        if let Some(market) = payload.selection.as_ref() {
            self.markets.push(MarketRow {
                role: "selection".to_string(),
                updated_at_ms: payload.updated_at_ms,
                revision: payload.revision,
                selection_updated_at_ms: payload.selection_updated_at_ms,
                next_selection_updated_at_ms: payload.next_selection_updated_at_ms,
                market: market.clone(),
            });
            self.stats.market_delta_rows = self.stats.market_delta_rows.saturating_add(1);
        }
        if let Some(market) = payload.next_selection.as_ref() {
            self.markets.push(MarketRow {
                role: "next_selection".to_string(),
                updated_at_ms: payload.updated_at_ms,
                revision: payload.revision,
                selection_updated_at_ms: payload.selection_updated_at_ms,
                next_selection_updated_at_ms: payload.next_selection_updated_at_ms,
                market: market.clone(),
            });
            self.stats.market_delta_rows = self.stats.market_delta_rows.saturating_add(1);
        }
    }

    fn push_book_deltas(&mut self, payload: &PolymarketOddsState) {
        let mut removed_tokens = self
            .previous
            .books
            .keys()
            .filter(|token_id| !payload.books.contains_key(token_id))
            .copied()
            .collect::<Vec<_>>();
        removed_tokens.sort_unstable();
        for token_id in removed_tokens {
            self.books.push(BookRow {
                remove: true,
                token_id: token_id.to_string(),
                token: Some(token_id),
                bid: None,
                ask: None,
                bid_sz: None,
                ask_sz: None,
                tick_size: 0.0,
                min_order_size: 0.0,
                ts_ms: payload.updated_at_ms,
                depth: None,
            });
            self.stats.token_book_delta_rows = self.stats.token_book_delta_rows.saturating_add(1);
            self.previous.books.remove(&token_id);
            self.previous.depths.remove(&token_id);
        }
        for (token_id, book) in &payload.books {
            let depth = payload.depths.get(token_id).cloned();
            if self.previous.books.get(token_id) != Some(book)
                || self.previous.depths.get(token_id) != depth.as_ref()
            {
                self.books.push(BookRow {
                    remove: false,
                    token_id: token_id.to_string(),
                    token: Some(*token_id),
                    bid: book.bid,
                    ask: book.ask,
                    bid_sz: book.bid_sz,
                    ask_sz: book.ask_sz,
                    tick_size: book.tick_size,
                    min_order_size: book.min_order_size,
                    ts_ms: book.ts_ms,
                    depth: depth.clone(),
                });
                self.stats.token_book_delta_rows =
                    self.stats.token_book_delta_rows.saturating_add(1);
            }
            self.previous.books.insert(*token_id, book.clone());
            if let Some(depth) = depth {
                self.previous.depths.insert(*token_id, depth);
            } else {
                self.previous.depths.remove(token_id);
            }
        }
    }

    fn push_lifecycle_deltas(&mut self, payload: &PolymarketOddsState) {
        if self.previous.lifecycle_status != payload.polymarket_lifecycle {
            self.lifecycles
                .push(LifecycleRow::status(&payload.polymarket_lifecycle));
            self.stats.lifecycle_delta_rows = self.stats.lifecycle_delta_rows.saturating_add(1);
            self.previous.lifecycle_status = payload.polymarket_lifecycle.clone();
        }
        for (token_id, lifecycle) in &payload.token_lifecycle {
            if self.previous.token_lifecycle.get(token_id) != Some(lifecycle) {
                self.lifecycles
                    .push(LifecycleRow::token(token_id, lifecycle));
                self.stats.lifecycle_delta_rows = self.stats.lifecycle_delta_rows.saturating_add(1);
            }
        }
        self.previous
            .token_lifecycle
            .clone_from(&payload.token_lifecycle);
    }

    fn push_last_trade_deltas(&mut self, session_ts_ms: u64, payload: &PolymarketOddsState) {
        let mut last_seen_seq = self.previous.last_trade_seq;
        for trade in &payload.last_trades {
            last_seen_seq = last_seen_seq.max(trade.seq);
            if trade.seq <= self.previous.last_trade_seq {
                continue;
            }
            self.last_trades
                .push(LastTradeRow::new(session_ts_ms, trade));
            self.stats.last_trade_delta_rows = self.stats.last_trade_delta_rows.saturating_add(1);
        }
        self.previous.last_trade_seq = last_seen_seq;
    }

    fn record_event(&mut self, session_ts_ms: u64) {
        self.stats.start_ts_ms = Some(
            self.stats
                .start_ts_ms
                .map_or(session_ts_ms, |start| start.min(session_ts_ms)),
        );
        self.stats.end_ts_ms = Some(
            self.stats
                .end_ts_ms
                .map_or(session_ts_ms, |end| end.max(session_ts_ms)),
        );
        self.stats.total_events = self.stats.total_events.saturating_add(1);
    }
    fn flush_if_needed(&mut self) -> Result<()> {
        if self.clocks.len() >= CLOCK_BATCH_SIZE {
            self.flush_clocks()?;
        }
        if self.markets.len() >= MARKET_BATCH_SIZE {
            self.flush_markets()?;
        }
        if self.books.len() >= BOOK_BATCH_SIZE {
            self.flush_books()?;
        }
        if self.lifecycles.len() >= LIFECYCLE_BATCH_SIZE {
            self.flush_lifecycles()?;
        }
        if self.resolutions.len() >= RESOLUTION_BATCH_SIZE {
            self.flush_resolutions()?;
        }
        if self.last_trades.len() >= LAST_TRADE_BATCH_SIZE {
            self.flush_last_trades()?;
        }
        Ok(())
    }
    fn flush_all(&mut self) -> Result<()> {
        self.flush_clocks()?;
        self.flush_markets()?;
        self.flush_books()?;
        self.flush_lifecycles()?;
        self.flush_resolutions()?;
        self.flush_last_trades()
    }
    fn flush_clocks(&mut self) -> Result<()> {
        if self.clocks.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.clocks);
        let batch = clock_batch(self.clock_schema.clone(), &rows)?;
        self.clock_writer
            .write(&batch)
            .context("write engine_clock")
    }
    fn flush_markets(&mut self) -> Result<()> {
        if self.markets.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.markets);
        let batch = market_batch(self.market_schema.clone(), &rows)?;
        self.market_writer
            .write(&batch)
            .context("write market_delta")
    }
    fn flush_books(&mut self) -> Result<()> {
        if self.books.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.books);
        let batch = book_batch(self.book_schema.clone(), &rows)?;
        self.book_writer
            .write(&batch)
            .context("write token_book_delta")
    }
    fn flush_lifecycles(&mut self) -> Result<()> {
        if self.lifecycles.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.lifecycles);
        let batch = lifecycle_batch(self.lifecycle_schema.clone(), &rows)?;
        self.lifecycle_writer
            .write(&batch)
            .context("write lifecycle_delta")
    }
    fn flush_last_trades(&mut self) -> Result<()> {
        if self.last_trades.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.last_trades);
        let batch = last_trade_batch(self.last_trade_schema.clone(), &rows)?;
        self.last_trade_writer
            .write(&batch)
            .context("write last_trade_delta")
    }
}

impl LastTradeRow {
    fn new(session_ts_ms: u64, trade: &LastTrade) -> Self {
        Self {
            session_ts_ms,
            seq: trade.seq,
            token_id: trade.token_id,
            price: trade.price,
            size: trade.size,
            side: trade.side,
            trade_ts_ms: trade.ts_ms,
        }
    }
}

impl LifecycleRow {
    fn token(token_id: &U256, lifecycle: &str) -> Self {
        Self {
            kind: "token".to_string(),
            token_id: token_id.to_string(),
            lifecycle: lifecycle.to_string(),
            ..Self::empty_status()
        }
    }
    fn status(status: &PolymarketLifecycleStatus) -> Self {
        Self {
            kind: "status".to_string(),
            sync_state: status.sync_state.clone(),
            sync_epoch: status.sync_epoch,
            rotation_epoch: status.rotation_epoch,
            updated_at_ms: status.updated_at_ms,
            rotation_started_at_ms: status.rotation_started_at_ms,
            last_error: status.last_error.clone(),
            last_error_at_ms: status.last_error_at_ms,
            active_tokens: status.active_tokens,
            pending_revalidation_tokens: status.pending_revalidation_tokens,
            pending_subscription_tokens: status.pending_subscription_tokens,
            invalid_after_404_tokens: status.invalid_after_404_tokens,
            resolved_tokens: status.resolved_tokens,
            quarantined_from_entry_tokens: status.quarantined_from_entry_tokens,
            recent_book_404_total: status.recent_book_404_total,
            recent_book_404_tokens: status.recent_book_404_tokens.join("\n"),
            token_id: String::new(),
            lifecycle: String::new(),
        }
    }
    fn empty_status() -> Self {
        Self {
            kind: String::new(),
            token_id: String::new(),
            lifecycle: String::new(),
            sync_state: String::new(),
            sync_epoch: 0,
            rotation_epoch: 0,
            updated_at_ms: None,
            rotation_started_at_ms: None,
            last_error: None,
            last_error_at_ms: None,
            active_tokens: 0,
            pending_revalidation_tokens: 0,
            pending_subscription_tokens: 0,
            invalid_after_404_tokens: 0,
            resolved_tokens: 0,
            quarantined_from_entry_tokens: 0,
            recent_book_404_total: 0,
            recent_book_404_tokens: String::new(),
        }
    }
}

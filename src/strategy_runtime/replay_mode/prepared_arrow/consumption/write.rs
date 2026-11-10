use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, StructBuilder, UInt32Builder, UInt64Builder,
    UInt8Builder,
};
use arrow_array::RecordBatch;
use arrow_ipc::writer::FileWriter;
use polymarket_client_sdk::types::U256;

use super::schema;
use crate::capture::events::{StrategyConsumptionInput, StrategyConsumptionRecord};
use crate::oracle_resolution::ChainlinkResolutionPrice;
use crate::polymarket::state::{
    BookDepth, BookTop, LastTrade, PolymarketLifecycleStatus, PriceLevel, TradeSide,
};
use crate::strategy_runtime::runtime_driver::RuntimeInput;
use crate::types::{MarketTick, MarketTickTrigger, SourceTsKind};
use bbwick_engine::{MarketSelectionSnapshot, PolymarketOddsState};

const BATCH_ROWS: usize = 512;
type DynListBuilder = ListBuilder<Box<dyn ArrayBuilder>>;

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) struct PreparedConsumptionWriter {
    path: PathBuf,
    writer: FileWriter<File>,
    rows: StructBuilder,
    row_count: usize,
}

impl PreparedConsumptionWriter {
    pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn create(
        path: PathBuf,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let schema = schema::schema();
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let writer = FileWriter::try_new_with_options(
            file,
            &schema,
            crate::capture::arrow::zstd_write_options()?,
        )
        .with_context(|| format!("create arrow writer {}", path.display()))?;
        Ok(Self {
            path,
            writer,
            rows: StructBuilder::from_fields(schema.fields().clone(), BATCH_ROWS),
            row_count: 0,
        })
    }

    pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn write(
        &mut self,
        record: &StrategyConsumptionRecord,
    ) -> Result<()> {
        append_record(&mut self.rows, record)?;
        self.rows.append(true);
        self.row_count += 1;
        if self.row_count >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }
        let rows = self.rows.finish();
        let batch = RecordBatch::try_new(schema::schema(), rows.columns().to_vec())
            .context("build typed strategy consumption Arrow batch")?;
        self.writer
            .write(&batch)
            .with_context(|| format!("write {}", self.path.display()))?;
        self.row_count = 0;
        Ok(())
    }

    pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .finish()
            .with_context(|| format!("finish {}", self.path.display()))
    }
}

fn append_record(rows: &mut StructBuilder, record: &StrategyConsumptionRecord) -> Result<()> {
    record.validate_schema()?;
    append_candle_state(rows, record);
    append_trade_flow(rows, record.trade_flow.as_ref());
    u32s(rows, schema::SCHEMA_VERSION).append_value(record.schema_version);
    u64s(rows, schema::SEQUENCE).append_value(record.consumption_sequence);
    u64s(rows, schema::CONSUMED_AT).append_value(record.consumed_at_ms);
    u8s(rows, schema::ACTOR).append_value(record.actor_symbol as u8);
    append_odds(rows, record.odds.as_ref())?;
    append_input(rows, &record.input)
}

fn append_trade_flow(
    rows: &mut StructBuilder,
    flow: Option<&bbwick_engine::OracleTradeFlowSnapshot>,
) {
    use bbwick_engine::OracleTradeFlowCoverage;
    let value = structs(rows, schema::TRADE_FLOW);
    u64s(value, 0).append_value(flow.map_or(0, |flow| flow.window_end_mono_ns));
    u64s(value, 1).append_value(flow.map_or(0, |flow| flow.coverage_started_mono_ns));
    f64s(value, 2).append_value(flow.map_or(0.0, |flow| flow.buy_quantity));
    f64s(value, 3).append_value(flow.map_or(0.0, |flow| flow.sell_quantity));
    u8s(value, 4).append_value(flow.map_or(0, |flow| match flow.coverage {
        OracleTradeFlowCoverage::Complete => 0,
        OracleTradeFlowCoverage::NoVolume => 1,
        OracleTradeFlowCoverage::WarmingUp => 2,
        OracleTradeFlowCoverage::Disconnected => 3,
        OracleTradeFlowCoverage::Discontinuity => 4,
        OracleTradeFlowCoverage::Malformed => 5,
        OracleTradeFlowCoverage::Overflow => 6,
    }));
    u64s(value, 5).append_value(flow.map_or(0, |flow| flow.revision));
    u64s(value, 6).append_value(flow.map_or(0, |flow| flow.received_mono_ns));
    u64s(value, 7).append_value(flow.map_or(0, |flow| flow.published_mono_ns));
    u64s(value, 8).append_option(flow.and_then(|flow| flow.last_aggregate_id));
    u64s(value, 9).append_option(flow.and_then(|flow| flow.event_ts_ms));
    u64s(value, 10).append_option(flow.and_then(|flow| flow.trade_ts_ms));
    value.append(flow.is_some());
}

fn append_candle_state(rows: &mut StructBuilder, record: &StrategyConsumptionRecord) {
    u64s(rows, schema::CONSUMED_MONO).append_value(record.consumed_mono_ns);
    let list = lists(rows, schema::CANDLE_STATE);
    if let Some(windows) = &record.candle_state {
        for window in windows.iter() {
            let value = list_structs(list);
            strings(value, 0).append_value(&window.symbol);
            strings(value, 1).append_value(&window.timeframe);
            u64s(value, 2).append_value(window.revision);
            u64s(value, 3).append_value(window.received_mono_ns);
            u64s(value, 4).append_value(window.published_mono_ns);
            let bars = lists(value, 5);
            for bar in &window.bars {
                let bar_value = list_structs(bars);
                i64s(bar_value, 0).append_value(bar.ts_ms);
                f64s(bar_value, 1).append_value(bar.open);
                f64s(bar_value, 2).append_value(bar.high);
                f64s(bar_value, 3).append_value(bar.low);
                f64s(bar_value, 4).append_value(bar.close);
                f64s(bar_value, 5).append_value(bar.volume);
                bar_value.append(true);
            }
            bars.append(true);
            value.append(true);
        }
    }
    list.append(record.candle_state.is_some());
}

fn append_odds(rows: &mut StructBuilder, odds: &PolymarketOddsState) -> Result<()> {
    u64s(rows, schema::ODDS_UPDATED).append_value(odds.updated_at_ms);
    u64s(rows, schema::ODDS_REVISION).append_value(odds.revision);
    {
        let value = structs(rows, schema::FINGERPRINT);
        u64s(value, 0).append_value(odds.trigger_fingerprint.count);
        u64s(value, 1).append_value(odds.trigger_fingerprint.xor);
        u64s(value, 2).append_value(odds.trigger_fingerprint.sum);
        value.append(true);
    }
    u64s(rows, schema::SELECTION_UPDATED).append_value(odds.selection_updated_at_ms);
    u64s(rows, schema::NEXT_SELECTION_UPDATED).append_value(odds.next_selection_updated_at_ms);

    {
        let list = lists(rows, schema::TOKEN_LIFECYCLE);
        let mut entries = odds.token_lifecycle.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(token, _)| **token);
        for (token, lifecycle) in entries {
            let value = list_structs(list);
            fixed(value, 0).append_value(token.to_be_bytes::<32>())?;
            strings(value, 1).append_value(lifecycle);
            value.append(true);
        }
        list.append(true);
    }
    {
        let list = lists(rows, schema::RESOLVED_WINNERS);
        let mut entries = odds.resolved_market_winners.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(condition, _)| **condition);
        for (condition, winner) in entries {
            let value = list_structs(list);
            fixed(value, 0).append_value(condition.as_slice())?;
            fixed(value, 1).append_value(winner.to_be_bytes::<32>())?;
            value.append(true);
        }
        list.append(true);
    }
    {
        let list = lists(rows, schema::RESOLVED_CONTEXTS);
        let mut entries = odds.resolved_market_contexts.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(condition, _)| **condition);
        for (condition, context) in entries {
            let value = list_structs(list);
            fixed(value, 0).append_value(condition.as_slice())?;
            fixed(value, 1).append_value(context.up_token.to_be_bytes::<32>())?;
            fixed(value, 2).append_value(context.down_token.to_be_bytes::<32>())?;
            u64s(value, 3).append_value(context.observed_at_ms);
            fixed(value, 4).append_value(context.raw_response_sha256)?;
            value.append(true);
        }
        list.append(true);
    }
    append_lifecycle(
        structs(rows, schema::LIFECYCLE_STATUS),
        &odds.polymarket_lifecycle,
    )?;
    append_market(structs(rows, schema::SELECTION), odds.selection.as_ref())?;
    append_market(
        structs(rows, schema::NEXT_SELECTION),
        odds.next_selection.as_ref(),
    )?;
    {
        let list = lists(rows, schema::MARKETS);
        for market in &odds.markets {
            append_market(list_structs(list), Some(market))?;
        }
        list.append(true);
    }
    append_book(structs(rows, schema::UP), odds.up.as_ref())?;
    append_book(structs(rows, schema::DOWN), odds.down.as_ref())?;
    append_book(structs(rows, schema::NEXT_UP), odds.next_up.as_ref())?;
    append_book(structs(rows, schema::NEXT_DOWN), odds.next_down.as_ref())?;
    {
        let list = lists(rows, schema::BOOKS);
        let mut entries = odds.books.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(token, _)| **token);
        for (key, book) in entries {
            let entry = list_structs(list);
            fixed(entry, 0).append_value(key.to_be_bytes::<32>())?;
            append_book(structs(entry, 1), Some(book))?;
            entry.append(true);
        }
        list.append(true);
    }
    {
        let list = lists(rows, schema::DEPTHS);
        let mut entries = odds.depths.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(token, _)| **token);
        for (key, depth) in entries {
            let entry = list_structs(list);
            fixed(entry, 0).append_value(key.to_be_bytes::<32>())?;
            append_depth(structs(entry, 1), depth)?;
            entry.append(true);
        }
        list.append(true);
    }
    {
        let list = lists(rows, schema::LAST_CANONICAL_BIDS);
        let mut entries = odds.last_canonical_bids.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(token, _)| **token);
        for (token, bid) in entries {
            let value = list_structs(list);
            fixed(value, 0).append_value(token.to_be_bytes::<32>())?;
            u64s(value, 1).append_value(bid.last_canonical_bid_units_8dp);
            u64s(value, 2).append_value(bid.book_ts_ms);
            u64s(value, 3).append_value(bid.observed_at_ms);
            value.append(true);
        }
        list.append(true);
    }
    {
        let list = lists(rows, schema::LAST_TRADES);
        for trade in &odds.last_trades {
            append_trade(list_structs(list), trade)?;
        }
        list.append(true);
    }
    u64s(rows, schema::NEXT_LAST_TRADE_SEQUENCE).append_value(odds.next_last_trade_seq);
    Ok(())
}

fn append_lifecycle(value: &mut StructBuilder, status: &PolymarketLifecycleStatus) -> Result<()> {
    strings(value, 0).append_value(&status.sync_state);
    u64s(value, 1).append_value(status.sync_epoch);
    u64s(value, 2).append_value(status.rotation_epoch);
    u64s(value, 3).append_option(status.updated_at_ms);
    u64s(value, 4).append_option(status.rotation_started_at_ms);
    strings(value, 5).append_option(status.last_error.as_deref());
    u64s(value, 6).append_option(status.last_error_at_ms);
    for (index, count) in [
        status.active_tokens,
        status.pending_revalidation_tokens,
        status.pending_subscription_tokens,
        status.invalid_after_404_tokens,
        status.resolved_tokens,
        status.quarantined_from_entry_tokens,
    ]
    .into_iter()
    .enumerate()
    {
        u64s(value, 7 + index).append_value(u64::try_from(count)?);
    }
    u64s(value, 13).append_value(status.recent_book_404_total);
    let recent = lists(value, 14);
    for token in &status.recent_book_404_tokens {
        list_strings(recent).append_value(token);
    }
    recent.append(true);
    value.append(true);
    Ok(())
}

fn append_market(
    value: &mut StructBuilder,
    market: Option<&MarketSelectionSnapshot>,
) -> Result<()> {
    let present = market.is_some();
    let empty = MarketSelectionSnapshot {
        market_id: String::new(),
        condition_id: String::new(),
        gamma_market_id: String::new(),
        condition_id_b256: None,
        underlying: String::new(),
        up_token: U256::ZERO,
        down_token: U256::ZERO,
        window_start_ms: 0,
        window_end_ms: 0,
        maker_base_fee_bps: None,
        taker_base_fee_bps: None,
        minimum_tick_size: None,
        minimum_order_size: None,
        rewards_min_size: None,
        rewards_max_spread: None,
        resolution_source: None,
        neg_risk: None,
        itode: None,
        standard_binary_ctf: false,
        question: None,
        slug: None,
        up_lifecycle: String::new(),
        down_lifecycle: String::new(),
        market_frame_received_at: None,
        market_state_published_at: None,
        market_connection_session_id: None,
        market_frame_sequence: None,
        market_state_revision: None,
    };
    let market = market.unwrap_or(&empty);
    strings(value, schema::MARKET_ID).append_value(&market.market_id);
    strings(value, schema::MARKET_CONDITION_ID).append_value(&market.condition_id);
    strings(value, schema::MARKET_GAMMA_ID).append_value(&market.gamma_market_id);
    strings(value, schema::MARKET_CONDITION_B256)
        .append_option(market.condition_id_b256.as_deref());
    strings(value, schema::MARKET_UNDERLYING).append_value(&market.underlying);
    fixed(value, schema::MARKET_UP_TOKEN).append_value(market.up_token.to_be_bytes::<32>())?;
    fixed(value, schema::MARKET_DOWN_TOKEN).append_value(market.down_token.to_be_bytes::<32>())?;
    u64s(value, schema::MARKET_WINDOW_START).append_value(market.window_start_ms);
    u64s(value, schema::MARKET_WINDOW_END).append_value(market.window_end_ms);
    i32s(value, schema::MARKET_MAKER_FEE).append_option(market.maker_base_fee_bps);
    i32s(value, schema::MARKET_TAKER_FEE).append_option(market.taker_base_fee_bps);
    f64s(value, schema::MARKET_MIN_TICK).append_option(market.minimum_tick_size);
    f64s(value, schema::MARKET_MIN_ORDER).append_option(market.minimum_order_size);
    f64s(value, schema::MARKET_REWARDS_MIN).append_option(market.rewards_min_size);
    f64s(value, schema::MARKET_REWARDS_SPREAD).append_option(market.rewards_max_spread);
    strings(value, schema::MARKET_RESOLUTION_SOURCE)
        .append_option(market.resolution_source.as_deref());
    bools(value, schema::MARKET_NEG_RISK).append_option(market.neg_risk);
    bools(value, schema::MARKET_ITODE).append_option(market.itode);
    bools(value, schema::MARKET_STANDARD_BINARY_CTF).append_value(market.standard_binary_ctf);
    strings(value, schema::MARKET_QUESTION).append_option(market.question.as_deref());
    strings(value, schema::MARKET_SLUG).append_option(market.slug.as_deref());
    strings(value, schema::MARKET_UP_LIFECYCLE).append_value(&market.up_lifecycle);
    strings(value, schema::MARKET_DOWN_LIFECYCLE).append_value(&market.down_lifecycle);
    value.append(present);
    Ok(())
}

fn append_book(value: &mut StructBuilder, book: Option<&BookTop>) -> Result<()> {
    let present = book.is_some();
    let empty = BookTop {
        token_id: U256::ZERO,
        bid: None,
        ask: None,
        bid_sz: None,
        ask_sz: None,
        tick_size: 0.0,
        min_order_size: 0.0,
        ts_ms: 0,
        observed_at_ms: 0,
    };
    let book = book.unwrap_or(&empty);
    fixed(value, schema::BOOK_TOKEN).append_value(book.token_id.to_be_bytes::<32>())?;
    f64s(value, schema::BOOK_BID).append_option(book.bid);
    f64s(value, schema::BOOK_ASK).append_option(book.ask);
    f64s(value, schema::BOOK_BID_SIZE).append_option(book.bid_sz);
    f64s(value, schema::BOOK_ASK_SIZE).append_option(book.ask_sz);
    f64s(value, schema::BOOK_TICK_SIZE).append_value(book.tick_size);
    f64s(value, schema::BOOK_MIN_ORDER_SIZE).append_value(book.min_order_size);
    u64s(value, schema::BOOK_TS).append_value(book.ts_ms);
    u64s(value, schema::BOOK_OBSERVED_AT).append_value(book.observed_at_ms);
    value.append(present);
    Ok(())
}

fn append_depth(value: &mut StructBuilder, depth: &BookDepth) -> Result<()> {
    fixed(value, schema::DEPTH_TOKEN).append_value(depth.token_id.to_be_bytes::<32>())?;
    append_levels(lists(value, schema::DEPTH_BIDS), &depth.bids);
    append_levels(lists(value, schema::DEPTH_ASKS), &depth.asks);
    u64s(value, schema::DEPTH_TS).append_value(depth.ts_ms);
    u64s(value, schema::DEPTH_REVISION).append_value(depth.revision);
    u64s(value, schema::DEPTH_OBSERVED_AT).append_value(depth.observed_at_ms);
    value.append(true);
    Ok(())
}

fn append_levels(list: &mut DynListBuilder, levels: &[PriceLevel]) {
    for level in levels {
        let value = list_structs(list);
        f64s(value, 0).append_value(level.price);
        f64s(value, 1).append_value(level.size);
        value.append(true);
    }
    list.append(true);
}

fn append_trade(value: &mut StructBuilder, trade: &LastTrade) -> Result<()> {
    u64s(value, 0).append_value(trade.seq);
    fixed(value, 1).append_value(trade.token_id.to_be_bytes::<32>())?;
    f64s(value, 2).append_value(trade.price);
    f64s(value, 3).append_value(trade.size);
    u8s(value, 4).append_value(match trade.side {
        None => 0,
        Some(TradeSide::Buy) => 1,
        Some(TradeSide::Sell) => 2,
    });
    u64s(value, 5).append_value(trade.ts_ms);
    value.append(true);
    Ok(())
}

fn append_input(rows: &mut StructBuilder, input: &StrategyConsumptionInput) -> Result<()> {
    match input {
        StrategyConsumptionInput::Runtime(input) => {
            u8s(rows, schema::INPUT_KIND).append_value(0);
            let (kind, tick, clock) = match input {
                RuntimeInput::AcceptedTick { tick } => (0, tick, None),
                RuntimeInput::Heartbeat {
                    tick,
                    now_ms,
                    now_ingest_mono_ns,
                } => (1, tick, Some((*now_ms, *now_ingest_mono_ns))),
                RuntimeInput::PortfolioChanged {
                    last_tick,
                    now_ms,
                    now_ingest_mono_ns,
                } => (2, last_tick, Some((*now_ms, *now_ingest_mono_ns))),
                RuntimeInput::PolymarketDataChanged {
                    last_tick,
                    now_ms,
                    now_ingest_mono_ns,
                } => (3, last_tick, Some((*now_ms, *now_ingest_mono_ns))),
            };
            u8s(rows, schema::RUNTIME_KIND).append_value(kind);
            append_tick(structs(rows, schema::TICK), Some(tick));
            u64s(rows, schema::INPUT_NOW).append_option(clock.map(|value| value.0));
            u64s(rows, schema::INPUT_NOW_MONO).append_option(clock.map(|value| value.1));
            append_chainlink(structs(rows, schema::CHAINLINK), None)?;
            u64s(rows, schema::CHAINLINK_LEARNING_TS).append_null();
        }
        StrategyConsumptionInput::Chainlink {
            price,
            learning_applied_ts_ms,
        } => {
            u8s(rows, schema::INPUT_KIND).append_value(1);
            u8s(rows, schema::RUNTIME_KIND).append_null();
            append_tick(structs(rows, schema::TICK), None);
            u64s(rows, schema::INPUT_NOW).append_null();
            u64s(rows, schema::INPUT_NOW_MONO).append_null();
            append_chainlink(structs(rows, schema::CHAINLINK), Some(price))?;
            u64s(rows, schema::CHAINLINK_LEARNING_TS).append_value(*learning_applied_ts_ms);
        }
    }
    Ok(())
}

fn append_tick(value: &mut StructBuilder, tick: Option<&MarketTick>) {
    let present = tick.is_some();
    let empty = MarketTick {
        instrument: String::new(),
        bid: 0.0,
        ask: 0.0,
        bid_sz: None,
        ask_sz: None,
        queue_imbalance_l5: None,
        source_ts_ns: 0,
        source_ts_kind: SourceTsKind::Unavailable,
        ingest_mono_ns: 0,
        ts_ns: 0,
        trigger: MarketTickTrigger::MarketData,
    };
    let tick = tick.unwrap_or(&empty);
    strings(value, schema::TICK_INSTRUMENT).append_value(&tick.instrument);
    f64s(value, schema::TICK_BID).append_value(tick.bid);
    f64s(value, schema::TICK_ASK).append_value(tick.ask);
    f64s(value, schema::TICK_BID_SIZE).append_option(tick.bid_sz);
    f64s(value, schema::TICK_ASK_SIZE).append_option(tick.ask_sz);
    f64s(value, schema::TICK_IMBALANCE).append_option(tick.queue_imbalance_l5);
    i64s(value, schema::TICK_SOURCE_TS).append_value(tick.source_ts_ns);
    u8s(value, schema::TICK_SOURCE_KIND).append_value(match tick.source_ts_kind {
        SourceTsKind::Unavailable => 0,
        SourceTsKind::Exchange => 1,
        SourceTsKind::ProviderEvent => 2,
        SourceTsKind::EngineIngestWallclock => 3,
    });
    u64s(value, schema::TICK_INGEST_MONO).append_value(tick.ingest_mono_ns);
    i64s(value, schema::TICK_TS).append_value(tick.ts_ns);
    u8s(value, schema::TICK_TRIGGER).append_value(match tick.trigger {
        MarketTickTrigger::MarketData => 0,
        MarketTickTrigger::OracleOnly => 1,
    });
    value.append(present);
}

fn append_chainlink(
    value: &mut StructBuilder,
    price: Option<&ChainlinkResolutionPrice>,
) -> Result<()> {
    let present = price.is_some();
    let empty = ChainlinkResolutionPrice {
        symbol: String::new(),
        topic: String::new(),
        window_s: 0,
        payload_ts_ms: 0,
        observed_ts_ms: 0,
        value_e18: 0,
        capture_sequence: 0,
    };
    let price = price.unwrap_or(&empty);
    strings(value, 0).append_value(&price.symbol);
    strings(value, 1).append_value(&price.topic);
    u64s(value, 2).append_value(price.window_s);
    u64s(value, 3).append_value(price.payload_ts_ms);
    u64s(value, 4).append_value(price.observed_ts_ms);
    fixed(value, 5).append_value(price.value_e18.to_le_bytes())?;
    u64s(value, 6).append_value(price.capture_sequence);
    value.append(present);
    Ok(())
}

fn u8s(value: &mut StructBuilder, index: usize) -> &mut UInt8Builder {
    value.field_builder(index).expect("typed UInt8 field")
}
fn u32s(value: &mut StructBuilder, index: usize) -> &mut UInt32Builder {
    value.field_builder(index).expect("typed UInt32 field")
}
fn u64s(value: &mut StructBuilder, index: usize) -> &mut UInt64Builder {
    value.field_builder(index).expect("typed UInt64 field")
}
fn i32s(value: &mut StructBuilder, index: usize) -> &mut Int32Builder {
    value.field_builder(index).expect("typed Int32 field")
}
fn i64s(value: &mut StructBuilder, index: usize) -> &mut Int64Builder {
    value.field_builder(index).expect("typed Int64 field")
}
fn f64s(value: &mut StructBuilder, index: usize) -> &mut Float64Builder {
    value.field_builder(index).expect("typed Float64 field")
}
fn bools(value: &mut StructBuilder, index: usize) -> &mut BooleanBuilder {
    value.field_builder(index).expect("typed Boolean field")
}
fn strings(value: &mut StructBuilder, index: usize) -> &mut StringBuilder {
    value.field_builder(index).expect("typed Utf8 field")
}
fn fixed(value: &mut StructBuilder, index: usize) -> &mut FixedSizeBinaryBuilder {
    value
        .field_builder(index)
        .expect("typed fixed binary field")
}
fn structs(value: &mut StructBuilder, index: usize) -> &mut StructBuilder {
    value.field_builder(index).expect("typed struct field")
}
fn lists(value: &mut StructBuilder, index: usize) -> &mut DynListBuilder {
    value.field_builder(index).expect("typed list field")
}
fn list_structs(value: &mut DynListBuilder) -> &mut StructBuilder {
    value
        .values()
        .as_any_mut()
        .downcast_mut()
        .expect("typed list struct values")
}
fn list_strings(value: &mut DynListBuilder) -> &mut StringBuilder {
    value
        .values()
        .as_any_mut()
        .downcast_mut()
        .expect("typed list string values")
}

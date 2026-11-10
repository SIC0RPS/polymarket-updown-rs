use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};

pub(super) const SCHEMA_VERSION: usize = 0;
pub(super) const SEQUENCE: usize = 1;
pub(super) const CONSUMED_AT: usize = 2;
pub(super) const ACTOR: usize = 3;
pub(super) const ODDS_UPDATED: usize = 4;
pub(super) const ODDS_REVISION: usize = 5;
pub(super) const FINGERPRINT: usize = 6;
pub(super) const SELECTION_UPDATED: usize = 7;
pub(super) const NEXT_SELECTION_UPDATED: usize = 8;
pub(super) const TOKEN_LIFECYCLE: usize = 9;
pub(super) const RESOLVED_WINNERS: usize = 10;
pub(super) const RESOLVED_CONTEXTS: usize = 11;
pub(super) const LIFECYCLE_STATUS: usize = 12;
pub(super) const SELECTION: usize = 13;
pub(super) const NEXT_SELECTION: usize = 14;
pub(super) const MARKETS: usize = 15;
pub(super) const UP: usize = 16;
pub(super) const DOWN: usize = 17;
pub(super) const NEXT_UP: usize = 18;
pub(super) const NEXT_DOWN: usize = 19;
pub(super) const BOOKS: usize = 20;
pub(super) const DEPTHS: usize = 21;
pub(super) const LAST_CANONICAL_BIDS: usize = 22;
pub(super) const LAST_TRADES: usize = 23;
pub(super) const NEXT_LAST_TRADE_SEQUENCE: usize = 24;
pub(super) const INPUT_KIND: usize = 25;
pub(super) const RUNTIME_KIND: usize = 26;
pub(super) const TICK: usize = 27;
pub(super) const INPUT_NOW: usize = 28;
pub(super) const INPUT_NOW_MONO: usize = 29;
pub(super) const CHAINLINK: usize = 30;
pub(super) const CHAINLINK_LEARNING_TS: usize = 31;
pub(super) const CONSUMED_MONO: usize = 32;
pub(super) const CANDLE_STATE: usize = 33;
pub(super) const TRADE_FLOW: usize = 34;

pub(super) const MARKET_ID: usize = 0;
pub(super) const MARKET_CONDITION_ID: usize = 1;
pub(super) const MARKET_GAMMA_ID: usize = 2;
pub(super) const MARKET_CONDITION_B256: usize = 3;
pub(super) const MARKET_UNDERLYING: usize = 4;
pub(super) const MARKET_UP_TOKEN: usize = 5;
pub(super) const MARKET_DOWN_TOKEN: usize = 6;
pub(super) const MARKET_WINDOW_START: usize = 7;
pub(super) const MARKET_WINDOW_END: usize = 8;
pub(super) const MARKET_MAKER_FEE: usize = 9;
pub(super) const MARKET_TAKER_FEE: usize = 10;
pub(super) const MARKET_MIN_TICK: usize = 11;
pub(super) const MARKET_MIN_ORDER: usize = 12;
pub(super) const MARKET_REWARDS_MIN: usize = 13;
pub(super) const MARKET_REWARDS_SPREAD: usize = 14;
pub(super) const MARKET_RESOLUTION_SOURCE: usize = 15;
pub(super) const MARKET_NEG_RISK: usize = 16;
pub(super) const MARKET_ITODE: usize = 17;
pub(super) const MARKET_STANDARD_BINARY_CTF: usize = 18;
pub(super) const MARKET_QUESTION: usize = 19;
pub(super) const MARKET_SLUG: usize = 20;
pub(super) const MARKET_UP_LIFECYCLE: usize = 21;
pub(super) const MARKET_DOWN_LIFECYCLE: usize = 22;

pub(super) const BOOK_TOKEN: usize = 0;
pub(super) const BOOK_BID: usize = 1;
pub(super) const BOOK_ASK: usize = 2;
pub(super) const BOOK_BID_SIZE: usize = 3;
pub(super) const BOOK_ASK_SIZE: usize = 4;
pub(super) const BOOK_TICK_SIZE: usize = 5;
pub(super) const BOOK_MIN_ORDER_SIZE: usize = 6;
pub(super) const BOOK_TS: usize = 7;
pub(super) const BOOK_OBSERVED_AT: usize = 8;

pub(super) const DEPTH_TOKEN: usize = 0;
pub(super) const DEPTH_BIDS: usize = 1;
pub(super) const DEPTH_ASKS: usize = 2;
pub(super) const DEPTH_TS: usize = 3;
pub(super) const DEPTH_REVISION: usize = 4;
pub(super) const DEPTH_OBSERVED_AT: usize = 5;

pub(super) const TICK_INSTRUMENT: usize = 0;
pub(super) const TICK_BID: usize = 1;
pub(super) const TICK_ASK: usize = 2;
pub(super) const TICK_BID_SIZE: usize = 3;
pub(super) const TICK_ASK_SIZE: usize = 4;
pub(super) const TICK_IMBALANCE: usize = 5;
pub(super) const TICK_SOURCE_TS: usize = 6;
pub(super) const TICK_SOURCE_KIND: usize = 7;
pub(super) const TICK_INGEST_MONO: usize = 8;
pub(super) const TICK_TS: usize = 9;
pub(super) const TICK_TRIGGER: usize = 10;

pub(super) fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        field("schema_version", DataType::UInt32, false),
        field("consumption_sequence", DataType::UInt64, false),
        field("consumed_at_ms", DataType::UInt64, false),
        field("actor_symbol", DataType::UInt8, false),
        field("odds_updated_at_ms", DataType::UInt64, false),
        field("odds_revision", DataType::UInt64, false),
        field("odds_fingerprint", struct_type(fingerprint_fields()), false),
        field("odds_selection_updated_at_ms", DataType::UInt64, false),
        field("odds_next_selection_updated_at_ms", DataType::UInt64, false),
        list("odds_token_lifecycle", token_lifecycle_fields()),
        list("odds_resolved_winners", resolved_winner_fields()),
        list("odds_resolved_contexts", resolved_context_fields()),
        field(
            "odds_lifecycle_status",
            struct_type(lifecycle_fields()),
            false,
        ),
        field("odds_selection", struct_type(market_fields()), true),
        field("odds_next_selection", struct_type(market_fields()), true),
        list("odds_markets", market_fields()),
        field("odds_up", struct_type(book_fields()), true),
        field("odds_down", struct_type(book_fields()), true),
        field("odds_next_up", struct_type(book_fields()), true),
        field("odds_next_down", struct_type(book_fields()), true),
        list("odds_books", book_map_fields()),
        list("odds_depths", depth_map_fields()),
        list("odds_last_canonical_bids", last_canonical_bid_fields()),
        list("odds_last_trades", last_trade_fields()),
        field("odds_next_last_trade_sequence", DataType::UInt64, false),
        field("input_kind", DataType::UInt8, false),
        field("runtime_kind", DataType::UInt8, true),
        field("tick", struct_type(tick_fields()), true),
        field("input_now_ms", DataType::UInt64, true),
        field("input_now_ingest_mono_ns", DataType::UInt64, true),
        field("chainlink", struct_type(chainlink_fields()), true),
        field("chainlink_learning_applied_ts_ms", DataType::UInt64, true),
        field("consumed_mono_ns", DataType::UInt64, false),
        list_field("candle_state", candle_window_fields(), true),
        field("trade_flow", struct_type(trade_flow_fields()), true),
    ]))
}

pub(super) fn fingerprint_fields() -> Fields {
    fields(vec![
        field("count", DataType::UInt64, false),
        field("xor", DataType::UInt64, false),
        field("sum", DataType::UInt64, false),
    ])
}

pub(super) fn token_lifecycle_fields() -> Fields {
    fields(vec![
        fixed("token_id", 32, false),
        field("lifecycle", DataType::Utf8, false),
    ])
}

pub(super) fn resolved_winner_fields() -> Fields {
    fields(vec![
        fixed("condition_id", 32, false),
        fixed("winning_token", 32, false),
    ])
}

pub(super) fn resolved_context_fields() -> Fields {
    fields(vec![
        fixed("condition_id", 32, false),
        fixed("up_token", 32, false),
        fixed("down_token", 32, false),
        field("observed_at_ms", DataType::UInt64, false),
        fixed("raw_response_sha256", 32, false),
    ])
}

pub(super) fn lifecycle_fields() -> Fields {
    fields(vec![
        field("sync_state", DataType::Utf8, false),
        field("sync_epoch", DataType::UInt64, false),
        field("rotation_epoch", DataType::UInt64, false),
        field("updated_at_ms", DataType::UInt64, true),
        field("rotation_started_at_ms", DataType::UInt64, true),
        field("last_error", DataType::Utf8, true),
        field("last_error_at_ms", DataType::UInt64, true),
        field("active_tokens", DataType::UInt64, false),
        field("pending_revalidation_tokens", DataType::UInt64, false),
        field("pending_subscription_tokens", DataType::UInt64, false),
        field("invalid_after_404_tokens", DataType::UInt64, false),
        field("resolved_tokens", DataType::UInt64, false),
        field("quarantined_from_entry_tokens", DataType::UInt64, false),
        field("recent_book_404_total", DataType::UInt64, false),
        string_list("recent_book_404_tokens"),
    ])
}

pub(super) fn market_fields() -> Fields {
    fields(vec![
        field("market_id", DataType::Utf8, false),
        field("condition_id", DataType::Utf8, false),
        field("gamma_market_id", DataType::Utf8, false),
        field("condition_id_b256", DataType::Utf8, true),
        field("underlying", DataType::Utf8, false),
        fixed("up_token", 32, false),
        fixed("down_token", 32, false),
        field("window_start_ms", DataType::UInt64, false),
        field("window_end_ms", DataType::UInt64, false),
        field("maker_base_fee_bps", DataType::Int32, true),
        field("taker_base_fee_bps", DataType::Int32, true),
        field("minimum_tick_size", DataType::Float64, true),
        field("minimum_order_size", DataType::Float64, true),
        field("rewards_min_size", DataType::Float64, true),
        field("rewards_max_spread", DataType::Float64, true),
        field("resolution_source", DataType::Utf8, true),
        field("neg_risk", DataType::Boolean, true),
        field("itode", DataType::Boolean, true),
        field("standard_binary_ctf", DataType::Boolean, false),
        field("question", DataType::Utf8, true),
        field("slug", DataType::Utf8, true),
        field("up_lifecycle", DataType::Utf8, false),
        field("down_lifecycle", DataType::Utf8, false),
    ])
}

pub(super) fn book_fields() -> Fields {
    fields(vec![
        fixed("token_id", 32, false),
        field("bid", DataType::Float64, true),
        field("ask", DataType::Float64, true),
        field("bid_size", DataType::Float64, true),
        field("ask_size", DataType::Float64, true),
        field("tick_size", DataType::Float64, false),
        field("minimum_order_size", DataType::Float64, false),
        field("ts_ms", DataType::UInt64, false),
        field("observed_at_ms", DataType::UInt64, false),
    ])
}

pub(super) fn book_map_fields() -> Fields {
    fields(vec![
        fixed("map_key", 32, false),
        field("value", struct_type(book_fields()), false),
    ])
}

pub(super) fn depth_fields() -> Fields {
    fields(vec![
        fixed("token_id", 32, false),
        list_field("bids", price_level_fields(), false),
        list_field("asks", price_level_fields(), false),
        field("ts_ms", DataType::UInt64, false),
        field("revision", DataType::UInt64, false),
        field("observed_at_ms", DataType::UInt64, false),
    ])
}

pub(super) fn depth_map_fields() -> Fields {
    fields(vec![
        fixed("map_key", 32, false),
        field("value", struct_type(depth_fields()), false),
    ])
}

pub(super) fn price_level_fields() -> Fields {
    fields(vec![
        field("price", DataType::Float64, false),
        field("size", DataType::Float64, false),
    ])
}

pub(super) fn last_canonical_bid_fields() -> Fields {
    fields(vec![
        fixed("token_id", 32, false),
        field("bid_units_8dp", DataType::UInt64, false),
        field("book_ts_ms", DataType::UInt64, false),
        field("observed_at_ms", DataType::UInt64, false),
    ])
}

pub(super) fn last_trade_fields() -> Fields {
    fields(vec![
        field("sequence", DataType::UInt64, false),
        fixed("token_id", 32, false),
        field("price", DataType::Float64, false),
        field("size", DataType::Float64, false),
        field("side", DataType::UInt8, false),
        field("ts_ms", DataType::UInt64, false),
    ])
}

pub(super) fn tick_fields() -> Fields {
    fields(vec![
        field("instrument", DataType::Utf8, false),
        field("bid", DataType::Float64, false),
        field("ask", DataType::Float64, false),
        field("bid_size", DataType::Float64, true),
        field("ask_size", DataType::Float64, true),
        field("queue_imbalance_l5", DataType::Float64, true),
        field("source_ts_ns", DataType::Int64, false),
        field("source_ts_kind", DataType::UInt8, false),
        field("ingest_mono_ns", DataType::UInt64, false),
        field("ts_ns", DataType::Int64, false),
        field("trigger", DataType::UInt8, false),
    ])
}

pub(super) fn chainlink_fields() -> Fields {
    fields(vec![
        field("symbol", DataType::Utf8, false),
        field("topic", DataType::Utf8, false),
        field("window_s", DataType::UInt64, false),
        field("payload_ts_ms", DataType::UInt64, false),
        field("observed_ts_ms", DataType::UInt64, false),
        fixed("value_e18", 16, false),
        field("capture_sequence", DataType::UInt64, false),
    ])
}

fn trade_flow_fields() -> Fields {
    fields(vec![
        field("window_end_mono_ns", DataType::UInt64, false),
        field("coverage_started_mono_ns", DataType::UInt64, false),
        field("buy_quantity", DataType::Float64, false),
        field("sell_quantity", DataType::Float64, false),
        field("coverage", DataType::UInt8, false),
        field("revision", DataType::UInt64, false),
        field("received_mono_ns", DataType::UInt64, false),
        field("published_mono_ns", DataType::UInt64, false),
        field("last_aggregate_id", DataType::UInt64, true),
        field("event_ts_ms", DataType::UInt64, true),
        field("trade_ts_ms", DataType::UInt64, true),
    ])
}

pub(super) fn candle_window_fields() -> Fields {
    fields(vec![
        field("symbol", DataType::Utf8, false),
        field("timeframe", DataType::Utf8, false),
        field("revision", DataType::UInt64, false),
        field("received_mono_ns", DataType::UInt64, false),
        field("published_mono_ns", DataType::UInt64, false),
        list("bars", candle_bar_fields()),
    ])
}

fn candle_bar_fields() -> Fields {
    fields(vec![
        field("ts_ms", DataType::Int64, false),
        field("open", DataType::Float64, false),
        field("high", DataType::Float64, false),
        field("low", DataType::Float64, false),
        field("close", DataType::Float64, false),
        field("volume", DataType::Float64, false),
    ])
}

fn fields(value: Vec<Field>) -> Fields {
    value.into()
}

fn struct_type(value: Fields) -> DataType {
    DataType::Struct(value)
}

fn field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

fn fixed(name: &str, width: i32, nullable: bool) -> Field {
    field(name, DataType::FixedSizeBinary(width), nullable)
}

fn list(name: &str, entry_fields: Fields) -> Field {
    list_field(name, entry_fields, false)
}

fn list_field(name: &str, entry_fields: Fields, nullable: bool) -> Field {
    field(
        name,
        DataType::List(Arc::new(field(
            "item",
            DataType::Struct(entry_fields),
            false,
        ))),
        nullable,
    )
}

fn string_list(name: &str) -> Field {
    field(
        name,
        DataType::List(Arc::new(field("item", DataType::Utf8, false))),
        false,
    )
}

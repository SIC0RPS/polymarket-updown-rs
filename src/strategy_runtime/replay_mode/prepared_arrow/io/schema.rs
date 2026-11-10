use super::*;

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn clock_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_ts_ms", DataType::Int64, false),
        Field::new("source_priority", DataType::Int64, false),
        Field::new("sequence", DataType::Int64, false),
        Field::new("updated_at_ms", DataType::Int64, false),
        Field::new("revision", DataType::Int64, false),
        Field::new("market_start", DataType::Int64, false),
        Field::new("market_len", DataType::Int64, false),
        Field::new("book_start", DataType::Int64, false),
        Field::new("book_len", DataType::Int64, false),
        Field::new("lifecycle_start", DataType::Int64, false),
        Field::new("lifecycle_len", DataType::Int64, false),
        Field::new("consumption_start", DataType::Int64, false),
        Field::new("consumption_len", DataType::Int64, false),
    ]))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn market_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("role", DataType::Utf8, false),
        Field::new("updated_at_ms", DataType::Int64, false),
        Field::new("revision", DataType::Int64, false),
        Field::new("selection_updated_at_ms", DataType::Int64, false),
        Field::new("next_selection_updated_at_ms", DataType::Int64, false),
        Field::new("market_id", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("gamma_market_id", DataType::Utf8, false),
        Field::new("condition_id_b256", DataType::Utf8, true),
        Field::new("underlying", DataType::Utf8, false),
        Field::new("up_token", DataType::Utf8, false),
        Field::new("down_token", DataType::Utf8, false),
        Field::new("window_start_ms", DataType::Int64, false),
        Field::new("window_end_ms", DataType::Int64, false),
        Field::new("maker_base_fee_bps", DataType::Int64, true),
        Field::new("taker_base_fee_bps", DataType::Int64, true),
        Field::new("minimum_tick_size", DataType::Float64, true),
        Field::new("minimum_order_size", DataType::Float64, true),
        Field::new("rewards_min_size", DataType::Float64, true),
        Field::new("rewards_max_spread", DataType::Float64, true),
        Field::new("resolution_source", DataType::Utf8, true),
        Field::new("neg_risk", DataType::Int64, true),
        Field::new("standard_binary_ctf", DataType::Int64, false),
        Field::new("question", DataType::Utf8, true),
        Field::new("slug", DataType::Utf8, true),
        Field::new("up_lifecycle", DataType::Utf8, false),
        Field::new("down_lifecycle", DataType::Utf8, false),
        Field::new("itode", DataType::Int64, true),
    ]))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn book_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("row_kind", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, true),
        Field::new("bid", DataType::Float64, true),
        Field::new("ask", DataType::Float64, true),
        Field::new("bid_sz", DataType::Float64, true),
        Field::new("ask_sz", DataType::Float64, true),
        Field::new("tick_size", DataType::Float64, true),
        Field::new("min_order_size", DataType::Float64, true),
        Field::new("ts_ms", DataType::Int64, true),
    ];
    for level in 0..STORED_DEPTH_LEVELS {
        fields.push(Field::new(
            format!("bid_l{}_px", level + 1),
            DataType::Float64,
            true,
        ));
        fields.push(Field::new(
            format!("bid_l{}_sz", level + 1),
            DataType::Float64,
            true,
        ));
        fields.push(Field::new(
            format!("ask_l{}_px", level + 1),
            DataType::Float64,
            true,
        ));
        fields.push(Field::new(
            format!("ask_l{}_sz", level + 1),
            DataType::Float64,
            true,
        ));
    }
    fields.push(Field::new("depth_revision", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn lifecycle_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, true),
        Field::new("lifecycle", DataType::Utf8, true),
        Field::new("sync_state", DataType::Utf8, true),
        Field::new("sync_epoch", DataType::Int64, true),
        Field::new("rotation_epoch", DataType::Int64, true),
        Field::new("updated_at_ms", DataType::Int64, true),
        Field::new("rotation_started_at_ms", DataType::Int64, true),
        Field::new("last_error", DataType::Utf8, true),
        Field::new("last_error_at_ms", DataType::Int64, true),
        Field::new("active_tokens", DataType::Int64, true),
        Field::new("pending_revalidation_tokens", DataType::Int64, true),
        Field::new("pending_subscription_tokens", DataType::Int64, true),
        Field::new("invalid_after_404_tokens", DataType::Int64, true),
        Field::new("resolved_tokens", DataType::Int64, true),
        Field::new("quarantined_from_entry_tokens", DataType::Int64, true),
        Field::new("recent_book_404_total", DataType::Int64, true),
        Field::new("recent_book_404_tokens", DataType::Utf8, true),
    ]))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn last_trade_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_ts_ms", DataType::Int64, false),
        Field::new("seq", DataType::Int64, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("side", DataType::Utf8, true),
        Field::new("trade_ts_ms", DataType::Int64, false),
    ]))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow::io) fn empty_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "reserved",
        DataType::Int64,
        false,
    )]))
}

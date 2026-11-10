use super::*;
use arrow_ipc::reader::FileReader;

pub(in crate::strategy_runtime::replay_mode) fn read_prepared_replay_data(
    dir: &Path,
    expected_symbol: &str,
) -> Result<PreparedReplayData> {
    let clocks = read_clock(&dir.join(ENGINE_CLOCK))?;
    let markets = read_market_rows(&dir.join(MARKET_DELTA))?;
    let books = PreparedBookRows::read(&dir.join(TOKEN_BOOK_DELTA))?;
    let consumptions = PreparedConsumptionSource::open(
        &dir.join(STRATEGY_CONSUMPTION_ARROW_FILE),
        expected_symbol,
    )?;
    Ok(PreparedReplayData {
        clocks,
        markets,
        books,
        consumptions,
        lifecycles: read_lifecycle_rows(&dir.join(LIFECYCLE_DELTA))?,
        resolutions: super::super::resolution::read_resolution_rows(&dir.join(RESOLUTION_DELTA))?,
        last_trades: read_last_trade_rows(&dir.join(LAST_TRADE_DELTA))?,
    })
}

impl PreparedReplayData {
    pub(in crate::strategy_runtime::replay_mode) fn read_prepared_market_rows(
        dir: &Path,
    ) -> Result<(Vec<MarketRow>, Vec<ResolutionRow>)> {
        Ok((
            read_market_rows(&dir.join(MARKET_DELTA))?,
            super::super::resolution::read_resolution_rows(&dir.join(RESOLUTION_DELTA))?,
        ))
    }
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn read_batches(
    path: &Path,
) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = FileReader::try_new(file, None)
        .with_context(|| format!("open arrow {}", path.display()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("read {}", path.display()))
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn read_clock(
    path: &Path,
) -> Result<Vec<ClockRow>> {
    let mut rows = Vec::new();
    for b in read_batches(path)? {
        let c = (0..13)
            .map(|i| int_col(&b, i, "clock"))
            .collect::<Result<Vec<_>>>()?;
        for i in 0..b.num_rows() {
            rows.push(ClockRow {
                session_ts_ms: req_i64(c[0], i)? as u64,
                source_priority: u8::try_from(req_i64(c[1], i)?)
                    .context("engine_clock source_priority outside u8 range")?,
                sequence: req_i64(c[2], i)? as u64,
                updated_at_ms: req_i64(c[3], i)? as u64,
                revision: req_i64(c[4], i)? as u64,
                market_start: req_i64(c[5], i)?,
                market_len: req_i64(c[6], i)?,
                book_start: req_i64(c[7], i)?,
                book_len: req_i64(c[8], i)?,
                lifecycle_start: req_i64(c[9], i)?,
                lifecycle_len: req_i64(c[10], i)?,
                consumption_start: req_i64(c[11], i)?,
                consumption_len: req_i64(c[12], i)?,
            });
        }
    }
    rows.sort_by_key(|r| (r.session_ts_ms, r.source_priority, r.sequence));
    Ok(rows)
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn read_market_rows(
    path: &Path,
) -> Result<Vec<MarketRow>> {
    let mut rows = Vec::new();
    for b in read_batches(path)? {
        let role = str_col(&b, 0, "role")?;
        let updated = int_col(&b, 1, "updated_at_ms")?;
        let revision = int_col(&b, 2, "revision")?;
        let sel_updated = int_col(&b, 3, "selection_updated_at_ms")?;
        let next_updated = int_col(&b, 4, "next_selection_updated_at_ms")?;
        let strings = (5..12)
            .map(|i| str_col(&b, i, "market"))
            .collect::<Result<Vec<_>>>()?;
        let win_start = int_col(&b, 12, "window_start_ms")?;
        let win_end = int_col(&b, 13, "window_end_ms")?;
        let maker = int_col(&b, 14, "maker_base_fee_bps")?;
        let taker = int_col(&b, 15, "taker_base_fee_bps")?;
        let min_tick = float_col(&b, 16, "minimum_tick_size")?;
        let min_order = float_col(&b, 17, "minimum_order_size")?;
        let rewards_min = float_col(&b, 18, "rewards_min_size")?;
        let rewards_spread = float_col(&b, 19, "rewards_max_spread")?;
        let resolution_source = str_col(&b, 20, "resolution_source")?;
        let neg_risk = int_col(&b, 21, "neg_risk")?;
        let standard = int_col(&b, 22, "standard_binary_ctf")?;
        let question = str_col(&b, 23, "question")?;
        let slug = str_col(&b, 24, "slug")?;
        let up_lifecycle = str_col(&b, 25, "up_lifecycle")?;
        let down_lifecycle = str_col(&b, 26, "down_lifecycle")?;
        let itode = int_col(&b, 27, "itode")?;
        for i in 0..b.num_rows() {
            rows.push(MarketRow {
                role: req_str(role, i)?.to_string(),
                updated_at_ms: req_i64(updated, i)? as u64,
                revision: req_i64(revision, i)? as u64,
                selection_updated_at_ms: req_i64(sel_updated, i)? as u64,
                next_selection_updated_at_ms: req_i64(next_updated, i)? as u64,
                market: bbwick_engine::state::MarketSelectionSnapshot {
                    market_id: req_str(strings[0], i)?.to_string(),
                    condition_id: req_str(strings[1], i)?.to_string(),
                    gamma_market_id: req_str(strings[2], i)?.to_string(),
                    condition_id_b256: opt_str(strings[3], i).map(str::to_string),
                    underlying: req_str(strings[4], i)?.to_string(),
                    up_token: U256::from_str(req_str(strings[5], i)?)?,
                    down_token: U256::from_str(req_str(strings[6], i)?)?,
                    window_start_ms: req_i64(win_start, i)? as u64,
                    window_end_ms: req_i64(win_end, i)? as u64,
                    maker_base_fee_bps: opt_i64(maker, i).map(|v| v as i32),
                    taker_base_fee_bps: opt_i64(taker, i).map(|v| v as i32),
                    minimum_tick_size: opt_f64(min_tick, i),
                    minimum_order_size: opt_f64(min_order, i),
                    rewards_min_size: opt_f64(rewards_min, i),
                    rewards_max_spread: opt_f64(rewards_spread, i),
                    resolution_source: opt_str(resolution_source, i).map(str::to_string),
                    neg_risk: opt_i64(neg_risk, i).map(|v| v != 0),
                    standard_binary_ctf: req_i64(standard, i)? != 0,
                    question: opt_str(question, i).map(str::to_string),
                    slug: opt_str(slug, i).map(str::to_string),
                    up_lifecycle: req_str(up_lifecycle, i)?.to_string(),
                    down_lifecycle: req_str(down_lifecycle, i)?.to_string(),
                    itode: opt_i64(itode, i).map(|v| v != 0),
                    market_frame_received_at: None,
                    market_state_published_at: None,
                    market_connection_session_id: None,
                    market_frame_sequence: None,
                    market_state_revision: None,
                },
            });
        }
    }
    Ok(rows)
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn read_lifecycle_rows(
    path: &Path,
) -> Result<Vec<LifecycleRow>> {
    let mut rows = Vec::new();
    for b in read_batches(path)? {
        let kind = str_col(&b, 0, "kind")?;
        let token = str_col(&b, 1, "token_id")?;
        let lifecycle = str_col(&b, 2, "lifecycle")?;
        let sync_state = str_col(&b, 3, "sync_state")?;
        let sync_epoch = int_col(&b, 4, "sync_epoch")?;
        let rotation_epoch = int_col(&b, 5, "rotation_epoch")?;
        let updated_at = int_col(&b, 6, "updated_at_ms")?;
        let rotation_started = int_col(&b, 7, "rotation_started_at_ms")?;
        let last_error = str_col(&b, 8, "last_error")?;
        let last_error_at = int_col(&b, 9, "last_error_at_ms")?;
        let active_tokens = int_col(&b, 10, "active_tokens")?;
        let pending_revalidation = int_col(&b, 11, "pending_revalidation_tokens")?;
        let pending_subscription = int_col(&b, 12, "pending_subscription_tokens")?;
        let invalid_after_404 = int_col(&b, 13, "invalid_after_404_tokens")?;
        let resolved_tokens = int_col(&b, 14, "resolved_tokens")?;
        let quarantined = int_col(&b, 15, "quarantined_from_entry_tokens")?;
        let recent_total = int_col(&b, 16, "recent_book_404_total")?;
        let recent = str_col(&b, 17, "recent_book_404_tokens")?;
        for i in 0..b.num_rows() {
            rows.push(LifecycleRow {
                kind: req_str(kind, i)?.to_string(),
                token_id: opt_str(token, i).unwrap_or_default().to_string(),
                lifecycle: opt_str(lifecycle, i).unwrap_or_default().to_string(),
                sync_state: opt_str(sync_state, i).unwrap_or_default().to_string(),
                sync_epoch: opt_i64(sync_epoch, i).unwrap_or(0) as u64,
                rotation_epoch: opt_i64(rotation_epoch, i).unwrap_or(0) as u64,
                updated_at_ms: opt_i64(updated_at, i).map(|v| v as u64),
                rotation_started_at_ms: opt_i64(rotation_started, i).map(|v| v as u64),
                last_error: opt_str(last_error, i).map(str::to_string),
                last_error_at_ms: opt_i64(last_error_at, i).map(|v| v as u64),
                active_tokens: opt_i64(active_tokens, i).unwrap_or(0) as usize,
                pending_revalidation_tokens: opt_i64(pending_revalidation, i).unwrap_or(0) as usize,
                pending_subscription_tokens: opt_i64(pending_subscription, i).unwrap_or(0) as usize,
                invalid_after_404_tokens: opt_i64(invalid_after_404, i).unwrap_or(0) as usize,
                resolved_tokens: opt_i64(resolved_tokens, i).unwrap_or(0) as usize,
                quarantined_from_entry_tokens: opt_i64(quarantined, i).unwrap_or(0) as usize,
                recent_book_404_total: opt_i64(recent_total, i).unwrap_or(0) as u64,
                recent_book_404_tokens: opt_str(recent, i).unwrap_or_default().to_string(),
            });
        }
    }
    Ok(rows)
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn int_col<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("{name} column {idx} is not int64"))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn float_col<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a Float64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("{name} column {idx} is not float64"))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn str_col<'a>(
    batch: &'a RecordBatch,
    idx: usize,
    name: &str,
) -> Result<&'a StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("{name} column {idx} is not utf8"))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn req_i64(
    array: &Int64Array,
    idx: usize,
) -> Result<i64> {
    if array.is_null(idx) {
        Err(anyhow!("int64 null row {idx}"))
    } else {
        Ok(array.value(idx))
    }
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn req_f64(
    array: &Float64Array,
    idx: usize,
) -> Result<f64> {
    if array.is_null(idx) {
        Err(anyhow!("float64 null row {idx}"))
    } else {
        Ok(array.value(idx))
    }
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn req_str(
    array: &StringArray,
    idx: usize,
) -> Result<&str> {
    if array.is_null(idx) {
        Err(anyhow!("utf8 null row {idx}"))
    } else {
        Ok(array.value(idx))
    }
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn opt_i64(
    array: &Int64Array,
    idx: usize,
) -> Option<i64> {
    (!array.is_null(idx)).then(|| array.value(idx))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn opt_f64(
    array: &Float64Array,
    idx: usize,
) -> Option<f64> {
    (!array.is_null(idx)).then(|| array.value(idx))
}
pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn opt_str(
    array: &StringArray,
    idx: usize,
) -> Option<&str> {
    (!array.is_null(idx)).then(|| array.value(idx))
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow::io) fn trade_side_label(
    side: TradeSide,
) -> &'static str {
    match side {
        TradeSide::Buy => "BUY",
        TradeSide::Sell => "SELL",
    }
}

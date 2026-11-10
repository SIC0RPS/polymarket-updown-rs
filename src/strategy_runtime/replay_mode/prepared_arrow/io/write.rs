use super::read::trade_side_label;
use super::schema::empty_schema;
use super::*;

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn clock_batch(
    schema: SchemaRef,
    rows: &[ClockRow],
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.session_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.source_priority as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.sequence as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.updated_at_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.revision as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.market_start).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.market_len).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.book_start).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.book_len).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.lifecycle_start).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.lifecycle_len).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.consumption_start).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.consumption_len).collect::<Vec<_>>(),
            )),
        ],
    )
    .context("build engine_clock batch")
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn market_batch(
    schema: SchemaRef,
    rows: &[MarketRow],
) -> Result<RecordBatch> {
    let m = |f: fn(&bbwick_engine::state::MarketSelectionSnapshot) -> &String| {
        rows.iter()
            .map(|r| f(&r.market).as_str())
            .collect::<Vec<_>>()
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.role.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.updated_at_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.revision as i64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.selection_updated_at_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.next_selection_updated_at_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(m(|x| &x.market_id))),
            Arc::new(StringArray::from(m(|x| &x.condition_id))),
            Arc::new(StringArray::from(m(|x| &x.gamma_market_id))),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.condition_id_b256.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(m(|x| &x.underlying))),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.up_token.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.down_token.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.window_start_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.window_end_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.maker_base_fee_bps.map(i64::from))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.taker_base_fee_bps.map(i64::from))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.market.minimum_tick_size)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.market.minimum_order_size)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.market.rewards_min_size)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.market.rewards_max_spread)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.resolution_source.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.neg_risk.map(|v| if v { 1 } else { 0 }))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| if r.market.standard_binary_ctf { 1 } else { 0 })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.question.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.market.slug.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(m(|x| &x.up_lifecycle))),
            Arc::new(StringArray::from(m(|x| &x.down_lifecycle))),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.market.itode.map(|v| if v { 1 } else { 0 }))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .context("build market_delta batch")
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn book_batch(
    schema: SchemaRef,
    rows: &[BookRow],
) -> Result<RecordBatch> {
    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| if r.remove { "remove" } else { "book" })
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| (!r.token_id.is_empty()).then_some(r.token_id.as_str()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| (!r.remove).then_some(r.bid).flatten())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| (!r.remove).then_some(r.ask).flatten())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.bid_sz).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.ask_sz).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| (!r.remove).then_some(r.tick_size))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| (!r.remove).then_some(r.min_order_size))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter()
                .map(|r| Some(r.ts_ms as i64))
                .collect::<Vec<_>>(),
        )),
    ];
    for level in 0..STORED_DEPTH_LEVELS {
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| depth_level(r, true, level).map(|level| level.price))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| depth_level(r, true, level).map(|level| level.size))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| depth_level(r, false, level).map(|level| level.price))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|r| depth_level(r, false, level).map(|level| level.size))
                .collect::<Vec<_>>(),
        )));
    }
    columns.push(Arc::new(Int64Array::from(
        rows.iter()
            .map(|row| row.depth.as_ref().map(|depth| depth.revision as i64))
            .collect::<Vec<_>>(),
    )));
    RecordBatch::try_new(schema, columns).context("build token_book_delta batch")
}

fn depth_level(row: &BookRow, bids: bool, idx: usize) -> Option<&PriceLevel> {
    let depth = row.depth.as_ref()?;
    if bids {
        depth.bids.get(idx)
    } else {
        depth.asks.get(idx)
    }
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn lifecycle_batch(
    schema: SchemaRef,
    rows: &[LifecycleRow],
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.kind.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| (!r.token_id.is_empty()).then_some(r.token_id.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| (!r.lifecycle.is_empty()).then_some(r.lifecycle.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| (!r.sync_state.is_empty()).then_some(r.sync_state.as_str()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.sync_epoch as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.rotation_epoch as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.updated_at_ms.map(|v| v as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.rotation_started_at_ms.map(|v| v as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.last_error.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.last_error_at_ms.map(|v| v as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.active_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.pending_revalidation_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.pending_subscription_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.invalid_after_404_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.resolved_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.quarantined_from_entry_tokens as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.kind == "status").then_some(r.recent_book_404_total as i64))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| {
                        (!r.recent_book_404_tokens.is_empty())
                            .then_some(r.recent_book_404_tokens.as_str())
                    })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .context("build lifecycle_delta batch")
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn last_trade_batch(
    schema: SchemaRef,
    rows: &[LastTradeRow],
) -> Result<RecordBatch> {
    let token_ids = rows
        .iter()
        .map(|row| row.token_id.to_string())
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.session_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.seq as i64).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                token_ids.iter().map(String::as_str).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.price).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.size).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.side.map(trade_side_label))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.trade_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .context("build last_trade_delta batch")
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn open_writer(
    path: &Path,
    schema: &SchemaRef,
) -> Result<FileWriter<File>> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    FileWriter::try_new_with_options(file, schema, crate::capture::arrow::zstd_write_options()?)
        .with_context(|| format!("create arrow writer {}", path.display()))
}

pub(in crate::strategy_runtime::replay_mode::prepared_arrow) fn write_empty(
    path: &Path,
) -> Result<()> {
    let schema = empty_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .context("build empty prepared batch")?;
    let mut writer = open_writer(path, &schema)?;
    writer
        .write(&batch)
        .with_context(|| format!("write {}", path.display()))?;
    writer
        .finish()
        .with_context(|| format!("finish {}", path.display()))
}

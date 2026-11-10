use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Array, Float64Array, Int64Array, StructArray, UInt32Array, UInt64Array, UInt8Array};
use bbwick_engine::OracleCandleWindow;

use super::{
    column, decode_candle_state, fixed_required, list_structs, optional, required,
    required_value, schema, string_required, struct_array, struct_required,
};
use crate::capture::events::{
    StrategyConsumptionInput, StrategyConsumptionKind, StrategyConsumptionRecord,
};
use crate::oracle_resolution::ChainlinkResolutionPrice;
use crate::strategy_runtime::runtime_driver::RuntimeInput;
use crate::types::{MarketTick, MarketTickTrigger, SourceTsKind};
use crate::strategy_runtime::symbol_process::SymbolId;

pub(in super::super) fn decode_earliest_input_ms(
    root: &StructArray,
    row: usize,
    expected_symbol: SymbolId,
    expected_sequence: u64,
    expected_timestamp_ms: u64,
) -> Result<u64> {
    anyhow::ensure!(
        row < root.len(),
        "strategy consumption row exceeds active batch"
    );
    let actor = SymbolId::try_from(required::<UInt8Array>(root, schema::ACTOR, row)?)?;
    anyhow::ensure!(
        actor == expected_symbol,
        "strategy consumption row has foreign symbol"
    );
    let input = decode_input(root, row)?;
    let (_, sequence, _, _, _) = decode_candle_boundary(root, row, expected_symbol)?;
    let consumed_at_ms = required::<UInt64Array>(root, schema::CONSUMED_AT, row)?;
    anyhow::ensure!(
        sequence == expected_sequence && consumed_at_ms == expected_timestamp_ms,
        "strategy consumption EventRef differs from captured sequence or timestamp expected_sequence={expected_sequence} actual_sequence={sequence} expected_timestamp_ms={expected_timestamp_ms} actual_timestamp_ms={consumed_at_ms}"
    );

    let mut earliest = consumed_at_ms;
    match &input {
        StrategyConsumptionInput::Runtime(input) => {
            let tick = input.tick();
            include(&mut earliest, tick.ts_ns.max(0) as u64 / 1_000_000);
            include(&mut earliest, tick.source_ts_ns.max(0) as u64 / 1_000_000);
        }
        StrategyConsumptionInput::Chainlink {
            price,
            learning_applied_ts_ms,
        } => {
            include(&mut earliest, price.payload_ts_ms);
            include(&mut earliest, price.observed_ts_ms);
            include(&mut earliest, *learning_applied_ts_ms);
        }
    }
    for timestamp_ms in [
        required::<UInt64Array>(root, schema::ODDS_UPDATED, row)?,
        required::<UInt64Array>(root, schema::SELECTION_UPDATED, row)?,
        required::<UInt64Array>(root, schema::NEXT_SELECTION_UPDATED, row)?,
    ] {
        include(&mut earliest, timestamp_ms);
    }
    for book_column in [schema::UP, schema::DOWN, schema::NEXT_UP, schema::NEXT_DOWN] {
        let book = struct_array(root, book_column)?;
        if !book.is_null(row) {
            include(
                &mut earliest,
                required::<UInt64Array>(book, schema::BOOK_TS, row)?,
            );
            include(
                &mut earliest,
                required::<UInt64Array>(book, schema::BOOK_OBSERVED_AT, row)?,
            );
        }
    }
    include_map_clocks(
        root,
        schema::BOOKS,
        schema::BOOK_TS,
        schema::BOOK_OBSERVED_AT,
        row,
        &mut earliest,
    )?;
    include_map_clocks(
        root,
        schema::DEPTHS,
        schema::DEPTH_TS,
        schema::DEPTH_OBSERVED_AT,
        row,
        &mut earliest,
    )?;
    Ok(earliest)
}

pub(in super::super) fn decode_candle_boundary(
    root: &StructArray,
    row: usize,
    expected_symbol: SymbolId,
) -> Result<(u32, u64, u64, StrategyConsumptionKind, Option<Arc<[Arc<OracleCandleWindow>]>>)> {
    anyhow::ensure!(row < root.len(), "strategy consumption row exceeds active batch");
    let actor = SymbolId::try_from(required::<UInt8Array>(root, schema::ACTOR, row)?)?;
    anyhow::ensure!(actor == expected_symbol, "strategy consumption row has foreign symbol");
    let version = required::<UInt32Array>(root, schema::SCHEMA_VERSION, row)?;
    let sequence = required::<UInt64Array>(root, schema::SEQUENCE, row)?;
    anyhow::ensure!(sequence > 0, "strategy consumption sequence is zero");
    let clock = required::<UInt64Array>(root, schema::CONSUMED_MONO, row)?;
    let state = decode_candle_state(root, row)?;
    StrategyConsumptionRecord::validate_schema_fields(
        version,
        clock,
        state.as_ref().map(|state| state.len()),
        decode_trade_flow(root, row)?.as_ref(),
    )?;
    let kind = match required::<UInt8Array>(root, schema::INPUT_KIND, row)? {
        0 => StrategyConsumptionKind::Runtime,
        1 => StrategyConsumptionKind::Chainlink,
        other => anyhow::bail!("invalid strategy consumption input kind={other}"),
    };
    Ok((version, sequence, clock, kind, state))
}

pub(super) fn decode_trade_flow(
    root: &StructArray,
    row: usize,
) -> Result<Option<bbwick_engine::OracleTradeFlowSnapshot>> {
    use bbwick_engine::{OracleTradeFlowCoverage, OracleTradeFlowSnapshot};
    let value = struct_array(root, schema::TRADE_FLOW)?;
    if value.is_null(row) {
        return Ok(None);
    }
    let coverage = match required::<UInt8Array>(value, 4, row)? {
        0 => OracleTradeFlowCoverage::Complete,
        1 => OracleTradeFlowCoverage::NoVolume,
        2 => OracleTradeFlowCoverage::WarmingUp,
        3 => OracleTradeFlowCoverage::Disconnected,
        4 => OracleTradeFlowCoverage::Discontinuity,
        5 => OracleTradeFlowCoverage::Malformed,
        6 => OracleTradeFlowCoverage::Overflow,
        other => anyhow::bail!("invalid consumed trade-flow coverage={other}"),
    };
    Ok(Some(OracleTradeFlowSnapshot {
        window_end_mono_ns: required::<UInt64Array>(value, 0, row)?,
        coverage_started_mono_ns: required::<UInt64Array>(value, 1, row)?,
        buy_quantity: required::<Float64Array>(value, 2, row)?,
        sell_quantity: required::<Float64Array>(value, 3, row)?,
        coverage,
        revision: required::<UInt64Array>(value, 5, row)?,
        received_mono_ns: required::<UInt64Array>(value, 6, row)?,
        published_mono_ns: required::<UInt64Array>(value, 7, row)?,
        last_aggregate_id: optional::<UInt64Array>(value, 8, row)?,
        event_ts_ms: optional::<UInt64Array>(value, 9, row)?,
        trade_ts_ms: optional::<UInt64Array>(value, 10, row)?,
    }))
}

pub(super) fn decode_input(root: &StructArray, row: usize) -> Result<StrategyConsumptionInput> {
    match required::<UInt8Array>(root, schema::INPUT_KIND, row)? {
        0 => Ok(StrategyConsumptionInput::Runtime(decode_runtime(
            root, row,
        )?)),
        1 => {
            let value = struct_required(root, schema::CHAINLINK, row)?;
            let raw_value: [u8; 16] = fixed_required(value, 5, row)?.try_into()?;
            Ok(StrategyConsumptionInput::Chainlink {
                price: ChainlinkResolutionPrice {
                    symbol: string_required(value, 0, row)?.to_string(),
                    topic: string_required(value, 1, row)?.to_string(),
                    window_s: required::<UInt64Array>(value, 2, row)?,
                    payload_ts_ms: required::<UInt64Array>(value, 3, row)?,
                    observed_ts_ms: required::<UInt64Array>(value, 4, row)?,
                    value_e18: i128::from_le_bytes(raw_value),
                    capture_sequence: required::<UInt64Array>(value, 6, row)?,
                },
                learning_applied_ts_ms: required::<UInt64Array>(
                    root,
                    schema::CHAINLINK_LEARNING_TS,
                    row,
                )?,
            })
        }
        other => anyhow::bail!("invalid strategy consumption input kind={other}"),
    }
}

fn decode_runtime(root: &StructArray, row: usize) -> Result<RuntimeInput> {
    let tick = decode_tick(struct_required(root, schema::TICK, row)?, row)?;
    let now = || -> Result<(u64, u64)> {
        Ok((
            required::<UInt64Array>(root, schema::INPUT_NOW, row)?,
            required::<UInt64Array>(root, schema::INPUT_NOW_MONO, row)?,
        ))
    };
    Ok(
        match required::<UInt8Array>(root, schema::RUNTIME_KIND, row)? {
            0 => RuntimeInput::AcceptedTick { tick },
            1 => {
                let (now_ms, now_ingest_mono_ns) = now()?;
                RuntimeInput::Heartbeat {
                    tick,
                    now_ms,
                    now_ingest_mono_ns,
                }
            }
            2 => {
                let (now_ms, now_ingest_mono_ns) = now()?;
                RuntimeInput::PortfolioChanged {
                    last_tick: tick,
                    now_ms,
                    now_ingest_mono_ns,
                }
            }
            3 => {
                let (now_ms, now_ingest_mono_ns) = now()?;
                RuntimeInput::PolymarketDataChanged {
                    last_tick: tick,
                    now_ms,
                    now_ingest_mono_ns,
                }
            }
            other => anyhow::bail!("invalid runtime input kind={other}"),
        },
    )
}

fn decode_tick(value: &StructArray, row: usize) -> Result<MarketTick> {
    Ok(MarketTick {
        instrument: string_required(value, schema::TICK_INSTRUMENT, row)?.to_string(),
        bid: required::<Float64Array>(value, schema::TICK_BID, row)?,
        ask: required::<Float64Array>(value, schema::TICK_ASK, row)?,
        bid_sz: optional::<Float64Array>(value, schema::TICK_BID_SIZE, row)?,
        ask_sz: optional::<Float64Array>(value, schema::TICK_ASK_SIZE, row)?,
        queue_imbalance_l5: optional::<Float64Array>(value, schema::TICK_IMBALANCE, row)?,
        source_ts_ns: required::<Int64Array>(value, schema::TICK_SOURCE_TS, row)?,
        source_ts_kind: match required::<UInt8Array>(value, schema::TICK_SOURCE_KIND, row)? {
            0 => SourceTsKind::Unavailable,
            1 => SourceTsKind::Exchange,
            2 => SourceTsKind::ProviderEvent,
            3 => SourceTsKind::EngineIngestWallclock,
            other => anyhow::bail!("invalid tick source timestamp kind={other}"),
        },
        ingest_mono_ns: required::<UInt64Array>(value, schema::TICK_INGEST_MONO, row)?,
        ts_ns: required::<Int64Array>(value, schema::TICK_TS, row)?,
        trigger: match required::<UInt8Array>(value, schema::TICK_TRIGGER, row)? {
            0 => MarketTickTrigger::MarketData,
            1 => MarketTickTrigger::OracleOnly,
            other => anyhow::bail!("invalid market tick trigger={other}"),
        },
    })
}

fn include_map_clocks(
    root: &StructArray,
    list_column: usize,
    timestamp_column: usize,
    observed_column: usize,
    row: usize,
    earliest: &mut u64,
) -> Result<()> {
    let (entries, range) = list_structs(root, list_column, row)?;
    let values = struct_array(entries, 1)?;
    let timestamps = column::<UInt64Array>(values, timestamp_column)?;
    let observed = column::<UInt64Array>(values, observed_column)?;
    for index in range {
        include(
            earliest,
            required_value(timestamps, timestamp_column, index)?,
        );
        include(earliest, required_value(observed, observed_column, index)?);
    }
    Ok(())
}

fn include(earliest: &mut u64, timestamp_ms: u64) {
    if timestamp_ms > 0 {
        *earliest = (*earliest).min(timestamp_ms);
    }
}

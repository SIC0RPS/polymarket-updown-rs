use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
    StringArray, StructArray, UInt32Array, UInt64Array, UInt8Array,
};
use polymarket_client_sdk::types::{B256, U256};

use super::schema;
use crate::capture::events::StrategyConsumptionRecord;
use crate::polymarket::state::{
    BookDepth, BookTop, LastTrade, PolymarketLifecycleStatus, PolymarketOddsState, PriceLevel,
    TradeSide,
};
use crate::strategy_runtime::symbol_process::SymbolId;
use bbwick_engine::state::{CapturedMarketResolutionContext, Fingerprint, LastCanonicalBid};
use bbwick_engine::{MarketSelectionSnapshot, OracleCandleWindow};
use canonical_ohlc_arrow::OhlcBar;

#[path = "read/input_boundary.rs"]
mod input_boundary;
pub(super) use input_boundary::{decode_candle_boundary, decode_earliest_input_ms};
use input_boundary::decode_input;

pub(super) fn decode_record(
    root: &StructArray,
    row: usize,
    expected_symbol: SymbolId,
) -> Result<StrategyConsumptionRecord> {
    let (schema_version, consumption_sequence, consumed_mono_ns, _, candle_state) =
        decode_candle_boundary(root, row, expected_symbol)?;
    let odds = PolymarketOddsState {
        updated_at_ms: required::<UInt64Array>(root, schema::ODDS_UPDATED, row)?,
        revision: required::<UInt64Array>(root, schema::ODDS_REVISION, row)?,
        trigger_fingerprint: decode_fingerprint(root, row)?,
        selection_updated_at_ms: required::<UInt64Array>(root, schema::SELECTION_UPDATED, row)?,
        next_selection_updated_at_ms: required::<UInt64Array>(
            root,
            schema::NEXT_SELECTION_UPDATED,
            row,
        )?,
        token_lifecycle: decode_token_lifecycle(root, row)?,
        resolved_market_winners: decode_resolved_winners(root, row)?,
        resolved_market_contexts: decode_resolved_contexts(root, row)?,
        polymarket_lifecycle: decode_lifecycle(root, row)?,
        selection: decode_optional_market(root, schema::SELECTION, row)?,
        next_selection: decode_optional_market(root, schema::NEXT_SELECTION, row)?,
        markets: decode_markets(root, row)?,
        up: decode_optional_book(root, schema::UP, row)?,
        down: decode_optional_book(root, schema::DOWN, row)?,
        next_up: decode_optional_book(root, schema::NEXT_UP, row)?,
        next_down: decode_optional_book(root, schema::NEXT_DOWN, row)?,
        books: decode_books(root, row)?,
        depths: decode_depths(root, row)?,
        last_canonical_bids: decode_last_canonical_bids(root, row)?,
        last_trades: decode_last_trades(root, row)?,
        next_last_trade_seq: required::<UInt64Array>(root, schema::NEXT_LAST_TRADE_SEQUENCE, row)?,
    };
    let input = decode_input(root, row)?;
    Ok(StrategyConsumptionRecord {
        schema_version,
        consumption_sequence,
        consumed_at_ms: required::<UInt64Array>(root, schema::CONSUMED_AT, row)?,
        consumed_mono_ns,
        candle_state,
        trade_flow: input_boundary::decode_trade_flow(root, row)?,
        actor_symbol: expected_symbol,
        odds: Arc::new(odds),
        input,
    })
}

fn decode_fingerprint(root: &StructArray, row: usize) -> Result<Fingerprint> {
    let value = struct_required(root, schema::FINGERPRINT, row)?;
    Ok(Fingerprint {
        count: required::<UInt64Array>(value, 0, row)?,
        xor: required::<UInt64Array>(value, 1, row)?,
        sum: required::<UInt64Array>(value, 2, row)?,
    })
}

fn decode_token_lifecycle(root: &StructArray, row: usize) -> Result<HashMap<U256, String>> {
    let (values, range) = list_structs(root, schema::TOKEN_LIFECYCLE, row)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let token = token(values, 0, index)?;
        let lifecycle = string_required(values, 1, index)?.to_string();
        anyhow::ensure!(
            out.insert(token, lifecycle).is_none(),
            "duplicate odds token lifecycle identity"
        );
    }
    Ok(out)
}

fn decode_resolved_winners(root: &StructArray, row: usize) -> Result<HashMap<B256, U256>> {
    let (values, range) = list_structs(root, schema::RESOLVED_WINNERS, row)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let condition = condition(values, 0, index)?;
        let winner = token(values, 1, index)?;
        anyhow::ensure!(
            out.insert(condition, winner).is_none(),
            "duplicate odds resolved winner identity"
        );
    }
    Ok(out)
}

fn decode_resolved_contexts(
    root: &StructArray,
    row: usize,
) -> Result<HashMap<B256, CapturedMarketResolutionContext>> {
    let (values, range) = list_structs(root, schema::RESOLVED_CONTEXTS, row)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let condition = condition(values, 0, index)?;
        let context = CapturedMarketResolutionContext {
            up_token: token(values, 1, index)?,
            down_token: token(values, 2, index)?,
            observed_at_ms: required::<UInt64Array>(values, 3, index)?,
            raw_response_sha256: fixed_required(values, 4, index)?.try_into()?,
        };
        anyhow::ensure!(
            out.insert(condition, context).is_none(),
            "duplicate odds resolved context identity"
        );
    }
    Ok(out)
}

fn decode_lifecycle(root: &StructArray, row: usize) -> Result<PolymarketLifecycleStatus> {
    let value = struct_required(root, schema::LIFECYCLE_STATUS, row)?;
    let (recent_values, recent) = list_strings(value, 14, row)?;
    let mut recent_book_404_tokens = Vec::with_capacity(recent.len());
    for index in recent {
        anyhow::ensure!(
            !recent_values.is_null(index),
            "typed string list value is null"
        );
        recent_book_404_tokens.push(recent_values.value(index).to_string());
    }
    Ok(PolymarketLifecycleStatus {
        sync_state: string_required(value, 0, row)?.to_string(),
        sync_epoch: required::<UInt64Array>(value, 1, row)?,
        rotation_epoch: required::<UInt64Array>(value, 2, row)?,
        updated_at_ms: optional::<UInt64Array>(value, 3, row)?,
        rotation_started_at_ms: optional::<UInt64Array>(value, 4, row)?,
        last_error: string_optional(value, 5, row)?.map(str::to_string),
        last_error_at_ms: optional::<UInt64Array>(value, 6, row)?,
        active_tokens: usize::try_from(required::<UInt64Array>(value, 7, row)?)?,
        pending_revalidation_tokens: usize::try_from(required::<UInt64Array>(value, 8, row)?)?,
        pending_subscription_tokens: usize::try_from(required::<UInt64Array>(value, 9, row)?)?,
        invalid_after_404_tokens: usize::try_from(required::<UInt64Array>(value, 10, row)?)?,
        resolved_tokens: usize::try_from(required::<UInt64Array>(value, 11, row)?)?,
        quarantined_from_entry_tokens: usize::try_from(required::<UInt64Array>(value, 12, row)?)?,
        recent_book_404_total: required::<UInt64Array>(value, 13, row)?,
        recent_book_404_tokens,
    })
}

fn decode_optional_market(
    root: &StructArray,
    column: usize,
    row: usize,
) -> Result<Option<MarketSelectionSnapshot>> {
    let value = struct_array(root, column)?;
    if value.is_null(row) {
        return Ok(None);
    }
    Ok(Some(decode_market(value, row)?))
}

fn decode_markets(root: &StructArray, row: usize) -> Result<Vec<MarketSelectionSnapshot>> {
    let (values, range) = list_structs(root, schema::MARKETS, row)?;
    let mut out = Vec::with_capacity(range.len());
    for index in range {
        out.push(decode_market(values, index)?);
    }
    Ok(out)
}

fn decode_market(value: &StructArray, row: usize) -> Result<MarketSelectionSnapshot> {
    Ok(MarketSelectionSnapshot {
        market_id: string_required(value, schema::MARKET_ID, row)?.to_string(),
        condition_id: string_required(value, schema::MARKET_CONDITION_ID, row)?.to_string(),
        gamma_market_id: string_required(value, schema::MARKET_GAMMA_ID, row)?.to_string(),
        condition_id_b256: string_optional(value, schema::MARKET_CONDITION_B256, row)?
            .map(str::to_string),
        underlying: string_required(value, schema::MARKET_UNDERLYING, row)?.to_string(),
        up_token: token(value, schema::MARKET_UP_TOKEN, row)?,
        down_token: token(value, schema::MARKET_DOWN_TOKEN, row)?,
        window_start_ms: required::<UInt64Array>(value, schema::MARKET_WINDOW_START, row)?,
        window_end_ms: required::<UInt64Array>(value, schema::MARKET_WINDOW_END, row)?,
        maker_base_fee_bps: optional::<Int32Array>(value, schema::MARKET_MAKER_FEE, row)?,
        taker_base_fee_bps: optional::<Int32Array>(value, schema::MARKET_TAKER_FEE, row)?,
        minimum_tick_size: optional::<Float64Array>(value, schema::MARKET_MIN_TICK, row)?,
        minimum_order_size: optional::<Float64Array>(value, schema::MARKET_MIN_ORDER, row)?,
        rewards_min_size: optional::<Float64Array>(value, schema::MARKET_REWARDS_MIN, row)?,
        rewards_max_spread: optional::<Float64Array>(value, schema::MARKET_REWARDS_SPREAD, row)?,
        resolution_source: string_optional(value, schema::MARKET_RESOLUTION_SOURCE, row)?
            .map(str::to_string),
        neg_risk: optional::<BooleanArray>(value, schema::MARKET_NEG_RISK, row)?,
        itode: optional::<BooleanArray>(value, schema::MARKET_ITODE, row)?,
        standard_binary_ctf: required::<BooleanArray>(
            value,
            schema::MARKET_STANDARD_BINARY_CTF,
            row,
        )?,
        question: string_optional(value, schema::MARKET_QUESTION, row)?.map(str::to_string),
        slug: string_optional(value, schema::MARKET_SLUG, row)?.map(str::to_string),
        up_lifecycle: string_required(value, schema::MARKET_UP_LIFECYCLE, row)?.to_string(),
        down_lifecycle: string_required(value, schema::MARKET_DOWN_LIFECYCLE, row)?.to_string(),
        market_frame_received_at: None,
        market_state_published_at: None,
        market_connection_session_id: None,
        market_frame_sequence: None,
        market_state_revision: None,
    })
}

fn decode_optional_book(root: &StructArray, column: usize, row: usize) -> Result<Option<BookTop>> {
    let value = struct_array(root, column)?;
    if value.is_null(row) {
        return Ok(None);
    }
    Ok(Some(decode_book(value, row)?))
}

fn decode_books(root: &StructArray, row: usize) -> Result<HashMap<U256, BookTop>> {
    let (entries, range) = list_structs(root, schema::BOOKS, row)?;
    let keys = column::<FixedSizeBinaryArray>(entries, 0)?;
    let values = struct_array(entries, 1)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let key = token_value(keys, 0, index)?;
        anyhow::ensure!(
            out.insert(key, decode_book(values, index)?).is_none(),
            "duplicate odds book map key"
        );
    }
    Ok(out)
}

fn decode_book(value: &StructArray, row: usize) -> Result<BookTop> {
    Ok(BookTop {
        token_id: token(value, schema::BOOK_TOKEN, row)?,
        bid: optional::<Float64Array>(value, schema::BOOK_BID, row)?,
        ask: optional::<Float64Array>(value, schema::BOOK_ASK, row)?,
        bid_sz: optional::<Float64Array>(value, schema::BOOK_BID_SIZE, row)?,
        ask_sz: optional::<Float64Array>(value, schema::BOOK_ASK_SIZE, row)?,
        tick_size: required::<Float64Array>(value, schema::BOOK_TICK_SIZE, row)?,
        min_order_size: required::<Float64Array>(value, schema::BOOK_MIN_ORDER_SIZE, row)?,
        ts_ms: required::<UInt64Array>(value, schema::BOOK_TS, row)?,
        observed_at_ms: required::<UInt64Array>(value, schema::BOOK_OBSERVED_AT, row)?,
    })
}

fn decode_depths(root: &StructArray, row: usize) -> Result<HashMap<U256, BookDepth>> {
    let (entries, range) = list_structs(root, schema::DEPTHS, row)?;
    let keys = column::<FixedSizeBinaryArray>(entries, 0)?;
    let values = struct_array(entries, 1)?;
    let tokens = column::<FixedSizeBinaryArray>(values, schema::DEPTH_TOKEN)?;
    let timestamps = column::<UInt64Array>(values, schema::DEPTH_TS)?;
    let revisions = column::<UInt64Array>(values, schema::DEPTH_REVISION)?;
    let observed = column::<UInt64Array>(values, schema::DEPTH_OBSERVED_AT)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let key = token_value(keys, 0, index)?;
        let depth = BookDepth {
            token_id: token_value(tokens, schema::DEPTH_TOKEN, index)?,
            bids: decode_levels(values, schema::DEPTH_BIDS, index)?,
            asks: decode_levels(values, schema::DEPTH_ASKS, index)?,
            ts_ms: required_value(timestamps, schema::DEPTH_TS, index)?,
            revision: required_value(revisions, schema::DEPTH_REVISION, index)?,
            observed_at_ms: required_value(observed, schema::DEPTH_OBSERVED_AT, index)?,
        };
        anyhow::ensure!(
            out.insert(key, depth).is_none(),
            "duplicate odds depth map key"
        );
    }
    Ok(out)
}

fn decode_levels(value: &StructArray, column: usize, row: usize) -> Result<Vec<PriceLevel>> {
    let (values, range) = list_structs(value, column, row)?;
    let prices = self::column::<Float64Array>(values, 0)?;
    let sizes = self::column::<Float64Array>(values, 1)?;
    let mut out = Vec::with_capacity(range.len());
    for index in range {
        out.push(PriceLevel {
            price: required_value(prices, 0, index)?,
            size: required_value(sizes, 1, index)?,
        });
    }
    Ok(out)
}

fn decode_last_canonical_bids(
    root: &StructArray,
    row: usize,
) -> Result<HashMap<U256, LastCanonicalBid>> {
    let (values, range) = list_structs(root, schema::LAST_CANONICAL_BIDS, row)?;
    let tokens = column::<FixedSizeBinaryArray>(values, 0)?;
    let units = column::<UInt64Array>(values, 1)?;
    let timestamps = column::<UInt64Array>(values, 2)?;
    let observed = column::<UInt64Array>(values, 3)?;
    let mut out = HashMap::with_capacity(range.len());
    for index in range {
        let key = token_value(tokens, 0, index)?;
        let value = LastCanonicalBid {
            last_canonical_bid_units_8dp: required_value(units, 1, index)?,
            book_ts_ms: required_value(timestamps, 2, index)?,
            observed_at_ms: required_value(observed, 3, index)?,
        };
        anyhow::ensure!(
            out.insert(key, value).is_none(),
            "duplicate odds canonical bid map key"
        );
    }
    Ok(out)
}

fn decode_last_trades(root: &StructArray, row: usize) -> Result<VecDeque<LastTrade>> {
    let (values, range) = list_structs(root, schema::LAST_TRADES, row)?;
    let sequences = column::<UInt64Array>(values, 0)?;
    let tokens = column::<FixedSizeBinaryArray>(values, 1)?;
    let prices = column::<Float64Array>(values, 2)?;
    let sizes = column::<Float64Array>(values, 3)?;
    let sides = column::<UInt8Array>(values, 4)?;
    let timestamps = column::<UInt64Array>(values, 5)?;
    let mut out = VecDeque::with_capacity(range.len());
    for index in range {
        let side = match required_value(sides, 4, index)? {
            0 => None,
            1 => Some(TradeSide::Buy),
            2 => Some(TradeSide::Sell),
            other => anyhow::bail!("invalid captured last-trade side={other}"),
        };
        out.push_back(LastTrade {
            seq: required_value(sequences, 0, index)?,
            token_id: token_value(tokens, 1, index)?,
            price: required_value(prices, 2, index)?,
            size: required_value(sizes, 3, index)?,
            side,
            ts_ms: required_value(timestamps, 5, index)?,
        });
    }
    Ok(out)
}

fn decode_candle_state(
    root: &StructArray,
    row: usize,
) -> Result<Option<Arc<[Arc<OracleCandleWindow>]>>> {
    if column::<ListArray>(root, schema::CANDLE_STATE)?.is_null(row) {
        return Ok(None);
    }
    let (values, range) = list_structs(root, schema::CANDLE_STATE, row)?;
    let mut windows = Vec::with_capacity(range.len());
    for index in range {
        anyhow::ensure!(!values.is_null(index), "null consumed candle window");
        let (bar_values, bar_range) = list_structs(values, 5, index)?;
        let mut bars = Vec::with_capacity(bar_range.len());
        for bar_index in bar_range {
            anyhow::ensure!(!bar_values.is_null(bar_index), "null consumed candle bar");
            bars.push(OhlcBar {
                ts_ms: required::<Int64Array>(bar_values, 0, bar_index)?,
                open: required::<Float64Array>(bar_values, 1, bar_index)?,
                high: required::<Float64Array>(bar_values, 2, bar_index)?,
                low: required::<Float64Array>(bar_values, 3, bar_index)?,
                close: required::<Float64Array>(bar_values, 4, bar_index)?,
                volume: required::<Float64Array>(bar_values, 5, bar_index)?,
            });
        }
        windows.push(Arc::new(OracleCandleWindow {
            symbol: string_required(values, 0, index)?.to_string(),
            timeframe: string_required(values, 1, index)?.to_string(),
            revision: required::<UInt64Array>(values, 2, index)?,
            received_mono_ns: required::<UInt64Array>(values, 3, index)?,
            published_mono_ns: required::<UInt64Array>(values, 4, index)?,
            bars,
        }));
    }
    Ok(Some(Arc::from(windows)))
}

trait PrimitiveValue: Array {
    type Native: Copy;
    fn value_at(&self, row: usize) -> Self::Native;
}

macro_rules! primitive_value {
    ($array:ty, $native:ty) => {
        impl PrimitiveValue for $array {
            type Native = $native;
            fn value_at(&self, row: usize) -> Self::Native {
                self.value(row)
            }
        }
    };
}
primitive_value!(BooleanArray, bool);
primitive_value!(UInt8Array, u8);
primitive_value!(UInt32Array, u32);
primitive_value!(UInt64Array, u64);
primitive_value!(Int32Array, i32);
primitive_value!(Int64Array, i64);
primitive_value!(Float64Array, f64);

fn column<T: Array + 'static>(value: &StructArray, index: usize) -> Result<&T> {
    value
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("typed strategy consumption column mismatch index={index}"))
}

fn required<T: PrimitiveValue + 'static>(
    value: &StructArray,
    index: usize,
    row: usize,
) -> Result<T::Native> {
    let array = column::<T>(value, index)?;
    required_value(array, index, row)
}

#[inline(always)]
fn required_value<T: PrimitiveValue>(array: &T, index: usize, row: usize) -> Result<T::Native> {
    anyhow::ensure!(
        !array.is_null(row),
        "required typed value is null index={index}"
    );
    Ok(array.value_at(row))
}

fn optional<T: PrimitiveValue + 'static>(
    value: &StructArray,
    index: usize,
    row: usize,
) -> Result<Option<T::Native>> {
    let array = column::<T>(value, index)?;
    Ok((!array.is_null(row)).then(|| array.value_at(row)))
}

fn string_required<'a>(value: &'a StructArray, index: usize, row: usize) -> Result<&'a str> {
    let array = column::<StringArray>(value, index)?;
    anyhow::ensure!(
        !array.is_null(row),
        "required typed string is null index={index}"
    );
    Ok(array.value(row))
}

fn string_optional(value: &StructArray, index: usize, row: usize) -> Result<Option<&str>> {
    let array = column::<StringArray>(value, index)?;
    Ok((!array.is_null(row)).then(|| array.value(row)))
}

fn fixed_required<'a>(value: &'a StructArray, index: usize, row: usize) -> Result<&'a [u8]> {
    let array = column::<FixedSizeBinaryArray>(value, index)?;
    anyhow::ensure!(
        !array.is_null(row),
        "required fixed identity is null index={index}"
    );
    Ok(array.value(row))
}

fn token(value: &StructArray, index: usize, row: usize) -> Result<U256> {
    token_value(column::<FixedSizeBinaryArray>(value, index)?, index, row)
}

fn token_value(array: &FixedSizeBinaryArray, index: usize, row: usize) -> Result<U256> {
    let bytes: [u8; 32] = fixed_value(array, index, row)?.try_into()?;
    Ok(U256::from_be_bytes(bytes))
}

fn fixed_value(array: &FixedSizeBinaryArray, index: usize, row: usize) -> Result<&[u8]> {
    anyhow::ensure!(
        !array.is_null(row),
        "required fixed identity is null index={index}"
    );
    Ok(array.value(row))
}

fn condition(value: &StructArray, index: usize, row: usize) -> Result<B256> {
    let bytes: [u8; 32] = fixed_required(value, index, row)?.try_into()?;
    Ok(B256::from(bytes))
}

fn struct_array(value: &StructArray, index: usize) -> Result<&StructArray> {
    column(value, index)
}

fn struct_required<'a>(
    value: &'a StructArray,
    index: usize,
    row: usize,
) -> Result<&'a StructArray> {
    let child = struct_array(value, index)?;
    anyhow::ensure!(
        !child.is_null(row),
        "required typed struct is null index={index}"
    );
    Ok(child)
}

fn list_structs(
    value: &StructArray,
    index: usize,
    row: usize,
) -> Result<(&StructArray, Range<usize>)> {
    let list = column::<ListArray>(value, index)?;
    anyhow::ensure!(
        !list.is_null(row),
        "required typed list is null index={index}"
    );
    let offsets = list.value_offsets();
    let start = usize::try_from(offsets[row])?;
    let end = usize::try_from(offsets[row + 1])?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .context("typed list values are not structs")?;
    Ok((values, start..end))
}

fn list_strings(
    value: &StructArray,
    index: usize,
    row: usize,
) -> Result<(&StringArray, Range<usize>)> {
    let list = column::<ListArray>(value, index)?;
    anyhow::ensure!(
        !list.is_null(row),
        "required typed string list is null index={index}"
    );
    let offsets = list.value_offsets();
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .context("typed list values are not strings")?;
    Ok((
        values,
        usize::try_from(offsets[row])?..usize::try_from(offsets[row + 1])?,
    ))
}

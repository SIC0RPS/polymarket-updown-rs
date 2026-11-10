use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use arrow_array::{
    Array, BinaryArray, Int64Array, RecordBatch, StringArray, UInt16Array, UInt64Array,
};
use arrow_ipc::reader::FileReader;

use bbwick_engine::polymarket_ws::PolymarketWsIngressRecord;
use bbwick_engine::{OracleCandleWindow, OracleHistory};

use crate::capture::events::{
    PolymarketMarketWsIn, StrategyConsumptionInput, StrategyConsumptionKind, StrategyConsumptionRecord,
};
use crate::strategy_runtime::SymbolId;

pub struct StrategyConsumptionArrowReader {
    reader: FileReader<File>,
    rows: VecDeque<Vec<u8>>,
    schema_version: Option<u32>,
    last_sequence: Option<u64>,
    last_consumed_mono_ns: u64,
    candle_states: HashMap<(SymbolId, StrategyConsumptionKind), Arc<[Arc<OracleCandleWindow>]>>,
}

impl StrategyConsumptionArrowReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = FileReader::try_new(file, None)
            .with_context(|| format!("open finalized Arrow {}", path.display()))?;
        anyhow::ensure!(
            reader.schema().as_ref() == super::strategy_consumption_schema().as_ref(),
            "strategy consumption Arrow schema mismatch"
        );
        Ok(Self {
            reader,
            rows: VecDeque::new(),
            schema_version: None,
            last_sequence: None,
            last_consumed_mono_ns: 0,
            candle_states: HashMap::new(),
        })
    }

    pub fn next_event(&mut self) -> Result<Option<(StrategyConsumptionRecord, Vec<u8>)>> {
        loop {
            if let Some(payload) = self.rows.pop_front() {
                let record: StrategyConsumptionRecord = rmp_serde::from_slice(&payload)
                    .context("decode strategy consumption payload_msgpack")?;
                self.validate_record(&record)?;
                return Ok(Some((record, payload)));
            }
            let Some(batch) = self
                .reader
                .next()
                .transpose()
                .context("read strategy consumption Arrow")?
            else {
                return Ok(None);
            };
            let payloads = binary_col(&batch, 0, "payload_msgpack")?;
            for index in 0..batch.num_rows() {
                self.rows
                    .push_back(required_binary(payloads, index, "payload_msgpack")?.to_vec());
            }
        }
    }

    fn validate_record(&mut self, record: &StrategyConsumptionRecord) -> Result<()> {
        record.validate_schema()?;
        anyhow::ensure!(
            self.schema_version.is_none_or(|version| version == record.schema_version),
            "strategy consumption schemas are mixed"
        );
        anyhow::ensure!(
            record.consumption_sequence > 0
                && self.last_sequence.is_none_or(|last| record.consumption_sequence > last),
            "strategy consumption sequence is not increasing"
        );
        if record.schema_version > 1 {
            let key = (record.actor_symbol, record.input.kind());
            let previous = self.candle_states.get(&key);
            let state = record.candle_state.as_ref().or(previous)
                .context("missing initial consumed candle state")?;
            let expected = self.candle_states.iter()
                .find(|((actor, _), _)| *actor == record.actor_symbol)
                .map(|(_, state)| state).unwrap_or(state);
            let symbol = &expected.first().context("empty consumed candle key set")?.symbol;
            match &record.input {
                StrategyConsumptionInput::Runtime(input) => anyhow::ensure!(
                    input.tick().instrument == *symbol,
                    "consumed candle symbol differs from runtime input"
                ),
                StrategyConsumptionInput::Chainlink { price, .. } => anyhow::ensure!(
                    crate::oracle_resolution::canonical_chainlink_symbol(&price.symbol)?
                        == crate::oracle_resolution::canonical_chainlink_symbol(record.actor_symbol.as_str())?,
                    "Chainlink consumption actor mismatch"
                ),
            }
            let state = OracleHistory::resolve_consumed_state(
                previous, record.candle_state.as_ref(), symbol,
                expected.iter().map(|window| window.timeframe.as_str()),
                usize::MAX, self.last_consumed_mono_ns, record.consumed_mono_ns,
            )?;
            self.candle_states.insert(key, state);
        }
        self.schema_version = Some(record.schema_version);
        self.last_sequence = Some(record.consumption_sequence);
        self.last_consumed_mono_ns = record.consumed_mono_ns;
        Ok(())
    }

}

struct StrategyConsumptionMergeSource {
    path: PathBuf,
    expected_actor: Option<SymbolId>,
    expected_kind: Option<StrategyConsumptionKind>,
    reader: StrategyConsumptionArrowReader,
    next: Option<(StrategyConsumptionRecord, Vec<u8>)>,
}

impl StrategyConsumptionMergeSource {
    fn fill(&mut self) -> Result<()> {
        if self.next.is_some() {
            return Ok(());
        }
        let Some((record, payload)) = self
            .reader
            .next_event()
            .with_context(|| format!("read {}", self.path.display()))?
        else {
            return Ok(());
        };

        if let Some(expected_actor) = self.expected_actor {
            anyhow::ensure!(
                record.actor_symbol == expected_actor,
                "strategy consumption actor mismatch file={} expected={} actual={}",
                self.path.display(),
                expected_actor.as_str(),
                record.actor_symbol.as_str()
            );
        }
        if let Some(expected_kind) = self.expected_kind {
            anyhow::ensure!(
                record.input.kind() == expected_kind,
                "strategy consumption kind mismatch file={} expected={} actual={}",
                self.path.display(),
                expected_kind.as_str(),
                record.input.kind().as_str()
            );
        }
        self.next = Some((record, payload));
        Ok(())
    }
}

pub struct StrategyConsumptionMergedArrowReader {
    sources: Vec<StrategyConsumptionMergeSource>,
    last_sequence: Option<u64>,
    schema_version: Option<u32>,
    last_consumed_mono_ns: u64,
    candle_states: HashMap<SymbolId, Arc<[Arc<OracleCandleWindow>]>>,
}

impl StrategyConsumptionMergedArrowReader {
    pub fn open(
        sources: &[(PathBuf, Option<SymbolId>, Option<StrategyConsumptionKind>)],
    ) -> Result<Self> {
        anyhow::ensure!(
            !sources.is_empty(),
            "strategy consumption sources are empty"
        );
        let sources = sources
            .iter()
            .map(|(path, expected_actor, expected_kind)| {
                let reader = StrategyConsumptionArrowReader::open(path)
                    .with_context(|| format!("open required strategy source {}", path.display()))?;
                Ok(StrategyConsumptionMergeSource {
                    path: path.clone(),
                    expected_actor: *expected_actor,
                    expected_kind: *expected_kind,
                    reader,
                    next: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            sources,
            last_sequence: None,
            schema_version: None,
            last_consumed_mono_ns: 0,
            candle_states: HashMap::new(),
        })
    }

    pub fn next_event(&mut self) -> Result<Option<(StrategyConsumptionRecord, Vec<u8>)>> {
        for source in &mut self.sources {
            source.fill()?;
        }
        let Some((selected_index, selected_sequence)) = self
            .sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| {
                source
                    .next
                    .as_ref()
                    .map(|(record, _)| (index, record.consumption_sequence))
            })
            .min_by_key(|(_, sequence)| *sequence)
        else {
            return Ok(None);
        };
        let duplicate_count = self
            .sources
            .iter()
            .filter(|source| {
                source
                    .next
                    .as_ref()
                    .is_some_and(|(record, _)| record.consumption_sequence == selected_sequence)
            })
            .count();
        anyhow::ensure!(
            duplicate_count == 1,
            "duplicate global strategy consumption sequence={selected_sequence}"
        );
        anyhow::ensure!(
            self.last_sequence
                .is_none_or(|last| selected_sequence > last),
            "merged strategy consumption sequence is not increasing sequence={selected_sequence}"
        );
        let event = self.sources[selected_index]
            .next
            .take()
            .expect("selected strategy consumption event exists");
        let record = &event.0;
        anyhow::ensure!(
            self.schema_version.is_none_or(|version| version == record.schema_version),
            "merged strategy consumption schemas are mixed"
        );
        if record.schema_version > 1 {
            let current = self.sources[selected_index].reader.candle_states
                .get(&(record.actor_symbol, record.input.kind()))
                .context("merged consumption lost its source candle state")?;
            let previous = self.candle_states.get(&record.actor_symbol);
            let expected = previous.unwrap_or(current);
            let symbol = &expected.first().context("empty consumed candle key set")?.symbol;
            let state = OracleHistory::resolve_consumed_state(
                previous, Some(current), symbol,
                expected.iter().map(|window| window.timeframe.as_str()),
                usize::MAX, self.last_consumed_mono_ns, record.consumed_mono_ns,
            )?;
            self.sources[selected_index].reader.candle_states.insert(
                (record.actor_symbol, record.input.kind()), Arc::clone(&state),
            );
            self.candle_states.insert(record.actor_symbol, state);
        }
        self.schema_version = Some(record.schema_version);
        self.last_consumed_mono_ns = record.consumed_mono_ns;
        self.last_sequence = Some(selected_sequence);
        Ok(Some(event))
    }
}

pub struct EngineArrowReader {
    reader: FileReader<File>,
    rows: Vec<PolymarketMarketWsIn>,
    next_row: usize,
    window: Option<(u64, u64)>,
    done: bool,
}

impl EngineArrowReader {
    pub fn open(path: &Path, window: Option<(u64, u64)>) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = FileReader::try_new(file, None)
            .with_context(|| format!("open finalized Arrow {}", path.display()))?;
        Ok(Self {
            reader,
            rows: Vec::new(),
            next_row: 0,
            window,
            done: false,
        })
    }

    pub fn next_event(&mut self) -> Result<Option<PolymarketMarketWsIn>> {
        loop {
            if self.next_row < self.rows.len() {
                let row = self.rows[self.next_row].clone();
                self.next_row += 1;
                if let Some((start_ms, end_ms)) = self.window {
                    if row.session_ts_ms < start_ms {
                        continue;
                    }
                    if row.session_ts_ms > end_ms {
                        self.done = true;
                        return Ok(None);
                    }
                }
                return Ok(Some(row));
            }
            if self.done {
                return Ok(None);
            }
            let Some(batch) = self
                .reader
                .next()
                .transpose()
                .context("read engine arrow")?
            else {
                self.done = true;
                return Ok(None);
            };
            self.rows = load_engine_batch(batch)?;
            self.next_row = 0;
        }
    }
}

/// Reads the raw Polymarket market WebSocket ingress arrow.
///
/// Mirrors `EngineArrowReader`: the same finalized open and next_event shape. Rows are
/// returned in captured `sequence` order so replay drives the production parser with the
/// exact frames, in the exact order, the live connection received them.
pub struct PolymarketWsIngressArrowReader {
    reader: FileReader<File>,
    rows: Vec<PolymarketWsIngressRecord>,
    next_row: usize,
    symbol: Option<String>,
    done: bool,
}

impl PolymarketWsIngressArrowReader {
    pub fn open(path: &Path, symbol: Option<String>) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = FileReader::try_new(file, None)
            .with_context(|| format!("open finalized Arrow {}", path.display()))?;
        Ok(Self {
            reader,
            rows: Vec::new(),
            next_row: 0,
            symbol: symbol.map(|value| value.trim().to_ascii_uppercase()),
            done: false,
        })
    }

    pub fn next_event(&mut self) -> Result<Option<PolymarketWsIngressRecord>> {
        loop {
            if self.next_row < self.rows.len() {
                let row = self.rows[self.next_row].clone();
                self.next_row += 1;
                if let Some(symbol) = self.symbol.as_deref() {
                    if !row.symbol.trim().eq_ignore_ascii_case(symbol) {
                        continue;
                    }
                }
                return Ok(Some(row));
            }
            if self.done {
                return Ok(None);
            }
            let Some(batch) = self
                .reader
                .next()
                .transpose()
                .context("read polymarket ws ingress arrow")?
            else {
                self.done = true;
                return Ok(None);
            };
            self.rows = load_polymarket_ws_ingress_batch(batch)?;
            self.next_row = 0;
        }
    }
}

fn load_polymarket_ws_ingress_batch(batch: RecordBatch) -> Result<Vec<PolymarketWsIngressRecord>> {
    let recv_ts_ms = uint_col(&batch, 0, "recv_ts_ms")?;
    let recv_mono_ns = uint_col(&batch, 1, "recv_mono_ns")?;
    let symbol = str_col(&batch, 2, "symbol")?;
    let role = str_col(&batch, 3, "role")?;
    let generation = uint_col(&batch, 4, "generation")?;
    let connection_session_id = uint_col(&batch, 5, "connection_session_id")?;
    let sequence = uint_col(&batch, 6, "sequence")?;
    let message_kind = str_col(&batch, 7, "message_kind")?;
    let payload = binary_col(&batch, 8, "payload")?;
    let close_code = uint16_col(&batch, 9, "close_code")?;
    let close_reason = str_col(&batch, 10, "close_reason")?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        rows.push(PolymarketWsIngressRecord {
            recv_ts_ms: required_u64(recv_ts_ms, idx, "recv_ts_ms")?,
            recv_mono_ns: required_u64(recv_mono_ns, idx, "recv_mono_ns")?,
            symbol: required_str(symbol, idx, "symbol")?.to_string(),
            role: required_str(role, idx, "role")?.parse()?,
            generation: required_u64(generation, idx, "generation")?,
            connection_session_id: required_u64(
                connection_session_id,
                idx,
                "connection_session_id",
            )?,
            sequence: required_u64(sequence, idx, "sequence")?,
            message_kind: required_str(message_kind, idx, "message_kind")?.parse()?,
            payload: payload.is_valid(idx).then(|| payload.value(idx).to_vec()),
            close_code: close_code.is_valid(idx).then(|| close_code.value(idx)),
            close_reason: close_reason
                .is_valid(idx)
                .then(|| close_reason.value(idx).to_string()),
        });
    }
    Ok(rows)
}

fn load_engine_batch(batch: RecordBatch) -> Result<Vec<PolymarketMarketWsIn>> {
    let session_ts_ms = int_col(&batch, 0, "session_ts_ms")?;
    let channel = str_col(&batch, 1, "channel")?;
    let asset_id = str_col(&batch, 2, "asset_id")?;
    let event_ts_ms = int_col(&batch, 3, "event_ts_ms")?;
    let recv_ts_ms = int_col(&batch, 4, "recv_ts_ms")?;
    let ingest_delay_ms = int_col(&batch, 5, "ingest_delay_ms")?;
    let payload_msgpack = binary_col(&batch, 6, "payload_msgpack")?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for idx in 0..batch.num_rows() {
        let payload: bbwick_engine::PolymarketOddsState =
            rmp_serde::from_slice(required_binary(payload_msgpack, idx, "payload_msgpack")?)
                .context("parse engine payload_msgpack")?;
        rows.push(PolymarketMarketWsIn {
            session_ts_ms: required_i64(session_ts_ms, idx, "session_ts_ms")? as u64,
            channel: required_str(channel, idx, "channel")?.to_string(),
            asset_id: required_str(asset_id, idx, "asset_id")?.to_string(),
            event_ts_ms: required_i64(event_ts_ms, idx, "event_ts_ms")? as u64,
            recv_ts_ms: required_i64(recv_ts_ms, idx, "recv_ts_ms")? as u64,
            ingest_delay_ms: required_i64(ingest_delay_ms, idx, "ingest_delay_ms")?,
            payload: Arc::new(payload),
        });
    }
    Ok(rows)
}

fn uint_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .with_context(|| format!("column {name} is not u64"))
}

fn uint16_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a UInt16Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .with_context(|| format!("column {name} is not u16"))
}

fn required_u64(array: &UInt64Array, idx: usize, name: &str) -> Result<u64> {
    anyhow::ensure!(array.is_valid(idx), "column {name} row {idx} is null");
    Ok(array.value(idx))
}

fn int_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow!("{name} column is not int64"))
}

fn str_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("{name} column is not utf8"))
}

fn binary_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a BinaryArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("{name} column is not binary"))
}

fn required_i64(array: &Int64Array, idx: usize, name: &str) -> Result<i64> {
    if array.is_null(idx) {
        return Err(anyhow!("{name} is null at row {idx}"));
    }
    Ok(array.value(idx))
}

fn required_str<'a>(array: &'a StringArray, idx: usize, name: &str) -> Result<&'a str> {
    if array.is_null(idx) {
        return Err(anyhow!("{name} is null at row {idx}"));
    }
    Ok(array.value(idx))
}

fn required_binary<'a>(array: &'a BinaryArray, idx: usize, name: &str) -> Result<&'a [u8]> {
    if array.is_null(idx) {
        return Err(anyhow!("{name} is null at row {idx}"));
    }
    Ok(array.value(idx))
}

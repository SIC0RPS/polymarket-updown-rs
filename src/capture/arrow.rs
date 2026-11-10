use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::builder::BinaryBuilder;
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray, UInt16Array, UInt64Array};
use arrow_ipc::{
    writer::{FileWriter, IpcWriteOptions},
    CompressionType,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::capture::events::{PolymarketMarketWsIn, StrategyConsumptionRecord};
use bbwick_engine::polymarket_ws::{
    PolymarketWsIngressMessageKind, PolymarketWsIngressRecord, PolymarketWsIngressRole,
};

#[path = "arrow/readers.rs"]
mod readers;
pub use readers::{
    EngineArrowReader, PolymarketWsIngressArrowReader, StrategyConsumptionArrowReader,
    StrategyConsumptionMergedArrowReader,
};

const ENGINE_BATCH_SIZE: usize = 512;
const POLYMARKET_WS_INGRESS_BATCH_SIZE: usize = 8192;
const STRATEGY_CONSUMPTION_BATCH_SIZE: usize = 512;

pub const POLYMARKET_WS_INGRESS_ARROW_FILE: &str = "polymarket_market_ws_ingress.arrow";
pub const STRATEGY_CONSUMPTION_ARROW_FILE: &str = "strategy_consumption.arrow";

pub(crate) fn zstd_write_options() -> Result<IpcWriteOptions> {
    IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .context("configure Arrow IPC ZSTD compression")
}

pub fn polymarket_ws_ingress_arrow_path(session_dir: &Path) -> PathBuf {
    session_dir.join(POLYMARKET_WS_INGRESS_ARROW_FILE)
}

pub fn strategy_consumption_arrow_path(session_dir: &Path) -> PathBuf {
    session_dir.join(STRATEGY_CONSUMPTION_ARROW_FILE)
}

pub(crate) fn strategy_consumption_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "payload_msgpack",
        DataType::Binary,
        false,
    )]))
}

pub struct StrategyConsumptionArrowWriter {
    path: PathBuf,
    writer: FileWriter<File>,
    schema: SchemaRef,
    payloads: BinaryBuilder,
    batch_rows: usize,
}

impl StrategyConsumptionArrowWriter {
    pub fn create(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let schema = strategy_consumption_schema();
        let writer = FileWriter::try_new_with_options(file, &schema, zstd_write_options()?)
            .with_context(|| format!("create arrow writer {}", path.display()))?;
        Ok(Self {
            path,
            writer,
            schema,
            payloads: BinaryBuilder::new(),
            batch_rows: 0,
        })
    }

    pub fn write(&mut self, payload: Vec<u8>) -> Result<()> {
        self.payloads.append_value(payload);
        self.batch_rows += 1;
        if self.batch_rows >= STRATEGY_CONSUMPTION_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn write_record(&mut self, record: &StrategyConsumptionRecord) -> Result<()> {
        record.validate_schema()?;
        rmp_serde::encode::write_named(&mut self.payloads, record)
            .context("serialize strategy consumption payload_msgpack")?;
        self.payloads.append_value([]);
        self.batch_rows += 1;
        if self.batch_rows >= STRATEGY_CONSUMPTION_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.batch_rows == 0 {
            return Ok(());
        }
        let batch =
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(self.payloads.finish())])
                .context("build strategy consumption Arrow batch")?;
        self.batch_rows = 0;
        self.writer
            .write(&batch)
            .with_context(|| format!("write {}", self.path.display()))
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .finish()
            .with_context(|| format!("finish {}", self.path.display()))
    }
}

pub fn engine_arrow_path(session_dir: &Path, underlying: &str) -> PathBuf {
    session_dir.join(format!(
        "polymarket_engine_state_replayable_{}.arrow",
        normalize_capture_symbol(underlying)
    ))
}

pub fn normalize_capture_symbol(raw: &str) -> String {
    raw.trim()
        .to_ascii_lowercase()
        .trim_end_matches("usdt")
        .trim_end_matches("usdc")
        .trim_end_matches("usd")
        .to_string()
}

fn engine_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("session_ts_ms", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("event_ts_ms", DataType::Int64, false),
        Field::new("recv_ts_ms", DataType::Int64, false),
        Field::new("ingest_delay_ms", DataType::Int64, false),
        Field::new("payload_msgpack", DataType::Binary, false),
    ]))
}

fn polymarket_ws_ingress_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("recv_ts_ms", DataType::UInt64, false),
        Field::new("recv_mono_ns", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("generation", DataType::UInt64, false),
        Field::new("connection_session_id", DataType::UInt64, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("message_kind", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, true),
        Field::new("close_code", DataType::UInt16, true),
        Field::new("close_reason", DataType::Utf8, true),
    ]))
}

pub struct EngineArrowWriter {
    path: PathBuf,
    writer: FileWriter<File>,
    schema: SchemaRef,
    rows: Vec<PolymarketMarketWsIn>,
    count: u64,
}

#[derive(Debug)]
struct PolymarketWsIngressSessionState {
    symbol: String,
    role: PolymarketWsIngressRole,
    generation: u64,
    last_sequence: u64,
    last_recv_mono_ns: u64,
}

pub struct PolymarketWsIngressArrowWriter {
    path: PathBuf,
    writer: FileWriter<File>,
    schema: SchemaRef,
    rows: Vec<PolymarketWsIngressRecord>,
    count: u64,
    sessions: HashMap<u64, PolymarketWsIngressSessionState>,
    validation_failures: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PolymarketWsIngressWriteSummary {
    pub rows: u64,
    pub validation_failures: u64,
}

impl PolymarketWsIngressArrowWriter {
    pub fn create(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let schema = polymarket_ws_ingress_schema();
        let writer = FileWriter::try_new_with_options(file, &schema, zstd_write_options()?)
            .with_context(|| format!("create arrow writer {}", path.display()))?;
        Ok(Self {
            path,
            writer,
            schema,
            rows: Vec::with_capacity(POLYMARKET_WS_INGRESS_BATCH_SIZE),
            count: 0,
            sessions: HashMap::new(),
            validation_failures: 0,
        })
    }

    pub fn write(&mut self, event: PolymarketWsIngressRecord) -> Result<()> {
        let mut validation_failure =
            event.symbol.is_empty() || event.connection_session_id == 0 || event.sequence == 0;
        match event.message_kind {
            PolymarketWsIngressMessageKind::Text => {
                validation_failure |= event
                    .payload
                    .as_deref()
                    .is_none_or(|payload| std::str::from_utf8(payload).is_err());
                validation_failure |= event.close_code.is_some() || event.close_reason.is_some();
            }
            PolymarketWsIngressMessageKind::Binary
            | PolymarketWsIngressMessageKind::Ping
            | PolymarketWsIngressMessageKind::Pong => {
                validation_failure |= event.payload.is_none();
                validation_failure |= event.close_code.is_some() || event.close_reason.is_some();
            }
            PolymarketWsIngressMessageKind::Close => {
                validation_failure |= event.payload.is_some();
            }
        }

        if let Some(session) = self.sessions.get_mut(&event.connection_session_id) {
            validation_failure |= session.symbol != event.symbol
                || session.role != event.role
                || session.generation != event.generation;
            validation_failure |= session
                .last_sequence
                .checked_add(1)
                .is_none_or(|expected| event.sequence != expected);
            validation_failure |= event.recv_mono_ns < session.last_recv_mono_ns;
            session.last_sequence = event.sequence;
            session.last_recv_mono_ns = event.recv_mono_ns;
        } else {
            validation_failure |= event.sequence != 1;
            self.sessions.insert(
                event.connection_session_id,
                PolymarketWsIngressSessionState {
                    symbol: event.symbol.clone(),
                    role: event.role,
                    generation: event.generation,
                    last_sequence: event.sequence,
                    last_recv_mono_ns: event.recv_mono_ns,
                },
            );
        }

        if validation_failure {
            self.validation_failures = self.validation_failures.saturating_add(1);
            if self.validation_failures == 1 {
                tracing::error!(
                    target: "capture.polymarket_market_ws_ingress",
                    connection_session_id = event.connection_session_id,
                    sequence = event.sequence,
                    "polymarket_market_ws_ingress_validation_failed"
                );
            }
        }

        self.rows.push(event);
        if self.rows.len() >= POLYMARKET_WS_INGRESS_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        for chunk in rows.chunks(POLYMARKET_WS_INGRESS_BATCH_SIZE) {
            let batch = polymarket_ws_ingress_batch(chunk, self.schema.clone())?;
            self.writer
                .write(&batch)
                .with_context(|| format!("write {}", self.path.display()))?;
            self.count = self.count.saturating_add(chunk.len() as u64);
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<PolymarketWsIngressWriteSummary> {
        self.flush()?;
        self.writer
            .finish()
            .with_context(|| format!("finish {}", self.path.display()))?;
        Ok(PolymarketWsIngressWriteSummary {
            rows: self.count,
            validation_failures: self.validation_failures,
        })
    }
}

impl EngineArrowWriter {
    pub fn create(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let schema = engine_schema();
        let writer = FileWriter::try_new_with_options(file, &schema, zstd_write_options()?)
            .with_context(|| format!("create arrow writer {}", path.display()))?;
        Ok(Self {
            path,
            writer,
            schema,
            rows: Vec::with_capacity(ENGINE_BATCH_SIZE),
            count: 0,
        })
    }

    pub fn write(&mut self, event: PolymarketMarketWsIn) -> Result<()> {
        self.rows.push(event);
        if self.rows.len() >= ENGINE_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        for chunk in rows.chunks(ENGINE_BATCH_SIZE) {
            let batch = engine_batch(chunk, self.schema.clone())?;
            self.writer
                .write(&batch)
                .with_context(|| format!("write {}", self.path.display()))?;
            self.count = self.count.saturating_add(chunk.len() as u64);
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64> {
        self.flush()?;
        self.writer
            .finish()
            .with_context(|| format!("finish {}", self.path.display()))?;
        Ok(self.count)
    }
}

fn engine_batch(rows: &[PolymarketMarketWsIn], schema: SchemaRef) -> Result<RecordBatch> {
    let mut payloads = BinaryBuilder::new();
    for row in rows {
        rmp_serde::encode::write_named(&mut payloads, &row.payload)
            .context("serialize engine payload_msgpack")?;
        payloads.append_value([]);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.session_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.channel.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.asset_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.event_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.recv_ts_ms as i64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.ingest_delay_ms)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(payloads.finish()),
        ],
    )
    .context("build engine arrow batch")
}

fn polymarket_ws_ingress_batch(
    rows: &[PolymarketWsIngressRecord],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.recv_ts_ms).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.recv_mono_ns).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.symbol.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.role.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.generation).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.connection_session_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.message_kind.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from_iter(
                rows.iter().map(|row| row.payload.as_deref()),
            )),
            Arc::new(UInt16Array::from(
                rows.iter().map(|row| row.close_code).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.close_reason.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .context("build raw Polymarket WS ingress Arrow batch")
}

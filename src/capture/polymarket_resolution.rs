use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use polymarket_client_sdk::types::{B256, U256};
use serde::{Deserialize, Serialize};

const ARROW_BATCH_SIZE: usize = 512;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketResolutionCapture {
    pub observed_at_ms: u64,
    pub underlying: String,
    pub condition_id: B256,
    pub up_token: U256,
    pub down_token: U256,
    pub winning_asset_id: U256,
    pub raw_response: Vec<u8>,
}

pub fn polymarket_resolution_arrow_path(session_dir: &Path, underlying: &str) -> PathBuf {
    session_dir.join(format!(
        "polymarket_resolution_{}.arrow",
        underlying.trim().to_ascii_lowercase()
    ))
}

fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("observed_at_ms", DataType::Int64, false),
        Field::new("underlying", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("up_token", DataType::Utf8, false),
        Field::new("down_token", DataType::Utf8, false),
        Field::new("winning_asset_id", DataType::Utf8, false),
        Field::new("raw_response", DataType::Binary, false),
    ]))
}

pub struct PolymarketResolutionArrowWriter {
    path: PathBuf,
    writer: FileWriter<File>,
    schema: SchemaRef,
    rows: Vec<PolymarketResolutionCapture>,
    count: u64,
}

impl PolymarketResolutionArrowWriter {
    pub fn create(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let schema = arrow_schema();
        let writer = FileWriter::try_new(file, &schema)
            .with_context(|| format!("create arrow writer {}", path.display()))?;
        Ok(Self {
            path,
            writer,
            schema,
            rows: Vec::with_capacity(ARROW_BATCH_SIZE),
            count: 0,
        })
    }

    pub fn write(&mut self, event: &PolymarketResolutionCapture) -> Result<()> {
        self.rows.push(event.clone());
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.rows);
        for chunk in rows.chunks(ARROW_BATCH_SIZE) {
            self.writer
                .write(&arrow_batch(chunk, self.schema.clone())?)
                .with_context(|| format!("write {}", self.path.display()))?;
            self.count = self.count.saturating_add(chunk.len() as u64);
        }
        self.writer
            .get_mut()
            .flush()
            .with_context(|| format!("flush {}", self.path.display()))?;
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

pub struct PolymarketResolutionArrowReader {
    reader: FileReader<File>,
    rows: Vec<PolymarketResolutionCapture>,
    next_row: usize,
    done: bool,
}

impl PolymarketResolutionArrowReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = FileReader::try_new(file, None)
            .with_context(|| format!("open finalized Arrow {}", path.display()))?;
        Ok(Self {
            reader,
            rows: Vec::new(),
            next_row: 0,
            done: false,
        })
    }

    pub fn next_event(&mut self) -> Result<Option<PolymarketResolutionCapture>> {
        loop {
            if self.next_row < self.rows.len() {
                let row = self.rows[self.next_row].clone();
                self.next_row += 1;
                return Ok(Some(row));
            }
            if self.done {
                return Ok(None);
            }
            let Some(batch) = self
                .reader
                .next()
                .transpose()
                .context("read Polymarket resolution arrow")?
            else {
                self.done = true;
                return Ok(None);
            };
            self.rows = load_arrow_batch(batch)?;
            self.next_row = 0;
        }
    }
}

fn arrow_batch(rows: &[PolymarketResolutionCapture], schema: SchemaRef) -> Result<RecordBatch> {
    let observed_at_ms = rows
        .iter()
        .map(|row| {
            i64::try_from(row.observed_at_ms)
                .context("Polymarket resolution observed_at_ms exceeds Arrow Int64")
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(observed_at_ms)),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.underlying.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.condition_id.to_string()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.up_token.to_string()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.down_token.to_string()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.winning_asset_id.to_string()),
            )),
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.raw_response.as_slice()),
            )),
        ],
    )
    .context("build Polymarket resolution arrow batch")
}

fn load_arrow_batch(batch: RecordBatch) -> Result<Vec<PolymarketResolutionCapture>> {
    let observed_at_ms = int_column(&batch, 0, "observed_at_ms")?;
    let underlying = string_column(&batch, 1, "underlying")?;
    let condition_id = string_column(&batch, 2, "condition_id")?;
    let up_token = string_column(&batch, 3, "up_token")?;
    let down_token = string_column(&batch, 4, "down_token")?;
    let winning_asset_id = string_column(&batch, 5, "winning_asset_id")?;
    let raw_response = binary_column(&batch, 6, "raw_response")?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        rows.push(PolymarketResolutionCapture {
            observed_at_ms: u64::try_from(required_i64(observed_at_ms, index, "observed_at_ms")?)
                .context("negative Polymarket resolution observed_at_ms")?,
            underlying: required_str(underlying, index, "underlying")?.to_string(),
            condition_id: required_str(condition_id, index, "condition_id")?
                .parse::<B256>()
                .context("parse Polymarket resolution condition_id")?,
            up_token: required_str(up_token, index, "up_token")?
                .parse::<U256>()
                .context("parse Polymarket resolution up_token")?,
            down_token: required_str(down_token, index, "down_token")?
                .parse::<U256>()
                .context("parse Polymarket resolution down_token")?,
            winning_asset_id: required_str(winning_asset_id, index, "winning_asset_id")?
                .parse::<U256>()
                .context("parse Polymarket resolution winning_asset_id")?,
            raw_response: required_binary(raw_response, index, "raw_response")?.to_vec(),
        });
    }
    Ok(rows)
}

fn int_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("Polymarket resolution arrow column {name} is not Int64"))
}

fn string_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("Polymarket resolution arrow column {name} is not Utf8"))
}

fn binary_column<'a>(batch: &'a RecordBatch, index: usize, name: &str) -> Result<&'a BinaryArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| format!("Polymarket resolution arrow column {name} is not Binary"))
}

fn required_i64(column: &Int64Array, index: usize, name: &str) -> Result<i64> {
    anyhow::ensure!(
        !column.is_null(index),
        "Polymarket resolution arrow column {name} is null"
    );
    Ok(column.value(index))
}

fn required_str<'a>(column: &'a StringArray, index: usize, name: &str) -> Result<&'a str> {
    anyhow::ensure!(
        !column.is_null(index),
        "Polymarket resolution arrow column {name} is null"
    );
    Ok(column.value(index))
}

fn required_binary<'a>(column: &'a BinaryArray, index: usize, name: &str) -> Result<&'a [u8]> {
    anyhow::ensure!(
        !column.is_null(index),
        "Polymarket resolution arrow column {name} is null"
    );
    Ok(column.value(index))
}

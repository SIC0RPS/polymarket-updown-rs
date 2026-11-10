use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{RecordBatch, StructArray};
use bbwick_engine::{OracleCandleWindow, OracleHistory};
use arrow_ipc::reader::FileReader;
use rayon::prelude::*;

use crate::capture::events::{StrategyConsumptionKind, StrategyConsumptionRecord};
use crate::strategy_runtime::symbol_process::SymbolId;

#[path = "consumption/read.rs"]
mod read;
#[path = "consumption/schema.rs"]
mod schema;
#[path = "consumption/write.rs"]
mod write;

pub(super) use write::PreparedConsumptionWriter;

#[derive(Debug, Clone, Copy)]
struct ConsumptionBatchIndex {
    file_batch: u32,
    start: u32,
    len: u32,
}

#[derive(Debug)]
pub(crate) struct PreparedConsumptionSource {
    path: Arc<PathBuf>,
    expected_symbol: SymbolId,
    batches: Arc<[ConsumptionBatchIndex]>,
    row_count: usize,
    source_schema_version: Option<u32>,
}

pub(crate) struct PreparedConsumptionCursor {
    path: Arc<PathBuf>,
    expected_symbol: SymbolId,
    batches: Arc<[ConsumptionBatchIndex]>,
    reader: FileReader<File>,
    current_file_batch: Option<u32>,
    current_batch: Option<StructArray>,
    source_schema_version: Option<u32>,
    resolved_through: Option<u32>,
    last_sequence: Option<u64>,
    last_consumed_mono_ns: u64,
    lane_candles: [Option<Arc<[Arc<OracleCandleWindow>]>>; 2],
    latest_candles: Option<Arc<[Arc<OracleCandleWindow>]>>,
}

impl PreparedConsumptionSource {
    pub(crate) fn open(path: &Path, expected_symbol: &str) -> Result<Self> {
        let path = Arc::new(path.to_path_buf());
        let expected_symbol = SymbolId::parse(expected_symbol)?;
        let mut cursor = PreparedConsumptionCursor::open(
            Arc::clone(&path), expected_symbol, Arc::from([]), None,
        )?;
        let mut batches = Vec::with_capacity(cursor.reader.num_batches());
        let mut row_count = 0usize;
        for file_batch in 0..cursor.reader.num_batches() {
            let batch = cursor.reader.next().transpose().with_context(|| {
                format!(
                    "read strategy consumption file={} batch={file_batch}",
                    path.display()
                )
            })?.context("strategy consumption indexed batch is missing")?;
            validate_batch_schema(&batch, path.as_path(), file_batch)?;
            let batch_rows = batch.num_rows();
            let batch_start = u32::try_from(row_count).with_context(|| {
                format!(
                    "strategy consumption row start exceeds u32 file={} batch={file_batch}",
                    path.display()
                )
            })?;
            let batch_len = u32::try_from(batch_rows).with_context(|| {
                format!(
                    "strategy consumption batch row count exceeds u32 file={} batch={file_batch}",
                    path.display()
                )
            })?;
            let root = StructArray::from(batch);
            for batch_row in 0..batch_rows {
                let (version, sequence, clock, kind, changed) =
                    read::decode_candle_boundary(&root, batch_row, expected_symbol)
                        .with_context(|| format!("decode candle state file={} batch={file_batch} row={batch_row}", path.display()))?;
                cursor.apply_candle_boundary(version, sequence, clock, kind, changed)
                    .with_context(|| format!("validate candle state file={} batch={file_batch} row={batch_row}", path.display()))?;
            }
            if batch_rows > 0 {
                batches.push(ConsumptionBatchIndex {
                    file_batch: u32::try_from(file_batch).with_context(|| {
                        format!(
                            "strategy consumption batch index exceeds u32 file={} batch={file_batch}",
                            path.display()
                        )
                    })?,
                    start: batch_start,
                    len: batch_len,
                });
            }
            row_count = row_count
                .checked_add(batch_rows)
                .context("strategy consumption row count overflow")?;
        }
        Ok(Self {
            path,
            expected_symbol,
            batches: Arc::from(batches),
            row_count,
            source_schema_version: cursor.source_schema_version,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.row_count
    }

    pub(crate) fn cursor(&self) -> Result<PreparedConsumptionCursor> {
        PreparedConsumptionCursor::open(
            Arc::clone(&self.path),
            self.expected_symbol,
            Arc::clone(&self.batches),
            self.source_schema_version,
        )
    }

    pub(crate) fn earliest_input_ms(
        &self,
        cursor: &mut PreparedConsumptionCursor,
        row: u32,
        expected_sequence: u64,
        expected_timestamp_ms: u64,
    ) -> Result<u64> {
        anyhow::ensure!(
            Arc::ptr_eq(&self.path, &cursor.path)
                && Arc::ptr_eq(&self.batches, &cursor.batches)
                && self.expected_symbol == cursor.expected_symbol,
            "strategy consumption cursor has a foreign source identity file={}",
            self.path.display()
        );
        cursor.earliest_input_ms(row, expected_sequence, expected_timestamp_ms)
    }

    pub(crate) fn read_rows(
        &self,
        cursor: &mut PreparedConsumptionCursor,
        rows: &[u32],
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<StrategyConsumptionRecord>> {
        anyhow::ensure!(
            Arc::ptr_eq(&self.path, &cursor.path)
                && Arc::ptr_eq(&self.batches, &cursor.batches)
                && self.expected_symbol == cursor.expected_symbol,
            "strategy consumption cursor has a foreign source identity file={}",
            self.path.display()
        );
        let Some(first_row) = rows.first().copied() else {
            return Ok(Vec::new());
        };
        let mut records = Vec::with_capacity(rows.len());
        let mut chunk_start = 0usize;
        let mut batch_index = locate_batch(&self.batches, self.path.as_path(), first_row)?;
        loop {
            let mut chunk_end = chunk_start + 1;
            let mut next_batch = None;
            while chunk_end < rows.len() {
                let actual = locate_batch(&self.batches, self.path.as_path(), rows[chunk_end])?;
                if actual.file_batch != batch_index.file_batch {
                    next_batch = Some(actual);
                    break;
                }
                chunk_end += 1;
            }

            let resolved_states = rows[chunk_start..chunk_end].iter()
                .map(|row| cursor.resolve_candles(*row))
                .collect::<Result<Vec<_>>>()?;
            let chunk_first_row = rows[chunk_start];
            cursor.load_batch(batch_index, chunk_first_row)?;
            let batch = cursor
                .current_batch
                .as_ref()
                .context("strategy consumption cursor lost its active batch")?;
            let decoded = pool.install(|| {
                rows[chunk_start..chunk_end]
                    .par_iter()
                    .map(|row| {
                        let batch_row = row
                            .checked_sub(batch_index.start)
                            .context("strategy consumption row precedes its active batch")?;
                        read::decode_record(batch, batch_row as usize, self.expected_symbol)
                            .with_context(|| {
                                format!(
                                    "decode strategy consumption file={} batch={} row={} global_row={row}",
                                    self.path.display(),
                                    batch_index.file_batch,
                                    batch_row
                                )
                            })
                    })
                    .collect::<Vec<_>>()
            });
            for (record, state) in decoded.into_iter().zip(resolved_states) {
                let mut record = record?;
                record.candle_state = state;
                records.push(record);
            }

            chunk_start = chunk_end;
            let Some(actual) = next_batch else {
                break;
            };
            batch_index = actual;
        }
        Ok(records)
    }

    pub(crate) fn location(&self, row: u32) -> Result<(&Path, u32, u32)> {
        let batch = locate_batch(&self.batches, self.path.as_path(), row)?;
        Ok((self.path.as_path(), batch.file_batch, row - batch.start))
    }
}

impl PreparedConsumptionCursor {
    fn open(
        path: Arc<PathBuf>,
        expected_symbol: SymbolId,
        batches: Arc<[ConsumptionBatchIndex]>,
        source_schema_version: Option<u32>,
    ) -> Result<Self> {
        let reader = open_reader(path.as_path())?;
        Ok(Self {
            path,
            expected_symbol,
            batches,
            reader,
            current_file_batch: None,
            current_batch: None,
            source_schema_version,
            resolved_through: None,
            last_sequence: None,
            last_consumed_mono_ns: 0,
            lane_candles: [None, None],
            latest_candles: None,
        })
    }

    pub(crate) fn source_schema_version(&self) -> Option<u32> {
        self.source_schema_version
    }

    fn apply_candle_boundary(
        &mut self,
        version: u32,
        sequence: u64,
        clock: u64,
        kind: StrategyConsumptionKind,
        changed: Option<Arc<[Arc<OracleCandleWindow>]>>,
    ) -> Result<()> {
        anyhow::ensure!(
            self.source_schema_version.is_none_or(|expected| expected == version),
            "strategy consumption source schemas are mixed"
        );
        anyhow::ensure!(
            self.last_sequence.is_none_or(|last| sequence > last),
            "strategy consumption source sequence is not increasing"
        );
        if version > 1 {
            let previous = self.lane_candles[kind as usize].as_ref();
            let expected = self.latest_candles.as_ref().or(changed.as_ref()).or(previous)
                .context("missing initial consumed candle state")?;
            let symbol = &expected.first().context("empty consumed candle key set")?.symbol;
            let state = OracleHistory::resolve_consumed_state(
                previous, changed.as_ref(), symbol,
                expected.iter().map(|window| window.timeframe.as_str()),
                usize::MAX, self.last_consumed_mono_ns, clock,
            )?;
            let state = OracleHistory::resolve_consumed_state(
                self.latest_candles.as_ref(), Some(&state), symbol,
                expected.iter().map(|window| window.timeframe.as_str()),
                usize::MAX, self.last_consumed_mono_ns, clock,
            )?;
            self.lane_candles[kind as usize] = Some(Arc::clone(&state));
            self.latest_candles = Some(state);
        }
        self.source_schema_version = Some(version);
        self.last_sequence = Some(sequence);
        self.last_consumed_mono_ns = clock;
        Ok(())
    }

    fn resolve_candles(&mut self, row: u32) -> Result<Option<Arc<[Arc<OracleCandleWindow>]>>> {
        if self.resolved_through == Some(row) {
            return Ok(self.latest_candles.clone());
        }
        if self.resolved_through.is_some_and(|last| row < last) {
            self.resolved_through = None;
            self.last_sequence = None;
            self.last_consumed_mono_ns = 0;
            self.lane_candles = [None, None];
            self.latest_candles = None;
        }
        let start = match self.resolved_through {
            Some(last) => last.checked_add(1).context("consumption prefix row overflow")?,
            None => 0,
        };
        for prefix_row in start..=row {
            let batch = locate_batch(&self.batches, self.path.as_path(), prefix_row)?;
            self.load_batch(batch, prefix_row)?;
            let root = self.current_batch.as_ref()
                .context("strategy consumption cursor lost its active batch")?;
            let (version, sequence, clock, kind, changed) = read::decode_candle_boundary(
                root, (prefix_row - batch.start) as usize, self.expected_symbol,
            ).with_context(|| format!("decode candle prefix file={} row={prefix_row}", self.path.display()))?;
            self.apply_candle_boundary(version, sequence, clock, kind, changed)
                .with_context(|| format!("validate candle prefix file={} row={prefix_row}", self.path.display()))?;
            self.resolved_through = Some(prefix_row);
        }
        Ok(self.latest_candles.clone())
    }

    fn earliest_input_ms(
        &mut self,
        row: u32,
        expected_sequence: u64,
        expected_timestamp_ms: u64,
    ) -> Result<u64> {
        let batch_index = locate_batch(&self.batches, self.path.as_path(), row)?;
        let batch_row = row - batch_index.start;
        self.load_batch(batch_index, row)?;
        read::decode_earliest_input_ms(
            self.current_batch.as_ref().with_context(|| {
                format!(
                    "strategy consumption cursor lost its active batch file={} batch={} row={batch_row} global_row={row}",
                    self.path.display(),
                    batch_index.file_batch
                )
            })?,
            batch_row as usize,
            self.expected_symbol,
            expected_sequence,
            expected_timestamp_ms,
        )
        .with_context(|| {
            format!(
                "decode strategy consumption input boundary file={} batch={} row={batch_row} global_row={row}",
                self.path.display(),
                batch_index.file_batch
            )
        })
    }

    fn load_batch(&mut self, batch_index: ConsumptionBatchIndex, row: u32) -> Result<()> {
        let batch_row = row - batch_index.start;
        if self.current_file_batch != Some(batch_index.file_batch) {
            self.current_batch = None;
            self.current_file_batch = None;
            self.reader
                .set_index(batch_index.file_batch as usize)
                .with_context(|| {
                    format!(
                        "seek strategy consumption file={} batch={} row={batch_row} global_row={row}",
                        self.path.display(),
                        batch_index.file_batch
                    )
                })?;
            let batch = self
                .reader
                .next()
                .transpose()
                .with_context(|| {
                    format!(
                        "read strategy consumption file={} batch={} row={batch_row} global_row={row}",
                        self.path.display(),
                        batch_index.file_batch
                    )
                })?
                .with_context(|| {
                    format!(
                        "strategy consumption batch is unavailable file={} batch={} row={batch_row} global_row={row}",
                        self.path.display(),
                        batch_index.file_batch
                    )
                })?;
            validate_batch_schema(&batch, self.path.as_path(), batch_index.file_batch as usize)?;
            anyhow::ensure!(
                batch.num_rows() == batch_index.len as usize,
                "strategy consumption batch row count changed file={} batch={} indexed={} actual={}",
                self.path.display(),
                batch_index.file_batch,
                batch_index.len,
                batch.num_rows()
            );
            self.current_batch = Some(StructArray::from(batch));
            self.current_file_batch = Some(batch_index.file_batch);
        }
        Ok(())
    }
}

fn open_reader(path: &Path) -> Result<FileReader<File>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = FileReader::try_new(file, None)
        .with_context(|| format!("open arrow {}", path.display()))?;
    anyhow::ensure!(
        reader.schema().as_ref() == schema::schema().as_ref(),
        "strategy consumption Arrow schema mismatch file={}",
        path.display()
    );
    Ok(reader)
}

fn validate_batch_schema(batch: &RecordBatch, path: &Path, file_batch: usize) -> Result<()> {
    anyhow::ensure!(
        batch.schema().as_ref() == schema::schema().as_ref(),
        "strategy consumption Arrow schema mismatch file={} batch={file_batch}",
        path.display()
    );
    Ok(())
}

fn locate_batch(
    batches: &[ConsumptionBatchIndex],
    path: &Path,
    row: u32,
) -> Result<ConsumptionBatchIndex> {
    batches
        .partition_point(|batch| batch.start <= row)
        .checked_sub(1)
        .and_then(|index| batches.get(index).copied())
        .filter(|batch| {
            u64::from(row) < u64::from(batch.start).saturating_add(u64::from(batch.len))
        })
        .with_context(|| {
            format!(
                "strategy consumption row is unavailable file={} global_row={row}",
                path.display()
            )
        })
}

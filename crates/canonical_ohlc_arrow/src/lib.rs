use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use thiserror::Error;

const BATCH_SIZE: usize = 8192;
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OhlcBar {
    pub ts_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WriteStats {
    pub count: usize,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bytes_written: u64,
}

#[derive(Error, Debug)]
pub enum OhlcArrowError {
    #[error("no bars to write")]
    EmptyInput,
    #[error("non-finite {field} at index {index}: {value}")]
    NonFinite {
        field: &'static str,
        index: usize,
        value: f64,
    },
    #[error(
        "timestamps must be strictly increasing and unique; violation at index {index}: {current} <= {previous}"
    )]
    NonMonotonic {
        index: usize,
        current: i64,
        previous: i64,
    },
    #[error("null volume at row {index}")]
    NullVolume { index: usize },
    #[error("null {field} at row {index}")]
    NullField { field: &'static str, index: usize },
    #[error("invalid IPC format: {details}")]
    InvalidIpcFormat { details: String },
    #[error("schema mismatch: {details}")]
    SchemaMismatch { details: String },
    #[error("path has no parent directory: {0}")]
    MissingParent(PathBuf),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

pub fn canonical_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, true),
    ]))
}

/// Holds the per-path transaction lock across current reads and persistence.
pub struct OhlcFileTransaction<'a> {
    path: &'a Path,
    _lock: File,
}

impl<'a> OhlcFileTransaction<'a> {
    pub fn open(path: &'a Path) -> Result<Self, OhlcArrowError> {
        let parent = path
            .parent()
            .ok_or_else(|| OhlcArrowError::MissingParent(path.to_path_buf()))?;
        std::fs::create_dir_all(parent)?;
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        // Never remove this sidecar: persistence replaces the data inode.
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(Path::new(&lock_path))?;
        loop {
            // SAFETY: the owned descriptor remains open throughout this call and
            // the transaction; closing it releases the lock.
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0 {
                break;
            }
            let err = std::io::Error::last_os_error();
            if err.kind() != ErrorKind::Interrupted {
                return Err(err.into());
            }
        }
        Ok(Self { path, _lock: lock })
    }

    pub fn read_all(&self) -> Result<Vec<OhlcBar>, OhlcArrowError> {
        read_all(self.path)
    }

    pub fn read_tail(&self, limit: usize) -> Result<Vec<OhlcBar>, OhlcArrowError> {
        read_tail(self.path, limit)
    }

    pub fn write(&mut self, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
        write_ohlc_bars_unlocked(self.path, bars)
    }

    pub fn append(&mut self, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
        append_ohlc_bars_unlocked(self.path, bars)
    }
}

pub fn write_ohlc_bars(path: &Path, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
    OhlcFileTransaction::open(path)?.write(bars)
}

fn write_ohlc_bars_unlocked(path: &Path, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
    let stats = validate_bars(bars)?;
    let parent = path
        .parent()
        .ok_or_else(|| OhlcArrowError::MissingParent(path.to_path_buf()))?;
    let schema = canonical_schema();
    let mut temp = NamedTempFile::new_in(parent)?;
    {
        let file = temp.as_file_mut();
        let mut writer = FileWriter::try_new_buffered(file, &schema)?;
        for batch in build_batches(bars, schema.clone())? {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let persist_res = temp.persist(path);
    if let Err(err) = persist_res {
        return Err(OhlcArrowError::Io(err.error));
    }
    let bytes_written = std::fs::metadata(path)?.len();
    Ok(WriteStats {
        count: stats.count,
        min_ts: stats.min_ts,
        max_ts: stats.max_ts,
        bytes_written,
    })
}

pub fn append_ohlc_bars(path: &Path, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
    OhlcFileTransaction::open(path)?.append(bars)
}

fn append_ohlc_bars_unlocked(path: &Path, bars: &[OhlcBar]) -> Result<WriteStats, OhlcArrowError> {
    let stats = validate_bars(bars)?;
    let mut file = File::open(path)?;
    ensure_file_magic(&mut file)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    let schema = reader.schema();
    validate_schema(schema.as_ref())?;

    let parent = path
        .parent()
        .ok_or_else(|| OhlcArrowError::MissingParent(path.to_path_buf()))?;
    let mut temp = NamedTempFile::new_in(parent)?;
    {
        let mut writer = FileWriter::try_new_buffered(temp.as_file_mut(), &schema)?;
        let mut last_ts: Option<i64> = None;
        for batch in reader {
            let batch = batch?;
            if let Some(batch_last) = last_ts_from_batch(&batch)? {
                last_ts = Some(batch_last);
            }
            writer.write(&batch)?;
        }
        let last_ts = last_ts.ok_or_else(|| OhlcArrowError::InvalidIpcFormat {
            details: "existing file has no bars".to_string(),
        })?;
        if stats.min_ts <= last_ts {
            return Err(OhlcArrowError::NonMonotonic {
                index: 0,
                current: stats.min_ts,
                previous: last_ts,
            });
        }
        for batch in build_batches(bars, schema.clone())? {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }

    let persist_res = temp.persist(path);
    if let Err(err) = persist_res {
        return Err(OhlcArrowError::Io(err.error));
    }
    let bytes_written = std::fs::metadata(path)?.len();
    Ok(WriteStats {
        count: stats.count,
        min_ts: stats.min_ts,
        max_ts: stats.max_ts,
        bytes_written,
    })
}

pub fn read_all(path: &Path) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    let mut file = File::open(path)?;
    ensure_file_magic(&mut file)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    read_all_from_file(reader)
}

pub fn read_tail(path: &Path, limit: usize) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    if limit == 0 {
        return read_all(path);
    }
    let mut file = File::open(path)?;
    ensure_file_magic(&mut file)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    read_tail_from_file(reader, limit)
}

pub fn read_range(
    path: &Path,
    start_ts: i64,
    end_ts: i64,
    limit: Option<usize>,
) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    if let Some(0) = limit {
        return Ok(Vec::new());
    }
    let mut file = File::open(path)?;
    ensure_file_magic(&mut file)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    read_range_from_file(reader, start_ts, end_ts, limit)
}

fn ensure_file_magic(file: &mut File) -> Result<(), OhlcArrowError> {
    let mut magic = [0u8; 6];
    if let Err(err) = file.read_exact(&mut magic) {
        let _ = file.seek(SeekFrom::Start(0));
        if err.kind() == ErrorKind::UnexpectedEof {
            return Err(OhlcArrowError::InvalidIpcFormat {
                details: "missing ARROW1 header".to_string(),
            });
        }
        return Err(OhlcArrowError::Io(err));
    }
    file.seek(SeekFrom::Start(0))?;
    if magic != *ARROW_MAGIC {
        return Err(OhlcArrowError::InvalidIpcFormat {
            details: "missing ARROW1 header".to_string(),
        });
    }
    Ok(())
}

pub struct ValidatedStats {
    count: usize,
    min_ts: i64,
    max_ts: i64,
}

pub fn validate_bars(bars: &[OhlcBar]) -> Result<ValidatedStats, OhlcArrowError> {
    if bars.is_empty() {
        return Err(OhlcArrowError::EmptyInput);
    }
    let mut last_ts: Option<i64> = None;
    for (idx, bar) in bars.iter().enumerate() {
        let ts = bar.ts_ms;
        for (field, value) in [
            ("open", bar.open),
            ("high", bar.high),
            ("low", bar.low),
            ("close", bar.close),
            ("volume", bar.volume),
        ] {
            if !value.is_finite() {
                return Err(OhlcArrowError::NonFinite {
                    field,
                    index: idx,
                    value,
                });
            }
        }
        if let Some(prev) = last_ts {
            if ts <= prev {
                return Err(OhlcArrowError::NonMonotonic {
                    index: idx,
                    current: ts,
                    previous: prev,
                });
            }
        }
        last_ts = Some(ts);
    }
    let min_ts = bars.first().map(|b| b.ts_ms).unwrap_or(0);
    let max_ts = bars.last().map(|b| b.ts_ms).unwrap_or(0);
    Ok(ValidatedStats {
        count: bars.len(),
        min_ts,
        max_ts,
    })
}

fn build_batches(bars: &[OhlcBar], schema: SchemaRef) -> Result<Vec<RecordBatch>, OhlcArrowError> {
    let mut batches = Vec::new();
    for chunk in bars.chunks(BATCH_SIZE) {
        let mut ts_vals = Vec::with_capacity(chunk.len());
        let mut open_vals = Vec::with_capacity(chunk.len());
        let mut high_vals = Vec::with_capacity(chunk.len());
        let mut low_vals = Vec::with_capacity(chunk.len());
        let mut close_vals = Vec::with_capacity(chunk.len());
        let mut volume_vals = Vec::with_capacity(chunk.len());
        for bar in chunk {
            ts_vals.push(bar.ts_ms);
            open_vals.push(bar.open);
            high_vals.push(bar.high);
            low_vals.push(bar.low);
            close_vals.push(bar.close);
            volume_vals.push(bar.volume);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts_vals)),
                Arc::new(Float64Array::from(open_vals)),
                Arc::new(Float64Array::from(high_vals)),
                Arc::new(Float64Array::from(low_vals)),
                Arc::new(Float64Array::from(close_vals)),
                Arc::new(Float64Array::from(volume_vals)),
            ],
        )?;
        batches.push(batch);
    }
    Ok(batches)
}

fn last_ts_from_batch(batch: &RecordBatch) -> Result<Option<i64>, OhlcArrowError> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "ts column is not int64".to_string(),
        })?;
    if col.is_null(col.len() - 1) {
        return Err(OhlcArrowError::NullField {
            field: "ts",
            index: col.len() - 1,
        });
    }
    Ok(Some(col.value(col.len() - 1)))
}

fn validate_schema(schema: &Schema) -> Result<(), OhlcArrowError> {
    let expected = [
        ("ts", DataType::Int64, false),
        ("open", DataType::Float64, false),
        ("high", DataType::Float64, false),
        ("low", DataType::Float64, false),
        ("close", DataType::Float64, false),
        ("volume", DataType::Float64, true),
    ];
    if schema.fields().len() != expected.len() {
        return Err(OhlcArrowError::SchemaMismatch {
            details: format!(
                "expected {} fields, got {}",
                expected.len(),
                schema.fields().len()
            ),
        });
    }
    for (idx, (name, dtype, nullable)) in expected.iter().enumerate() {
        let field = schema.field(idx);
        if field.name() != *name {
            return Err(OhlcArrowError::SchemaMismatch {
                details: format!("field {idx} name mismatch: {found}", found = field.name()),
            });
        }
        if field.data_type() != dtype {
            return Err(OhlcArrowError::SchemaMismatch {
                details: format!(
                    "field {name} type mismatch: {found:?}",
                    found = field.data_type()
                ),
            });
        }
        if field.is_nullable() != *nullable {
            return Err(OhlcArrowError::SchemaMismatch {
                details: format!(
                    "field {name} nullability mismatch: {found} != {expected}",
                    found = field.is_nullable(),
                    expected = nullable
                ),
            });
        }
    }
    Ok(())
}

fn read_all_from_file<R: Read + Seek>(
    reader: FileReader<R>,
) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    validate_schema(reader.schema().as_ref())?;
    let mut out = Vec::new();
    let mut base_index = 0usize;
    for batch in reader {
        let batch = batch?;
        append_batch_rows(&batch, 0, batch.num_rows(), base_index, &mut out)?;
        base_index = out.len();
    }
    Ok(out)
}

fn read_tail_from_file<R: Read + Seek>(
    mut reader: FileReader<R>,
    limit: usize,
) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    validate_schema(reader.schema().as_ref())?;
    let num_batches = reader.num_batches();
    let mut reversed: Vec<OhlcBar> = Vec::new();
    'outer: for idx in (0..num_batches).rev() {
        reader.set_index(idx)?;
        let batch = match reader.next().transpose()? {
            Some(batch) => batch,
            None => continue,
        };
        let rows = batch.num_rows();
        if rows == 0 {
            continue;
        }
        let mut local = Vec::new();
        append_batch_rows(&batch, 0, rows, 0, &mut local)?;
        for bar in local.into_iter().rev() {
            reversed.push(bar);
            if reversed.len() == limit {
                break 'outer;
            }
        }
    }
    reversed.reverse();
    Ok(reversed)
}

fn read_range_from_file<R: Read + Seek>(
    reader: FileReader<R>,
    start_ts: i64,
    end_ts: i64,
    limit: Option<usize>,
) -> Result<Vec<OhlcBar>, OhlcArrowError> {
    validate_schema(reader.schema().as_ref())?;
    let mut out = Vec::new();
    let mut base_index = 0usize;
    for batch in reader {
        let batch = batch?;
        let done = append_batch_range(&batch, base_index, start_ts, end_ts, limit, &mut out)?;
        base_index += batch.num_rows();
        if done {
            break;
        }
    }
    Ok(out)
}

fn append_batch_rows(
    batch: &RecordBatch,
    start: usize,
    end: usize,
    base_index: usize,
    out: &mut Vec<OhlcBar>,
) -> Result<(), OhlcArrowError> {
    let ts_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "ts column type mismatch".to_string(),
        })?;
    let open_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "open column type mismatch".to_string(),
        })?;
    let high_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "high column type mismatch".to_string(),
        })?;
    let low_array = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "low column type mismatch".to_string(),
        })?;
    let close_array = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "close column type mismatch".to_string(),
        })?;
    let volume_array = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "volume column type mismatch".to_string(),
        })?;

    for row in start..end {
        let global_index = base_index + row;
        if ts_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "ts",
                index: global_index,
            });
        }
        if open_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "open",
                index: global_index,
            });
        }
        if high_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "high",
                index: global_index,
            });
        }
        if low_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "low",
                index: global_index,
            });
        }
        if close_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "close",
                index: global_index,
            });
        }
        if volume_array.is_null(row) {
            return Err(OhlcArrowError::NullVolume {
                index: global_index,
            });
        }
        out.push(OhlcBar {
            ts_ms: ts_array.value(row),
            open: open_array.value(row),
            high: high_array.value(row),
            low: low_array.value(row),
            close: close_array.value(row),
            volume: volume_array.value(row),
        });
    }
    Ok(())
}

fn append_batch_range(
    batch: &RecordBatch,
    base_index: usize,
    start_ts: i64,
    end_ts: i64,
    limit: Option<usize>,
    out: &mut Vec<OhlcBar>,
) -> Result<bool, OhlcArrowError> {
    if batch.num_rows() == 0 {
        return Ok(false);
    }
    let ts_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "ts column type mismatch".to_string(),
        })?;
    ensure_no_nulls(ts_array, "ts", base_index)?;
    let batch_min = ts_array.value(0);
    let batch_max = ts_array.value(batch.num_rows() - 1);
    if batch_max < start_ts {
        return Ok(false);
    }
    if batch_min > end_ts {
        return Ok(true);
    }
    let open_array = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "open column type mismatch".to_string(),
        })?;
    let high_array = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "high column type mismatch".to_string(),
        })?;
    let low_array = batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "low column type mismatch".to_string(),
        })?;
    let close_array = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "close column type mismatch".to_string(),
        })?;
    let volume_array = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OhlcArrowError::SchemaMismatch {
            details: "volume column type mismatch".to_string(),
        })?;

    let start_idx = if start_ts <= batch_min {
        0
    } else {
        lower_bound_i64(ts_array, start_ts)
    };
    let end_excl = if end_ts >= batch_max {
        batch.num_rows()
    } else {
        upper_bound_i64(ts_array, end_ts)
    };
    if start_idx >= end_excl {
        return Ok(false);
    }

    for row in start_idx..end_excl {
        if open_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "open",
                index: base_index + row,
            });
        }
        if high_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "high",
                index: base_index + row,
            });
        }
        if low_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "low",
                index: base_index + row,
            });
        }
        if close_array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field: "close",
                index: base_index + row,
            });
        }
        if volume_array.is_null(row) {
            return Err(OhlcArrowError::NullVolume {
                index: base_index + row,
            });
        }
        let ts = ts_array.value(row);
        out.push(OhlcBar {
            ts_ms: ts,
            open: open_array.value(row),
            high: high_array.value(row),
            low: low_array.value(row),
            close: close_array.value(row),
            volume: volume_array.value(row),
        });
        if let Some(max) = limit {
            if out.len() >= max {
                return Ok(true);
            }
        }
    }
    if batch_max > end_ts {
        return Ok(true);
    }
    Ok(false)
}

fn ensure_no_nulls(
    array: &dyn Array,
    field: &'static str,
    base_index: usize,
) -> Result<(), OhlcArrowError> {
    if array.null_count() == 0 {
        return Ok(());
    }
    for row in 0..array.len() {
        if array.is_null(row) {
            return Err(OhlcArrowError::NullField {
                field,
                index: base_index + row,
            });
        }
    }
    Ok(())
}

fn lower_bound_i64(values: &Int64Array, target: i64) -> usize {
    let mut left = 0usize;
    let mut right = values.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if values.value(mid) < target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound_i64(values: &Int64Array, target: i64) -> usize {
    let mut left = 0usize;
    let mut right = values.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if values.value(mid) <= target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

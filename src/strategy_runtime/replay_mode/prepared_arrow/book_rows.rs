use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use polymarket_client_sdk::types::U256;

use super::io::{float_col, int_col, opt_f64, opt_i64, req_str, str_col};
use super::mapped_ipc::read_mapped_batches;
use super::*;

#[derive(Debug, Clone)]
pub(in crate::strategy_runtime::replay_mode) struct BookRow {
    pub(super) remove: bool,
    pub(super) token_id: String,
    pub token: Option<U256>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub(super) bid_sz: Option<f64>,
    pub(super) ask_sz: Option<f64>,
    pub(super) tick_size: f64,
    pub(super) min_order_size: f64,
    pub ts_ms: u64,
    pub(super) depth: Option<BookDepth>,
}

#[derive(Debug)]
pub(in crate::strategy_runtime::replay_mode) struct PreparedBookRows {
    batches: Vec<RecordBatch>,
    starts: Vec<usize>,
    tokens: HashMap<String, U256>,
    len: usize,
}

impl PreparedBookRows {
    pub(in crate::strategy_runtime::replay_mode) fn read(path: &Path) -> Result<Self> {
        let batches =
            read_mapped_batches(path).with_context(|| format!("read mapped {}", path.display()))?;
        let expected_schema = book_schema();
        let mut starts = Vec::with_capacity(batches.len());
        let mut tokens = HashMap::new();
        let mut len = 0usize;
        for batch in &batches {
            anyhow::ensure!(
                batch.schema().as_ref() == expected_schema.as_ref(),
                "token_book_delta Arrow schema mismatch"
            );
            validate_batch(batch, len, &mut tokens)?;
            starts.push(len);
            len = len
                .checked_add(batch.num_rows())
                .context("token_book_delta row count overflow")?;
        }
        Ok(Self {
            batches,
            starts,
            tokens,
            len,
        })
    }

    pub(in crate::strategy_runtime::replay_mode) fn visit_rows<F>(
        &self,
        start: i64,
        len: i64,
        name: &str,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(BookRow) -> Result<()>,
    {
        let Some((start, end)) = self.checked_range(start, len, name)? else {
            return Ok(());
        };

        let mut cursor = start;
        while cursor < end {
            let batch_index = self
                .starts
                .partition_point(|batch_start| *batch_start <= cursor)
                - 1;
            let batch_start = self.starts[batch_index];
            let batch = &self.batches[batch_index];
            let local_start = cursor - batch_start;
            let count = (end - cursor).min(batch.num_rows() - local_start);
            decode_rows(
                batch,
                local_start,
                count,
                batch_start,
                &self.tokens,
                &mut visit,
            )?;
            cursor += count;
        }
        Ok(())
    }

    pub(in crate::strategy_runtime::replay_mode) fn len(&self) -> usize {
        self.len
    }

    pub(in crate::strategy_runtime::replay_mode) fn validate_tokens(
        &self,
        expected: &HashSet<U256>,
    ) -> Result<()> {
        for token in self.tokens.values() {
            anyhow::ensure!(
                expected.contains(token),
                "token_book_delta references foreign token {token}"
            );
        }
        Ok(())
    }

    fn checked_range(&self, start: i64, len: i64, name: &str) -> Result<Option<(usize, usize)>> {
        anyhow::ensure!(len >= 0, "{name} negative len");
        if len == 0 {
            return Ok(None);
        }
        anyhow::ensure!(start >= 0, "{name} negative start");
        let start = usize::try_from(start).context("book row start exceeds usize")?;
        let len = usize::try_from(len).context("book row length exceeds usize")?;
        let end = start.checked_add(len).context("book row range overflow")?;
        anyhow::ensure!(
            end <= self.len,
            "engine_clock references missing {name} rows"
        );
        Ok(Some((start, end)))
    }
}

fn validate_batch(
    batch: &RecordBatch,
    global_start: usize,
    tokens: &mut HashMap<String, U256>,
) -> Result<()> {
    let kind = str_col(batch, 0, "row_kind")?;
    let token_id = str_col(batch, 1, "token_id")?;
    let depth_revision = int_col(batch, 9 + STORED_DEPTH_LEVELS * 4, "depth_revision")?;
    for index in 0..batch.num_rows() {
        let row = global_start + index;
        match req_str(kind, index)? {
            row_kind @ ("book" | "remove") => {
                let raw = req_str(token_id, index)?;
                if !tokens.contains_key(raw) {
                    let token = U256::from_str(raw).with_context(|| {
                        format!("invalid token_book_delta token_id at row {row}")
                    })?;
                    tokens.insert(raw.to_string(), token);
                }
                if row_kind == "book" {
                    if let Some(revision) = opt_i64(depth_revision, index) {
                        anyhow::ensure!(revision > 0, "token depth revision is zero at row {row}");
                    }
                }
            }
            other => anyhow::bail!("unknown token_book_delta row kind `{other}` at row {row}"),
        }
    }
    Ok(())
}

fn decode_rows<F>(
    batch: &RecordBatch,
    start: usize,
    len: usize,
    global_start: usize,
    tokens: &HashMap<String, U256>,
    visit: &mut F,
) -> Result<()>
where
    F: FnMut(BookRow) -> Result<()>,
{
    let kind = str_col(batch, 0, "row_kind")?;
    let token_id = str_col(batch, 1, "token_id")?;
    let bid = float_col(batch, 2, "bid")?;
    let ask = float_col(batch, 3, "ask")?;
    let bid_sz = float_col(batch, 4, "bid_sz")?;
    let ask_sz = float_col(batch, 5, "ask_sz")?;
    let tick = float_col(batch, 6, "tick_size")?;
    let min_order = float_col(batch, 7, "min_order_size")?;
    let ts = int_col(batch, 8, "ts_ms")?;
    let depth_cols = depth_columns(batch, 9)?;
    let depth_revision = int_col(batch, 9 + STORED_DEPTH_LEVELS * 4, "depth_revision")?;

    for index in start..start + len {
        let row_index = global_start + index;
        let row_kind = req_str(kind, index)?;
        let token = match row_kind {
            "book" | "remove" => Some(
                *tokens
                    .get(req_str(token_id, index)?)
                    .with_context(|| format!("missing interned token at row {row_index}"))?,
            ),
            other => {
                anyhow::bail!("unknown token_book_delta row kind `{other}` at row {row_index}")
            }
        };
        let depth = if let Some(token_id) = token {
            let (bids, asks) = decode_ladder(&depth_cols, index);
            if row_kind == "remove" {
                None
            } else if let Some(revision) = opt_i64(depth_revision, index) {
                anyhow::ensure!(
                    revision > 0,
                    "token depth revision is zero at row {row_index}"
                );
                Some(BookDepth {
                    token_id,
                    bids,
                    asks,
                    ts_ms: opt_i64(ts, index).unwrap_or(0) as u64,
                    revision: revision as u64,
                    observed_at_ms: 0,
                })
            } else {
                anyhow::ensure!(
                    bids.is_empty() && asks.is_empty(),
                    "token depth levels exist without revision at row {row_index}"
                );
                None
            }
        } else {
            None
        };
        visit(BookRow {
            remove: row_kind == "remove",
            token_id: String::new(),
            token,
            bid: opt_f64(bid, index),
            ask: opt_f64(ask, index),
            bid_sz: opt_f64(bid_sz, index),
            ask_sz: opt_f64(ask_sz, index),
            tick_size: opt_f64(tick, index).unwrap_or(0.0),
            min_order_size: opt_f64(min_order, index).unwrap_or(0.0),
            ts_ms: opt_i64(ts, index).unwrap_or(0) as u64,
            depth,
        })?;
    }
    Ok(())
}

type DepthColumns<'a> = Vec<(
    &'a arrow_array::Float64Array,
    &'a arrow_array::Float64Array,
    &'a arrow_array::Float64Array,
    &'a arrow_array::Float64Array,
)>;

fn depth_columns(batch: &RecordBatch, first: usize) -> Result<DepthColumns<'_>> {
    (0..STORED_DEPTH_LEVELS)
        .map(|level| {
            let base = first + level * 4;
            Ok((
                float_col(batch, base, "bid_depth_px")?,
                float_col(batch, base + 1, "bid_depth_sz")?,
                float_col(batch, base + 2, "ask_depth_px")?,
                float_col(batch, base + 3, "ask_depth_sz")?,
            ))
        })
        .collect()
}

fn decode_ladder(columns: &DepthColumns<'_>, index: usize) -> (Vec<PriceLevel>, Vec<PriceLevel>) {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for (bid_px, bid_size, ask_px, ask_size) in columns {
        if let (Some(price), Some(size)) = (opt_f64(bid_px, index), opt_f64(bid_size, index)) {
            bids.push(PriceLevel { price, size });
        }
        if let (Some(price), Some(size)) = (opt_f64(ask_px, index), opt_f64(ask_size, index)) {
            asks.push(PriceLevel { price, size });
        }
    }
    (bids, asks)
}

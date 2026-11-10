use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use polymarket_client_sdk::types::{B256, U256};
use sha2::{Digest, Sha256};

use super::{MarketRow, PreparedReplayWriter};
use crate::capture::polymarket_resolution::PolymarketResolutionCapture;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::strategy_runtime::replay_mode) struct ResolutionRow {
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
    pub winning_asset_id: String,
    pub observed_at_ms: u64,
    pub raw_response_sha256: String,
}

pub(super) fn resolution_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("up_token", DataType::Utf8, false),
        Field::new("down_token", DataType::Utf8, false),
        Field::new("winning_asset_id", DataType::Utf8, false),
        Field::new("observed_at_ms", DataType::Int64, false),
        Field::new("raw_response_sha256", DataType::Utf8, false),
    ]))
}

fn resolution_batch(schema: SchemaRef, rows: &[ResolutionRow]) -> Result<RecordBatch> {
    let observed_timestamps = rows
        .iter()
        .map(|row| {
            i64::try_from(row.observed_at_ms)
                .context("resolution observation timestamp exceeds Arrow signed range")
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.condition_id.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.up_token.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.down_token.as_str()),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.winning_asset_id.as_str()),
            )),
            Arc::new(Int64Array::from(observed_timestamps)),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.raw_response_sha256.as_str()),
            )),
        ],
    )
    .context("build resolution_delta batch")
}

pub(super) fn read_resolution_rows(path: &std::path::Path) -> Result<Vec<ResolutionRow>> {
    let mut rows = Vec::new();
    for batch in super::io::read_batches(path)? {
        let strings = (0..4)
            .map(|index| super::io::str_col(&batch, index, "resolution"))
            .collect::<Result<Vec<_>>>()?;
        let observed = super::io::int_col(&batch, 4, "observed_at_ms")?;
        let raw_response_sha256 = super::io::str_col(&batch, 5, "raw_response_sha256")?;
        for index in 0..batch.num_rows() {
            rows.push(ResolutionRow {
                condition_id: super::io::req_str(strings[0], index)?.to_string(),
                up_token: super::io::req_str(strings[1], index)?.to_string(),
                down_token: super::io::req_str(strings[2], index)?.to_string(),
                winning_asset_id: super::io::req_str(strings[3], index)?.to_string(),
                observed_at_ms: u64::try_from(super::io::req_i64(observed, index)?)
                    .context("resolution observation timestamp is negative")?,
                raw_response_sha256: super::io::req_str(raw_response_sha256, index)?.to_string(),
            });
        }
    }
    Ok(rows)
}

impl PreparedReplayWriter {
    pub(crate) fn push_resolution(&mut self, capture: PolymarketResolutionCapture) -> Result<()> {
        anyhow::ensure!(
            capture.observed_at_ms > 0,
            "captured Polymarket resolution observation timestamp is zero"
        );
        anyhow::ensure!(
            capture.up_token != capture.down_token
                && (capture.winning_asset_id == capture.up_token
                    || capture.winning_asset_id == capture.down_token),
            "captured Polymarket resolution identity is not one exact binary pair"
        );
        anyhow::ensure!(
            !capture.raw_response.is_empty(),
            "captured Polymarket resolution raw response is empty"
        );
        self.resolutions.push(ResolutionRow {
            condition_id: capture.condition_id.to_string(),
            up_token: capture.up_token.to_string(),
            down_token: capture.down_token.to_string(),
            winning_asset_id: capture.winning_asset_id.to_string(),
            observed_at_ms: capture.observed_at_ms,
            raw_response_sha256: format!("{:x}", Sha256::digest(&capture.raw_response)),
        });
        self.stats.resolution_rows = self.stats.resolution_rows.saturating_add(1);
        self.stats.resolution_delta_rows = self.stats.resolution_delta_rows.saturating_add(1);
        self.flush_if_needed()
    }

    pub(super) fn flush_resolutions(&mut self) -> Result<()> {
        if self.resolutions.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.resolutions);
        self.resolution_writer
            .write(&resolution_batch(self.resolution_schema.clone(), &rows)?)
            .context("write resolution_delta")
    }
}

impl ResolutionRow {
    pub(super) fn validate(
        &self,
        markets: &[MarketRow],
        tokens: &std::collections::HashSet<U256>,
    ) -> Result<()> {
        let condition = B256::from_str(&self.condition_id)?;
        let up_token = U256::from_str(&self.up_token)?;
        let down_token = U256::from_str(&self.down_token)?;
        let winner = U256::from_str(&self.winning_asset_id)?;
        let market = markets
            .iter()
            .find(|row| row.market.condition_id_b256.as_deref() == Some(&condition.to_string()))
            .context("resolution condition has no captured market")?;
        anyhow::ensure!(
            tokens.contains(&winner)
                && tokens.contains(&up_token)
                && tokens.contains(&down_token)
                && up_token == market.market.up_token
                && down_token == market.market.down_token
                && (winner == up_token || winner == down_token)
                && market.market.window_end_ms <= self.observed_at_ms,
            "resolution identity is foreign or predates close"
        );
        anyhow::ensure!(
            self.raw_response_sha256.len() == 64
                && self
                    .raw_response_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "resolution raw response SHA-256 is not canonical lowercase hex"
        );
        Ok(())
    }
}

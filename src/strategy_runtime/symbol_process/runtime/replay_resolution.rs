use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::B256;

use super::{CandidateSymbolRuntime, WorkerRuntimeOutput};
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::symbol_process::protocol::OracleResolvedLabelRecord;

impl CandidateSymbolRuntime {
    pub(super) fn process_consumed_chainlink(
        &mut self,
        price: &crate::oracle_resolution::ChainlinkResolutionPrice,
        learning_applied_ts_ms: u64,
    ) -> Result<WorkerRuntimeOutput> {
        let actual_symbol = crate::oracle_resolution::canonical_chainlink_symbol(&price.symbol)?;
        let expected_symbol =
            crate::oracle_resolution::canonical_chainlink_symbol(self.symbol.as_str())?;
        anyhow::ensure!(
            actual_symbol == expected_symbol,
            "prepared Chainlink symbol is foreign to worker"
        );
        let catalog = self.catalog.clone();
        let candidate = self.candidate_mut()?;
        let labels = candidate
            .actor
            .strategy
            .observe_chainlink_resolution_price(price, learning_applied_ts_ms)?;
        let oracle_labels = labels
            .iter()
            .map(|label| oracle_label_record(label, &catalog))
            .collect::<Result<Vec<_>>>()?;
        Ok(WorkerRuntimeOutput {
            oracle_labels,
            ..WorkerRuntimeOutput::default()
        })
    }
}

fn oracle_label_record(
    label: &crate::oracle_resolution::OracleResolvedLabel,
    catalog: &MarketCatalog,
) -> Result<OracleResolvedLabelRecord> {
    anyhow::ensure!(
        label.market_id.as_str() == label.condition_id,
        "oracle label market/condition identity mismatch"
    );
    let condition = B256::from_str(&label.condition_id).with_context(|| {
        format!(
            "oracle label condition identity is invalid `{}`",
            label.condition_id
        )
    })?;
    let market_id = catalog
        .market_handle_for_condition(condition.0)
        .context("oracle label condition is absent from frozen catalog")?;
    OracleResolvedLabelRecord {
        market_id,
        winner: match label.winner {
            crate::strategy::hold_to_resolution::PairTokenSide::Up => 1,
            crate::strategy::hold_to_resolution::PairTokenSide::Down => 2,
        },
        flags: 0,
        window_start_ms: label.window_start_ms,
        window_end_ms: label.window_end_ms,
        release_ts_ms: label.release_ts_ms,
        open_price_e18: label.open.price_e18,
        close_price_e18: label.close.price_e18,
        open_source_ts_ms: label.open.source_ts_ms,
        open_observed_ts_ms: label.open.observed_ts_ms,
        open_capture_sequence: label.open.capture_sequence,
        close_source_ts_ms: label.close.source_ts_ms,
        close_observed_ts_ms: label.close.observed_ts_ms,
        close_capture_sequence: label.close.capture_sequence,
    }
    .validate()?
    .validate_catalog(catalog)
}

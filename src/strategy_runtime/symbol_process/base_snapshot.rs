use anyhow::{Context, Result};
use serde::Serialize;

use super::SymbolId;
use crate::config::{BbwickOracleConfig, MakerRebateMmStrategyConfig};

const WORKER_BASE_SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WorkerBaseSnapshot {
    pub version: u32,
    pub symbol: String,
    pub strategy_config: MakerRebateMmStrategyConfig,
    pub oracle_config: BbwickOracleConfig,
    pub equity_baseline_usdt: f64,
    pub replay_window: Option<WorkerReplayWindow>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct WorkerReplayWindow {
    pub start_ms: u64,
    pub end_ms: u64,
}

impl WorkerBaseSnapshot {
    pub(crate) fn new(
        symbol: SymbolId,
        strategy_config: MakerRebateMmStrategyConfig,
        oracle_config: BbwickOracleConfig,
        equity_baseline_usdt: f64,
        replay_window: Option<WorkerReplayWindow>,
    ) -> Result<Self> {
        let snapshot = Self {
            version: WORKER_BASE_SNAPSHOT_VERSION,
            symbol: symbol.as_str().to_string(),
            strategy_config,
            oracle_config,
            equity_baseline_usdt,
            replay_window,
        };
        snapshot.validate(symbol)?;
        Ok(snapshot)
    }

    pub(crate) fn encode_json(&self, expected_symbol: SymbolId) -> Result<Vec<u8>> {
        self.validate(expected_symbol)?;
        serde_json::to_vec(self).context("encode symbol runtime base snapshot")
    }

    fn validate(&self, expected_symbol: SymbolId) -> Result<()> {
        anyhow::ensure!(
            self.version == WORKER_BASE_SNAPSHOT_VERSION,
            "unsupported symbol runtime base snapshot version={} expected={WORKER_BASE_SNAPSHOT_VERSION}",
            self.version
        );
        anyhow::ensure!(
            self.symbol == expected_symbol.as_str(),
            "symbol runtime base snapshot mismatch expected={} actual={}",
            expected_symbol.as_str(),
            self.symbol
        );
        self.strategy_config
            .validate_symbol_runtime(expected_symbol.as_str())?;
        let active_oracle_symbols = self.oracle_config.active_symbols();
        anyhow::ensure!(
            active_oracle_symbols.len() == 1
                && active_oracle_symbols[0].eq_ignore_ascii_case(
                    &self
                        .oracle_config
                        .resolve_symbol_for_underlying(expected_symbol.as_str()),
                ),
            "symbol runtime base oracle config is not isolated symbol={}",
            expected_symbol.as_str()
        );
        anyhow::ensure!(
            self.equity_baseline_usdt.is_finite() && self.equity_baseline_usdt >= 0.0,
            "symbol runtime equity baseline is invalid"
        );
        if let Some(window) = self.replay_window {
            anyhow::ensure!(
                window.start_ms > 0 && window.start_ms <= window.end_ms,
                "symbol runtime replay window is invalid start={} end={}",
                window.start_ms,
                window.end_ms
            );
        }
        Ok(())
    }
}

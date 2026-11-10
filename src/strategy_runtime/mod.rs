use anyhow::{Context, Result};
use std::path::PathBuf;

pub mod actors;
pub mod backtest_config_truth;
pub mod backtest_data_quality;
pub mod backtest_live_latency;
pub mod backtest_matrix;
pub mod backtest_matrix_report;
pub mod backtest_ohlc_manifest;
pub mod backtest_run_identity;
pub mod driver;
pub mod exchange_sim;
pub(crate) mod market_catalog;
pub mod prediction_metrics;
pub mod recompute;
pub mod replay_mode;
pub mod runtime_driver;
pub mod state;
pub(crate) mod symbol_process;
pub use symbol_process::SymbolId;
pub mod types;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    Live,
    Replay,
}

impl RuntimeMode {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "live" => Ok(Self::Live),
            "replay" => Ok(Self::Replay),
            other => anyhow::bail!("unsupported --mode `{other}`; expected live|replay"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeArgs {
    pub mode: Option<RuntimeMode>,
    pub capture_session: Option<PathBuf>,
}

impl RuntimeArgs {
    pub fn effective_mode(&self) -> RuntimeMode {
        self.mode.unwrap_or(RuntimeMode::Live)
    }
}

pub fn parse_runtime_args(args: &[String]) -> Result<RuntimeArgs> {
    let mut parsed = RuntimeArgs::default();
    let mut idx = 1usize;
    while idx < args.len() {
        match args[idx].as_str() {
            "--mode" => {
                let value = args.get(idx + 1).context("--mode requires a value")?;
                parsed.mode = Some(RuntimeMode::parse(value)?);
                idx += 2;
            }
            "--capture-session" => {
                let value = args
                    .get(idx + 1)
                    .context("--capture-session requires a value")?;
                parsed.capture_session = Some(PathBuf::from(value));
                idx += 2;
            }
            _ => {
                idx += 1;
            }
        }
    }

    if parsed.effective_mode() == RuntimeMode::Replay && parsed.capture_session.is_none() {
        anyhow::bail!("replay mode requires --capture-session <dir|latest>");
    }

    Ok(parsed)
}

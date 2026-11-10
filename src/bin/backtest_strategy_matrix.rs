use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use polymarket_hft::config::AppConfig;
use polymarket_hft::event_ledger::ensure_runtime_layout;
use polymarket_hft::strategy_runtime::backtest_matrix::{
    acquire_offline_run_lane, run_backtest_strategy_matrix, BacktestMatrixArgs,
    BacktestMatrixEvaluator, BacktestOracleResearchVariant,
};

#[tokio::main]
async fn main() -> Result<()> {
    ensure_runtime_layout()?;
    let (args, prepare_only) = parse_args()?;
    let cfg = AppConfig::load()?;
    if prepare_only {
        acquire_offline_run_lane()?;
        if args.symbols.is_empty() {
            BacktestMatrixEvaluator::prepare(&cfg, &args.capture_session)?;
        } else {
            BacktestMatrixEvaluator::prepare_for_symbols(
                &cfg,
                &args.capture_session,
                &args.symbols,
            )?;
        }
        return Ok(());
    }
    let code = run_backtest_strategy_matrix(&cfg, args).await?;
    std::process::exit(code);
}

fn parse_args() -> Result<(BacktestMatrixArgs, bool)> {
    let mut parsed = BacktestMatrixArgs::default();
    let mut prepare_only = false;
    // Env is the debug/fast path: BACKTEST_STRATEGY_SYMBOLS=sol or btc,eth
    if let Ok(raw) = std::env::var("BACKTEST_STRATEGY_SYMBOLS") {
        parsed.symbols = parse_symbol_list(&raw);
    }
    let args = std::env::args().collect::<Vec<_>>();
    let mut idx = 1usize;
    while idx < args.len() {
        match args[idx].as_str() {
            "--prepare-only" => {
                prepare_only = true;
                idx += 1;
            }
            "--capture-session" => {
                parsed.capture_session = PathBuf::from(
                    args.get(idx + 1)
                        .context("--capture-session requires a value")?,
                );
                idx += 2;
            }
            "--symbols" => {
                let raw = args
                    .get(idx + 1)
                    .context("--symbols requires a value (e.g. sol or btc,eth)")?;
                parsed.symbols = parse_symbol_list(raw);
                idx += 2;
            }
            "--oracle-learning-seed" => {
                parsed.oracle_learning_seed = Some(PathBuf::from(
                    args.get(idx + 1)
                        .context("--oracle-learning-seed requires a value")?,
                ));
                idx += 2;
            }
            "--oracle-learning-seed-sha256" => {
                parsed.oracle_learning_seed_sha256 = Some(
                    args.get(idx + 1)
                        .context("--oracle-learning-seed-sha256 requires a value")?
                        .clone(),
                );
                idx += 2;
            }
            "--oracle-research-variant" => {
                parsed.oracle_research_variant = Some(BacktestOracleResearchVariant::parse(
                    args.get(idx + 1)
                        .context("--oracle-research-variant requires a value")?,
                )?);
                idx += 2;
            }
            "--oracle-research-root" => {
                parsed.oracle_research_root = Some(PathBuf::from(
                    args.get(idx + 1)
                        .context("--oracle-research-root requires a value")?,
                ));
                idx += 2;
            }
            other => bail!("unknown argument {other}"),
        }
    }
    anyhow::ensure!(
        parsed.oracle_learning_seed.is_some() == parsed.oracle_learning_seed_sha256.is_some(),
        "research oracle learning seed path/hash mismatch"
    );
    anyhow::ensure!(
        parsed.oracle_research_variant.is_some() == parsed.oracle_research_root.is_some(),
        "oracle research variant/root mismatch"
    );
    anyhow::ensure!(
        !prepare_only
            || (parsed.oracle_learning_seed.is_none()
                && parsed.oracle_learning_seed_sha256.is_none()
                && parsed.oracle_research_variant.is_none()
                && parsed.oracle_research_root.is_none()),
        "--prepare-only cannot be combined with oracle research or learner seed controls"
    );
    Ok((parsed, prepare_only))
}

fn parse_symbol_list(raw: &str) -> Vec<String> {
    raw.split([',', ' ', ';'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
        .collect()
}

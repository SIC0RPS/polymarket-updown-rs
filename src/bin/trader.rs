#[path = "../app/mod.rs"]
mod app;

use std::time::Duration;

use anyhow::Result;
use polymarket_hft::config::AppConfig;
use polymarket_hft::event_ledger::ensure_runtime_layout;
use polymarket_hft::execution::http_support::advance_capped_full_jitter_backoff_ms;
use polymarket_hft::strategy_runtime::{parse_runtime_args, RuntimeMode};

use crate::app::{
    detect_cli_mode, enforce_live_env_requirements, install_rustls_crypto_provider, load_env_file,
    run_main_runtime, CliMode,
};

const LIVE_RETRY_BASE_MS: u64 = 1_000;
const LIVE_RETRY_MAX_MS: u64 = 30_000;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let runtime_args = parse_runtime_args(&args)?;

    install_rustls_crypto_provider();
    match detect_cli_mode(&args) {
        CliMode::Run => match runtime_args.effective_mode() {
            RuntimeMode::Live => {
                crate::app::live_runtime::startup::install_panic_hook();
                let mut attempt = 0_usize;
                let mut backoff_ms = LIVE_RETRY_BASE_MS;
                let signal_rx = loop {
                    match crate::app::shutdown::install_signal_incident_channel() {
                        Ok(signal_rx) => break signal_rx,
                        Err(err) => tracing::error!(
                            target: "app.live_supervisor",
                            error = %err,
                            "live_signal_registration_failed"
                        ),
                    }
                    attempt = attempt.saturating_add(1);
                    let sleep_ms = advance_capped_full_jitter_backoff_ms(
                        attempt,
                        &mut backoff_ms,
                        LIVE_RETRY_MAX_MS,
                    )
                    .max(LIVE_RETRY_BASE_MS);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                };
                attempt = 0;
                backoff_ms = LIVE_RETRY_BASE_MS;
                let cfg = loop {
                    attempt = attempt.saturating_add(1);
                    let env_load_res = load_env_file();
                    match ensure_runtime_layout()
                        .and_then(|()| AppConfig::load())
                        .and_then(|cfg| {
                            enforce_live_env_requirements(&cfg, &env_load_res)?;
                            Ok(cfg)
                        }) {
                        Ok(cfg) => break cfg,
                        Err(err) => {
                            tracing::error!(target: "app.live_supervisor", attempt, error = %err, "live_configuration_failed");
                            let sleep_ms = advance_capped_full_jitter_backoff_ms(
                                attempt,
                                &mut backoff_ms,
                                LIVE_RETRY_MAX_MS,
                            )
                            .max(LIVE_RETRY_BASE_MS);
                            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        }
                    }
                };
                run_main_runtime(cfg, signal_rx).await
            }
            RuntimeMode::Replay => {
                let env_load_res = load_env_file();
                ensure_runtime_layout()?;
                let cfg = AppConfig::load()?;
                let _ = env_load_res;
                let exit_code = polymarket_hft::strategy_runtime::backtest_matrix::run_backtest_strategy_matrix(
                    &cfg,
                    polymarket_hft::strategy_runtime::backtest_matrix::BacktestMatrixArgs {
                        capture_session: runtime_args
                            .capture_session
                            .clone()
                            .expect("replay mode requires capture session"),
                        ..Default::default()
                    },
                )
                .await?;
                anyhow::ensure!(exit_code == 0, "replay failed with exit code {exit_code}");
                Ok(())
            }
        },
        CliMode::OptimizeUnsupported => anyhow::bail!(
            "`trader --optimize` was removed; use `make optimize` for Rust cmaes optimization"
        ),
        CliMode::Approve => {
            let _ = load_env_file();
            ensure_runtime_layout()?;
            let cfg = AppConfig::load()?;
            polymarket_hft::approve::run_approve(&cfg).await
        }
    }
}

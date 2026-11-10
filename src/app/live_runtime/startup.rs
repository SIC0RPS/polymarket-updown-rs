use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use bbwick_engine::{
    ensure_engine_history_ready, OracleEngineConfig, OracleHistory, OracleHistoryConfig,
};
use polymarket_hft::capture::session::{init_capture_session, MaybeCapture};
use polymarket_hft::config::{
    installed_optimizer_symbol_path, optimizer_learning_selection, AppConfig,
};
use polymarket_hft::event_ledger::{
    append_event, current_run_id, latest_config_hash_after, runtime_panic_incident_event,
    runtime_stop_event, RunIdProvider,
};
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::observability::Observability;
use polymarket_hft::runtime_control::latest_operator_pause_command;
use polymarket_hft::runtime_files::{write_terminal_health_file, PAUSE_JSONL_PATH};
use polymarket_hft::strategy::OracleLearningSnapshot;
use polymarket_hft::strategy_runtime::actors::SYMBOL_ACTOR_ORDER;
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::app::bootstrap::{now_ms, MAIN_CONFIG_PATH};

const LIVE_HEARTBEAT_MONITOR_INTERVAL_SECS: u64 = 5;
const ORACLE_LEARNING_CONTINUATION_DIR: &str = "data/oracle_learning/continuation";

pub(super) struct SessionCancellationGuard(pub(super) tokio_util::sync::CancellationToken);

impl Drop for SessionCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn publish_startup_status(cfg: &AppConfig, obs: &Observability, mode: &str) {
    obs.set_market_data_source(mode);
    obs.set_enabled_assets(cfg.strategy.maker_rebate_mm.enabled_assets.clone());
    tracing::info!(target: "app.live_runtime", mode, "runtime_startup");

    tracing::info!(
        target: "trader::app::live_runtime",
        symbols = cfg.symbols.len(),
        max_order_qty = cfg.max_order_qty,
        mode,
        polymarket_market_data_enabled = cfg.polymarket.market_data_enabled,
        polymarket_execution_mode = "live",
        polymarket_execution_pause = cfg.polymarket.execution.pause,
        "loaded config from main.toml"
    );
}

pub(super) async fn load_live_oracle_learning(
    cfg: &AppConfig,
) -> Result<(
    BTreeMap<String, OracleLearningSnapshot>,
    BTreeMap<String, OracleLearningSnapshot>,
    BTreeMap<String, PathBuf>,
)> {
    let cfg = cfg.clone();
    let startup_ts_ms = now_ms();
    tokio::task::spawn_blocking(move || {
        let mut selected_before_continuation = BTreeMap::new();
        let mut initial = BTreeMap::new();
        let mut continuation_paths = BTreeMap::new();
        for symbol in SYMBOL_ACTOR_ORDER {
            let symbol_spec = installed_optimizer_symbol_path(
                Path::new("."),
                symbol,
                cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
            );
            let Some(selection) = optimizer_learning_selection(&symbol_spec)? else {
                continue;
            };
            let selected_path = if selection.path.is_absolute() {
                selection.path.clone()
            } else {
                symbol_spec
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(&selection.path)
            };
            let selected_bytes = fs::read(&selected_path).with_context(|| {
                format!("read selected oracle learning {}", selected_path.display())
            })?;
            ensure!(
                format!("{:x}", Sha256::digest(&selected_bytes)) == selection.sha256,
                "selected oracle learning content changed symbol={symbol}"
            );
            let selected = decode_symbol_learning(&selected_bytes, symbol, &cfg, startup_ts_ms)
                .with_context(|| format!("decode selected oracle learning symbol={symbol}"))?;
            let continuation_path = Path::new(ORACLE_LEARNING_CONTINUATION_DIR)
                .join(format!("{symbol}-{}.rmp", selection.sha256));
            ensure!(
                selected_path != continuation_path,
                "selected oracle learning and continuation paths coincide symbol={symbol}"
            );
            let resolved = match fs::read(&continuation_path) {
                Ok(bytes) => {
                    selected_before_continuation.insert(symbol.to_string(), selected);
                    decode_symbol_learning(&bytes, symbol, &cfg, startup_ts_ms).with_context(
                        || {
                            format!(
                                "decode live oracle learning continuation {}",
                                continuation_path.display()
                            )
                        },
                    )?
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => selected,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "read live oracle learning continuation {}",
                            continuation_path.display()
                        )
                    });
                }
            };
            ensure!(
                initial.insert(symbol.to_string(), resolved).is_none(),
                "duplicate live oracle learning symbol={symbol}"
            );
            continuation_paths.insert(symbol.to_string(), continuation_path);
        }
        Ok((selected_before_continuation, initial, continuation_paths))
    })
    .await
    .context("join live oracle learning file reads")?
}

fn decode_symbol_learning(
    bytes: &[u8],
    symbol: &str,
    cfg: &AppConfig,
    startup_ts_ms: u64,
) -> Result<OracleLearningSnapshot> {
    let mut by_symbol: BTreeMap<String, OracleLearningSnapshot> = rmp_serde::from_slice(bytes)?;
    ensure!(
        by_symbol.len() == 1 && by_symbol.contains_key(symbol),
        "oracle learning file must contain only symbol={symbol}"
    );
    let snapshot = by_symbol
        .remove(symbol)
        .expect("validated oracle learning symbol");
    ensure!(
        snapshot.symbol() == cfg.bbwick_oracle.resolve_symbol_for_underlying(symbol),
        "oracle learning provider symbol mismatch symbol={symbol}"
    );
    ensure!(
        snapshot.knowledge_cutoff_ms() <= startup_ts_ms,
        "oracle learning knowledge cutoff is in the future symbol={symbol}"
    );
    Ok(snapshot)
}

pub(super) async fn ensure_optional_engine_history_ready(
    cfg: &AppConfig,
) -> Result<Option<Arc<OracleHistory>>> {
    if cfg.bbwick_oracle.enabled && cfg.polymarket.market_data_enabled {
        let engine_cfg = OracleEngineConfig {
            transport: cfg.bbwick_oracle.to_transport_settings(),
            ingest_channel_capacity: cfg.bbwick_oracle.runtime.ingest_channel_capacity,
            tick_channel_capacity: cfg.bbwick_oracle.runtime.tick_channel_capacity,
            active_symbols: polymarket_hft::capture::scope::required_bbwick_symbols_for_capture(
                cfg,
            ),
            history: OracleHistoryConfig {
                enabled: true,
                data_root: cfg.bbwick_oracle.history.resolved_data_root(),
                max_bars: cfg.bbwick_oracle.history.max_bars,
            },
            polymarket_ws_ingress_capture: None,
            polymarket_book_admission_capture: None,
            binance_provider_capture: None,
        };
        Ok(Some(ensure_engine_history_ready(&engine_cfg).await?))
    } else {
        Ok(None)
    }
}

pub(super) async fn start_live_capture_session(
    cfg: &AppConfig,
    shutdown: CancellationToken,
) -> Result<(MaybeCapture, Option<JoinHandle<Result<()>>>)> {
    if cfg.capture.enabled {
        let oracle_buy_and_hold = cfg.strategy.maker_rebate_mm.oracle_buy_and_hold;
        let config_toml = tokio::task::spawn_blocking(move || {
            let config_toml = fs::read_to_string(MAIN_CONFIG_PATH)
                .with_context(|| format!("read {MAIN_CONFIG_PATH} for live capture"))?;
            let mut captured_config: toml::Value =
                toml::from_str(&config_toml).context("parse live capture configuration")?;
            if captured_config
                .get("strategy")
                .and_then(|strategy| strategy.get("maker_rebate_mm"))
                .and_then(|strategy| strategy.get("oracle_buy_and_hold"))
                .and_then(toml::Value::as_bool)
                == Some(oracle_buy_and_hold)
            {
                return Ok(config_toml);
            }
            let captured_mode = captured_config
                .as_table_mut()
                .context("live capture configuration must be a table")?
                .entry("strategy")
                .or_insert_with(|| toml::Value::Table(Default::default()))
                .as_table_mut()
                .context("live capture strategy must be a table")?
                .entry("maker_rebate_mm")
                .or_insert_with(|| toml::Value::Table(Default::default()))
                .as_table_mut()
                .context("live capture maker_rebate_mm must be a table")?
                .entry("oracle_buy_and_hold")
                .or_insert(toml::Value::Boolean(oracle_buy_and_hold));
            *captured_mode = toml::Value::Boolean(oracle_buy_and_hold);
            toml::to_string(&captured_config).context("serialize live capture configuration")
        })
        .await
        .context("join live capture configuration read")??;
        let strategy_kind = "maker_rebate_mm".to_string();
        let capture_underlyings =
            polymarket_hft::capture::scope::configured_capture_underlyings(cfg);
        let replay_required_underlyings =
            polymarket_hft::capture::scope::replay_required_underlyings_for_capture(cfg);
        let capture_symbols = capture_underlyings
            .iter()
            .map(|symbol| format!("POLYMARKET-{}-USD", symbol))
            .collect::<Vec<_>>();
        let capture_ohlc_symbols =
            polymarket_hft::capture::scope::required_bbwick_symbols_for_capture(cfg);
        let (handle, task) = init_capture_session(
            &cfg.capture.base_dir,
            cfg.capture.max_duration_ms,
            cfg.capture.min_save_duration_ms,
            &capture_underlyings,
            &replay_required_underlyings,
            cfg.capture.max_sessions,
            &config_toml,
            oracle_buy_and_hold,
            &strategy_kind,
            &capture_symbols,
            cfg.bbwick_oracle.history.resolved_data_root(),
            capture_ohlc_symbols,
            cfg.bbwick_oracle.transport.step_timeframes.clone(),
            shutdown,
        )
        .await?;
        Ok((handle, Some(task)))
    } else {
        Ok((MaybeCapture::default(), None))
    }
}

pub(super) async fn latest_operator_pause_file_enabled() -> Result<Option<bool>> {
    tokio::task::spawn_blocking(|| {
        let contents = match fs::read_to_string(PAUSE_JSONL_PATH) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        Ok(latest_operator_pause_command(&contents)?.map(|command| command.enabled))
    })
    .await
    .context("join operator pause command read")?
}

pub(super) fn spawn_execution_heartbeat_monitor_task(
    exec: ExecutionClient,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut prev = exec.heartbeats_active();
        exec.live_ops().record_heartbeat_sample(Some(prev));
        let mut tick =
            tokio::time::interval(Duration::from_secs(LIVE_HEARTBEAT_MONITOR_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => {
                    let active = exec.heartbeats_active();
                    exec.live_ops().record_heartbeat_sample(Some(active));
                    if active == prev {
                        continue;
                    }
                    if active {
                        tracing::info!(
                            target: "app.live_runtime",
                            "execution_heartbeat_recovered"
                        );
                    } else {
                        tracing::warn!(
                            target: "app.live_runtime",
                            "execution_heartbeat_inactive"
                        );
                    }
                    prev = active;
                }
            }
        }
    })
}

pub(crate) fn install_panic_hook() {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let ts_ms = now_ms();
            let current_run_id = current_run_id().unwrap_or_else(|_| "live_startup".to_string());
            let event = runtime_panic_incident_event(
                &current_run_id,
                ts_ms,
                std::process::id(),
                format!("{info}"),
            );
            let _ = append_event(&event);
            default_hook(info);
        }));
    });
}

pub(super) fn append_shutdown_failure(result: &mut Result<()>, owner: &str, error: anyhow::Error) {
    let prior = std::mem::replace(result, Ok(()));
    *result = Err(match prior {
        Ok(()) => error.context(format!("{owner} shutdown failed")),
        Err(original) => original.context(format!("{owner} shutdown also failed: {error:#}")),
    });
}

pub(super) fn publish_terminal_result(
    run_id: &str,
    process_start_ms: u64,
    config_hash: Option<&str>,
    mut run_result: Result<()>,
) -> Result<()> {
    let stop_ts_ms = now_ms();
    let stop_config_hash = latest_config_hash_after().or_else(|| config_hash.map(str::to_string));
    let runtime_stop_record = runtime_stop_event(
        run_id,
        stop_ts_ms,
        std::process::id(),
        stop_ts_ms.saturating_sub(process_start_ms) as f64 / 1000.0,
        if run_result.is_ok() {
            "operator"
        } else {
            "fatal_error"
        },
    );
    if let Err(error) = append_event(&runtime_stop_record) {
        append_shutdown_failure(&mut run_result, "runtime stop event", error);
    }
    if let Err(error) = RunIdProvider::clear() {
        append_shutdown_failure(&mut run_result, "run identity release", error);
    }
    if let Err(error) = write_terminal_health_file(
        if run_result.is_ok() {
            "stopped"
        } else {
            "failed"
        },
        Some(run_id),
        Some(&runtime_stop_record.event_id),
        Some(stop_ts_ms),
        stop_config_hash.as_deref(),
    ) {
        append_shutdown_failure(&mut run_result, "terminal health", error);
    }

    run_result
}

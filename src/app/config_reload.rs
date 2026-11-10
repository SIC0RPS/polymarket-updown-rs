use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use polymarket_hft::capture::events::ControlActuationCapture;
use polymarket_hft::capture::session::{CaptureMsg, MaybeCapture};
use polymarket_hft::config::{AppConfig, SharedStrategyConfig};
use polymarket_hft::event_ledger::{
    append_event, config_reload_failed_event, current_run_id, snapshot_config_path,
};
use polymarket_hft::risk::RiskHandle;
use polymarket_hft::runtime_control::{PauseAuditInput, SharedRuntimeConfigState};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::app::bootstrap::{now_ms, MAIN_CONFIG_PATH};

const CONFIG_RELOAD_DEBOUNCE_MS: u64 = 150;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigFileStamp {
    len: u64,
    modified_at_ms: u128,
}

fn config_file_stamp(path: &str) -> Result<ConfigFileStamp> {
    let meta =
        std::fs::metadata(path).with_context(|| format!("failed to stat config path {path}"))?;
    let modified = meta
        .modified()
        .with_context(|| format!("failed to read modified time for {path}"))?;
    let modified_at_ms = modified
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    Ok(ConfigFileStamp {
        len: meta.len(),
        modified_at_ms,
    })
}

fn event_targets_main_toml(event: &Event) -> bool {
    let target = Path::new(MAIN_CONFIG_PATH);
    event
        .paths
        .iter()
        .any(|path| path == target || path.ends_with(target))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_config_reload_task(
    obs: polymarket_hft::observability::Observability,
    risk: RiskHandle,
    shared_strategy_cfg: SharedStrategyConfig,
    runtime_config_state: SharedRuntimeConfigState,
    shutdown: CancellationToken,
    run_id: String,
    capture: MaybeCapture,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let config_dir = Path::new(MAIN_CONFIG_PATH)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let (watch_tx, mut watch_rx) = mpsc::unbounded_channel::<notify::Result<Event>>();
        let mut watcher: RecommendedWatcher = match notify::recommended_watcher(move |res| {
            let _ = watch_tx.send(res);
        }) {
            Ok(watcher) => watcher,
            Err(err) => {
                tracing::error!(target: "app.config_reload", path = MAIN_CONFIG_PATH, error = %err, "main_toml_watch_init_failed");
                return;
            }
        };
        if let Err(err) = watcher.watch(&config_dir, RecursiveMode::NonRecursive) {
            tracing::error!(target: "app.config_reload", path = MAIN_CONFIG_PATH, error = %err, "main_toml_watch_start_failed");
            return;
        }
        let mut last_seen_stamp = config_file_stamp(MAIN_CONFIG_PATH).ok();
        let mut metadata_error_active = false;
        let mut pending_reload = false;
        let debounce_sleep = tokio::time::sleep(Duration::from_secs(24 * 60 * 60));
        tokio::pin!(debounce_sleep);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe_event = watch_rx.recv() => {
                    let Some(event) = maybe_event else {
                        break;
                    };
                    match event {
                        Ok(event) => {
                            if !event_targets_main_toml(&event) {
                                continue;
                            }
                            pending_reload = true;
                            debounce_sleep.as_mut().reset(
                                tokio::time::Instant::now()
                                    + Duration::from_millis(CONFIG_RELOAD_DEBOUNCE_MS),
                            );
                        }
                        Err(err) => {
                            tracing::warn!(target: "app.config_reload", path = MAIN_CONFIG_PATH, error = %err, "main_toml_watch_event_failed");
                        }
                    }
                }
                _ = &mut debounce_sleep, if pending_reload => {
                    pending_reload = false;
                    let stamp = match config_file_stamp(MAIN_CONFIG_PATH) {
                        Ok(stamp) => {
                            metadata_error_active = false;
                            stamp
                        }
                        Err(err) => {
                            if !metadata_error_active {
                                metadata_error_active = true;
                                tracing::warn!(target: "app.config_reload", path = MAIN_CONFIG_PATH, error = %err, "main_toml_watch_failed");
                            }
                            continue;
                        }
                    };
                    if last_seen_stamp.as_ref() == Some(&stamp) {
                        continue;
                    }
                    last_seen_stamp = Some(stamp);

                    match AppConfig::load() {
                        Ok(new_cfg) => {
                            let requested_pause = new_cfg.polymarket.execution.pause;
                            let pause_transition = match runtime_config_state
                                .set_requested_pause(
                                    requested_pause,
                                    PauseAuditInput {
                                        actor: "main_toml_hot_reload".to_string(),
                                        reason_code: "config_pause_source_changed".to_string(),
                                        reason: "polymarket_execution.PAUSE changed".to_string(),
                                        evidence: Some(json!({
                                            "source": MAIN_CONFIG_PATH,
                                            "requested_enabled": requested_pause,
                                        })),
                                    },
                                )
                                .await
                            {
                                Ok(transition) => transition,
                                Err(err) => {
                                    tracing::error!(
                                        target: "app.config_reload",
                                        path = MAIN_CONFIG_PATH,
                                        error = %err,
                                        "main_toml_pause_audit_failed"
                                    );
                                    continue;
                                }
                            };
                            let new_pause = pause_transition.after.pause;
                            if !requested_pause && new_pause {
                                tracing::info!(
                                    target: "app.config_reload",
                                    "startup_pause_floor_ignored_main_toml_unpause"
                                );
                            }
                            if pause_transition.source_changed {
                                capture.emit(CaptureMsg::ControlActuation(ControlActuationCapture {
                                    session_ts_ms: pause_transition
                                        .audit_ts_ms
                                        .expect("source-changing config pause has audit timestamp"),
                                    subject: "pause".to_string(),
                                    enabled: new_pause,
                                    source: "main_toml_hot_reload".to_string(),
                                    reason: Some(if new_pause == requested_pause {
                                        "polymarket_execution.PAUSE changed".to_string()
                                    } else {
                                        "startup pause floor forced PAUSE".to_string()
                                    }),
                                    actor: None,
                                }));
                            }
                            let new_strategy_cfg = new_cfg.strategy.maker_rebate_mm.clone();
                            shared_strategy_cfg.send_replace(new_strategy_cfg.clone());
                            risk.update_runtime_config(new_cfg.risk.clone(), new_cfg.max_order_qty);
                            obs.set_enabled_assets(new_strategy_cfg.enabled_assets.clone());
                            let new_config_snapshot =
                                snapshot_config_path(Path::new(MAIN_CONFIG_PATH)).ok();
                            runtime_config_state
                                .replace_from_config(
                                    &new_cfg,
                                    new_config_snapshot
                                        .as_ref()
                                        .map(|snapshot| snapshot.hash.clone()),
                                )
                                .await;
                            if new_cfg.capture.enabled {
                                if let Err(error) = capture.update_retention(
                                    &new_cfg.capture.base_dir,
                                    new_cfg.capture.max_sessions,
                                    new_cfg.capture.min_save_duration_ms,
                                )
                                .await
                                {
                                    tracing::warn!(
                                        target: "capture.retention",
                                        base_dir = %new_cfg.capture.base_dir,
                                        error = %error,
                                        "capture retention pruning failed after config reload"
                                    );
                                }
                            }
                            tracing::info!(
                                target: "app.config_reload",
                                path = MAIN_CONFIG_PATH,
                                "main_toml_reloaded"
                            );
                        }
                        Err(err) => {
                            let ts_ms = now_ms();
                            let current_run_id =
                                current_run_id().unwrap_or_else(|_| run_id.clone());
                            let event = config_reload_failed_event(
                                &current_run_id,
                                ts_ms,
                                std::process::id(),
                                MAIN_CONFIG_PATH,
                                format!("{err:#}"),
                            );
                            let _ = append_event(&event);
                            tracing::error!(target: "app.config_reload", path = MAIN_CONFIG_PATH, error = %err, "main_toml_reload_failed");
                        }
                    }
                }
            }
        }
    })
}

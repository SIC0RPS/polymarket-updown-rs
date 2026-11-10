use std::fs::{self, OpenOptions};
use std::path::Path;

use anyhow::Result;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::capture::events::ControlActuationCapture;
use crate::capture::session::{CaptureMsg, MaybeCapture};
use crate::event_ledger::ensure_runtime_layout;
use crate::runtime_control::{
    latest_operator_pause_command, PauseAuditInput, PauseFileCommand, SharedRuntimeConfigState,
};

use super::PAUSE_JSONL_PATH;

pub(super) async fn run_pause_file_watcher(
    runtime_config_state: SharedRuntimeConfigState,
    capture: MaybeCapture,
    shutdown: CancellationToken,
) {
    if ensure_runtime_layout().is_err() {
        return;
    }
    let _ = OpenOptions::new()
        .create(true)
        .append(true)
        .open(PAUSE_JSONL_PATH);
    let runtime_dir = Path::new(PAUSE_JSONL_PATH)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = match notify::recommended_watcher(move |res| {
        let _ = watch_tx.send(res);
    }) {
        Ok(watcher) => watcher,
        Err(err) => {
            tracing::error!(
                target: "runtime.control",
                path = PAUSE_JSONL_PATH,
                error = %err,
                "pause_watch_init_failed"
            );
            return;
        }
    };
    if let Err(err) = watcher.watch(&runtime_dir, RecursiveMode::NonRecursive) {
        tracing::error!(
            target: "runtime.control",
            path = %runtime_dir.display(),
            error = %err,
            "pause_watch_start_failed"
        );
        return;
    }
    let mut last_applied_line = String::new();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            maybe_event = watch_rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };
                match event {
                    Ok(event) => {
                        if !event_targets_pause_jsonl(&event) {
                            continue;
                        }
                        if let Err(err) = read_pause_command(
                            &runtime_config_state,
                            &capture,
                            &mut last_applied_line,
                        ).await {
                            tracing::error!(
                                target: "runtime.control",
                                path = PAUSE_JSONL_PATH,
                                error = %err,
                                "pause_command_apply_failed"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "runtime.control",
                            path = PAUSE_JSONL_PATH,
                            error = %err,
                            "pause_watch_event_failed"
                        );
                    }
                }
            }
        }
    }
}

fn event_targets_pause_jsonl(event: &Event) -> bool {
    let target = Path::new(PAUSE_JSONL_PATH);
    event
        .paths
        .iter()
        .any(|path| path == target || path.ends_with(target))
}

pub(super) fn write_pause_state_file(
    enabled: bool,
    actor: &str,
    reason: &str,
    comment: &str,
) -> Result<()> {
    let line = json!({
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "target": "all",
        "command": "set_pause",
        "enabled": enabled,
        "actor": actor,
        "reason": reason,
        "comment": comment,
    });
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(PAUSE_JSONL_PATH)?;
    use std::io::Write;
    writeln!(file, "{}", serde_json::to_string(&line)?)?;
    Ok(())
}

pub(super) async fn read_pause_command(
    runtime_config_state: &SharedRuntimeConfigState,
    capture: &MaybeCapture,
    last_applied_line: &mut String,
) -> Result<()> {
    let contents = fs::read_to_string(PAUSE_JSONL_PATH)?;
    let Some(command) = latest_operator_pause_command(&contents)? else {
        last_applied_line.clear();
        return Ok(());
    };
    let command_identity = serde_json::to_string(&command)?;
    if command_identity == last_applied_line.as_str() {
        return Ok(());
    }
    apply_pause_command(runtime_config_state, capture, command).await?;
    last_applied_line.clear();
    last_applied_line.push_str(&command_identity);

    Ok(())
}

async fn apply_pause_command(
    runtime_config_state: &SharedRuntimeConfigState,
    capture: &MaybeCapture,
    parsed: PauseFileCommand,
) -> Result<()> {
    let requested_pause = parsed.enabled;
    let actor = parsed
        .actor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("runtime_file")
        .to_string();
    let reason_code = parsed.command;
    let reason = parsed
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| parsed.comment.as_deref().map(str::trim))
        .filter(|value| !value.is_empty())
        .unwrap_or(if requested_pause {
            "operator requested stop"
        } else {
            "operator requested resume"
        })
        .to_string();
    let evidence = json!({
        "source": "data/runtime/PAUSE.jsonl",
        "requested_enabled": requested_pause,
        "timestamp": parsed.timestamp,
        "comment": parsed.comment,
        "target": parsed.target,
    });
    let pause_transition = runtime_config_state
        .set_requested_pause(
            requested_pause,
            PauseAuditInput {
                actor: actor.clone(),
                reason_code: reason_code.clone(),
                reason: reason.clone(),
                evidence: Some(evidence),
            },
        )
        .await?;
    let effective_pause = pause_transition.after.pause;
    if !requested_pause && effective_pause {
        tracing::info!(
            target: "runtime.control",
            "startup_pause_floor_ignored_unpause"
        );
    }
    if !pause_transition.source_changed {
        return Ok(());
    }
    let ts_ms = pause_transition
        .audit_ts_ms
        .expect("source-changing pause transition has canonical audit timestamp");
    capture.emit(CaptureMsg::ControlActuation(ControlActuationCapture {
        session_ts_ms: ts_ms,
        subject: "pause".to_string(),
        enabled: effective_pause,
        source: "data/runtime/PAUSE.jsonl".to_string(),
        reason: Some(reason.clone()),
        actor: Some(actor.clone()),
    }));
    tracing::info!(
        target: "runtime.control",
        ts_ms,
        enabled = effective_pause,
        requested_enabled = requested_pause,
        actor = %actor,
        reason_code = %reason_code,
        "pause_set"
    );
    Ok(())
}

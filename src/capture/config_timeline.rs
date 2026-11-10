use std::hash::{Hash, Hasher};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::event_ledger::{current_run_id, history_state_snapshot, CanonicalEventType};

pub(super) struct CaptureConfigTimelineInput<'a> {
    pub session_dir: &'a Path,
    pub session_id: &'a str,
    pub start_ts_ms: u64,
    pub end_ts_ms: u64,
    pub startup_config_hash: &'a str,
}

pub(super) struct WrittenCaptureConfigTimeline {
    pub timeline_hash: String,
    pub final_config_hash: String,
    pub change_count: usize,
}

#[derive(Debug, Serialize)]
struct CaptureConfigTimeline {
    schema_version: &'static str,
    session_id: String,
    run_id: Option<String>,
    start_ts_ms: u64,
    end_ts_ms: u64,
    startup_config_hash: String,
    final_config_hash: String,
    change_count: usize,
    changes: Vec<CapturedConfigChange>,
}

#[derive(Debug, Serialize)]
struct CapturedConfigChange {
    event_id: String,
    ts_ms: u64,
    subject: String,
    source: String,
    config_hash_before: Option<String>,
    config_hash_after: Option<String>,
    changed_paths: Vec<String>,
    changes: serde_json::Value,
}

pub(super) async fn write_capture_config_timeline(
    input: CaptureConfigTimelineInput<'_>,
) -> Result<WrittenCaptureConfigTimeline> {
    let run_id = current_run_id().ok();
    let mut changes = history_state_snapshot()
        .rows
        .iter()
        .filter(|event| event.event_type == CanonicalEventType::ConfigChange)
        .cloned()
        .filter(|event| {
            event.ts_ms >= input.start_ts_ms
                && event.ts_ms <= input.end_ts_ms
                && run_id
                    .as_ref()
                    .is_none_or(|run_id| event.run_id.as_str() == run_id.as_str())
        })
        .map(|event| {
            let changed_paths = event
                .context
                .get("changed_paths")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            CapturedConfigChange {
                event_id: event.event_id,
                ts_ms: event.ts_ms,
                subject: event.subject,
                source: event.source.as_str().to_string(),
                config_hash_before: event
                    .refs
                    .get("config_hash_before")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned),
                config_hash_after: event
                    .refs
                    .get("config_hash_after")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned),
                changed_paths,
                changes: event
                    .context
                    .get("changes")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            }
        })
        .collect::<Vec<_>>();
    changes.sort_by_key(|change| change.ts_ms);
    let final_config_hash = changes
        .iter()
        .rev()
        .find_map(|change| change.config_hash_after.clone())
        .unwrap_or_else(|| input.startup_config_hash.to_string());
    let timeline = CaptureConfigTimeline {
        schema_version: "capture_config_timeline.v1",
        session_id: input.session_id.to_string(),
        run_id,
        start_ts_ms: input.start_ts_ms,
        end_ts_ms: input.end_ts_ms,
        startup_config_hash: input.startup_config_hash.to_string(),
        final_config_hash: final_config_hash.clone(),
        change_count: changes.len(),
        changes,
    };
    let body = serde_json::to_string_pretty(&timeline).context("serialize config timeline")?;
    let timeline_hash = hash_timeline(&body);
    tokio::fs::write(input.session_dir.join("config_change_timeline.json"), body)
        .await
        .context("write config_change_timeline.json")?;
    Ok(WrittenCaptureConfigTimeline {
        timeline_hash,
        final_config_hash,
        change_count: timeline.change_count,
    })
}

fn hash_timeline(contents: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    contents.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

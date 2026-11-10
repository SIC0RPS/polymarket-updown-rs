use std::path::Path;

use serde_json::json;

use crate::event_ledger::{
    append_event, current_run_id, make_event_id, CanonicalEventRecord, CanonicalEventSource,
    CanonicalEventType,
};
use std::collections::HashMap;

use crate::capture::scope::capture_underlying_from_text;

pub(crate) fn capture_completeness_failure(
    symbols: &[String],
    replay_required_underlyings: &[String],
    max_duration_ms: u64,
    min_save_duration_ms: u64,
    exit_reason: &str,
    duration_ms: u64,
    replayable_rows_by_underlying: &HashMap<String, u64>,
) -> Option<String> {
    if (exit_reason != "max_duration" || duration_ms < max_duration_ms)
        && duration_ms < min_save_duration_ms
    {
        return Some(format!(
            "short_capture exit_reason={exit_reason} duration_ms={duration_ms} min_save_ms={min_save_duration_ms}",
        ));
    }
    let replay_required = capture_required_underlyings(symbols, replay_required_underlyings);
    if replay_required.is_empty() {
        return None;
    }
    let missing_replay = replay_required
        .iter()
        .filter(|underlying| {
            replayable_rows_by_underlying
                .get(*underlying)
                .copied()
                .unwrap_or(0)
                == 0
        })
        .cloned()
        .collect::<Vec<_>>();
    (!missing_replay.is_empty()).then(|| format!("missing_replay={}", missing_replay.join(",")))
}

fn capture_required_underlyings(symbols: &[String], symbols_allowlist: &[String]) -> Vec<String> {
    let source = if symbols_allowlist.is_empty() {
        symbols
    } else {
        symbols_allowlist
    };
    let mut required = source
        .iter()
        .filter_map(|symbol| capture_underlying_from_text(symbol))
        .collect::<Vec<_>>();
    required.sort();
    required.dedup();
    required
}

pub(crate) async fn delete_short_capture(
    session_dir: &Path,
    session_id: &str,
    strategy_kind: &str,
    symbols: &[String],
    config_hash: &str,
    finalized_ts_ms: u64,
    duration_ms: u64,
    min_save_duration_ms: u64,
    order_submit_latency_rows: u64,
    reason: &str,
) {
    let _ = append_event(&CanonicalEventRecord {
        event_id: make_event_id(
            CanonicalEventType::Actuation,
            finalized_ts_ms,
            session_id.to_string(),
        ),
        run_id: current_run_id().unwrap_or_else(|_| format!("run_capture_{finalized_ts_ms}")),
        ts_ms: finalized_ts_ms,
        event_type: CanonicalEventType::Actuation,
        source: CanonicalEventSource::System,
        subject: "capture".to_string(),
        refs: json!({
            "session_id": session_id,
            "config_hash": config_hash,
            "path": session_dir,
        }),
        metrics: json!({
            "duration_ms": duration_ms,
            "min_save_duration_ms": min_save_duration_ms,
            "order_submit_latency_rows": order_submit_latency_rows,
        }),
        context: json!({
            "action": "capture_deleted_short",
            "strategy_kind": strategy_kind,
            "symbols": symbols,
            "reason": reason,
        }),
    });
    match tokio::fs::remove_dir_all(session_dir).await {
        Ok(()) => tracing::warn!(
            session_id,
            duration_ms,
            min_save_duration_ms,
            path = %session_dir.display(),
            "capture deleted short session"
        ),
        Err(err) => tracing::warn!(
            session_id,
            duration_ms,
            min_save_duration_ms,
            path = %session_dir.display(),
            error = %err,
            "capture short session delete failed"
        ),
    }
}

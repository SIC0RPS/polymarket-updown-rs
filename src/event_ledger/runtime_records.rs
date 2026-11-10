use super::*;

pub struct RuntimeStartEventInput<'a> {
    pub run_id: &'a str,
    pub ts_ms: u64,
    pub pid: u32,
    pub commit_sha: &'a str,
    pub commit_subject: &'a str,
    pub dirty_worktree: bool,
    pub config_hash: Option<&'a str>,
    pub argv_summary: &'a str,
    pub runtime_config_fingerprint: &'a str,
    pub changed_paths: &'a [String],
    pub mode: &'a str,
}

pub fn runtime_start_event(input: RuntimeStartEventInput<'_>) -> CanonicalEventRecord {
    CanonicalEventRecord {
        event_id: make_event_id(
            CanonicalEventType::RuntimeStart,
            input.ts_ms,
            input.pid.to_string(),
        ),
        run_id: input.run_id.to_string(),
        ts_ms: input.ts_ms,
        event_type: CanonicalEventType::RuntimeStart,
        source: CanonicalEventSource::Runtime,
        subject: "trader".to_string(),
        refs: json!({
            "commit_sha": input.commit_sha,
            "commit_subject": input.commit_subject,
            "dirty_worktree": input.dirty_worktree,
            "config_hash": input.config_hash,
        }),
        metrics: json!({}),
        context: json!({
            "pid": input.pid,
            "argv_summary": input.argv_summary,
            "runtime_config_fingerprint": input.runtime_config_fingerprint,
            "changed_paths": input.changed_paths,
            "mode": input.mode,
        }),
    }
}

pub fn runtime_stop_event(
    run_id: &str,
    ts_ms: u64,
    pid: u32,
    uptime_sec: f64,
    reason: &str,
) -> CanonicalEventRecord {
    CanonicalEventRecord {
        event_id: make_event_id(CanonicalEventType::RuntimeStop, ts_ms, pid.to_string()),
        run_id: run_id.to_string(),
        ts_ms,
        event_type: CanonicalEventType::RuntimeStop,
        source: CanonicalEventSource::Runtime,
        subject: "trader".to_string(),
        refs: json!({}),
        metrics: json!({
            "uptime_sec": uptime_sec,
        }),
        context: json!({
            "pid": pid,
            "reason": reason,
        }),
    }
}

pub fn runtime_panic_incident_event(
    run_id: &str,
    ts_ms: u64,
    pid: u32,
    panic: String,
) -> CanonicalEventRecord {
    CanonicalEventRecord {
        event_id: make_event_id(CanonicalEventType::Incident, ts_ms, pid.to_string()),
        run_id: run_id.to_string(),
        ts_ms,
        event_type: CanonicalEventType::Incident,
        source: CanonicalEventSource::Runtime,
        subject: "runtime_panic".to_string(),
        refs: json!({}),
        metrics: json!({}),
        context: json!({
            "cause": "panic_hook",
            "panic": panic,
        }),
    }
}

pub fn config_reload_failed_event(
    run_id: &str,
    ts_ms: u64,
    pid: u32,
    path: &str,
    error: String,
) -> CanonicalEventRecord {
    CanonicalEventRecord {
        event_id: make_event_id(CanonicalEventType::Incident, ts_ms, pid.to_string()),
        run_id: run_id.to_string(),
        ts_ms,
        event_type: CanonicalEventType::Incident,
        source: CanonicalEventSource::Runtime,
        subject: "config_reload_failed".to_string(),
        refs: json!({}),
        metrics: json!({}),
        context: json!({
            "cause": "main_toml_hot_reload",
            "path": path,
            "error": error,
        }),
    }
}

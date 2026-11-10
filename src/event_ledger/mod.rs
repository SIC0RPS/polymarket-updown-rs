use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

mod read;
mod rotation;
mod runtime_records;
mod validation;
mod write;

pub const EVENTS_JSONL_PATH: &str = "data/events/events.jsonl";
pub const EVENTS_ARCHIVE_DIR: &str = "data/events/archive";
pub const CONFIG_OBJECTS_DIR: &str = "data/config/objects";
pub const RUNTIME_DIR: &str = "data/runtime";
pub const CURRENT_RUN_ID_PATH: &str = "data/runtime/current_run_id";
pub const HEALTH_JSON_PATH: &str = "data/runtime/health.json";
pub const TRADE_SESSION_LEDGER_DIR: &str = "logs/runs";

pub(crate) const MAX_EVENT_LINE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_VALUE_DEPTH: usize = 5;
pub(crate) const MAX_OBJECT_KEYS: usize = 128;
pub(crate) const MAX_ARRAY_LEN: usize = 256;
pub(crate) const EVENT_ROTATE_MAX_BYTES: u64 = 32 * 1024 * 1024;
pub(crate) const GZIP_SUFFIX: &str = "jsonl.gz";

pub(crate) static EVENT_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);
pub(crate) static CURRENT_RUN_ID: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalEventType {
    RuntimeStart,
    RuntimeStop,
    ConfigChange,
    CodeChange,
    Incident,
    Actuation,
    EquitySnapshot,
}

impl CanonicalEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeStart => "runtime_start",
            Self::RuntimeStop => "runtime_stop",
            Self::ConfigChange => "config_change",
            Self::CodeChange => "code_change",
            Self::Incident => "incident",
            Self::Actuation => "actuation",
            Self::EquitySnapshot => "equity_snapshot",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalEventSource {
    Runtime,
    System,
}

impl CanonicalEventSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEventRecord {
    pub event_id: String,
    pub run_id: String,
    pub ts_ms: u64,
    pub event_type: CanonicalEventType,
    pub source: CanonicalEventSource,
    pub subject: String,
    pub refs: Value,
    pub metrics: Value,
    pub context: Value,
}

#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MachineHistoryStatus {
    pub run_id: Option<String>,
    pub active_config_hash: Option<String>,
    pub latest_event_id: Option<String>,
    pub latest_event_ts_ms: Option<u64>,
    pub archive_count: usize,
}

pub(crate) use read::history_state_snapshot;
pub use read::{latest_config_hash_after, latest_event, load_events};
pub use runtime_records::{
    config_reload_failed_event, runtime_panic_incident_event, runtime_start_event,
    runtime_stop_event, RuntimeStartEventInput,
};
pub(crate) use validation::validate_event;
pub use write::{
    append_event, append_jsonl_record, current_run_id, make_event_id, now_ms, snapshot_config_path,
    RunIdProvider,
};

pub fn ensure_runtime_layout() -> Result<()> {
    for dir in [
        Path::new("data/events"),
        Path::new(EVENTS_ARCHIVE_DIR),
        Path::new(CONFIG_OBJECTS_DIR),
        Path::new("data/state"),
        Path::new(RUNTIME_DIR),
        Path::new(TRADE_SESSION_LEDGER_DIR),
    ] {
        fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }
    Ok(())
}

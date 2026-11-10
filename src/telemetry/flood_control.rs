use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::ErrorCounter;
use tracing_subscriber::layer::{Context, Layer};

const INTERNAL_TARGET: &str = "polymarket_hft::telemetry::flood_control";
const STABLE_SIGNATURE_FIELDS: &[&str] = &[
    "symbol",
    "reason",
    "mode",
    "scope",
    "status",
    "instrument",
    "market_id",
    "condition_id",
    "token",
    "asset_id",
    "role",
    "leg",
    "generation",
    "readiness_role",
    "observed_at_mono_ns",
    "source",
];
const SNAPSHOT_TOP_ACTIVE_LIMIT: usize = 10;
const HOUSEKEEPING_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy)]
pub struct FloodControlConfig {
    pub enabled: bool,
    pub summary_interval: Duration,
    pub recovery_timeout: Duration,
    pub max_tracked_signatures: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FloodControlSnapshot {
    pub enabled: bool,
    pub active_signatures: usize,
    pub max_tracked_signatures: usize,
    pub total_events_suppressed: u64,
    pub summary_events_emitted: u64,
    pub recovery_events_emitted: u64,
    pub signatures_evicted: u64,
    pub sink_dropped_lines: u64,
    pub top_active: Vec<ActiveFloodSignatureSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActiveFloodSignatureSnapshot {
    pub target: String,
    pub level: String,
    pub callsite: String,
    pub stable_fields: BTreeMap<String, String>,
    pub total_events: u64,
    pub suppressed_events: u64,
    pub suppressed_since_summary: u64,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    pub last_seen_age_ms: u64,
}

#[derive(Clone)]
pub struct FloodControlHandle {
    cfg: FloodControlConfig,
    state: Arc<Mutex<FloodControlState>>,
    sink_error_counters: Arc<Mutex<Vec<ErrorCounter>>>,
}

pub struct FloodControlGuard {
    shutdown_tx: Option<Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct FloodControlLayer {
    cfg: FloodControlConfig,
    state: Arc<Mutex<FloodControlState>>,
}

#[derive(Debug, Default)]
struct FloodControlState {
    entries: HashMap<String, FloodEntry>,
    total_events_suppressed: u64,
    summary_events_emitted: u64,
    recovery_events_emitted: u64,
    signatures_evicted: u64,
}

#[derive(Debug, Clone)]
struct FloodEntry {
    target: String,
    level: Level,
    callsite: String,
    stable_fields: BTreeMap<String, String>,
    last_seen_mono: Instant,
    first_seen_ms: u64,
    last_seen_ms: u64,
    last_summary_mono: Instant,
    total_events: u64,
    suppressed_events: u64,
    suppressed_since_summary: u64,
}

#[derive(Debug)]
struct SignatureData {
    key: String,
    target: String,
    level: Level,
    callsite: String,
    stable_fields: BTreeMap<String, String>,
}

#[derive(Debug)]
struct HousekeepingSummary {
    target: String,
    level: Level,
    callsite: String,
    stable_fields: BTreeMap<String, String>,
    suppressed_events: u64,
    total_events: u64,
    first_seen_ms: u64,
    last_seen_ms: u64,
}

#[derive(Debug)]
struct HousekeepingRecovery {
    target: String,
    callsite: String,
    stable_fields: BTreeMap<String, String>,
    total_events: u64,
    suppressed_events: u64,
    first_seen_ms: u64,
    last_seen_ms: u64,
}

impl FloodControlLayer {
    pub fn new(cfg: FloodControlConfig) -> (Self, FloodControlHandle) {
        let state = Arc::new(Mutex::new(FloodControlState::default()));
        let handle = FloodControlHandle {
            cfg,
            state: state.clone(),
            sink_error_counters: Arc::new(Mutex::new(Vec::new())),
        };
        (Self { cfg, state }, handle)
    }
}

impl<S> Layer<S> for FloodControlLayer
where
    S: Subscriber,
{
    fn event_enabled(&self, event: &Event<'_>, _ctx: Context<'_, S>) -> bool {
        if !self.cfg.enabled || event.metadata().target() == INTERNAL_TARGET {
            return true;
        }

        let now_mono = Instant::now();
        let now_ms = unix_ms();
        let signature = SignatureData::from_event(event);
        let mut state = match self.state.lock() {
            Ok(guard) => guard,
            Err(_) => return true,
        };

        if let Some(entry) = state.entries.get_mut(&signature.key) {
            entry.last_seen_mono = now_mono;
            entry.last_seen_ms = now_ms;
            entry.total_events = entry.total_events.saturating_add(1);
            entry.suppressed_events = entry.suppressed_events.saturating_add(1);
            entry.suppressed_since_summary = entry.suppressed_since_summary.saturating_add(1);
            state.total_events_suppressed = state.total_events_suppressed.saturating_add(1);
            return false;
        }

        if state.entries.len() >= self.cfg.max_tracked_signatures {
            if let Some(oldest_key) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_seen_mono)
                .map(|(key, _)| key.clone())
            {
                state.entries.remove(&oldest_key);
                state.signatures_evicted = state.signatures_evicted.saturating_add(1);
            }
        }

        state.entries.insert(
            signature.key,
            FloodEntry {
                target: signature.target,
                level: signature.level,
                callsite: signature.callsite,
                stable_fields: signature.stable_fields,
                last_seen_mono: now_mono,
                first_seen_ms: now_ms,
                last_seen_ms: now_ms,
                last_summary_mono: now_mono,
                total_events: 1,
                suppressed_events: 0,
                suppressed_since_summary: 0,
            },
        );
        true
    }
}

impl FloodControlHandle {
    pub fn set_sink_error_counters(&self, counters: Vec<ErrorCounter>) {
        if let Ok(mut guard) = self.sink_error_counters.lock() {
            *guard = counters;
        }
    }

    pub fn snapshot(&self) -> FloodControlSnapshot {
        let now_ms = unix_ms();
        let dropped = self
            .sink_error_counters
            .lock()
            .ok()
            .map(|counters| {
                counters.iter().fold(0_u64, |acc, counter| {
                    acc.saturating_add(counter.dropped_lines() as u64)
                })
            })
            .unwrap_or(0);

        let (
            mut top_active,
            active_signatures,
            total_events_suppressed,
            summary_events_emitted,
            recovery_events_emitted,
            signatures_evicted,
        ) = {
            let state = match self.state.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    return FloodControlSnapshot {
                        enabled: self.cfg.enabled,
                        active_signatures: 0,
                        max_tracked_signatures: self.cfg.max_tracked_signatures,
                        total_events_suppressed: 0,
                        summary_events_emitted: 0,
                        recovery_events_emitted: 0,
                        signatures_evicted: 0,
                        sink_dropped_lines: dropped,
                        top_active: Vec::new(),
                    };
                }
            };
            let top_active = state
                .entries
                .values()
                .map(|entry| ActiveFloodSignatureSnapshot {
                    target: entry.target.clone(),
                    level: entry.level.as_str().to_string(),
                    callsite: entry.callsite.clone(),
                    stable_fields: entry.stable_fields.clone(),
                    total_events: entry.total_events,
                    suppressed_events: entry.suppressed_events,
                    suppressed_since_summary: entry.suppressed_since_summary,
                    first_seen_ms: entry.first_seen_ms,
                    last_seen_ms: entry.last_seen_ms,
                    last_seen_age_ms: now_ms.saturating_sub(entry.last_seen_ms),
                })
                .collect::<Vec<_>>();
            (
                top_active,
                state.entries.len(),
                state.total_events_suppressed,
                state.summary_events_emitted,
                state.recovery_events_emitted,
                state.signatures_evicted,
            )
        };
        top_active.sort_by(|a, b| {
            b.suppressed_events
                .cmp(&a.suppressed_events)
                .then_with(|| b.total_events.cmp(&a.total_events))
                .then_with(|| a.callsite.cmp(&b.callsite))
        });
        top_active.truncate(SNAPSHOT_TOP_ACTIVE_LIMIT);

        FloodControlSnapshot {
            enabled: self.cfg.enabled,
            active_signatures,
            max_tracked_signatures: self.cfg.max_tracked_signatures,
            total_events_suppressed,
            summary_events_emitted,
            recovery_events_emitted,
            signatures_evicted,
            sink_dropped_lines: dropped,
            top_active,
        }
    }

    pub fn start_housekeeping(&self) -> FloodControlGuard {
        if !self.cfg.enabled {
            return FloodControlGuard {
                shutdown_tx: None,
                worker: None,
            };
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
        let state = self.state.clone();
        let cfg = self.cfg;
        let worker = thread::Builder::new()
            .name("pmhft-log-flood-control".to_string())
            .spawn(move || loop {
                match shutdown_rx.recv_timeout(HOUSEKEEPING_POLL_INTERVAL) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                    Err(RecvTimeoutError::Timeout) => {
                        let (summaries, recoveries) = match state.lock() {
                            Ok(mut guard) => guard.housekeeping(cfg, Instant::now()),
                            Err(_) => break,
                        };
                        for summary in summaries {
                            emit_summary(summary);
                        }
                        for recovery in recoveries {
                            emit_recovery(recovery);
                        }
                    }
                }
            })
            .ok();

        FloodControlGuard {
            shutdown_tx: Some(shutdown_tx),
            worker,
        }
    }
}

impl Drop for FloodControlGuard {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl FloodControlState {
    fn housekeeping(
        &mut self,
        cfg: FloodControlConfig,
        now: Instant,
    ) -> (Vec<HousekeepingSummary>, Vec<HousekeepingRecovery>) {
        let mut summaries = Vec::new();
        let mut recoveries = Vec::new();
        let mut expired = Vec::new();

        for (key, entry) in &mut self.entries {
            let active_for = now.saturating_duration_since(entry.last_seen_mono);
            if active_for >= cfg.recovery_timeout {
                if entry.total_events > 1 {
                    recoveries.push(HousekeepingRecovery {
                        target: entry.target.clone(),
                        callsite: entry.callsite.clone(),
                        stable_fields: entry.stable_fields.clone(),
                        total_events: entry.total_events,
                        suppressed_events: entry.suppressed_events,
                        first_seen_ms: entry.first_seen_ms,
                        last_seen_ms: entry.last_seen_ms,
                    });
                    self.recovery_events_emitted = self.recovery_events_emitted.saturating_add(1);
                }
                expired.push(key.clone());
                continue;
            }

            if entry.suppressed_since_summary > 0
                && now.saturating_duration_since(entry.last_summary_mono) >= cfg.summary_interval
            {
                summaries.push(HousekeepingSummary {
                    target: entry.target.clone(),
                    level: entry.level,
                    callsite: entry.callsite.clone(),
                    stable_fields: entry.stable_fields.clone(),
                    suppressed_events: entry.suppressed_since_summary,
                    total_events: entry.total_events,
                    first_seen_ms: entry.first_seen_ms,
                    last_seen_ms: entry.last_seen_ms,
                });
                entry.last_summary_mono = now;
                entry.suppressed_since_summary = 0;
                self.summary_events_emitted = self.summary_events_emitted.saturating_add(1);
            }
        }

        for key in expired {
            self.entries.remove(&key);
        }

        (summaries, recoveries)
    }
}

impl SignatureData {
    fn from_event(event: &Event<'_>) -> Self {
        let meta = event.metadata();
        let mut visitor = SignatureVisitor::default();
        event.record(&mut visitor);
        let target = meta.target().to_string();
        let level = *meta.level();
        let callsite = callsite(meta.file(), meta.line(), meta.name());
        let mut key = format!("{target}|{}|{callsite}", level.as_str());
        for (name, value) in &visitor.stable_fields {
            key.push('|');
            key.push_str(name);
            key.push('=');
            key.push_str(value);
        }
        Self {
            key,
            target,
            level,
            callsite,
            stable_fields: visitor.stable_fields,
        }
    }
}

#[derive(Default)]
struct SignatureVisitor {
    stable_fields: BTreeMap<String, String>,
}

impl tracing::field::Visit for SignatureVisitor {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), value.to_string());
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), value.to_string());
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), value.to_string());
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), value.to_string());
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if Self::is_stable(field.name()) {
            self.record_value(field.name(), format!("{value:?}"));
        }
    }
}

impl SignatureVisitor {
    fn is_stable(name: &str) -> bool {
        STABLE_SIGNATURE_FIELDS.contains(&name)
    }

    fn record_value(&mut self, name: &str, value: String) {
        self.stable_fields.insert(name.to_string(), value);
    }
}

fn emit_summary(summary: HousekeepingSummary) {
    match summary.level {
        Level::ERROR => tracing::error!(
            target: INTERNAL_TARGET,
            source_target = %summary.target,
            callsite = %summary.callsite,
            stable_fields = ?summary.stable_fields,
            suppressed_events = summary.suppressed_events,
            total_events = summary.total_events,
            first_seen_ms = summary.first_seen_ms,
            last_seen_ms = summary.last_seen_ms,
            "persistent log condition suppressed"
        ),
        Level::WARN => tracing::warn!(
            target: INTERNAL_TARGET,
            source_target = %summary.target,
            callsite = %summary.callsite,
            stable_fields = ?summary.stable_fields,
            suppressed_events = summary.suppressed_events,
            total_events = summary.total_events,
            first_seen_ms = summary.first_seen_ms,
            last_seen_ms = summary.last_seen_ms,
            "persistent log condition suppressed"
        ),
        Level::INFO => tracing::info!(
            target: INTERNAL_TARGET,
            source_target = %summary.target,
            callsite = %summary.callsite,
            stable_fields = ?summary.stable_fields,
            suppressed_events = summary.suppressed_events,
            total_events = summary.total_events,
            first_seen_ms = summary.first_seen_ms,
            last_seen_ms = summary.last_seen_ms,
            "persistent log condition suppressed"
        ),
        Level::DEBUG => tracing::debug!(
            target: INTERNAL_TARGET,
            source_target = %summary.target,
            callsite = %summary.callsite,
            stable_fields = ?summary.stable_fields,
            suppressed_events = summary.suppressed_events,
            total_events = summary.total_events,
            first_seen_ms = summary.first_seen_ms,
            last_seen_ms = summary.last_seen_ms,
            "persistent log condition suppressed"
        ),
        Level::TRACE => tracing::trace!(
            target: INTERNAL_TARGET,
            source_target = %summary.target,
            callsite = %summary.callsite,
            stable_fields = ?summary.stable_fields,
            suppressed_events = summary.suppressed_events,
            total_events = summary.total_events,
            first_seen_ms = summary.first_seen_ms,
            last_seen_ms = summary.last_seen_ms,
            "persistent log condition suppressed"
        ),
    }
}

fn emit_recovery(recovery: HousekeepingRecovery) {
    tracing::info!(
        target: INTERNAL_TARGET,
        source_target = %recovery.target,
        callsite = %recovery.callsite,
        stable_fields = ?recovery.stable_fields,
        suppressed_events = recovery.suppressed_events,
        total_events = recovery.total_events,
        first_seen_ms = recovery.first_seen_ms,
        last_seen_ms = recovery.last_seen_ms,
        "persistent log condition recovered"
    );
}

fn callsite(file: Option<&str>, line: Option<u32>, name: &str) -> String {
    match (file, line) {
        (Some(file), Some(line)) => format!("{file}:{line}:{name}"),
        _ => name.to_string(),
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

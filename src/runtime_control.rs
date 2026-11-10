use std::sync::Arc;

use crate::config::AppConfig;
use crate::event_ledger::{
    append_event, current_run_id, history_state_snapshot, make_event_id, now_ms,
    CanonicalEventRecord, CanonicalEventSource, CanonicalEventType,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct SharedRuntimeConfigState {
    state: Arc<tokio::sync::RwLock<RuntimeConfigState>>,
    tx: watch::Sender<RuntimeConfigState>,
    pause_tx: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfigState {
    pub hot_reload_cfg: Value,
    pub config_hash: Option<String>,
    pub pause: bool,
    requested_pause: bool,
    latest_unpause_ms: Option<u64>,
}

impl RuntimeConfigState {
    pub fn allows_new_market_entry(&self, window_start_ms: u64) -> bool {
        !self.pause
            && self
                .latest_unpause_ms
                .is_none_or(|unpause_ms| window_start_ms >= unpause_ms)
    }
}

#[derive(Debug, Clone)]
pub struct RuntimePauseTransition {
    pub after: RuntimeConfigState,
    pub source_changed: bool,
    pub audit_ts_ms: Option<u64>,
}

pub struct PauseAuditInput {
    pub actor: String,
    pub reason_code: String,
    pub reason: String,
    pub evidence: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfigProjection {
    value: Value,
    fingerprint: String,
}

impl RuntimeConfigProjection {
    pub fn from_config_with_pause(cfg: &AppConfig, pause: bool) -> Self {
        let value = json!({
            "strategy": cfg.strategy.maker_rebate_mm,
            "risk": cfg.risk,
            "max_order_qty": cfg.max_order_qty,
            "polymarket_execution": {
                "PAUSE": pause,
            },
        });
        let fingerprint = runtime_config_fingerprint(&value);
        Self { value, fingerprint }
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PauseFileCommand {
    pub timestamp: Value,
    pub target: String,
    pub command: String,
    pub enabled: bool,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

struct ActuationEventInput {
    pub enabled: bool,
    pub requested_enabled: bool,
    pub run_id: String,
    pub source: CanonicalEventSource,
    pub actor: String,
    pub reason_code: String,
    pub reason: String,
    pub evidence: Option<Value>,
    pub snapshot_id: String,
    pub config_hash_after: String,
}

pub fn runtime_config_fingerprint(value: &Value) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    let digest = hasher.finalize();
    let mut fingerprint = String::with_capacity(12);
    for byte in digest.iter().take(6) {
        use std::fmt::Write as _;
        let _ = write!(&mut fingerprint, "{byte:02x}");
    }
    fingerprint
}

impl SharedRuntimeConfigState {
    pub fn from_initial_pause(
        cfg: &AppConfig,
        requested_pause: bool,
        config_hash: Option<String>,
    ) -> Self {
        let pause = requested_pause;
        let projection = RuntimeConfigProjection::from_config_with_pause(cfg, pause);
        let initial = RuntimeConfigState {
            hot_reload_cfg: projection.value.clone(),
            config_hash,
            pause,
            requested_pause,
            latest_unpause_ms: None,
        };
        let (tx, _rx) = watch::channel(initial.clone());
        let (pause_tx, _pause_rx) = watch::channel(pause);
        Self {
            state: Arc::new(tokio::sync::RwLock::new(initial)),
            tx,
            pause_tx,
        }
    }

    pub async fn snapshot(&self) -> RuntimeConfigState {
        self.state.read().await.clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<RuntimeConfigState> {
        self.tx.subscribe()
    }

    pub fn subscribe_pause(&self) -> watch::Receiver<bool> {
        self.pause_tx.subscribe()
    }

    pub async fn replace_from_config(
        &self,
        cfg: &AppConfig,
        config_hash: Option<String>,
    ) -> RuntimeConfigState {
        let mut guard = self.state.write().await;
        let projection = RuntimeConfigProjection::from_config_with_pause(cfg, guard.pause);
        guard.hot_reload_cfg = projection.value;
        guard.config_hash = config_hash;
        let next = guard.clone();
        self.publish(next.clone());
        drop(guard);
        next
    }

    pub async fn set_requested_pause(
        &self,
        enabled: bool,
        audit: PauseAuditInput,
    ) -> Result<RuntimePauseTransition> {
        let mut guard = self.state.write().await;
        let before = guard.clone();
        let source_changed = before.requested_pause != enabled;
        let mut after = before.clone();
        after.requested_pause = enabled;
        update_effective_pause(&mut after);
        let mut transition = pause_transition(after.clone(), source_changed);
        if enabled {
            *guard = after.clone();
            self.publish(after);
            let audit_result = audit_pause_transition(&mut transition, true, audit);
            drop(guard);
            audit_result?;
            return Ok(transition);
        }
        audit_pause_transition(&mut transition, false, audit)?;
        if before.pause && !transition.after.pause {
            transition.after.latest_unpause_ms = transition.audit_ts_ms;
        }
        *guard = transition.after.clone();
        self.publish(transition.after.clone());
        drop(guard);
        Ok(transition)
    }

    fn publish(&self, next: RuntimeConfigState) {
        let effective_pause = next.pause;
        let _ = self.tx.send_replace(next);
        let _ = self.pause_tx.send_if_modified(|current| {
            if *current == effective_pause {
                return false;
            }
            *current = effective_pause;
            true
        });
    }
}

fn pause_transition(after: RuntimeConfigState, source_changed: bool) -> RuntimePauseTransition {
    RuntimePauseTransition {
        after,
        source_changed,
        audit_ts_ms: None,
    }
}

fn audit_pause_transition(
    transition: &mut RuntimePauseTransition,
    requested_enabled: bool,
    audit: PauseAuditInput,
) -> Result<()> {
    if !transition.source_changed {
        return Ok(());
    }
    let machine_history = history_state_snapshot().machine_history;
    let run_id = current_run_id().context("pause audit requires current run identity")?;
    let latest_event_id = machine_history
        .latest_event_id
        .context("pause audit requires latest canonical event identity")?;
    let snapshot_id = runtime_snapshot_id(Some(run_id.clone()), Some(latest_event_id));
    let ts_ms = now_ms();
    let event = build_pause_actuation_event(
        ts_ms,
        ActuationEventInput {
            enabled: transition.after.pause,
            requested_enabled,
            run_id,
            source: CanonicalEventSource::System,
            actor: audit.actor,
            reason_code: audit.reason_code,
            reason: audit.reason,
            evidence: audit.evidence,
            snapshot_id,
            config_hash_after: transition
                .after
                .config_hash
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        },
    );
    append_event(&event)?;
    transition.audit_ts_ms = Some(ts_ms);
    Ok(())
}

pub fn latest_operator_pause_command(contents: &str) -> Result<Option<PauseFileCommand>> {
    for line in contents
        .lines()
        .rev()
        .filter(|line| !line.trim().is_empty())
    {
        let command: PauseFileCommand =
            serde_json::from_str(line.trim()).context("invalid typed pause command")?;
        anyhow::ensure!(
            command.target == "all" && command.command == "set_pause",
            "noncanonical pause command"
        );
        let startup_projection = command.actor.as_deref() == Some("runtime_bootstrap")
            && command
                .reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("startup_"));
        if startup_projection {
            continue;
        }
        return Ok(Some(command));
    }
    Ok(None)
}

fn update_effective_pause(state: &mut RuntimeConfigState) {
    state.pause = state.requested_pause;
    let mut next = state.hot_reload_cfg.clone();
    if let Some(root) = next.as_object_mut() {
        let polymarket_execution = root
            .entry("polymarket_execution".to_string())
            .or_insert_with(|| json!({}));
        if let Some(object) = polymarket_execution.as_object_mut() {
            object.insert("PAUSE".to_string(), json!(state.pause));
        }
    }
    state.hot_reload_cfg = next;
}

pub fn runtime_control_run_id(
    current_run_id: Option<String>,
    history_run_id: Option<String>,
) -> String {
    current_run_id
        .or(history_run_id)
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn runtime_snapshot_id(run_id: Option<String>, latest_event_id: Option<String>) -> String {
    let run_id = run_id.unwrap_or_else(|| "unknown".to_string());
    let latest_event_id = latest_event_id.unwrap_or_else(|| "evt_none".to_string());
    format!("snap_{run_id}_{latest_event_id}")
}

fn build_pause_actuation_event(now_ms: u64, input: ActuationEventInput) -> CanonicalEventRecord {
    let event_id = make_event_id(
        CanonicalEventType::Actuation,
        now_ms,
        format!("pause_{}_{}", input.actor, input.snapshot_id),
    );
    CanonicalEventRecord {
        event_id: event_id.clone(),
        run_id: input.run_id,
        ts_ms: now_ms,
        event_type: CanonicalEventType::Actuation,
        source: input.source,
        subject: "pause".to_string(),
        refs: json!({
            "actor": input.actor,
            "reason_code": input.reason_code,
            "snapshot_id": input.snapshot_id,
            "config_hash_after": input.config_hash_after,
        }),
        metrics: json!({
            "enabled": if input.enabled { 1 } else { 0 },
            "requested_enabled": if input.requested_enabled { 1 } else { 0 },
        }),
        context: json!({
            "reason": input.reason,
            "evidence": input.evidence.unwrap_or(Value::Null),
        }),
    }
}

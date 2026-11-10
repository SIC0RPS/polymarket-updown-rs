use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use crate::polymarket::identity::MarketId;

mod handle;

const MAX_RECENT_INCIDENTS: usize = 32;
const MAX_REDEEM_CONDITIONS: usize = 32;

#[derive(Debug, Clone, Default)]
pub struct LiveOpsHandle {
    inner: Arc<Mutex<LiveOpsState>>,
}

#[derive(Debug, Clone, Default)]
pub struct LiveOpsSnapshot {
    pub heartbeat: HeartbeatTelemetry,
    pub inventory: InventoryTelemetry,
    pub redeem: RedeemTelemetry,
    pub recent_incidents: Vec<LiveIncident>,
}

#[derive(Debug, Clone, Default)]
pub struct HeartbeatTelemetry {
    pub active: Option<bool>,
    pub last_sample_ms: Option<u64>,
    pub last_change_ms: Option<u64>,
    pub inactive_since_ms: Option<u64>,
    pub inactive_events: u64,
}

#[derive(Debug, Clone, Default)]
pub struct InventoryTelemetry {
    pub active: Vec<InventoryConditionSnapshot>,
    pub last_update_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct InventoryConditionSnapshot {
    pub condition_id: MarketId,
    pub market_id: MarketId,
    pub operation: String,
    pub status: String,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub tx_ref: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RedeemTelemetry {
    pub auto_enabled: bool,
    pub route: Option<String>,
    pub disabled_reason: Option<String>,
    pub last_poll_ms: Option<u64>,
    pub last_poll_error: Option<String>,
    pub tx_submit_attempts: u64,
    pub tx_failures: u64,
    pub last_condition_id: Option<MarketId>,
    pub redeemables: Vec<RedeemClaimSnapshot>,
    pub conditions: Vec<RedeemConditionSnapshot>,
}

#[derive(Debug, Clone, Default)]
pub struct RedeemConditionSnapshot {
    pub condition_id: MarketId,
    pub route: String,
    pub status: String,
    pub attempts: u32,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub tx_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedeemClaimSnapshot {
    pub condition_id: MarketId,
    pub token: String,
    pub title: String,
    pub outcome: String,
    pub outcome_index: u8,
    pub quantity_units_6dp: u128,
    pub payout_numerator: u128,
    pub payout_denominator: u128,
    pub claim_value_units_6dp: u128,
}

#[derive(Debug, Clone, Default)]
pub struct LiveIncident {
    pub ts_ms: u64,
    pub kind: String,
    pub scope: String,
    pub detail: String,
}

#[derive(Debug, Default)]
struct LiveOpsState {
    heartbeat: HeartbeatTelemetry,
    matching_engine_restart_active: bool,
    inventory: HashMap<String, InventoryConditionSnapshot>,
    inventory_last_update_ms: Option<u64>,
    redeem: RedeemState,
    recent_incidents: VecDeque<LiveIncident>,
}

#[derive(Debug, Default)]
struct RedeemState {
    auto_enabled: bool,
    route: Option<String>,
    disabled_reason: Option<String>,
    last_poll_ms: Option<u64>,
    last_poll_error: Option<String>,
    tx_submit_attempts: u64,
    tx_failures: u64,
    last_condition_id: Option<MarketId>,
    redeemables: Vec<RedeemClaimSnapshot>,
    conditions: HashMap<MarketId, RedeemConditionSnapshot>,
}

impl LiveOpsState {
    fn inventory_key(condition_id: &MarketId, operation: &str) -> String {
        format!("{condition_id}:{operation}")
    }

    fn snapshot(&self) -> LiveOpsSnapshot {
        let mut conditions = self.redeem.conditions.values().cloned().collect::<Vec<_>>();
        conditions.sort_by(|a, b| {
            b.last_update_ms
                .cmp(&a.last_update_ms)
                .then_with(|| a.condition_id.cmp(&b.condition_id))
        });
        conditions.truncate(MAX_REDEEM_CONDITIONS);
        let mut redeemables = self.redeem.redeemables.clone();
        redeemables.sort_by(|a, b| {
            a.condition_id
                .cmp(&b.condition_id)
                .then_with(|| a.outcome_index.cmp(&b.outcome_index))
                .then_with(|| a.token.cmp(&b.token))
        });

        LiveOpsSnapshot {
            heartbeat: self.heartbeat.clone(),
            inventory: InventoryTelemetry {
                active: {
                    let mut active = self.inventory.values().cloned().collect::<Vec<_>>();
                    active.sort_by(|a, b| {
                        b.last_update_ms
                            .cmp(&a.last_update_ms)
                            .then_with(|| a.condition_id.cmp(&b.condition_id))
                    });
                    active
                },
                last_update_ms: self.inventory_last_update_ms,
            },
            redeem: RedeemTelemetry {
                auto_enabled: self.redeem.auto_enabled,
                route: self.redeem.route.clone(),
                disabled_reason: self.redeem.disabled_reason.clone(),
                last_poll_ms: self.redeem.last_poll_ms,
                last_poll_error: self.redeem.last_poll_error.clone(),
                tx_submit_attempts: self.redeem.tx_submit_attempts,
                tx_failures: self.redeem.tx_failures,
                last_condition_id: self.redeem.last_condition_id.clone(),
                redeemables,
                conditions,
            },
            recent_incidents: self.recent_incidents.iter().cloned().collect(),
        }
    }

    fn record_heartbeat_sample(&mut self, now: u64, active: Option<bool>) {
        self.heartbeat.last_sample_ms = Some(now);
        let changed = self.heartbeat.active != active;
        if changed {
            self.heartbeat.last_change_ms = Some(now);
            match active {
                Some(false) => {
                    self.heartbeat.inactive_since_ms = Some(now);
                    self.heartbeat.inactive_events =
                        self.heartbeat.inactive_events.saturating_add(1);
                    self.push_incident(
                        now,
                        "heartbeat_inactive",
                        "heartbeat",
                        "execution heartbeat is inactive",
                    );
                }
                Some(true) => {
                    self.heartbeat.inactive_since_ms = None;
                    self.push_incident(
                        now,
                        "heartbeat_recovered",
                        "heartbeat",
                        "execution heartbeat recovered",
                    );
                }
                None => {
                    self.heartbeat.inactive_since_ms = None;
                }
            }
        }
        self.heartbeat.active = active;
    }

    fn record_http_result(
        &mut self,
        now: u64,
        endpoint: &str,
        method: &str,
        http_status: Option<u16>,
        ok: bool,
        backoff_ms: u64,
    ) {
        if !ok && http_status == Some(425) {
            let first_425 = !self.matching_engine_restart_active;
            self.matching_engine_restart_active = true;
            if first_425 {
                self.push_incident(
                    now,
                    "matching_engine_restart",
                    endpoint,
                    &format!(
                        "endpoint={endpoint} method={method} status=425 backoff_ms={backoff_ms}"
                    ),
                );
            }
            return;
        }

        if ok && self.matching_engine_restart_active {
            self.matching_engine_restart_active = false;
            self.push_incident(
                now,
                "matching_engine_recovered",
                endpoint,
                &format!("endpoint={endpoint} method={method}"),
            );
        }
    }

    fn record_inventory_status(
        &mut self,
        now: u64,
        market_id: &str,
        condition_id: &str,
        operation: &str,
        status: &str,
        tx_ref: Option<String>,
        error: Option<String>,
    ) {
        self.inventory_last_update_ms = Some(now);
        let condition_id = MarketId::new(condition_id);
        let market_id = MarketId::new(market_id);
        let inventory_key = Self::inventory_key(&condition_id, operation);

        if matches!(status, "confirmed" | "cleared") {
            self.inventory.remove(&inventory_key);
        } else {
            let entry =
                self.inventory
                    .entry(inventory_key)
                    .or_insert_with(|| InventoryConditionSnapshot {
                        condition_id: condition_id.clone(),
                        market_id: market_id.clone(),
                        operation: operation.to_string(),
                        status: status.to_string(),
                        first_seen_ms: now,
                        last_update_ms: now,
                        tx_ref: None,
                        error: None,
                    });
            entry.market_id = market_id.clone();
            entry.operation = operation.to_string();
            entry.status = status.to_string();
            entry.last_update_ms = now;
            if tx_ref.is_some() {
                entry.tx_ref = tx_ref.clone();
            }
            entry.error = error.clone();
        }

        match status {
            "failed" => {
                let detail = error.unwrap_or_else(|| format!("{operation} failed"));
                self.push_incident(
                    now,
                    "ctf_inventory_failed",
                    condition_id.as_str(),
                    &format!("market_id={market_id} operation={operation} {detail}"),
                );
            }
            "refresh_failed" => {
                let detail =
                    error.unwrap_or_else(|| format!("{operation} confirmed but refresh failed"));
                self.push_incident(
                    now,
                    "ctf_inventory_refresh_failed",
                    condition_id.as_str(),
                    &format!("market_id={market_id} operation={operation} {detail}"),
                );
            }
            "pending" => {
                self.push_incident(
                    now,
                    "ctf_inventory_pending",
                    condition_id.as_str(),
                    &format!("market_id={market_id} operation={operation}"),
                );
            }
            _ => {}
        }
    }

    fn record_redeem_disabled(&mut self, now: u64, reason: String) {
        self.redeem.auto_enabled = false;
        self.redeem.disabled_reason = Some(reason.clone());
        self.redeem.redeemables.clear();
        self.push_incident(now, "redeem_disabled", "redeem", &reason);
    }

    fn record_redeem_enabled(&mut self, now: u64, route: &str) {
        self.redeem.auto_enabled = true;
        self.redeem.route = Some(route.to_string());
        self.redeem.disabled_reason = None;
        self.redeem.last_poll_ms = Some(now);
        self.redeem.last_poll_error = None;
    }

    fn record_redeem_poll_ok(&mut self, now: u64) {
        self.redeem.last_poll_ms = Some(now);
        self.redeem.last_poll_error = None;
        self.redeem.redeemables.clear();
    }

    fn record_redeem_poll_error(&mut self, now: u64, error: String) {
        self.redeem.last_poll_ms = Some(now);
        let new_error = self.redeem.last_poll_error.as_deref() != Some(error.as_str());
        self.redeem.last_poll_error = Some(error.clone());
        self.redeem.redeemables.clear();
        if new_error {
            self.push_incident(now, "redeem_poll_error", "redeem", &error);
        }
    }

    fn record_redeem_tx_submit_attempt(&mut self, now: u64, condition_id: &str) {
        self.redeem.tx_submit_attempts = self.redeem.tx_submit_attempts.saturating_add(1);
        self.redeem.last_condition_id = Some(MarketId::new(condition_id));
        self.redeem.last_poll_ms = Some(now);
    }

    fn record_redeem_status(
        &mut self,
        now: u64,
        condition_id: &str,
        route: &str,
        status: &str,
        tx_hash: Option<String>,
        error: Option<String>,
    ) {
        let condition_key = MarketId::new(condition_id);
        let detail_for_incident = {
            let entry = self
                .redeem
                .conditions
                .entry(condition_key.clone())
                .or_insert_with(|| RedeemConditionSnapshot {
                    condition_id: condition_key.clone(),
                    route: route.to_string(),
                    status: status.to_string(),
                    attempts: 0,
                    first_seen_ms: now,
                    last_update_ms: now,
                    tx_hash: None,
                    error: None,
                });

            if status == "pending" {
                entry.attempts = entry.attempts.saturating_add(1);
            }
            self.redeem.last_condition_id = Some(condition_key.clone());
            entry.route = route.to_string();
            entry.status = status.to_string();
            entry.last_update_ms = now;
            if let Some(tx_hash) = tx_hash {
                entry.tx_hash = Some(tx_hash);
            }
            if let Some(error) = error {
                entry.error = Some(error);
            } else if matches!(status, "mined" | "submitted" | "retrying" | "confirmed") {
                entry.error = None;
            }

            entry.error.clone()
        };
        if status == "confirmed" {
            self.redeem
                .redeemables
                .retain(|claim| claim.condition_id != condition_key);
        }

        self.prune_redeem_conditions();

        if status == "failed" {
            self.redeem.tx_failures = self.redeem.tx_failures.saturating_add(1);
            let detail =
                detail_for_incident.unwrap_or_else(|| format!("condition_id={condition_id}"));
            self.push_incident(now, "redeem_failed", condition_id, &detail);
        }
    }

    fn record_redeem_claims(
        &mut self,
        now: u64,
        condition_id: &str,
        route: &str,
        claims: Vec<RedeemClaimSnapshot>,
    ) {
        let condition_key = MarketId::new(condition_id);
        let entry = self
            .redeem
            .conditions
            .entry(condition_key.clone())
            .or_insert_with(|| RedeemConditionSnapshot {
                condition_id: condition_key.clone(),
                route: route.to_string(),
                status: "ready".to_string(),
                attempts: 0,
                first_seen_ms: now,
                last_update_ms: now,
                tx_hash: None,
                error: None,
            });
        entry.route = route.to_string();
        entry.status = "ready".to_string();
        entry.last_update_ms = now;
        entry.error = None;
        self.redeem
            .redeemables
            .retain(|claim| claim.condition_id != condition_key);
        self.redeem.redeemables.extend(claims);
        self.redeem.last_condition_id = Some(condition_key);
        self.prune_redeem_conditions();
    }

    fn prune_redeem_conditions(&mut self) {
        if self.redeem.conditions.len() <= MAX_REDEEM_CONDITIONS {
            return;
        }
        let mut by_update = self
            .redeem
            .conditions
            .values()
            .map(|item| (item.condition_id.clone(), item.last_update_ms))
            .collect::<Vec<_>>();
        by_update.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let keep = by_update
            .into_iter()
            .take(MAX_REDEEM_CONDITIONS)
            .map(|(id, _)| id)
            .collect::<HashSet<_>>();
        self.redeem.conditions.retain(|id, _| keep.contains(id));
    }

    fn push_incident(&mut self, ts_ms: u64, kind: &str, scope: &str, detail: &str) {
        self.recent_incidents.push_front(LiveIncident {
            ts_ms,
            kind: kind.to_string(),
            scope: scope.to_string(),
            detail: detail.to_string(),
        });
        while self.recent_incidents.len() > MAX_RECENT_INCIDENTS {
            let _ = self.recent_incidents.pop_back();
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

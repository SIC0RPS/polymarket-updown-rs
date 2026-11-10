use serde::Serialize;

use crate::telemetry::FloodControlSnapshot;

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugStateV2 {
    pub(super) schema_version: String,
    pub(super) snapshot_id: String,
    pub(super) ts_ms: u64,
    pub(super) health: DebugHealth,
    pub(super) runtime: DebugRuntime,
    pub(super) freshness: DebugFreshness,
    pub(super) portfolio: DebugPortfolio,
    pub(super) risk: DebugRisk,
    pub(super) execution: DebugExecution,
    pub(super) settlement: DebugSettlement,
    pub(super) observability: DebugObservability,
    pub(super) machine_history: DebugMachineHistory,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct RuntimeManifestV1 {
    pub(super) schema_version: String,
    pub(super) generated_at_ms: u64,
    pub(super) current_run_id: Option<String>,
    pub(super) health_path: String,
    pub(super) debug_state_path: String,
    pub(super) pause_path: String,
    pub(super) events_path: String,
    pub(super) trade_session_path: Option<String>,
    pub(super) trade_run_index_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugHealth {
    pub(super) status: String,
    pub(super) degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugRuntime {
    pub(super) run_id: String,
    pub(super) mode: String,
    pub(super) pause: bool,
    pub(super) config_hash: Option<String>,
    pub(super) code_identity: String,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugFreshness {
    pub(super) market_data_age_ms: Option<u64>,
    pub(super) live_account_age_ms: Option<u64>,
    pub(super) risk_age_ms: Option<u64>,
    pub(super) machine_history_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugPortfolio {
    pub(super) marked_remaining_value_units_6dp: Option<String>,
    pub(super) receipt_realized_pnl_units_6dp: Option<String>,
    pub(super) open_pnl_units_6dp: Option<String>,
    pub(super) total_pnl_units_6dp: Option<String>,
    pub(super) valuation_portfolio_revision: Option<u64>,
    pub(super) valuation_canonical_revision: Option<u64>,
    pub(super) valuation_odds_revision: Option<u64>,
    pub(super) valued_at_ms: Option<u64>,
    pub(super) has_owned_inventory: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugRisk {
    pub(super) alerts: Vec<DebugAlert>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugAlert {
    pub(super) code: String,
    pub(super) severity: String,
    pub(super) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) age_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugExecution {
    pub(super) open_orders: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugSettlement {
    pub(super) status: String,
    pub(super) redeemable_positions: usize,
    pub(super) redeemable_inventory_qty: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) last_reclaim_check_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) active_condition_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) active_route: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) active_tx_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugObservability {
    pub(super) logging: DebugLogging,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugLogging {
    pub(super) flood_control: FloodControlSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DebugMachineHistory {
    pub(super) latest_event_id: Option<String>,
    pub(super) latest_event_ts_ms: Option<u64>,
    pub(super) archive_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct RuntimeHealthFile {
    pub(super) status: String,
    pub(super) run_id: Option<String>,
    pub(super) latest_event_id: Option<String>,
    pub(super) latest_event_ts_ms: Option<u64>,
    pub(super) config_hash: Option<String>,
    pub(super) updated_at_ms: u64,
    pub(super) active_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) settlement: Option<HealthSettlement>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct HealthSettlement {
    pub(super) status: String,
    pub(super) redeemable_positions: usize,
}

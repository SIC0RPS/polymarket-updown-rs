use anyhow::Result;

use crate::build_info::current_build_info;
use crate::event_ledger::{
    current_run_id, history_state_snapshot, latest_event, now_ms, EVENTS_JSONL_PATH,
};
use crate::observability::{Observability, RuntimeState};
use crate::runtime_control::{runtime_control_run_id, runtime_snapshot_id};
use crate::telemetry::FloodControlSnapshot;
use crate::trade_session_ledger::{
    trade_session_ledger_path_for_run, trade_session_run_index_path_for_run,
};

use super::compact::{
    age_ms, build_alerts, degraded_reasons, has_owned_inventory, market_data_age_ms,
    readiness_gaps, settlement_state,
};
use super::json_output::write_json_file;
use super::schema::*;
use super::{DEBUG_STATE_JSON_PATH, MANIFEST_JSON_PATH, PAUSE_JSONL_PATH};

pub(super) fn write_runtime_files(
    obs: &Observability,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
    runtime_state: &RuntimeState,
    runtime_config: &crate::runtime_control::RuntimeConfigState,
    account_source_ts_ms: Option<u64>,
) -> Result<()> {
    let machine_history = history_state_snapshot().machine_history;
    let manifest_run_id = current_run_id().ok().or(machine_history.run_id.clone());
    let debug_state = build_debug_state(
        runtime_state,
        runtime_config,
        live_ops,
        obs.flood_control_snapshot(),
        account_source_ts_ms,
    );
    write_json_file(DEBUG_STATE_JSON_PATH, &debug_state)?;
    write_json_file(
        crate::event_ledger::HEALTH_JSON_PATH,
        &build_health_file(runtime_state, runtime_config, live_ops),
    )?;
    write_json_file(MANIFEST_JSON_PATH, &build_runtime_manifest(manifest_run_id))?;
    Ok(())
}

pub(super) fn build_runtime_manifest(current_run_id: Option<String>) -> RuntimeManifestV1 {
    let generated_at_ms = now_ms();
    let trade_session_path = current_run_id
        .as_deref()
        .map(trade_session_ledger_path_for_run)
        .map(|path| path.to_string_lossy().to_string());
    let trade_run_index_path = current_run_id
        .as_deref()
        .map(trade_session_run_index_path_for_run)
        .map(|path| path.to_string_lossy().to_string());
    RuntimeManifestV1 {
        schema_version: "runtime_manifest.v1".to_string(),
        generated_at_ms,
        current_run_id,
        health_path: crate::event_ledger::HEALTH_JSON_PATH.to_string(),
        debug_state_path: DEBUG_STATE_JSON_PATH.to_string(),
        pause_path: PAUSE_JSONL_PATH.to_string(),
        events_path: EVENTS_JSONL_PATH.to_string(),
        trade_session_path,
        trade_run_index_path,
    }
}

pub(super) fn build_debug_state(
    runtime_state: &RuntimeState,
    runtime_config: &crate::runtime_control::RuntimeConfigState,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
    flood_control: FloodControlSnapshot,
    account_source_ts_ms: Option<u64>,
) -> DebugStateV2 {
    let ts_ms = now_ms();
    let machine_history = history_state_snapshot().machine_history;
    let run_id = runtime_control_run_id(current_run_id().ok(), machine_history.run_id.clone());
    let snapshot_id = runtime_snapshot_id(
        Some(run_id.clone()),
        machine_history.latest_event_id.clone(),
    );
    let pause = runtime_config.pause;
    let mut degraded_reasons = degraded_reasons(runtime_state, live_ops, ts_ms);
    for reason in readiness_gaps(runtime_state, live_ops) {
        if !degraded_reasons.iter().any(|existing| existing == &reason) {
            degraded_reasons.push(reason);
        }
    }
    let status = if pause {
        "blocked"
    } else if !degraded_reasons.is_empty() {
        "degraded"
    } else {
        "ready"
    }
    .to_string();
    let market_data_age_ms = market_data_age_ms(runtime_state.oracle_runtime.as_ref());
    let risk_age_ms = (runtime_state.risk.updated_at_ms > 0)
        .then_some(age_ms(ts_ms, runtime_state.risk.updated_at_ms));
    let machine_history_age_ms = machine_history
        .latest_event_ts_ms
        .map(|value| age_ms(ts_ms, value));
    let settlement = settlement_state(runtime_state, live_ops);

    DebugStateV2 {
        schema_version: "agent_state.v2".to_string(),
        snapshot_id,
        ts_ms,
        health: DebugHealth {
            status,
            degraded_reasons: degraded_reasons.clone(),
        },
        runtime: DebugRuntime {
            run_id,
            mode: runtime_state.mode.clone(),
            pause,
            config_hash: runtime_config
                .config_hash
                .clone()
                .or(machine_history.active_config_hash.clone()),
            code_identity: format!("git:{}", current_build_info().commit_sha),
        },
        freshness: DebugFreshness {
            market_data_age_ms,
            live_account_age_ms: account_source_ts_ms.map(|value| age_ms(ts_ms, value)),
            risk_age_ms,
            machine_history_age_ms,
        },
        portfolio: DebugPortfolio {
            marked_remaining_value_units_6dp: runtime_state
                .portfolio
                .marked_remaining_value_units_6dp
                .map(|units| units.to_string()),
            receipt_realized_pnl_units_6dp: runtime_state
                .portfolio
                .receipt_realized_pnl_units_6dp
                .map(|units| units.to_string()),
            open_pnl_units_6dp: runtime_state
                .portfolio
                .total_unrealized_pnl_units_6dp
                .map(|units| units.to_string()),
            total_pnl_units_6dp: runtime_state
                .portfolio
                .total_pnl_units_6dp
                .map(|units| units.to_string()),
            valuation_portfolio_revision: runtime_state
                .portfolio
                .valued_at_ms
                .map(|_| runtime_state.portfolio.revision),
            valuation_canonical_revision: runtime_state.portfolio.valuation_canonical_revision,
            valuation_odds_revision: runtime_state.portfolio.valuation_odds_revision,
            valued_at_ms: runtime_state.portfolio.valued_at_ms,
            has_owned_inventory: has_owned_inventory(&runtime_state.portfolio),
        },
        risk: DebugRisk {
            alerts: build_alerts(
                &degraded_reasons,
                runtime_state.risk.frozen_reason().as_deref(),
                runtime_state
                    .risk
                    .last_recon_at_ms
                    .map(|value| age_ms(ts_ms, value)),
            ),
        },
        execution: DebugExecution {
            open_orders: runtime_state.open_orders.orders.len(),
        },
        settlement,
        observability: DebugObservability {
            logging: DebugLogging { flood_control },
        },
        machine_history: DebugMachineHistory {
            latest_event_id: machine_history.latest_event_id.clone(),
            latest_event_ts_ms: machine_history.latest_event_ts_ms,
            archive_count: machine_history.archive_count,
        },
    }
}

pub(super) fn build_health_file(
    runtime_state: &RuntimeState,
    runtime_config: &crate::runtime_control::RuntimeConfigState,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
) -> RuntimeHealthFile {
    let latest_event = latest_event();
    let status = if runtime_config.pause {
        "pause".to_string()
    } else {
        "running".to_string()
    };
    let settlement = settlement_state(runtime_state, live_ops);
    RuntimeHealthFile {
        status,
        run_id: current_run_id()
            .ok()
            .or_else(|| history_state_snapshot().machine_history.run_id),
        latest_event_id: latest_event.as_ref().map(|event| event.event_id.clone()),
        latest_event_ts_ms: latest_event.as_ref().map(|event| event.ts_ms),
        config_hash: runtime_config
            .config_hash
            .clone()
            .or_else(|| history_state_snapshot().machine_history.active_config_hash),
        updated_at_ms: now_ms(),
        active_path: EVENTS_JSONL_PATH.to_string(),
        settlement: (settlement.redeemable_positions > 0).then_some(HealthSettlement {
            status: settlement.status,
            redeemable_positions: settlement.redeemable_positions,
        }),
    }
}

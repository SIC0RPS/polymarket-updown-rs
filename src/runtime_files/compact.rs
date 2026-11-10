use crate::observability::ui::OracleRuntimeStatusSnapshot;
use crate::observability::RuntimeState;

use super::schema::*;

pub(super) fn settlement_state(
    runtime_state: &RuntimeState,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
) -> DebugSettlement {
    let (redeemable_positions, redeemable_inventory_qty, mergeable_positions) = runtime_state
        .portfolio
        .positions
        .values()
        .filter(|position| crate::polymarket::fixed_math::has_fixed_6dp_units(position.qty))
        .fold((0usize, 0.0, 0usize), |mut totals, position| {
            if position.resolved_redeemable {
                totals.0 += 1;
                totals.1 += position.qty;
            }
            if position.mergeable {
                totals.2 += 1;
            }
            totals
        });
    let active_condition = live_ops.redeem.conditions.iter().max_by(|left, right| {
        left.last_update_ms
            .cmp(&right.last_update_ms)
            .then_with(|| left.condition_id.cmp(&right.condition_id))
    });
    let active_status = active_condition.map(|value| value.status.as_str());
    let has_settlement_inventory = redeemable_positions > 0 || mergeable_positions > 0;
    let status = if !live_ops.redeem.auto_enabled {
        "reclaim_disabled"
    } else if has_settlement_inventory
        && matches!(
            active_status,
            Some("pending" | "submitted" | "retrying" | "confirmed" | "mined")
        )
    {
        "reclaim_in_progress"
    } else if has_settlement_inventory {
        "reclaim_ready"
    } else {
        "idle"
    };
    let active_condition = (status == "reclaim_in_progress")
        .then_some(active_condition)
        .flatten();

    DebugSettlement {
        status: status.to_string(),
        redeemable_positions,
        redeemable_inventory_qty,
        last_reclaim_check_ms: live_ops.redeem.last_poll_ms,
        active_condition_id: active_condition.map(|value| value.condition_id.to_string()),
        active_route: active_condition.map(|value| value.route.clone()),
        active_tx_hash: active_condition.and_then(|value| value.tx_hash.clone()),
    }
}

pub(super) fn has_owned_inventory(portfolio: &crate::portfolio::PortfolioState) -> bool {
    portfolio
        .positions
        .values()
        .any(|position| position.qty_units_6dp > 0)
}

pub(super) fn market_data_age_ms(snapshot: Option<&OracleRuntimeStatusSnapshot>) -> Option<u64> {
    snapshot.and_then(|value| {
        value
            .symbol_last_tick_age_ms
            .iter()
            .map(|(_, age_ms)| *age_ms)
            .max()
    })
}

pub(super) fn degraded_reasons(
    runtime_state: &RuntimeState,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
    now_ts_ms: u64,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if live_ops.heartbeat.active == Some(false) {
        reasons.push("heartbeat_inactive".to_string());
    }
    if let Some(error) = runtime_state.last_error.as_ref() {
        reasons.push(format!("{}: {}", error.scope, error.msg));
    }
    if let Some(oracle_runtime) = runtime_state.oracle_runtime.as_ref() {
        if !oracle_runtime.connected {
            reasons.push(format!("oracle_runtime_{}", oracle_runtime.phase));
        }
    }
    if let Some(last_recon_at_ms) = runtime_state.risk.last_recon_at_ms {
        if age_ms(now_ts_ms, last_recon_at_ms) > 30_000 {
            reasons.push("risk_recon_stale".to_string());
        }
    }
    reasons
}

pub(super) fn readiness_gaps(
    runtime_state: &RuntimeState,
    live_ops: &crate::execution::live_ops::LiveOpsSnapshot,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if live_ops.heartbeat.active != Some(true) {
        reasons.push("heartbeat_inactive".to_string());
    }
    if runtime_state
        .last_strategy_status
        .as_ref()
        .map(|status| status.status.trim())
        .is_none_or(|status| status.is_empty() || status == "initializing")
    {
        reasons.push("strategy_initializing".to_string());
    }
    match runtime_state.oracle_runtime.as_ref() {
        Some(runtime) if runtime.connected => {}
        Some(runtime) if runtime.phase == "waiting_for_oracle" => {
            reasons.push("oracle_waiting".to_string());
        }
        Some(runtime) if runtime.phase == "recovering" => {
            reasons.push("oracle_recovering".to_string());
        }
        Some(_) | None => reasons.push("oracle_disconnected".to_string()),
    }
    reasons
}

pub(super) fn build_alerts(
    degraded_reasons: &[String],
    freeze_reason: Option<&str>,
    last_recon_age_ms: Option<u64>,
) -> Vec<DebugAlert> {
    let mut alerts = degraded_reasons
        .iter()
        .map(|reason| DebugAlert {
            code: slugify(reason),
            severity: "warn".to_string(),
            message: reason.clone(),
            age_ms: None,
            detail: None,
        })
        .collect::<Vec<_>>();
    if let Some(reason) = freeze_reason.filter(|value| !value.trim().is_empty()) {
        alerts.push(DebugAlert {
            code: "risk_freeze".to_string(),
            severity: "critical".to_string(),
            message: reason.to_string(),
            age_ms: last_recon_age_ms,
            detail: None,
        });
    }
    alerts
}

pub(super) fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut prev_underscore = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            prev_underscore = false;
            ch.to_ascii_lowercase()
        } else if prev_underscore {
            continue;
        } else {
            prev_underscore = true;
            '_'
        };
        out.push(mapped);
    }
    out.trim_matches('_').to_string()
}

pub(super) fn age_ms(now_ts_ms: u64, event_ts_ms: u64) -> u64 {
    now_ts_ms.saturating_sub(event_ts_ms)
}

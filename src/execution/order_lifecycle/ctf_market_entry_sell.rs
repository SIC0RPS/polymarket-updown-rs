use super::clean_key_component;
use crate::ctf_inventory::{is_confirmed_ctf_entry_cohort_id, is_confirmed_ctf_split_cohort_id};
use crate::orders::{ActiveOrderLifecycle, OpenOrdersState, OrderLifecycleKey};
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::order_context::OrderRuntimeMetadata;
use crate::portfolio::FillEvent;
use crate::types::{OrderCloseReason, OrderIntent, Side};

pub(super) fn is_ctf_market_entry_sell_key(key: &OrderLifecycleKey) -> bool {
    key.side == Side::Sell
        && match key.close_reason {
            OrderCloseReason::PairedFirstManagedSell => {
                is_confirmed_ctf_split_cohort_id(&key.cohort)
            }
            OrderCloseReason::PairedPostProceedsSell => {
                is_confirmed_ctf_entry_cohort_id(&key.cohort)
            }
            OrderCloseReason::PredictivePostProceedsSell => false,
        }
}

pub(crate) fn is_ctf_market_entry_sell_intent(intent: &OrderIntent) -> bool {
    OrderLifecycleKey::from_intent(intent)
        .as_ref()
        .is_some_and(is_ctf_market_entry_sell_key)
}

pub(super) fn retain_terminal_no_fill(
    key: OrderLifecycleKey,
    active: &ActiveOrderLifecycle,
    open_orders: &mut OpenOrdersState,
    now_ms: u64,
) {
    open_orders.record_active_lifecycle(
        key,
        None,
        active.remaining_qty_units_6dp,
        active.instruction_qty_units_6dp,
        active.instruction_limit_px_units_8dp,
        active.time_in_force.clone(),
        active.post_only,
        active.execution_evidence_revision,
        true,
        Some(now_ms.saturating_add(super::NO_FILL_RELEASE_MS)),
        now_ms,
    );
}

pub(super) fn market_entry_sell_block_reason(
    key: &OrderLifecycleKey,
    intent: &OrderIntent,
    open_orders: &mut OpenOrdersState,
    now_ms: u64,
) -> Option<String> {
    if !is_ctf_market_entry_sell_key(key) {
        return None;
    }
    let requested_qty_units_6dp = intent.execution_instruction_qty_units_6dp;
    let requested_limit_px_units_8dp = intent.execution_instruction_limit_px_units_8dp;
    let requested_evidence_revision = intent.execution_evidence_revision;
    if requested_qty_units_6dp <= 1
        || requested_limit_px_units_8dp == 0
        || requested_evidence_revision == 0
    {
        return Some(format!(
            "local_order_block ctf_market_entry_sell_instruction_identity_missing requested_key={}",
            key.label()
        ));
    }
    let Some(active) = open_orders.active_order_lifecycles.get(key) else {
        return None;
    };
    if active.terminal_no_fill {
        if active
            .active_until_ms
            .is_some_and(|active_until_ms| now_ms >= active_until_ms)
        {
            return None;
        }
        return Some(format!(
            "local_order_block ctf_market_entry_sell_terminal_no_fill requested_key={}",
            key.label()
        ));
    }
    let expired = active
        .active_until_ms
        .is_some_and(|active_until_ms| now_ms >= active_until_ms)
        || active
            .time_in_force
            .expiration_ms()
            .is_some_and(|expiration_ms| now_ms >= expiration_ms);
    if expired && active.order_id.is_none() {
        return None;
    }
    let exact_duplicate = !expired
        && active.remaining_qty_units_6dp == requested_qty_units_6dp
        && active.instruction_limit_px_units_8dp == requested_limit_px_units_8dp
        && active.time_in_force == intent.time_in_force
        && active.post_only == intent.post_only;
    let reason = if exact_duplicate {
        "ctf_market_entry_sell_active"
    } else if expired {
        "ctf_market_entry_sell_awaiting_terminal"
    } else {
        "ctf_market_entry_sell_identity_conflict"
    };
    Some(format!(
        "local_order_block {reason} requested_key={}",
        key.label()
    ))
}

pub(super) fn key_and_intent_for_observation(
    observation: &UnboundCtfTradeObservation,
    metadata: Option<&OrderRuntimeMetadata>,
) -> anyhow::Result<Option<(OrderLifecycleKey, u64)>> {
    key_and_intent(
        observation.market_id().as_str(),
        observation.asset_id(),
        metadata,
    )
}

pub(super) fn key_and_intent_for_fill(
    fill: &FillEvent,
    metadata: Option<&OrderRuntimeMetadata>,
) -> anyhow::Result<Option<(OrderLifecycleKey, u64)>> {
    let Some(market) = fill.market_id.as_ref() else {
        return Ok(None);
    };
    key_and_intent(market.as_str(), fill.token.as_str(), metadata)
}

fn key_and_intent(
    market: &str,
    token: &str,
    metadata: Option<&OrderRuntimeMetadata>,
) -> anyhow::Result<Option<(OrderLifecycleKey, u64)>> {
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let Some(close_reason) = metadata.order_close_reason else {
        return Ok(None);
    };
    if !matches!(
        close_reason,
        OrderCloseReason::PairedFirstManagedSell | OrderCloseReason::PairedPostProceedsSell
    ) {
        return Ok(None);
    }
    let cohort = clean_key_component(metadata.lifecycle_cohort.as_deref())
        .ok_or_else(|| anyhow::anyhow!("matched execution has no lifecycle cohort"))?;
    let symbol = clean_key_component(metadata.lifecycle_symbol.as_deref())
        .ok_or_else(|| anyhow::anyhow!("matched execution has no lifecycle symbol"))?;
    let execution_intent_id = metadata
        .execution_intent_id
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow::anyhow!("matched execution has no intent identity"))?;
    let key = OrderLifecycleKey {
        market: market.to_string(),
        token: token.to_string(),
        side: Side::Sell,
        close_reason,
        cohort: cohort.to_string(),
        symbol: symbol.to_string(),
    };
    Ok(is_ctf_market_entry_sell_key(&key).then_some((key, execution_intent_id)))
}

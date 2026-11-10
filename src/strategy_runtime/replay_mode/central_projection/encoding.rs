use std::str::FromStr;

use polymarket_client_sdk::types::U256;

use crate::types::{Side, TimeInForce};

use super::validation::{
    close_reason, resolve_market, resolve_market_token, MAX_EXACT_F64_INTEGER,
};
use super::validation::{rounded_fixed_6dp_i64, rounded_fixed_8dp};
use super::*;

const ACTIVE_LIFECYCLE_MATCH_FEE_PRESENT: u8 = 16;

pub(super) fn reserve_order_handle_in_map(
    order_handles: &mut BTreeMap<String, u64>,
    next_order_handle: &mut u64,
    order_id: &str,
) -> Result<u64> {
    anyhow::ensure!(
        !order_id.is_empty() && order_id.trim() == order_id,
        "central order identity is empty or noncanonical"
    );
    if let Some(handle) = order_handles.get(order_id).copied() {
        return Ok(handle);
    }
    anyhow::ensure!(*next_order_handle != 0, "open-order handle overflow");
    let handle = *next_order_handle;
    let following = handle
        .checked_add(1)
        .context("open-order handle overflow")?;
    anyhow::ensure!(
        order_handles.insert(order_id.to_string(), handle).is_none(),
        "central order handle reservation raced existing identity"
    );
    *next_order_handle = following;
    Ok(handle)
}

pub(super) fn encode_position(
    token_id: u32,
    portfolio_updated_at_ms: u64,
    position: &PositionState,
    token_expiry_ms: Option<u64>,
) -> Result<PositionDeltaRecord> {
    anyhow::ensure!(
        position.qty_units_6dp <= MAX_EXACT_F64_INTEGER,
        "position quantity exceeds exact strategy-view transport"
    );
    anyhow::ensure!(
        position.sellable_qty_units_6dp <= position.qty_units_6dp,
        "position sellable quantity exceeds owned quantity"
    );
    ensure_qty_view(position.qty, position.qty_units_6dp, "position quantity")?;
    ensure_qty_view(
        position.sellable_qty,
        position.sellable_qty_units_6dp,
        "position sellable quantity",
    )?;
    anyhow::ensure!(
        position.opened_at_ms.unwrap_or_default() <= position.last_update_ms
            && position.last_update_ms <= portfolio_updated_at_ms,
        "position timestamps exceed symbol portfolio time"
    );

    let avg_px_units_8dp = rounded_fixed_8dp(position.avg_px, true, "position average price")?;
    let cost_basis_units_6dp = rounded_fixed_6dp_i64(position.cost_basis, "position cost basis")?;
    let flags = u8::from(position.resolved_redeemable) * POSITION_RESOLVED_REDEEMABLE
        | u8::from(position.mergeable) * POSITION_MERGEABLE
        | u8::from(position.end_time_ms.is_some()) * POSITION_END_TIME_PRESENT
        | u8::from(token_expiry_ms.is_some()) * POSITION_TOKEN_EXPIRY_PRESENT;

    Ok(PositionDeltaRecord {
        token_id,
        operation: POSITION_UPSERT,
        flags,
        qty_units_6dp: position.qty_units_6dp,
        sellable_qty_units_6dp: position.sellable_qty_units_6dp,
        avg_px_units_8dp,
        cost_basis_units_6dp,
        opened_at_ms: position.opened_at_ms.unwrap_or_default(),
        last_update_ms: position.last_update_ms,
        end_time_ms: position.end_time_ms.or(token_expiry_ms).unwrap_or_default(),
        ..PositionDeltaRecord::default()
    })
}

pub(super) fn encode_active_lifecycle(
    catalog: &MarketCatalog,
    order_handles: &BTreeMap<String, u64>,
    key: &OrderLifecycleKey,
    active: &ActiveOrderLifecycle,
) -> Result<(LifecycleIdentity, ActiveLifecycleDeltaRecord)> {
    anyhow::ensure!(
        key.symbol == catalog.symbol(),
        "active lifecycle symbol is foreign to frozen catalog"
    );
    anyhow::ensure!(
        key.side == Side::Sell,
        "CTF CLOB buy lifecycle is forbidden"
    );
    let market = resolve_market(catalog, key.market.as_str())?;
    anyhow::ensure!(
        key.token.trim() == key.token,
        "active-lifecycle token identity is noncanonical"
    );
    let token = U256::from_str(key.token.as_str()).context("active-lifecycle token is invalid")?;
    anyhow::ensure!(
        token.to_string() == key.token,
        "active-lifecycle token identity is noncanonical"
    );
    let token_id = resolve_market_token(market, token.to_be_bytes::<32>())?;
    let cohort_id = CohortIdentityRecord::required(&key.cohort)?;
    let close_reason = close_reason(Some(key.close_reason))?;
    let remaining_qty_units_6dp = active.remaining_qty_units_6dp;
    let instruction_qty_units_6dp = active.instruction_qty_units_6dp;
    let limit_px_units_8dp = active.instruction_limit_px_units_8dp;
    anyhow::ensure!(
        remaining_qty_units_6dp > 1,
        "active-lifecycle remaining quantity is not strategy-actionable"
    );
    let order_handle = active
        .order_id
        .as_ref()
        .map(|order_id| {
            order_handles
                .get(order_id)
                .copied()
                .context("active-lifecycle order handle assignment is missing")
        })
        .transpose()?
        .unwrap_or_default();
    if active.active_until_ms == Some(0) {
        anyhow::bail!("active-lifecycle expiry is zero");
    }
    let (time_in_force, time_in_force_expiration_ms) =
        active_lifecycle_time_in_force(&active.time_in_force)?;
    let flags = u8::from(active.order_id.is_some()) * ACTIVE_LIFECYCLE_ORDER_PRESENT
        | u8::from(active.active_until_ms.is_some()) * ACTIVE_LIFECYCLE_EXPIRY_PRESENT
        | u8::from(active.terminal_no_fill) * ACTIVE_LIFECYCLE_TERMINAL_NO_FILL
        | u8::from(active.post_only) * ACTIVE_LIFECYCLE_POST_ONLY
        | u8::from(active.matched_execution_fee_units_6dp.is_some())
            * ACTIVE_LIFECYCLE_MATCH_FEE_PRESENT;
    let identity = LifecycleIdentity {
        market_id: market.market_handle,
        token_id,
        side: 2,
        close_reason,
    };
    Ok((
        identity,
        ActiveLifecycleDeltaRecord {
            operation: ACTIVE_LIFECYCLE_UPSERT,
            side: identity.side,
            close_reason: identity.close_reason,
            flags,
            time_in_force,
            market_id: identity.market_id,
            token_id: identity.token_id,
            cohort_id,
            order_handle,
            remaining_qty_units_6dp,
            instruction_qty_units_6dp,
            limit_px_units_8dp,
            execution_evidence_revision: active.execution_evidence_revision,
            execution_intent_id: active.execution_intent_id.unwrap_or_default(),
            matched_execution_qty_units_6dp: active.matched_execution_qty_units_6dp,
            matched_execution_gross_units_6dp: active.matched_execution_gross_units_6dp,
            matched_execution_fee_units_6dp: active
                .matched_execution_fee_units_6dp
                .unwrap_or_default(),
            matched_execution_portfolio_revision: active.matched_execution_portfolio_revision,
            active_until_ms: active.active_until_ms.unwrap_or_default(),
            time_in_force_expiration_ms,
            ..ActiveLifecycleDeltaRecord::default()
        },
    ))
}

fn active_lifecycle_time_in_force(value: &TimeInForce) -> Result<(u8, u64)> {
    match value {
        TimeInForce::Gtc => Ok((1, 0)),
        TimeInForce::Gtd { expiration_ms } if *expiration_ms > 0 => Ok((2, *expiration_ms)),
        TimeInForce::Gtd { .. } => anyhow::bail!("active-lifecycle GTD expiration is zero"),
        TimeInForce::Fok => Ok((3, 0)),
        TimeInForce::Fak => Ok((4, 0)),
    }
}

use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::validation::{
    close_reason, exact_qty_units, fixed_8dp, resolve_market, resolve_market_token, time_in_force,
};
use super::{
    OPEN_ORDER_ACTIVE, OPEN_ORDER_POST_ONLY, OPEN_ORDER_QUOTE_LEVEL_PRESENT, OPEN_ORDER_UPSERT,
};
use crate::orders::OpenOrderState;
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::strategy_runtime::symbol_process::{CohortIdentityRecord, OpenOrderDeltaRecord};
use crate::types::Side;

pub(super) fn encode_open_order(
    catalog: &MarketCatalog,
    open_orders_updated_at_ms: u64,
    order_handle: u64,
    order: &OpenOrderState,
) -> Result<OpenOrderDeltaRecord> {
    anyhow::ensure!(
        order.placed_at_ms <= order.last_update_ms
            && order.last_update_ms <= open_orders_updated_at_ms,
        "open-order timestamps exceed symbol open-orders time"
    );
    let intent_id = order
        .execution_intent_id
        .filter(|intent_id| *intent_id > 0)
        .context("open-order intent identity is missing")?;
    let market = resolve_market(catalog, order.market.as_str())?;
    let canonical_token = order.token.trim();
    anyhow::ensure!(
        canonical_token == order.token,
        "open-order token identity is noncanonical"
    );
    let token = U256::from_str(canonical_token).context("open-order token is invalid")?;
    anyhow::ensure!(
        token.to_string() == canonical_token,
        "open-order token identity is noncanonical"
    );
    let token_id = resolve_market_token(market, token.to_be_bytes::<32>())?;
    let (time_in_force, active_until_ms) = time_in_force(&order.time_in_force)?;
    let (side, close_reason) = match order.side {
        Side::Sell => (2, close_reason(order.order_close_reason)?),
        Side::Buy => {
            let cohort = crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                B256::from(market.condition_id),
                intent_id,
            )?;
            anyhow::ensure!(
                order.order_close_reason.is_none()
                    && !order.post_only
                    && time_in_force == 2
                    && active_until_ms == market.window_end_ms
                    && order.placed_at_ms < active_until_ms
                    && order.lifecycle_cohort.as_deref() == Some(cohort.as_str()),
                "open BUY differs from centrally bound entry identity"
            );
            (1, 0)
        }
    };
    validate_order_identity(catalog, market, order, close_reason)?;
    let cohort_id = CohortIdentityRecord::required(
        order
            .lifecycle_cohort
            .as_deref()
            .context("open-order lifecycle cohort is missing")?,
    )?;

    let original_qty_units_6dp =
        exact_qty_units(order.original_size, false, "open-order quantity")?;
    let matched_qty_units_6dp =
        exact_qty_units(order.matched_size, true, "open-order matched quantity")?;
    anyhow::ensure!(
        matched_qty_units_6dp <= original_qty_units_6dp,
        "open-order matched quantity exceeds original quantity"
    );
    let limit_px_units_8dp = fixed_8dp(order.px, false, "open-order price")?;
    let mut flags = u8::from(order.post_only) * OPEN_ORDER_POST_ONLY;
    let quote_level = order.quote_level.unwrap_or_default();
    if order.quote_level.is_some() {
        flags |= OPEN_ORDER_QUOTE_LEVEL_PRESENT;
    }

    Ok(OpenOrderDeltaRecord {
        order_handle,
        intent_id,
        operation: OPEN_ORDER_UPSERT,
        side,
        time_in_force,
        close_reason,
        status: OPEN_ORDER_ACTIVE,
        flags,
        quote_level,
        market_id: market.market_handle,
        token_id,
        cohort_id,
        original_qty_units_6dp,
        matched_qty_units_6dp,
        limit_px_units_8dp,
        placed_at_ms: order.placed_at_ms,
        last_update_ms: order.last_update_ms,
        active_until_ms,
        ..OpenOrderDeltaRecord::default()
    })
}

fn validate_order_identity(
    catalog: &MarketCatalog,
    market: &MarketCatalogRecord,
    order: &OpenOrderState,
    close_reason: u8,
) -> Result<()> {
    anyhow::ensure!(
        order.lifecycle_symbol.as_deref() == Some(catalog.symbol()),
        "open-order lifecycle symbol is missing or foreign"
    );
    let market_id = B256::from(market.condition_id).to_string();
    let cohort = order
        .lifecycle_cohort
        .as_deref()
        .context("open-order lifecycle cohort is missing")?;
    anyhow::ensure!(
        cohort.trim() == cohort,
        "open-order lifecycle cohort is noncanonical"
    );
    CohortIdentityRecord::required(cohort)?;
    let route = match close_reason {
        0 => "oracle_buy_and_hold",
        1 => "paired_first_managed_sell",
        2 => "paired_post_proceeds_sell",
        _ => unreachable!("close reason encoder returned an invalid tag"),
    };
    let expected_quote_group = format!("{route}:{market_id}:{}", order.token);
    anyhow::ensure!(
        order.quote_group.as_deref() == Some(expected_quote_group.as_str()),
        "open-order quote group is missing or noncanonical"
    );
    Ok(())
}

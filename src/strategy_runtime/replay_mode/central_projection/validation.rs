use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::types::{OrderCloseReason, TimeInForce};

const FIXED_6DP_SCALE: f64 = 1_000_000.0;
const FIXED_8DP_SCALE: f64 = 100_000_000.0;
pub(super) const MAX_EXACT_F64_INTEGER: u128 = 1_u128 << 53;

pub(super) fn resolve_market<'a>(
    catalog: &'a MarketCatalog,
    raw: &str,
) -> Result<&'a MarketCatalogRecord> {
    anyhow::ensure!(
        raw.trim() == raw,
        "managed-sell market identity is noncanonical"
    );
    let condition = B256::from_str(raw).context("managed-sell market identity is invalid")?;
    anyhow::ensure!(
        condition.to_string() == raw,
        "managed-sell market identity is noncanonical"
    );
    let mut condition_id = [0_u8; 32];
    condition_id.copy_from_slice(condition.as_slice());
    let handle = catalog
        .market_handle_for_condition(condition_id)
        .context("open-order market is foreign to frozen symbol catalog")?;
    catalog
        .record(handle)
        .context("open-order catalog market handle is missing")
}

pub(super) fn position_token_handle(catalog: &MarketCatalog, raw: &str) -> Result<u32> {
    let canonical = raw.trim();
    anyhow::ensure!(canonical == raw, "position token identity is noncanonical");
    let token = U256::from_str(canonical).context("position token is invalid")?;
    anyhow::ensure!(
        token.to_string() == canonical,
        "position token identity is noncanonical"
    );
    catalog
        .token_handle(token.to_be_bytes::<32>())
        .context("position token is foreign to frozen symbol catalog")
}

pub(super) fn resolve_market_token(market: &MarketCatalogRecord, token: [u8; 32]) -> Result<u32> {
    if token == market.up_token {
        Ok(market.up_token_handle)
    } else if token == market.down_token {
        Ok(market.down_token_handle)
    } else {
        anyhow::bail!("open-order token is foreign to market")
    }
}

pub(super) fn time_in_force(value: &TimeInForce) -> Result<(u8, u64)> {
    match value {
        TimeInForce::Gtc => Ok((1, 0)),
        TimeInForce::Gtd { expiration_ms } if *expiration_ms > 0 => Ok((2, *expiration_ms)),
        TimeInForce::Gtd { .. } => anyhow::bail!("open-order GTD expiration is zero"),
        TimeInForce::Fok | TimeInForce::Fak => {
            anyhow::bail!("immediate order is present in central open-order state")
        }
    }
}

pub(super) fn close_reason(value: Option<OrderCloseReason>) -> Result<u8> {
    match value {
        Some(OrderCloseReason::PairedFirstManagedSell) => Ok(1),
        Some(OrderCloseReason::PairedPostProceedsSell) => Ok(2),
        Some(OrderCloseReason::PredictivePostProceedsSell) => {
            anyhow::bail!("predictive post-proceeds sell is forbidden")
        }
        None => anyhow::bail!("open managed-sell close reason is missing"),
    }
}

pub(super) fn ensure_qty_view(value: f64, units: u128, owner: &str) -> Result<()> {
    anyhow::ensure!(
        exact_qty_units(value, units == 0, owner)? == units,
        "{owner} conflicts with its fixed-6dp owner units"
    );
    Ok(())
}

pub(super) fn exact_qty_units(value: f64, allow_zero: bool, owner: &str) -> Result<u128> {
    anyhow::ensure!(
        value.is_finite() && value >= 0.0 && !value.is_sign_negative(),
        "{owner} is invalid"
    );
    if value == 0.0 && allow_zero {
        return Ok(0);
    }
    let units = crate::ctf_accounting::qty_to_units_6dp(value)
        .with_context(|| format!("{owner} is not exact positive fixed-6dp"))?;
    anyhow::ensure!(
        units <= MAX_EXACT_F64_INTEGER,
        "{owner} exceeds exact strategy-view transport"
    );
    anyhow::ensure!(
        (units as f64 / FIXED_6DP_SCALE).to_bits() == value.to_bits(),
        "{owner} is not an exact fixed-6dp strategy view"
    );
    Ok(units)
}

pub(super) fn rounded_fixed_6dp_i64(value: f64, owner: &str) -> Result<i64> {
    anyhow::ensure!(
        value.is_finite() && value >= 0.0 && !value.is_sign_negative(),
        "{owner} is invalid"
    );
    let scaled = value * FIXED_6DP_SCALE;
    anyhow::ensure!(scaled.is_finite(), "{owner} overflows fixed-6dp");
    let rounded = scaled.round();
    anyhow::ensure!(
        rounded <= MAX_EXACT_F64_INTEGER as f64,
        "{owner} exceeds exact strategy-view transport"
    );
    Ok(rounded as i64)
}

pub(super) fn fixed_8dp(value: f64, allow_zero: bool, owner: &str) -> Result<u64> {
    anyhow::ensure!(
        value.is_finite()
            && value >= 0.0
            && !value.is_sign_negative()
            && value <= 1.0
            && (allow_zero || value > 0.0),
        "{owner} is outside probability range"
    );
    let scaled = value * FIXED_8DP_SCALE;
    let rounded = scaled.round();
    anyhow::ensure!(rounded <= u64::MAX as f64, "{owner} overflows fixed-8dp");
    let units = rounded as u64;
    anyhow::ensure!(
        (units as f64 / FIXED_8DP_SCALE).to_bits() == value.to_bits(),
        "{owner} is not an exact fixed-8dp strategy view"
    );
    Ok(units)
}

pub(super) fn rounded_fixed_8dp(value: f64, allow_zero: bool, owner: &str) -> Result<u64> {
    anyhow::ensure!(
        value.is_finite()
            && value >= 0.0
            && !value.is_sign_negative()
            && value <= 1.0
            && (allow_zero || value > 0.0),
        "{owner} is outside probability range"
    );
    let scaled = value * FIXED_8DP_SCALE;
    let rounded = scaled.round();
    anyhow::ensure!(rounded <= u64::MAX as f64, "{owner} overflows fixed-8dp");
    Ok(rounded as u64)
}

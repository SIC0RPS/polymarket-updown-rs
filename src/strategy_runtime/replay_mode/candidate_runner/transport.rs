use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};
use sha2::{Digest, Sha256};

use crate::ctf_inventory::ConfirmedCtfInventoryOperation;
use crate::portfolio::{ConfirmedLifecycleSellFill, ExpectedCtfPosition};
use crate::risk::{MarketBudgetView, RiskBudgetSnapshot};
use crate::strategy::InventoryOperation;
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::strategy_runtime::symbol_process::{
    CohortIdentityRecord, InventoryReceiptOperation, InventoryReceiptRecord,
    ManagedSellReceiptRecord, MarketBudgetRecord, RiskBudget,
};
use crate::types::OrderCloseReason;

const FIXED_8DP_SCALE: u128 = 100_000_000;
const RISK_ENABLED: u16 = 1;
const RISK_TRADING_ALLOWED: u16 = 2;
const MARKET_BUDGET_CAP_EQUITY_PRESENT: u16 = 1 << 0;
const MARKET_BUDGET_PCT_CAP_PRESENT: u16 = 1 << 1;
const MARKET_BUDGET_EFFECTIVE_CAP_PRESENT: u16 = 1 << 2;
const MARKET_BUDGET_EFFECTIVE_EXPOSURE_PRESENT: u16 = 1 << 3;
const MARKET_BUDGET_MARKET_HEADROOM_PRESENT: u16 = 1 << 4;
const MARKET_BUDGET_REQUIRED_FACTS: u16 =
    MARKET_BUDGET_EFFECTIVE_EXPOSURE_PRESENT | MARKET_BUDGET_MARKET_HEADROOM_PRESENT;

pub(crate) fn risk_records(
    snapshot: &RiskBudgetSnapshot,
    catalog: &MarketCatalog,
    source_sequence: u64,
    risk_central_state_seq: u64,
) -> Result<(RiskBudget, Vec<MarketBudgetRecord>)> {
    ensure_frozen_catalog(catalog)?;
    anyhow::ensure!(
        risk_central_state_seq > 0,
        "risk central-state sequence is zero"
    );
    anyhow::ensure!(snapshot.updated_at_ms > 0, "risk captured time is zero");

    let mut flags = 0;
    if snapshot.enabled {
        flags |= RISK_ENABLED;
    }
    if snapshot.trading_allowed {
        flags |= RISK_TRADING_ALLOWED;
    }
    let risk = RiskBudget {
        source_sequence,
        central_state_seq: risk_central_state_seq,
        updated_at_ms: snapshot.updated_at_ms,
        flags,
        freeze_level: snapshot.freeze_level,
        freeze_reason_code: optional_hash_u16(
            snapshot.frozen_rule.as_deref(),
            b"risk-freeze-rule-v1\0",
        )?,
        freeze_detail_hash: optional_hash_u64(
            snapshot.frozen_details.as_deref(),
            b"risk-freeze-detail-v1\0",
        )?,
        max_order_qty_units_6dp: u128::from(snapshot.max_order_qty.units()),
        available_buy_budget_units_6dp: risk_units_i64(
            snapshot.available_buy_budget_usdt,
            "available buy budget",
        )?,
        max_entry_notional_units_6dp: risk_units_i64(
            snapshot.max_entry_notional_usdt,
            "max entry notional",
        )?,
        max_inventory_total_units_6dp: u128::from(snapshot.max_inventory_total.units()),
        remaining_total_inventory_units_6dp: u128::from(
            snapshot.remaining_total_inventory_qty.units(),
        ),
    }
    .validate()?;

    let mut markets = Vec::with_capacity(snapshot.market_budgets.len());
    for (market_id, budget) in &snapshot.market_budgets {
        anyhow::ensure!(
            budget.market_id == *market_id,
            "market budget map key conflicts with owner identity"
        );
        markets.push(market_budget_record(budget, catalog)?);
    }
    markets.sort_unstable_by_key(|record| record.market_id);
    Ok((risk, markets))
}

pub(crate) fn managed_sell_receipt(
    fill: &ConfirmedLifecycleSellFill,
    catalog: &MarketCatalog,
    order_handle: u64,
    fill_handle: u64,
) -> Result<ManagedSellReceiptRecord> {
    ensure_frozen_catalog(catalog)?;
    anyhow::ensure!(
        !fill.fill_id.is_empty() && fill.fill_id.trim() == fill.fill_id,
        "managed-sell fill identity is empty or noncanonical"
    );
    anyhow::ensure!(
        !fill.order_id.is_empty() && fill.order_id.trim() == fill.order_id,
        "managed-sell order identity is empty or noncanonical"
    );
    let receipt_identity = fill
        .receipt_identity
        .as_ref()
        .context("central managed-sell receipt identity is unavailable")?;
    anyhow::ensure!(
        receipt_identity.fill_id(&fill.order_id)? == fill.fill_id,
        "central managed-sell receipt identity differs from fill identity"
    );
    let market = resolve_market(catalog, fill.market_id.as_str())?;
    let token_id = resolve_token(market, &fill.token)?;
    let cohort_id = CohortIdentityRecord::required(&fill.lifecycle_cohort)?;
    let close_reason = match fill.close_reason {
        OrderCloseReason::PairedFirstManagedSell => 1,
        OrderCloseReason::PairedPostProceedsSell => 2,
        OrderCloseReason::PredictivePostProceedsSell => {
            anyhow::bail!("predictive post-proceeds managed sell is forbidden")
        }
    };
    let record = ManagedSellReceiptRecord {
        fill_handle,
        receipt_identity_sha256: domain_hash(
            b"polymarket-managed-sell-receipt-identity-v1\0",
            &fill.fill_id,
        ),
        order_handle,
        intent_id: fill.intent_id,
        market_id: market.market_handle,
        token_id,
        cohort_id,
        close_reason,
        status: 1,
        flags: 0,
        qty_units_6dp: fill.qty_units_6dp,
        fill_px_units_8dp: gross_unit_price_8dp(fill.gross_proceeds_units_6dp, fill.qty_units_6dp)?,
        gross_proceeds_units_6dp: fill.gross_proceeds_units_6dp,
        fee_units_6dp: fill.fee_units_6dp,
        confirmed_at_ms: fill.confirmed_at_ms,
    };
    record.validate()?.validate_catalog(catalog)
}

pub(crate) fn inventory_receipt(
    confirmed: &ConfirmedCtfInventoryOperation,
    expected: &[ExpectedCtfPosition; 2],
    catalog: &MarketCatalog,
    receipt_handle: u64,
    transaction_handle: u64,
    intent_id: u64,
) -> Result<InventoryReceiptRecord> {
    ensure_frozen_catalog(catalog)?;
    anyhow::ensure!(
        confirmed.operation == InventoryOperation::SplitCompleteSet,
        "candidate inventory transport accepts confirmed split receipts only"
    );
    anyhow::ensure!(
        !confirmed.tx_hash.is_empty() && confirmed.tx_hash.trim() == confirmed.tx_hash,
        "confirmed split transaction identity is empty or noncanonical"
    );
    anyhow::ensure!(
        confirmed.receipt_confirmed_ts_ms > 0
            && confirmed.materialized_ts_ms >= confirmed.receipt_confirmed_ts_ms,
        "confirmed split receipt/materialization time is invalid"
    );
    let condition_id = confirmed.condition_id.to_string();
    anyhow::ensure!(
        confirmed.market_id == condition_id,
        "confirmed split market/condition identity mismatch"
    );
    let market = resolve_market(catalog, &condition_id)?;
    ensure_catalog_tokens(market, &confirmed.up_token, &confirmed.down_token)?;
    let cohort_id = CohortIdentityRecord::required(&confirmed.cohort_id)?;
    let gamma_market_id = confirmed
        .gamma_market_id
        .as_deref()
        .context("confirmed split gamma market identity is unavailable")?;
    let parsed_gamma_market_id = gamma_market_id
        .parse::<u64>()
        .context("confirmed split gamma market identity is invalid")?;
    anyhow::ensure!(
        parsed_gamma_market_id > 0
            && parsed_gamma_market_id == market.gamma_market_id
            && parsed_gamma_market_id.to_string() == gamma_market_id,
        "confirmed split gamma market identity conflicts with frozen catalog"
    );
    let expected_allocation = |position: &ExpectedCtfPosition, token: &str| {
        anyhow::ensure!(
            position.token == token
                && position.market_id.as_str() == confirmed.market_id
                && position.engine_trade_id.as_deref() == Some(confirmed.cohort_id.as_str())
                && position.confirmed_entry_tx_hash.as_deref() == Some(confirmed.tx_hash.as_str())
                && position.confirmed_token_created_qty_units_6dp
                    == Some(confirmed.amount_units_6dp)
                && position.min_qty_units_6dp == Some(confirmed.amount_units_6dp)
                && position.require_positive_balance
                && position.portfolio_actionable,
            "canonical expected split position conflicts with materialized operation token={token}"
        );
        Ok((
            position
                .confirmed_token_principal_units_6dp
                .context("canonical expected split principal is unavailable")?,
            position
                .confirmed_token_entry_fee_units_6dp
                .context("canonical expected split entry fee is unavailable")?,
            position
                .confirmed_token_approved_direct_cost_units_6dp
                .context("canonical expected split direct cost is unavailable")?,
        ))
    };
    let (up_principal_units_6dp, up_entry_fee_units_6dp, up_direct_cost_units_6dp) =
        expected_allocation(&expected[0], &confirmed.up_token)?;
    let (down_principal_units_6dp, down_entry_fee_units_6dp, down_direct_cost_units_6dp) =
        expected_allocation(&expected[1], &confirmed.down_token)?;
    let fee_units_6dp = up_entry_fee_units_6dp
        .checked_add(down_entry_fee_units_6dp)
        .context("canonical expected split fee overflow")?;
    let direct_cost_units_6dp = up_direct_cost_units_6dp
        .checked_add(down_direct_cost_units_6dp)
        .context("canonical expected split direct-cost overflow")?;
    let entry_outlay_units_6dp = confirmed
        .amount_units_6dp
        .checked_add(fee_units_6dp)
        .and_then(|value| value.checked_add(direct_cost_units_6dp))
        .context("canonical expected split outlay overflow")?;
    anyhow::ensure!(
        expected.iter().all(|position| {
            position.confirmed_entry_outlay_units_6dp == Some(entry_outlay_units_6dp)
        }),
        "canonical expected split outlay conflicts across tokens"
    );
    let record = InventoryReceiptRecord {
        receipt_handle,
        transaction_handle,
        settlement_identity_handle: 0,
        intent_id,
        operation: InventoryReceiptOperation::Split,
        status: 1,
        payout_side: 0,
        flags: 0,
        market_id: market.market_handle,
        condition_id: market.market_handle,
        up_token_id: market.up_token_handle,
        down_token_id: market.down_token_handle,
        cohort_id,
        qty_units_6dp: confirmed.amount_units_6dp,
        collateral_debit_units_6dp: confirmed.amount_units_6dp,
        collateral_receipt_units_6dp: 0,
        up_principal_units_6dp,
        down_principal_units_6dp,
        up_entry_fee_units_6dp,
        down_entry_fee_units_6dp,
        up_direct_cost_units_6dp,
        down_direct_cost_units_6dp,
        fee_units_6dp,
        direct_cost_units_6dp,
        confirmed_at_ms: confirmed.receipt_confirmed_ts_ms,
    };
    record.validate()?.validate_catalog(catalog)
}

fn market_budget_record(
    budget: &MarketBudgetView,
    catalog: &MarketCatalog,
) -> Result<MarketBudgetRecord> {
    let market = resolve_market(catalog, budget.market_id.as_str())?;
    let mut flags = MARKET_BUDGET_REQUIRED_FACTS;
    let cap_equity_units_6dp = if let Some(value) = budget.cap_equity_usdt {
        flags |= MARKET_BUDGET_CAP_EQUITY_PRESENT;
        risk_units_i64(value, "market cap equity")?
    } else {
        0
    };
    let pct_cap_notional_units_6dp = if let Some(value) = budget.pct_cap_notional {
        flags |= MARKET_BUDGET_PCT_CAP_PRESENT;
        risk_units_i64(value, "market percentage cap")?
    } else {
        0
    };
    let effective_cap_units_6dp = if let Some(value) = budget.effective_market_cap_notional {
        flags |= MARKET_BUDGET_EFFECTIVE_CAP_PRESENT;
        risk_units_i64(value, "effective market cap")?
    } else {
        0
    };
    let record = MarketBudgetRecord {
        market_id: market.market_handle,
        flags,
        binding_rule: binding_rule(budget.binding_rule.as_deref())?,
        cap_equity_units_6dp,
        pct_cap_notional_units_6dp,
        effective_cap_units_6dp,
        effective_exposure_units_6dp: risk_units_i64(
            budget.effective_market_exposure,
            "effective market exposure",
        )?,
        remaining_market_headroom_units_6dp: risk_units_i64(
            budget.remaining_market_headroom_usdt,
            "remaining market headroom",
        )?,
    };
    record.validate()?.validate_catalog(catalog)
}

fn ensure_frozen_catalog(catalog: &MarketCatalog) -> Result<()> {
    anyhow::ensure!(
        !catalog.is_empty(),
        "candidate transport requires a nonempty frozen market catalog"
    );
    Ok(())
}

fn resolve_market<'a>(
    catalog: &'a MarketCatalog,
    market_id: &str,
) -> Result<&'a MarketCatalogRecord> {
    anyhow::ensure!(
        !market_id.is_empty() && market_id.trim() == market_id,
        "market identity is empty or noncanonical"
    );
    let condition = B256::from_str(market_id).context("market identity is invalid")?;
    anyhow::ensure!(
        condition.to_string() == market_id,
        "market identity is noncanonical"
    );
    let handle = catalog
        .market_handle_for_condition(condition.into())
        .context("market identity is foreign to frozen symbol catalog")?;
    catalog
        .record(handle)
        .context("frozen catalog market handle is missing")
}

fn resolve_token(market: &MarketCatalogRecord, token: &str) -> Result<u32> {
    anyhow::ensure!(
        !token.is_empty() && token.trim() == token,
        "token identity is empty or noncanonical"
    );
    let token_id = U256::from_str(token).context("token identity is invalid")?;
    anyhow::ensure!(
        token_id.to_string() == token,
        "token identity is noncanonical"
    );
    let bytes = token_id.to_be_bytes::<32>();
    if bytes == market.up_token {
        Ok(market.up_token_handle)
    } else if bytes == market.down_token {
        Ok(market.down_token_handle)
    } else {
        anyhow::bail!("token identity is foreign to market")
    }
}

fn ensure_catalog_tokens(
    market: &MarketCatalogRecord,
    up_token: &str,
    down_token: &str,
) -> Result<()> {
    anyhow::ensure!(
        resolve_token(market, up_token)? == market.up_token_handle
            && resolve_token(market, down_token)? == market.down_token_handle,
        "confirmed inventory outcome token orientation conflicts with frozen catalog"
    );
    Ok(())
}

fn gross_unit_price_8dp(gross_units_6dp: i128, qty_units_6dp: u128) -> Result<u64> {
    let gross =
        u128::try_from(gross_units_6dp).context("managed-sell gross proceeds are negative")?;
    anyhow::ensure!(gross > 0, "managed-sell gross proceeds are zero");
    anyhow::ensure!(qty_units_6dp > 0, "managed-sell quantity is zero");
    let numerator = gross
        .checked_mul(FIXED_8DP_SCALE)
        .context("managed-sell transport price overflow")?;
    let quotient = numerator / qty_units_6dp;
    let remainder = numerator % qty_units_6dp;
    let rounded = quotient
        .checked_add(u128::from(remainder >= qty_units_6dp - remainder))
        .context("managed-sell transport price rounding overflow")?;
    anyhow::ensure!(
        (1..=FIXED_8DP_SCALE).contains(&rounded),
        "managed-sell transport price outside probability range"
    );
    u64::try_from(rounded).context("managed-sell transport price exceeds u64")
}

fn risk_units_i64(value: crate::risk::RiskFixed6, owner: &str) -> Result<i64> {
    i64::try_from(value.units()).with_context(|| format!("{owner} exceeds i64"))
}

fn binding_rule(rule: Option<&str>) -> Result<u16> {
    match rule {
        None => Ok(0),
        Some("max_window_cost") => Ok(1),
        Some("max_market_equity_pct") => Ok(2),
        Some(other) => anyhow::bail!("unknown market-budget binding rule `{other}`"),
    }
}

fn optional_hash_u16(value: Option<&str>, domain: &[u8]) -> Result<u16> {
    let Some(value) = canonical_optional(value)? else {
        return Ok(0);
    };
    let digest = domain_hash(domain, value);
    let hash = u16::from_be_bytes(digest[..2].try_into().expect("SHA-256 prefix width"));
    anyhow::ensure!(hash != 0, "nonempty risk identity hashes to zero");
    Ok(hash)
}

fn optional_hash_u64(value: Option<&str>, domain: &[u8]) -> Result<u64> {
    let Some(value) = canonical_optional(value)? else {
        return Ok(0);
    };
    let digest = domain_hash(domain, value);
    let hash = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix width"));
    anyhow::ensure!(hash != 0, "nonempty risk detail hashes to zero");
    Ok(hash)
}

fn canonical_optional(value: Option<&str>) -> Result<Option<&str>> {
    match value {
        None => Ok(None),
        Some(value) => {
            anyhow::ensure!(
                !value.is_empty() && value.trim() == value,
                "risk identity is empty or noncanonical"
            );
            Ok(Some(value))
        }
    }
}

fn domain_hash(domain: &[u8], value: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(value.as_bytes());
    hasher.finalize().into()
}

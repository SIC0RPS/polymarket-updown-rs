use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::CandidateSymbolRuntime;
use crate::orders::{OrderLifecycleKey, OrderUpdateEvent};
use crate::polymarket::identity::MarketId;
use crate::polymarket::order_context::OrderRuntimeMetadata;
use crate::portfolio::{ConfirmedLifecycleSellFill, ExpectedCtfPosition, PositionState};
use crate::risk::{MarketBudgetView, RiskBudgetSnapshot};
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::strategy_runtime::symbol_process::{
    ActiveLifecycleDeltaRecord, InventoryReceiptOperation, InventoryReceiptRecord,
    ManagedSellReceiptRecord, MarketBudgetRecord, OpenOrderDeltaRecord, PositionDeltaRecord,
    RiskBudget,
};
use crate::types::{OrderCloseReason, Side, TimeInForce};

const MAX_EXACT_F64_INTEGER: u128 = 1_u128 << 53;
const POSITION_RESOLVED_REDEEMABLE: u8 = 1;
const POSITION_MERGEABLE: u8 = 2;
const POSITION_END_TIME_PRESENT: u8 = 4;
const POSITION_TOKEN_EXPIRY_PRESENT: u8 = 8;
const OPEN_ORDER_POST_ONLY: u8 = 1;
const OPEN_ORDER_QUOTE_LEVEL_PRESENT: u8 = 2;
const ACTIVE_LIFECYCLE_ORDER_PRESENT: u8 = 1;
const ACTIVE_LIFECYCLE_EXPIRY_PRESENT: u8 = 2;
const ACTIVE_LIFECYCLE_TERMINAL_NO_FILL: u8 = 4;
const ACTIVE_LIFECYCLE_POST_ONLY: u8 = 8;
const ACTIVE_LIFECYCLE_MATCH_FEE_PRESENT: u8 = 16;
const RISK_ENABLED: u16 = 1;
const RISK_TRADING_ALLOWED: u16 = 2;
const MARKET_BUDGET_CAP_EQUITY_PRESENT: u16 = 1 << 0;
const MARKET_BUDGET_PCT_CAP_PRESENT: u16 = 1 << 1;
const MARKET_BUDGET_EFFECTIVE_CAP_PRESENT: u16 = 1 << 2;
const MARKET_BUDGET_EFFECTIVE_EXPOSURE_PRESENT: u16 = 1 << 3;
const MARKET_BUDGET_MARKET_HEADROOM_PRESENT: u16 = 1 << 4;
const MARKET_BUDGET_REQUIRED_FACTS: u16 =
    MARKET_BUDGET_EFFECTIVE_EXPOSURE_PRESENT | MARKET_BUDGET_MARKET_HEADROOM_PRESENT;

#[path = "views/receipts.rs"]
mod receipts;
#[path = "views/revision.rs"]
mod revision;

use revision::accept_revision;

impl CandidateSymbolRuntime {
    pub(super) fn ingest_position_deltas(
        &mut self,
        records: &[PositionDeltaRecord],
        updated_at_ms: u64,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let candidate = self.candidate_mut()?;
        for record in records {
            anyhow::ensure!(
                record.opened_at_ms <= updated_at_ms
                    && record.last_update_ms <= updated_at_ms,
                "position timestamps exceed frame time opened_at_ms={} last_update_ms={} frame_timestamp_ms={updated_at_ms}",
                record.opened_at_ms,
                record.last_update_ms
            );
            accept_revision(
                &mut candidate.position_revision_by_token,
                record.token_id,
                record.central_revision,
                "position",
            )?;
            let (market, token) = resolve_token(&catalog, record.token_id)?;
            if record.operation == 2 {
                candidate.portfolio.positions.remove(&token);
                candidate.portfolio.token_market_hint.remove(&token);
                candidate.portfolio.token_expiry_ms.remove(&token);
                continue;
            }
            candidate.portfolio.token_market_hint.insert(
                token.clone(),
                MarketId::new(B256::from(market.condition_id).to_string()),
            );
            if record.flags & POSITION_TOKEN_EXPIRY_PRESENT != 0 {
                candidate
                    .portfolio
                    .token_expiry_ms
                    .insert(token.clone(), record.end_time_ms);
            } else {
                candidate.portfolio.token_expiry_ms.remove(&token);
            }
            candidate.portfolio.positions.insert(
                token.clone(),
                PositionState {
                    token,
                    qty_units_6dp: record.qty_units_6dp,
                    qty: units_u128_to_f64(record.qty_units_6dp)?,
                    sellable_qty_units_6dp: record.sellable_qty_units_6dp,
                    sellable_qty: units_u128_to_f64(record.sellable_qty_units_6dp)?,
                    avg_px: price_units_to_f64(record.avg_px_units_8dp),
                    cost_basis: units_i64_to_f64(record.cost_basis_units_6dp)?,
                    notional_basis: units_i64_to_f64(record.cost_basis_units_6dp)?,
                    opened_at_ms: (record.opened_at_ms > 0).then_some(record.opened_at_ms),
                    last_update_ms: record.last_update_ms,
                    end_time_ms: (record.flags & POSITION_END_TIME_PRESENT != 0)
                        .then_some(record.end_time_ms),
                    resolved_redeemable: record.flags & POSITION_RESOLVED_REDEEMABLE != 0,
                    mergeable: record.flags & POSITION_MERGEABLE != 0,
                    ..PositionState::default()
                },
            );
        }
        candidate.portfolio.note_runtime_mutation(updated_at_ms);
        Ok(())
    }

    pub(super) fn ingest_open_order_deltas(
        &mut self,
        records: &[OpenOrderDeltaRecord],
        updated_at_ms: u64,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let symbol = self.symbol.as_str();
        let candidate = self.candidate_mut()?;
        for record in records {
            anyhow::ensure!(
                record.placed_at_ms <= updated_at_ms
                    && record.last_update_ms <= updated_at_ms,
                "open-order timestamps exceed frame time placed_at_ms={} last_update_ms={} frame_timestamp_ms={updated_at_ms}",
                record.placed_at_ms,
                record.last_update_ms
            );
            accept_revision(
                &mut candidate.order_revision_by_handle,
                record.order_handle,
                record.central_revision,
                "open order",
            )?;
            let market = catalog
                .record(record.market_id)
                .context("open-order market handle is missing")?;
            let market_id = B256::from(market.condition_id).to_string();
            let (_, token) = resolve_token(&catalog, record.token_id)?;
            let order_id = format!("central-order-{}", record.order_handle);
            let side = side(record.side)?;
            let close_reason = close_reason_optional(record.close_reason)?;
            let lifecycle_cohort = Some(record.cohort_id.required_str()?.to_string());
            let time_in_force = time_in_force(record.time_in_force, record.active_until_ms)?;
            let original_size = units_u128_to_f64(record.original_qty_units_6dp)?;
            let matched_size = units_u128_to_f64(record.matched_qty_units_6dp)?;
            let terminal = record.operation == 2;
            let fully_matched = matched_size >= original_size && original_size > 0.0;
            let metadata = OrderRuntimeMetadata {
                execution_intent_id: Some(record.intent_id),
                time_in_force,
                post_only: record.flags & OPEN_ORDER_POST_ONLY != 0,
                quote_level: (record.flags & OPEN_ORDER_QUOTE_LEVEL_PRESENT != 0)
                    .then_some(record.quote_level),
                quote_group: close_reason
                    .map(|reason| format!("{}:{market_id}:{token}", reason.as_str())),
                lifecycle_cohort: lifecycle_cohort.clone(),
                lifecycle_symbol: Some(symbol.to_string()),
                order_close_reason: close_reason,
            };
            candidate.open_orders.apply_update_with_metadata_at(
                OrderUpdateEvent {
                    ts_ms: record.last_update_ms,
                    order_id: order_id.clone(),
                    market: market_id.clone(),
                    token: token.clone(),
                    side,
                    px: price_units_to_f64(record.limit_px_units_8dp),
                    original_size: Some(original_size),
                    matched_size: Some(matched_size),
                    status: Some(
                        if terminal {
                            if fully_matched {
                                "MATCHED"
                            } else {
                                "CANCELED"
                            }
                        } else if matched_size > 0.0 {
                            "PARTIALLY_FILLED"
                        } else {
                            "OPEN"
                        }
                        .to_string(),
                    ),
                    msg_type: terminal.then(|| format!("CENTRAL_TERMINAL_{}", record.status)),
                    terminal_reason: None,
                    terminal_reason_provenance: None,
                },
                Some(&metadata),
                updated_at_ms,
            );
        }
        Ok(())
    }

    pub(super) fn ingest_active_lifecycle_deltas(
        &mut self,
        records: &[ActiveLifecycleDeltaRecord],
        updated_at_ms: u64,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let symbol = self.symbol.as_str();
        let candidate = self.candidate_mut()?;
        for record in records {
            let identity = (
                record.market_id,
                record.token_id,
                record.side,
                record.close_reason,
            );
            accept_revision(
                &mut candidate.lifecycle_revision_by_identity,
                identity,
                record.central_revision,
                "active lifecycle",
            )?;
            let market = catalog
                .record(record.market_id)
                .context("active-lifecycle market handle is missing")?;
            let market_id = B256::from(market.condition_id).to_string();
            let (_, token) = resolve_token(&catalog, record.token_id)?;
            let key = OrderLifecycleKey {
                market: market_id.clone(),
                token,
                side: side(record.side)?,
                close_reason: close_reason(record.close_reason)?,
                cohort: record.cohort_id.required_str()?.to_string(),
                symbol: symbol.to_string(),
            };
            if record.operation == 2 {
                candidate
                    .open_orders
                    .remove_active_lifecycle(&key, updated_at_ms);
                continue;
            }
            let order_id = (record.flags & ACTIVE_LIFECYCLE_ORDER_PRESENT != 0)
                .then(|| format!("central-order-{}", record.order_handle));
            let active_until_ms = (record.flags & ACTIVE_LIFECYCLE_EXPIRY_PRESENT != 0)
                .then_some(record.active_until_ms);
            let time_in_force =
                time_in_force(record.time_in_force, record.time_in_force_expiration_ms)?;
            candidate.open_orders.record_active_lifecycle(
                key.clone(),
                order_id,
                record.remaining_qty_units_6dp,
                record.instruction_qty_units_6dp,
                record.limit_px_units_8dp,
                time_in_force,
                record.flags & ACTIVE_LIFECYCLE_POST_ONLY != 0,
                record.execution_evidence_revision,
                record.flags & ACTIVE_LIFECYCLE_TERMINAL_NO_FILL != 0,
                active_until_ms,
                updated_at_ms,
            );
            if record.execution_intent_id > 0 {
                candidate.open_orders.bind_active_lifecycle_intent(
                    &key,
                    record.execution_intent_id,
                    updated_at_ms,
                );
            }
            if record.matched_execution_qty_units_6dp > 0 {
                candidate.open_orders.record_matched_execution(
                    &key,
                    record.matched_execution_qty_units_6dp,
                    record.matched_execution_gross_units_6dp,
                    (record.flags & ACTIVE_LIFECYCLE_MATCH_FEE_PRESENT != 0)
                        .then_some(record.matched_execution_fee_units_6dp),
                    record.matched_execution_portfolio_revision,
                    updated_at_ms,
                );
            } else {
                candidate.open_orders.clear_matched_execution(
                    &key,
                    record.matched_execution_portfolio_revision,
                    updated_at_ms,
                );
            }
        }
        Ok(())
    }

    pub(super) fn begin_risk_view(&mut self, record: RiskBudget) -> Result<()> {
        let candidate = self.candidate_mut()?;
        anyhow::ensure!(
            candidate.pending_risk.is_none(),
            "risk snapshot already pending"
        );
        candidate.pending_risk = Some(RiskBudgetSnapshot {
            updated_at_ms: record.updated_at_ms,
            enabled: record.flags & RISK_ENABLED != 0,
            trading_allowed: record.flags & RISK_TRADING_ALLOWED != 0,
            freeze_level: record.freeze_level,
            frozen_rule: (record.freeze_reason_code != 0)
                .then(|| format!("central_freeze_code_{}", record.freeze_reason_code)),
            frozen_details: (record.freeze_detail_hash != 0)
                .then(|| format!("central_freeze_detail_{:016x}", record.freeze_detail_hash)),
            max_order_qty: crate::risk::RiskFixed6::from_units(
                record.max_order_qty_units_6dp,
                "worker maximum order quantity",
            )?,
            available_buy_budget_usdt: risk_units_from_i64(
                record.available_buy_budget_units_6dp,
                "worker available buy budget",
            )?,
            max_entry_notional_usdt: risk_units_from_i64(
                record.max_entry_notional_units_6dp,
                "worker maximum entry notional",
            )?,
            max_inventory_total: crate::risk::RiskFixed6::from_units(
                record.max_inventory_total_units_6dp,
                "worker maximum total inventory",
            )?,
            remaining_total_inventory_qty: crate::risk::RiskFixed6::from_units(
                record.remaining_total_inventory_units_6dp,
                "worker remaining total inventory",
            )?,
            market_budgets: Default::default(),
        });
        Ok(())
    }

    pub(super) fn complete_risk_view(&mut self, records: &[MarketBudgetRecord]) -> Result<()> {
        let catalog = self.catalog.clone();
        let candidate = self.candidate_mut()?;
        let mut risk = candidate
            .pending_risk
            .take()
            .context("market budgets have no pending risk snapshot")?;
        for record in records {
            anyhow::ensure!(
                record.flags & MARKET_BUDGET_REQUIRED_FACTS == MARKET_BUDGET_REQUIRED_FACTS,
                "market budget is missing required fixed facts flags={:#x}",
                record.flags
            );
            let market = catalog
                .record(record.market_id)
                .context("market-budget handle is missing")?;
            let market_id = MarketId::new(B256::from(market.condition_id).to_string());
            risk.market_budgets.insert(
                market_id.clone(),
                MarketBudgetView {
                    market_id,
                    cap_equity_usdt: optional_risk_units_from_i64(
                        record.cap_equity_units_6dp,
                        record.flags,
                        MARKET_BUDGET_CAP_EQUITY_PRESENT,
                        "worker market cap equity",
                    )?,
                    pct_cap_notional: optional_risk_units_from_i64(
                        record.pct_cap_notional_units_6dp,
                        record.flags,
                        MARKET_BUDGET_PCT_CAP_PRESENT,
                        "worker market percentage cap",
                    )?,
                    effective_market_cap_notional: optional_risk_units_from_i64(
                        record.effective_cap_units_6dp,
                        record.flags,
                        MARKET_BUDGET_EFFECTIVE_CAP_PRESENT,
                        "worker effective market cap",
                    )?,
                    effective_market_exposure: risk_units_from_i64(
                        record.effective_exposure_units_6dp,
                        "worker effective market exposure",
                    )?,
                    remaining_market_headroom_usdt: risk_units_from_i64(
                        record.remaining_market_headroom_units_6dp,
                        "worker remaining market headroom",
                    )?,
                    binding_rule: match record.binding_rule {
                        0 => None,
                        1 => Some("max_window_cost".to_string()),
                        2 => Some("max_market_equity_pct".to_string()),
                        other => anyhow::bail!("unknown worker market binding rule={other}"),
                    },
                },
            );
        }
        candidate.risk = risk;
        Ok(())
    }
}

fn resolve_token(
    catalog: &MarketCatalog,
    token_handle: u32,
) -> Result<(&MarketCatalogRecord, String)> {
    for market in catalog.records() {
        if token_handle == market.up_token_handle {
            return Ok((market, U256::from_be_bytes(market.up_token).to_string()));
        }
        if token_handle == market.down_token_handle {
            return Ok((market, U256::from_be_bytes(market.down_token).to_string()));
        }
    }
    anyhow::bail!("worker token handle is absent from catalog handle={token_handle}")
}

fn side(value: u8) -> Result<Side> {
    match value {
        1 => Ok(Side::Buy),
        2 => Ok(Side::Sell),
        other => anyhow::bail!("invalid worker order side={other}"),
    }
}

fn close_reason(value: u8) -> Result<OrderCloseReason> {
    match value {
        1 => Ok(OrderCloseReason::PairedFirstManagedSell),
        2 => Ok(OrderCloseReason::PairedPostProceedsSell),
        other => anyhow::bail!("invalid worker close reason={other}"),
    }
}

fn close_reason_optional(value: u8) -> Result<Option<OrderCloseReason>> {
    match value {
        0 => Ok(None),
        value => close_reason(value).map(Some),
    }
}

fn time_in_force(value: u8, active_until_ms: u64) -> Result<TimeInForce> {
    match value {
        1 => Ok(TimeInForce::Gtc),
        2 if active_until_ms > 0 => Ok(TimeInForce::Gtd {
            expiration_ms: active_until_ms,
        }),
        2 => anyhow::bail!("worker GTD order has zero expiration"),
        3 => Ok(TimeInForce::Fok),
        4 => Ok(TimeInForce::Fak),
        other => anyhow::bail!("invalid worker time in force={other}"),
    }
}

fn units_u128_to_f64(value: u128) -> Result<f64> {
    anyhow::ensure!(
        value <= MAX_EXACT_F64_INTEGER,
        "fixed-6dp integer exceeds exact strategy-view transport value={value}"
    );
    let converted = value as f64 / 1_000_000.0;
    anyhow::ensure!(
        converted.is_finite(),
        "fixed-6dp quantity exceeds f64 view range"
    );
    Ok(converted)
}

fn units_i64_to_f64(value: i64) -> Result<f64> {
    anyhow::ensure!(
        value.unsigned_abs() <= MAX_EXACT_F64_INTEGER as u64,
        "fixed-6dp integer exceeds exact strategy-view transport value={value}"
    );
    Ok(value as f64 / 1_000_000.0)
}

fn risk_units_from_i64(value: i64, owner: &str) -> Result<crate::risk::RiskFixed6> {
    anyhow::ensure!(value >= 0, "{owner} is negative");
    crate::risk::RiskFixed6::from_units(value as u128, owner)
}

fn optional_risk_units_from_i64(
    value: i64,
    flags: u16,
    presence_flag: u16,
    owner: &str,
) -> Result<Option<crate::risk::RiskFixed6>> {
    if flags & presence_flag != 0 {
        return risk_units_from_i64(value, owner).map(Some);
    }
    anyhow::ensure!(value == 0, "absent {owner} carries a nonzero value");
    Ok(None)
}

fn price_units_to_f64(value: u64) -> f64 {
    value as f64 / 100_000_000.0
}

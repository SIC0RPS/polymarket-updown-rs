use anyhow::Result;

use super::{CohortIdentityRecord, MarketCatalog, MarketCatalogRecord};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PositionDeltaRecord {
    pub token_id: u32,
    pub operation: u8,
    pub flags: u8,
    pub central_revision: u64,
    pub qty_units_6dp: u128,
    pub sellable_qty_units_6dp: u128,
    pub avg_px_units_8dp: u64,
    pub cost_basis_units_6dp: i64,
    pub opened_at_ms: u64,
    pub last_update_ms: u64,
    pub end_time_ms: u64,
}

impl PositionDeltaRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.token_id != 0, "position token identity is zero");
        anyhow::ensure!(
            matches!(self.operation, 1 | 2),
            "invalid position delta operation"
        );
        anyhow::ensure!(
            self.flags & !15 == 0,
            "position delta contains unknown flags"
        );
        anyhow::ensure!(
            (self.flags & 12 != 0) == (self.end_time_ms > 0),
            "position expiry presence mismatch"
        );
        anyhow::ensure!(
            self.avg_px_units_8dp <= 100_000_000,
            "position average price exceeds probability range"
        );
        anyhow::ensure!(
            self.cost_basis_units_6dp >= 0,
            "position cost basis is negative"
        );
        anyhow::ensure!(
            self.sellable_qty_units_6dp <= self.qty_units_6dp,
            "position sellable quantity exceeds owned quantity"
        );
        if self.operation == 1 {
            anyhow::ensure!(self.qty_units_6dp > 0, "position upsert quantity is zero");
        } else {
            anyhow::ensure!(
                self.flags == 0
                    && self.qty_units_6dp == 0
                    && self.sellable_qty_units_6dp == 0
                    && self.avg_px_units_8dp == 0
                    && self.cost_basis_units_6dp == 0
                    && self.opened_at_ms == 0
                    && self.last_update_ms == 0
                    && self.end_time_ms == 0,
                "position removal carries mutable state"
            );
        }
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        catalog_token(catalog, self.token_id, "position")?;
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActiveLifecycleDeltaRecord {
    pub operation: u8,
    pub side: u8,
    pub close_reason: u8,
    pub flags: u8,
    pub time_in_force: u8,
    pub market_id: u32,
    pub token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub order_handle: u64,
    pub remaining_qty_units_6dp: u128,
    pub instruction_qty_units_6dp: u128,
    pub limit_px_units_8dp: u64,
    pub execution_evidence_revision: u64,
    pub execution_intent_id: u64,
    pub matched_execution_qty_units_6dp: u128,
    pub matched_execution_gross_units_6dp: u128,
    pub matched_execution_fee_units_6dp: u128,
    pub matched_execution_portfolio_revision: u64,
    pub active_until_ms: u64,
    pub time_in_force_expiration_ms: u64,
    pub central_revision: u64,
}

impl ActiveLifecycleDeltaRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            matches!(self.operation, 1 | 2),
            "invalid active-lifecycle delta operation"
        );
        anyhow::ensure!(self.side == 2, "CTF CLOB buy lifecycle is forbidden");
        anyhow::ensure!(
            matches!(self.close_reason, 1 | 2),
            "invalid active managed-sell close reason"
        );
        anyhow::ensure!(
            self.market_id != 0 && self.token_id != 0,
            "active-lifecycle interned identity is zero"
        );
        self.cohort_id.required_str()?;
        anyhow::ensure!(
            self.flags & !31 == 0,
            "active-lifecycle delta contains unknown flags"
        );
        anyhow::ensure!(
            (self.flags & 1 != 0) == (self.order_handle > 0),
            "active-lifecycle order identity presence mismatch"
        );
        anyhow::ensure!(
            (self.flags & 2 != 0) == (self.active_until_ms > 0),
            "active-lifecycle expiry presence mismatch"
        );
        if self.operation == 1 {
            anyhow::ensure!(
                matches!(self.time_in_force, 1..=4),
                "active-lifecycle time in force is invalid"
            );
            anyhow::ensure!(
                (self.time_in_force == 2) == (self.time_in_force_expiration_ms > 0),
                "active-lifecycle GTD expiration mismatch"
            );
            anyhow::ensure!(
                self.flags & 8 == 0 || matches!(self.time_in_force, 1 | 2),
                "active-lifecycle post-only instruction is immediate"
            );
            anyhow::ensure!(
                self.remaining_qty_units_6dp > 1,
                "active-lifecycle remaining quantity is not strategy-actionable"
            );
            anyhow::ensure!(
                self.instruction_qty_units_6dp > 1,
                "active-lifecycle instruction quantity is not strategy-actionable"
            );
            anyhow::ensure!(
                self.remaining_qty_units_6dp <= self.instruction_qty_units_6dp,
                "active-lifecycle remaining quantity exceeds instruction quantity"
            );
            anyhow::ensure!(
                (1..=100_000_000).contains(&self.limit_px_units_8dp),
                "active-lifecycle limit price is invalid"
            );
            anyhow::ensure!(
                self.execution_evidence_revision > 0,
                "active-lifecycle execution evidence revision is zero"
            );
            anyhow::ensure!(
                (self.matched_execution_qty_units_6dp == 0)
                    == (self.matched_execution_gross_units_6dp == 0),
                "active-lifecycle matched quantity and gross presence differ"
            );
            anyhow::ensure!(
                self.matched_execution_qty_units_6dp == 0
                    || self.matched_execution_portfolio_revision > 0,
                "active-lifecycle matched execution has no portfolio revision"
            );
            anyhow::ensure!(
                self.flags & 16 != 0 || self.matched_execution_fee_units_6dp == 0,
                "active-lifecycle matched fee presence mismatch"
            );
            if self.matched_execution_qty_units_6dp == 0 {
                anyhow::ensure!(
                    self.flags & 16 == 0 && self.matched_execution_fee_units_6dp == 0,
                    "empty active-lifecycle match carries fee economics"
                );
            }
            if self.flags & 4 != 0 {
                anyhow::ensure!(
                    self.order_handle == 0 && self.active_until_ms > 0,
                    "terminal no-fill lifecycle carries pending order or no release deadline"
                );
            }
        } else {
            anyhow::ensure!(
                self.flags == 0
                    && self.time_in_force == 0
                    && self.order_handle == 0
                    && self.remaining_qty_units_6dp == 0
                    && self.instruction_qty_units_6dp == 0
                    && self.limit_px_units_8dp == 0
                    && self.execution_evidence_revision == 0
                    && self.execution_intent_id == 0
                    && self.matched_execution_qty_units_6dp == 0
                    && self.matched_execution_gross_units_6dp == 0
                    && self.matched_execution_fee_units_6dp == 0
                    && self.matched_execution_portfolio_revision == 0
                    && self.active_until_ms == 0
                    && self.time_in_force_expiration_ms == 0,
                "active-lifecycle removal carries mutable state"
            );
        }
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog_market(catalog, self.market_id)?;
        ensure_market_token(market, self.token_id, "active lifecycle")?;
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OpenOrderDeltaRecord {
    pub order_handle: u64,
    pub intent_id: u64,
    pub operation: u8,
    pub side: u8,
    pub time_in_force: u8,
    pub close_reason: u8,
    pub status: u8,
    pub flags: u8,
    pub quote_level: u8,
    pub market_id: u32,
    pub token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub original_qty_units_6dp: u128,
    pub matched_qty_units_6dp: u128,
    pub limit_px_units_8dp: u64,
    pub placed_at_ms: u64,
    pub last_update_ms: u64,
    pub active_until_ms: u64,
    pub central_revision: u64,
}

impl OpenOrderDeltaRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.order_handle != 0, "open-order handle is zero");
        anyhow::ensure!(self.intent_id != 0, "open-order intent identity is zero");
        anyhow::ensure!(
            matches!(self.operation, 1 | 2),
            "invalid open-order delta operation"
        );
        anyhow::ensure!(matches!(self.side, 1 | 2), "invalid open-order side");
        anyhow::ensure!(
            matches!(self.time_in_force, 1..=4),
            "invalid open-order time in force"
        );
        anyhow::ensure!(
            self.market_id != 0 && self.token_id != 0,
            "open-order interned identity is zero"
        );
        self.cohort_id.required_str()?;
        anyhow::ensure!(
            self.flags & !3 == 0,
            "open-order delta contains unknown flags"
        );
        anyhow::ensure!(
            self.matched_qty_units_6dp <= self.original_qty_units_6dp,
            "open-order matched quantity exceeds original quantity"
        );
        anyhow::ensure!(
            self.limit_px_units_8dp <= 100_000_000,
            "open-order price exceeds probability range"
        );
        anyhow::ensure!(
            self.flags & 1 == 0 || matches!(self.time_in_force, 1 | 2),
            "post-only open order requires GTC or GTD"
        );
        anyhow::ensure!(
            matches!((self.side, self.close_reason), (1, 0) | (2, 1 | 2)),
            "invalid open-order side and close reason"
        );
        if self.side == 1 {
            anyhow::ensure!(
                self.time_in_force == 2
                    && self.flags & 1 == 0
                    && self.placed_at_ms < self.active_until_ms
                    && self.original_qty_units_6dp > 0
                    && self.limit_px_units_8dp > 0
                    && self.limit_px_units_8dp < 100_000_000,
                "open BUY differs from native entry order contract"
            );
        }
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog_market(catalog, self.market_id)?;
        ensure_market_token(market, self.token_id, "open order")?;
        if self.side == 1 {
            let cohort = crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                polymarket_client_sdk::types::B256::from(market.condition_id),
                self.intent_id,
            )?;
            anyhow::ensure!(
                self.active_until_ms == market.window_end_ms
                    && self.cohort_id.required_str()? == cohort,
                "open BUY has foreign cohort or market expiry"
            );
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RiskBudget {
    pub source_sequence: u64,
    pub central_state_seq: u64,
    pub updated_at_ms: u64,
    pub flags: u16,
    pub freeze_level: u8,
    pub freeze_reason_code: u16,
    pub freeze_detail_hash: u64,
    pub max_order_qty_units_6dp: u128,
    pub available_buy_budget_units_6dp: i64,
    pub max_entry_notional_units_6dp: i64,
    pub max_inventory_total_units_6dp: u128,
    pub remaining_total_inventory_units_6dp: u128,
}

impl RiskBudget {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            self.central_state_seq > 0,
            "risk budget central-state sequence is zero"
        );
        anyhow::ensure!(self.flags & !3 == 0, "risk budget contains unknown flags");
        anyhow::ensure!(self.freeze_level <= 2, "invalid risk budget freeze level");
        anyhow::ensure!(
            self.available_buy_budget_units_6dp >= 0,
            "risk available buy budget is negative"
        );
        anyhow::ensure!(
            self.max_entry_notional_units_6dp >= 0,
            "risk max entry notional is negative"
        );
        anyhow::ensure!(
            self.remaining_total_inventory_units_6dp <= self.max_inventory_total_units_6dp,
            "risk remaining total inventory exceeds account limit"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MarketBudgetRecord {
    pub market_id: u32,
    pub flags: u16,
    pub binding_rule: u16,
    pub cap_equity_units_6dp: i64,
    pub pct_cap_notional_units_6dp: i64,
    pub effective_cap_units_6dp: i64,
    pub effective_exposure_units_6dp: i64,
    pub remaining_market_headroom_units_6dp: i64,
}

impl MarketBudgetRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.market_id != 0, "market-budget identity is zero");
        anyhow::ensure!(
            self.flags & !0x1f == 0,
            "market budget contains unknown flags"
        );
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        catalog_market(catalog, self.market_id)?;
        Ok(self)
    }
}

pub(super) fn catalog_market(catalog: &MarketCatalog, handle: u32) -> Result<&MarketCatalogRecord> {
    catalog.record(handle).ok_or_else(|| {
        anyhow::anyhow!(
            "symbol IPC market handle is absent from frozen catalog symbol={} handle={handle}",
            catalog.symbol()
        )
    })
}

pub(super) fn catalog_token<'a>(
    catalog: &'a MarketCatalog,
    handle: u32,
    owner: &str,
) -> Result<&'a MarketCatalogRecord> {
    catalog
        .records()
        .iter()
        .find(|record| record.up_token_handle == handle || record.down_token_handle == handle)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{owner} token handle is absent from frozen catalog symbol={} handle={handle}",
                catalog.symbol()
            )
        })
}

pub(super) fn ensure_market_token(
    market: &MarketCatalogRecord,
    token_handle: u32,
    owner: &str,
) -> Result<()> {
    anyhow::ensure!(
        token_handle == market.up_token_handle || token_handle == market.down_token_handle,
        "{owner} token handle is foreign to market market_handle={} token_handle={token_handle}",
        market.market_handle
    );
    Ok(())
}

pub(super) fn ensure_inventory_identity(
    market: &MarketCatalogRecord,
    condition_handle: u32,
    up_token_handle: u32,
    down_token_handle: u32,
    owner: &str,
) -> Result<()> {
    anyhow::ensure!(
        condition_handle == market.market_handle
            && up_token_handle == market.up_token_handle
            && down_token_handle == market.down_token_handle,
        "{owner} identity is foreign to frozen catalog market_handle={} condition_handle={} up_token_handle={} down_token_handle={}",
        market.market_handle,
        condition_handle,
        up_token_handle,
        down_token_handle
    );
    Ok(())
}

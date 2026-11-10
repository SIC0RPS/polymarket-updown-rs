use anyhow::Result;

use super::{
    catalog_market, ensure_inventory_identity, ensure_market_token, CohortIdentityRecord,
    MarketCatalog,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ManagedSellReceiptRecord {
    pub fill_handle: u64,
    pub receipt_identity_sha256: [u8; 32],
    pub order_handle: u64,
    pub intent_id: u64,
    pub market_id: u32,
    pub token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub close_reason: u8,
    pub status: u8,
    pub flags: u8,
    pub qty_units_6dp: u128,
    pub fill_px_units_8dp: u64,
    pub gross_proceeds_units_6dp: i128,
    pub fee_units_6dp: u128,
    pub confirmed_at_ms: u64,
}

impl ManagedSellReceiptRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            self.fill_handle != 0 && self.order_handle != 0 && self.intent_id != 0,
            "managed-sell receipt lineage identity is zero"
        );
        anyhow::ensure!(
            self.receipt_identity_sha256 != [0; 32],
            "managed-sell receipt identity digest is zero"
        );
        anyhow::ensure!(
            self.market_id != 0 && self.token_id != 0,
            "managed-sell receipt interned identity is zero"
        );
        self.cohort_id.required_str()?;
        anyhow::ensure!(
            matches!(self.close_reason, 1 | 2),
            "invalid managed-sell receipt close reason"
        );
        anyhow::ensure!(
            self.status == 1,
            "unconfirmed managed-sell receipt crossed confirmed-fact boundary"
        );
        anyhow::ensure!(
            self.confirmed_at_ms > 0,
            "managed-sell receipt confirmation time is zero"
        );
        anyhow::ensure!(
            self.flags & !3 == 0,
            "managed-sell receipt contains unknown flags"
        );
        anyhow::ensure!(
            self.qty_units_6dp > 0,
            "managed-sell receipt quantity is zero"
        );
        anyhow::ensure!(
            (1..=100_000_000).contains(&self.fill_px_units_8dp),
            "managed-sell fill price outside probability range"
        );
        anyhow::ensure!(
            self.gross_proceeds_units_6dp >= 0,
            "managed-sell gross proceeds are negative"
        );
        anyhow::ensure!(
            self.fee_units_6dp <= self.gross_proceeds_units_6dp as u128,
            "managed-sell fee exceeds gross proceeds"
        );
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog_market(catalog, self.market_id)?;
        ensure_market_token(market, self.token_id, "managed-sell receipt")?;
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum InventoryReceiptOperation {
    Split = 1,
    Merge = 2,
    Redeem = 3,
    Settlement = 4,
}

impl TryFrom<u8> for InventoryReceiptOperation {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Split),
            2 => Ok(Self::Merge),
            3 => Ok(Self::Redeem),
            4 => Ok(Self::Settlement),
            other => anyhow::bail!("invalid inventory receipt operation={other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InventoryReceiptRecord {
    pub receipt_handle: u64,
    pub transaction_handle: u64,
    pub settlement_identity_handle: u64,
    pub intent_id: u64,
    pub operation: InventoryReceiptOperation,
    pub status: u8,
    pub payout_side: u8,
    pub flags: u8,
    pub market_id: u32,
    pub condition_id: u32,
    pub up_token_id: u32,
    pub down_token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub qty_units_6dp: u128,
    pub collateral_debit_units_6dp: u128,
    pub collateral_receipt_units_6dp: i128,
    pub up_principal_units_6dp: u128,
    pub down_principal_units_6dp: u128,
    pub up_entry_fee_units_6dp: u128,
    pub down_entry_fee_units_6dp: u128,
    pub up_direct_cost_units_6dp: u128,
    pub down_direct_cost_units_6dp: u128,
    pub fee_units_6dp: u128,
    pub direct_cost_units_6dp: u128,
    pub confirmed_at_ms: u64,
}

impl InventoryReceiptRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            self.receipt_handle != 0,
            "inventory receipt identity is zero"
        );
        anyhow::ensure!(
            !matches!(
                self.operation,
                InventoryReceiptOperation::Split | InventoryReceiptOperation::Merge
            ) || self.intent_id != 0,
            "intent-owned inventory receipt intent identity is zero"
        );
        anyhow::ensure!(
            self.status == 1,
            "unconfirmed inventory receipt crossed confirmed-fact boundary"
        );
        anyhow::ensure!(
            self.confirmed_at_ms > 0,
            "inventory receipt confirmation time is zero"
        );
        anyhow::ensure!(
            self.flags & !3 == 0,
            "inventory receipt contains unknown flags"
        );
        anyhow::ensure!(
            self.market_id != 0
                && self.condition_id != 0
                && self.up_token_id != 0
                && self.down_token_id != 0,
            "inventory receipt interned identity is zero"
        );
        self.cohort_id.required_str()?;
        anyhow::ensure!(self.qty_units_6dp > 0, "inventory receipt quantity is zero");
        if self.operation != InventoryReceiptOperation::Split {
            anyhow::ensure!(
                self.up_principal_units_6dp == 0
                    && self.down_principal_units_6dp == 0
                    && self.up_entry_fee_units_6dp == 0
                    && self.down_entry_fee_units_6dp == 0
                    && self.up_direct_cost_units_6dp == 0
                    && self.down_direct_cost_units_6dp == 0,
                "non-split inventory receipt carries entry allocation"
            );
        }
        match self.operation {
            InventoryReceiptOperation::Split => {
                anyhow::ensure!(
                    self.transaction_handle != 0,
                    "split transaction identity is zero"
                );
                anyhow::ensure!(
                    self.settlement_identity_handle == 0,
                    "split carries terminal settlement identity"
                );
                anyhow::ensure!(self.payout_side == 0, "split carries payout side");
                anyhow::ensure!(
                    self.collateral_debit_units_6dp > 0,
                    "split principal debit is zero"
                );
                anyhow::ensure!(
                    self.collateral_receipt_units_6dp == 0,
                    "split carries collateral receipt"
                );
                anyhow::ensure!(
                    self.up_principal_units_6dp
                        .checked_add(self.down_principal_units_6dp)
                        == Some(self.collateral_debit_units_6dp),
                    "split token principal allocations differ from collateral debit"
                );
                anyhow::ensure!(
                    self.up_entry_fee_units_6dp
                        .checked_add(self.down_entry_fee_units_6dp)
                        == Some(self.fee_units_6dp),
                    "split token entry-fee allocations differ from receipt fee"
                );
                anyhow::ensure!(
                    self.up_direct_cost_units_6dp
                        .checked_add(self.down_direct_cost_units_6dp)
                        == Some(self.direct_cost_units_6dp),
                    "split token direct-cost allocations differ from receipt cost"
                );
            }
            InventoryReceiptOperation::Merge => {
                anyhow::ensure!(
                    self.transaction_handle != 0,
                    "merge transaction identity is zero"
                );
                anyhow::ensure!(
                    self.settlement_identity_handle == 0,
                    "merge carries terminal settlement identity"
                );
                anyhow::ensure!(self.payout_side == 0, "merge carries payout side");
                anyhow::ensure!(
                    self.collateral_debit_units_6dp == 0,
                    "merge carries collateral debit"
                );
                anyhow::ensure!(
                    self.collateral_receipt_units_6dp > 0,
                    "merge collateral receipt is unavailable"
                );
            }
            InventoryReceiptOperation::Redeem => {
                anyhow::ensure!(
                    self.transaction_handle != 0,
                    "redeem transaction identity is zero"
                );
                self.validate_terminal_receipt()?;
            }
            InventoryReceiptOperation::Settlement => self.validate_terminal_receipt()?,
        }
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog_market(catalog, self.market_id)?;
        ensure_inventory_identity(
            market,
            self.condition_id,
            self.up_token_id,
            self.down_token_id,
            "inventory receipt",
        )?;
        Ok(self)
    }

    fn validate_terminal_receipt(self) -> Result<()> {
        anyhow::ensure!(
            self.settlement_identity_handle != 0,
            "terminal settlement identity is zero"
        );
        anyhow::ensure!(
            matches!(self.payout_side, 1 | 2),
            "invalid terminal payout side"
        );
        anyhow::ensure!(
            self.collateral_debit_units_6dp == 0,
            "terminal receipt carries collateral debit"
        );
        anyhow::ensure!(
            self.collateral_receipt_units_6dp >= 0,
            "terminal collateral receipt is negative"
        );
        Ok(())
    }
}

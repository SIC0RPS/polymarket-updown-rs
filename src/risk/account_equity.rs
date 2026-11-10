use anyhow::Result;

use super::RiskFixed6;

#[derive(Clone)]
pub struct AccountEquitySnapshot {
    pub wallet_usdc_balance: Option<RiskFixed6>,
    pub wallet_matic_balance: Option<f64>,
    pub marked_positions_value: Option<RiskFixed6>,
    pub current_wallet_equity: Option<RiskFixed6>,
    pub trading_collateral_balance: Option<RiskFixed6>,
    pub source_ts_ms: u64,
}

impl AccountEquitySnapshot {
    pub fn from_units(
        wallet_usdc_balance: Option<u128>,
        wallet_matic_balance: Option<f64>,
        marked_positions_value: Option<u128>,
        trading_collateral_balance: Option<u128>,
        source_ts_ms: u64,
    ) -> Result<Self> {
        let wallet_usdc_balance = wallet_usdc_balance
            .map(|units| RiskFixed6::from_units(units, "wallet USDC.e balance"))
            .transpose()?;
        let wallet_matic_balance = finite_nonnegative(wallet_matic_balance);
        let marked_positions_value = marked_positions_value
            .map(|units| RiskFixed6::from_units(units, "marked positions value"))
            .transpose()?;
        let trading_collateral_balance = trading_collateral_balance
            .map(|units| RiskFixed6::from_units(units, "trading collateral balance"))
            .transpose()?;
        let trading_cash_balance = wallet_usdc_balance
            .zip(trading_collateral_balance)
            .map(|(usdc_e, pusd)| usdc_e.checked_add(pusd, "trading cash balance"))
            .transpose()?;
        let current_wallet_equity = trading_cash_balance
            .zip(marked_positions_value)
            .map(|(cash, positions)| cash.checked_add(positions, "current wallet equity"))
            .transpose()?;

        Ok(Self {
            wallet_usdc_balance,
            wallet_matic_balance,
            marked_positions_value,
            current_wallet_equity,
            trading_collateral_balance,
            source_ts_ms,
        })
    }
}

fn finite_nonnegative(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

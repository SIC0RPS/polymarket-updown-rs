use anyhow::{Context, Result};

use super::financial::FinancialMutation;
use super::{ConfirmedReceiptCashflow, LiveWalletAccounting, ReconciledWallet};

pub(super) fn project_wallet(
    reconciled: &ReconciledWallet,
    cashflows: &[ConfirmedReceiptCashflow],
) -> Result<LiveWalletAccounting> {
    let mut wallet = LiveWalletAccounting {
        reconciled_block: reconciled.block.clone(),
        reconciled_usdc_units_6dp: reconciled.usdc_units_6dp,
        reconciled_pusd_units_6dp: reconciled.pusd_units_6dp,
        projected_pusd_units_6dp: reconciled.pusd_units_6dp,
        source_ts_ms: reconciled.source_ts_ms,
    };
    for cashflow in cashflows {
        apply_cashflow(&mut wallet, cashflow)?;
    }
    Ok(wallet)
}

pub(super) fn apply_cashflow(
    wallet: &mut LiveWalletAccounting,
    cashflow: &ConfirmedReceiptCashflow,
) -> Result<()> {
    if !applies_after_reconciliation(wallet, cashflow) {
        return Ok(());
    }
    let projected = i128::try_from(wallet.projected_pusd_units_6dp)
        .context("projected pUSD balance exceeds signed range")?
        .checked_add(cashflow.net_pusd_units_6dp)
        .context("projected pUSD balance overflow")?;
    anyhow::ensure!(projected >= 0, "projected pUSD balance is negative");
    wallet.projected_pusd_units_6dp =
        u128::try_from(projected).context("projected pUSD balance exceeds unsigned range")?;
    Ok(())
}

pub(super) fn apply_cashflow_mutation(
    wallet: &mut LiveWalletAccounting,
    mutation: &FinancialMutation,
) -> Result<()> {
    let mut projected = i128::try_from(wallet.projected_pusd_units_6dp)
        .context("projected pUSD balance exceeds signed range")?;
    for cashflow in &mutation.removed_cashflows {
        if applies_after_reconciliation(wallet, cashflow) {
            projected = projected
                .checked_sub(cashflow.net_pusd_units_6dp)
                .context("projected pUSD cashflow withdrawal overflow")?;
        }
    }
    for cashflow in &mutation.added_cashflows {
        if applies_after_reconciliation(wallet, cashflow) {
            projected = projected
                .checked_add(cashflow.net_pusd_units_6dp)
                .context("projected pUSD cashflow admission overflow")?;
        }
    }
    anyhow::ensure!(projected >= 0, "projected pUSD balance is negative");
    wallet.projected_pusd_units_6dp =
        u128::try_from(projected).context("projected pUSD balance exceeds unsigned range")?;
    Ok(())
}

fn applies_after_reconciliation(
    wallet: &LiveWalletAccounting,
    cashflow: &ConfirmedReceiptCashflow,
) -> bool {
    if wallet.reconciled_block.hash.is_none() {
        return cashflow.block.hash.is_none();
    }
    cashflow.block.number > wallet.reconciled_block.number
}

pub(super) fn project_cashflow_mutation(
    wallet: &LiveWalletAccounting,
    mutation: &FinancialMutation,
) -> Result<LiveWalletAccounting> {
    let mut next = wallet.clone();
    apply_cashflow_mutation(&mut next, mutation)?;
    Ok(next)
}

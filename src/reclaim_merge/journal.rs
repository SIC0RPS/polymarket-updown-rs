use crate::ctf_inventory::CtfInventoryRuntime;
use crate::engine_trade_journal::{
    append_confirmed_ctf_terminal_record_once, build_confirmed_ctf_terminal_record,
    ConfirmedCtfTerminalClearance,
};
use crate::execution::ConditionPayoutVector;
use crate::portfolio::{PortfolioState, SettlementInventoryClearance};
use crate::trade_session_ledger::{DirectEoaOperation, SettlementCollateralReceipt};
use anyhow::{Context, Result};

fn append_terminal_record(
    ctf_inventory: &CtfInventoryRuntime,
    portfolio: &PortfolioState,
    cohort_id: &str,
    receipt: &SettlementCollateralReceipt,
    payout: Option<&ConditionPayoutVector>,
    up_qty_units_6dp: u128,
    down_qty_units_6dp: u128,
    up_outcome_index: Option<u8>,
    down_outcome_index: Option<u8>,
    confirmed_at_ms: u64,
) -> Result<()> {
    let (market_id, condition_id, up_token, down_token, acquired_up, acquired_down) =
        confirmed_entry_identity(ctf_inventory, portfolio, cohort_id)?;
    anyhow::ensure!(
        condition_id.to_string() == receipt.condition_id,
        "terminal current-session entry condition does not match finalized receipt"
    );
    let current_up_units_6dp =
        portfolio.confirmed_ctf_cohort_qty_units_6dp(&up_token, cohort_id)?;
    let current_down_units_6dp =
        portfolio.confirmed_ctf_cohort_qty_units_6dp(&down_token, cohort_id)?;
    anyhow::ensure!(
        current_up_units_6dp <= acquired_up && current_down_units_6dp <= acquired_down,
        "terminal current-session portfolio quantity exceeds confirmed acquisition"
    );
    let collateral_receipt_units_6dp = receipt.confirmed_amount_units_6dp()?;
    match receipt.operation {
        DirectEoaOperation::Merge => {
            anyhow::ensure!(
                payout.is_none(),
                "merge terminal accounting carries payout evidence"
            );
            anyhow::ensure!(
                up_qty_units_6dp > 0
                    && up_qty_units_6dp == down_qty_units_6dp
                    && up_qty_units_6dp == collateral_receipt_units_6dp
                    && up_qty_units_6dp <= current_up_units_6dp
                    && down_qty_units_6dp <= current_down_units_6dp,
                "merge exact clearances do not match current portfolio and finalized collateral receipt"
            );
        }
        DirectEoaOperation::Redeem => {
            let payout = payout.context("redeem terminal accounting has no finalized payout")?;
            anyhow::ensure!(
                payout.condition_id.to_string() == receipt.condition_id,
                "redeem payout/collateral condition mismatch"
            );
            anyhow::ensure!(
                up_qty_units_6dp == current_up_units_6dp
                    && down_qty_units_6dp == current_down_units_6dp,
                "redeem exact clearances do not consume the current cohort quantities"
            );
        }
        DirectEoaOperation::Approval | DirectEoaOperation::Split => {
            anyhow::bail!("terminal receipt operation is not merge or redeem")
        }
    }
    let clearance = match receipt.operation {
        DirectEoaOperation::Merge => ConfirmedCtfTerminalClearance::confirmed_merge(
            market_id,
            receipt.condition_id.clone(),
            cohort_id.to_string(),
            up_token,
            down_token,
            up_qty_units_6dp,
            down_qty_units_6dp,
            confirmed_at_ms,
        ),
        DirectEoaOperation::Redeem => ConfirmedCtfTerminalClearance::confirmed_redeem(
            market_id,
            receipt.condition_id.clone(),
            cohort_id.to_string(),
            up_token,
            down_token,
            up_qty_units_6dp,
            down_qty_units_6dp,
            up_outcome_index,
            down_outcome_index,
            confirmed_at_ms,
            payout.expect("redeem payout checked").clone(),
        ),
        DirectEoaOperation::Approval | DirectEoaOperation::Split => {
            unreachable!("terminal operation checked before clearance construction")
        }
    };
    let record = build_confirmed_ctf_terminal_record(&receipt.run_id, &clearance, receipt)?
        .context("terminal accounting record is unavailable")?;
    append_confirmed_ctf_terminal_record_once(record)
}

pub(super) fn append_portfolio_close_receipts_to_journal(
    clearances: &[SettlementInventoryClearance],
    receipt: &SettlementCollateralReceipt,
    payout: Option<&ConditionPayoutVector>,
    portfolio: &PortfolioState,
    ctf_inventory: &CtfInventoryRuntime,
) -> Result<()> {
    anyhow::ensure!(!clearances.is_empty(), "terminal clearance set is empty");
    let mut cohorts = clearances
        .iter()
        .map(|clearance| {
            clearance
                .engine_trade_id
                .as_deref()
                .map(str::trim)
                .filter(|cohort| !cohort.is_empty())
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("terminal clearance has no exact cohort identity"))
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    anyhow::ensure!(
        cohorts.len() == 1,
        "terminal collateral receipt spans multiple cohorts"
    );
    let cohort_id = cohorts.pop_first().expect("one terminal cohort checked");
    let (market_id, _, up_token, down_token, _, _) =
        confirmed_entry_identity(ctf_inventory, portfolio, &cohort_id)?;
    let mut confirmed_at_ms = 0u64;
    let mut up_qty_units_6dp = 0u128;
    let mut down_qty_units_6dp = 0u128;
    let mut up_outcome_index = None;
    let mut down_outcome_index = None;
    for clearance in clearances {
        anyhow::ensure!(
            clearance.market_id == market_id
                && clearance.settlement_tx_hash.as_deref() == Some(receipt.tx_hash.as_str()),
            "terminal clearance identity does not match finalized receipt"
        );
        let (total, outcome_index) = if clearance.token == up_token {
            (&mut up_qty_units_6dp, &mut up_outcome_index)
        } else if clearance.token == down_token {
            (&mut down_qty_units_6dp, &mut down_outcome_index)
        } else {
            anyhow::bail!("terminal clearance token is not owned by its current-session cohort")
        };
        if let Some(existing) = *outcome_index {
            anyhow::ensure!(
                clearance.outcome_index == Some(existing),
                "terminal clearance token has conflicting outcome indices"
            );
        } else {
            *outcome_index = clearance.outcome_index;
        }
        *total = total
            .checked_add(clearance.qty_units_6dp)
            .context("terminal exact clearance quantity overflow")?;
        confirmed_at_ms = confirmed_at_ms.max(clearance.closed_at_ms);
    }
    append_terminal_record(
        ctf_inventory,
        portfolio,
        &cohort_id,
        receipt,
        payout,
        up_qty_units_6dp,
        down_qty_units_6dp,
        up_outcome_index,
        down_outcome_index,
        confirmed_at_ms,
    )
}

fn confirmed_entry_identity(
    ctf_inventory: &CtfInventoryRuntime,
    portfolio: &PortfolioState,
    cohort_id: &str,
) -> Result<(
    crate::polymarket::identity::MarketId,
    polymarket_client_sdk::types::B256,
    String,
    String,
    u128,
    u128,
)> {
    if let Some(receipt) = portfolio
        .confirmed_direct_buy_receipts()
        .find(|receipt| receipt.cohort_id == cohort_id)
    {
        let authorization = receipt.authorization;
        let acquired = portfolio.confirmed_direct_buy_qty_units_6dp(cohort_id);
        return Ok((
            receipt.market_id.clone(),
            authorization.condition_id,
            authorization.up_token.to_string(),
            authorization.down_token.to_string(),
            if authorization.selected_up {
                acquired
            } else {
                0
            },
            if authorization.selected_up {
                0
            } else {
                acquired
            },
        ));
    }
    let (market_id, condition_id, up_token, down_token, amount) =
        ctf_inventory.confirmed_split_identity_for_cohort(cohort_id)?;
    Ok((
        market_id,
        condition_id,
        up_token,
        down_token,
        amount,
        amount,
    ))
}

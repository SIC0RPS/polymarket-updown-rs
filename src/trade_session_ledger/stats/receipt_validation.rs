use std::collections::BTreeMap;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use super::StampedFact;
use crate::trade_session_ledger::{
    ConfirmedCtfTerminalFact, CtfTerminalOperation, CtfTerminalSettlementEvidence,
    DirectEoaOperation, FinalizedConditionResolutionReceipt, SettlementCollateralReceipt,
};

pub(super) fn validate_terminal_receipts(
    stamped: &StampedFact,
    operation: CtfTerminalOperation,
    terminal: &ConfirmedCtfTerminalFact,
    resolution_by_condition: &BTreeMap<String, &FinalizedConditionResolutionReceipt>,
    settlement_by_tx: &BTreeMap<String, &SettlementCollateralReceipt>,
) -> Result<()> {
    if let CtfTerminalSettlementEvidence::SimulatedMergeOperation { operation_id } =
        &terminal.settlement_evidence
    {
        anyhow::ensure!(
            operation == CtfTerminalOperation::Merge
                && operation_id == terminal.terminal_operation_identity()
                && terminal.up_terminal_qty_units_6dp == terminal.down_terminal_qty_units_6dp
                && i128::try_from(terminal.up_terminal_qty_units_6dp).ok()
                    == Some(terminal.collateral_proceeds_units_6dp),
            "simulated merge accounting does not equal its exact operation"
        );
        return Ok(());
    }
    if let CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
        operation_id,
        condition_id,
        up_token,
        down_token,
        winning_token,
        resolution_observed_at_ms,
        raw_response_sha256,
    } = &terminal.settlement_evidence
    {
        let expected_up = if winning_token == up_token {
            terminal.up_terminal_qty_units_6dp
        } else {
            0
        };
        let expected_down = if winning_token == down_token {
            terminal.down_terminal_qty_units_6dp
        } else {
            0
        };
        anyhow::ensure!(
            operation == CtfTerminalOperation::Redeem
                && operation_id == terminal.terminal_operation_identity()
                && condition_id == &terminal.condition_id
                && up_token == &terminal.up_token
                && down_token == &terminal.down_token
                && (winning_token == up_token || winning_token == down_token)
                && *resolution_observed_at_ms <= stamped.ts_ms
                && raw_response_sha256.len() == 64
                && terminal.up_collateral_proceeds_units_6dp == Some(expected_up)
                && terminal.down_collateral_proceeds_units_6dp == Some(expected_down)
                && expected_up
                    .checked_add(expected_down)
                    .and_then(|value| i128::try_from(value).ok())
                    == Some(terminal.collateral_proceeds_units_6dp),
            "simulated redemption accounting does not equal its captured binary resolution"
        );
        return Ok(());
    }
    let (receipt_id, transaction_id) = terminal
        .confirmed_receipt_identity()
        .context("terminal accounting has no confirmed collateral receipt identity")?;
    let receipt = settlement_by_tx
        .get(transaction_id)
        .context("terminal accounting has no finalized collateral receipt")?;
    let collateral_units = receipt.confirmed_amount_units_6dp()?;
    anyhow::ensure!(
        receipt.accounting_receipt_id() == receipt_id
            && receipt.condition_id == terminal.condition_id
            && receipt.ts_ms <= stamped.ts_ms
            && i128::try_from(collateral_units).ok()
                == Some(terminal.collateral_proceeds_units_6dp),
        "terminal accounting does not equal finalized collateral receipt"
    );
    match (operation, receipt.operation) {
        (CtfTerminalOperation::Merge, DirectEoaOperation::Merge) => Ok(()),
        (CtfTerminalOperation::Redeem, DirectEoaOperation::Redeem) => {
            let resolution = resolution_by_condition
                .get(&terminal.condition_id)
                .context("redeem accounting has no finalized resolution")?;
            anyhow::ensure!(
                resolution.observed_at_ms <= stamped.ts_ms,
                "redeem resolution follows terminal accounting"
            );
            let proceeds = |qty: u128, index: Option<u8>| -> Result<u128> {
                if qty == 0 {
                    return Ok(0);
                }
                let numerator = resolution
                    .numerator(usize::from(index.context("redeem outcome index missing")?))
                    .context("redeem outcome index outside payout vector")?;
                u128::try_from(
                    U256::from(qty)
                        .checked_mul(U256::from(numerator))
                        .context("redeem payout multiplication overflow")?
                        / U256::from(resolution.denominator),
                )
                .context("redeem token payout exceeds accounting range")
            };
            anyhow::ensure!(
                terminal.up_collateral_proceeds_units_6dp
                    == Some(proceeds(
                        terminal.up_terminal_qty_units_6dp,
                        terminal.up_outcome_index,
                    )?)
                    && terminal.down_collateral_proceeds_units_6dp
                        == Some(proceeds(
                            terminal.down_terminal_qty_units_6dp,
                            terminal.down_outcome_index,
                        )?),
                "redeem per-token proceeds do not match finalized payout"
            );
            Ok(())
        }
        _ => anyhow::bail!("terminal operation does not match finalized collateral receipt"),
    }
}

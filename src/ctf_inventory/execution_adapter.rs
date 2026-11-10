use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use crate::execution::{CtfSettlementConfirmation, ExecutionClient, SplitReceiptStatus};
use crate::portfolio::ExpectedCtfPosition;
use crate::strategy::{InventoryIntent, InventoryOperation};
use crate::trade_session_ledger::ConfirmedCtfSplitFact;
use crate::trade_session_ledger::DirectEoaSplitIdentity;

use super::CtfInventoryReceipt;

pub async fn submit_inventory_operation(
    exec: &ExecutionClient,
    underlying: &str,
    intent: &InventoryIntent,
    entry_journal: &crate::engine_trade_journal::CtfSplitEntryJournal,
) -> Result<CtfInventoryReceipt> {
    let amount = U256::from(intent.amount_units_6dp);
    match intent.operation {
        InventoryOperation::SplitCompleteSet => {
            let split_identity = DirectEoaSplitIdentity {
                market_id: intent.market_id.to_string(),
                gamma_market_id: intent
                    .gamma_market_id
                    .clone()
                    .context("split submission gamma market identity is missing")?,
                condition_id: intent.condition_id.to_string(),
                underlying: underlying.trim().to_ascii_uppercase(),
                up_token: intent.up_token.clone(),
                down_token: intent.down_token.clone(),
                amount_units_6dp: intent.amount_units_6dp,
            };
            split_identity.validate()?;
            split_identity.validate_entry_journal(entry_journal)?;
            let confirmation = exec
                .split_binary_condition(&split_identity, entry_journal)
                .await
                .context("split_binary_condition failed")?
                .context("split_binary_condition returned no confirmation")?;
            Ok(split_receipt(&confirmation.tx_hash, confirmation.receipt))
        }
        InventoryOperation::MergeCompleteSet => {
            let confirmation = exec
                .merge_binary_conditions(&[(intent.condition_id, amount)])
                .await
                .context("merge_binary_conditions failed")?
                .context("merge_binary_conditions returned no confirmation")?;
            Ok(match confirmation {
                CtfSettlementConfirmation::Confirmed { receipt, .. } => {
                    CtfInventoryReceipt::Confirmed {
                        tx_hash: receipt.tx_hash,
                    }
                }
                CtfSettlementConfirmation::ReconciliationRequired { tx_hash, error } => {
                    CtfInventoryReceipt::ReconciliationRequired { tx_hash, error }
                }
            })
        }
    }
}

pub fn split_receipt(tx_hash: &str, receipt: SplitReceiptStatus) -> CtfInventoryReceipt {
    match receipt {
        SplitReceiptStatus::Pending => CtfInventoryReceipt::Pending {
            tx_hash: tx_hash.to_string(),
        },
        SplitReceiptStatus::Confirmed => CtfInventoryReceipt::Confirmed {
            tx_hash: tx_hash.to_string(),
        },
        SplitReceiptStatus::Reverted => CtfInventoryReceipt::Reverted {
            tx_hash: tx_hash.to_string(),
        },
        SplitReceiptStatus::ReconciliationRequired { error } => {
            CtfInventoryReceipt::ReconciliationRequired {
                tx_hash: tx_hash.to_string(),
                error,
            }
        }
    }
}

pub fn expected_ctf_positions_for_confirmed_split(
    split: &ConfirmedCtfSplitFact,
    materialized_ts_ms: u64,
) -> Result<[ExpectedCtfPosition; 2]> {
    crate::trade_session_ledger::CanonicalCtfEvent::SplitConfirmed(split.clone()).validate()?;
    anyhow::ensure!(
        materialized_ts_ms > 0,
        "confirmed split materialization timestamp is missing"
    );
    let up_weight_units_8dp = split
        .up_cost_basis_weight_units_8dp
        .context("confirmed split is missing UP cost-basis weight")?;
    let down_weight_units_8dp = split
        .down_cost_basis_weight_units_8dp
        .context("confirmed split is missing DOWN cost-basis weight")?;
    let (up_principal_units_6dp, down_principal_units_6dp) = crate::ctf_accounting::allocate_units(
        split.collateral_debit_units_6dp,
        up_weight_units_8dp,
        down_weight_units_8dp,
    )?;
    let (up_entry_fee_units_6dp, down_entry_fee_units_6dp) = crate::ctf_accounting::allocate_units(
        split.fee_units_6dp,
        up_weight_units_8dp,
        down_weight_units_8dp,
    )?;
    let (up_direct_cost_units_6dp, down_direct_cost_units_6dp) =
        crate::ctf_accounting::allocate_units(
            split.approved_direct_cost_units_6dp,
            up_weight_units_8dp,
            down_weight_units_8dp,
        )?;
    let confirmed_entry_outlay_units_6dp = split
        .collateral_debit_units_6dp
        .checked_add(split.fee_units_6dp)
        .and_then(|value| value.checked_add(split.approved_direct_cost_units_6dp))
        .context("confirmed split entry outlay overflow")?;
    Ok([
        ExpectedCtfPosition {
            token: split.up_token.clone(),
            market_id: split.market_id.clone(),
            end_time_ms: None,
            confirmed_entry_outlay_units_6dp: Some(confirmed_entry_outlay_units_6dp),
            confirmed_materialized_ts_ms: Some(materialized_ts_ms),
            confirmed_token_created_qty_units_6dp: Some(split.up_qty_units_6dp),
            confirmed_token_principal_units_6dp: Some(up_principal_units_6dp),
            confirmed_token_entry_fee_units_6dp: Some(up_entry_fee_units_6dp),
            confirmed_token_approved_direct_cost_units_6dp: Some(up_direct_cost_units_6dp),
            engine_trade_id: Some(split.cohort_id.clone()),
            confirmed_entry_tx_hash: Some(split.transaction_id.clone()),
            require_positive_balance: true,
            min_qty_units_6dp: Some(split.collateral_debit_units_6dp),
            portfolio_actionable: true,
        },
        ExpectedCtfPosition {
            token: split.down_token.clone(),
            market_id: split.market_id.clone(),
            end_time_ms: None,
            confirmed_entry_outlay_units_6dp: Some(confirmed_entry_outlay_units_6dp),
            confirmed_materialized_ts_ms: Some(materialized_ts_ms),
            confirmed_token_created_qty_units_6dp: Some(split.down_qty_units_6dp),
            confirmed_token_principal_units_6dp: Some(down_principal_units_6dp),
            confirmed_token_entry_fee_units_6dp: Some(down_entry_fee_units_6dp),
            confirmed_token_approved_direct_cost_units_6dp: Some(down_direct_cost_units_6dp),
            engine_trade_id: Some(split.cohort_id.clone()),
            confirmed_entry_tx_hash: Some(split.transaction_id.clone()),
            require_positive_balance: true,
            min_qty_units_6dp: Some(split.collateral_debit_units_6dp),
            portfolio_actionable: true,
        },
    ])
}

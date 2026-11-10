use anyhow::{Context, Result};

use super::{
    DirectEoaGasReceipt, DirectEoaOperation, DirectEoaSubmissionReceipt, SessionReceipt,
    SettlementCollateralOutcome, SettlementCollateralReceipt,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ConfirmedSettlementReceiptLineage {
    pub submission: DirectEoaSubmissionReceipt,
    pub gas: DirectEoaGasReceipt,
    pub collateral: SettlementCollateralReceipt,
}

pub fn load_confirmed_settlement_collateral_receipt_for_run(
    run_id: &str,
    tx_hash: &str,
) -> Result<Option<SettlementCollateralReceipt>> {
    let receipts = super::load_session_receipts_for_run(run_id)?;
    validate_confirmed_settlement_collateral_receipt(&receipts, run_id, tx_hash)
}

pub fn validate_confirmed_settlement_collateral_receipt(
    receipts: &[SessionReceipt],
    run_id: &str,
    tx_hash: &str,
) -> Result<Option<SettlementCollateralReceipt>> {
    Ok(
        validate_confirmed_settlement_receipt_lineage(receipts, run_id, tx_hash)?
            .map(|lineage| lineage.collateral),
    )
}

pub fn validate_confirmed_settlement_receipt_lineage(
    receipts: &[SessionReceipt],
    run_id: &str,
    tx_hash: &str,
) -> Result<Option<ConfirmedSettlementReceiptLineage>> {
    let tx_hash = tx_hash.trim();
    anyhow::ensure!(
        !tx_hash.is_empty(),
        "settlement transaction identity is empty"
    );
    let settlement_identity = format!("settlement_collateral:{tx_hash}");
    let submission_identity = format!("direct_eoa_submission:{tx_hash}");
    let gas_identity = format!("direct_eoa_gas:{tx_hash}");
    let mut confirmed = None;
    let mut submission = None;
    let mut gas = None;
    for receipt in receipts {
        match receipt {
            SessionReceipt::SettlementCollateral(receipt) if receipt.tx_hash == tx_hash => {
                SessionReceipt::SettlementCollateral(receipt.clone()).validate()?;
                anyhow::ensure!(
                    receipt.run_id == run_id,
                    "settlement collateral receipt run mismatch tx_hash={tx_hash}"
                );
                anyhow::ensure!(
                    matches!(
                        &receipt.outcome,
                        SettlementCollateralOutcome::Confirmed { .. }
                    ),
                    "settlement collateral receipt requires reconciliation tx_hash={tx_hash}"
                );
                anyhow::ensure!(
                    confirmed
                        .as_ref()
                        .is_none_or(|existing| existing == receipt),
                    "conflicting settlement collateral receipts tx_hash={tx_hash}"
                );
                confirmed = Some(receipt.clone());
            }
            SessionReceipt::DirectEoaSubmission(receipt) if receipt.tx_hash == tx_hash => {
                SessionReceipt::DirectEoaSubmission(receipt.clone()).validate()?;
                anyhow::ensure!(
                    receipt.run_id == run_id,
                    "settlement submission run mismatch tx_hash={tx_hash}"
                );
                anyhow::ensure!(
                    submission
                        .as_ref()
                        .is_none_or(|existing| existing == receipt),
                    "conflicting settlement submissions tx_hash={tx_hash}"
                );
                submission = Some(receipt.clone());
            }
            SessionReceipt::DirectEoaGas(receipt) if receipt.tx_hash == tx_hash => {
                SessionReceipt::DirectEoaGas(receipt.clone()).validate()?;
                anyhow::ensure!(
                    receipt.run_id == run_id,
                    "settlement gas receipt run mismatch tx_hash={tx_hash}"
                );
                anyhow::ensure!(
                    gas.as_ref().is_none_or(|existing| existing == receipt),
                    "conflicting settlement gas receipts tx_hash={tx_hash}"
                );
                gas = Some(receipt.clone());
            }
            SessionReceipt::ReceiptConflict(conflict)
                if conflict.receipt_identity == settlement_identity
                    || conflict.receipt_identity == submission_identity
                    || conflict.receipt_identity == gas_identity =>
            {
                anyhow::bail!("settlement receipt lineage is conflicted tx_hash={tx_hash}");
            }
            SessionReceipt::DirectEoaSubmission(_)
            | SessionReceipt::DirectEoaGas(_)
            | SessionReceipt::MakerRebate(_)
            | SessionReceipt::SettlementCollateral(_)
            | SessionReceipt::ReceiptConflict(_) => {}
        }
    }
    let Some(confirmed) = confirmed else {
        return Ok(None);
    };
    let submission = submission
        .with_context(|| format!("settlement signed submission is missing tx_hash={tx_hash}"))?;
    let gas = gas.with_context(|| {
        format!("settlement finalized gas receipt is missing tx_hash={tx_hash}")
    })?;
    let wrapped_block_number = match &confirmed.outcome {
        SettlementCollateralOutcome::Confirmed { wrapped, .. } => wrapped.log.block_number,
        SettlementCollateralOutcome::ReconciliationRequired { .. } => {
            anyhow::bail!("settlement collateral receipt is not confirmed tx_hash={tx_hash}")
        }
    };
    anyhow::ensure!(
        submission.ledger_seq < gas.ledger_seq
            && gas.ledger_seq < confirmed.ledger_seq
            && submission.ts_ms <= gas.ts_ms
            && gas.ts_ms <= confirmed.ts_ms
            && submission.payer == confirmed.recipient
            && submission.operation == confirmed.operation
            && submission.condition_id.as_deref() == Some(confirmed.condition_id.as_str()),
        "settlement signed submission identity mismatch tx_hash={tx_hash}"
    );
    match confirmed.operation {
        DirectEoaOperation::Merge => anyhow::ensure!(
            submission.amount_units_6dp == Some(confirmed.confirmed_amount_units_6dp()?),
            "merge confirmed collateral differs from signed amount tx_hash={tx_hash}"
        ),
        DirectEoaOperation::Redeem => anyhow::ensure!(
            submission.amount_units_6dp.is_none(),
            "redeem signed submission carries an amount tx_hash={tx_hash}"
        ),
        DirectEoaOperation::Approval | DirectEoaOperation::Split => {
            anyhow::bail!("settlement operation is not merge or redeem tx_hash={tx_hash}")
        }
    }
    anyhow::ensure!(
        gas.status
            && gas.payer == confirmed.recipient
            && gas.operation == confirmed.operation
            && gas.condition_id.as_deref() == Some(confirmed.condition_id.as_str())
            && gas.block_number == Some(wrapped_block_number),
        "settlement finalized gas identity mismatch tx_hash={tx_hash}"
    );
    Ok(Some(ConfirmedSettlementReceiptLineage {
        submission,
        gas,
        collateral: confirmed,
    }))
}

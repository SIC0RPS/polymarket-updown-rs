use std::io::{Cursor, Read};

use super::payload_codec::{
    decode_hex, decode_operation, get_hex, get_u128, get_u64, get_u8, operation_code, put_u128,
    put_u64,
};
use super::*;
use crate::trade_session_ledger::{
    DirectEoaGasReceipt, DirectEoaSubmissionReceipt, SettlementFinalizedLogIdentity,
    SettlementPayoutRedemptionEventReceipt, SettlementWrappedEventReceipt,
};

pub(super) fn put_settlement_lineage(
    out: &mut Vec<u8>,
    lineage: &ConfirmedSettlementReceiptLineage,
) -> Result<()> {
    put_submission(out, &lineage.submission)?;
    put_gas(out, &lineage.gas)?;
    put_settlement(out, &lineage.collateral)
}

pub(super) fn get_settlement_lineage(
    input: &mut Cursor<&[u8]>,
    run_id: &str,
) -> Result<ConfirmedSettlementReceiptLineage> {
    let submission = get_submission(input, run_id)?;
    let gas = get_gas(input, run_id)?;
    let collateral = get_settlement(input, run_id)?;
    let receipts = [
        SessionReceipt::DirectEoaSubmission(submission),
        SessionReceipt::DirectEoaGas(gas),
        SessionReceipt::SettlementCollateral(collateral),
    ];
    validate_confirmed_settlement_receipt_lineage(
        &receipts,
        run_id,
        match &receipts[2] {
            SessionReceipt::SettlementCollateral(receipt) => &receipt.tx_hash,
            _ => unreachable!("fixed settlement lineage receipt layout"),
        },
    )?
    .context("decoded settlement lineage lacks confirmed collateral")
}

fn put_submission(out: &mut Vec<u8>, receipt: &DirectEoaSubmissionReceipt) -> Result<()> {
    anyhow::ensure!(
        receipt.split_identity.is_none() && receipt.split_entry_journal.is_none(),
        "settlement submission carries split identity or journal"
    );
    put_u64(out, receipt.ledger_seq);
    put_u64(out, receipt.ts_ms);
    out.extend_from_slice(&decode_hex::<32>(&receipt.tx_hash)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.payer)?);
    out.push(operation_code(receipt.operation)?);
    out.extend_from_slice(&decode_hex::<32>(
        receipt
            .condition_id
            .as_deref()
            .context("settlement submission condition_id is missing")?,
    )?);
    out.push(u8::from(receipt.amount_units_6dp.is_some()));
    put_u128(out, receipt.amount_units_6dp.unwrap_or_default());
    Ok(())
}

fn get_submission(input: &mut Cursor<&[u8]>, run_id: &str) -> Result<DirectEoaSubmissionReceipt> {
    let ledger_seq = get_u64(input)?;
    let ts_ms = get_u64(input)?;
    let tx_hash = get_hex::<32>(input)?;
    let payer = get_hex::<20>(input)?;
    let operation = decode_operation(get_u8(input)?)?;
    let condition_id = Some(get_hex::<32>(input)?);
    let has_amount = get_u8(input)?;
    anyhow::ensure!(has_amount <= 1, "invalid submission amount presence tag");
    let amount = get_u128(input)?;
    anyhow::ensure!(
        has_amount == 1 || amount == 0,
        "nonzero absent submission amount"
    );
    Ok(DirectEoaSubmissionReceipt {
        run_id: run_id.to_string(),
        ledger_seq,
        ts_ms,
        tx_hash,
        payer,
        operation,
        condition_id,
        amount_units_6dp: (has_amount == 1).then_some(amount),
        split_identity: None,
        split_entry_journal: None,
    })
}

fn put_gas(out: &mut Vec<u8>, receipt: &DirectEoaGasReceipt) -> Result<()> {
    put_u64(out, receipt.ledger_seq);
    put_u64(out, receipt.ts_ms);
    out.extend_from_slice(&decode_hex::<32>(&receipt.tx_hash)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.payer)?);
    put_u64(out, receipt.gas_used);
    put_u128(out, receipt.effective_gas_price_wei);
    put_u64(
        out,
        receipt
            .block_number
            .context("settlement gas block_number is missing")?,
    );
    out.push(u8::from(receipt.status));
    out.push(operation_code(receipt.operation)?);
    out.extend_from_slice(&decode_hex::<32>(
        receipt
            .condition_id
            .as_deref()
            .context("settlement gas condition_id is missing")?,
    )?);
    Ok(())
}

fn get_gas(input: &mut Cursor<&[u8]>, run_id: &str) -> Result<DirectEoaGasReceipt> {
    let ledger_seq = get_u64(input)?;
    let ts_ms = get_u64(input)?;
    let tx_hash = get_hex::<32>(input)?;
    let payer = get_hex::<20>(input)?;
    let gas_used = get_u64(input)?;
    let effective_gas_price_wei = get_u128(input)?;
    let block_number = Some(get_u64(input)?);
    let status = get_u8(input)?;
    anyhow::ensure!(status <= 1, "invalid settlement gas status tag");
    let operation = decode_operation(get_u8(input)?)?;
    let condition_id = Some(get_hex::<32>(input)?);
    Ok(DirectEoaGasReceipt {
        run_id: run_id.to_string(),
        ledger_seq,
        ts_ms,
        tx_hash,
        payer,
        gas_used,
        effective_gas_price_wei,
        block_number,
        status: status == 1,
        operation,
        condition_id,
    })
}

fn put_settlement(out: &mut Vec<u8>, receipt: &SettlementCollateralReceipt) -> Result<()> {
    let (wrapped, payout_redemption) = match &receipt.outcome {
        SettlementCollateralOutcome::Confirmed {
            wrapped,
            payout_redemption,
        } => (wrapped, payout_redemption.as_ref()),
        SettlementCollateralOutcome::ReconciliationRequired { .. } => {
            anyhow::bail!("non-confirmed settlement entered central receipt tape")
        }
    };
    put_u64(out, receipt.ledger_seq);
    put_u64(out, receipt.ts_ms);
    out.extend_from_slice(&decode_hex::<32>(&receipt.tx_hash)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.token_contract)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.recipient)?);
    out.extend_from_slice(&decode_hex::<32>(&receipt.condition_id)?);
    out.push(operation_code(receipt.operation)?);
    out.push(u8::from(payout_redemption.is_some()));
    put_wrapped(out, wrapped)?;
    if let Some(payout) = payout_redemption {
        put_payout_redemption(out, payout)?;
    } else {
        out.extend_from_slice(&[0_u8; 252]);
    }
    Ok(())
}

fn get_settlement(input: &mut Cursor<&[u8]>, run_id: &str) -> Result<SettlementCollateralReceipt> {
    let ledger_seq = get_u64(input)?;
    let ts_ms = get_u64(input)?;
    let tx_hash = get_hex::<32>(input)?;
    let token_contract = get_hex::<20>(input)?;
    let recipient = get_hex::<20>(input)?;
    let condition_id = get_hex::<32>(input)?;
    let operation = decode_operation(get_u8(input)?)?;
    let has_payout = get_u8(input)?;
    anyhow::ensure!(has_payout <= 1, "invalid settlement payout presence tag");
    let wrapped = get_wrapped(input)?;
    let payout_redemption = if has_payout == 1 {
        Some(get_payout_redemption(input)?)
    } else {
        let mut padding = [0_u8; 252];
        input.read_exact(&mut padding)?;
        anyhow::ensure!(
            padding.iter().all(|byte| *byte == 0),
            "nonzero absent payout padding"
        );
        None
    };
    let receipt = SettlementCollateralReceipt {
        run_id: run_id.to_string(),
        ledger_seq,
        ts_ms,
        tx_hash,
        token_contract,
        recipient,
        condition_id,
        operation,
        outcome: SettlementCollateralOutcome::Confirmed {
            wrapped,
            payout_redemption,
        },
    };
    receipt.confirmed_amount_units_6dp()?;
    Ok(receipt)
}

fn put_wrapped(out: &mut Vec<u8>, receipt: &SettlementWrappedEventReceipt) -> Result<()> {
    put_log(out, &receipt.log)?;
    out.extend_from_slice(&decode_hex::<20>(&receipt.caller)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.asset)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.to)?);
    put_u128(out, receipt.amount_units_6dp);
    Ok(())
}

fn get_wrapped(input: &mut Cursor<&[u8]>) -> Result<SettlementWrappedEventReceipt> {
    Ok(SettlementWrappedEventReceipt {
        log: get_log(input)?,
        caller: get_hex::<20>(input)?,
        asset: get_hex::<20>(input)?,
        to: get_hex::<20>(input)?,
        amount_units_6dp: get_u128(input)?,
    })
}

fn put_payout_redemption(
    out: &mut Vec<u8>,
    receipt: &SettlementPayoutRedemptionEventReceipt,
) -> Result<()> {
    put_log(out, &receipt.log)?;
    out.extend_from_slice(&decode_hex::<20>(&receipt.redeemer)?);
    out.extend_from_slice(&decode_hex::<20>(&receipt.collateral_token)?);
    out.extend_from_slice(&decode_hex::<32>(&receipt.parent_collection_id)?);
    out.extend_from_slice(&decode_hex::<32>(&receipt.condition_id)?);
    put_u128(out, receipt.index_sets[0]);
    put_u128(out, receipt.index_sets[1]);
    put_u128(out, receipt.payout_units_6dp);
    Ok(())
}

fn get_payout_redemption(
    input: &mut Cursor<&[u8]>,
) -> Result<SettlementPayoutRedemptionEventReceipt> {
    Ok(SettlementPayoutRedemptionEventReceipt {
        log: get_log(input)?,
        redeemer: get_hex::<20>(input)?,
        collateral_token: get_hex::<20>(input)?,
        parent_collection_id: get_hex::<32>(input)?,
        condition_id: get_hex::<32>(input)?,
        index_sets: [get_u128(input)?, get_u128(input)?],
        payout_units_6dp: get_u128(input)?,
    })
}

fn put_log(out: &mut Vec<u8>, log: &SettlementFinalizedLogIdentity) -> Result<()> {
    put_u64(out, log.log_index);
    out.extend_from_slice(&decode_hex::<20>(&log.emitter)?);
    out.extend_from_slice(&decode_hex::<32>(&log.transaction_hash)?);
    put_u64(out, log.block_number);
    out.extend_from_slice(&decode_hex::<32>(&log.block_hash)?);
    Ok(())
}

fn get_log(input: &mut Cursor<&[u8]>) -> Result<SettlementFinalizedLogIdentity> {
    Ok(SettlementFinalizedLogIdentity {
        log_index: get_u64(input)?,
        emitter: get_hex::<20>(input)?,
        transaction_hash: get_hex::<32>(input)?,
        block_number: get_u64(input)?,
        block_hash: get_hex::<32>(input)?,
    })
}

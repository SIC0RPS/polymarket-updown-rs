use std::fmt::Write as _;
use std::fs;
use std::io::{Cursor, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

use super::*;

pub(super) fn encode_payload(
    resolutions: &[FinalizedConditionResolutionReceipt],
    settlements: &[ConfirmedSettlementReceiptLineage],
    events: &[EventRef],
) -> Result<Vec<u8>> {
    let capacity = 24_usize
        .checked_add(resolutions.len().saturating_mul(RESOLUTION_ROW_BYTES))
        .and_then(|size| {
            size.checked_add(
                settlements
                    .len()
                    .saturating_mul(SETTLEMENT_LINEAGE_ROW_BYTES),
            )
        })
        .and_then(|size| size.checked_add(events.len().saturating_mul(EVENT_REF_BYTES)))
        .context("central receipt payload size overflow")?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(&PAYLOAD_MAGIC);
    put_u32(&mut out, SCHEMA_VERSION);
    put_u32(&mut out, u32::try_from(resolutions.len())?);
    put_u32(&mut out, u32::try_from(settlements.len())?);
    put_u32(&mut out, u32::try_from(events.len())?);
    for receipt in resolutions {
        put_resolution(&mut out, receipt)?;
    }
    for lineage in settlements {
        put_settlement_lineage(&mut out, lineage)?;
    }
    for event in events {
        put_event(&mut out, *event);
    }
    anyhow::ensure!(
        out.len() == capacity,
        "central receipt payload layout mismatch"
    );
    Ok(out)
}

pub(super) fn decode_payload(payload: &[u8], source_run_id: String) -> Result<PreparedReceiptTape> {
    let mut input = Cursor::new(payload);
    let mut magic = [0_u8; 8];
    input.read_exact(&mut magic)?;
    anyhow::ensure!(
        magic == PAYLOAD_MAGIC,
        "invalid central receipt payload magic"
    );
    anyhow::ensure!(
        get_u32(&mut input)? == SCHEMA_VERSION,
        "invalid central receipt payload schema"
    );
    let resolution_count = usize::try_from(get_u32(&mut input)?)?;
    let settlement_count = usize::try_from(get_u32(&mut input)?)?;
    let event_count = usize::try_from(get_u32(&mut input)?)?;
    let mut resolutions = Vec::with_capacity(resolution_count);
    for _ in 0..resolution_count {
        resolutions.push(get_resolution(&mut input, &source_run_id)?);
    }
    let mut settlements = Vec::with_capacity(settlement_count);
    for _ in 0..settlement_count {
        settlements.push(get_settlement_lineage(&mut input, &source_run_id)?);
    }
    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        events.push(get_event(&mut input)?);
    }
    anyhow::ensure!(
        input.position() == payload.len() as u64,
        "central receipt payload contains trailing bytes"
    );
    validate_events(&resolutions, &settlements, &events)?;
    Ok(PreparedReceiptTape {
        resolutions,
        settlements,
        events: Arc::from(events),
    })
}

fn put_resolution(out: &mut Vec<u8>, receipt: &FinalizedConditionResolutionReceipt) -> Result<()> {
    put_u64(out, receipt.ledger_seq);
    put_u64(out, receipt.observed_at_ms);
    out.extend_from_slice(&decode_hex::<32>(&receipt.condition_id)?);
    out.extend_from_slice(&decode_hex::<32>(&receipt.resolution_tx_hash)?);
    put_u64(out, receipt.resolution_log_index);
    put_u64(out, receipt.finalized_block_number);
    out.extend_from_slice(&decode_hex::<32>(&receipt.finalized_block_hash)?);
    put_u64(out, receipt.finalized_block_timestamp_ms);
    put_u128(out, receipt.outcome_slot_0_numerator);
    put_u128(out, receipt.outcome_slot_1_numerator);
    put_u128(out, receipt.denominator);
    Ok(())
}

fn get_resolution(
    input: &mut Cursor<&[u8]>,
    run_id: &str,
) -> Result<FinalizedConditionResolutionReceipt> {
    let receipt = FinalizedConditionResolutionReceipt {
        run_id: run_id.to_string(),
        ledger_seq: get_u64(input)?,
        observed_at_ms: get_u64(input)?,
        condition_id: get_hex::<32>(input)?,
        resolution_tx_hash: get_hex::<32>(input)?,
        resolution_log_index: get_u64(input)?,
        finalized_block_number: get_u64(input)?,
        finalized_block_hash: get_hex::<32>(input)?,
        finalized_block_timestamp_ms: get_u64(input)?,
        outcome_slot_0_numerator: get_u128(input)?,
        outcome_slot_1_numerator: get_u128(input)?,
        denominator: get_u128(input)?,
    };
    receipt.validate_exact()?;
    Ok(receipt)
}

fn put_event(out: &mut Vec<u8>, event: EventRef) {
    put_u64(out, event.sequence);
    put_u64(out, event.timestamp_ms);
    out.push(event.symbol);
    out.push(event.kind);
    out.extend_from_slice(&event.flags.to_le_bytes());
    put_u32(out, event.start);
    put_u32(out, event.len);
}

fn get_event(input: &mut Cursor<&[u8]>) -> Result<EventRef> {
    Ok(EventRef {
        sequence: get_u64(input)?,
        timestamp_ms: get_u64(input)?,
        symbol: get_u8(input)?,
        kind: get_u8(input)?,
        flags: get_u16(input)?,
        start: get_u32(input)?,
        len: get_u32(input)?,
    })
}

fn validate_events(
    resolutions: &[FinalizedConditionResolutionReceipt],
    settlements: &[ConfirmedSettlementReceiptLineage],
    events: &[EventRef],
) -> Result<()> {
    let mut seen_resolutions = vec![false; resolutions.len()];
    let mut seen_settlements = vec![false; settlements.len()];
    let mut previous_key = None;
    for (index, event) in events.iter().copied().enumerate() {
        anyhow::ensure!(
            event.sequence == index as u64 + 1
                && event.symbol == CENTRAL_EVENT_SYMBOL
                && event.flags == 0
                && event.len == 1,
            "invalid central receipt EventRef at index={index}"
        );
        let row = usize::try_from(event.start)?;
        let (timestamp_ms, ledger_seq, priority) = match event.kind {
            EVENT_KIND_CONDITION_RESOLUTION => {
                let receipt = resolutions
                    .get(row)
                    .context("central resolution EventRef out of range")?;
                anyhow::ensure!(
                    !seen_resolutions[row],
                    "duplicate central resolution EventRef"
                );
                seen_resolutions[row] = true;
                (receipt.observed_at_ms, receipt.ledger_seq, 0_u8)
            }
            EVENT_KIND_SETTLEMENT_COLLATERAL => {
                let lineage = settlements
                    .get(row)
                    .context("central settlement EventRef out of range")?;
                anyhow::ensure!(
                    !seen_settlements[row],
                    "duplicate central settlement EventRef"
                );
                seen_settlements[row] = true;
                let receipt = &lineage.collateral;
                (receipt.ts_ms, receipt.ledger_seq, 1_u8)
            }
            other => anyhow::bail!("invalid central receipt EventRef kind={other}"),
        };
        anyhow::ensure!(
            event.timestamp_ms == timestamp_ms,
            "central EventRef timestamp mismatch"
        );
        let key = (timestamp_ms, ledger_seq, priority);
        anyhow::ensure!(
            previous_key.is_none_or(|previous| previous <= key),
            "central EventRefs are not deterministic"
        );
        previous_key = Some(key);
    }
    anyhow::ensure!(
        seen_resolutions.iter().all(|seen| *seen) && seen_settlements.iter().all(|seen| *seen),
        "central receipt tape has unreferenced rows"
    );
    Ok(())
}

pub(super) fn operation_code(operation: DirectEoaOperation) -> Result<u8> {
    match operation {
        DirectEoaOperation::Merge => Ok(0),
        DirectEoaOperation::Redeem => Ok(1),
        DirectEoaOperation::Approval | DirectEoaOperation::Split => {
            anyhow::bail!("invalid settlement operation")
        }
    }
}

pub(super) fn decode_operation(value: u8) -> Result<DirectEoaOperation> {
    match value {
        0 => Ok(DirectEoaOperation::Merge),
        1 => Ok(DirectEoaOperation::Redeem),
        other => anyhow::bail!("invalid settlement operation code={other}"),
    }
}

pub(super) fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N]> {
    let digits = value
        .strip_prefix("0x")
        .with_context(|| format!("fixed hex value lacks 0x prefix: {value}"))?;
    anyhow::ensure!(
        digits.len() == N * 2,
        "fixed hex value has invalid length: {value}"
    );
    let mut out = [0_u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16)
            .with_context(|| format!("fixed hex value is invalid: {value}"))?;
    }
    anyhow::ensure!(
        encode_hex(&out) == value,
        "fixed hex value is noncanonical: {value}"
    );
    Ok(out)
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing fixed hex to String cannot fail");
    }
    out
}

pub(super) fn encode_sha256(bytes: [u8; 32]) -> String {
    encode_hex(&bytes).trim_start_matches("0x").to_string()
}

pub(super) fn get_hex<const N: usize>(input: &mut Cursor<&[u8]>) -> Result<String> {
    let mut bytes = [0_u8; N];
    input.read_exact(&mut bytes)?;
    Ok(encode_hex(&bytes))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u128(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn get_u8(input: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut bytes = [0_u8; 1];
    input.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn get_u16(input: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut bytes = [0_u8; 2];
    input.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn get_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0_u8; 4];
    input.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

pub(super) fn get_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

pub(super) fn get_u128(input: &mut Cursor<&[u8]>) -> Result<u128> {
    let mut bytes = [0_u8; 16];
    input.read_exact(&mut bytes)?;
    Ok(u128::from_le_bytes(bytes))
}

pub(super) fn file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(encode_sha256(Sha256::digest(bytes).into()))
}

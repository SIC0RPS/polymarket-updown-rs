use std::collections::BTreeSet;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::capture::events::{CtfInventoryOperationLatencyCapture, OrderSubmitLatencyCapture};
use crate::capture::FinalizedCaptureIdentity;
use crate::trade_session_ledger::{
    load_records_from_path, CanonicalCtfEvent, ManagedSellReceiptIdentity, TradeLedgerRecord,
    CAPTURED_SESSION_LEDGER_FILE,
};
use crate::types::TimeInForce;

use super::{
    ReplayClobReceiptAdmissionObservation, ReplayClobTimingObservation, ReplayClobTimingSource,
    ReplayCtfTimingObservation, ReplayCtfTimingSource, CLOB_JOURNAL, CTF_JOURNAL,
    CURRENT_EXECUTION_PROTOCOL_START_MS,
};

fn timing_journal_manifest(
    identity: &FinalizedCaptureIdentity,
    file: &str,
) -> Result<Option<(u64, String)>> {
    let mut files = identity
        .journals
        .iter()
        .flat_map(|journal| &journal.files)
        .filter(|candidate| candidate.path == file);
    let Some(journal) = files.next() else {
        return Ok(None);
    };
    anyhow::ensure!(
        files.next().is_none(),
        "timing journal appears more than once in finalized manifest"
    );
    let sha256 = journal
        .sha256
        .clone()
        .filter(|value| !value.is_empty())
        .context("finalized timing journal has no digest")?;
    Ok(Some((journal.written, sha256)))
}

fn read_clob_rows(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
) -> Result<Option<(PathBuf, String, Vec<OrderSubmitLatencyCapture>)>> {
    let Some((expected_rows, expected_sha256)) = timing_journal_manifest(identity, CLOB_JOURNAL)?
    else {
        return Ok(None);
    };
    let path = capture.join(CLOB_JOURNAL);
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let journal_sha256 = format!("{:x}", Sha256::digest(&bytes));
    anyhow::ensure!(
        journal_sha256 == expected_sha256,
        "CLOB timing journal digest differs from manifest"
    );
    let mut rows = Vec::new();
    for line in BufReader::new(bytes.as_slice()).lines() {
        let line = line?;
        if !line.trim().is_empty() {
            rows.push(serde_json::from_str::<OrderSubmitLatencyCapture>(&line)?);
        }
    }
    anyhow::ensure!(
        rows.len() as u64 == expected_rows,
        "CLOB timing journal row count differs from manifest"
    );
    Ok(Some((path, journal_sha256, rows)))
}

pub(super) fn load_clob_source_v1(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
) -> Result<Option<ReplayClobTimingSource>> {
    let Some((path, journal_sha256, rows)) = read_clob_rows(capture, identity)? else {
        return Ok(None);
    };
    let ack_keys = rows
        .iter()
        .filter(|row| successful_ack(row) && row.attempt == 0)
        .map(legacy_ack_key)
        .collect::<BTreeSet<_>>();
    let observations = rows
        .iter()
        .filter(|row| ack_keys.contains(&legacy_ack_key(row)))
        .filter_map(clob_transport_observation)
        .collect::<Vec<_>>();
    clob_source(
        capture,
        identity,
        source_config_sha256,
        path,
        journal_sha256,
        observations,
    )
}

pub(super) fn load_clob_source_v2(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
) -> Result<Option<ReplayClobTimingSource>> {
    load_clob_source(capture, identity, source_config_sha256, false)
}

pub(super) fn load_clob_source_v5(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
) -> Result<Option<ReplayClobTimingSource>> {
    load_clob_source(capture, identity, source_config_sha256, true)
}

fn load_clob_source(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
    include_admission: bool,
) -> Result<Option<ReplayClobTimingSource>> {
    let Some((path, journal_sha256, rows)) = read_clob_rows(capture, identity)? else {
        return Ok(None);
    };
    let mut associated_transports = BTreeSet::new();
    let mut associated_members = BTreeSet::new();
    for ack in rows.iter().filter(|row| successful_ack(row)) {
        let mut matches = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| valid_clob_transport(row))
            .filter(|(_, row)| ack_matches_transport(ack, row))
            .map(|(index, _)| index);
        let transport_index = matches.next();
        anyhow::ensure!(
            matches.next().is_none(),
            "accepted order acknowledgment has ambiguous transport association"
        );
        let Some(transport_index) = transport_index else {
            continue;
        };
        anyhow::ensure!(
            associated_members.insert((transport_index, ack.input_index)),
            "accepted order acknowledgment duplicates a transport member"
        );
        associated_transports.insert(transport_index);
    }
    let canonical_path = capture.join(CAPTURED_SESSION_LEDGER_FILE);
    let canonical = if include_admission
        && rows.iter().any(|row| row.stage == "receipt_admission")
        && canonical_path.is_file()
    {
        let bytes = fs::read(&canonical_path)
            .with_context(|| format!("read {}", canonical_path.display()))?;
        Some((
            format!("{:x}", Sha256::digest(&bytes)),
            load_records_from_path(&canonical_path)?,
        ))
    } else {
        None
    };
    let observations = rows
        .iter()
        .enumerate()
        .filter(|(index, _)| associated_transports.contains(index))
        .filter_map(|(_, row)| {
            let mut observation = clob_transport_observation(row)?;
            if let Some((_, records)) = &canonical {
                observation.receipt_admission =
                    receipt_admission_observation(row, &rows, records, &identity.run_id);
            }
            Some(observation)
        })
        .collect::<Vec<_>>();
    let mut source = clob_source(
        capture,
        identity,
        source_config_sha256,
        path,
        journal_sha256,
        observations,
    )?;
    if let (Some(source), Some((sha256, _))) = (&mut source, canonical) {
        source.canonical_journal_path = Some(canonical_path.display().to_string());
        source.canonical_journal_sha256 = Some(sha256);
    }
    Ok(source)
}

fn receipt_admission_observation(
    transport: &OrderSubmitLatencyCapture,
    rows: &[OrderSubmitLatencyCapture],
    records: &[TradeLedgerRecord],
    run_id: &str,
) -> Option<ReplayClobReceiptAdmissionObservation> {
    let start = transport.input_range_start?;
    if transport.input_range_end? != start.checked_add(1)? || transport.side != "sell" {
        return None;
    }
    let ack = unique_match(
        rows.iter()
            .filter(|row| successful_ack(row) && ack_matches_transport(row, transport)),
    )?;
    let intent_id = ack.execution_intent_id.filter(|id| *id > 0)?;
    if ack.attempt == 0
        || ack.input_index != Some(start)
        || rows
            .iter()
            .filter(|row| row.stage == "order_ack" && row.order_id == ack.order_id)
            .count()
            != 1
    {
        return None;
    }
    let admission = unique_match(
        rows.iter()
            .filter(|row| row.stage == "receipt_admission" && row.order_id == ack.order_id),
    )?;
    if !ack_matches_transport(admission, transport)
        || admission.input_index != ack.input_index
        || !representative_identity_matches(admission, ack)
        || admission.submit_status.as_deref() != Some("confirmed_receipt_admitted")
        || admission.attempt_result != transport.attempt_result
        || admission.response_evidence != transport.response_evidence
        || admission.interpretation_error.is_some()
        || admission.response_headers_ts_ms != transport.response_headers_ts_ms
        || admission.invocation_elapsed_us != transport.invocation_elapsed_us
        || admission.send_invocation_to_response_us != transport.invocation_elapsed_us
        || admission.response_headers_elapsed_us != transport.response_headers_elapsed_us
        || admission.response_body_elapsed_us != transport.response_body_elapsed_us
        || admission.http_status != transport.http_status
        || admission
            .timing_missing_phases
            .iter()
            .chain(&admission.timing_reversed_phases)
            .any(|phase| phase == "receipt_admission")
    {
        return None;
    }
    let canonical_receipt_id = admission.canonical_receipt_id.as_ref()?;
    if rows
        .iter()
        .filter(|row| {
            row.stage == "receipt_admission"
                && row.canonical_receipt_id.as_ref() == Some(canonical_receipt_id)
        })
        .count()
        != 1
    {
        return None;
    }
    let (accepted_record, accepted) =
        unique_match(
            records
                .iter()
                .filter_map(|record| match record.ctf_event.as_ref()? {
                    CanonicalCtfEvent::OrderAccepted(fact) if fact.order_id == ack.order_id => {
                        Some((record, fact))
                    }
                    _ => None,
                }),
        )?;
    let (receipt_record, receipt) =
        unique_match(
            records
                .iter()
                .filter_map(|record| match record.ctf_event.as_ref()? {
                    CanonicalCtfEvent::TradeConfirmed(fact)
                        if fact.order_id == ack.order_id
                            || fact.receipt_id == *canonical_receipt_id =>
                    {
                        Some((record, fact))
                    }
                    _ => None,
                }),
        )?;
    if accepted_record.run_id != run_id
        || receipt_record.run_id != run_id
        || accepted_record.event_kind != "order_accepted"
        || receipt_record.event_kind != "trade_confirmed"
        || accepted_record.ctf_event.as_ref()?.validate().is_err()
        || receipt_record.ctf_event.as_ref()?.validate().is_err()
        || !matches!(&accepted.time_in_force, TimeInForce::Fak)
        || accepted.post_only
        || accepted.intent_id != intent_id
        || ack.market_id.as_deref() != Some(accepted.market_id.as_str())
        || accepted.asset_id != ack.token
        || Some(accepted.instruction_qty_units_6dp)
            != ack
                .qty
                .and_then(|qty| crate::ctf_accounting::qty_to_units_6dp(qty).ok())
        || Some(accepted.instruction_limit_px_units_8dp)
            != ack
                .limit_px
                .and_then(crate::polymarket::fixed_math::price_to_units_8dp)
        || receipt.receipt_id != *canonical_receipt_id
        || receipt.order_id != accepted.order_id
        || receipt.intent_id != intent_id
        || receipt.market_id != accepted.market_id
        || receipt.cohort_id != accepted.cohort_id
        || receipt.token != accepted.asset_id
        || receipt.qty_units_6dp > accepted.instruction_qty_units_6dp
        || receipt.confirmed_at_ms != admission.session_ts_ms
        || !matches!(
            &receipt.receipt_identity,
            ManagedSellReceiptIdentity::ExchangeV2 { .. }
        )
        || receipt.matched_trade.as_ref()?.taker_order_id.as_deref() != Some(ack.order_id.as_str())
    {
        return None;
    }
    Some(ReplayClobReceiptAdmissionObservation {
        response_to_canonical_append_us: admission.receipt_admission_us?,
        order_id: ack.order_id.clone(),
        canonical_receipt_id: canonical_receipt_id.clone(),
        execution_intent_id: intent_id,
        input_index: start,
    })
}

fn unique_match<T>(mut rows: impl Iterator<Item = T>) -> Option<T> {
    let row = rows.next()?;
    rows.next().is_none().then_some(row)
}

fn clob_source(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
    path: PathBuf,
    journal_sha256: String,
    observations: Vec<ReplayClobTimingObservation>,
) -> Result<Option<ReplayClobTimingSource>> {
    if observations.is_empty() {
        return Ok(None);
    }
    Ok(Some(ReplayClobTimingSource {
        source_capture: capture.display().to_string(),
        source_run_id: identity.run_id.clone(),
        source_config_sha256,
        journal_path: path.display().to_string(),
        journal_sha256,
        canonical_journal_path: None,
        canonical_journal_sha256: None,
        endpoint: "post_request_send_to_synchronous_response".to_string(),
        unit: "microseconds".to_string(),
        observations,
    }))
}

fn successful_ack(row: &OrderSubmitLatencyCapture) -> bool {
    row.stage == "order_ack"
        && !row.order_id.trim().is_empty()
        && row.submit_status.as_deref().is_some_and(|status| {
            matches!(
                status.trim().to_ascii_lowercase().as_str(),
                "matched" | "live" | "unmatched"
            )
        })
}

fn legacy_ack_key(
    row: &OrderSubmitLatencyCapture,
) -> (u64, u64, u64, u64, Option<String>, String, String) {
    (
        row.decision_ts_ms,
        row.supervisor_admission_ts_ms,
        row.request_ts_ms,
        row.response_ts_ms,
        row.market_id.clone(),
        row.token.clone(),
        row.side.clone(),
    )
}

fn ack_matches_transport(
    ack: &OrderSubmitLatencyCapture,
    transport: &OrderSubmitLatencyCapture,
) -> bool {
    if ack.decision_ts_ms != transport.decision_ts_ms
        || ack.supervisor_admission_ts_ms != transport.supervisor_admission_ts_ms
        || ack.request_ts_ms != transport.request_ts_ms
        || ack.response_ts_ms != transport.response_ts_ms
    {
        return false;
    }
    if ack.attempt == 0 {
        return legacy_ack_matches_transport(ack, transport);
    }
    let (Some(index), Some(ack_start), Some(ack_end), Some(start), Some(end)) = (
        ack.input_index,
        ack.input_range_start,
        ack.input_range_end,
        transport.input_range_start,
        transport.input_range_end,
    ) else {
        return false;
    };
    ack.attempt == transport.attempt
        && start < end
        && ack_start == start
        && ack_end == end
        && (start..end).contains(&index)
        && (index != start || representative_identity_matches(ack, transport))
}

fn legacy_ack_matches_transport(
    ack: &OrderSubmitLatencyCapture,
    transport: &OrderSubmitLatencyCapture,
) -> bool {
    if let Some(index) = ack.input_index {
        let (Some(start), Some(end)) = (transport.input_range_start, transport.input_range_end)
        else {
            return false;
        };
        if start >= end
            || !(start..end).contains(&index)
            || ack
                .input_range_start
                .is_some_and(|ack_start| ack_start != start)
            || ack.input_range_end.is_some_and(|ack_end| ack_end != end)
        {
            return false;
        }
        return index != start || representative_identity_matches(ack, transport);
    }
    if ack.input_range_start.is_some() || ack.input_range_end.is_some() {
        if ack.input_range_start != transport.input_range_start
            || ack.input_range_end != transport.input_range_end
        {
            return false;
        }
    }
    representative_identity_matches(ack, transport)
}

fn representative_identity_matches(
    ack: &OrderSubmitLatencyCapture,
    transport: &OrderSubmitLatencyCapture,
) -> bool {
    ack.market_id == transport.market_id
        && ack.token == transport.token
        && ack.side == transport.side
        && ack.close_hint == transport.close_hint
        && ack.qty == transport.qty
        && ack.limit_px == transport.limit_px
        && ack.market_connection_session_id == transport.market_connection_session_id
        && ack.market_frame_sequence == transport.market_frame_sequence
        && ack.market_state_revision == transport.market_state_revision
        && ack.strategy_consumption_sequence == transport.strategy_consumption_sequence
        && ack.strategy_input_kind == transport.strategy_input_kind
        && ack.execution_intent_id == transport.execution_intent_id
}

fn clob_transport_observation(
    row: &OrderSubmitLatencyCapture,
) -> Option<ReplayClobTimingObservation> {
    if !valid_clob_transport(row) {
        return None;
    }
    Some(ReplayClobTimingObservation {
        attempt: row.attempt,
        request_ts_ms: row.request_ts_ms,
        response_ts_ms: row.response_ts_ms,
        decision_to_handoff_us: row.decision_to_handoff_us?,
        handoff_to_send_invocation_us: row.handoff_to_send_invocation_us?,
        send_invocation_to_response_us: row.send_invocation_to_response_us?,
        response_to_owner_us: row.response_to_owner_us?,
        invocation_elapsed_us: row.invocation_elapsed_us?,
        response_headers_elapsed_us: row.response_headers_elapsed_us,
        response_body_elapsed_us: row.response_body_elapsed_us,
        http_status: row.http_status?,
        receipt_admission: None,
    })
}

fn valid_clob_transport(row: &OrderSubmitLatencyCapture) -> bool {
    row.stage == "transport_complete"
        && row.attempt > 0
        && row.attempt_result.as_deref() == Some("response_decoded")
        && row.order_id.is_empty()
        && row.request_ts_ms >= CURRENT_EXECUTION_PROTOCOL_START_MS
        && row.response_ts_ms >= row.request_ts_ms
        && row.decision_to_handoff_us.is_some()
        && row.handoff_to_send_invocation_us.is_some()
        && row.send_invocation_to_response_us.is_some()
        && row.response_to_owner_us.is_some()
        && row.invocation_elapsed_us.is_some()
        && row
            .http_status
            .is_some_and(|status| (200..300).contains(&status))
}

pub(super) fn load_ctf_source(
    capture: &Path,
    identity: &FinalizedCaptureIdentity,
    source_config_sha256: Option<String>,
) -> Result<Option<ReplayCtfTimingSource>> {
    let Some((expected_rows, expected_sha256)) = timing_journal_manifest(identity, CTF_JOURNAL)?
    else {
        return Ok(None);
    };
    let path = capture.join(CTF_JOURNAL);
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let journal_sha256 = format!("{:x}", Sha256::digest(&bytes));
    anyhow::ensure!(
        journal_sha256 == expected_sha256,
        "CTF timing journal digest differs from manifest"
    );
    let mut observations = Vec::new();
    let mut tx_hashes = BTreeSet::new();
    let mut rows = 0_u64;
    for line in BufReader::new(bytes.as_slice()).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        rows += 1;
        let row: CtfInventoryOperationLatencyCapture = serde_json::from_str(&line)?;
        let Some(tx_hash) = row.tx_hash else {
            continue;
        };
        if row.operation == "split_complete_set"
            && row.status == "materialized"
            && row.materialization_source == "receipt_confirmed_current_session_split"
            && row.error.is_none()
            && row.pending_ts_ms > 0
            && row.materialized_ts_ms >= row.pending_ts_ms
            && row.latency_ms == row.materialized_ts_ms.saturating_sub(row.pending_ts_ms)
            && !tx_hash.trim().is_empty()
            && tx_hashes.insert(tx_hash.clone())
        {
            observations.push(ReplayCtfTimingObservation {
                pending_ts_ms: row.pending_ts_ms,
                materialized_ts_ms: row.materialized_ts_ms,
                latency_ms: row.latency_ms,
                tx_hash,
            });
        }
    }
    anyhow::ensure!(
        rows == expected_rows,
        "CTF timing journal row count differs from manifest"
    );
    if observations.is_empty() {
        return Ok(None);
    }
    Ok(Some(ReplayCtfTimingSource {
        source_capture: capture.display().to_string(),
        source_run_id: identity.run_id.clone(),
        source_config_sha256,
        journal_path: path.display().to_string(),
        journal_sha256,
        endpoint: "confirmed_pending_to_materialized_inventory".to_string(),
        unit: "milliseconds".to_string(),
        observations,
    }))
}

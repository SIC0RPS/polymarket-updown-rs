use super::*;
use crate::capture::session::CaptureMsg;

#[derive(Debug, Clone)]
pub(crate) struct OrderExecutionTiming {
    pub(super) market_frame_received_at: Option<Instant>,
    pub(super) market_state_published_at: Option<Instant>,
    pub(super) decision_at: Option<Instant>,
    pub(super) signed_order_completed_at: Option<Instant>,
    pub(super) server_time_request_started_at: Option<Instant>,
    pub(super) server_time_request_completed_at: Option<Instant>,
    pub(super) submit_invocation_at: Option<Instant>,
    pub(super) interpreted_terminal_at: Option<Instant>,
    pub(super) market_connection_session_id: Option<u64>,
    pub(super) market_frame_sequence: Option<u64>,
    pub(super) market_state_revision: Option<u64>,
    pub(super) strategy_consumption_sequence: Option<u64>,
    pub(super) strategy_input_kind: Option<&'static str>,
    pub(super) execution_intent_id: Option<u64>,
    pub(super) input_index: Option<usize>,
    pub(super) input_range: Option<std::ops::Range<usize>>,
    pub(super) attempt: Option<usize>,
    market_id: Option<String>,
    token: String,
    side: String,
    close_hint: Option<String>,
    qty: f64,
    limit_px: f64,
    decision_ts_ms: u64,
    supervisor_admission_ts_ms: u64,
    request_ts_ms: Option<u64>,
    response_headers_ts_ms: Option<u64>,
    terminal_ts_ms: Option<u64>,
    response_headers_elapsed_us: Option<u64>,
    invocation_elapsed_us: Option<u64>,
    response_body_elapsed_us: Option<u64>,
    http_status: Option<u16>,
    attempt_result: Option<String>,
    interpretation_error: Option<String>,
    response_evidence: Option<String>,
}

pub(super) fn duration_to_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

pub(super) fn order_execution_timing_from_intent(intent: &OrderIntent) -> OrderExecutionTiming {
    OrderExecutionTiming {
        market_frame_received_at: intent.market_frame_received_at,
        market_state_published_at: intent.market_state_published_at,
        decision_at: intent.decision_at,
        signed_order_completed_at: None,
        server_time_request_started_at: None,
        server_time_request_completed_at: None,
        submit_invocation_at: None,
        interpreted_terminal_at: None,
        market_connection_session_id: intent.market_connection_session_id,
        market_frame_sequence: intent.market_frame_sequence,
        market_state_revision: intent.market_state_revision,
        strategy_consumption_sequence: intent.strategy_consumption_sequence,
        strategy_input_kind: intent.strategy_input_kind,
        execution_intent_id: intent.execution_intent_id,
        input_index: None,
        input_range: None,
        attempt: None,
        market_id: intent_market_id(intent).map(|market| market.to_string()),
        token: intent.instrument.clone(),
        side: match intent.side() {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
        .to_string(),
        close_hint: intent.order_close_reason_str().map(str::to_string),
        qty: intent.qty,
        limit_px: intent.limit_px,
        decision_ts_ms: 0,
        supervisor_admission_ts_ms: 0,
        request_ts_ms: None,
        response_headers_ts_ms: None,
        terminal_ts_ms: None,
        response_headers_elapsed_us: None,
        invocation_elapsed_us: None,
        response_body_elapsed_us: None,
        http_status: None,
        attempt_result: None,
        interpretation_error: None,
        response_evidence: None,
    }
}

pub(super) fn order_execution_timing_for_input(
    intent: &OrderIntent,
    input_index: usize,
    order_id: &str,
    decision_ts_ms: u64,
    supervisor_admission_ts_ms: u64,
    timings: &[ExecutionPostTiming],
) -> OrderExecutionTiming {
    let mut lineage = order_execution_timing_from_intent(intent);
    lineage.decision_ts_ms = decision_ts_ms;
    lineage.supervisor_admission_ts_ms = supervisor_admission_ts_ms;
    lineage.input_index = Some(input_index);
    let matching_range = timings
        .iter()
        .rev()
        .find(|timing| timing.input_range.contains(&input_index));
    if let Some(timing) = matching_range {
        lineage.signed_order_completed_at = timing
            .signed_order_completed_at
            .get(input_index.saturating_sub(timing.input_range.start))
            .copied();
        lineage.input_range = Some(timing.input_range.clone());
    }
    let Some(timing) = timings.iter().rev().find(|timing| {
        if !timing.input_range.contains(&input_index) {
            return false;
        }
        timing
            .accepted_order_ids
            .get(input_index.saturating_sub(timing.input_range.start))
            .and_then(Option::as_deref)
            == Some(order_id)
    }) else {
        return lineage;
    };
    lineage.submit_invocation_at = Some(timing.invocation_instant);
    lineage.server_time_request_started_at = timing.server_time_request_started_at;
    lineage.server_time_request_completed_at = timing.server_time_request_completed_at;
    lineage.interpreted_terminal_at = Some(timing.terminal_instant);
    lineage.attempt = Some(timing.attempt);
    lineage.request_ts_ms = Some(timing.request_ts_ms);
    lineage.response_headers_ts_ms = timing.response_headers_ts_ms;
    lineage.terminal_ts_ms = Some(timing.terminal_ts_ms);
    lineage.response_headers_elapsed_us = timing.response_headers_elapsed_us;
    lineage.invocation_elapsed_us = Some(timing.invocation_elapsed_us);
    lineage.response_body_elapsed_us = timing.response_body_elapsed_us;
    lineage.http_status = timing.http_status;
    lineage.attempt_result = Some(timing.attempt_result.clone());
    lineage.interpretation_error = timing.interpretation_error.clone();
    lineage.response_evidence = timing.response_evidence.clone();
    lineage
}

#[allow(
    clippy::too_many_arguments,
    reason = "observational transport timing fields"
)]
pub(super) fn emit_submission_transport_timings(
    capture: &MaybeCapture,
    decision_ts_ms: u64,
    supervisor_admission_ts_ms: u64,
    decision_instant: Option<Instant>,
    handoff_instant: Option<Instant>,
    owner_completion_instant: Instant,
    oldest_pending_intent_age_us: Option<u64>,
    stage: &str,
    intents: &[OrderIntent],
    timings: &[ExecutionPostTiming],
) {
    for timing in timings {
        let representative = intents.get(timing.input_range.start);
        let signed_order_completed_at = timing.signed_order_completed_at.first().copied();
        let mut missing = Vec::new();
        let mut reversed = Vec::new();
        let signed_to_server_time_us = checked_phase_us(
            "signed_to_server_time",
            signed_order_completed_at,
            timing.server_time_request_started_at,
            &mut missing,
            &mut reversed,
        );
        let server_time_request_us = checked_phase_us(
            "server_time_request",
            timing.server_time_request_started_at,
            timing.server_time_request_completed_at,
            &mut missing,
            &mut reversed,
        );
        let server_time_to_submit_us = checked_phase_us(
            "server_time_to_submit",
            timing.server_time_request_completed_at,
            Some(timing.invocation_instant),
            &mut missing,
            &mut reversed,
        );
        capture.emit(CaptureMsg::OrderSubmitLatency(OrderSubmitLatencyCapture {
            session_ts_ms: timing.terminal_ts_ms,
            decision_ts_ms,
            supervisor_admission_ts_ms,
            stage: stage.to_string(),
            attempt: timing.attempt,
            attempt_result: Some(timing.attempt_result.clone()),
            interpretation_error: timing.interpretation_error.clone(),
            response_evidence: timing.response_evidence.clone(),
            order_id: String::new(),
            canonical_receipt_id: None,
            market_id: representative
                .and_then(intent_market_id)
                .map(|market| market.to_string()),
            token: representative
                .map(|intent| intent.instrument.clone())
                .unwrap_or_default(),
            side: representative
                .map(|intent| match intent.side() {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                })
                .unwrap_or("unknown")
                .to_string(),
            close_hint: representative
                .and_then(|intent| intent.order_close_reason_str())
                .map(str::to_string),
            submit_status: Some(stage.to_string()),
            qty: representative.map(|intent| intent.qty),
            limit_px: representative.map(|intent| intent.limit_px),
            market_connection_session_id: representative
                .and_then(|intent| intent.market_connection_session_id),
            market_frame_sequence: representative.and_then(|intent| intent.market_frame_sequence),
            market_state_revision: representative.and_then(|intent| intent.market_state_revision),
            strategy_consumption_sequence: representative
                .and_then(|intent| intent.strategy_consumption_sequence),
            strategy_input_kind: representative
                .and_then(|intent| intent.strategy_input_kind)
                .map(str::to_string),
            execution_intent_id: representative.and_then(|intent| intent.execution_intent_id),
            input_index: None,
            input_range_start: Some(timing.input_range.start),
            input_range_end: Some(timing.input_range.end),
            request_ts_ms: timing.request_ts_ms,
            decision_to_request_ms: timing.request_ts_ms.saturating_sub(decision_ts_ms),
            decision_to_admission_ms: supervisor_admission_ts_ms.saturating_sub(decision_ts_ms),
            admission_to_request_ms: timing
                .request_ts_ms
                .saturating_sub(supervisor_admission_ts_ms),
            response_ts_ms: timing.terminal_ts_ms,
            ack_latency_ms: timing.terminal_ts_ms.saturating_sub(timing.request_ts_ms),
            decision_to_handoff_us: decision_instant
                .zip(handoff_instant)
                .and_then(|(start, end)| end.checked_duration_since(start))
                .map(duration_to_us),
            handoff_to_send_invocation_us: handoff_instant
                .and_then(|start| timing.invocation_instant.checked_duration_since(start))
                .map(duration_to_us),
            send_invocation_to_response_us: timing
                .terminal_instant
                .checked_duration_since(timing.invocation_instant)
                .map(duration_to_us),
            response_to_owner_us: owner_completion_instant
                .checked_duration_since(timing.terminal_instant)
                .map(duration_to_us),
            oldest_pending_intent_age_us,
            response_headers_ts_ms: timing.response_headers_ts_ms,
            invocation_elapsed_us: Some(timing.invocation_elapsed_us),
            response_headers_elapsed_us: timing.response_headers_elapsed_us,
            response_body_elapsed_us: timing.response_body_elapsed_us,
            http_status: timing.http_status,
            applied_us: None,
            decision_us: None,
            construction_us: None,
            signing_to_submit_us: None,
            signed_to_server_time_us,
            server_time_request_us,
            server_time_to_submit_us,
            submit_to_headers_us: None,
            response_body_interpreted_us: None,
            receipt_admission_us: None,
            market_to_decision_us: None,
            market_to_submit_us: None,
            total_reaction_us: None,
            timing_missing_phases: missing,
            timing_reversed_phases: reversed,
        }));
    }
}

pub(crate) fn emit_receipt_admission_timing(
    capture: &MaybeCapture,
    timing: &OrderExecutionTiming,
    order_id: &str,
    canonical_receipt_id: String,
    receipt_ts_ms: u64,
    receipt_admission_at: Instant,
) {
    let mut missing = Vec::new();
    let mut reversed = Vec::new();
    let applied_us = checked_phase_us(
        "applied",
        timing.market_frame_received_at,
        timing.market_state_published_at,
        &mut missing,
        &mut reversed,
    );
    let decision_us = checked_phase_us(
        "decision",
        timing.market_state_published_at,
        timing.decision_at,
        &mut missing,
        &mut reversed,
    );
    let construction_us = checked_phase_us(
        "construction",
        timing.decision_at,
        timing.signed_order_completed_at,
        &mut missing,
        &mut reversed,
    );
    let signing_to_submit_us = checked_phase_us(
        "signing_to_submit",
        timing.signed_order_completed_at,
        timing.submit_invocation_at,
        &mut missing,
        &mut reversed,
    );
    let signed_to_server_time_us = checked_phase_us(
        "signed_to_server_time",
        timing.signed_order_completed_at,
        timing.server_time_request_started_at,
        &mut missing,
        &mut reversed,
    );
    let server_time_request_us = checked_phase_us(
        "server_time_request",
        timing.server_time_request_started_at,
        timing.server_time_request_completed_at,
        &mut missing,
        &mut reversed,
    );
    let server_time_to_submit_us = checked_phase_us(
        "server_time_to_submit",
        timing.server_time_request_completed_at,
        timing.submit_invocation_at,
        &mut missing,
        &mut reversed,
    );
    let submit_to_headers_us = match timing.response_headers_elapsed_us {
        Some(value) => Some(value),
        None => {
            missing.push("submit_to_headers".to_string());
            None
        }
    };
    let submit_to_terminal_us = checked_phase_us(
        "response_body_interpreted",
        timing.submit_invocation_at,
        timing.interpreted_terminal_at,
        &mut missing,
        &mut reversed,
    );
    let response_body_interpreted_us = match (submit_to_terminal_us, submit_to_headers_us) {
        (Some(total), Some(headers)) => match total.checked_sub(headers) {
            Some(value) => Some(value),
            None => {
                reversed.push("response_body_interpreted".to_string());
                None
            }
        },
        _ => None,
    };
    let receipt_admission_us = checked_phase_us(
        "receipt_admission",
        timing.interpreted_terminal_at,
        Some(receipt_admission_at),
        &mut missing,
        &mut reversed,
    );
    let market_to_decision_us = checked_phase_us(
        "market_to_decision",
        timing.market_frame_received_at,
        timing.decision_at,
        &mut missing,
        &mut reversed,
    );
    let market_to_submit_us = checked_phase_us(
        "market_to_submit",
        timing.market_frame_received_at,
        timing.submit_invocation_at,
        &mut missing,
        &mut reversed,
    );
    let total_reaction_us = checked_phase_us(
        "total_reaction",
        timing.market_frame_received_at,
        Some(receipt_admission_at),
        &mut missing,
        &mut reversed,
    );
    let request_ts_ms = timing.request_ts_ms.unwrap_or_default();
    let response_ts_ms = timing.terminal_ts_ms.unwrap_or_default();
    capture.emit(CaptureMsg::OrderSubmitLatency(OrderSubmitLatencyCapture {
        session_ts_ms: receipt_ts_ms,
        decision_ts_ms: timing.decision_ts_ms,
        supervisor_admission_ts_ms: timing.supervisor_admission_ts_ms,
        stage: "receipt_admission".to_string(),
        attempt: timing.attempt.unwrap_or_default(),
        attempt_result: timing.attempt_result.clone(),
        interpretation_error: timing.interpretation_error.clone(),
        response_evidence: timing.response_evidence.clone(),
        order_id: order_id.to_string(),
        canonical_receipt_id: Some(canonical_receipt_id),
        market_id: timing.market_id.clone(),
        token: timing.token.clone(),
        side: timing.side.clone(),
        close_hint: timing.close_hint.clone(),
        submit_status: Some("confirmed_receipt_admitted".to_string()),
        qty: Some(timing.qty),
        limit_px: Some(timing.limit_px),
        market_connection_session_id: timing.market_connection_session_id,
        market_frame_sequence: timing.market_frame_sequence,
        market_state_revision: timing.market_state_revision,
        strategy_consumption_sequence: timing.strategy_consumption_sequence,
        strategy_input_kind: timing.strategy_input_kind.map(str::to_string),
        execution_intent_id: timing.execution_intent_id,
        input_index: timing.input_index,
        input_range_start: timing.input_range.as_ref().map(|range| range.start),
        input_range_end: timing.input_range.as_ref().map(|range| range.end),
        request_ts_ms,
        decision_to_request_ms: request_ts_ms.saturating_sub(timing.decision_ts_ms),
        decision_to_admission_ms: timing
            .supervisor_admission_ts_ms
            .saturating_sub(timing.decision_ts_ms),
        admission_to_request_ms: request_ts_ms.saturating_sub(timing.supervisor_admission_ts_ms),
        response_ts_ms,
        ack_latency_ms: response_ts_ms.saturating_sub(request_ts_ms),
        decision_to_handoff_us: None,
        handoff_to_send_invocation_us: None,
        send_invocation_to_response_us: timing.invocation_elapsed_us,
        response_to_owner_us: None,
        oldest_pending_intent_age_us: None,
        response_headers_ts_ms: timing.response_headers_ts_ms,
        invocation_elapsed_us: timing.invocation_elapsed_us,
        response_headers_elapsed_us: timing.response_headers_elapsed_us,
        response_body_elapsed_us: timing.response_body_elapsed_us,
        http_status: timing.http_status,
        applied_us,
        decision_us,
        construction_us,
        signing_to_submit_us,
        signed_to_server_time_us,
        server_time_request_us,
        server_time_to_submit_us,
        submit_to_headers_us,
        response_body_interpreted_us,
        receipt_admission_us,
        market_to_decision_us,
        market_to_submit_us,
        total_reaction_us,
        timing_missing_phases: missing,
        timing_reversed_phases: reversed,
    }));
}

pub(super) fn checked_phase_us(
    phase: &str,
    start: Option<Instant>,
    end: Option<Instant>,
    missing: &mut Vec<String>,
    reversed: &mut Vec<String>,
) -> Option<u64> {
    let (Some(start), Some(end)) = (start, end) else {
        missing.push(phase.to_string());
        return None;
    };
    match end.checked_duration_since(start) {
        Some(duration) => Some(duration_to_us(duration)),
        None => {
            reversed.push(phase.to_string());
            None
        }
    }
}

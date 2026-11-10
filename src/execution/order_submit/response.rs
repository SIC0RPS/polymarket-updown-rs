use super::super::client::{ExecutionBatchSendOutcome, ExecutionPostTiming, IndexedExecutionAck};
use super::super::http_support::RoutedPostEntry;
use super::super::*;

const VENUE_ERROR_MAX_BYTES: usize = 2_048;
const VENUE_ERROR_TRUNCATION_MARKER: &str = "...[truncated]";

pub(super) fn reduce_batch_responses(
    mut known: Vec<IndexedExecutionAck>,
    responses: Vec<RoutedPostEntry>,
    chunk: &[OrderIntent],
    chunk_start: usize,
    intents_len: usize,
    request_ts_ms: u64,
    response_ts_ms: u64,
    mut post_timings: Vec<ExecutionPostTiming>,
) -> ExecutionBatchSendOutcome {
    let response_count = responses.len();
    let chunk_end = chunk_start.saturating_add(chunk.len());
    if response_count != chunk.len() {
        let error = format!(
            "response count mismatch after transport interpretation: expected {}, received {response_count}",
            chunk.len()
        );
        record_attempt_interpretation_error(&mut post_timings, error.clone());
        return ExecutionBatchSendOutcome::Unknown {
            known,
            unknown: (chunk_start..chunk_end).collect(),
            unsent: chunk_end..intents_len,
            post_timings,
            error: anyhow::anyhow!(
                "POST /orders {error} request_ts_ms={request_ts_ms} response_ts_ms={response_ts_ms}"
            ),
        };
    }

    let mut unknown = Vec::new();
    let mut interpretation_errors = Vec::new();
    for (offset, response) in responses.into_iter().enumerate() {
        let input_index = chunk_start.saturating_add(offset);
        let intent = &chunk[offset];
        let response = match response {
            RoutedPostEntry::Decoded(response) => response,
            RoutedPostEntry::Undecodable { error } => {
                unknown.push(input_index);
                interpretation_errors.push(error);
                continue;
            }
        };
        let Some(accepted) = post_order_response_acceptance(&response) else {
            let venue_error = response.error_msg.as_deref().map(bounded_venue_error);
            let has_error = response
                .error_msg
                .as_deref()
                .is_some_and(|message| !message.trim().is_empty());
            let has_order_id = !response.order_id.trim().is_empty();
            let error = format!(
                "inconsistent response: input_index={input_index} response_idx={offset} success={} has_error={has_error} has_order_id={has_order_id} order_id={:?} status={:?} venue_error={venue_error:?}",
                response.success,
                response.order_id,
                response.status,
            );
            tracing::warn!(
                target: "execution.submit",
                batch_size = chunk.len(),
                response_idx = offset,
                input_index,
                response_success = response.success,
                response_has_error = has_error,
                response_has_order_id = has_order_id,
                response_order_id = ?response.order_id,
                status = ?response.status,
                response_error = ?venue_error,
                "inconsistent_batch_response: acceptance claim and order identity disagree"
            );
            unknown.push(input_index);
            interpretation_errors.push(error);
            continue;
        };
        let reject = if accepted {
            None
        } else {
            Some(decode_reject(intent, response.error_msg.as_deref()))
        };
        if accepted {
            tracing::info!(
                instrument = intent.instrument.as_str(),
                side = ?intent.side(),
                qty = intent.qty,
                limit_px = intent.limit_px,
                post_only = intent.post_only,
                order_id = %response.order_id,
                status = ?response.status,
                "polymarket order accepted"
            );
        } else if matches!(reject, Some(RejectKind::AggressiveNoMatch)) {
            tracing::info!(
                instrument = intent.instrument.as_str(),
                side = ?intent.side(),
                qty = intent.qty,
                limit_px = intent.limit_px,
                post_only = intent.post_only,
                order_id = %response.order_id,
                status = ?response.status,
                error_msg = ?response.error_msg,
                "polymarket aggressive order missed available liquidity"
            );
        } else {
            tracing::warn!(
                target: "execution.submit",
                instrument = intent.instrument.as_str(),
                side = ?intent.side(),
                qty = intent.qty,
                limit_px = intent.limit_px,
                post_only = intent.post_only,
                order_id = %response.order_id,
                status = ?response.status,
                error_msg = ?response.error_msg,
                "polymarket_order_rejected_by_venue"
            );
        }

        known.push(IndexedExecutionAck {
            input_index,
            ack: ExecutionAck {
                status: Some(response.status.to_string()),
                order_id: accepted.then_some(response.order_id),
                accepted,
                error_msg: response.error_msg,
                reject_kind: reject,
                request_ts_ms: Some(request_ts_ms),
                response_ts_ms: Some(response_ts_ms),
            },
        });
    }

    if !unknown.is_empty() {
        let interpretation_error = interpretation_errors.join("; ");
        record_attempt_interpretation_error(&mut post_timings, interpretation_error.clone());
        return ExecutionBatchSendOutcome::Unknown {
            known,
            unknown: unknown.clone(),
            unsent: chunk_end..intents_len,
            post_timings,
            error: anyhow::anyhow!(
                "POST /orders left submitted indices {unknown:?} unresolved: {interpretation_error} request_ts_ms={request_ts_ms} response_ts_ms={response_ts_ms}"
            ),
        };
    }

    ExecutionBatchSendOutcome::Complete {
        known,
        post_timings,
    }
}

fn record_attempt_interpretation_error(
    post_timings: &mut [ExecutionPostTiming],
    interpretation_error: String,
) {
    if let Some(timing) = post_timings.last_mut() {
        timing.attempt_result = "response_partially_interpreted".to_string();
        timing.interpretation_error = Some(interpretation_error);
    }
}

fn bounded_venue_error(message: &str) -> std::borrow::Cow<'_, str> {
    if message.len() <= VENUE_ERROR_MAX_BYTES {
        return std::borrow::Cow::Borrowed(message);
    }

    let mut end = VENUE_ERROR_MAX_BYTES - VENUE_ERROR_TRUNCATION_MARKER.len();
    while !message.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut bounded = String::with_capacity(VENUE_ERROR_MAX_BYTES);
    bounded.push_str(&message[..end]);
    bounded.push_str(VENUE_ERROR_TRUNCATION_MARKER);
    std::borrow::Cow::Owned(bounded)
}

fn post_order_response_acceptance(response: &PostOrderResponse) -> Option<bool> {
    let has_error = response
        .error_msg
        .as_deref()
        .is_some_and(|message| !message.trim().is_empty());
    let has_order_identity = !response.order_id.trim().is_empty();
    match (response.success, has_error, has_order_identity) {
        (true, false, true) => Some(true),
        (false, _, false) => Some(false),
        _ => None,
    }
}

fn decode_reject(intent: &OrderIntent, msg: Option<&str>) -> RejectKind {
    let lower = msg.unwrap_or_default().to_ascii_lowercase();
    if intent.time_in_force.is_immediate()
        && (lower.contains("no orders found to match with fak order")
            || lower.contains("order couldn't be fully filled")
            || lower.contains("fok_order_not_filled_error")
            || lower.contains("fok")
                && (lower.contains("not filled") || lower.contains("couldn't be fully filled")))
    {
        RejectKind::AggressiveNoMatch
    } else if lower.contains("market is not yet ready")
        || lower.contains("not yet ready to process new orders")
        || lower.contains("market_not_ready")
        || lower.contains("order timed out")
    {
        RejectKind::MarketNotReady
    } else if lower.contains("orderbook") && lower.contains("does not exist") {
        RejectKind::NoOrderbook
    } else if (lower.contains("fee") && lower.contains("rate"))
        || lower.contains("feeratebps")
        || lower.contains("fee rate bps")
    {
        RejectKind::FeeRate
    } else if lower.contains("tick")
        || lower.contains("price increment")
        || lower.contains("min tick")
        || lower.contains("minimum tick")
    {
        RejectKind::TickSize
    } else if lower.contains("balance")
        || lower.contains("allowance")
        || lower.contains("insufficient")
        || lower.contains("collateral")
        || lower.contains("funds")
        || lower.contains("reserve")
    {
        RejectKind::Balance
    } else if lower.contains("post-only")
        || lower.contains("post only")
        || lower.contains("post_only")
        || lower.contains("postonly")
        || lower.contains("would cross")
        || lower.contains("crossing")
        || lower.contains("crosses book")
    {
        RejectKind::PostOnly
    } else if lower.contains("invalid_price") || lower.contains("invalid price") {
        RejectKind::InvalidPrice
    } else {
        RejectKind::Other
    }
}

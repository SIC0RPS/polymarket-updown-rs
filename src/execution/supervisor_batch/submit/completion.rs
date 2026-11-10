use super::super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "execution completion owner plumbing"
)]
pub(in crate::execution::supervisor) fn apply_submission_completion(
    receipt_now_ms: u64,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    market_projection: Option<&crate::polymarket::state::MarketProjection>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    hard_submit_block: &mut Option<String>,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
    submission_recovery: &mut SubmissionRecovery,
    completion: ActiveSubmissionCompletion,
) -> Result<()> {
    let ActiveSubmissionCompletion {
        intents: to_place,
        decision_ts_ms,
        supervisor_admission_ts_ms,
        decision_instant,
        handoff_instant,
        oldest_pending_intent_age_us,
        reservation_ids,
        outcome: send_result,
    } = completion;
    let (acks, unknown, definitive, unsent, terminal_error, post_timings, outcome_stage) =
        match send_result {
            ExecutionBatchSendOutcome::Complete {
                known,
                post_timings,
            } => (
                known,
                None,
                None,
                None,
                None,
                post_timings,
                "transport_complete",
            ),
            ExecutionBatchSendOutcome::Unknown {
                known,
                unknown,
                unsent,
                post_timings,
                error,
            } => {
                tracing::error!(
                    target: "execution.supervisor_batch",
                    error = %format!("{error:#}"),
                    unknown_indices = ?unknown,
                    unsent_start = unsent.start,
                    unsent_end = unsent.end,
                    "order_submission_outcome_unknown"
                );
                (
                    known,
                    Some(unknown),
                    None,
                    Some(unsent),
                    Some(error),
                    post_timings,
                    "transport_unknown",
                )
            }
            ExecutionBatchSendOutcome::DefinitiveFailure {
                known,
                failed,
                unsent,
                post_timings,
                error,
            } => (
                known,
                None,
                Some(failed),
                Some(unsent),
                Some(error),
                post_timings,
                "transport_definitive_failure",
            ),
        };
    let owner_completion_instant = Instant::now();
    emit_submission_transport_timings(
        capture,
        decision_ts_ms,
        supervisor_admission_ts_ms,
        decision_instant,
        handoff_instant,
        owner_completion_instant,
        oldest_pending_intent_age_us,
        outcome_stage,
        &to_place,
        &post_timings,
    );
    if let Some(indices) = unknown.as_ref() {
        for &index in indices {
            if let Some(intent) = to_place.get(index) {
                submission_recovery.mark_unknown(std::slice::from_ref(intent));
            }
        }
    }
    if let Some(range) = unsent.as_ref() {
        for index in range.clone() {
            let intent = &to_place[index];
            if submission_recovery.recovered_order_id(intent).is_some() {
                continue;
            }
            submission_recovery.remove_intent(intent);
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                intent,
                "definitively_unsent",
                receipt_now_ms,
                execution_audit_records,
            )?;
            order_lifecycle.record_terminal(open_orders, intent, receipt_now_ms);
            risk.release_inflight_order(&reservation_ids[index]);
        }
    }
    if let Some(range) = definitive.as_ref() {
        let error = terminal_error
            .as_ref()
            .expect("definitive submission failure has an error");
        let error_text = format!("{error:#}");
        for index in range.clone() {
            let intent = &to_place[index];
            if submission_recovery.recovered_order_id(intent).is_some() {
                continue;
            }
            if let Some(fact) = build_order_terminal_fact(
                intent,
                None,
                OrderTerminalProvenance::SubmissionRejected,
                None,
                None,
                Some(&error_text),
                Some("execution_send_batch_error"),
                hard_submit_reject_reason(error),
                receipt_now_ms,
            )? {
                execution_audit_records.push(build_order_terminal_record(fact)?);
            }
            order_lifecycle.record_terminal(open_orders, intent, receipt_now_ms);
            submission_recovery.remove_intent(intent);
            risk.release_inflight_order(&reservation_ids[index]);
        }
    }
    if let Some(err) = terminal_error.as_ref() {
        if definitive.as_ref().is_some_and(|range| !range.is_empty()) {
            if err.chain().any(|cause| {
                cause
                    .downcast_ref::<polymarket_client_sdk::error::Error>()
                    .is_some_and(|error| {
                        crate::execution::http_support::http_status_code(error) == Some(403)
                    })
            }) {
                obs.emit_action(ActionEvent::RuntimeIssue {
                    ts_ms: receipt_now_ms,
                    scope: "execution.order_submit".to_string(),
                    kind: crate::observability::action::RuntimeIssueKind::GeographicRestriction,
                    msg: Some(
                        "GeographicRestriction: Polymarket rejected order submission with HTTP 403"
                            .to_string(),
                    ),
                    detail: BTreeMap::from([("http_status".to_string(), "403".to_string())]),
                });
            }
            if let Some(reason) = hard_submit_reject_reason(err) {
                let err_msg = format!("{err:#}");
                *hard_submit_block = Some(reason.to_string());
                emit_local_order_block(
                    obs,
                    capture,
                    receipt_now_ms,
                    format!(
                        "local_order_block hard_submit_blocked reason={} rejected_intents={}",
                        reason,
                        definitive.as_ref().map_or(0, |range| range.len())
                    ),
                );
                tracing::error!(
                    target: "execution.supervisor_batch",
                    reason,
                    rejected_intents = definitive.as_ref().map_or(0, |range| range.len()),
                    error = %err_msg,
                    "hard_submit_reject_trip"
                );
            }
        }
    }

    for indexed_ack in acks {
        let index = indexed_ack.input_index;
        let effective_intent = to_place
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("submission result index {index} is out of bounds"))?;
        let reservation_id = reservation_ids.get(index).cloned().ok_or_else(|| {
            anyhow::anyhow!("submission reservation index {index} is out of bounds")
        })?;
        let effective_ack = indexed_ack.ack;
        if !effective_ack.accepted {
            anyhow::ensure!(
                submission_recovery
                    .recovered_order_id(&effective_intent)
                    .is_none(),
                "HTTP rejection contradicts authenticated trade acceptance intent_id={:?} order_id={}",
                effective_intent.execution_intent_id,
                submission_recovery
                    .recovered_order_id(&effective_intent)
                    .unwrap_or("-")
            );
            risk.release_inflight_order(&reservation_id);
            submission_recovery.remove_intent(&effective_intent);
            let raw_error_msg = effective_ack
                .error_msg
                .as_deref()
                .map(str::trim)
                .filter(|message| !message.is_empty());
            let err_msg = raw_error_msg.unwrap_or("order_rejected").to_string();
            let reject_kind = effective_ack.reject_kind.unwrap_or(RejectKind::Other);
            let expected_aggressive_no_match = matches!(reject_kind, RejectKind::AggressiveNoMatch);
            if let Some(fact) = build_order_terminal_fact(
                &effective_intent,
                effective_ack.order_id.as_deref(),
                OrderTerminalProvenance::SubmissionRejected,
                effective_ack.status.as_deref(),
                None,
                raw_error_msg,
                raw_error_msg.map(|_| "execution_ack_error_msg"),
                Some(reject_kind_label(reject_kind)),
                receipt_now_ms,
            )? {
                let record = build_order_terminal_record(fact)?;
                execution_audit_records.push(record);
            }
            if expected_aggressive_no_match {
                order_lifecycle.record_no_fill(open_orders, &effective_intent, receipt_now_ms);
            } else {
                order_lifecycle.record_terminal(open_orders, &effective_intent, receipt_now_ms);
            }
            if matches!(reject_kind, RejectKind::PostOnly) {
                register_post_only_reject(
                    post_only_rejects,
                    &effective_intent,
                    strategy_cfg,
                    market_projection,
                    receipt_now_ms,
                );
            }
            if expected_aggressive_no_match {
                tracing::info!(
                    target: "execution.supervisor_batch",
                    token = %effective_intent.instrument,
                    side = ?effective_intent.side(),
                    px = effective_intent.limit_px,
                    qty = effective_intent.qty,
                    close_hint = effective_intent.order_close_reason_str().unwrap_or("-"),
                    error = %err_msg,
                    "aggressive_no_match"
                );
            } else if matches!(
                reject_kind,
                RejectKind::PostOnly | RejectKind::InvalidPrice | RejectKind::TickSize
            ) {
                tracing::warn!(
                    target: "execution.supervisor_batch",
                    reject_kind = ?reject_kind,
                    error = %err_msg,
                    "order_rejected_operational"
                );
            } else {
                tracing::error!(
                    target: "execution.supervisor_batch",
                    reject_kind = ?reject_kind,
                    error = %err_msg,
                    "order_rejected"
                );
            }
            if let Some(rule) = match reject_kind {
                RejectKind::FeeRate => Some("fee_rate_rejection"),
                RejectKind::TickSize => Some("tick_size_rejection"),
                _ => None,
            } {
                risk.freeze(
                    rule,
                    format!(
                        "token={} side={:?} msg={err_msg}",
                        effective_intent.instrument,
                        effective_intent.side()
                    ),
                );
            }
            capture.emit(CaptureMsg::OrderSubmitReject(OrderSubmitRejectCapture {
                session_ts_ms: effective_ack.response_ts_ms.unwrap_or(receipt_now_ms),
                market_id: intent_market_id(&effective_intent).map(|market| market.to_string()),
                token: effective_intent.instrument.clone(),
                side: match effective_intent.side() {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                }
                .to_string(),
                close_hint: effective_intent
                    .order_close_reason_str()
                    .map(str::to_string),
                qty: effective_intent.qty,
                limit_px: effective_intent.limit_px,
                reject_kind: reject_kind_label(reject_kind).to_string(),
                error_msg: err_msg,
                request_ts_ms: effective_ack.request_ts_ms,
                response_ts_ms: effective_ack.response_ts_ms,
            }));
            continue;
        }
        register_clean_post_only_ack(
            post_only_rejects,
            &effective_intent,
            strategy_cfg,
            receipt_now_ms,
        );

        let Some(order_id) = effective_ack.order_id else {
            if effective_intent.buy_authorization().is_some() {
                submission_recovery.mark_unknown(std::slice::from_ref(&effective_intent));
                tracing::error!(target: "execution.supervisor_batch",
                    "accepted_buy_missing_order_identity_remains_unresolved");
                continue;
            }
            order_lifecycle.record_terminal(open_orders, &effective_intent, receipt_now_ms);
            risk.release_inflight_order(&reservation_id);
            submission_recovery.remove_intent(&effective_intent);
            tracing::warn!(
                target: "execution.supervisor_batch",
                "order_submitted_no_order_id"
            );
            continue;
        };
        let execution_timing = order_execution_timing_for_input(
            &effective_intent,
            index,
            order_id.as_str(),
            decision_ts_ms,
            supervisor_admission_ts_ms,
            &post_timings,
        );
        risk.bind_inflight_order(&reservation_id, order_id.clone());
        let already_recovered = submission_recovery
            .known_ack_already_recovered(&effective_intent, order_id.as_str())?;

        obs.emit_action(ActionEvent::OrderAck {
            ts_ms: receipt_now_ms,
            order_id: order_id.clone(),
        });
        if let (Some(request_ts_ms), Some(response_ts_ms)) =
            (effective_ack.request_ts_ms, effective_ack.response_ts_ms)
        {
            let mut timing_missing_phases = Vec::new();
            let mut timing_reversed_phases = Vec::new();
            let signed_to_server_time_us = checked_phase_us(
                "signed_to_server_time",
                execution_timing.signed_order_completed_at,
                execution_timing.server_time_request_started_at,
                &mut timing_missing_phases,
                &mut timing_reversed_phases,
            );
            let server_time_request_us = checked_phase_us(
                "server_time_request",
                execution_timing.server_time_request_started_at,
                execution_timing.server_time_request_completed_at,
                &mut timing_missing_phases,
                &mut timing_reversed_phases,
            );
            let server_time_to_submit_us = checked_phase_us(
                "server_time_to_submit",
                execution_timing.server_time_request_completed_at,
                execution_timing.submit_invocation_at,
                &mut timing_missing_phases,
                &mut timing_reversed_phases,
            );
            let latency = OrderSubmitLatencyCapture {
                session_ts_ms: response_ts_ms,
                decision_ts_ms,
                supervisor_admission_ts_ms,
                stage: "order_ack".to_string(),
                attempt: execution_timing.attempt.unwrap_or_default(),
                attempt_result: None,
                interpretation_error: None,
                response_evidence: None,
                order_id: order_id.clone(),
                canonical_receipt_id: None,
                market_id: intent_market_id(&effective_intent).map(|market| market.to_string()),
                token: effective_intent.instrument.clone(),
                side: match effective_intent.side() {
                    Side::Buy => "buy",
                    Side::Sell => "sell",
                }
                .to_string(),
                close_hint: effective_intent
                    .order_close_reason_str()
                    .map(str::to_string),
                submit_status: effective_ack.status.clone(),
                qty: Some(effective_intent.qty),
                limit_px: Some(effective_intent.limit_px),
                market_connection_session_id: effective_intent.market_connection_session_id,
                market_frame_sequence: effective_intent.market_frame_sequence,
                market_state_revision: effective_intent.market_state_revision,
                strategy_consumption_sequence: effective_intent.strategy_consumption_sequence,
                strategy_input_kind: effective_intent.strategy_input_kind.map(str::to_string),
                execution_intent_id: effective_intent.execution_intent_id,
                input_index: execution_timing.input_index,
                input_range_start: execution_timing
                    .input_range
                    .as_ref()
                    .map(|range| range.start),
                input_range_end: execution_timing.input_range.as_ref().map(|range| range.end),
                request_ts_ms,
                decision_to_request_ms: request_ts_ms.saturating_sub(decision_ts_ms),
                decision_to_admission_ms: supervisor_admission_ts_ms.saturating_sub(decision_ts_ms),
                admission_to_request_ms: request_ts_ms.saturating_sub(supervisor_admission_ts_ms),
                response_ts_ms,
                ack_latency_ms: response_ts_ms.saturating_sub(request_ts_ms),
                decision_to_handoff_us: decision_instant
                    .zip(handoff_instant)
                    .and_then(|(start, end)| end.checked_duration_since(start))
                    .map(duration_to_us),
                handoff_to_send_invocation_us: None,
                send_invocation_to_response_us: None,
                response_to_owner_us: None,
                oldest_pending_intent_age_us,
                response_headers_ts_ms: None,
                invocation_elapsed_us: None,
                response_headers_elapsed_us: None,
                response_body_elapsed_us: None,
                http_status: None,
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
                timing_missing_phases,
                timing_reversed_phases,
            };
            capture.emit(CaptureMsg::OrderSubmitLatency(latency));
        }
        if already_recovered {
            if execution_timing.attempt.is_some() {
                order_fill_ctx.refresh_order_execution_timing(
                    order_id.as_str(),
                    effective_intent.execution_intent_id,
                    execution_timing,
                );
            }
            continue;
        }
        let accepted_order_record = record_accepted_order_submission_with_timing(
            order_fill_ctx,
            &effective_intent,
            order_id.as_str(),
            receipt_now_ms,
            execution_timing,
        );
        let mut terminal_record = None;
        let submit_unmatched = effective_intent.time_in_force.is_immediate()
            && effective_ack
                .status
                .as_deref()
                .is_some_and(|status| status.trim().eq_ignore_ascii_case("unmatched"));
        order_lifecycle.record_submitted(
            open_orders,
            &effective_intent,
            order_id.as_str(),
            receipt_now_ms,
        );
        let (pre_accept_terminal, pre_accept_matched, pre_accept_fact) = order_lifecycle
            .bind_pre_accept_update(
                open_orders,
                &effective_intent,
                order_id.as_str(),
                receipt_now_ms,
            )?;
        if let Some(fact) = pre_accept_fact {
            terminal_record = Some(build_order_terminal_record(fact)?);
        }
        if submit_unmatched {
            if !pre_accept_matched {
                order_lifecycle.record_no_fill(open_orders, &effective_intent, receipt_now_ms);
                order_lifecycle.note_execution_complete(order_id.as_str());
                if terminal_record.is_none() {
                    if let Some(fact) = build_order_terminal_fact(
                        &effective_intent,
                        Some(order_id.as_str()),
                        OrderTerminalProvenance::SubmissionUnmatched,
                        effective_ack.status.as_deref(),
                        None,
                        None,
                        None,
                        None,
                        receipt_now_ms,
                    )? {
                        terminal_record = Some(build_order_terminal_record(fact)?);
                    }
                }
            }
            risk.release_inflight_order(&reservation_id);
        }
        promote_accepted_order(
            receipt_now_ms,
            open_orders,
            live_quote_cancel_index,
            seen_quote_cancel_keys,
            &effective_intent,
            order_id.as_str(),
            effective_ack.status.as_deref(),
            pre_accept_terminal,
        );
        if effective_intent.buy_authorization().is_none()
            && open_orders.orders.contains_key(order_id.as_str())
        {
            risk.release_inflight_order(&reservation_id);
        }
        let accepted_order_record = accepted_order_record?;
        submission_recovery.note_accepted_audit_pending(order_id.as_str());
        execution_audit_records.push(accepted_order_record);
        if let Some(record) = terminal_record {
            execution_audit_records.push(record);
        }
    }
    Ok(())
}

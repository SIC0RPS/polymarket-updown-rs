use super::order_updates::{apply_live_cancel_requests, reconcile_live_order_lifecycle};
use super::*;
use crate::observability::action::RuntimeIssueKind;
use std::collections::BTreeMap;

#[allow(
    clippy::too_many_arguments,
    reason = "current cancellation fact application"
)]
pub(super) fn apply_live_cancellation(
    completion: CancellationCompletion,
    now_ms: u64,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    active_cancellations: &mut ActiveCancellations,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    obs: &Observability,
    submission_recovery: &SubmissionRecovery,
) {
    match completion {
        CancellationCompletion::Orders {
            targets,
            label,
            terminal_status,
            outcome,
        } => {
            let unresolved = match outcome {
                Ok(ack) => {
                    let canceled: HashSet<&str> = ack.canceled.iter().map(String::as_str).collect();
                    let unresolved: Vec<_> = targets
                        .iter()
                        .filter(|target| !canceled.contains(target.order_id.as_str()))
                        .filter(|target| open_orders.orders.contains_key(&target.order_id))
                        .cloned()
                        .collect();
                    apply_cancel_ack(
                        now_ms,
                        risk,
                        ack,
                        open_orders,
                        obs,
                        terminal_status,
                        "cancel_failed",
                    );
                    unresolved
                }
                Err(error) => {
                    tracing::error!(
                        target: "execution.cancel_router",
                        label,
                        error = %error,
                        "cancel_request_error"
                    );
                    targets
                        .iter()
                        .filter(|target| open_orders.orders.contains_key(&target.order_id))
                        .cloned()
                        .collect()
                }
            };
            active_cancellations.retain_scope_heals(
                &unresolved,
                label,
                fenced_scopes,
                healing_scopes,
            );
            active_cancellations.finish_order_targets(&targets);
        }
        CancellationCompletion::Market {
            request,
            attempt,
            targets,
            cancel_outcome,
            verify_outcome,
        } => {
            match cancel_outcome {
                Ok(ack) => {
                    apply_cancel_ack(
                        now_ms,
                        risk,
                        ack,
                        open_orders,
                        obs,
                        "CANCEL_OK",
                        "strategy_cancel_market_failed",
                    );
                }
                Err(error) => {
                    tracing::error!(
                        target: "execution.cancel_router",
                        market = %request.market_id,
                        gamma_market_id = request.gamma_market_id.as_deref().unwrap_or("-"),
                        attempt,
                        error = %error,
                        "cancel_market_error"
                    );
                }
            }
            match verify_outcome {
                Ok(remote_ids) => {
                    let target_ids = targets
                        .iter()
                        .map(|target| target.order_id.clone())
                        .collect();
                    reconcile_local_market_orders(
                        now_ms,
                        risk,
                        open_orders,
                        &request,
                        &remote_ids,
                        Some(&target_ids),
                    );
                    let local_open = open_orders.orders.values().any(|order| {
                        market_identifier_matches(&order.market, &request)
                            && !order.status.eq_ignore_ascii_case("CANCELED")
                    });
                    if remote_ids.is_empty()
                        && !local_open
                        && !submission_recovery
                            .has_unresolved_submission(request.market_id.as_str(), None)
                    {
                        risk.release_market_inflight_buy_notional(&request.market_id);
                        active_cancellations.finish_market(request.market_id.as_str(), true);
                        fenced_scopes.retain(|scope| scope.market != request.market_id.as_str());
                        tracing::info!(
                            target: "execution.cancel_router",
                            market = %request.market_id,
                            gamma_market_id = request.gamma_market_id.as_deref().unwrap_or("-"),
                            attempt,
                            "cancel_market_verify_ok"
                        );
                        return;
                    }
                    obs.emit_action(ActionEvent::RuntimeIssue {
                        ts_ms: now_ms,
                        scope: "execution".to_string(),
                        kind: RuntimeIssueKind::StrategyCancelMarketVerifyNonEmpty,
                        msg: Some(format!(
                            "strategy_cancel_market_verify_non_empty market={} gamma_market_id={} attempt={attempt} open_count={}",
                            request.market_id,
                            request.gamma_market_id.as_deref().unwrap_or("-"),
                            remote_ids.len()
                        )),
                        detail: BTreeMap::from([
                            ("label".to_string(), "strategy_cancel_market".to_string()),
                            ("market".to_string(), request.market_id.to_string()),
                            (
                                "gamma_market_id".to_string(),
                                request.gamma_market_id.as_deref().unwrap_or("-").to_string(),
                            ),
                            ("attempt".to_string(), attempt.to_string()),
                            ("open_count".to_string(), remote_ids.len().to_string()),
                        ]),
                    });
                }
                Err(error) => {
                    tracing::error!(
                        target: "execution.cancel_router",
                        market = %request.market_id,
                        gamma_market_id = request.gamma_market_id.as_deref().unwrap_or("-"),
                        attempt,
                        error = %error,
                        "cancel_market_verify_error"
                    );
                }
            }
            if attempt < INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS {
                active_cancellations.retry_market(request, attempt + 1);
            } else {
                let remaining_ids: Vec<_> = open_orders
                    .orders
                    .values()
                    .filter(|order| market_identifier_matches(&order.market, &request))
                    .map(|order| order.order_id.clone())
                    .collect();
                let remaining_targets =
                    ActiveCancellations::targets_for_ids(open_orders, &remaining_ids);
                active_cancellations.finish_market(request.market_id.as_str(), false);
                active_cancellations.retain_scope_heals(
                    &remaining_targets,
                    "strategy_cancel_market",
                    fenced_scopes,
                    healing_scopes,
                );
                tracing::error!(
                    target: "execution.cancel_router",
                    market = %request.market_id,
                    gamma_market_id = request.gamma_market_id.as_deref().unwrap_or("-"),
                    attempts = INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS,
                    "cancel_market_verify_timeout"
                );
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "current scoped verification application"
)]
pub(super) fn apply_live_heal_result(
    result: ScopedCancelHealResult,
    now_ms: u64,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    active_cancellations: &mut ActiveCancellations,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    integrity_block: &mut Option<String>,
    obs: &Observability,
    submission_recovery: &SubmissionRecovery,
) {
    let Some(target_ids) = active_cancellations.take_heal_targets(&result.scope) else {
        healing_scopes.remove(&result.scope);
        fenced_scopes.insert(result.scope.clone());
        *integrity_block = Some(format!(
            "cancel heal result has no retained target scope market={} token={}",
            result.scope.market, result.scope.token
        ));
        return;
    };
    let has_unresolved_submission = submission_recovery
        .has_unresolved_submission(&result.scope.market, Some(&result.scope.token));
    if let Err(error) = apply_scoped_cancel_heal_result(
        now_ms,
        risk,
        open_orders,
        obs,
        healing_scopes,
        fenced_scopes,
        result,
        Some(&target_ids),
        has_unresolved_submission,
    ) {
        tracing::error!(
            target: "execution.supervisor_loop",
            error = %format!("{error:#}"),
            "cancel_scope_heal_result_failed"
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "original replacement continuation under current owners"
)]
pub(super) async fn submit_live_replacements(
    limit: usize,
    owner_ready: bool,
    pause: bool,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    clock: &ExecutionSupervisorClock,
    cfg: &AppConfig,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    shared_strategy_cfg: &SharedStrategyConfig,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
    market_projection_rx: Option<&watch::Receiver<MarketProjection>>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    fenced_scopes: &HashSet<ScopedOrderFenceKey>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    hard_submit_block: &mut Option<String>,
    active_cancellations: &mut ActiveCancellations,
    active_submissions: &mut ActiveSubmissions,
    submission_recovery: &mut SubmissionRecovery,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
) -> bool {
    let mut did_work = false;
    for _ in 0..limit {
        let Some(replacement) = active_cancellations.take_ready_replacement() else {
            break;
        };
        let now_ms = clock.now_ms();
        if open_orders.has_pending_confirmed_cancel() {
            reconcile_live_order_lifecycle(
                risk,
                &portfolio_rx.borrow(),
                open_orders,
                order_lifecycle,
                order_fill_ctx,
                now_ms,
                pending_audit_records,
                integrity_block,
            );
        }
        let mut records = Vec::new();
        let result = if owner_ready && integrity_block.is_none() {
            let strategy_cfg = live_strategy_cfg(shared_strategy_cfg);
            let market_projection = snapshot_market_projection(market_projection_rx);
            submit_ready_orders(
                now_ms,
                replacement.decision_ts_ms,
                replacement.decision_instant,
                replacement.handoff_instant,
                clock,
                exec,
                risk,
                ctf_inventory_runtime,
                order_fill_ctx,
                &strategy_cfg,
                odds_rx,
                market_projection.as_ref(),
                open_orders,
                order_lifecycle,
                live_quote_cancel_index,
                seen_quote_cancel_keys,
                fenced_scopes,
                post_only_rejects,
                obs,
                capture,
                open_orders_tx,
                hard_submit_block,
                cfg.polymarket.execution.order_type,
                cfg.risk.order_timeout_ms,
                replacement.intents,
                &mut records,
                Some(active_submissions),
                submission_recovery,
                pause,
                runtime_control_rx,
                Some(portfolio_rx),
                Some(active_cancellations),
            )
            .await
        } else {
            record_batch_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &replacement.intents,
                "execution_owner_unavailable",
                now_ms,
                &mut records,
            )
        };
        if let Err(error) = pending_audit_records.extend(records, order_fill_ctx, order_lifecycle) {
            *integrity_block = Some(format!("{error:#}"));
        }
        if let Err(error) = result {
            let text = format!("{error:#}");
            tracing::error!(
                target: "execution.supervisor_loop",
                error = %text,
                "cancel_replacement_continuation_failed"
            );
            *integrity_block = Some(text);
        }
        publish_open_orders(open_orders_tx, obs, open_orders);
        did_work = true;
    }
    did_work
}

#[allow(
    clippy::too_many_arguments,
    reason = "existing live cancellation progress boundary"
)]
pub(super) async fn progress_live_cancellations(
    limit: usize,
    upstream_requested: bool,
    pause: bool,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    clock: &ExecutionSupervisorClock,
    cfg: &AppConfig,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    shared_strategy_cfg: &SharedStrategyConfig,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
    market_projection_rx: Option<&watch::Receiver<MarketProjection>>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    scoped_heal_result_rx: &mut mpsc::UnboundedReceiver<ScopedCancelHealResult>,
    cancel_requests_rx: &mut mpsc::UnboundedReceiver<Vec<CancelRequest>>,
    pending_heal_results: &mut VecDeque<ScopedCancelHealResult>,
    pending_cancel_requests: &mut VecDeque<CancelRequest>,
    pending_completion: &mut Option<CancellationCompletion>,
    active_cancellations: &mut ActiveCancellations,
    active_submissions: &mut ActiveSubmissions,
    availability: &mut LiveInputAvailability,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    hard_submit_block: &mut Option<String>,
    submission_recovery: &mut SubmissionRecovery,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
) -> bool {
    let mut did_work = false;
    for _ in 0..limit {
        let completion = pending_completion.take().or_else(|| {
            active_cancellations
                .next_completion()
                .now_or_never()
                .flatten()
        });
        let Some(completion) = completion else { break };
        apply_live_cancellation(
            completion,
            clock.now_ms(),
            risk,
            open_orders,
            active_cancellations,
            fenced_scopes,
            healing_scopes,
            obs,
            submission_recovery,
        );
        did_work = true;
    }
    for _ in 0..limit {
        let result = pending_heal_results.pop_front().or_else(|| {
            availability
                .heal_results
                .then(|| match scoped_heal_result_rx.try_recv() {
                    Ok(result) => Some(result),
                    Err(mpsc::error::TryRecvError::Empty) => None,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        availability.heal_results = false;
                        None
                    }
                })
                .flatten()
        });
        let Some(result) = result else { break };
        apply_live_heal_result(
            result,
            clock.now_ms(),
            risk,
            open_orders,
            active_cancellations,
            healing_scopes,
            fenced_scopes,
            integrity_block,
            obs,
            submission_recovery,
        );
        did_work = true;
    }
    if !availability.heal_results {
        did_work |= active_cancellations.clear_unavailable_heals();
    }
    if open_orders.has_pending_confirmed_cancel() {
        reconcile_live_order_lifecycle(
            risk,
            &portfolio_rx.borrow(),
            open_orders,
            order_lifecycle,
            order_fill_ctx,
            clock.now_ms(),
            pending_audit_records,
            integrity_block,
        );
    }
    while availability.cancel_requests && pending_cancel_requests.len() < limit {
        match cancel_requests_rx.try_recv() {
            Ok(batch) => pending_cancel_requests.extend(batch),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                availability.cancel_requests = false;
                break;
            }
        }
    }
    let cancel_count = pending_cancel_requests.len().min(limit);
    if cancel_count > 0 {
        let requests = pending_cancel_requests.drain(..cancel_count).collect();
        apply_live_cancel_requests(
            requests,
            clock,
            exec,
            risk,
            open_orders,
            order_lifecycle,
            order_fill_ctx,
            live_quote_cancel_index,
            seen_quote_cancel_keys,
            fenced_scopes,
            healing_scopes,
            scoped_heal_tx,
            pending_audit_records,
            integrity_block,
            open_orders_tx,
            obs,
            portfolio_rx,
            active_cancellations,
        )
        .await;
        did_work = true;
    }
    if availability.heal_results {
        match active_cancellations.dispatch_ready_heals(
            open_orders,
            active_submissions,
            scoped_heal_tx,
        ) {
            Ok(dispatched) => did_work |= dispatched,
            Err(error) => {
                let text = format!("{error:#}");
                tracing::error!(
                    target: "execution.supervisor_loop",
                    error = %text,
                    "cancel_scope_heal_handoff_failed"
                );
                *integrity_block = Some(text);
            }
        }
    }
    did_work |= active_cancellations.dispatch_ready_markets(exec, open_orders, active_submissions);
    let owner_ready = !upstream_requested
        && availability.pause
        && availability.order_updates
        && availability.heal_results
        && availability.cancel_requests
        && availability.intents
        && availability.portfolio
        && integrity_block.is_none();
    did_work |= submit_live_replacements(
        limit,
        owner_ready,
        pause,
        runtime_control_rx,
        clock,
        cfg,
        exec,
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        shared_strategy_cfg,
        portfolio_rx,
        odds_rx,
        market_projection_rx,
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        fenced_scopes,
        post_only_rejects,
        obs,
        capture,
        open_orders_tx,
        hard_submit_block,
        active_cancellations,
        active_submissions,
        submission_recovery,
        pending_audit_records,
        integrity_block,
    )
    .await;
    if did_work {
        publish_open_orders(open_orders_tx, obs, open_orders);
    }
    did_work
}

#[allow(
    clippy::too_many_arguments,
    reason = "one supervisor upstream completion boundary"
)]
pub(super) fn finish_upstream_work(
    now_ms: u64,
    pending_audit_records: &mut SubmissionAuditQueue,
    submission_recovery: &mut SubmissionRecovery,
    order_fill_ctx: &OrderFillContextRegistry,
    healing_scopes: &HashSet<ScopedOrderFenceKey>,
    open_orders: &OpenOrdersState,
    fenced_scopes: &HashSet<ScopedOrderFenceKey>,
    active_cancellations: &ActiveCancellations,
    integrity_block: &mut Option<String>,
    upstream_done: &mut Option<oneshot::Sender<std::result::Result<(), String>>>,
) {
    if let Err(error) =
        pending_audit_records.persist_shutdown(now_ms, submission_recovery, order_fill_ctx)
    {
        *integrity_block = Some(error);
    }
    let failed = submission_recovery.fail_unresolved_authorizations();
    if failed > 0 {
        *integrity_block = Some(format!(
            "unresolved submission authorization acknowledgments count={failed}"
        ));
    }
    if let Some(done) = upstream_done.take() {
        let unknown_submissions = submission_recovery.unresolved_submissions();
        let outcome = if !healing_scopes.is_empty()
            || !fenced_scopes.is_empty()
            || !open_orders.orders.is_empty()
            || !active_cancellations.is_empty()
            || active_cancellations.has_unresolved_markets()
            || unknown_submissions != 0
        {
            Err(format!(
                "supervisor upstream unresolved: healing_scopes={} fenced_scopes={} open_orders={} pending_cancellations={} unresolved_market_cancellation={} unknown_submissions={}",
                healing_scopes.len(),
                fenced_scopes.len(),
                open_orders.orders.len(),
                !active_cancellations.is_empty(),
                active_cancellations.has_unresolved_markets(),
                unknown_submissions,
            ))
        } else {
            match integrity_block.as_ref() {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            }
        };
        let _ = done.send(outcome);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one supervisor final obligation check"
)]
pub(super) fn finish_supervisor_shutdown(
    now_ms: u64,
    mut upstream_done: Option<oneshot::Sender<std::result::Result<(), String>>>,
    integrity_block: Option<String>,
    pending_audit_records: &mut SubmissionAuditQueue,
    submission_recovery: &mut SubmissionRecovery,
    order_fill_ctx: &OrderFillContextRegistry,
    open_orders: &OpenOrdersState,
    fenced_scopes: &HashSet<ScopedOrderFenceKey>,
    healing_scopes: &HashSet<ScopedOrderFenceKey>,
    active_cancellations: &ActiveCancellations,
) -> Result<()> {
    if let Some(done) = upstream_done.take() {
        let _ = done.send(Err(
            "supervisor stopped before upstream completion".to_string()
        ));
    }
    let audit_result =
        pending_audit_records.persist_shutdown(now_ms, submission_recovery, order_fill_ctx);
    let failed_authorizations = submission_recovery.fail_unresolved_authorizations();
    let unknown_submissions = submission_recovery.unresolved_submissions();
    anyhow::ensure!(
        integrity_block.is_none() && audit_result.is_ok()
            && failed_authorizations == 0 && unknown_submissions == 0
            && open_orders.orders.is_empty() && fenced_scopes.is_empty() && healing_scopes.is_empty()
            && active_cancellations.is_empty() && !active_cancellations.has_unresolved_markets(),
        "supervisor shutdown unresolved: integrity={:?} audit={:?} authorizations={} unknown_submissions={} open_orders={} fenced_scopes={} healing_scopes={} pending_cancellations={} unresolved_market_cancellation={}",
        integrity_block, audit_result.err(), failed_authorizations, unknown_submissions,
        open_orders.orders.len(), fenced_scopes.len(), healing_scopes.len(),
        !active_cancellations.is_empty(), active_cancellations.has_unresolved_markets(),
    );
    Ok(())
}

use super::*;

#[path = "live_wake/submission_completion.rs"]
mod submission_completion;
use submission_completion::finish_live_submission;

#[path = "live_wake/cancellation_completion.rs"]
mod cancellation_completion;
use cancellation_completion::{
    finish_supervisor_shutdown, finish_upstream_work, progress_live_cancellations,
};

#[path = "live_wake/order_updates.rs"]
mod order_updates;
use order_updates::{
    apply_live_order_update, apply_live_pause_cancel, apply_live_timeouts, reconcile_live_portfolio,
};

pub(super) struct LiveSupervisor<'a> {
    pub cfg: &'a AppConfig,
    pub exec: &'a ExecutionClient,
    pub risk: &'a RiskHandle,
    pub ctf_inventory_runtime: &'a Mutex<CtfInventoryRuntime>,
    pub shared_strategy_cfg: &'a SharedStrategyConfig,
    pub order_fill_ctx: &'a OrderFillContextRegistry,
    pub pause: &'a mut bool,
    pub pause_rx: &'a mut watch::Receiver<bool>,
    pub runtime_control_rx: Option<&'a watch::Receiver<RuntimeConfigState>>,
    pub portfolio_rx: &'a mut watch::Receiver<PortfolioState>,
    pub odds_rx: &'a mut Option<watch::Receiver<Arc<PolymarketOddsState>>>,
    pub market_projection_rx: &'a mut Option<watch::Receiver<MarketProjection>>,
    pub order_updates_rx: &'a mut mpsc::Receiver<ExecutionOrderEvent>,
    pub intents_rx: &'a mut mpsc::UnboundedReceiver<ExecutionIntentBatch>,
    pub cancel_requests_rx: &'a mut mpsc::UnboundedReceiver<Vec<CancelRequest>>,
    pub open_orders: &'a mut OpenOrdersState,
    pub order_lifecycle: &'a mut OrderLifecycleSupervisor,
    pub live_quote_cancel_index: &'a mut HashMap<QuoteCancelKey, HashSet<String>>,
    pub seen_quote_cancel_keys: &'a mut HashSet<QuoteCancelKey>,
    pub fenced_scopes: &'a mut HashSet<ScopedOrderFenceKey>,
    pub healing_scopes: &'a mut HashSet<ScopedOrderFenceKey>,
    pub scoped_heal_tx: &'a mpsc::UnboundedSender<ScopedCancelHealRequest>,
    pub scoped_heal_result_rx: &'a mut mpsc::UnboundedReceiver<ScopedCancelHealResult>,
    pub post_only_rejects: &'a mut HashMap<String, PostOnlyRejectState>,
    pub hard_submit_block: &'a mut Option<String>,
    pub observed_portfolio_revision: &'a mut u64,
    pub next_timeout_due_ms: &'a mut u64,
    pub timeout_interval_ms: u64,
    pub startup_block_until_ms: u64,
    pub startup_cooldown_last_log_ms: &'a mut u64,
    pub open_orders_tx: &'a watch::Sender<OpenOrdersState>,
    pub obs: &'a Observability,
    pub capture: &'a MaybeCapture,
    pub shutdown: &'a CancellationToken,
    pub clock: &'a ExecutionSupervisorClock,
    pub submission_recovery: &'a mut SubmissionRecovery,
    pub upstream_stop: &'a CancellationToken,
    pub upstream_done: Option<oneshot::Sender<std::result::Result<(), String>>>,
}

pub(super) async fn run_live_supervisor(ctx: LiveSupervisor<'_>) -> Result<()> {
    const ROUND_CLASS_LIMIT: usize = 256;
    let LiveSupervisor {
        cfg,
        exec,
        risk,
        ctf_inventory_runtime,
        shared_strategy_cfg,
        order_fill_ctx,
        pause,
        pause_rx,
        runtime_control_rx,
        portfolio_rx,
        odds_rx,
        market_projection_rx,
        order_updates_rx,
        intents_rx,
        cancel_requests_rx,
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        fenced_scopes,
        healing_scopes,
        scoped_heal_tx,
        scoped_heal_result_rx,
        post_only_rejects,
        hard_submit_block,
        observed_portfolio_revision,
        next_timeout_due_ms,
        timeout_interval_ms,
        startup_block_until_ms,
        startup_cooldown_last_log_ms,
        open_orders_tx,
        obs,
        capture,
        shutdown,
        clock,
        submission_recovery,
        upstream_stop,
        mut upstream_done,
    } = ctx;
    let mut pending_pause = false;
    let mut pending_order_events = VecDeque::new();
    let mut pending_heal_results = VecDeque::new();
    let mut pending_cancel_requests = VecDeque::new();
    let mut pending_intent_batch = None::<ExecutionIntentBatch>;
    let mut pending_audit_records = SubmissionAuditQueue::new();
    let mut integrity_block: Option<String> = None;
    let mut active_submissions = ActiveSubmissions::new();
    let mut active_cancellations = ActiveCancellations::new();
    let mut pending_submission_completion = None::<ActiveSubmissionCompletion>;
    let mut pending_cancellation_completion = None::<CancellationCompletion>;
    let mut availability = LiveInputAvailability {
        pause: true,
        order_updates: true,
        heal_results: true,
        cancel_requests: true,
        intents: true,
        portfolio: true,
    };

    let mut upstream_requested = false;
    let mut shutdown_requested = false;
    loop {
        if !upstream_requested && (upstream_stop.is_cancelled() || shutdown.is_cancelled()) {
            upstream_requested = true;
            intents_rx.close();
            cancel_requests_rx.close();
            availability.pause = false;
            pending_pause = false;
        }
        if !shutdown_requested && shutdown.is_cancelled() {
            shutdown_requested = true;
            order_updates_rx.close();
        }
        let mut round_did_work = false;
        let mut local_order_fact_applied = false;

        let submission_completion = pending_submission_completion.take().or_else(|| {
            active_submissions
                .next_completion()
                .now_or_never()
                .flatten()
        });
        if let Some(completion) = submission_completion {
            finish_live_submission(
                completion,
                clock.now_ms(),
                risk,
                ctf_inventory_runtime,
                order_fill_ctx,
                shared_strategy_cfg,
                market_projection_rx.as_ref(),
                portfolio_rx,
                open_orders,
                order_lifecycle,
                live_quote_cancel_index,
                seen_quote_cancel_keys,
                post_only_rejects,
                obs,
                capture,
                hard_submit_block,
                submission_recovery,
                &mut pending_audit_records,
                &mut integrity_block,
                open_orders_tx,
            );
            round_did_work = true;
            local_order_fact_applied = true;
        }

        let audit_now_ms = clock.now_ms();
        if integrity_block.is_none() {
            match pending_audit_records.persist_due(
                audit_now_ms,
                ROUND_CLASS_LIMIT,
                submission_recovery,
                order_fill_ctx,
            ) {
                Ok(did_work) => round_did_work |= did_work,
                Err(text) => {
                    tracing::error!(
                        target: "execution.supervisor_loop",
                        error = %text,
                        "execution_audit_permanent_block"
                    );
                    integrity_block = Some(text);
                }
            }
        }

        if availability.pause && pause_rx.has_changed().is_err() {
            availability.pause = false;
            tracing::error!(target: "execution.supervisor_loop", "pause_input_closed");
            round_did_work = true;
        } else if availability.pause && (pending_pause || pause_rx.has_changed().unwrap_or(false)) {
            pending_pause = false;
            let next_pause = *pause_rx.borrow_and_update();
            if next_pause != *pause {
                *pause = next_pause;
                tracing::info!(target: "execution.supervisor_loop", pause = *pause, "execution_pause_updated");
                if *pause {
                    apply_live_pause_cancel(
                        clock,
                        exec,
                        risk,
                        open_orders,
                        order_lifecycle,
                        order_fill_ctx,
                        fenced_scopes,
                        healing_scopes,
                        scoped_heal_tx,
                        &mut pending_audit_records,
                        &mut integrity_block,
                        obs,
                        portfolio_rx,
                        &mut active_cancellations,
                    )
                    .await;
                }
                publish_open_orders(open_orders_tx, obs, open_orders);
                round_did_work = true;
            }
        }

        for _ in 0..ROUND_CLASS_LIMIT {
            let event = pending_order_events.pop_front().or_else(|| {
                availability
                    .order_updates
                    .then(|| match order_updates_rx.try_recv() {
                        Ok(event) => Some(event),
                        Err(mpsc::error::TryRecvError::Empty) => None,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            availability.order_updates = false;
                            None
                        }
                    })
                    .flatten()
            });
            let Some(event) = event else { break };
            match event {
                ExecutionOrderEvent::Update { update, context } => {
                    if let Some(text) = apply_live_order_update(
                        update,
                        context,
                        risk,
                        &portfolio_rx.borrow(),
                        order_fill_ctx,
                        order_lifecycle,
                        open_orders,
                        clock.now_ms(),
                        &mut pending_audit_records,
                    ) {
                        integrity_block = Some(text);
                    }
                    round_did_work = true;
                    local_order_fact_applied = true;
                }
                ExecutionOrderEvent::UnboundTrade { observation, ack } => {
                    match apply_authenticated_trade_event(
                        clock.now_ms(),
                        observation,
                        ack,
                        risk,
                        order_fill_ctx,
                        &portfolio_rx.borrow(),
                        open_orders,
                        order_lifecycle,
                        live_quote_cancel_index,
                        seen_quote_cancel_keys,
                        submission_recovery,
                        &mut pending_audit_records,
                    ) {
                        Ok(applied) => local_order_fact_applied |= applied,
                        Err(error) => {
                            let text = format!("{error:#}");
                            tracing::error!(target: "execution.supervisor_loop", error = %text, "authenticated_trade_recovery_failed");
                            integrity_block = Some(text);
                        }
                    }
                    round_did_work = true;
                }
            }
        }

        let portfolio_watch_changed = if availability.portfolio {
            match portfolio_rx.has_changed() {
                Err(_) => {
                    availability.portfolio = false;
                    tracing::error!(target: "execution.supervisor_loop", "portfolio_input_closed");
                    round_did_work = true;
                    false
                }
                Ok(changed) => changed,
            }
        } else {
            false
        };
        {
            let portfolio = if portfolio_watch_changed {
                portfolio_rx.borrow_and_update()
            } else {
                portfolio_rx.borrow()
            };
            round_did_work |= reconcile_live_portfolio(
                risk,
                &portfolio,
                observed_portfolio_revision,
                local_order_fact_applied,
                open_orders,
                order_lifecycle,
                order_fill_ctx,
                clock.now_ms(),
                &mut pending_audit_records,
                &mut integrity_block,
                open_orders_tx,
                obs,
            );
        }

        if local_order_fact_applied {
            if *pause && !upstream_requested {
                apply_live_pause_cancel(
                    clock,
                    exec,
                    risk,
                    open_orders,
                    order_lifecycle,
                    order_fill_ctx,
                    fenced_scopes,
                    healing_scopes,
                    scoped_heal_tx,
                    &mut pending_audit_records,
                    &mut integrity_block,
                    obs,
                    portfolio_rx,
                    &mut active_cancellations,
                )
                .await;
            }
            publish_open_orders(open_orders_tx, obs, open_orders);
        }

        round_did_work |= progress_live_cancellations(
            ROUND_CLASS_LIMIT,
            upstream_requested,
            *pause,
            runtime_control_rx,
            clock,
            cfg,
            exec,
            risk,
            ctf_inventory_runtime,
            order_fill_ctx,
            shared_strategy_cfg,
            portfolio_rx,
            odds_rx.as_ref(),
            market_projection_rx.as_ref(),
            open_orders,
            order_lifecycle,
            live_quote_cancel_index,
            seen_quote_cancel_keys,
            fenced_scopes,
            healing_scopes,
            scoped_heal_tx,
            scoped_heal_result_rx,
            cancel_requests_rx,
            &mut pending_heal_results,
            &mut pending_cancel_requests,
            &mut pending_cancellation_completion,
            &mut active_cancellations,
            &mut active_submissions,
            &mut availability,
            post_only_rejects,
            obs,
            capture,
            open_orders_tx,
            hard_submit_block,
            submission_recovery,
            &mut pending_audit_records,
            &mut integrity_block,
        )
        .await;

        let intent_batch = pending_intent_batch.take().or_else(|| {
            if !availability.intents {
                return None;
            }
            match intents_rx.try_recv() {
                Ok(batch) => Some(batch),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    availability.intents = false;
                    None
                }
            }
        });
        if let Some(intent_batch) = intent_batch {
            let ExecutionIntentBatch {
                decision_ts_ms,
                decision_instant,
                handoff_instant,
                mut intents,
            } = intent_batch;
            let admission_now_ms = clock.now_ms();
            if admission_now_ms < startup_block_until_ms {
                let (filtered, blocked_count) = filter_buy_entry_intents(
                    intents,
                    true,
                    "startup_cooldown",
                    admission_now_ms,
                    risk,
                    ctf_inventory_runtime,
                    submission_recovery,
                )?;
                intents = filtered;
                if blocked_count > 0
                    && admission_now_ms.saturating_sub(*startup_cooldown_last_log_ms) >= 1_000
                {
                    *startup_cooldown_last_log_ms = admission_now_ms;
                    tracing::info!(
                        target: "execution.supervisor_loop",
                        remaining_ms = startup_block_until_ms.saturating_sub(admission_now_ms),
                        blocked_intents = blocked_count,
                        "startup_cooldown_active"
                    );
                }
            }
            let owner_ready = !upstream_requested
                && availability.pause
                && availability.order_updates
                && availability.heal_results
                && availability.cancel_requests
                && availability.intents
                && availability.portfolio
                && integrity_block.is_none();
            if !intents.is_empty() && !owner_ready {
                emit_execution_owner_block(
                    obs,
                    capture,
                    admission_now_ms,
                    format!(
                        "local_order_block execution_owner_unavailable dropped_intents={}",
                        intents.len()
                    ),
                );
                let mut records = Vec::new();
                if let Err(error) = record_batch_pre_submission_abandoned(
                    risk,
                    ctf_inventory_runtime,
                    submission_recovery,
                    &intents,
                    "execution_owner_unavailable",
                    admission_now_ms,
                    &mut records,
                ) {
                    integrity_block = Some(format!("{error:#}"));
                }
                if let Err(error) =
                    pending_audit_records.extend(records, order_fill_ctx, order_lifecycle)
                {
                    integrity_block = Some(format!("{error:#}"));
                }
            } else if !intents.is_empty() {
                let strategy_cfg = live_strategy_cfg(shared_strategy_cfg);
                let market_projection = snapshot_market_projection(market_projection_rx.as_ref());
                let mut produced_audit_records = Vec::new();
                let result = handle_intents_batch(
                    admission_now_ms,
                    decision_ts_ms,
                    Some(decision_instant),
                    Some(handoff_instant),
                    clock,
                    *pause,
                    runtime_control_rx,
                    exec,
                    risk,
                    ctf_inventory_runtime,
                    order_fill_ctx,
                    &strategy_cfg,
                    portfolio_rx,
                    odds_rx.as_ref(),
                    market_projection.as_ref(),
                    open_orders,
                    order_lifecycle,
                    live_quote_cancel_index,
                    seen_quote_cancel_keys,
                    fenced_scopes,
                    healing_scopes,
                    scoped_heal_tx,
                    post_only_rejects,
                    cfg.polymarket.execution.order_type,
                    cfg.risk.order_timeout_ms,
                    obs,
                    capture,
                    open_orders_tx,
                    hard_submit_block,
                    intents,
                    &mut produced_audit_records,
                    Some(&mut active_submissions),
                    Some(&mut active_cancellations),
                    submission_recovery,
                )
                .await;
                if let Err(error) = pending_audit_records.extend(
                    produced_audit_records,
                    order_fill_ctx,
                    order_lifecycle,
                ) {
                    integrity_block = Some(format!("{error:#}"));
                }
                if let Err(error) = result {
                    let text = format!("{error:#}");
                    tracing::error!(target: "execution.supervisor_loop", error = %text, "handle_intents_batch_failed");
                    integrity_block = Some(text);
                }
                obs.set_post_only_reject_status(current_post_only_reject_status(
                    post_only_rejects,
                    &strategy_cfg,
                    clock.now_ms(),
                ));
                publish_open_orders(open_orders_tx, obs, open_orders);
            }
            round_did_work = true;
        }

        let timeout_check_ms = clock.now_ms();
        if !upstream_requested && timeout_check_ms >= *next_timeout_due_ms {
            apply_live_timeouts(
                timeout_check_ms,
                clock,
                exec,
                risk,
                open_orders,
                order_lifecycle,
                order_fill_ctx,
                fenced_scopes,
                healing_scopes,
                scoped_heal_tx,
                &mut pending_audit_records,
                &mut integrity_block,
                open_orders_tx,
                obs,
                portfolio_rx,
                &mut active_cancellations,
            )
            .await;
            advance_deadline(next_timeout_due_ms, timeout_interval_ms, clock.now_ms());
            round_did_work = true;
        }

        let upstream_finished = upstream_requested
            && !availability.intents
            && pending_intent_batch.is_none()
            && !availability.cancel_requests
            && pending_cancel_requests.is_empty()
            && active_submissions.is_empty()
            && pending_submission_completion.is_none()
            && active_cancellations.is_empty()
            && pending_cancellation_completion.is_none()
            && pending_heal_results.is_empty()
            && (healing_scopes.is_empty() || !availability.heal_results);
        if upstream_finished {
            if upstream_done.is_some() || round_did_work {
                finish_upstream_work(
                    clock.now_ms(),
                    &mut pending_audit_records,
                    submission_recovery,
                    order_fill_ctx,
                    healing_scopes,
                    open_orders,
                    fenced_scopes,
                    &active_cancellations,
                    &mut integrity_block,
                    &mut upstream_done,
                );
            }
        }
        if shutdown_requested
            && upstream_finished
            && !availability.order_updates
            && pending_order_events.is_empty()
        {
            break;
        }

        let audit_due = integrity_block.is_none() && pending_audit_records.is_due(clock.now_ms());
        let ready_inputs = pending_submission_completion.is_some()
            || pending_cancellation_completion.is_some()
            || pending_pause
            || (availability.pause && pause_rx.has_changed().unwrap_or(false))
            || (availability.portfolio && portfolio_rx.has_changed().unwrap_or(false))
            || !pending_order_events.is_empty()
            || (availability.order_updates && !order_updates_rx.is_empty())
            || !pending_heal_results.is_empty()
            || (availability.heal_results && !scoped_heal_result_rx.is_empty())
            || !pending_cancel_requests.is_empty()
            || (availability.cancel_requests && !cancel_requests_rx.is_empty())
            || pending_intent_batch.is_some()
            || (availability.intents && !intents_rx.is_empty());
        if round_did_work
            || ready_inputs
            || audit_due
            || (!upstream_requested && clock.now_ms() >= *next_timeout_due_ms)
        {
            continue;
        }

        let next_wake_ms = (if upstream_requested {
            clock.now_ms().saturating_add(timeout_interval_ms)
        } else {
            *next_timeout_due_ms
        })
        .min(
            (!pending_audit_records.is_empty() && integrity_block.is_none())
                .then_some(pending_audit_records.next_due_ms())
                .unwrap_or(u64::MAX),
        );
        let wake = wait_for_live_supervisor_wake(
            Duration::from_millis(next_wake_ms.saturating_sub(clock.now_ms())),
            &availability,
            pause_rx,
            order_updates_rx,
            scoped_heal_result_rx,
            cancel_requests_rx,
            intents_rx,
            portfolio_rx,
            shutdown,
            shutdown_requested,
            upstream_stop,
            upstream_requested,
            &mut active_submissions,
            &mut active_cancellations,
        )
        .await;
        match wake {
            SupervisorWake::Shutdown | SupervisorWake::UpstreamStop => {}
            SupervisorWake::Pause => pending_pause = true,
            SupervisorWake::InputClosed(name) => {
                tracing::error!(target: "execution.supervisor_loop", input = name, "execution_supervisor_input_closed");
                match name {
                    "pause" => availability.pause = false,
                    "order_updates" => availability.order_updates = false,
                    "scoped_heal_results" => availability.heal_results = false,
                    "cancel_requests" => availability.cancel_requests = false,
                    "intents" => availability.intents = false,
                    "portfolio" => availability.portfolio = false,
                    _ => {}
                }
            }
            SupervisorWake::OrderEvent(event) => pending_order_events.push_back(event),
            SupervisorWake::SubmissionCompleted(completion) => {
                pending_submission_completion = Some(completion);
            }
            SupervisorWake::CancellationCompleted(completion) => {
                pending_cancellation_completion = Some(completion);
            }
            SupervisorWake::HealResult(result) => pending_heal_results.push_back(result),
            SupervisorWake::CancelRequests(requests) => pending_cancel_requests.extend(requests),
            SupervisorWake::Intents(intents) => pending_intent_batch = Some(intents),
            SupervisorWake::Portfolio | SupervisorWake::Timer => {}
        }
    }
    finish_supervisor_shutdown(
        clock.now_ms(),
        upstream_done,
        integrity_block,
        &mut pending_audit_records,
        submission_recovery,
        order_fill_ctx,
        open_orders,
        fenced_scopes,
        healing_scopes,
        &active_cancellations,
    )
}

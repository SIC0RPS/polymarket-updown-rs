use super::*;
use crate::engine_trade_journal::EngineTradeJournalRecord;

#[path = "supervisor_loop/ambiguous_submission.rs"]
mod ambiguous_submission;
#[path = "supervisor_loop/live_wake.rs"]
mod live_wake;
pub(in crate::execution::supervisor) use ambiguous_submission::SubmissionRecovery;
use ambiguous_submission::{
    apply_authenticated_trade_event, retry_deferred_authenticated_trade_events,
    SubmissionAuditQueue,
};

pub(super) struct LiveInputAvailability {
    pub pause: bool,
    pub order_updates: bool,
    pub heal_results: bool,
    pub cancel_requests: bool,
    pub intents: bool,
    pub portfolio: bool,
}

pub(super) enum SupervisorWake {
    Shutdown,
    UpstreamStop,
    Pause,
    InputClosed(&'static str),
    OrderEvent(ExecutionOrderEvent),
    SubmissionCompleted(ActiveSubmissionCompletion),
    CancellationCompleted(CancellationCompletion),
    HealResult(ScopedCancelHealResult),
    CancelRequests(Vec<CancelRequest>),
    Intents(ExecutionIntentBatch),
    Portfolio,
    Timer,
}

fn reconcile_captured_order_lifecycle(
    risk: &RiskHandle,
    portfolio: &PortfolioState,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    now_ms: u64,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
) -> Result<bool> {
    let mut terminal_audits = Vec::new();
    let reconciliation = order_lifecycle.reconcile_terminal_no_fills(
        portfolio,
        risk,
        open_orders,
        &mut terminal_audits,
        now_ms,
    );
    let (changed, mut first_error) = match reconciliation {
        Ok(changed) => (changed, None),
        Err(error) => (false, Some(error)),
    };
    for fact in terminal_audits {
        match build_order_terminal_record(fact) {
            Ok(record) => execution_audit_records.push(record),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let buy_changed = order_lifecycle.reconcile_confirmed_direct_buys(
        portfolio,
        risk,
        order_fill_ctx,
        open_orders,
        now_ms,
    )?;
    order_lifecycle.finish_execution_completions(order_fill_ctx);
    Ok(changed || buy_changed)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one concrete supervisor wake boundary"
)]
pub(super) async fn wait_for_live_supervisor_wake(
    live_wait: Duration,
    availability: &LiveInputAvailability,
    pause_rx: &mut watch::Receiver<bool>,
    order_updates_rx: &mut mpsc::Receiver<ExecutionOrderEvent>,
    scoped_heal_result_rx: &mut mpsc::UnboundedReceiver<ScopedCancelHealResult>,
    cancel_requests_rx: &mut mpsc::UnboundedReceiver<Vec<CancelRequest>>,
    intents_rx: &mut mpsc::UnboundedReceiver<ExecutionIntentBatch>,
    portfolio_rx: &mut watch::Receiver<PortfolioState>,
    shutdown: &CancellationToken,
    shutdown_requested: bool,
    upstream_stop: &CancellationToken,
    upstream_requested: bool,
    active_submissions: &mut ActiveSubmissions,
    active_cancellations: &mut ActiveCancellations,
) -> SupervisorWake {
    tokio::select! {
        _ = shutdown.cancelled(), if !shutdown_requested => SupervisorWake::Shutdown,
        _ = upstream_stop.cancelled(), if !upstream_requested => SupervisorWake::UpstreamStop,
        changed = pause_rx.changed(), if availability.pause => match changed {
            Ok(()) => SupervisorWake::Pause,
            Err(_) => SupervisorWake::InputClosed("pause"),
        },
        next = order_updates_rx.recv(), if availability.order_updates => next
            .map(SupervisorWake::OrderEvent)
            .unwrap_or(SupervisorWake::InputClosed("order_updates")),
        completion = active_submissions.next_completion(), if !active_submissions.is_empty() => {
            SupervisorWake::SubmissionCompleted(
                completion.expect("nonempty active submission set produced no completion")
            )
        },
        completion = active_cancellations.next_completion(), if active_cancellations.has_network_work() => {
            SupervisorWake::CancellationCompleted(
                completion.expect("nonempty cancellation set produced no completion")
            )
        },
        next = scoped_heal_result_rx.recv(), if availability.heal_results => next
            .map(SupervisorWake::HealResult)
            .unwrap_or(SupervisorWake::InputClosed("scoped_heal_results")),
        next = cancel_requests_rx.recv(), if availability.cancel_requests => next
            .map(SupervisorWake::CancelRequests)
            .unwrap_or(SupervisorWake::InputClosed("cancel_requests")),
        next = intents_rx.recv(), if availability.intents => next
            .map(SupervisorWake::Intents)
            .unwrap_or(SupervisorWake::InputClosed("intents")),
        changed = portfolio_rx.changed(), if availability.portfolio => match changed {
            Ok(()) => SupervisorWake::Portfolio,
            Err(_) => SupervisorWake::InputClosed("portfolio"),
        },
        _ = tokio::time::sleep(live_wait) => SupervisorWake::Timer,
    }
}
use live_wake::{run_live_supervisor, LiveSupervisor};

fn advance_deadline(next_due_ms: &mut u64, interval_ms: u64, now_ms: u64) {
    while *next_due_ms <= now_ms {
        *next_due_ms = next_due_ms.saturating_add(interval_ms.max(1));
    }
}

async fn recv_captured_batch(
    batch_rx: &mut Option<mpsc::UnboundedReceiver<CapturedExecutionSupervisorRequest>>,
) -> Option<CapturedExecutionSupervisorRequest> {
    match batch_rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

fn snapshot_market_projection(
    market_projection_rx: Option<&watch::Receiver<MarketProjection>>,
) -> Option<MarketProjection> {
    market_projection_rx.map(|rx| rx.borrow().clone())
}

pub(super) async fn run_supervisor(
    cfg: AppConfig,
    exec: ExecutionClient,
    risk: RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    shared_strategy_cfg: SharedStrategyConfig,
    order_fill_ctx: OrderFillContextRegistry,
    pause_rx: &mut watch::Receiver<bool>,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    blocked_markets_tx: watch::Sender<HashSet<String>>,
    portfolio_rx: &mut watch::Receiver<PortfolioState>,
    odds_rx: &mut Option<watch::Receiver<Arc<PolymarketOddsState>>>,
    _oracle_runtime_rx: &mut Option<watch::Receiver<OracleRuntimeStatus>>,
    market_projection_rx: &mut Option<watch::Receiver<MarketProjection>>,
    order_updates_rx: &mut mpsc::Receiver<ExecutionOrderEvent>,
    mut intents_rx: mpsc::UnboundedReceiver<ExecutionIntentBatch>,
    mut cancel_requests_rx: mpsc::UnboundedReceiver<Vec<CancelRequest>>,
    open_orders_tx: watch::Sender<OpenOrdersState>,
    obs: Observability,
    capture: MaybeCapture,
    shutdown: CancellationToken,
    mut clock: ExecutionSupervisorClock,
    live_upstream: Option<(
        CancellationToken,
        oneshot::Sender<std::result::Result<(), String>>,
    )>,
) -> Result<()> {
    let mut pause = *pause_rx.borrow();
    let mut open_orders = OpenOrdersState::default();
    let mut live_quote_cancel_index: HashMap<QuoteCancelKey, HashSet<String>> = HashMap::new();
    let mut seen_quote_cancel_keys: HashSet<QuoteCancelKey> = HashSet::new();
    let mut post_only_rejects: HashMap<String, PostOnlyRejectState> = HashMap::new();
    let mut order_lifecycle = OrderLifecycleSupervisor::default();
    let mut submission_recovery = SubmissionRecovery::default();
    let mut fenced_scopes: HashSet<ScopedOrderFenceKey> = HashSet::new();
    let mut healing_scopes: HashSet<ScopedOrderFenceKey> = HashSet::new();
    let mut hard_submit_block: Option<String> = None;
    let mut observed_portfolio_revision = portfolio_rx.borrow().revision;
    let (scoped_heal_tx, scoped_heal_rx) = mpsc::unbounded_channel::<ScopedCancelHealRequest>();
    let (scoped_heal_result_tx, mut scoped_heal_result_rx) =
        mpsc::unbounded_channel::<ScopedCancelHealResult>();
    let scoped_heal_task = tokio::spawn(run_scoped_cancel_heal_worker(
        exec.clone(),
        scoped_heal_rx,
        scoped_heal_result_tx,
        shutdown.clone(),
    ));
    let _ = blocked_markets_tx.send(HashSet::new());

    let timeout_interval_ms = 100_u64;
    let initial_now_ms = clock.now_ms();
    let mut next_timeout_due_ms = initial_now_ms.saturating_add(timeout_interval_ms);
    let startup_cooldown_ms = if cfg.risk.enabled {
        cfg.risk.startup_cooldown_ms
    } else {
        0
    };
    let startup_block_until_ms = initial_now_ms.saturating_add(startup_cooldown_ms);
    let mut startup_cooldown_last_log_ms = 0_u64;
    let mut captured_batch_rx = clock.take_batch_rx();
    if !clock.is_live() {
        let mut last_input_sequence = 0_u64;
        let mut completed_work_watermark = 0_u64;
        let mut captured_error: Option<anyhow::Error> = None;
        loop {
            let request = tokio::select! {
                _ = shutdown.cancelled() => break,
                next = recv_captured_batch(&mut captured_batch_rx) => match next {
                    Some(request) => request,
                    None => {
                        captured_error = Some(anyhow::anyhow!(
                            "captured execution supervisor batch channel closed"
                        ));
                        break;
                    }
                }
            };
            let result: Result<ExecutionSupervisorBatchAck> = async {
                let mut execution_audit_records = Vec::new();
                let CapturedExecutionSupervisorBatch {
                    input_sequence,
                    timestamp_ms,
                    pause: batch_pause,
                    order_updates,
                    matched_fills,
                    cancel_requests,
                    intents,
                } = request.batch;
                let expected_sequence = last_input_sequence.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!(
                        "captured execution supervisor input sequence overflow at {last_input_sequence}"
                    )
                })?;
                if input_sequence != expected_sequence {
                    anyhow::bail!(
                        "captured execution supervisor sequence mismatch expected={} received={input_sequence}",
                        expected_sequence
                    );
                }
                clock.advance_to(timestamp_ms).map_err(anyhow::Error::msg)?;
                let now_ms = clock.now_ms();
                let local_order_facts_present =
                    !order_updates.is_empty() || !matched_fills.is_empty();

                if let Some(next_pause) = batch_pause {
                    if next_pause != pause {
                        pause = next_pause;
                        tracing::info!(
                            target: "execution.supervisor_loop",
                            pause,
                            "execution_pause_updated"
                        );
                    }
                }
                if pause {
                    cancel_open_buy_orders_for_pause(
                        now_ms,
                        &clock,
                        &exec,
                        &risk,
                        &mut open_orders,
                        &mut fenced_scopes,
                        &mut healing_scopes,
                        &scoped_heal_tx,
                        &obs,
                        None,
                    )
                    .await?;
                    if open_orders.has_pending_confirmed_cancel() {
                        reconcile_captured_order_lifecycle(
                            &risk,
                            &portfolio_rx.borrow(),
                            &mut open_orders,
                            &mut order_lifecycle,
                            &order_fill_ctx,
                            now_ms,
                            &mut execution_audit_records,
                        )?;
                    }
                }

                for update in order_updates {
                    risk.release_inflight_order_by_order_id(update.order_id.as_str());
                    let metadata = order_fill_ctx.order_metadata(update.order_id.as_str());
                    if let Some(authorization) = order_fill_ctx.direct_buy_authorization(&update.order_id) {
                        order_lifecycle.consume_direct_buy_order_update(
                            &update, authorization, &risk, &portfolio_rx.borrow(), &mut open_orders, now_ms,
                        )?;
                    } else if let Some(fact) = order_lifecycle.consume_closing_update(
                        &update,
                        metadata.as_ref(),
                        &mut open_orders,
                        now_ms,
                    )? {
                        execution_audit_records.push(build_order_terminal_record(fact)?);
                    }
                    open_orders.apply_update_with_metadata_at(update, metadata.as_ref(), now_ms);
                }
                {
                    let portfolio = portfolio_rx.borrow();
                    for fill in matched_fills {
                        let metadata = order_fill_ctx.order_metadata(fill.order_id.as_str());
                        order_lifecycle.consume_captured_matched_fill(
                            &fill,
                            metadata.as_ref(),
                            &portfolio,
                            &mut open_orders,
                            now_ms,
                            order_fill_ctx.direct_buy_authorization(&fill.order_id),
                            &risk,
                        )?;
                    }
                }
                {
                    let portfolio = portfolio_rx.borrow();
                    if portfolio.revision != observed_portfolio_revision || local_order_facts_present
                    {
                        reconcile_captured_order_lifecycle(
                            &risk,
                            &portfolio,
                            &mut open_orders,
                            &mut order_lifecycle,
                            &order_fill_ctx,
                            now_ms,
                            &mut execution_audit_records,
                        )?;
                        observed_portfolio_revision = portfolio.revision;
                    }
                }
                if pause {
                    cancel_open_buy_orders_for_pause(
                        now_ms,
                        &clock,
                        &exec,
                        &risk,
                        &mut open_orders,
                        &mut fenced_scopes,
                        &mut healing_scopes,
                        &scoped_heal_tx,
                        &obs,
                        None,
                    )
                    .await?;
                }

                if !cancel_requests.is_empty() {
                    handle_cancel_requests(
                        &clock,
                        false,
                        &exec,
                        &risk,
                        &mut open_orders,
                        &mut live_quote_cancel_index,
                        &seen_quote_cancel_keys,
                        &mut fenced_scopes,
                        &mut healing_scopes,
                        &scoped_heal_tx,
                        cancel_requests,
                        &obs,
                        None,
                    )
                    .await?;
                }

                if open_orders.has_pending_confirmed_cancel() {
                    let portfolio = portfolio_rx.borrow();
                    reconcile_captured_order_lifecycle(
                        &risk,
                        &portfolio,
                        &mut open_orders,
                        &mut order_lifecycle,
                        &order_fill_ctx,
                        now_ms,
                        &mut execution_audit_records,
                    )?;
                    observed_portfolio_revision = portfolio.revision;
                }

                if !intents.is_empty() {
                    let intents = if now_ms < startup_block_until_ms {
                        filter_buy_entry_intents(
                            intents, true, "startup_cooldown", now_ms, &risk,
                            ctf_inventory_runtime, &submission_recovery,
                        )?.0
                    } else {
                        intents
                    };
                    if !intents.is_empty() {
                        let strategy_cfg = live_strategy_cfg(&shared_strategy_cfg);
                        let market_projection =
                            snapshot_market_projection(market_projection_rx.as_ref());
                        handle_intents_batch(
                            now_ms,
                            timestamp_ms,
                            None,
                            None,
                            &clock,
                            pause,
                            runtime_control_rx,
                            &exec,
                            &risk,
                            ctf_inventory_runtime,
                            &order_fill_ctx,
                            &strategy_cfg,
                            portfolio_rx,
                            odds_rx.as_ref(),
                            market_projection.as_ref(),
                            &mut open_orders,
                            &mut order_lifecycle,
                            &mut live_quote_cancel_index,
                            &mut seen_quote_cancel_keys,
                            &mut fenced_scopes,
                            &mut healing_scopes,
                            &scoped_heal_tx,
                            &mut post_only_rejects,
                            cfg.polymarket.execution.order_type,
                            cfg.risk.order_timeout_ms,
                            &obs,
                            &capture,
                            &open_orders_tx,
                            &mut hard_submit_block,
                            intents,
                            &mut execution_audit_records,
                            None,
                            None,
                            &mut submission_recovery,
                        )
                        .await?;
                        order_lifecycle.finish_execution_completions(&order_fill_ctx);
                        obs.set_post_only_reject_status(current_post_only_reject_status(
                            &post_only_rejects,
                            &strategy_cfg,
                            now_ms,
                        ));
                    }
                }

                if now_ms >= next_timeout_due_ms {
                    handle_timeouts(
                        now_ms,
                        &clock,
                        &exec,
                        &risk,
                        &mut open_orders,
                        &mut fenced_scopes,
                        &mut healing_scopes,
                        &scoped_heal_tx,
                        &obs,
                        None,
                    )
                    .await?;
                    if open_orders.has_pending_confirmed_cancel() {
                        reconcile_captured_order_lifecycle(
                            &risk,
                            &portfolio_rx.borrow(),
                            &mut open_orders,
                            &mut order_lifecycle,
                            &order_fill_ctx,
                            now_ms,
                            &mut execution_audit_records,
                        )?;
                    }
                    advance_deadline(&mut next_timeout_due_ms, timeout_interval_ms, now_ms);
                }
                while !healing_scopes.is_empty() {
                    let result = tokio::select! {
                        _ = shutdown.cancelled() => {
                            anyhow::bail!("captured execution supervisor shutdown during heal")
                        }
                        result = scoped_heal_result_rx.recv() => result.ok_or_else(|| {
                            anyhow::anyhow!("cancel scope heal result channel closed")
                        })?,
                    };
                    apply_scoped_cancel_heal_result(
                        now_ms,
                        &risk,
                        &mut open_orders,
                        &obs,
                        &mut healing_scopes,
                        &mut fenced_scopes,
                        result,
                        None,
                        false,
                    )?;
                    completed_work_watermark = completed_work_watermark.saturating_add(1);
                }
                order_lifecycle.reconcile_confirmed_direct_buys(
                    &portfolio_rx.borrow(), &risk, &order_fill_ctx, &mut open_orders, now_ms,
                )?;
                publish_open_orders(&open_orders_tx, &obs, &open_orders);
                Ok(ExecutionSupervisorBatchAck {
                    input_sequence,
                    timestamp_ms: now_ms,
                    completed_work_watermark,
                    open_orders_revision: open_orders.revision,
                    execution_audit_records,
                })
            }
            .await;
            match result {
                Ok(ack) => {
                    last_input_sequence = ack.input_sequence;
                    let _ = request.ack_tx.send(Ok(ack));
                }
                Err(error) => {
                    let error_text = format!("{error:#}");
                    let _ = request.ack_tx.send(Err(error_text.clone()));
                    captured_error = Some(anyhow::anyhow!(error_text));
                    break;
                }
            }
        }
        scoped_heal_task.abort();
        let _ = scoped_heal_task.await;
        return match captured_error {
            Some(error) => Err(error),
            None => Ok(()),
        };
    }

    let (upstream_stop, upstream_done) = live_upstream
        .ok_or_else(|| anyhow::anyhow!("live supervisor upstream completion owner missing"))?;
    let result = run_live_supervisor(LiveSupervisor {
        cfg: &cfg,
        exec: &exec,
        risk: &risk,
        ctf_inventory_runtime,
        shared_strategy_cfg: &shared_strategy_cfg,
        order_fill_ctx: &order_fill_ctx,
        pause: &mut pause,
        pause_rx,
        runtime_control_rx,
        portfolio_rx,
        odds_rx,
        market_projection_rx,
        order_updates_rx,
        intents_rx: &mut intents_rx,
        cancel_requests_rx: &mut cancel_requests_rx,
        open_orders: &mut open_orders,
        order_lifecycle: &mut order_lifecycle,
        live_quote_cancel_index: &mut live_quote_cancel_index,
        seen_quote_cancel_keys: &mut seen_quote_cancel_keys,
        fenced_scopes: &mut fenced_scopes,
        healing_scopes: &mut healing_scopes,
        scoped_heal_tx: &scoped_heal_tx,
        scoped_heal_result_rx: &mut scoped_heal_result_rx,
        post_only_rejects: &mut post_only_rejects,
        hard_submit_block: &mut hard_submit_block,
        observed_portfolio_revision: &mut observed_portfolio_revision,
        next_timeout_due_ms: &mut next_timeout_due_ms,
        timeout_interval_ms,
        startup_block_until_ms,
        startup_cooldown_last_log_ms: &mut startup_cooldown_last_log_ms,
        open_orders_tx: &open_orders_tx,
        obs: &obs,
        capture: &capture,
        shutdown: &shutdown,
        clock: &clock,
        submission_recovery: &mut submission_recovery,
        upstream_stop: &upstream_stop,
        upstream_done: Some(upstream_done),
    })
    .await;
    drop(scoped_heal_tx);
    let heal_result = scoped_heal_task
        .await
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    match (result, heal_result) {
        (Ok(()), result) => result,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(heal_error)) => {
            Err(error.context(format!("cancel heal task also failed: {heal_error:#}")))
        }
    }
}

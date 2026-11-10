use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "existing cancel-before-replacement ownership"
)]
pub(super) async fn cancel_then_submit_replacements(
    decision_ts_ms: u64,
    decision_instant: Option<Instant>,
    handoff_instant: Option<Instant>,
    clock: &ExecutionSupervisorClock,
    pause: bool,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
    market_projection: Option<&MarketProjection>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    hard_submit_block: &mut Option<String>,
    execution_order_type: PolymarketOrderType,
    resting_order_timeout_ms: u64,
    to_cancel: HashSet<String>,
    to_place_after_cancel: Vec<OrderIntent>,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
    active_submissions: Option<&mut ActiveSubmissions>,
    active_cancellations: Option<&mut ActiveCancellations>,
    submission_recovery: &mut SubmissionRecovery,
) -> Result<()> {
    let mut cancel_ids: Vec<String> = to_cancel.into_iter().collect();
    cancel_ids.sort();
    if let Some(active) = active_cancellations {
        let targets = ActiveCancellations::targets_for_ids(open_orders, &cancel_ids);
        active.schedule_orders(
            exec,
            targets.clone(),
            "supervisor_cancel_replace",
            "CANCEL_OK",
        );
        active.retain_replacement(PendingReplacement {
            decision_ts_ms,
            decision_instant,
            handoff_instant,
            targets,
            intents: to_place_after_cancel,
        });
        return Ok(());
    }
    if !cancel_ids.is_empty() {
        let outcome = exec.cancel_orders(&cancel_ids).await;
        let receipt_now_ms = clock.now_ms();
        let failed_ids = match outcome {
            Ok(ack) => {
                let canceled: HashSet<&str> = ack.canceled.iter().map(String::as_str).collect();
                let unresolved: Vec<String> = cancel_ids
                    .iter()
                    .filter(|id| !canceled.contains(id.as_str()))
                    .cloned()
                    .collect();
                apply_cancel_ack(
                    receipt_now_ms,
                    risk,
                    ack,
                    open_orders,
                    obs,
                    "CANCEL_OK",
                    "cancel_failed",
                );
                unresolved
            }
            Err(error) => {
                tracing::error!(
                    target: "execution.supervisor_batch",
                    error = %error,
                    "cancel_replace_error"
                );
                cancel_ids
            }
        };
        if open_orders.has_pending_confirmed_cancel() {
            let mut terminal_audits = Vec::new();
            let mut first_error = order_lifecycle
                .reconcile_terminal_no_fills(
                    &portfolio_rx.borrow(),
                    risk,
                    open_orders,
                    &mut terminal_audits,
                    receipt_now_ms,
                )
                .err();
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
        }
        enqueue_failed_cancel_scope_heals(
            open_orders,
            &failed_ids,
            "supervisor_cancel_replace",
            fenced_scopes,
            scoped_heal_inflight,
            scoped_heal_tx,
        )?;
    }
    submit_ready_orders(
        clock.now_ms(),
        decision_ts_ms,
        decision_instant,
        handoff_instant,
        clock,
        exec,
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        strategy_cfg,
        odds_rx,
        market_projection,
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
        execution_order_type,
        resting_order_timeout_ms,
        to_place_after_cancel,
        execution_audit_records,
        active_submissions,
        submission_recovery,
        pause,
        runtime_control_rx,
        None,
        None,
    )
    .await
}

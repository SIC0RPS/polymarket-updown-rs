use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "one supervisor submission completion owner"
)]
pub(super) fn finish_live_submission(
    completion: ActiveSubmissionCompletion,
    now_ms: u64,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    shared_strategy_cfg: &SharedStrategyConfig,
    market_projection_rx: Option<&watch::Receiver<MarketProjection>>,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    hard_submit_block: &mut Option<String>,
    submission_recovery: &mut SubmissionRecovery,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
) {
    let strategy_cfg = live_strategy_cfg(shared_strategy_cfg);
    let market_projection = snapshot_market_projection(market_projection_rx);
    let mut produced_audit_records = Vec::new();
    if let Err(error) = apply_submission_completion(
        now_ms,
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        &strategy_cfg,
        market_projection.as_ref(),
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        post_only_rejects,
        obs,
        capture,
        hard_submit_block,
        &mut produced_audit_records,
        submission_recovery,
        completion,
    ) {
        let text = format!("{error:#}");
        tracing::error!(target: "execution.supervisor_loop", error = %text, "submission_completion_failed");
        *integrity_block = Some(text);
    }
    if let Err(error) =
        pending_audit_records.extend(produced_audit_records, order_fill_ctx, order_lifecycle)
    {
        *integrity_block = Some(format!("{error:#}"));
    }
    if let Err(error) = retry_deferred_authenticated_trade_events(
        now_ms,
        risk,
        order_fill_ctx,
        &portfolio_rx.borrow(),
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        submission_recovery,
        pending_audit_records,
    ) {
        let text = format!("{error:#}");
        tracing::error!(target: "execution.supervisor_loop", error = %text, "deferred_authenticated_trade_recovery_failed");
        *integrity_block = Some(text);
    }
    order_updates::reconcile_live_order_lifecycle(
        risk,
        &portfolio_rx.borrow(),
        open_orders,
        order_lifecycle,
        order_fill_ctx,
        now_ms,
        pending_audit_records,
        integrity_block,
    );
    publish_open_orders(open_orders_tx, obs, open_orders);
}

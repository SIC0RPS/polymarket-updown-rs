use super::*;
use crate::execution::supervisor::cancel_router::ActiveCancellations;

pub(super) fn reconcile_live_order_lifecycle(
    risk: &RiskHandle,
    portfolio: &PortfolioState,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    now_ms: u64,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
) -> bool {
    let mut terminal_audits = Vec::new();
    let reconciliation = order_lifecycle.reconcile_terminal_no_fills(
        portfolio,
        risk,
        open_orders,
        &mut terminal_audits,
        now_ms,
    );
    let (mut changed, mut first_error) = match reconciliation {
        Ok(changed) => (changed, None),
        Err(error) => (false, Some(error)),
    };
    for fact in terminal_audits {
        match build_order_terminal_record(fact) {
            Ok(record) => {
                if let Err(error) = pending_audit_records.push_from_registry(record, order_fill_ctx)
                {
                    first_error.get_or_insert(error);
                }
            }
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    match order_lifecycle.reconcile_confirmed_direct_buys(
        portfolio,
        risk,
        order_fill_ctx,
        open_orders,
        now_ms,
    ) {
        Ok(buy_changed) => changed |= buy_changed,
        Err(error) => {
            first_error.get_or_insert(error);
        }
    }
    let result = if let Some(error) = first_error {
        Err(error)
    } else {
        order_lifecycle.finish_execution_completions(order_fill_ctx);
        Ok(changed)
    };
    match result {
        Ok(changed) => changed,
        Err(error) => {
            let text = format!("{error:#}");
            tracing::error!(
                target: "execution.supervisor_loop",
                error = %text,
                "order_lifecycle_reconciliation_failed"
            );
            *integrity_block = Some(text);
            false
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one live terminal-order reconciliation boundary"
)]
pub(super) fn reconcile_live_portfolio(
    risk: &RiskHandle,
    portfolio: &PortfolioState,
    observed_portfolio_revision: &mut u64,
    local_order_fact_applied: bool,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    now_ms: u64,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    obs: &Observability,
) -> bool {
    if portfolio.revision == *observed_portfolio_revision && !local_order_fact_applied {
        return false;
    }
    let changed = reconcile_live_order_lifecycle(
        risk,
        portfolio,
        open_orders,
        order_lifecycle,
        order_fill_ctx,
        now_ms,
        pending_audit_records,
        integrity_block,
    );
    *observed_portfolio_revision = portfolio.revision;
    if changed && !local_order_fact_applied {
        publish_open_orders(open_orders_tx, obs, open_orders);
    }
    true
}

#[allow(
    clippy::too_many_arguments,
    reason = "one live pause-cancel handoff boundary"
)]
pub(super) async fn apply_live_pause_cancel(
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
    obs: &Observability,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    active_cancellations: &mut ActiveCancellations,
) {
    if let Err(error) = cancel_open_buy_orders_for_pause(
        clock.now_ms(),
        clock,
        exec,
        risk,
        open_orders,
        fenced_scopes,
        healing_scopes,
        scoped_heal_tx,
        obs,
        Some(active_cancellations),
    )
    .await
    {
        tracing::error!(
            target: "execution.supervisor_loop",
            error = %format!("{error:#}"),
            "pause_buy_cancel_failed"
        );
        *integrity_block = Some(format!("{error:#}"));
    }
    if open_orders.has_pending_confirmed_cancel() {
        let portfolio = portfolio_rx.borrow();
        reconcile_live_order_lifecycle(
            risk,
            &portfolio,
            open_orders,
            order_lifecycle,
            order_fill_ctx,
            clock.now_ms(),
            pending_audit_records,
            integrity_block,
        );
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one live confirmed-cancel handoff boundary"
)]
pub(super) async fn apply_live_cancel_requests(
    requests: Vec<CancelRequest>,
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    obs: &Observability,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    active_cancellations: &mut ActiveCancellations,
) {
    if let Err(error) = handle_cancel_requests(
        clock,
        true,
        exec,
        risk,
        open_orders,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        fenced_scopes,
        healing_scopes,
        scoped_heal_tx,
        requests,
        obs,
        Some(active_cancellations),
    )
    .await
    {
        tracing::error!(
            target: "execution.supervisor_loop",
            error = %format!("{error:#}"),
            "cancel_requests_failed"
        );
        *integrity_block = Some(format!("{error:#}"));
    }
    if open_orders.has_pending_confirmed_cancel() {
        let portfolio = portfolio_rx.borrow();
        reconcile_live_order_lifecycle(
            risk,
            &portfolio,
            open_orders,
            order_lifecycle,
            order_fill_ctx,
            clock.now_ms(),
            pending_audit_records,
            integrity_block,
        );
    }
    publish_open_orders(open_orders_tx, obs, open_orders);
}

#[allow(
    clippy::too_many_arguments,
    reason = "one live timeout-cancel handoff boundary"
)]
pub(super) async fn apply_live_timeouts(
    timeout_check_ms: u64,
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    order_fill_ctx: &OrderFillContextRegistry,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    pending_audit_records: &mut SubmissionAuditQueue,
    integrity_block: &mut Option<String>,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    obs: &Observability,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    active_cancellations: &mut ActiveCancellations,
) {
    if let Err(error) = handle_timeouts(
        timeout_check_ms,
        clock,
        exec,
        risk,
        open_orders,
        fenced_scopes,
        healing_scopes,
        scoped_heal_tx,
        obs,
        Some(active_cancellations),
    )
    .await
    {
        tracing::error!(
            target: "execution.supervisor_loop",
            error = %format!("{error:#}"),
            "execution_timeout_check_failed"
        );
        *integrity_block = Some(format!("{error:#}"));
    }
    if open_orders.has_pending_confirmed_cancel() {
        let portfolio = portfolio_rx.borrow();
        reconcile_live_order_lifecycle(
            risk,
            &portfolio,
            open_orders,
            order_lifecycle,
            order_fill_ctx,
            clock.now_ms(),
            pending_audit_records,
            integrity_block,
        );
    }
    publish_open_orders(open_orders_tx, obs, open_orders);
}

#[allow(
    clippy::too_many_arguments,
    reason = "one live order-update ownership boundary"
)]
pub(super) fn apply_live_order_update(
    update: OrderUpdateEvent,
    mut owned_context: Option<OrderFillContext>,
    risk: &RiskHandle,
    portfolio: &PortfolioState,
    order_fill_ctx: &OrderFillContextRegistry,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    open_orders: &mut OpenOrdersState,
    now_ms: u64,
    pending_audit_records: &mut SubmissionAuditQueue,
) -> Option<String> {
    risk.release_inflight_order_by_order_id(update.order_id.as_str());
    if owned_context.is_none() {
        owned_context = order_fill_ctx.owned_context(update.order_id.as_str()).ok();
    }
    let metadata = owned_context
        .as_ref()
        .map(OrderFillContext::runtime_metadata)
        .or_else(|| order_fill_ctx.order_metadata(update.order_id.as_str()));
    if let Some(authorization) = owned_context
        .as_ref()
        .and_then(OrderFillContext::direct_buy_authorization)
        .or_else(|| order_fill_ctx.direct_buy_authorization(&update.order_id))
    {
        let error = order_lifecycle
            .consume_direct_buy_order_update(
                &update,
                authorization,
                risk,
                portfolio,
                open_orders,
                now_ms,
            )
            .err()
            .map(|error| format!("{error:#}"));
        open_orders.apply_update_with_metadata_at(update, metadata.as_ref(), now_ms);
        return error;
    }
    let integrity_error = match order_lifecycle.consume_closing_update(
        &update,
        metadata.as_ref(),
        open_orders,
        now_ms,
    ) {
        Ok(Some(fact)) => match build_order_terminal_record(fact) {
            Ok(record) => {
                pending_audit_records.push_with_context(record, owned_context.take());
                None
            }
            Err(error) => {
                let text = format!("{error:#}");
                tracing::error!(
                    target: "execution.supervisor_loop",
                    error = %text,
                    "execution_terminal_audit_invalid"
                );
                Some(text)
            }
        },
        Ok(None) => None,
        Err(error) => {
            let text = format!("{error:#}");
            tracing::error!(
                target: "execution.supervisor_loop",
                error = %text,
                "execution_terminal_fact_invalid"
            );
            Some(text)
        }
    };
    open_orders.apply_update_with_metadata_at(update, metadata.as_ref(), now_ms);
    integrity_error
}

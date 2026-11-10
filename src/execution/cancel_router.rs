use super::*;
use crate::observability::action::RuntimeIssueKind;
use std::collections::BTreeMap;

#[path = "cancel_router/pending.rs"]
mod pending;
pub(super) use pending::{ActiveCancellations, CancellationCompletion, PendingReplacement};

#[path = "cancel_router/quote_cancel.rs"]
mod quote_cancel;
use quote_cancel::handle_cancel_quote_request;
pub(super) use quote_cancel::register_quote_cancel_key;

pub(super) async fn handle_cancel_requests(
    clock: &ExecutionSupervisorClock,
    allow_wall_sleep: bool,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    requests: Vec<CancelRequest>,
    obs: &Observability,
    mut active_cancellations: Option<&mut ActiveCancellations>,
) -> Result<Vec<String>> {
    let mut seen_markets = HashSet::new();
    let mut seen_quote_groups: HashSet<(Option<MarketId>, String, Option<Side>)> = HashSet::new();
    let mut failed_ids = Vec::new();
    for request in requests {
        match request {
            CancelRequest::Market(market) => {
                if market.market_id.is_empty()
                    || market.market_id == "-"
                    || !seen_markets.insert(market.market_id.clone())
                {
                    continue;
                }
                if let Some(active) = active_cancellations.as_deref_mut() {
                    active.schedule_market(market);
                    continue;
                }
                let open_count = open_orders
                    .orders
                    .values()
                    .filter(|order| market_identifier_matches(order.market.as_str(), &market))
                    .count();
                if clean_inventory_market_to_empty(
                    clock,
                    allow_wall_sleep,
                    exec,
                    risk,
                    open_orders,
                    &market,
                    obs,
                    "strategy_cancel_market",
                )
                .await
                {
                    tracing::info!(
                        target: "execution.cancel_router",
                        market = %market.market_id,
                        gamma_market_id = market.gamma_market_id.as_deref().unwrap_or("-"),
                        open_count,
                        "strategy_cancel_market_ok"
                    );
                }
            }
            CancelRequest::Quotes(request) => {
                if request.quote_group.trim().is_empty()
                    || !seen_quote_groups.insert((
                        request.market_id.clone(),
                        request.quote_group.clone(),
                        request.side,
                    ))
                {
                    continue;
                }
                handle_cancel_quote_request(
                    clock,
                    exec,
                    risk,
                    open_orders,
                    live_quote_cancel_index,
                    seen_quote_cancel_keys,
                    fenced_scopes,
                    scoped_heal_inflight,
                    scoped_heal_tx,
                    &request,
                    obs,
                    active_cancellations.as_deref_mut(),
                )
                .await?
                .into_iter()
                .for_each(|failed_id| failed_ids.push(failed_id));
            }
        }
    }
    Ok(failed_ids)
}

pub(super) async fn cancel_open_buy_orders_for_pause(
    _request_now_ms: u64,
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    obs: &Observability,
    active_cancellations: Option<&mut ActiveCancellations>,
) -> Result<Vec<String>> {
    let ids: Vec<String> = open_orders
        .orders
        .values()
        .filter(|order| order.side == Side::Buy)
        .map(|order| order.order_id.clone())
        .collect();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(active) = active_cancellations {
        active.schedule_orders(
            exec,
            ActiveCancellations::targets_for_ids(open_orders, &ids),
            "pause_cancel_open_buys",
            "CANCEL_OK",
        );
        return Ok(Vec::new());
    }
    let cancel_result = exec.cancel_orders(&ids).await;
    let receipt_now_ms = clock.now_ms();
    match cancel_result {
        Ok(ack) => {
            let requested = ids.len();
            let failed_ids = apply_cancel_ack(
                receipt_now_ms,
                risk,
                ack,
                open_orders,
                obs,
                "CANCEL_OK",
                "PAUSE_cancel_failed",
            );
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &failed_ids,
                "pause_cancel_open_buys",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
            tracing::info!(
                target: "execution.cancel_router",
                requested,
                "pause_cancel_open_buys"
            );
            Ok(failed_ids)
        }
        Err(err) => {
            tracing::error!(
                target: "execution.cancel_router",
                error = %err,
                "pause_cancel_open_buys_error"
            );
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &ids,
                "pause_cancel_open_buys",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
            Ok(ids)
        }
    }
}

pub(super) fn apply_cancel_ack(
    now_ms: u64,
    risk: &RiskHandle,
    ack: CancelAck,
    open_orders: &mut OpenOrdersState,
    _obs: &Observability,
    terminal_status: &str,
    fail_log: &str,
) -> Vec<String> {
    let mut failed_ids = Vec::new();
    let mut mutated = false;
    for id in ack.canceled {
        risk.release_inflight_buy_order(id.as_str());
        mutated |= open_orders.record_cancel_terminal(&id, now_ms, terminal_status);
    }
    for (id, reason) in ack.not_canceled {
        failed_ids.push(id.clone());
        tracing::error!(
            target: "execution.cancel_router",
            order_id = %id,
            reason = %reason,
            fail_log = fail_log,
            "cancel_ack_failed"
        );
    }
    if mutated {
        open_orders.note_runtime_mutation(now_ms);
    }
    failed_ids
}

pub(super) fn reconcile_local_order_scope(
    now_ms: u64,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    market_id: &str,
    token: &str,
    remote_ids: &[String],
    target_ids: Option<&HashSet<String>>,
) {
    let keep: HashSet<&str> = remote_ids.iter().map(String::as_str).collect();
    let removed_ids: Vec<String> = open_orders
        .orders
        .iter()
        .filter_map(|(id, order)| {
            (order.market == market_id
                && order.token == token
                && target_ids.is_none_or(|targets| targets.contains(id))
                && !keep.contains(id.as_str()))
            .then_some(id.clone())
        })
        .collect();
    for id in &removed_ids {
        open_orders.orders.remove(id);
        risk.release_inflight_buy_order(id.as_str());
    }
    if !removed_ids.is_empty() {
        open_orders.note_runtime_mutation(now_ms);
    }
}

pub(super) fn enqueue_failed_cancel_scope_heals(
    open_orders: &OpenOrdersState,
    failed_ids: &[String],
    label: &'static str,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
) -> Result<usize> {
    let mut scheduled = 0_usize;
    for failed_id in failed_ids {
        let Some(order) = open_orders.orders.get(failed_id) else {
            continue;
        };
        let scope = ScopedOrderFenceKey {
            market: order.market.clone(),
            token: order.token.clone(),
        };
        fenced_scopes.insert(scope.clone());
        if !scoped_heal_inflight.insert(scope.clone()) {
            continue;
        }
        if scoped_heal_tx
            .send(ScopedCancelHealRequest {
                scope: scope.clone(),
                order_id: failed_id.clone(),
                label,
            })
            .is_err()
        {
            scoped_heal_inflight.remove(&scope);
            tracing::error!(
                target: "execution.cancel_router",
                order_id = %failed_id,
                label = label,
                "cancel_scope_heal_worker_closed"
            );
            return Err(anyhow::Error::new(ScopedHealQueueClosed));
        }
        scheduled = scheduled.saturating_add(1);
    }
    Ok(scheduled)
}

async fn verify_and_heal_failed_cancel_order_scope(
    exec: &ExecutionClient,
    request: ScopedCancelHealRequest,
) -> ScopedCancelHealResult {
    let scope_market = request.scope.market.clone();
    let scope_token = request.scope.token.clone();

    let order_ids = match exec.open_order_ids_by_id(&request.order_id).await {
        Ok(order_ids) => order_ids,
        Err(err) => {
            tracing::error!(
                target: "execution.cancel_router",
                order_id = %request.order_id,
                market = %scope_market,
                token = %scope_token,
                label = request.label,
                error = %err,
                "cancel_order_verify_error"
            );
            return ScopedCancelHealResult {
                scope: request.scope,
                order_id: request.order_id,
                label: request.label,
                final_open_ids: Err(format!("cancel_order_verify_error: {err:#}")),
            };
        }
    };
    if !order_ids.iter().any(|id| id == &request.order_id) {
        tracing::info!(
            target: "execution.cancel_router",
            order_id = %request.order_id,
            market = %scope_market,
            token = %scope_token,
            label = request.label,
            "cancel_order_verify_stale"
        );
        let final_open_ids = match exec
            .open_order_ids_in_scope(Some(scope_market.as_str()), Some(scope_token.as_str()))
            .await
        {
            Ok(order_ids) => Ok(order_ids),
            Err(err) => Err(format!("cancel_scope_stale_verify_error: {err:#}")),
        };
        return ScopedCancelHealResult {
            scope: request.scope,
            order_id: request.order_id,
            label: request.label,
            final_open_ids,
        };
    }

    let scope_ids = match exec
        .open_order_ids_in_scope(Some(scope_market.as_str()), Some(scope_token.as_str()))
        .await
    {
        Ok(order_ids) => order_ids,
        Err(err) => {
            tracing::error!(
                target: "execution.cancel_router",
                order_id = %request.order_id,
                market = %scope_market,
                token = %scope_token,
                label = request.label,
                error = %err,
                "cancel_scope_verify_error"
            );
            return ScopedCancelHealResult {
                scope: request.scope,
                order_id: request.order_id,
                label: request.label,
                final_open_ids: Err(format!("cancel_scope_verify_error: {err:#}")),
            };
        }
    };

    if !scope_ids.iter().any(|id| id == &request.order_id) {
        tracing::info!(
            target: "execution.cancel_router",
            order_id = %request.order_id,
            market = %scope_market,
            token = %scope_token,
            label = request.label,
            "cancel_scope_verify_stale"
        );
        return ScopedCancelHealResult {
            scope: request.scope,
            order_id: request.order_id,
            label: request.label,
            final_open_ids: Ok(scope_ids),
        };
    }

    if let Err(err) = exec
        .cancel_market_orders(Some(scope_market.as_str()), Some(scope_token.as_str()))
        .await
    {
        tracing::error!(
            target: "execution.cancel_router",
            order_id = %request.order_id,
            market = %scope_market,
            token = %scope_token,
            label = request.label,
            error = %err,
            "cancel_scope_escalation_error"
        );
        return ScopedCancelHealResult {
            scope: request.scope,
            order_id: request.order_id,
            label: request.label,
            final_open_ids: Err(format!("cancel_scope_escalation_error: {err:#}")),
        };
    }

    let final_open_ids = match exec
        .open_order_ids_in_scope(Some(scope_market.as_str()), Some(scope_token.as_str()))
        .await
    {
        Ok(order_ids) => order_ids,
        Err(err) => {
            tracing::error!(
                target: "execution.cancel_router",
                order_id = %request.order_id,
                market = %scope_market,
                token = %scope_token,
                label = request.label,
                error = %err,
                "cancel_scope_reverify_error"
            );
            return ScopedCancelHealResult {
                scope: request.scope,
                order_id: request.order_id,
                label: request.label,
                final_open_ids: Err(format!("cancel_scope_reverify_error: {err:#}")),
            };
        }
    };

    ScopedCancelHealResult {
        scope: request.scope,
        order_id: request.order_id,
        label: request.label,
        final_open_ids: Ok(final_open_ids),
    }
}

pub(super) async fn run_scoped_cancel_heal_worker(
    exec: ExecutionClient,
    mut scoped_heal_rx: mpsc::UnboundedReceiver<ScopedCancelHealRequest>,
    scoped_heal_result_tx: mpsc::UnboundedSender<ScopedCancelHealResult>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut stopping = false;
    let mut requests_open = true;
    let mut delivery_failed = false;
    let mut active_scopes = FuturesUnordered::new();
    while requests_open || !active_scopes.is_empty() {
        tokio::select! {
            _ = shutdown.cancelled(), if !stopping => {
                stopping = true;
                scoped_heal_rx.close();
            },
            maybe_request = scoped_heal_rx.recv(), if requests_open => {
                match maybe_request {
                    Some(request) => {
                        active_scopes.push(verify_and_heal_failed_cancel_order_scope(&exec, request));
                    }
                    None => requests_open = false,
                }
            },
            Some(result) = active_scopes.next(), if !active_scopes.is_empty() => {
                if scoped_heal_result_tx.send(result).is_err() {
                    delivery_failed = true;
                    stopping = true;
                    scoped_heal_rx.close();
                }
            }
        }
    }
    anyhow::ensure!(!delivery_failed, "cancel heal result delivery closed");
    Ok(())
}

pub(super) fn apply_scoped_cancel_heal_result(
    now_ms: u64,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    obs: &Observability,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    result: ScopedCancelHealResult,
    target_ids: Option<&HashSet<String>>,
    has_unresolved_submission: bool,
) -> Result<()> {
    scoped_heal_inflight.remove(&result.scope);
    match result.final_open_ids {
        Ok(remote_ids) => {
            reconcile_local_order_scope(
                now_ms,
                risk,
                open_orders,
                &result.scope.market,
                &result.scope.token,
                &remote_ids,
                target_ids,
            );
            let local_scope_present = open_orders.orders.values().any(|order| {
                order.market == result.scope.market && order.token == result.scope.token
            });
            if remote_ids.is_empty() && !local_scope_present && !has_unresolved_submission {
                fenced_scopes.remove(&result.scope);
                return Ok(());
            }
            fenced_scopes.insert(result.scope.clone());
            let detail = BTreeMap::from([
                ("label".to_string(), result.label.to_string()),
                ("order_id".to_string(), result.order_id.clone()),
                ("market".to_string(), result.scope.market.clone()),
                ("token".to_string(), result.scope.token.clone()),
                ("open_count".to_string(), remote_ids.len().to_string()),
            ]);
            obs.emit_action(ActionEvent::RuntimeIssue {
                ts_ms: now_ms,
                scope: "execution".to_string(),
                kind: RuntimeIssueKind::StuckOpenOrder,
                msg: Some(format!(
                    "stuck_open_order order_id={} market={} token={} label={} open_count={}",
                    result.order_id,
                    result.scope.market,
                    result.scope.token,
                    result.label,
                    remote_ids.len()
                )),
                detail,
            });
            tracing::error!(
                target: "execution.cancel_router",
                order_id = %result.order_id,
                market = %result.scope.market,
                token = %result.scope.token,
                label = result.label,
                open_count = remote_ids.len(),
                "cancel_scope_verify_non_empty"
            );
        }
        Err(error) => {
            fenced_scopes.insert(result.scope.clone());
            return Err(anyhow::anyhow!(
                "cancel scope heal failed market={} token={} order_id={} label={} error={error}",
                result.scope.market,
                result.scope.token,
                result.order_id,
                result.label
            ));
        }
    }
    Ok(())
}

pub(super) async fn clean_inventory_market_to_empty(
    clock: &ExecutionSupervisorClock,
    allow_wall_sleep: bool,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    market: &CancelMarketRequest,
    obs: &Observability,
    label: &str,
) -> bool {
    let active_market_ids = |state: &OpenOrdersState| -> Vec<String> {
        state
            .orders
            .values()
            .filter(|order| {
                market_identifier_matches(order.market.as_str(), market)
                    && !order.status.eq_ignore_ascii_case("CANCELED")
                    && !crate::polymarket::fixed_math::below_one_fixed_6dp_unit(
                        order.remaining_size(),
                    )
            })
            .map(|order| order.order_id.clone())
            .collect()
    };
    for attempt in 1..=INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS {
        let cancel_result = if clock.is_live() {
            exec.cancel_market_orders(Some(market.market_id.as_str()), None)
                .await
        } else {
            exec.cancel_orders(&active_market_ids(open_orders)).await
        };
        let cancel_receipt_ms = clock.now_ms();
        match cancel_result {
            Ok(ack) => {
                let _ = apply_cancel_ack(
                    cancel_receipt_ms,
                    risk,
                    ack,
                    open_orders,
                    obs,
                    "CANCEL_OK",
                    &format!("{label}_failed"),
                );
            }
            Err(err) => {
                tracing::error!(
                    target: "execution.cancel_router",
                    market = %market.market_id,
                    gamma_market_id = market.gamma_market_id.as_deref().unwrap_or("-"),
                    attempt,
                    error = %err,
                    label = label,
                    "cancel_market_error"
                );
            }
        }

        let verify_result = if clock.is_live() {
            exec.open_order_ids_in_scope(Some(market.market_id.as_str()), None)
                .await
        } else {
            Ok(active_market_ids(open_orders))
        };
        let verify_receipt_ms = clock.now_ms();
        match verify_result {
            Ok(order_ids) => {
                if clock.is_live() {
                    reconcile_local_market_orders(
                        verify_receipt_ms,
                        risk,
                        open_orders,
                        market,
                        &order_ids,
                        None,
                    );
                }
                if order_ids.is_empty() {
                    risk.release_market_inflight_buy_notional(&market.market_id);
                    tracing::info!(
                        target: "execution.cancel_router",
                        market = %market.market_id,
                        gamma_market_id = market.gamma_market_id.as_deref().unwrap_or("-"),
                        attempt,
                        label = label,
                        "cancel_market_verify_ok"
                    );
                    return true;
                }
                obs.emit_action(ActionEvent::RuntimeIssue {
                    ts_ms: verify_receipt_ms,
                    scope: "execution".to_string(),
                    kind: RuntimeIssueKind::StrategyCancelMarketVerifyNonEmpty,
                    msg: Some(format!(
                        "{label}_verify_non_empty market={} gamma_market_id={} attempt={attempt} open_count={}",
                        market.market_id,
                        market.gamma_market_id.as_deref().unwrap_or("-"),
                        order_ids.len()
                    )),
                    detail: BTreeMap::from([
                        ("label".to_string(), label.to_string()),
                        ("market".to_string(), market.market_id.to_string()),
                        (
                            "gamma_market_id".to_string(),
                            market.gamma_market_id.as_deref().unwrap_or("-").to_string(),
                        ),
                        ("attempt".to_string(), attempt.to_string()),
                        ("open_count".to_string(), order_ids.len().to_string()),
                    ]),
                });
            }
            Err(err) => {
                tracing::error!(
                    target: "execution.cancel_router",
                    market = %market.market_id,
                    gamma_market_id = market.gamma_market_id.as_deref().unwrap_or("-"),
                    attempt,
                    error = %err,
                    label = label,
                    "cancel_market_verify_error"
                );
            }
        }

        if allow_wall_sleep && attempt < INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(
                INVENTORY_MARKET_CLEAN_VERIFY_DELAY_MS,
            ))
            .await;
        }
    }

    tracing::error!(
        target: "execution.cancel_router",
        market = %market.market_id,
        gamma_market_id = market.gamma_market_id.as_deref().unwrap_or("-"),
        attempts = INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS,
        label = label,
        "cancel_market_verify_timeout"
    );
    false
}

pub(super) fn market_identifier_matches(order_market: &str, market: &CancelMarketRequest) -> bool {
    order_market == market.market_id.as_str()
}

pub(super) fn reconcile_local_market_orders(
    now_ms: u64,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    market: &CancelMarketRequest,
    remote_ids: &[String],
    target_ids: Option<&HashSet<String>>,
) {
    let keep: HashSet<&str> = remote_ids.iter().map(String::as_str).collect();
    let removed_ids: Vec<String> = open_orders
        .orders
        .iter()
        .filter_map(|(id, order)| {
            (market_identifier_matches(order.market.as_str(), market)
                && target_ids.is_none_or(|targets| targets.contains(id))
                && !order.status.eq_ignore_ascii_case("CANCELED")
                && !keep.contains(id.as_str()))
            .then_some(id.clone())
        })
        .collect();
    for id in &removed_ids {
        open_orders.orders.remove(id);
        risk.release_inflight_buy_order(id.as_str());
    }
    if !removed_ids.is_empty() {
        open_orders.note_runtime_mutation(now_ms);
    }
}

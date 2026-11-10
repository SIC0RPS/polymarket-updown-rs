use super::*;

pub(in crate::execution::supervisor) fn quote_cancel_key_from_order(
    order: &OpenOrderState,
) -> Option<QuoteCancelKey> {
    Some(QuoteCancelKey {
        market: Some(order.market.clone()),
        quote_group: order.quote_group.clone()?,
        side: Some(order.side),
    })
}

pub(in crate::execution::supervisor) fn register_quote_cancel_key(
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    order: &OpenOrderState,
) {
    let Some(key) = quote_cancel_key_from_order(order) else {
        return;
    };
    seen_quote_cancel_keys.insert(key.clone());
    live_quote_cancel_index
        .entry(key)
        .or_default()
        .insert(order.order_id.clone());
}

#[derive(Debug)]
pub(super) enum QuoteCancelResolution {
    Orders(Vec<String>),
    AlreadyAbsent,
    UnindexedNoop,
}

pub(super) fn resolve_quote_cancel_order_ids(
    open_orders: &OpenOrdersState,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &HashSet<QuoteCancelKey>,
    request: &CancelQuotesRequest,
) -> QuoteCancelResolution {
    let key_matches_request = |key: &QuoteCancelKey| {
        key.quote_group == request.quote_group
            && request
                .market_id
                .as_ref()
                .is_none_or(|market_id| key.market.as_deref() == Some(market_id.as_str()))
            && request.side.is_none_or(|side| key.side == Some(side))
    };
    let order_matches_request = |order: &OpenOrderState| {
        order.quote_group.as_deref() == Some(request.quote_group.as_str())
            && request
                .market_id
                .as_ref()
                .is_none_or(|market_id| order.market == market_id.as_str())
            && request.side.is_none_or(|side| order.side == side)
    };

    let mut indexed_ids = Vec::new();
    live_quote_cancel_index.retain(|key, order_ids| {
        order_ids.retain(|order_id| open_orders.orders.contains_key(order_id));
        if key_matches_request(key) {
            indexed_ids.extend(order_ids.iter().cloned());
        }
        !order_ids.is_empty()
    });
    indexed_ids.sort();
    indexed_ids.dedup();
    if !indexed_ids.is_empty() {
        return QuoteCancelResolution::Orders(indexed_ids);
    }

    let direct_match_ids: Vec<String> = open_orders
        .orders
        .values()
        .filter(|order| order_matches_request(order))
        .map(|order| order.order_id.clone())
        .collect();
    if !direct_match_ids.is_empty() {
        return QuoteCancelResolution::Orders(direct_match_ids);
    }

    if seen_quote_cancel_keys.iter().any(key_matches_request) {
        QuoteCancelResolution::AlreadyAbsent
    } else {
        QuoteCancelResolution::UnindexedNoop
    }
}

pub(super) async fn handle_cancel_quote_request(
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &HashSet<QuoteCancelKey>,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    request: &CancelQuotesRequest,
    obs: &Observability,
    active_cancellations: Option<&mut ActiveCancellations>,
) -> Result<Vec<String>> {
    let market_label = request.market_id.as_deref().unwrap_or("-");
    let side_label = quote_cancel_side_label(request.side);
    let order_ids = match resolve_quote_cancel_order_ids(
        open_orders,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        request,
    ) {
        QuoteCancelResolution::Orders(order_ids) => order_ids,
        QuoteCancelResolution::AlreadyAbsent => {
            tracing::debug!(
                target: "execution.cancel_router",
                market = %market_label,
                quote_group = %request.quote_group,
                side = %side_label,
                "strategy_cancel_quotes_already_absent"
            );
            return Ok(Vec::new());
        }
        QuoteCancelResolution::UnindexedNoop => {
            tracing::debug!(
                target: "execution.cancel_router",
                market = %market_label,
                quote_group = %request.quote_group,
                side = %side_label,
                "strategy_cancel_quotes_unindexed_noop"
            );
            return Ok(Vec::new());
        }
    };

    if let Some(active) = active_cancellations {
        active.schedule_orders(
            exec,
            ActiveCancellations::targets_for_ids(open_orders, &order_ids),
            "strategy_cancel_quotes",
            "CANCEL_OK",
        );
        return Ok(Vec::new());
    }
    let cancel_result = exec.cancel_orders(&order_ids).await;
    let receipt_now_ms = clock.now_ms();
    match cancel_result {
        Ok(ack) => {
            let requested = order_ids.len();
            let failed_ids = apply_cancel_ack(
                receipt_now_ms,
                risk,
                ack,
                open_orders,
                obs,
                "CANCEL_OK",
                "strategy_cancel_quotes_failed",
            );
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &failed_ids,
                "strategy_cancel_quotes",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
            tracing::info!(
                target: "execution.cancel_router",
                market = %market_label,
                quote_group = %request.quote_group,
                side = %side_label,
                requested,
                "strategy_cancel_quotes_ok"
            );
            Ok(failed_ids)
        }
        Err(err) => {
            tracing::error!(
                target: "execution.cancel_router",
                market = %market_label,
                quote_group = %request.quote_group,
                side = %side_label,
                error = %err,
                "strategy_cancel_quotes_error"
            );
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &order_ids,
                "strategy_cancel_quotes",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
            Ok(order_ids)
        }
    }
}

fn quote_cancel_side_label(side: Option<Side>) -> &'static str {
    match side {
        Some(Side::Buy) => "buy",
        Some(Side::Sell) => "sell",
        None => "-",
    }
}

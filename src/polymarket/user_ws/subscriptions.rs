use super::*;

pub(super) fn sync_subscriptions(
    ws: &AuthenticatedWsClient,
    subscribed: &mut HashSet<B256>,
    orders_stream: &mut Option<OrderEventStream<'static>>,
    trades_stream: &mut Option<TradeEventStream<'static>>,
    snapshot: &PolymarketOddsState,
) -> Result<()> {
    let desired = desired_markets(snapshot);
    let desired_set: HashSet<B256> = desired.iter().copied().collect();

    for id in subscribed
        .difference(&desired_set)
        .copied()
        .collect::<Vec<_>>()
    {
        if let Err(err) = emit_user_unsubscribe(ws, id, "orders") {
            tracing::error!(target: "polymarket.user_ws", market = %id, error = %err, "unsubscribe_orders_failed");
        }
        if let Err(err) = emit_user_unsubscribe(ws, id, "trades") {
            tracing::error!(target: "polymarket.user_ws", market = %id, error = %err, "unsubscribe_trades_failed");
        }
        subscribed.remove(&id);
    }

    let mut created_streams = false;
    if orders_stream.is_none() && !desired.is_empty() {
        *orders_stream = Some(
            ws.subscribe_orders(desired.clone())
                .context("failed to subscribe to user orders")?
                .boxed(),
        );
        created_streams = true;
        tracing::info!(target: "polymarket.user_ws", markets = desired.len(), "subscribed_orders");
    }

    if trades_stream.is_none() && !desired.is_empty() {
        *trades_stream = Some(
            ws.subscribe_trades(desired.clone())
                .context("failed to subscribe to user trades")?
                .boxed(),
        );
        created_streams = true;
        tracing::info!(target: "polymarket.user_ws", markets = desired.len(), "subscribed_trades");
    }

    if created_streams && orders_stream.is_some() && trades_stream.is_some() {
        for id in desired_set.difference(subscribed) {
            tracing::info!(target: "polymarket.user_ws", market = %id, "subscribed_user_market");
        }
        *subscribed = desired_set;
        return Ok(());
    }

    for id in desired_set
        .difference(subscribed)
        .copied()
        .collect::<Vec<_>>()
    {
        emit_user_subscribe(ws, id, "orders")
            .with_context(|| format!("failed to subscribe to user orders market={id}"))?;
        if let Err(err) = emit_user_subscribe(ws, id, "trades") {
            let _ = emit_user_unsubscribe(ws, id, "orders");
            return Err(err)
                .with_context(|| format!("failed to subscribe to user trades market={id}"));
        }
        subscribed.insert(id);
        tracing::info!(target: "polymarket.user_ws", market = %id, "subscribed_user_market");
    }

    Ok(())
}

fn emit_user_subscribe(ws: &AuthenticatedWsClient, market: B256, kind: &str) -> Result<()> {
    let result = match kind {
        "orders" => ws.subscribe_orders(vec![market]).map(|_| ()),
        "trades" => ws.subscribe_trades(vec![market]).map(|_| ()),
        other => anyhow::bail!("unsupported subscription kind {other}"),
    };
    Ok(result?)
}

fn emit_user_unsubscribe(ws: &AuthenticatedWsClient, market: B256, kind: &str) -> Result<()> {
    let result = match kind {
        "orders" => ws.unsubscribe_orders(&[market]),
        "trades" => ws.unsubscribe_trades(&[market]),
        other => anyhow::bail!("unsupported unsubscribe kind {other}"),
    };
    Ok(result?)
}

fn desired_markets(snapshot: &PolymarketOddsState) -> Vec<B256> {
    let mut out = Vec::new();
    for market in &snapshot.markets {
        if let Some(id) = market
            .condition_id_b256
            .as_deref()
            .and_then(|raw| B256::from_str(raw).ok())
        {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    if let Some(selection) = snapshot
        .selection
        .as_ref()
        .and_then(|selection| selection.condition_id_b256.as_deref())
        .and_then(|raw| B256::from_str(raw).ok())
    {
        out.push(selection);
    }
    if let Some(next) = snapshot
        .next_selection
        .as_ref()
        .and_then(|selection| selection.condition_id_b256.as_deref())
        .and_then(|raw| B256::from_str(raw).ok())
    {
        if !out.contains(&next) {
            out.push(next);
        }
    }
    out
}

use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::StreamExt as _;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::ApiKey;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::ws::types::response::{OrderMessage, TradeMessage};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::types::B256;
use polymarket_client_sdk::ws::config::{Config as WsConfig, ReconnectConfig as WsReconnectConfig};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capture::session::{CaptureMsg, MaybeCapture};
use crate::execution::supervisor::ExecutionOrderEvent;
use crate::execution::{ExecutionClient, PolymarketUserWsAuth};
use crate::observability::Observability;
use crate::orders::OrderUpdateEvent;
use crate::polymarket::fills::{
    decimal_to_f64, ts_to_ms, ws_trade_observations, ws_trade_to_update,
};
use crate::polymarket::order_context::{OrderFillContext, OrderFillContextRegistry};
use crate::polymarket::state::PolymarketOddsState;
use crate::polymarket::user_ws_capture::{
    user_order_capture, user_trade_belongs_to_us, user_trade_capture,
};
use crate::portfolio::{FillEvent, NormalizedStrategyFill};
use crate::trade_session_ledger::ObservedCtfTradeFact;
use crate::types::Side;

mod subscriptions;
use subscriptions::sync_subscriptions;
#[path = "user_ws/submission_recovery.rs"]
mod submission_recovery;
use submission_recovery::{
    authorize_trade_message, handoff_order_update, ActiveOrderHandoffs, ActiveTradeAuthorizations,
    AuthorizedTradeMessage,
};
#[path = "user_ws/receipt_delivery.rs"]
mod receipt_delivery;
use receipt_delivery::{ReceiptAdmission, ReceiptDelivery};

type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;
type OrderEventStream<'a> =
    futures_util::stream::BoxStream<'a, polymarket_client_sdk::Result<OrderMessage>>;
type TradeEventStream<'a> =
    futures_util::stream::BoxStream<'a, polymarket_client_sdk::Result<TradeMessage>>;
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const WS_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
struct AuthorizedFill {
    fill: FillEvent,
    context: OrderFillContext,
    matched_trade: ObservedCtfTradeFact,
}

pub struct PolymarketUserWsRuntime {
    task: JoinHandle<Result<()>>,
}

pub struct PolymarketUserWsStart {
    pub ws_url: String,
    pub auth: PolymarketUserWsAuth,
    pub exec: ExecutionClient,
    pub fills_tx: mpsc::Sender<NormalizedStrategyFill>,
    pub orders_tx: mpsc::Sender<ExecutionOrderEvent>,
    pub order_fill_ctx: OrderFillContextRegistry,
    pub odds_rx: watch::Receiver<Arc<PolymarketOddsState>>,
    pub obs: Observability,
    pub capture: MaybeCapture,
    pub shutdown: CancellationToken,
}

impl PolymarketUserWsRuntime {
    pub async fn start(start: PolymarketUserWsStart) -> Result<Self> {
        let task = tokio::spawn(async move { run_polymarket_user_ws(start).await });
        Ok(Self { task })
    }

    pub async fn shutdown(self) -> Result<()> {
        self.task.await??;
        Ok(())
    }
}

async fn run_polymarket_user_ws(start: PolymarketUserWsStart) -> Result<()> {
    let PolymarketUserWsStart {
        ws_url,
        auth,
        exec,
        fills_tx,
        orders_tx,
        order_fill_ctx,
        mut odds_rx,
        obs,
        capture,
        shutdown,
    } = start;
    let mut ws_cfg = WsConfig::default();
    ws_cfg.heartbeat_interval = WS_HEARTBEAT_INTERVAL;
    ws_cfg.heartbeat_timeout = WS_HEARTBEAT_TIMEOUT;
    let mut reconnect_cfg = WsReconnectConfig::default();
    reconnect_cfg.max_attempts = None;
    reconnect_cfg.initial_backoff = Duration::from_millis(250);
    reconnect_cfg.max_backoff = Duration::from_secs(5);
    reconnect_cfg.backoff_multiplier = 2.0;
    ws_cfg.reconnect = reconnect_cfg;
    let own_api_key = auth.credentials.key();
    let ws = loop {
        let client = WsClient::new(&ws_url, ws_cfg.clone())
            .with_context(|| format!("failed to create Polymarket user WS client for {ws_url}"))
            .and_then(|client| {
                client
                    .authenticate(auth.credentials.clone(), auth.address)
                    .context("failed to authenticate Polymarket user WS client")
            });
        match client {
            Ok(client) => break client,
            Err(error) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    error = %format!("{error:#}"),
                    "user_ws_client_start_failed"
                );
                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    };

    let mut subscribed: HashSet<B256> = HashSet::new();
    let mut orders_stream = None::<OrderEventStream<'static>>;
    let mut trades_stream = None::<TradeEventStream<'static>>;
    let mut active_trade_authorizations = ActiveTradeAuthorizations::new();
    let mut active_order_handoffs = ActiveOrderHandoffs::new();
    let max_active_authorizations = orders_tx.max_capacity();
    let max_active_order_handoffs = orders_tx.max_capacity();
    let mut pending_authorized_fills = None::<VecDeque<AuthorizedFill>>;
    let finalized_sell_chain_id = exec.finalized_sell_chain_id();
    let mut receipt_delivery = ReceiptDelivery::new(exec, fills_tx, obs, capture.clone());

    if let Err(error) = sync_subscriptions(
        &ws,
        &mut subscribed,
        &mut orders_stream,
        &mut trades_stream,
        &odds_rx.borrow(),
    ) {
        tracing::error!(
            target: "polymarket.user_ws",
            error = %format!("{error:#}"),
            "user_ws_subscription_sync_failed"
        );
    }

    let mut delivery_failure = None::<String>;
    let mut ws = Some(ws);
    let mut last_err_sent_at = Instant::now() - Duration::from_secs(60);
    let mut odds_open = true;
    let mut subscription_sync_tick = tokio::time::interval(Duration::from_millis(250));
    subscription_sync_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        if ws.is_none() && orders_stream.is_none() && trades_stream.is_none() {
            break;
        }
        admit_authorized_fills(
            &mut pending_authorized_fills,
            &mut receipt_delivery,
            &mut delivery_failure,
        );
        tokio::select! {
            _ = shutdown.cancelled(), if ws.is_some() => {
                let client = ws.take().expect("authenticated client disappeared before teardown");
                if let Err(error) = client.deauthenticate() {
                    delivery_failure.get_or_insert_with(|| {
                        format!("authenticated publisher teardown failed: {error:#}")
                    });
                }
                odds_open = false;
            }
            _ = receipt_delivery.drive(), if receipt_delivery.has_work() => {}
            handoff = active_order_handoffs.next(), if !active_order_handoffs.is_empty() => {
                if let Some((order_id, Err(error))) = handoff {
                    tracing::error!(
                        target: "polymarket.user_ws",
                        order_id,
                        error,
                        "order_handoff_failed"
                    );
                    delivery_failure.get_or_insert(error);
                }
            }
            _ = subscription_sync_tick.tick(), if ws.is_some() && odds_open => {
                if let Some(ws) = ws.as_ref() {
                    if let Err(error) = sync_subscriptions(
                        ws,
                        &mut subscribed,
                        &mut orders_stream,
                        &mut trades_stream,
                        &odds_rx.borrow(),
                    ) {
                        tracing::error!(
                            target: "polymarket.user_ws",
                            error = %format!("{error:#}"),
                            "user_ws_subscription_sync_failed"
                        );
                    }
                }
            }
            changed = odds_rx.changed(), if ws.is_some() && odds_open => {
                if changed.is_err() {
                    odds_open = false;
                    tracing::error!(target: "polymarket.user_ws", "user_ws_odds_input_closed");
                    continue;
                }
                let snapshot = odds_rx.borrow_and_update();
                if let Err(error) = sync_subscriptions(
                    ws.as_ref().expect("subscription client disappeared"),
                    &mut subscribed,
                    &mut orders_stream,
                    &mut trades_stream,
                    &snapshot,
                ) {
                    tracing::error!(
                        target: "polymarket.user_ws",
                        error = %format!("{error:#}"),
                        "user_ws_subscription_sync_failed"
                    );
                }
            }
            next = async {
                if let Some(s) = &mut trades_stream {
                    s.next().await
                } else {
                    None
                }
            }, if trades_stream.is_some()
                && active_trade_authorizations.len() < max_active_authorizations => {
                let Some(result) = next else {
                    trades_stream = None;
                    if let Some(ws) = ws.as_ref() {
                        sync_subscriptions(
                            ws,
                            &mut subscribed,
                            &mut orders_stream,
                            &mut trades_stream,
                            &odds_rx.borrow(),
                        ).unwrap_or_else(|error| {
                            tracing::error!(
                                target: "polymarket.user_ws",
                                error = %format!("{error:#}"),
                                "user_ws_subscription_sync_failed"
                            );
                        });
                    }
                    continue;
                };
                match result {
                    Ok(trade) => {
                        let recv_ts = now_ms();
                        let event_ts = trade.matchtime.or(trade.timestamp).unwrap_or(recv_ts as i64);
                        if user_trade_belongs_to_us(&trade, own_api_key) {
                            capture.emit(CaptureMsg::PolymarketUserTrade(
                                user_trade_capture(&trade, recv_ts, event_ts),
                            ));
                        }
                        let observations = match ws_trade_observations(&trade, own_api_key) {
                            Ok(observations) => observations,
                            Err(error) => {
                                tracing::error!(target: "polymarket.user_ws", error = %error, "bad_trade_observation");
                                continue;
                            }
                        };
                        active_trade_authorizations.push(authorize_trade_message(
                            trade,
                            observations,
                            finalized_sell_chain_id,
                            orders_tx.clone(),
                            shutdown.clone(),
                        ));
                    }
                    Err(err) => {
                        record_receiver_loss(&err, &capture, &mut delivery_failure);
                        let now = Instant::now();
                        if now.duration_since(last_err_sent_at) >= Duration::from_secs(1) {
                            last_err_sent_at = now;
                            tracing::error!(target: "polymarket.user_ws", error = %err, "trade_stream_error");
                        }
                    }
                }
            }
            authorized = active_trade_authorizations.next(), if pending_authorized_fills.is_none()
                && !active_trade_authorizations.is_empty() => {
                let authorized = authorized
                    .expect("nonempty trade authorization set produced no completion");
                if let Some(error) = authorized.failure.as_ref() {
                    delivery_failure.get_or_insert_with(|| error.clone());
                }
                match authorized_trade_fills(authorized) {
                    Ok(fills) if fills.is_empty() => {}
                    Ok(fills) => {
                        pending_authorized_fills = Some(fills);
                    }
                    Err(error) => {
                        tracing::error!(target: "polymarket.user_ws", error = %error, "bad_trade_message");
                        delivery_failure.get_or_insert_with(|| format!("{error:#}"));
                    }
                }
            }
            next = async {
                if let Some(s) = &mut orders_stream {
                    s.next().await
                } else {
                    None
                }
            }, if orders_stream.is_some()
                && active_order_handoffs.len() < max_active_order_handoffs => {
                let Some(result) = next else {
                    orders_stream = None;
                    if let Some(ws) = ws.as_ref() {
                        sync_subscriptions(
                            ws,
                            &mut subscribed,
                            &mut orders_stream,
                            &mut trades_stream,
                            &odds_rx.borrow(),
                        ).unwrap_or_else(|error| {
                            tracing::error!(
                                target: "polymarket.user_ws",
                                error = %format!("{error:#}"),
                                "user_ws_subscription_sync_failed"
                            );
                        });
                    }
                    continue;
                };
                match result {
                    Ok(order) => {
                        let recv_ts = now_ms();
                        let event_ts = order.timestamp.unwrap_or(recv_ts as i64);
                        match to_order_update(&order, own_api_key) {
                            Ok(Some(update)) => {
                                capture.emit(CaptureMsg::PolymarketUserOrder(user_order_capture(
                                    &order, recv_ts, event_ts,
                                )));
                                order_fill_ctx.record_order_update(&update);
                                let context = order_fill_ctx.owned_context(&update.order_id).ok();
                                if let Some(handoff) = handoff_order_update(
                                    update,
                                    context,
                                    &orders_tx,
                                ) {
                                    active_order_handoffs.push(handoff);
                                }
                            }
                            Ok(None) => {}
                            Err(err) => tracing::error!(target: "polymarket.user_ws", error = %err, "bad_order_message"),
                        }
                    }
                    Err(err) => {
                        record_receiver_loss(&err, &capture, &mut delivery_failure);
                        let now = Instant::now();
                        if now.duration_since(last_err_sent_at) >= Duration::from_secs(1) {
                            last_err_sent_at = now;
                            tracing::error!(target: "polymarket.user_ws", error = %err, "order_stream_error");
                        }
                    }
                }
            }
        }
    }

    while let Some((_, result)) = active_order_handoffs.next().await {
        if let Err(error) = result {
            delivery_failure.get_or_insert(error);
        }
    }
    while let Some(authorized) = active_trade_authorizations.next().await {
        if let Some(error) = authorized.failure.as_ref() {
            delivery_failure.get_or_insert_with(|| error.clone());
        }
        match authorized_trade_fills(authorized) {
            Ok(mut fills) => pending_authorized_fills
                .get_or_insert_with(VecDeque::new)
                .append(&mut fills),
            Err(error) => {
                delivery_failure.get_or_insert_with(|| format!("{error:#}"));
            }
        }
    }
    loop {
        admit_authorized_fills(
            &mut pending_authorized_fills,
            &mut receipt_delivery,
            &mut delivery_failure,
        );
        if let Err(error) = receipt_delivery.finish().await {
            delivery_failure.get_or_insert_with(|| format!("{error:#}"));
            break;
        }
        if pending_authorized_fills.is_none() {
            break;
        }
    }
    let pending_fills = pending_authorized_fills.as_ref().map_or(0, VecDeque::len);
    anyhow::ensure!(
        delivery_failure.is_none() && pending_fills == 0,
        "authenticated delivery shutdown incomplete: pending_authorized_fills={} application_failure={:?}",
        pending_fills, delivery_failure,
    );
    Ok(())
}

fn record_receiver_loss(
    error: &polymarket_client_sdk::error::Error,
    capture: &MaybeCapture,
    delivery_failure: &mut Option<String>,
) {
    if let Some(RecvError::Lagged(skipped)) = error.downcast_ref::<RecvError>() {
        capture.mark_required_capture_lost();
        delivery_failure
            .get_or_insert_with(|| format!("authenticated receiver lost {skipped} messages"));
    }
}

fn authorized_trade_fills(authorized: AuthorizedTradeMessage) -> Result<VecDeque<AuthorizedFill>> {
    let observations = authorized
        .observations
        .iter()
        .map(|authorized| authorized.observation.clone())
        .collect::<Vec<_>>();
    let update = ws_trade_to_update(&authorized.trade, observations)?;
    let mut contexts = authorized
        .observations
        .into_iter()
        .map(|authorized| {
            (
                authorized.observation.order_id().to_string(),
                (authorized.context, authorized.matched_trade),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut fills = VecDeque::with_capacity(update.fills.len());
    for fill in update.fills {
        let (context, matched_trade) = contexts.remove(&fill.order_id).with_context(|| {
            format!(
                "authorized fill has no owned context order_id={}",
                fill.order_id
            )
        })?;
        fills.push_back(AuthorizedFill {
            fill,
            context,
            matched_trade,
        });
    }
    anyhow::ensure!(
        contexts.is_empty(),
        "authorized observations did not produce exact fills"
    );
    Ok(fills)
}

/// Convert a WebSocket trade message into fill events.
///
/// When we are the **maker**, the `trade.size` field is the total taker sweep
/// across all makers — not our fill portion.  Our actual filled quantity lives
/// in `trade.maker_orders[].matched_amount`.  We emit one `FillEvent` per
/// maker-order entry with its own `matched_amount` and `order_id`.
///
/// When we are the **taker** (or role is unknown), `trade.size` is our fill.
fn to_order_update(order: &OrderMessage, own_api_key: ApiKey) -> Result<Option<OrderUpdateEvent>> {
    let is_owned = order.owner == Some(own_api_key) || order.order_owner == Some(own_api_key);
    if !is_owned {
        return Ok(None);
    }

    let px = decimal_to_f64(order.price)?;

    let side = match order.side {
        polymarket_client_sdk::clob::types::Side::Buy => Side::Buy,
        polymarket_client_sdk::clob::types::Side::Sell => Side::Sell,
        _ => {
            // Non-exhaustive SDK enum; ignore unknown values defensively.
            return Ok(None);
        }
    };

    let ts = order.timestamp.unwrap_or(now_ms() as i64);
    let ts_ms = ts_to_ms(ts);

    let original_size = order
        .original_size
        .as_ref()
        .map(|v| decimal_to_f64(*v))
        .transpose()?;
    let matched_size = order
        .size_matched
        .as_ref()
        .map(|v| decimal_to_f64(*v))
        .transpose()?;

    let status = order.status.as_ref().map(ToString::to_string);
    let msg_type = order.msg_type.as_ref().map(|t| match t {
        polymarket_client_sdk::clob::ws::types::response::OrderMessageType::Placement => {
            "PLACEMENT".to_string()
        }
        polymarket_client_sdk::clob::ws::types::response::OrderMessageType::Update => {
            "UPDATE".to_string()
        }
        polymarket_client_sdk::clob::ws::types::response::OrderMessageType::Cancellation => {
            "CANCELLATION".to_string()
        }
        polymarket_client_sdk::clob::ws::types::response::OrderMessageType::Unknown(s) => s.clone(),
        _ => "UNKNOWN".to_string(),
    });

    Ok(Some(OrderUpdateEvent {
        ts_ms,
        order_id: order.id.clone(),
        market: order.market.to_string(),
        token: order.asset_id.to_string(),
        side,
        px,
        original_size,
        matched_size,
        status,
        msg_type,
        terminal_reason: None,
        terminal_reason_provenance: None,
    }))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn admit_authorized_fills(
    pending_authorized_fills: &mut Option<VecDeque<AuthorizedFill>>,
    receipt_delivery: &mut ReceiptDelivery,
    delivery_failure: &mut Option<String>,
) {
    if let Some(work) = pending_authorized_fills.as_mut() {
        while let Some(next) = work.front() {
            let needs_receipt = next.fill.receipt_qty_units_6dp.is_some();
            if needs_receipt && !receipt_delivery.can_admit_receipt() {
                break;
            }
            if !needs_receipt && !receipt_delivery.can_admit_direct() {
                break;
            }
            let next = work.pop_front().expect("authorized fill front disappeared");
            if needs_receipt {
                match receipt_delivery.admit_receipt(next.fill, next.context, next.matched_trade) {
                    ReceiptAdmission::Admitted | ReceiptAdmission::IdenticalPending => {}
                    ReceiptAdmission::Rejected(error) => {
                        tracing::error!(target: "polymarket.user_ws", error, "confirmed_sell_receipt_rejected");
                        delivery_failure.get_or_insert(error);
                    }
                }
            } else {
                receipt_delivery.admit_direct(next.fill, next.context, next.matched_trade);
            }
        }
        if work.is_empty() {
            *pending_authorized_fills = None;
        }
    }
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
use polymarket_client_sdk::clob::Client as ClobClient;
use polymarket_client_sdk::error::{Method as SdkMethod, Status as SdkStatus, StatusCode};
use polymarket_client_sdk::types::U256;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use super::runtime::health::PingResponseEstimator;
use super::runtime::{STATIC_SYMBOL, WS_SYMBOLS};
use crate::live_lanes::PolymarketStateLane;
use crate::state::{DiscoverySnapshot, MarketSelectionSnapshot, PolymarketOddsState};

use super::endpoint::market_ws_endpoint;
use super::{
    next_ws_connection_session_id, PolymarketBookAdmissionOutcome, PolymarketBookAdmissionRecord,
    PolymarketWsIngressMessageKind, PolymarketWsIngressRecord, PolymarketWsIngressRole,
    PolymarketWsRuntimeConfig, BOOK_ADMISSION_TIMEOUT, WS_CONNECT_TIMEOUT, WS_HEARTBEAT_INTERVAL,
    WS_HEARTBEAT_TIMEOUT,
};

mod subscription_updates;
use subscription_updates::{apply_market_ws_asset_update, send_initial_market_subscription};

pub(super) type MarketWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(super) type MarketWsWrite = SplitSink<MarketWsStream, Message>;
type MarketWsRead = SplitStream<MarketWsStream>;

#[derive(Clone, Copy)]
pub(super) struct ConnectedHealth {
    pub(super) connection_session_id: u64,
    pub(super) asset_count: usize,
    pub(super) connected_at: Instant,
}

#[derive(Clone, Copy)]
pub(super) struct PendingPongHealth {
    pub(super) ping_started: Instant,
    pub(super) hard_deadline: Instant,
    pub(super) health_allowance: Option<Duration>,
}

#[derive(Clone)]
pub(super) struct GenerationHealth {
    pub(super) revision: u64,
    pub(super) connected: Option<ConnectedHealth>,
    pub(super) subscribed: Arc<[U256]>,
    pub(super) subscription_observed_at: Option<Instant>,
    pub(super) pending_pong: Option<PendingPongHealth>,
}

impl GenerationHealth {
    pub(super) fn initial() -> Self {
        Self {
            revision: 0,
            connected: None,
            subscribed: Arc::from([]),
            subscription_observed_at: None,
            pending_pong: None,
        }
    }
}

pub(super) struct MarketFrame {
    pub(super) slot: usize,
    pub(super) generation: u64,
    pub(super) connection_session_id: u64,
    pub(super) sequence: u64,
    pub(super) recv_ts_ms: u64,
    pub(super) recv_at: Instant,
    pub(super) health: Arc<GenerationHealth>,
    pub(super) message: Message,
}

pub(super) struct MarketLegTerminal {
    pub(super) last_enqueued_sequence: u64,
    pub(super) loss_reason: Option<&'static str>,
    pub(super) channel_closed: bool,
    pub(super) outcome: Result<()>,
}

/// Runs one market WebSocket leg until it ends for any reason.
///
/// The reader owns socket and subscription I/O, heartbeat handling, receipt identity, and the
/// nonblocking frame handoff. REST admission and downstream market processing stay outside it.
pub(super) async fn run_market_leg(
    slot: usize,
    generation: u64,
    config: &PolymarketWsRuntimeConfig,
    assets_rx: &mut watch::Receiver<Vec<U256>>,
    frame_tx: &mpsc::Sender<MarketFrame>,
    health_tx: &watch::Sender<Arc<GenerationHealth>>,
    stop_rx: &mut watch::Receiver<bool>,
) -> Result<MarketLegTerminal> {
    anyhow::ensure!(!*stop_rx.borrow(), "Polymarket market reader stopped");
    let mut asset_ids = loop {
        let asset_ids = assets_rx.borrow_and_update().clone();
        if !asset_ids.is_empty() {
            break asset_ids;
        }
        tokio::select! {
            changed = stop_rx.changed() => {
                anyhow::ensure!(changed.is_ok() && !*stop_rx.borrow_and_update(),
                    "Polymarket market reader stopped");
            }
            changed = assets_rx.changed() => {
                changed.context(
                    "Polymarket market asset control channel closed before subscription"
                )?;
            }
        }
    };

    let endpoint = market_ws_endpoint(&config.ws_url);
    let connect = tokio::time::timeout(WS_CONNECT_TIMEOUT, connect_async(&endpoint));
    tokio::pin!(connect);
    let (mut ws, _) = tokio::select! {
        connected = &mut connect => connected
            .context("Polymarket market WebSocket connect timed out")?
            .with_context(|| format!("failed to connect Polymarket market WebSocket {endpoint}"))?,
        changed = stop_rx.changed() => {
            anyhow::ensure!(changed.is_ok() && !*stop_rx.borrow_and_update(),
                "Polymarket market reader stopped");
            unreachable!()
        }
    };
    let connection_session_id = next_ws_connection_session_id();
    let mut health = Arc::new(GenerationHealth {
        revision: 1,
        connected: Some(ConnectedHealth {
            connection_session_id,
            asset_count: asset_ids.len(),
            connected_at: Instant::now(),
        }),
        ..GenerationHealth::initial()
    });
    health_tx.send_replace(health.clone());

    {
        let initial_subscription = send_initial_market_subscription(&mut ws, &asset_ids);
        tokio::pin!(initial_subscription);
        tokio::select! {
            result = &mut initial_subscription => {
                result.context("failed to send initial Polymarket market subscription")?;
            }
            changed = stop_rx.changed() => {
                anyhow::ensure!(changed.is_ok() && !*stop_rx.borrow_and_update(),
                    "Polymarket market reader stopped");
            }
        }
    }
    health = Arc::new(GenerationHealth {
        revision: health.revision.saturating_add(1),
        subscribed: Arc::from(asset_ids.clone()),
        subscription_observed_at: Some(Instant::now()),
        ..(*health).clone()
    });
    health_tx.send_replace(health.clone());

    let (mut write, mut read): (MarketWsWrite, MarketWsRead) = ws.split();
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let pong_deadline = tokio::time::sleep(WS_HEARTBEAT_TIMEOUT);
    tokio::pin!(pong_deadline);
    let mut pending_pong = false;
    let mut response_estimator = PingResponseEstimator::default();
    let mut measured_ping_started = None;
    let mut response_attribution_valid = true;
    let mut last_enqueued_sequence = 0_u64;

    let outcome = loop {
        tokio::select! {
            changed = stop_rx.changed() => {
                if changed.is_err() || *stop_rx.borrow_and_update() {
                    let _ = write.close().await;
                    break Ok(());
                }
            }
            changed = assets_rx.changed() => {
                if changed.is_err() {
                    let _ = write.close().await;
                    break Err(anyhow::anyhow!(
                        "Polymarket market asset control channel closed while connected"
                    ));
                }
                let next_assets = assets_rx.borrow_and_update().clone();
                if let Err(err) = apply_market_ws_asset_update(
                    &mut write,
                    &mut asset_ids,
                    next_assets,
                )
                .await
                .context("failed to update Polymarket market subscription")
                {
                    break Err(err);
                }
                health = Arc::new(GenerationHealth {
                    revision: health.revision.saturating_add(1),
                    subscribed: Arc::from(asset_ids.clone()),
                    subscription_observed_at: Some(Instant::now()),
                    ..(*health).clone()
                });
                health_tx.send_replace(health.clone());
            }
            _ = heartbeat.tick() => {
                let ping_started = Instant::now();
                let frozen_health_allowance = response_estimator.allowance();
                if let Err(err) = write
                    .send(Message::Text("PING".into()))
                    .await
                    .context("failed to send Polymarket market heartbeat")
                {
                    break Err(err);
                }
                if pending_pong {
                    response_attribution_valid = false;
                    measured_ping_started = None;
                } else {
                    pending_pong = true;
                    measured_ping_started = response_attribution_valid.then_some(ping_started);
                    let hard_deadline = Instant::now() + WS_HEARTBEAT_TIMEOUT;
                    pong_deadline
                        .as_mut()
                        .reset(tokio::time::Instant::from_std(hard_deadline));
                    health = Arc::new(GenerationHealth {
                        revision: health.revision.saturating_add(1),
                        pending_pong: Some(PendingPongHealth {
                            ping_started,
                            hard_deadline,
                            health_allowance: frozen_health_allowance,
                        }),
                        ..(*health).clone()
                    });
                    health_tx.send_replace(health.clone());
                }
            }
            _ = &mut pong_deadline, if pending_pong => {
                let _ = write.close().await;
                break Err(anyhow::anyhow!("Polymarket market WebSocket PONG timeout"));
            }
            next = read.next() => {
                let recv_at = Instant::now();
                let recv_ts_ms = crate::oracle_state_updates::now_ms();
                match next {
                    Some(Ok(message)) => {
                        if matches!(&message, Message::Text(text) if text == "PONG") {
                            if pending_pong {
                                pending_pong = false;
                                if let Some(ping_started) = measured_ping_started.take() {
                                    response_estimator.observe(
                                        recv_at.saturating_duration_since(ping_started),
                                    );
                                }
                                health = Arc::new(GenerationHealth {
                                    revision: health.revision.saturating_add(1),
                                    pending_pong: None,
                                    ..(*health).clone()
                                });
                                health_tx.send_replace(health.clone());
                            }
                        }
                        let Some(sequence) = last_enqueued_sequence.checked_add(1) else {
                            return Ok(MarketLegTerminal {
                                last_enqueued_sequence,
                                loss_reason: Some("connection_session_sequence_overflow"),
                                channel_closed: false,
                                outcome: Err(anyhow::anyhow!(
                                    "Polymarket market connection sequence overflow"
                                )),
                            });
                        };
                        let is_close = matches!(message, Message::Close(_));
                        let ping_payload = match &message {
                            Message::Ping(payload) => Some(payload.clone()),
                            _ => None,
                        };
                        match frame_tx.try_send(MarketFrame {
                            slot,
                            generation,
                            connection_session_id,
                            sequence,
                            recv_ts_ms,
                            recv_at,
                            health: Arc::clone(&health),
                            message,
                        }) {
                            Ok(()) => last_enqueued_sequence = sequence,
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                return Ok(MarketLegTerminal {
                                    last_enqueued_sequence,
                                    loss_reason: Some("symbol_market_frame_channel_full"),
                                    channel_closed: false,
                                    outcome: Err(anyhow::anyhow!(
                                        "Polymarket symbol market-frame channel full"
                                    )),
                                });
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                return Ok(MarketLegTerminal {
                                    last_enqueued_sequence,
                                    loss_reason: Some("symbol_market_frame_channel_closed"),
                                    channel_closed: true,
                                    outcome: Err(anyhow::anyhow!(
                                        "Polymarket symbol market-frame channel closed"
                                    )),
                                });
                            }
                        }
                        if let Some(payload) = ping_payload {
                            if let Err(err) = write
                                .send(Message::Pong(payload))
                                .await
                                .context("failed to answer Polymarket WebSocket ping")
                            {
                                break Err(err);
                            }
                        }
                        if is_close {
                            break Err(anyhow::anyhow!(
                                "Polymarket market WebSocket closed remotely"
                            ));
                        }
                    }
                    Some(Err(err)) => {
                        break Err(err).context("Polymarket market WebSocket read failed");
                    }
                    None => {
                        break Err(anyhow::anyhow!(
                            "Polymarket market WebSocket stream ended"
                        ));
                    }
                }
            }
        }
    };

    Ok(MarketLegTerminal {
        last_enqueued_sequence,
        loss_reason: None,
        channel_closed: false,
        outcome,
    })
}

fn capture_book_admission(
    config: &PolymarketWsRuntimeConfig,
    asset_id: &U256,
    outcome: PolymarketBookAdmissionOutcome,
) {
    let Some(lane) = config.book_admission_capture.as_ref() else {
        return;
    };
    lane.try_capture(PolymarketBookAdmissionRecord {
        observed_at_ms: crate::oracle_state_updates::now_ms(),
        asset_id: asset_id.to_string(),
        outcome,
    });
}

pub(super) async fn fetch_book_admission(
    client: &ClobClient,
    asset_id: U256,
) -> PolymarketBookAdmissionOutcome {
    let request = OrderBookSummaryRequest::builder()
        .token_id(asset_id)
        .build();
    match tokio::time::timeout(BOOK_ADMISSION_TIMEOUT, client.order_book(&request)).await {
        Err(_elapsed) => {
            tracing::warn!(
                target: "engine.polymarket_ws",
                asset_id = %asset_id,
                timeout_ms = BOOK_ADMISSION_TIMEOUT.as_millis(),
                "bbwick_polymarket_rest_book_admission_timed_out"
            );
            PolymarketBookAdmissionOutcome::Failed {
                error: "polymarket book admission timed out".to_string(),
            }
        }
        Ok(Ok(response)) => PolymarketBookAdmissionOutcome::Seeded(Box::new(response)),
        Ok(Err(err)) if is_missing_book(&err) => PolymarketBookAdmissionOutcome::NoOrderbook,
        Ok(Err(err)) => {
            tracing::warn!(
                target: "engine.polymarket_ws",
                asset_id = %asset_id,
                error = %err,
                "bbwick_polymarket_rest_book_admission_failed"
            );
            PolymarketBookAdmissionOutcome::Failed {
                error: err.to_string(),
            }
        }
    }
}

pub(super) type AdmissionMarket = Option<MarketSelectionSnapshot>;

pub(super) fn same_admission_market(
    left: Option<&MarketSelectionSnapshot>,
    right: Option<&MarketSelectionSnapshot>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            left.market_id == right.market_id
                && left.gamma_market_id == right.gamma_market_id
                && left.condition_id == right.condition_id
                && left.condition_id_b256 == right.condition_id_b256
                && left
                    .underlying
                    .trim()
                    .eq_ignore_ascii_case(right.underlying.trim())
                && left.up_token == right.up_token
                && left.down_token == right.down_token
        }
        (None, None) => true,
        _ => false,
    }
}

fn admission_is_current(
    state: &PolymarketOddsState,
    asset_id: U256,
    market: Option<&MarketSelectionSnapshot>,
) -> bool {
    state.token_lifecycle.contains_key(&asset_id)
        && (market.is_some() || state.markets.is_empty())
        && same_admission_market(
            state
                .markets
                .iter()
                .find(|current| current.up_token == asset_id || current.down_token == asset_id),
            market,
        )
}

pub(super) fn apply_book_admission(
    config: &PolymarketWsRuntimeConfig,
    state_lane: &PolymarketStateLane,
    asset_id: U256,
    market: Option<&MarketSelectionSnapshot>,
    outcome: PolymarketBookAdmissionOutcome,
) -> Result<bool> {
    let mut accepted = false;
    state_lane.sync_state(|store, state, now| {
        if !admission_is_current(state, asset_id, market) {
            return false;
        }
        accepted = true;
        capture_book_admission(config, &asset_id, outcome.clone());
        match outcome {
            PolymarketBookAdmissionOutcome::Seeded(response) => {
                let changed = store.sync_rest_book(state, asset_id, &response, now);
                if !changed {
                    tracing::warn!(
                        target: "engine.polymarket_ws",
                        asset_id = %asset_id,
                        response_asset_id = %response.asset_id,
                        response_market = %response.market,
                        response_hash = ?response.hash,
                        "bbwick_polymarket_rest_book_admission_rejected"
                    );
                }
                changed
            }
            PolymarketBookAdmissionOutcome::NoOrderbook => {
                let changed = store.sync_tombstone_no_orderbook(state, asset_id, now);
                tracing::info!(
                    target: "engine.polymarket_ws",
                    asset_id = %asset_id,
                    "bbwick_polymarket_rest_book_admission_unavailable"
                );
                changed
            }
            PolymarketBookAdmissionOutcome::Failed { .. } => {
                store.sync_needs_snapshot(state, asset_id, now)
            }
        }
    })?;
    Ok(accepted)
}

pub(super) fn routing_is_current(
    state: &PolymarketOddsState,
    symbol: &str,
    desired: &[U256],
    required: &HashMap<U256, AdmissionMarket>,
) -> bool {
    if !desired.iter().all(|asset| {
        required
            .get(asset)
            .is_some_and(|market| admission_is_current(state, *asset, market.as_ref()))
    }) {
        return false;
    }
    if state.markets.is_empty() {
        return if symbol == STATIC_SYMBOL {
            state.token_lifecycle.len() <= 1 && state.token_lifecycle.len() == desired.len()
        } else {
            desired.is_empty()
        };
    }
    state.token_lifecycle.keys().all(|asset| {
        asset_symbol_index(&state.markets, *asset)
            .is_ok_and(|index| (WS_SYMBOLS[index] == symbol) == desired.contains(asset))
    })
}

pub(super) fn route_symbol_assets(
    snapshot: &DiscoverySnapshot,
    desired: &[U256],
) -> Result<[Vec<U256>; WS_SYMBOLS.len()]> {
    let mut routed: [Vec<U256>; WS_SYMBOLS.len()] = std::array::from_fn(|_| Vec::new());
    for asset in desired {
        routed[asset_symbol_index(&snapshot.markets, *asset)?].push(*asset);
    }
    Ok(routed)
}

fn asset_symbol_index(markets: &[MarketSelectionSnapshot], asset: U256) -> Result<usize> {
    let mut assigned = None;
    for market in markets {
        if market.up_token != asset && market.down_token != asset {
            continue;
        }
        let Some(index) = WS_SYMBOLS
            .iter()
            .position(|symbol| market.underlying.trim().eq_ignore_ascii_case(symbol))
        else {
            bail!(
                "required asset {asset} has unsupported underlying {}",
                market.underlying
            );
        };
        if assigned.is_some_and(|current| current != index) {
            bail!("required asset {asset} has conflicting symbol mappings");
        }
        assigned = Some(index);
    }
    assigned
        .ok_or_else(|| anyhow::anyhow!("required asset {asset} has no discovery symbol mapping"))
}

fn is_missing_book(err: &polymarket_client_sdk::error::Error) -> bool {
    err.downcast_ref::<SdkStatus>().is_some_and(|status| {
        status.method == SdkMethod::GET
            && matches!(
                status.status_code,
                StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST
            )
    })
}

pub(super) fn capture_market_ws_ingress(config: &PolymarketWsRuntimeConfig, frame: &MarketFrame) {
    let Some(capture) = config.ingress_capture.as_ref() else {
        return;
    };
    let Some(recv_mono_ns) = capture.recv_mono_ns(frame.recv_at) else {
        capture.mark_loss("receive_monotonic_origin_regression");
        return;
    };
    let (message_kind, payload, close_code, close_reason) = match &frame.message {
        Message::Text(text) => (
            PolymarketWsIngressMessageKind::Text,
            Some(text.as_bytes().to_vec()),
            None,
            None,
        ),
        Message::Binary(payload) => (
            PolymarketWsIngressMessageKind::Binary,
            Some(payload.clone()),
            None,
            None,
        ),
        Message::Ping(payload) => (
            PolymarketWsIngressMessageKind::Ping,
            Some(payload.clone()),
            None,
            None,
        ),
        Message::Pong(payload) => (
            PolymarketWsIngressMessageKind::Pong,
            Some(payload.clone()),
            None,
            None,
        ),
        Message::Close(frame) => (
            PolymarketWsIngressMessageKind::Close,
            None,
            frame.as_ref().map(|frame| u16::from(&frame.code)),
            frame.as_ref().map(|frame| frame.reason.to_string()),
        ),
        Message::Frame(_) => {
            capture.mark_loss("unexpected_raw_frame_from_tungstenite_read");
            return;
        }
    };
    capture.try_capture(PolymarketWsIngressRecord {
        recv_ts_ms: frame.recv_ts_ms,
        recv_mono_ns,
        symbol: "POLYMARKET".to_string(),
        role: PolymarketWsIngressRole::Primary,
        generation: frame.generation,
        connection_session_id: frame.connection_session_id,
        sequence: frame.sequence,
        message_kind,
        payload,
        close_code,
        close_reason,
    });
}

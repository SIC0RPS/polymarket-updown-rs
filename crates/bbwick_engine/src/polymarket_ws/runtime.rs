use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt as _;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::U256;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::live_lanes::PolymarketStateLane;
use crate::state::DiscoverySnapshot;

use super::assets::dedup_asset_ids;
use super::connection::{
    apply_book_admission, capture_market_ws_ingress, fetch_book_admission, route_symbol_assets,
    routing_is_current, same_admission_market, AdmissionMarket, GenerationHealth, MarketFrame,
    MarketLegTerminal,
};
use super::dedupe::EventDedupe;
use super::{PolymarketWsRuntimeConfig, WS_MARKET_LEG_COUNT};

mod coverage;
pub(super) mod health;
mod leg;
pub(super) use coverage::ServingLegs;
use leg::run_leg_forever;

const WS_DELIVERY_SLOT_COUNT: usize = WS_MARKET_LEG_COUNT * 2;
const CLOCK_PRECISION_NS: u64 = 1;
pub(super) const WS_SYMBOLS: [&str; 4] = ["BTC", "ETH", "XRP", "SOL"];
pub(super) const STATIC_SYMBOL: &str = "POLYMARKET";

struct MarketPair {
    symbol: &'static str,
    assets_tx: watch::Sender<Vec<U256>>,
    live_legs: Arc<ServingLegs>,
}

pub(super) type ReaderJoin = Arc<tokio::sync::Mutex<Option<JoinHandle<MarketLegTerminal>>>>;

struct RegisteredReader {
    slot: usize,
    generation: u64,
    stop_tx: watch::Sender<bool>,
    health_rx: watch::Receiver<Arc<GenerationHealth>>,
    task: ReaderJoin,
}

#[derive(Default)]
pub(super) struct LegReaders {
    readers: Mutex<Vec<RegisteredReader>>,
}

impl LegReaders {
    pub(super) fn register(
        &self,
        slot: usize,
        generation: u64,
        stop_tx: watch::Sender<bool>,
        health_rx: watch::Receiver<Arc<GenerationHealth>>,
        task: JoinHandle<MarketLegTerminal>,
    ) -> ReaderJoin {
        let task = Arc::new(tokio::sync::Mutex::new(Some(task)));
        self.readers
            .lock()
            .expect("market reader registry poisoned")
            .push(RegisteredReader {
                slot,
                generation,
                stop_tx,
                health_rx,
                task: Arc::clone(&task),
            });
        task
    }

    pub(super) fn remove(&self, slot: usize, generation: u64) {
        self.readers
            .lock()
            .expect("market reader registry poisoned")
            .retain(|reader| reader.slot != slot || reader.generation != generation);
    }

    async fn stop_and_join_remaining(
        &self,
        leg: usize,
        config: &PolymarketWsRuntimeConfig,
        live_legs: &ServingLegs,
    ) -> Result<()> {
        let readers = std::mem::take(
            &mut *self
                .readers
                .lock()
                .expect("market reader registry poisoned"),
        );
        for reader in &readers {
            reader.stop_tx.send_replace(true);
        }
        let mut first_error = None;
        for mut reader in readers {
            let joined = match reader.task.lock().await.take() {
                Some(task) => task.await.context("Polymarket market reader task failed"),
                None => continue,
            };
            let health = reader.health_rx.borrow_and_update().clone();
            live_legs.apply_generation_health(reader.slot, leg, reader.generation, health.as_ref());
            let settled = async {
                let terminal = joined?;
                if let (Some(capture), Some(reason)) =
                    (&config.ingress_capture, terminal.loss_reason)
                {
                    capture.mark_loss(reason);
                }
                if terminal.last_enqueued_sequence > 0 {
                    live_legs
                        .wait_for_processed(
                            reader.slot,
                            reader.generation,
                            terminal.last_enqueued_sequence,
                        )
                        .await?;
                }
                anyhow::ensure!(
                    !terminal.channel_closed,
                    "Polymarket symbol reducer channel closed"
                );
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if first_error.is_none() {
                first_error = settled.err();
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

#[derive(Clone, Copy)]
struct GenerationEvidence {
    leg: usize,
    generation: u64,
    launch_at: Instant,
    connection_session_id: Option<u64>,
    connected_at: Option<Instant>,
}

/// Supervises the redundant market WebSocket legs and returns their join handles to the engine.
///
/// Owns book admission and the asset set; the legs own only their sockets. Feed loss is a
/// routine event that the surviving leg absorbs and the supervisor repairs by rebuilding.
pub(crate) fn spawn_polymarket_ws_runtime(
    config: PolymarketWsRuntimeConfig,
    state_lane: PolymarketStateLane,
    mut control_rx: mpsc::Receiver<DiscoverySnapshot>,
) -> (JoinHandle<()>, watch::Sender<bool>) {
    let clob_cfg = ClobConfig::builder()
        .use_server_time(config.use_server_time)
        .build();
    let client = match ClobClient::new(&config.clob_http_url, clob_cfg) {
        Ok(client) => client,
        Err(err) => {
            let (shutdown_tx, _) = watch::channel(false);
            return (
                tokio::spawn(async move {
                    tracing::error!(
                        target: "engine.polymarket_ws",
                        error = %err,
                        clob_http_url = %config.clob_http_url,
                        "bbwick_polymarket_clob_client_unavailable"
                    );
                }),
                shutdown_tx,
            );
        }
    };

    let config = Arc::new(config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (fault_tx, mut fault_rx) = mpsc::unbounded_channel::<String>();
    let shutting_down = Arc::new(AtomicBool::new(false));
    let mut pairs = Vec::new();
    let mut legs = Vec::new();
    let mut reducers = Vec::new();
    for symbol in WS_SYMBOLS.into_iter().chain([STATIC_SYMBOL]) {
        let (pair, pair_legs, reducer) = spawn_market_pair(
            symbol,
            Arc::clone(&config),
            state_lane.clone(),
            shutdown_rx.clone(),
            fault_tx.clone(),
            Arc::clone(&shutting_down),
        );
        pairs.push(pair);
        legs.extend(pair_legs);
        reducers.push(reducer);
    }
    drop(fault_tx);

    let runtime_shutdown_tx = shutdown_tx.clone();
    let supervisor = tokio::spawn(async move {
        // Admission owns the REST path so no socket task ever awaits a venue response.
        let mut required: HashMap<U256, AdmissionMarket> = HashMap::new();
        let mut admitted: HashMap<U256, AdmissionMarket> = HashMap::new();
        let mut active = HashSet::new();
        let mut admissions = FuturesUnordered::new();
        let mut desired: [Vec<U256>; WS_SYMBOLS.len() + 1] = std::array::from_fn(|_| Vec::new());
        let mut runtime_shutdown_rx = shutdown_rx;
        let mut control_open = true;
        loop {
            tokio::select! {
                biased;
                changed = runtime_shutdown_rx.changed() => {
                    if changed.is_err() || *runtime_shutdown_rx.borrow_and_update() {
                        break;
                    }
                    continue;
                }
                fault = fault_rx.recv() => {
                    if let Some(fault) = fault {
                        tracing::error!(
                            target: "engine.polymarket_ws",
                            error = %fault,
                            "bbwick_polymarket_runtime_failed"
                        );
                    }
                    break;
                }
                snapshot = control_rx.recv(), if control_open => {
                    let snapshot = match snapshot {
                        Some(snapshot) => snapshot,
                        None => {
                            control_open = false;
                            tracing::warn!(
                                target: "engine.polymarket_ws",
                                "bbwick_polymarket_discovery_control_closed"
                            );
                            continue;
                        }
                    };
                    let next_assets = dedup_asset_ids(snapshot.asset_ids.clone());
                    let routed = if snapshot.markets.is_empty() {
                        if next_assets.len() > 1 {
                            tracing::error!(
                                target: "engine.polymarket_ws",
                                asset_count = next_assets.len(),
                                "bbwick_polymarket_ws_unmapped_required_assets"
                            );
                            Err(())
                        } else {
                            Ok(None)
                        }
                    } else {
                        match route_symbol_assets(&snapshot, &next_assets) {
                            Ok(routed) => Ok(Some(routed)),
                            Err(err) => {
                                tracing::error!(
                                    target: "engine.polymarket_ws",
                                    error = %err,
                                    asset_count = next_assets.len(),
                                    "bbwick_polymarket_ws_symbol_routing_failed"
                                );
                                Err(())
                            }
                        }
                    };
                    let Ok(routed) = routed else {
                        required.clear();
                        admitted.clear();
                        continue;
                    };
                    required = next_assets
                        .iter()
                        .map(|asset| {
                            let market = snapshot.markets.iter().find(|market| {
                                market.up_token == *asset || market.down_token == *asset
                            });
                            (*asset, market.cloned())
                        })
                        .collect();
                    desired = std::array::from_fn(|index| {
                        if index == WS_SYMBOLS.len() {
                            routed.as_ref().map(|_| Vec::new())
                                .unwrap_or_else(|| next_assets.clone())
                        } else {
                            routed.as_ref().map(|routes| routes[index].clone()).unwrap_or_default()
                        }
                    });
                    admitted.retain(|asset, market| {
                        required.get(asset).is_some_and(|required| {
                            same_admission_market(market.as_ref(), required.as_ref())
                        })
                    });
                }
                completion = admissions.next(), if !admissions.is_empty() => {
                    let Some((asset, market, outcome)) = completion else {
                        continue;
                    };
                    let market: AdmissionMarket = market;
                    active.remove(&asset);
                    if required.get(&asset).is_some_and(|required| {
                        same_admission_market(market.as_ref(), required.as_ref())
                    }) {
                        match apply_book_admission(
                            &config, &state_lane, asset, market.as_ref(), outcome,
                        ) {
                            Ok(true) => {
                                admitted.insert(asset, market);
                            }
                            Ok(false) => {}
                            Err(err) => {
                                tracing::error!(
                                    target: "engine.polymarket_ws",
                                    error = %err,
                                    asset_id = %asset,
                                    "bbwick_polymarket_book_admission_failed"
                                );
                                admitted.insert(asset, market);
                            }
                        }
                    }
                }
            }
            for (asset, market) in &required {
                if admitted.contains_key(asset) || active.contains(asset) {
                    continue;
                }
                let asset = *asset;
                let market = market.clone();
                let client = client.clone();
                active.insert(asset);
                admissions.push(async move {
                    let outcome = fetch_book_admission(&client, asset).await;
                    (asset, market, outcome)
                });
            }
            for (pair, pair_assets) in pairs.iter().zip(&desired) {
                if !pair_assets.iter().all(|asset| admitted.contains_key(asset))
                    || *pair.assets_tx.borrow() == *pair_assets
                {
                    continue;
                }
                match pair.live_legs.set_desired(
                    pair_assets.clone(),
                    |state| routing_is_current(state, pair.symbol, pair_assets, &required),
                    || {
                        let _ = pair.assets_tx.send(pair_assets.clone());
                    },
                ) {
                    Ok(true) => tracing::info!(
                        target: "engine.polymarket_ws",
                        symbol = pair.symbol,
                        asset_count = pair_assets.len(),
                        "bbwick_polymarket_ws_symbol_subscription_routed"
                    ),
                    Ok(false) => {}
                    Err(err) => tracing::error!(
                        target: "engine.polymarket_ws",
                        symbol = pair.symbol,
                        error = %err,
                        "bbwick_polymarket_coverage_observation_failed"
                    ),
                }
            }
        }
        drop(admissions);

        shutting_down.store(true, Ordering::Release);
        runtime_shutdown_tx.send_replace(true);
        for task in legs {
            if let Err(err) = task.await {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    error = %err,
                    "bbwick_polymarket_leg_task_join_failed"
                );
            }
        }
        drop(pairs);
        for task in reducers {
            if let Err(err) = task.await {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    error = %err,
                    "bbwick_polymarket_reducer_task_join_failed"
                );
            }
        }
    });
    (supervisor, shutdown_tx)
}

fn spawn_market_pair(
    symbol: &'static str,
    config: Arc<PolymarketWsRuntimeConfig>,
    state_lane: PolymarketStateLane,
    shutdown_rx: watch::Receiver<bool>,
    fault_tx: mpsc::UnboundedSender<String>,
    shutting_down: Arc<AtomicBool>,
) -> (MarketPair, Vec<JoinHandle<()>>, JoinHandle<()>) {
    let (assets_tx, assets_rx) = watch::channel(Vec::<U256>::new());
    let dedupe = Arc::new(EventDedupe::default());
    let live_legs = Arc::new(ServingLegs::new(symbol, state_lane.clone()));
    let (frame_tx, frame_rx) = mpsc::channel(config.ingress_channel_capacity.max(1));
    let reducer_live_legs = Arc::clone(&live_legs);
    let reducer_config = Arc::clone(&config);
    let reducer_state_lane = state_lane.clone();
    let reducer_dedupe = Arc::clone(&dedupe);
    let reducer_fault_tx = fault_tx.clone();
    let reducer_shutting_down = Arc::clone(&shutting_down);
    let reducer_failure_config = Arc::clone(&config);
    let reducer = tokio::spawn(async move {
        let reducer_task_live_legs = Arc::clone(&reducer_live_legs);
        let result = tokio::spawn(async move {
            run_symbol_reducer(
                frame_rx,
                &reducer_config,
                &reducer_state_lane,
                &reducer_dedupe,
                &reducer_task_live_legs,
            )
            .await
        })
        .await;
        reducer_live_legs.mark_reducer_stopped();
        let ordered_stop =
            reducer_shutting_down.load(Ordering::Acquire) && matches!(&result, Ok(Ok(())));
        if !ordered_stop {
            if let Some(capture) = &reducer_failure_config.ingress_capture {
                capture.mark_loss("symbol_market_reducer_stopped");
            }
            let detail = match result {
                Ok(Ok(())) => "symbol reducer stopped unexpectedly".to_string(),
                Ok(Err(err)) => format!("{err:#}"),
                Err(err) => format!("symbol reducer task failed: {err}"),
            };
            if reducer_shutting_down.load(Ordering::Acquire) {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    symbol,
                    error = %detail,
                    "bbwick_polymarket_reducer_shutdown_failed"
                );
            } else {
                let _ = reducer_fault_tx.send(format!("{symbol}: {detail}"));
            }
        }
    });
    let legs = (0..WS_MARKET_LEG_COUNT)
        .map(|leg| {
            let config = Arc::clone(&config);
            let assets_rx = assets_rx.clone();
            let dedupe = Arc::clone(&dedupe);
            let live_legs = Arc::clone(&live_legs);
            let frame_tx = frame_tx.clone();
            let shutdown_rx = shutdown_rx.clone();
            let fault_tx = fault_tx.clone();
            let shutting_down = Arc::clone(&shutting_down);
            let reader_config = Arc::clone(&config);
            let reader_live_legs = Arc::clone(&live_legs);
            let readers = Arc::new(LegReaders::default());
            let lifecycle_readers = Arc::clone(&readers);
            tokio::spawn(async move {
                let result = tokio::spawn(run_leg_forever(
                    symbol,
                    leg,
                    config,
                    assets_rx,
                    dedupe,
                    live_legs,
                    frame_tx,
                    shutdown_rx,
                    lifecycle_readers,
                ))
                .await;
                let cleanup = readers
                    .stop_and_join_remaining(leg, &reader_config, &reader_live_legs)
                    .await;
                if !shutting_down.load(Ordering::Acquire) {
                    let detail = match (result, cleanup) {
                        (_, Err(err)) => format!("reader cleanup failed: {err:#}"),
                        (Ok(Ok(())), Ok(())) => {
                            "market leg supervisor stopped unexpectedly".to_string()
                        }
                        (Ok(Err(err)), Ok(())) => format!("{err:#}"),
                        (Err(err), Ok(())) => {
                            format!("market leg supervisor task failed: {err}")
                        }
                    };
                    let _ = fault_tx.send(format!("{symbol}-{leg}: {detail}"));
                }
            })
        })
        .collect();
    drop(frame_tx);
    (
        MarketPair {
            symbol,
            assets_tx,
            live_legs,
        },
        legs,
        reducer,
    )
}

async fn run_symbol_reducer(
    mut frame_rx: mpsc::Receiver<MarketFrame>,
    config: &PolymarketWsRuntimeConfig,
    state_lane: &PolymarketStateLane,
    dedupe: &EventDedupe,
    live_legs: &ServingLegs,
) -> Result<()> {
    let mut last_err_sent_at = Instant::now() - Duration::from_secs(60);
    while let Some(frame) = frame_rx.recv().await {
        capture_market_ws_ingress(config, &frame);
        live_legs.apply_generation_health(
            frame.slot,
            frame
                .health
                .connected
                .map(|_| {
                    if frame.slot >= WS_MARKET_LEG_COUNT {
                        frame.slot - WS_MARKET_LEG_COUNT
                    } else {
                        frame.slot
                    }
                })
                .unwrap_or(frame.slot % WS_MARKET_LEG_COUNT),
            frame.generation,
            frame.health.as_ref(),
        );
        if let tokio_tungstenite::tungstenite::Message::Text(text) = &frame.message {
            if text != "PONG" {
                live_legs.process_market_text(
                    frame.slot,
                    state_lane,
                    dedupe,
                    text.as_bytes(),
                    config,
                    &mut last_err_sent_at,
                    Instant::now(),
                    frame.health.subscribed.as_ref(),
                    frame.recv_at,
                    frame.connection_session_id,
                    frame.sequence,
                )?;
            }
        }
        live_legs.acknowledge_frame(frame.slot, frame.generation, frame.sequence);
    }
    Ok(())
}

fn log_first_book(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    evidence: Option<GenerationEvidence>,
    asset: U256,
    epoch: u64,
) {
    let Some(evidence) = evidence else {
        return;
    };
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        leg = evidence.leg,
        generation = evidence.generation,
        connection_session_id = ?evidence.connection_session_id,
        asset_id = %asset,
        book_epoch = epoch,
        observed_at_mono_ns = mono_ns(clock_origin, now),
        launch_to_book_ms = elapsed_ms(evidence.launch_at, now),
        connect_to_book_ms = ?evidence.connected_at.map(|connected_at| elapsed_ms(connected_at, now)),
        clock_precision_ns = CLOCK_PRECISION_NS,
        "bbwick_polymarket_ws_first_admitted_book"
    );
}

fn log_readiness(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    rotation_leg: usize,
    readiness_role: &'static str,
    evidence: Option<GenerationEvidence>,
    ready: bool,
) {
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        leg = rotation_leg,
        readiness_role,
        subject_leg = ?evidence.map(|evidence| evidence.leg),
        subject_generation = ?evidence.map(|evidence| evidence.generation),
        connection_session_id = ?evidence.and_then(|evidence| evidence.connection_session_id),
        ready,
        observed_at_mono_ns = mono_ns(clock_origin, now),
        clock_precision_ns = CLOCK_PRECISION_NS,
        "bbwick_polymarket_ws_handover_readiness_changed"
    );
}

fn log_handover(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    leg: usize,
    incumbent: Option<GenerationEvidence>,
    replacement: Option<GenerationEvidence>,
    peer: Option<GenerationEvidence>,
) {
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        leg,
        incumbent_generation = ?incumbent.map(|evidence| evidence.generation),
        incumbent_connection_session_id = ?incumbent.and_then(|evidence| evidence.connection_session_id),
        replacement_generation = ?replacement.map(|evidence| evidence.generation),
        replacement_connection_session_id = ?replacement.and_then(|evidence| evidence.connection_session_id),
        peer_leg = ?peer.map(|evidence| evidence.leg),
        peer_generation = ?peer.map(|evidence| evidence.generation),
        peer_connection_session_id = ?peer.and_then(|evidence| evidence.connection_session_id),
        observed_at_mono_ns = mono_ns(clock_origin, now),
        replacement_launch_to_handover_ms = ?replacement.map(|evidence| elapsed_ms(evidence.launch_at, now)),
        replacement_connect_to_handover_ms = ?replacement.and_then(|evidence| evidence.connected_at).map(|connected_at| elapsed_ms(connected_at, now)),
        clock_precision_ns = CLOCK_PRECISION_NS,
        "bbwick_polymarket_ws_handover"
    );
}

fn log_generation_censored(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    evidence: GenerationEvidence,
    reason: &'static str,
) {
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        leg = evidence.leg,
        generation = evidence.generation,
        connection_session_id = ?evidence.connection_session_id,
        launch_to_end_ms = elapsed_ms(evidence.launch_at, now),
        connected_to_end_ms = ?evidence.connected_at.map(|connected_at| elapsed_ms(connected_at, now)),
        end_mono_ns = mono_ns(clock_origin, now),
        clock_precision_ns = CLOCK_PRECISION_NS,
        right_censored = true,
        reason,
        "bbwick_polymarket_ws_generation_ended"
    );
}

fn mono_ns(origin: Instant, now: Instant) -> u64 {
    u64::try_from(now.saturating_duration_since(origin).as_nanos()).unwrap_or(u64::MAX)
}

fn elapsed_ms(start: Instant, end: Instant) -> u64 {
    u64::try_from(end.saturating_duration_since(start).as_millis()).unwrap_or(u64::MAX)
}

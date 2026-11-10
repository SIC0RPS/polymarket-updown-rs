use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};
use tokio::sync::{broadcast, mpsc, watch};

use crate::config::TransportSettings;
use crate::market_data::CanonicalTopOfBook;
use crate::metrics::{MetricsSnapshot, MetricsState};
use crate::oracle_history::OracleHistory;
use crate::oracle_runtime::start_engine_runtime;
use crate::oracle_state_updates;
use crate::polymarket_ws::{PolymarketBookAdmissionCaptureLane, PolymarketWsIngressCaptureLane};
use crate::provider_ws::ProviderCapture;
use crate::state::{
    DiscoverySnapshot, LastCanonicalBid, PolymarketOddsState, PolymarketStateStore,
};
use crate::wire::StepFrame;

#[derive(Debug, Clone)]
pub struct OracleTick {
    pub symbol: String,
    pub ts_ms: u64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub struct OracleEngineConfig {
    pub transport: TransportSettings,
    pub ingest_channel_capacity: usize,
    pub tick_channel_capacity: usize,
    pub active_symbols: Vec<String>,
    pub history: OracleHistoryConfig,
    pub polymarket_ws_ingress_capture: Option<PolymarketWsIngressCaptureLane>,
    pub polymarket_book_admission_capture: Option<PolymarketBookAdmissionCaptureLane>,
    pub binance_provider_capture: Option<Arc<dyn ProviderCapture>>,
}

#[derive(Debug, Clone)]
pub struct OracleHistoryConfig {
    pub enabled: bool,
    pub data_root: PathBuf,
    pub max_bars: usize,
}

impl Default for OracleHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_root: PathBuf::from("crates/bbwick_engine/mData"),
            max_bars: 1_000,
        }
    }
}

impl Default for OracleEngineConfig {
    fn default() -> Self {
        Self {
            transport: TransportSettings::default(),
            ingest_channel_capacity: 2048,
            tick_channel_capacity: 2048,
            active_symbols: Vec::new(),
            history: OracleHistoryConfig::default(),
            polymarket_ws_ingress_capture: None,
            polymarket_book_admission_capture: None,
            binance_provider_capture: None,
        }
    }
}

#[derive(Clone)]
pub struct PolymarketDiscoveryController {
    control_tx: mpsc::Sender<DiscoverySnapshot>,
    state_store: Arc<Mutex<PolymarketStateStore>>,
    state_tx: watch::Sender<PolymarketOddsState>,
}

impl PolymarketDiscoveryController {
    pub(crate) fn new(
        control_tx: mpsc::Sender<DiscoverySnapshot>,
        state_store: Arc<Mutex<PolymarketStateStore>>,
        state_tx: watch::Sender<PolymarketOddsState>,
    ) -> Self {
        Self {
            control_tx,
            state_store,
            state_tx,
        }
    }

    pub async fn update(&self, snapshot: DiscoverySnapshot) -> Result<()> {
        let publish_snapshot = snapshot.clone();
        oracle_state_updates::sync_discovery_snapshot(&self.state_store, &self.state_tx, snapshot)?;
        self.control_tx
            .send(publish_snapshot)
            .await
            .context("failed to send discovery snapshot to Polymarket WS runtime")?;
        Ok(())
    }

    pub fn preflight_market_resolution(
        &self,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
    ) -> Result<bool> {
        oracle_state_updates::preflight_market_resolution(
            &self.state_store,
            condition_id,
            up_token,
            down_token,
            winning_asset_id,
        )
    }

    pub fn commit_market_resolution(
        &self,
        condition_id: B256,
        up_token: U256,
        down_token: U256,
        winning_asset_id: U256,
        observed_at_ms: u64,
        raw_response_sha256: [u8; 32],
    ) -> Result<bool> {
        oracle_state_updates::commit_market_resolution(
            &self.state_store,
            &self.state_tx,
            condition_id,
            up_token,
            down_token,
            winning_asset_id,
            observed_at_ms,
            raw_response_sha256,
        )
    }

    pub fn tombstone_no_orderbook(&self, token_id: U256) -> Result<bool> {
        oracle_state_updates::sync_tombstone_no_orderbook(
            &self.state_store,
            &self.state_tx,
            token_id,
        )
    }
}

pub struct OracleEngine {
    history: Arc<OracleHistory>,
    metrics: Arc<MetricsState>,
    ingest_mono_origin: Instant,
    tick_tx: broadcast::Sender<OracleTick>,
    top_of_book_rx: Option<broadcast::Receiver<CanonicalTopOfBook>>,
    step_tx: watch::Sender<StepFrame>,
    polymarket_state_tx: watch::Sender<PolymarketOddsState>,
    polymarket_controller: PolymarketDiscoveryController,
    provider_task: Option<tokio::task::JoinHandle<()>>,
    polymarket_task: Option<tokio::task::JoinHandle<()>>,
    polymarket_shutdown_tx: watch::Sender<bool>,
    ingest_task: Option<tokio::task::JoinHandle<()>>,
    history_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleRuntimeTask {
    Provider,
    Polymarket,
    Ingest,
    History,
}

impl std::fmt::Display for OracleRuntimeTask {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Provider => "provider",
            Self::Polymarket => "polymarket_two_leg",
            Self::Ingest => "ingest",
            Self::History => "history",
        })
    }
}

pub(crate) struct OracleEngineParts {
    pub history: Arc<OracleHistory>,
    pub metrics: Arc<MetricsState>,
    pub ingest_mono_origin: Instant,
    pub tick_tx: broadcast::Sender<OracleTick>,
    pub top_of_book_rx: broadcast::Receiver<CanonicalTopOfBook>,
    pub step_tx: watch::Sender<StepFrame>,
    pub polymarket_state_tx: watch::Sender<PolymarketOddsState>,
    pub polymarket_controller: PolymarketDiscoveryController,
    pub provider_task: tokio::task::JoinHandle<()>,
    pub polymarket_task: tokio::task::JoinHandle<()>,
    pub polymarket_shutdown_tx: watch::Sender<bool>,
    pub ingest_task: tokio::task::JoinHandle<()>,
    pub history_task: Option<tokio::task::JoinHandle<()>>,
}

impl OracleEngine {
    pub fn start(
        config: OracleEngineConfig,
        retained_bids: HashMap<U256, LastCanonicalBid>,
        history: Arc<OracleHistory>,
    ) -> Result<Self> {
        Ok(Self::from_parts(start_engine_runtime(
            config,
            retained_bids,
            history,
        )?))
    }

    fn from_parts(parts: OracleEngineParts) -> Self {
        Self {
            history: parts.history,
            metrics: parts.metrics,
            ingest_mono_origin: parts.ingest_mono_origin,
            tick_tx: parts.tick_tx,
            top_of_book_rx: Some(parts.top_of_book_rx),
            step_tx: parts.step_tx,
            polymarket_state_tx: parts.polymarket_state_tx,
            polymarket_controller: parts.polymarket_controller,
            provider_task: Some(parts.provider_task),
            polymarket_task: Some(parts.polymarket_task),
            polymarket_shutdown_tx: parts.polymarket_shutdown_tx,
            ingest_task: Some(parts.ingest_task),
            history_task: parts.history_task,
        }
    }

    pub fn subscribe_ticks(&self) -> broadcast::Receiver<OracleTick> {
        self.tick_tx.subscribe()
    }

    pub fn take_top_of_book_receiver(&mut self) -> Result<broadcast::Receiver<CanonicalTopOfBook>> {
        self.top_of_book_rx
            .take()
            .context("bbwick top-of-book receiver already taken")
    }

    pub fn current_ingest_mono_ns(&self) -> u64 {
        self.ingest_mono_origin.elapsed().as_nanos() as u64
    }

    pub fn subscribe_steps(&self) -> watch::Receiver<StepFrame> {
        self.step_tx.subscribe()
    }

    pub fn subscribe_polymarket_state(&self) -> watch::Receiver<PolymarketOddsState> {
        self.polymarket_state_tx.subscribe()
    }

    pub fn discovery_controller(&self) -> PolymarketDiscoveryController {
        self.polymarket_controller.clone()
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn take_finished_runtime_task(&mut self) -> Option<OracleRuntimeTask> {
        if self
            .provider_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Some(OracleRuntimeTask::Provider);
        }
        if self
            .polymarket_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Some(OracleRuntimeTask::Polymarket);
        }
        if self
            .ingest_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.history.mark_persistence_failed();
            return Some(OracleRuntimeTask::Ingest);
        }
        if self
            .history_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.history.mark_persistence_failed();
            return Some(OracleRuntimeTask::History);
        }
        None
    }

    pub async fn shutdown(mut self) -> HashMap<U256, LastCanonicalBid> {
        for (kind, task) in [
            (OracleRuntimeTask::Ingest, &self.ingest_task),
            (OracleRuntimeTask::History, &self.history_task),
        ] {
            if task.as_ref().is_some_and(tokio::task::JoinHandle::is_finished) {
                self.history.mark_persistence_failed();
                tracing::error!(
                    target: "engine.oracle",
                    task = %kind,
                    "bbwick_history_writer_exited_before_shutdown"
                );
            }
        }
        if let Some(task) = self.provider_task.take() {
            task.abort();
            if let Err(err) = task.await {
                if !err.is_cancelled() {
                    self.history.mark_persistence_failed();
                    tracing::error!(
                        target: "engine.oracle",
                        error = %err,
                        task = %OracleRuntimeTask::Provider,
                        "bbwick_oracle_shutdown_join_failed"
                    );
                }
            }
        }
        self.polymarket_shutdown_tx.send_replace(true);
        for (kind, task) in [
            (OracleRuntimeTask::Polymarket, self.polymarket_task.take()),
            (OracleRuntimeTask::Ingest, self.ingest_task.take()),
            (OracleRuntimeTask::History, self.history_task.take()),
        ] {
            if let Some(task) = task {
                if let Err(err) = task.await {
                    self.history.mark_persistence_failed();
                    tracing::error!(
                        target: "engine.oracle",
                        error = %err,
                        task = %kind,
                        "bbwick_oracle_shutdown_join_failed"
                    );
                }
            }
        }
        let history = self.history.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || {
            let registry = history.key_registry();
            for symbol in registry.symbols() {
                for timeframe in registry.timeframes() {
                    if let Err(err) = history.persist(symbol, timeframe) {
                        tracing::error!(
                            target: "engine.oracle",
                            error = %err,
                            symbol,
                            timeframe,
                            "bbwick_history_shutdown_persistence_failed"
                        );
                    }
                }
            }
        }).await {
            self.history.mark_persistence_failed();
            tracing::error!(
                target: "engine.oracle",
                error = %err,
                "bbwick_history_shutdown_flush_task_failed"
            );
        }
        self.polymarket_state_tx
            .borrow()
            .last_canonical_bids
            .clone()
    }
}

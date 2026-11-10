use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bbwick_engine::state::is_valid_polymarket_tick_size as is_valid_tick_size;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt as _, StreamExt as _};
use polymarket_client_sdk::types::U256;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::client::{ExecutionBatchSendOutcome, ExecutionPostTiming};
use crate::capture::events::{OrderSubmitLatencyCapture, OrderSubmitRejectCapture};
use crate::capture::session::MaybeCapture;
use crate::config::{
    AppConfig, MakerRebateMmStrategyConfig, PolymarketOrderType, SharedStrategyConfig,
};
use crate::ctf_inventory::CtfInventoryRuntime;
use crate::execution::{CancelAck, ExecutionClient};
use crate::observability::action::ActionEvent;
use crate::observability::Observability;
use crate::orders::{OpenOrderState, OpenOrdersState, OrderTerminalProvenance, OrderUpdateEvent};
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::identity::MarketId;
use crate::polymarket::order_context::{OrderFillContext, OrderFillContextRegistry};
use crate::polymarket::order_rules::{
    normalized_min_order_size, truncate_to_decimals, truncate_to_vendor_qty,
    VENDOR_LOT_SIZE_DECIMALS,
};
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::portfolio::PortfolioState;
use crate::risk::{RiskBlock, RiskHandle};
use crate::runtime_control::RuntimeConfigState;
use crate::strategy::{CancelMarketRequest, CancelQuotesRequest, CancelRequest};
use crate::types::{OrderIntent, Side};

#[path = "cancel_router.rs"]
mod cancel_router;
#[path = "entry_gates.rs"]
mod entry_gates;
#[path = "order_normalization.rs"]
mod order_normalization;
#[path = "post_only_guards.rs"]
mod post_only_guards;
#[path = "supervisor_batch.rs"]
mod supervisor_batch;
#[path = "supervisor_loop.rs"]
mod supervisor_loop;
#[path = "timeout_repricing.rs"]
mod timeout_repricing;

pub(crate) use self::supervisor_batch::latency::{
    emit_receipt_admission_timing, OrderExecutionTiming,
};
pub(crate) use self::timeout_repricing::{
    working_close_quote_replacement_for_open_orders, WorkingCloseQuoteReplacementDecision,
};
use self::{
    cancel_router::*, entry_gates::*, order_normalization::*, post_only_guards::*,
    supervisor_batch::*, supervisor_loop::*, timeout_repricing::*,
};
use crate::execution::order_lifecycle::*;

pub struct ExecutionSupervisorRuntime {
    upstream_stop: CancellationToken,
    upstream_done: Option<oneshot::Receiver<std::result::Result<(), String>>>,
    intents_tx: mpsc::UnboundedSender<ExecutionIntentBatch>,
    cancel_requests_tx: mpsc::UnboundedSender<Vec<CancelRequest>>,
    open_orders_rx: watch::Receiver<OpenOrdersState>,
    task: JoinHandle<Result<()>>,
}

#[derive(Debug)]
pub struct ExecutionIntentBatch {
    pub decision_ts_ms: u64,
    pub decision_instant: Instant,
    pub handoff_instant: Instant,
    pub intents: Vec<OrderIntent>,
}

pub struct ExecutionSupervisorStart {
    pub cfg: AppConfig,
    pub exec: ExecutionClient,
    pub risk: RiskHandle,
    pub ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
    pub shared_strategy_cfg: SharedStrategyConfig,
    pub order_fill_ctx: OrderFillContextRegistry,
    pub pause_rx: watch::Receiver<bool>,
    pub runtime_control_rx: watch::Receiver<RuntimeConfigState>,
    pub blocked_markets_tx: watch::Sender<HashSet<String>>,
    pub portfolio_rx: watch::Receiver<PortfolioState>,
    pub odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
    pub oracle_runtime_rx: Option<watch::Receiver<OracleRuntimeStatus>>,
    pub market_projection_rx: Option<watch::Receiver<MarketProjection>>,
    pub order_updates_rx: mpsc::Receiver<ExecutionOrderEvent>,
    pub obs: Observability,
    pub capture: MaybeCapture,
    pub shutdown: CancellationToken,
    pub clock: ExecutionSupervisorClock,
}

#[derive(Debug)]
pub enum ExecutionOrderEvent {
    Update {
        update: OrderUpdateEvent,
        context: Option<OrderFillContext>,
    },
    UnboundTrade {
        observation: UnboundCtfTradeObservation,
        ack: oneshot::Sender<std::result::Result<OrderFillContext, String>>,
    },
}

pub struct CapturedExecutionSupervisorStart {
    pub cfg: AppConfig,
    pub exec: ExecutionClient,
    pub risk: RiskHandle,
    pub ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
    pub shared_strategy_cfg: SharedStrategyConfig,
    pub order_fill_ctx: OrderFillContextRegistry,
    pub blocked_markets_tx: watch::Sender<HashSet<String>>,
    pub portfolio_rx: watch::Receiver<PortfolioState>,
    pub odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
    pub oracle_runtime_rx: Option<watch::Receiver<OracleRuntimeStatus>>,
    pub market_projection_rx: Option<watch::Receiver<MarketProjection>>,
    pub obs: Observability,
    pub capture: MaybeCapture,
    pub shutdown: CancellationToken,
    pub initial_pause: bool,
    pub initial_timestamp_ms: u64,
}

pub struct CapturedExecutionSupervisorRuntime {
    control: CapturedExecutionSupervisorControl,
    open_orders_rx: watch::Receiver<OpenOrdersState>,
    shutdown: CancellationToken,
    task: JoinHandle<Result<()>>,
}

#[derive(Debug)]
pub enum ExecutionSupervisorClock {
    Live,
    Captured(CapturedExecutionSupervisorClock),
}

#[derive(Debug)]
pub struct CapturedExecutionSupervisorClock {
    now_ms: u64,
    batch_rx: Option<mpsc::UnboundedReceiver<CapturedExecutionSupervisorRequest>>,
}

#[derive(Debug, Clone)]
pub struct CapturedExecutionSupervisorControl {
    batch_tx: mpsc::UnboundedSender<CapturedExecutionSupervisorRequest>,
}

#[derive(Debug)]
pub struct CapturedExecutionSupervisorBatch {
    pub input_sequence: u64,
    pub timestamp_ms: u64,
    pub pause: Option<bool>,
    pub order_updates: Vec<OrderUpdateEvent>,
    pub matched_fills: Vec<crate::portfolio::FillEvent>,
    pub cancel_requests: Vec<CancelRequest>,
    pub intents: Vec<OrderIntent>,
}

#[derive(Debug, Clone)]
pub struct ExecutionSupervisorBatchAck {
    pub input_sequence: u64,
    pub timestamp_ms: u64,
    pub completed_work_watermark: u64,
    pub open_orders_revision: u64,
    pub execution_audit_records: Vec<crate::engine_trade_journal::EngineTradeJournalRecord>,
}

#[derive(Debug)]
struct CapturedExecutionSupervisorRequest {
    batch: CapturedExecutionSupervisorBatch,
    ack_tx: oneshot::Sender<std::result::Result<ExecutionSupervisorBatchAck, String>>,
}

const CAPTURED_SUPERVISOR_CLASS_LIMIT: usize = 256;
const CAPTURED_SUPERVISOR_INTENT_LIMIT: usize = 2_048;

impl ExecutionSupervisorClock {
    fn captured(initial_timestamp_ms: u64) -> (Self, CapturedExecutionSupervisorControl) {
        let (batch_tx, batch_rx) = mpsc::unbounded_channel();
        (
            Self::Captured(CapturedExecutionSupervisorClock {
                now_ms: initial_timestamp_ms,
                batch_rx: Some(batch_rx),
            }),
            CapturedExecutionSupervisorControl { batch_tx },
        )
    }

    fn now_ms(&self) -> u64 {
        match self {
            Self::Live => live_now_ms(),
            Self::Captured(captured) => captured.now_ms,
        }
    }

    fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }

    fn take_batch_rx(
        &mut self,
    ) -> Option<mpsc::UnboundedReceiver<CapturedExecutionSupervisorRequest>> {
        match self {
            Self::Live => None,
            Self::Captured(captured) => captured.batch_rx.take(),
        }
    }

    fn advance_to(&mut self, timestamp_ms: u64) -> std::result::Result<(), String> {
        match self {
            Self::Live => Err("live execution supervisor clock cannot be advanced".to_string()),
            Self::Captured(captured) if timestamp_ms < captured.now_ms => Err(format!(
                "captured execution supervisor clock moved backward current_ms={} requested_ms={timestamp_ms}",
                captured.now_ms
            )),
            Self::Captured(captured) => {
                captured.now_ms = timestamp_ms;
                Ok(())
            }
        }
    }
}

impl CapturedExecutionSupervisorControl {
    pub async fn process_batch(
        &self,
        risk: &RiskHandle,
        ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
        batch: CapturedExecutionSupervisorBatch,
    ) -> Result<ExecutionSupervisorBatchAck> {
        if batch.order_updates.len() > CAPTURED_SUPERVISOR_CLASS_LIMIT
            || batch.matched_fills.len() > CAPTURED_SUPERVISOR_CLASS_LIMIT
            || batch.cancel_requests.len() > CAPTURED_SUPERVISOR_CLASS_LIMIT
            || batch.intents.len() > CAPTURED_SUPERVISOR_INTENT_LIMIT
        {
            anyhow::bail!(
                "captured execution supervisor batch exceeds bounds sequence={} updates={} fills={} cancels={} intents={}",
                batch.input_sequence,
                batch.order_updates.len(),
                batch.matched_fills.len(),
                batch.cancel_requests.len(),
                batch.intents.len()
            );
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        if let Err(error) = self
            .batch_tx
            .send(CapturedExecutionSupervisorRequest { batch, ack_tx })
        {
            for intent in error.0.batch.intents {
                if risk.abandon_unsubmitted_direct_buy(&intent)? {
                    let authorization = intent.buy_authorization().ok_or_else(|| {
                        anyhow::anyhow!("abandoned BUY has no central authorization")
                    })?;
                    ctf_inventory_runtime
                        .lock()
                        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                        .release_reserved_direct_buy(authorization);
                }
            }
            anyhow::bail!("execution supervisor captured clock is closed");
        }
        ack_rx
            .await
            .map_err(|_| anyhow::anyhow!("execution supervisor captured clock ack is closed"))?
            .map_err(anyhow::Error::msg)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OracleRuntimeStatus {
    pub phase: String,
    pub connected: bool,
    pub last_error: Option<String>,
}

impl ExecutionSupervisorRuntime {
    pub fn start(start: ExecutionSupervisorStart) -> Self {
        let ExecutionSupervisorStart {
            cfg,
            exec,
            risk,
            ctf_inventory_runtime,
            shared_strategy_cfg,
            order_fill_ctx,
            mut pause_rx,
            runtime_control_rx,
            blocked_markets_tx,
            mut portfolio_rx,
            mut odds_rx,
            mut oracle_runtime_rx,
            mut market_projection_rx,
            mut order_updates_rx,
            obs,
            capture,
            shutdown,
            clock,
        } = start;
        assert!(
            clock.is_live(),
            "captured execution supervisor must use CapturedExecutionSupervisorRuntime::start"
        );
        let (intents_tx, intents_rx) = mpsc::unbounded_channel::<ExecutionIntentBatch>();
        let (cancel_requests_tx, cancel_requests_rx) =
            mpsc::unbounded_channel::<Vec<CancelRequest>>();
        let (open_orders_tx, open_orders_rx) =
            watch::channel::<OpenOrdersState>(OpenOrdersState::default());

        let upstream_stop = CancellationToken::new();
        let task_upstream_stop = upstream_stop.clone();
        let (upstream_done_tx, upstream_done) = oneshot::channel();
        let task = tokio::spawn(async move {
            run_supervisor(
                cfg,
                exec,
                risk,
                &ctf_inventory_runtime,
                shared_strategy_cfg,
                order_fill_ctx,
                &mut pause_rx,
                Some(&runtime_control_rx),
                blocked_markets_tx,
                &mut portfolio_rx,
                &mut odds_rx,
                &mut oracle_runtime_rx,
                &mut market_projection_rx,
                &mut order_updates_rx,
                intents_rx,
                cancel_requests_rx,
                open_orders_tx,
                obs,
                capture,
                shutdown,
                clock,
                Some((task_upstream_stop, upstream_done_tx)),
            )
            .await
        });

        Self {
            upstream_stop,
            upstream_done: Some(upstream_done),
            intents_tx,
            cancel_requests_tx,
            open_orders_rx,
            task,
        }
    }

    pub fn intents(&self) -> mpsc::UnboundedSender<ExecutionIntentBatch> {
        self.intents_tx.clone()
    }

    pub fn open_orders(&self) -> watch::Receiver<OpenOrdersState> {
        self.open_orders_rx.clone()
    }

    pub fn cancel_requests(&self) -> mpsc::UnboundedSender<Vec<CancelRequest>> {
        self.cancel_requests_tx.clone()
    }

    pub async fn finish_upstream(&mut self) -> Result<()> {
        self.upstream_stop.cancel();
        if let Some(done) = self.upstream_done.take() {
            done.await
                .map_err(|_| {
                    anyhow::anyhow!("supervisor upstream completion acknowledgment closed")
                })?
                .map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        self.task.await??;
        Ok(())
    }
}

impl CapturedExecutionSupervisorRuntime {
    pub fn start(start: CapturedExecutionSupervisorStart) -> Self {
        let CapturedExecutionSupervisorStart {
            cfg,
            exec,
            risk,
            ctf_inventory_runtime,
            shared_strategy_cfg,
            order_fill_ctx,
            blocked_markets_tx,
            mut portfolio_rx,
            mut odds_rx,
            mut oracle_runtime_rx,
            mut market_projection_rx,
            obs,
            capture,
            shutdown,
            initial_pause,
            initial_timestamp_ms,
        } = start;
        let (pause_tx, mut pause_rx) = watch::channel(initial_pause);
        drop(pause_tx);
        let (order_updates_tx, mut order_updates_rx) = mpsc::channel::<ExecutionOrderEvent>(1);
        drop(order_updates_tx);
        let (intents_tx, intents_rx) = mpsc::unbounded_channel::<ExecutionIntentBatch>();
        drop(intents_tx);
        let (cancel_requests_tx, cancel_requests_rx) =
            mpsc::unbounded_channel::<Vec<CancelRequest>>();
        drop(cancel_requests_tx);
        let (open_orders_tx, open_orders_rx) =
            watch::channel::<OpenOrdersState>(OpenOrdersState::default());
        let (clock, control) = ExecutionSupervisorClock::captured(initial_timestamp_ms);
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            run_supervisor(
                cfg,
                exec,
                risk,
                &ctf_inventory_runtime,
                shared_strategy_cfg,
                order_fill_ctx,
                &mut pause_rx,
                None,
                blocked_markets_tx,
                &mut portfolio_rx,
                &mut odds_rx,
                &mut oracle_runtime_rx,
                &mut market_projection_rx,
                &mut order_updates_rx,
                intents_rx,
                cancel_requests_rx,
                open_orders_tx,
                obs,
                capture,
                task_shutdown,
                clock,
                None,
            )
            .await
        });
        Self {
            control,
            open_orders_rx,
            shutdown,
            task,
        }
    }

    pub fn control(&self) -> CapturedExecutionSupervisorControl {
        self.control.clone()
    }

    pub fn open_orders(&self) -> watch::Receiver<OpenOrdersState> {
        self.open_orders_rx.clone()
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown.cancel();
        (&mut self.task).await??;
        Ok(())
    }
}

impl Drop for CapturedExecutionSupervisorRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct QuoteKey {
    token: String,
    side: Side,
    quote_level: u8,
    quote_group: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct QuoteCancelKey {
    market: Option<String>,
    quote_group: String,
    side: Option<Side>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ScopedOrderFenceKey {
    market: String,
    token: String,
}

#[derive(Debug, Clone)]
struct ScopedCancelHealRequest {
    scope: ScopedOrderFenceKey,
    order_id: String,
    label: &'static str,
}

#[derive(Debug, Clone)]
struct ScopedCancelHealResult {
    scope: ScopedOrderFenceKey,
    order_id: String,
    label: &'static str,
    final_open_ids: std::result::Result<Vec<String>, String>,
}

#[derive(Debug)]
struct ScopedHealQueueClosed;

impl std::fmt::Display for ScopedHealQueueClosed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("cancel scope heal worker closed")
    }
}

impl std::error::Error for ScopedHealQueueClosed {}

const CLOSE_ORDER_QTY_EPS: f64 = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
const VENDOR_MAX_PRICE_DECIMALS: u32 = 8;
const INVENTORY_MARKET_CLEAN_VERIFY_ATTEMPTS: usize = 8;
const INVENTORY_MARKET_CLEAN_VERIFY_DELAY_MS: u64 = 250;
const POST_ONLY_FINAL_SEND_EXTRA_BUFFER_TICKS: u8 = 1;
const POST_ONLY_VENUE_RACE_SAFETY_TICKS: u8 = 2;
const QUOTE_REPLACE_PRICE_TOLERANCE_TICKS: f64 = 3.0;
const QUOTE_REPLACE_QTY_REL_TOLERANCE: f64 = 0.10;
const QUOTE_REPLACE_QTY_ABS_TOLERANCE_MIN: f64 = 0.50;
const QUOTE_REPLACE_QTY_ABS_TOLERANCE_MAX: f64 = 3.00;

#[derive(Debug, Clone, Default)]
struct PostOnlyRejectState {
    streak: u8,
    cooldown_until_ms: u64,
    window_end_ms: u64,
}

fn live_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

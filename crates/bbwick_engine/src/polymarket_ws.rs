use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

mod assets;
mod connection;
mod endpoint;
mod events;
/// Production parse and apply entry for one raw market WebSocket payload.
///
/// Public so replay drives the identical path live runs, exactly as the nautilus donor
/// exposes its parsers for non-live callers. Nothing is duplicated: this calls the vendor
/// SDK parser and the same state transitions the live loop uses.
pub use events::forward_raw_market_text;
mod dedupe;
mod frames;
mod generation;
mod runtime;

pub(crate) use runtime::spawn_polymarket_ws_runtime;

pub(super) const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
pub(super) const WS_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
pub(super) const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
/// Bounds one REST book admission so a slow venue cannot park the market WS drain.
pub(super) const BOOK_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);
/// Redundant market legs. Two independent connections carrying the same assets, so a
/// disconnect or a silent freeze on one is absorbed by the other instead of becoming an
/// outage. Kraken documents redundant connections as the optimal reliability answer and
/// the venue has an unfixed silent-freeze defect, so this is not optional.
pub(super) const WS_MARKET_LEG_COUNT: usize = 2;
/// Backoff ladder between leg rebuilds.
pub(super) const WS_REBUILD_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
pub(super) const WS_REBUILD_MAX_BACKOFF: Duration = Duration::from_secs(5);
pub(super) const WS_REBUILD_BACKOFF_JITTER_MS: u64 = 200;
static NEXT_WS_CONNECTION_SESSION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolymarketWsIngressRole {
    Primary,
}

impl PolymarketWsIngressRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "Primary",
        }
    }
}

impl std::str::FromStr for PolymarketWsIngressRole {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "Primary" => Ok(Self::Primary),
            other => anyhow::bail!("unknown Polymarket ws ingress role {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolymarketWsIngressMessageKind {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

impl PolymarketWsIngressMessageKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "Text",
            Self::Binary => "Binary",
            Self::Ping => "Ping",
            Self::Pong => "Pong",
            Self::Close => "Close",
        }
    }
}

impl std::str::FromStr for PolymarketWsIngressMessageKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "Text" => Ok(Self::Text),
            "Binary" => Ok(Self::Binary),
            "Ping" => Ok(Self::Ping),
            "Pong" => Ok(Self::Pong),
            "Close" => Ok(Self::Close),
            other => anyhow::bail!("unknown Polymarket ws ingress message kind {other}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PolymarketWsIngressRecord {
    pub recv_ts_ms: u64,
    pub recv_mono_ns: u64,
    pub symbol: String,
    pub role: PolymarketWsIngressRole,
    pub generation: u64,
    pub connection_session_id: u64,
    pub sequence: u64,
    pub message_kind: PolymarketWsIngressMessageKind,
    pub payload: Option<Vec<u8>>,
    pub close_code: Option<u16>,
    pub close_reason: Option<String>,
}

/// One typed REST book admission outcome, exactly as the venue answered it.
///
/// The market WebSocket never carries the "this token has no orderbook" fact, so replay
/// cannot reproduce `sync_tombstone_no_orderbook` from raw frames alone. Recording the
/// vendor response verbatim lets replay drive the same three state transitions live drove.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PolymarketBookAdmissionOutcome {
    Seeded(Box<polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse>),
    NoOrderbook,
    Failed { error: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PolymarketBookAdmissionRecord {
    pub observed_at_ms: u64,
    pub asset_id: String,
    pub outcome: PolymarketBookAdmissionOutcome,
}

#[derive(Debug, Clone)]
pub struct PolymarketBookAdmissionCaptureLane {
    tx: mpsc::Sender<PolymarketBookAdmissionRecord>,
    state: Arc<Mutex<PolymarketBookAdmissionCaptureState>>,
}

#[derive(Debug)]
struct PolymarketBookAdmissionCaptureState {
    active: bool,
    attempted: u64,
    accepted: u64,
    full: u64,
    closed: u64,
}

impl PolymarketBookAdmissionCaptureLane {
    pub fn bounded(capacity: usize) -> (Self, mpsc::Receiver<PolymarketBookAdmissionRecord>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                state: Arc::new(Mutex::new(PolymarketBookAdmissionCaptureState {
                    active: true,
                    attempted: 0,
                    accepted: 0,
                    full: 0,
                    closed: 0,
                })),
            },
            rx,
        )
    }

    pub(crate) fn try_capture(&self, record: PolymarketBookAdmissionRecord) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !state.active {
            return;
        }
        state.attempted = state.attempted.saturating_add(1);
        match self.tx.try_send(record) {
            Ok(()) => state.accepted = state.accepted.saturating_add(1),
            Err(err) => {
                match &err {
                    mpsc::error::TrySendError::Full(_) => {
                        state.full = state.full.saturating_add(1);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        state.closed = state.closed.saturating_add(1);
                    }
                }
                tracing::error!(
                    target: "capture.polymarket_book_admission",
                    error = %err,
                    "polymarket_book_admission_capture_lost"
                );
            }
        }
    }

    pub fn deactivate(&self) {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .active = false;
    }

    pub fn attempted_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .attempted
    }

    pub fn accepted_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .accepted
    }

    pub fn full_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .full
    }

    pub fn closed_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .closed
    }
}

#[derive(Debug, Clone)]
pub struct PolymarketWsIngressCaptureLane {
    tx: mpsc::Sender<PolymarketWsIngressRecord>,
    state: Arc<Mutex<PolymarketWsIngressCaptureState>>,
    mono_origin: std::time::Instant,
}

#[derive(Debug)]
struct PolymarketWsIngressCaptureState {
    active: bool,
    attempted: u64,
    accepted: u64,
    full: u64,
    closed: u64,
    lost: u64,
}

impl PolymarketWsIngressCaptureLane {
    pub fn bounded(capacity: usize) -> (Self, mpsc::Receiver<PolymarketWsIngressRecord>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                state: Arc::new(Mutex::new(PolymarketWsIngressCaptureState {
                    active: true,
                    attempted: 0,
                    accepted: 0,
                    full: 0,
                    closed: 0,
                    lost: 0,
                })),
                mono_origin: std::time::Instant::now(),
            },
            rx,
        )
    }

    pub(crate) fn try_capture(&self, record: PolymarketWsIngressRecord) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !state.active {
            return;
        }
        state.attempted = state.attempted.saturating_add(1);
        match self.tx.try_send(record) {
            Ok(()) => state.accepted = state.accepted.saturating_add(1),
            Err(err) => {
                match &err {
                    mpsc::error::TrySendError::Full(_) => {
                        state.full = state.full.saturating_add(1);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        state.closed = state.closed.saturating_add(1);
                    }
                }
                state.lost = state.lost.saturating_add(1);
                if state.lost == 1 {
                    tracing::error!(
                        target: "capture.polymarket_market_ws_ingress",
                        error = %err,
                        "polymarket_market_ws_ingress_capture_lost"
                    );
                }
            }
        }
    }

    pub(crate) fn mark_loss(&self, reason: &'static str) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !state.active {
            return;
        }
        state.lost = state.lost.saturating_add(1);
        if state.lost == 1 {
            tracing::error!(
                target: "capture.polymarket_market_ws_ingress",
                reason,
                "polymarket_market_ws_ingress_capture_lost"
            );
        }
    }

    pub(crate) fn recv_mono_ns(&self, recv_at: std::time::Instant) -> Option<u64> {
        recv_at
            .checked_duration_since(self.mono_origin)
            .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
    }

    pub fn deactivate(&self) {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .active = false;
    }

    pub fn attempted_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .attempted
    }

    pub fn accepted_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .accepted
    }

    pub fn full_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .full
    }

    pub fn closed_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .closed
    }

    pub fn lost_count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .lost
    }
}

pub(super) fn next_ws_connection_session_id() -> u64 {
    NEXT_WS_CONNECTION_SESSION_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone)]
pub struct PolymarketWsRuntimeConfig {
    pub ws_url: String,
    pub clob_http_url: String,
    pub use_server_time: bool,
    pub subscribe_orderbook: bool,
    pub subscribe_prices: bool,
    pub subscribe_tick_size: bool,
    pub subscribe_last_trade: bool,
    pub ingress_channel_capacity: usize,
    pub ingress_capture: Option<PolymarketWsIngressCaptureLane>,
    pub book_admission_capture: Option<PolymarketBookAdmissionCaptureLane>,
}

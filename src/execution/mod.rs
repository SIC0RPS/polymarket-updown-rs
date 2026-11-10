use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;
use std::{
    env,
    sync::{Arc, Mutex},
};

use alloy::providers::{fillers::CachedNonceManager, ProviderBuilder, RootProvider};
use alloy::sol;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{ensure, Context, Result};
use chrono::{TimeZone, Utc};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal, Signer as _};
use polymarket_client_sdk::clob::types::request::{
    BalanceAllowanceRequest, CancelMarketOrderRequest, OrdersRequest,
};
use polymarket_client_sdk::clob::types::response::CancelOrdersResponse;
use polymarket_client_sdk::clob::types::response::{
    BalanceAllowanceResponse, OpenOrderResponse, PostOrderResponse,
};
use polymarket_client_sdk::clob::types::{
    Amount, AssetType, OrderType, Side as PolymarketSide, SignatureType, SignedOrder, TickSize,
};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::error::{Error as PolymarketClientError, Status as PolymarketStatus};
use polymarket_client_sdk::types::{Address, Decimal, B256, U256};
use tokio::sync::Notify;

use crate::config::{AppConfig, PolymarketExecutionMode};
use crate::polymarket::order_rules::{truncate_to_decimals, VENDOR_LOT_SIZE_DECIMALS};
use crate::types::{OrderIntent, Side, TimeInForce};

pub mod live_ops;
pub(crate) mod order_lifecycle;
pub(crate) mod order_type;
pub mod sizing;
pub mod supervisor;

pub use order_lifecycle::build_order_terminal_fact;

type AuthenticatedClobClient = ClobClient<Authenticated<Normal>>;
const MAX_BATCH_ORDERS: usize = 15;
const HTTP_425_RETRY_ATTEMPTS: usize = 8;
const HTTP_425_RETRY_BASE_BACKOFF_MS: u64 = 1_000;
const HTTP_425_RETRY_MAX_BACKOFF_MS: u64 = 30_000;
const HTTP_429_RETRY_ATTEMPTS: usize = 5;
const HTTP_429_RETRY_BASE_BACKOFF_MS: u64 = 1_000;
const HTTP_429_RETRY_MAX_BACKOFF_MS: u64 = 30_000;
pub(crate) const POLYMARKET_GTD_SECURITY_BUFFER_MS: u64 = 60_000;
pub(crate) const POLYMARKET_GTD_EFFECTIVE_EXPIRY_GUARD_MS: u64 = 1_000;

#[derive(Clone)]
struct ApiRetryPolicy<'a> {
    endpoint: &'static str,
    method: &'a str,
    allow_read_retry: bool,
    retry_425_log: Option<&'static str>,
    retry_429_log: Option<&'static str>,
}

#[derive(Clone, Copy)]
enum ApiRetryKind {
    Http425,
    Http429,
    ReadTransient,
}

struct ApiRetryState {
    attempt: usize,
    backoff_ms: u64,
    rl_retries: usize,
    rl_backoff_ms: u64,
    read_retry: ReadRetryState,
}

impl ApiRetryState {
    fn new() -> Self {
        Self {
            attempt: 0,
            backoff_ms: HTTP_425_RETRY_BASE_BACKOFF_MS,
            rl_retries: 0,
            rl_backoff_ms: HTTP_429_RETRY_BASE_BACKOFF_MS,
            read_retry: ReadRetryState::default(),
        }
    }
}

enum ApiRetryOutcome<T> {
    Success(T),
    Retry(ApiRetryKind),
    Error(PolymarketClientError),
}

sol! {
    #[sol(rpc)]
    interface IConditionalTokens {
        event ConditionResolution(
            bytes32 indexed conditionId,
            address indexed oracle,
            bytes32 indexed questionId,
            uint256 outcomeSlotCount,
            uint256[] payoutNumerators
        );
        event PayoutRedemption(
            address indexed redeemer,
            address indexed collateralToken,
            bytes32 indexed parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets,
            uint256 payout
        );
        function splitPosition(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata partition,
            uint256 amount
        ) external;
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] calldata indexSets
        ) external;
        function payoutDenominator(bytes32 conditionId) external view returns (uint256);
        function payoutNumerators(bytes32 conditionId, uint256 index) external view returns (uint256);
    }
    interface IPolymarketCollateralToken {
        event Wrapped(
            address indexed caller,
            address indexed asset,
            address indexed to,
            uint256 amount
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionPayoutVector {
    pub condition_id: B256,
    pub outcome_slot_0_numerator: u128,
    pub outcome_slot_1_numerator: u128,
    pub denominator: u128,
}

impl ConditionPayoutVector {
    pub fn payout(&self, outcome_index: usize) -> Option<f64> {
        let numerator = match outcome_index {
            0 => self.outcome_slot_0_numerator,
            1 => self.outcome_slot_1_numerator,
            _ => return None,
        };
        Some(numerator as f64 / self.denominator as f64)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitConfirmation {
    pub tx_hash: String,
    pub receipt: SplitReceiptStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitReceiptStatus {
    Pending,
    Confirmed,
    Reverted,
    ReconciliationRequired { error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtfSettlementConfirmation {
    Confirmed {
        receipt: crate::trade_session_ledger::SettlementCollateralReceipt,
    },
    ReconciliationRequired {
        tx_hash: String,
        error: String,
    },
}

#[derive(Clone)]
enum LiveClobClient {
    Normal(AuthenticatedClobClient),
}

macro_rules! dispatch_client {
    ($self:expr, |$c:ident| $body:expr) => {
        match $self {
            LiveClobClient::Normal($c) => $body,
        }
    };
}

impl LiveClobClient {
    fn address(&self) -> Address {
        dispatch_client!(self, |c| c.address())
    }

    fn credentials(&self) -> &Credentials {
        dispatch_client!(self, |c| c.credentials())
    }

    fn heartbeats_active(&self) -> bool {
        dispatch_client!(self, |c| c.heartbeats_active())
    }

    fn set_tick_size(&self, token_id: U256, tick_size: TickSize) {
        dispatch_client!(self, |c| c.set_tick_size(token_id, tick_size))
    }

    fn set_neg_risk(&self, token_id: U256, neg_risk: bool) {
        dispatch_client!(self, |c| c.set_neg_risk(token_id, neg_risk))
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionAck {
    pub order_id: Option<String>,
    pub accepted: bool,
    pub status: Option<String>,
    pub error_msg: Option<String>,
    pub reject_kind: Option<RejectKind>,
    pub request_ts_ms: Option<u64>,
    pub response_ts_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CancelAck {
    pub canceled: Vec<String>,
    pub not_canceled: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectKind {
    AggressiveNoMatch,
    Http425ServiceNotReady,
    MarketNotReady,
    NoOrderbook,
    FeeRate,
    TickSize,
    Balance,
    PostOnly,
    InvalidPrice,
    Other,
}

#[derive(Debug, Clone)]
pub(crate) struct DetectedOrder {
    id: String,
    asset_id: U256,
    side: PolymarketSide,
    price: Decimal,
    original_size: Decimal,
}

#[derive(Debug, Clone)]
pub struct PolymarketUserWsAuth {
    pub address: Address,
    pub credentials: Credentials,
}

#[derive(Clone)]
pub struct ExecutionClient {
    backend: ExecutionBackend,
}

#[derive(Clone)]
enum ExecutionBackend {
    Live(PolymarketExecutionLive),
    Sim(ExecutionSimClient),
}

#[derive(Clone)]
pub struct PolymarketExecutionLive {
    signer: Arc<PrivateKeySigner>,
    cached_nonce_manager: CachedNonceManager,
    client: LiveClobClient,
    order_post_client: reqwest::Client,
    use_server_time: bool,
    chain_id: u64,
    account_rpc_url: String,
    finality_http: reqwest::Client,
    finality_provider: RootProvider,
    funder: Option<Address>,
    live_ops: LiveOpsHandle,
    reclaim_trigger: Arc<Notify>,
}

#[derive(Clone)]
struct ExecutionSimClient {
    state: ExecutionSimHandle,
}

#[derive(Debug, Clone)]
pub struct ExecutionSimHandle {
    state: Arc<Mutex<ExecutionSimState>>,
}

#[derive(Debug, Clone)]
pub struct SimSubmittedOrder {
    pub order_id: String,
    pub intent: OrderIntent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimSubmittedCtfOperationKind {
    Split,
    Merge,
    Redeem,
}

#[derive(Debug, Clone)]
pub struct SimSubmittedCtfOperation {
    pub tx_hash: String,
    pub kind: SimSubmittedCtfOperationKind,
    pub condition_ids: Vec<B256>,
    pub amount_units_6dp: Vec<(B256, U256)>,
}

#[derive(Debug, Clone)]
struct ExecutionSimState {
    heartbeats_active: bool,
    cancel_delay_ms: u64,
    next_order_seq: u64,
    next_ctf_seq: u64,
    submitted: Vec<SimSubmittedOrder>,
    consumed_submitted: usize,
    submitted_ctf_ops: Vec<SimSubmittedCtfOperation>,
    consumed_submitted_ctf_ops: usize,
    next_split_receipt: Option<SplitReceiptStatus>,
    canceled: Vec<String>,
    consumed_canceled: usize,
    live_ops: LiveOpsHandle,
    reclaim_trigger: Arc<Notify>,
}

impl ExecutionSimHandle {
    pub fn heartbeats_active(&self) -> bool {
        self.state
            .lock()
            .expect("execution sim lock poisoned")
            .heartbeats_active
    }

    pub fn cancel_delay_ms(&self) -> u64 {
        self.state
            .lock()
            .expect("execution sim lock poisoned")
            .cancel_delay_ms
    }

    pub fn submitted_orders(&self) -> Vec<SimSubmittedOrder> {
        self.state
            .lock()
            .expect("execution sim lock poisoned")
            .submitted
            .clone()
    }

    pub(crate) fn take_submitted_orders_delta(&self) -> Result<Vec<SimSubmittedOrder>> {
        let mut state = self.state.lock().expect("execution sim lock poisoned");
        let end = state.submitted.len();
        let delta = clone_append_only_delta(
            &state.submitted,
            state.consumed_submitted,
            "submitted orders",
        )?;
        state.consumed_submitted = end;
        Ok(delta)
    }

    pub(crate) fn take_canceled_orders_delta(&self) -> Result<Vec<String>> {
        let mut state = self.state.lock().expect("execution sim lock poisoned");
        let end = state.canceled.len();
        let delta =
            clone_append_only_delta(&state.canceled, state.consumed_canceled, "canceled orders")?;
        state.consumed_canceled = end;
        Ok(delta)
    }

    pub(crate) fn take_submitted_ctf_ops_delta(&self) -> Result<Vec<SimSubmittedCtfOperation>> {
        let mut state = self.state.lock().expect("execution sim lock poisoned");
        let end = state.submitted_ctf_ops.len();
        let delta = clone_append_only_delta(
            &state.submitted_ctf_ops,
            state.consumed_submitted_ctf_ops,
            "submitted CTF operations",
        )?;
        state.consumed_submitted_ctf_ops = end;
        Ok(delta)
    }

    pub fn live_ops(&self) -> LiveOpsHandle {
        self.state
            .lock()
            .expect("execution sim lock poisoned")
            .live_ops
            .clone()
    }

    pub fn reclaim_trigger(&self) -> Arc<Notify> {
        self.state
            .lock()
            .expect("execution sim lock poisoned")
            .reclaim_trigger
            .clone()
    }
}

fn clone_append_only_delta<T: Clone>(
    values: &[T],
    consumed: usize,
    stream: &str,
) -> Result<Vec<T>> {
    let end = values.len();
    ensure!(
        consumed <= end,
        "execution simulator {stream} regressed or consumed offset is out of range: consumed={consumed} len={end}"
    );
    let delta = values
        .get(consumed..end)
        .context("validated execution simulator consumed range was unavailable")?
        .to_vec();
    Ok(delta)
}

mod cancel;
mod client;
pub mod http_support;
mod live_client;
mod order_query;
mod order_submit;
mod relayer;
mod vendor_types;

pub use self::live_ops::LiveOpsHandle;
use self::{http_support::*, order_submit::*, vendor_types::*};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

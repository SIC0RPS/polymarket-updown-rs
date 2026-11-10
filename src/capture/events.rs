use serde::{Deserialize, Serialize};

use crate::polymarket::identity::MarketId;
use crate::risk::{AuthoritativeMarketExposure, RiskSnapshot};

pub fn signed_delay_ms(recv_ts_ms: u64, source_ts_ms: u64) -> i64 {
    let delay = recv_ts_ms as i128 - source_ts_ms as i128;
    delay.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}
// ---------------------------------------------------------------------------
// Polymarket market WS inbound
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketMarketWsIn {
    pub session_ts_ms: u64,
    pub channel: String,
    pub asset_id: String,
    pub event_ts_ms: u64,
    pub recv_ts_ms: u64,
    pub ingest_delay_ms: i64,
    pub payload: std::sync::Arc<bbwick_engine::PolymarketOddsState>,
}

// ---------------------------------------------------------------------------
// Live execution latency facts
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderSubmitLatencyCapture {
    pub session_ts_ms: u64,
    #[serde(default)]
    pub decision_ts_ms: u64,
    #[serde(default)]
    pub supervisor_admission_ts_ms: u64,
    #[serde(default)]
    pub stage: String,
    #[serde(default)]
    pub attempt: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpretation_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_evidence: Option<String>,
    pub order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_id: Option<String>,
    pub token: String,
    pub side: String,
    pub close_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qty: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_px: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_connection_session_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_frame_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_state_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_consumption_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_input_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_intent_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_range_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_range_end: Option<usize>,
    pub request_ts_ms: u64,
    #[serde(default)]
    pub decision_to_request_ms: u64,
    #[serde(default)]
    pub decision_to_admission_ms: u64,
    #[serde(default)]
    pub admission_to_request_ms: u64,
    pub response_ts_ms: u64,
    pub ack_latency_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_to_handoff_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_to_send_invocation_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_invocation_to_response_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_to_owner_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_pending_intent_age_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_headers_ts_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_elapsed_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_headers_elapsed_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_elapsed_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub construction_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_to_submit_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_to_server_time_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_time_request_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_time_to_submit_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submit_to_headers_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_interpreted_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_admission_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_to_decision_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_to_submit_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_reaction_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timing_missing_phases: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timing_reversed_phases: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrderSubmitRejectCapture {
    pub session_ts_ms: u64,
    pub market_id: Option<String>,
    pub token: String,
    pub side: String,
    pub close_hint: Option<String>,
    pub qty: f64,
    pub limit_px: f64,
    pub reject_kind: String,
    pub error_msg: String,
    pub request_ts_ms: Option<u64>,
    pub response_ts_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalOrderBlockCapture {
    pub session_ts_ms: u64,
    pub reason: String,
    pub token: Option<String>,
    pub side: Option<String>,
    #[serde(default)]
    pub detail: std::collections::BTreeMap<String, String>,
    pub raw: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RiskBlockCapture {
    pub session_ts_ms: u64,
    pub rule: String,
    pub details: String,
    pub market_id: Option<String>,
    pub token: String,
    pub side: String,
    pub close_hint: Option<String>,
    pub qty: f64,
    pub limit_px: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketUserOrderCapture {
    pub session_ts_ms: u64,
    pub recv_ts_ms: u64,
    pub event_ts_ms: u64,
    pub order_id: String,
    pub market: String,
    pub token: String,
    pub side: String,
    pub price: String,
    pub msg_type: Option<String>,
    pub outcome: Option<String>,
    pub owner: Option<String>,
    pub order_owner: Option<String>,
    pub original_size: Option<String>,
    pub size_matched: Option<String>,
    pub timestamp: Option<i64>,
    pub associate_trades: Option<Vec<String>>,
    pub status: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketUserTradeCapture {
    pub session_ts_ms: u64,
    pub recv_ts_ms: u64,
    pub event_ts_ms: u64,
    pub trade_id: String,
    pub market: String,
    pub token: String,
    pub side: String,
    pub size: String,
    pub price: String,
    pub status: String,
    pub msg_type: Option<String>,
    pub last_update: Option<i64>,
    pub matchtime: Option<i64>,
    pub timestamp: Option<i64>,
    pub outcome: Option<String>,
    pub owner: Option<String>,
    pub trade_owner: Option<String>,
    pub taker_order_id: Option<String>,
    pub fee_rate_bps: Option<String>,
    pub transaction_hash: Option<String>,
    pub trader_side: Option<String>,
    #[serde(default)]
    pub maker_orders: Vec<PolymarketUserTradeMakerOrderCapture>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketUserTradeMakerOrderCapture {
    pub order_id: String,
    pub owner: String,
    pub token: String,
    pub matched_amount: String,
    pub price: String,
    pub outcome: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplaySeedCapture {
    pub session_ts_ms: u64,
    pub usdc_balance: Option<f64>,
    pub trading_collateral_balance: Option<f64>,
    pub current_wallet_equity: Option<f64>,
    pub current_equity: f64,
    pub max_order_qty: f64,
    pub max_market_equity_pct: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplayRiskSnapshotCapture {
    pub session_ts_ms: u64,
    pub updated_at_ms: u64,
    pub enabled: bool,
    pub trading_allowed: bool,
    pub freeze_level: u8,
    pub frozen_rule: Option<String>,
    pub frozen_details: Option<String>,
    pub last_recon_at_ms: Option<u64>,
    pub usdc_balance: Option<f64>,
    pub wallet_usdc_balance: Option<f64>,
    pub wallet_matic_balance: Option<f64>,
    pub marked_positions_value: Option<f64>,
    pub current_wallet_equity: Option<f64>,
    pub trading_collateral_balance: Option<f64>,
    pub account_equity_updated_at_ms: Option<u64>,
    pub exchange_allowance: Option<f64>,
    pub rest_open_orders: Option<usize>,
    pub rest_positions: Option<usize>,
    pub peak_equity: f64,
    pub current_equity: f64,
    pub last_positive_equity: Option<f64>,
    pub max_order_qty: f64,
    pub max_window_cost: f64,
    pub max_market_equity_pct: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_account_audit: Option<ExternalAccountAuditCapture>,
    #[serde(default)]
    pub authoritative_market_exposures: Vec<ReplayMarketExposureCapture>,
    #[serde(default)]
    pub inflight_buy_notional_by_market: Vec<ReplayMarketNotionalCapture>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalAccountAuditCapture {
    pub requested_at_ms: u64,
    pub completed_at_ms: u64,
    pub trigger: String,
    pub wallet: ExternalWalletAuditOutcome,
    pub clob: ExternalClobAuditOutcome,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExternalWalletAuditOutcome {
    Success {
        usdc_balance: f64,
        pusd_balance: f64,
    },
    Error {
        message: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExternalClobAuditOutcome {
    Success { balance: f64, allowance: f64 },
    Error { message: String },
}

mod strategy_consumption;
pub use strategy_consumption::{
    StrategyConsumptionInput, StrategyConsumptionKind, StrategyConsumptionRecord,
    STRATEGY_CONSUMPTION_SCHEMA_VERSION,
};

/// Discovery snapshot exactly as published to the Polymarket WS runtime.
///
/// Replay needs this because `PolymarketStateStore` only admits a book frame whose token
/// maps to a known market, and that mapping comes solely from `sync_discovery_snapshot`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolymarketDiscoverySnapshotCapture {
    pub session_ts_ms: u64,
    pub snapshot: bbwick_engine::DiscoverySnapshot,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ControlActuationCapture {
    pub session_ts_ms: u64,
    pub subject: String,
    pub enabled: bool,
    pub source: String,
    pub reason: Option<String>,
    pub actor: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CtfInventoryOperationLatencyCapture {
    pub session_ts_ms: u64,
    pub market_id: String,
    pub condition_id: String,
    pub operation: String,
    pub status: String,
    pub amount_units_6dp: u128,
    pub pending_ts_ms: u64,
    pub materialized_ts_ms: u64,
    pub latency_ms: u64,
    pub materialization_source: String,
    pub tx_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplayMarketExposureCapture {
    pub market_id: String,
    pub open_cohort_committed_outlay_units_6dp: u128,
    pub open_buy_notional: f64,
    pub snapshot_ts_ms: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplayMarketNotionalCapture {
    pub market_id: String,
    pub notional: f64,
}

impl ReplayRiskSnapshotCapture {
    pub fn from_snapshot(session_ts_ms: u64, snap: &RiskSnapshot) -> Self {
        let authoritative_market_exposures = snap
            .authoritative_market_exposures
            .values()
            .map(|exposure| ReplayMarketExposureCapture {
                market_id: exposure.market_id.to_string(),
                open_cohort_committed_outlay_units_6dp: exposure
                    .open_cohort_committed_outlay_units_6dp,
                open_buy_notional: exposure.open_buy_notional,
                snapshot_ts_ms: exposure.snapshot_ts_ms,
            })
            .collect();
        let inflight_buy_notional_by_market = snap
            .inflight_buy_notional_by_market
            .iter()
            .map(|(market_id, notional)| ReplayMarketNotionalCapture {
                market_id: market_id.to_string(),
                notional: *notional,
            })
            .collect();
        Self {
            session_ts_ms,
            updated_at_ms: snap.updated_at_ms,
            enabled: snap.enabled,
            trading_allowed: snap.trading_allowed,
            freeze_level: snap.freeze_level,
            frozen_rule: snap.frozen_rule.clone(),
            frozen_details: snap.frozen_details.clone(),
            last_recon_at_ms: snap.last_recon_at_ms,
            usdc_balance: snap.usdc_balance,
            wallet_usdc_balance: snap.wallet_usdc_balance,
            wallet_matic_balance: snap.wallet_matic_balance,
            marked_positions_value: snap.marked_positions_value,
            current_wallet_equity: snap.current_wallet_equity,
            trading_collateral_balance: snap.trading_collateral_balance,
            account_equity_updated_at_ms: snap.account_equity_updated_at_ms,
            exchange_allowance: snap.exchange_allowance,
            rest_open_orders: snap.rest_open_orders,
            rest_positions: snap.rest_positions,
            peak_equity: snap.peak_equity,
            current_equity: snap.current_equity,
            last_positive_equity: snap.last_positive_equity,
            max_order_qty: snap.max_order_qty,
            max_window_cost: snap.max_window_cost,
            max_market_equity_pct: snap.max_market_equity_pct,
            external_account_audit: None,
            authoritative_market_exposures,
            inflight_buy_notional_by_market,
        }
    }

    pub fn from_snapshot_with_external_account_audit(
        session_ts_ms: u64,
        snap: &RiskSnapshot,
        audit: ExternalAccountAuditCapture,
    ) -> Self {
        let mut row = Self::from_snapshot(session_ts_ms, snap);
        row.external_account_audit = Some(audit);
        row
    }

    pub fn into_snapshot(self) -> RiskSnapshot {
        let authoritative_market_exposures = self
            .authoritative_market_exposures
            .into_iter()
            .map(|exposure| {
                let market_id = MarketId::new(exposure.market_id);
                (
                    market_id.clone(),
                    AuthoritativeMarketExposure {
                        market_id,
                        open_cohort_committed_outlay_units_6dp: exposure
                            .open_cohort_committed_outlay_units_6dp,
                        open_buy_notional: exposure.open_buy_notional,
                        snapshot_ts_ms: exposure.snapshot_ts_ms,
                    },
                )
            })
            .collect();
        let inflight_buy_notional_by_market = self
            .inflight_buy_notional_by_market
            .into_iter()
            .map(|value| (MarketId::new(value.market_id), value.notional))
            .collect();
        RiskSnapshot {
            updated_at_ms: self.updated_at_ms,
            enabled: self.enabled,
            trading_allowed: self.trading_allowed,
            freeze_level: self.freeze_level,
            frozen_rule: self.frozen_rule,
            frozen_details: self.frozen_details,
            last_recon_at_ms: self.last_recon_at_ms,
            usdc_balance: self.usdc_balance,
            wallet_usdc_balance: self.wallet_usdc_balance,
            wallet_matic_balance: self.wallet_matic_balance,
            marked_positions_value: self.marked_positions_value,
            current_wallet_equity: self.current_wallet_equity,
            trading_collateral_balance: self.trading_collateral_balance,
            account_equity_updated_at_ms: self.account_equity_updated_at_ms,
            exchange_allowance: self.exchange_allowance,
            rest_open_orders: self.rest_open_orders,
            rest_positions: self.rest_positions,
            peak_equity: self.peak_equity,
            current_equity: self.current_equity,
            last_positive_equity: self.last_positive_equity,
            max_order_qty: self.max_order_qty,
            max_window_cost: self.max_window_cost,
            max_market_equity_pct: self.max_market_equity_pct,
            authoritative_market_exposures,
            inflight_buy_notional_by_market,
        }
    }
}

// ---------------------------------------------------------------------------
// Session metadata
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionMeta {
    pub session_id: String,
    pub run_id: String,
    pub start_ts_ms: u64,
    pub end_ts_ms: Option<u64>,
    pub pid: u32,
    pub execution_mode: String,
    pub strategy_kind: String,
    pub config_hash: String,
    #[serde(default)]
    pub optimizer_config_hash: Option<String>,
    #[serde(default)]
    pub final_config_hash: Option<String>,
    #[serde(default)]
    pub config_timeline_hash: Option<String>,
    #[serde(default)]
    pub config_change_count: Option<usize>,
    pub symbols: Vec<String>,
    pub market_ids: Vec<String>,
    pub exit_reason: Option<String>,
}

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;

use crate::observability::action::RuntimeIssueKind;
use crate::observability::strategy_status::{
    OracleWaitReason, QuoteBlockReasonCount, QuoteRecomputeReasonObs, StrategyActivityKind,
    StrategySelectionStatus,
};
use crate::orders::OpenOrdersState;
use crate::polymarket::identity::MarketId;
use crate::portfolio::PortfolioState;
use crate::risk::RiskSnapshot;

/// Persistent per-asset closed-trade statistics, accumulated incrementally.
#[derive(Debug, Clone, Default)]
pub struct TuiAssetStats {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    pub total_pnl: f64,
    pub best_pnl: f64,
    pub worst_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct UiState {
    pub started_at_ms: u64,
    pub mode: String,
    pub market_data_source: String,
    pub execution_backend: String,
    pub log_level: String,

    pub last_oracle: Option<OracleSnapshot>,
    pub oracle_runtime: Option<OracleRuntimeStatusSnapshot>,
    pub oracle_quotes: HashMap<String, OracleSnapshot>,
    pub oracle_tick_history: HashMap<String, VecDeque<OracleSnapshot>>,
    pub oracle_candles: HashMap<String, VecDeque<OracleCandleSnapshot>>,
    pub cex_quotes: HashMap<String, CexQuoteSnapshot>,
    pub last_market: Option<MarketSnapshot>,
    pub next_market: Option<MarketSnapshot>,
    pub last_strategy_status: Option<StrategyStatusSnapshot>,
    pub last_signal: Option<SignalSnapshot>,
    pub last_order_intent: Option<OrderIntentSnapshot>,
    pub last_order_ack: Option<OrderAckSnapshot>,
    pub last_error: Option<ErrorSnapshot>,
    pub post_only_reject_status: Option<PostOnlyRejectStatusSnapshot>,
    pub recent_risk_block: Option<RecentRiskBlockSnapshot>,
    pub recent_local_order_block: Option<LocalOrderBlockSnapshot>,
    pub recent_exit_execution: Option<RecentExitExecutionSnapshot>,

    pub market_names: HashMap<MarketId, String>,
    pub market_snapshots: HashMap<MarketId, MarketSnapshot>,
    /// Reverse map: token string → condition_id, built from market snapshots.
    pub token_to_condition_id: HashMap<String, MarketId>,
    pub counters: UiCounters,
    pub portfolio: PortfolioState,
    pub open_orders: OpenOrdersState,
    pub risk: RiskSnapshot,
    pub wallet_observed_at_ms: Option<u64>,
    /// Persistent per-asset stats accumulated from every closed trade (survives buffer eviction).
    pub asset_stats: HashMap<String, TuiAssetStats>,
    /// Sequence number of last processed closed trade for incremental updates.
    pub last_asset_stats_seq: u64,
    /// Optimization progress snapshot (set when running optimizer mode).
    pub optimization: Option<OptimizationSnapshot>,
    /// Which underlyings are enabled for quoting (dashboard toggle).
    pub enabled_assets: Vec<String>,
}

impl UiState {
    pub fn new(started_at_ms: u64, mode: String) -> Self {
        Self {
            started_at_ms,
            mode,
            market_data_source: "-".to_string(),
            execution_backend: "-".to_string(),
            log_level: "-".to_string(),
            last_oracle: None,
            oracle_runtime: None,
            oracle_quotes: HashMap::new(),
            oracle_tick_history: HashMap::new(),
            oracle_candles: HashMap::new(),
            cex_quotes: HashMap::new(),
            last_market: None,
            next_market: None,
            last_strategy_status: None,
            last_signal: None,
            last_order_intent: None,
            last_order_ack: None,
            last_error: None,
            post_only_reject_status: None,
            recent_risk_block: None,
            recent_local_order_block: None,
            recent_exit_execution: None,
            market_names: HashMap::new(),
            market_snapshots: HashMap::new(),
            token_to_condition_id: HashMap::new(),
            counters: UiCounters::default(),
            portfolio: PortfolioState::default(),
            open_orders: OpenOrdersState::default(),
            risk: RiskSnapshot::default(),
            wallet_observed_at_ms: None,
            asset_stats: HashMap::new(),
            last_asset_stats_seq: 0,
            optimization: None,
            enabled_assets: Vec::new(),
        }
    }

    pub fn dashboard_snapshot_clone(&self) -> Self {
        Self {
            started_at_ms: self.started_at_ms,
            mode: self.mode.clone(),
            market_data_source: self.market_data_source.clone(),
            execution_backend: self.execution_backend.clone(),
            log_level: self.log_level.clone(),
            last_oracle: self.last_oracle.clone(),
            oracle_runtime: self.oracle_runtime.clone(),
            oracle_quotes: self.oracle_quotes.clone(),
            oracle_tick_history: HashMap::new(),
            oracle_candles: HashMap::new(),
            cex_quotes: self.cex_quotes.clone(),
            last_market: self.last_market.clone(),
            next_market: self.next_market.clone(),
            last_strategy_status: self.last_strategy_status.clone(),
            last_signal: self.last_signal.clone(),
            last_order_intent: self.last_order_intent.clone(),
            last_order_ack: self.last_order_ack.clone(),
            last_error: self.last_error.clone(),
            post_only_reject_status: self.post_only_reject_status.clone(),
            recent_risk_block: self.recent_risk_block.clone(),
            recent_local_order_block: self.recent_local_order_block.clone(),
            recent_exit_execution: self.recent_exit_execution.clone(),
            market_names: self.market_names.clone(),
            market_snapshots: self.market_snapshots.clone(),
            token_to_condition_id: self.token_to_condition_id.clone(),
            counters: self.counters.clone(),
            portfolio: self.portfolio.clone(),
            open_orders: self.open_orders.clone(),
            risk: self.risk.clone(),
            wallet_observed_at_ms: self.wallet_observed_at_ms,
            asset_stats: self.asset_stats.clone(),
            last_asset_stats_seq: self.last_asset_stats_seq,
            optimization: self.optimization.clone(),
            enabled_assets: self.enabled_assets.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PostOnlyRejectStatusSnapshot {
    pub active_count: usize,
    pub tokens: Vec<PostOnlyRejectTokenSnapshot>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RecentRiskBlockSnapshot {
    pub ts_ms: u64,
    pub rule: String,
    pub detail: std::collections::HashMap<String, String>,
    pub raw: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalOrderBlockSnapshot {
    pub ts_ms: u64,
    pub reason: String,
    pub token: Option<String>,
    pub side: Option<String>,
    pub detail: std::collections::HashMap<String, String>,
    pub raw: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PostOnlyRejectTokenSnapshot {
    pub token: String,
    pub streak: u8,
    pub cooldown_until_ms: u64,
    pub extra_buffer_ticks: u8,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RecentExitExecutionSnapshot {
    pub ts_ms: u64,
    pub market_id: Option<MarketId>,
    pub token: String,
    pub side: String,
    pub close_hint: String,
    pub reason: Option<String>,
    pub signal_reason: String,
    pub intent_px: Option<f64>,
    pub intent_qty: Option<f64>,
    pub ack_observed: bool,
    pub ack_ts_ms: Option<u64>,
    pub fill_observed: bool,
    pub fill_ts_ms: Option<u64>,
    pub fill_px: Option<f64>,
    pub fill_qty: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct OptimizationSnapshot {
    pub ts_ms: u64,
    pub active: bool,
    pub phase: String,
    pub record_elapsed_secs: u64,
    pub record_total_secs: u64,
    pub generation: usize,
    pub max_generations: usize,
    pub population_size: usize,
    pub workers_total: usize,
    pub workers_busy: usize,
    pub queue_depth: usize,
    pub evals_completed: usize,
    pub evals_planned: usize,
    pub evals_per_min: f64,
    pub best_name: String,
    pub best_score: f64,
    pub best_pnl: f64,
    pub best_trades: u64,
    pub best_win_rate: f64,
    pub best_profit_factor: f64,
    pub best_drawdown_pct: f64,
    pub best_open_positions: u64,
    pub best_rebate_accrual: f64,
    pub best_score_realized: f64,
    pub best_score_activity: f64,
    pub best_score_quality: f64,
    pub best_score_unrealized: f64,
    pub best_score_penalties: f64,
    pub min_trade_target: u64,
    pub candidates_with_trades: usize,
    pub profitable_candidates: usize,
    pub total_trades: u64,
    pub failed_evals: u64,
    pub lagged_ticks: u64,
}

#[derive(Debug, Clone)]
pub struct OracleSnapshot {
    pub ts_ms: u64,
    pub symbol: String,
    pub px: f64,
    pub vol: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct OracleCandleSnapshot {
    pub symbol: String,
    pub timeframe: String,
    pub ts_ms: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct CexQuoteSnapshot {
    pub ts_ms: u64,
    pub provider: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_sz: Option<f64>,
    pub ask_sz: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub ts_ms: u64,
    pub condition_id: MarketId,
    pub up_token: String,
    pub down_token: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub question: Option<String>,
    pub slug: Option<String>,
    pub underlying: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SignalSnapshot {
    pub ts_ms: u64,
    pub side: String,
    pub p_up: Option<f64>,
    pub edge: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct StrategyStatusSnapshot {
    pub ts_ms: u64,
    pub kind: String,
    pub status: String,
    pub p_up: Option<f64>,
    pub edge: Option<f64>,
    pub gate_drop_oracle: u64,
    pub gate_drop_book: u64,
    pub gate_drop_stale: u64,
    pub gate_drop_open_delay: u64,
    pub gate_drop_open_price: u64,
    pub gate_drop_entry_time: u64,
    pub gate_drop_blocked_market: u64,
    pub gate_drop_markout_toxicity: u64,
    pub gate_drop_oracle_total: u64,
    pub gate_drop_book_total: u64,
    pub gate_drop_stale_total: u64,
    pub gate_drop_open_delay_total: u64,
    pub gate_drop_open_price_total: u64,
    pub gate_drop_entry_time_total: u64,
    pub gate_drop_blocked_market_total: u64,
    pub gate_drop_markout_toxicity_total: u64,
    pub selection_status: Option<StrategySelectionStatus>,
    pub activity: StrategyActivityKind,
    pub recompute_reason: Option<QuoteRecomputeReasonObs>,
    pub queued: usize,
    pub drained: usize,
    pub ranked: usize,
    pub managed_exits: usize,
    pub quote_blocks: Vec<QuoteBlockReasonCount>,
    pub primary_block_detail: Option<BTreeMap<String, String>>,
    pub shock_toxicity: Option<f64>,
    pub markout_toxicity: Option<f64>,
    pub markout_samples: Option<u64>,
    pub oracle_wait_reason: Option<OracleWaitReason>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OracleRuntimeStatusSnapshot {
    pub ts_ms: u64,
    pub phase: String,
    pub connected: bool,
    pub required_symbols: Vec<String>,
    pub ready_symbols: Vec<String>,
    pub symbol_last_tick_ts_ms: Vec<(String, u64)>,
    pub symbol_last_tick_age_ms: Vec<(String, u64)>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderIntentSnapshot {
    pub ts_ms: u64,
    pub side: String,
    pub token: String,
    pub px: f64,
    pub size: f64,
    pub time_in_force: String,
    pub post_only: bool,
}

#[derive(Debug, Clone)]
pub struct OrderAckSnapshot {
    pub ts_ms: u64,
    pub order_id: String,
}

#[derive(Debug, Clone)]
pub struct ErrorSnapshot {
    pub ts_ms: u64,
    pub scope: String,
    pub msg: String,
    pub kind: Option<RuntimeIssueKind>,
    pub detail: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct UiCounters {
    pub actions: u64,
    pub errors: u64,
    pub signals: u64,
    pub risk_blocks: u64,
    pub order_intents: u64,
    pub order_acks: u64,
    pub fills: u64,
}

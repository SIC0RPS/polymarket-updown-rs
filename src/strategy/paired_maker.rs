use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
#[path = "support/admission.rs"]
pub(crate) mod admission;
#[path = "support/ctf_inventory.rs"]
pub(crate) mod ctf_inventory;
#[path = "support/market_index.rs"]
pub(crate) mod market_index;
#[path = "support/market_ranking/mod.rs"]
pub(crate) mod market_ranking;
#[path = "support/markout.rs"]
pub(crate) mod markout;
#[path = "support/orchestrator.rs"]
pub(crate) mod orchestrator;

use bbwick_engine::{OracleHistory, OracleHistoryView, OracleTradeFlowSnapshot};
use tokio::sync::watch;

use self::markout::{MarkoutState, PendingMarkout};
use super::pair_lifecycle::{PairLifecycleState, PairedCohort, PairedCohortKey};
use super::support::oracle_authority::ta::OracleTaFeatures;
use super::support::oracle_authority::{
    OracleAuthoritySnapshot, OracleBetaCalibrationTrainer, OracleMarketCalibrationTrainer,
    OracleMicrostructureLogisticTrainer,
};
use super::{StrategyDecision, StrategyGateDropCounters};
use crate::config::{BbwickOracleConfig, MakerRebateMmStrategyConfig, SharedStrategyConfig};
use crate::observability::strategy_status::{
    OpenCaptureAlertState, PersistentDiagnosticState, StrategySelectionStatus,
};
use crate::oracle_resolution::ChainlinkResolutionReducer;
use crate::orders::OpenOrdersState;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::polymarket::order_rules::normalized_min_order_size;
use crate::polymarket::state::{
    book_for_token, depth_for_token, BookDepth, BookTop, MarketProjection, PolymarketOddsState,
};
use crate::portfolio::PortfolioState;
use crate::strategy::support::shared::oracle_state::{
    OpenPriceState, OraclePxMode, OracleState, OracleVolatilityDiagnostics,
};
use polymarket_client_sdk::types::U256;

pub(crate) const OPEN_PX_MAX_DELAY_MS: u64 = 5_000;
pub(crate) const OPEN_PX_LATE_CONFIDENCE_DECAY_MS: u64 = 40_000;
pub(crate) const OPEN_PX_LATE_PENALTY_CAP: f64 = 0.6;
pub(crate) const OPEN_PX_LATE_MAX_CONFIDENCE_PENALTY: f64 = 0.75;
pub(crate) const OPEN_PX_LATE_MAX_TOXICITY_PENALTY: f64 = 1.5;
pub(crate) const BOOK_STALE_SOFT_MAX_MS: u64 = 8_000;
pub(crate) const BOOK_STALE_HARD_DROP_MS: u64 = 60_000;
pub(crate) const ORACLE_VOL_UPDATE_MIN_DT_MS: u64 = 100;
pub(crate) const ORACLE_MAX_ABS_RETURN_BPS_PER_UPDATE: f64 = 30.0;
pub(crate) const MARKOUT_MAX_PENDING: usize = 4_096;
pub(crate) const INVALID_TICK_KILL_AFTER_MS: u64 = 10_000;
pub(crate) const MARKOUT_REENTRY_COOLDOWN_TRIGGER: f64 = 1.0;
pub(crate) const MARKOUT_REENTRY_COOLDOWN_MARKOUT_THRESHOLD: f64 = -0.01;
pub(crate) const MARKOUT_REENTRY_COOLDOWN_MS: u64 = 30_000;
pub(crate) const MARKOUT_TOXICITY_HALF_LIFE_MS: u64 = 30_000;
pub(crate) const PAIRED_RELEASE_QTY_EPS: f64 = 1e-6;
pub(crate) const PAIR_LIFECYCLE_STALE_MS: u64 = 120_000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ConfirmedCtfEntryFact {
    cohort_id: String,
    amount_units_6dp: u128,
    entry_outlay_units_6dp: u128,
    materialized_ts_ms: u64,
}

impl ConfirmedCtfEntryFact {
    pub(crate) fn new(
        cohort_id: String,
        amount_units_6dp: u128,
        entry_outlay_units_6dp: u128,
        materialized_ts_ms: u64,
    ) -> Self {
        Self {
            cohort_id,
            amount_units_6dp,
            entry_outlay_units_6dp,
            materialized_ts_ms,
        }
    }

    pub(crate) fn cohort_id(&self) -> &str {
        &self.cohort_id
    }

    pub(crate) const fn amount_units_6dp(&self) -> u128 {
        self.amount_units_6dp
    }

    pub(crate) const fn entry_outlay_units_6dp(&self) -> u128 {
        self.entry_outlay_units_6dp
    }

    pub(crate) const fn materialized_ts_ms(&self) -> u64 {
        self.materialized_ts_ms
    }
}

// Decision-only First reference; these terms never establish inventory or cashflow.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OracleBuyEntryReference {
    // First's waiting clock starts at ordinary entry admission, before activation.
    pub(crate) admitted_at_ms: u64,
    pub(crate) market_end_ms: u64,
    // Native complete-set sizing uses the same quantity and principal units.
    pub(crate) amount_units_6dp: u128,
    pub(crate) up_basis_units_6dp: u128,
    pub(crate) down_basis_units_6dp: u128,
    // Winning side, activation time, original prediction time and original p_fair.
    // The central order owner stamps BUY authorization separately after admission.
    pub(crate) first_activation: Option<(
        super::hold_to_resolution::PairTokenSide,
        u64,
        Option<u64>,
        Option<f64>,
    )>,
}

pub struct MakerRebateMmStrategyInit {
    pub cfg: MakerRebateMmStrategyConfig,
    pub oracle_symbol: String,
    pub equity_baseline_usdt: f64,
    pub risk_rx: Option<watch::Receiver<crate::risk::RiskBudgetSnapshot>>,
    pub blocked_markets_rx: Option<watch::Receiver<HashSet<String>>>,
    pub open_orders_rx: Option<watch::Receiver<OpenOrdersState>>,
    pub bbwick_oracle_cfg: BbwickOracleConfig,
    pub oracle_history: Arc<OracleHistory>,
    pub odds: Arc<crate::polymarket::state::PolymarketOddsState>,
    pub market_projection: MarketProjection,
    pub portfolio_rx: watch::Receiver<PortfolioState>,
    pub shared_cfg: SharedStrategyConfig,
}

#[derive(Clone)]
pub struct MakerRebateMmStrategy {
    pub(crate) cfg: MakerRebateMmStrategyConfig,
    pub(crate) shared_cfg: SharedStrategyConfig,
    pub(crate) risk_rx: Option<watch::Receiver<crate::risk::RiskBudgetSnapshot>>,
    pub(crate) blocked_markets_rx: Option<watch::Receiver<HashSet<String>>>,
    pub(crate) open_orders_rx: Option<watch::Receiver<OpenOrdersState>>,
    pub(crate) bbwick_oracle_cfg: BbwickOracleConfig,
    pub(crate) oracle_history: Arc<OracleHistory>,
    pub(crate) oracle_candle_view: Arc<OracleHistoryView>,
    oracle_candle_consumed_mono_ns: u64,
    oracle_trade_flow_snapshot: Option<OracleTradeFlowSnapshot>,
    pub(crate) oracle_symbol: String,
    pub(crate) odds: Arc<crate::polymarket::state::PolymarketOddsState>,
    pub(crate) market_projection: MarketProjection,
    pub(crate) portfolio_rx: watch::Receiver<PortfolioState>,

    pub(crate) oracle_state: OracleState,
    pub(crate) current_engine_mono_ns: u64,
    pub(crate) last_oracle_px_mode: Option<OraclePxMode>,
    pub(crate) oracle_price_degraded_mode: Option<PersistentDiagnosticState<OraclePxMode>>,
    pub(crate) last_oracle_volatility: Option<OracleVolatilityDiagnostics>,

    pub(crate) open_px_by_market: HashMap<MarketId, OpenPriceState>,
    pub(crate) open_capture_diagnostic_by_market:
        HashMap<MarketId, PersistentDiagnosticState<OpenCaptureAlertState>>,
    pub(crate) pending: VecDeque<StrategyDecision>,

    pub(crate) pending_markouts: Vec<PendingMarkout>,
    pub(in crate::strategy) markout_state: HashMap<String, MarkoutState>,
    pub(crate) last_fill_seq: u64,
    pub(crate) last_fair_by_token: HashMap<String, f64>,
    pub(crate) gate_drops_cycle: StrategyGateDropCounters,
    pub(crate) gate_drops_total: StrategyGateDropCounters,
    pub(crate) last_selection_status: Option<StrategySelectionStatus>,
    pub(crate) quote_block_reasons_cycle: HashMap<&'static str, u32>,
    pub(crate) quote_block_detail_cycle: Option<(&'static str, String)>,
    pub(crate) ctf_admission_by_market: HashMap<String, String>,
    pub(crate) cancel_market_guard_by_id: HashMap<String, u64>,
    pub(crate) cancel_quote_guard_by_key: HashMap<String, u64>,
    pub(crate) pair_lifecycle_by_group: HashMap<String, PairLifecycleState>,
    pub(crate) paired_cohorts: HashMap<PairedCohortKey, PairedCohort>,
    pub(crate) last_paired_authority_sig: Option<(u64, u64, u64, Option<u64>, u64)>,
    pub(crate) oracle_direct_market_research: bool,
    pub(crate) oracle_authority_by_market: HashMap<MarketId, OracleAuthoritySnapshot>,
    pub(crate) oracle_buy_entry_by_condition:
        HashMap<polymarket_client_sdk::types::B256, OracleBuyEntryReference>,
    pub(crate) resolution_label_reducer: ChainlinkResolutionReducer,
    pub(crate) micro_logistic_trainer: OracleMicrostructureLogisticTrainer,
    pub(crate) market_calibration_trainer: OracleMarketCalibrationTrainer,
    pub(crate) beta_calibration_trainer: OracleBetaCalibrationTrainer,
    pub(crate) oracle_ta_feature_cache:
        RefCell<HashMap<OracleTaFeatureCacheKey, OracleTaFeatureCacheEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OracleTaFeatureCacheKey {
    pub(crate) candle_revision: u64,
    pub(crate) timeframe_ms: u64,
    pub(crate) closed_bar_bucket: u64,
    pub(crate) window_start_ms: u64,
    pub(crate) window_end_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OracleTaFeatureCacheEntry {
    pub(crate) features: Option<(OracleTaFeatures, u64, u32)>,
}

impl MakerRebateMmStrategy {
    pub(crate) fn market_is_blocked_for_entry(&self, selection: &MarketSelection) -> bool {
        self.blocked_markets_rx
            .as_ref()
            .map(|rx| rx.borrow())
            .is_some_and(|blocked| {
                blocked.contains(selection.market_id.as_str())
                    || blocked.contains(selection.condition_id.as_str())
            })
    }

    pub fn new(init: MakerRebateMmStrategyInit) -> anyhow::Result<Self> {
        let MakerRebateMmStrategyInit {
            cfg,
            oracle_symbol,
            equity_baseline_usdt: _equity_baseline_usdt,
            risk_rx,
            blocked_markets_rx,
            open_orders_rx,
            bbwick_oracle_cfg,
            oracle_history,
            odds,
            market_projection,
            portfolio_rx,
            shared_cfg,
        } = init;
        let micro_logistic_trainer = OracleMicrostructureLogisticTrainer::new(
            cfg.live_fair_value_model.micro_logistic_l2_penalty,
        );
        let oracle_candle_view = oracle_history.pin()?;
        Ok(Self {
            cfg,
            shared_cfg,
            risk_rx,
            blocked_markets_rx,
            open_orders_rx,
            bbwick_oracle_cfg,
            oracle_history,
            oracle_candle_view,
            oracle_candle_consumed_mono_ns: 0,
            oracle_trade_flow_snapshot: None,
            oracle_symbol,
            odds,
            market_projection,
            portfolio_rx,
            oracle_state: OracleState::default(),
            current_engine_mono_ns: 0,
            last_oracle_px_mode: None,
            oracle_price_degraded_mode: None,
            last_oracle_volatility: None,
            open_px_by_market: HashMap::new(),
            open_capture_diagnostic_by_market: HashMap::new(),
            pending: VecDeque::new(),
            pending_markouts: Vec::new(),
            markout_state: HashMap::new(),
            last_fill_seq: 0,
            last_fair_by_token: HashMap::new(),
            gate_drops_cycle: StrategyGateDropCounters::default(),
            gate_drops_total: StrategyGateDropCounters::default(),
            last_selection_status: None,
            quote_block_reasons_cycle: HashMap::new(),
            quote_block_detail_cycle: None,
            ctf_admission_by_market: HashMap::new(),
            cancel_market_guard_by_id: HashMap::new(),
            cancel_quote_guard_by_key: HashMap::new(),
            pair_lifecycle_by_group: HashMap::new(),
            paired_cohorts: HashMap::new(),
            last_paired_authority_sig: None,
            oracle_direct_market_research: false,
            oracle_authority_by_market: HashMap::new(),
            oracle_buy_entry_by_condition: HashMap::new(),
            resolution_label_reducer: ChainlinkResolutionReducer::default(),
            micro_logistic_trainer,
            market_calibration_trainer: OracleMarketCalibrationTrainer::default(),
            beta_calibration_trainer: OracleBetaCalibrationTrainer::default(),
            oracle_ta_feature_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn pin_oracle_candles(&mut self) -> anyhow::Result<()> {
        (
            self.oracle_candle_view,
            self.oracle_trade_flow_snapshot,
            self.oracle_candle_consumed_mono_ns,
        ) = self.oracle_history.pin_for_symbol(&self.oracle_symbol)?;
        Ok(())
    }

    pub fn consumed_trade_flow_snapshot(&self) -> Option<OracleTradeFlowSnapshot> {
        self.oracle_trade_flow_snapshot
    }

    pub fn consumed_candle_view(&self) -> &OracleHistoryView {
        &self.oracle_candle_view
    }

    pub fn consumed_candle_mono_ns(&self) -> u64 {
        self.oracle_candle_consumed_mono_ns
    }

    pub(in crate::strategy) fn current_book_for_token(
        &self,
        odds: &PolymarketOddsState,
        token_id: U256,
    ) -> Option<BookTop> {
        book_for_token(odds, token_id).cloned()
    }

    pub(in crate::strategy) fn current_books_for_market(
        &self,
        odds: &PolymarketOddsState,
        market: &MarketSelection,
    ) -> Option<(BookTop, BookTop)> {
        Some((
            self.current_book_for_token(odds, market.up_token)?,
            self.current_book_for_token(odds, market.down_token)?,
        ))
    }

    pub(in crate::strategy) fn current_depth_for_token(
        &self,
        odds: &PolymarketOddsState,
        token_id: U256,
    ) -> Option<BookDepth> {
        depth_for_token(odds, token_id).cloned()
    }

    pub(in crate::strategy) fn current_depths_for_market(
        &self,
        odds: &PolymarketOddsState,
        market: &MarketSelection,
    ) -> (Option<BookDepth>, Option<BookDepth>) {
        (
            self.current_depth_for_token(odds, market.up_token),
            self.current_depth_for_token(odds, market.down_token),
        )
    }

    pub(in crate::strategy) fn current_books_for_tokens(
        &self,
        odds: &PolymarketOddsState,
        up_token: &str,
        down_token: &str,
    ) -> Option<(BookTop, BookTop)> {
        let up_token_id = up_token.parse::<U256>().ok()?;
        let down_token_id = down_token.parse::<U256>().ok()?;
        Some((
            self.current_book_for_token(odds, up_token_id)?,
            self.current_book_for_token(odds, down_token_id)?,
        ))
    }
}

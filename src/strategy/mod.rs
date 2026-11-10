#[path = "core/exit_router.rs"]
pub(crate) mod exit_router;
pub mod facts;
#[path = "support/fair_value/mod.rs"]
pub mod fair_value;
#[path = "core/hold_to_resolution.rs"]
pub(crate) mod hold_to_resolution;
#[path = "support/market_quarantine.rs"]
pub(crate) mod market_quarantine;
#[path = "core/pair_lifecycle/mod.rs"]
pub(crate) mod pair_lifecycle;
pub(crate) mod paired_maker;
#[path = "support/passive_quotes.rs"]
pub(crate) mod passive_quotes;
pub mod support;
pub use facts::{
    CtfLegDecisionReason, CtfLegEconomicOutcome, CtfOwnedLegCloseReason, CtfOwnedLegOutcomeFact,
    StrategyFact, StrategyFactSink,
};
pub use paired_maker::{MakerRebateMmStrategy, MakerRebateMmStrategyInit};
pub use support::oracle_authority::{OracleLearningMetrics, OracleLearningSnapshot};

use crate::polymarket::identity::MarketId;
use crate::types::{OrderIntent, Side};
use polymarket_client_sdk::types::{Decimal, B256};
use serde::{Deserialize, Serialize};

pub const CTF_FULL_SET_DECIMALS: u32 = 6;
pub const CTF_FULL_SET_SCALE: u128 = 1_000_000;
pub const CTF_CLOB_LOT_UNITS_6DP: u128 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InventoryOperation {
    SplitCompleteSet,
    MergeCompleteSet,
}

impl InventoryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SplitCompleteSet => "split_complete_set",
            Self::MergeCompleteSet => "merge_complete_set",
        }
    }
}

pub const fn inventory_operation_allowed_under_pause(
    operation: InventoryOperation,
    paused: bool,
) -> bool {
    !(paused && matches!(operation, InventoryOperation::SplitCompleteSet))
}

#[derive(Debug, Clone)]
pub struct InventoryIntent {
    pub operation: InventoryOperation,
    pub cohort_id: Option<String>,
    pub market_id: MarketId,
    pub gamma_market_id: Option<String>,
    pub condition_id: B256,
    pub up_token: String,
    pub down_token: String,
    pub amount_units_6dp: u128,
}

impl InventoryIntent {
    pub fn amount_qty(&self) -> f64 {
        full_set_units_to_qty(self.amount_units_6dp)
    }
}

pub fn full_set_qty_to_units_6dp(qty: Decimal) -> Option<u128> {
    if qty <= Decimal::ZERO {
        return None;
    }
    let scaled = (qty * Decimal::from(CTF_FULL_SET_SCALE)).trunc();
    if scaled <= Decimal::ZERO {
        return None;
    }
    u128::try_from(scaled.mantissa()).ok()
}

pub fn full_set_units_to_qty(units_6dp: u128) -> f64 {
    units_6dp as f64 / CTF_FULL_SET_SCALE as f64
}

pub fn ctf_units_to_clob_qty(units_6dp: u128) -> f64 {
    full_set_units_to_qty(ctf_units_to_clob_units_6dp(units_6dp))
}

pub fn ctf_units_to_clob_units_6dp(units_6dp: u128) -> u128 {
    (units_6dp / CTF_CLOB_LOT_UNITS_6DP) * CTF_CLOB_LOT_UNITS_6DP
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CtfSplitBookTop {
    pub token_id: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_sz: Option<f64>,
    pub ask_sz: Option<f64>,
    pub ts_ms: u64,
}

impl CtfSplitBookTop {
    // Relative entry-book midpoint value allocates the joint entry basis.
    // Summing bid and ask avoids midpoint truncation.
    pub(crate) fn basis_weight_units_8dp(&self) -> Option<u128> {
        let fixed = |price: f64| {
            if price == 0.0 {
                Some(0)
            } else {
                crate::polymarket::fixed_math::price_to_units_8dp(price).map(u128::from)
            }
        };
        fixed(self.bid).and_then(|bid| fixed(self.ask).and_then(|ask| bid.checked_add(ask)))
    }

    pub(crate) fn from_book_top(top: &crate::polymarket::state::BookTop) -> Option<Self> {
        Some(Self {
            token_id: top.token_id.to_string(),
            bid: top.bid?,
            ask: top.ask?,
            bid_sz: top.bid_sz,
            ask_sz: top.ask_sz,
            ts_ms: top.ts_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CtfSplitEntryContext {
    pub market_id: String,
    pub condition_id: String,
    pub gamma_market_id: String,
    pub market_underlying: String,
    pub entry_oracle_symbol: String,
    pub up_token: String,
    pub down_token: String,
    pub qty: f64,
    pub amount_units_6dp: u128,
    pub entry_raw_target_gap_qty: Option<f64>,
    pub entry_min_quote_qty: Option<f64>,
    pub entry_market_budget_share_cap: Option<f64>,
    pub entry_total_inventory_share_cap: Option<f64>,
    pub entry_adverse_sized_qty: Option<f64>,
    pub entry_liquidity_manufacturer_base_qty: Option<f64>,
    pub entry_liquidity_manufacturer_manufactured_qty: Option<f64>,
    pub entry_liquidity_manufacturer_cap_qty: Option<f64>,
    pub p_up: Option<f64>,
    pub model_p_up_raw: Option<f64>,
    pub model_p_up: Option<f64>,
    pub market_p_up: Option<f64>,
    pub base_p_fair: Option<f64>,
    pub p_fair: Option<f64>,
    pub calibration_confidence: Option<f64>,
    pub calibration_center: Option<f64>,
    pub calibration_market_center_weight: Option<f64>,
    pub calibration_spread_quality: Option<f64>,
    pub calibration_depth_quality: Option<f64>,
    pub beta_calibration_enabled: bool,
    pub beta_calibration_input_p: Option<f64>,
    pub beta_calibration_p_fair: Option<f64>,
    pub beta_calibration_a: Option<f64>,
    pub beta_calibration_b: Option<f64>,
    pub beta_calibration_c: Option<f64>,
    pub beta_calibration_sample_count: Option<u32>,
    pub beta_calibration_positive_count: Option<u32>,
    pub beta_calibration_brier_score: Option<f64>,
    pub beta_calibration_log_loss: Option<f64>,
    pub beta_calibration_uncertainty: Option<f64>,
    pub market_raw_p_up: Option<f64>,
    pub market_calibration_enabled: bool,
    pub market_calibration_a: Option<f64>,
    pub market_calibration_b: Option<f64>,
    pub market_calibration_c: Option<f64>,
    pub market_calibration_sample_count: Option<u32>,
    pub market_calibration_positive_count: Option<u32>,
    pub market_calibration_brier_score: Option<f64>,
    pub market_calibration_log_loss: Option<f64>,
    pub market_calibration_uncertainty: Option<f64>,
    pub model_weight: Option<f64>,
    pub depth_pressure: Option<f64>,
    pub micro_queue_imbalance_l1: Option<f64>,
    pub micro_queue_imbalance_l5: Option<f64>,
    pub micro_ofi_depth: Option<f64>,
    pub micro_microprice_gap_ticks: Option<f64>,
    pub micro_signed_return_1s: Option<f64>,
    pub micro_signed_return_5s: Option<f64>,
    pub micro_volatility_state: Option<f64>,
    pub micro_jump_proxy: Option<f64>,
    pub micro_periodic_hour_sin: Option<f64>,
    pub micro_periodic_hour_cos: Option<f64>,
    pub micro_periodic_minute_sin: Option<f64>,
    pub micro_periodic_minute_cos: Option<f64>,
    pub micro_feature_availability: Option<f64>,
    pub micro_logistic_enabled: bool,
    pub micro_logistic_p_up: Option<f64>,
    pub micro_logistic_logit: Option<f64>,
    pub micro_logistic_feature_count: Option<u32>,
    pub micro_logistic_l2_penalty: Option<f64>,
    pub micro_logistic_fit_sample_count: Option<u32>,
    pub micro_logistic_fit_positive_count: Option<u32>,
    pub micro_logistic_fit_residual_trust_alpha: Option<f64>,
    pub micro_logistic_fit_brier_score: Option<f64>,
    pub micro_logistic_fit_log_loss: Option<f64>,
    pub micro_logistic_fit_prior_brier_score: Option<f64>,
    pub micro_logistic_fit_prior_log_loss: Option<f64>,
    pub micro_logistic_fit_weight_norm: Option<f64>,
    pub micro_logistic_fit_weights: Option<[f64; 8]>,
    pub unwind_up: Option<f64>,
    pub unwind_down: Option<f64>,
    pub adv_up: Option<f64>,
    pub adv_down: Option<f64>,
    pub adv_up_ewma: Option<f64>,
    pub adv_down_ewma: Option<f64>,
    pub uncertainty_edge_threshold: Option<f64>,
    pub uncertainty_edge_weight: Option<f64>,
    pub theta_enter_up: Option<f64>,
    pub theta_enter_down: Option<f64>,
    pub oracle_side_confidence: Option<f64>,
    pub market_confirmation: Option<f64>,
    pub market_confirmation_probability_agreement: Option<f64>,
    pub market_confirmation_spread_quality: Option<f64>,
    pub market_confirmation_depth_support: Option<f64>,
    pub market_confirmation_book_complete: Option<f64>,
    pub shock_toxicity: Option<f64>,
    pub markout_toxicity: Option<f64>,
    pub ta_alignment: Option<f64>,
    pub ta_ema_trend: Option<f64>,
    pub ta_macd_trend: Option<f64>,
    pub ta_rsi_regime: Option<f64>,
    pub ta_bollinger_position: Option<f64>,
    pub ta_recent_return_sign: Option<f64>,
    pub ta_drift_score: Option<f64>,
    pub oracle_side_reason: Option<String>,
    pub top_up_bid_delta: Option<f64>,
    pub top_down_bid_delta: Option<f64>,
    pub up_book_top: CtfSplitBookTop,
    pub down_book_top: CtfSplitBookTop,
    pub decision_ts_ms: u64,
}

#[derive(Debug, Clone)]
pub enum StrategyDecisionContext {
    CtfSplitEntry(CtfSplitEntryContext),
    CtfOwnedLegSell(crate::strategy::facts::CtfOwnedLegOutcomeFact),
}

#[derive(Debug, Clone)]
pub struct CancelMarketRequest {
    pub market_id: MarketId,
    pub gamma_market_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CancelQuotesRequest {
    pub market_id: Option<MarketId>,
    pub quote_group: String,
    pub side: Option<Side>,
}

#[derive(Debug, Clone)]
pub enum CancelRequest {
    Market(CancelMarketRequest),
    Quotes(CancelQuotesRequest),
}

#[derive(Debug, Clone)]
pub enum StrategyAction {
    Order(OrderIntent),
    Inventory(InventoryIntent),
    CancelMarket(CancelMarketRequest),
    CancelQuotes(CancelQuotesRequest),
}

#[derive(Debug, Clone)]
pub struct StrategyDecision {
    pub action: StrategyAction,
    pub context: Option<StrategyDecisionContext>,
    pub p_up: Option<f64>,
    pub edge: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct StrategyTickOutcome {
    pub decisions: Vec<StrategyDecision>,
    pub facts: Vec<crate::strategy::facts::StrategyFact>,
    pub status: Option<crate::observability::strategy_status::StrategyStatus>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StrategyGateDropCounters {
    pub oracle: u64,
    pub book: u64,
    pub stale: u64,
    pub open_delay: u64,
    pub open_price: u64,
    pub entry_time: u64,
    pub blocked_market: u64,
    pub markout_toxicity: u64,
}

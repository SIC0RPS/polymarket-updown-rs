use std::collections::HashMap;

use crate::observability::strategy_status::StrategySelectionStatus;
use crate::strategy::{StrategyDecision, StrategyGateDropCounters};

#[derive(Debug, Clone)]
pub struct RankedMarketRuntimeView {
    pub oracle_symbol: String,
    pub up_token: String,
    pub down_token: String,
    pub toxicity: f64,
    pub markout_toxicity: f64,
}

#[derive(Debug, Clone)]
pub struct StrategyRecomputeCycleResult {
    pub decisions: Vec<StrategyDecision>,
    pub ranked: Vec<RankedMarketRuntimeView>,
    pub paired_managed_exit_count: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyRuntimeSnapshot {
    pub pending_len: usize,
    pub gate_drops_cycle: StrategyGateDropCounters,
    pub gate_drops_total: StrategyGateDropCounters,
    pub selection_status: Option<StrategySelectionStatus>,
    pub quote_block_reasons_cycle: HashMap<&'static str, u32>,
    pub quote_block_detail_cycle: Option<(&'static str, String)>,
}

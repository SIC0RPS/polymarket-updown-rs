use std::collections::{HashMap, VecDeque};

use bbwick_engine::state::Fingerprint;

use crate::observability::action::ActionEvent;

#[derive(Debug, Default, Clone)]
pub struct StrategyRuntimeControl {
    pub invalid_tick_since_ms: HashMap<String, u64>,
    pub last_invalid_tick_input_sig: Option<Fingerprint>,
    pub fatal_tick_errors: u64,
    pub tick_kill_switch: bool,
    pub last_book_trigger_sig: Option<Fingerprint>,
    pub next_book_trigger_deadline_ms: Option<u64>,
    pub last_oracle_trigger_sig: Option<u64>,
    pub last_portfolio_update_sig: Option<(u64, Fingerprint)>,
    pub last_risk_update_sig: Option<u64>,
    pub last_open_orders_update_sig: Option<(u64, Fingerprint)>,
    pub last_blocked_markets_sig: Option<Fingerprint>,
    pub cached_blocked_markets_sig: Option<Fingerprint>,
    pub last_paired_exit_candidate_sig: Option<u64>,
    pub next_recompute_deadline_ms: Option<u64>,
    pub deferred_rebalance_deadline_ms: Option<u64>,
    pub last_rebalance_ms: u64,
}

#[derive(Debug, Default, Clone)]
pub struct StrategyRuntimeState {
    pub pending_action_events: VecDeque<ActionEvent>,
    pub control: StrategyRuntimeControl,
}

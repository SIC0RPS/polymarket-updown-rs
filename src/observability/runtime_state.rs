use super::*;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct RuntimeState {
    pub started_at_ms: u64,
    pub mode: String,
    pub market_data_source: String,
    pub execution_backend: String,
    pub oracle_runtime: Option<crate::observability::ui::OracleRuntimeStatusSnapshot>,
    pub last_strategy_status: Option<StrategyStatusSnapshot>,
    pub last_error: Option<ErrorSnapshot>,
    pub post_only_reject_status: Option<PostOnlyRejectStatusSnapshot>,
    pub portfolio: crate::portfolio::PortfolioState,
    pub open_orders: crate::orders::OpenOrdersState,
    pub runtime_managed_paired_tokens: HashSet<String>,
    pub risk: crate::risk::RiskSnapshot,
}

impl RuntimeState {
    pub fn new(started_at_ms: u64, mode: String) -> Self {
        Self {
            started_at_ms,
            mode,
            market_data_source: "-".to_string(),
            execution_backend: "-".to_string(),
            oracle_runtime: None,
            last_strategy_status: None,
            last_error: None,
            post_only_reject_status: None,
            portfolio: crate::portfolio::PortfolioState::default(),
            open_orders: crate::orders::OpenOrdersState::default(),
            runtime_managed_paired_tokens: HashSet::new(),
            risk: crate::risk::RiskSnapshot::default(),
        }
    }
}

impl Observability {
    pub(crate) fn apply_to_runtime_state(&self, event: &ActionEvent) {
        self.runtime_state_tx.send_modify(|state| match event {
            ActionEvent::OracleRuntimeStatus {
                ts_ms,
                phase,
                connected,
                required_symbols,
                ready_symbols,
                symbol_last_tick_ts_ms,
                symbol_last_tick_age_ms,
                last_error,
            } => {
                state.oracle_runtime =
                    Some(crate::observability::ui::OracleRuntimeStatusSnapshot {
                        ts_ms: *ts_ms,
                        phase: phase.clone(),
                        connected: *connected,
                        required_symbols: required_symbols.clone(),
                        ready_symbols: ready_symbols.clone(),
                        symbol_last_tick_ts_ms: symbol_last_tick_ts_ms.clone(),
                        symbol_last_tick_age_ms: symbol_last_tick_age_ms.clone(),
                        last_error: last_error.clone(),
                    });
            }
            ActionEvent::StrategyStatus {
                ts_ms,
                kind,
                status,
                p_up,
                edge,
                gate_drop_oracle,
                gate_drop_book,
                gate_drop_stale,
                gate_drop_open_delay,
                gate_drop_open_price,
                gate_drop_entry_time,
                gate_drop_blocked_market,
                gate_drop_markout_toxicity,
                gate_drop_oracle_total,
                gate_drop_book_total,
                gate_drop_stale_total,
                gate_drop_open_delay_total,
                gate_drop_open_price_total,
                gate_drop_entry_time_total,
                gate_drop_blocked_market_total,
                gate_drop_markout_toxicity_total,
                selection_status,
                activity,
                recompute_reason,
                queued,
                drained,
                ranked,
                managed_exits,
                quote_blocks,
                primary_block_detail,
                shock_toxicity,
                markout_toxicity,
                markout_samples,
                oracle_wait_reason,
            } => {
                state.last_strategy_status = Some(StrategyStatusSnapshot {
                    ts_ms: *ts_ms,
                    kind: kind.clone(),
                    status: status.clone(),
                    p_up: *p_up,
                    edge: *edge,
                    gate_drop_oracle: *gate_drop_oracle,
                    gate_drop_book: *gate_drop_book,
                    gate_drop_stale: *gate_drop_stale,
                    gate_drop_open_delay: *gate_drop_open_delay,
                    gate_drop_open_price: *gate_drop_open_price,
                    gate_drop_entry_time: *gate_drop_entry_time,
                    gate_drop_blocked_market: *gate_drop_blocked_market,
                    gate_drop_markout_toxicity: *gate_drop_markout_toxicity,
                    gate_drop_oracle_total: *gate_drop_oracle_total,
                    gate_drop_book_total: *gate_drop_book_total,
                    gate_drop_stale_total: *gate_drop_stale_total,
                    gate_drop_open_delay_total: *gate_drop_open_delay_total,
                    gate_drop_open_price_total: *gate_drop_open_price_total,
                    gate_drop_entry_time_total: *gate_drop_entry_time_total,
                    gate_drop_blocked_market_total: *gate_drop_blocked_market_total,
                    gate_drop_markout_toxicity_total: *gate_drop_markout_toxicity_total,
                    selection_status: *selection_status,
                    activity: *activity,
                    recompute_reason: *recompute_reason,
                    queued: *queued,
                    drained: *drained,
                    ranked: *ranked,
                    managed_exits: *managed_exits,
                    quote_blocks: quote_blocks.clone(),
                    primary_block_detail: primary_block_detail.clone(),
                    shock_toxicity: *shock_toxicity,
                    markout_toxicity: *markout_toxicity,
                    markout_samples: *markout_samples,
                    oracle_wait_reason: *oracle_wait_reason,
                });
            }
            ActionEvent::RuntimeIssue {
                ts_ms,
                scope,
                kind,
                msg,
                detail,
            } => {
                state.last_error = Some(ErrorSnapshot {
                    ts_ms: *ts_ms,
                    scope: scope.clone(),
                    msg: msg.clone().unwrap_or_else(|| format!("{kind:?}")),
                    kind: Some(*kind),
                    detail: detail.clone(),
                });
            }
            ActionEvent::ReclaimResolved { .. } => {
                state.last_error = None;
            }
            _ => {}
        });
    }
}

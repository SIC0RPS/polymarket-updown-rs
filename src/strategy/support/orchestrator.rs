// Strategy support required for behavior: recompute cycle orchestration around
// the paired-maker strategy. Runtime owns the outer tick/recompute/status loop.
use std::collections::HashSet;

use crate::polymarket::state::MarketProjection;
use crate::strategy::support::oracle_authority::{
    OracleAuthorityMode, OracleAuthoritySnapshot, OracleSideReason,
};
use crate::strategy::support::shared::common::now_ms;
use crate::strategy::StrategyGateDropCounters;
use crate::strategy::{
    InventoryOperation, MakerRebateMmStrategy, StrategyAction, StrategyDecision,
    StrategyDecisionContext, StrategyFactSink,
};
use crate::types::{MarketTick, Side};

impl MakerRebateMmStrategy {
    pub(crate) fn refresh_shared_cfg(&mut self) {
        let cfg = self.shared_cfg.borrow().clone();
        if cfg.enabled_assets != self.cfg.enabled_assets {
            self.pending.retain(|decision| match &decision.action {
                StrategyAction::Order(intent) if intent.side() == Side::Buy => intent
                    .buy_authorization()
                    .is_none_or(|authorization| cfg.is_asset_enabled(authorization.symbol.as_str())),
                StrategyAction::Inventory(intent)
                    if intent.operation == InventoryOperation::SplitCompleteSet =>
                {
                    match &decision.context {
                        Some(StrategyDecisionContext::CtfSplitEntry(context)) => {
                            cfg.is_asset_enabled(&context.market_underlying)
                        }
                        _ => true,
                    }
                }
                _ => true,
            });
        }
        self.micro_logistic_trainer
            .set_l2_penalty(cfg.live_fair_value_model.micro_logistic_l2_penalty);
        self.cfg = cfg;
    }

    pub(crate) fn has_pending_decisions(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(crate) fn clear_pending_decisions(&mut self) {
        self.pending.clear();
    }

    pub(crate) fn prepend_pending_decisions(&mut self, decisions: Vec<StrategyDecision>) {
        for decision in decisions.into_iter().rev() {
            self.pending.push_front(decision);
        }
    }

    pub(crate) fn drain_pending_decisions(
        &mut self,
        max_decisions: usize,
    ) -> Vec<StrategyDecision> {
        let mut drained = Vec::with_capacity(max_decisions.min(self.pending.len()));
        for _ in 0..max_decisions {
            let Some(next) = self.pending.pop_front() else {
                break;
            };
            drained.push(next);
        }
        drained
    }

    pub(crate) fn runtime_snapshot_parts(
        &self,
    ) -> (
        usize,
        StrategyGateDropCounters,
        StrategyGateDropCounters,
        Option<crate::observability::strategy_status::StrategySelectionStatus>,
        std::collections::HashMap<&'static str, u32>,
        Option<(&'static str, String)>,
    ) {
        (
            self.pending.len(),
            self.gate_drops_cycle,
            self.gate_drops_total,
            self.last_selection_status,
            self.quote_block_reasons_cycle.clone(),
            self.quote_block_detail_cycle
                .as_ref()
                .map(|(reason, detail)| (*reason, detail.clone())),
        )
    }

    pub(crate) fn run_recompute_cycle(
        &mut self,
        fact_sink: &mut StrategyFactSink,
        now_ms: u64,
        market_projection: &MarketProjection,
    ) -> (
        Vec<StrategyDecision>,
        Vec<(String, String, String, f64, f64)>,
        usize,
    ) {
        self.reset_quote_block_reasons();
        self.pending.clear();
        self.process_fills_and_markouts(now_ms);
        self.observe_active_market_oracle_authority(now_ms, market_projection);
        self.materialize_paired_authority_if_needed(now_ms, market_projection);
        let protected_market_ids =
            self.queue_protected_inventory_market_cancels(market_projection, now_ms);
        let paired_managed_exit_market_ids =
            self.queue_paired_managed_sells(fact_sink, now_ms, market_projection);
        let (ranked, ctf_entry_candidates) =
            self.rank_markets_with_projection(now_ms, market_projection);

        if !(ctf_entry_candidates.is_empty() && self.pending.is_empty()) {
            let blocked_quote_markets = paired_managed_exit_market_ids
                .iter()
                .chain(protected_market_ids.iter())
                .cloned()
                .collect::<HashSet<_>>();
            self.queue_market_quotes_filtered(
                &ctf_entry_candidates,
                now_ms,
                &blocked_quote_markets,
            );
        }

        let decisions = self.drain_pending_decisions(usize::MAX);
        let ranked = ranked
            .into_iter()
            .map(|market| {
                (
                    market.oracle_symbol().to_string(),
                    market.selection().up_token_id().to_string(),
                    market.selection().down_token_id().to_string(),
                    market.toxicity(),
                    market.markout_toxicity(),
                )
            })
            .collect();

        (decisions, ranked, paired_managed_exit_market_ids.len())
    }

    fn observe_active_market_oracle_authority(
        &mut self,
        now_ms: u64,
        market_projection: &MarketProjection,
    ) {
        let base_cfg = self.cfg.clone();
        let active_markets = market_projection
            .markets()
            .iter()
            .filter(|market| market.window_start_ms <= now_ms && now_ms < market.window_end_ms)
            .cloned()
            .collect::<Vec<_>>();
        for market in active_markets {
            self.cfg = base_cfg.config_for_symbol(&market.underlying).clone();
            let previous = self
                .oracle_authority_by_market
                .get(&market.market_id)
                .copied();
            let previous_side = previous.and_then(|snapshot| snapshot.oracle_signal_side);
            let books = {
                let odds = self.odds.as_ref();
                self.find_books(&odds, &market).map(|(up, down)| {
                    let (up_depth, down_depth) = self.current_depths_for_market(&odds, &market);
                    (up, down, up_depth, down_depth)
                })
            };
            let snapshot = match books {
                Some((up, down, up_depth, down_depth)) => self.build_oracle_authority_snapshot(
                    &market,
                    &market.market_id,
                    &up,
                    &down,
                    up_depth.as_ref(),
                    down_depth.as_ref(),
                    previous,
                    previous_side,
                    OracleAuthorityMode::Market,
                    now_ms,
                ),
                None => OracleAuthoritySnapshot {
                    controller_mode: OracleAuthorityMode::Market,
                    oracle_side_reason: Some(OracleSideReason::NoMarketData),
                    oracle_data_issue: Some("missing_market_books"),
                    ..Default::default()
                },
            };
            self.oracle_authority_by_market
                .insert(market.market_id.clone(), snapshot);
        }

        self.cfg = base_cfg;
    }

    pub(crate) fn apply_tick_and_get_projection(
        &mut self,
        tick: &MarketTick,
    ) -> Option<(u64, MarketProjection)> {
        self.refresh_shared_cfg();
        let now = crate::strategy::support::shared::common::ts_ms_from_ns(tick.ts_ns)
            .unwrap_or_else(now_ms);
        let market_projection = self.market_projection.clone();
        self.apply_tick_update(tick, now, &market_projection)?;
        Some((now, market_projection))
    }
}

// Strategy support required for behavior: market selection and ordering before
// the paired-maker contract decides whether to enter.
mod feasibility;

use super::*;
use crate::strategy::fair_value::live_market_penalty_surface;

#[derive(Debug, Clone)]
pub(crate) struct RankedMarket {
    pub(in crate::strategy) selection: MarketSelection,
    pub(in crate::strategy) oracle_symbol: String,
    pub(in crate::strategy) toxicity: f64,
    pub(in crate::strategy) markout_toxicity: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct CtfEntryCandidate {
    pub(in crate::strategy) selection: MarketSelection,
    pub(in crate::strategy) oracle_symbol: String,
    pub(in crate::strategy) up: BookTop,
    pub(in crate::strategy) down: BookTop,
}

impl RankedMarket {
    pub(crate) fn selection(&self) -> &MarketSelection {
        &self.selection
    }

    pub(crate) fn oracle_symbol(&self) -> &str {
        &self.oracle_symbol
    }

    pub(crate) fn toxicity(&self) -> f64 {
        self.toxicity
    }

    pub(crate) fn markout_toxicity(&self) -> f64 {
        self.markout_toxicity
    }
}

impl MakerRebateMmStrategy {
    pub(crate) fn rank_markets_with_projection(
        &mut self,
        now_ms: u64,
        market_projection: &MarketProjection,
    ) -> (Vec<RankedMarket>, Vec<CtfEntryCandidate>) {
        let portfolio_snapshot = self.portfolio_rx.borrow().clone();
        let (markets, next_selection_time_to_open_ms) = {
            let markets = market_projection
                .markets()
                .iter()
                .filter(|market| market.window_end_ms > now_ms)
                .filter(|market| self.cfg.is_asset_enabled(&market.underlying))
                .cloned()
                .collect::<Vec<_>>();
            let next_selection_time_to_open_ms =
                market_projection.next_selection().and_then(|selection| {
                    (selection.window_start_ms > now_ms)
                        .then_some(selection.window_start_ms.saturating_sub(now_ms))
                });
            (markets, next_selection_time_to_open_ms)
        };
        let known_markets = markets.len() as u64;
        let active_now: Vec<MarketSelection> = markets
            .iter()
            .filter(|m| m.window_start_ms <= now_ms && now_ms < m.window_end_ms)
            .cloned()
            .collect();
        let active_now_markets = active_now.len() as u64;
        let mut active_now_budget_deadzone_markets = 0u64;
        let active_now_actionable_markets = active_now
            .iter()
            .filter(|market| {
                let has_management = self.market_has_live_quotes_or_inventory(market);
                if has_management {
                    return true;
                }
                if self.market_is_blocked_for_entry(market) {
                    return false;
                }
                let Some((up, down)) = ({
                    let odds = self.odds.as_ref();
                    self.find_books(&odds, market)
                }) else {
                    return false;
                };
                let actionable =
                    self.market_has_feasible_fresh_entry(&portfolio_snapshot, market, &up, &down);
                if !actionable {
                    active_now_budget_deadzone_markets =
                        active_now_budget_deadzone_markets.saturating_add(1);
                }
                actionable
            })
            .count() as u64;
        let active_now_alternative_markets = active_now_markets.saturating_sub(1);
        let active_now_only_filter_applied = active_now.len() < markets.len();

        let mut out: Vec<RankedMarket> = Vec::new();
        let mut ctf_entry_candidates = Vec::new();
        let mut fair_tokens = HashSet::new();
        let mut cycle_gate_drops = StrategyGateDropCounters::default();
        let mut ranked_budget_deadzone_markets = 0u64;

        let base_cfg = self.cfg.clone();
        for market in active_now {
            self.cfg = base_cfg.config_for_symbol(&market.underlying).clone();
            let observation = {
                let odds = self.odds.as_ref();
                self.find_books(&odds, &market).map(|(up, down)| {
                    let (up_depth, down_depth) = self.current_depths_for_market(&odds, &market);
                    (up, down, up_depth, down_depth)
                })
            };
            let (up, down, up_depth, down_depth) = match observation {
                Some(observation) => observation,
                None => {
                    cycle_gate_drops.book = cycle_gate_drops.book.saturating_add(1);
                    continue;
                }
            };
            let penalty_surface = live_market_penalty_surface(
                now_ms,
                up.observed_at_ms,
                down.observed_at_ms,
                market.window_end_ms,
                self.cfg.ranking_book_max_age_ms,
                self.cfg.hard_entry_gate_ms(),
            );
            if penalty_surface.hard_drop_stale {
                cycle_gate_drops.stale = cycle_gate_drops.stale.saturating_add(1);
                continue;
            }
            let up_token = market.up_token_id();
            let down_token = market.down_token_id();
            let up_inv = portfolio_snapshot
                .position(up_token)
                .map(|p| p.qty)
                .unwrap_or(0.0);
            let down_inv = portfolio_snapshot
                .position(down_token)
                .map(|p| p.qty)
                .unwrap_or(0.0);
            let has_inventory_to_manage =
                crate::polymarket::fixed_math::has_fixed_6dp_units(up_inv.abs())
                    || crate::polymarket::fixed_math::has_fixed_6dp_units(down_inv.abs());
            if self.market_is_blocked_for_entry(&market) && !has_inventory_to_manage {
                cycle_gate_drops.blocked_market = cycle_gate_drops.blocked_market.saturating_add(1);
                continue;
            }
            if !has_inventory_to_manage
                && !self.market_has_feasible_fresh_entry(&portfolio_snapshot, &market, &up, &down)
            {
                ranked_budget_deadzone_markets = ranked_budget_deadzone_markets.saturating_add(1);
                continue;
            }
            ctf_entry_candidates.push(CtfEntryCandidate {
                selection: market.clone(),
                oracle_symbol: self.oracle_symbol_for_underlying(&market.underlying),
                up: up.clone(),
                down: down.clone(),
            });
            let (oracle_symbol, ts_ms, oracle_px, last_return_bps) =
                match self.market_oracle_input(&market, now_ms) {
                    Some((oracle_symbol, ts_ms, oracle_px, _, last_return_bps)) => {
                        (oracle_symbol, ts_ms, oracle_px, last_return_bps)
                    }
                    None => {
                        cycle_gate_drops.oracle = cycle_gate_drops.oracle.saturating_add(1);
                        continue;
                    }
                };
            let Some(open_ctx) = self.upsert_window_open_px(&market, ts_ms, oracle_px) else {
                cycle_gate_drops.open_price = cycle_gate_drops.open_price.saturating_add(1);
                continue;
            };
            if open_ctx.open_delay_ms > OPEN_PX_MAX_DELAY_MS {
                cycle_gate_drops.open_delay = cycle_gate_drops.open_delay.saturating_add(1);
            }

            let late_toxicity_penalty =
                OPEN_PX_LATE_MAX_TOXICITY_PENALTY * open_ctx.open_bracket_penalty;
            let stale_toxicity_penalty = penalty_surface.stale_soft_penalty;
            let close_toxicity_penalty = penalty_surface.close_window_penalty;
            let shock_toxicity = Self::shock_toxicity(last_return_bps, self.cfg.oracle_shock_bps)
                + late_toxicity_penalty
                + stale_toxicity_penalty
                + close_toxicity_penalty;

            let markout_toxicity = self
                .markout_toxicity_at(market.up_token_id(), now_ms)
                .max(self.markout_toxicity_at(market.down_token_id(), now_ms));
            let p_up = self
                .market_calibrated_live_fair_result(
                    &market,
                    &up,
                    &down,
                    up_depth.as_ref(),
                    down_depth.as_ref(),
                    now_ms,
                )
                .map(|(_, live_fair, _)| live_fair.p_fair);
            if let Some(p_up) = p_up {
                let up_token = market.up_token_id.clone();
                let down_token = market.down_token_id.clone();
                self.last_fair_by_token.insert(up_token.clone(), p_up);
                self.last_fair_by_token
                    .insert(down_token.clone(), 1.0 - p_up);
                fair_tokens.insert(up_token);
                fair_tokens.insert(down_token);
            }
            out.push(RankedMarket {
                selection: market,
                oracle_symbol,
                toxicity: shock_toxicity,
                markout_toxicity,
            });
        }

        self.cfg = base_cfg;
        self.last_fair_by_token
            .retain(|token, _| fair_tokens.contains(token));
        self.gate_drops_cycle = cycle_gate_drops;
        self.gate_drops_total
            .saturating_add_assign(cycle_gate_drops);
        self.last_selection_status = Some(StrategySelectionStatus {
            known_markets,
            active_now_markets,
            active_now_alternative_markets,
            active_now_actionable_markets,
            active_now_budget_deadzone_markets,
            ranked_markets: out.len() as u64,
            ranked_budget_deadzone_markets,
            entry_time_blocked_active_markets: cycle_gate_drops.entry_time,
            blocked_market_count: cycle_gate_drops.blocked_market,
            preopen_next_selection_present: next_selection_time_to_open_ms.is_some(),
            active_now_only_filter_applied,
            next_selection_time_to_open_ms,
        });
        (out, ctf_entry_candidates)
    }
}

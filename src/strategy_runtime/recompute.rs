use std::hash::{Hash, Hasher};

use bbwick_engine::state::Fingerprint;

use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::strategy::paired_maker::{
    MakerRebateMmStrategy, BOOK_STALE_HARD_DROP_MS, BOOK_STALE_SOFT_MAX_MS,
    INVALID_TICK_KILL_AFTER_MS, OPEN_PX_MAX_DELAY_MS,
};
use crate::strategy::support::shared::common::{ingest_age_ms, is_valid_tick_size};
use crate::strategy_runtime::state::StrategyRuntimeControl;
use bbwick_engine::state::fingerprint_hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteRecomputeReason {
    Initial,
    OracleChanged,
    PortfolioStateChanged,
    OpenOrdersChanged,
    RiskStateChanged,
    BlockedMarketsChanged,
    ThresholdFired,
    BookChanged,
}

impl QuoteRecomputeReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::OracleChanged => "oracle_changed",
            Self::PortfolioStateChanged => "portfolio_state_changed",
            Self::OpenOrdersChanged => "open_orders_changed",
            Self::RiskStateChanged => "risk_state_changed",
            Self::BlockedMarketsChanged => "blocked_markets_changed",
            Self::ThresholdFired => "threshold_fired",
            Self::BookChanged => "book_changed",
        }
    }

    pub(crate) fn bypass_rebalance_interval(self) -> bool {
        !matches!(self, Self::OracleChanged | Self::BookChanged)
    }
}

pub fn update_invalid_tick_control(
    strategy: &MakerRebateMmStrategy,
    runtime_control: &mut StrategyRuntimeControl,
    market_projection: &MarketProjection,
    now_ms: u64,
) {
    let odds = strategy.odds.as_ref();
    let mut input_sig = Fingerprint::default();
    for market in market_projection.markets() {
        for token in [market.up_token, market.down_token] {
            let tick_size_bits = crate::polymarket::state::book_for_token(&odds, token)
                .map(|book| book.tick_size.to_bits());
            input_sig.insert_hash(fingerprint_hash(&(token, tick_size_bits)));
        }
    }
    if runtime_control.last_invalid_tick_input_sig == Some(input_sig) {
        if runtime_control.invalid_tick_since_ms.is_empty() {
            return;
        }
        for since_ms in runtime_control.invalid_tick_since_ms.values() {
            runtime_control.fatal_tick_errors = runtime_control.fatal_tick_errors.saturating_add(1);
            if now_ms.saturating_sub(*since_ms) >= INVALID_TICK_KILL_AFTER_MS {
                runtime_control.tick_kill_switch = true;
            }
        }
        return;
    }
    runtime_control.last_invalid_tick_input_sig = Some(input_sig);

    let mut seen_tokens = std::collections::HashSet::new();

    for market in market_projection.markets() {
        for token in [market.up_token, market.down_token] {
            let Some(book) = crate::polymarket::state::book_for_token(&odds, token) else {
                continue;
            };
            let token_id = token.to_string();
            seen_tokens.insert(token_id.clone());
            if book.tick_size.is_finite()
                && book.tick_size > 0.0
                && is_valid_tick_size(book.tick_size)
            {
                runtime_control.invalid_tick_since_ms.remove(&token_id);
                continue;
            }
            let since = *runtime_control
                .invalid_tick_since_ms
                .entry(token_id)
                .or_insert(now_ms);
            let age_ms = now_ms.saturating_sub(since);
            runtime_control.fatal_tick_errors = runtime_control.fatal_tick_errors.saturating_add(1);
            if age_ms >= INVALID_TICK_KILL_AFTER_MS {
                runtime_control.tick_kill_switch = true;
            }
        }
    }

    runtime_control
        .invalid_tick_since_ms
        .retain(|token, _| seen_tokens.contains(token));
}

pub fn rebalance_gate_elapsed(
    strategy: &MakerRebateMmStrategy,
    runtime_control: &StrategyRuntimeControl,
    now_ms: u64,
) -> bool {
    let rebalance_ms = rebalance_interval_ms(strategy);
    now_ms.saturating_sub(runtime_control.last_rebalance_ms) >= rebalance_ms
}

fn rebalance_interval_ms(strategy: &MakerRebateMmStrategy) -> u64 {
    if !strategy.pending_markouts.is_empty()
        || strategy.has_pending_decisions()
        || strategy.gate_drops_cycle.book <= 1
    {
        strategy.cfg.fast_rebalance_interval_ms
    } else {
        strategy.cfg.normal_rebalance_interval_ms
    }
}

pub fn defer_rebalance_until_cadence(
    strategy: &MakerRebateMmStrategy,
    runtime_control: &mut StrategyRuntimeControl,
) {
    let deadline_ms = runtime_control
        .last_rebalance_ms
        .saturating_add(rebalance_interval_ms(strategy));
    runtime_control.deferred_rebalance_deadline_ms = Some(deadline_ms);
}

pub fn current_portfolio_update_sig(strategy: &MakerRebateMmStrategy) -> (u64, Fingerprint) {
    let portfolio = strategy.portfolio_rx.borrow();
    let mut fingerprint = portfolio.positions_fingerprint;
    for (token, position) in &portfolio.positions {
        if let Some(basis) = position.remaining_basis_units_6dp {
            fingerprint.insert_hash(fingerprint_hash(&(token.as_str(), basis)));
        }
    }
    (portfolio.revision, fingerprint)
}

pub fn current_oracle_update_sig(strategy: &MakerRebateMmStrategy) -> u64 {
    let state = &strategy.oracle_state;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (
        strategy.oracle_symbol.as_str(),
        state.latest_px.unwrap_or(0.0).to_bits(),
        state.sigma2.to_bits(),
        state.latest_ts_ms.unwrap_or(0),
        state.latest_top.map(|top| {
            (
                top.bid.to_bits(),
                top.ask.to_bits(),
                top.bid_sz.to_bits(),
                top.ask_sz.to_bits(),
            )
        }),
        state.previous_top.map(|top| {
            (
                top.bid.to_bits(),
                top.ask.to_bits(),
                top.bid_sz.to_bits(),
                top.ask_sz.to_bits(),
            )
        }),
        state.ofi_fingerprint(),
    )
        .hash(&mut hasher);
    hasher.finish()
}

pub fn current_risk_update_sig(strategy: &MakerRebateMmStrategy) -> Option<u64> {
    let risk = strategy.risk_rx.as_ref()?.borrow();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    risk.enabled.hash(&mut hasher);
    risk.trading_allowed.hash(&mut hasher);
    risk.frozen_rule.hash(&mut hasher);
    risk.frozen_details.hash(&mut hasher);
    risk.available_buy_budget_usdt.hash(&mut hasher);
    risk.max_entry_notional_usdt.hash(&mut hasher);
    risk.max_order_qty.hash(&mut hasher);
    risk.max_inventory_total.hash(&mut hasher);
    risk.remaining_total_inventory_qty.hash(&mut hasher);
    let mut market_budgets = risk.market_budgets.values().collect::<Vec<_>>();
    market_budgets.sort_unstable_by(|left, right| left.market_id.cmp(&right.market_id));
    for budget in market_budgets {
        budget.market_id.hash(&mut hasher);
        budget.effective_market_cap_notional.hash(&mut hasher);
        budget.remaining_market_headroom_usdt.hash(&mut hasher);
        budget.binding_rule.hash(&mut hasher);
    }
    Some(hasher.finish())
}

pub fn current_open_orders_update_sig(
    strategy: &MakerRebateMmStrategy,
) -> Option<(u64, Fingerprint)> {
    let open_orders = strategy.open_orders_rx.as_ref()?.borrow();
    Some((open_orders.revision, open_orders.orders_fingerprint))
}

pub fn current_blocked_markets_sig(
    strategy: &mut MakerRebateMmStrategy,
    runtime_control: &mut StrategyRuntimeControl,
) -> Option<Fingerprint> {
    let cached_is_none = runtime_control.cached_blocked_markets_sig.is_none();
    let rx = strategy.blocked_markets_rx.as_mut()?;
    let has_changed = rx.has_changed().unwrap_or(true);
    let refreshed = if cached_is_none || has_changed {
        let blocked = rx.borrow_and_update();
        let mut fingerprint = Fingerprint::default();
        for market_id in blocked.iter() {
            fingerprint.insert_hash(fingerprint_hash(market_id));
        }
        Some(fingerprint)
    } else {
        None
    };
    if let Some(fingerprint) = refreshed {
        runtime_control.cached_blocked_markets_sig = Some(fingerprint);
    }
    runtime_control.cached_blocked_markets_sig
}

fn current_book_trigger_deadline_ms(
    strategy: &MakerRebateMmStrategy,
    market_projection: &MarketProjection,
    odds: &PolymarketOddsState,
    after_ms: u64,
) -> Option<u64> {
    let mut next_deadline_ms: Option<u64> = None;
    let mut note_deadline = |deadline_ms: u64| {
        if deadline_ms <= after_ms {
            return;
        }
        next_deadline_ms = Some(match next_deadline_ms {
            Some(current) => current.min(deadline_ms),
            None => deadline_ms,
        });
    };
    for market in market_projection.markets() {
        let up = crate::polymarket::state::book_for_token(odds, market.up_token);
        let down = crate::polymarket::state::book_for_token(odds, market.down_token);
        if let Some(book) = up {
            if book.observed_at_ms > 0 {
                for max_age_ms in [
                    strategy.cfg.ranking_book_max_age_ms.max(1),
                    strategy
                        .cfg
                        .ranking_book_max_age_ms
                        .max(1)
                        .max(BOOK_STALE_SOFT_MAX_MS),
                    strategy
                        .cfg
                        .ranking_book_max_age_ms
                        .max(1)
                        .max(BOOK_STALE_SOFT_MAX_MS)
                        .max(BOOK_STALE_HARD_DROP_MS),
                ] {
                    note_deadline(
                        book.observed_at_ms
                            .saturating_add(max_age_ms)
                            .saturating_add(1),
                    );
                }
            }
        }
        if let Some(book) = down {
            if book.observed_at_ms > 0 {
                for max_age_ms in [
                    strategy.cfg.ranking_book_max_age_ms.max(1),
                    strategy
                        .cfg
                        .ranking_book_max_age_ms
                        .max(1)
                        .max(BOOK_STALE_SOFT_MAX_MS),
                    strategy
                        .cfg
                        .ranking_book_max_age_ms
                        .max(1)
                        .max(BOOK_STALE_SOFT_MAX_MS)
                        .max(BOOK_STALE_HARD_DROP_MS),
                ] {
                    note_deadline(
                        book.observed_at_ms
                            .saturating_add(max_age_ms)
                            .saturating_add(1),
                    );
                }
            }
        }
    }
    next_deadline_ms
}

fn derived_book_trigger_sig(
    market_projection: &MarketProjection,
    odds: &PolymarketOddsState,
) -> Fingerprint {
    let mut fingerprint = Fingerprint::default();
    for market in market_projection.markets() {
        let up = crate::polymarket::state::book_for_token(odds, market.up_token);
        let down = crate::polymarket::state::book_for_token(odds, market.down_token);
        let up_depth = crate::polymarket::state::depth_for_token(odds, market.up_token);
        let down_depth = crate::polymarket::state::depth_for_token(odds, market.down_token);
        fingerprint.insert_hash(fingerprint_hash(&(
            (
                market.market_id.as_str(),
                market.condition_id.as_str(),
                market.underlying.as_str(),
                market.up_token,
                market.down_token,
                market.window_start_ms,
                market.window_end_ms,
            ),
            (
                up.map(|book| {
                    (
                        book.bid.map(f64::to_bits),
                        book.ask.map(f64::to_bits),
                        book.bid_sz.map(f64::to_bits),
                        book.ask_sz.map(f64::to_bits),
                        book.tick_size.to_bits(),
                        book.ts_ms,
                        book.observed_at_ms,
                    )
                }),
                down.map(|book| {
                    (
                        book.bid.map(f64::to_bits),
                        book.ask.map(f64::to_bits),
                        book.bid_sz.map(f64::to_bits),
                        book.ask_sz.map(f64::to_bits),
                        book.tick_size.to_bits(),
                        book.ts_ms,
                        book.observed_at_ms,
                    )
                }),
                up_depth.map(|depth| (depth.revision, depth.observed_at_ms)),
                down_depth.map(|depth| (depth.revision, depth.observed_at_ms)),
            ),
        )));
    }
    fingerprint
}

pub fn current_book_trigger_sig(
    _strategy: &MakerRebateMmStrategy,
    market_projection: &MarketProjection,
    odds: &PolymarketOddsState,
) -> Fingerprint {
    if odds.revision == 0 && odds.trigger_fingerprint == Fingerprint::default() {
        return derived_book_trigger_sig(market_projection, odds);
    }
    odds.trigger_fingerprint
}

fn current_paired_exit_candidate_sig(strategy: &MakerRebateMmStrategy) -> u64 {
    fingerprint_hash(&strategy.paired_exit_candidate_trace_rows())
}

fn note_future_deadline(next_deadline_ms: &mut Option<u64>, deadline_ms: u64, after_ms: u64) {
    if deadline_ms <= after_ms {
        return;
    }
    *next_deadline_ms =
        Some(next_deadline_ms.map_or(deadline_ms, |current| current.min(deadline_ms)));
}

fn next_threshold_recompute_deadline_ms(
    strategy: &MakerRebateMmStrategy,
    runtime_control: &StrategyRuntimeControl,
    market_projection: &MarketProjection,
    now_ms: u64,
    now_ingest_mono_ns: u64,
) -> Option<u64> {
    let mut next_deadline_ms: Option<u64> = None;
    let after_ms = runtime_control.last_rebalance_ms;
    let has_paired_exit_candidates = strategy.has_paired_exit_candidates();
    for market in market_projection.markets() {
        note_future_deadline(&mut next_deadline_ms, market.window_start_ms, after_ms);
        note_future_deadline(
            &mut next_deadline_ms,
            market
                .window_start_ms
                .saturating_add(OPEN_PX_MAX_DELAY_MS)
                .saturating_add(1),
            after_ms,
        );
        note_future_deadline(&mut next_deadline_ms, market.window_end_ms, after_ms);
    }

    if let Some(latest_ingest_mono_ns) = strategy.oracle_state.latest_ingest_mono_ns {
        if let Some(age_ms) = ingest_age_ms(now_ingest_mono_ns, latest_ingest_mono_ns) {
            let remaining_ms = strategy
                .cfg
                .max_oracle_staleness_ms
                .saturating_add(1)
                .saturating_sub(age_ms);
            note_future_deadline(
                &mut next_deadline_ms,
                now_ms.saturating_add(remaining_ms),
                after_ms,
            );
        }
    }
    if has_paired_exit_candidates {
        note_future_deadline(
            &mut next_deadline_ms,
            runtime_control
                .last_rebalance_ms
                .saturating_add(strategy.cfg.fast_rebalance_interval_ms),
            after_ms,
        );
    }
    if let Some(deadline_ms) = strategy.next_protection_dwell_deadline_ms(after_ms) {
        note_future_deadline(&mut next_deadline_ms, deadline_ms, after_ms);
    }
    if let Some(deadline_ms) = strategy.next_pending_markout_deadline_ms(after_ms) {
        note_future_deadline(&mut next_deadline_ms, deadline_ms, after_ms);
    }
    next_deadline_ms
}

pub fn next_strategy_deadline_ms(runtime_control: &StrategyRuntimeControl) -> Option<u64> {
    if runtime_control.tick_kill_switch {
        return None;
    }
    let book_deadline_ms = runtime_control
        .deferred_rebalance_deadline_ms
        .is_none()
        .then_some(runtime_control.next_book_trigger_deadline_ms)
        .flatten();
    [
        runtime_control.next_recompute_deadline_ms,
        book_deadline_ms,
        runtime_control.deferred_rebalance_deadline_ms,
        runtime_control
            .invalid_tick_since_ms
            .values()
            .map(|since_ms| since_ms.saturating_add(INVALID_TICK_KILL_AFTER_MS))
            .min(),
    ]
    .into_iter()
    .flatten()
    .min()
}

pub fn update_recompute_trigger_state(
    strategy: &mut MakerRebateMmStrategy,
    runtime_control: &mut StrategyRuntimeControl,
    market_projection: &MarketProjection,
    now_ms: u64,
) {
    let blocked_markets_sig = current_blocked_markets_sig(strategy, runtime_control);
    let last_oracle_trigger_sig = Some(current_oracle_update_sig(strategy));
    let last_portfolio_update_sig = Some(current_portfolio_update_sig(strategy));
    let last_risk_update_sig = current_risk_update_sig(strategy);
    let last_open_orders_update_sig = current_open_orders_update_sig(strategy);
    let (last_book_trigger_sig, next_book_trigger_deadline_ms) = {
        let odds = strategy.odds.as_ref();
        (
            Some(current_book_trigger_sig(strategy, market_projection, &odds)),
            current_book_trigger_deadline_ms(
                strategy,
                market_projection,
                &odds,
                runtime_control.last_rebalance_ms,
            ),
        )
    };
    let next_recompute_deadline_ms = next_threshold_recompute_deadline_ms(
        strategy,
        runtime_control,
        market_projection,
        now_ms,
        strategy.current_engine_mono_ns,
    );
    let last_paired_exit_candidate_sig = Some(current_paired_exit_candidate_sig(strategy));
    runtime_control.last_book_trigger_sig = last_book_trigger_sig;
    runtime_control.last_oracle_trigger_sig = last_oracle_trigger_sig;
    runtime_control.last_portfolio_update_sig = last_portfolio_update_sig;
    runtime_control.last_risk_update_sig = last_risk_update_sig;
    runtime_control.last_open_orders_update_sig = last_open_orders_update_sig;
    runtime_control.last_blocked_markets_sig = blocked_markets_sig;
    runtime_control.last_paired_exit_candidate_sig = last_paired_exit_candidate_sig;
    runtime_control.next_book_trigger_deadline_ms = next_book_trigger_deadline_ms;
    runtime_control.next_recompute_deadline_ms = next_recompute_deadline_ms;
}

pub fn next_recompute_reason(
    strategy: &mut MakerRebateMmStrategy,
    runtime_control: &mut StrategyRuntimeControl,
    market_projection: &MarketProjection,
    now_ms: u64,
) -> Option<QuoteRecomputeReason> {
    let blocked_markets_sig = current_blocked_markets_sig(strategy, runtime_control);
    let odds = strategy.odds.as_ref();
    if runtime_control.last_book_trigger_sig.is_none() {
        return Some(QuoteRecomputeReason::Initial);
    }
    if runtime_control.last_portfolio_update_sig != Some(current_portfolio_update_sig(strategy)) {
        return Some(QuoteRecomputeReason::PortfolioStateChanged);
    }
    if runtime_control.last_open_orders_update_sig != current_open_orders_update_sig(strategy) {
        return Some(QuoteRecomputeReason::OpenOrdersChanged);
    }
    if runtime_control.last_risk_update_sig != current_risk_update_sig(strategy) {
        return Some(QuoteRecomputeReason::RiskStateChanged);
    }
    if runtime_control.last_blocked_markets_sig != blocked_markets_sig {
        return Some(QuoteRecomputeReason::BlockedMarketsChanged);
    }
    if runtime_control.last_paired_exit_candidate_sig
        != Some(current_paired_exit_candidate_sig(strategy))
    {
        return Some(QuoteRecomputeReason::ThresholdFired);
    }
    if runtime_control
        .next_recompute_deadline_ms
        .is_some_and(|deadline_ms| now_ms >= deadline_ms)
    {
        return Some(QuoteRecomputeReason::ThresholdFired);
    }
    if runtime_control.last_oracle_trigger_sig != Some(current_oracle_update_sig(strategy)) {
        return Some(QuoteRecomputeReason::OracleChanged);
    }
    if runtime_control.last_book_trigger_sig
        != Some(current_book_trigger_sig(strategy, market_projection, &odds))
    {
        return Some(QuoteRecomputeReason::BookChanged);
    }
    if runtime_control
        .next_book_trigger_deadline_ms
        .is_some_and(|deadline_ms| now_ms >= deadline_ms)
    {
        return Some(QuoteRecomputeReason::BookChanged);
    }
    if runtime_control
        .deferred_rebalance_deadline_ms
        .is_some_and(|deadline_ms| now_ms >= deadline_ms)
    {
        return Some(QuoteRecomputeReason::ThresholdFired);
    }
    None
}

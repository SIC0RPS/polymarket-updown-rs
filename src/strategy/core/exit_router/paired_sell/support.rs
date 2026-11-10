use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::state::{BookTop, PolymarketOddsState};
use crate::strategy::paired_maker::{MakerRebateMmStrategy, PAIRED_RELEASE_QTY_EPS};
use crate::strategy::support::shared::common::is_valid_tick_size;

use super::super::super::pair_lifecycle::{
    is_confirmed_ctf_split_cohort_id, AuthoritativePairedCohortView, PairLifecycleState,
};

fn short_trace_token(token: &str) -> String {
    if token.len() <= 12 {
        return token.to_string();
    }
    format!("{}..{}", &token[..6], &token[token.len() - 6..])
}

fn paired_exit_inventory_is_present(
    portfolio_snapshot: &crate::portfolio::PortfolioState,
    quote_group: &str,
    up_token: &str,
    down_token: &str,
) -> bool {
    if is_confirmed_ctf_split_cohort_id(quote_group) {
        return portfolio_snapshot
            .position(up_token)
            .is_some_and(|position| position.qty_units_6dp > 0)
            || portfolio_snapshot
                .position(down_token)
                .is_some_and(|position| position.qty_units_6dp > 0);
    }
    portfolio_snapshot
        .position(up_token)
        .is_some_and(|position| position.sellable_qty > PAIRED_RELEASE_QTY_EPS)
        || portfolio_snapshot
            .position(down_token)
            .is_some_and(|position| position.sellable_qty > PAIRED_RELEASE_QTY_EPS)
}

fn direct_buy_exit_entries(
    portfolio: &crate::portfolio::PortfolioState,
) -> impl Iterator<
    Item = (
        &crate::trade_session_ledger::ConfirmedCtfBuyFact,
        u128,
        u128,
    ),
> {
    portfolio
        .confirmed_direct_buy_entries()
        .filter_map(|entry| {
            let qty_units_6dp = portfolio
                .confirmed_ctf_cohort_qty_units_6dp(&entry.token, &entry.cohort_id)
                .ok()?;
            if qty_units_6dp == 0 {
                return None;
            }
            let outlay_units_6dp = portfolio
                .confirmed_ctf_entry_outlay_units_6dp(&entry.cohort_id)
                .ok()?;
            Some((entry, qty_units_6dp, outlay_units_6dp))
        })
}

pub(super) fn paired_managed_sell_book_is_current(
    cfg: &crate::config::MakerRebateMmStrategyConfig,
    top: &BookTop,
    now_ms: u64,
) -> bool {
    let max_staleness_ms = cfg.paired_first_managed_sell_book_max_age_ms.max(1);
    top.bid.is_some_and(|bid| bid.is_finite() && bid > 0.0)
        && top.observed_at_ms > 0
        && now_ms
            .checked_sub(top.observed_at_ms)
            .is_some_and(|age_ms| age_ms <= max_staleness_ms)
}

impl MakerRebateMmStrategy {
    pub(super) fn paired_books_for_tokens(
        &self,
        odds: &PolymarketOddsState,
        market: Option<&MarketSelection>,
        up_token: &str,
        down_token: &str,
    ) -> (Option<BookTop>, Option<BookTop>) {
        if let Some(market) = market {
            if let Some(books) = self.find_books(odds, market) {
                return (Some(books.0), Some(books.1));
            }
        }

        (
            up_token
                .parse()
                .ok()
                .and_then(|token| self.current_book_for_token(odds, token)),
            down_token
                .parse()
                .ok()
                .and_then(|token| self.current_book_for_token(odds, token)),
        )
    }

    pub(super) fn paired_exit_candidates(
        &self,
        portfolio_snapshot: &crate::portfolio::PortfolioState,
    ) -> Vec<AuthoritativePairedCohortView> {
        self.authoritative_paired_cohort_keys()
            .into_iter()
            .filter_map(|key| self.authoritative_paired_cohort_view_by_key(&key))
            .filter(|cohort| {
                paired_exit_inventory_is_present(
                    portfolio_snapshot,
                    &cohort.quote_group,
                    &cohort.up_token,
                    &cohort.down_token,
                )
            })
            .chain(direct_buy_exit_entries(portfolio_snapshot).map(
                |(entry, qty_units_6dp, outlay_units_6dp)| {
                    AuthoritativePairedCohortView::from_confirmed_buy(
                        entry,
                        qty_units_6dp,
                        outlay_units_6dp,
                    )
                },
            ))
            .collect()
    }

    fn paired_exit_candidate_lifecycle_states(&self) -> Vec<&PairLifecycleState> {
        let mut states = self.pair_lifecycle_by_group.values().collect::<Vec<_>>();
        states.sort_by(|left, right| {
            left.market_id
                .cmp(&right.market_id)
                .then_with(|| left.up_token.cmp(&right.up_token))
                .then_with(|| left.down_token.cmp(&right.down_token))
        });
        states.dedup_by(|left, right| {
            left.market_id == right.market_id
                && left.up_token == right.up_token
                && left.down_token == right.down_token
        });
        states
    }

    pub(crate) fn paired_exit_candidate_trace_rows(&self) -> Vec<String> {
        let portfolio_snapshot = self.portfolio_rx.borrow();
        let mut rows = self
            .paired_exit_candidate_lifecycle_states()
            .into_iter()
            .filter(|state| {
                paired_exit_inventory_is_present(
                    &portfolio_snapshot,
                    &state.quote_group,
                    &state.up_token,
                    &state.down_token,
                )
            })
            .map(|state| {
                let up_qty = portfolio_snapshot
                    .position(&state.up_token)
                    .map(|position| position.sellable_qty)
                    .unwrap_or(0.0);
                let down_qty = portfolio_snapshot
                    .position(&state.down_token)
                    .map(|position| position.sellable_qty)
                    .unwrap_or(0.0);
                format!(
                    "{}:{}:{}:{:.4}:{:.4}",
                    state.market_id,
                    short_trace_token(&state.up_token),
                    short_trace_token(&state.down_token),
                    up_qty,
                    down_qty
                )
            })
            .collect::<Vec<_>>();
        rows.extend(direct_buy_exit_entries(&portfolio_snapshot).map(
            |(entry, qty_units_6dp, _)| {
                let qty = crate::strategy::full_set_units_to_qty(qty_units_6dp);
                let (up_qty, down_qty) = if entry.authorization.selected_up {
                    (qty, 0.0)
                } else {
                    (0.0, qty)
                };
                format!(
                    "{}:{}:{}:{:.4}:{:.4}",
                    entry.market_id,
                    short_trace_token(&entry.authorization.up_token.to_string()),
                    short_trace_token(&entry.authorization.down_token.to_string()),
                    up_qty,
                    down_qty
                )
            },
        ));
        rows.sort();
        rows
    }

    pub(crate) fn has_paired_exit_candidates(&self) -> bool {
        let portfolio_snapshot = self.portfolio_rx.borrow();
        if direct_buy_exit_entries(&portfolio_snapshot)
            .next()
            .is_some()
        {
            return true;
        }
        self.paired_exit_candidate_lifecycle_states()
            .into_iter()
            .any(|state| {
                paired_exit_inventory_is_present(
                    &portfolio_snapshot,
                    &state.quote_group,
                    &state.up_token,
                    &state.down_token,
                )
            })
    }

    pub(crate) fn paired_managed_sell_book_is_current(&self, top: &BookTop, now_ms: u64) -> bool {
        paired_managed_sell_book_is_current(&self.cfg, top, now_ms)
    }

    pub(crate) fn paired_managed_sell_limit_px(&self, top: &BookTop) -> Option<f64> {
        if !is_valid_tick_size(top.tick_size) {
            return None;
        }
        let tick = top.tick_size;
        let bid = top.bid.filter(|bid| bid.is_finite() && *bid > 0.0)?;

        let tolerance = self.cfg.paired_first_managed_sell_slippage_tolerance;
        let min_px = (bid * (1.0 - tolerance)).clamp(tick, 1.0 - tick);
        let configured_slip = self.cfg.paired_first_managed_sell_aggressiveness.max(0.0);
        let aggressive_slip = configured_slip.max(tick);
        let raw = (bid - aggressive_slip).clamp(min_px, 1.0 - tick);
        let px = crate::strategy::support::shared::common::round_to_tick(raw, tick, false);

        (px.is_finite() && px > 0.0).then_some(px)
    }
}

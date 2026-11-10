use super::*;

impl MakerRebateMmStrategy {
    pub(in crate::strategy) fn shock_toxicity(last_return_bps: f64, shock_bps: f64) -> f64 {
        (last_return_bps.abs() / shock_bps.max(1e-6)).clamp(0.0, 3.0)
    }

    pub(in crate::strategy) fn find_books(
        &self,
        odds: &crate::polymarket::state::PolymarketOddsState,
        market: &MarketSelection,
    ) -> Option<(BookTop, BookTop)> {
        self.current_books_for_market(odds, market)
    }

    pub(super) fn market_has_inventory(&self, selection: &MarketSelection) -> bool {
        let portfolio = self.portfolio_rx.borrow();
        portfolio
            .position(selection.up_token_id())
            .is_some_and(|p| crate::polymarket::fixed_math::has_fixed_6dp_units(p.qty.abs()))
            || portfolio
                .position(selection.down_token_id())
                .is_some_and(|p| crate::polymarket::fixed_math::has_fixed_6dp_units(p.qty.abs()))
    }

    pub(super) fn market_has_live_quotes_or_inventory(&self, selection: &MarketSelection) -> bool {
        if self.market_has_inventory(selection) {
            return true;
        }
        self.open_orders_rx
            .as_ref()
            .map(|rx| rx.borrow())
            .is_some_and(|open_orders| {
                open_orders.orders.values().any(|order| {
                    selection.market_id == order.market
                        || order.token == selection.up_token_id()
                        || order.token == selection.down_token_id()
                })
            })
    }

    pub(in crate::strategy) fn market_has_feasible_fresh_entry(
        &mut self,
        portfolio: &PortfolioState,
        selection: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
    ) -> bool {
        if !self.market_ctf_entry_ready(selection, up, down) {
            self.note_quote_block("ctf_split_entry_required");
            return false;
        }

        let up_inventory_qty = portfolio
            .position(selection.up_token_id())
            .map(|position| position.qty)
            .unwrap_or(0.0);
        let down_inventory_qty = portfolio
            .position(selection.down_token_id())
            .map(|position| position.qty)
            .unwrap_or(0.0);

        if !selection.standard_binary_ctf() || self.cfg.max_inventory_per_token <= 0.0 {
            return false;
        }
        let complete_qty = up_inventory_qty.min(down_inventory_qty).max(0.0);
        if complete_qty + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
            >= self.venue_min_size(up).max(self.venue_min_size(down))
        {
            return true;
        }

        up_inventory_qty < self.cfg.max_inventory_per_token
            || down_inventory_qty < self.cfg.max_inventory_per_token
    }

    pub(in crate::strategy) fn market_ctf_entry_ready(
        &self,
        selection: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
    ) -> bool {
        selection.ctf_inventory_eligible()
            || (selection.standard_binary_ctf()
                && book_has_entry_constraints(up)
                && book_has_entry_constraints(down))
    }
}

fn book_has_entry_constraints(top: &BookTop) -> bool {
    top.tick_size.is_finite()
        && top.tick_size > 0.0
        && top.min_order_size.is_finite()
        && top.min_order_size > 0.0
}

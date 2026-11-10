use super::state::{ConfirmedCtfEntryOutlay, PortfolioState};

impl PortfolioState {
    pub(crate) fn project_canonical_buy_basis(
        &mut self,
        financial: crate::trade_session_ledger::CanonicalFinancialView<'_>,
    ) {
        use crate::trade_session_ledger::CanonicalUnresolvedCohortState;
        use polymarket_client_sdk::types::U256;

        for position in self.positions.values_mut() {
            position.remaining_basis_units_6dp = None;
        }
        for cohort in financial.unresolved_cohorts.iter() {
            let CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } =
                &cohort.state
            else {
                continue;
            };
            let Some(authorization) = cashflow.buy_authorization else {
                continue;
            };
            let Some(ConfirmedCtfEntryOutlay::Buy(receipts)) = self
                .confirmed_ctf_split_outlays
                .get(&cohort.paired_cohort_key)
            else {
                continue;
            };
            let Some((_, buy)) = receipts.first_key_value() else {
                continue;
            };
            if buy.authorization != authorization
                || buy.cohort_id != cohort.paired_cohort_key
                || buy.market_id != cohort.market_id
                || buy.condition_id != cashflow.condition_id
                || cohort.symbol != authorization.symbol.as_str()
                || cashflow.up_token.parse::<U256>().ok() != Some(authorization.up_token)
                || cashflow.down_token.parse::<U256>().ok() != Some(authorization.down_token)
            {
                continue;
            }
            let (token, acquired_qty, remaining_qty, basis) = if authorization.selected_up {
                (
                    cashflow.up_token.as_str(),
                    cashflow.up_created_qty_units_6dp,
                    cashflow.up_remaining_qty_units_6dp,
                    cashflow.up_remaining_basis_units_6dp,
                )
            } else {
                (
                    cashflow.down_token.as_str(),
                    cashflow.down_created_qty_units_6dp,
                    cashflow.down_remaining_qty_units_6dp,
                    cashflow.down_remaining_basis_units_6dp,
                )
            };
            if buy.token != token
                || remaining_qty == 0
                || acquired_qty
                    != self.confirmed_direct_buy_qty_units_6dp(&cohort.paired_cohort_key)
                || self
                    .confirmed_ctf_cohort_qty_units_6dp(token, &cohort.paired_cohort_key)
                    .ok()
                    != Some(remaining_qty)
            {
                continue;
            }
            if let Some(position) = self.positions.get_mut(token) {
                if position.token == token && position.qty_units_6dp == remaining_qty {
                    position.remaining_basis_units_6dp = basis;
                }
            }
        }
    }

    pub fn is_filtered_for_underlying(
        &self,
        underlying: &str,
        ownership: &crate::polymarket::symbol_ownership::SymbolOwnershipIndex,
    ) -> bool {
        self.filtered_mismatch_reason(underlying, ownership)
            .is_none()
    }

    pub(crate) fn filtered_mismatch_reason(
        &self,
        underlying: &str,
        ownership: &crate::polymarket::symbol_ownership::SymbolOwnershipIndex,
    ) -> Option<&'static str> {
        let filtered = self.filtered_for_underlying(underlying, ownership);
        macro_rules! check {
            ($condition:expr, $name:literal) => {
                if !$condition {
                    return Some($name);
                }
            };
        }
        check!(
            self.updated_at_ms == filtered.updated_at_ms,
            "updated_at_ms"
        );
        check!(self.revision == filtered.revision, "revision");
        check!(
            self.positions_fingerprint == filtered.positions_fingerprint,
            "positions_fingerprint"
        );
        check!(
            self.total_cost_basis.to_bits() == filtered.total_cost_basis.to_bits(),
            "total_cost_basis"
        );
        check!(
            self.next_fill_seq == filtered.next_fill_seq,
            "next_fill_seq"
        );
        check!(
            self.fill_quality.fills_total == filtered.fill_quality.fills_total,
            "fill_quality.fills_total"
        );
        check!(
            self.fill_quality.top_of_book_fills == filtered.fill_quality.top_of_book_fills,
            "fill_quality.top_of_book_fills"
        );
        check!(
            self.fill_quality.queue_ahead_sum.to_bits()
                == filtered.fill_quality.queue_ahead_sum.to_bits(),
            "fill_quality.queue_ahead_sum"
        );
        check!(
            self.fill_quality.queue_ahead_samples == filtered.fill_quality.queue_ahead_samples,
            "fill_quality.queue_ahead_samples"
        );
        check!(
            self.fill_quality.time_to_fill_ms_sum == filtered.fill_quality.time_to_fill_ms_sum,
            "fill_quality.time_to_fill_ms_sum"
        );
        check!(
            self.fill_quality.time_to_fill_samples == filtered.fill_quality.time_to_fill_samples,
            "fill_quality.time_to_fill_samples"
        );
        check!(
            self.positions.len() == filtered.positions.len(),
            "positions"
        );
        check!(self.fills.len() == filtered.fills.len(), "fills");
        check!(
            self.confirmed_lifecycle_sell_fills.len()
                == filtered.confirmed_lifecycle_sell_fills.len(),
            "confirmed_lifecycle_sell_fills"
        );
        check!(
            self.quarantined_fills.len() == filtered.quarantined_fills.len(),
            "quarantined_fills"
        );
        check!(
            self.closed_trades.len() == filtered.closed_trades.len(),
            "closed_trades"
        );
        check!(
            self.token_market_hint.len() == filtered.token_market_hint.len(),
            "token_market_hint"
        );
        check!(
            self.open_lots.len() == filtered.open_lots.len(),
            "open_lots"
        );
        check!(
            self.confirmed_ctf_split_lot_identities.len()
                == filtered.confirmed_ctf_split_lot_identities.len(),
            "confirmed_ctf_split_lot_identities"
        );
        check!(
            self.confirmed_ctf_split_outlays.len() == filtered.confirmed_ctf_split_outlays.len(),
            "confirmed_ctf_split_outlays"
        );
        check!(
            self.token_expiry_ms.len() == filtered.token_expiry_ms.len(),
            "token_expiry_ms"
        );
        check!(
            self.seen_fill_keys.len() == filtered.seen_fill_keys.len(),
            "seen_fill_keys"
        );
        check!(
            self.seen_terminal_close_keys.len() == filtered.seen_terminal_close_keys.len(),
            "seen_terminal_close_keys"
        );
        None
    }

    pub fn filtered_for_underlying(
        &self,
        underlying: &str,
        ownership: &crate::polymarket::symbol_ownership::SymbolOwnershipIndex,
    ) -> Self {
        let token_belongs = |token: &str| ownership.token_belongs(token, underlying);
        let fact_belongs = |market_id: Option<&str>, token: &str| {
            ownership.facts_belong_to(underlying, None, market_id, &[token])
        };
        let cohort_belongs = |cohort: Option<&str>| {
            cohort.is_none_or(|cohort| ownership.cohort_belongs(cohort, underlying))
        };
        let mut filtered = Self {
            updated_at_ms: self.updated_at_ms,
            revision: self.revision,
            ..Default::default()
        };
        filtered.positions = self
            .positions
            .iter()
            .filter(|(token, _)| token_belongs(token))
            .map(|(token, position)| (token.clone(), position.clone()))
            .collect();
        filtered.fills = self
            .fills
            .iter()
            .filter(|queued| {
                fact_belongs(queued.fill.market_id.as_deref(), &queued.fill.token)
                    && cohort_belongs(queued.fill.lifecycle_cohort.as_deref())
            })
            .cloned()
            .collect();
        filtered.next_fill_seq = filtered
            .fills
            .iter()
            .map(|queued| queued.seq)
            .max()
            .unwrap_or(0);
        filtered.confirmed_lifecycle_sell_fills = self
            .confirmed_lifecycle_sell_fills
            .iter()
            .filter(|((cohort, _), fill)| {
                fact_belongs(Some(fill.market_id.as_str()), &fill.token)
                    && ownership.cohort_belongs(cohort, underlying)
            })
            .map(|(key, fill)| (key.clone(), fill.clone()))
            .collect();
        filtered.quarantined_fills = self
            .quarantined_fills
            .iter()
            .filter(|queued| {
                fact_belongs(queued.fill.market_id.as_deref(), &queued.fill.token)
                    && cohort_belongs(queued.fill.lifecycle_cohort.as_deref())
            })
            .cloned()
            .collect();
        filtered.closed_trades = self
            .closed_trades
            .iter()
            .filter(|trade| fact_belongs(Some(trade.market_id.as_str()), &trade.token))
            .cloned()
            .collect();
        filtered.token_market_hint = self
            .token_market_hint
            .iter()
            .filter(|(token, market_id)| fact_belongs(Some(market_id.as_str()), token))
            .map(|(token, market_id)| (token.clone(), market_id.clone()))
            .collect();
        filtered.open_lots = self
            .open_lots
            .iter()
            .filter(|(token, _)| token_belongs(token))
            .map(|(token, lots)| (token.clone(), lots.clone()))
            .collect();
        let projected_ctf_split_outlays: std::collections::BTreeMap<
            String,
            ConfirmedCtfEntryOutlay,
        > = self
            .confirmed_ctf_split_outlays
            .iter()
            .filter(|(_, outlay)| ownership.market_belongs(outlay.market_id().as_str(), underlying))
            .map(|(cohort, outlay)| (cohort.clone(), outlay.clone()))
            .collect();
        filtered.confirmed_ctf_split_lot_identities = self
            .confirmed_ctf_split_lot_identities
            .iter()
            .filter(|(token, cohort, transaction_id)| {
                token_belongs(token)
                    && projected_ctf_split_outlays
                        .get(cohort.as_str())
                        .is_some_and(|outlay| {
                            outlay.split().is_some_and(|split| {
                                split.transaction_id.as_str() == transaction_id.as_str()
                            })
                        })
            })
            .cloned()
            .collect();
        filtered.confirmed_ctf_split_outlays = projected_ctf_split_outlays;
        filtered.token_expiry_ms = self
            .token_expiry_ms
            .iter()
            .filter(|(token, _)| token_belongs(token))
            .map(|(token, expiry)| (token.clone(), *expiry))
            .collect();
        filtered.refresh_position_aggregates();
        filtered.positions_fingerprint = filtered.compute_positions_fingerprint();
        filtered
    }
}

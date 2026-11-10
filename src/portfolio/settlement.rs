use std::collections::HashMap;

use super::closed_trades::{RedeemCloseRequest, SettlementInventoryClearance};
use super::hints::TOKEN_MARKET_HINT_GRACE_MS;
use super::state::PortfolioState;

impl PortfolioState {
    pub fn ingest_expiry(&mut self, odds_expiry: &HashMap<String, u64>) {
        let mut changed = false;
        for (token, &expiry) in odds_expiry {
            self.token_expiry_ms
                .entry(token.clone())
                .and_modify(|current| {
                    let next = (*current).max(expiry);
                    changed |= *current != next;
                    *current = next;
                })
                .or_insert_with(|| {
                    changed = true;
                    expiry
                });
        }

        for (token, pos) in &mut self.positions {
            if pos.end_time_ms.is_none() {
                pos.end_time_ms = self.token_expiry_ms.get(token).copied();
                changed |= pos.end_time_ms.is_some();
            }
        }

        let now = super::state::now_ms();
        let grace_ms: u64 = TOKEN_MARKET_HINT_GRACE_MS;
        let previous_expiry_len = self.token_expiry_ms.len();
        self.token_expiry_ms.retain(|t, &mut expiry| {
            self.positions.contains_key(t)
                || self.open_lots.contains_key(t)
                || now < expiry.saturating_add(grace_ms)
        });
        changed |= self.token_expiry_ms.len() != previous_expiry_len;

        self.prune_token_market_hints(now);
        if changed {
            self.refresh_positions_metadata(now);
        }
    }

    pub(crate) fn close_redeemed_positions(
        &mut self,
        requests: &[RedeemCloseRequest],
    ) -> anyhow::Result<Vec<SettlementInventoryClearance>> {
        let mut next = self.clone();
        let clearances = next.apply_terminal_clearances(requests)?;
        *self = next;
        Ok(clearances)
    }

    fn apply_terminal_clearances(
        &mut self,
        requests: &[RedeemCloseRequest],
    ) -> anyhow::Result<Vec<SettlementInventoryClearance>> {
        let mut clearances = Vec::new();
        let now = requests
            .iter()
            .map(|request| request.closed_at_ms)
            .max()
            .unwrap_or_else(super::state::now_ms);
        for request in requests {
            if request.qty_units_6dp == 0 {
                continue;
            }
            let terminal_close_key = super::closed_trades::TerminalCloseKey::from_request(request);
            if terminal_close_key
                .as_ref()
                .is_some_and(|key| self.seen_terminal_close_keys.contains(key))
            {
                continue;
            }
            let Some(mut pos) = self.positions.get(&request.token).cloned() else {
                continue;
            };
            if pos.qty_units_6dp == 0 {
                continue;
            }

            let mut lots = self
                .open_lots
                .get(&request.token)
                .cloned()
                .unwrap_or_default();
            anyhow::ensure!(
                request.qty_units_6dp <= pos.qty_units_6dp,
                "terminal requested quantity exceeds exact portfolio quantity token={} requested_units={} portfolio_units={}",
                request.token,
                request.qty_units_6dp,
                pos.qty_units_6dp
            );
            let mut remaining_units_6dp = request.qty_units_6dp;
            let clearance_start = clearances.len();

            while remaining_units_6dp > 0 {
                let Some(front) = lots.front_mut() else {
                    break;
                };
                let lot_units_6dp = front.qty_units_6dp;
                if lot_units_6dp == 0 {
                    let _ = lots.pop_front();
                    continue;
                }
                let engine_trade_id = front.engine_trade_id.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "terminal exact lot has no cohort identity token={}",
                        request.token
                    )
                })?;
                anyhow::ensure!(
                    !engine_trade_id.is_empty() && engine_trade_id.trim() == engine_trade_id,
                    "terminal exact lot has invalid cohort identity token={}",
                    request.token
                );
                let close_units_6dp = remaining_units_6dp.min(lot_units_6dp);
                let close_qty = crate::strategy::full_set_units_to_qty(close_units_6dp);
                let mut entry_cost_per_unit = front.entry_px;
                if let Some(entry_fee_per_unit) = front.entry_fee_per_unit {
                    entry_cost_per_unit += entry_fee_per_unit;
                }
                clearances.push(SettlementInventoryClearance {
                    market_id: request.market_id.clone(),
                    token: request.token.clone(),
                    outcome_index: request.outcome_index,
                    engine_trade_id: Some(engine_trade_id.to_string()),
                    qty_units_6dp: close_units_6dp,
                    closed_at_ms: request.closed_at_ms,
                    settlement_tx_hash: request.exit_tx_hash.clone(),
                });
                pos.qty_units_6dp = pos
                    .qty_units_6dp
                    .checked_sub(close_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("terminal position quantity underflow"))?;
                pos.sellable_qty_units_6dp = pos.sellable_qty_units_6dp.min(pos.qty_units_6dp);
                pos.qty = crate::strategy::full_set_units_to_qty(pos.qty_units_6dp);
                pos.sellable_qty =
                    crate::strategy::full_set_units_to_qty(pos.sellable_qty_units_6dp);
                pos.cost_basis = (pos.cost_basis - entry_cost_per_unit * close_qty).max(0.0);
                pos.notional_basis = (pos.notional_basis - front.entry_px * close_qty).max(0.0);
                let remaining_lot_units_6dp = lot_units_6dp
                    .checked_sub(close_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("terminal lot quantity underflow"))?;
                front.qty_units_6dp = remaining_lot_units_6dp;
                front.qty = crate::strategy::full_set_units_to_qty(remaining_lot_units_6dp);
                remaining_units_6dp = remaining_units_6dp
                    .checked_sub(close_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("terminal request quantity underflow"))?;
                if remaining_lot_units_6dp == 0 {
                    let _ = lots.pop_front();
                }
            }

            anyhow::ensure!(
                remaining_units_6dp == 0,
                "terminal exact lots do not cover request token={} missing_units={}",
                request.token,
                remaining_units_6dp
            );
            if let Some(key) = terminal_close_key {
                self.seen_terminal_close_keys.insert(key);
            }

            let cleared_units_6dp = clearances[clearance_start..]
                .iter()
                .try_fold(0u128, |total, clearance| {
                    total.checked_add(clearance.qty_units_6dp)
                });
            anyhow::ensure!(
                cleared_units_6dp == Some(request.qty_units_6dp),
                "terminal exact clearance quantity does not equal request token={} requested_units={} cleared_units={:?}",
                request.token,
                request.qty_units_6dp,
                cleared_units_6dp
            );

            if pos.qty_units_6dp > 0 {
                pos.avg_px = if pos.notional_basis > 0.0 {
                    pos.notional_basis / pos.qty
                } else if pos.cost_basis > 0.0 {
                    pos.cost_basis / pos.qty
                } else {
                    0.0
                };
                pos.last_update_ms = request.closed_at_ms;
                self.positions.insert(request.token.clone(), pos);
            } else {
                self.positions.remove(&request.token);
            }
            if !lots.is_empty() {
                self.open_lots.insert(request.token.clone(), lots);
            } else {
                self.open_lots.remove(&request.token);
            }
        }

        self.token_expiry_ms
            .retain(|token, _| self.positions.contains_key(token));
        self.prune_token_market_hints(now);
        self.refresh_position_aggregates();
        self.refresh_positions_metadata(now);

        Ok(clearances)
    }
}

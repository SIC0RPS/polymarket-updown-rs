use crate::{polymarket::identity::MarketId, types::OrderCloseReason};

use super::closed_trades::ClosedTrade;
use super::state::{
    f64_qty_to_units_6dp, sync_position_units_6dp, ExpectedCtfPosition, PortfolioState,
    PositionState, EPS,
};

#[derive(Debug, Clone)]
pub(in crate::portfolio) struct OpenLot {
    pub(in crate::portfolio) qty_units_6dp: u128,
    pub(in crate::portfolio) qty: f64,
    pub(in crate::portfolio) entry_px: f64,
    pub(in crate::portfolio) entry_fee_per_unit: Option<f64>,
    pub(in crate::portfolio) engine_trade_id: Option<String>,
    pub(in crate::portfolio) entry_order_id: Option<String>,
    pub(in crate::portfolio) entry_fill_id: Option<String>,
    pub(in crate::portfolio) entry_tx_hash: Option<String>,
}

impl PortfolioState {
    pub(in crate::portfolio) fn reconcile_expected_ctf_lots(
        &mut self,
        expected: &ExpectedCtfPosition,
        total_units_6dp: u128,
        entry_px: f64,
    ) -> anyhow::Result<bool> {
        let token = expected.token.trim();
        anyhow::ensure!(
            entry_px.is_finite() && (0.0..=1.0).contains(&entry_px),
            "confirmed CTF split lot entry price is invalid token={token}"
        );
        let mut lots = self.open_lots.get(token).cloned().unwrap_or_default();
        let mut changed = false;
        if lots.is_empty() && total_units_6dp > 0 {
            lots.push_back(OpenLot {
                qty_units_6dp: total_units_6dp,
                qty: crate::strategy::full_set_units_to_qty(total_units_6dp),
                entry_px,
                entry_fee_per_unit: None,
                engine_trade_id: None,
                entry_order_id: None,
                entry_fill_id: None,
                entry_tx_hash: None,
            });
            changed = true;
        }

        let identity = expected
            .engine_trade_id
            .as_ref()
            .zip(expected.confirmed_entry_tx_hash.as_ref())
            .map(|(cohort, tx_hash)| (token.to_string(), cohort.clone(), tx_hash.clone()));
        let confirmed_units = (expected.require_positive_balance
            && expected.portfolio_actionable
            && identity.is_some())
        .then_some(expected.min_qty_units_6dp)
        .flatten()
        .unwrap_or(0);
        let identity_seen = identity
            .as_ref()
            .is_some_and(|key| self.confirmed_ctf_split_lot_identities.contains(key));
        if confirmed_units > 0 && !identity_seen {
            changed = true;
            let mut remaining = confirmed_units;
            for lot in lots.iter_mut().rev().filter(|lot| {
                lot.engine_trade_id.is_none()
                    && lot.entry_order_id.is_none()
                    && lot.entry_fill_id.is_none()
                    && lot.entry_tx_hash.is_none()
            }) {
                let taken = lot.qty_units_6dp.min(remaining);
                lot.qty_units_6dp -= taken;
                lot.qty = crate::strategy::full_set_units_to_qty(lot.qty_units_6dp);
                remaining -= taken;
                if remaining == 0 {
                    break;
                }
            }
            anyhow::ensure!(
                remaining == 0,
                "confirmed CTF split cannot be carved from anonymous inventory for token {token}"
            );
            lots.retain(|lot| lot.qty_units_6dp > 0);
            lots.push_front(OpenLot {
                qty_units_6dp: confirmed_units,
                qty: crate::strategy::full_set_units_to_qty(confirmed_units),
                entry_px,
                entry_fee_per_unit: None,
                engine_trade_id: expected.engine_trade_id.clone(),
                entry_order_id: None,
                entry_fill_id: None,
                entry_tx_hash: expected.confirmed_entry_tx_hash.clone(),
            });
        }

        let current_units = lots
            .iter()
            .try_fold(0u128, |total, lot| total.checked_add(lot.qty_units_6dp));
        let Some(current_units) = current_units else {
            anyhow::bail!("CTF lot quantity overflow for token {token}");
        };
        if current_units > total_units_6dp {
            changed = true;
            let mut reduction = current_units - total_units_6dp;
            for prefer_named in [true, false] {
                for lot in lots
                    .iter_mut()
                    .filter(|lot| (lot.engine_trade_id == expected.engine_trade_id) == prefer_named)
                {
                    let taken = lot.qty_units_6dp.min(reduction);
                    lot.qty_units_6dp -= taken;
                    lot.qty = crate::strategy::full_set_units_to_qty(lot.qty_units_6dp);
                    reduction -= taken;
                    if reduction == 0 {
                        break;
                    }
                }
                if reduction == 0 {
                    break;
                }
            }
            anyhow::ensure!(reduction == 0, "CTF lots exceed balance for token {token}");
            lots.retain(|lot| lot.qty_units_6dp > 0);
        }
        let reconciled_units = lots
            .iter()
            .try_fold(0u128, |total, lot| total.checked_add(lot.qty_units_6dp));
        anyhow::ensure!(
            reconciled_units == Some(total_units_6dp),
            "CTF lots do not equal confirmed balance for token {token}"
        );
        if confirmed_units > 0 && !identity_seen {
            self.confirmed_ctf_split_lot_identities
                .insert(identity.expect("confirmed identity checked above"));
        }
        self.open_lots.insert(token.to_string(), lots);
        Ok(changed)
    }

    pub(crate) fn has_matching_ctf_split_lot(&self, token: &str, cohort: &str) -> bool {
        self.open_lots.get(token).is_some_and(|lots| {
            lots.iter()
                .any(|lot| lot.engine_trade_id.as_deref() == Some(cohort))
        })
    }

    pub(in crate::portfolio) fn apply_buy_position_change(
        &mut self,
        token: &str,
        market_id: Option<&str>,
        ts_ms: u64,
        px: f64,
        qty: f64,
        total_notional: f64,
        total_fee: f64,
        engine_trade_id: Option<String>,
        entry_order_id: Option<String>,
        entry_fill_id: Option<String>,
        entry_tx_hash: Option<String>,
        confirmed_buy: Option<&crate::trade_session_ledger::ConfirmedCtfBuyFact>,
    ) -> anyhow::Result<()> {
        let canonical_basis = confirmed_buy
            .map(crate::ctf_accounting::buy_receipt_basis_units_6dp)
            .transpose()?;
        let (px, qty, total_notional, total_fee) = match confirmed_buy {
            Some(fact) => {
                anyhow::ensure!(fact.qty_units_6dp > 0, "confirmed BUY quantity is zero");
                let qty = crate::strategy::full_set_units_to_qty(fact.qty_units_6dp);
                let principal = crate::strategy::full_set_units_to_qty(fact.principal_units_6dp);
                (
                    principal / qty,
                    qty,
                    principal,
                    crate::strategy::full_set_units_to_qty(fact.fee_units_6dp),
                )
            }
            None => (px, qty, total_notional, total_fee),
        };
        if confirmed_buy.is_none() && !(px.is_finite() && px > 0.0 && qty.is_finite() && qty > EPS)
        {
            return Ok(());
        }
        if confirmed_buy.is_none() && !(total_notional.is_finite() && total_notional > 0.0) {
            return Ok(());
        }
        let qty_units_6dp = match confirmed_buy {
            Some(fact) => fact.qty_units_6dp,
            None => f64_qty_to_units_6dp(qty)
                .expect("validated buy quantity converts to exact six-decimal units"),
        };
        let exact_position_units = confirmed_buy
            .map(|_| {
                let position = self.positions.get(token);
                let total = position
                    .map_or(0, |position| position.qty_units_6dp)
                    .checked_add(qty_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("confirmed BUY position quantity overflow"))?;
                let sellable = position
                    .map_or(0, |position| position.sellable_qty_units_6dp)
                    .checked_add(qty_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("confirmed BUY sellable quantity overflow"))?;
                Ok::<_, anyhow::Error>((total, sellable))
            })
            .transpose()?;
        if let Some(market_id) = market_id
            .map(str::trim)
            .filter(|v| !v.is_empty() && *v != "-")
        {
            self.token_market_hint
                .insert(token.to_string(), MarketId::new(market_id));
        }

        let mut pos = self
            .positions
            .remove(token)
            .unwrap_or_else(|| PositionState {
                token: token.to_string(),
                ..Default::default()
            });
        if pos.qty <= 0.0 {
            pos.opened_at_ms = Some(ts_ms);
            pos.qty = 0.0;
            pos.sellable_qty = 0.0;
            pos.avg_px = 0.0;
            pos.cost_basis = 0.0;
            pos.notional_basis = 0.0;
        }
        if pos.end_time_ms.is_none() {
            pos.end_time_ms = self.token_expiry_ms.get(token).copied();
        }
        let fee_per_unit = total_fee / qty;
        let lot_entry_px = canonical_basis.map_or(px, |basis| {
            crate::strategy::full_set_units_to_qty(basis) / qty
        });
        self.open_lots
            .entry(token.to_string())
            .or_default()
            .push_back(OpenLot {
                qty_units_6dp,
                qty,
                entry_px: lot_entry_px,
                // Canonical BUY basis already contains charged fees, costs and refunds.
                entry_fee_per_unit: canonical_basis.is_none().then_some(fee_per_unit.max(0.0)),
                engine_trade_id,
                entry_order_id,
                entry_fill_id,
                entry_tx_hash,
            });
        if let Some((total, sellable)) = exact_position_units {
            pos.qty_units_6dp = total;
            pos.sellable_qty_units_6dp = sellable;
            pos.qty = crate::strategy::full_set_units_to_qty(total);
            pos.sellable_qty = crate::strategy::full_set_units_to_qty(sellable);
        } else {
            pos.qty += qty;
            pos.sellable_qty += qty;
            sync_position_units_6dp(&mut pos);
        }
        pos.cost_basis += canonical_basis
            .map(crate::strategy::full_set_units_to_qty)
            .unwrap_or(total_notional);
        pos.notional_basis += total_notional;
        if pos.qty > 0.0 {
            pos.avg_px = pos.notional_basis / pos.qty;
        }
        pos.last_update_ms = ts_ms;
        if pos.qty > 0.0 {
            self.positions.insert(token.to_string(), pos);
        }
        Ok(())
    }

    pub(in crate::portfolio) fn apply_sell_position_change(
        &mut self,
        token: &str,
        market_id: MarketId,
        ts_ms: u64,
        px: f64,
        requested_qty: f64,
        confirmed_qty_units_6dp: Option<u128>,
        total_fee: Option<f64>,
        close_reason: OrderCloseReason,
        exit_order_id: Option<String>,
        exit_fill_id: Option<String>,
        exit_tx_hash: Option<String>,
        exit_lifecycle_cohort: Option<String>,
    ) {
        let close_reason = close_reason.to_string();
        let mut pos = self
            .positions
            .remove(token)
            .unwrap_or_else(|| PositionState {
                token: token.to_string(),
                ..Default::default()
            });
        if confirmed_qty_units_6dp.is_some() && pos.qty_units_6dp == 0
            || confirmed_qty_units_6dp.is_none() && pos.qty <= 0.0
        {
            pos.last_update_ms = ts_ms;
            return;
        }

        let sell_qty = confirmed_qty_units_6dp.map_or_else(
            || {
                requested_qty
                    .min(pos.sellable_qty.max(0.0))
                    .min(pos.qty.max(0.0))
            },
            crate::strategy::full_set_units_to_qty,
        );
        if confirmed_qty_units_6dp == Some(0)
            || confirmed_qty_units_6dp.is_none() && sell_qty <= EPS
        {
            pos.last_update_ms = ts_ms;
            if pos.qty_units_6dp > 0 {
                self.positions.insert(token.to_string(), pos);
            }
            return;
        }
        let sell_fee_per_unit = total_fee.map(|fee| fee / sell_qty.max(f64::EPSILON));
        let lots = self.open_lots.entry(token.to_string()).or_default();
        let mut remaining = sell_qty;
        let mut remaining_units_6dp = confirmed_qty_units_6dp;
        let mut closed_trades: Vec<ClosedTrade> = Vec::new();

        while remaining_units_6dp.is_some_and(|units| units > 0)
            || remaining_units_6dp.is_none() && remaining > EPS
        {
            let Some(front) = lots.front_mut() else {
                break;
            };
            let lot_qty = front.qty.max(0.0);
            let lot_qty_units_6dp = front.qty_units_6dp;
            if remaining_units_6dp.is_some() && lot_qty_units_6dp == 0
                || remaining_units_6dp.is_none() && lot_qty <= EPS
            {
                let _ = lots.pop_front();
                continue;
            }
            let matched_qty_units_6dp =
                remaining_units_6dp.map(|remaining_units| remaining_units.min(lot_qty_units_6dp));
            let matched_qty = matched_qty_units_6dp.map_or_else(
                || remaining.min(lot_qty),
                crate::strategy::full_set_units_to_qty,
            );
            let mut entry_cost_per_unit = front.entry_px;
            if let Some(entry_fee_per_unit) = front.entry_fee_per_unit {
                entry_cost_per_unit += entry_fee_per_unit;
            }
            let entry_total = entry_cost_per_unit * matched_qty;
            if let Some(matched_units) = matched_qty_units_6dp {
                pos.qty_units_6dp = pos.qty_units_6dp.saturating_sub(matched_units);
                pos.sellable_qty_units_6dp = pos
                    .sellable_qty_units_6dp
                    .saturating_sub(matched_units)
                    .min(pos.qty_units_6dp);
                pos.qty = crate::strategy::full_set_units_to_qty(pos.qty_units_6dp);
                pos.sellable_qty =
                    crate::strategy::full_set_units_to_qty(pos.sellable_qty_units_6dp);
            } else {
                pos.qty = (pos.qty - matched_qty).max(0.0);
                pos.sellable_qty = (pos.sellable_qty - matched_qty).max(0.0);
                sync_position_units_6dp(&mut pos);
            }
            pos.cost_basis = (pos.cost_basis - entry_total).max(0.0);
            pos.notional_basis = (pos.notional_basis - front.entry_px * matched_qty).max(0.0);
            closed_trades.push(ClosedTrade {
                seq: self.next_fill_seq,
                market_id: market_id.clone(),
                token: token.to_string(),
                engine_trade_id: exit_lifecycle_cohort
                    .clone()
                    .or_else(|| front.engine_trade_id.clone()),
                close_reason: close_reason.clone(),
                exit_fee: sell_fee_per_unit.map(|fee_per_unit| fee_per_unit * matched_qty),
                exit_order_id: exit_order_id.clone(),
                exit_fill_id: exit_fill_id.clone(),
                exit_tx_hash: exit_tx_hash.clone(),
            });

            if let Some(matched_units) = matched_qty_units_6dp {
                let remaining_lot_units = lot_qty_units_6dp.saturating_sub(matched_units);
                front.qty_units_6dp = remaining_lot_units;
                front.qty = crate::strategy::full_set_units_to_qty(remaining_lot_units);
                remaining_units_6dp = remaining_units_6dp
                    .map(|remaining_units| remaining_units.saturating_sub(matched_units));
                remaining = remaining_units_6dp
                    .map(crate::strategy::full_set_units_to_qty)
                    .unwrap_or(0.0);
            } else {
                front.qty = (front.qty - matched_qty).max(0.0);
                front.qty_units_6dp = f64_qty_to_units_6dp(front.qty).unwrap_or(0);
                remaining = (remaining - matched_qty).max(0.0);
            }
            if remaining_units_6dp.is_some()
                && lot_qty_units_6dp == matched_qty_units_6dp.unwrap_or(0)
                || remaining_units_6dp.is_none() && front.qty <= EPS
            {
                let _ = lots.pop_front();
            }
        }

        if remaining_units_6dp.is_some_and(|units| units > 0)
            || remaining_units_6dp.is_none() && remaining > EPS
        {
            if let Some(remaining_units) = remaining_units_6dp {
                remaining = crate::strategy::full_set_units_to_qty(remaining_units);
            }
            let avg = if pos.qty > EPS {
                pos.avg_px
            } else {
                px.max(0.0)
            };
            let entry_total = avg * remaining;
            if let Some(remaining_units) = remaining_units_6dp {
                pos.qty_units_6dp = pos.qty_units_6dp.saturating_sub(remaining_units);
                pos.sellable_qty_units_6dp = pos
                    .sellable_qty_units_6dp
                    .saturating_sub(remaining_units)
                    .min(pos.qty_units_6dp);
                pos.qty = crate::strategy::full_set_units_to_qty(pos.qty_units_6dp);
                pos.sellable_qty =
                    crate::strategy::full_set_units_to_qty(pos.sellable_qty_units_6dp);
            } else {
                pos.qty = (pos.qty - remaining).max(0.0);
                pos.sellable_qty = pos.sellable_qty.min(pos.qty).max(0.0);
                sync_position_units_6dp(&mut pos);
            }
            pos.cost_basis = (pos.cost_basis - entry_total).max(0.0);
            pos.notional_basis = (pos.notional_basis - avg * remaining).max(0.0);
            tracing::warn!(
                target: "portfolio.lots",
                token,
                qty = remaining,
                "closed_quantity_missing_entry_fee_lineage"
            );
        }

        for trade in coalesce_closed_trades(closed_trades) {
            self.record_closed_trade(trade);
        }

        if confirmed_qty_units_6dp.is_some() && pos.qty_units_6dp == 0
            || confirmed_qty_units_6dp.is_none() && pos.qty <= EPS
        {
            pos.qty = 0.0;
            pos.sellable_qty = 0.0;
            pos.qty_units_6dp = 0;
            pos.sellable_qty_units_6dp = 0;
            pos.avg_px = 0.0;
            pos.cost_basis = 0.0;
            pos.notional_basis = 0.0;
            pos.opened_at_ms = None;
        } else if pos.notional_basis.is_finite() && pos.notional_basis > 0.0 {
            pos.avg_px = pos.notional_basis / pos.qty;
        }
        pos.last_update_ms = ts_ms;
        if pos.qty_units_6dp > 0 {
            self.positions.insert(token.to_string(), pos);
        }
    }

    pub(in crate::portfolio) fn refresh_position_aggregates(&mut self) {
        self.positions.retain(|_, p| p.qty_units_6dp > 0);
        self.open_lots.retain(|_, lots| !lots.is_empty());
        let total_cost_basis = self
            .positions
            .values()
            .map(|p| p.cost_basis)
            .filter(|v| v.is_finite() && *v > 0.0)
            .sum();
        self.total_cost_basis = if total_cost_basis == 0.0 {
            0.0
        } else {
            total_cost_basis
        };
    }
}

fn coalesce_closed_trades(closed_trades: Vec<ClosedTrade>) -> Vec<ClosedTrade> {
    let mut coalesced: Vec<ClosedTrade> = Vec::new();
    for trade in closed_trades {
        let already_present = coalesced.iter().any(|candidate| {
            candidate.market_id == trade.market_id
                && candidate.token == trade.token
                && candidate.engine_trade_id == trade.engine_trade_id
                && candidate.exit_fill_id == trade.exit_fill_id
        });
        if !already_present {
            coalesced.push(trade);
        }
    }
    coalesced
}

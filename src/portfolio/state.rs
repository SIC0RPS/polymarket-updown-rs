use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_client_sdk::types::Decimal;
use std::str::FromStr as _;

use crate::polymarket::identity::MarketId;
use bbwick_engine::state::{fingerprint_hash, Fingerprint};

use super::closed_trades::{ClosedTrade, TerminalCloseKey};
use super::fills::{ConfirmedLifecycleSellFill, FillDedupeKey, QuarantinedFill, QueuedFill};
use super::lots::OpenLot;
use crate::trade_session_ledger::ConfirmedCtfBuyFact;

pub(in crate::portfolio) const EPS: f64 = 0.005;

#[derive(Debug, Clone, Default)]
pub struct PositionState {
    pub token: String,
    pub qty_units_6dp: u128,
    pub qty: f64,
    pub sellable_qty_units_6dp: u128,
    pub sellable_qty: f64,
    pub avg_px: f64,
    pub cost_basis: f64,
    pub notional_basis: f64,
    pub opened_at_ms: Option<u64>,
    pub last_update_ms: u64,
    pub mark_price: Option<f64>,
    pub unrealized_pnl_units_6dp: Option<i128>,
    pub remaining_basis_units_6dp: Option<u128>,
    pub mark_updated_at_ms: Option<u64>,
    pub end_time_ms: Option<u64>,
    pub resolved_redeemable: bool,
    pub mergeable: bool,
}

#[derive(Debug, Clone)]
pub struct ExpectedCtfPosition {
    pub token: String,
    pub market_id: MarketId,
    pub end_time_ms: Option<u64>,
    pub confirmed_entry_outlay_units_6dp: Option<u128>,
    pub confirmed_materialized_ts_ms: Option<u64>,
    pub confirmed_token_created_qty_units_6dp: Option<u128>,
    pub confirmed_token_principal_units_6dp: Option<u128>,
    pub confirmed_token_entry_fee_units_6dp: Option<u128>,
    pub confirmed_token_approved_direct_cost_units_6dp: Option<u128>,
    pub engine_trade_id: Option<String>,
    pub confirmed_entry_tx_hash: Option<String>,
    pub require_positive_balance: bool,
    pub min_qty_units_6dp: Option<u128>,
    pub portfolio_actionable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::portfolio) struct ConfirmedCtfTokenEntryBasis {
    pub(in crate::portfolio) token: String,
    pub(in crate::portfolio) created_qty_units_6dp: u128,
    pub(in crate::portfolio) principal_units_6dp: u128,
    pub(in crate::portfolio) entry_fee_units_6dp: u128,
    pub(in crate::portfolio) approved_direct_cost_units_6dp: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::portfolio) struct ConfirmedCtfSplitOutlay {
    pub(in crate::portfolio) market_id: MarketId,
    pub(in crate::portfolio) transaction_id: String,
    pub(in crate::portfolio) outlay_units_6dp: u128,
    pub(in crate::portfolio) materialized_ts_ms: u64,
    pub(in crate::portfolio) first_token_basis: ConfirmedCtfTokenEntryBasis,
    pub(in crate::portfolio) second_token_basis: ConfirmedCtfTokenEntryBasis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::portfolio) enum ConfirmedCtfEntryOutlay {
    Split(ConfirmedCtfSplitOutlay),
    Buy(BTreeMap<String, ConfirmedCtfBuyFact>),
}

#[derive(Debug, Clone, Default)]
pub struct FillQualityStats {
    pub fills_total: u64,
    pub top_of_book_fills: u64,
    pub queue_ahead_sum: f64,
    pub queue_ahead_samples: u64,
    pub time_to_fill_ms_sum: u128,
    pub time_to_fill_samples: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PortfolioState {
    pub updated_at_ms: u64,
    pub revision: u64,
    pub positions_fingerprint: Fingerprint,
    pub total_cost_basis: f64,
    pub marked_remaining_value_units_6dp: Option<u128>,
    pub receipt_realized_pnl_units_6dp: Option<i128>,
    pub total_unrealized_pnl_units_6dp: Option<i128>,
    pub total_pnl_units_6dp: Option<i128>,
    pub market_marked_unrealized_pnl_units_6dp: Option<i128>,
    pub market_marked_total_pnl_units_6dp: Option<i128>,
    pub valuation_canonical_revision: Option<u64>,
    pub valuation_odds_revision: Option<u64>,
    pub valued_at_ms: Option<u64>,
    pub positions: HashMap<String, PositionState>,
    pub fills: VecDeque<QueuedFill>,
    pub confirmed_lifecycle_sell_fills: BTreeMap<(String, String), ConfirmedLifecycleSellFill>,
    pub quarantined_fills: VecDeque<QuarantinedFill>,
    pub closed_trades: VecDeque<ClosedTrade>,
    pub fill_quality: FillQualityStats,
    pub next_fill_seq: u64,
    pub(in crate::portfolio) seen_fill_keys: HashSet<FillDedupeKey>,
    pub(in crate::portfolio) seen_terminal_close_keys: HashSet<TerminalCloseKey>,
    pub token_market_hint: HashMap<String, MarketId>,
    pub(in crate::portfolio) open_lots: HashMap<String, VecDeque<OpenLot>>,
    pub(in crate::portfolio) confirmed_ctf_split_lot_identities: HashSet<(String, String, String)>,
    pub(in crate::portfolio) confirmed_ctf_split_outlays: BTreeMap<String, ConfirmedCtfEntryOutlay>,
    pub token_expiry_ms: HashMap<String, u64>,
}

impl PortfolioState {
    pub(crate) fn note_runtime_mutation(&mut self, updated_at_ms: u64) {
        self.refresh_position_aggregates();
        self.refresh_positions_metadata(updated_at_ms);
    }

    pub(in crate::portfolio) fn refresh_positions_metadata(&mut self, updated_at_ms: u64) {
        self.updated_at_ms = updated_at_ms;
        self.revision = self.revision.saturating_add(1);
        self.positions_fingerprint = self.compute_positions_fingerprint();
    }

    pub(super) fn compute_positions_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = Fingerprint::default();
        for (token, position) in &self.positions {
            fingerprint.insert_hash(fingerprint_hash(&(
                token.as_str(),
                position.qty_units_6dp,
                position.qty.to_bits(),
                position.sellable_qty_units_6dp,
                position.sellable_qty.to_bits(),
                position.avg_px.to_bits(),
                position.cost_basis.to_bits(),
                position.opened_at_ms.unwrap_or_default(),
                position.end_time_ms.unwrap_or_default(),
                position.resolved_redeemable,
                position.mergeable,
            )));
        }
        for (token, expiry_ms) in &self.token_expiry_ms {
            fingerprint.insert_hash(fingerprint_hash(&(token.as_str(), *expiry_ms)));
        }
        for ((cohort, fill_id), fill) in &self.confirmed_lifecycle_sell_fills {
            fingerprint.insert_hash(fingerprint_hash(&(
                cohort.as_str(),
                fill_id.as_str(),
                &fill.receipt_identity,
                fill.market_id.as_str(),
                fill.token.as_str(),
                fill.intent_id,
                fill.qty_units_6dp,
                fill.gross_proceeds_units_6dp,
                fill.fee_units_6dp,
                fill.confirmed_at_ms,
                fill.close_reason,
            )));
        }
        for (cohort, outlay) in &self.confirmed_ctf_split_outlays {
            outlay.add_fingerprint(cohort, &mut fingerprint);
        }
        fingerprint.insert_hash(fingerprint_hash(&("next_fill_seq", self.next_fill_seq)));
        fingerprint
    }

    pub fn hydrate_expected_ctf_balances(
        &mut self,
        expected_positions: &[ExpectedCtfPosition],
        onchain_position_balances: &HashMap<String, u128>,
        now_ms: u64,
    ) -> Result<usize> {
        let mut hydrated = 0usize;
        let mut changed = self.reconcile_confirmed_ctf_split_outlays(expected_positions)?;
        for expected in expected_positions {
            let token = expected.token.trim();
            if token.is_empty() {
                continue;
            }
            let qty_units_6dp =
                onchain_position_balances
                    .get(token)
                    .copied()
                    .with_context(|| {
                        format!("missing on-chain CTF balance for expected token {token}")
                    })?;
            let qty = crate::strategy::full_set_units_to_qty(qty_units_6dp);
            if expected.require_positive_balance {
                anyhow::ensure!(
                    qty_units_6dp > 0,
                    "expected positive on-chain CTF balance for token {token}, got {qty:.6}"
                );
            }
            if let Some(min_qty_units_6dp) = expected.min_qty_units_6dp {
                anyhow::ensure!(
                    qty_units_6dp >= min_qty_units_6dp,
                    "expected on-chain CTF balance for token {token} below submitted share quantity: got_units={qty_units_6dp} min_units={min_qty_units_6dp}"
                );
            }
            if qty_units_6dp == 0 {
                if self.positions.remove(token).is_some() {
                    self.open_lots.remove(token);
                    self.token_market_hint.remove(token);
                    self.token_expiry_ms.remove(token);
                    changed = true;
                }
                continue;
            }

            let (avg_px, cost_basis, notional_basis) = if let Some(principal_units_6dp) =
                expected.confirmed_token_principal_units_6dp
            {
                anyhow::ensure!(
                    expected.require_positive_balance && expected.portfolio_actionable,
                    "confirmed CTF split token basis is not actionable token={token}"
                );
                let confirmed_qty_units_6dp = expected
                    .min_qty_units_6dp
                    .context("confirmed CTF split token basis is missing exact quantity")?;
                anyhow::ensure!(
                        qty_units_6dp == confirmed_qty_units_6dp,
                        "confirmed CTF split token basis quantity mismatch token={token} balance_units={qty_units_6dp} confirmed_units={confirmed_qty_units_6dp}"
                    );
                let cost_basis = crate::strategy::full_set_units_to_qty(principal_units_6dp);
                let avg_px = cost_basis / qty;
                (avg_px, cost_basis, cost_basis)
            } else {
                let position = self.positions.get(token).with_context(|| {
                    format!(
                        "compact confirmed CTF split receipt has no position basis token={token}"
                    )
                })?;
                anyhow::ensure!(
                        position.qty_units_6dp == qty_units_6dp
                            && position.cost_basis.is_finite()
                            && position.cost_basis >= 0.0
                            && position.notional_basis.is_finite()
                            && position.notional_basis >= 0.0,
                        "compact confirmed CTF split receipt conflicts with position basis token={token}"
                    );
                (
                    position.avg_px,
                    position.cost_basis,
                    position.notional_basis,
                )
            };

            changed |= self.reconcile_expected_ctf_lots(expected, qty_units_6dp, avg_px)?;
            let next_end_time_ms = expected
                .end_time_ms
                .or_else(|| self.token_expiry_ms.get(token).copied());

            changed |= self.token_market_hint.get(token) != Some(&expected.market_id);
            self.token_market_hint
                .insert(token.to_string(), expected.market_id.clone());
            if let Some(end_time_ms) = next_end_time_ms {
                changed |= self.token_expiry_ms.get(token) != Some(&end_time_ms);
                self.token_expiry_ms.insert(token.to_string(), end_time_ms);
            }

            let sellable_qty = if expected.portfolio_actionable {
                qty
            } else {
                self.positions
                    .get(token)
                    .map(|position| position.sellable_qty.min(qty).max(0.0))
                    .unwrap_or(0.0)
            };
            let sellable_qty_units_6dp = if expected.portfolio_actionable {
                qty_units_6dp
            } else {
                self.positions
                    .get(token)
                    .map(|position| position.sellable_qty_units_6dp.min(qty_units_6dp))
                    .unwrap_or(0)
            };
            match self.positions.get_mut(token) {
                Some(position) => {
                    changed |= position.qty_units_6dp != qty_units_6dp;
                    changed |= (position.qty - qty).abs() > EPS;
                    changed |= position.sellable_qty_units_6dp != sellable_qty_units_6dp;
                    changed |= (position.sellable_qty - sellable_qty).abs() > EPS;
                    changed |= position.avg_px.to_bits() != avg_px.to_bits();
                    changed |= position.cost_basis.to_bits() != cost_basis.to_bits();
                    changed |= position.notional_basis.to_bits() != notional_basis.to_bits();
                    changed |= position.end_time_ms != next_end_time_ms;
                    position.qty_units_6dp = qty_units_6dp;
                    position.qty = qty;
                    position.sellable_qty_units_6dp = sellable_qty_units_6dp;
                    position.sellable_qty = sellable_qty;
                    position.avg_px = avg_px;
                    position.cost_basis = cost_basis;
                    position.notional_basis = notional_basis;
                    position.last_update_ms = now_ms;
                    position.end_time_ms = next_end_time_ms;
                }
                None => {
                    self.positions.insert(
                        token.to_string(),
                        PositionState {
                            token: token.to_string(),
                            qty_units_6dp,
                            qty,
                            sellable_qty_units_6dp,
                            sellable_qty,
                            avg_px,
                            cost_basis,
                            notional_basis,
                            opened_at_ms: Some(now_ms),
                            last_update_ms: now_ms,
                            mark_price: None,
                            unrealized_pnl_units_6dp: None,
                            remaining_basis_units_6dp: None,
                            mark_updated_at_ms: None,
                            end_time_ms: next_end_time_ms,
                            resolved_redeemable: false,
                            mergeable: false,
                        },
                    );
                    hydrated = hydrated.saturating_add(1);
                    changed = true;
                }
            }
        }

        if changed {
            self.note_runtime_mutation(now_ms);
        }
        Ok(hydrated)
    }

    pub fn position(&self, token: &str) -> Option<&PositionState> {
        self.positions.get(token)
    }
}

pub(in crate::portfolio) fn decimal_qty_to_units_6dp(value: Decimal) -> Option<u128> {
    if value.is_sign_negative() {
        return None;
    }
    let normalized = value.normalize();
    let mantissa = normalized.mantissa();
    let scale = normalized.scale();
    let units = if scale <= 6 {
        mantissa.checked_mul(10_i128.pow(6 - scale))?
    } else {
        let divisor = 10_i128.pow(scale - 6);
        if mantissa % divisor != 0 {
            return None;
        }
        mantissa / divisor
    };
    u128::try_from(units).ok()
}

pub(crate) fn f64_qty_to_units_6dp(value: f64) -> Option<u128> {
    if !(value.is_finite() && value > 0.0) {
        return None;
    }
    Decimal::from_str(&format!("{value:.6}"))
        .ok()
        .and_then(decimal_qty_to_units_6dp)
}

pub(in crate::portfolio) fn sync_position_units_6dp(position: &mut PositionState) {
    position.qty_units_6dp = f64_qty_to_units_6dp(position.qty).unwrap_or(0);
    position.sellable_qty_units_6dp = f64_qty_to_units_6dp(position.sellable_qty).unwrap_or(0);
}

pub(in crate::portfolio) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

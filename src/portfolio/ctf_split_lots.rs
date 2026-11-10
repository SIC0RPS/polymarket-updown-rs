use std::collections::BTreeMap;

use anyhow::{Context, Result};

use super::lots::OpenLot;
use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{ConfirmedCtfBuyFact, ManagedSellReceiptIdentity};
use bbwick_engine::state::{fingerprint_hash, Fingerprint};

use super::state::{
    ConfirmedCtfEntryOutlay, ConfirmedCtfSplitOutlay, ConfirmedCtfTokenEntryBasis,
    ExpectedCtfPosition, PortfolioState,
};

#[derive(Debug, Clone)]
pub(crate) struct ConfirmedCtfSplitLot {
    pub(crate) qty_units_6dp: u128,
    pub(crate) entry_outlay_units_6dp: u128,
    pub(crate) materialized_ts_ms: u64,
    pub(crate) entry_tx_hash: String,
}

impl PortfolioState {
    pub(super) fn reconcile_confirmed_ctf_split_outlays(
        &mut self,
        expected_positions: &[ExpectedCtfPosition],
    ) -> Result<bool> {
        let mut by_cohort = BTreeMap::<&str, Vec<&ExpectedCtfPosition>>::new();
        for expected in expected_positions {
            if !expected.require_positive_balance || !expected.portfolio_actionable {
                continue;
            }
            let cohort = expected
                .engine_trade_id
                .as_deref()
                .context("confirmed CTF split is missing cohort identity")?;
            by_cohort.entry(cohort).or_default().push(expected);
        }

        let mut changed = false;
        for (cohort, mut expected) in by_cohort {
            anyhow::ensure!(
                expected.len() == 2,
                "confirmed CTF split does not contain exactly two tokens cohort={cohort}"
            );
            expected.sort_unstable_by(|left, right| left.token.cmp(&right.token));
            let first = expected[0];
            let second = expected[1];
            anyhow::ensure!(
                !first.token.is_empty() && first.token != second.token,
                "confirmed CTF split token identity is invalid cohort={cohort}"
            );
            let transaction_id = first
                .confirmed_entry_tx_hash
                .as_deref()
                .context("confirmed CTF split is missing transaction identity")?;
            let outlay_units_6dp = first
                .confirmed_entry_outlay_units_6dp
                .context("confirmed CTF split is missing exact entry outlay")?;
            let materialized_ts_ms = first
                .confirmed_materialized_ts_ms
                .context("confirmed CTF split is missing materialization timestamp")?;
            anyhow::ensure!(
                materialized_ts_ms > 0,
                "confirmed CTF split materialization timestamp is zero cohort={cohort}"
            );
            anyhow::ensure!(
                second.market_id == first.market_id
                    && second.engine_trade_id.as_deref() == Some(cohort)
                    && second.confirmed_entry_tx_hash.as_deref() == Some(transaction_id)
                    && second.confirmed_entry_outlay_units_6dp == Some(outlay_units_6dp)
                    && second.confirmed_materialized_ts_ms == Some(materialized_ts_ms),
                "confirmed CTF split cross-token identity conflicts cohort={cohort}"
            );
            let token_basis =
                |position: &ExpectedCtfPosition| -> Result<ConfirmedCtfTokenEntryBasis> {
                    let created_qty_units_6dp = position
                        .confirmed_token_created_qty_units_6dp
                        .context("confirmed CTF split is missing token creation quantity")?;
                    anyhow::ensure!(
                        created_qty_units_6dp > 0
                            && position.min_qty_units_6dp == Some(created_qty_units_6dp),
                        "confirmed CTF split token creation quantity is invalid cohort={cohort} token={}",
                        position.token
                    );
                    Ok(ConfirmedCtfTokenEntryBasis {
                        token: position.token.clone(),
                        created_qty_units_6dp,
                        principal_units_6dp: position
                            .confirmed_token_principal_units_6dp
                            .context("confirmed CTF split is missing token principal allocation")?,
                        entry_fee_units_6dp: position
                            .confirmed_token_entry_fee_units_6dp
                            .context("confirmed CTF split is missing token entry-fee allocation")?,
                        approved_direct_cost_units_6dp: position
                            .confirmed_token_approved_direct_cost_units_6dp
                            .context(
                                "confirmed CTF split is missing token direct-cost allocation",
                            )?,
                    })
                };
            let first_token_basis = token_basis(first)?;
            let second_token_basis = token_basis(second)?;
            anyhow::ensure!(
                first_token_basis.created_qty_units_6dp == second_token_basis.created_qty_units_6dp,
                "confirmed CTF split token creation quantities differ cohort={cohort}"
            );
            let allocated_outlay_units_6dp = first_token_basis
                .principal_units_6dp
                .checked_add(second_token_basis.principal_units_6dp)
                .and_then(|value| value.checked_add(first_token_basis.entry_fee_units_6dp))
                .and_then(|value| value.checked_add(second_token_basis.entry_fee_units_6dp))
                .and_then(|value| {
                    value.checked_add(first_token_basis.approved_direct_cost_units_6dp)
                })
                .and_then(|value| {
                    value.checked_add(second_token_basis.approved_direct_cost_units_6dp)
                })
                .context("confirmed CTF split allocated entry outlay overflow")?;
            anyhow::ensure!(
                outlay_units_6dp > 0 && allocated_outlay_units_6dp == outlay_units_6dp,
                "confirmed CTF split allocated entry outlay differs cohort={cohort}"
            );
            let fact = ConfirmedCtfEntryOutlay::Split(ConfirmedCtfSplitOutlay {
                market_id: first.market_id.clone(),
                transaction_id: transaction_id.to_string(),
                outlay_units_6dp,
                materialized_ts_ms,
                first_token_basis,
                second_token_basis,
            });
            if let Some(existing) = self.confirmed_ctf_split_outlays.get(cohort) {
                anyhow::ensure!(
                    existing == &fact,
                    "confirmed CTF split entry outlay conflicts with existing cohort={cohort}"
                );
            } else {
                self.confirmed_ctf_split_outlays
                    .insert(cohort.to_string(), fact);
                changed = true;
            }
        }
        Ok(changed)
    }

    pub fn open_cohort_committed_outlay_units_6dp(&self) -> Result<BTreeMap<MarketId, u128>> {
        self.cohort_committed_outlay_units_6dp(None)
    }

    pub fn active_cohort_committed_outlay_units_6dp(
        &self,
        now_ms: u64,
    ) -> Result<BTreeMap<MarketId, u128>> {
        self.cohort_committed_outlay_units_6dp(Some(now_ms))
    }

    fn cohort_committed_outlay_units_6dp(
        &self,
        active_at_ms: Option<u64>,
    ) -> Result<BTreeMap<MarketId, u128>> {
        let mut by_market = BTreeMap::<MarketId, u128>::new();
        for (cohort, fact) in &self.confirmed_ctf_split_outlays {
            let mut open_lots = self
                .open_lots
                .iter()
                .flat_map(|(token, lots)| lots.iter().map(move |lot| (token, lot)))
                .filter(|(_, lot)| {
                    lot.qty_units_6dp > 0 && lot.engine_trade_id.as_deref() == Some(cohort)
                });
            let Some(first) = open_lots.next() else {
                continue;
            };
            let mut active = active_at_ms.is_none();
            for (token, lot) in std::iter::once(first).chain(open_lots) {
                anyhow::ensure!(
                    fact.matches_lot(token, lot),
                    "open CTF lot receipt conflicts with cohort outlay cohort={cohort}"
                );
                if let Some(now_ms) = active_at_ms {
                    let expiry_ms = self.token_expiry_ms.get(token).copied().or_else(|| {
                        self.positions
                            .get(token)
                            .and_then(|position| position.end_time_ms)
                    });
                    active |= expiry_ms.is_none_or(|expiry_ms| expiry_ms > now_ms);
                }
            }
            if !active {
                continue;
            }
            let next = by_market
                .get(fact.market_id())
                .copied()
                .unwrap_or(0)
                .checked_add(fact.outlay_units_6dp()?)
                .context("open CTF cohort outlay overflow")?;
            by_market.insert(fact.market_id().clone(), next);
        }
        Ok(by_market)
    }

    fn confirmed_ctf_split_lot(&self, token: &str, cohort: &str) -> Option<ConfirmedCtfSplitLot> {
        let lots = self.open_lots.get(token)?;
        let mut matching = lots
            .iter()
            .filter(|lot| lot.engine_trade_id.as_deref() == Some(cohort));
        let lot = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        let entry_tx_hash = lot.entry_tx_hash.as_deref()?.trim();
        if entry_tx_hash.is_empty() {
            return None;
        }
        let split = self
            .confirmed_ctf_split_outlays
            .get(cohort)
            .and_then(ConfirmedCtfEntryOutlay::split)
            .filter(|fact| fact.transaction_id == entry_tx_hash)?;
        Some(ConfirmedCtfSplitLot {
            qty_units_6dp: lot.qty_units_6dp,
            entry_outlay_units_6dp: split.outlay_units_6dp,
            materialized_ts_ms: split.materialized_ts_ms,
            entry_tx_hash: entry_tx_hash.to_string(),
        })
    }

    pub(crate) fn confirmed_ctf_split_pair(
        &self,
        market_id: &MarketId,
        up_token: &str,
        down_token: &str,
    ) -> Option<(String, ConfirmedCtfSplitLot, ConfirmedCtfSplitLot)> {
        let mut matching = self
            .confirmed_ctf_split_outlays
            .iter()
            .filter(|(_, fact)| {
                fact.split()
                    .is_some_and(|split| &split.market_id == market_id)
            })
            .filter_map(|(cohort, _)| {
                let up = self.confirmed_ctf_split_lot(up_token, cohort)?;
                let down = self.confirmed_ctf_split_lot(down_token, cohort)?;
                Some((cohort.clone(), up, down))
            });
        let pair = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        Some(pair)
    }

    pub(crate) fn confirmed_ctf_cohort_qty_units_6dp(
        &self,
        token: &str,
        cohort: &str,
    ) -> anyhow::Result<u128> {
        let split = self
            .confirmed_ctf_split_outlays
            .get(cohort)
            .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort outlay is absent"))?;
        self.open_lots
            .get(token)
            .into_iter()
            .flatten()
            .filter(|lot| lot.engine_trade_id.as_deref() == Some(cohort))
            .try_fold(0u128, |total, lot| {
                anyhow::ensure!(
                    split.matches_lot(token, lot),
                    "confirmed CTF lot receipt conflicts with cohort outlay"
                );
                total
                    .checked_add(lot.qty_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort quantity overflow"))
            })
    }

    pub(crate) fn confirmed_ctf_first_basis_units_6dp(
        &self,
        token: &str,
        cohort: &str,
        qty_units_6dp: u128,
        pending_qty_units_6dp: u128,
    ) -> anyhow::Result<(u128, u128)> {
        anyhow::ensure!(
            qty_units_6dp > 0,
            "confirmed CTF First basis quantity is zero"
        );
        let split = self
            .confirmed_ctf_split_outlays
            .get(cohort)
            .and_then(ConfirmedCtfEntryOutlay::split)
            .context("confirmed CTF split cohort outlay is absent")?;
        let token_basis = if split.first_token_basis.token == token {
            &split.first_token_basis
        } else if split.second_token_basis.token == token {
            &split.second_token_basis
        } else {
            anyhow::bail!("confirmed CTF token is outside cohort entry");
        };
        let remaining_qty_units_6dp = self.confirmed_ctf_cohort_qty_units_6dp(token, cohort)?;
        let confirmed_qty_units_6dp = token_basis
            .created_qty_units_6dp
            .checked_sub(remaining_qty_units_6dp)
            .context("confirmed CTF remaining quantity exceeds creation")?;
        let before_units_6dp = confirmed_qty_units_6dp
            .checked_add(pending_qty_units_6dp)
            .context("confirmed CTF pending First quantity overflow")?;
        let after_units_6dp = before_units_6dp
            .checked_add(qty_units_6dp)
            .context("confirmed CTF First quantity overflow")?;
        anyhow::ensure!(
            before_units_6dp <= token_basis.created_qty_units_6dp
                && after_units_6dp <= token_basis.created_qty_units_6dp,
            "confirmed and pending CTF First quantity exceeds creation"
        );
        let allocation_increment = |total| {
            crate::ctf_accounting::allocation_increment(
                total,
                before_units_6dp,
                after_units_6dp,
                token_basis.created_qty_units_6dp,
            )
        };
        let principal_units_6dp = allocation_increment(token_basis.principal_units_6dp)?;
        let entry_fee_units_6dp = allocation_increment(token_basis.entry_fee_units_6dp)?;
        let direct_cost_units_6dp =
            allocation_increment(token_basis.approved_direct_cost_units_6dp)?;
        let allocated_basis_units_6dp = principal_units_6dp
            .checked_add(entry_fee_units_6dp)
            .and_then(|value| value.checked_add(direct_cost_units_6dp))
            .context("confirmed CTF First basis allocation overflow")?;
        Ok((allocated_basis_units_6dp, remaining_qty_units_6dp))
    }

    pub(crate) fn reduce_confirmed_ctf_cohort_lot(
        &mut self,
        token: &str,
        cohort: &str,
        qty_units_6dp: u128,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            qty_units_6dp > 0,
            "confirmed CTF cohort lot reduction quantity is zero"
        );
        let entry = self
            .confirmed_ctf_split_outlays
            .get(cohort)
            .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort outlay is absent"))?;
        let lots = self
            .open_lots
            .get_mut(token)
            .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort lots are absent"))?;
        let mut matching_qty_units_6dp = 0u128;
        let mut matching_lot_count = 0usize;
        for lot in lots
            .iter()
            .filter(|lot| lot.engine_trade_id.as_deref() == Some(cohort))
        {
            matching_lot_count = matching_lot_count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort lot count overflow"))?;
            anyhow::ensure!(
                entry.matches_lot(token, lot),
                "confirmed CTF lot receipt conflicts with cohort outlay"
            );
            matching_qty_units_6dp = matching_qty_units_6dp
                .checked_add(lot.qty_units_6dp)
                .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort quantity overflow"))?;
        }
        anyhow::ensure!(matching_lot_count > 0, "confirmed CTF cohort lot is absent");
        anyhow::ensure!(
            matching_qty_units_6dp >= qty_units_6dp,
            "confirmed CTF cohort lot quantity is insufficient"
        );

        let mut remaining_units_6dp = qty_units_6dp;
        for lot in lots
            .iter_mut()
            .filter(|lot| lot.engine_trade_id.as_deref() == Some(cohort))
        {
            let reduced_units_6dp = lot.qty_units_6dp.min(remaining_units_6dp);
            lot.qty_units_6dp = lot
                .qty_units_6dp
                .checked_sub(reduced_units_6dp)
                .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort lot reduction underflow"))?;
            lot.qty = crate::strategy::full_set_units_to_qty(lot.qty_units_6dp);
            remaining_units_6dp = remaining_units_6dp
                .checked_sub(reduced_units_6dp)
                .ok_or_else(|| anyhow::anyhow!("confirmed CTF cohort reduction underflow"))?;
            if remaining_units_6dp == 0 {
                break;
            }
        }
        anyhow::ensure!(
            remaining_units_6dp == 0,
            "confirmed CTF cohort lot reduction is incomplete"
        );
        lots.retain(|lot| lot.qty_units_6dp > 0);
        Ok(())
    }
}

impl ConfirmedCtfEntryOutlay {
    pub(super) fn split(&self) -> Option<&ConfirmedCtfSplitOutlay> {
        match self {
            Self::Split(split) => Some(split),
            Self::Buy(_) => None,
        }
    }

    pub(super) fn market_id(&self) -> &MarketId {
        match self {
            Self::Split(split) => &split.market_id,
            Self::Buy(receipts) => {
                &receipts
                    .first_key_value()
                    .expect("BUY entry has receipts")
                    .1
                    .market_id
            }
        }
    }

    pub(super) fn outlay_units_6dp(&self) -> Result<u128> {
        match self {
            Self::Split(split) => Ok(split.outlay_units_6dp),
            Self::Buy(receipts) => {
                let cash_delta = receipts.values().try_fold(0i128, |total, fact| {
                    total
                        .checked_add(fact.collateral_cash_delta_units_6dp)
                        .context("BUY entry cash delta overflow")
                })?;
                Ok(cash_delta
                    .checked_neg()
                    .context("BUY entry debit overflow")?
                    .max(0) as u128)
            }
        }
    }

    pub(super) fn matches_lot(&self, token: &str, lot: &OpenLot) -> bool {
        match self {
            Self::Split(split) => {
                (split.first_token_basis.token == token || split.second_token_basis.token == token)
                    && lot.entry_tx_hash.as_deref() == Some(split.transaction_id.as_str())
            }
            Self::Buy(receipts) => lot
                .entry_fill_id
                .as_ref()
                .and_then(|fill_id| receipts.get(fill_id))
                .is_some_and(|fact| {
                    fact.token == token
                        && lot.entry_order_id.as_deref() == Some(fact.order_id.as_str())
                        && lot.qty_units_6dp <= fact.qty_units_6dp
                        && match &fact.receipt_identity {
                            ManagedSellReceiptIdentity::ExchangeV2 {
                                transaction_hash, ..
                            } => lot.entry_tx_hash.as_deref() == Some(transaction_hash.as_str()),
                            ManagedSellReceiptIdentity::CapturedSimulation { .. } => true,
                        }
                }),
        }
    }

    pub(super) fn add_fingerprint(&self, cohort: &str, fingerprint: &mut Fingerprint) {
        match self {
            Self::Split(outlay) => fingerprint.insert_hash(fingerprint_hash(&(
                cohort,
                outlay.market_id.as_str(),
                outlay.transaction_id.as_str(),
                outlay.outlay_units_6dp,
                outlay.materialized_ts_ms,
                (
                    &outlay.first_token_basis.token,
                    outlay.first_token_basis.created_qty_units_6dp,
                    outlay.first_token_basis.principal_units_6dp,
                    outlay.first_token_basis.entry_fee_units_6dp,
                    outlay.first_token_basis.approved_direct_cost_units_6dp,
                ),
                (
                    &outlay.second_token_basis.token,
                    outlay.second_token_basis.created_qty_units_6dp,
                    outlay.second_token_basis.principal_units_6dp,
                    outlay.second_token_basis.entry_fee_units_6dp,
                    outlay.second_token_basis.approved_direct_cost_units_6dp,
                ),
            ))),
            Self::Buy(receipts) => {
                for fact in receipts.values() {
                    fingerprint.insert_hash(fingerprint_hash(&(
                        cohort,
                        &fact.fill_id,
                        &fact.receipt_identity,
                        &fact.order_id,
                        fact.market_id.as_str(),
                        &fact.token,
                        fact.qty_units_6dp,
                        (
                            fact.principal_units_6dp,
                            fact.fee_units_6dp,
                            fact.approved_direct_cost_units_6dp,
                            fact.collateral_refund_units_6dp,
                            fact.collateral_cash_delta_units_6dp,
                            fact.confirmed_at_ms,
                        ),
                        (
                            fact.authorization.intent_id,
                            fact.authorization.condition_id,
                            fact.authorization.up_token,
                            fact.authorization.down_token,
                            fact.authorization.symbol.as_str(),
                            fact.authorization.selected_up,
                            fact.authorization.prediction_at_ms,
                            fact.authorization.authorized_at_ms,
                            fact.authorization.target_qty_units_6dp,
                            fact.authorization.market_end_ms,
                            fact.authorization.fee_rate_bps_bits,
                        ),
                    )));
                }
            }
        }
    }
}

impl PortfolioState {
    pub(crate) fn confirmed_direct_buy_entries(
        &self,
    ) -> impl Iterator<Item = &ConfirmedCtfBuyFact> {
        self.confirmed_ctf_split_outlays
            .values()
            .filter_map(|entry| match entry {
                ConfirmedCtfEntryOutlay::Buy(receipts) => {
                    receipts.first_key_value().map(|(_, fact)| fact)
                }
                ConfirmedCtfEntryOutlay::Split(_) => None,
            })
    }

    pub(crate) fn confirmed_ctf_entry_outlay_units_6dp(&self, cohort: &str) -> Result<u128> {
        self.confirmed_ctf_split_outlays
            .get(cohort)
            .context("confirmed CTF cohort outlay is absent")?
            .outlay_units_6dp()
    }

    pub(crate) fn confirmed_direct_buy_receipts(
        &self,
    ) -> impl Iterator<Item = &ConfirmedCtfBuyFact> {
        self.confirmed_ctf_split_outlays
            .values()
            .filter_map(|entry| match entry {
                ConfirmedCtfEntryOutlay::Buy(receipts) => Some(receipts.values()),
                ConfirmedCtfEntryOutlay::Split(_) => None,
            })
            .flatten()
    }

    pub(crate) fn confirmed_direct_buy_qty_units_6dp(&self, cohort: &str) -> u128 {
        match self.confirmed_ctf_split_outlays.get(cohort) {
            Some(ConfirmedCtfEntryOutlay::Buy(receipts)) => receipts
                .values()
                .try_fold(0u128, |total, fact| total.checked_add(fact.qty_units_6dp))
                .expect("admitted BUY receipt quantities fit authorization"),
            _ => 0,
        }
    }

    pub(super) fn validate_confirmed_buy_entry(&self, fact: &ConfirmedCtfBuyFact) -> Result<bool> {
        match self.confirmed_ctf_split_outlays.get(&fact.cohort_id) {
            Some(ConfirmedCtfEntryOutlay::Split(_)) => {
                anyhow::bail!("BUY conflicts with split entry")
            }
            Some(ConfirmedCtfEntryOutlay::Buy(receipts)) => {
                if let Some(existing) = receipts.get(&fact.fill_id) {
                    anyhow::ensure!(
                        existing == fact,
                        "BUY receipt conflicts with admitted receipt"
                    );
                    return Ok(false);
                }
                let first = receipts
                    .first_key_value()
                    .context("BUY entry has no receipts")?
                    .1;
                anyhow::ensure!(
                    first.authorization == fact.authorization
                        && first.market_id == fact.market_id
                        && first.token == fact.token
                        && first.cohort_id == fact.cohort_id,
                    "BUY receipt conflicts with original authorization"
                );
            }
            None => {}
        }
        let acquired = self
            .confirmed_direct_buy_qty_units_6dp(&fact.cohort_id)
            .checked_add(fact.qty_units_6dp)
            .context("BUY acquired quantity overflow")?;
        anyhow::ensure!(
            acquired <= fact.authorization.target_qty_units_6dp,
            "BUY acquired quantity exceeds original target"
        );
        let prior_cash_delta = match self.confirmed_ctf_split_outlays.get(&fact.cohort_id) {
            Some(ConfirmedCtfEntryOutlay::Buy(receipts)) => receipts
                .values()
                .try_fold(0i128, |total, prior| {
                    total.checked_add(prior.collateral_cash_delta_units_6dp)
                })
                .context("BUY entry cash delta overflow")?,
            _ => 0,
        };
        prior_cash_delta
            .checked_add(fact.collateral_cash_delta_units_6dp)
            .and_then(i128::checked_neg)
            .context("BUY entry cash delta overflow")?;
        Ok(true)
    }
}

use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};
use polymarket_client_sdk::types::U256;

use crate::trade_session_ledger::{ConfirmedCtfBuyFact, CtfTerminalOperation};

#[path = "ctf_accounting/entry.rs"]
mod entry;
pub(crate) use entry::buy_receipt_basis_units_6dp;

const FIXED_6DP_TOLERANCE: f64 = 0.000_001;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SellReceipt {
    token: String,
    qty_units: u128,
    gross_proceeds_units: i128,
    fee_units: u128,
    approved_direct_cost_units: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SettlementReceipt {
    operation: CtfTerminalOperation,
    terminal_qty_by_token: BTreeMap<String, u128>,
    collateral_receipt_units: i128,
    collateral_proceeds_by_token: BTreeMap<String, i128>,
    fee_units: u128,
    approved_direct_cost_units: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CtfEntryAllocation {
    principal: u128,
    fee: u128,
    cost: u128,
    refund: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CtfDisposalRealization {
    pub proceeds: i128,
    pub principal: u128,
    pub fees: u128,
    pub costs: u128,
    pub refunds: u128,
    pub pnl: i128,
}

#[derive(Debug, Clone)]
pub(crate) struct CtfMarketEntryCashflow {
    entry_id: String,
    buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    buy_receipts: BTreeMap<String, ConfirmedCtfBuyFact>,
    allocation_origin_qty_by_token: BTreeMap<String, u128>,
    confirmed_collateral_refund_units: u128,
    created_qty_by_token: BTreeMap<String, u128>,
    entry_allocation_by_token: BTreeMap<String, CtfEntryAllocation>,
    value_weight_by_token: BTreeMap<String, u128>,
    residual_token: String,
    principal_units: u128,
    confirmed_disposed_qty_by_token: BTreeMap<String, u128>,
    confirmed_sell_proceeds_units: i128,
    confirmed_terminal_receipts_units: i128,
    confirmed_fee_units: u128,
    confirmed_approved_direct_cost_units: u128,
    sell_receipts: BTreeMap<String, SellReceipt>,
    terminal_receipts: BTreeMap<String, SettlementReceipt>,
    sell_realizations: BTreeMap<String, CtfDisposalRealization>,
    terminal_realizations: BTreeMap<(String, String), CtfDisposalRealization>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CtfMarketEntryCashflowSummary {
    pub created_qty_by_token: BTreeMap<String, u128>,
    pub confirmed_sold_qty_by_token: BTreeMap<String, u128>,
    pub confirmed_merged_qty_by_token: BTreeMap<String, u128>,
    pub confirmed_redeemed_qty_by_token: BTreeMap<String, u128>,
    pub confirmed_disposed_qty_by_token: BTreeMap<String, u128>,
    pub remaining_qty_by_token: BTreeMap<String, u128>,
    pub principal_units: u128,
    pub confirmed_collateral_refund_units: u128,
    pub remaining_basis_units_by_token: BTreeMap<String, u128>,
    pub confirmed_sell_proceeds_units: i128,
    pub confirmed_terminal_receipts_units: i128,
    pub confirmed_fee_units: u128,
    pub confirmed_approved_direct_cost_units: u128,
    pub sell_realizations: BTreeMap<String, CtfDisposalRealization>,
    pub terminal_realizations: BTreeMap<(String, String), CtfDisposalRealization>,
}

impl CtfMarketEntryCashflow {
    pub(crate) fn confirm_sell(
        &mut self,
        receipt_id: impl Into<String>,
        token: impl Into<String>,
        qty_units: u128,
        gross_proceeds_units: i128,
        fee_units: u128,
        approved_direct_cost_units: u128,
    ) -> Result<()> {
        let receipt_id = nonempty(receipt_id.into(), "sell receipt identity")?;
        let receipt = self.sell_receipt(
            token.into(),
            qty_units,
            gross_proceeds_units,
            fee_units,
            approved_direct_cost_units,
        )?;
        if let Some(previous) = self.sell_receipts.get(&receipt_id) {
            ensure!(
                previous == &receipt,
                "conflicting sell receipt {receipt_id}"
            );
            return Ok(());
        }
        let allocation = self.disposal_allocation(&receipt.token, receipt.qty_units)?;
        let realization = realize(
            receipt.gross_proceeds_units,
            allocation,
            receipt.fee_units,
            receipt.approved_direct_cost_units,
        )?;
        let next_quantities = self.next_terminal_quantities(std::iter::once((
            receipt.token.as_str(),
            receipt.qty_units,
        )))?;
        let next_sell_proceeds = self
            .confirmed_sell_proceeds_units
            .checked_add(receipt.gross_proceeds_units)
            .context("sell proceeds overflow")?;
        let (next_fees, next_direct_costs) =
            self.next_deductions(receipt.fee_units, receipt.approved_direct_cost_units)?;
        self.confirmed_disposed_qty_by_token = next_quantities;
        self.confirmed_sell_proceeds_units = next_sell_proceeds;
        self.confirmed_fee_units = next_fees;
        self.confirmed_approved_direct_cost_units = next_direct_costs;
        self.sell_realizations
            .insert(receipt_id.clone(), realization);
        self.sell_receipts.insert(receipt_id, receipt);
        Ok(())
    }

    pub(crate) fn confirm_terminal(
        &mut self,
        receipt_id: impl Into<String>,
        operation: CtfTerminalOperation,
        terminal_qty_by_token: BTreeMap<String, u128>,
        collateral_receipt_units: i128,
        collateral_proceeds_by_token: BTreeMap<String, i128>,
        fee_units: u128,
        approved_direct_cost_units: u128,
    ) -> Result<()> {
        ensure!(
            self.buy_authorization.is_none() || operation == CtfTerminalOperation::Redeem,
            "direct BUY holdings require redemption"
        );
        let receipt_id = nonempty(receipt_id.into(), "terminal receipt identity")?;
        ensure!(
            !terminal_qty_by_token.is_empty(),
            "terminal receipt has no quantities"
        );
        ensure!(collateral_receipt_units >= 0, "negative terminal receipt");

        let mut normalized_quantities = BTreeMap::new();
        for (token, qty_units) in terminal_qty_by_token {
            let token = self.entry_token(token)?;
            ensure!(qty_units > 0, "zero settlement quantity");
            ensure!(
                normalized_quantities.insert(token, qty_units).is_none(),
                "duplicate terminal token"
            );
        }
        let mut normalized_proceeds = BTreeMap::new();
        for (token, proceeds) in collateral_proceeds_by_token {
            let token = self.entry_token(token)?;
            ensure!(proceeds >= 0, "negative token proceeds");
            ensure!(
                normalized_proceeds.insert(token, proceeds).is_none(),
                "duplicate proceeds token"
            );
        }
        match operation {
            CtfTerminalOperation::Merge => ensure!(
                normalized_proceeds.is_empty(),
                "merge carries token proceeds"
            ),
            CtfTerminalOperation::Redeem => {
                ensure!(
                    normalized_proceeds
                        .keys()
                        .eq(self.created_qty_by_token.keys()),
                    "redeem proceeds token mismatch"
                );
                let total = normalized_proceeds
                    .values()
                    .try_fold(0_i128, |sum, value| {
                        sum.checked_add(*value).context("redeem sum overflow")
                    })?;
                ensure!(
                    total == collateral_receipt_units,
                    "redeem token proceeds differ from receipt"
                );
            }
        }
        let receipt = SettlementReceipt {
            operation,
            terminal_qty_by_token: normalized_quantities,
            collateral_receipt_units,
            collateral_proceeds_by_token: normalized_proceeds,
            fee_units,
            approved_direct_cost_units,
        };
        if let Some(previous) = self.terminal_receipts.get(&receipt_id) {
            ensure!(
                previous == &receipt,
                "conflicting terminal receipt {receipt_id}"
            );
            return Ok(());
        }
        let allocations = receipt
            .terminal_qty_by_token
            .iter()
            .map(|(token, qty)| {
                self.disposal_allocation(token, *qty)
                    .map(|allocation| (token.clone(), allocation))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let proceeds = if receipt.operation == CtfTerminalOperation::Merge {
            self.allocate_terminal(
                receipt.collateral_receipt_units,
                &receipt.terminal_qty_by_token,
            )?
        } else {
            receipt.collateral_proceeds_by_token.clone()
        };
        let fees =
            self.allocate_terminal_unsigned(receipt.fee_units, &receipt.terminal_qty_by_token)?;
        let costs = self.allocate_terminal_unsigned(
            receipt.approved_direct_cost_units,
            &receipt.terminal_qty_by_token,
        )?;
        let realizations = allocations
            .iter()
            .map(|(token, allocation)| {
                let realization = realize(proceeds[token], *allocation, fees[token], costs[token])?;
                Ok(((receipt_id.clone(), token.clone()), realization))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let next_quantities = self.next_terminal_quantities(
            receipt
                .terminal_qty_by_token
                .iter()
                .map(|(token, qty)| (token.as_str(), *qty)),
        )?;
        let next_terminal_receipts = self
            .confirmed_terminal_receipts_units
            .checked_add(receipt.collateral_receipt_units)
            .context("terminal receipts overflow")?;
        let (next_fees, next_direct_costs) =
            self.next_deductions(receipt.fee_units, receipt.approved_direct_cost_units)?;
        self.confirmed_disposed_qty_by_token = next_quantities;
        self.confirmed_terminal_receipts_units = next_terminal_receipts;
        self.confirmed_fee_units = next_fees;
        self.confirmed_approved_direct_cost_units = next_direct_costs;
        self.terminal_realizations.extend(realizations);
        self.terminal_receipts.insert(receipt_id, receipt);
        Ok(())
    }

    pub(crate) fn realized_pnl_units(&self) -> Result<Option<i128>> {
        for (token, created_qty) in &self.created_qty_by_token {
            if self
                .confirmed_disposed_qty_by_token
                .get(token)
                .copied()
                .unwrap_or(0)
                != *created_qty
            {
                return Ok(None);
            }
        }
        let principal = i128::try_from(self.principal_units)
            .with_context(|| format!("principal overflow entry={}", self.entry_id))?;
        let fees = i128::try_from(self.confirmed_fee_units)
            .with_context(|| format!("fee overflow entry={}", self.entry_id))?;
        let direct_costs = i128::try_from(self.confirmed_approved_direct_cost_units)
            .with_context(|| format!("cost overflow entry={}", self.entry_id))?;
        let deductions = principal
            .checked_add(fees)
            .and_then(|value| value.checked_add(direct_costs))
            .context("PnL deduction overflow")?;
        let confirmed_receipts = self
            .confirmed_sell_proceeds_units
            .checked_add(self.confirmed_terminal_receipts_units)
            .context("receipt total overflow")?;
        let refunds = i128::try_from(self.confirmed_collateral_refund_units)
            .context("collateral refund exceeds signed range")?;
        let aggregate = confirmed_receipts
            .checked_add(refunds)
            .and_then(|value| value.checked_sub(deductions))
            .context("realized PnL overflow")?;
        let receipt_sum = self
            .sell_realizations
            .values()
            .chain(self.terminal_realizations.values())
            .try_fold(0_i128, |sum, item| {
                sum.checked_add(item.pnl)
                    .context("receipt realized PnL overflow")
            })?;
        ensure!(receipt_sum == aggregate, "receipt and cohort PnL differ");
        Ok(Some(aggregate))
    }

    pub(crate) fn summary(&self) -> Result<CtfMarketEntryCashflowSummary> {
        let mut confirmed_sold_qty_by_token = BTreeMap::new();
        for receipt in self.sell_receipts.values() {
            add_quantity(
                &mut confirmed_sold_qty_by_token,
                &receipt.token,
                receipt.qty_units,
                self.created_qty_by_token[&receipt.token],
            )?;
        }
        let mut confirmed_merged_qty_by_token = BTreeMap::new();
        let mut confirmed_redeemed_qty_by_token = BTreeMap::new();
        for receipt in self.terminal_receipts.values() {
            let quantities = match receipt.operation {
                CtfTerminalOperation::Merge => &mut confirmed_merged_qty_by_token,
                CtfTerminalOperation::Redeem => &mut confirmed_redeemed_qty_by_token,
            };
            for (token, qty_units) in &receipt.terminal_qty_by_token {
                add_quantity(
                    quantities,
                    token,
                    *qty_units,
                    self.created_qty_by_token[token],
                )?;
            }
        }
        let mut remaining_qty_by_token = BTreeMap::new();
        for (token, created_qty) in &self.created_qty_by_token {
            let disposed_qty = self
                .confirmed_disposed_qty_by_token
                .get(token)
                .copied()
                .unwrap_or(0);
            let remaining_qty = created_qty
                .checked_sub(disposed_qty)
                .context("disposed quantity exceeds creation")?;
            let classified_disposed_qty = confirmed_sold_qty_by_token
                .get(token)
                .copied()
                .unwrap_or(0)
                .checked_add(
                    confirmed_merged_qty_by_token
                        .get(token)
                        .copied()
                        .unwrap_or(0),
                )
                .and_then(|qty| {
                    qty.checked_add(
                        confirmed_redeemed_qty_by_token
                            .get(token)
                            .copied()
                            .unwrap_or(0),
                    )
                })
                .context("classified disposal overflow")?;
            ensure!(
                classified_disposed_qty == disposed_qty
                    && classified_disposed_qty
                        .checked_add(remaining_qty)
                        .is_some_and(|total| total == *created_qty),
                "share conservation failed token={token}"
            );
            remaining_qty_by_token.insert(token.clone(), remaining_qty);
        }
        Ok(CtfMarketEntryCashflowSummary {
            created_qty_by_token: self.created_qty_by_token.clone(),
            confirmed_sold_qty_by_token,
            confirmed_merged_qty_by_token,
            confirmed_redeemed_qty_by_token,
            confirmed_disposed_qty_by_token: self.confirmed_disposed_qty_by_token.clone(),
            remaining_qty_by_token,
            principal_units: self.principal_units,
            confirmed_collateral_refund_units: self.confirmed_collateral_refund_units,
            remaining_basis_units_by_token: self.remaining_basis_units_by_token()?,
            confirmed_sell_proceeds_units: self.confirmed_sell_proceeds_units,
            confirmed_terminal_receipts_units: self.confirmed_terminal_receipts_units,
            confirmed_fee_units: self.confirmed_fee_units,
            confirmed_approved_direct_cost_units: self.confirmed_approved_direct_cost_units,
            sell_realizations: self.sell_realizations.clone(),
            terminal_realizations: self.terminal_realizations.clone(),
        })
    }

    fn sell_receipt(
        &self,
        token: String,
        qty_units: u128,
        gross_proceeds_units: i128,
        fee_units: u128,
        approved_direct_cost_units: u128,
    ) -> Result<SellReceipt> {
        let token = self.entry_token(token)?;
        ensure!(qty_units > 0, "zero terminal quantity");
        ensure!(gross_proceeds_units >= 0, "negative sell proceeds");
        Ok(SellReceipt {
            token,
            qty_units,
            gross_proceeds_units,
            fee_units,
            approved_direct_cost_units,
        })
    }

    fn entry_token(&self, token: String) -> Result<String> {
        let token = nonempty(token, "terminal token")?;
        ensure!(
            self.created_qty_by_token.contains_key(&token),
            "terminal token outside CTF entry"
        );
        Ok(token)
    }

    fn next_terminal_quantities<'a>(
        &self,
        additions: impl Iterator<Item = (&'a str, u128)>,
    ) -> Result<BTreeMap<String, u128>> {
        let mut next_quantities = self.confirmed_disposed_qty_by_token.clone();
        for (token, qty_units) in additions {
            let created_qty = self
                .created_qty_by_token
                .get(token)
                .copied()
                .context("terminal token outside CTF entry")?;
            add_quantity(&mut next_quantities, token, qty_units, created_qty)?;
        }
        Ok(next_quantities)
    }

    fn next_deductions(
        &self,
        fee_units: u128,
        approved_direct_cost_units: u128,
    ) -> Result<(u128, u128)> {
        let fees = self
            .confirmed_fee_units
            .checked_add(fee_units)
            .context("fee total overflow")?;
        let direct_costs = self
            .confirmed_approved_direct_cost_units
            .checked_add(approved_direct_cost_units)
            .context("approved costs overflow")?;
        Ok((fees, direct_costs))
    }

    fn disposal_allocation(&self, token: &str, qty: u128) -> Result<CtfEntryAllocation> {
        let origin = self
            .allocation_origin_qty_by_token
            .get(token)
            .copied()
            .unwrap_or(0);
        let created = self.created_qty_by_token[token]
            .checked_sub(origin)
            .context("allocation origin exceeds creation")?;
        let before = self
            .confirmed_disposed_qty_by_token
            .get(token)
            .copied()
            .unwrap_or(0)
            .checked_sub(origin)
            .context("allocation origin exceeds disposals")?;
        let after = before
            .checked_add(qty)
            .context("disposal quantity overflow")?;
        ensure!(after <= created, "disposal exceeds creation");
        let total = self.entry_allocation_by_token[token];
        Ok(CtfEntryAllocation {
            principal: allocation_increment(total.principal, before, after, created)?,
            fee: allocation_increment(total.fee, before, after, created)?,
            cost: allocation_increment(total.cost, before, after, created)?,
            refund: allocation_increment(total.refund, before, after, created)?,
        })
    }

    fn allocate_terminal_unsigned(
        &self,
        total: u128,
        quantities: &BTreeMap<String, u128>,
    ) -> Result<BTreeMap<String, u128>> {
        ensure!(!quantities.is_empty(), "terminal has no token");
        if quantities.len() == 1 {
            return Ok(BTreeMap::from([(
                quantities.keys().next().unwrap().clone(),
                total,
            )]));
        }
        ensure!(quantities.len() == 2, "terminal is not binary");
        let first = quantities
            .keys()
            .find(|token| token.as_str() != self.residual_token)
            .context("terminal first token missing")?;
        let first_weight = self.value_weight_by_token[first]
            .checked_mul(quantities[first])
            .context("terminal first weight overflow")?;
        let residual_weight = self.value_weight_by_token[&self.residual_token]
            .checked_mul(
                *quantities
                    .get(&self.residual_token)
                    .context("terminal residual token missing")?,
            )
            .context("terminal residual weight overflow")?;
        let (first_value, residual_value) = allocate_units(total, first_weight, residual_weight)?;
        Ok(BTreeMap::from([
            (first.clone(), first_value),
            (self.residual_token.clone(), residual_value),
        ]))
    }

    fn allocate_terminal(
        &self,
        total: i128,
        quantities: &BTreeMap<String, u128>,
    ) -> Result<BTreeMap<String, i128>> {
        self.allocate_terminal_unsigned(
            u128::try_from(total).context("terminal proceeds exceed unsigned range")?,
            quantities,
        )?
        .into_iter()
        .map(|(token, value)| {
            i128::try_from(value)
                .context("terminal proceeds exceed signed range")
                .map(|value| (token, value))
        })
        .collect()
    }
}

pub(crate) fn allocate_units(
    total: u128,
    first_weight: u128,
    second_weight: u128,
) -> Result<(u128, u128)> {
    let denominator = first_weight
        .checked_add(second_weight)
        .context("allocation weight overflow")?;
    ensure!(denominator > 0, "allocation weights are zero");
    let first = rounded_ratio(total, first_weight, denominator)?;
    Ok((
        first,
        total
            .checked_sub(first)
            .context("allocation exceeds total")?,
    ))
}

pub(crate) fn allocation_increment(
    total: u128,
    before: u128,
    after: u128,
    quantity: u128,
) -> Result<u128> {
    let before = rounded_ratio(total, before, quantity)?;
    let after = if after == quantity {
        total
    } else {
        rounded_ratio(total, after, quantity)?
    };
    after.checked_sub(before).context("allocation regressed")
}

fn rounded_ratio(total: u128, numerator: u128, denominator: u128) -> Result<u128> {
    ensure!(
        denominator > 0 && numerator <= denominator,
        "invalid allocation ratio"
    );
    u128::try_from(
        (U256::from(total) * U256::from(numerator) + U256::from(denominator / 2))
            / U256::from(denominator),
    )
    .context("allocation exceeds exact range")
}

fn realize(
    proceeds: i128,
    entry: CtfEntryAllocation,
    fee: u128,
    cost: u128,
) -> Result<CtfDisposalRealization> {
    let fees = entry
        .fee
        .checked_add(fee)
        .context("fee allocation overflow")?;
    let costs = entry
        .cost
        .checked_add(cost)
        .context("cost allocation overflow")?;
    let deductions = entry
        .principal
        .checked_add(fees)
        .and_then(|value| value.checked_add(costs))
        .and_then(|value| i128::try_from(value).ok())
        .context("deductions exceed signed range")?;
    Ok(CtfDisposalRealization {
        proceeds,
        principal: entry.principal,
        fees,
        costs,
        refunds: entry.refund,
        pnl: proceeds
            .checked_add(i128::try_from(entry.refund).context("refund exceeds signed range")?)
            .and_then(|value| value.checked_sub(deductions))
            .context("disposal PnL overflow")?,
    })
}

pub(crate) fn qty_to_units_6dp(value: f64) -> Result<u128> {
    checked_qty_to_units_6dp(value).map_err(anyhow::Error::msg)
}

pub(crate) fn checked_qty_to_units_6dp(value: f64) -> std::result::Result<u128, &'static str> {
    if !(value.is_finite() && value >= 0.0) {
        return Err("invalid fixed-6dp quantity");
    }
    let scaled = value * crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64;
    let rounded = scaled.round();
    if !(rounded <= u128::MAX as f64 && rounded >= 1.0) {
        return Err("fixed-6dp quantity overflow or zero");
    }
    if !((scaled - rounded).abs() <= FIXED_6DP_TOLERANCE) {
        return Err("quantity exceeds fixed-6dp precision");
    }
    Ok(rounded as u128)
}

pub(crate) fn units_6dp_to_f64(value: i128) -> f64 {
    value as f64 / crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64
}

fn add_quantity(
    quantities: &mut BTreeMap<String, u128>,
    token: &str,
    qty_units: u128,
    principal_units: u128,
) -> Result<()> {
    let current = quantities.get(token).copied().unwrap_or(0);
    let next = current
        .checked_add(qty_units)
        .context("terminal quantity overflow")?;
    ensure!(
        next <= principal_units,
        "terminal quantity exceeds split principal"
    );
    quantities.insert(token.to_string(), next);
    Ok(())
}

fn nonempty(value: String, label: &str) -> Result<String> {
    let trimmed = value.trim();
    ensure!(!trimmed.is_empty(), "missing {label}");
    Ok(trimmed.to_string())
}

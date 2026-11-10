use super::*;

impl CtfMarketEntryCashflow {
    pub(crate) fn new(
        entry_id: impl Into<String>,
        first_token: impl Into<String>,
        first_qty_units: u128,
        second_token: impl Into<String>,
        second_qty_units: u128,
        principal_units: u128,
        entry_fee_units: u128,
        entry_approved_direct_cost_units: u128,
        first_value_weight: u128,
        second_value_weight: u128,
    ) -> Result<Self> {
        let entry_id = nonempty(entry_id.into(), "entry identity")?;
        let first_token = nonempty(first_token.into(), "first entry token")?;
        let second_token = nonempty(second_token.into(), "second entry token")?;
        ensure!(first_token != second_token, "duplicate CTF entry token");
        ensure!(
            first_qty_units > 0
                && first_qty_units == second_qty_units
                && first_qty_units == principal_units,
            "split quantities/principal differ or are zero"
        );
        let (first_principal, second_principal) =
            allocate_units(principal_units, first_value_weight, second_value_weight)?;
        let (first_fee, second_fee) =
            allocate_units(entry_fee_units, first_value_weight, second_value_weight)?;
        let (first_cost, second_cost) = allocate_units(
            entry_approved_direct_cost_units,
            first_value_weight,
            second_value_weight,
        )?;
        Ok(Self {
            entry_id,
            buy_authorization: None,
            buy_receipts: BTreeMap::new(),
            allocation_origin_qty_by_token: BTreeMap::new(),
            confirmed_collateral_refund_units: 0,
            created_qty_by_token: BTreeMap::from([
                (first_token.clone(), first_qty_units),
                (second_token.clone(), second_qty_units),
            ]),
            entry_allocation_by_token: BTreeMap::from([
                (
                    first_token.clone(),
                    CtfEntryAllocation {
                        principal: first_principal,
                        fee: first_fee,
                        cost: first_cost,
                        refund: 0,
                    },
                ),
                (
                    second_token.clone(),
                    CtfEntryAllocation {
                        principal: second_principal,
                        fee: second_fee,
                        cost: second_cost,
                        refund: 0,
                    },
                ),
            ]),
            value_weight_by_token: BTreeMap::from([
                (first_token.clone(), first_value_weight),
                (second_token.clone(), second_value_weight),
            ]),
            residual_token: second_token,
            principal_units,
            confirmed_disposed_qty_by_token: BTreeMap::new(),
            confirmed_sell_proceeds_units: 0,
            confirmed_terminal_receipts_units: 0,
            confirmed_fee_units: entry_fee_units,
            confirmed_approved_direct_cost_units: entry_approved_direct_cost_units,
            sell_receipts: BTreeMap::new(),
            terminal_receipts: BTreeMap::new(),
            sell_realizations: BTreeMap::new(),
            terminal_realizations: BTreeMap::new(),
        })
    }

    pub(crate) fn new_buy(receipt: &ConfirmedCtfBuyFact) -> Result<Self> {
        receipt.validate()?;
        let authorization = receipt.authorization;
        let up = authorization.up_token.to_string();
        let down = authorization.down_token.to_string();
        let zero = CtfEntryAllocation {
            principal: 0,
            fee: 0,
            cost: 0,
            refund: 0,
        };
        let mut entry = Self {
            entry_id: receipt.cohort_id.clone(),
            buy_authorization: Some(authorization),
            buy_receipts: BTreeMap::new(),
            allocation_origin_qty_by_token: BTreeMap::new(),
            confirmed_collateral_refund_units: 0,
            created_qty_by_token: BTreeMap::from([(up.clone(), 0), (down.clone(), 0)]),
            entry_allocation_by_token: BTreeMap::from([(up.clone(), zero), (down.clone(), zero)]),
            value_weight_by_token: BTreeMap::from([
                (up, u128::from(authorization.selected_up)),
                (down.clone(), u128::from(!authorization.selected_up)),
            ]),
            residual_token: down,
            principal_units: 0,
            confirmed_disposed_qty_by_token: BTreeMap::new(),
            confirmed_sell_proceeds_units: 0,
            confirmed_terminal_receipts_units: 0,
            confirmed_fee_units: 0,
            confirmed_approved_direct_cost_units: 0,
            sell_receipts: BTreeMap::new(),
            terminal_receipts: BTreeMap::new(),
            sell_realizations: BTreeMap::new(),
            terminal_realizations: BTreeMap::new(),
        };
        entry.confirm_buy(receipt)?;
        Ok(entry)
    }

    pub(crate) fn confirm_buy(&mut self, receipt: &ConfirmedCtfBuyFact) -> Result<()> {
        receipt.validate()?;
        ensure!(
            self.buy_authorization == Some(receipt.authorization)
                && self.entry_id == receipt.cohort_id,
            "BUY acquisition differs from original entry authorization"
        );
        if let Some(previous) = self.buy_receipts.get(&receipt.receipt_id) {
            ensure!(
                previous == receipt,
                "conflicting BUY receipt {}",
                receipt.receipt_id
            );
            return Ok(());
        }
        let token = &receipt.token;
        let previous_qty = *self
            .created_qty_by_token
            .get(token)
            .context("BUY token outside authorized pair")?;
        let next_qty = previous_qty
            .checked_add(receipt.qty_units_6dp)
            .context("BUY quantity overflow")?;
        ensure!(
            next_qty <= receipt.authorization.target_qty_units_6dp,
            "BUY receipts exceed original authorized target"
        );
        let remaining = self.remaining_entry_allocation(token)?;
        let allocation = CtfEntryAllocation {
            principal: remaining
                .principal
                .checked_add(receipt.principal_units_6dp)
                .context("BUY principal allocation overflow")?,
            fee: remaining
                .fee
                .checked_add(receipt.fee_units_6dp)
                .context("BUY fee allocation overflow")?,
            cost: remaining
                .cost
                .checked_add(receipt.approved_direct_cost_units_6dp)
                .context("BUY cost allocation overflow")?,
            refund: remaining
                .refund
                .checked_add(receipt.collateral_refund_units_6dp)
                .context("BUY refund allocation overflow")?,
        };
        let principal = self
            .principal_units
            .checked_add(receipt.principal_units_6dp)
            .context("BUY principal total overflow")?;
        let refunds = self
            .confirmed_collateral_refund_units
            .checked_add(receipt.collateral_refund_units_6dp)
            .context("BUY refund total overflow")?;
        let (fees, costs) = self.next_deductions(
            receipt.fee_units_6dp,
            receipt.approved_direct_cost_units_6dp,
        )?;
        let origin = self
            .confirmed_disposed_qty_by_token
            .get(token)
            .copied()
            .unwrap_or(0);
        self.created_qty_by_token.insert(token.clone(), next_qty);
        self.entry_allocation_by_token
            .insert(token.clone(), allocation);
        self.allocation_origin_qty_by_token
            .insert(token.clone(), origin);
        self.principal_units = principal;
        self.confirmed_collateral_refund_units = refunds;
        self.confirmed_fee_units = fees;
        self.confirmed_approved_direct_cost_units = costs;
        self.buy_receipts
            .insert(receipt.receipt_id.clone(), receipt.clone());
        Ok(())
    }

    fn remaining_entry_allocation(&self, token: &str) -> Result<CtfEntryAllocation> {
        let created = self.created_qty_by_token[token];
        let disposed = self
            .confirmed_disposed_qty_by_token
            .get(token)
            .copied()
            .unwrap_or(0);
        let remaining = created
            .checked_sub(disposed)
            .context("disposal exceeds creation")?;
        if remaining == 0 {
            return Ok(CtfEntryAllocation {
                principal: 0,
                fee: 0,
                cost: 0,
                refund: 0,
            });
        }
        self.disposal_allocation(token, remaining)
    }

    pub(super) fn remaining_basis_units_by_token(&self) -> Result<BTreeMap<String, u128>> {
        self.created_qty_by_token
            .keys()
            .map(|token| {
                let allocation = self.remaining_entry_allocation(token)?;
                Ok((
                    token.clone(),
                    net_entry_basis(
                        allocation.principal,
                        allocation.fee,
                        allocation.cost,
                        allocation.refund,
                    )?,
                ))
            })
            .collect()
    }
}

pub(crate) fn buy_receipt_basis_units_6dp(receipt: &ConfirmedCtfBuyFact) -> Result<u128> {
    net_entry_basis(
        receipt.principal_units_6dp,
        receipt.fee_units_6dp,
        receipt.approved_direct_cost_units_6dp,
        receipt.collateral_refund_units_6dp,
    )
}

fn net_entry_basis(principal: u128, fee: u128, cost: u128, refund: u128) -> Result<u128> {
    let outlay = principal
        .checked_add(fee)
        .and_then(|value| value.checked_add(cost))
        .context("acquisition outlay overflow")?;
    Ok(outlay.saturating_sub(refund))
}

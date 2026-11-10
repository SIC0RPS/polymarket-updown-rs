use super::*;

impl SettlementCollateralReceipt {
    pub fn confirmed_block_number(&self) -> Result<u64> {
        self.validate_exact()?;
        match &self.outcome {
            SettlementCollateralOutcome::Confirmed { wrapped, .. } => Ok(wrapped.log.block_number),
            SettlementCollateralOutcome::ReconciliationRequired { .. } => {
                anyhow::bail!("settlement collateral receipt is not confirmed")
            }
        }
    }

    pub(super) fn validate_exact(&self) -> Result<()> {
        anyhow::ensure!(
            !self.run_id.trim().is_empty(),
            "settlement collateral run_id is empty"
        );
        anyhow::ensure!(self.ts_ms > 0, "settlement collateral timestamp is zero");
        let tx_hash = B256::from_str(&self.tx_hash)
            .context("settlement collateral tx_hash is not canonical bytes32")?;
        anyhow::ensure!(
            tx_hash != B256::ZERO && self.tx_hash == tx_hash.to_string(),
            "settlement collateral tx_hash is noncanonical"
        );
        anyhow::ensure!(
            self.token_contract == crate::polymarket::contracts_v2::PUSD_COLLATERAL.to_string(),
            "settlement collateral token is not canonical pUSD"
        );
        let recipient = Address::from_str(&self.recipient)
            .context("settlement collateral recipient is not an address")?;
        anyhow::ensure!(
            recipient != Address::ZERO && self.recipient == recipient.to_string(),
            "settlement collateral recipient is noncanonical"
        );
        let condition_id = B256::from_str(&self.condition_id)
            .context("settlement collateral condition_id is not canonical bytes32")?;
        anyhow::ensure!(
            condition_id != B256::ZERO && self.condition_id == condition_id.to_string(),
            "settlement collateral condition_id is noncanonical"
        );
        anyhow::ensure!(
            matches!(
                self.operation,
                DirectEoaOperation::Merge | DirectEoaOperation::Redeem
            ),
            "settlement collateral operation is not merge or redeem"
        );
        match &self.outcome {
            SettlementCollateralOutcome::Confirmed {
                wrapped,
                payout_redemption,
            } => {
                wrapped
                    .log
                    .validate_exact(crate::polymarket::contracts_v2::PUSD_COLLATERAL, tx_hash)?;
                anyhow::ensure!(
                    wrapped.caller
                        == crate::polymarket::contracts_v2::CTF_COLLATERAL_ADAPTER.to_string()
                        && wrapped.asset
                            == crate::polymarket::contracts_v2::USDC_E_COLLATERAL.to_string()
                        && wrapped.to == self.recipient,
                    "confirmed settlement Wrapped evidence is inconsistent"
                );
                match self.operation {
                    DirectEoaOperation::Merge => {
                        anyhow::ensure!(
                            wrapped.amount_units_6dp > 0 && payout_redemption.is_none(),
                            "confirmed merge collateral identity is incomplete"
                        );
                    }
                    DirectEoaOperation::Redeem => {
                        let payout_redemption = payout_redemption.as_ref().context(
                            "confirmed redeem collateral is missing PayoutRedemption identity",
                        )?;
                        payout_redemption.log.validate_exact(
                            crate::polymarket::contracts_v2::CONDITIONAL_TOKENS,
                            tx_hash,
                        )?;
                        anyhow::ensure!(
                            payout_redemption.log.block_number == wrapped.log.block_number
                                && payout_redemption.log.block_hash == wrapped.log.block_hash
                                && payout_redemption.log.log_index != wrapped.log.log_index,
                            "confirmed redeem collateral reuses one log index for two events"
                        );
                        anyhow::ensure!(
                            payout_redemption.redeemer
                                == crate::polymarket::contracts_v2::CTF_COLLATERAL_ADAPTER
                                    .to_string()
                                && payout_redemption.collateral_token
                                    == crate::polymarket::contracts_v2::USDC_E_COLLATERAL
                                        .to_string()
                                && payout_redemption.parent_collection_id == B256::ZERO.to_string()
                                && payout_redemption.condition_id == self.condition_id
                                && payout_redemption.index_sets == [1, 2]
                                && payout_redemption.payout_units_6dp == wrapped.amount_units_6dp,
                            "confirmed redeem PayoutRedemption evidence is inconsistent"
                        );
                    }
                    DirectEoaOperation::Approval | DirectEoaOperation::Split => {
                        anyhow::bail!("settlement collateral operation is not merge or redeem")
                    }
                }
            }
            SettlementCollateralOutcome::ReconciliationRequired {
                reason,
                matching_wrapped_count,
                matching_payout_redemption_count,
                expected_amount_units_6dp,
                observed_amount_units_6dp,
            } => {
                let required_payout_count =
                    usize::from(self.operation == DirectEoaOperation::Redeem);
                match reason {
                    SettlementCollateralReconciliationReason::MissingExpectedAmount => {
                        anyhow::ensure!(
                            self.operation == DirectEoaOperation::Merge
                                && *matching_payout_redemption_count == 0
                                && expected_amount_units_6dp.is_none(),
                            "missing expected settlement amount evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::MissingWrapped => {
                        anyhow::ensure!(
                            *matching_wrapped_count == 0
                                && *matching_payout_redemption_count == required_payout_count
                                && observed_amount_units_6dp.is_none(),
                            "missing settlement Wrapped reconciliation evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::MultipleWrapped => {
                        anyhow::ensure!(
                            *matching_wrapped_count > 1
                                && *matching_payout_redemption_count == required_payout_count
                                && observed_amount_units_6dp.is_none(),
                            "multiple settlement Wrapped reconciliation evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::InvalidWrapped => {
                        anyhow::ensure!(
                            *matching_wrapped_count == 1
                                && *matching_payout_redemption_count == required_payout_count,
                            "invalid settlement Wrapped reconciliation evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::AmountMismatch
                    | SettlementCollateralReconciliationReason::AmountOverflow => {
                        anyhow::ensure!(
                            *matching_wrapped_count == 1
                                && *matching_payout_redemption_count == required_payout_count
                                && observed_amount_units_6dp
                                    .as_deref()
                                    .is_some_and(|value| !value.is_empty()),
                            "settlement amount reconciliation evidence is invalid"
                        );
                        if *reason == SettlementCollateralReconciliationReason::AmountMismatch {
                            anyhow::ensure!(
                                expected_amount_units_6dp
                                    .as_deref()
                                    .is_some_and(|value| !value.is_empty()),
                                "settlement amount mismatch is missing expected amount"
                            );
                        }
                    }
                    SettlementCollateralReconciliationReason::MissingPayoutRedemption => {
                        anyhow::ensure!(
                            self.operation == DirectEoaOperation::Redeem
                                && *matching_payout_redemption_count == 0,
                            "missing PayoutRedemption reconciliation evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::MultiplePayoutRedemptions => {
                        anyhow::ensure!(
                            self.operation == DirectEoaOperation::Redeem
                                && *matching_payout_redemption_count > 1,
                            "multiple PayoutRedemption reconciliation evidence is invalid"
                        );
                    }
                    SettlementCollateralReconciliationReason::InvalidPayoutRedemption => {
                        anyhow::ensure!(
                            self.operation == DirectEoaOperation::Redeem
                                && *matching_payout_redemption_count == 1,
                            "invalid PayoutRedemption reconciliation evidence is invalid"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub fn confirmed_amount_units_6dp(&self) -> Result<u128> {
        self.validate_exact()?;
        match &self.outcome {
            SettlementCollateralOutcome::Confirmed { wrapped, .. } => Ok(wrapped.amount_units_6dp),
            SettlementCollateralOutcome::ReconciliationRequired { .. } => anyhow::bail!(
                "settlement collateral receipt requires reconciliation tx_hash={}",
                self.tx_hash
            ),
        }
    }

    pub fn accounting_receipt_id(&self) -> String {
        format!("settlement_collateral:{}", self.tx_hash)
    }
}

impl SettlementFinalizedLogIdentity {
    pub(super) fn validate_exact(
        &self,
        expected_emitter: Address,
        expected_tx_hash: B256,
    ) -> Result<()> {
        anyhow::ensure!(
            self.emitter == expected_emitter.to_string(),
            "settlement event emitter is not canonical"
        );
        let transaction_hash = B256::from_str(&self.transaction_hash)
            .context("settlement event transaction_hash is not bytes32")?;
        anyhow::ensure!(
            transaction_hash == expected_tx_hash
                && self.transaction_hash == expected_tx_hash.to_string(),
            "settlement event transaction identity mismatch"
        );
        anyhow::ensure!(
            self.block_number > 0,
            "settlement event finalized block number is zero"
        );
        let block_hash = B256::from_str(&self.block_hash)
            .context("settlement event block_hash is not bytes32")?;
        anyhow::ensure!(
            block_hash != B256::ZERO && self.block_hash == block_hash.to_string(),
            "settlement event block_hash is noncanonical"
        );
        Ok(())
    }
}

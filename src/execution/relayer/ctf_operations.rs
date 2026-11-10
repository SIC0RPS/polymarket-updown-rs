use super::*;
use crate::trade_session_ledger::SessionReceiptAppendOutcome;

mod condition_resolution;

impl PolymarketExecutionLive {
    async fn confirm_terminal_transaction(
        &self,
        operation: DirectEoaOperation,
        condition_id: B256,
        expected_amount: Option<U256>,
        tx_hash_value: B256,
    ) -> Result<CtfSettlementConfirmation> {
        let (operation_label, poll_interval) = match operation {
            DirectEoaOperation::Merge => ("merge", Self::SETTLEMENT_FINALITY_POLL_INTERVAL),
            DirectEoaOperation::Redeem => ("redeem", Duration::from_secs(1)),
            DirectEoaOperation::Approval | DirectEoaOperation::Split => {
                anyhow::bail!("terminal confirmation operation is not merge or redeem")
            }
        };
        anyhow::ensure!(
            (operation == DirectEoaOperation::Merge && expected_amount.is_some())
                || (operation == DirectEoaOperation::Redeem && expected_amount.is_none()),
            "terminal confirmation amount does not match operation"
        );
        let tx_hash = tx_hash_value.to_string();
        let receipt = match self
            .await_polygon_finality(tx_hash_value, poll_interval)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                    tx_hash,
                    error: format!("{operation_label} finality reconciliation required: {error:#}"),
                });
            }
        };
        anyhow::ensure!(
            receipt.transaction_hash == tx_hash_value,
            "terminal finalized receipt identity changed tx_hash={tx_hash}"
        );
        let gas_append = match self
            .persist_direct_eoa_receipt(
                operation,
                Some(condition_id),
                receipt.transaction_hash.to_string(),
                receipt.status(),
                receipt.from,
                receipt.gas_used,
                receipt.effective_gas_price,
                receipt.block_number,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                    tx_hash,
                    error: format!(
                        "{operation_label} finalized gas receipt run identity requires reconciliation: {error:#}"
                    ),
                });
            }
        };
        if !receipt.status() {
            if matches!(
                gas_append,
                SessionReceiptAppendOutcome::Inserted
                    | SessionReceiptAppendOutcome::IdenticalExisting
            ) {
                anyhow::bail!(
                    "{operation_label} transaction reverted condition_id={condition_id} tx_hash={tx_hash}"
                );
            }
            return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                tx_hash,
                error: format!(
                    "{operation_label} reverted receipt identity requires reconciliation gas_append={gas_append:?}"
                ),
            });
        }
        let (settlement_receipt, settlement_append) = match self
            .persist_settlement_collateral_receipt(
                operation,
                condition_id,
                expected_amount,
                &receipt,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                    tx_hash,
                    error: format!(
                        "{operation_label} finalized settlement run identity requires reconciliation: {error:#}"
                    ),
                });
            }
        };
        if !matches!(
            gas_append,
            SessionReceiptAppendOutcome::Inserted | SessionReceiptAppendOutcome::IdenticalExisting
        ) || !matches!(
            &settlement_receipt.outcome,
            SettlementCollateralOutcome::Confirmed { .. }
        ) || !matches!(
            settlement_append,
            SessionReceiptAppendOutcome::Inserted | SessionReceiptAppendOutcome::IdenticalExisting
        ) {
            return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                tx_hash,
                error: format!(
                    "{operation_label} finalized receipt reconciliation required gas_append={gas_append:?} settlement_append={settlement_append:?} settlement_outcome={:?}",
                    settlement_receipt.outcome
                ),
            });
        }
        Ok(CtfSettlementConfirmation::Confirmed {
            receipt: settlement_receipt,
        })
    }

    pub(crate) async fn redeem_binary_condition_single(
        &self,
        condition_id: B256,
    ) -> Result<CtfSettlementConfirmation> {
        let rpc_url = self.rpc_url();
        let provider = ProviderBuilder::default()
            .with_gas_estimation()
            .with_blob_gas_estimation()
            .with_nonce_management(self.cached_nonce_manager.clone())
            .fetch_chain_id()
            .wallet(self.signer.as_ref().clone())
            .connect(&rpc_url)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to connect provider for redeem via {}",
                    crate::polygon_rpc::endpoint_label(&rpc_url)
                )
            })?;

        let ctf_contract = IConditionalTokens::new(contracts_v2::CTF_COLLATERAL_ADAPTER, &provider);
        let redeem_call = ctf_contract.redeemPositions(
            contracts_v2::PUSD_COLLATERAL,
            B256::ZERO,
            condition_id,
            vec![U256::from(1), U256::from(2)],
        );
        let signed = provider
            .fill(redeem_call.into_transaction_request())
            .await
            .with_context(|| format!("redeemPositions signing failed condition_id={condition_id}"))?
            .try_into_envelope()
            .map_err(|_| anyhow::anyhow!("redeemPositions transaction was not signed locally"))?;
        let redeem_route = self.redeem_route_label()?;
        let tx_hash_value = match self
            .broadcast_signed_transaction(
                &provider,
                signed,
                DirectEoaOperation::Redeem,
                Some(condition_id),
                None,
                None,
                None,
            )
            .await?
        {
            DirectEoaBroadcastOutcome::Submitted(tx_hash) => tx_hash,
            DirectEoaBroadcastOutcome::ReconciliationRequired { tx_hash, error } => {
                return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                    tx_hash: tx_hash.to_string(),
                    error,
                });
            }
        };
        let tx_hash = tx_hash_value.to_string();
        self.live_ops.record_redeem_tx_status(
            &condition_id.to_string(),
            redeem_route,
            "submitted",
            Some(tx_hash.clone()),
        );
        self.confirm_terminal_transaction(
            DirectEoaOperation::Redeem,
            condition_id,
            None,
            tx_hash_value,
        )
        .await
    }

    pub(crate) async fn redeem_binary_conditions(
        &self,
        condition_ids: &[B256],
    ) -> Result<CtfSettlementConfirmation> {
        anyhow::ensure!(
            condition_ids.len() == 1,
            "redeem_binary_conditions requires exactly one condition_id"
        );
        self.redeem_binary_condition_single(condition_ids[0]).await
    }

    pub(crate) async fn merge_binary_condition_single(
        &self,
        condition_id: B256,
        amount: U256,
    ) -> Result<CtfSettlementConfirmation> {
        anyhow::ensure!(amount > U256::ZERO, "merge amount must be positive");
        anyhow::ensure!(
            amount <= U256::from(u128::MAX),
            "merge amount exceeds exact u128 receipt range"
        );
        let amount_units_6dp = amount.to::<u128>();

        let rpc_url = self.rpc_url();
        let provider = ProviderBuilder::default()
            .with_gas_estimation()
            .with_blob_gas_estimation()
            .with_nonce_management(self.cached_nonce_manager.clone())
            .fetch_chain_id()
            .wallet(self.signer.as_ref().clone())
            .connect(&rpc_url)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to connect provider for merge via {}",
                    crate::polygon_rpc::endpoint_label(&rpc_url)
                )
            })?;

        let ctf_contract = IConditionalTokens::new(contracts_v2::CTF_COLLATERAL_ADAPTER, &provider);
        let merge_call = ctf_contract.mergePositions(
            contracts_v2::PUSD_COLLATERAL,
            B256::ZERO,
            condition_id,
            vec![U256::from(1), U256::from(2)],
            amount,
        );
        let signed = provider
            .fill(merge_call.into_transaction_request())
            .await
            .with_context(|| {
                format!("mergePositions signing failed condition_id={condition_id} amount={amount}")
            })?
            .try_into_envelope()
            .map_err(|_| anyhow::anyhow!("mergePositions transaction was not signed locally"))?;
        let redeem_route = self.redeem_route_label()?;
        let tx_hash_value = match self
            .broadcast_signed_transaction(
                &provider,
                signed,
                DirectEoaOperation::Merge,
                Some(condition_id),
                Some(amount_units_6dp),
                None,
                None,
            )
            .await?
        {
            DirectEoaBroadcastOutcome::Submitted(tx_hash) => tx_hash,
            DirectEoaBroadcastOutcome::ReconciliationRequired { tx_hash, error } => {
                return Ok(CtfSettlementConfirmation::ReconciliationRequired {
                    tx_hash: tx_hash.to_string(),
                    error,
                });
            }
        };
        let tx_hash = tx_hash_value.to_string();
        self.live_ops.record_redeem_tx_status(
            &condition_id.to_string(),
            redeem_route,
            "submitted",
            Some(tx_hash.clone()),
        );
        self.confirm_terminal_transaction(
            DirectEoaOperation::Merge,
            condition_id,
            Some(amount),
            tx_hash_value,
        )
        .await
    }

    pub(crate) async fn merge_binary_conditions(
        &self,
        merges: &[(B256, U256)],
    ) -> Result<CtfSettlementConfirmation> {
        anyhow::ensure!(
            merges.len() == 1,
            "merge_binary_conditions requires exactly one condition_id"
        );
        self.merge_binary_condition_single(merges[0].0, merges[0].1)
            .await
    }

    pub(crate) async fn split_binary_condition(
        &self,
        split_identity: &DirectEoaSplitIdentity,
        entry_journal: &crate::engine_trade_journal::CtfSplitEntryJournal,
    ) -> Result<SplitConfirmation> {
        split_identity.validate_entry_journal(entry_journal)?;
        let condition_id = split_identity
            .condition_id
            .parse::<B256>()
            .context("direct EOA split condition identity is invalid")?;
        let amount = U256::from(split_identity.amount_units_6dp);
        anyhow::ensure!(amount > U256::ZERO, "split amount must be positive");
        anyhow::ensure!(
            amount <= U256::from(u128::MAX),
            "split amount exceeds exact u128 receipt range"
        );
        let amount_units_6dp = amount.to::<u128>();

        let collateral = contracts_v2::PUSD_COLLATERAL;
        let ctf = contracts_v2::CTF_COLLATERAL_ADAPTER;
        let owner = self.signer.address();
        self.preflight_split_funding(owner, collateral, ctf, amount)
            .await?;

        let rpc_url = self.rpc_url();
        let provider = ProviderBuilder::default()
            .with_gas_estimation()
            .with_blob_gas_estimation()
            .with_nonce_management(self.cached_nonce_manager.clone())
            .fetch_chain_id()
            .wallet(self.signer.as_ref().clone())
            .connect(&rpc_url)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to connect provider for split via {}",
                    crate::polygon_rpc::endpoint_label(&rpc_url)
                )
            })?;
        let ctf_contract = IConditionalTokens::new(ctf, &provider);
        let split_call = ctf_contract.splitPosition(
            collateral,
            B256::ZERO,
            condition_id,
            vec![U256::from(1), U256::from(2)],
            amount,
        );
        let signed = provider
            .fill(split_call.into_transaction_request())
            .await
            .with_context(|| {
                format!("splitPosition signing failed condition_id={condition_id} amount={amount}")
            })?
            .try_into_envelope()
            .map_err(|_| anyhow::anyhow!("splitPosition transaction was not signed locally"))?;
        let tx_hash_value = match self
            .broadcast_signed_transaction(
                &provider,
                signed,
                DirectEoaOperation::Split,
                Some(condition_id),
                Some(amount_units_6dp),
                Some(split_identity.clone()),
                Some(entry_journal.clone()),
            )
            .await?
        {
            DirectEoaBroadcastOutcome::Submitted(tx_hash) => tx_hash,
            DirectEoaBroadcastOutcome::ReconciliationRequired { tx_hash, error } => {
                return Ok(SplitConfirmation {
                    tx_hash: tx_hash.to_string(),
                    receipt: SplitReceiptStatus::ReconciliationRequired { error },
                });
            }
        };
        let tx_hash = tx_hash_value.to_string();
        Ok(SplitConfirmation {
            tx_hash,
            receipt: SplitReceiptStatus::Pending,
        })
    }

    pub(crate) async fn split_transaction_receipt_status(
        &self,
        condition_id: B256,
        tx_hash: &str,
    ) -> Result<Option<SplitReceiptStatus>> {
        let tx_hash_value = tx_hash
            .parse::<B256>()
            .with_context(|| format!("invalid split transaction hash {tx_hash}"))?;
        let Some(finalized) = self
            .observe_polygon_finality_once(tx_hash_value)
            .await
            .with_context(|| format!("split receipt observation failed tx_hash={tx_hash}"))?
        else {
            return Ok(None);
        };
        let succeeded = finalized.status();
        let append_outcome = self
            .persist_direct_eoa_receipt(
                DirectEoaOperation::Split,
                Some(condition_id),
                finalized.transaction_hash.to_string(),
                succeeded,
                finalized.from,
                finalized.gas_used,
                finalized.effective_gas_price,
                finalized.block_number,
            )
            .await?;
        Ok(Some(if !succeeded {
            SplitReceiptStatus::Reverted
        } else if matches!(
            append_outcome,
            SessionReceiptAppendOutcome::Inserted | SessionReceiptAppendOutcome::IdenticalExisting
        ) {
            SplitReceiptStatus::Confirmed
        } else {
            SplitReceiptStatus::ReconciliationRequired {
                error: format!(
                    "split finalized receipt reconciliation required append_outcome={append_outcome:?}"
                ),
            }
        }))
    }
}

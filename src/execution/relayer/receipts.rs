use super::*;
use crate::trade_session_ledger::{
    direct_eoa_submission_receipt, DirectEoaGasReceipt, SettlementCollateralReceipt,
    SettlementCollateralReconciliationReason, SettlementFinalizedLogIdentity,
    SettlementPayoutRedemptionEventReceipt, SettlementWrappedEventReceipt,
};
use alloy::sol_types::SolEvent;

impl PolymarketExecutionLive {
    pub(super) async fn persist_direct_eoa_receipt(
        &self,
        operation: DirectEoaOperation,
        condition_id: Option<B256>,
        tx_hash: String,
        status: bool,
        payer: Address,
        gas_used: u64,
        effective_gas_price_wei: u128,
        block_number: Option<u64>,
    ) -> Result<SessionReceiptAppendOutcome> {
        let submission = direct_eoa_submission_receipt(&tx_hash)?;
        if operation == DirectEoaOperation::Split {
            submission.required_split_identity()?;
        }
        anyhow::ensure!(
            submission.operation == operation
                && submission.condition_id == condition_id.map(|value| value.to_string())
                && submission.payer == payer.to_string(),
            "finalized direct EOA gas receipt does not match durable submission tx_hash={tx_hash}"
        );
        append_session_receipt(SessionReceipt::DirectEoaGas(DirectEoaGasReceipt {
            run_id: submission.run_id,
            ledger_seq: 0,
            ts_ms: crate::event_ledger::now_ms(),
            tx_hash,
            payer: payer.to_string(),
            gas_used,
            effective_gas_price_wei,
            block_number,
            status,
            operation,
            condition_id: condition_id.map(|value| value.to_string()),
        }))
    }

    pub(super) async fn persist_settlement_collateral_receipt(
        &self,
        operation: DirectEoaOperation,
        condition_id: B256,
        expected_amount_units_6dp: Option<U256>,
        receipt: &alloy::rpc::types::TransactionReceipt,
    ) -> Result<(SettlementCollateralReceipt, SessionReceiptAppendOutcome)> {
        anyhow::ensure!(
            matches!(
                operation,
                DirectEoaOperation::Merge | DirectEoaOperation::Redeem
            ),
            "settlement collateral receipt requires merge or redeem operation"
        );
        let tx_hash = receipt.transaction_hash.to_string();
        let submission = direct_eoa_submission_receipt(&tx_hash)?;
        let condition_id_text = condition_id.to_string();
        let expected_amount_units_6dp_exact = expected_amount_units_6dp
            .map(|amount| {
                anyhow::ensure!(
                    amount <= U256::from(u128::MAX),
                    "settlement signed amount exceeds exact u128 receipt range tx_hash={tx_hash}"
                );
                Ok(amount.to::<u128>())
            })
            .transpose()?;
        anyhow::ensure!(
            submission.operation == operation
                && submission.condition_id.as_deref() == Some(condition_id_text.as_str())
                && submission.payer == receipt.from.to_string()
                && submission.amount_units_6dp == expected_amount_units_6dp_exact,
            "finalized settlement collateral receipt does not match durable submission tx_hash={tx_hash}"
        );
        let run_id = submission.run_id;
        let recipient = receipt.from;
        let wrapped_logs = receipt
            .logs()
            .iter()
            .filter(|log| {
                !log.removed
                    && log.address() == contracts_v2::PUSD_COLLATERAL
                    && log.topics().first().copied()
                        == Some(IPolymarketCollateralToken::Wrapped::SIGNATURE_HASH)
            })
            .collect::<Vec<_>>();
        let payout_redemption_logs = if operation == DirectEoaOperation::Redeem {
            receipt
                .logs()
                .iter()
                .filter(|log| {
                    !log.removed
                        && log.address() == contracts_v2::CONDITIONAL_TOKENS
                        && log.topics().first().copied()
                            == Some(IConditionalTokens::PayoutRedemption::SIGNATURE_HASH)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut wrapped_failure = None;
        let wrapped_evidence = match wrapped_logs.as_slice() {
            [] => {
                wrapped_failure = Some(SettlementCollateralReconciliationReason::MissingWrapped);
                None
            }
            [log] => match log.log_decode_validate::<IPolymarketCollateralToken::Wrapped>() {
                Ok(decoded) => {
                    let event = &decoded.inner.data;
                    let valid_identity = event.caller == contracts_v2::CTF_COLLATERAL_ADAPTER
                        && event.asset == contracts_v2::USDC_E_COLLATERAL
                        && event.to == recipient;
                    match (
                        valid_identity,
                        log.transaction_hash,
                        log.block_number,
                        log.block_hash,
                        log.log_index,
                    ) {
                        (
                            true,
                            Some(transaction_hash),
                            Some(block_number),
                            Some(block_hash),
                            Some(log_index),
                        ) if transaction_hash == receipt.transaction_hash
                            && Some(block_number) == receipt.block_number
                            && Some(block_hash) == receipt.block_hash =>
                        {
                            Some((
                                SettlementFinalizedLogIdentity {
                                    log_index,
                                    emitter: log.address().to_string(),
                                    transaction_hash: transaction_hash.to_string(),
                                    block_number,
                                    block_hash: block_hash.to_string(),
                                },
                                event.amount,
                            ))
                        }
                        _ => {
                            wrapped_failure =
                                Some(SettlementCollateralReconciliationReason::InvalidWrapped);
                            None
                        }
                    }
                }
                Err(_) => {
                    wrapped_failure =
                        Some(SettlementCollateralReconciliationReason::InvalidWrapped);
                    None
                }
            },
            _ => {
                wrapped_failure = Some(SettlementCollateralReconciliationReason::MultipleWrapped);
                None
            }
        };
        let mut payout_failure = None;
        let mut payout_redemption_evidence = None;
        let canonical_amount = match operation {
            DirectEoaOperation::Merge => match expected_amount_units_6dp {
                Some(amount) => Some(amount),
                None => {
                    payout_failure =
                        Some(SettlementCollateralReconciliationReason::MissingExpectedAmount);
                    None
                }
            },
            DirectEoaOperation::Redeem => match payout_redemption_logs.as_slice() {
                [] => {
                    payout_failure =
                        Some(SettlementCollateralReconciliationReason::MissingPayoutRedemption);
                    None
                }
                [log] => match log.log_decode_validate::<IConditionalTokens::PayoutRedemption>() {
                    Ok(decoded) => {
                        let event = &decoded.inner.data;
                        let valid_identity = event.redeemer == contracts_v2::CTF_COLLATERAL_ADAPTER
                            && event.collateralToken == contracts_v2::USDC_E_COLLATERAL
                            && event.parentCollectionId == B256::ZERO
                            && event.conditionId == condition_id
                            && event.indexSets == vec![U256::from(1), U256::from(2)]
                            && event.payout <= U256::from(u128::MAX);
                        match (
                            valid_identity,
                            log.transaction_hash,
                            log.block_number,
                            log.block_hash,
                            log.log_index,
                        ) {
                            (
                                true,
                                Some(transaction_hash),
                                Some(block_number),
                                Some(block_hash),
                                Some(log_index),
                            ) if transaction_hash == receipt.transaction_hash
                                && Some(block_number) == receipt.block_number
                                && Some(block_hash) == receipt.block_hash =>
                            {
                                payout_redemption_evidence = Some((
                                    SettlementFinalizedLogIdentity {
                                        log_index,
                                        emitter: log.address().to_string(),
                                        transaction_hash: transaction_hash.to_string(),
                                        block_number,
                                        block_hash: block_hash.to_string(),
                                    },
                                    event.payout,
                                ));
                                Some(event.payout)
                            }
                            _ => {
                                payout_failure = Some(
                                    SettlementCollateralReconciliationReason::InvalidPayoutRedemption,
                                );
                                None
                            }
                        }
                    }
                    Err(_) => {
                        payout_failure =
                            Some(SettlementCollateralReconciliationReason::InvalidPayoutRedemption);
                        None
                    }
                },
                _ => {
                    payout_failure =
                        Some(SettlementCollateralReconciliationReason::MultiplePayoutRedemptions);
                    None
                }
            },
            DirectEoaOperation::Approval | DirectEoaOperation::Split => {
                payout_failure =
                    Some(SettlementCollateralReconciliationReason::MissingExpectedAmount);
                None
            }
        };
        let observed_wrapped_amount = wrapped_evidence
            .as_ref()
            .map(|(_, amount)| amount.to_string());
        let outcome = if let Some(reason) = payout_failure {
            SettlementCollateralOutcome::ReconciliationRequired {
                reason,
                matching_wrapped_count: wrapped_logs.len(),
                matching_payout_redemption_count: payout_redemption_logs.len(),
                expected_amount_units_6dp: canonical_amount.map(|value| value.to_string()),
                observed_amount_units_6dp: observed_wrapped_amount,
            }
        } else if let Some(reason) = wrapped_failure {
            SettlementCollateralOutcome::ReconciliationRequired {
                reason,
                matching_wrapped_count: wrapped_logs.len(),
                matching_payout_redemption_count: payout_redemption_logs.len(),
                expected_amount_units_6dp: canonical_amount.map(|value| value.to_string()),
                observed_amount_units_6dp: observed_wrapped_amount,
            }
        } else if let Some(canonical_amount) = canonical_amount {
            match wrapped_evidence {
                Some((_wrapped_log, amount)) if amount > U256::from(u128::MAX) => {
                    SettlementCollateralOutcome::ReconciliationRequired {
                        reason: SettlementCollateralReconciliationReason::AmountOverflow,
                        matching_wrapped_count: 1,
                        matching_payout_redemption_count: payout_redemption_logs.len(),
                        expected_amount_units_6dp: Some(canonical_amount.to_string()),
                        observed_amount_units_6dp: Some(amount.to_string()),
                    }
                }
                Some((_wrapped_log, amount)) if amount != canonical_amount => {
                    SettlementCollateralOutcome::ReconciliationRequired {
                        reason: SettlementCollateralReconciliationReason::AmountMismatch,
                        matching_wrapped_count: 1,
                        matching_payout_redemption_count: payout_redemption_logs.len(),
                        expected_amount_units_6dp: Some(canonical_amount.to_string()),
                        observed_amount_units_6dp: Some(amount.to_string()),
                    }
                }
                Some((wrapped_log, amount)) => SettlementCollateralOutcome::Confirmed {
                    wrapped: SettlementWrappedEventReceipt {
                        log: wrapped_log,
                        caller: contracts_v2::CTF_COLLATERAL_ADAPTER.to_string(),
                        asset: contracts_v2::USDC_E_COLLATERAL.to_string(),
                        to: recipient.to_string(),
                        amount_units_6dp: amount.to::<u128>(),
                    },
                    payout_redemption: payout_redemption_evidence.map(|(log, payout)| {
                        SettlementPayoutRedemptionEventReceipt {
                            log,
                            redeemer: contracts_v2::CTF_COLLATERAL_ADAPTER.to_string(),
                            collateral_token: contracts_v2::USDC_E_COLLATERAL.to_string(),
                            parent_collection_id: B256::ZERO.to_string(),
                            condition_id: condition_id_text.clone(),
                            index_sets: [1, 2],
                            payout_units_6dp: payout.to::<u128>(),
                        }
                    }),
                },
                None => SettlementCollateralOutcome::ReconciliationRequired {
                    reason: SettlementCollateralReconciliationReason::InvalidWrapped,
                    matching_wrapped_count: wrapped_logs.len(),
                    matching_payout_redemption_count: payout_redemption_logs.len(),
                    expected_amount_units_6dp: Some(canonical_amount.to_string()),
                    observed_amount_units_6dp: None,
                },
            }
        } else {
            SettlementCollateralOutcome::ReconciliationRequired {
                reason: if operation == DirectEoaOperation::Redeem {
                    SettlementCollateralReconciliationReason::InvalidPayoutRedemption
                } else {
                    SettlementCollateralReconciliationReason::MissingExpectedAmount
                },
                matching_wrapped_count: wrapped_logs.len(),
                matching_payout_redemption_count: payout_redemption_logs.len(),
                expected_amount_units_6dp: None,
                observed_amount_units_6dp: observed_wrapped_amount,
            }
        };
        if matches!(
            &outcome,
            SettlementCollateralOutcome::ReconciliationRequired { .. }
        ) {
            tracing::error!(
                target: "execution.relayer",
                condition_id = %condition_id_text,
                tx_hash = %receipt.transaction_hash,
                ?operation,
                expected_amount_units_6dp = ?expected_amount_units_6dp,
                ?outcome,
                "settlement_collateral_receipt_reconciliation_required"
            );
        }
        let settlement_receipt = SettlementCollateralReceipt {
            run_id,
            ledger_seq: 0,
            ts_ms: crate::event_ledger::now_ms(),
            tx_hash: receipt.transaction_hash.to_string(),
            token_contract: contracts_v2::PUSD_COLLATERAL.to_string(),
            recipient: recipient.to_string(),
            condition_id: condition_id_text,
            operation,
            outcome,
        };
        let append_outcome = append_session_receipt(SessionReceipt::SettlementCollateral(
            settlement_receipt.clone(),
        ))?;
        Ok((settlement_receipt, append_outcome))
    }
}

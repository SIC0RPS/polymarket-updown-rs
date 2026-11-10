use super::*;
use crate::polymarket::contracts_v2;
use crate::trade_session_ledger::{
    append_session_receipt, DirectEoaOperation, DirectEoaSplitIdentity, DirectEoaSubmissionReceipt,
    SessionReceipt, SessionReceiptAppendOutcome, SettlementCollateralOutcome,
};
use alloy::eips::eip2718::Encodable2718;
use alloy::providers::Provider;
use serde::Deserialize;

mod ctf_operations;
mod receipts;

#[derive(Debug, Deserialize)]
struct PolygonBlockRpc {
    number: String,
    hash: String,
}

#[derive(Debug)]
enum DirectEoaBroadcastOutcome {
    Submitted(B256),
    ReconciliationRequired { tx_hash: B256, error: String },
}

sol! {
    #[sol(rpc)]
    interface IERC20Approval {
        function approve(address spender, uint256 value) external returns (bool);
    }
}

impl PolymarketExecutionLive {
    const SETTLEMENT_RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);
    const SETTLEMENT_FINALITY_POLL_INTERVAL: Duration = Duration::from_millis(250);

    fn rpc_hex_u64(raw: &str, field: &str) -> Result<u64> {
        let digits = raw
            .strip_prefix("0x")
            .filter(|digits| !digits.is_empty())
            .with_context(|| format!("invalid Polygon RPC {field} {raw}"))?;
        u64::from_str_radix(digits, 16)
            .with_context(|| format!("invalid Polygon RPC {field} {raw}"))
    }

    pub(super) async fn await_polygon_finality(
        &self,
        transaction_hash: B256,
        poll_interval: Duration,
    ) -> Result<alloy::rpc::types::TransactionReceipt> {
        let observed_chain_id = self
            .finality_provider
            .get_chain_id()
            .await
            .context("query Polygon finality provider chain")?;
        anyhow::ensure!(
            observed_chain_id == self.chain_id,
            "Polygon receipt provider chain mismatch expected={} actual={observed_chain_id}",
            self.chain_id
        );
        loop {
            let receipt = match tokio::time::timeout(
                Self::SETTLEMENT_RECEIPT_TIMEOUT,
                self.finality_provider
                    .get_transaction_receipt(transaction_hash),
            )
            .await
            {
                Ok(Ok(Some(receipt))) => receipt,
                Ok(Ok(None)) => {
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        target: "execution.relayer",
                        transaction_hash = %transaction_hash,
                        error = %error,
                        "polygon_receipt_query_failed_retrying"
                    );
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        target: "execution.relayer",
                        transaction_hash = %transaction_hash,
                        error = %error,
                        "polygon_receipt_query_timed_out_retrying"
                    );
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };
            anyhow::ensure!(
                receipt.transaction_hash == transaction_hash,
                "Polygon receipt transaction identity mismatch requested={transaction_hash} received={}",
                receipt.transaction_hash
            );
            match self
                .finalized_receipt_is_canonical(transaction_hash, &receipt)
                .await
            {
                Ok(true) => return Ok(receipt),
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    target: "execution.relayer",
                    transaction_hash = %transaction_hash,
                    error = %error,
                    "polygon_finality_check_failed_retrying"
                ),
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn finalized_receipt_is_canonical(
        &self,
        transaction_hash: B256,
        receipt: &alloy::rpc::types::TransactionReceipt,
    ) -> Result<bool> {
        let (Some(receipt_block_number), Some(receipt_block_hash)) =
            (receipt.block_number, receipt.block_hash)
        else {
            return Ok(false);
        };
        let rpc_url = self.rpc_url();
        let finalized = crate::polygon_rpc::result_value(
            &self.finality_http,
            &rpc_url,
            "eth_getBlockByNumber",
            serde_json::json!(["finalized", false]),
        )
        .await
        .and_then(|value| {
            serde_json::from_value::<PolygonBlockRpc>(value)
                .context("decode Polygon finalized block")
        })
        .and_then(|block| Self::rpc_hex_u64(&block.number, "finalized block number"))?;
        if finalized < receipt_block_number {
            return Ok(false);
        }
        let canonical = crate::polygon_rpc::result_value(
            &self.finality_http,
            &rpc_url,
            "eth_getBlockByNumber",
            serde_json::json!([format!("0x{receipt_block_number:x}"), false]),
        )
        .await
        .and_then(|value| {
            serde_json::from_value::<PolygonBlockRpc>(value)
                .context("decode Polygon canonical receipt block")
        })
        .and_then(|block| {
            anyhow::ensure!(
                Self::rpc_hex_u64(&block.number, "canonical block number")? == receipt_block_number,
                "Polygon canonical block number mismatch"
            );
            block
                .hash
                .parse::<B256>()
                .context("decode Polygon canonical block hash")
        })?;
        if canonical != receipt_block_hash {
            tracing::warn!(
                target: "execution.relayer",
                transaction_hash = %transaction_hash,
                receipt_block_number,
                receipt_block_hash = %receipt_block_hash,
                canonical_block_hash = %canonical,
                "polygon_receipt_block_hash_mismatch"
            );
            return Ok(false);
        }
        Ok(true)
    }

    pub(super) async fn observe_polygon_finality_once(
        &self,
        transaction_hash: B256,
    ) -> Result<Option<alloy::rpc::types::TransactionReceipt>> {
        let attempt = async {
            let observed_chain_id = match self.finality_provider.get_chain_id().await {
                Ok(chain_id) => chain_id,
                Err(error) => {
                    tracing::warn!(
                        target: "execution.relayer",
                        transaction_hash = %transaction_hash,
                        error = %error,
                        "polygon_receipt_chain_query_unresolved"
                    );
                    return Ok(None);
                }
            };
            anyhow::ensure!(
                observed_chain_id == self.chain_id,
                "Polygon receipt provider chain mismatch expected={} actual={observed_chain_id}",
                self.chain_id
            );
            let receipt = match self
                .finality_provider
                .get_transaction_receipt(transaction_hash)
                .await
            {
                Ok(Some(receipt)) => receipt,
                Ok(None) => return Ok(None),
                Err(error) => {
                    tracing::warn!(
                        target: "execution.relayer",
                        transaction_hash = %transaction_hash,
                        error = %error,
                        "polygon_receipt_query_unresolved"
                    );
                    return Ok(None);
                }
            };
            anyhow::ensure!(
                receipt.transaction_hash == transaction_hash,
                "Polygon receipt transaction identity mismatch requested={transaction_hash} received={}",
                receipt.transaction_hash
            );
            match self
                .finalized_receipt_is_canonical(transaction_hash, &receipt)
                .await
            {
                Ok(true) => Ok(Some(receipt)),
                Ok(false) => Ok(None),
                Err(error) => {
                    tracing::warn!(
                        target: "execution.relayer",
                        transaction_hash = %transaction_hash,
                        error = %error,
                        "polygon_finality_check_unresolved"
                    );
                    Ok(None)
                }
            }
        };
        match tokio::time::timeout(Self::SETTLEMENT_RECEIPT_TIMEOUT, attempt).await {
            Ok(result) => result,
            Err(error) => {
                tracing::warn!(
                    target: "execution.relayer",
                    transaction_hash = %transaction_hash,
                    error = %error,
                    "polygon_finality_observation_timed_out_unresolved"
                );
                Ok(None)
            }
        }
    }

    async fn persist_direct_eoa_submission(
        &self,
        operation: DirectEoaOperation,
        condition_id: Option<B256>,
        amount_units_6dp: Option<u128>,
        split_identity: Option<DirectEoaSplitIdentity>,
        split_entry_journal: Option<crate::engine_trade_journal::CtfSplitEntryJournal>,
        tx_hash: B256,
    ) -> Result<SessionReceiptAppendOutcome> {
        let run_id = crate::event_ledger::current_run_id()
            .context("direct EOA submission requires active run identity")?;
        anyhow::ensure!(
            crate::trade_session_ledger::is_live_run_id(&run_id),
            "direct EOA submission requires live run identity"
        );
        append_session_receipt(SessionReceipt::DirectEoaSubmission(
            DirectEoaSubmissionReceipt {
                run_id,
                ledger_seq: 0,
                ts_ms: crate::event_ledger::now_ms(),
                tx_hash: tx_hash.to_string(),
                payer: self.signer.address().to_string(),
                operation,
                condition_id: condition_id.map(|value| value.to_string()),
                amount_units_6dp,
                split_identity,
                split_entry_journal,
            },
        ))
    }

    async fn broadcast_signed_transaction<P>(
        &self,
        provider: &P,
        envelope: alloy::consensus::TxEnvelope,
        operation: DirectEoaOperation,
        condition_id: Option<B256>,
        amount_units_6dp: Option<u128>,
        split_identity: Option<DirectEoaSplitIdentity>,
        split_entry_journal: Option<crate::engine_trade_journal::CtfSplitEntryJournal>,
    ) -> Result<DirectEoaBroadcastOutcome>
    where
        P: Provider,
    {
        match operation {
            DirectEoaOperation::Split => {
                let identity = split_identity
                    .as_ref()
                    .context("signed direct EOA split transaction identity is missing")?;
                let journal = split_entry_journal
                    .as_ref()
                    .context("signed direct EOA split entry journal is missing")?;
                identity.validate_entry_journal(journal)?;
                anyhow::ensure!(
                    condition_id.map(|value| value.to_string())
                        == Some(identity.condition_id.clone())
                        && amount_units_6dp == Some(identity.amount_units_6dp),
                    "signed direct EOA split transaction identity conflicts with call fields"
                );
            }
            DirectEoaOperation::Merge => {
                anyhow::ensure!(
                    amount_units_6dp.is_some_and(|amount| amount > 0),
                    "signed direct EOA merge transaction amount is missing or zero"
                );
                anyhow::ensure!(
                    split_identity.is_none() && split_entry_journal.is_none(),
                    "signed direct EOA merge transaction carries split identity or journal"
                );
            }
            DirectEoaOperation::Approval | DirectEoaOperation::Redeem => anyhow::ensure!(
                amount_units_6dp.is_none()
                    && split_identity.is_none()
                    && split_entry_journal.is_none(),
                "signed direct EOA approval/redeem transaction carries amount, split identity, or journal"
            ),
        }
        let tx_hash = *envelope.tx_hash();
        let append_outcome = self
            .persist_direct_eoa_submission(
                operation,
                condition_id,
                amount_units_6dp,
                split_identity,
                split_entry_journal,
                tx_hash,
            )
            .await?;
        anyhow::ensure!(
            matches!(
                append_outcome,
                SessionReceiptAppendOutcome::Inserted
                    | SessionReceiptAppendOutcome::IdenticalExisting
            ),
            "signed direct EOA transaction identity rejected before broadcast tx_hash={tx_hash} operation={operation:?}"
        );
        let encoded = envelope.encoded_2718();
        loop {
            match provider.send_raw_transaction(&encoded).await {
                Ok(pending) => {
                    if *pending.tx_hash() != tx_hash {
                        return Ok(DirectEoaBroadcastOutcome::ReconciliationRequired {
                            tx_hash,
                            error: format!(
                                "Polygon broadcast transaction identity mismatch local={tx_hash} remote={}",
                                pending.tx_hash()
                            ),
                        });
                    }
                    return Ok(DirectEoaBroadcastOutcome::Submitted(tx_hash));
                }
                Err(error) => {
                    match provider.get_transaction_by_hash(tx_hash).await {
                        Ok(Some(_)) => {
                            return Ok(DirectEoaBroadcastOutcome::Submitted(tx_hash));
                        }
                        Ok(None) => tracing::warn!(
                            target: "execution.relayer",
                            transaction_hash = %tx_hash,
                            error = %error,
                            "signed_transaction_broadcast_failed_retrying_same_raw_transaction"
                        ),
                        Err(lookup_error) => tracing::warn!(
                            target: "execution.relayer",
                            transaction_hash = %tx_hash,
                            error = %error,
                            lookup_error = %lookup_error,
                            "signed_transaction_broadcast_and_lookup_failed_retrying_same_raw_transaction"
                        ),
                    }
                    tokio::time::sleep(Self::SETTLEMENT_FINALITY_POLL_INTERVAL).await;
                }
            }
        }
    }

    fn erc20_balance_of_call(owner: Address) -> String {
        let owner = owner.to_string();
        let owner = owner.strip_prefix("0x").unwrap_or(owner.as_str());
        format!("0x70a08231{owner:0>64}")
    }

    fn erc20_allowance_call(owner: Address, spender: Address) -> String {
        let owner = owner.to_string();
        let spender = spender.to_string();
        let owner = owner.strip_prefix("0x").unwrap_or(owner.as_str());
        let spender = spender.strip_prefix("0x").unwrap_or(spender.as_str());
        format!("0xdd62ed3e{owner:0>64}{spender:0>64}")
    }

    async fn eth_call_u256(&self, to: Address, data: String, context: &str) -> Result<U256> {
        let rpc_url = self.rpc_url();
        let http = reqwest::Client::new();
        let hex = crate::polygon_rpc::eth_call(&http, &rpc_url, &to.to_string(), &data)
            .await?
            .with_context(|| format!("{context}: missing eth_call result"))?;
        U256::from_str_radix(hex.strip_prefix("0x").unwrap_or(hex.as_str()), 16)
            .with_context(|| format!("{context}: invalid u256 eth_call result {hex}"))
    }

    async fn preflight_split_funding(
        &self,
        owner: Address,
        collateral: Address,
        ctf: Address,
        amount: U256,
    ) -> Result<()> {
        let balance = self
            .eth_call_u256(
                collateral,
                Self::erc20_balance_of_call(owner),
                "ctf split balance",
            )
            .await?;
        let allowance = self
            .eth_call_u256(
                collateral,
                Self::erc20_allowance_call(owner, ctf),
                "ctf split allowance",
            )
            .await?;
        anyhow::ensure!(
            balance >= amount,
            "ctf_split_preflight_failed owner={owner} balance={balance} required={amount}"
        );
        if allowance < amount {
            tracing::info!(
                owner = %owner,
                spender = %ctf,
                allowance = %allowance,
                required = %amount,
                "ctf_split_approval_missing_auto_approving"
            );
            self.ensure_split_allowance(collateral, ctf)
                .await
                .context("ctf split auto-approve failed")?;
            let allowance = self
                .eth_call_u256(
                    collateral,
                    Self::erc20_allowance_call(owner, ctf),
                    "ctf split allowance after approve",
                )
                .await?;
            anyhow::ensure!(
                allowance >= amount,
                "ctf_split_preflight_failed owner={owner} spender={ctf} allowance={allowance} required={amount}"
            );
        }
        Ok(())
    }

    async fn ensure_split_allowance(&self, collateral: Address, ctf: Address) -> Result<()> {
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
                    "failed to connect provider for split approval via {}",
                    crate::polygon_rpc::endpoint_label(&rpc_url)
                )
            })?;
        let usdc = IERC20Approval::new(collateral, &provider);
        let signed = provider
            .fill(usdc.approve(ctf, U256::MAX).into_transaction_request())
            .await
            .with_context(|| {
                format!("pUSD approve signing failed collateral={collateral} spender={ctf}")
            })?
            .try_into_envelope()
            .map_err(|_| anyhow::anyhow!("pUSD approve transaction was not signed locally"))?;
        let tx_hash = match self
            .broadcast_signed_transaction(
                &provider,
                signed,
                DirectEoaOperation::Approval,
                None,
                None,
                None,
                None,
            )
            .await?
        {
            DirectEoaBroadcastOutcome::Submitted(tx_hash) => tx_hash,
            DirectEoaBroadcastOutcome::ReconciliationRequired { tx_hash, error } => {
                anyhow::bail!(
                    "pUSD approve broadcast requires reconciliation tx_hash={tx_hash}: {error}"
                );
            }
        };
        let receipt = self
            .await_polygon_finality(tx_hash, Self::SETTLEMENT_FINALITY_POLL_INTERVAL)
            .await
            .with_context(|| {
                format!("pUSD approve receipt failed collateral={collateral} spender={ctf}")
            })?;
        let append_outcome = self
            .persist_direct_eoa_receipt(
                DirectEoaOperation::Approval,
                None,
                receipt.transaction_hash.to_string(),
                receipt.status(),
                receipt.from,
                receipt.gas_used,
                receipt.effective_gas_price,
                receipt.block_number,
            )
            .await?;
        anyhow::ensure!(
            matches!(
                append_outcome,
                SessionReceiptAppendOutcome::Inserted
                    | SessionReceiptAppendOutcome::IdenticalExisting
            ),
            "pUSD approve finalized receipt requires reconciliation tx_hash={tx_hash} append_outcome={append_outcome:?}"
        );
        anyhow::ensure!(
            receipt.status(),
            "pUSD approve reverted collateral={collateral} spender={ctf} tx_hash={tx_hash}"
        );
        tracing::info!(
            owner = %self.signer.address(),
            collateral = %collateral,
            spender = %ctf,
            tx_hash = %tx_hash,
            "ctf_split_approval_confirmed"
        );
        Ok(())
    }

    pub fn redeem_route_label(&self) -> Result<&'static str> {
        Ok("direct_eoa")
    }

    pub fn redeem_precheck(&self) -> Result<()> {
        Ok(())
    }

    pub fn merge_precheck(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn trigger_redeem_now(&self) -> Result<()> {
        self.reclaim_trigger.notify_one();
        Ok(())
    }

    pub fn reclaim_trigger(&self) -> Arc<Notify> {
        self.reclaim_trigger.clone()
    }

    pub(crate) fn rpc_url(&self) -> String {
        crate::polygon_rpc::preferred_url(&self.account_rpc_url)
    }
}

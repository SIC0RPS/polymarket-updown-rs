use crate::trade_session_ledger::{
    load_finalized_condition_resolution_receipt_for_run, FinalizedConditionResolutionReceipt,
};
use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;

use super::*;

const CONDITION_RESOLUTION_LOG_BLOCK_RANGE: u64 = 10_000;

impl PolymarketExecutionLive {
    pub(crate) async fn condition_payout_vector(
        &self,
        condition_id: B256,
    ) -> Result<Option<ConditionPayoutVector>> {
        let run_id = crate::event_ledger::current_run_id()
            .context("condition resolution run identity is unavailable")?;
        anyhow::ensure!(
            !run_id.trim().is_empty() && run_id.trim() == run_id,
            "condition resolution run identity is missing or noncanonical"
        );
        if let Some(resolution) =
            load_finalized_condition_resolution_receipt_for_run(&run_id, &condition_id.to_string())?
        {
            return payout_vector_from_resolution(condition_id, &resolution).map(Some);
        }

        let rpc_url = self.rpc_url();
        let provider = ProviderBuilder::new()
            .connect(&rpc_url)
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "failed to connect provider for payout read via {}",
                    crate::polygon_rpc::endpoint_label(&rpc_url)
                )
            })?;
        let finalized_block = provider
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .context("read Polygon finalized block for payout state")?
            .context("Polygon finalized block is unavailable for payout state")?;
        let finalized_head_number = finalized_block.header.inner.number;
        let finalized = BlockId::Hash(finalized_block.hash().into());
        let ctf = IConditionalTokens::new(contracts_v2::CONDITIONAL_TOKENS, provider.clone());
        let denominator = ctf
            .payoutDenominator(condition_id)
            .block(finalized)
            .call()
            .await
            .with_context(|| {
                format!("payoutDenominator finalized read failed condition_id={condition_id}")
            })?;
        if denominator == U256::ZERO {
            return Ok(None);
        }
        let outcome_slot_0_numerator = ctf
            .payoutNumerators(condition_id, U256::ZERO)
            .block(finalized)
            .call()
            .await
            .with_context(|| {
                format!(
                    "payoutNumerators finalized read failed condition_id={condition_id} index=0"
                )
            })?;
        let outcome_slot_1_numerator = ctf
            .payoutNumerators(condition_id, U256::from(1))
            .block(finalized)
            .call()
            .await
            .with_context(|| {
                format!(
                    "payoutNumerators finalized read failed condition_id={condition_id} index=1"
                )
            })?;
        let denominator = u256_to_u128_exact(denominator, "payout denominator")?;
        let outcome_slot_0_numerator =
            u256_to_u128_exact(outcome_slot_0_numerator, "outcome slot 0 payout numerator")?;
        let outcome_slot_1_numerator =
            u256_to_u128_exact(outcome_slot_1_numerator, "outcome slot 1 payout numerator")?;
        anyhow::ensure!(
            denominator > 0
                && outcome_slot_0_numerator.checked_add(outcome_slot_1_numerator)
                    == Some(denominator)
                && ((outcome_slot_0_numerator == denominator && outcome_slot_1_numerator == 0)
                    || (outcome_slot_1_numerator == denominator && outcome_slot_0_numerator == 0)),
            "finalized condition payout is not one exact binary winner condition_id={condition_id}"
        );

        let mut range_end = finalized_head_number;
        let resolution_log = loop {
            let range_start = range_end.saturating_sub(CONDITION_RESOLUTION_LOG_BLOCK_RANGE - 1);
            let filter = Filter::new()
                .address(contracts_v2::CONDITIONAL_TOKENS)
                .event_signature(IConditionalTokens::ConditionResolution::SIGNATURE_HASH)
                .topic1(condition_id)
                .from_block(range_start)
                .to_block(range_end);
            let logs = provider.get_logs(&filter).await.with_context(|| {
                format!(
                    "ConditionResolution log read failed condition_id={condition_id} range={range_start}..={range_end}"
                )
            })?;
            anyhow::ensure!(
                logs.len() <= 1,
                "condition has multiple finalized ConditionResolution logs condition_id={condition_id} range={range_start}..={range_end} count={}",
                logs.len()
            );
            if let Some(log) = logs.into_iter().next() {
                break log;
            }
            anyhow::ensure!(
                range_start > 0,
                "finalized payout state has no ConditionResolution log condition_id={condition_id}"
            );
            range_end = range_start - 1;
        };
        anyhow::ensure!(
            !resolution_log.removed,
            "ConditionResolution log is removed condition_id={condition_id}"
        );
        let decoded = resolution_log
            .log_decode_validate::<IConditionalTokens::ConditionResolution>()
            .context("decode ConditionResolution log")?;
        let event = &decoded.inner.data;
        anyhow::ensure!(
            event.conditionId == condition_id
                && event.outcomeSlotCount == U256::from(2)
                && event.payoutNumerators.len() == 2,
            "ConditionResolution log has foreign identity or non-binary payout vector condition_id={condition_id}"
        );
        let event_outcome_slot_0_numerator = u256_to_u128_exact(
            event.payoutNumerators[0],
            "event outcome slot 0 payout numerator",
        )?;
        let event_outcome_slot_1_numerator = u256_to_u128_exact(
            event.payoutNumerators[1],
            "event outcome slot 1 payout numerator",
        )?;
        let event_denominator = event_outcome_slot_0_numerator
            .checked_add(event_outcome_slot_1_numerator)
            .context("ConditionResolution payout denominator overflow")?;
        anyhow::ensure!(
            event_outcome_slot_0_numerator == outcome_slot_0_numerator
                && event_outcome_slot_1_numerator == outcome_slot_1_numerator
                && event_denominator == denominator,
            "ConditionResolution log does not match finalized payout state condition_id={condition_id}"
        );

        let resolution_tx_hash = resolution_log
            .transaction_hash
            .context("ConditionResolution log has no transaction hash")?;
        let resolution_log_index = resolution_log
            .log_index
            .context("ConditionResolution log has no log index")?;
        let resolution_block_number = resolution_log
            .block_number
            .context("ConditionResolution log has no block number")?;
        let resolution_block_hash = resolution_log
            .block_hash
            .context("ConditionResolution log has no block hash")?;
        anyhow::ensure!(
            resolution_block_number <= finalized_head_number,
            "ConditionResolution block follows pinned finalized head condition_id={condition_id}"
        );
        let resolution_tx_receipt = self
            .await_polygon_finality(resolution_tx_hash, Self::SETTLEMENT_FINALITY_POLL_INTERVAL)
            .await
            .with_context(|| {
                format!(
                    "ConditionResolution transaction finality failed condition_id={condition_id} tx_hash={resolution_tx_hash}"
                )
            })?;
        anyhow::ensure!(
            resolution_tx_receipt.status()
                && resolution_tx_receipt.transaction_hash == resolution_tx_hash
                && resolution_tx_receipt.block_number == Some(resolution_block_number)
                && resolution_tx_receipt.block_hash == Some(resolution_block_hash),
            "ConditionResolution transaction receipt identity mismatch condition_id={condition_id}"
        );
        let resolution_block = provider
            .get_block_by_hash(resolution_block_hash)
            .await
            .context("read ConditionResolution block")?
            .context("ConditionResolution block is unavailable")?;
        anyhow::ensure!(
            resolution_block.hash() == resolution_block_hash
                && resolution_block.header.inner.number == resolution_block_number,
            "ConditionResolution block identity mismatch condition_id={condition_id}"
        );
        let finalized_block_timestamp_ms = resolution_block
            .header
            .inner
            .timestamp
            .checked_mul(1_000)
            .context("ConditionResolution block timestamp overflow")?;
        let observed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock before UNIX epoch")?
            .as_millis()
            .try_into()
            .context("condition resolution observation timestamp overflow")?;
        let receipt = FinalizedConditionResolutionReceipt {
            run_id: run_id.clone(),
            ledger_seq: 0,
            observed_at_ms,
            condition_id: condition_id.to_string(),
            resolution_tx_hash: resolution_tx_hash.to_string(),
            resolution_log_index,
            finalized_block_number: resolution_block_number,
            finalized_block_hash: resolution_block_hash.to_string(),
            finalized_block_timestamp_ms,
            outcome_slot_0_numerator,
            outcome_slot_1_numerator,
            denominator,
        };
        receipt.validate_exact()?;
        let record =
            crate::engine_trade_journal::build_resolution_observed_record(&run_id, receipt)?;
        crate::engine_trade_journal::append(record)?;
        let resolution = load_finalized_condition_resolution_receipt_for_run(
            &run_id,
            &condition_id.to_string(),
        )?
        .context("persisted condition resolution receipt is unavailable")?;
        payout_vector_from_resolution(condition_id, &resolution).map(Some)
    }
}

fn payout_vector_from_resolution(
    condition_id: B256,
    resolution: &FinalizedConditionResolutionReceipt,
) -> Result<ConditionPayoutVector> {
    resolution.validate_exact()?;
    anyhow::ensure!(
        resolution.condition_id == condition_id.to_string()
            && ((resolution.outcome_slot_0_numerator == resolution.denominator
                && resolution.outcome_slot_1_numerator == 0)
                || (resolution.outcome_slot_1_numerator == resolution.denominator
                    && resolution.outcome_slot_0_numerator == 0)),
        "persisted condition resolution is foreign or not one exact binary winner condition_id={condition_id}"
    );
    Ok(ConditionPayoutVector {
        condition_id,
        outcome_slot_0_numerator: resolution.outcome_slot_0_numerator,
        outcome_slot_1_numerator: resolution.outcome_slot_1_numerator,
        denominator: resolution.denominator,
    })
}

fn u256_to_u128_exact(value: U256, label: &str) -> Result<u128> {
    anyhow::ensure!(
        value <= U256::from(u128::MAX),
        "{label} exceeds exact u128 range"
    );
    Ok(value.to::<u128>())
}

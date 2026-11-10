use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use super::EngineTradeJournalRecord;
use crate::execution::ConditionPayoutVector;
use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{
    load_records_from_path, trade_session_ledger_path_for_run, CanonicalCtfEvent,
    ConfirmedCtfTerminalFact, CtfTerminalOperation, CtfTerminalSettlementEvidence,
    DirectEoaOperation, SettlementCollateralReceipt,
};

#[path = "ctf_terminal/simulated_resolution.rs"]
mod simulated_resolution;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedCtfTerminalClearance {
    pub market_id: MarketId,
    pub condition_id: String,
    pub cohort_id: String,
    pub up_token: String,
    pub down_token: String,
    pub up_qty_units_6dp: u128,
    pub down_qty_units_6dp: u128,
    pub confirmed_at_ms: u64,
    up_outcome_index: Option<u8>,
    down_outcome_index: Option<u8>,
    payout: Option<ConditionPayoutVector>,
}

impl ConfirmedCtfTerminalClearance {
    pub fn confirmed_merge(
        market_id: MarketId,
        condition_id: String,
        cohort_id: String,
        up_token: String,
        down_token: String,
        up_qty_units_6dp: u128,
        down_qty_units_6dp: u128,
        confirmed_at_ms: u64,
    ) -> Self {
        Self {
            market_id,
            condition_id,
            cohort_id,
            up_token,
            down_token,
            up_qty_units_6dp,
            down_qty_units_6dp,
            confirmed_at_ms,
            up_outcome_index: None,
            down_outcome_index: None,
            payout: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn confirmed_redeem(
        market_id: MarketId,
        condition_id: String,
        cohort_id: String,
        up_token: String,
        down_token: String,
        up_qty_units_6dp: u128,
        down_qty_units_6dp: u128,
        up_outcome_index: Option<u8>,
        down_outcome_index: Option<u8>,
        confirmed_at_ms: u64,
        payout: ConditionPayoutVector,
    ) -> Self {
        Self {
            market_id,
            condition_id,
            cohort_id,
            up_token,
            down_token,
            up_qty_units_6dp,
            down_qty_units_6dp,
            confirmed_at_ms,
            up_outcome_index,
            down_outcome_index,
            payout: Some(payout),
        }
    }

    pub fn build_simulated_merge_record(
        &self,
        record_run_id: &str,
        operation_id: &str,
        amount_units_6dp: u128,
    ) -> Result<EngineTradeJournalRecord> {
        anyhow::ensure!(
            !record_run_id.trim().is_empty() && record_run_id.trim() == record_run_id,
            "simulated merge record run identity is missing or noncanonical"
        );
        anyhow::ensure!(
            operation_id.starts_with("sim-ctf-merge-")
                && self.up_qty_units_6dp > 0
                && self.up_qty_units_6dp == self.down_qty_units_6dp
                && self.up_qty_units_6dp == amount_units_6dp
                && self.up_outcome_index.is_none()
                && self.down_outcome_index.is_none()
                && self.payout.is_none(),
            "simulated merge operation and terminal clearance differ"
        );
        let collateral_proceeds_units_6dp = i128::try_from(amount_units_6dp)
            .context("simulated merge proceeds exceed signed accounting range")?;
        let fact = CanonicalCtfEvent::MergeConfirmed(ConfirmedCtfTerminalFact {
            settlement_evidence: CtfTerminalSettlementEvidence::SimulatedMergeOperation {
                operation_id: operation_id.to_string(),
            },
            market_id: self.market_id.clone(),
            condition_id: self.condition_id.clone(),
            cohort_id: self.cohort_id.clone(),
            up_token: self.up_token.clone(),
            down_token: self.down_token.clone(),
            up_terminal_qty_units_6dp: self.up_qty_units_6dp,
            down_terminal_qty_units_6dp: self.down_qty_units_6dp,
            up_outcome_index: None,
            down_outcome_index: None,
            up_collateral_proceeds_units_6dp: None,
            down_collateral_proceeds_units_6dp: None,
            collateral_proceeds_units_6dp,
            fee_units_6dp: 0,
            approved_direct_cost_units_6dp: 0,
        });
        fact.validate()?;
        Ok(terminal_record(
            record_run_id,
            self,
            fact,
            CtfTerminalOperation::Merge,
            operation_id,
            "simulated_terminal_operation",
        ))
    }
}

fn quantity_weighted_payout(
    legs: [(u128, Option<u8>, &str); 2],
    numerators: &[u128],
    denominator: u128,
) -> Result<u128> {
    anyhow::ensure!(denominator > 0, "terminal payout denominator is zero");
    let payout = legs
        .into_iter()
        .try_fold(U256::ZERO, |total, (quantity, index, side)| {
            if quantity == 0 {
                anyhow::ensure!(
                    index.is_none(),
                    "zero terminal {side} quantity carries an outcome index"
                );
                return Ok(total);
            }
            let index = index.context(format!(
                "nonzero terminal {side} quantity has no outcome index"
            ))?;
            let numerator = numerators
                .get(usize::from(index))
                .copied()
                .context("terminal outcome index is outside payout vector")?;
            let leg_payout = U256::from(quantity)
                .checked_mul(U256::from(numerator))
                .context("terminal payout multiplication overflow")?
                / U256::from(denominator);
            total
                .checked_add(leg_payout)
                .context("terminal payout sum overflow")
        })?;
    u128::try_from(payout).context("terminal payout exceeds exact accounting range")
}

pub fn build_confirmed_ctf_terminal_record(
    record_run_id: &str,
    clearance: &ConfirmedCtfTerminalClearance,
    receipt: &SettlementCollateralReceipt,
) -> Result<Option<EngineTradeJournalRecord>> {
    anyhow::ensure!(
        !record_run_id.trim().is_empty() && record_run_id.trim() == record_run_id,
        "confirmed terminal record run identity is missing or noncanonical"
    );
    let tx_hash = receipt.tx_hash.trim();
    anyhow::ensure!(
        !receipt.run_id.trim().is_empty() && !tx_hash.is_empty() && tx_hash == receipt.tx_hash,
        "confirmed terminal transaction identity is missing or noncanonical"
    );
    anyhow::ensure!(
        receipt.condition_id == clearance.condition_id,
        "terminal receipt/clearance condition identity mismatch"
    );
    anyhow::ensure!(
        receipt.token_contract == crate::polymarket::contracts_v2::PUSD_COLLATERAL.to_string()
            && !receipt.recipient.trim().is_empty(),
        "terminal receipt collateral or recipient identity mismatch"
    );
    anyhow::ensure!(
        clearance.confirmed_at_ms >= receipt.ts_ms && receipt.ts_ms > 0,
        "terminal clearance predates finalized collateral receipt"
    );
    anyhow::ensure!(
        clearance.up_token != clearance.down_token
            && !clearance.up_token.trim().is_empty()
            && !clearance.down_token.trim().is_empty(),
        "terminal outcome token identities are missing or duplicate"
    );
    let collateral_receipt_units_6dp = receipt.confirmed_amount_units_6dp()?;
    let mut up_collateral_proceeds_units_6dp = None;
    let mut down_collateral_proceeds_units_6dp = None;
    let operation = match receipt.operation {
        DirectEoaOperation::Merge => {
            anyhow::ensure!(
                clearance.payout.is_none(),
                "merge terminal receipt carries payout evidence"
            );
            anyhow::ensure!(
                clearance.up_qty_units_6dp > 0
                    && clearance.up_qty_units_6dp == clearance.down_qty_units_6dp
                    && clearance.up_qty_units_6dp == collateral_receipt_units_6dp,
                "merge terminal quantities do not equal finalized collateral receipt"
            );
            CtfTerminalOperation::Merge
        }
        DirectEoaOperation::Redeem => {
            let payout = clearance
                .payout
                .as_ref()
                .context("redeem terminal receipt has no finalized payout")?;
            anyhow::ensure!(
                payout.condition_id.to_string() == clearance.condition_id,
                "redeem finalized payout is foreign"
            );
            if clearance.up_qty_units_6dp > 0 && clearance.down_qty_units_6dp > 0 {
                anyhow::ensure!(
                    clearance.up_outcome_index != clearance.down_outcome_index,
                    "redeem terminal binary legs have duplicate outcome indices"
                );
            }
            let expected = quantity_weighted_payout(
                [
                    (clearance.up_qty_units_6dp, clearance.up_outcome_index, "up"),
                    (
                        clearance.down_qty_units_6dp,
                        clearance.down_outcome_index,
                        "down",
                    ),
                ],
                &[
                    payout.outcome_slot_0_numerator,
                    payout.outcome_slot_1_numerator,
                ],
                payout.denominator,
            )?;
            let up_proceeds = quantity_weighted_payout(
                [
                    (clearance.up_qty_units_6dp, clearance.up_outcome_index, "up"),
                    (0, None, "down"),
                ],
                &[
                    payout.outcome_slot_0_numerator,
                    payout.outcome_slot_1_numerator,
                ],
                payout.denominator,
            )?;
            let down_proceeds = quantity_weighted_payout(
                [
                    (0, None, "up"),
                    (
                        clearance.down_qty_units_6dp,
                        clearance.down_outcome_index,
                        "down",
                    ),
                ],
                &[
                    payout.outcome_slot_0_numerator,
                    payout.outcome_slot_1_numerator,
                ],
                payout.denominator,
            )?;
            anyhow::ensure!(
                expected == collateral_receipt_units_6dp
                    && up_proceeds.checked_add(down_proceeds) == Some(expected),
                "redeem quantity-weighted payout does not equal finalized collateral receipt expected_units={} receipt_units={collateral_receipt_units_6dp}",
                expected
            );
            up_collateral_proceeds_units_6dp = Some(up_proceeds);
            down_collateral_proceeds_units_6dp = Some(down_proceeds);
            CtfTerminalOperation::Redeem
        }
        DirectEoaOperation::Approval | DirectEoaOperation::Split => {
            anyhow::bail!("terminal collateral receipt operation is not merge or redeem")
        }
    };
    let collateral_receipt_units_6dp = i128::try_from(collateral_receipt_units_6dp)
        .context("terminal collateral receipt exceeds signed accounting range")?;
    let terminal_fact = ConfirmedCtfTerminalFact {
        settlement_evidence: CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt {
            receipt_id: receipt.accounting_receipt_id(),
            transaction_id: tx_hash.to_string(),
        },
        market_id: clearance.market_id.clone(),
        condition_id: clearance.condition_id.clone(),
        cohort_id: clearance.cohort_id.clone(),
        up_token: clearance.up_token.clone(),
        down_token: clearance.down_token.clone(),
        up_terminal_qty_units_6dp: clearance.up_qty_units_6dp,
        down_terminal_qty_units_6dp: clearance.down_qty_units_6dp,
        up_outcome_index: clearance.up_outcome_index,
        down_outcome_index: clearance.down_outcome_index,
        up_collateral_proceeds_units_6dp,
        down_collateral_proceeds_units_6dp,
        collateral_proceeds_units_6dp: collateral_receipt_units_6dp,
        fee_units_6dp: 0,
        approved_direct_cost_units_6dp: 0,
    };
    let fact = match operation {
        CtfTerminalOperation::Merge => CanonicalCtfEvent::MergeConfirmed(terminal_fact),
        CtfTerminalOperation::Redeem => CanonicalCtfEvent::RedeemConfirmed(terminal_fact),
    };
    fact.validate()?;

    Ok(Some(terminal_record(
        record_run_id,
        clearance,
        fact,
        operation,
        tx_hash,
        "confirmed_terminal_receipt",
    )))
}

fn terminal_record(
    record_run_id: &str,
    clearance: &ConfirmedCtfTerminalClearance,
    fact: CanonicalCtfEvent,
    operation: CtfTerminalOperation,
    operation_id: &str,
    reconciliation_status: &str,
) -> EngineTradeJournalRecord {
    let mut record = EngineTradeJournalRecord::new(
        clearance.cohort_id.clone(),
        fact.event_kind(),
        clearance.confirmed_at_ms,
    );
    record.run_id = record_run_id.to_string();
    record.market_id = Some(clearance.market_id.clone());
    record.tokens = vec![clearance.up_token.clone(), clearance.down_token.clone()];
    record.paired_cohort_key = Some(clearance.cohort_id.clone());
    record.exit_ts_ms = Some(clearance.confirmed_at_ms);
    record.hold_state = Some("terminal_settlement".to_string());
    record.strategy_exit_reason = Some(
        match operation {
            CtfTerminalOperation::Merge => "merge",
            CtfTerminalOperation::Redeem => "redeem",
        }
        .to_string(),
    );
    record.settlement_tx_hash = Some(operation_id.to_string());
    record.reconciliation_status = Some(reconciliation_status.to_string());
    record.ctf_event = Some(fact);
    record
}

pub fn append_confirmed_ctf_terminal_record_once(record: EngineTradeJournalRecord) -> Result<()> {
    let expected_fact = record
        .ctf_event
        .as_ref()
        .context("terminal record has no accounting fact")?;
    anyhow::ensure!(
        matches!(
            expected_fact,
            CanonicalCtfEvent::MergeConfirmed(_) | CanonicalCtfEvent::RedeemConfirmed(_)
        ),
        "confirmed terminal append received a non-terminal accounting fact"
    );
    let receipt_id = expected_fact
        .receipt_id()
        .context("terminal event has no receipt identity")?;
    let inserted = super::append_once(
        format!("{}:{receipt_id}", expected_fact.event_kind()),
        record.clone(),
    )?;
    if inserted {
        return Ok(());
    }

    verify_durable_terminal_record(&record)
}

fn verify_durable_terminal_record(expected: &EngineTradeJournalRecord) -> Result<()> {
    let expected_fact = expected
        .ctf_event
        .as_ref()
        .context("terminal record has no accounting fact")?;
    let expected_receipt_id = expected_fact
        .receipt_id()
        .context("terminal event has no receipt identity")?;
    let run_id = expected.run_id.trim();
    anyhow::ensure!(
        !run_id.is_empty(),
        "terminal durable duplicate run identity is unavailable"
    );
    let matching = load_records_from_path(&trade_session_ledger_path_for_run(&run_id))?
        .into_iter()
        .filter(|record| {
            record.ctf_event.as_ref().is_some_and(|fact| {
                fact.receipt_id().as_deref() == Some(expected_receipt_id.as_str())
            })
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matching.len() == 1,
        "terminal append dedupe has no unique durable record receipt_id={}",
        expected_receipt_id
    );
    let existing = &matching[0];
    anyhow::ensure!(
        existing.run_id == run_id
            && existing.ts_ms == expected.ts_ms
            && existing.trade_id == expected.trade_id
            && existing.event_kind == expected.event_kind
            && existing.ctf_event.as_ref() == Some(expected_fact)
            && existing.market_id == expected.market_id
            && existing.paired_cohort_key == expected.paired_cohort_key
            && existing.tokens == expected.tokens
            && existing.settlement_tx_hash == expected.settlement_tx_hash,
        "terminal durable duplicate does not equal expected record receipt_id={}",
        expected_receipt_id
    );
    Ok(())
}

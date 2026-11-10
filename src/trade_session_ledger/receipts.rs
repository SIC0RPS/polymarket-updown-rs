use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

use alloy::primitives::Address;
use anyhow::{Context, Result};
use polymarket_client_sdk::types::B256;
use serde::{Deserialize, Serialize};

use crate::event_ledger::append_jsonl_record;

use super::{
    allocate_trade_ledger_seq, canonical_ledger_lock, ensure_run_trade_ledger_layout,
    run_start_ts_ms, trade_session_ledger_path_for_run, TradeLedgerRecord,
};

#[path = "receipts/direct_eoa_split_identity.rs"]
mod direct_eoa_split_identity;
#[path = "receipts/session_store.rs"]
mod session_store;
#[path = "receipts/settlement_collateral.rs"]
mod settlement_collateral;
#[path = "receipts/settlement_lineage.rs"]
mod settlement_lineage;

pub use direct_eoa_split_identity::DirectEoaSplitIdentity;
pub use session_store::append_session_receipt;
pub(super) use session_store::{load_session_rows_from_path, sync_session_receipt_path};
pub use settlement_lineage::{
    load_confirmed_settlement_collateral_receipt_for_run,
    validate_confirmed_settlement_collateral_receipt,
    validate_confirmed_settlement_receipt_lineage, ConfirmedSettlementReceiptLineage,
};

pub const CAPTURED_SESSION_LEDGER_FILE: &str = "canonical_session_ledger.jsonl";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirectEoaOperation {
    Approval,
    Split,
    Merge,
    Redeem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectEoaSubmissionReceipt {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub tx_hash: String,
    pub payer: String,
    pub operation: DirectEoaOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "super::exact_serde::optional_u128_string"
    )]
    pub amount_units_6dp: Option<u128>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_identity: Option<DirectEoaSplitIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_entry_journal: Option<crate::engine_trade_journal::CtfSplitEntryJournal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalizedConditionResolutionReceipt {
    pub run_id: String,
    #[serde(with = "super::exact_serde::u64_string")]
    pub ledger_seq: u64,
    #[serde(with = "super::exact_serde::u64_string")]
    pub observed_at_ms: u64,
    pub condition_id: String,
    pub resolution_tx_hash: String,
    #[serde(with = "super::exact_serde::u64_string")]
    pub resolution_log_index: u64,
    #[serde(with = "super::exact_serde::u64_string")]
    pub finalized_block_number: u64,
    pub finalized_block_hash: String,
    #[serde(with = "super::exact_serde::u64_string")]
    pub finalized_block_timestamp_ms: u64,
    #[serde(with = "super::exact_serde::u128_string")]
    pub outcome_slot_0_numerator: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub outcome_slot_1_numerator: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub denominator: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectEoaGasReceipt {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub tx_hash: String,
    pub payer: String,
    pub gas_used: u64,
    #[serde(with = "super::exact_serde::u128_string")]
    pub effective_gas_price_wei: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    pub status: bool,
    pub operation: DirectEoaOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MakerRebateReceipt {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub tx_hash: String,
    pub log_index: u64,
    pub token_contract: String,
    pub recipient: String,
    pub amount_units_6dp: u64,
    pub activity_amount_units_6dp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementCollateralReceipt {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub tx_hash: String,
    pub token_contract: String,
    pub recipient: String,
    pub condition_id: String,
    pub operation: DirectEoaOperation,
    pub outcome: SettlementCollateralOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementFinalizedLogIdentity {
    pub log_index: u64,
    pub emitter: String,
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementWrappedEventReceipt {
    pub log: SettlementFinalizedLogIdentity,
    pub caller: String,
    pub asset: String,
    pub to: String,
    #[serde(with = "super::exact_serde::u128_string")]
    pub amount_units_6dp: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SettlementPayoutRedemptionEventReceipt {
    pub log: SettlementFinalizedLogIdentity,
    pub redeemer: String,
    pub collateral_token: String,
    pub parent_collection_id: String,
    pub condition_id: String,
    #[serde(with = "super::exact_serde::u128_string_array_2")]
    pub index_sets: [u128; 2],
    #[serde(with = "super::exact_serde::u128_string")]
    pub payout_units_6dp: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SettlementCollateralOutcome {
    Confirmed {
        wrapped: SettlementWrappedEventReceipt,
        #[serde(skip_serializing_if = "Option::is_none")]
        payout_redemption: Option<SettlementPayoutRedemptionEventReceipt>,
    },
    ReconciliationRequired {
        reason: SettlementCollateralReconciliationReason,
        matching_wrapped_count: usize,
        matching_payout_redemption_count: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        expected_amount_units_6dp: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        observed_amount_units_6dp: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SettlementCollateralReconciliationReason {
    MissingExpectedAmount,
    MissingWrapped,
    MultipleWrapped,
    InvalidWrapped,
    AmountMismatch,
    AmountOverflow,
    MissingPayoutRedemption,
    MultiplePayoutRedemptions,
    InvalidPayoutRedemption,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptConflict {
    pub run_id: String,
    pub ledger_seq: u64,
    pub ts_ms: u64,
    pub receipt_identity: String,
    pub existing_ledger_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "receipt_type", rename_all = "snake_case")]
pub enum SessionReceipt {
    DirectEoaSubmission(DirectEoaSubmissionReceipt),
    DirectEoaGas(DirectEoaGasReceipt),
    MakerRebate(MakerRebateReceipt),
    SettlementCollateral(SettlementCollateralReceipt),
    ReceiptConflict(ReceiptConflict),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionReceiptAppendOutcome {
    Inserted,
    IdenticalExisting,
    RejectedOutOfWindow,
    Conflict,
}

#[derive(Debug, Clone)]
pub(super) enum SessionLedgerRow {
    Receipt(SessionReceipt),
    Trade(TradeLedgerRecord),
}

pub fn load_records_from_path(path: &Path) -> Result<Vec<TradeLedgerRecord>> {
    Ok(load_session_rows_from_path(path)?
        .into_iter()
        .filter_map(|row| match row {
            SessionLedgerRow::Trade(record) => Some(record),
            SessionLedgerRow::Receipt(_) => None,
        })
        .collect())
}

pub fn load_session_receipts_for_run(run_id: &str) -> Result<Vec<SessionReceipt>> {
    let path = trade_session_ledger_path_for_run(run_id);
    load_session_receipts_from_path(&path, run_id)
}

pub fn load_session_receipts_from_path(path: &Path, run_id: &str) -> Result<Vec<SessionReceipt>> {
    let mut receipts = Vec::new();
    for row in load_session_rows_from_path(path)? {
        let SessionLedgerRow::Receipt(receipt) = row else {
            continue;
        };
        receipt.validate()?;
        anyhow::ensure!(
            receipt.run_id() == run_id,
            "session receipt run mismatch in {}",
            path.display()
        );
        receipts.push(receipt);
    }
    if let Some(conflict) = receipts.iter().find_map(|receipt| match receipt {
        SessionReceipt::ReceiptConflict(conflict) => Some(conflict),
        SessionReceipt::DirectEoaSubmission(_)
        | SessionReceipt::DirectEoaGas(_)
        | SessionReceipt::MakerRebate(_)
        | SessionReceipt::SettlementCollateral(_) => None,
    }) {
        anyhow::bail!(
            "durable session receipt identity is conflicted run_id={run_id} receipt_identity={}",
            conflict.receipt_identity
        );
    }
    Ok(receipts)
}

pub fn load_finalized_condition_resolution_receipt_for_run(
    run_id: &str,
    condition_id: &str,
) -> Result<Option<FinalizedConditionResolutionReceipt>> {
    let condition_id = condition_id.trim();
    anyhow::ensure!(
        !condition_id.is_empty(),
        "condition resolution condition_id is empty"
    );
    let path = trade_session_ledger_path_for_run(run_id);
    let mut confirmed = None;
    for row in load_session_rows_from_path(&path)? {
        match row {
            SessionLedgerRow::Trade(record)
                if matches!(
                    record.ctf_event.as_ref(),
                    Some(super::CanonicalCtfEvent::ResolutionObserved(receipt))
                        if receipt.condition_id == condition_id
                ) =>
            {
                super::validate_ctf_event_record(&record)?;
                let Some(super::CanonicalCtfEvent::ResolutionObserved(receipt)) = record.ctf_event
                else {
                    unreachable!("resolution observation match checked")
                };
                anyhow::ensure!(
                    record.run_id == run_id && receipt.ledger_seq == record.ledger_seq,
                    "condition resolution record identity mismatch condition_id={condition_id}"
                );
                anyhow::ensure!(
                    confirmed
                        .as_ref()
                        .is_none_or(|existing| existing == &receipt),
                    "conflicting finalized condition resolution receipts condition_id={condition_id}"
                );
                confirmed = Some(receipt);
            }
            SessionLedgerRow::Receipt(_) | SessionLedgerRow::Trade(_) => {}
        }
    }
    Ok(confirmed)
}

impl FinalizedConditionResolutionReceipt {
    pub fn validate_exact(&self) -> Result<()> {
        anyhow::ensure!(
            !self.run_id.trim().is_empty(),
            "condition resolution run_id is empty"
        );
        anyhow::ensure!(
            self.observed_at_ms > 0,
            "condition resolution observation time is zero"
        );
        anyhow::ensure!(
            self.finalized_block_number > 0 && self.finalized_block_timestamp_ms > 0,
            "condition resolution finalized block identity is incomplete"
        );
        for (value, label) in [
            (&self.condition_id, "condition_id"),
            (&self.resolution_tx_hash, "resolution transaction hash"),
            (&self.finalized_block_hash, "finalized block hash"),
        ] {
            anyhow::ensure!(
                !value.trim().is_empty() && value == value.trim(),
                "condition resolution {label} is empty or noncanonical"
            );
            B256::from_str(value)
                .with_context(|| format!("condition resolution {label} is not bytes32"))?;
        }
        let summed = self
            .outcome_slot_0_numerator
            .checked_add(self.outcome_slot_1_numerator)
            .context("condition resolution payout numerator sum overflow")?;
        anyhow::ensure!(
            self.denominator > 0 && summed == self.denominator,
            "condition resolution binary payout vector does not equal its denominator"
        );
        Ok(())
    }

    pub fn numerator(&self, outcome_index: usize) -> Option<u128> {
        match outcome_index {
            0 => Some(self.outcome_slot_0_numerator),
            1 => Some(self.outcome_slot_1_numerator),
            _ => None,
        }
    }
}

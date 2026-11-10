use anyhow::{ensure, Context, Result};
use polymarket_client_sdk::types::B256;

use crate::strategy::{InventoryIntent, InventoryOperation};

use super::{ConfirmedCtfInventoryOperation, CTF_COHORT_ID_MAX_BYTES};

const CONFIRMED_CTF_COHORT_PREFIX: &str = "ctf_cohort:";
const CONFIRMED_DIRECT_BUY_COHORT_PREFIX: &str = "buy_cohort:";

pub fn confirmed_direct_buy_cohort_id(condition_id: B256, intent_id: u64) -> Result<String> {
    ensure!(
        condition_id != B256::ZERO,
        "BUY cohort condition identity is zero"
    );
    ensure!(
        intent_id > 0,
        "BUY cohort execution intent identity is zero"
    );
    let cohort_id = format!("{CONFIRMED_DIRECT_BUY_COHORT_PREFIX}{condition_id}:{intent_id}");
    ensure!(
        cohort_id.len() <= CTF_COHORT_ID_MAX_BYTES,
        "confirmed BUY cohort identity exceeds transport capacity"
    );
    Ok(cohort_id)
}

pub fn is_confirmed_ctf_entry_cohort_id(cohort_id: &str) -> bool {
    if is_confirmed_ctf_split_cohort_id(cohort_id) {
        return true;
    }
    let Some((condition, intent)) = cohort_id
        .strip_prefix(CONFIRMED_DIRECT_BUY_COHORT_PREFIX)
        .and_then(|identity| identity.rsplit_once(':'))
    else {
        return false;
    };
    let (Ok(condition), Ok(intent)) = (condition.parse::<B256>(), intent.parse::<u64>()) else {
        return false;
    };
    confirmed_direct_buy_cohort_id(condition, intent).is_ok_and(|canonical| canonical == cohort_id)
}

pub fn confirmed_ctf_split_cohort_id(transaction_id: &str) -> Result<String> {
    ensure!(
        !transaction_id.is_empty() && transaction_id == transaction_id.trim(),
        "confirmed CTF split transaction identity is missing or noncanonical"
    );
    let cohort_id = format!("{CONFIRMED_CTF_COHORT_PREFIX}{transaction_id}");
    ensure!(
        cohort_id.len() <= CTF_COHORT_ID_MAX_BYTES,
        "confirmed CTF split cohort identity exceeds transport capacity"
    );
    Ok(cohort_id)
}

pub fn is_confirmed_ctf_split_cohort_id(cohort_id: &str) -> bool {
    cohort_id.starts_with(CONFIRMED_CTF_COHORT_PREFIX)
        && cohort_id.len() > CONFIRMED_CTF_COHORT_PREFIX.len()
        && cohort_id.len() <= CTF_COHORT_ID_MAX_BYTES
        && cohort_id == cohort_id.trim()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum OperationKey {
    Entry(B256),
    Merge(B256),
}

impl OperationKey {
    pub(super) fn new(operation: InventoryOperation, condition_id: B256) -> Self {
        match operation {
            InventoryOperation::SplitCompleteSet => Self::Entry(condition_id),
            InventoryOperation::MergeCompleteSet => Self::Merge(condition_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OperationIdentity {
    pub(super) operation: InventoryOperation,
    pub(super) market_id: String,
    pub(super) gamma_market_id: Option<String>,
    pub(super) condition_id: B256,
    pub(super) up_token: String,
    pub(super) down_token: String,
    pub(super) cohort_id: Option<String>,
    pub(super) amount_units_6dp: u128,
}

impl OperationIdentity {
    pub(super) fn from_intent(intent: &InventoryIntent) -> Result<Self> {
        ensure!(
            intent.amount_units_6dp > 0,
            "CTF inventory amount must be positive"
        );
        ensure!(
            !intent.up_token.trim().is_empty() && !intent.down_token.trim().is_empty(),
            "CTF inventory token identity is missing"
        );
        ensure!(
            intent.up_token != intent.down_token,
            "CTF inventory outcome tokens must differ"
        );
        match intent.operation {
            InventoryOperation::SplitCompleteSet => ensure!(
                intent.cohort_id.is_none(),
                "CTF split intent cannot name a cohort before confirmation"
            ),
            InventoryOperation::MergeCompleteSet => {
                let cohort_id = intent
                    .cohort_id
                    .as_deref()
                    .context("CTF merge intent cohort identity is missing")?;
                ensure!(
                    !cohort_id.is_empty()
                        && cohort_id == cohort_id.trim()
                        && cohort_id.len() <= CTF_COHORT_ID_MAX_BYTES,
                    "CTF merge intent cohort identity is noncanonical"
                );
            }
        }
        Ok(Self {
            operation: intent.operation,
            market_id: intent.market_id.to_string(),
            gamma_market_id: intent.gamma_market_id.clone(),
            condition_id: intent.condition_id,
            up_token: intent.up_token.clone(),
            down_token: intent.down_token.clone(),
            cohort_id: intent.cohort_id.clone(),
            amount_units_6dp: intent.amount_units_6dp,
        })
    }

    pub(super) fn key(&self) -> OperationKey {
        OperationKey::new(self.operation, self.condition_id)
    }

    pub(super) fn confirmed(
        &self,
        tx_hash: String,
        pending_ts_ms: u64,
        receipt_confirmed_ts_ms: u64,
        materialized_ts_ms: u64,
    ) -> Result<ConfirmedCtfInventoryOperation> {
        let cohort_id = match self.operation {
            InventoryOperation::SplitCompleteSet => confirmed_ctf_split_cohort_id(&tx_hash)?,
            InventoryOperation::MergeCompleteSet => self
                .cohort_id
                .clone()
                .context("confirmed CTF merge cohort identity is missing")?,
        };
        Ok(ConfirmedCtfInventoryOperation {
            operation: self.operation,
            market_id: self.market_id.clone(),
            gamma_market_id: self.gamma_market_id.clone(),
            condition_id: self.condition_id,
            up_token: self.up_token.clone(),
            down_token: self.down_token.clone(),
            cohort_id,
            amount_units_6dp: self.amount_units_6dp,
            tx_hash,
            pending_ts_ms,
            receipt_confirmed_ts_ms,
            materialized_ts_ms,
        })
    }
}

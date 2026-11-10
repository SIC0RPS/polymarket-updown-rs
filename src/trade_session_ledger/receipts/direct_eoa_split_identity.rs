use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};
use serde::{Deserialize, Serialize};

use super::{DirectEoaOperation, DirectEoaSubmissionReceipt};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectEoaSplitIdentity {
    pub market_id: String,
    pub gamma_market_id: String,
    pub condition_id: String,
    pub underlying: String,
    pub up_token: String,
    pub down_token: String,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u128_string")]
    pub amount_units_6dp: u128,
}

impl DirectEoaSplitIdentity {
    pub fn validate(&self) -> Result<()> {
        let condition_id = B256::from_str(&self.condition_id)
            .context("direct EOA split condition identity is invalid")?;
        anyhow::ensure!(
            condition_id.to_string() == self.condition_id && self.market_id == self.condition_id,
            "direct EOA split market/condition identity is noncanonical or mismatched"
        );
        let gamma_market_id = self
            .gamma_market_id
            .parse::<u64>()
            .context("direct EOA split gamma market identity is invalid")?;
        anyhow::ensure!(
            gamma_market_id > 0 && gamma_market_id.to_string() == self.gamma_market_id,
            "direct EOA split gamma market identity is noncanonical"
        );
        let underlying = self.underlying.trim().to_ascii_uppercase();
        anyhow::ensure!(
            !underlying.is_empty() && underlying == self.underlying,
            "direct EOA split underlying is empty or noncanonical"
        );
        let up_token = U256::from_str(&self.up_token)
            .context("direct EOA split UP token identity is invalid")?;
        let down_token = U256::from_str(&self.down_token)
            .context("direct EOA split DOWN token identity is invalid")?;
        anyhow::ensure!(
            up_token != down_token
                && up_token.to_string() == self.up_token
                && down_token.to_string() == self.down_token,
            "direct EOA split token identities are equal or noncanonical"
        );
        anyhow::ensure!(self.amount_units_6dp > 0, "direct EOA split amount is zero");
        Ok(())
    }

    pub fn validate_entry_journal(
        &self,
        journal: &crate::engine_trade_journal::CtfSplitEntryJournal,
    ) -> Result<()> {
        self.validate()?;
        let intent = crate::strategy::InventoryIntent {
            operation: crate::strategy::InventoryOperation::SplitCompleteSet,
            cohort_id: None,
            market_id: crate::polymarket::identity::MarketId::from(self.market_id.clone()),
            gamma_market_id: Some(self.gamma_market_id.clone()),
            condition_id: B256::from_str(&self.condition_id)
                .context("direct EOA split condition identity is invalid")?,
            up_token: self.up_token.clone(),
            down_token: self.down_token.clone(),
            amount_units_6dp: self.amount_units_6dp,
        };
        journal.validate_split_intent(&intent)
    }
}

impl DirectEoaSubmissionReceipt {
    pub fn required_split_identity(&self) -> Result<&DirectEoaSplitIdentity> {
        anyhow::ensure!(
            self.operation == DirectEoaOperation::Split,
            "direct EOA submission is not a split tx_hash={}",
            self.tx_hash
        );
        self.split_identity.as_ref().with_context(|| {
            format!(
                "durable direct EOA split identity is unavailable tx_hash={}",
                self.tx_hash
            )
        })
    }

    pub fn required_split_entry_journal(
        &self,
    ) -> Result<&crate::engine_trade_journal::CtfSplitEntryJournal> {
        anyhow::ensure!(
            self.operation == DirectEoaOperation::Split,
            "direct EOA submission is not a split tx_hash={}",
            self.tx_hash
        );
        let journal = self.split_entry_journal.as_ref().with_context(|| {
            format!(
                "durable direct EOA split entry journal is unavailable tx_hash={}",
                self.tx_hash
            )
        })?;
        self.required_split_identity()?
            .validate_entry_journal(journal)?;
        Ok(journal)
    }
}

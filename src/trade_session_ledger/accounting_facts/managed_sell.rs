use std::str::FromStr;

use alloy::{
    hex,
    primitives::{Address, B256},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::polymarket::identity::MarketId;

use super::{nonempty, validate_common, CtfTradeStatus, ObservedCtfTradeFact};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedCtfManagedSellFact {
    pub receipt_id: String,
    pub fill_id: String,
    pub receipt_identity: ManagedSellReceiptIdentity,
    pub order_id: String,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
    )]
    pub intent_id: u64,
    pub market_id: MarketId,
    pub condition_id: String,
    pub cohort_id: String,
    pub token: String,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub qty_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_i128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_i128_string"
    )]
    pub gross_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub fee_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
    )]
    pub confirmed_at_ms: u64,
    pub close_reason: String,
    pub matched_trade: Option<ObservedCtfTradeFact>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(tag = "receipt_identity_kind", rename_all = "snake_case")]
pub enum ManagedSellReceiptIdentity {
    ExchangeV2 {
        venue_trade_id: String,
        order_hash: String,
        #[serde(
            serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
            deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
        )]
        chain_id: u64,
        exchange: String,
        transaction_hash: String,
        #[serde(
            serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
            deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
        )]
        block_number: u64,
        block_hash: String,
        #[serde(
            serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
            deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
        )]
        log_index: u64,
    },
    CapturedSimulation {
        tape_sha256: [u8; 32],
        symbol: String,
        event_kind: u8,
        #[serde(
            serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
            deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
        )]
        event_timestamp_ms: u64,
        event_start: u32,
        event_len: u32,
    },
}

impl ManagedSellReceiptIdentity {
    pub fn validate(&self, order_id: &str) -> Result<()> {
        nonempty(order_id, "sell order identity")?;
        match self {
            Self::ExchangeV2 {
                venue_trade_id,
                order_hash,
                chain_id,
                exchange,
                transaction_hash,
                block_number,
                block_hash,
                ..
            } => {
                nonempty(venue_trade_id, "venue trade identity")?;
                ensure!(*chain_id > 0, "sell receipt chain identity is zero");
                ensure!(*block_number > 0, "sell receipt block number is zero");
                let order_hash = B256::from_str(order_hash.trim())
                    .context("sell receipt order hash is invalid")?;
                let expected_order_hash = B256::from_str(order_id.trim())
                    .context("sell order identity is not bytes32")?;
                ensure!(
                    order_hash != B256::ZERO && order_hash == expected_order_hash,
                    "sell receipt order hash differs from order identity"
                );
                let exchange = Address::from_str(exchange.trim())
                    .context("sell receipt exchange identity is invalid")?;
                ensure!(
                    exchange == crate::polymarket::contracts_v2::CTF_EXCHANGE_V2
                        || exchange == crate::polymarket::contracts_v2::NEG_RISK_CTF_EXCHANGE_V2,
                    "sell receipt exchange is not an allowed V2 exchange"
                );
                let transaction_hash = B256::from_str(transaction_hash.trim())
                    .context("sell receipt transaction hash is invalid")?;
                ensure!(
                    transaction_hash != B256::ZERO,
                    "sell receipt transaction hash is zero"
                );
                let block_hash = B256::from_str(block_hash.trim())
                    .context("sell receipt block hash is invalid")?;
                ensure!(block_hash != B256::ZERO, "sell receipt block hash is zero");
            }
            Self::CapturedSimulation {
                tape_sha256,
                symbol,
                event_kind,
                event_timestamp_ms,
                event_len,
                ..
            } => {
                ensure!(
                    *tape_sha256 != [0; 32],
                    "captured sell receipt tape identity is zero"
                );
                ensure!(
                    matches!(symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP"),
                    "captured sell receipt symbol is not canonical"
                );
                ensure!(*event_kind > 0, "captured sell receipt event kind is zero");
                ensure!(
                    *event_timestamp_ms > 0,
                    "captured sell receipt event timestamp is zero"
                );
                ensure!(*event_len > 0, "captured sell receipt event length is zero");
            }
        }
        Ok(())
    }

    pub fn fill_id(&self, order_id: &str) -> Result<String> {
        self.validate(order_id)?;
        Ok(match self {
            Self::ExchangeV2 {
                chain_id,
                exchange,
                transaction_hash,
                block_number,
                block_hash,
                log_index,
                ..
            } => format!(
                "exchange_v2:{chain_id}:{exchange}:{block_number}:{block_hash}:{transaction_hash}:{log_index}"
            ),
            Self::CapturedSimulation {
                tape_sha256,
                symbol,
                event_kind,
                event_timestamp_ms,
                event_start,
                event_len,
            } => format!(
                "captured:{}:{symbol}:{event_kind}:{event_timestamp_ms}:{event_start}:{event_len}:{order_id}",
                hex::encode(tape_sha256)
            ),
        })
    }
}

impl ConfirmedCtfManagedSellFact {
    pub(super) fn validate(&self) -> Result<()> {
        validate_common(&self.receipt_id, &self.condition_id, &self.cohort_id)?;
        nonempty(self.market_id.as_str(), "managed-sell market identity")?;
        nonempty(&self.fill_id, "sell fill identity")?;
        self.receipt_identity.validate(&self.order_id)?;
        ensure!(
            self.fill_id == self.receipt_identity.fill_id(&self.order_id)?,
            "managed-sell fill identity does not match receipt identity"
        );
        ensure!(
            self.market_id.as_str() == self.condition_id,
            "managed-sell market identity does not equal condition identity"
        );
        ensure!(
            self.receipt_id == format!("ctf_managed_sell:{}", self.fill_id),
            "managed-sell receipt identity does not match fill identity"
        );
        nonempty(&self.order_id, "sell order identity")?;
        ensure!(self.intent_id > 0, "sell intent identity is zero");
        nonempty(&self.token, "sold token identity")?;
        nonempty(&self.close_reason, "managed-sell close reason")?;
        ensure!(self.qty_units_6dp > 0, "confirmed sell quantity is zero");
        ensure!(
            self.confirmed_at_ms > 0,
            "confirmed sell confirmation time is zero"
        );
        ensure!(
            self.gross_proceeds_units_6dp >= 0,
            "confirmed sell gross proceeds are negative"
        );
        ensure!(
            self.fee_units_6dp <= self.gross_proceeds_units_6dp as u128,
            "confirmed sell fee exceeds gross proceeds"
        );
        match (&self.receipt_identity, self.matched_trade.as_ref()) {
            (
                ManagedSellReceiptIdentity::ExchangeV2 { venue_trade_id, .. },
                Some(matched_trade),
            ) => {
                matched_trade.validate()?;
                ensure!(
                    matched_trade.status == CtfTradeStatus::Confirmed,
                    "confirmed sell matched lineage is not CONFIRMED"
                );
                ensure!(
                    matched_trade.trade_id == venue_trade_id.as_str()
                        && matched_trade.market_id == self.market_id
                        && matched_trade.cohort_id == self.cohort_id
                        && matched_trade.asset_id == self.token
                        && matched_trade.order_id == self.order_id
                        && matched_trade.owned_size_units_6dp == self.qty_units_6dp,
                    "confirmed sell matched lineage differs from receipt identity or quantity"
                );
            }
            (ManagedSellReceiptIdentity::ExchangeV2 { .. }, None) => {
                anyhow::bail!("live confirmed sell is missing matched-trade lineage")
            }
            (ManagedSellReceiptIdentity::CapturedSimulation { .. }, Some(_)) => {
                anyhow::bail!("captured simulation sell carries live matched-trade lineage")
            }
            (ManagedSellReceiptIdentity::CapturedSimulation { .. }, None) => {}
        }
        Ok(())
    }
}

use alloy::hex;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::polymarket::identity::MarketId;

use super::FinalizedConditionResolutionReceipt;

#[path = "accounting_facts/managed_sell.rs"]
mod managed_sell;
pub use managed_sell::{ConfirmedCtfManagedSellFact, ManagedSellReceiptIdentity};
#[path = "accounting_facts/order_submission.rs"]
mod order_submission;
pub use order_submission::ConfirmedOrderSubmissionFact;
#[path = "accounting_facts/terminal.rs"]
mod terminal;
pub use terminal::{ConfirmedCtfTerminalFact, CtfTerminalSettlementEvidence};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CtfTerminalOperation {
    Merge,
    Redeem,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CtfTradeStatus {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

impl CtfTradeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "MATCHED",
            Self::Mined => "MINED",
            Self::Confirmed => "CONFIRMED",
            Self::Retrying => "RETRYING",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "ctf_event_kind", rename_all = "snake_case")]
pub enum CanonicalCtfEvent {
    SplitConfirmed(ConfirmedCtfSplitFact),
    BuyConfirmed(ConfirmedCtfBuyFact),
    OrderAccepted(ConfirmedOrderSubmissionFact),
    TradeMatched(ObservedCtfTradeFact),
    TradeConfirmed(ConfirmedCtfManagedSellFact),
    TradeFailed(ObservedCtfTradeFact),
    MergeConfirmed(ConfirmedCtfTerminalFact),
    RedeemConfirmed(ConfirmedCtfTerminalFact),
    FeeApplied(ConfirmedCtfChargeFact),
    CostApplied(ConfirmedCtfChargeFact),
    ResolutionObserved(FinalizedConditionResolutionReceipt),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ObservedCtfTradeFact {
    pub event_id: String,
    pub trade_id: String,
    pub market_id: MarketId,
    pub cohort_id: String,
    pub trade_asset_id: String,
    pub asset_id: String,
    pub order_id: String,
    pub taker_order_id: Option<String>,
    pub maker_order_ids: Vec<String>,
    pub status: CtfTradeStatus,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub price_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub size_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub owned_price_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub owned_size_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u64_string",
        deserialize_with = "super::exact_serde::deserialize_u64_string"
    )]
    pub matched_at_ms: u64,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u64_string",
        deserialize_with = "super::exact_serde::deserialize_u64_string"
    )]
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedCtfChargeFact {
    pub receipt_id: String,
    pub cohort_id: String,
    pub source_event_identity: String,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub amount_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u64_string",
        deserialize_with = "super::exact_serde::deserialize_u64_string"
    )]
    pub confirmed_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedCtfSplitFact {
    pub receipt_id: String,
    pub transaction_id: String,
    pub market_id: MarketId,
    pub condition_id: String,
    pub cohort_id: String,
    pub underlying: String,
    pub market_name: Option<String>,
    pub up_token: String,
    pub down_token: String,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub up_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub down_qty_units_6dp: u128,
    #[serde(
        default,
        serialize_with = "super::exact_serde::serialize_optional_u128_string",
        deserialize_with = "super::exact_serde::deserialize_optional_u128_string"
    )]
    pub up_cost_basis_weight_units_8dp: Option<u128>,
    #[serde(
        default,
        serialize_with = "super::exact_serde::serialize_optional_u128_string",
        deserialize_with = "super::exact_serde::deserialize_optional_u128_string"
    )]
    pub down_cost_basis_weight_units_8dp: Option<u128>,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub collateral_debit_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub fee_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_u128_string",
        deserialize_with = "super::exact_serde::deserialize_u128_string"
    )]
    pub approved_direct_cost_units_6dp: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedCtfBuyFact {
    pub receipt_id: String,
    pub fill_id: String,
    pub receipt_identity: ManagedSellReceiptIdentity,
    pub order_id: String,
    pub authorization: crate::types::DirectBuyAuthorization,
    pub market_id: MarketId,
    pub condition_id: String,
    pub cohort_id: String,
    pub token: String,
    #[serde(with = "super::exact_serde::u128_string")]
    pub qty_units_6dp: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub principal_units_6dp: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub fee_units_6dp: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub approved_direct_cost_units_6dp: u128,
    #[serde(with = "super::exact_serde::u128_string")]
    pub collateral_refund_units_6dp: u128,
    #[serde(
        serialize_with = "super::exact_serde::serialize_i128_string",
        deserialize_with = "super::exact_serde::deserialize_i128_string"
    )]
    pub collateral_cash_delta_units_6dp: i128,
    #[serde(with = "super::exact_serde::u64_string")]
    pub confirmed_at_ms: u64,
    pub wallet: Option<String>,
    pub matched_trade: Option<ObservedCtfTradeFact>,
}

impl ConfirmedCtfBuyFact {
    pub fn validate(&self) -> Result<()> {
        use alloy::primitives::Address;
        self.authorization.validate()?;
        validate_common(&self.receipt_id, &self.condition_id, &self.cohort_id)?;
        self.receipt_identity.validate(&self.order_id)?;
        ensure!(
            self.fill_id == self.receipt_identity.fill_id(&self.order_id)?
                && self.receipt_id == format!("ctf_buy:{}", self.fill_id),
            "BUY receipt identity differs from its canonical fill"
        );
        ensure!(
            self.condition_id == self.authorization.condition_id.to_string()
                && self.market_id.as_str() == self.condition_id
                && self.cohort_id
                    == crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                        self.authorization.condition_id,
                        self.authorization.intent_id,
                    )?
                && self.token == self.authorization.selected_token().to_string(),
            "BUY receipt differs from its immutable authorization"
        );
        ensure!(
            self.qty_units_6dp > 0
                && self.qty_units_6dp <= self.authorization.target_qty_units_6dp
                && self.principal_units_6dp > 0,
            "BUY receipt quantity or principal is invalid"
        );
        ensure!(self.confirmed_at_ms > 0, "BUY confirmation time is zero");
        let debit = self
            .principal_units_6dp
            .checked_add(self.fee_units_6dp)
            .and_then(|value| value.checked_add(self.approved_direct_cost_units_6dp))
            .and_then(|value| i128::try_from(value).ok())
            .ok_or_else(|| anyhow::anyhow!("BUY cash debit exceeds exact signed range"))?;
        let refund = i128::try_from(self.collateral_refund_units_6dp)?;
        ensure!(
            refund.checked_sub(debit) == Some(self.collateral_cash_delta_units_6dp),
            "BUY exact cash does not reconcile with principal, charges and refund"
        );
        match (&self.receipt_identity, self.matched_trade.as_ref()) {
            (ManagedSellReceiptIdentity::ExchangeV2 { venue_trade_id, .. }, Some(trade)) => {
                trade.validate()?;
                ensure!(
                    trade.status == CtfTradeStatus::Confirmed
                        && trade.trade_id == *venue_trade_id
                        && trade.market_id == self.market_id
                        && trade.cohort_id == self.cohort_id
                        && trade.asset_id == self.token
                        && trade.order_id == self.order_id
                        && trade.owned_size_units_6dp == self.qty_units_6dp,
                    "BUY matched lineage differs from confirmed receipt"
                );
                let wallet = self
                    .wallet
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("BUY wallet proof is absent"))?
                    .parse::<Address>()?;
                ensure!(wallet != Address::ZERO, "BUY wallet proof is zero");
            }
            (ManagedSellReceiptIdentity::ExchangeV2 { .. }, None) => {
                anyhow::bail!("live confirmed BUY is missing matched-trade lineage");
            }
            (ManagedSellReceiptIdentity::CapturedSimulation { symbol, .. }, None) => {
                ensure!(
                    self.wallet.is_none() && symbol == self.authorization.symbol.as_str(),
                    "simulated BUY carries foreign wallet or symbol identity"
                );
            }
            (ManagedSellReceiptIdentity::CapturedSimulation { .. }, Some(_)) => {
                anyhow::bail!("simulated BUY carries live matched-trade lineage");
            }
        }
        Ok(())
    }
}

impl CanonicalCtfEvent {
    pub fn from_observed_trade(fact: ObservedCtfTradeFact) -> Result<Self> {
        fact.validate()?;
        Ok(if fact.status == CtfTradeStatus::Failed {
            Self::TradeFailed(fact)
        } else {
            Self::TradeMatched(fact)
        })
    }

    pub const fn event_kind(&self) -> &'static str {
        match self {
            Self::SplitConfirmed(_) => "split_confirmed",
            Self::BuyConfirmed(_) => "buy_confirmed",
            Self::OrderAccepted(_) => "order_accepted",
            Self::TradeMatched(_) => "trade_matched",
            Self::TradeConfirmed(_) => "trade_confirmed",
            Self::TradeFailed(_) => "trade_failed",
            Self::MergeConfirmed(_) => "merge_confirmed",
            Self::RedeemConfirmed(_) => "redeem_confirmed",
            Self::FeeApplied(_) => "fee_applied",
            Self::CostApplied(_) => "cost_applied",
            Self::ResolutionObserved(_) => "resolution_observed",
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::SplitConfirmed(fact) => fact.validate(),
            Self::BuyConfirmed(fact) => fact.validate(),
            Self::OrderAccepted(fact) => fact.validate(),
            Self::TradeMatched(fact) => {
                fact.validate()?;
                ensure!(
                    fact.status != CtfTradeStatus::Failed,
                    "matched trade event carries FAILED status"
                );
                Ok(())
            }
            Self::TradeFailed(fact) => {
                fact.validate()?;
                ensure!(
                    fact.status == CtfTradeStatus::Failed,
                    "failed trade event does not carry FAILED status"
                );
                Ok(())
            }
            Self::TradeConfirmed(fact) => fact.validate(),
            Self::MergeConfirmed(fact) => fact.validate_for(CtfTerminalOperation::Merge),
            Self::RedeemConfirmed(fact) => fact.validate_for(CtfTerminalOperation::Redeem),
            Self::FeeApplied(fact) | Self::CostApplied(fact) => fact.validate(),
            Self::ResolutionObserved(fact) => fact.validate_exact(),
        }
    }

    pub fn receipt_id(&self) -> Option<String> {
        match self {
            Self::SplitConfirmed(fact) => Some(fact.receipt_id.clone()),
            Self::BuyConfirmed(fact) => Some(fact.receipt_id.clone()),
            Self::TradeConfirmed(fact) => Some(fact.receipt_id.clone()),
            Self::MergeConfirmed(fact) | Self::RedeemConfirmed(fact) => Some(fact.settlement_id()),
            Self::FeeApplied(fact) | Self::CostApplied(fact) => Some(fact.receipt_id.clone()),
            Self::ResolutionObserved(fact) => {
                Some(format!("ctf_condition_resolution:{}", fact.condition_id))
            }
            Self::OrderAccepted(_) | Self::TradeMatched(_) | Self::TradeFailed(_) => None,
        }
    }

    pub fn durable_identity(&self) -> Option<String> {
        match self {
            Self::TradeMatched(fact) | Self::TradeFailed(fact) => Some(fact.event_id.clone()),
            _ => self.receipt_id(),
        }
    }

    pub fn cohort_id(&self) -> Option<&str> {
        match self {
            Self::SplitConfirmed(fact) => Some(&fact.cohort_id),
            Self::BuyConfirmed(fact) => Some(&fact.cohort_id),
            Self::OrderAccepted(fact) => Some(&fact.cohort_id),
            Self::TradeMatched(fact) | Self::TradeFailed(fact) => Some(&fact.cohort_id),
            Self::TradeConfirmed(fact) => Some(&fact.cohort_id),
            Self::MergeConfirmed(fact) | Self::RedeemConfirmed(fact) => Some(&fact.cohort_id),
            Self::FeeApplied(fact) | Self::CostApplied(fact) => Some(&fact.cohort_id),
            Self::ResolutionObserved(_) => None,
        }
    }
}

impl ObservedCtfTradeFact {
    pub(crate) fn from_vendor(
        trade_id: String,
        market_id: MarketId,
        cohort_id: String,
        trade_asset_id: String,
        asset_id: String,
        order_id: String,
        taker_order_id: Option<String>,
        maker_order_ids: Vec<String>,
        status: CtfTradeStatus,
        price_units_6dp: u128,
        size_units_6dp: u128,
        owned_price_units_6dp: u128,
        owned_size_units_6dp: u128,
        matched_at_ms: u64,
        observed_at_ms: u64,
    ) -> Result<Self> {
        let fact = Self {
            event_id: String::new(),
            trade_id,
            market_id,
            cohort_id,
            trade_asset_id,
            asset_id,
            order_id,
            taker_order_id,
            maker_order_ids,
            status,
            price_units_6dp,
            size_units_6dp,
            owned_price_units_6dp,
            owned_size_units_6dp,
            matched_at_ms,
            observed_at_ms,
        };
        let mut fact = fact;
        fact.event_id = fact.expected_event_id();
        fact.validate()?;
        Ok(fact)
    }

    pub const fn status_str(&self) -> &'static str {
        self.status.as_str()
    }

    fn validate(&self) -> Result<()> {
        nonempty(&self.trade_id, "trade identity")?;
        nonempty(&self.event_id, "trade event identity")?;
        nonempty(self.market_id.as_str(), "trade market identity")?;
        nonempty(&self.cohort_id, "trade cohort identity")?;
        nonempty(&self.trade_asset_id, "trade top-level asset identity")?;
        nonempty(&self.asset_id, "trade asset identity")?;
        nonempty(&self.order_id, "trade order identity")?;
        if let Some(taker_order_id) = self.taker_order_id.as_deref() {
            nonempty(taker_order_id, "trade taker order identity")?;
        }
        let mut order_ids = std::collections::BTreeSet::new();
        if let Some(taker_order_id) = self.taker_order_id.as_ref() {
            ensure!(
                order_ids.insert(taker_order_id.as_str()),
                "duplicate trade order identity"
            );
        }
        for maker_order_id in &self.maker_order_ids {
            nonempty(maker_order_id, "trade maker order identity")?;
            ensure!(
                order_ids.insert(maker_order_id.as_str()),
                "duplicate trade order identity"
            );
        }
        ensure!(
            self.maker_order_ids
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
            "trade maker order identities are not canonical sorted unique"
        );
        ensure!(
            order_ids.contains(self.order_id.as_str()),
            "owned trade order is absent from vendor order graph"
        );
        ensure!(
            self.price_units_6dp > 0 && self.price_units_6dp <= 1_000_000,
            "trade price is zero or exceeds one pUSD"
        );
        ensure!(self.size_units_6dp > 0, "trade size is zero");
        ensure!(
            self.owned_price_units_6dp > 0 && self.owned_price_units_6dp <= 1_000_000,
            "owned trade price is zero or exceeds one pUSD"
        );
        ensure!(self.owned_size_units_6dp > 0, "owned trade size is zero");
        ensure!(self.matched_at_ms > 0, "trade match time is zero");
        ensure!(
            self.observed_at_ms >= self.matched_at_ms,
            "trade observation precedes match time"
        );
        ensure!(
            self.event_id == self.expected_event_id(),
            "trade event identity differs from immutable vendor fact"
        );
        Ok(())
    }

    fn expected_event_id(&self) -> String {
        let mut digest = Sha256::new();
        update_digest_str(&mut digest, "polymarket_clob_trade_update_v1");
        update_digest_str(&mut digest, &self.trade_id);
        update_digest_str(&mut digest, &self.order_id);
        update_digest_str(&mut digest, self.status.as_str());
        format!("clob_trade_update:{}", hex::encode(digest.finalize()))
    }
}

fn update_digest_str(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

impl ConfirmedCtfChargeFact {
    fn validate(&self) -> Result<()> {
        nonempty(&self.receipt_id, "charge receipt identity")?;
        nonempty(&self.cohort_id, "charge cohort identity")?;
        nonempty(&self.source_event_identity, "charge source event identity")?;
        ensure!(self.amount_units_6dp > 0, "charge amount is zero");
        ensure!(self.confirmed_at_ms > 0, "charge confirmation time is zero");
        Ok(())
    }
}

impl ConfirmedCtfSplitFact {
    fn validate(&self) -> Result<()> {
        validate_common(&self.receipt_id, &self.condition_id, &self.cohort_id)?;
        nonempty(self.market_id.as_str(), "split market identity")?;
        nonempty(&self.transaction_id, "split transaction identity")?;
        ensure!(
            self.receipt_id == format!("ctf_split:{}", self.transaction_id),
            "split receipt identity does not match transaction identity"
        );
        ensure!(
            matches!(self.underlying.as_str(), "BTC" | "ETH" | "SOL" | "XRP"),
            "split underlying is not a canonical symbol"
        );
        validate_tokens(&self.up_token, &self.down_token)?;
        ensure!(
            self.market_id.as_str() == self.condition_id,
            "split market identity does not equal condition identity"
        );
        ensure!(
            self.cohort_id
                == crate::ctf_inventory::confirmed_ctf_split_cohort_id(&self.transaction_id)?,
            "split cohort identity does not equal confirmed split transaction identity"
        );
        ensure!(
            self.up_qty_units_6dp > 0 && self.up_qty_units_6dp == self.down_qty_units_6dp,
            "confirmed split outcome quantities differ or are zero"
        );
        ensure!(
            self.collateral_debit_units_6dp == self.up_qty_units_6dp,
            "confirmed split principal does not equal created complete-set quantity"
        );
        ensure!(
            self.up_cost_basis_weight_units_8dp
                .zip(self.down_cost_basis_weight_units_8dp)
                .and_then(|(up, down)| up.checked_add(down))
                .is_some_and(|total| total > 0),
            "confirmed split cost-basis weights are zero or overflow"
        );
        Ok(())
    }
}

fn validate_common(receipt_id: &str, condition_id: &str, cohort_id: &str) -> Result<()> {
    nonempty(receipt_id, "accounting receipt identity")?;
    nonempty(condition_id, "condition identity")?;
    nonempty(cohort_id, "cohort identity")?;
    Ok(())
}

fn validate_tokens(up_token: &str, down_token: &str) -> Result<()> {
    nonempty(up_token, "UP token identity")?;
    nonempty(down_token, "DOWN token identity")?;
    ensure!(
        up_token != down_token,
        "duplicate CTF outcome token identity"
    );
    Ok(())
}

fn nonempty<'a>(value: &'a str, label: &str) -> Result<&'a str> {
    let value = value.trim();
    ensure!(!value.is_empty(), "missing {label}");
    Ok(value)
}

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCtfChargeFact, ConfirmedCtfManagedSellFact, ConfirmedCtfSplitFact,
    ConfirmedCtfTerminalFact, ConfirmedOrderSubmissionFact, CtfTradeStatus,
    FinalizedConditionResolutionReceipt, ManagedSellReceiptIdentity, ObservedCtfTradeFact,
};

use super::super::{
    deserialize_i128_string, deserialize_u128_string, deserialize_u64_string,
    serialize_i128_string, serialize_u128_string, serialize_u64_string,
};

#[derive(Deserialize, Serialize)]
#[serde(remote = "ObservedCtfTradeFact")]
struct ObservedCtfTradeFactExact {
    event_id: String,
    trade_id: String,
    market_id: MarketId,
    cohort_id: String,
    trade_asset_id: String,
    asset_id: String,
    order_id: String,
    taker_order_id: Option<String>,
    maker_order_ids: Vec<String>,
    status: CtfTradeStatus,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    price_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    size_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    owned_price_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    owned_size_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    matched_at_ms: u64,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    observed_at_ms: u64,
}

struct ExactObservedCtfTradeFactRef<'a>(&'a ObservedCtfTradeFact);

impl Serialize for ExactObservedCtfTradeFactRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ObservedCtfTradeFactExact::serialize(self.0, serializer)
    }
}

struct ExactObservedCtfTradeFact(ObservedCtfTradeFact);

impl<'de> Deserialize<'de> for ExactObservedCtfTradeFact {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ObservedCtfTradeFactExact::deserialize(deserializer).map(Self)
    }
}

mod optional_observed_ctf_trade_fact_exact {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{ExactObservedCtfTradeFact, ExactObservedCtfTradeFactRef, ObservedCtfTradeFact};

    pub(super) fn serialize<S>(
        value: &Option<ObservedCtfTradeFact>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .as_ref()
            .map(ExactObservedCtfTradeFactRef)
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<ObservedCtfTradeFact>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Option::<ExactObservedCtfTradeFact>::deserialize(deserializer)?.map(|value| value.0))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "ConfirmedCtfSplitFact")]
struct ConfirmedCtfSplitFactExact {
    receipt_id: String,
    transaction_id: String,
    market_id: MarketId,
    condition_id: String,
    cohort_id: String,
    underlying: String,
    market_name: Option<String>,
    up_token: String,
    down_token: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    up_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    down_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    collateral_debit_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    approved_direct_cost_units_6dp: u128,
}

#[derive(Deserialize, Serialize)]
#[serde(
    remote = "ManagedSellReceiptIdentity",
    tag = "receipt_identity_kind",
    rename_all = "snake_case"
)]
enum ManagedSellReceiptIdentityExact {
    ExchangeV2 {
        venue_trade_id: String,
        order_hash: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        chain_id: u64,
        exchange: String,
        transaction_hash: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        block_number: u64,
        block_hash: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        log_index: u64,
    },
    CapturedSimulation {
        tape_sha256: [u8; 32],
        symbol: String,
        event_kind: u8,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        event_timestamp_ms: u64,
        event_start: u32,
        event_len: u32,
    },
}

pub(super) fn serialize_managed_sell_receipt_identity<S>(
    value: &ManagedSellReceiptIdentity,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ManagedSellReceiptIdentityExact::serialize(value, serializer)
}

pub(super) fn deserialize_managed_sell_receipt_identity<'de, D>(
    deserializer: D,
) -> Result<ManagedSellReceiptIdentity, D::Error>
where
    D: Deserializer<'de>,
{
    ManagedSellReceiptIdentityExact::deserialize(deserializer)
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "ConfirmedOrderSubmissionFact")]
struct ConfirmedOrderSubmissionFactExact {
    order_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    intent_id: u64,
    market_id: MarketId,
    cohort_id: String,
    underlying: String,
    asset_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    accepted_at_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "ConfirmedCtfManagedSellFact")]
struct ConfirmedCtfManagedSellFactExact {
    receipt_id: String,
    fill_id: String,
    #[serde(
        serialize_with = "serialize_managed_sell_receipt_identity",
        deserialize_with = "deserialize_managed_sell_receipt_identity"
    )]
    receipt_identity: ManagedSellReceiptIdentity,
    order_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    intent_id: u64,
    market_id: MarketId,
    condition_id: String,
    cohort_id: String,
    token: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    gross_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    confirmed_at_ms: u64,
    close_reason: String,
    #[serde(with = "optional_observed_ctf_trade_fact_exact")]
    matched_trade: Option<ObservedCtfTradeFact>,
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "ConfirmedCtfTerminalFact")]
struct ConfirmedCtfTerminalFactExact {
    settlement_evidence: crate::trade_session_ledger::CtfTerminalSettlementEvidence,
    market_id: MarketId,
    condition_id: String,
    cohort_id: String,
    up_token: String,
    down_token: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    up_terminal_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    down_terminal_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    collateral_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    approved_direct_cost_units_6dp: u128,
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "ConfirmedCtfChargeFact")]
struct ConfirmedCtfChargeFactExact {
    receipt_id: String,
    cohort_id: String,
    source_event_identity: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    amount_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    confirmed_at_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "FinalizedConditionResolutionReceipt")]
struct FinalizedConditionResolutionReceiptExact {
    run_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    ledger_seq: u64,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    observed_at_ms: u64,
    condition_id: String,
    resolution_tx_hash: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    resolution_log_index: u64,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    finalized_block_number: u64,
    finalized_block_hash: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    finalized_block_timestamp_ms: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    outcome_slot_0_numerator: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    outcome_slot_1_numerator: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    denominator: u128,
}

#[derive(Deserialize, Serialize)]
#[serde(
    remote = "CanonicalCtfEvent",
    tag = "ctf_event_kind",
    rename_all = "snake_case"
)]
enum CanonicalCtfEventExact {
    SplitConfirmed(#[serde(with = "ConfirmedCtfSplitFactExact")] ConfirmedCtfSplitFact),
    OrderAccepted(
        #[serde(with = "ConfirmedOrderSubmissionFactExact")] ConfirmedOrderSubmissionFact,
    ),
    TradeMatched(#[serde(with = "ObservedCtfTradeFactExact")] ObservedCtfTradeFact),
    TradeConfirmed(#[serde(with = "ConfirmedCtfManagedSellFactExact")] ConfirmedCtfManagedSellFact),
    TradeFailed(#[serde(with = "ObservedCtfTradeFactExact")] ObservedCtfTradeFact),
    MergeConfirmed(#[serde(with = "ConfirmedCtfTerminalFactExact")] ConfirmedCtfTerminalFact),
    RedeemConfirmed(#[serde(with = "ConfirmedCtfTerminalFactExact")] ConfirmedCtfTerminalFact),
    FeeApplied(#[serde(with = "ConfirmedCtfChargeFactExact")] ConfirmedCtfChargeFact),
    CostApplied(#[serde(with = "ConfirmedCtfChargeFactExact")] ConfirmedCtfChargeFact),
    ResolutionObserved(
        #[serde(with = "FinalizedConditionResolutionReceiptExact")]
        FinalizedConditionResolutionReceipt,
    ),
}

pub(super) fn serialize<S>(value: &CanonicalCtfEvent, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    CanonicalCtfEventExact::serialize(value, serializer)
}

pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<CanonicalCtfEvent, D::Error>
where
    D: Deserializer<'de>,
{
    CanonicalCtfEventExact::deserialize(deserializer)
}

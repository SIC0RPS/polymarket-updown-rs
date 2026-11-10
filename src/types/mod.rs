use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::time::Instant;

use polymarket_client_sdk::types::{B256, U256};

use crate::polymarket::identity::MarketId;
use crate::strategy_runtime::symbol_process::SymbolId;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectBuyAuthorization {
    #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
    pub intent_id: u64,
    pub condition_id: B256,
    pub up_token: U256,
    pub down_token: U256,
    pub symbol: SymbolId,
    pub selected_up: bool,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
    pub fee_rate_bps_bits: u64,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
    pub prediction_at_ms: u64,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
    pub authorized_at_ms: u64,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u128_string")]
    pub target_qty_units_6dp: u128,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
    pub market_end_ms: u64,
}

impl DirectBuyAuthorization {
    pub fn fee_rate_bps(&self) -> f64 {
        f64::from_bits(self.fee_rate_bps_bits)
    }

    pub const fn selected_token(&self) -> U256 {
        if self.selected_up {
            self.up_token
        } else {
            self.down_token
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.intent_id > 0, "BUY execution intent identity is zero");
        anyhow::ensure!(
            self.condition_id != B256::ZERO,
            "BUY condition identity is zero"
        );
        anyhow::ensure!(
            self.up_token != U256::ZERO
                && self.down_token != U256::ZERO
                && self.up_token != self.down_token,
            "BUY outcome token identities are invalid"
        );
        anyhow::ensure!(self.target_qty_units_6dp > 0, "BUY target quantity is zero");
        anyhow::ensure!(
            self.fee_rate_bps().is_finite() && (0.0..=10_000.0).contains(&self.fee_rate_bps()),
            "BUY fee rate is outside its finite basis-point range"
        );
        anyhow::ensure!(
            self.prediction_at_ms > 0
                && self.prediction_at_ms <= self.authorized_at_ms
                && self.authorized_at_ms < self.market_end_ms,
            "BUY prediction or authorization time is invalid"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Gtd {
        expiration_ms: u64,
    },
    Fok,
    Fak,
}

impl TimeInForce {
    pub const fn expiration_ms(&self) -> Option<u64> {
        match self {
            Self::Gtd { expiration_ms } => Some(*expiration_ms),
            Self::Gtc | Self::Fok | Self::Fak => None,
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Gtc => "gtc",
            Self::Gtd { .. } => "gtd",
            Self::Fok => "fok",
            Self::Fak => "fak",
        }
    }

    pub const fn is_immediate(&self) -> bool {
        matches!(self, Self::Fok | Self::Fak)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OrderCloseReason {
    PairedFirstManagedSell,
    PairedPostProceedsSell,
    PredictivePostProceedsSell,
}

impl OrderCloseReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PairedFirstManagedSell => "paired_first_managed_sell",
            Self::PairedPostProceedsSell => "paired_post_proceeds_sell",
            Self::PredictivePostProceedsSell => "predictive_post_proceeds_sell",
        }
    }

    pub fn from_order_str(value: &str) -> Option<Self> {
        match value.trim() {
            "paired_first_managed_sell" => Some(Self::PairedFirstManagedSell),
            "paired_post_proceeds_sell" => Some(Self::PairedPostProceedsSell),
            "predictive_post_proceeds_sell" => Some(Self::PredictivePostProceedsSell),
            _ => None,
        }
    }
}

impl fmt::Display for OrderCloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Deref for OrderCloseReason {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl FromStr for OrderCloseReason {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_order_str(value).ok_or(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SettlementCloseReason {
    Redeem,
    Merge,
}

impl SettlementCloseReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Redeem => "redeem",
            Self::Merge => "merge",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum MarketTickTrigger {
    #[default]
    MarketData,
    OracleOnly,
}

impl MarketTickTrigger {
    pub fn should_update_oracle(self) -> bool {
        matches!(self, Self::OracleOnly)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceTsKind {
    Unavailable,
    Exchange,
    ProviderEvent,
    #[default]
    EngineIngestWallclock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntentFields {
    #[serde(skip)]
    pub execution_intent_id: Option<u64>,
    #[serde(skip)]
    pub market_frame_received_at: Option<Instant>,
    #[serde(skip)]
    pub market_state_published_at: Option<Instant>,
    #[serde(skip)]
    pub market_connection_session_id: Option<u64>,
    #[serde(skip)]
    pub market_frame_sequence: Option<u64>,
    #[serde(skip)]
    pub market_state_revision: Option<u64>,
    #[serde(skip)]
    pub strategy_consumption_sequence: Option<u64>,
    #[serde(skip)]
    pub strategy_input_kind: Option<&'static str>,
    #[serde(skip)]
    pub decision_at: Option<Instant>,
    #[serde(default)]
    pub execution_evidence_revision: u64,
    #[serde(default)]
    pub execution_instruction_qty_units_6dp: u128,
    #[serde(default)]
    pub execution_instruction_limit_px_units_8dp: u64,
    pub instrument: String,
    #[serde(default)]
    pub market_id: Option<MarketId>,
    pub qty: f64,
    pub limit_px: f64,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub post_only: bool,
    #[serde(default)]
    pub quote_level: Option<u8>,
    #[serde(default)]
    pub quote_group: Option<String>,
    #[serde(default)]
    pub lifecycle_cohort: Option<String>,
    #[serde(default)]
    pub lifecycle_symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyIntent {
    #[serde(flatten)]
    pub fields: OrderIntentFields,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<DirectBuyAuthorization>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SellIntent {
    #[serde(flatten)]
    pub fields: OrderIntentFields,
    pub close_reason: OrderCloseReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_target_qty_units_6dp: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "side", rename_all = "snake_case")]
pub enum OrderIntent {
    Buy(BuyIntent),
    Sell(SellIntent),
}

impl OrderIntent {
    pub fn buy(fields: OrderIntentFields) -> Self {
        Self::Buy(BuyIntent {
            fields,
            authorization: None,
        })
    }

    pub fn authorized_buy(
        fields: OrderIntentFields,
        authorization: DirectBuyAuthorization,
    ) -> Self {
        Self::Buy(BuyIntent {
            fields,
            authorization: Some(authorization),
        })
    }

    pub fn buy_authorization(&self) -> Option<&DirectBuyAuthorization> {
        match self {
            Self::Buy(intent) => intent.authorization.as_ref(),
            Self::Sell(_) => None,
        }
    }

    pub fn sell(fields: OrderIntentFields, close_reason: OrderCloseReason) -> Self {
        Self::Sell(SellIntent {
            fields,
            close_reason,
            first_target_qty_units_6dp: None,
        })
    }

    pub fn common(&self) -> &OrderIntentFields {
        match self {
            Self::Buy(intent) => &intent.fields,
            Self::Sell(intent) => &intent.fields,
        }
    }

    pub fn common_mut(&mut self) -> &mut OrderIntentFields {
        match self {
            Self::Buy(intent) => &mut intent.fields,
            Self::Sell(intent) => &mut intent.fields,
        }
    }

    pub fn side(&self) -> Side {
        match self {
            Self::Buy(_) => Side::Buy,
            Self::Sell(_) => Side::Sell,
        }
    }

    pub fn order_close_reason(&self) -> Option<OrderCloseReason> {
        match self {
            Self::Buy(_) => None,
            Self::Sell(intent) => Some(intent.close_reason),
        }
    }

    pub fn order_close_reason_str(&self) -> Option<&'static str> {
        self.order_close_reason().map(|reason| reason.as_str())
    }
}

impl Deref for OrderIntent {
    type Target = OrderIntentFields;

    fn deref(&self) -> &Self::Target {
        self.common()
    }
}

impl DerefMut for OrderIntent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.common_mut()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTick {
    pub instrument: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_sz: Option<f64>,
    pub ask_sz: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_imbalance_l5: Option<f64>,
    pub source_ts_ns: i64,
    #[serde(default)]
    pub source_ts_kind: SourceTsKind,
    pub ingest_mono_ns: u64,
    pub ts_ns: i64,
    #[serde(default)]
    pub trigger: MarketTickTrigger,
}

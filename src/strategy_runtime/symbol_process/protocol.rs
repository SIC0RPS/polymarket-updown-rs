use anyhow::Result;

#[path = "protocol/records.rs"]
pub(super) mod records;

pub(crate) use crate::strategy_runtime::replay_mode::prepared_event_stream::EventRef;
pub(crate) use records::{
    ActiveLifecycleDeltaRecord, CandidateReset, CohortIdentityRecord, Drive, DriveKind,
    EntryContextRecord, Finalize, IntentRecord, InventoryReceiptOperation, InventoryReceiptRecord,
    ManagedSellReceiptRecord, MarketBudgetRecord, OpenOrderDeltaRecord, OracleResolvedLabelRecord,
    OwnerAuditRecord, PositionDeltaRecord, ProtocolMode, ResultIdentity, RiskBudget,
    SplitEntryContextRecord, OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG,
};

pub const MAX_PARAMETER_COUNT: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[repr(u8)]
pub enum SymbolId {
    Btc = 0,
    Eth = 1,
    Sol = 2,
    Xrp = 3,
}

impl SymbolId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "BTC",
            Self::Eth => "ETH",
            Self::Sol => "SOL",
            Self::Xrp => "XRP",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        match raw {
            value if value.eq_ignore_ascii_case("BTC") => Ok(Self::Btc),
            value if value.eq_ignore_ascii_case("ETH") => Ok(Self::Eth),
            value if value.eq_ignore_ascii_case("SOL") => Ok(Self::Sol),
            value if value.eq_ignore_ascii_case("XRP") => Ok(Self::Xrp),
            other => anyhow::bail!("unsupported symbol runtime `{other}`"),
        }
    }
}

impl TryFrom<u8> for SymbolId {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Btc),
            1 => Ok(Self::Eth),
            2 => Ok(Self::Sol),
            3 => Ok(Self::Xrp),
            other => anyhow::bail!("invalid symbol id={other}"),
        }
    }
}

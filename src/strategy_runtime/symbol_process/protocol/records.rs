use anyhow::Result;

use super::MAX_PARAMETER_COUNT;
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};

#[path = "records/central_state.rs"]
mod central_state;
#[path = "records/cohort_identity.rs"]
mod cohort_identity;
#[path = "records/receipts.rs"]
mod receipts;
#[path = "records/split_context.rs"]
mod split_context;
#[path = "records/worker_output.rs"]
mod worker_output;
use central_state::{catalog_market, ensure_inventory_identity, ensure_market_token};
pub(crate) use central_state::{
    ActiveLifecycleDeltaRecord, MarketBudgetRecord, OpenOrderDeltaRecord, PositionDeltaRecord,
    RiskBudget,
};
pub(crate) use cohort_identity::CohortIdentityRecord;
pub(crate) use receipts::{
    InventoryReceiptOperation, InventoryReceiptRecord, ManagedSellReceiptRecord,
};
pub(crate) use split_context::{EntryContextRecord, SplitEntryContextRecord};
pub(crate) use worker_output::{
    IntentRecord, OracleResolvedLabelRecord, OwnerAuditRecord, OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProtocolMode {
    Live = 1,
    Replay = 2,
    Optimize = 3,
}
impl TryFrom<u8> for ProtocolMode {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Live),
            2 => Ok(Self::Replay),
            3 => Ok(Self::Optimize),
            other => anyhow::bail!("invalid symbol runtime mode={other}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateReset {
    pub candidate_sha256: [u8; 32],
    pub base_config_sha256: [u8; 32],
    pub parameter_schema_sha256: [u8; 32],
    pub flags: u32,
    pub parameter_bits: Vec<u64>,
}

impl CandidateReset {
    pub fn new(
        candidate_sha256: [u8; 32],
        base_config_sha256: [u8; 32],
        parameter_schema_sha256: [u8; 32],
        flags: u32,
        parameter_values: &[f64],
    ) -> Result<Self> {
        anyhow::ensure!(
            parameter_values.len() <= MAX_PARAMETER_COUNT,
            "candidate parameter count exceeds limit count={}",
            parameter_values.len()
        );
        let mut parameter_bits = Vec::with_capacity(parameter_values.len());
        for (index, value) in parameter_values.iter().copied().enumerate() {
            anyhow::ensure!(
                value.is_finite(),
                "candidate parameter is nonfinite index={index}"
            );
            parameter_bits.push(value.to_bits());
        }
        Ok(Self {
            candidate_sha256,
            base_config_sha256,
            parameter_schema_sha256,
            flags,
            parameter_bits,
        })
    }

    pub fn parameter_values(&self) -> impl ExactSizeIterator<Item = f64> + '_ {
        self.parameter_bits.iter().copied().map(f64::from_bits)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.candidate_sha256.iter().any(|byte| *byte != 0),
            "candidate identity is zero"
        );
        anyhow::ensure!(self.flags == 0, "candidate reset contains unknown flags");
        anyhow::ensure!(
            self.parameter_bits.len() <= MAX_PARAMETER_COUNT,
            "candidate parameter count exceeds limit count={}",
            self.parameter_bits.len()
        );
        for (index, bits) in self.parameter_bits.iter().copied().enumerate() {
            anyhow::ensure!(
                f64::from_bits(bits).is_finite(),
                "candidate parameter is nonfinite index={index}"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DriveKind {
    PortfolioChanged = 2,
    LiveStep = 3,
}

impl TryFrom<u8> for DriveKind {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::PortfolioChanged),
            3 => Ok(Self::LiveStep),
            other => anyhow::bail!("invalid symbol IPC drive kind={other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drive {
    pub source_sequence: u64,
    pub central_state_seq: u64,
    pub kind: DriveKind,
    pub flags: u16,
}

impl Drive {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.flags == 0, "drive contains unknown flags");
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Finalize {
    pub candidate_sha256: [u8; 32],
    pub last_global_sequence: u64,
    pub last_central_state_seq: u64,
    pub local_event_count: u64,
}

impl Finalize {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            self.candidate_sha256.iter().any(|byte| *byte != 0),
            "finalize candidate identity is zero"
        );
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultIdentity {
    pub candidate_sha256: [u8; 32],
    pub last_global_sequence: u64,
    pub last_central_state_seq: u64,
    pub local_event_count: u64,
    pub artifact_sha256: [u8; 32],
    pub artifact_size: u64,
    pub status: u8,
}

impl ResultIdentity {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(
            self.candidate_sha256.iter().any(|byte| *byte != 0),
            "result candidate identity is zero"
        );
        anyhow::ensure!(
            self.artifact_sha256.iter().any(|byte| *byte != 0),
            "result artifact identity is zero"
        );
        anyhow::ensure!(self.artifact_size > 0, "result artifact is empty");
        anyhow::ensure!(self.status == 1, "result status is not successful");
        Ok(self)
    }
}

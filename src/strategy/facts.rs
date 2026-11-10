use serde::{Deserialize, Serialize};

use crate::types::OrderCloseReason;

use super::hold_to_resolution::PairTokenSide;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum CtfLegEconomicOutcome {
    SellNow = 1,
    HoldForSettlement = 2,
}

impl TryFrom<u8> for CtfLegEconomicOutcome {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::SellNow),
            2 => Ok(Self::HoldForSettlement),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum CtfOwnedLegCloseReason {
    PairedFirstManagedSell = 1,
    PairedPostProceedsSell = 2,
}

impl TryFrom<u8> for CtfOwnedLegCloseReason {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PairedFirstManagedSell),
            2 => Ok(Self::PairedPostProceedsSell),
            _ => Err(()),
        }
    }
}

impl TryFrom<OrderCloseReason> for CtfOwnedLegCloseReason {
    type Error = ();

    fn try_from(value: OrderCloseReason) -> Result<Self, Self::Error> {
        match value {
            OrderCloseReason::PairedFirstManagedSell => Ok(Self::PairedFirstManagedSell),
            OrderCloseReason::PairedPostProceedsSell => Ok(Self::PairedPostProceedsSell),
            OrderCloseReason::PredictivePostProceedsSell => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u16)]
pub enum CtfLegDecisionReason {
    NoLifecycleOwnedQty = 1,
    MarketWindowElapsed = 2,
    NoSellableActionableQty = 3,
    CtfEntryNoExecutableSellSurface = 4,
    BelowVenueMinSize = 5,
    CtfSellLifecycleActive = 6,
    ConfirmedFirstSellReceiptUnavailable = 7,
    ProjectionMissing = 8,
    InvalidLimitPx = 9,
    CtfSellLimitDepthNotProven = 10,
    JointEntrySellUpSelected = 14,
    JointEntrySellDownSelected = 15,
    JointEntryHoldForSettlementSelected = 16,
    /// Post-proceeds second sell: full-clear salvage after keep-side moved away.
    DeadCat = 46,
    /// Post-proceeds second sell: settlement-like or economically superior to hold.
    ProfitNow = 47,
    CtfFirstSellNoSide = 24,
    NoFillableQty = 28,
    NonPositiveNetProceeds = 29,
    LossCutFloorNotMet = 30,
    PairedBooksUnavailable = 31,
    MissingMarketOracleInput = 32,
    MissingLiveFairResult = 33,
    MissingTopUnwindUp = 34,
    MissingTopUnwindDown = 35,
    MissingExecutableLadderBoth = 36,
    MissingExecutableLadderUp = 37,
    MissingExecutableLadderDown = 38,
    MissingExecutableLadder = 39,
    InvalidProbabilityCalibrationInputs = 40,
    OracleEmissionBoundsUnavailable = 41,
    OracleEmissionLcbNonpositive = 42,
    OracleEmissionDominanceInsufficient = 43,
    OracleCleanupLcbBelowRelease = 44,
    MarketQuarantinedBeforeSellRoute = 45,
}

impl CtfLegDecisionReason {
    pub(crate) const fn permits_outcome(self, outcome: CtfLegEconomicOutcome) -> bool {
        match outcome {
            CtfLegEconomicOutcome::SellNow => !matches!(
                self,
                Self::JointEntryHoldForSettlementSelected | Self::CtfFirstSellNoSide
            ),
            CtfLegEconomicOutcome::HoldForSettlement => !matches!(
                self,
                Self::JointEntrySellUpSelected
                    | Self::JointEntrySellDownSelected
                    | Self::DeadCat
                    | Self::ProfitNow
            ),
        }
    }
}

impl TryFrom<u16> for CtfLegDecisionReason {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::NoLifecycleOwnedQty),
            2 => Ok(Self::MarketWindowElapsed),
            3 => Ok(Self::NoSellableActionableQty),
            4 => Ok(Self::CtfEntryNoExecutableSellSurface),
            5 => Ok(Self::BelowVenueMinSize),
            6 => Ok(Self::CtfSellLifecycleActive),
            7 => Ok(Self::ConfirmedFirstSellReceiptUnavailable),
            8 => Ok(Self::ProjectionMissing),
            9 => Ok(Self::InvalidLimitPx),
            10 => Ok(Self::CtfSellLimitDepthNotProven),
            14 => Ok(Self::JointEntrySellUpSelected),
            15 => Ok(Self::JointEntrySellDownSelected),
            16 => Ok(Self::JointEntryHoldForSettlementSelected),
            24 => Ok(Self::CtfFirstSellNoSide),
            28 => Ok(Self::NoFillableQty),
            29 => Ok(Self::NonPositiveNetProceeds),
            30 => Ok(Self::LossCutFloorNotMet),
            31 => Ok(Self::PairedBooksUnavailable),
            32 => Ok(Self::MissingMarketOracleInput),
            33 => Ok(Self::MissingLiveFairResult),
            34 => Ok(Self::MissingTopUnwindUp),
            35 => Ok(Self::MissingTopUnwindDown),
            36 => Ok(Self::MissingExecutableLadderBoth),
            37 => Ok(Self::MissingExecutableLadderUp),
            38 => Ok(Self::MissingExecutableLadderDown),
            39 => Ok(Self::MissingExecutableLadder),
            40 => Ok(Self::InvalidProbabilityCalibrationInputs),
            41 => Ok(Self::OracleEmissionBoundsUnavailable),
            42 => Ok(Self::OracleEmissionLcbNonpositive),
            43 => Ok(Self::OracleEmissionDominanceInsufficient),
            44 => Ok(Self::OracleCleanupLcbBelowRelease),
            45 => Ok(Self::MarketQuarantinedBeforeSellRoute),
            46 => Ok(Self::DeadCat),
            47 => Ok(Self::ProfitNow),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CtfOwnedLegOutcomeFact {
    ts_ms: u64,
    market_id: String,
    cohort_id: String,
    token: String,
    owned_qty_units_6dp: u128,
    action_qty_units_6dp: u128,
    execution_evidence_revision: u64,
    causal_winner_side: Option<PairTokenSide>,
    first_instruction_economics: Option<CtfFirstInstructionEconomicsEvidence>,
    post_sell_economics: Option<CtfPostSellEconomicsEvidence>,
    outcome: CtfLegEconomicOutcome,
    reason: CtfLegDecisionReason,
    close_reason: Option<CtfOwnedLegCloseReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CtfFirstInstructionEconomicsEvidence {
    pub(crate) instruction_limit_px_bits: u64,
    pub(crate) depth_gross_proceeds_bits: u64,
    pub(crate) depth_fee_bits: u64,
    pub(crate) depth_net_proceeds_bits: u64,
    pub(crate) authorization_fee_bits: u64,
    pub(crate) authorization_net_proceeds_bits: u64,
    pub(crate) delay_cushion_bits: u64,
    pub(crate) allocated_basis_units_6dp: u128,
    pub(crate) upper_valuation_bits: Option<u64>,
    pub(crate) reservation_bits: u64,
    pub(crate) mandatory_urgency: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CtfPostSellEconomicsEvidence {
    pub(crate) sell_full_entry_pnl_bits: u64,
    pub(crate) hold_full_entry_pnl_bits: u64,
    pub(crate) fillable_qty_bits: u64,
}

impl CtfOwnedLegOutcomeFact {
    #[allow(
        clippy::too_many_arguments,
        reason = "The canonical owned-leg fact carries one complete validated owner decision"
    )]
    pub(crate) fn new(
        ts_ms: u64,
        market_id: String,
        cohort_id: String,
        token: String,
        owned_qty_units_6dp: u128,
        action_qty_units_6dp: u128,
        execution_evidence_revision: u64,
        causal_winner_side: Option<PairTokenSide>,
        first_instruction_economics: Option<CtfFirstInstructionEconomicsEvidence>,
        post_sell_economics: Option<CtfPostSellEconomicsEvidence>,
        outcome: CtfLegEconomicOutcome,
        reason: CtfLegDecisionReason,
        close_reason: Option<CtfOwnedLegCloseReason>,
    ) -> Self {
        assert!(ts_ms > 0, "CTF owner fact timestamp is zero");
        assert!(
            !market_id.trim().is_empty(),
            "CTF owner fact market is empty"
        );
        assert!(
            !cohort_id.trim().is_empty(),
            "CTF owner fact cohort is empty"
        );
        assert!(!token.trim().is_empty(), "CTF owner fact token is empty");
        assert!(
            reason.permits_outcome(outcome),
            "CTF owner fact reason is invalid for outcome"
        );
        assert!(
            close_reason != Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
                || causal_winner_side.is_some(),
            "first-managed-sell owner fact requires causal winner side"
        );
        match outcome {
            CtfLegEconomicOutcome::SellNow => {
                let blocked = action_qty_units_6dp == 0 && execution_evidence_revision == 0;
                let executable = action_qty_units_6dp > 0
                    && action_qty_units_6dp <= owned_qty_units_6dp
                    && execution_evidence_revision > 0;
                assert!(
                    owned_qty_units_6dp > 0 && (blocked || executable) && close_reason.is_some(),
                    "SELL_NOW owner fact requires owned inventory, close reason, and either blocked or executable action fields"
                );
            }
            CtfLegEconomicOutcome::HoldForSettlement => assert!(
                owned_qty_units_6dp > 0 && action_qty_units_6dp == 0 && close_reason.is_none(),
                "HOLD_FOR_SETTLEMENT owner fact requires owned inventory and no action"
            ),
        }
        assert!(
            post_sell_economics.is_none()
                || close_reason == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell),
            "post-sell economics evidence requires post-proceeds SELL_NOW"
        );
        assert!(
            close_reason != Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
                || action_qty_units_6dp == 0
                || post_sell_economics.is_some(),
            "executable post-proceeds SELL_NOW requires exact economics evidence"
        );
        assert!(
            outcome == CtfLegEconomicOutcome::SellNow || execution_evidence_revision == 0,
            "non-SELL_NOW owner fact carries execution evidence revision"
        );
        Self {
            ts_ms,
            market_id,
            cohort_id,
            token,
            owned_qty_units_6dp,
            action_qty_units_6dp,
            execution_evidence_revision,
            causal_winner_side,
            first_instruction_economics,
            post_sell_economics,
            outcome,
            reason,
            close_reason,
        }
    }

    pub(crate) fn fields(
        &self,
    ) -> (
        u64,
        &str,
        &str,
        &str,
        u128,
        u128,
        u64,
        Option<PairTokenSide>,
        Option<CtfFirstInstructionEconomicsEvidence>,
        Option<CtfPostSellEconomicsEvidence>,
        CtfLegEconomicOutcome,
        CtfLegDecisionReason,
        Option<CtfOwnedLegCloseReason>,
    ) {
        (
            self.ts_ms,
            self.market_id.as_str(),
            self.cohort_id.as_str(),
            self.token.as_str(),
            self.owned_qty_units_6dp,
            self.action_qty_units_6dp,
            self.execution_evidence_revision,
            self.causal_winner_side,
            self.first_instruction_economics,
            self.post_sell_economics,
            self.outcome,
            self.reason,
            self.close_reason,
        )
    }

    pub(crate) fn with_execution_action_qty_units_6dp(&self, action_qty_units_6dp: u128) -> Self {
        Self::new(
            self.ts_ms,
            self.market_id.clone(),
            self.cohort_id.clone(),
            self.token.clone(),
            self.owned_qty_units_6dp,
            action_qty_units_6dp,
            if action_qty_units_6dp == 0 {
                0
            } else {
                self.execution_evidence_revision
            },
            self.causal_winner_side,
            if action_qty_units_6dp == 0 {
                None
            } else {
                self.first_instruction_economics
            },
            self.post_sell_economics,
            self.outcome,
            self.reason,
            self.close_reason,
        )
    }
}

#[derive(Debug, Clone)]
pub enum StrategyFact {
    CtfOwnedLegOutcome(CtfOwnedLegOutcomeFact),
}

#[derive(Debug, Default, Clone)]
pub struct StrategyFactSink {
    facts: Vec<StrategyFact>,
}

impl StrategyFactSink {
    pub fn push(&mut self, fact: StrategyFact) {
        self.facts.push(fact);
    }

    pub fn drain(&mut self) -> Vec<StrategyFact> {
        self.facts.drain(..).collect()
    }
}

use super::super::PairTokenSide;
use crate::strategy::facts::{CtfFirstInstructionEconomicsEvidence, CtfLegDecisionReason};
use crate::strategy::pair_lifecycle::CtfOwnedLegDisposition;

#[derive(Debug, Clone, Copy)]
pub(in crate::strategy::hold_to_resolution) struct CtfEntryLedgerProjection {
    pub(in crate::strategy::hold_to_resolution) instruction_qty_units_6dp: u128,
    pub(in crate::strategy::hold_to_resolution) confirmed_first_cohort_qty_units_6dp: Option<u128>,
    pub(in crate::strategy::hold_to_resolution) execution_evidence_revision: u64,
    pub(in crate::strategy::hold_to_resolution) prior_net_proceeds: f64,
    pub(in crate::strategy::hold_to_resolution) current_fillable_qty: f64,
    pub(in crate::strategy::hold_to_resolution) current_net_proceeds: f64,
    pub(in crate::strategy::hold_to_resolution) current_authorization_net_proceeds: f64,
    pub(in crate::strategy::hold_to_resolution) current_fair_value: f64,
    pub(in crate::strategy::hold_to_resolution) exit_fair_value: f64,
    pub(in crate::strategy::hold_to_resolution) entry_cost: f64,
    pub(in crate::strategy::hold_to_resolution) limit_px: f64,
    pub(in crate::strategy::hold_to_resolution) first_instruction_economics:
        Option<CtfFirstInstructionEconomicsEvidence>,
    pub(in crate::strategy::hold_to_resolution) first_sell_allowed: bool,
    pub(in crate::strategy::hold_to_resolution) dead_cat_price_below_reference: bool,
    pub(in crate::strategy::hold_to_resolution) dead_cat_instruction: Option<CtfDeadCatInstruction>,
    pub(in crate::strategy::hold_to_resolution) oracle_signal_side: Option<PairTokenSide>,
    pub(in crate::strategy::hold_to_resolution) p_fair: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::strategy::hold_to_resolution) struct CtfDeadCatInstruction {
    pub(in crate::strategy::hold_to_resolution) limit_px: f64,
    pub(in crate::strategy::hold_to_resolution) current: super::super::FakSellDepthValuation,
}

impl CtfEntryLedgerProjection {
    pub(in crate::strategy::hold_to_resolution) fn with_dead_cat_instruction(
        self,
        instruction: CtfDeadCatInstruction,
    ) -> Self {
        Self {
            limit_px: instruction.limit_px,
            current_fillable_qty: instruction.current.fillable_qty,
            current_net_proceeds: instruction.current.net,
            current_authorization_net_proceeds: instruction.current.authorization_net,
            dead_cat_price_below_reference: true,
            dead_cat_instruction: None,
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::strategy::hold_to_resolution) struct CtfMarketEntryLegSnapshot {
    pub(in crate::strategy::hold_to_resolution) token_side: PairTokenSide,
    pub(in crate::strategy::hold_to_resolution) owned_qty_units_6dp: u128,
    pub(in crate::strategy::hold_to_resolution) economic_owned_qty: f64,
    pub(in crate::strategy::hold_to_resolution) route_block_reason: Option<CtfLegDecisionReason>,
    pub(in crate::strategy::hold_to_resolution) projection: Option<CtfEntryLedgerProjection>,
}

#[derive(Debug, Clone)]
pub(in crate::strategy::hold_to_resolution) struct CtfMarketEntryLegDecision {
    pub(in crate::strategy::hold_to_resolution) disposition: CtfOwnedLegDisposition,
    pub(in crate::strategy::hold_to_resolution) fresh_first_instruction_authorized: bool,
    pub(in crate::strategy::hold_to_resolution) selected_dead_cat_instruction:
        Option<CtfDeadCatInstruction>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CtfLegMarketEvaluation {
    pub(super) projection: CtfEntryLedgerProjection,
}

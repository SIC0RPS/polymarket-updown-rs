use anyhow::{ensure, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::polymarket::identity::MarketId;
use crate::strategy::facts::{
    CtfFirstInstructionEconomicsEvidence, CtfLegDecisionReason, CtfLegEconomicOutcome,
    CtfOwnedLegCloseReason, CtfOwnedLegOutcomeFact,
};
use crate::strategy::hold_to_resolution::PairTokenSide;

use super::TradeLedgerRecord;

#[path = "owned_leg_outcomes/current_projection.rs"]
mod current_projection;
pub(super) use current_projection::{
    project_owned_leg_outcomes, validate_joined_owned_leg_outcome, OwnedLegValidationProjection,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalOwnedLegOutcome {
    pub symbol: String,
    pub cohort_id: String,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub ledger_seq: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub ts_ms: u64,
    pub market_id: MarketId,
    pub token: String,
    #[serde(
        serialize_with = "super::stats::serialize_u128_string",
        deserialize_with = "super::stats::deserialize_u128_string"
    )]
    pub owned_qty_units_6dp: u128,
    #[serde(
        serialize_with = "super::stats::serialize_u128_string",
        deserialize_with = "super::stats::deserialize_u128_string"
    )]
    pub action_qty_units_6dp: u128,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub execution_intent_id: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub execution_evidence_revision: Option<u64>,
    pub(crate) causal_winner_side: Option<PairTokenSide>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_instruction_economics: Option<CanonicalCtfFirstInstructionEconomics>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub post_sell_full_entry_pnl_bits: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub post_hold_full_entry_pnl_bits: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub post_fillable_qty_bits: Option<u64>,
    pub outcome: CtfLegEconomicOutcome,
    pub reason: CtfLegDecisionReason,
    pub close_reason: Option<CtfOwnedLegCloseReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfFirstInstructionEconomics {
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub instruction_limit_px_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub execution_evidence_revision: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub depth_gross_proceeds_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub depth_fee_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub depth_net_proceeds_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub authorization_fee_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub authorization_net_proceeds_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub delay_cushion_bits: u64,
    #[serde(
        serialize_with = "super::stats::serialize_u128_string",
        deserialize_with = "super::stats::deserialize_u128_string"
    )]
    pub allocated_basis_units_6dp: u128,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_u64_string",
        deserialize_with = "deserialize_optional_u64_string"
    )]
    pub upper_valuation_bits: Option<u64>,
    #[serde(
        serialize_with = "super::stats::serialize_u64_string",
        deserialize_with = "super::stats::deserialize_u64_string"
    )]
    pub reservation_bits: u64,
    pub mandatory_urgency: bool,
}

#[derive(Debug)]
pub(crate) struct UnjoinedOwnedLegOutcome {
    pub(crate) symbol: String,
    pub(crate) candidate_slot: u32,
    pub(crate) worker_epoch: u32,
    pub(crate) audit_seq: u64,
    pub(crate) source_sequence: u64,
    pub(crate) central_state_seq: u64,
    pub(crate) ts_ms: u64,
    pub(crate) market_id: MarketId,
    pub(crate) cohort_id: String,
    pub(crate) token: String,
    pub(crate) owned_qty_units_6dp: u128,
    pub(crate) action_qty_units_6dp: u128,
    pub(crate) execution_intent_id: Option<u64>,
    pub(crate) execution_evidence_revision: Option<u64>,
    pub(crate) causal_winner_side: Option<PairTokenSide>,
    pub(crate) first_instruction_economics: Option<CtfFirstInstructionEconomicsEvidence>,
    pub(crate) post_sell_full_entry_pnl_bits: Option<u64>,
    pub(crate) post_hold_full_entry_pnl_bits: Option<u64>,
    pub(crate) post_fillable_qty_bits: Option<u64>,
    pub(crate) outcome: CtfLegEconomicOutcome,
    pub(crate) reason: CtfLegDecisionReason,
    pub(crate) close_reason: Option<CtfOwnedLegCloseReason>,
}

impl UnjoinedOwnedLegOutcome {
    pub(crate) fn execution_intent_source_identity(&self) -> Result<Option<(&str, u32, u64)>> {
        match self.outcome {
            CtfLegEconomicOutcome::SellNow => {
                if self.action_qty_units_6dp == 0 {
                    ensure!(
                        self.execution_intent_id.is_none()
                            && self.execution_evidence_revision.is_none(),
                        "blocked SELL_NOW owner outcome carries execution identity or evidence"
                    );
                    return Ok(None);
                }
                ensure!(
                    self.execution_evidence_revision
                        .is_some_and(|revision| revision > 0),
                    "executable SELL_NOW owner outcome has no evidence revision"
                );
                let intent_id = self
                    .execution_intent_id
                    .filter(|intent_id| *intent_id > 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!("SELL_NOW owner outcome has no source intent identity")
                    })?;
                ensure!(
                    matches!(self.symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP")
                        && self.worker_epoch > 0,
                    "SELL_NOW owner outcome source identity is foreign or incomplete"
                );
                Ok(Some((self.symbol.as_str(), self.worker_epoch, intent_id)))
            }
            CtfLegEconomicOutcome::HoldForSettlement => {
                ensure!(
                    self.execution_intent_id.is_none(),
                    "non-SELL owner outcome carries execution intent identity"
                );
                Ok(None)
            }
        }
    }

    pub(crate) fn bind_central_execution_intent_id(
        &mut self,
        source_symbol: &str,
        source_worker_epoch: u32,
        source_intent_id: u64,
        central_intent_id: u64,
    ) -> Result<()> {
        ensure!(
            self.outcome == CtfLegEconomicOutcome::SellNow
                && self.symbol == source_symbol
                && self.worker_epoch == source_worker_epoch
                && self.execution_intent_id == Some(source_intent_id),
            "SELL_NOW owner outcome source intent identity changed before central binding"
        );
        ensure!(
            central_intent_id > 0,
            "SELL_NOW central execution intent identity is zero"
        );
        self.execution_intent_id = Some(central_intent_id);
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn join(
        &self,
        ledger_seq: u64,
    ) -> Result<CanonicalOwnedLegOutcome> {
        ensure!(
            matches!(self.symbol.as_str(), "BTC" | "ETH" | "SOL" | "XRP")
                && self.audit_seq > 0
                && self.source_sequence > 0
                && self.central_state_seq > 0
                && self.ts_ms > 0
                && !self.market_id.as_str().trim().is_empty()
                && !self.token.trim().is_empty(),
            "owned-leg transport identity is incomplete"
        );
        let joined = canonical_outcome(
            self.symbol.clone(),
            self.cohort_id.clone(),
            ledger_seq,
            self.ts_ms,
            self.market_id.clone(),
            self.token.clone(),
            self.owned_qty_units_6dp,
            self.action_qty_units_6dp,
            self.execution_intent_id,
            self.execution_evidence_revision,
            self.causal_winner_side,
            self.first_instruction_economics.map(|evidence| {
                canonical_first_instruction_economics(
                    evidence,
                    self.execution_evidence_revision.unwrap_or(0),
                )
            }),
            self.post_sell_full_entry_pnl_bits,
            self.post_hold_full_entry_pnl_bits,
            self.post_fillable_qty_bits,
            self.outcome,
            self.reason,
            self.close_reason,
        );
        Ok(joined)
    }
}

pub(super) fn join_live_owned_leg_outcome<'a>(
    symbol: &str,
    fact: &CtfOwnedLegOutcomeFact,
    execution_intent_id: Option<u64>,
    ledger_seq: u64,
    records: impl IntoIterator<Item = &'a TradeLedgerRecord>,
) -> Result<CanonicalOwnedLegOutcome> {
    let (
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
    ) = fact.fields();
    let market_id = MarketId::new(market_id);
    let joined = canonical_outcome(
        symbol.to_string(),
        cohort_id.to_string(),
        ledger_seq,
        ts_ms,
        market_id,
        token.to_string(),
        owned_qty_units_6dp,
        action_qty_units_6dp,
        execution_intent_id,
        (execution_evidence_revision > 0).then_some(execution_evidence_revision),
        causal_winner_side,
        first_instruction_economics.map(|evidence| {
            canonical_first_instruction_economics(evidence, execution_evidence_revision)
        }),
        post_sell_economics.map(|evidence| evidence.sell_full_entry_pnl_bits),
        post_sell_economics.map(|evidence| evidence.hold_full_entry_pnl_bits),
        post_sell_economics.map(|evidence| evidence.fillable_qty_bits),
        outcome,
        reason,
        close_reason,
    );
    validate_joined_owned_leg_outcome(&joined, records)?;
    Ok(joined)
}

pub(super) fn repeats_latest_hold_decision<'a, R, P>(
    candidate: &CanonicalOwnedLegOutcome,
    records: R,
    pending_records: P,
) -> bool
where
    R: IntoIterator<Item = &'a TradeLedgerRecord>,
    R::IntoIter: DoubleEndedIterator,
    P: IntoIterator<Item = &'a TradeLedgerRecord>,
    P::IntoIter: DoubleEndedIterator,
{
    if candidate.outcome != CtfLegEconomicOutcome::HoldForSettlement {
        return false;
    }
    pending_records
        .into_iter()
        .rev()
        .chain(records.into_iter().rev())
        .filter_map(|record| record.owned_leg_outcome.as_ref())
        .find(|previous| {
            previous.symbol == candidate.symbol
                && previous.market_id == candidate.market_id
                && previous.cohort_id == candidate.cohort_id
                && previous.token == candidate.token
        })
        .is_some_and(|previous| {
            previous.owned_qty_units_6dp == candidate.owned_qty_units_6dp
                && previous.action_qty_units_6dp == candidate.action_qty_units_6dp
                && previous.execution_intent_id == candidate.execution_intent_id
                && previous.execution_evidence_revision == candidate.execution_evidence_revision
                && previous.causal_winner_side == candidate.causal_winner_side
                && previous.first_instruction_economics == candidate.first_instruction_economics
                && previous.post_sell_full_entry_pnl_bits == candidate.post_sell_full_entry_pnl_bits
                && previous.post_hold_full_entry_pnl_bits == candidate.post_hold_full_entry_pnl_bits
                && previous.post_fillable_qty_bits == candidate.post_fillable_qty_bits
                && previous.outcome == candidate.outcome
                && previous.reason == candidate.reason
                && previous.close_reason == candidate.close_reason
        })
}

fn canonical_outcome(
    symbol: String,
    cohort_id: String,
    ledger_seq: u64,
    ts_ms: u64,
    market_id: MarketId,
    token: String,
    owned_qty_units_6dp: u128,
    action_qty_units_6dp: u128,
    execution_intent_id: Option<u64>,
    execution_evidence_revision: Option<u64>,
    causal_winner_side: Option<PairTokenSide>,
    first_instruction_economics: Option<CanonicalCtfFirstInstructionEconomics>,
    post_sell_full_entry_pnl_bits: Option<u64>,
    post_hold_full_entry_pnl_bits: Option<u64>,
    post_fillable_qty_bits: Option<u64>,
    outcome: CtfLegEconomicOutcome,
    reason: CtfLegDecisionReason,
    close_reason: Option<CtfOwnedLegCloseReason>,
) -> CanonicalOwnedLegOutcome {
    CanonicalOwnedLegOutcome {
        symbol,
        cohort_id,
        ledger_seq,
        ts_ms,
        market_id,
        token,
        owned_qty_units_6dp,
        action_qty_units_6dp,
        execution_intent_id,
        execution_evidence_revision,
        causal_winner_side,
        first_instruction_economics,
        post_sell_full_entry_pnl_bits,
        post_hold_full_entry_pnl_bits,
        post_fillable_qty_bits,
        outcome,
        reason,
        close_reason,
    }
}

fn canonical_first_instruction_economics(
    evidence: CtfFirstInstructionEconomicsEvidence,
    execution_evidence_revision: u64,
) -> CanonicalCtfFirstInstructionEconomics {
    CanonicalCtfFirstInstructionEconomics {
        instruction_limit_px_bits: evidence.instruction_limit_px_bits,
        execution_evidence_revision,
        depth_gross_proceeds_bits: evidence.depth_gross_proceeds_bits,
        depth_fee_bits: evidence.depth_fee_bits,
        depth_net_proceeds_bits: evidence.depth_net_proceeds_bits,
        authorization_fee_bits: evidence.authorization_fee_bits,
        authorization_net_proceeds_bits: evidence.authorization_net_proceeds_bits,
        delay_cushion_bits: evidence.delay_cushion_bits,
        allocated_basis_units_6dp: evidence.allocated_basis_units_6dp,
        upper_valuation_bits: evidence.upper_valuation_bits,
        reservation_bits: evidence.reservation_bits,
        mandatory_urgency: evidence.mandatory_urgency,
    }
}

fn validate_owner_decision(
    owned_qty_units_6dp: u128,
    action_qty_units_6dp: u128,
    execution_intent_id: Option<u64>,
    execution_evidence_revision: Option<u64>,
    causal_winner_side: Option<PairTokenSide>,
    post_sell_full_entry_pnl_bits: Option<u64>,
    post_hold_full_entry_pnl_bits: Option<u64>,
    post_fillable_qty_bits: Option<u64>,
    outcome: CtfLegEconomicOutcome,
    reason: CtfLegDecisionReason,
    close_reason: Option<CtfOwnedLegCloseReason>,
) -> Result<()> {
    ensure!(
        reason.permits_outcome(outcome),
        "owned-leg reason is invalid for outcome"
    );
    ensure!(
        close_reason != Some(CtfOwnedLegCloseReason::PairedFirstManagedSell)
            || causal_winner_side.is_some(),
        "first-managed-sell owned-leg outcome omitted causal winner side"
    );
    let post_sell_economics = match (
        post_sell_full_entry_pnl_bits,
        post_hold_full_entry_pnl_bits,
        post_fillable_qty_bits,
    ) {
        (Some(sell_bits), Some(hold_bits), Some(fillable_bits)) => {
            let sell_full_entry_pnl = f64::from_bits(sell_bits);
            let hold_full_entry_pnl = f64::from_bits(hold_bits);
            let fillable_qty = f64::from_bits(fillable_bits);
            ensure!(
                sell_full_entry_pnl.is_finite()
                    && hold_full_entry_pnl.is_finite()
                    && fillable_qty.is_finite()
                    && fillable_qty > 0.0,
                "post-proceeds owned-leg economics evidence is non-finite or empty"
            );
            Some((sell_full_entry_pnl, hold_full_entry_pnl, fillable_qty))
        }
        (None, None, None) => None,
        _ => anyhow::bail!("post-proceeds owned-leg economics evidence is incomplete"),
    };
    ensure!(
        post_sell_economics.is_none()
            || close_reason == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell),
        "post-sell economics evidence requires post-proceeds SELL_NOW"
    );
    ensure!(
        close_reason != Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
            || action_qty_units_6dp == 0
            || post_sell_economics.is_some(),
        "executable post-proceeds SELL_NOW requires exact economics evidence"
    );
    match outcome {
        CtfLegEconomicOutcome::SellNow => {
            let blocked = action_qty_units_6dp == 0
                && execution_intent_id.is_none()
                && execution_evidence_revision.is_none();
            let executable = action_qty_units_6dp > 0
                && action_qty_units_6dp <= owned_qty_units_6dp
                && execution_intent_id.is_some_and(|intent_id| intent_id > 0)
                && execution_evidence_revision.is_some_and(|revision| revision > 0);
            ensure!(
                owned_qty_units_6dp > 0
                    && (blocked || executable)
                    && close_reason.is_some(),
                "SELL_NOW owned-leg outcome requires owned inventory, close reason, and either blocked or executable action fields"
            );
            if let Some((_, _, fillable_qty)) = post_sell_economics {
                let quantum = crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
                ensure!(
                    fillable_qty + quantum
                        >= crate::strategy::full_set_units_to_qty(owned_qty_units_6dp),
                    "post-proceeds SELL_NOW economics evidence does not fill the owned leg"
                );
            }
        }
        CtfLegEconomicOutcome::HoldForSettlement => ensure!(
            owned_qty_units_6dp > 0
                && action_qty_units_6dp == 0
                && execution_intent_id.is_none()
                && execution_evidence_revision.is_none()
                && close_reason.is_none(),
            "HOLD_FOR_SETTLEMENT owned-leg outcome carries execution action"
        ),
    }
    Ok(())
}

fn serialize_optional_u64_string<S>(
    value: &Option<u64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional_u64_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(<D::Error as serde::de::Error>::custom)
        })
        .transpose()
}

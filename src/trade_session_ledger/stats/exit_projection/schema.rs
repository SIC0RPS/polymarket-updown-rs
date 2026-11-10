use serde::{Deserialize, Serialize};

use crate::trade_session_ledger::owned_leg_outcomes::CanonicalCtfFirstInstructionEconomics;

use super::super::{
    deserialize_i128_string, deserialize_u128_string, deserialize_u64_string,
    serialize_i128_string, serialize_u128_string, serialize_u64_string, CanonicalCohortCashflow,
    CanonicalCtfLegRealization,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalCtfExitClass {
    FirstToPostProceeds,
    FirstToSettlement,
    SettlementOnlyNoCanonicalOracle,
    SettlementOnlyFailedMandatoryExecution,
    Merge,
    BuyAndHold,
    BuyManagedExit,
}

impl CanonicalCtfExitClass {
    pub fn label(self) -> &'static str {
        match self {
            Self::FirstToPostProceeds => "First→Post-proceeds",
            Self::FirstToSettlement => "First→Settlement",
            Self::SettlementOnlyNoCanonicalOracle => "Settlement-only — no canonical oracle",
            Self::SettlementOnlyFailedMandatoryExecution => {
                "Settlement-only — FAILED mandatory execution"
            }
            Self::Merge => "Merge",
            Self::BuyAndHold => "BUY→Settlement",
            Self::BuyManagedExit => "BUY→Exit",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfExitBucket {
    pub cohort_count: u64,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub realized_pnl_units_6dp: i128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfExitSummaryRow {
    pub symbol: String,
    pub first_to_post_proceeds: CanonicalCtfExitBucket,
    pub first_to_settlement: CanonicalCtfExitBucket,
    pub settlement_only_no_canonical_oracle: CanonicalCtfExitBucket,
    pub settlement_only_failed_mandatory_execution: CanonicalCtfExitBucket,
    pub merge: CanonicalCtfExitBucket,
    #[serde(default, skip_serializing_if = "CanonicalCtfExitBucket::is_empty")]
    pub buy_and_hold: CanonicalCtfExitBucket,
    #[serde(default, skip_serializing_if = "CanonicalCtfExitBucket::is_empty")]
    pub buy_managed_exit: CanonicalCtfExitBucket,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub total_realized_pnl_units_6dp: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfExitConfirmedFill {
    pub receipt_id: String,
    pub fill_id: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub quantity_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u64_string"
    )]
    pub matched_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfExitInstructionRow {
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub execution_intent_id: u64,
    pub order_id: Option<String>,
    pub accepted: bool,
    pub decision_reason: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub instruction_quantity_units_6dp: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_economics: Option<CanonicalCtfFirstInstructionEconomics>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u128_string"
    )]
    pub terminal_matched_quantity_units_6dp: Option<u128>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u128_string"
    )]
    pub terminal_unfilled_quantity_units_6dp: Option<u128>,
    pub confirmed_fills: Vec<CanonicalCtfExitConfirmedFill>,
    pub confirmed_realization: CanonicalCtfLegRealization,
    pub evidence_complete: bool,
    pub integrity_valid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfExitCohortRow {
    pub symbol: String,
    pub paired_cohort_key: String,
    pub market_id: String,
    pub market_name: Option<String>,
    pub exit_class: CanonicalCtfExitClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::trade_session_ledger::exact_serde::optional_u128_string"
    )]
    pub buy_principal_units_6dp: Option<u128>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::trade_session_ledger::exact_serde::optional_u128_string"
    )]
    pub buy_created_quantity_units_6dp: Option<u128>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::trade_session_ledger::exact_serde::optional_u128_string"
    )]
    pub buy_collateral_refund_units_6dp: Option<u128>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u64_string"
    )]
    pub buy_first_confirmed_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_realization: Option<CanonicalCtfLegRealization>,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub split_ts_ms: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub market_window_start_ms: Option<Option<u64>>,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub split_principal_units_6dp: u128,
    pub oracle_winner_side: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub first_authorized_at_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub venue_winner_side: Option<Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub venue_resolution_observed_at_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub venue_resolution_provenance: Option<Option<String>>,
    pub first_sell_token: Option<String>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u128_string"
    )]
    pub first_authorized_quantity_units_6dp: Option<u128>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u64_string"
    )]
    pub first_evidence_revision: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub first_confirmed_at_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub first_full_target_confirmed_at_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub first_deadline_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_present_optional_u64",
        deserialize_with = "deserialize_present_optional_u64"
    )]
    pub first_full_target_matched_at_ms: Option<Option<u64>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub first_pre_deadline_status: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub first_instruction_evidence_complete: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub first_instructions: Option<Vec<CanonicalCtfExitInstructionRow>>,
    pub first_submitted_order_count: u64,
    pub first_confirmed_fill_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub first_confirmed_fill_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub first_confirmed_proceeds_units_6dp: i128,
    pub first_realization: Option<CanonicalCtfLegRealization>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub first_terminal_realization: Option<Option<CanonicalCtfLegRealization>>,
    pub sibling_token: Option<String>,
    pub sibling_disposition: Option<String>,
    pub sibling_reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub post_instruction_evidence_complete: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub post_instructions: Option<Vec<CanonicalCtfExitInstructionRow>>,
    pub post_submitted_order_count: u64,
    pub post_confirmed_fill_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub post_confirmed_fill_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub post_confirmed_proceeds_units_6dp: i128,
    pub remaining_realization: Option<CanonicalCtfLegRealization>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present"
    )]
    pub remaining_terminal_realization: Option<Option<CanonicalCtfLegRealization>>,
    pub unassigned_up_realization: Option<CanonicalCtfLegRealization>,
    pub unassigned_down_realization: Option<CanonicalCtfLegRealization>,
    pub terminal_operation: String,
    pub terminal_receipt_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub terminal_up_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub terminal_down_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub terminal_receipt_value_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub confirmed_fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub confirmed_approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_remaining_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_remaining_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub realized_pnl_units_6dp: i128,
}

impl CanonicalCtfExitCohortRow {
    pub(super) fn base(
        symbol: &str,
        paired_cohort_key: &str,
        market_name: Option<&String>,
        cashflow: &CanonicalCohortCashflow,
        exit_class: CanonicalCtfExitClass,
        terminal_operation: String,
        terminal: (u64, u128, u128, i128),
        realized_pnl_units_6dp: i128,
    ) -> Self {
        Self {
            symbol: symbol.to_string(),
            paired_cohort_key: paired_cohort_key.to_string(),
            market_id: cashflow.condition_id.clone(),
            market_name: market_name.cloned(),
            exit_class,
            buy_authorization: cashflow.buy_authorization,
            buy_principal_units_6dp: cashflow
                .buy_authorization
                .map(|_| cashflow.buy_principal_units_6dp),
            buy_created_quantity_units_6dp: cashflow.buy_authorization.map(|authorization| {
                if authorization.selected_up {
                    cashflow.up_created_qty_units_6dp
                } else {
                    cashflow.down_created_qty_units_6dp
                }
            }),
            buy_collateral_refund_units_6dp: cashflow
                .buy_authorization
                .map(|_| cashflow.confirmed_collateral_refund_units_6dp),
            buy_first_confirmed_at_ms: cashflow
                .events
                .iter()
                .filter_map(|event| match &event.event {
                    crate::trade_session_ledger::CanonicalCtfEvent::BuyConfirmed(buy) => {
                        Some(buy.confirmed_at_ms)
                    }
                    _ => None,
                })
                .min(),
            buy_realization: None,
            split_ts_ms: cashflow.split_ts_ms,
            market_window_start_ms: Some(None),
            split_principal_units_6dp: cashflow.split_principal_units_6dp,
            oracle_winner_side: None,
            first_authorized_at_ms: Some(None),
            venue_winner_side: Some(None),
            venue_resolution_observed_at_ms: Some(None),
            venue_resolution_provenance: Some(None),
            first_sell_token: None,
            first_authorized_quantity_units_6dp: None,
            first_evidence_revision: None,
            first_confirmed_at_ms: Some(None),
            first_full_target_confirmed_at_ms: Some(None),
            first_deadline_ms: Some(None),
            first_full_target_matched_at_ms: Some(None),
            first_pre_deadline_status: Some("UNKNOWN".to_string()),
            first_instruction_evidence_complete: Some(true),
            first_instructions: Some(Vec::new()),
            first_submitted_order_count: 0,
            first_confirmed_fill_count: 0,
            first_confirmed_fill_quantity_units_6dp: 0,
            first_confirmed_proceeds_units_6dp: 0,
            first_realization: None,
            first_terminal_realization: Some(None),
            sibling_token: None,
            sibling_disposition: None,
            sibling_reason: None,
            post_instruction_evidence_complete: Some(true),
            post_instructions: Some(Vec::new()),
            post_submitted_order_count: 0,
            post_confirmed_fill_count: 0,
            post_confirmed_fill_quantity_units_6dp: 0,
            post_confirmed_proceeds_units_6dp: 0,
            remaining_realization: None,
            remaining_terminal_realization: Some(None),
            unassigned_up_realization: None,
            unassigned_down_realization: None,
            terminal_operation,
            terminal_receipt_count: terminal.0,
            terminal_up_quantity_units_6dp: terminal.1,
            terminal_down_quantity_units_6dp: terminal.2,
            terminal_receipt_value_units_6dp: terminal.3,
            confirmed_fee_units_6dp: cashflow.confirmed_fee_units_6dp,
            confirmed_approved_direct_cost_units_6dp: cashflow
                .confirmed_approved_direct_cost_units_6dp,
            up_remaining_quantity_units_6dp: cashflow.up_remaining_qty_units_6dp,
            down_remaining_quantity_units_6dp: cashflow.down_remaining_qty_units_6dp,
            realized_pnl_units_6dp,
        }
    }
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

fn serialize_present_optional_u64<S>(
    value: &Option<Option<u64>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    crate::trade_session_ledger::exact_serde::serialize_optional_u64_string(
        &value.flatten(),
        serializer,
    )
}

fn deserialize_present_optional_u64<'de, D>(
    deserializer: D,
) -> Result<Option<Option<u64>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::trade_session_ledger::exact_serde::deserialize_optional_u64_string(deserializer)
        .map(Some)
}

impl CanonicalCtfExitBucket {
    fn is_empty(&self) -> bool {
        self.cohort_count == 0 && self.realized_pnl_units_6dp == 0
    }
}

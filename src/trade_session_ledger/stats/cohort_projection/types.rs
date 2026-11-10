use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfLegRealization {
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub confirmed_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub allocated_principal_units_6dp: u128,
    #[serde(
        default,
        skip_serializing_if = "is_zero_units",
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub allocated_collateral_refund_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub attributable_fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub attributable_approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub realized_pnl_units_6dp: i128,
}

impl CanonicalCtfLegRealization {
    pub(crate) const ZERO: Self = Self {
        confirmed_proceeds_units_6dp: 0,
        allocated_principal_units_6dp: 0,
        allocated_collateral_refund_units_6dp: 0,
        attributable_fee_units_6dp: 0,
        attributable_approved_direct_cost_units_6dp: 0,
        realized_pnl_units_6dp: 0,
    };

    pub(crate) fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            confirmed_proceeds_units_6dp: self
                .confirmed_proceeds_units_6dp
                .checked_add(other.confirmed_proceeds_units_6dp)?,
            allocated_principal_units_6dp: self
                .allocated_principal_units_6dp
                .checked_add(other.allocated_principal_units_6dp)?,
            allocated_collateral_refund_units_6dp: self
                .allocated_collateral_refund_units_6dp
                .checked_add(other.allocated_collateral_refund_units_6dp)?,
            attributable_fee_units_6dp: self
                .attributable_fee_units_6dp
                .checked_add(other.attributable_fee_units_6dp)?,
            attributable_approved_direct_cost_units_6dp: self
                .attributable_approved_direct_cost_units_6dp
                .checked_add(other.attributable_approved_direct_cost_units_6dp)?,
            realized_pnl_units_6dp: self
                .realized_pnl_units_6dp
                .checked_add(other.realized_pnl_units_6dp)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCohortOrderIdentity {
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub ledger_seq: u64,
    pub order_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub intent_id: u64,
    pub asset_id: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub instruction_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub instruction_limit_px_units_8dp: u64,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub accepted_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCohortEvent {
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub ledger_seq: u64,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub ts_ms: u64,
    pub event: CanonicalCtfEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCohortSellReceipt {
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub ledger_seq: u64,
    pub receipt_id: String,
    pub fill_id: String,
    pub receipt_identity: ManagedSellReceiptIdentity,
    pub order_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub intent_id: u64,
    pub token: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub gross_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub confirmed_at_ms: u64,
    pub close_reason: String,
    pub realization: CanonicalCtfLegRealization,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "evidence_kind", rename_all = "snake_case")]
pub enum CanonicalCohortTerminalReceiptEvidence {
    ConfirmedCollateralReceipt {
        transaction_id: String,
        settlement_run_id: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        settlement_ledger_seq: u64,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        settlement_ts_ms: u64,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        finalized_log_index: u64,
        finalized_log_emitter: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        finalized_block_number: u64,
        finalized_block_hash: String,
    },
    SimulatedMergeOperation {
        operation_id: String,
    },
    SimulatedResolutionRedemption {
        operation_id: String,
        condition_id: String,
        up_token: String,
        down_token: String,
        winning_token: String,
        #[serde(
            serialize_with = "serialize_u64_string",
            deserialize_with = "deserialize_u64_string"
        )]
        resolution_observed_at_ms: u64,
        raw_response_sha256: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCohortTerminalReceipt {
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub accounting_ledger_seq: u64,
    pub receipt_id: String,
    pub operation: CtfTerminalOperation,
    pub evidence: CanonicalCohortTerminalReceiptEvidence,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_terminal_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_terminal_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub collateral_receipt_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub approved_direct_cost_units_6dp: u128,
    pub up_realization: Option<CanonicalCtfLegRealization>,
    pub down_realization: Option<CanonicalCtfLegRealization>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCohortCashflow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    #[serde(
        default,
        skip_serializing_if = "is_zero_units",
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub buy_principal_units_6dp: u128,
    #[serde(
        default,
        skip_serializing_if = "is_zero_units",
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub confirmed_collateral_refund_units_6dp: u128,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::trade_session_ledger::exact_serde::optional_u128_string"
    )]
    pub up_remaining_basis_units_6dp: Option<u128>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::trade_session_ledger::exact_serde::optional_u128_string"
    )]
    pub down_remaining_basis_units_6dp: Option<u128>,
    pub run_id: String,
    pub split_receipt_id: String,
    pub split_transaction_id: String,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub split_ledger_seq: u64,
    #[serde(
        serialize_with = "serialize_u64_string",
        deserialize_with = "deserialize_u64_string"
    )]
    pub split_ts_ms: u64,
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_created_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_created_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_confirmed_sold_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_confirmed_sold_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_confirmed_merged_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_confirmed_merged_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_confirmed_redeemed_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_confirmed_redeemed_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_confirmed_disposed_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_confirmed_disposed_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub up_remaining_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub down_remaining_qty_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub split_principal_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub split_fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub split_approved_direct_cost_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub confirmed_sell_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub confirmed_terminal_receipts_units_6dp: i128,
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
    pub events: Vec<CanonicalCohortEvent>,
    pub order_submissions: Vec<CanonicalCohortOrderIdentity>,
    pub managed_sell_receipts: Vec<CanonicalCohortSellReceipt>,
    pub terminal_receipts: Vec<CanonicalCohortTerminalReceipt>,
}

impl CanonicalCohortCashflow {
    pub(crate) fn entry_principal_units_6dp(&self) -> Option<u128> {
        self.split_principal_units_6dp
            .checked_add(self.buy_principal_units_6dp)
    }

    pub(crate) fn realization_for_token(&self, token: &str) -> Option<CanonicalCtfLegRealization> {
        let sells = self
            .managed_sell_receipts
            .iter()
            .filter(|receipt| receipt.token == token)
            .map(|receipt| receipt.realization);
        let terminals = self.terminal_receipts.iter().filter_map(|receipt| {
            if token == self.up_token {
                receipt.up_realization
            } else if token == self.down_token {
                receipt.down_realization
            } else {
                None
            }
        });
        sells
            .chain(terminals)
            .try_fold(CanonicalCtfLegRealization::ZERO, |sum, item| {
                sum.checked_add(item)
            })
    }

    pub(crate) fn total_realization(&self) -> Option<CanonicalCtfLegRealization> {
        self.realization_for_token(&self.up_token)?
            .checked_add(self.realization_for_token(&self.down_token)?)
    }

    pub(crate) fn unrecovered_cash_units_6dp(&self) -> Option<i128> {
        let principal = i128::try_from(self.entry_principal_units_6dp()?).ok()?;
        let refunds = i128::try_from(self.confirmed_collateral_refund_units_6dp).ok()?;
        let fees = i128::try_from(self.confirmed_fee_units_6dp).ok()?;
        let direct_costs = i128::try_from(self.confirmed_approved_direct_cost_units_6dp).ok()?;
        principal
            .checked_add(fees)?
            .checked_add(direct_costs)?
            .checked_sub(refunds)?
            .checked_sub(self.confirmed_sell_proceeds_units_6dp)?
            .checked_sub(self.confirmed_terminal_receipts_units_6dp)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalClosedCohortStats {
    pub symbol: String,
    pub market_id: Option<MarketId>,
    pub market_name: Option<String>,
    pub trade_id: String,
    pub paired_cohort_key: String,
    pub authority_source: Option<String>,
    pub owner: Option<String>,
    pub durability_state: Option<String>,
    pub close_hint: Option<String>,
    pub reconciliation_status: Option<String>,
    pub started_at_ms: u64,
    pub last_ts_ms: u64,
    pub event_count: usize,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub closed_outcome_share_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub realized_pnl_units_6dp: i128,
    pub cashflow: CanonicalCohortCashflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum CanonicalUnresolvedCohortState {
    ConfirmedReceiptQuantityIncomplete { cashflow: CanonicalCohortCashflow },
    InvalidConfirmedReceiptSet { detail: String },
}

impl CanonicalUnresolvedCohortState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ConfirmedReceiptQuantityIncomplete { .. } => {
                "confirmed_receipt_quantity_incomplete"
            }
            Self::InvalidConfirmedReceiptSet { .. } => "invalid_confirmed_receipt_set",
        }
    }

    pub fn detail(&self) -> Option<&str> {
        match self {
            Self::ConfirmedReceiptQuantityIncomplete { .. } => None,
            Self::InvalidConfirmedReceiptSet { detail, .. } => Some(detail),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalUnresolvedCohortStats {
    pub symbol: String,
    pub market_id: MarketId,
    pub market_name: Option<String>,
    pub paired_cohort_key: String,
    pub started_at_ms: u64,
    pub last_ts_ms: u64,
    pub event_count: usize,
    pub state: CanonicalUnresolvedCohortState,
}

fn is_zero_units(value: &u128) -> bool {
    *value == 0
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::trade_session_ledger) struct CohortEntryIdentity {
    pub(in crate::trade_session_ledger) market_id: MarketId,
    pub(in crate::trade_session_ledger) condition_id: String,
    pub(in crate::trade_session_ledger) cohort_id: String,
    pub(in crate::trade_session_ledger) underlying: String,
    pub(in crate::trade_session_ledger) market_name: Option<String>,
    pub(in crate::trade_session_ledger) up_token: String,
    pub(in crate::trade_session_ledger) down_token: String,
    pub(in crate::trade_session_ledger) buy_authorization:
        Option<crate::types::DirectBuyAuthorization>,
}

impl CohortEntryIdentity {
    pub(in crate::trade_session_ledger) fn from_split(split: &ConfirmedCtfSplitFact) -> Self {
        Self {
            market_id: split.market_id.clone(),
            condition_id: split.condition_id.clone(),
            cohort_id: split.cohort_id.clone(),
            underlying: split.underlying.clone(),
            market_name: split.market_name.clone(),
            up_token: split.up_token.clone(),
            down_token: split.down_token.clone(),
            buy_authorization: None,
        }
    }

    pub(in crate::trade_session_ledger) fn from_event(
        event: &CanonicalCtfEvent,
    ) -> anyhow::Result<Self> {
        match event {
            CanonicalCtfEvent::SplitConfirmed(split) => Ok(Self::from_split(split)),
            CanonicalCtfEvent::BuyConfirmed(buy) => Self::from_buy_authorization(buy.authorization),
            _ => anyhow::bail!("cohort has no confirmed acquisition"),
        }
    }

    pub(in crate::trade_session_ledger) fn from_buy_authorization(
        authorization: crate::types::DirectBuyAuthorization,
    ) -> anyhow::Result<Self> {
        authorization.validate()?;
        Ok(Self {
            market_id: MarketId::new(authorization.condition_id.to_string()),
            condition_id: authorization.condition_id.to_string(),
            cohort_id: crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                authorization.condition_id,
                authorization.intent_id,
            )?,
            underlying: authorization.symbol.as_str().to_string(),
            market_name: None,
            up_token: authorization.up_token.to_string(),
            down_token: authorization.down_token.to_string(),
            buy_authorization: Some(authorization),
        })
    }

    pub(in crate::trade_session_ledger) fn validate_buy(
        &self,
        buy: &crate::trade_session_ledger::ConfirmedCtfBuyFact,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.buy_authorization == Some(buy.authorization)
                && self.market_id == buy.market_id
                && self.condition_id == buy.condition_id
                && self.cohort_id == buy.cohort_id
                && buy.token == buy.authorization.selected_token().to_string(),
            "BUY receipt differs from original cohort acquisition"
        );
        Ok(())
    }
}

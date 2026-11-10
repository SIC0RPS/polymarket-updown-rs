use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::types::{OrderCloseReason, Side};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OrderTerminalProvenance {
    PreSubmissionAbandoned,
    SubmissionRejected,
    SubmissionUnmatched,
    OrderUpdate,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct OrderTerminalFact {
    pub execution_intent_id: u64,
    pub order_id: Option<String>,
    pub market: String,
    pub cohort: String,
    pub symbol: String,
    pub token: String,
    pub side: Side,
    pub close_reason: OrderCloseReason,
    pub provenance: OrderTerminalProvenance,
    pub instruction_qty_units_6dp: u128,
    pub matched_qty_units_6dp: u128,
    pub unfilled_qty_units_6dp: u128,
    pub raw_status: Option<String>,
    pub raw_message_type: Option<String>,
    pub raw_reason: Option<String>,
    pub raw_reason_provenance: Option<String>,
    pub normalized_reject_kind: Option<String>,
    pub observed_at_ms: u64,
}

impl OrderTerminalFact {
    pub(crate) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.execution_intent_id > 0,
            "order terminal fact execution intent identity is zero"
        );
        anyhow::ensure!(
            self.order_id
                .as_deref()
                .is_none_or(|order_id| !order_id.trim().is_empty()),
            "order terminal fact order identity is empty"
        );
        anyhow::ensure!(
            !self.market.trim().is_empty()
                && !self.cohort.trim().is_empty()
                && !self.symbol.trim().is_empty()
                && !self.token.trim().is_empty(),
            "order terminal fact scope identity is incomplete"
        );
        anyhow::ensure!(
            self.instruction_qty_units_6dp > 0
                && self
                    .matched_qty_units_6dp
                    .checked_add(self.unfilled_qty_units_6dp)
                    == Some(self.instruction_qty_units_6dp),
            "order terminal fact quantity conservation failed"
        );
        anyhow::ensure!(
            self.raw_status
                .as_deref()
                .or(self.raw_message_type.as_deref())
                .or(self.raw_reason.as_deref())
                .or(self.normalized_reject_kind.as_deref())
                .is_some_and(|value| !value.trim().is_empty()),
            "order terminal fact has no raw terminal value"
        );
        anyhow::ensure!(
            self.raw_reason.is_some() == self.raw_reason_provenance.is_some(),
            "order terminal fact raw reason provenance is incomplete"
        );
        anyhow::ensure!(
            self.observed_at_ms > 0,
            "order terminal fact observation time is zero"
        );
        match self.provenance {
            OrderTerminalProvenance::PreSubmissionAbandoned => anyhow::ensure!(
                self.order_id.is_none(),
                "pre-submission terminal fact has an order identity"
            ),
            OrderTerminalProvenance::SubmissionRejected => {}
            OrderTerminalProvenance::SubmissionUnmatched | OrderTerminalProvenance::OrderUpdate => {
                anyhow::ensure!(
                    self.order_id.is_some(),
                    "accepted terminal order fact has no order identity"
                )
            }
        }
        Ok(())
    }

    pub(crate) fn semantically_matches(&self, other: &Self) -> bool {
        self.execution_intent_id == other.execution_intent_id
            && self.order_id == other.order_id
            && self.market == other.market
            && self.cohort == other.cohort
            && self.symbol == other.symbol
            && self.token == other.token
            && self.side == other.side
            && self.close_reason == other.close_reason
            && self.provenance == other.provenance
            && self.instruction_qty_units_6dp == other.instruction_qty_units_6dp
            && self.matched_qty_units_6dp == other.matched_qty_units_6dp
            && self.unfilled_qty_units_6dp == other.unfilled_qty_units_6dp
            && normalized_optional_eq(
                self.normalized_reject_kind.as_deref(),
                other.normalized_reject_kind.as_deref(),
            )
    }
}

fn normalized_optional_eq(left: Option<&str>, right: Option<&str>) -> bool {
    match (left.map(str::trim), right.map(str::trim)) {
        (None, None) => true,
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

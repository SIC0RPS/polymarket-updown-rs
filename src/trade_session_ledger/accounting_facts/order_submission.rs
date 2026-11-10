use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::polymarket::identity::MarketId;
use crate::types::TimeInForce;

use super::nonempty;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedOrderSubmissionFact {
    pub order_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
    )]
    pub intent_id: u64,
    pub market_id: MarketId,
    pub cohort_id: String,
    pub underlying: String,
    pub asset_id: String,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub instruction_qty_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
    )]
    pub instruction_limit_px_units_8dp: u64,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u64_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u64_string"
    )]
    pub accepted_at_ms: u64,
}

impl ConfirmedOrderSubmissionFact {
    pub fn validate(&self) -> Result<()> {
        nonempty(&self.order_id, "submitted order identity")?;
        nonempty(self.market_id.as_str(), "submitted order market identity")?;
        nonempty(&self.cohort_id, "submitted order cohort identity")?;
        ensure!(
            self.intent_id > 0,
            "submitted order intent identity is zero"
        );
        ensure!(
            matches!(self.underlying.as_str(), "BTC" | "ETH" | "SOL" | "XRP"),
            "submitted order underlying is not a canonical symbol"
        );
        nonempty(&self.asset_id, "submitted order asset identity")?;
        ensure!(
            self.instruction_qty_units_6dp > 0,
            "submitted order instruction quantity is zero"
        );
        ensure!(
            self.instruction_limit_px_units_8dp > 0,
            "submitted order instruction limit price is zero"
        );
        if let Some(authorization) = self.buy_authorization.as_ref() {
            authorization.validate()?;
            ensure!(
                self.intent_id == authorization.intent_id
                    && self.market_id.as_str() == authorization.condition_id.to_string()
                    && self.cohort_id
                        == crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                            authorization.condition_id,
                            authorization.intent_id,
                        )?
                    && self.underlying == authorization.symbol.as_str()
                    && self.asset_id == authorization.selected_token().to_string()
                    && self.instruction_qty_units_6dp <= authorization.target_qty_units_6dp
                    && self.instruction_limit_px_units_8dp <= 100_000_000
                    && self.accepted_at_ms >= authorization.authorized_at_ms,
                "submitted BUY differs from immutable authorization"
            );
            ensure!(
                matches!(
                    &self.time_in_force,
                    TimeInForce::Gtc | TimeInForce::Gtd { .. }
                ) && !self.post_only,
                "submitted BUY is not a resting non-post-only limit order"
            );
            if let TimeInForce::Gtd { expiration_ms } = self.time_in_force {
                ensure!(
                    expiration_ms == authorization.market_end_ms,
                    "submitted BUY expiry differs from authorized market end"
                );
            }
        } else {
            ensure!(
                matches!(&self.time_in_force, TimeInForce::Fak) && !self.post_only,
                "submitted managed sell is not FAK/non-post-only"
            );
        }
        ensure!(
            self.accepted_at_ms > 0,
            "submitted order acceptance time is zero"
        );
        Ok(())
    }
}

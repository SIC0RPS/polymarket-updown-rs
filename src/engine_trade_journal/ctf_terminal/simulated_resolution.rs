use anyhow::{Context, Result};

use super::{terminal_record, ConfirmedCtfTerminalClearance};
use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCtfTerminalFact, CtfTerminalOperation,
    CtfTerminalSettlementEvidence,
};

impl ConfirmedCtfTerminalClearance {
    #[allow(clippy::too_many_arguments)]
    pub fn simulated_resolution_redeem(
        market_id: MarketId,
        condition_id: String,
        cohort_id: String,
        up_token: String,
        down_token: String,
        up_qty_units_6dp: u128,
        down_qty_units_6dp: u128,
        confirmed_at_ms: u64,
    ) -> Self {
        Self {
            market_id,
            condition_id,
            cohort_id,
            up_token,
            down_token,
            up_qty_units_6dp,
            down_qty_units_6dp,
            confirmed_at_ms,
            up_outcome_index: None,
            down_outcome_index: None,
            payout: None,
        }
    }

    pub fn build_simulated_resolution_redemption_record(
        &self,
        record_run_id: &str,
        operation_id: &str,
        winning_token: &str,
        resolution_observed_at_ms: u64,
        raw_response_sha256: &str,
    ) -> Result<EngineTradeJournalRecord> {
        anyhow::ensure!(
            !record_run_id.trim().is_empty() && record_run_id.trim() == record_run_id,
            "simulated redemption record run identity is missing or noncanonical"
        );
        anyhow::ensure!(
            operation_id.starts_with("sim-ctf-redeem-")
                && (self.up_qty_units_6dp > 0 || self.down_qty_units_6dp > 0)
                && self.up_outcome_index.is_none()
                && self.down_outcome_index.is_none()
                && self.payout.is_none()
                && self.confirmed_at_ms >= resolution_observed_at_ms
                && (winning_token == self.up_token || winning_token == self.down_token),
            "simulated redemption operation and terminal clearance differ"
        );
        let up_proceeds = if winning_token == self.up_token {
            self.up_qty_units_6dp
        } else {
            0
        };
        let down_proceeds = if winning_token == self.down_token {
            self.down_qty_units_6dp
        } else {
            0
        };
        let collateral_proceeds = up_proceeds
            .checked_add(down_proceeds)
            .context("simulated redemption proceeds overflow")?;
        let collateral_proceeds_units_6dp = i128::try_from(collateral_proceeds)
            .context("simulated redemption proceeds exceed signed accounting range")?;
        let fact = CanonicalCtfEvent::RedeemConfirmed(ConfirmedCtfTerminalFact {
            settlement_evidence: CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
                operation_id: operation_id.to_string(),
                condition_id: self.condition_id.clone(),
                up_token: self.up_token.clone(),
                down_token: self.down_token.clone(),
                winning_token: winning_token.to_string(),
                resolution_observed_at_ms,
                raw_response_sha256: raw_response_sha256.to_string(),
            },
            market_id: self.market_id.clone(),
            condition_id: self.condition_id.clone(),
            cohort_id: self.cohort_id.clone(),
            up_token: self.up_token.clone(),
            down_token: self.down_token.clone(),
            up_terminal_qty_units_6dp: self.up_qty_units_6dp,
            down_terminal_qty_units_6dp: self.down_qty_units_6dp,
            up_outcome_index: None,
            down_outcome_index: None,
            up_collateral_proceeds_units_6dp: Some(up_proceeds),
            down_collateral_proceeds_units_6dp: Some(down_proceeds),
            collateral_proceeds_units_6dp,
            fee_units_6dp: 0,
            approved_direct_cost_units_6dp: 0,
        });
        fact.validate()?;
        Ok(terminal_record(
            record_run_id,
            self,
            fact,
            CtfTerminalOperation::Redeem,
            operation_id,
            "simulated_resolution_redemption",
        ))
    }
}

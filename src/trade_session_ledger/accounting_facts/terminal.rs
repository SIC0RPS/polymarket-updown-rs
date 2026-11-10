use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::polymarket::identity::MarketId;

use super::{nonempty, validate_common, validate_tokens, CtfTerminalOperation};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "settlement_evidence_kind", rename_all = "snake_case")]
pub enum CtfTerminalSettlementEvidence {
    ConfirmedCollateralReceipt {
        receipt_id: String,
        transaction_id: String,
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
        #[serde(with = "crate::trade_session_ledger::exact_serde::u64_string")]
        resolution_observed_at_ms: u64,
        raw_response_sha256: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConfirmedCtfTerminalFact {
    pub settlement_evidence: CtfTerminalSettlementEvidence,
    pub market_id: MarketId,
    pub condition_id: String,
    pub cohort_id: String,
    pub up_token: String,
    pub down_token: String,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub up_terminal_qty_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub down_terminal_qty_units_6dp: u128,
    pub up_outcome_index: Option<u8>,
    pub down_outcome_index: Option<u8>,
    #[serde(
        default,
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u128_string"
    )]
    pub up_collateral_proceeds_units_6dp: Option<u128>,
    #[serde(
        default,
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_optional_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_optional_u128_string"
    )]
    pub down_collateral_proceeds_units_6dp: Option<u128>,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_i128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_i128_string"
    )]
    pub collateral_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub fee_units_6dp: u128,
    #[serde(
        serialize_with = "crate::trade_session_ledger::exact_serde::serialize_u128_string",
        deserialize_with = "crate::trade_session_ledger::exact_serde::deserialize_u128_string"
    )]
    pub approved_direct_cost_units_6dp: u128,
}

impl ConfirmedCtfTerminalFact {
    pub fn settlement_id(&self) -> String {
        match &self.settlement_evidence {
            CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt { receipt_id, .. } => {
                receipt_id.clone()
            }
            CtfTerminalSettlementEvidence::SimulatedMergeOperation { operation_id } => {
                operation_id.clone()
            }
            CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
                operation_id, ..
            } => operation_id.clone(),
        }
    }

    pub fn confirmed_receipt_identity(&self) -> Option<(&str, &str)> {
        match &self.settlement_evidence {
            CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt {
                receipt_id,
                transaction_id,
            } => Some((receipt_id, transaction_id)),
            CtfTerminalSettlementEvidence::SimulatedMergeOperation { .. }
            | CtfTerminalSettlementEvidence::SimulatedResolutionRedemption { .. } => None,
        }
    }

    pub fn terminal_operation_identity(&self) -> &str {
        match &self.settlement_evidence {
            CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt {
                transaction_id, ..
            } => transaction_id,
            CtfTerminalSettlementEvidence::SimulatedMergeOperation { operation_id } => operation_id,
            CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
                operation_id, ..
            } => operation_id,
        }
    }

    pub(super) fn validate_for(&self, expected_operation: CtfTerminalOperation) -> Result<()> {
        validate_common(&self.settlement_id(), &self.condition_id, &self.cohort_id)?;
        nonempty(self.market_id.as_str(), "terminal market identity")?;
        validate_tokens(&self.up_token, &self.down_token)?;
        ensure!(
            self.market_id.as_str() == self.condition_id,
            "terminal market identity does not equal condition identity"
        );
        ensure!(
            crate::ctf_inventory::is_confirmed_ctf_entry_cohort_id(&self.cohort_id)
                && (expected_operation != CtfTerminalOperation::Merge
                    || crate::ctf_inventory::is_confirmed_ctf_split_cohort_id(&self.cohort_id)),
            "terminal cohort identity is not valid for its acquisition operation"
        );
        ensure!(
            self.up_terminal_qty_units_6dp > 0 || self.down_terminal_qty_units_6dp > 0,
            "terminal settlement has no confirmed token quantity"
        );
        ensure!(
            self.collateral_proceeds_units_6dp >= 0,
            "terminal collateral proceeds are negative"
        );
        match (&self.settlement_evidence, expected_operation) {
            (
                CtfTerminalSettlementEvidence::SimulatedMergeOperation { operation_id },
                CtfTerminalOperation::Merge,
            ) => {
                ensure!(
                    operation_id.starts_with("sim-ctf-merge-")
                        && operation_id["sim-ctf-merge-".len()..]
                            .parse::<u64>()
                            .is_ok_and(|sequence| sequence > 0),
                    "simulated merge operation identity is invalid"
                );
                self.validate_merge_proceeds()?;
            }
            (CtfTerminalSettlementEvidence::SimulatedMergeOperation { .. }, _) => {
                anyhow::bail!("simulated merge evidence labels a non-merge terminal operation")
            }
            (
                CtfTerminalSettlementEvidence::SimulatedResolutionRedemption {
                    operation_id,
                    condition_id,
                    up_token,
                    down_token,
                    winning_token,
                    resolution_observed_at_ms,
                    raw_response_sha256,
                },
                CtfTerminalOperation::Redeem,
            ) => {
                ensure!(
                    operation_id.starts_with("sim-ctf-redeem-")
                        && operation_id["sim-ctf-redeem-".len()..]
                            .parse::<u64>()
                            .is_ok_and(|sequence| sequence > 0),
                    "simulated redemption operation identity is invalid"
                );
                ensure!(
                    condition_id == &self.condition_id
                        && up_token == &self.up_token
                        && down_token == &self.down_token
                        && (winning_token == up_token || winning_token == down_token)
                        && *resolution_observed_at_ms > 0,
                    "simulated redemption resolution identity is foreign or incomplete"
                );
                ensure!(
                    raw_response_sha256.len() == 64
                        && raw_response_sha256
                            .bytes()
                            .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) }),
                    "simulated redemption resolution digest is not canonical lowercase SHA-256"
                );
                ensure!(
                    self.up_outcome_index.is_none() && self.down_outcome_index.is_none(),
                    "simulated resolution redemption carries unavailable outcome-slot identity"
                );
                self.validate_redeem_proceeds()?;
                let expected_up = if winning_token == up_token {
                    self.up_terminal_qty_units_6dp
                } else {
                    0
                };
                let expected_down = if winning_token == down_token {
                    self.down_terminal_qty_units_6dp
                } else {
                    0
                };
                ensure!(
                    self.up_collateral_proceeds_units_6dp == Some(expected_up)
                        && self.down_collateral_proceeds_units_6dp == Some(expected_down),
                    "simulated redemption proceeds do not equal captured binary winner payout"
                );
            }
            (CtfTerminalSettlementEvidence::SimulatedResolutionRedemption { .. }, _) => {
                anyhow::bail!(
                    "simulated resolution redemption evidence labels a non-redeem terminal operation"
                )
            }
            (
                CtfTerminalSettlementEvidence::ConfirmedCollateralReceipt {
                    receipt_id,
                    transaction_id,
                },
                operation,
            ) => {
                nonempty(transaction_id, "terminal transaction identity")?;
                ensure!(
                    receipt_id == &format!("settlement_collateral:{transaction_id}"),
                    "terminal receipt identity does not match transaction identity"
                );
                if operation == CtfTerminalOperation::Merge {
                    self.validate_merge_proceeds()?;
                } else {
                    ensure!(
                        self.up_outcome_index.is_some() == (self.up_terminal_qty_units_6dp > 0)
                            && self.down_outcome_index.is_some()
                                == (self.down_terminal_qty_units_6dp > 0)
                            && (self.up_terminal_qty_units_6dp == 0
                                || self.up_outcome_index != self.down_outcome_index),
                        "redeem outcome indices do not match terminal quantities"
                    );
                    self.validate_redeem_proceeds()?;
                }
            }
        }
        Ok(())
    }

    fn validate_merge_proceeds(&self) -> Result<()> {
        ensure!(
            self.up_outcome_index.is_none()
                && self.down_outcome_index.is_none()
                && self.up_collateral_proceeds_units_6dp.is_none()
                && self.down_collateral_proceeds_units_6dp.is_none(),
            "merge terminal fact carries redeem attribution"
        );
        let proceeds = u128::try_from(self.collateral_proceeds_units_6dp).map_err(|_| {
            anyhow::anyhow!("merge collateral proceeds exceed exact unsigned range")
        })?;
        ensure!(
            self.up_terminal_qty_units_6dp == self.down_terminal_qty_units_6dp
                && self.up_terminal_qty_units_6dp == proceeds,
            "merge terminal quantities do not equal collateral proceeds"
        );
        Ok(())
    }

    fn validate_redeem_proceeds(&self) -> Result<()> {
        let (Some(up), Some(down)) = (
            self.up_collateral_proceeds_units_6dp,
            self.down_collateral_proceeds_units_6dp,
        ) else {
            anyhow::bail!("redeem terminal fact lacks per-token proceeds");
        };
        ensure!(
            up.checked_add(down)
                .and_then(|value| i128::try_from(value).ok())
                == Some(self.collateral_proceeds_units_6dp)
                && (self.up_terminal_qty_units_6dp > 0 || up == 0)
                && (self.down_terminal_qty_units_6dp > 0 || down == 0),
            "redeem per-token proceeds do not reconcile"
        );
        Ok(())
    }
}

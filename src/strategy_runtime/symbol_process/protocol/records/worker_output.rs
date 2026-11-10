use anyhow::{Context, Result};

use super::{
    catalog_market, ensure_inventory_identity, ensure_market_token, CohortIdentityRecord,
    MarketCatalog,
};
use crate::strategy::facts::{CtfLegDecisionReason, CtfLegEconomicOutcome, CtfOwnedLegCloseReason};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IntentRecord {
    pub intent_id: u64,
    pub action_kind: u8,
    pub side_or_operation: u8,
    pub time_in_force: u8,
    pub close_reason: u8,
    pub flags: u16,
    pub quote_level: u8,
    pub market_id: u32,
    pub condition_id: u32,
    pub token_or_up_id: u32,
    pub down_token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub qty_or_amount_units_6dp: u128,
    pub limit_px_units_8dp: u64,
    pub execution_evidence_revision: u64,
    pub expiration_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OwnerAuditRecord {
    pub audit_seq: u64,
    pub source_sequence: u64,
    pub central_state_seq: u64,
    pub ts_ms: u64,
    /// Local intent ordinal at encode time; remapped to worker local intent_id
    /// then rebound to central execution intent id for SELL_NOW.
    pub execution_intent_id: u64,
    pub market_id: u32,
    pub token_id: u32,
    pub cohort_id: CohortIdentityRecord,
    pub owned_qty_units_6dp: u128,
    pub action_qty_units_6dp: u128,
    pub execution_evidence_revision: u64,
    pub sell_full_entry_pnl_bits: u64,
    pub hold_full_entry_pnl_bits: u64,
    pub fillable_qty_bits: u64,
    pub first_instruction_limit_px_bits: u64,
    pub first_depth_gross_proceeds_bits: u64,
    pub first_depth_fee_bits: u64,
    pub first_depth_net_proceeds_bits: u64,
    pub first_authorization_fee_bits: u64,
    pub first_authorization_net_proceeds_bits: u64,
    pub first_delay_cushion_bits: u64,
    pub first_allocated_basis_units_6dp: u128,
    pub first_upper_valuation_bits: u64,
    pub first_reservation_bits: u64,
    pub first_mandatory_urgency: bool,
    pub outcome: u8,
    pub close_reason: u8,
    pub causal_winner_side: u8,
    pub reason: u16,
    pub flags: u16,
}

pub(crate) const OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG: u16 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OracleResolvedLabelRecord {
    pub market_id: u32,
    pub winner: u8,
    pub flags: u8,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub release_ts_ms: u64,
    pub open_price_e18: i128,
    pub close_price_e18: i128,
    pub open_source_ts_ms: u64,
    pub open_observed_ts_ms: u64,
    pub open_capture_sequence: u64,
    pub close_source_ts_ms: u64,
    pub close_observed_ts_ms: u64,
    pub close_capture_sequence: u64,
}

impl OracleResolvedLabelRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.market_id != 0, "oracle label market identity is zero");
        anyhow::ensure!(matches!(self.winner, 1 | 2), "invalid oracle label winner");
        anyhow::ensure!(self.flags == 0, "oracle label contains unknown flags");
        anyhow::ensure!(
            self.window_start_ms > 0 && self.window_start_ms < self.window_end_ms,
            "invalid oracle label market window"
        );
        anyhow::ensure!(
            self.open_price_e18 > 0 && self.close_price_e18 > 0,
            "oracle label contains invalid boundary price"
        );
        validate_oracle_boundary(
            "open",
            self.window_start_ms,
            self.open_source_ts_ms,
            self.open_observed_ts_ms,
            self.open_capture_sequence,
        )?;
        validate_oracle_boundary(
            "close",
            self.window_end_ms,
            self.close_source_ts_ms,
            self.close_observed_ts_ms,
            self.close_capture_sequence,
        )?;
        anyhow::ensure!(
            self.release_ts_ms == self.open_observed_ts_ms.max(self.close_observed_ts_ms),
            "oracle label release timestamp mismatch"
        );
        anyhow::ensure!(
            self.winner
                == if self.close_price_e18 >= self.open_price_e18 {
                    1
                } else {
                    2
                },
            "oracle label winner conflicts with boundary prices"
        );
        Ok(self)
    }

    pub fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog
            .record(self.market_id)
            .context("oracle label market handle is absent from frozen catalog")?;
        anyhow::ensure!(
            self.window_start_ms == market.window_start_ms
                && self.window_end_ms == market.window_end_ms,
            "oracle label window conflicts with frozen catalog"
        );
        Ok(self)
    }
}

fn validate_oracle_boundary(
    name: &str,
    boundary_ts_ms: u64,
    source_ts_ms: u64,
    observed_ts_ms: u64,
    capture_sequence: u64,
) -> Result<()> {
    anyhow::ensure!(
        source_ts_ms == boundary_ts_ms,
        "invalid oracle {name} boundary timestamp"
    );
    anyhow::ensure!(
        observed_ts_ms > 0,
        "oracle {name} observation timestamp is zero"
    );
    anyhow::ensure!(
        capture_sequence > 0,
        "invalid oracle {name} capture sequence"
    );
    Ok(())
}

impl OwnerAuditRecord {
    pub(crate) const FIRST_INSTRUCTION_ECONOMICS_FLAG: u16 = 1 << 1;
    pub(crate) const FIRST_UPPER_VALUATION_FLAG: u16 = 1 << 2;

    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.audit_seq != 0, "owner audit identity is zero");
        anyhow::ensure!(
            self.source_sequence != 0,
            "owner audit source sequence is zero"
        );
        anyhow::ensure!(
            self.central_state_seq != 0,
            "owner audit central-state sequence is zero"
        );
        anyhow::ensure!(self.ts_ms != 0, "owner audit timestamp is zero");
        anyhow::ensure!(self.market_id != 0, "owner audit market identity is zero");
        anyhow::ensure!(self.token_id != 0, "owner audit token identity is zero");
        self.cohort_id.required_str()?;
        let outcome = CtfLegEconomicOutcome::try_from(self.outcome)
            .map_err(|_| anyhow::anyhow!("invalid owner audit outcome"))?;
        let close_reason = match self.close_reason {
            0 => None,
            value => Some(
                CtfOwnedLegCloseReason::try_from(value)
                    .map_err(|_| anyhow::anyhow!("invalid owner audit close reason"))?,
            ),
        };
        anyhow::ensure!(
            matches!(self.causal_winner_side, 0..=2),
            "invalid owner audit causal winner side"
        );
        let reason = CtfLegDecisionReason::try_from(self.reason)
            .map_err(|_| anyhow::anyhow!("invalid owner audit reason"))?;
        anyhow::ensure!(
            reason.permits_outcome(outcome),
            "owner audit reason is invalid for outcome"
        );
        let bounded_action = self.owned_qty_units_6dp > 0
            && self.action_qty_units_6dp > 0
            && self.action_qty_units_6dp <= self.owned_qty_units_6dp;
        let blocked_sell = self.owned_qty_units_6dp > 0
            && self.action_qty_units_6dp == 0
            && self.execution_evidence_revision == 0
            && self.execution_intent_id == 0
            && close_reason.is_some();
        let no_action = self.action_qty_units_6dp == 0 && self.close_reason == 0;
        match outcome {
            CtfLegEconomicOutcome::SellNow => anyhow::ensure!(
                blocked_sell
                    || (bounded_action
                        && self.execution_evidence_revision > 0
                        && close_reason.is_some()
                        && self.execution_intent_id > 0),
                "SELL_NOW owner audit requires owned inventory, managed-sell close reason, and either blocked or executable action fields"
            ),
            CtfLegEconomicOutcome::HoldForSettlement => anyhow::ensure!(
                self.owned_qty_units_6dp > 0 && no_action && self.execution_intent_id == 0,
                "HOLD_FOR_SETTLEMENT owner audit requires owned inventory and no execution action"
            ),
        }
        anyhow::ensure!(
            outcome == CtfLegEconomicOutcome::SellNow || self.execution_evidence_revision == 0,
            "non-SELL owner audit carries execution evidence revision"
        );
        anyhow::ensure!(
            self.flags
                & !(OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG
                    | Self::FIRST_INSTRUCTION_ECONOMICS_FLAG
                    | Self::FIRST_UPPER_VALUATION_FLAG)
                == 0,
            "owner audit contains unknown flags"
        );
        let has_post_sell_economics = self.flags & OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG != 0;
        anyhow::ensure!(
            !has_post_sell_economics
                || close_reason == Some(CtfOwnedLegCloseReason::PairedPostProceedsSell),
            "post-sell owner audit economics requires post-proceeds SELL_NOW"
        );
        anyhow::ensure!(
            close_reason != Some(CtfOwnedLegCloseReason::PairedPostProceedsSell)
                || self.action_qty_units_6dp == 0
                || has_post_sell_economics,
            "executable post-proceeds owner audit requires economics evidence"
        );
        let sell_pnl = f64::from_bits(self.sell_full_entry_pnl_bits);
        let hold_pnl = f64::from_bits(self.hold_full_entry_pnl_bits);
        let fillable_qty = f64::from_bits(self.fillable_qty_bits);
        let valid_post_economics = sell_pnl.is_finite()
            && hold_pnl.is_finite()
            && fillable_qty.is_finite()
            && fillable_qty > 0.0;
        let empty_post_economics = self.sell_full_entry_pnl_bits == 0
            && self.hold_full_entry_pnl_bits == 0
            && self.fillable_qty_bits == 0;
        anyhow::ensure!(
            if has_post_sell_economics {
                valid_post_economics
            } else {
                empty_post_economics
            },
            "owner audit carries invalid post-sell economics evidence"
        );
        Ok(self)
    }

    pub fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog
            .record(self.market_id)
            .context("owner audit market handle is absent from frozen catalog")?;
        ensure_market_token(market, self.token_id, "owner audit")?;
        Ok(self)
    }
}

impl IntentRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.intent_id != 0, "intent identity is zero");
        anyhow::ensure!(self.market_id != 0, "intent market identity is zero");
        anyhow::ensure!(self.flags & !1 == 0, "intent contains unknown flags");
        match self.action_kind {
            1 => {
                anyhow::ensure!(
                    matches!(self.side_or_operation, 1 | 2),
                    "invalid CLOB order side"
                );
                anyhow::ensure!(matches!(self.time_in_force, 1..=4), "invalid time in force");
                anyhow::ensure!(self.token_or_up_id != 0, "order token identity is zero");
                anyhow::ensure!(
                    self.condition_id == 0 && self.down_token_id == 0,
                    "order carries inventory identities"
                );
                anyhow::ensure!(self.qty_or_amount_units_6dp > 0, "order quantity is zero");
                anyhow::ensure!(
                    (1..=100_000_000).contains(&self.limit_px_units_8dp),
                    "order price is outside Polymarket probability range"
                );
                anyhow::ensure!(
                    (self.time_in_force == 2) == (self.expiration_ms > 0),
                    "GTD expiration identity mismatch"
                );
                anyhow::ensure!(
                    self.flags & 1 == 0 || matches!(self.time_in_force, 1 | 2),
                    "post-only order requires GTC or GTD"
                );
                if self.side_or_operation == 1 {
                    anyhow::ensure!(
                        self.close_reason == 0
                            && self.cohort_id.is_empty()
                            && self.execution_evidence_revision == 0
                            && self.flags == 0
                            && self.time_in_force == 2,
                        "BUY entry requires non-post-only GTD without sell ownership fields"
                    );
                } else {
                    anyhow::ensure!(
                        matches!(self.close_reason, 1 | 2),
                        "invalid managed-sell close reason"
                    );
                    self.cohort_id.required_str()?;
                    anyhow::ensure!(
                        self.execution_evidence_revision > 0,
                        "managed sell execution evidence revision is zero"
                    );
                }
            }
            2 => {
                anyhow::ensure!(
                    matches!(self.side_or_operation, 1 | 2),
                    "invalid inventory operation"
                );
                anyhow::ensure!(
                    self.condition_id != 0,
                    "inventory condition identity is zero"
                );
                anyhow::ensure!(
                    self.token_or_up_id != 0 && self.down_token_id != 0,
                    "inventory token identity is zero"
                );
                if self.side_or_operation == 1 {
                    anyhow::ensure!(
                        self.cohort_id.is_empty(),
                        "split intent cannot carry pre-confirmation cohort identity"
                    );
                } else {
                    self.cohort_id.required_str()?;
                }
                anyhow::ensure!(
                    self.qty_or_amount_units_6dp > 0,
                    "inventory quantity is zero"
                );
                anyhow::ensure!(
                    self.time_in_force == 0
                        && self.close_reason == 0
                        && self.limit_px_units_8dp == 0
                        && self.execution_evidence_revision == 0
                        && self.expiration_ms == 0,
                    "inventory intent carries order fields"
                );
            }
            3 => {
                anyhow::ensure!(self.side_or_operation == 0, "market cancel carries side");
                self.validate_cancel_fields(false)?;
            }
            4 => {
                anyhow::ensure!(
                    matches!(self.side_or_operation, 0..=2),
                    "invalid quote cancel side"
                );
                anyhow::ensure!(
                    self.token_or_up_id != 0,
                    "quote cancel token identity is zero"
                );
                self.validate_cancel_fields(true)?;
            }
            other => anyhow::bail!("invalid intent action kind={other}"),
        }
        Ok(self)
    }

    pub(crate) fn validate_catalog(self, catalog: &MarketCatalog) -> Result<Self> {
        let market = catalog_market(catalog, self.market_id)?;
        match self.action_kind {
            1 => {
                ensure_market_token(market, self.token_or_up_id, "intent")?;
                if self.side_or_operation == 1 {
                    anyhow::ensure!(
                        self.expiration_ms <= market.window_end_ms,
                        "BUY expiration exceeds the market window"
                    );
                }
            }
            2 => ensure_inventory_identity(
                market,
                self.condition_id,
                self.token_or_up_id,
                self.down_token_id,
                "inventory intent",
            )?,
            3 => {}
            4 => ensure_market_token(market, self.token_or_up_id, "quote cancel")?,
            _ => unreachable!("validated intent action kind"),
        }
        Ok(self)
    }
    fn validate_cancel_fields(self, carries_token: bool) -> Result<()> {
        anyhow::ensure!(
            self.time_in_force == 0
                && self.close_reason == 0
                && self.condition_id == 0
                && (carries_token || self.token_or_up_id == 0)
                && self.down_token_id == 0
                && self.cohort_id.is_empty()
                && self.qty_or_amount_units_6dp == 0
                && self.limit_px_units_8dp == 0
                && self.execution_evidence_revision == 0
                && self.expiration_ms == 0,
            "cancel intent carries order or inventory fields"
        );
        Ok(())
    }
}

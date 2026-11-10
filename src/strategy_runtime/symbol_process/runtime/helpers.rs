use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::{CandidateState, OwnerAuditRecord};
use crate::strategy::facts::StrategyFact;
use crate::strategy_runtime::actors::MakerRebateMmSymbolViewUpdate;
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::symbol_process::protocol::{
    CohortIdentityRecord, OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG,
};

pub(super) fn apply_views(candidate: &mut CandidateState) -> Result<()> {
    candidate
        .actor
        .apply_symbol_views(MakerRebateMmSymbolViewUpdate {
            external_view: None,
            portfolio: candidate.portfolio.clone(),
            open_orders: candidate.open_orders.clone(),
            risk: candidate.risk.clone(),
            blocked_markets: HashSet::new(),
            cfg: None,
        })
}

pub(super) fn owner_audit_record_from_fact(
    fact: &StrategyFact,
    catalog: &MarketCatalog,
    execution_intent_ordinal: u64,
) -> Result<OwnerAuditRecord> {
    let StrategyFact::CtfOwnedLegOutcome(fact) = fact;
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
    let blocked_sell = outcome == crate::strategy::facts::CtfLegEconomicOutcome::SellNow
        && action_qty_units_6dp == 0
        && execution_evidence_revision == 0;
    anyhow::ensure!(
        if outcome == crate::strategy::facts::CtfLegEconomicOutcome::SellNow {
            (blocked_sell && execution_intent_ordinal == 0)
                || (!blocked_sell && execution_intent_ordinal > 0)
        } else {
            execution_intent_ordinal == 0
        },
        "owner audit execution intent ordinal does not match blocked or executable outcome"
    );
    let condition = B256::from_str(market_id)
        .with_context(|| format!("owner audit condition identity is invalid `{market_id}`"))?;
    let market_handle = catalog
        .market_handle_for_condition(condition.0)
        .context("owner audit condition is absent from frozen catalog")?;
    let token = U256::from_str(token)
        .with_context(|| format!("owner audit token identity is invalid `{token}`"))?;
    let token_handle = catalog
        .token_handle(token.to_be_bytes::<32>())
        .context("owner audit token is absent from frozen catalog")?;
    let close_reason = close_reason.map_or(0, |reason| reason as u8);
    let (flags, sell_full_entry_pnl_bits, hold_full_entry_pnl_bits, fillable_qty_bits) =
        post_sell_economics.map_or((0, 0, 0, 0), |evidence| {
            (
                OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG,
                evidence.sell_full_entry_pnl_bits,
                evidence.hold_full_entry_pnl_bits,
                evidence.fillable_qty_bits,
            )
        });
    let (
        first_flags,
        first_instruction_limit_px_bits,
        first_depth_gross_proceeds_bits,
        first_depth_fee_bits,
        first_depth_net_proceeds_bits,
        first_authorization_fee_bits,
        first_authorization_net_proceeds_bits,
        first_delay_cushion_bits,
        first_allocated_basis_units_6dp,
        first_upper_valuation_bits,
        first_reservation_bits,
        first_mandatory_urgency,
    ) = first_instruction_economics.map_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false), |evidence| {
        (
            OwnerAuditRecord::FIRST_INSTRUCTION_ECONOMICS_FLAG
                | evidence
                    .upper_valuation_bits
                    .map_or(0, |_| OwnerAuditRecord::FIRST_UPPER_VALUATION_FLAG),
            evidence.instruction_limit_px_bits,
            evidence.depth_gross_proceeds_bits,
            evidence.depth_fee_bits,
            evidence.depth_net_proceeds_bits,
            evidence.authorization_fee_bits,
            evidence.authorization_net_proceeds_bits,
            evidence.delay_cushion_bits,
            evidence.allocated_basis_units_6dp,
            evidence.upper_valuation_bits.unwrap_or(0),
            evidence.reservation_bits,
            evidence.mandatory_urgency,
        )
    });
    Ok(OwnerAuditRecord {
        audit_seq: 0,
        source_sequence: 0,
        central_state_seq: 0,
        ts_ms,
        execution_intent_id: execution_intent_ordinal,
        market_id: market_handle,
        token_id: token_handle,
        cohort_id: CohortIdentityRecord::required(cohort_id)?,
        owned_qty_units_6dp,
        action_qty_units_6dp,
        execution_evidence_revision,
        sell_full_entry_pnl_bits,
        hold_full_entry_pnl_bits,
        fillable_qty_bits,
        first_instruction_limit_px_bits,
        first_depth_gross_proceeds_bits,
        first_depth_fee_bits,
        first_depth_net_proceeds_bits,
        first_authorization_fee_bits,
        first_authorization_net_proceeds_bits,
        first_delay_cushion_bits,
        first_allocated_basis_units_6dp,
        first_upper_valuation_bits,
        first_reservation_bits,
        first_mandatory_urgency,
        outcome: outcome as u8,
        close_reason,
        causal_winner_side: causal_winner_side.map_or(0, |side| match side {
            crate::strategy::hold_to_resolution::PairTokenSide::Up => 1,
            crate::strategy::hold_to_resolution::PairTokenSide::Down => 2,
        }),
        reason: reason as u16,
        flags: flags | first_flags,
    })
}

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{append_once, EngineTradeJournalRecord};
use crate::ctf_inventory::ConfirmedCtfInventoryOperation;
use crate::polymarket::identity::MarketId;
use crate::strategy::{
    full_set_units_to_qty, CtfSplitBookTop, InventoryIntent, InventoryOperation,
    StrategyDecisionContext,
};
use crate::trade_session_ledger::{
    direct_eoa_submission_receipt, CanonicalCtfEvent, ConfirmedCtfSplitFact, DirectEoaOperation,
};

#[path = "ctf_entry/decision_value.rs"]
mod decision_value;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CtfSplitEntryJournal {
    pub market_id: Option<String>,
    pub tokens: Vec<String>,
    pub reasons: Vec<String>,
    pub decisions: Vec<CtfSplitEntryDecisionContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CtfSplitEntryDecisionContext {
    pub market_id: String,
    pub condition_id: String,
    pub gamma_market_id: String,
    pub market_underlying: Option<String>,
    pub entry_oracle_symbol: String,
    pub up_token: String,
    pub down_token: String,
    pub qty: f64,
    #[serde(with = "crate::trade_session_ledger::exact_serde::u128_string")]
    pub amount_units_6dp: u128,
    pub entry_market_budget_share_cap: Option<f64>,
    pub entry_total_inventory_share_cap: Option<f64>,
    pub p_up: Option<f64>,
    pub model_p_up_raw: Option<f64>,
    pub model_p_up: Option<f64>,
    pub market_p_up: Option<f64>,
    pub base_p_fair: Option<f64>,
    pub p_fair: Option<f64>,
    pub calibration_confidence: Option<f64>,
    pub calibration_center: Option<f64>,
    pub calibration_market_center_weight: Option<f64>,
    pub calibration_spread_quality: Option<f64>,
    pub calibration_depth_quality: Option<f64>,
    pub model_weight: Option<f64>,
    pub depth_pressure: Option<f64>,
    pub unwind_up: Option<f64>,
    pub unwind_down: Option<f64>,
    pub adv_up: Option<f64>,
    pub adv_down: Option<f64>,
    pub adv_up_ewma: Option<f64>,
    pub adv_down_ewma: Option<f64>,
    pub theta_enter_up: Option<f64>,
    pub theta_enter_down: Option<f64>,
    pub oracle_side_confidence: Option<f64>,
    pub market_confirmation: Option<f64>,
    pub shock_toxicity: Option<f64>,
    pub markout_toxicity: Option<f64>,
    pub ta_alignment: Option<f64>,
    pub ta_ema_trend: Option<f64>,
    pub ta_macd_trend: Option<f64>,
    pub ta_rsi_regime: Option<f64>,
    pub ta_bollinger_position: Option<f64>,
    pub ta_recent_return_sign: Option<f64>,
    pub ta_drift_score: Option<f64>,
    pub oracle_side_reason: Option<String>,
    pub top_up_bid_delta: Option<f64>,
    pub top_down_bid_delta: Option<f64>,
    pub up_book_top: CtfSplitBookTop,
    pub down_book_top: CtfSplitBookTop,
    pub decision_ts_ms: u64,
}

impl CtfSplitEntryJournal {
    pub fn validate_split_intent(&self, intent: &InventoryIntent) -> Result<()> {
        anyhow::ensure!(
            intent.operation == InventoryOperation::SplitCompleteSet,
            "split journal target is not a split intent"
        );
        anyhow::ensure!(
            self.decisions.len() == 1 && self.reasons.len() == 1,
            "split journal requires exactly one decision and reason"
        );
        let decision = &self.decisions[0];
        let underlying = decision
            .market_underlying
            .as_deref()
            .context("split journal underlying identity is missing")?;
        let reason = &self.reasons[0];
        anyhow::ensure!(
            !reason.is_empty()
                && reason.trim() == reason
                && decision.decision_ts_ms > 0
                && decision.up_book_top.ts_ms > 0
                && decision.down_book_top.ts_ms > 0,
            "split journal reason, side, or timestamps are invalid"
        );
        for (name, book) in [
            ("up", &decision.up_book_top),
            ("down", &decision.down_book_top),
        ] {
            anyhow::ensure!(
                book.bid.is_finite()
                    && book.ask.is_finite()
                    && (0.0..=1.0).contains(&book.bid)
                    && (0.0..=1.0).contains(&book.ask)
                    && book.ask >= book.bid
                    && book
                        .bid_sz
                        .is_none_or(|value| value.is_finite() && value >= 0.0)
                    && book
                        .ask_sz
                        .is_none_or(|value| value.is_finite() && value >= 0.0),
                "split journal {name} book evidence is invalid"
            );
        }
        for value in [
            decision.entry_market_budget_share_cap,
            decision.entry_total_inventory_share_cap,
            decision.p_up,
            decision.model_p_up_raw,
            decision.model_p_up,
            decision.market_p_up,
            decision.base_p_fair,
            decision.p_fair,
            decision.calibration_confidence,
            decision.calibration_center,
            decision.calibration_market_center_weight,
            decision.calibration_spread_quality,
            decision.calibration_depth_quality,
            decision.model_weight,
            decision.depth_pressure,
            decision.unwind_up,
            decision.unwind_down,
            decision.adv_up,
            decision.adv_down,
            decision.adv_up_ewma,
            decision.adv_down_ewma,
            decision.theta_enter_up,
            decision.theta_enter_down,
            decision.oracle_side_confidence,
            decision.market_confirmation,
            decision.shock_toxicity,
            decision.markout_toxicity,
            decision.ta_alignment,
            decision.ta_ema_trend,
            decision.ta_macd_trend,
            decision.ta_rsi_regime,
            decision.ta_bollinger_position,
            decision.ta_recent_return_sign,
            decision.ta_drift_score,
            decision.top_up_bid_delta,
            decision.top_down_bid_delta,
        ] {
            anyhow::ensure!(
                value.is_none_or(f64::is_finite),
                "split journal numeric decision evidence is nonfinite"
            );
        }
        anyhow::ensure!(
            !underlying.is_empty()
                && underlying.trim() == underlying
                && !decision.entry_oracle_symbol.is_empty()
                && decision.entry_oracle_symbol.trim() == decision.entry_oracle_symbol
                && self.market_id.as_deref() == Some(intent.market_id.as_str())
                && decision.market_id == intent.market_id.as_str()
                && decision.condition_id == intent.condition_id.to_string()
                && Some(decision.gamma_market_id.as_str()) == intent.gamma_market_id.as_deref()
                && self.tokens.as_slice() == [intent.up_token.as_str(), intent.down_token.as_str()]
                && decision.up_token == intent.up_token
                && decision.down_token == intent.down_token
                && decision.up_book_top.token_id == intent.up_token
                && decision.down_book_top.token_id == intent.down_token
                && decision.amount_units_6dp == intent.amount_units_6dp
                && decision.qty.to_bits()
                    == full_set_units_to_qty(intent.amount_units_6dp).to_bits(),
            "split journal does not match split intent identity"
        );
        Ok(())
    }
}

pub fn ctf_split_entry_decision_context(
    context: &StrategyDecisionContext,
) -> Option<CtfSplitEntryDecisionContext> {
    let StrategyDecisionContext::CtfSplitEntry(context) = context else {
        return None;
    };
    Some(CtfSplitEntryDecisionContext {
        market_id: context.market_id.clone(),
        condition_id: context.condition_id.clone(),
        gamma_market_id: context.gamma_market_id.clone(),
        market_underlying: Some(context.market_underlying.clone()),
        entry_oracle_symbol: context.entry_oracle_symbol.clone(),
        up_token: context.up_token.clone(),
        down_token: context.down_token.clone(),
        qty: context.qty,
        amount_units_6dp: context.amount_units_6dp,
        entry_market_budget_share_cap: context.entry_market_budget_share_cap,
        entry_total_inventory_share_cap: context.entry_total_inventory_share_cap,
        p_up: context.p_up,
        model_p_up_raw: context.model_p_up_raw,
        model_p_up: context.model_p_up,
        market_p_up: context.market_p_up,
        base_p_fair: context.base_p_fair,
        p_fair: context.p_fair,
        calibration_confidence: context.calibration_confidence,
        calibration_center: context.calibration_center,
        calibration_market_center_weight: context.calibration_market_center_weight,
        calibration_spread_quality: context.calibration_spread_quality,
        calibration_depth_quality: context.calibration_depth_quality,
        model_weight: context.model_weight,
        depth_pressure: context.depth_pressure,
        unwind_up: context.unwind_up,
        unwind_down: context.unwind_down,
        adv_up: context.adv_up,
        adv_down: context.adv_down,
        adv_up_ewma: context.adv_up_ewma,
        adv_down_ewma: context.adv_down_ewma,
        theta_enter_up: context.theta_enter_up,
        theta_enter_down: context.theta_enter_down,
        oracle_side_confidence: context.oracle_side_confidence,
        market_confirmation: context.market_confirmation,
        shock_toxicity: context.shock_toxicity,
        markout_toxicity: context.markout_toxicity,
        ta_alignment: context.ta_alignment,
        ta_ema_trend: context.ta_ema_trend,
        ta_macd_trend: context.ta_macd_trend,
        ta_rsi_regime: context.ta_rsi_regime,
        ta_bollinger_position: context.ta_bollinger_position,
        ta_recent_return_sign: context.ta_recent_return_sign,
        ta_drift_score: context.ta_drift_score,
        oracle_side_reason: context.oracle_side_reason.clone(),
        top_up_bid_delta: context.top_up_bid_delta,
        top_down_bid_delta: context.top_down_bid_delta,
        up_book_top: context.up_book_top.clone(),
        down_book_top: context.down_book_top.clone(),
        decision_ts_ms: context.decision_ts_ms,
    })
}

pub fn build_confirmed_ctf_split_entry_record(
    confirmed: &ConfirmedCtfInventoryOperation,
    underlying: &str,
    journal: &CtfSplitEntryJournal,
) -> Result<EngineTradeJournalRecord> {
    anyhow::ensure!(
        confirmed.operation == InventoryOperation::SplitCompleteSet,
        "confirmed CTF entry record requires a split operation"
    );
    anyhow::ensure!(
        confirmed.materialized_ts_ms > 0,
        "confirmed split materialization timestamp is missing"
    );
    anyhow::ensure!(
        confirmed.pending_ts_ms > 0
            && confirmed.pending_ts_ms <= confirmed.receipt_confirmed_ts_ms
            && confirmed.receipt_confirmed_ts_ms <= confirmed.materialized_ts_ms,
        "confirmed split submission/receipt/materialization timestamps are invalid"
    );
    let underlying = underlying.trim().to_ascii_uppercase();
    let tx_hash = confirmed.tx_hash.trim();
    anyhow::ensure!(
        !tx_hash.is_empty() && tx_hash == confirmed.tx_hash,
        "confirmed split transaction identity is missing or noncanonical"
    );
    let market_id = MarketId::from(confirmed.market_id.clone());
    let expected_cohort_id =
        crate::ctf_inventory::confirmed_ctf_split_cohort_id(&confirmed.tx_hash)?;
    anyhow::ensure!(
        confirmed.cohort_id == expected_cohort_id,
        "confirmed split cohort identity mismatch"
    );
    let gamma_market_id = confirmed
        .gamma_market_id
        .as_deref()
        .context("confirmed split gamma market identity is missing")?;
    anyhow::ensure!(
        !gamma_market_id.is_empty() && gamma_market_id == gamma_market_id.trim(),
        "confirmed split gamma market identity is noncanonical"
    );

    anyhow::ensure!(
        journal.decisions.len() == 1,
        "confirmed split must have exactly one decision context"
    );
    let decision = &journal.decisions[0];
    anyhow::ensure!(
        journal.market_id.as_deref() == Some(confirmed.market_id.as_str())
            && decision.market_id == confirmed.market_id,
        "confirmed split operation/journal market identity mismatch"
    );
    anyhow::ensure!(
        decision.condition_id == confirmed.condition_id.to_string(),
        "confirmed split operation/journal condition identity mismatch"
    );
    anyhow::ensure!(
        gamma_market_id == decision.gamma_market_id,
        "confirmed split operation/journal gamma market identity mismatch"
    );
    anyhow::ensure!(
        journal.tokens.as_slice() == [confirmed.up_token.as_str(), confirmed.down_token.as_str()]
            && decision.up_token == confirmed.up_token
            && decision.down_token == confirmed.down_token,
        "confirmed split operation/journal token identity mismatch"
    );
    anyhow::ensure!(
        decision.amount_units_6dp == confirmed.amount_units_6dp,
        "confirmed split operation/journal amount mismatch"
    );
    let journal_underlying = decision
        .market_underlying
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("confirmed split missing market underlying")?
        .to_ascii_uppercase();
    anyhow::ensure!(
        journal_underlying == underlying,
        "confirmed split operation/journal underlying mismatch"
    );
    // Relative entry-book midpoint value allocates the one joint split basis.
    // Using bid+ask as the weight avoids midpoint truncation; DOWN owns residual units.
    let up_cost_basis_weight_units_8dp = decision
        .up_book_top
        .basis_weight_units_8dp()
        .context("split entry book is not exact fixed-8dp accounting evidence")?;
    let down_cost_basis_weight_units_8dp = decision
        .down_book_top
        .basis_weight_units_8dp()
        .context("split entry book is not exact fixed-8dp accounting evidence")?;

    let accounting_fact = CanonicalCtfEvent::SplitConfirmed(ConfirmedCtfSplitFact {
        receipt_id: format!("ctf_split:{tx_hash}"),
        transaction_id: tx_hash.to_string(),
        market_id: market_id.clone(),
        condition_id: confirmed.condition_id.to_string(),
        cohort_id: confirmed.cohort_id.clone(),
        underlying,
        market_name: None,
        up_token: confirmed.up_token.clone(),
        down_token: confirmed.down_token.clone(),
        up_qty_units_6dp: confirmed.amount_units_6dp,
        down_qty_units_6dp: confirmed.amount_units_6dp,
        up_cost_basis_weight_units_8dp: Some(up_cost_basis_weight_units_8dp),
        down_cost_basis_weight_units_8dp: Some(down_cost_basis_weight_units_8dp),
        collateral_debit_units_6dp: confirmed.amount_units_6dp,
        fee_units_6dp: 0,
        approved_direct_cost_units_6dp: 0,
    });
    accounting_fact.validate()?;

    let mut record = EngineTradeJournalRecord::new(
        confirmed.cohort_id.clone(),
        accounting_fact.event_kind(),
        confirmed.materialized_ts_ms,
    );
    record.market_id = Some(market_id);
    record.paired_cohort_key = Some(confirmed.cohort_id.clone());
    record.tokens = vec![confirmed.up_token.clone(), confirmed.down_token.clone()];
    record.entry_ts_ms = Some(confirmed.materialized_ts_ms);
    record.ctf_event = Some(accounting_fact);
    record.split_tx_hash = Some(tx_hash.to_string());
    record.tx_hashes.push(tx_hash.to_string());
    record.strategy_entry_reason = Some(journal.reasons.join(" | "));
    record.decision_context = Some(json!({
        "action_kind": "ctf_split_entry_accepted",
        "inventory_operation": "split_complete_set",
        "lifecycle_receipt_schema_version": 1,
        "decisions": journal
            .decisions
            .iter()
            .map(CtfSplitEntryDecisionContext::to_value)
            .collect::<Vec<_>>(),
    }));
    Ok(record)
}

pub fn prepare_ctf_split_entry_accepted(
    confirmed: &ConfirmedCtfInventoryOperation,
) -> Result<(EngineTradeJournalRecord, ConfirmedCtfSplitFact)> {
    let submission = direct_eoa_submission_receipt(&confirmed.tx_hash)?;
    let condition_id = confirmed.condition_id.to_string();
    let split_identity = submission.required_split_identity()?;
    let journal = submission.required_split_entry_journal()?;
    split_identity.validate_entry_journal(journal)?;
    anyhow::ensure!(
        submission.operation == DirectEoaOperation::Split
            && submission.condition_id.as_deref() == Some(condition_id.as_str())
            && submission.amount_units_6dp == Some(confirmed.amount_units_6dp)
            && split_identity.market_id == confirmed.market_id
            && split_identity.gamma_market_id.as_str()
                == confirmed.gamma_market_id.as_deref().unwrap_or_default()
            && split_identity.condition_id == condition_id
            && split_identity.up_token == confirmed.up_token
            && split_identity.down_token == confirmed.down_token
            && split_identity.amount_units_6dp == confirmed.amount_units_6dp,
        "confirmed split does not match durable direct EOA submission tx_hash={}",
        confirmed.tx_hash
    );
    let mut record =
        build_confirmed_ctf_split_entry_record(confirmed, &split_identity.underlying, journal)?;
    record.run_id = submission.run_id;
    let split = match record.ctf_event.as_ref() {
        Some(CanonicalCtfEvent::SplitConfirmed(split)) => split.clone(),
        _ => anyhow::bail!("confirmed split record is missing canonical split fact"),
    };
    Ok((record, split))
}

pub fn append_prepared_ctf_split_entry_accepted(record: EngineTradeJournalRecord) -> Result<()> {
    let tx_hash = record
        .split_tx_hash
        .as_deref()
        .context("prepared confirmed split record is missing transaction identity")?
        .to_string();
    append_once(format!("ctf_split:{tx_hash}"), record)?;
    Ok(())
}

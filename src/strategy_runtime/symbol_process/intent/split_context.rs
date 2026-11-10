use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::super::protocol::{IntentRecord, SplitEntryContextRecord, SymbolId};
use crate::engine_trade_journal::{CtfSplitEntryDecisionContext, CtfSplitEntryJournal};
use crate::strategy::{full_set_units_to_qty, CtfSplitBookTop};
use crate::strategy_runtime::market_catalog::MarketCatalog;

pub(crate) fn decision_context_record(
    context: &CtfSplitEntryDecisionContext,
    reason: &str,
    target_per_side: f64,
    liquidity_manufacturer: bool,
    expected_oracle_symbol: &str,
    intent: IntentRecord,
    intent_ordinal: u64,
    catalog: &MarketCatalog,
) -> Result<SplitEntryContextRecord> {
    anyhow::ensure!(intent_ordinal > 0, "split context intent ordinal is zero");
    anyhow::ensure!(
        !liquidity_manufacturer,
        "split context cannot enable liquidity manufacturer"
    );
    let mut intent = intent;
    intent.intent_id = intent_ordinal;
    ensure_context_identity(context, intent, catalog)?;
    anyhow::ensure!(
        context.entry_oracle_symbol == expected_oracle_symbol,
        "split context oracle symbol differs from frozen worker config"
    );
    let expected_reason = canonical_reason(
        context.market_id.as_str(),
        context.qty,
        target_per_side,
        liquidity_manufacturer,
    )?;
    anyhow::ensure!(
        reason == expected_reason,
        "split decision reason is noncanonical"
    );
    let optional = optional_metrics(context);
    let mut presence_mask = 0u64;
    let mut optional_metric_bits = [0u64; 36];
    for (index, value) in optional.into_iter().enumerate() {
        pack_optional(
            &mut presence_mask,
            index,
            value,
            &mut optional_metric_bits[index],
        )?;
    }
    let mut book_size_bits = [0u64; 4];
    for (offset, value) in [
        context.up_book_top.bid_sz,
        context.up_book_top.ask_sz,
        context.down_book_top.bid_sz,
        context.down_book_top.ask_sz,
    ]
    .into_iter()
    .enumerate()
    {
        pack_optional(
            &mut presence_mask,
            36 + offset,
            value,
            &mut book_size_bits[offset],
        )?;
    }
    let (oracle_symbol_len, oracle_symbol) = fixed_oracle_symbol(expected_oracle_symbol)?;
    SplitEntryContextRecord {
        intent_id: intent_ordinal,
        decision_ts_ms: context.decision_ts_ms,
        presence_mask,
        oracle_side_reason: oracle_reason_tag(context.oracle_side_reason.as_deref())?,
        oracle_symbol_len,
        flags: 0,
        oracle_symbol,
        reason_target_per_side_bits: target_per_side.to_bits(),
        up_bid_bits: context.up_book_top.bid.to_bits(),
        up_ask_bits: context.up_book_top.ask.to_bits(),
        up_ts_ms: context.up_book_top.ts_ms,
        down_bid_bits: context.down_book_top.bid.to_bits(),
        down_ask_bits: context.down_book_top.ask.to_bits(),
        down_ts_ms: context.down_book_top.ts_ms,
        optional_metric_bits,
        book_size_bits,
    }
    .validate()
}

pub(crate) fn record_to_journal(
    record: SplitEntryContextRecord,
    intent: IntentRecord,
    catalog: &MarketCatalog,
    source_symbol: SymbolId,
    expected_oracle_symbol: &str,
) -> Result<CtfSplitEntryJournal> {
    let record = record.validate()?;
    anyhow::ensure!(
        catalog.symbol() == source_symbol.as_str(),
        "split context source symbol differs from catalog"
    );
    ensure_split_intent(intent, catalog)?;
    anyhow::ensure!(
        record_oracle_symbol(&record)? == expected_oracle_symbol,
        "split context oracle symbol differs from frozen parent config"
    );
    anyhow::ensure!(
        record.intent_id == intent.intent_id,
        "split context intent identity mismatch"
    );
    let market = catalog
        .record(intent.market_id)
        .context("split context market handle is absent from catalog")?;
    let market_id = B256::from(market.condition_id).to_string();
    let up_token = U256::from_be_bytes(market.up_token).to_string();
    let down_token = U256::from_be_bytes(market.down_token).to_string();
    let qty = full_set_units_to_qty(intent.qty_or_amount_units_6dp);
    let target_per_side = f64::from_bits(record.reason_target_per_side_bits);
    let reason = canonical_reason(market_id.as_str(), qty, target_per_side, false)?;
    let context = CtfSplitEntryDecisionContext {
        market_id: market_id.clone(),
        condition_id: market_id.clone(),
        gamma_market_id: market.gamma_market_id.to_string(),
        market_underlying: Some(source_symbol.as_str().to_string()),
        entry_oracle_symbol: expected_oracle_symbol.to_string(),
        up_token: up_token.clone(),
        down_token: down_token.clone(),
        qty,
        amount_units_6dp: intent.qty_or_amount_units_6dp,
        entry_market_budget_share_cap: unpack_optional(record, 0),
        entry_total_inventory_share_cap: unpack_optional(record, 1),
        p_up: unpack_optional(record, 2),
        model_p_up_raw: unpack_optional(record, 3),
        model_p_up: unpack_optional(record, 4),
        market_p_up: unpack_optional(record, 5),
        base_p_fair: unpack_optional(record, 6),
        p_fair: unpack_optional(record, 7),
        calibration_confidence: unpack_optional(record, 8),
        calibration_center: unpack_optional(record, 9),
        calibration_market_center_weight: unpack_optional(record, 10),
        calibration_spread_quality: unpack_optional(record, 11),
        calibration_depth_quality: unpack_optional(record, 12),
        model_weight: unpack_optional(record, 13),
        depth_pressure: unpack_optional(record, 14),
        unwind_up: unpack_optional(record, 15),
        unwind_down: unpack_optional(record, 16),
        adv_up: unpack_optional(record, 17),
        adv_down: unpack_optional(record, 18),
        adv_up_ewma: unpack_optional(record, 19),
        adv_down_ewma: unpack_optional(record, 20),
        theta_enter_up: unpack_optional(record, 21),
        theta_enter_down: unpack_optional(record, 22),
        oracle_side_confidence: unpack_optional(record, 23),
        market_confirmation: unpack_optional(record, 24),
        shock_toxicity: unpack_optional(record, 25),
        markout_toxicity: unpack_optional(record, 26),
        ta_alignment: unpack_optional(record, 27),
        ta_ema_trend: unpack_optional(record, 28),
        ta_macd_trend: unpack_optional(record, 29),
        ta_rsi_regime: unpack_optional(record, 30),
        ta_bollinger_position: unpack_optional(record, 31),
        ta_recent_return_sign: unpack_optional(record, 32),
        ta_drift_score: unpack_optional(record, 33),
        oracle_side_reason: oracle_reason(record.oracle_side_reason).map(str::to_string),
        top_up_bid_delta: unpack_optional(record, 34),
        top_down_bid_delta: unpack_optional(record, 35),
        up_book_top: CtfSplitBookTop {
            token_id: up_token.clone(),
            bid: f64::from_bits(record.up_bid_bits),
            ask: f64::from_bits(record.up_ask_bits),
            bid_sz: unpack_book_size(record, 0),
            ask_sz: unpack_book_size(record, 1),
            ts_ms: record.up_ts_ms,
        },
        down_book_top: CtfSplitBookTop {
            token_id: down_token.clone(),
            bid: f64::from_bits(record.down_bid_bits),
            ask: f64::from_bits(record.down_ask_bits),
            bid_sz: unpack_book_size(record, 2),
            ask_sz: unpack_book_size(record, 3),
            ts_ms: record.down_ts_ms,
        },
        decision_ts_ms: record.decision_ts_ms,
    };
    Ok(CtfSplitEntryJournal {
        market_id: Some(market_id),
        tokens: vec![up_token, down_token],
        reasons: vec![reason],
        decisions: vec![context],
    })
}

fn ensure_context_identity(
    context: &CtfSplitEntryDecisionContext,
    intent: IntentRecord,
    catalog: &MarketCatalog,
) -> Result<()> {
    ensure_split_intent(intent, catalog)?;
    let market = catalog
        .record(intent.market_id)
        .context("split context market handle is absent from catalog")?;
    let market_id = B256::from(market.condition_id).to_string();
    let up_token = U256::from_be_bytes(market.up_token).to_string();
    let down_token = U256::from_be_bytes(market.down_token).to_string();
    anyhow::ensure!(
        context.market_id == market_id
            && context.condition_id == market_id
            && context.gamma_market_id == market.gamma_market_id.to_string()
            && context.market_underlying.as_deref() == Some(catalog.symbol())
            && context.up_token == up_token
            && context.down_token == down_token
            && context.up_book_top.token_id == up_token
            && context.down_book_top.token_id == down_token
            && context.amount_units_6dp == intent.qty_or_amount_units_6dp
            && context.qty.to_bits()
                == full_set_units_to_qty(intent.qty_or_amount_units_6dp).to_bits(),
        "split decision context does not match intent/catalog identity"
    );
    Ok(())
}

fn ensure_split_intent(intent: IntentRecord, catalog: &MarketCatalog) -> Result<()> {
    let intent = intent.validate()?.validate_catalog(catalog)?;
    anyhow::ensure!(
        intent.action_kind == 2 && intent.side_or_operation == 1,
        "split context target is not a split inventory intent"
    );
    Ok(())
}

fn optional_metrics(context: &CtfSplitEntryDecisionContext) -> [Option<f64>; 36] {
    [
        context.entry_market_budget_share_cap,
        context.entry_total_inventory_share_cap,
        context.p_up,
        context.model_p_up_raw,
        context.model_p_up,
        context.market_p_up,
        context.base_p_fair,
        context.p_fair,
        context.calibration_confidence,
        context.calibration_center,
        context.calibration_market_center_weight,
        context.calibration_spread_quality,
        context.calibration_depth_quality,
        context.model_weight,
        context.depth_pressure,
        context.unwind_up,
        context.unwind_down,
        context.adv_up,
        context.adv_down,
        context.adv_up_ewma,
        context.adv_down_ewma,
        context.theta_enter_up,
        context.theta_enter_down,
        context.oracle_side_confidence,
        context.market_confirmation,
        context.shock_toxicity,
        context.markout_toxicity,
        context.ta_alignment,
        context.ta_ema_trend,
        context.ta_macd_trend,
        context.ta_rsi_regime,
        context.ta_bollinger_position,
        context.ta_recent_return_sign,
        context.ta_drift_score,
        context.top_up_bid_delta,
        context.top_down_bid_delta,
    ]
}

fn pack_optional(mask: &mut u64, index: usize, value: Option<f64>, bits: &mut u64) -> Result<()> {
    if let Some(value) = value {
        anyhow::ensure!(
            value.is_finite(),
            "split context optional value is nonfinite"
        );
        *mask |= 1u64 << index;
        *bits = value.to_bits();
    }
    Ok(())
}

fn unpack_optional(record: SplitEntryContextRecord, index: usize) -> Option<f64> {
    (record.presence_mask & (1u64 << index) != 0)
        .then(|| f64::from_bits(record.optional_metric_bits[index]))
}

fn unpack_book_size(record: SplitEntryContextRecord, offset: usize) -> Option<f64> {
    (record.presence_mask & (1u64 << (36 + offset)) != 0)
        .then(|| f64::from_bits(record.book_size_bits[offset]))
}

fn canonical_reason(
    market_id: &str,
    qty: f64,
    target_per_side: f64,
    liquidity_manufacturer: bool,
) -> Result<String> {
    anyhow::ensure!(
        qty.is_finite() && qty > 0.0 && target_per_side.is_finite() && target_per_side > 0.0,
        "split decision reason values are invalid"
    );
    let suffix = if liquidity_manufacturer {
        " liquidity_manufacturer=true"
    } else {
        ""
    };
    Ok(format!(
        "ctf_split_complete_set market={market_id} qty={qty:.6} target_per_side={target_per_side:.6}{suffix}"
    ))
}

fn oracle_reason_tag(value: Option<&str>) -> Result<u8> {
    match value {
        None => Ok(0),
        Some("NoMarketData") => Ok(1),
        Some("NoActionableEdge") => Ok(2),
        Some("PreserveUp") => Ok(3),
        Some("PreserveDown") => Ok(4),
        Some("SelectUp") => Ok(5),
        Some("SelectDown") => Ok(6),
        Some("FlipToUp") => Ok(7),
        Some("FlipToDown") => Ok(8),
        Some(_) => anyhow::bail!("split context oracle-side reason is invalid"),
    }
}

fn oracle_reason(tag: u8) -> Option<&'static str> {
    match tag {
        0 => None,
        1 => Some("NoMarketData"),
        2 => Some("NoActionableEdge"),
        3 => Some("PreserveUp"),
        4 => Some("PreserveDown"),
        5 => Some("SelectUp"),
        6 => Some("SelectDown"),
        7 => Some("FlipToUp"),
        8 => Some("FlipToDown"),
        _ => unreachable!("validated oracle-side-reason tag"),
    }
}

fn fixed_oracle_symbol(value: &str) -> Result<(u8, [u8; 32])> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 32
            && value
                .chars()
                .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit()),
        "split context oracle symbol is noncanonical or too long"
    );
    let mut bytes = [0u8; 32];
    bytes[..value.len()].copy_from_slice(value.as_bytes());
    Ok((u8::try_from(value.len()).unwrap(), bytes))
}

fn record_oracle_symbol(record: &SplitEntryContextRecord) -> Result<&str> {
    let len = usize::from(record.oracle_symbol_len);
    std::str::from_utf8(&record.oracle_symbol[..len])
        .context("split context oracle symbol is not UTF-8")
}

use super::*;

pub(super) fn runtime_snapshot(strategy: &MakerRebateMmStrategy) -> StrategyRuntimeSnapshot {
    let (
        pending_len,
        gate_drops_cycle,
        gate_drops_total,
        selection_status,
        quote_block_reasons_cycle,
        quote_block_detail_cycle,
    ) = strategy.runtime_snapshot_parts();
    StrategyRuntimeSnapshot {
        pending_len,
        gate_drops_cycle,
        gate_drops_total,
        selection_status,
        quote_block_reasons_cycle,
        quote_block_detail_cycle,
    }
}

pub(super) fn recompute_cycle_result(
    strategy: &mut MakerRebateMmStrategy,
    fact_sink: &mut StrategyFactSink,
    now_ms: u64,
    market_projection: &crate::polymarket::state::MarketProjection,
) -> StrategyRecomputeCycleResult {
    let (decisions, ranked, paired_managed_exit_count) =
        strategy.run_recompute_cycle(fact_sink, now_ms, market_projection);
    StrategyRecomputeCycleResult {
        decisions,
        ranked: ranked
            .into_iter()
            .map(
                |(oracle_symbol, up_token, down_token, toxicity, markout_toxicity)| {
                    crate::strategy_runtime::types::RankedMarketRuntimeView {
                        oracle_symbol,
                        up_token,
                        down_token,
                        toxicity,
                        markout_toxicity,
                    }
                },
            )
            .collect(),
        paired_managed_exit_count,
    }
}

pub(super) fn quantize_status_float(value: Option<f64>) -> i64 {
    match value {
        Some(value) if value.is_finite() => (value * 1_000_000.0).round() as i64,
        _ => 0,
    }
}

pub(super) fn map_recompute_reason(reason: QuoteRecomputeReason) -> QuoteRecomputeReasonObs {
    match reason {
        QuoteRecomputeReason::Initial => QuoteRecomputeReasonObs::Initial,
        QuoteRecomputeReason::OracleChanged => QuoteRecomputeReasonObs::OracleChanged,
        QuoteRecomputeReason::PortfolioStateChanged => {
            QuoteRecomputeReasonObs::PortfolioStateChanged
        }
        QuoteRecomputeReason::OpenOrdersChanged => QuoteRecomputeReasonObs::OpenOrdersChanged,
        QuoteRecomputeReason::RiskStateChanged => QuoteRecomputeReasonObs::RiskStateChanged,
        QuoteRecomputeReason::BlockedMarketsChanged => {
            QuoteRecomputeReasonObs::BlockedMarketsChanged
        }
        QuoteRecomputeReason::ThresholdFired => QuoteRecomputeReasonObs::ThresholdFired,
        QuoteRecomputeReason::BookChanged => QuoteRecomputeReasonObs::BookChanged,
    }
}

pub(super) fn quote_blocks_from_snapshot(
    snapshot: &StrategyRuntimeSnapshot,
) -> Vec<QuoteBlockReasonCount> {
    sorted_quote_block_items(&snapshot.quote_block_reasons_cycle)
        .into_iter()
        .map(|(reason, count)| QuoteBlockReasonCount {
            reason: reason.to_string(),
            count: u64::from(count),
        })
        .collect()
}

pub(super) fn quote_block_detail_map(
    snapshot: &StrategyRuntimeSnapshot,
) -> Option<BTreeMap<String, String>> {
    let (reason, detail) = snapshot.quote_block_detail_cycle.as_ref()?;
    let mut parsed = BTreeMap::new();
    parsed.insert("reason".to_string(), (*reason).to_string());
    parsed.insert("detail".to_string(), detail.clone());
    Some(parsed)
}

pub(super) fn oracle_wait_snapshot(
    strategy: &mut MakerRebateMmStrategy,
    now_ms: u64,
    market_projection: &crate::polymarket::state::MarketProjection,
) -> Option<(OracleWaitReason, BTreeMap<String, String>, String)> {
    let required = strategy.required_oracle_symbols_for_status(market_projection);
    if required.is_empty() {
        return None;
    }
    let ready = strategy.ready_oracle_symbols_for_status(now_ms, &required);
    if ready.len() == required.len() {
        return None;
    }
    let missing = required
        .iter()
        .filter(|symbol| !ready.iter().any(|ready_symbol| ready_symbol == *symbol))
        .cloned()
        .collect::<Vec<_>>();
    let reason = if ready.is_empty() {
        OracleWaitReason::WaitingForOracle
    } else {
        OracleWaitReason::OracleDegraded
    };
    let mut detail = BTreeMap::new();
    detail.insert("ready_count".to_string(), ready.len().to_string());
    detail.insert("required_count".to_string(), required.len().to_string());
    detail.insert("missing".to_string(), missing.join(","));
    let phase = match reason {
        OracleWaitReason::WaitingForOracle => "waiting_for_oracle",
        OracleWaitReason::OracleDegraded => "oracle_degraded",
    };
    let status = format!(
        "{phase} ready={}/{} missing={}",
        ready.len(),
        required.len(),
        missing.join(",")
    );
    Some((reason, detail, status))
}

pub(super) fn apply_status_fields(
    status_state: &mut StrategyStatusState,
    activity: StrategyActivityKind,
    recompute_reason: Option<QuoteRecomputeReasonObs>,
    queued: usize,
    drained: usize,
    ranked: usize,
    managed_exits: usize,
    quote_blocks: Vec<QuoteBlockReasonCount>,
    primary_block_detail: Option<std::collections::BTreeMap<String, String>>,
    shock_toxicity: Option<f64>,
    markout_toxicity: Option<f64>,
    markout_samples: Option<u64>,
    oracle_wait_reason: Option<OracleWaitReason>,
) {
    if let Some(status) = status_state.last_status.as_mut() {
        status.activity = activity;
        status.recompute_reason = recompute_reason;
        status.queued = queued;
        status.drained = drained;
        status.ranked = ranked;
        status.managed_exits = managed_exits;
        status.quote_blocks = quote_blocks;
        status.primary_block_detail = primary_block_detail;
        status.shock_toxicity = shock_toxicity;
        status.markout_toxicity = markout_toxicity;
        status.markout_samples = markout_samples;
        status.oracle_wait_reason = oracle_wait_reason;
    }
}

use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::observability::action::ActionEvent;
use crate::observability::strategy_status::{
    hash_status_render_sig, primary_quote_block_detail, sorted_quote_block_items,
    status_display_millis, OracleWaitReason, QuoteBlockReasonCount, QuoteRecomputeReasonObs,
    StrategyActivityKind, StrategyStatus, StrategyStatusState,
};
use crate::strategy::{
    MakerRebateMmStrategy, StrategyDecision, StrategyFact, StrategyFactSink, StrategyTickOutcome,
};
use crate::strategy_runtime::recompute::{
    defer_rebalance_until_cadence, next_recompute_reason, rebalance_gate_elapsed,
    update_invalid_tick_control, update_recompute_trigger_state, QuoteRecomputeReason,
};
use crate::strategy_runtime::state::StrategyRuntimeState;
use crate::strategy_runtime::types::{StrategyRecomputeCycleResult, StrategyRuntimeSnapshot};
use crate::types::MarketTick;

#[path = "driver/fact_translation.rs"]
mod fact_translation;
pub use fact_translation::{translate_strategy_fact, translate_strategy_facts};

#[path = "driver/status_helpers.rs"]
mod status_helpers;
use status_helpers::*;

pub fn strategy_status_emit_sig(st: &StrategyStatus) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    st.kind.hash(&mut hasher);
    quantize_status_float(st.p_up).hash(&mut hasher);
    quantize_status_float(st.edge).hash(&mut hasher);
    st.gate_drops_cycle.oracle.hash(&mut hasher);
    st.gate_drops_cycle.book.hash(&mut hasher);
    st.gate_drops_cycle.stale.hash(&mut hasher);
    st.gate_drops_cycle.open_delay.hash(&mut hasher);
    st.gate_drops_cycle.open_price.hash(&mut hasher);
    st.gate_drops_cycle.entry_time.hash(&mut hasher);
    st.gate_drops_cycle.blocked_market.hash(&mut hasher);
    st.gate_drops_cycle.markout_toxicity.hash(&mut hasher);
    st.gate_drops_total.oracle.hash(&mut hasher);
    st.gate_drops_total.book.hash(&mut hasher);
    st.gate_drops_total.stale.hash(&mut hasher);
    st.gate_drops_total.open_delay.hash(&mut hasher);
    st.gate_drops_total.open_price.hash(&mut hasher);
    st.gate_drops_total.entry_time.hash(&mut hasher);
    st.gate_drops_total.blocked_market.hash(&mut hasher);
    st.gate_drops_total.markout_toxicity.hash(&mut hasher);
    st.activity.hash(&mut hasher);
    st.recompute_reason.hash(&mut hasher);
    st.queued.hash(&mut hasher);
    st.drained.hash(&mut hasher);
    st.ranked.hash(&mut hasher);
    st.managed_exits.hash(&mut hasher);
    st.quote_blocks.hash(&mut hasher);
    st.primary_block_detail.hash(&mut hasher);
    st.shock_toxicity.map(f64::to_bits).hash(&mut hasher);
    st.markout_toxicity.map(f64::to_bits).hash(&mut hasher);
    st.markout_samples.hash(&mut hasher);
    st.oracle_wait_reason.hash(&mut hasher);
    if let Some(selection) = st.selection_status {
        selection.known_markets.hash(&mut hasher);
        selection.active_now_markets.hash(&mut hasher);
        selection.active_now_alternative_markets.hash(&mut hasher);
        selection.active_now_actionable_markets.hash(&mut hasher);
        selection
            .active_now_budget_deadzone_markets
            .hash(&mut hasher);
        selection.ranked_markets.hash(&mut hasher);
        selection.ranked_budget_deadzone_markets.hash(&mut hasher);
        selection
            .entry_time_blocked_active_markets
            .hash(&mut hasher);
        selection.blocked_market_count.hash(&mut hasher);
        selection.preopen_next_selection_present.hash(&mut hasher);
        selection.active_now_only_filter_applied.hash(&mut hasher);
        selection.next_selection_time_to_open_ms.hash(&mut hasher);
    }
    hasher.finish()
}

pub fn append_translated_strategy_facts(
    runtime_state: &mut StrategyRuntimeState,
    facts: Vec<StrategyFact>,
) {
    runtime_state
        .pending_action_events
        .extend(translate_strategy_facts(facts));
}

pub fn drain_runtime_action_events(runtime_state: &mut StrategyRuntimeState) -> Vec<ActionEvent> {
    runtime_state.pending_action_events.drain(..).collect()
}

pub struct AcceptedTickCoreOutcome {
    pub decisions: Vec<StrategyDecision>,
    pub facts: Vec<StrategyFact>,
    pub status: Option<StrategyStatus>,
    pub runtime_managed_paired_tokens: HashSet<String>,
}

pub fn drive_accepted_tick_core(
    strategy: &mut MakerRebateMmStrategy,
    fact_sink: &mut StrategyFactSink,
    runtime_state: &mut StrategyRuntimeState,
    status_state: &mut StrategyStatusState,
    tick: &MarketTick,
    max_decisions: usize,
) -> AcceptedTickCoreOutcome {
    let driven = drive_tick(
        strategy,
        fact_sink,
        runtime_state,
        status_state,
        tick,
        max_decisions,
    );
    let has_runtime_output =
        !driven.decisions.is_empty() || !driven.facts.is_empty() || driven.status.is_some();
    let runtime_managed_paired_tokens = if has_runtime_output {
        strategy.runtime_managed_paired_tokens()
    } else {
        HashSet::new()
    };
    AcceptedTickCoreOutcome {
        decisions: driven.decisions,
        facts: driven.facts,
        status: driven.status,
        runtime_managed_paired_tokens,
    }
}

fn emit_tick_kill_switch_status(
    status_state: &mut StrategyStatusState,
    now_ms: u64,
    snapshot: &StrategyRuntimeSnapshot,
) {
    status_state.set_status(
        now_ms,
        "tick_size_kill_switch",
        None,
        None,
        snapshot.gate_drops_cycle,
        snapshot.gate_drops_total,
        snapshot.selection_status,
    );
    apply_status_fields(
        status_state,
        StrategyActivityKind::HaltedTickGuard,
        None,
        snapshot.pending_len,
        0,
        0,
        0,
        quote_blocks_from_snapshot(snapshot),
        None,
        None,
        None,
        None,
        None,
    );
}

fn emit_queued_status(
    status_state: &mut StrategyStatusState,
    now_ms: u64,
    snapshot: &StrategyRuntimeSnapshot,
    drained: &[StrategyDecision],
    intents_left: usize,
) {
    status_state.set_status(
        now_ms,
        format!(
            "emit_queued drained={} intents_left={}",
            drained.len(),
            intents_left
        ),
        drained.last().and_then(|d| d.p_up),
        drained.last().and_then(|d| d.edge),
        snapshot.gate_drops_cycle,
        snapshot.gate_drops_total,
        snapshot.selection_status,
    );
    apply_status_fields(
        status_state,
        StrategyActivityKind::PlacingQuotes,
        None,
        intents_left,
        drained.len(),
        0,
        0,
        quote_blocks_from_snapshot(snapshot),
        None,
        None,
        None,
        None,
        None,
    );
}

fn emit_no_actionable_status(
    strategy: &mut MakerRebateMmStrategy,
    status_state: &mut StrategyStatusState,
    now_ms: u64,
    market_projection: &crate::polymarket::state::MarketProjection,
    snapshot: &StrategyRuntimeSnapshot,
) {
    let blocks = sorted_quote_block_items(&snapshot.quote_block_reasons_cycle);
    let block_detail =
        primary_quote_block_detail(&blocks, snapshot.quote_block_detail_cycle.as_ref());
    let primary_block_detail = quote_block_detail_map(snapshot);
    let wait_snapshot = oracle_wait_snapshot(strategy, now_ms, market_projection);
    let wait_reason = wait_snapshot.as_ref().map(|(reason, _, _)| *reason);
    let wait_status = wait_snapshot.as_ref().map(|(_, _, status)| status.clone());
    let gate_drops_cycle = snapshot.gate_drops_cycle;
    let render_sig = hash_status_render_sig((
        wait_reason,
        gate_drops_cycle.oracle,
        gate_drops_cycle.book,
        gate_drops_cycle.stale,
        gate_drops_cycle.open_delay,
        gate_drops_cycle.open_price,
        gate_drops_cycle.entry_time,
        gate_drops_cycle.blocked_market,
        gate_drops_cycle.markout_toxicity,
        blocks.as_slice(),
        wait_snapshot.as_ref().map(|(_, detail, _)| detail),
        primary_block_detail.as_ref(),
    ));
    status_state.set_status_with_cached_render(
        now_ms,
        render_sig,
        || {
            let mut status = wait_status.clone().unwrap_or_else(|| {
                format!(
                    "no_actionable_markets drops(o={} b={} s={} open={} opx={} t={} blk={} tox={})",
                    gate_drops_cycle.oracle,
                    gate_drops_cycle.book,
                    gate_drops_cycle.stale,
                    gate_drops_cycle.open_delay,
                    gate_drops_cycle.open_price,
                    gate_drops_cycle.entry_time,
                    gate_drops_cycle.blocked_market,
                    gate_drops_cycle.markout_toxicity,
                )
            });
            if !blocks.is_empty() {
                status.push_str(" blocks=");
                for (idx, (reason, count)) in blocks.iter().take(3).enumerate() {
                    if idx > 0 {
                        status.push(',');
                    }
                    let _ =
                        std::fmt::Write::write_fmt(&mut status, format_args!("{reason}={count}"));
                }
                if let Some(block_detail) = block_detail.as_ref() {
                    let _ = std::fmt::Write::write_fmt(
                        &mut status,
                        format_args!(" block_detail={block_detail}"),
                    );
                }
            }
            status
        },
        None,
        None,
        snapshot.gate_drops_cycle,
        snapshot.gate_drops_total,
        snapshot.selection_status,
    );
    let activity = match wait_reason {
        Some(OracleWaitReason::WaitingForOracle) => StrategyActivityKind::WaitingForOracle,
        Some(OracleWaitReason::OracleDegraded) => StrategyActivityKind::PausedOracleStale,
        None if !blocks.is_empty() => StrategyActivityKind::IdleQuoteBlocked,
        None => StrategyActivityKind::IdleNoActionableMarket,
    };
    apply_status_fields(
        status_state,
        activity,
        None,
        snapshot.pending_len,
        0,
        0,
        0,
        blocks
            .iter()
            .map(|(reason, count)| QuoteBlockReasonCount {
                reason: (*reason).to_string(),
                count: u64::from(*count),
            })
            .collect(),
        primary_block_detail,
        None,
        None,
        None,
        wait_reason,
    );
}

fn emit_recompute_status(
    strategy: &MakerRebateMmStrategy,
    status_state: &mut StrategyStatusState,
    now_ms: u64,
    reason: QuoteRecomputeReason,
    recompute: &StrategyRecomputeCycleResult,
    snapshot: &StrategyRuntimeSnapshot,
    drained: &[StrategyDecision],
) {
    let queued = snapshot.pending_len + drained.len();
    let first_ranked = recompute.ranked.first();
    let markout_effective_samples = first_ranked
        .map(|market| {
            strategy.market_markout_effective_samples(
                market.up_token.as_str(),
                market.down_token.as_str(),
            )
        })
        .unwrap_or_default();
    let first_oracle_symbol = first_ranked
        .map(|m| m.oracle_symbol.as_str())
        .unwrap_or("-");
    let blocks =
        (queued == 0).then(|| sorted_quote_block_items(&snapshot.quote_block_reasons_cycle));
    let block_detail = blocks.as_ref().and_then(|items| {
        primary_quote_block_detail(items, snapshot.quote_block_detail_cycle.as_ref())
    });
    let primary_block_detail = quote_block_detail_map(snapshot);
    let render_sig = hash_status_render_sig((
        reason.as_str(),
        recompute.ranked.len(),
        queued,
        drained.len(),
        recompute.paired_managed_exit_count,
        first_ranked
            .map(|m| status_display_millis(m.toxicity))
            .unwrap_or_default(),
        first_ranked
            .map(|m| status_display_millis(m.markout_toxicity))
            .unwrap_or_default(),
        markout_effective_samples,
        first_oracle_symbol,
        blocks.as_deref().unwrap_or(&[]),
        primary_block_detail.as_ref(),
    ));
    status_state.set_status_with_cached_render(
        now_ms,
        render_sig,
        || {
            let mut status = format!(
                "reason={} ranked={} queued={} drained={} exits={} shock_toxicity={:.3} markout_toxicity={:.3} markout_samples={} oracle={}",
                reason.as_str(),
                recompute.ranked.len(),
                queued,
                drained.len(),
                recompute.paired_managed_exit_count,
                first_ranked.map(|m| m.toxicity).unwrap_or_default(),
                first_ranked.map(|m| m.markout_toxicity).unwrap_or_default(),
                markout_effective_samples,
                first_oracle_symbol,
            );
            if let Some(blocks) = blocks.as_ref() {
                if !blocks.is_empty() {
                    status.push_str(" blocks=");
                    for (idx, (reason, count)) in blocks.iter().take(3).enumerate() {
                        if idx > 0 {
                            status.push(',');
                        }
                        let _ = std::fmt::Write::write_fmt(
                            &mut status,
                            format_args!("{reason}={count}"),
                        );
                    }
                    if let Some(block_detail) = block_detail.as_ref() {
                        let _ = std::fmt::Write::write_fmt(
                            &mut status,
                            format_args!(" block_detail={block_detail}"),
                        );
                    }
                }
            }
            status
        },
        drained.last().and_then(|d| d.p_up),
        drained.last().and_then(|d| d.edge),
        snapshot.gate_drops_cycle,
        snapshot.gate_drops_total,
        snapshot.selection_status,
    );
    apply_status_fields(
        status_state,
        if queued > 0 {
            StrategyActivityKind::PlacingQuotes
        } else {
            StrategyActivityKind::Quoting
        },
        Some(map_recompute_reason(reason)),
        queued,
        drained.len(),
        recompute.ranked.len(),
        recompute.paired_managed_exit_count,
        blocks
            .unwrap_or_default()
            .into_iter()
            .map(|(reason, count)| QuoteBlockReasonCount {
                reason: reason.to_string(),
                count: u64::from(count),
            })
            .collect(),
        primary_block_detail,
        first_ranked.map(|m| m.toxicity),
        first_ranked.map(|m| m.markout_toxicity),
        Some(markout_effective_samples as u64),
        None,
    );
}

pub fn drive_tick(
    strategy: &mut MakerRebateMmStrategy,
    fact_sink: &mut StrategyFactSink,
    runtime_state: &mut StrategyRuntimeState,
    status_state: &mut StrategyStatusState,
    tick: &MarketTick,
    max_decisions: usize,
) -> StrategyTickOutcome {
    if max_decisions == 0 {
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: Vec::new(),
            status: None,
        };
    }

    let Some((now_ms, market_projection)) = strategy.apply_tick_and_get_projection(tick) else {
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: Vec::new(),
            status: None,
        };
    };

    update_invalid_tick_control(
        strategy,
        &mut runtime_state.control,
        &market_projection,
        now_ms,
    );
    if runtime_state.control.tick_kill_switch {
        strategy.clear_pending_decisions();
        let snapshot = runtime_snapshot(strategy);
        emit_tick_kill_switch_status(status_state, now_ms, &snapshot);
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: fact_sink.drain(),
            status: status_state.last_status.clone(),
        };
    }

    if strategy.has_pending_decisions() {
        let drained = strategy.drain_pending_decisions(max_decisions);
        let snapshot = runtime_snapshot(strategy);
        emit_queued_status(
            status_state,
            now_ms,
            &snapshot,
            &drained,
            snapshot.pending_len,
        );
        return StrategyTickOutcome {
            decisions: drained,
            facts: fact_sink.drain(),
            status: status_state.last_status.clone(),
        };
    }

    let Some(reason) = next_recompute_reason(
        strategy,
        &mut runtime_state.control,
        &market_projection,
        now_ms,
    ) else {
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: fact_sink.drain(),
            status: None,
        };
    };

    if !reason.bypass_rebalance_interval()
        && !rebalance_gate_elapsed(strategy, &runtime_state.control, now_ms)
    {
        defer_rebalance_until_cadence(strategy, &mut runtime_state.control);
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: fact_sink.drain(),
            status: None,
        };
    }
    runtime_state.control.deferred_rebalance_deadline_ms = None;
    runtime_state.control.last_rebalance_ms = now_ms;

    let mut recompute = recompute_cycle_result(strategy, fact_sink, now_ms, &market_projection);

    if recompute.ranked.is_empty() && recompute.decisions.is_empty() {
        let snapshot = runtime_snapshot(strategy);
        emit_no_actionable_status(
            strategy,
            status_state,
            now_ms,
            &market_projection,
            &snapshot,
        );
        update_recompute_trigger_state(
            strategy,
            &mut runtime_state.control,
            &market_projection,
            now_ms,
        );
        return StrategyTickOutcome {
            decisions: Vec::new(),
            facts: fact_sink.drain(),
            status: status_state.last_status.clone(),
        };
    }

    if recompute.decisions.len() > max_decisions {
        let overflow = recompute.decisions.split_off(max_decisions);
        strategy.prepend_pending_decisions(overflow);
    }
    let snapshot = runtime_snapshot(strategy);
    emit_recompute_status(
        strategy,
        status_state,
        now_ms,
        reason,
        &recompute,
        &snapshot,
        &recompute.decisions,
    );
    update_recompute_trigger_state(
        strategy,
        &mut runtime_state.control,
        &market_projection,
        now_ms,
    );
    StrategyTickOutcome {
        decisions: recompute.decisions,
        facts: fact_sink.drain(),
        status: status_state.last_status.clone(),
    }
}

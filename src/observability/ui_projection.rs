use super::parsers::{parse_recent_exit_signal, parse_recent_risk_block};
use super::*;
use std::collections::HashMap;

const UI_MARKET_SNAPSHOT_GRACE_MS: u64 = 3 * 60 * 60 * 1000;
const UI_MARKET_SNAPSHOT_MAX: usize = 2_048;
pub(crate) const UI_CEX_QUOTES_MAX: usize = 256;
pub(crate) const UI_CEX_QUOTE_STALE_MS: u64 = 15 * 60 * 1000;
const UI_ORACLE_TICKS_MAX_PER_SYMBOL: usize = 1_000;
const UI_ORACLE_CANDLES_MAX_PER_STREAM: usize = 512;

mod marks;
pub(crate) use marks::{asset_label_for_closed_trade, prune_ui_cex_quotes, prune_ui_market_cache};

impl Observability {
    pub(crate) fn apply_to_ui(&self, event: &ActionEvent) {
        self.ui_state_tx.send_modify(|state| {
            state.counters.actions = state.counters.actions.saturating_add(1);

            match event {
                ActionEvent::OracleTick {
                    ts_ms,
                    px,
                    vol,
                    symbol,
                } => {
                    let snapshot = OracleSnapshot {
                        ts_ms: *ts_ms,
                        symbol: symbol.clone(),
                        px: *px,
                        vol: *vol,
                    };
                    let ticks = state.oracle_tick_history.entry(symbol.clone()).or_default();
                    if let Some(last) = ticks.back_mut() {
                        if last.ts_ms == *ts_ms {
                            *last = snapshot.clone();
                        } else {
                            ticks.push_back(snapshot.clone());
                        }
                    } else {
                        ticks.push_back(snapshot.clone());
                    }
                    while ticks.len() > UI_ORACLE_TICKS_MAX_PER_SYMBOL {
                        let _ = ticks.pop_front();
                    }
                    state.oracle_quotes.insert(symbol.clone(), snapshot.clone());
                    state.last_oracle = Some(snapshot);
                }
                ActionEvent::OracleStep {
                    ts_ms,
                    symbol,
                    timeframe,
                    open,
                    high,
                    low,
                    close,
                    volume,
                } => {
                    let key = format!("{}:{}", symbol.to_ascii_uppercase(), timeframe);
                    let candles = state.oracle_candles.entry(key).or_default();
                    let snapshot = crate::observability::ui::OracleCandleSnapshot {
                        symbol: symbol.clone(),
                        timeframe: timeframe.clone(),
                        ts_ms: *ts_ms,
                        open: *open,
                        high: *high,
                        low: *low,
                        close: *close,
                        volume: *volume,
                    };
                    if let Some(last) = candles.back_mut() {
                        if last.ts_ms == *ts_ms {
                            *last = snapshot;
                        } else {
                            candles.push_back(snapshot);
                        }
                    } else {
                        candles.push_back(snapshot);
                    }
                    while candles.len() > UI_ORACLE_CANDLES_MAX_PER_STREAM {
                        let _ = candles.pop_front();
                    }
                }
                ActionEvent::OracleRuntimeStatus {
                    ts_ms,
                    phase,
                    connected,
                    required_symbols,
                    ready_symbols,
                    symbol_last_tick_ts_ms,
                    symbol_last_tick_age_ms,
                    last_error,
                } => {
                    state.oracle_runtime =
                        Some(crate::observability::ui::OracleRuntimeStatusSnapshot {
                            ts_ms: *ts_ms,
                            phase: phase.clone(),
                            connected: *connected,
                            required_symbols: required_symbols.clone(),
                            ready_symbols: ready_symbols.clone(),
                            symbol_last_tick_ts_ms: symbol_last_tick_ts_ms.clone(),
                            symbol_last_tick_age_ms: symbol_last_tick_age_ms.clone(),
                            last_error: last_error.clone(),
                        });
                }
                ActionEvent::CexQuote {
                    ts_ms,
                    provider,
                    symbol,
                    bid,
                    ask,
                    bid_sz,
                    ask_sz,
                } => {
                    let key = format!("{provider}:{symbol}");
                    state.cex_quotes.insert(
                        key,
                        CexQuoteSnapshot {
                            ts_ms: *ts_ms,
                            provider: provider.clone(),
                            symbol: symbol.clone(),
                            bid: *bid,
                            ask: *ask,
                            bid_sz: *bid_sz,
                            ask_sz: *ask_sz,
                        },
                    );
                    prune_ui_cex_quotes(state, *ts_ms);
                }
                ActionEvent::Signal { .. } => {
                    state.counters.signals = state.counters.signals.saturating_add(1);
                    if let ActionEvent::Signal {
                        ts_ms,
                        side,
                        p_up,
                        edge,
                        reason,
                    } = event
                    {
                        state.last_signal = Some(SignalSnapshot {
                            ts_ms: *ts_ms,
                            side: side.clone(),
                            p_up: *p_up,
                            edge: *edge,
                            reason: reason.clone(),
                        });
                        if let Some(snapshot) =
                            parse_recent_exit_signal(state, *ts_ms, side, reason)
                        {
                            state.recent_exit_execution = Some(snapshot);
                        }
                    }
                }
                ActionEvent::StrategyStatus {
                    ts_ms,
                    kind,
                    status,
                    p_up,
                    edge,
                    gate_drop_oracle,
                    gate_drop_book,
                    gate_drop_stale,
                    gate_drop_open_delay,
                    gate_drop_open_price,
                    gate_drop_entry_time,
                    gate_drop_blocked_market,
                    gate_drop_markout_toxicity,
                    gate_drop_oracle_total,
                    gate_drop_book_total,
                    gate_drop_stale_total,
                    gate_drop_open_delay_total,
                    gate_drop_open_price_total,
                    gate_drop_entry_time_total,
                    gate_drop_blocked_market_total,
                    gate_drop_markout_toxicity_total,
                    selection_status,
                    activity,
                    recompute_reason,
                    queued,
                    drained,
                    ranked,
                    managed_exits,
                    quote_blocks,
                    primary_block_detail,
                    shock_toxicity,
                    markout_toxicity,
                    markout_samples,
                    oracle_wait_reason,
                } => {
                    state.last_strategy_status = Some(StrategyStatusSnapshot {
                        ts_ms: *ts_ms,
                        kind: kind.clone(),
                        status: status.clone(),
                        p_up: *p_up,
                        edge: *edge,
                        gate_drop_oracle: *gate_drop_oracle,
                        gate_drop_book: *gate_drop_book,
                        gate_drop_stale: *gate_drop_stale,
                        gate_drop_open_delay: *gate_drop_open_delay,
                        gate_drop_open_price: *gate_drop_open_price,
                        gate_drop_entry_time: *gate_drop_entry_time,
                        gate_drop_blocked_market: *gate_drop_blocked_market,
                        gate_drop_markout_toxicity: *gate_drop_markout_toxicity,
                        gate_drop_oracle_total: *gate_drop_oracle_total,
                        gate_drop_book_total: *gate_drop_book_total,
                        gate_drop_stale_total: *gate_drop_stale_total,
                        gate_drop_open_delay_total: *gate_drop_open_delay_total,
                        gate_drop_open_price_total: *gate_drop_open_price_total,
                        gate_drop_entry_time_total: *gate_drop_entry_time_total,
                        gate_drop_blocked_market_total: *gate_drop_blocked_market_total,
                        gate_drop_markout_toxicity_total: *gate_drop_markout_toxicity_total,
                        selection_status: *selection_status,
                        activity: *activity,
                        recompute_reason: *recompute_reason,
                        queued: *queued,
                        drained: *drained,
                        ranked: *ranked,
                        managed_exits: *managed_exits,
                        quote_blocks: quote_blocks.clone(),
                        primary_block_detail: primary_block_detail.clone(),
                        shock_toxicity: *shock_toxicity,
                        markout_toxicity: *markout_toxicity,
                        markout_samples: *markout_samples,
                        oracle_wait_reason: *oracle_wait_reason,
                    });
                }
                ActionEvent::OrderIntent {
                    ts_ms,
                    side,
                    token,
                    px,
                    size,
                    time_in_force,
                    post_only,
                    ..
                } => {
                    state.counters.order_intents = state.counters.order_intents.saturating_add(1);
                    state.last_order_intent = Some(OrderIntentSnapshot {
                        ts_ms: *ts_ms,
                        side: side.clone(),
                        token: token.clone(),
                        px: *px,
                        size: *size,
                        time_in_force: time_in_force.clone(),
                        post_only: *post_only,
                    });
                    if let Some(exit) = state.recent_exit_execution.as_mut() {
                        if exit.token == *token
                            && exit.side.eq_ignore_ascii_case(side)
                            && ts_ms.saturating_sub(exit.ts_ms) <= 120_000
                        {
                            exit.intent_px = Some(*px);
                            exit.intent_qty = Some(*size);
                        }
                    }
                }
                ActionEvent::OrderAck { ts_ms, order_id } => {
                    state.counters.order_acks = state.counters.order_acks.saturating_add(1);
                    state.last_order_ack = Some(OrderAckSnapshot {
                        ts_ms: *ts_ms,
                        order_id: order_id.clone(),
                    });
                    if let Some(exit) = state.recent_exit_execution.as_mut() {
                        if ts_ms.saturating_sub(exit.ts_ms) <= 120_000 {
                            exit.ack_observed = true;
                            exit.ack_ts_ms = Some(*ts_ms);
                        }
                    }
                }
                ActionEvent::Fill {
                    ts_ms,
                    token,
                    side,
                    px,
                    size,
                    ..
                } => {
                    state.counters.fills = state.counters.fills.saturating_add(1);
                    if let Some(exit) = state.recent_exit_execution.as_mut() {
                        if exit.token == *token
                            && exit.side.eq_ignore_ascii_case(side)
                            && ts_ms.saturating_sub(exit.ts_ms) <= 120_000
                        {
                            exit.fill_observed = true;
                            exit.fill_ts_ms = Some(*ts_ms);
                            exit.fill_px = Some(*px);
                            exit.fill_qty = Some(*size);
                        }
                    }
                }
                ActionEvent::RiskBlock {
                    ts_ms,
                    rule,
                    details,
                } => {
                    state.counters.risk_blocks = state.counters.risk_blocks.saturating_add(1);
                    state.recent_risk_block = Some(parse_recent_risk_block(*ts_ms, rule, details));
                }
                ActionEvent::RuntimeIssue {
                    ts_ms,
                    scope,
                    kind,
                    msg,
                    detail,
                } => {
                    state.counters.errors = state.counters.errors.saturating_add(1);
                    state.last_error = Some(ErrorSnapshot {
                        ts_ms: *ts_ms,
                        scope: scope.clone(),
                        msg: msg.clone().unwrap_or_else(|| format!("{kind:?}")),
                        kind: Some(*kind),
                        detail: detail.clone(),
                    });
                }
                ActionEvent::MarketSelected {
                    ts_ms,
                    condition_id,
                    up_token,
                    down_token,
                    window_start_ms,
                    window_end_ms,
                    question,
                    slug,
                    underlying,
                } => {
                    if let Some(name) = question.as_ref().or(slug.as_ref()) {
                        state
                            .market_names
                            .insert(condition_id.clone(), name.clone());
                    }
                    let snap = MarketSnapshot {
                        ts_ms: *ts_ms,
                        condition_id: condition_id.clone(),
                        up_token: up_token.clone(),
                        down_token: down_token.clone(),
                        window_start_ms: *window_start_ms,
                        window_end_ms: *window_end_ms,
                        question: question.clone(),
                        slug: slug.clone(),
                        underlying: underlying.clone(),
                    };
                    state
                        .token_to_condition_id
                        .insert(up_token.clone(), condition_id.clone());
                    state
                        .token_to_condition_id
                        .insert(down_token.clone(), condition_id.clone());
                    state
                        .market_snapshots
                        .insert(condition_id.clone(), snap.clone());
                    state.last_market = Some(snap);
                    // Avoid showing stale "next" info when the current market rolls.
                    state.next_market = None;
                    let active_market_ids: HashSet<MarketId> = HashSet::new();
                    prune_ui_market_cache(state, &active_market_ids, *ts_ms);
                }
                ActionEvent::NextMarketSelected {
                    ts_ms,
                    condition_id,
                    up_token,
                    down_token,
                    window_start_ms,
                    window_end_ms,
                    question,
                    slug,
                    underlying,
                } => {
                    if let Some(name) = question.as_ref().or(slug.as_ref()) {
                        state
                            .market_names
                            .insert(condition_id.clone(), name.clone());
                    }
                    let snap = MarketSnapshot {
                        ts_ms: *ts_ms,
                        condition_id: condition_id.clone(),
                        up_token: up_token.clone(),
                        down_token: down_token.clone(),
                        window_start_ms: *window_start_ms,
                        window_end_ms: *window_end_ms,
                        question: question.clone(),
                        slug: slug.clone(),
                        underlying: underlying.clone(),
                    };
                    state
                        .token_to_condition_id
                        .insert(up_token.clone(), condition_id.clone());
                    state
                        .token_to_condition_id
                        .insert(down_token.clone(), condition_id.clone());
                    state
                        .market_snapshots
                        .insert(condition_id.clone(), snap.clone());
                    state.next_market = Some(snap);
                    let active_market_ids: HashSet<MarketId> = HashSet::new();
                    prune_ui_market_cache(state, &active_market_ids, *ts_ms);
                }
                ActionEvent::NextMarketCleared { .. } => {
                    state.next_market = None;
                }
                ActionEvent::ReclaimResolved { .. } => {
                    state.last_error = None;
                }
                ActionEvent::LocalOrderBlock {
                    ts_ms,
                    reason,
                    token,
                    side,
                    detail,
                    raw,
                } => {
                    state.recent_local_order_block = Some(LocalOrderBlockSnapshot {
                        ts_ms: *ts_ms,
                        reason: reason.clone(),
                        token: token.clone(),
                        side: side.clone(),
                        detail: detail.clone().into_iter().collect(),
                        raw: raw.clone(),
                    });
                }
            }
        });
    }
}

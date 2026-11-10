use super::*;

pub(super) fn effective_admission_buffer_ticks(
    strategy_cfg: &MakerRebateMmStrategyConfig,
    extra_buffer_ticks: u8,
) -> u8 {
    strategy_cfg
        .admission_buffer_ticks
        .saturating_add(extra_buffer_ticks)
        .saturating_add(POST_ONLY_VENUE_RACE_SAFETY_TICKS)
}

pub(super) fn active_post_only_reject_cooldown_reason(
    rejects: &HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    now_ms: u64,
) -> Option<String> {
    if !is_unsafe_buy_without_close_hint(intent) {
        return None;
    }
    let state = active_post_only_reject_state(rejects, intent, now_ms)?;
    let remaining_ms = state.cooldown_until_ms.saturating_sub(now_ms);
    Some(format!(
        "local_order_block post_only_reject_cooldown token={} side={:?} streak={} remaining_ms={remaining_ms}",
        intent.instrument, intent.side(), state.streak
    ))
}

pub(super) fn post_only_reject_key(intent: &OrderIntent) -> Option<String> {
    if !intent.post_only || !is_unsafe_buy_without_close_hint(intent) {
        return None;
    }
    let token = intent.instrument.trim();
    (!token.is_empty()).then_some(token.to_string())
}

pub(super) fn active_post_only_reject_state<'a>(
    rejects: &'a HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    now_ms: u64,
) -> Option<&'a PostOnlyRejectState> {
    let key = post_only_reject_key(intent)?;
    let state = rejects.get(&key)?;
    (state.cooldown_until_ms > now_ms && state.streak > 0).then_some(state)
}

pub(super) fn active_post_only_extra_buffer_ticks(
    rejects: &HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    now_ms: u64,
) -> u8 {
    active_post_only_reject_state(rejects, intent, now_ms)
        .map(|state| {
            state
                .streak
                .min(strategy_cfg.post_only_reject_max_extra_buffer_ticks)
        })
        .unwrap_or(0)
}

pub(super) fn active_post_only_size_multiplier(
    rejects: &HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    now_ms: u64,
) -> f64 {
    let extra_ticks =
        active_post_only_extra_buffer_ticks(rejects, intent, strategy_cfg, now_ms) as i32;
    if extra_ticks <= 0 {
        return 1.0;
    }
    strategy_cfg
        .post_only_reject_size_multiplier
        .powi(extra_ticks)
        .clamp(0.0, 1.0)
}

pub(super) fn register_post_only_reject(
    rejects: &mut HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    market_projection: Option<&crate::polymarket::state::MarketProjection>,
    now_ms: u64,
) {
    let Some(reject_key) = post_only_reject_key(intent) else {
        return;
    };
    let current_window_end_ms = market_selection_for_intent(intent, market_projection)
        .map(|selection| selection.window_end_ms)
        .filter(|window_end_ms| *window_end_ms > now_ms)
        .unwrap_or(0);
    let next_streak = rejects
        .get(&reject_key)
        .map(|state| {
            let same_market_window =
                current_window_end_ms > 0 && state.window_end_ms == current_window_end_ms;
            if same_market_window || state.cooldown_until_ms > now_ms {
                state.streak
            } else {
                0
            }
        })
        .unwrap_or(0)
        .saturating_add(1)
        .min(strategy_cfg.post_only_reject_max_extra_buffer_ticks.max(1));

    let state = rejects.entry(reject_key).or_default();
    state.streak = next_streak;
    state.cooldown_until_ms = now_ms.saturating_add(strategy_cfg.post_only_reject_cooldown_ms);
    state.window_end_ms = current_window_end_ms;
}

pub(super) fn register_clean_post_only_ack(
    rejects: &mut HashMap<String, PostOnlyRejectState>,
    intent: &OrderIntent,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    now_ms: u64,
) {
    let Some(key) = post_only_reject_key(intent) else {
        return;
    };
    let Some(state) = rejects.get_mut(&key) else {
        return;
    };
    if state.window_end_ms > now_ms {
        // Preserve same-window reject memory so a later repeat reject on the
        // same token can still escalate, while clearing any active cooldown.
        state.cooldown_until_ms = now_ms;
        return;
    }
    if state.streak <= 1 {
        rejects.remove(&key);
        return;
    }
    state.streak = state.streak.saturating_sub(1);
    state.cooldown_until_ms = now_ms.saturating_add(strategy_cfg.post_only_reject_cooldown_ms);
}

pub(super) fn post_only_gap_ticks(admission_buffer_ticks: u8) -> f64 {
    1.0 + f64::from(admission_buffer_ticks)
}

pub(super) fn max_maker_buy_px(ask: f64, tick: f64, admission_buffer_ticks: u8) -> Option<f64> {
    if !(ask.is_finite() && tick.is_finite() && tick > 0.0) {
        return None;
    }
    let gap = post_only_gap_ticks(admission_buffer_ticks) * tick;
    if ask <= gap {
        return None;
    }
    Some(round_to_tick((ask - gap).max(tick), tick, false).clamp(tick, (1.0 - tick).max(tick)))
}

pub(super) fn min_maker_sell_px(bid: f64, tick: f64, admission_buffer_ticks: u8) -> Option<f64> {
    if !(bid.is_finite() && tick.is_finite() && tick > 0.0) {
        return None;
    }
    let gap = post_only_gap_ticks(admission_buffer_ticks) * tick;
    if !(bid > 0.0 && bid < 1.0 - gap) {
        return None;
    }
    Some(round_to_tick((bid + gap).min(1.0 - tick), tick, true).clamp(tick, (1.0 - tick).max(tick)))
}

pub(super) fn enforce_post_only_non_crossing(
    intent: &mut OrderIntent,
    odds_state: Option<&PolymarketOddsState>,
    admission_buffer_ticks: u8,
) {
    if !intent.post_only || is_ctf_market_entry_sell_intent(intent) {
        return;
    }
    let Some(odds) = odds_state else {
        return;
    };
    let Ok(token_id) = U256::from_str(intent.instrument.trim()) else {
        return;
    };
    let Some(top) = crate::polymarket::state::book_for_token(odds, token_id) else {
        return;
    };
    let tick = top.tick_size;
    if !is_valid_tick_size(tick) {
        return;
    }

    match intent.side() {
        Side::Buy => {
            let Some(ask) = top.ask else {
                return;
            };
            let Some(max_maker_px) = max_maker_buy_px(ask, tick, admission_buffer_ticks) else {
                return;
            };
            if intent.limit_px > max_maker_px + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM {
                intent.limit_px = max_maker_px;
            }
        }
        Side::Sell => {
            let Some(bid) = top.bid else {
                return;
            };
            let Some(min_maker_px) = min_maker_sell_px(bid, tick, admission_buffer_ticks) else {
                return;
            };
            if intent.limit_px + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM < min_maker_px {
                intent.limit_px = min_maker_px;
            }
        }
    }
}

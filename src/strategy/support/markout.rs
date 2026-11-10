// Strategy support required for behavior: post-fill toxicity controls that
// throttle or suppress future paired entries.
use super::*;
use crate::portfolio::FillSide;

#[derive(Debug, Clone)]
pub(crate) struct PendingMarkout {
    pub(crate) token: String,
    pub(crate) side: FillSide,
    pub(crate) fill_px: f64,
    pub(crate) fill_ts_ms: u64,
    pub(crate) windows_done: Vec<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct MarkoutState {
    pub(super) ewma_by_window: Vec<f64>,
    pub(in crate::strategy) sample_count: usize,
    pub(super) entry_cooldown_until_ms: u64,
    pub(super) last_update_ts_ms: Option<u64>,
    pub(super) last_adverse_markout: Option<f64>,
    pub(super) last_adverse_markout_ts_ms: Option<u64>,
}

impl MarkoutState {
    pub(super) fn new(windows: usize) -> Self {
        Self {
            ewma_by_window: vec![0.0; windows],
            sample_count: 0,
            entry_cooldown_until_ms: 0,
            last_update_ts_ms: None,
            last_adverse_markout: None,
            last_adverse_markout_ts_ms: None,
        }
    }
}

impl MakerRebateMmStrategy {
    pub(super) fn markout_windows(&self) -> &[u64] {
        &self.cfg.markout_windows_ms
    }

    pub(crate) fn next_pending_markout_deadline_ms(&self, after_ms: u64) -> Option<u64> {
        self.pending_markouts
            .iter()
            .flat_map(|pending| {
                self.markout_windows()
                    .iter()
                    .enumerate()
                    .filter_map(move |(index, window_ms)| {
                        (!pending.windows_done.get(index).copied().unwrap_or(false))
                            .then_some(pending.fill_ts_ms.saturating_add(*window_ms))
                    })
            })
            .filter(|deadline_ms| *deadline_ms > after_ms)
            .min()
    }

    pub(super) fn markout_decay_factor(last_update_ts_ms: Option<u64>, now_ms: u64) -> f64 {
        let Some(last_update_ts_ms) = last_update_ts_ms else {
            return 1.0;
        };
        let age_ms = now_ms.saturating_sub(last_update_ts_ms);
        if age_ms == 0 {
            return 1.0;
        }
        0.5_f64.powf(age_ms as f64 / MARKOUT_TOXICITY_HALF_LIFE_MS as f64)
    }

    pub(crate) fn markout_toxicity_at(&self, token: &str, now_ms: u64) -> f64 {
        let Some(state) = self.markout_state.get(token) else {
            return 0.0;
        };
        if state.ewma_by_window.is_empty() {
            return 0.0;
        }
        let decay = Self::markout_decay_factor(state.last_update_ts_ms, now_ms);
        let mut weighted_penalty = 0.0;
        let mut total_weight = 0.0;
        let weights = &self.cfg.markout_window_weights;
        for (idx, value) in state.ewma_by_window.iter().enumerate() {
            let w = weights
                .get(idx)
                .copied()
                .unwrap_or(*weights.last().unwrap_or(&0.1));
            if *value < 0.0 {
                weighted_penalty += (-*value) * decay * w;
            }
            total_weight += w;
        }
        if total_weight <= 0.0 {
            return 0.0;
        }
        let penalty = weighted_penalty / total_weight;
        (penalty / self.cfg.base_half_spread.max(1e-4)).clamp(0.0, 3.0)
    }

    pub(in crate::strategy) fn update_markout_state(
        &mut self,
        now_ms: u64,
        token: &str,
        window_idx: usize,
        value: f64,
    ) {
        let windows = self.markout_windows().len();
        let state = self
            .markout_state
            .entry(token.to_string())
            .or_insert_with(|| MarkoutState::new(windows));
        if state.ewma_by_window.len() < windows {
            state.ewma_by_window.resize(windows, 0.0);
        }
        let alpha = 0.25;
        let prev = state
            .ewma_by_window
            .get(window_idx)
            .copied()
            .unwrap_or_default();
        state.ewma_by_window[window_idx] = (1.0 - alpha) * prev + alpha * value;
        state.sample_count = state.sample_count.saturating_add(1);
        state.last_update_ts_ms = Some(now_ms);
        let mut weighted_penalty = 0.0;
        let mut total_weight = 0.0;
        let weights = &self.cfg.markout_window_weights;
        for (idx, markout) in state.ewma_by_window.iter().enumerate() {
            let weight = weights
                .get(idx)
                .copied()
                .unwrap_or(*weights.last().unwrap_or(&0.1));
            if *markout < 0.0 {
                weighted_penalty += (-*markout) * weight;
            }
            total_weight += weight;
        }
        let toxicity = if total_weight > 0.0 {
            ((weighted_penalty / total_weight) / self.cfg.base_half_spread.max(1e-4))
                .clamp(0.0, 3.0)
        } else {
            0.0
        };
        let severe_adverse_markout_threshold = -self
            .cfg
            .base_half_spread
            .max(MARKOUT_REENTRY_COOLDOWN_MARKOUT_THRESHOLD.abs());
        let severe_adverse_markout = window_idx <= 1 && value <= severe_adverse_markout_threshold;
        let ewma_cooldown_trigger = window_idx <= 1
            && value <= MARKOUT_REENTRY_COOLDOWN_MARKOUT_THRESHOLD
            && toxicity >= MARKOUT_REENTRY_COOLDOWN_TRIGGER;
        if severe_adverse_markout || ewma_cooldown_trigger {
            state.entry_cooldown_until_ms = state
                .entry_cooldown_until_ms
                .max(now_ms.saturating_add(MARKOUT_REENTRY_COOLDOWN_MS));
            state.last_adverse_markout = Some(value);
            state.last_adverse_markout_ts_ms = Some(now_ms);
        }
    }

    pub(crate) fn process_fills_and_markouts(&mut self, now_ms: u64) {
        let new_fills: Vec<_> = {
            let portfolio = self.portfolio_rx.borrow();
            portfolio
                .fills
                .iter()
                .filter(|queued| queued.seq > self.last_fill_seq)
                .cloned()
                .collect()
        };
        for queued in new_fills {
            self.last_fill_seq = self.last_fill_seq.max(queued.seq);
            let fill = &queued.fill;
            if !(fill.size.is_finite() && fill.size > 0.0 && fill.px.is_finite() && fill.px > 0.0) {
                continue;
            }
            let windows = self.markout_windows().len();
            self.pending_markouts.push(PendingMarkout {
                token: fill.token.clone(),
                side: fill.side,
                fill_px: fill.px,
                fill_ts_ms: fill.ts_ms,
                windows_done: vec![false; windows],
            });
            if self.pending_markouts.len() > MARKOUT_MAX_PENDING {
                let overflow = self.pending_markouts.len() - MARKOUT_MAX_PENDING;
                self.pending_markouts.drain(0..overflow);
            }
        }

        if self.pending_markouts.is_empty() {
            return;
        }

        let windows = self.markout_windows().to_vec();
        let mut markout_updates: Vec<(String, usize, f64)> = Vec::new();
        for pending in &mut self.pending_markouts {
            let Some(fair_now) = self.last_fair_by_token.get(&pending.token).copied() else {
                continue;
            };
            for (w_idx, w_ms) in windows.iter().enumerate() {
                if pending.windows_done.get(w_idx).copied().unwrap_or(false) {
                    continue;
                }
                let due_ts = pending.fill_ts_ms.saturating_add(*w_ms);
                if now_ms < due_ts {
                    continue;
                }
                let markout = match pending.side {
                    FillSide::Buy => fair_now - pending.fill_px,
                    FillSide::Sell => pending.fill_px - fair_now,
                };
                if let Some(done) = pending.windows_done.get_mut(w_idx) {
                    *done = true;
                }
                markout_updates.push((pending.token.clone(), w_idx, markout));
            }
        }

        for (token, idx, markout) in markout_updates {
            self.update_markout_state(now_ms, &token, idx, markout);
        }
        self.pending_markouts
            .retain(|pending| !pending.windows_done.iter().all(|done| *done));
    }

    pub(super) fn markout_effective_samples(&self, token: &str) -> usize {
        let windows = self.markout_windows().len().max(1);
        self.markout_state
            .get(token)
            .map(|state| state.sample_count / windows)
            .unwrap_or(0)
    }

    pub(crate) fn market_markout_effective_samples(
        &self,
        up_token: &str,
        down_token: &str,
    ) -> usize {
        self.markout_effective_samples(up_token)
            .max(self.markout_effective_samples(down_token))
    }
}

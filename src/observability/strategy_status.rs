use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use crate::polymarket::identity::MarketId;
use crate::strategy::support::shared::oracle_state::{
    CaptureModeObs as CaptureMode, OpenPriceState, PxModeObs as PxMode,
};
use crate::strategy::StrategyGateDropCounters;
use crate::types::MarketTick;

#[derive(Debug, Clone, Copy, serde::Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyActivityKind {
    Starting,
    HaltedTickGuard,
    PausedOracleStale,
    WaitingForOracle,
    IdleNoActionableMarket,
    WaitingRebalance,
    PlacingQuotes,
    IdleQuoteBlocked,
    Quoting,
    Running,
}

impl StrategyActivityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::HaltedTickGuard => "halted_tick_guard",
            Self::PausedOracleStale => "paused_oracle_stale",
            Self::WaitingForOracle => "waiting_for_oracle",
            Self::IdleNoActionableMarket => "idle_no_actionable_market",
            Self::WaitingRebalance => "waiting_rebalance",
            Self::PlacingQuotes => "placing_quotes",
            Self::IdleQuoteBlocked => "idle_quote_blocked",
            Self::Quoting => "quoting",
            Self::Running => "running",
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuoteRecomputeReasonObs {
    Initial,
    OracleChanged,
    PortfolioStateChanged,
    OpenOrdersChanged,
    RiskStateChanged,
    BlockedMarketsChanged,
    ThresholdFired,
    BookChanged,
}

#[derive(Debug, Clone, Copy, serde::Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OracleWaitReason {
    WaitingForOracle,
    OracleDegraded,
}

#[derive(Debug, Clone, serde::Serialize, Hash, PartialEq, Eq)]
pub struct QuoteBlockReasonCount {
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, Hash, PartialEq, Eq)]
pub struct StrategySelectionStatus {
    pub known_markets: u64,
    pub active_now_markets: u64,
    pub active_now_alternative_markets: u64,
    pub active_now_actionable_markets: u64,
    pub active_now_budget_deadzone_markets: u64,
    pub ranked_markets: u64,
    pub ranked_budget_deadzone_markets: u64,
    pub entry_time_blocked_active_markets: u64,
    pub blocked_market_count: u64,
    pub preopen_next_selection_present: bool,
    pub active_now_only_filter_applied: bool,
    pub next_selection_time_to_open_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct StrategyStatus {
    pub ts_ms: u64,
    pub kind: String,
    pub msg: String,
    pub p_up: Option<f64>,
    pub edge: Option<f64>,
    pub gate_drops_cycle: StrategyGateDropCounters,
    pub gate_drops_total: StrategyGateDropCounters,
    pub selection_status: Option<StrategySelectionStatus>,
    pub activity: StrategyActivityKind,
    pub recompute_reason: Option<QuoteRecomputeReasonObs>,
    pub queued: usize,
    pub drained: usize,
    pub ranked: usize,
    pub managed_exits: usize,
    pub quote_blocks: Vec<QuoteBlockReasonCount>,
    pub primary_block_detail: Option<BTreeMap<String, String>>,
    pub shock_toxicity: Option<f64>,
    pub markout_toxicity: Option<f64>,
    pub markout_samples: Option<u64>,
    pub oracle_wait_reason: Option<OracleWaitReason>,
}

impl StrategyStatus {
    pub fn new(
        ts_ms: u64,
        kind: impl Into<String>,
        msg: impl Into<String>,
        p_up: Option<f64>,
        edge: Option<f64>,
    ) -> Self {
        Self {
            ts_ms,
            kind: kind.into(),
            msg: msg.into(),
            p_up,
            edge,
            gate_drops_cycle: StrategyGateDropCounters::default(),
            gate_drops_total: StrategyGateDropCounters::default(),
            selection_status: None,
            activity: StrategyActivityKind::Starting,
            recompute_reason: None,
            queued: 0,
            drained: 0,
            ranked: 0,
            managed_exits: 0,
            quote_blocks: Vec::new(),
            primary_block_detail: None,
            shock_toxicity: None,
            markout_toxicity: None,
            markout_samples: None,
            oracle_wait_reason: None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct StrategyStatusState {
    pub last_status: Option<StrategyStatus>,
    pub last_status_render_sig: Option<u64>,
}

impl StrategyStatusState {
    pub(crate) fn set_status(
        &mut self,
        now_ms: u64,
        msg: impl Into<String>,
        p_up: Option<f64>,
        edge: Option<f64>,
        gate_drops_cycle: StrategyGateDropCounters,
        gate_drops_total: StrategyGateDropCounters,
        selection_status: Option<StrategySelectionStatus>,
    ) {
        self.last_status_render_sig = None;
        self.last_status = Some(StrategyStatus {
            ts_ms: now_ms,
            kind: "maker_rebate_mm".to_string(),
            msg: msg.into(),
            p_up,
            edge,
            gate_drops_cycle,
            gate_drops_total,
            selection_status,
            activity: StrategyActivityKind::Running,
            recompute_reason: None,
            queued: 0,
            drained: 0,
            ranked: 0,
            managed_exits: 0,
            quote_blocks: Vec::new(),
            primary_block_detail: None,
            shock_toxicity: None,
            markout_toxicity: None,
            markout_samples: None,
            oracle_wait_reason: None,
        });
    }

    pub(crate) fn set_status_with_cached_render(
        &mut self,
        now_ms: u64,
        render_sig: u64,
        render: impl FnOnce() -> String,
        p_up: Option<f64>,
        edge: Option<f64>,
        gate_drops_cycle: StrategyGateDropCounters,
        gate_drops_total: StrategyGateDropCounters,
        selection_status: Option<StrategySelectionStatus>,
    ) {
        let msg = if self.last_status_render_sig == Some(render_sig) {
            self.last_status
                .as_ref()
                .map(|status| status.msg.clone())
                .unwrap_or_else(render)
        } else {
            let rendered = render();
            self.last_status_render_sig = Some(render_sig);
            rendered
        };
        self.last_status = Some(StrategyStatus {
            ts_ms: now_ms,
            kind: "maker_rebate_mm".to_string(),
            msg,
            p_up,
            edge,
            gate_drops_cycle,
            gate_drops_total,
            selection_status,
            activity: StrategyActivityKind::Running,
            recompute_reason: None,
            queued: 0,
            drained: 0,
            ranked: 0,
            managed_exits: 0,
            quote_blocks: Vec::new(),
            primary_block_detail: None,
            shock_toxicity: None,
            markout_toxicity: None,
            markout_samples: None,
            oracle_wait_reason: None,
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenCaptureAlertState {
    pub(crate) capture_mode: CaptureMode,
    pub(crate) penalty_bucket: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistentDiagnosticState<T: Copy + Eq> {
    pub(crate) signature: T,
    pub(crate) active_since_ms: u64,
    pub(crate) occurrences: u64,
    pub(crate) last_summary_ms: u64,
}

impl<T: Copy + Eq> PersistentDiagnosticState<T> {
    pub(crate) fn new(signature: T, now_ms: u64) -> Self {
        Self {
            signature,
            active_since_ms: now_ms,
            occurrences: 1,
            last_summary_ms: now_ms,
        }
    }

    pub(crate) fn should_emit_summary(&mut self, now_ms: u64, summary_interval_ms: u64) -> bool {
        self.occurrences = self.occurrences.saturating_add(1);
        if now_ms.saturating_sub(self.last_summary_ms) < summary_interval_ms {
            return false;
        }
        self.last_summary_ms = now_ms;
        true
    }
}

pub(crate) fn hash_status_render_sig(parts: impl Hash) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn status_display_millis(value: f64) -> i64 {
    if !value.is_finite() {
        return 0;
    }
    (value * 1_000.0).round() as i64
}

pub(crate) fn note_oracle_price_mode(
    last_oracle_px_mode: &mut Option<PxMode>,
    oracle_price_degraded_mode: &mut Option<PersistentDiagnosticState<PxMode>>,
    symbol: &str,
    oracle_px_mode: PxMode,
    tick: &MarketTick,
    now_ms: u64,
) {
    let previous_mode = last_oracle_px_mode.replace(oracle_px_mode);
    match oracle_px_mode {
        PxMode::Microprice => {
            if let Some(state) = oracle_price_degraded_mode.take() {
                tracing::debug!(
                    target: "oracle.price_mode",
                    symbol,
                    recovered_mode = ?oracle_px_mode,
                    active_for_ms = now_ms.saturating_sub(state.active_since_ms),
                    occurrences = state.occurrences,
                    "oracle_price_mode_degraded_cleared"
                );
            }
        }
        PxMode::MidpointSecondary => match oracle_price_degraded_mode {
            None => {
                *oracle_price_degraded_mode =
                    Some(PersistentDiagnosticState::new(oracle_px_mode, now_ms));
                tracing::debug!(
                    target: "oracle.price_mode",
                    symbol,
                    previous_mode = ?previous_mode,
                    oracle_px_mode = ?oracle_px_mode,
                    bid = tick.bid,
                    ask = tick.ask,
                    bid_sz = tick.bid_sz,
                    ask_sz = tick.ask_sz,
                    "oracle_price_mode_degraded_entered"
                );
            }
            Some(state) => {
                if state.should_emit_summary(now_ms, 30_000) {
                    tracing::debug!(
                        target: "oracle.price_mode",
                        symbol,
                        oracle_px_mode = ?oracle_px_mode,
                        bid = tick.bid,
                        ask = tick.ask,
                        bid_sz = tick.bid_sz,
                        ask_sz = tick.ask_sz,
                        active_for_ms = now_ms.saturating_sub(state.active_since_ms),
                        occurrences = state.occurrences,
                        "oracle_price_mode_degraded_summary"
                    );
                }
            }
        },
    }
}

pub(crate) fn note_open_capture_diagnostic(
    open_capture_diagnostic_by_market: &mut HashMap<
        MarketId,
        PersistentDiagnosticState<OpenCaptureAlertState>,
    >,
    market_id: &MarketId,
    alert_state: Option<OpenCaptureAlertState>,
    open_state: OpenPriceState,
    open_bracket_penalty: f64,
    now_ms: u64,
) {
    match alert_state {
        Some(alert_state) => match open_capture_diagnostic_by_market.entry(market_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PersistentDiagnosticState::new(alert_state, now_ms));
                tracing::debug!(
                    target: "oracle.open_capture",
                    market_id = %market_id,
                    open_capture_mode = ?open_state.capture_mode,
                    open_left_gap_ms = open_state.left_gap_ms,
                    open_right_gap_ms = open_state.right_gap_ms,
                    open_bracket_span_ms = open_state.span_ms,
                    open_bracket_asymmetry_ms = open_state.asymmetry_ms,
                    open_bracket_penalty,
                    open_bracket_penalty_bucket = alert_state.penalty_bucket,
                    "open_capture_degraded_entered"
                );
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if state.signature != alert_state {
                    *state = PersistentDiagnosticState::new(alert_state, now_ms);
                    tracing::debug!(
                        target: "oracle.open_capture",
                        market_id = %market_id,
                        open_capture_mode = ?open_state.capture_mode,
                        open_left_gap_ms = open_state.left_gap_ms,
                        open_right_gap_ms = open_state.right_gap_ms,
                        open_bracket_span_ms = open_state.span_ms,
                        open_bracket_asymmetry_ms = open_state.asymmetry_ms,
                        open_bracket_penalty,
                        open_bracket_penalty_bucket = alert_state.penalty_bucket,
                        "open_capture_degraded_transition"
                    );
                } else if state.should_emit_summary(now_ms, 30_000) {
                    tracing::debug!(
                        target: "oracle.open_capture",
                        market_id = %market_id,
                        open_capture_mode = ?open_state.capture_mode,
                        open_left_gap_ms = open_state.left_gap_ms,
                        open_right_gap_ms = open_state.right_gap_ms,
                        open_bracket_span_ms = open_state.span_ms,
                        open_bracket_asymmetry_ms = open_state.asymmetry_ms,
                        open_bracket_penalty,
                        open_bracket_penalty_bucket = alert_state.penalty_bucket,
                        active_for_ms = now_ms.saturating_sub(state.active_since_ms),
                        occurrences = state.occurrences,
                        "open_capture_degraded_summary"
                    );
                }
            }
        },
        None => {
            if let Some(state) = open_capture_diagnostic_by_market.remove(market_id) {
                tracing::debug!(
                    target: "oracle.open_capture",
                    market_id = %market_id,
                    open_capture_mode = ?open_state.capture_mode,
                    active_for_ms = now_ms.saturating_sub(state.active_since_ms),
                    occurrences = state.occurrences,
                    "open_capture_degraded_cleared"
                );
            }
        }
    }
}

pub(crate) fn sorted_quote_block_items(
    quote_block_reasons_cycle: &HashMap<&'static str, u32>,
) -> Vec<(&'static str, u32)> {
    let mut items: Vec<(&'static str, u32)> = quote_block_reasons_cycle
        .iter()
        .map(|(reason, count)| (*reason, *count))
        .collect();
    items.sort_by(|(reason_a, count_a), (reason_b, count_b)| {
        count_b.cmp(count_a).then_with(|| reason_a.cmp(reason_b))
    });
    items
}

pub(crate) fn primary_quote_block_detail(
    blocks: &[(&'static str, u32)],
    quote_block_detail_cycle: Option<&(&'static str, String)>,
) -> Option<String> {
    let (reason, _) = blocks.first()?;
    let (detail_reason, raw_detail) = quote_block_detail_cycle?;
    if detail_reason != reason {
        return None;
    }
    encode_status_block_detail(raw_detail)
}

fn encode_status_block_detail(raw_detail: &str) -> Option<String> {
    let detail = raw_detail.trim();
    if detail.is_empty() {
        return None;
    }

    let mut parts = Vec::new();
    let mut saw_reason = false;
    for token in detail.split_whitespace() {
        let cleaned = token.trim_matches(',');
        if cleaned.is_empty() {
            continue;
        }
        if let Some((key, value)) = cleaned.split_once('=') {
            if !key.is_empty() && !value.is_empty() {
                parts.push(format!("{key}:{value}"));
            }
            continue;
        }
        if !saw_reason {
            parts.push(format!("reason:{cleaned}"));
            saw_reason = true;
        }
    }

    (!parts.is_empty()).then_some(parts.join("|"))
}

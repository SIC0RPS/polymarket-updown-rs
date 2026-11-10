// Strategy support required for behavior: pricing/model inputs that feed
// CTF inventory entry and managed-exit decisions.
mod current_blended;
mod strategy_oracle;

use crate::config::CurrentBlendedFairValueConfig;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::state::{BookDepth, BookTop, MarketProjection};
use crate::strategy::paired_maker::{
    MakerRebateMmStrategy, BOOK_STALE_HARD_DROP_MS, BOOK_STALE_SOFT_MAX_MS,
    OPEN_PX_LATE_CONFIDENCE_DECAY_MS, OPEN_PX_LATE_MAX_CONFIDENCE_PENALTY,
    OPEN_PX_LATE_MAX_TOXICITY_PENALTY, OPEN_PX_LATE_PENALTY_CAP, OPEN_PX_MAX_DELAY_MS,
    ORACLE_MAX_ABS_RETURN_BPS_PER_UPDATE, ORACLE_VOL_UPDATE_MIN_DT_MS,
};
use crate::strategy::support::shared::common::{canonical_symbol, ingest_age_ms, ts_ms_from_ns};
use crate::strategy::support::shared::oracle_state::{
    OpenCaptureMode, OpenPriceState, OpenWindowContext, OraclePxMode, OracleVolatilityDiagnostics,
    VolSkipReason,
};
use crate::types::{MarketTick, SourceTsKind};

use super::exit_router::PairedFirstManagedSellFairValues;
use crate::observability::strategy_status::{
    note_open_capture_diagnostic, note_oracle_price_mode, OpenCaptureAlertState,
};

pub use current_blended::CurrentBlended;

#[derive(Debug, Clone, Copy)]
pub struct FairValueContext {
    pub spot_px: f64,
    pub open_px: f64,
    pub horizon_s: f64,
    pub sigma2: f64,
    pub p_market: Option<f64>,
    pub spot_l1_imbalance: Option<f64>,
    pub depth_pressure: f64,
    pub shock_toxicity: f64,
    pub markout_toxicity: f64,
    pub confidence_scale: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct FairValueResult {
    pub p_model_raw: f64,
    pub p_model: f64,
    pub p_fair: f64,
    pub model_weight: Option<f64>,
    pub depth_pressure: f64,
    pub spot_z: Option<f64>,
}

pub trait FairValueModel {
    fn compute(&self, ctx: &FairValueContext) -> FairValueResult;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FairValueEngine;

impl FairValueEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn compute_live_with_config(
        &self,
        ctx: &FairValueContext,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> FairValueResult {
        CurrentBlended::new(cfg).compute(ctx)
    }
}

pub fn market_depth_pressure(up: Option<&BookDepth>, down: Option<&BookDepth>) -> f64 {
    let up_pressure = up.and_then(token_depth_pressure);
    let down_pressure = down.and_then(token_depth_pressure);
    match (up_pressure, down_pressure) {
        (Some(up), Some(down)) => (0.5 * (up - down)).clamp(-1.0, 1.0),
        (Some(up), None) => (0.5 * up).clamp(-1.0, 1.0),
        (None, Some(down)) => (-0.5 * down).clamp(-1.0, 1.0),
        (None, None) => 0.0,
    }
}

fn token_depth_pressure(depth: &BookDepth) -> Option<f64> {
    let weighted = depth_weighted_imbalance(depth)?;
    let l1_size = l1_size_imbalance(depth).unwrap_or(weighted);
    let microprice = l1_microprice_pressure(depth).unwrap_or(l1_size);
    Some((0.50 * microprice + 0.25 * l1_size + 0.25 * weighted).clamp(-1.0, 1.0))
}

fn depth_weighted_imbalance(depth: &BookDepth) -> Option<f64> {
    let bid = weighted_depth_notional(depth.bids.iter().take(5));
    let ask = weighted_depth_notional(depth.asks.iter().take(5));
    normalized_imbalance(bid, ask)
}

fn weighted_depth_notional<'a>(
    levels: impl Iterator<Item = &'a crate::polymarket::state::PriceLevel>,
) -> f64 {
    levels
        .enumerate()
        .filter_map(|(index, level)| {
            valid_level(level).map(|notional| notional / ((index + 1) as f64))
        })
        .sum::<f64>()
}

fn l1_size_imbalance(depth: &BookDepth) -> Option<f64> {
    let bid = depth.bids.first()?;
    let ask = depth.asks.first()?;
    if !(bid.size.is_finite() && bid.size > 0.0 && ask.size.is_finite() && ask.size > 0.0) {
        return None;
    }
    normalized_imbalance(bid.size, ask.size)
}

fn l1_microprice_pressure(depth: &BookDepth) -> Option<f64> {
    let bid = depth.bids.first()?;
    let ask = depth.asks.first()?;
    if !(bid.price.is_finite()
        && ask.price.is_finite()
        && bid.price > 0.0
        && ask.price > 0.0
        && ask.price >= bid.price
        && bid.size.is_finite()
        && ask.size.is_finite()
        && bid.size > 0.0
        && ask.size > 0.0)
    {
        return None;
    }
    let spread = ask.price - bid.price;
    if spread <= 0.0 {
        return l1_size_imbalance(depth);
    }
    let micro = (ask.price * bid.size + bid.price * ask.size) / (bid.size + ask.size);
    let mid = 0.5 * (bid.price + ask.price);
    Some(((micro - mid) / (0.5 * spread)).clamp(-1.0, 1.0))
}

fn normalized_imbalance(left: f64, right: f64) -> Option<f64> {
    let total = left + right;
    (total.is_finite() && total > 0.0).then_some(((left - right) / total).clamp(-1.0, 1.0))
}

fn valid_level(level: &crate::polymarket::state::PriceLevel) -> Option<f64> {
    (level.price.is_finite() && level.price > 0.0 && level.size.is_finite() && level.size > 0.0)
        .then_some(level.price * level.size)
}

pub(crate) fn normal_cdf(x: f64) -> f64 {
    let x = x.clamp(-10.0, 10.0);
    let t = 1.0 / (1.0 + 0.2316419 * x.abs());
    let d = 0.3989423 * (-0.5 * x * x).exp();
    let prob =
        d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    if x >= 0.0 {
        1.0 - prob
    } else {
        prob
    }
}

pub(crate) fn sigmoid(x: f64) -> f64 {
    if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let exp_x = x.exp();
        exp_x / (1.0 + exp_x)
    }
}

pub fn interpolate_log_price(
    t_minus: u64,
    p_minus: f64,
    t_plus: u64,
    p_plus: f64,
    t0: u64,
) -> Option<f64> {
    if !(t_minus < t0
        && t0 <= t_plus
        && p_minus.is_finite()
        && p_plus.is_finite()
        && p_minus > 0.0
        && p_plus > 0.0)
    {
        return None;
    }
    let span_ms = t_plus.saturating_sub(t_minus);
    if span_ms == 0 {
        return None;
    }
    let weight = (t0.saturating_sub(t_minus) as f64) / span_ms as f64;
    let log_open = p_minus.ln() + weight * (p_plus.ln() - p_minus.ln());
    let open_px = log_open.exp();
    (open_px.is_finite() && open_px > 0.0).then_some(open_px)
}

pub fn open_bracket_penalty(span_ms: u64, post_open_projection: bool) -> f64 {
    let late_penalty = if span_ms <= OPEN_PX_MAX_DELAY_MS {
        0.0
    } else {
        ((span_ms - OPEN_PX_MAX_DELAY_MS) as f64 / OPEN_PX_LATE_CONFIDENCE_DECAY_MS as f64)
            .clamp(0.0, OPEN_PX_LATE_PENALTY_CAP)
    };
    let projection_quality_penalty = if post_open_projection && span_ms > 0 {
        (span_ms as f64 / OPEN_PX_MAX_DELAY_MS.max(1) as f64).clamp(0.0, OPEN_PX_LATE_PENALTY_CAP)
    } else {
        0.0
    };
    late_penalty.max(projection_quality_penalty)
}

pub fn open_capture_penalty_bucket(penalty: f64) -> u8 {
    if penalty <= 0.0 {
        0
    } else if penalty <= 0.2 {
        1
    } else if penalty <= 0.5 {
        2
    } else {
        3
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LiveMarketPenaltySurface {
    pub time_left_ms: u64,
    pub stale_soft_penalty: f64,
    pub close_window_penalty: f64,
    pub hard_drop_stale: bool,
}

pub(crate) fn live_market_penalty_surface(
    now_ms: u64,
    up_observed_at_ms: u64,
    down_observed_at_ms: u64,
    window_end_ms: u64,
    ranking_book_max_age_ms: u64,
    hard_entry_gate_ms: u64,
) -> LiveMarketPenaltySurface {
    let book_age_ms = if up_observed_at_ms == 0 || down_observed_at_ms == 0 {
        u64::MAX
    } else {
        match (
            now_ms.checked_sub(up_observed_at_ms),
            now_ms.checked_sub(down_observed_at_ms),
        ) {
            (Some(up_age_ms), Some(down_age_ms)) => up_age_ms.max(down_age_ms),
            _ => u64::MAX,
        }
    };
    let configured_stale_ms = ranking_book_max_age_ms.max(1);
    let hard_stale_ms = configured_stale_ms.max(BOOK_STALE_SOFT_MAX_MS);
    let hard_drop_ms = hard_stale_ms.max(BOOK_STALE_HARD_DROP_MS);
    let stale_soft_penalty = if book_age_ms > hard_stale_ms {
        1.0
    } else if book_age_ms > configured_stale_ms {
        let over_ms = book_age_ms.saturating_sub(configured_stale_ms);
        let denom_ms = hard_stale_ms.saturating_sub(configured_stale_ms).max(1);
        (over_ms as f64 / denom_ms as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };

    let time_left_ms = window_end_ms.saturating_sub(now_ms);
    let entry_time_left_blocked = time_left_ms < hard_entry_gate_ms;
    let close_window_penalty = if entry_time_left_blocked {
        let gap = hard_entry_gate_ms.saturating_sub(time_left_ms) as f64;
        let denom = hard_entry_gate_ms.max(1) as f64;
        (gap / denom).clamp(0.0, 1.0)
    } else {
        0.0
    };

    LiveMarketPenaltySurface {
        time_left_ms,
        stale_soft_penalty,
        close_window_penalty,
        hard_drop_stale: book_age_ms > hard_drop_ms,
    }
}

fn monotonic_dt_ms(now_ns: u64, prev_ns: u64) -> u64 {
    let nanos = now_ns.saturating_sub(prev_ns);
    ((nanos.saturating_add(999_999)) / 1_000_000).max(1)
}

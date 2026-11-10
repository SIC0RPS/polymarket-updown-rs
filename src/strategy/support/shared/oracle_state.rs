use crate::strategy::fair_value::interpolate_log_price;
use crate::types::SourceTsKind;
use serde::Serialize;
use std::collections::VecDeque;

const ORACLE_RETURN_HISTORY_KEEP_MS: u64 = 10_000;
const ORACLE_OFI_WINDOW_MS: u64 = 5_000;
const ORACLE_OFI_EVENT_CAPACITY: usize = 8_192;

#[derive(Debug, Clone, Copy)]
struct OracleOfiEvent {
    ts_ms: u64,
    flow: f64,
    adjacent_depth: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OracleTopOfBook {
    pub(crate) bid: f64,
    pub(crate) ask: f64,
    pub(crate) bid_sz: f64,
    pub(crate) ask_sz: f64,
    pub(crate) queue_imbalance_l5: Option<f64>,
}

impl OracleTopOfBook {
    pub(crate) fn new(
        bid: f64,
        ask: f64,
        bid_sz: Option<f64>,
        ask_sz: Option<f64>,
        queue_imbalance_l5: Option<f64>,
    ) -> Option<Self> {
        let bid_sz = bid_sz?;
        let ask_sz = ask_sz?;
        (bid.is_finite()
            && ask.is_finite()
            && bid > 0.0
            && ask >= bid
            && bid_sz.is_finite()
            && ask_sz.is_finite()
            && bid_sz > 0.0
            && ask_sz > 0.0)
            .then_some(Self {
                bid,
                ask,
                bid_sz,
                ask_sz,
                queue_imbalance_l5: queue_imbalance_l5
                    .filter(|value| value.is_finite() && (-1.0..=1.0).contains(value)),
            })
    }

    pub(crate) fn microprice(self) -> f64 {
        (self.ask * self.bid_sz + self.bid * self.ask_sz) / (self.bid_sz + self.ask_sz)
    }

    pub(crate) fn queue_imbalance(self) -> Option<f64> {
        let total = self.bid_sz + self.ask_sz;
        (self.bid_sz.is_finite() && self.ask_sz.is_finite() && total > 0.0)
            .then_some(((self.bid_sz - self.ask_sz) / total).clamp(-1.0, 1.0))
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OracleState {
    pub(crate) prev_latest_px: Option<f64>,
    pub(crate) prev_latest_ts_ms: Option<u64>,
    pub(crate) last_px: Option<f64>,
    pub(crate) last_ts_ms: Option<u64>,
    pub(crate) last_ingest_mono_ns: Option<u64>,
    pub(crate) latest_px: Option<f64>,
    pub(crate) latest_ts_ms: Option<u64>,
    pub(crate) latest_source_ts_kind: SourceTsKind,
    pub(crate) latest_ingest_mono_ns: Option<u64>,
    pub(crate) sigma2: f64,
    pub(crate) last_return_bps: f64,
    pub(crate) vol_samples: u32,
    pub(crate) previous_top: Option<OracleTopOfBook>,
    pub(crate) latest_top: Option<OracleTopOfBook>,
    recent_px: VecDeque<(u64, f64)>,
    ofi_events: VecDeque<OracleOfiEvent>,
    ofi_flow_sum: f64,
    ofi_flow_correction: f64,
    ofi_depth_sum: f64,
    ofi_depth_correction: f64,
    ofi_chain_start_ts_ms: Option<u64>,
    ofi_last_top: Option<(u64, OracleTopOfBook)>,
    ofi_reset_generation: u64,
}

impl OracleState {
    pub(crate) fn observe_top_of_book(&mut self, ts_ms: u64, top: Option<OracleTopOfBook>) {
        self.previous_top = self.latest_top;
        self.latest_top = top;

        let Some(top) = top else {
            self.reset_ofi_chain();
            return;
        };
        let Some((previous_ts_ms, previous_top)) = self.ofi_last_top else {
            self.start_ofi_chain(ts_ms, top);
            return;
        };
        if ts_ms.saturating_sub(previous_ts_ms) > ORACLE_OFI_WINDOW_MS {
            self.reset_ofi_chain();
            self.start_ofi_chain(ts_ms, top);
            return;
        }
        self.prune_ofi_events(ts_ms);
        if self.ofi_events.len() >= ORACLE_OFI_EVENT_CAPACITY {
            self.reset_ofi_chain();
            self.start_ofi_chain(ts_ms, top);
            return;
        }
        let Some((flow, adjacent_depth)) = top_of_book_flow(top, previous_top) else {
            self.reset_ofi_chain();
            self.start_ofi_chain(ts_ms, top);
            return;
        };
        self.ofi_events.push_back(OracleOfiEvent {
            ts_ms,
            flow,
            adjacent_depth,
        });
        compensated_add(&mut self.ofi_flow_sum, &mut self.ofi_flow_correction, flow);
        compensated_add(
            &mut self.ofi_depth_sum,
            &mut self.ofi_depth_correction,
            adjacent_depth,
        );
        self.ofi_last_top = Some((ts_ms, top));
        if !(self.ofi_flow_total().is_finite() && self.ofi_depth_total().is_finite()) {
            self.reset_ofi_chain();
            self.start_ofi_chain(ts_ms, top);
        }
    }

    pub(crate) fn normalized_ofi_5s(&self) -> Option<f64> {
        let (latest_ts_ms, _) = self.ofi_last_top?;
        let chain_start_ts_ms = self.ofi_chain_start_ts_ms?;
        if latest_ts_ms.saturating_sub(chain_start_ts_ms) < ORACLE_OFI_WINDOW_MS {
            return None;
        }
        let count = self.ofi_events.len();
        let flow_total = self.ofi_flow_total();
        let depth_total = self.ofi_depth_total();
        if count == 0 || !(flow_total.is_finite() && depth_total.is_finite()) {
            return None;
        }
        let average_depth = depth_total / count as f64;
        (average_depth.is_finite() && average_depth > 0.0)
            .then_some((flow_total / average_depth).clamp(-1.0, 1.0))
            .filter(|value| value.is_finite())
    }

    pub(crate) fn ofi_fingerprint(&self) -> (u64, u64, usize, u64, u64, bool) {
        (
            self.ofi_flow_total().to_bits(),
            self.ofi_depth_total().to_bits(),
            self.ofi_events.len(),
            self.ofi_chain_start_ts_ms.unwrap_or(0),
            self.ofi_reset_generation,
            self.normalized_ofi_5s().is_some(),
        )
    }

    fn start_ofi_chain(&mut self, ts_ms: u64, top: OracleTopOfBook) {
        self.ofi_chain_start_ts_ms = Some(ts_ms);
        self.ofi_last_top = Some((ts_ms, top));
    }

    fn reset_ofi_chain(&mut self) {
        self.ofi_events.clear();
        self.ofi_flow_sum = 0.0;
        self.ofi_flow_correction = 0.0;
        self.ofi_depth_sum = 0.0;
        self.ofi_depth_correction = 0.0;
        self.ofi_chain_start_ts_ms = None;
        self.ofi_last_top = None;
        self.ofi_reset_generation = self.ofi_reset_generation.wrapping_add(1);
    }

    fn prune_ofi_events(&mut self, latest_ts_ms: u64) {
        let cutoff_ts_ms = latest_ts_ms.saturating_sub(ORACLE_OFI_WINDOW_MS);
        while self
            .ofi_events
            .front()
            .is_some_and(|event| event.ts_ms <= cutoff_ts_ms)
        {
            if let Some(event) = self.ofi_events.pop_front() {
                compensated_add(
                    &mut self.ofi_flow_sum,
                    &mut self.ofi_flow_correction,
                    -event.flow,
                );
                compensated_add(
                    &mut self.ofi_depth_sum,
                    &mut self.ofi_depth_correction,
                    -event.adjacent_depth,
                );
            }
        }
    }

    fn ofi_flow_total(&self) -> f64 {
        self.ofi_flow_sum + self.ofi_flow_correction
    }

    fn ofi_depth_total(&self) -> f64 {
        self.ofi_depth_sum + self.ofi_depth_correction
    }

    pub(crate) fn observe_recent_px(&mut self, ts_ms: u64, px: f64) {
        if !(px.is_finite() && px > 0.0) {
            return;
        }
        if self
            .recent_px
            .back()
            .is_some_and(|(last_ts_ms, _)| ts_ms < *last_ts_ms)
        {
            self.recent_px.clear();
        }
        self.recent_px.push_back((ts_ms, px));
        while self.recent_px.front().is_some_and(|(front_ts_ms, _)| {
            front_ts_ms.saturating_add(ORACLE_RETURN_HISTORY_KEEP_MS) < ts_ms
        }) {
            self.recent_px.pop_front();
        }
    }

    pub(crate) fn signed_return_since_ms(&self, window_ms: u64) -> Option<f64> {
        let latest_ts = self.latest_ts_ms?;
        let latest_px = self.latest_px?;
        if !(latest_px.is_finite() && latest_px > 0.0) {
            return None;
        }
        let cutoff = latest_ts.saturating_sub(window_ms);
        let mut right = None;
        for &(ts_ms, px) in self.recent_px.iter().rev() {
            if ts_ms > cutoff {
                if right.is_none_or(|(right_ts, _)| ts_ms < right_ts) {
                    right = Some((ts_ms, px));
                }
                continue;
            }
            let cutoff_px = if ts_ms == cutoff {
                px
            } else {
                let (right_ts, right_px) = right?;
                interpolate_log_price(ts_ms, px, right_ts, right_px, cutoff)?
            };
            let signed_return = (latest_px / cutoff_px) - 1.0;
            return signed_return
                .is_finite()
                .then_some(signed_return.clamp(-1.0, 1.0));
        }
        None
    }
}

fn compensated_add(sum: &mut f64, correction: &mut f64, value: f64) {
    let next = *sum + value;
    if sum.abs() >= value.abs() {
        *correction += (*sum - next) + value;
    } else {
        *correction += (value - next) + *sum;
    }
    *sum = next;
}

fn top_of_book_flow(top: OracleTopOfBook, previous: OracleTopOfBook) -> Option<(f64, f64)> {
    let bid_flow = if top.bid > previous.bid {
        top.bid_sz
    } else if top.bid < previous.bid {
        -previous.bid_sz
    } else {
        top.bid_sz - previous.bid_sz
    };
    let ask_flow = if top.ask < previous.ask {
        top.ask_sz
    } else if top.ask > previous.ask {
        -previous.ask_sz
    } else {
        top.ask_sz - previous.ask_sz
    };
    let flow = bid_flow - ask_flow;
    let adjacent_depth = (top.bid_sz + top.ask_sz + previous.bid_sz + previous.ask_sz) / 4.0;
    (flow.is_finite() && adjacent_depth.is_finite() && adjacent_depth > 0.0)
        .then_some((flow, adjacent_depth))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenCaptureMode {
    BracketInterpolated,
    PostOpenProjection,
}

pub type CaptureModeObs = OpenCaptureMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OraclePxMode {
    Microprice,
    MidpointSecondary,
}

pub type PxModeObs = OraclePxMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolSkipReason {
    DtZeroOrGap,
    DtBelowFloor,
    InvalidReturnRatio,
    InvalidLogReturn,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OracleVolatilityDiagnostics {
    pub(crate) vol_update_dt_ms: Option<u64>,
    pub(crate) vol_return_bps_raw: Option<f64>,
    pub(crate) vol_return_bps_clipped: Option<f64>,
    pub(crate) vol_skip_reason: Option<VolSkipReason>,
}

#[derive(Debug, Clone, Copy)]
pub struct OpenPriceState {
    pub(crate) px: f64,
    pub(crate) span_ms: u64,
    pub(crate) capture_mode: OpenCaptureMode,
    pub(crate) left_gap_ms: Option<u64>,
    pub(crate) right_gap_ms: Option<u64>,
    pub(crate) asymmetry_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct OpenWindowContext {
    pub(crate) open_px: f64,
    pub(crate) open_delay_ms: u64,
    pub(crate) open_bracket_penalty: f64,
}

use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::shared::oracle_state::OracleTopOfBook;

const FITTED_MARKET_STATE_FEATURE_COUNT: f64 = 8.0;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleMicrostructureFeatures {
    pub(crate) queue_imbalance_l1: Option<f64>,
    pub(crate) queue_imbalance_l5: Option<f64>,
    pub(crate) ofi_depth: Option<f64>,
    pub(crate) microprice_gap_ticks: Option<f64>,
    pub(crate) signed_return_1s: Option<f64>,
    pub(crate) signed_return_5s: Option<f64>,
    pub(crate) trade_flow_imbalance_5s: Option<f64>,
    pub(crate) volatility_state: Option<f64>,
    pub(crate) jump_proxy: Option<f64>,
    pub(crate) periodic_hour_sin: Option<f64>,
    pub(crate) periodic_hour_cos: Option<f64>,
    pub(crate) periodic_minute_sin: Option<f64>,
    pub(crate) periodic_minute_cos: Option<f64>,
    pub(crate) availability: f64,
}

impl MakerRebateMmStrategy {
    pub(super) fn oracle_microstructure_features(
        &self,
        sigma2: f64,
        now_ms: u64,
    ) -> OracleMicrostructureFeatures {
        let state = &self.oracle_state;
        let top = state.latest_top.as_ref();
        let previous_top = state.previous_top.as_ref();
        let signed_return_1s = state.signed_return_since_ms(1_000);
        let signed_return_5s = state.signed_return_since_ms(5_000);
        let queue_imbalance_l1 = top.and_then(|top| top.queue_imbalance());
        let ofi_depth = state.normalized_ofi_5s().and_then(finite);
        let microprice_gap_ticks =
            top.and_then(|top| spot_microprice_gap_ticks(*top, previous_top.copied()));
        let volatility_state = finite(sigma2.max(0.0).sqrt());
        let jump_proxy = signed_return_1s
            .and_then(finite)
            .zip(volatility_state)
            .and_then(|(ret, vol)| {
                let denom = vol.max(1e-9);
                finite((ret.abs() / denom).clamp(0.0, 100.0))
            });
        let (hour_sin, hour_cos, minute_sin, minute_cos) = periodic_features(now_ms);

        let features = OracleMicrostructureFeatures {
            queue_imbalance_l1,
            queue_imbalance_l5: top.and_then(|top| top.queue_imbalance_l5),
            ofi_depth,
            microprice_gap_ticks,
            signed_return_1s: signed_return_1s.and_then(finite),
            signed_return_5s: signed_return_5s.and_then(finite),
            trade_flow_imbalance_5s: self
                .consumed_trade_flow_snapshot()
                .and_then(|flow| flow.imbalance()),
            volatility_state,
            jump_proxy,
            periodic_hour_sin: Some(hour_sin),
            periodic_hour_cos: Some(hour_cos),
            periodic_minute_sin: Some(minute_sin),
            periodic_minute_cos: Some(minute_cos),
            availability: 0.0,
        };
        OracleMicrostructureFeatures {
            availability: feature_availability(&features),
            ..features
        }
    }
}

fn spot_microprice_gap_ticks(
    top: OracleTopOfBook,
    previous: Option<OracleTopOfBook>,
) -> Option<f64> {
    let spread = top.ask - top.bid;
    let tick = previous
        .into_iter()
        .flat_map(|previous| {
            [
                (top.bid - previous.bid).abs(),
                (top.ask - previous.ask).abs(),
            ]
        })
        .chain(std::iter::once(spread))
        .filter(|value| value.is_finite() && *value > 0.0)
        .min_by(f64::total_cmp)?;
    let mid = 0.5 * (top.bid + top.ask);
    finite(((top.microprice() - mid) / tick).clamp(-100.0, 100.0))
}

fn periodic_features(now_ms: u64) -> (f64, f64, f64, f64) {
    let seconds = (now_ms / 1_000) % 86_400;
    let hour_angle = (seconds as f64 / 86_400.0) * std::f64::consts::TAU;
    let minute_angle = ((seconds % 3_600) as f64 / 3_600.0) * std::f64::consts::TAU;
    (
        hour_angle.sin(),
        hour_angle.cos(),
        minute_angle.sin(),
        minute_angle.cos(),
    )
}

fn feature_availability(features: &OracleMicrostructureFeatures) -> f64 {
    let present = [
        features.queue_imbalance_l1,
        features.queue_imbalance_l5,
        features.ofi_depth,
        features.microprice_gap_ticks,
        features.signed_return_1s,
        features.signed_return_5s,
        features.volatility_state,
        features.jump_proxy,
    ]
    .into_iter()
    .flatten()
    .filter(|value| value.is_finite())
    .count();
    present as f64 / FITTED_MARKET_STATE_FEATURE_COUNT
}

fn finite(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

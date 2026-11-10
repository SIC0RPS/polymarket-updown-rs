use ::ta::indicators::{
    BollingerBands, ExponentialMovingAverage, MovingAverageConvergenceDivergence,
    RelativeStrengthIndex,
};
use ::ta::{DataItem, Next};
use anyhow::{anyhow, Result};

const ORACLE_TA_MIN_BARS: usize = 50;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleTaFeatures {
    pub(crate) ta_ema_trend: f64,
    pub(crate) ta_macd_trend: f64,
    pub(crate) ta_rsi_regime: f64,
    pub(crate) ta_bollinger_position: f64,
    pub(crate) ta_recent_return_sign: f64,
    pub(crate) ta_drift_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OracleTaBar {
    pub(crate) ts_ms: i64,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) volume: f64,
}

#[derive(Debug)]
pub(crate) struct OracleTaFeatureCalculator {
    ema_fast_20: ExponentialMovingAverage,
    ema_slow_50: ExponentialMovingAverage,
    rsi_14: RelativeStrengthIndex,
    macd_12_26_9: MovingAverageConvergenceDivergence,
    bb_20_2: BollingerBands,
}

#[derive(Debug, Clone, Copy)]
struct RawTaSnapshot {
    ema_fast_20: f64,
    ema_slow_50: f64,
    rsi_14: f64,
    macd_line_12_26_9: f64,
    macd_signal_12_26_9: f64,
    macd_hist_12_26_9: f64,
    bb_basis_20_2: f64,
    bb_upper_20_2: f64,
    bb_lower_20_2: f64,
}

impl RawTaSnapshot {
    fn is_finite(self) -> bool {
        [
            self.ema_fast_20,
            self.ema_slow_50,
            self.rsi_14,
            self.macd_line_12_26_9,
            self.macd_signal_12_26_9,
            self.macd_hist_12_26_9,
            self.bb_basis_20_2,
            self.bb_upper_20_2,
            self.bb_lower_20_2,
        ]
        .into_iter()
        .all(|value| value.is_finite())
    }
}

impl OracleTaFeatures {
    fn from_raw(snapshot: RawTaSnapshot, close: f64, previous_close: Option<f64>) -> Self {
        let ema_delta = snapshot.ema_fast_20 - snapshot.ema_slow_50;
        let bb_width = (snapshot.bb_upper_20_2 - snapshot.bb_lower_20_2)
            .abs()
            .max(1e-6);
        let rsi_regime = ((snapshot.rsi_14 - 50.0) / 50.0).clamp(-1.0, 1.0);
        let recent_return_sign = previous_close
            .filter(|prev| prev.is_finite() && *prev > 0.0)
            .map(|prev| f64::from(trend_sign(((close / prev) - 1.0) * 10_000.0, 0.5)))
            .unwrap_or(0.0);
        Self {
            ta_ema_trend: trend_value(ema_delta, close.max(1e-6) * 0.0001),
            ta_macd_trend: trend_value(snapshot.macd_hist_12_26_9, close.max(1e-6) * 0.00001),
            ta_rsi_regime: rsi_regime,
            ta_bollinger_position: ((close - snapshot.bb_basis_20_2) / bb_width * 2.0)
                .clamp(-1.0, 1.0),
            ta_recent_return_sign: recent_return_sign,
            ta_drift_score: trend_value(
                ema_delta + snapshot.macd_hist_12_26_9,
                close.max(1e-6) * 0.0001,
            ),
        }
    }
}

impl OracleTaFeatureCalculator {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            ema_fast_20: ExponentialMovingAverage::new(20)
                .map_err(|err| anyhow!("ema_fast_20 init failed: {err}"))?,
            ema_slow_50: ExponentialMovingAverage::new(50)
                .map_err(|err| anyhow!("ema_slow_50 init failed: {err}"))?,
            rsi_14: RelativeStrengthIndex::new(14)
                .map_err(|err| anyhow!("rsi_14 init failed: {err}"))?,
            macd_12_26_9: MovingAverageConvergenceDivergence::new(12, 26, 9)
                .map_err(|err| anyhow!("macd_12_26_9 init failed: {err}"))?,
            bb_20_2: BollingerBands::new(20, 2.0)
                .map_err(|err| anyhow!("bb_20_2 init failed: {err}"))?,
        })
    }

    pub(crate) fn features_from_bars(bars: &[OracleTaBar]) -> Result<Option<OracleTaFeatures>> {
        if bars.len() < ORACLE_TA_MIN_BARS {
            return Ok(None);
        }
        let mut calculator = Self::new()?;
        let mut last_snapshot = None;
        let mut last_close = None;
        let mut previous_close = None;
        for bar in bars {
            let item = data_item_from_bar(*bar)?;
            last_snapshot = Some(calculator.next(&item));
            previous_close = last_close;
            last_close = Some(bar.close);
        }
        let Some(snapshot) = last_snapshot.filter(|snapshot| snapshot.is_finite()) else {
            return Ok(None);
        };
        Ok(last_close.map(|close| OracleTaFeatures::from_raw(snapshot, close, previous_close)))
    }

    fn next(&mut self, item: &DataItem) -> RawTaSnapshot {
        let macd = self.macd_12_26_9.next(item);
        let bb = self.bb_20_2.next(item);
        RawTaSnapshot {
            ema_fast_20: self.ema_fast_20.next(item),
            ema_slow_50: self.ema_slow_50.next(item),
            rsi_14: self.rsi_14.next(item),
            macd_line_12_26_9: macd.macd,
            macd_signal_12_26_9: macd.signal,
            macd_hist_12_26_9: macd.histogram,
            bb_basis_20_2: bb.average,
            bb_upper_20_2: bb.upper,
            bb_lower_20_2: bb.lower,
        }
    }
}

fn data_item_from_bar(bar: OracleTaBar) -> Result<DataItem> {
    DataItem::builder()
        .open(bar.open)
        .high(bar.high)
        .low(bar.low)
        .close(bar.close)
        .volume(bar.volume)
        .build()
        .map_err(|err| anyhow!("invalid oracle TA bar at ts_ms={}: {err}", bar.ts_ms))
}

fn trend_sign(value: f64, deadband: f64) -> i8 {
    if value > deadband {
        1
    } else if value < -deadband {
        -1
    } else {
        0
    }
}

fn trend_value(value: f64, deadband: f64) -> f64 {
    f64::from(trend_sign(value, deadband))
}

use crate::config::CurrentBlendedFairValueConfig;
use crate::oracle_resolution::OracleResolvedLabel;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::polymarket::state::{BookDepth, BookTop, PriceLevel};
use crate::strategy::fair_value::{sigmoid, FairValueResult};
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use super::calibration::posterior::{self, BetaCalibrationPosterior, BetaCalibrationSample};
use super::calibration::{binary_log_loss, clamp_probability};
use super::probability_interval::{probability_hull, ProbabilityInterval, ONE_SIDED_95_Z};

const MARKET_CALIBRATION_MAX_SAMPLES: usize = 512;
const PROBABILITY_EPSILON: f64 = 1e-6;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleMarketCalibrationScore {
    pub(crate) enabled: bool,
    pub(crate) raw_p: Option<f64>,
    pub(crate) p_market: Option<f64>,
    pub(crate) param_a: Option<f64>,
    pub(crate) param_b: Option<f64>,
    pub(crate) param_c: Option<f64>,
    pub(crate) sample_count: Option<u32>,
    pub(crate) positive_count: Option<u32>,
    pub(crate) brier_score: Option<f64>,
    pub(crate) log_loss: Option<f64>,
    pub(crate) uncertainty: Option<f64>,
    pub(super) calibrated_logit_std: Option<f64>,
    pub(super) calibrated_interval: Option<ProbabilityInterval>,
    pub(super) base_blend_interval: Option<ProbabilityInterval>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct OracleMarketCalibrationFit {
    a: f64,
    b: f64,
    c: f64,
    sample_count: u32,
    independent_window_count: u32,
    positive_count: u32,
    brier_score: Option<f64>,
    log_loss: Option<f64>,
    raw_brier_score: Option<f64>,
    raw_log_loss: Option<f64>,
    posterior: Option<BetaCalibrationPosterior>,
}

impl Default for OracleMarketCalibrationFit {
    fn default() -> Self {
        Self {
            a: 1.0,
            b: 1.0,
            c: 0.0,
            sample_count: 0,
            independent_window_count: 0,
            positive_count: 0,
            brier_score: None,
            log_loss: None,
            raw_brier_score: None,
            raw_log_loss: None,
            posterior: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingMarketCalibrationExample {
    raw_p: f64,
    candidate_p: f64,
    observation_ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct LabeledMarketCalibrationExample {
    market_id: MarketId,
    y_up: f64,
    raw_p: f64,
    candidate_p: f64,
    window_start_ms: u64,
    window_end_ms: u64,
    observation_ts_ms: u64,
    label_release_ts_ms: u64,
    learning_applied_ts_ms: u64,
    calibrated_brier_loss: f64,
    calibrated_log_loss: f64,
    raw_brier_loss: f64,
    raw_log_loss: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct OracleMarketCalibrationLearning {
    samples: Vec<LabeledMarketCalibrationExample>,
    fit_dirty: bool,
    fit_l2_penalty: f64,
    knowledge_cutoff_ms: u64,
}

impl OracleMarketCalibrationLearning {
    pub(crate) fn retained_prequential_metrics(&self) -> (usize, Option<[f64; 4]>) {
        if self.samples.is_empty() {
            return (0, None);
        }
        let samples = self.samples.iter().cloned().collect();
        (
            independent_window_count(&samples) as usize,
            Some(window_averaged_losses(&samples)),
        )
    }
}

impl BetaCalibrationSample for LabeledMarketCalibrationExample {
    fn probability(&self) -> f64 {
        self.raw_p
    }

    fn label(&self) -> f64 {
        self.y_up
    }

    fn window_identity(&self) -> (u64, u64) {
        (self.window_start_ms, self.window_end_ms)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OracleMarketCalibrationTrainer {
    pending_by_market: HashMap<MarketId, PendingMarketCalibrationExample>,
    resolved_markets: HashSet<MarketId>,
    samples: VecDeque<LabeledMarketCalibrationExample>,
    fit: OracleMarketCalibrationFit,
    fit_dirty: bool,
    fit_l2_penalty: f64,
    knowledge_cutoff_ms: u64,
}

impl OracleMarketCalibrationTrainer {
    pub(crate) fn observe_market(
        &mut self,
        market: &MarketSelection,
        raw_p: Option<f64>,
        observation_ts_ms: u64,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> OracleMarketCalibrationScore {
        self.refit_if_needed(cfg);
        if self.resolved_markets.contains(&market.market_id) {
            return self.score(raw_p, cfg);
        }
        if let Some(raw_p) = raw_p.and_then(finite_probability) {
            let raw_p = clamp_probability(raw_p);
            let candidate_p = clamp_probability(apply_beta_fit(raw_p, self.fit));
            self.pending_by_market
                .entry(market.market_id.clone())
                .or_insert(PendingMarketCalibrationExample {
                    raw_p,
                    candidate_p,
                    observation_ts_ms,
                });
        }
        self.score(raw_p, cfg)
    }

    fn score(
        &self,
        raw_p: Option<f64>,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> OracleMarketCalibrationScore {
        let Some(raw_p) = raw_p.and_then(finite_probability) else {
            return OracleMarketCalibrationScore {
                sample_count: Some(self.fit.sample_count),
                positive_count: Some(self.fit.positive_count),
                brier_score: self.fit.brier_score,
                log_loss: self.fit.log_loss,
                ..Default::default()
            };
        };
        let min_samples = cfg.market_calibration_min_samples;
        let calibrated = finite_probability(apply_beta_fit(raw_p, self.fit));
        let calibrated_logit_std = (cfg.market_calibration_enabled
            && calibration_fit_beats_raw(self.fit, min_samples))
        .then_some(self.fit.posterior)
        .flatten()
        .and_then(|posterior| posterior.logit_std_at(raw_p));
        let enabled = calibrated.is_some() && calibrated_logit_std.is_some();
        let p_market = if enabled {
            calibrated.unwrap_or(raw_p)
        } else {
            raw_p
        };
        let calibrated_logit_std = enabled.then_some(calibrated_logit_std).flatten();
        let calibrated_interval = calibrated_logit_std.and_then(|std| {
            let logit = (p_market / (1.0 - p_market)).ln();
            ProbabilityInterval::from_logit(logit, std, ONE_SIDED_95_Z).and_then(|interval| {
                probability_hull(p_market, [interval.lo(), interval.hi(), p_market])
            })
        });
        OracleMarketCalibrationScore {
            enabled,
            raw_p: Some(raw_p),
            p_market: Some(p_market),
            param_a: Some(self.fit.a),
            param_b: Some(self.fit.b),
            param_c: Some(self.fit.c),
            sample_count: Some(self.fit.sample_count),
            positive_count: Some(self.fit.positive_count),
            brier_score: self.fit.brier_score,
            log_loss: self.fit.log_loss,
            uncertainty: self.fit.brier_score.map(f64::sqrt),
            calibrated_logit_std,
            calibrated_interval,
            base_blend_interval: None,
        }
    }

    pub(crate) fn observe_resolved_label(
        &mut self,
        label: &OracleResolvedLabel,
        learning_applied_ts_ms: u64,
    ) {
        let Some(pending) = self.pending_by_market.remove(&label.market_id) else {
            return;
        };
        if !self.resolved_markets.insert(label.market_id.clone()) {
            return;
        }
        let y_up = label.y_up();
        let raw_p = pending.raw_p;
        let candidate_p = pending.candidate_p;
        self.samples.push_back(LabeledMarketCalibrationExample {
            market_id: label.market_id.clone(),
            y_up,
            raw_p,
            candidate_p,
            window_start_ms: label.window_start_ms,
            window_end_ms: label.window_end_ms,
            observation_ts_ms: pending.observation_ts_ms,
            label_release_ts_ms: label.release_ts_ms,
            learning_applied_ts_ms,
            calibrated_brier_loss: (candidate_p - y_up).powi(2),
            calibrated_log_loss: binary_log_loss(y_up, candidate_p),
            raw_brier_loss: (raw_p - y_up).powi(2),
            raw_log_loss: binary_log_loss(y_up, raw_p),
        });
        self.knowledge_cutoff_ms = self
            .knowledge_cutoff_ms
            .max(pending.observation_ts_ms)
            .max(label.release_ts_ms)
            .max(learning_applied_ts_ms);
        self.fit_dirty = true;
        while self.samples.len() > MARKET_CALIBRATION_MAX_SAMPLES {
            if let Some(evicted) = self.samples.pop_front() {
                self.resolved_markets.remove(&evicted.market_id);
            }
        }
    }

    pub(crate) fn learning(&self) -> OracleMarketCalibrationLearning {
        OracleMarketCalibrationLearning {
            samples: self.samples.iter().cloned().collect(),
            fit_dirty: self.fit_dirty,
            fit_l2_penalty: self.fit_l2_penalty,
            knowledge_cutoff_ms: self.knowledge_cutoff_ms,
        }
    }

    pub(crate) fn knowledge_cutoff_ms(&self) -> u64 {
        self.knowledge_cutoff_ms
    }

    pub(crate) fn from_learning(learning: OracleMarketCalibrationLearning) -> Result<Self> {
        ensure!(
            learning.samples.len() <= MARKET_CALIBRATION_MAX_SAMPLES,
            "market calibration learning exceeds retained history"
        );
        ensure!(
            learning.fit_l2_penalty.is_finite() && learning.fit_l2_penalty >= 0.0,
            "invalid market calibration fit configuration"
        );
        let mut market_ids = HashSet::new();
        for sample in &learning.samples {
            ensure!(
                !sample.market_id.as_str().is_empty()
                    && market_ids.insert(sample.market_id.clone())
                    && sample.window_start_ms > 0
                    && sample.window_start_ms < sample.window_end_ms
                    && sample.observation_ts_ms > 0
                    && sample.observation_ts_ms <= sample.learning_applied_ts_ms
                    && sample.label_release_ts_ms > 0
                    && sample.learning_applied_ts_ms >= sample.label_release_ts_ms
                    && sample.learning_applied_ts_ms <= learning.knowledge_cutoff_ms
                    && (sample.y_up == 0.0 || sample.y_up == 1.0)
                    && (0.0..=1.0).contains(&sample.raw_p)
                    && (0.0..=1.0).contains(&sample.candidate_p)
                    && sample.calibrated_brier_loss.to_bits()
                        == (sample.candidate_p - sample.y_up).powi(2).to_bits()
                    && sample.calibrated_log_loss.to_bits()
                        == binary_log_loss(sample.y_up, sample.candidate_p).to_bits()
                    && sample.raw_brier_loss.to_bits()
                        == (sample.raw_p - sample.y_up).powi(2).to_bits()
                    && sample.raw_log_loss.to_bits()
                        == binary_log_loss(sample.y_up, sample.raw_p).to_bits(),
                "invalid market calibration learning sample"
            );
        }
        let samples = learning.samples.into_iter().collect::<VecDeque<_>>();
        let resolved_markets = samples
            .iter()
            .map(|sample| sample.market_id.clone())
            .collect();
        let fit = if learning.fit_dirty {
            OracleMarketCalibrationFit::default()
        } else {
            fit_beta_calibration(&samples, learning.fit_l2_penalty)
        };
        Ok(Self {
            pending_by_market: HashMap::new(),
            resolved_markets,
            samples,
            fit,
            fit_dirty: learning.fit_dirty,
            fit_l2_penalty: learning.fit_l2_penalty,
            knowledge_cutoff_ms: learning.knowledge_cutoff_ms,
        })
    }

    fn refit_if_needed(&mut self, cfg: &CurrentBlendedFairValueConfig) {
        let l2_penalty = cfg.market_calibration_l2_penalty.max(0.0);
        if !self.fit_dirty && self.fit_l2_penalty.to_bits() == l2_penalty.to_bits() {
            return;
        }
        self.fit_dirty = false;
        self.fit_l2_penalty = l2_penalty;
        self.fit = fit_beta_calibration(&self.samples, self.fit_l2_penalty);
    }
}

impl MakerRebateMmStrategy {
    fn market_calibration_score_for_market(
        &mut self,
        market: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        now_ms: u64,
    ) -> OracleMarketCalibrationScore {
        let raw_p = depth_weighted_market_probability(up, down, up_depth, down_depth);
        self.market_calibration_trainer.observe_market(
            market,
            raw_p,
            now_ms,
            &self.cfg.live_fair_value_model,
        )
    }

    pub(crate) fn market_calibrated_live_fair_result(
        &mut self,
        market: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        now_ms: u64,
    ) -> Option<(OracleMarketCalibrationScore, FairValueResult, Option<f64>)> {
        let mut score = self
            .market_calibration_score_for_market(market, up, down, up_depth, down_depth, now_ms);
        let p_market = score.p_market?;
        let (fair_result, base_blend_interval) = self
            .market_live_fair_result_with_market_probability(
                market,
                up,
                down,
                up_depth,
                down_depth,
                now_ms,
                Some(p_market),
                score.calibrated_interval,
            )?;
        score.base_blend_interval = base_blend_interval;
        Some((score, fair_result, Some(p_market)))
    }
}

pub(crate) fn depth_weighted_market_probability(
    up: &BookTop,
    down: &BookTop,
    up_depth: Option<&BookDepth>,
    down_depth: Option<&BookDepth>,
) -> Option<f64> {
    let up_mid = top_mid(up)?;
    let down_mid = top_mid(down)?;
    let q_up = executable_depth_quality(up_depth, up);
    let q_down = executable_depth_quality(down_depth, down);
    let denom = q_up + q_down;
    if !(denom.is_finite() && denom > 0.0) {
        return None;
    }
    Some(((q_up * up_mid + q_down * (1.0 - down_mid)) / denom).clamp(0.0, 1.0))
}

fn fit_beta_calibration(
    samples: &VecDeque<LabeledMarketCalibrationExample>,
    l2_penalty: f64,
) -> OracleMarketCalibrationFit {
    if samples.is_empty() {
        return OracleMarketCalibrationFit::default();
    }
    let independent_window_count = independent_window_count(samples);
    let positive_count = samples.iter().filter(|sample| sample.y_up >= 0.5).count() as u32;
    let fitted = posterior::fit_map(samples, l2_penalty);
    let (a, b, c, posterior) = fitted
        .map(|(parameters, posterior)| {
            (parameters[0], parameters[1], parameters[2], Some(posterior))
        })
        .unwrap_or((1.0, 1.0, 0.0, None));
    let [brier, log_loss, raw_brier, raw_log_loss] = window_averaged_losses(samples);
    OracleMarketCalibrationFit {
        a,
        b,
        c,
        sample_count: samples.len() as u32,
        independent_window_count,
        positive_count,
        brier_score: Some(brier),
        log_loss: Some(log_loss),
        raw_brier_score: Some(raw_brier),
        raw_log_loss: Some(raw_log_loss),
        posterior,
    }
}

fn independent_window_count(samples: &VecDeque<LabeledMarketCalibrationExample>) -> u32 {
    samples
        .iter()
        .map(|sample| (sample.window_start_ms, sample.window_end_ms))
        .collect::<BTreeSet<_>>()
        .len() as u32
}

fn window_averaged_losses(samples: &VecDeque<LabeledMarketCalibrationExample>) -> [f64; 4] {
    let mut windows = BTreeMap::<(u64, u64), ([f64; 4], u32)>::new();
    for sample in samples {
        let (losses, count) = windows
            .entry((sample.window_start_ms, sample.window_end_ms))
            .or_default();
        losses[0] += sample.calibrated_brier_loss;
        losses[1] += sample.calibrated_log_loss;
        losses[2] += sample.raw_brier_loss;
        losses[3] += sample.raw_log_loss;
        *count = count.saturating_add(1);
    }
    let window_count = windows.len();
    debug_assert!(window_count > 0);
    let totals = windows
        .values()
        .fold([0.0; 4], |mut totals, (losses, count)| {
            for index in 0..totals.len() {
                totals[index] += losses[index] / *count as f64;
            }
            totals
        });
    totals.map(|total| total / window_count as f64)
}

fn calibration_fit_beats_raw(fit: OracleMarketCalibrationFit, min_samples: u32) -> bool {
    if fit.independent_window_count < min_samples {
        return false;
    }
    let (Some(brier), Some(raw_brier), Some(log_loss), Some(raw_log_loss)) = (
        fit.brier_score,
        fit.raw_brier_score,
        fit.log_loss,
        fit.raw_log_loss,
    ) else {
        return false;
    };
    raw_brier - brier > PROBABILITY_EPSILON && log_loss <= raw_log_loss
}

fn apply_beta_fit(p: f64, fit: OracleMarketCalibrationFit) -> f64 {
    let p = clamp_probability(p);
    sigmoid(fit.a * p.ln() - fit.b * (1.0 - p).ln() + fit.c)
        .clamp(PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
}

fn executable_depth_quality(depth: Option<&BookDepth>, top: &BookTop) -> f64 {
    let depth_quality = depth
        .map(|depth| {
            depth
                .bids
                .iter()
                .take(5)
                .filter_map(valid_bid_level)
                .sum::<f64>()
        })
        .unwrap_or(0.0);
    if depth_quality.is_finite() && depth_quality > 0.0 {
        return depth_quality;
    }
    top.bid_sz
        .filter(|size| size.is_finite() && *size > 0.0)
        .unwrap_or(0.0)
}

fn valid_bid_level(level: &PriceLevel) -> Option<f64> {
    (level.price.is_finite() && level.price > 0.0 && level.size.is_finite() && level.size > 0.0)
        .then_some(level.price * level.size)
}

fn top_mid(top: &BookTop) -> Option<f64> {
    let (Some(bid), Some(ask)) = (top.bid, top.ask) else {
        return None;
    };
    if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0) {
        return None;
    }
    (ask >= bid).then_some((0.5 * (bid + ask)).clamp(0.0, 1.0))
}

fn finite_probability(value: f64) -> Option<f64> {
    (value.is_finite() && (0.0..=1.0).contains(&value)).then_some(clamp_probability(value))
}

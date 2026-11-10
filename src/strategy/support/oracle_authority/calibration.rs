use crate::config::CurrentBlendedFairValueConfig;
use crate::oracle_resolution::OracleResolvedLabel;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::strategy::fair_value::sigmoid;
use crate::strategy::support::oracle_authority::probability_interval::{
    probability_hull, ProbabilityInterval,
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

#[path = "calibration/posterior.rs"]
pub(super) mod posterior;

use posterior::BetaCalibrationPosterior;

const BETA_CALIBRATION_MAX_SAMPLES: usize = 512;
const ONE_SIDED_95_Z: f64 = 1.644_853_626_951_472_2;
const PROBABILITY_EPSILON: f64 = 1e-6;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct OracleBetaCalibrationScore {
    pub(crate) enabled: bool,
    pub(crate) input_p: Option<f64>,
    pub(crate) p_fair: Option<f64>,
    pub(crate) param_a: Option<f64>,
    pub(crate) param_b: Option<f64>,
    pub(crate) param_c: Option<f64>,
    pub(crate) sample_count: Option<u32>,
    pub(crate) positive_count: Option<u32>,
    pub(crate) brier_score: Option<f64>,
    pub(crate) log_loss: Option<f64>,
    pub(crate) uncertainty: Option<f64>,
    posterior: Option<BetaCalibrationPosterior>,
}

impl OracleBetaCalibrationScore {
    pub(super) fn identity(mut self, input_p: f64) -> Self {
        self.enabled = false;
        self.p_fair = Some(input_p);
        self
    }

    pub(super) fn rescore(mut self, input_p: f64) -> Self {
        let Some(input_p) = finite_probability(input_p) else {
            self.input_p = None;
            self.p_fair = None;
            return self;
        };
        let calibrated = self
            .param_a
            .zip(self.param_b)
            .zip(self.param_c)
            .map(|((a, b), c)| apply_beta_parameters(input_p, a, b, c))
            .unwrap_or(input_p);
        self.input_p = Some(input_p);
        self.p_fair = Some(if self.enabled { calibrated } else { input_p });
        self
    }

    pub(crate) fn apply_interval(
        self,
        input: ProbabilityInterval,
        mean: f64,
    ) -> Option<ProbabilityInterval> {
        let Some(posterior) = self.posterior.filter(|_| self.enabled) else {
            return input.contains(mean).then_some(input);
        };
        let (a, b, c) = (self.param_a?, self.param_b?, self.param_c?);
        if !(a.is_finite() && b.is_finite() && c.is_finite()) {
            return None;
        }
        let endpoint_values = [
            apply_beta_parameters(input.lo(), a, b, c),
            apply_beta_parameters(input.hi(), a, b, c),
        ];
        let denominator = a - b;
        let stationary_value = if denominator.is_finite() && denominator.abs() > f64::EPSILON {
            let stationary = a / denominator;
            if stationary.is_finite() && input.lo() < stationary && stationary < input.hi() {
                Some(apply_beta_parameters(stationary, a, b, c))
            } else {
                None
            }
        } else {
            None
        };
        let point_interval =
            probability_hull(mean, endpoint_values.into_iter().chain(stationary_value))?;
        let parameter_std = posterior.logit_std_upper(input.lo(), input.hi())?;
        let radius = outward_up(ONE_SIDED_95_Z * parameter_std);
        let lower = outward_down(logit_from_probability(point_interval.lo())? - radius);
        let upper = outward_up(logit_from_probability(point_interval.hi())? + radius);
        let center = 0.5 * (lower + upper);
        let half_width = outward_up((center - lower).max(upper - center));
        let interval = ProbabilityInterval::from_logit(center, half_width, 1.0)?;
        ProbabilityInterval::new(
            outward_down(interval.lo()).max(0.0),
            outward_up(interval.hi()).min(1.0),
        )
        .filter(|interval| interval.contains(mean))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OracleBetaCalibrationFit {
    pub(crate) a: f64,
    pub(crate) b: f64,
    pub(crate) c: f64,
    pub(crate) sample_count: u32,
    independent_window_count: u32,
    pub(crate) positive_count: u32,
    pub(crate) brier_score: Option<f64>,
    pub(crate) log_loss: Option<f64>,
    raw_brier_score: Option<f64>,
    raw_log_loss: Option<f64>,
    posterior: Option<BetaCalibrationPosterior>,
}

impl Default for OracleBetaCalibrationFit {
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
struct PendingBetaCalibrationExample {
    p_base: f64,
    candidate_p: f64,
    observation_ts_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct LabeledBetaCalibrationExample {
    market_id: MarketId,
    pub(super) y_up: f64,
    pub(super) p_base: f64,
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
pub(crate) struct OracleBetaCalibrationLearning {
    samples: Vec<LabeledBetaCalibrationExample>,
    fit_dirty: bool,
    fit_l2_penalty: f64,
    knowledge_cutoff_ms: u64,
}

impl OracleBetaCalibrationLearning {
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

impl posterior::BetaCalibrationSample for LabeledBetaCalibrationExample {
    fn probability(&self) -> f64 {
        self.p_base
    }

    fn label(&self) -> f64 {
        self.y_up
    }

    fn window_identity(&self) -> (u64, u64) {
        (self.window_start_ms, self.window_end_ms)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OracleBetaCalibrationTrainer {
    pending_by_market: HashMap<MarketId, PendingBetaCalibrationExample>,
    resolved_markets: HashSet<MarketId>,
    samples: VecDeque<LabeledBetaCalibrationExample>,
    fit: OracleBetaCalibrationFit,
    fit_dirty: bool,
    fit_l2_penalty: f64,
    knowledge_cutoff_ms: u64,
}

impl OracleBetaCalibrationTrainer {
    pub(crate) fn observe_market(
        &mut self,
        market: &MarketSelection,
        p_base: f64,
        observation_ts_ms: u64,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> OracleBetaCalibrationScore {
        self.refit_if_needed(cfg);
        if self.resolved_markets.contains(&market.market_id) {
            return self.score(p_base, cfg);
        }
        if let Some(p_base) = finite_probability(p_base) {
            let p_base = clamp_probability(p_base);
            let candidate_p = apply_beta_fit(p_base, self.fit)
                .clamp(PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON);
            self.pending_by_market
                .entry(market.market_id.clone())
                .or_insert(PendingBetaCalibrationExample {
                    p_base,
                    candidate_p,
                    observation_ts_ms,
                });
        }
        self.score(p_base, cfg)
    }

    fn score(
        &self,
        p_base: f64,
        cfg: &CurrentBlendedFairValueConfig,
    ) -> OracleBetaCalibrationScore {
        let Some(input_p) = finite_probability(p_base) else {
            return OracleBetaCalibrationScore {
                sample_count: Some(self.fit.sample_count),
                positive_count: Some(self.fit.positive_count),
                brier_score: self.fit.brier_score,
                log_loss: self.fit.log_loss,
                ..Default::default()
            };
        };
        let min_samples = cfg.beta_calibration_min_samples;
        let enabled = cfg.beta_calibration_enabled
            && self.fit.posterior.is_some()
            && calibration_fit_beats_identity(self.fit, min_samples);
        let calibrated = apply_beta_fit(input_p, self.fit);
        OracleBetaCalibrationScore {
            enabled,
            input_p: Some(input_p),
            p_fair: Some(if enabled { calibrated } else { input_p }),
            param_a: Some(self.fit.a),
            param_b: Some(self.fit.b),
            param_c: Some(self.fit.c),
            sample_count: Some(self.fit.sample_count),
            positive_count: Some(self.fit.positive_count),
            brier_score: self.fit.brier_score,
            log_loss: self.fit.log_loss,
            uncertainty: self.fit.brier_score.map(f64::sqrt),
            posterior: self.fit.posterior,
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
        let p_base = pending.p_base;
        let candidate_p = pending.candidate_p;
        self.samples.push_back(LabeledBetaCalibrationExample {
            market_id: label.market_id.clone(),
            y_up,
            p_base,
            candidate_p,
            window_start_ms: label.window_start_ms,
            window_end_ms: label.window_end_ms,
            observation_ts_ms: pending.observation_ts_ms,
            label_release_ts_ms: label.release_ts_ms,
            learning_applied_ts_ms,
            calibrated_brier_loss: (candidate_p - y_up).powi(2),
            calibrated_log_loss: binary_log_loss(y_up, candidate_p),
            raw_brier_loss: (p_base - y_up).powi(2),
            raw_log_loss: binary_log_loss(y_up, p_base),
        });
        self.knowledge_cutoff_ms = self
            .knowledge_cutoff_ms
            .max(pending.observation_ts_ms)
            .max(label.release_ts_ms)
            .max(learning_applied_ts_ms);
        self.fit_dirty = true;
        while self.samples.len() > BETA_CALIBRATION_MAX_SAMPLES {
            if let Some(evicted) = self.samples.pop_front() {
                self.resolved_markets.remove(&evicted.market_id);
            }
        }
    }

    pub(crate) fn learning(&self) -> OracleBetaCalibrationLearning {
        OracleBetaCalibrationLearning {
            samples: self.samples.iter().cloned().collect(),
            fit_dirty: self.fit_dirty,
            fit_l2_penalty: self.fit_l2_penalty,
            knowledge_cutoff_ms: self.knowledge_cutoff_ms,
        }
    }

    pub(crate) fn knowledge_cutoff_ms(&self) -> u64 {
        self.knowledge_cutoff_ms
    }

    pub(crate) fn from_learning(learning: OracleBetaCalibrationLearning) -> Result<Self> {
        ensure!(
            learning.samples.len() <= BETA_CALIBRATION_MAX_SAMPLES,
            "beta calibration learning exceeds retained history"
        );
        ensure!(
            learning.fit_l2_penalty.is_finite() && learning.fit_l2_penalty >= 0.0,
            "invalid beta calibration fit configuration"
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
                    && (0.0..=1.0).contains(&sample.p_base)
                    && (0.0..=1.0).contains(&sample.candidate_p)
                    && sample.calibrated_brier_loss.to_bits()
                        == (sample.candidate_p - sample.y_up).powi(2).to_bits()
                    && sample.calibrated_log_loss.to_bits()
                        == binary_log_loss(sample.y_up, sample.candidate_p).to_bits()
                    && sample.raw_brier_loss.to_bits()
                        == (sample.p_base - sample.y_up).powi(2).to_bits()
                    && sample.raw_log_loss.to_bits()
                        == binary_log_loss(sample.y_up, sample.p_base).to_bits(),
                "invalid beta calibration learning sample"
            );
        }
        let samples = learning.samples.into_iter().collect::<VecDeque<_>>();
        let resolved_markets = samples
            .iter()
            .map(|sample| sample.market_id.clone())
            .collect();
        let fit = if learning.fit_dirty {
            OracleBetaCalibrationFit::default()
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
        let l2_penalty = cfg.beta_calibration_l2_penalty.max(0.0);
        if !self.fit_dirty && self.fit_l2_penalty.to_bits() == l2_penalty.to_bits() {
            return;
        }
        self.fit_dirty = false;
        self.fit_l2_penalty = l2_penalty;
        self.fit = fit_beta_calibration(&self.samples, self.fit_l2_penalty);
    }
}

fn fit_beta_calibration(
    samples: &VecDeque<LabeledBetaCalibrationExample>,
    l2_penalty: f64,
) -> OracleBetaCalibrationFit {
    if samples.is_empty() {
        return OracleBetaCalibrationFit::default();
    }
    let independent_window_count = independent_window_count(samples);
    let positive_count = samples.iter().filter(|sample| sample.y_up >= 0.5).count() as u32;
    let fitted = posterior::fit_map(samples, l2_penalty);
    let (a, b, c, posterior) = fitted
        .map(|(parameters, posterior)| {
            (parameters[0], parameters[1], parameters[2], Some(posterior))
        })
        .unwrap_or((1.0, 1.0, 0.0, None));
    let [brier_score, log_loss, raw_brier_score, raw_log_loss] = window_averaged_losses(samples);
    OracleBetaCalibrationFit {
        a,
        b,
        c,
        sample_count: samples.len() as u32,
        independent_window_count,
        positive_count,
        brier_score: Some(brier_score),
        log_loss: Some(log_loss),
        raw_brier_score: Some(raw_brier_score),
        raw_log_loss: Some(raw_log_loss),
        posterior,
    }
}

fn independent_window_count(samples: &VecDeque<LabeledBetaCalibrationExample>) -> u32 {
    samples
        .iter()
        .map(|sample| (sample.window_start_ms, sample.window_end_ms))
        .collect::<BTreeSet<_>>()
        .len() as u32
}

fn window_averaged_losses(samples: &VecDeque<LabeledBetaCalibrationExample>) -> [f64; 4] {
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

fn calibration_fit_beats_identity(fit: OracleBetaCalibrationFit, min_samples: u32) -> bool {
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

fn apply_beta_fit(p: f64, fit: OracleBetaCalibrationFit) -> f64 {
    apply_beta_parameters(p, fit.a, fit.b, fit.c)
}

fn apply_beta_parameters(p: f64, a: f64, b: f64, c: f64) -> f64 {
    let (x_a, x_b) = beta_features(p);
    sigmoid(a * x_a + b * x_b + c).clamp(0.0, 1.0)
}

fn beta_features(p: f64) -> (f64, f64) {
    let p = clamp_probability(p);
    (p.ln(), -(1.0 - p).ln())
}

pub(super) fn binary_log_loss(y_up: f64, p_up: f64) -> f64 {
    let p_up = clamp_probability(p_up);
    -(y_up * p_up.ln() + (1.0 - y_up) * (1.0 - p_up).ln())
}

fn finite_probability(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.clamp(0.0, 1.0))
}

pub(super) fn clamp_probability(value: f64) -> f64 {
    value.clamp(PROBABILITY_EPSILON, 1.0 - PROBABILITY_EPSILON)
}

fn logit_from_probability(value: f64) -> Option<f64> {
    let value = clamp_probability(value);
    let logit = (value / (1.0 - value)).ln();
    logit.is_finite().then_some(logit)
}

fn outward_down(value: f64) -> f64 {
    if !value.is_finite() || value == f64::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return -f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value > 0.0 { bits - 1 } else { bits + 1 })
}

fn outward_up(value: f64) -> f64 {
    if !value.is_finite() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value > 0.0 { bits + 1 } else { bits - 1 })
}

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CurrentBlendedFairValueConfig {
    pub micro_logistic_enabled: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub micro_logistic_trade_flow_enabled: bool,
    pub micro_logistic_l2_penalty: f64,
    pub market_calibration_enabled: bool,
    pub market_calibration_min_samples: u32,
    pub market_calibration_l2_penalty: f64,
    pub prob_calibration_logit_bias: f64,
    pub prob_calibration_logit_scale: f64,
    pub depth_pressure_logit_weight: f64,
    pub min_forecast_sigma2: f64,
    pub model_blend_min_weight: f64,
    pub model_blend_max_weight: f64,
    pub model_blend_edge_full: f64,
    pub model_blend_toxicity_k: f64,
    pub beta_calibration_enabled: bool,
    pub beta_calibration_min_samples: u32,
    pub beta_calibration_l2_penalty: f64,
    pub uncertainty_edge_weight: f64,
    pub executable_ev_scale: f64,
    pub adverse_selection_k: f64,
    pub top_loser_adverse_selection_k: f64,
}

impl CurrentBlendedFairValueConfig {
    pub(crate) fn validate(&self, prefix: &str) -> Result<()> {
        ensure!(
            self.micro_logistic_l2_penalty.is_finite() && self.micro_logistic_l2_penalty >= 0.0,
            "{prefix}.micro_logistic_l2_penalty must be finite and >= 0"
        );
        ensure!(
            self.market_calibration_min_samples > 0,
            "{prefix}.market_calibration_min_samples must be > 0"
        );
        ensure!(
            self.market_calibration_l2_penalty.is_finite()
                && self.market_calibration_l2_penalty >= 0.0,
            "{prefix}.market_calibration_l2_penalty must be finite and >= 0"
        );
        ensure!(
            self.prob_calibration_logit_bias.is_finite(),
            "{prefix}.prob_calibration_logit_bias must be finite"
        );
        ensure!(
            self.prob_calibration_logit_scale.is_finite()
                && self.prob_calibration_logit_scale > 0.0,
            "{prefix}.prob_calibration_logit_scale must be finite and > 0"
        );
        ensure!(
            self.depth_pressure_logit_weight.is_finite() && self.depth_pressure_logit_weight >= 0.0,
            "{prefix}.depth_pressure_logit_weight must be finite and >= 0"
        );
        ensure!(
            self.min_forecast_sigma2 > 0.0 && self.min_forecast_sigma2.is_finite(),
            "{prefix}.min_forecast_sigma2 must be > 0 and finite"
        );
        ensure!(
            self.model_blend_min_weight >= 0.0 && self.model_blend_min_weight <= 1.0,
            "{prefix}.model_blend_min_weight must be in [0, 1]"
        );
        ensure!(
            self.model_blend_max_weight >= 0.0 && self.model_blend_max_weight <= 1.0,
            "{prefix}.model_blend_max_weight must be in [0, 1]"
        );
        ensure!(
            self.model_blend_min_weight <= self.model_blend_max_weight,
            "{prefix}.model_blend_min_weight must be <= model_blend_max_weight"
        );
        ensure!(
            self.model_blend_edge_full > 0.0 && self.model_blend_edge_full.is_finite(),
            "{prefix}.model_blend_edge_full must be finite and > 0"
        );
        ensure!(
            self.model_blend_toxicity_k >= 0.0 && self.model_blend_toxicity_k.is_finite(),
            "{prefix}.model_blend_toxicity_k must be finite and >= 0"
        );
        ensure!(
            self.beta_calibration_min_samples > 0,
            "{prefix}.beta_calibration_min_samples must be > 0"
        );
        ensure!(
            self.beta_calibration_l2_penalty.is_finite() && self.beta_calibration_l2_penalty >= 0.0,
            "{prefix}.beta_calibration_l2_penalty must be finite and >= 0"
        );
        ensure!(
            self.uncertainty_edge_weight.is_finite() && self.uncertainty_edge_weight >= 0.0,
            "{prefix}.uncertainty_edge_weight must be finite and >= 0"
        );
        ensure!(
            self.executable_ev_scale.is_finite() && self.executable_ev_scale > 0.0,
            "{prefix}.executable_ev_scale must be finite and > 0"
        );
        ensure!(
            self.adverse_selection_k.is_finite() && (0.0..=0.6).contains(&self.adverse_selection_k),
            "{prefix}.adverse_selection_k must be finite and in [0, 0.6]"
        );
        ensure!(
            self.top_loser_adverse_selection_k.is_finite()
                && (0.0..=1.0).contains(&self.top_loser_adverse_selection_k),
            "{prefix}.top_loser_adverse_selection_k must be finite and in [0, 1]"
        );
        Ok(())
    }
}

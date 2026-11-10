use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use super::calibration::OracleBetaCalibrationLearning;
use super::logistic::{
    OracleLogisticApplicationPolicy, OracleLogisticFinalOutputPairRef,
    OracleMicrostructureLogisticLearning, OracleMicrostructureLogisticTrainer,
};
use super::market_calibration::OracleMarketCalibrationLearning;
use crate::strategy::paired_maker::MakerRebateMmStrategy;

const ORACLE_LEARNING_SCHEMA: u32 = 1;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum OracleLogisticForecastFeatureIdentity {
    #[default]
    EightInputV1,
    TradeFlowV1,
}

impl OracleLogisticForecastFeatureIdentity {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::EightInputV1 => "eight_input_v1",
            Self::TradeFlowV1 => "trade_flow_v1",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum OracleLogisticFeatureLayout {
    #[default]
    Baseline,
    MarketOffset,
    ReferenceOnly,
    RepresentationOnly,
    Combined,
}

impl OracleLogisticFeatureLayout {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "baseline" => Ok(Self::Baseline),
            "market_offset" => Ok(Self::MarketOffset),
            "reference_only" => Ok(Self::ReferenceOnly),
            "representation_only" => Ok(Self::RepresentationOnly),
            "combined" => Ok(Self::Combined),
            _ => anyhow::bail!("unsupported oracle logistic feature layout {value}"),
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::MarketOffset => "market_offset",
            Self::ReferenceOnly => "reference_only",
            Self::RepresentationOnly => "representation_only",
            Self::Combined => "combined",
        }
    }

    pub(super) fn serialized(self) -> Option<Self> {
        (self != Self::Baseline).then_some(self)
    }

    pub(super) fn accepts_inputs(self, spot_z: Option<f64>, reference_available: bool) -> bool {
        let z_available = spot_z.is_some_and(f64::is_finite);
        match self {
            Self::Baseline | Self::MarketOffset => spot_z.is_none() && !reference_available,
            Self::ReferenceOnly => spot_z.is_none() && reference_available,
            Self::RepresentationOnly => z_available && !reference_available,
            Self::Combined => z_available && reference_available,
        }
    }

    pub(super) fn uses_spot_z(self) -> bool {
        matches!(self, Self::RepresentationOnly | Self::Combined)
    }

    pub(super) fn uses_chainlink_reference(self) -> bool {
        matches!(self, Self::ReferenceOnly | Self::Combined)
    }

    pub(super) fn uses_market_offset(self) -> bool {
        matches!(self, Self::MarketOffset)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OracleLearningSnapshot {
    schema: u32,
    symbol: String,
    configuration_provenance: Vec<u8>,
    capture_identities: Vec<String>,
    knowledge_cutoff_ms: u64,
    logistic: OracleMicrostructureLogisticLearning,
    market_calibration: OracleMarketCalibrationLearning,
    beta_calibration: OracleBetaCalibrationLearning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OracleLearningMetrics {
    pub logistic_resolved_windows: usize,
    pub logistic_candidate_brier_loss: Option<f64>,
    pub logistic_candidate_log_loss: Option<f64>,
    pub logistic_prior_brier_loss: Option<f64>,
    pub logistic_prior_log_loss: Option<f64>,
    pub market_calibration_resolved_windows: usize,
    pub market_calibration_brier_loss: Option<f64>,
    pub market_calibration_log_loss: Option<f64>,
    pub market_raw_brier_loss: Option<f64>,
    pub market_raw_log_loss: Option<f64>,
    pub beta_calibration_resolved_windows: usize,
    pub beta_calibration_brier_loss: Option<f64>,
    pub beta_calibration_log_loss: Option<f64>,
    pub beta_raw_brier_loss: Option<f64>,
    pub beta_raw_log_loss: Option<f64>,
}

impl OracleLearningSnapshot {
    pub const LOGISTIC_FORECAST_FEATURE_EIGHT_INPUT_V1: &'static str = "eight_input_v1";
    pub const LOGISTIC_FORECAST_FEATURE_TRADE_FLOW_V1: &'static str = "trade_flow_v1";
    pub const LOGISTIC_FEATURE_LAYOUT_BASELINE: &'static str = "baseline";
    pub const LOGISTIC_FEATURE_LAYOUT_MARKET_OFFSET: &'static str = "market_offset";
    pub const LOGISTIC_FEATURE_LAYOUT_REFERENCE_ONLY: &'static str = "reference_only";
    pub const LOGISTIC_FEATURE_LAYOUT_REPRESENTATION_ONLY: &'static str = "representation_only";
    pub const LOGISTIC_FEATURE_LAYOUT_COMBINED: &'static str = "combined";
    pub const LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_PHASE_TRUST: &'static str =
        "market_offset_phase_trust";
    pub const LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_FIRST_DECISION_TRUST: &'static str =
        "market_offset_first_decision_trust";
    pub const LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_FINAL_OUTPUT_PHASE_TRUST: &'static str =
        "market_offset_final_output_phase_trust_v2";

    pub(crate) fn logistic_final_output_pairs(
        &self,
    ) -> impl Iterator<Item = OracleLogisticFinalOutputPairRef<'_>> + '_ {
        self.logistic.final_output_pairs()
    }

    pub fn knowledge_cutoff_ms(&self) -> u64 {
        self.knowledge_cutoff_ms
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn capture_identities(&self) -> &[String] {
        &self.capture_identities
    }

    pub fn logistic_feature_layout(&self) -> &'static str {
        self.logistic.feature_layout().as_str()
    }

    pub fn logistic_application_policy(&self) -> Option<&'static str> {
        self.logistic
            .application_policy()
            .map(|policy| policy.as_str())
    }

    pub fn fresh_logistic_research_seed(&self, feature_layout: &str) -> Result<Self> {
        let layout = OracleLogisticFeatureLayout::parse(feature_layout)?;
        let mut seed = self.clone();
        seed.logistic = self.logistic.fresh_for_layout(layout);
        Ok(seed)
    }

    pub fn fresh_logistic_research_seed_with_policy(
        &self,
        feature_layout: &str,
        application_policy: &str,
    ) -> Result<Self> {
        let layout = OracleLogisticFeatureLayout::parse(feature_layout)?;
        let policy = OracleLogisticApplicationPolicy::parse(application_policy)?;
        let mut seed = self.clone();
        seed.logistic = self.logistic.fresh_for_layout_with_policy(layout, policy)?;
        Ok(seed)
    }

    pub fn metrics(&self) -> OracleLearningMetrics {
        let (logistic_resolved_windows, logistic) = self.logistic.retained_prequential_metrics();
        let (market_calibration_resolved_windows, market) =
            self.market_calibration.retained_prequential_metrics();
        let (beta_calibration_resolved_windows, beta) =
            self.beta_calibration.retained_prequential_metrics();
        OracleLearningMetrics {
            logistic_resolved_windows,
            logistic_candidate_brier_loss: logistic.map(|losses| losses[0]),
            logistic_candidate_log_loss: logistic.map(|losses| losses[1]),
            logistic_prior_brier_loss: logistic.map(|losses| losses[2]),
            logistic_prior_log_loss: logistic.map(|losses| losses[3]),
            market_calibration_resolved_windows,
            market_calibration_brier_loss: market.map(|losses| losses[0]),
            market_calibration_log_loss: market.map(|losses| losses[1]),
            market_raw_brier_loss: market.map(|losses| losses[2]),
            market_raw_log_loss: market.map(|losses| losses[3]),
            beta_calibration_resolved_windows,
            beta_calibration_brier_loss: beta.map(|losses| losses[0]),
            beta_calibration_log_loss: beta.map(|losses| losses[1]),
            beta_raw_brier_loss: beta.map(|losses| losses[2]),
            beta_raw_log_loss: beta.map(|losses| losses[3]),
        }
    }
}

impl MakerRebateMmStrategy {
    pub(crate) fn oracle_learning(
        &self,
        capture_identities: Vec<String>,
        knowledge_cutoff_ms: u64,
    ) -> Result<OracleLearningSnapshot> {
        validate_provenance(&capture_identities, knowledge_cutoff_ms)?;
        let included_cutoff_ms = self
            .micro_logistic_trainer
            .knowledge_cutoff_ms()
            .max(self.market_calibration_trainer.knowledge_cutoff_ms())
            .max(self.beta_calibration_trainer.knowledge_cutoff_ms());
        ensure!(
            knowledge_cutoff_ms >= included_cutoff_ms,
            "oracle learning cutoff precedes included learning"
        );
        Ok(OracleLearningSnapshot {
            schema: ORACLE_LEARNING_SCHEMA,
            symbol: self.oracle_symbol.clone(),
            configuration_provenance: rmp_serde::to_vec_named(&self.cfg.live_fair_value_model)?,
            capture_identities,
            knowledge_cutoff_ms,
            logistic: self.micro_logistic_trainer.learning(),
            market_calibration: self.market_calibration_trainer.learning(),
            beta_calibration: self.beta_calibration_trainer.learning(),
        })
    }

    pub(crate) fn restore_oracle_learning(
        &mut self,
        learning: OracleLearningSnapshot,
    ) -> Result<()> {
        ensure!(
            learning.schema == ORACLE_LEARNING_SCHEMA,
            "incompatible oracle learning schema"
        );
        ensure!(
            learning.symbol == self.oracle_symbol,
            "oracle learning symbol mismatch"
        );
        validate_provenance(&learning.capture_identities, learning.knowledge_cutoff_ms)?;
        let mut logistic = OracleMicrostructureLogisticTrainer::from_learning(
            learning.logistic,
            &learning.symbol,
        )?;
        let market_calibration =
            super::market_calibration::OracleMarketCalibrationTrainer::from_learning(
                learning.market_calibration,
            )?;
        let beta_calibration = super::calibration::OracleBetaCalibrationTrainer::from_learning(
            learning.beta_calibration,
        )?;
        let included_cutoff_ms = logistic
            .knowledge_cutoff_ms()
            .max(market_calibration.knowledge_cutoff_ms())
            .max(beta_calibration.knowledge_cutoff_ms());
        ensure!(
            learning.knowledge_cutoff_ms >= included_cutoff_ms,
            "oracle learning cutoff precedes included learning"
        );
        logistic.set_l2_penalty(self.cfg.live_fair_value_model.micro_logistic_l2_penalty);
        logistic.set_trade_flow_enabled(
            self.cfg
                .live_fair_value_model
                .micro_logistic_trade_flow_enabled,
        );
        self.micro_logistic_trainer = logistic;
        self.market_calibration_trainer = market_calibration;
        self.beta_calibration_trainer = beta_calibration;
        Ok(())
    }
}

fn validate_provenance(capture_identities: &[String], knowledge_cutoff_ms: u64) -> Result<()> {
    ensure!(knowledge_cutoff_ms > 0, "oracle learning cutoff is zero");
    ensure!(
        capture_identities
            .iter()
            .all(|identity| !identity.trim().is_empty()),
        "oracle learning capture identity is empty"
    );
    let mut identities = capture_identities.to_vec();
    identities.sort_unstable();
    identities.dedup();
    ensure!(
        identities.len() == capture_identities.len(),
        "duplicate oracle learning capture identity"
    );
    Ok(())
}

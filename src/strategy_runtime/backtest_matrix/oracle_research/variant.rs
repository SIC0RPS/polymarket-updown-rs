use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};

use super::{digest_bytes, OracleResearchRun, PreparedOracleResearchSeed};

use crate::config::MakerRebateMmStrategyConfig;
use crate::strategy::OracleLearningSnapshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BacktestOracleResearchVariant {
    OperationalBaseline,
    TradeFlow,
    EqualStartBaseline,
    MarketOffset,
    MarketOffsetPhaseTrust,
    MarketOffsetFinalOutputPhaseTrust,
    MarketOffsetFirstDecisionTrust,
    ReferenceOnly,
    RepresentationOnly,
    Combined,
    DirectMarket,
}

impl BacktestOracleResearchVariant {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "operational_baseline" => Ok(Self::OperationalBaseline),
            "trade_flow" => Ok(Self::TradeFlow),
            "equal_start_baseline" => Ok(Self::EqualStartBaseline),
            "market_offset" => Ok(Self::MarketOffset),
            "market_offset_phase_trust" => Ok(Self::MarketOffsetPhaseTrust),
            "market_offset_final_output_phase_trust" => Ok(Self::MarketOffsetFinalOutputPhaseTrust),
            "market_offset_first_decision_trust" => Ok(Self::MarketOffsetFirstDecisionTrust),
            "reference_only" => Ok(Self::ReferenceOnly),
            "representation_only" => Ok(Self::RepresentationOnly),
            "combined" => Ok(Self::Combined),
            "direct_market" => Ok(Self::DirectMarket),
            _ => anyhow::bail!("unsupported oracle research variant {value}"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OperationalBaseline => "operational_baseline",
            Self::TradeFlow => "trade_flow",
            Self::EqualStartBaseline => "equal_start_baseline",
            Self::MarketOffset => "market_offset",
            Self::MarketOffsetPhaseTrust => "market_offset_phase_trust",
            Self::MarketOffsetFinalOutputPhaseTrust => "market_offset_final_output_phase_trust",
            Self::MarketOffsetFirstDecisionTrust => "market_offset_first_decision_trust",
            Self::ReferenceOnly => "reference_only",
            Self::RepresentationOnly => "representation_only",
            Self::Combined => "combined",
            Self::DirectMarket => "direct_market",
        }
    }

    pub(in crate::strategy_runtime) const fn feature_layout(self) -> &'static str {
        match self {
            Self::OperationalBaseline
            | Self::TradeFlow
            | Self::EqualStartBaseline
            | Self::DirectMarket => OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_BASELINE,
            Self::MarketOffset
            | Self::MarketOffsetPhaseTrust
            | Self::MarketOffsetFinalOutputPhaseTrust
            | Self::MarketOffsetFirstDecisionTrust => {
                OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_MARKET_OFFSET
            }
            Self::ReferenceOnly => OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_REFERENCE_ONLY,
            Self::RepresentationOnly => {
                OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_REPRESENTATION_ONLY
            }
            Self::Combined => OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_COMBINED,
        }
    }

    pub(super) const fn uses_captured_operational_seed(self) -> bool {
        matches!(
            self,
            Self::OperationalBaseline | Self::TradeFlow | Self::DirectMarket
        )
    }

    pub(in crate::strategy_runtime) fn apply_trade_flow_setting(
        self,
        config: &mut MakerRebateMmStrategyConfig,
    ) {
        if matches!(self, Self::OperationalBaseline | Self::TradeFlow) {
            config
                .live_fair_value_model
                .micro_logistic_trade_flow_enabled = self == Self::TradeFlow;
            for symbol_config in config.optimizer_symbol_configs.values_mut() {
                self.apply_trade_flow_setting(symbol_config);
            }
        }
    }

    pub(in crate::strategy_runtime) const fn uses_final_output_trust(self) -> bool {
        matches!(self, Self::MarketOffsetFinalOutputPhaseTrust)
    }

    pub(in crate::strategy_runtime) const fn application_policy(self) -> Option<&'static str> {
        match self {
            Self::MarketOffsetPhaseTrust => {
                Some(OracleLearningSnapshot::LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_PHASE_TRUST)
            }
            variant if variant.uses_final_output_trust() => Some(
                OracleLearningSnapshot::LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_FINAL_OUTPUT_PHASE_TRUST,
            ),
            Self::MarketOffsetFirstDecisionTrust => Some(
                OracleLearningSnapshot::LOGISTIC_APPLICATION_POLICY_MARKET_OFFSET_FIRST_DECISION_TRUST,
            ),
            _ => None,
        }
    }
}

impl OracleResearchRun {
    pub(in crate::strategy_runtime::backtest_matrix) fn prepare_seed(
        &self,
        capture: &Path,
        source: BTreeMap<String, OracleLearningSnapshot>,
        source_sha256: &str,
        explicit_seed: bool,
    ) -> Result<PreparedOracleResearchSeed> {
        if self.variant.uses_captured_operational_seed() {
            anyhow::ensure!(
                !explicit_seed,
                "operational baseline must use the capture-owned seed"
            );
        } else if self.previous_capture.is_none() {
            anyhow::ensure!(
                !explicit_seed,
                "first eligible research capture must create the equal-start seed"
            );
        } else {
            anyhow::ensure!(
                explicit_seed,
                "later research capture requires the preceding variant seed"
            );
        }
        let mut recorded = if explicit_seed {
            self.verify_previous_seed(source_sha256)?
        } else {
            let bytes = crate::capture::session::captured_initial_oracle_learning_bytes(capture)?
                .context("frozen oracle research capture has no recorded learning seed")?;
            anyhow::ensure!(
                digest_bytes(&bytes) == source_sha256
                    && source_sha256 == self.expected_captured_seed_sha256,
                "captured oracle seed differs from frozen research identity"
            );
            rmp_serde::from_slice::<BTreeMap<String, OracleLearningSnapshot>>(&bytes)
                .context("decode recorded oracle research seed")?
        };
        anyhow::ensure!(
            recorded.keys().all(|symbol| crate::config::VALID_ASSETS
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(symbol))),
            "recorded oracle research seed contains an unknown symbol"
        );
        let recorded_count = recorded.len();
        recorded.retain(|symbol, _| {
            self.symbols
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(symbol))
        });
        anyhow::ensure!(
            recorded == source,
            "oracle research seed differs from its recorded symbol projection"
        );
        let projected_source_sha256 = digest_bytes(&rmp_serde::to_vec_named(&recorded)?);
        if recorded_count == recorded.len() {
            anyhow::ensure!(
                projected_source_sha256 == source_sha256.to_ascii_lowercase(),
                "original oracle research seed serialization identity changed"
            );
        }

        let expected_symbols = self
            .symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<BTreeSet<_>>();
        let actual_symbols = source
            .keys()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            actual_symbols == expected_symbols && actual_symbols.len() == source.len(),
            "oracle research seed symbol coverage differs from frozen inputs"
        );
        for (symbol, learning) in &source {
            anyhow::ensure!(
                learning.knowledge_cutoff_ms() < self.capture_start_ts_ms,
                "oracle research seed cutoff includes current capture symbol={symbol}"
            );
            anyhow::ensure!(
                !learning
                    .capture_identities()
                    .iter()
                    .any(|identity| identity == &self.capture_run_id),
                "oracle research seed includes current capture symbol={symbol}"
            );
        }

        let target_layout = self.variant.feature_layout();
        let target_policy = self.variant.application_policy();
        let transformed_once =
            !self.variant.uses_captured_operational_seed() && self.previous_capture.is_none();
        let learning = if transformed_once {
            source
                .into_iter()
                .map(|(symbol, learning)| {
                    anyhow::ensure!(
                        learning.logistic_feature_layout()
                            == OracleLearningSnapshot::LOGISTIC_FEATURE_LAYOUT_BASELINE,
                        "equal-start source is not the historical baseline symbol={symbol}"
                    );
                    let learning = match target_policy {
                        Some(policy) => learning
                            .fresh_logistic_research_seed_with_policy(target_layout, policy)?,
                        None => learning.fresh_logistic_research_seed(target_layout)?,
                    };
                    Ok((symbol, learning))
                })
                .collect::<Result<BTreeMap<_, _>>>()?
        } else {
            source
        };
        for (symbol, learning) in &learning {
            anyhow::ensure!(
                learning.logistic_feature_layout() == target_layout,
                "oracle research seed layout mismatch symbol={symbol} expected={target_layout} actual={}",
                learning.logistic_feature_layout()
            );
            anyhow::ensure!(
                learning.logistic_application_policy() == target_policy,
                "oracle research seed policy mismatch symbol={symbol}"
            );
        }

        let bytes = rmp_serde::to_vec_named(&learning)?;
        let sha256 = digest_bytes(&bytes);
        let restored: BTreeMap<String, OracleLearningSnapshot> =
            rmp_serde::from_slice(&bytes).context("read back oracle research seed")?;
        anyhow::ensure!(
            restored == learning,
            "oracle research seed changed during serializer readback"
        );
        for snapshot in restored.values() {
            anyhow::ensure!(
                snapshot.logistic_feature_layout() == target_layout,
                "oracle research seed layout changed during serializer readback"
            );
            anyhow::ensure!(
                snapshot.logistic_application_policy() == target_policy,
                "oracle research seed policy changed during serializer readback"
            );
        }
        if !transformed_once {
            anyhow::ensure!(
                sha256 == projected_source_sha256,
                "unchanged oracle research seed serialization identity changed"
            );
        }

        Ok(PreparedOracleResearchSeed {
            learning,
            sha256,
            source: if self.variant.uses_captured_operational_seed() {
                "captured_operational"
            } else if transformed_once {
                "captured_equal_start"
            } else {
                "research_continuation"
            },
            transformed_once,
            application_policy: target_policy,
        })
    }
}

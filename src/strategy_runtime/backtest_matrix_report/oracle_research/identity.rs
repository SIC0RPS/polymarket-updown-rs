use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::Value;

use super::types::OracleResearchComparisonIdentity;
use crate::strategy_runtime::backtest_matrix::oracle_research::{
    BacktestOracleResearchVariant, OracleResearchRun, PreparedOracleResearchSeed,
};
use crate::strategy_runtime::backtest_run_identity::BacktestRunIdentity;

pub(in crate::strategy_runtime::backtest_matrix_report) struct ValidatedOracleResearchTrace {
    pub variant: BacktestOracleResearchVariant,
    pub runtime_log_path: PathBuf,
}

pub(in crate::strategy_runtime::backtest_matrix_report) fn validated_completed_oracle_research_traces(
    run: &OracleResearchRun,
    execution_timing_sha256: &str,
) -> Result<Vec<ValidatedOracleResearchTrace>> {
    let direct = [
        BacktestOracleResearchVariant::OperationalBaseline,
        BacktestOracleResearchVariant::DirectMarket,
    ];
    let market_offset = [
        BacktestOracleResearchVariant::OperationalBaseline,
        BacktestOracleResearchVariant::EqualStartBaseline,
        BacktestOracleResearchVariant::MarketOffset,
    ];
    let market_offset_phase_trust = [
        BacktestOracleResearchVariant::MarketOffset,
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust,
    ];
    let final_output = [
        BacktestOracleResearchVariant::OperationalBaseline,
        run.variant(),
    ];
    let market_offset_first_decision_trust = [
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust,
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust,
    ];
    let established = [
        BacktestOracleResearchVariant::OperationalBaseline,
        BacktestOracleResearchVariant::EqualStartBaseline,
        BacktestOracleResearchVariant::ReferenceOnly,
        BacktestOracleResearchVariant::RepresentationOnly,
        BacktestOracleResearchVariant::Combined,
    ];
    let variants = match run.variant() {
        BacktestOracleResearchVariant::DirectMarket => direct.as_slice(),
        BacktestOracleResearchVariant::TradeFlow => final_output.as_slice(),
        BacktestOracleResearchVariant::MarketOffset => market_offset.as_slice(),
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust => {
            market_offset_phase_trust.as_slice()
        }
        variant if variant.uses_final_output_trust() => final_output.as_slice(),
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust => {
            market_offset_first_decision_trust.as_slice()
        }
        _ => established.as_slice(),
    };
    let capture_root = run
        .output_root()
        .parent()
        .context("oracle research output has no capture root")?;
    let mut completed = Vec::with_capacity(variants.len());
    for &variant in variants {
        let runtime_log_path = capture_root.join(variant.as_str()).join("runtime.log");
        if variant != run.variant() {
            let report_path = capture_root
                .join(variant.as_str())
                .join("backtest_strategy_report.json");
            if !report_path.is_file() {
                continue;
            }
            let report = run.completed_report(run.capture_name(), run.capture_run_id(), variant)?;
            let artifact = report
                .get("oracle_research")
                .context("completed report omits oracle research artifact")?;
            anyhow::ensure!(
                artifact
                    .get("common_input_identity_sha256")
                    .and_then(Value::as_str)
                    == Some(run.common_input_identity_sha256()),
                "completed research common input identity mismatch"
            );
            anyhow::ensure!(
                artifact
                    .get("execution_timing_sha256")
                    .and_then(Value::as_str)
                    == Some(execution_timing_sha256),
                "completed research execution timing identity mismatch"
            );
        }
        anyhow::ensure!(
            runtime_log_path.is_file(),
            "completed research runtime log is absent {}",
            runtime_log_path.display()
        );
        completed.push(ValidatedOracleResearchTrace {
            variant,
            runtime_log_path,
        });
    }
    anyhow::ensure!(
        run.variant() != BacktestOracleResearchVariant::TradeFlow || completed.len() == 2,
        "trade-flow comparison requires both completed arms"
    );
    Ok(completed)
}

pub(in crate::strategy_runtime::backtest_matrix_report) fn comparison_identity(
    run: &OracleResearchRun,
    run_identity: &BacktestRunIdentity,
    seed: &PreparedOracleResearchSeed,
) -> Result<Option<OracleResearchComparisonIdentity>> {
    let reference_variant = match run.variant() {
        BacktestOracleResearchVariant::DirectMarket | BacktestOracleResearchVariant::TradeFlow => {
            anyhow::ensure!(
                seed.source == "captured_operational" && !seed.transformed_once,
                "candidate must use the unchanged captured operational seed"
            );
            BacktestOracleResearchVariant::OperationalBaseline
        }
        BacktestOracleResearchVariant::MarketOffsetPhaseTrust => {
            anyhow::ensure!(
                seed.application_policy
                    == BacktestOracleResearchVariant::MarketOffsetPhaseTrust.application_policy(),
                "phase trust candidate seed policy identity is absent"
            );
            BacktestOracleResearchVariant::MarketOffset
        }
        variant if variant.uses_final_output_trust() => {
            anyhow::ensure!(
                seed.application_policy == variant.application_policy(),
                "final-output phase-trust candidate seed policy identity is absent"
            );
            anyhow::ensure!(
                matches!(
                    (seed.source, seed.transformed_once),
                    ("captured_equal_start", true) | ("research_continuation", false)
                ),
                "final-output phase-trust candidate seed lineage is invalid"
            );
            BacktestOracleResearchVariant::OperationalBaseline
        }
        BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust => {
            anyhow::ensure!(
                seed.application_policy
                    == BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust
                        .application_policy(),
                "First-decision trust candidate seed policy identity is absent"
            );
            BacktestOracleResearchVariant::MarketOffsetPhaseTrust
        }
        _ => return Ok(None),
    };
    let reference =
        run.completed_report(run.capture_name(), run.capture_run_id(), reference_variant)?;
    let reference_artifact = reference
        .get("oracle_research")
        .context("comparison reference omits oracle research artifact")?;
    let reference_seed = reference_artifact
        .get("seed_initial_sha256")
        .and_then(Value::as_str)
        .context("comparison reference omits initial seed identity")?;
    if run.variant().uses_final_output_trust() {
        anyhow::ensure!(
            reference_artifact
                .get("seed_source")
                .and_then(Value::as_str)
                == Some("captured_operational")
                && reference_artifact
                    .get("seed_transformed_once")
                    .and_then(Value::as_bool)
                    == Some(false),
            "final-output comparison reference must use the unchanged captured operational seed"
        );
    } else {
        anyhow::ensure!(
            reference_artifact
                .get("seed_source")
                .and_then(Value::as_str)
                == Some(seed.source)
                && reference_artifact
                    .get("seed_transformed_once")
                    .and_then(Value::as_bool)
                    == Some(seed.transformed_once),
            "candidate and reference seed lineage differ"
        );
    }
    if matches!(
        run.variant(),
        BacktestOracleResearchVariant::DirectMarket | BacktestOracleResearchVariant::TradeFlow
    ) {
        anyhow::ensure!(
            reference_seed == seed.sha256,
            "candidate and operational reference seed identities differ"
        );
    } else if run.variant() == BacktestOracleResearchVariant::MarketOffsetFirstDecisionTrust {
        anyhow::ensure!(
            reference_artifact
                .get("application_policy")
                .and_then(Value::as_str)
                == BacktestOracleResearchVariant::MarketOffsetPhaseTrust.application_policy(),
            "candidate reference policy identity differs from phase trust"
        );
    } else {
        anyhow::ensure!(
            reference_artifact.get("application_policy").is_none(),
            "market offset reference has a non-legacy application policy"
        );
    }
    let mut candidate = serde_json::to_value(run_identity)?;
    let mut reference_identity = reference
        .get("run_identity")
        .cloned()
        .context("operational reference omits run identity")?;
    let candidate_config_inputs = nonempty_string_map(&candidate, "config_input_sha256")
        .context("direct market candidate config input identity is absent")?;
    let reference_config_inputs =
        nonempty_string_map(&reference_identity, "config_input_sha256")
            .context("operational reference config input identity is absent")?;
    anyhow::ensure!(
        candidate_config_inputs == reference_config_inputs,
        "direct market candidate and operational reference config inputs differ"
    );
    let candidate_config_snapshot = nonempty_string(&candidate, "config_snapshot_sha256")
        .context("direct market candidate config snapshot identity is absent")?;
    let reference_config_snapshot = nonempty_string(&reference_identity, "config_snapshot_sha256")
        .context("operational reference config snapshot identity is absent")?;
    anyhow::ensure!(
        candidate_config_snapshot == reference_config_snapshot,
        "direct market candidate and operational reference config snapshots differ"
    );
    for identity in [&candidate, &reference_identity] {
        anyhow::ensure!(
            identity
                .get("matrix_binary")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
                && identity
                    .get("matrix_binary_sha256")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty()),
            "direct market comparison run identity omits executable identity"
        );
    }
    if run.variant() == BacktestOracleResearchVariant::TradeFlow {
        anyhow::ensure!(
            candidate == reference_identity,
            "trade-flow comparison capture, config inputs, executable or source identities differ"
        );
        let mut candidate_config = recorded_strategy_config(&run.runtime_log_path())?;
        let reference_log = run
            .output_root()
            .parent()
            .context("trade-flow comparison has no capture root")?
            .join(reference_variant.as_str())
            .join("runtime.log");
        let mut reference_config = recorded_strategy_config(&reference_log)?;
        candidate["effective_strategy_config"] = candidate_config.clone();
        reference_identity["effective_strategy_config"] = reference_config.clone();
        remove_trade_flow_setting(&mut candidate_config, true)?;
        remove_trade_flow_setting(&mut reference_config, false)?;
        anyhow::ensure!(
            candidate_config == reference_config,
            "trade-flow comparison differs beyond micro_logistic_trade_flow_enabled"
        );
    }
    Ok(Some(OracleResearchComparisonIdentity {
        candidate_run_identity: candidate,
        reference_run_identity: reference_identity,
        candidate_seed_initial_sha256: seed.sha256.clone(),
        reference_seed_initial_sha256: reference_seed.to_string(),
        candidate_application_policy: seed.application_policy.map(str::to_string),
        reference_application_policy: reference_artifact
            .get("application_policy")
            .and_then(Value::as_str)
            .map(str::to_string),
    }))
}

fn recorded_strategy_config(path: &Path) -> Result<Value> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)?;
        let Some(fields) = value.get("fields") else {
            continue;
        };
        if fields.get("event").and_then(Value::as_str) == Some("oracle_research_run_start") {
            let config = fields
                .get("strategy_config")
                .and_then(Value::as_str)
                .context("research start omits effective strategy configuration")?;
            return serde_json::from_str(config).context("decode effective strategy configuration");
        }
    }
    anyhow::bail!("research start is absent {}", path.display())
}

fn remove_trade_flow_setting(config: &mut Value, enabled: bool) -> Result<()> {
    let model = config
        .get_mut("live_fair_value_model")
        .and_then(Value::as_object_mut)
        .context("effective strategy omits fair-value settings")?;
    let actual = model
        .remove("micro_logistic_trade_flow_enabled")
        .unwrap_or(Value::Bool(false));
    anyhow::ensure!(
        actual == Value::Bool(enabled),
        "effective strategy trade-flow setting differs from comparison arm"
    );
    if let Some(symbols) = config
        .get_mut("optimizer_symbol_configs")
        .and_then(Value::as_object_mut)
    {
        for symbol in symbols.values_mut() {
            remove_trade_flow_setting(symbol, enabled)?;
        }
    }
    Ok(())
}

fn nonempty_string<'a>(identity: &'a Value, field: &str) -> Option<&'a str> {
    identity
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

fn nonempty_string_map<'a>(
    identity: &'a Value,
    field: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    identity
        .get(field)
        .and_then(Value::as_object)
        .filter(|map| {
            !map.is_empty()
                && map
                    .values()
                    .all(|value| value.as_str().is_some_and(|value| !value.is_empty()))
        })
}

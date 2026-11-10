use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::{Yaml, YamlLoader};

use super::optimizer_parameters::{
    OptimizerParameterKind, OptimizerParameterSchema, OptimizerParameterValue,
};
use super::strategy::VALID_ASSETS;
use super::MakerRebateMmStrategyConfig;

type OptimizerParamKind = OptimizerParameterKind;

pub fn installed_optimizer_symbol_path(
    dir: &Path,
    symbol: &str,
    oracle_buy_and_hold: bool,
) -> PathBuf {
    let suffix = if oracle_buy_and_hold {
        "optimization.buy.yml"
    } else {
        "optimization.yml"
    };
    dir.join(format!("{}.{suffix}", symbol.to_ascii_uppercase()))
}

pub(crate) fn apply_optimizer_defaults(
    cfg: &mut MakerRebateMmStrategyConfig,
    path: &Path,
    oracle_buy_and_hold: bool,
) -> Result<()> {
    let root_schema = OptimizerParameterSchema::load_root(path)?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    cfg.optimizer_symbol_configs =
        load_symbol_config_files(dir, cfg, &root_schema, oracle_buy_and_hold)?;
    Ok(())
}

pub(crate) fn apply_runtime_symbol_defaults(
    cfg: &mut MakerRebateMmStrategyConfig,
    dir: &Path,
) -> Result<()> {
    let first_symbol = VALID_ASSETS
        .first()
        .context("runtime symbol configuration list is empty")?;
    let oracle_buy_and_hold = cfg.oracle_buy_and_hold;
    let first_path = installed_optimizer_symbol_path(dir, first_symbol, oracle_buy_and_hold);
    let symbol_schema = OptimizerParameterSchema::load_symbol(&first_path)?;
    cfg.optimizer_symbol_configs =
        load_symbol_config_files(dir, cfg, &symbol_schema, oracle_buy_and_hold)?;
    Ok(())
}

pub(crate) fn apply_optimizer_symbol_defaults(
    cfg: &mut MakerRebateMmStrategyConfig,
    root_path: &Path,
    symbol_path: &Path,
) -> Result<()> {
    let root_schema = OptimizerParameterSchema::load_root(root_path)?;
    let symbol_schema = OptimizerParameterSchema::load_symbol(symbol_path)?;
    root_schema.with_symbol_schema_defaults(&symbol_schema, symbol_path)?;
    let mut symbol_config = symbol_schema
        .apply_defaults(cfg)
        .with_context(|| format!("apply symbol optimizer defaults {}", symbol_path.display()))?;
    let symbol = symbol_from_optimizer_path(symbol_path)?;
    symbol_config.max_inventory_per_token = load_symbol_capacity(symbol_path, &symbol)?;
    *cfg = symbol_config;
    Ok(())
}

fn load_symbol_config_files(
    dir: &Path,
    base: &MakerRebateMmStrategyConfig,
    schema: &OptimizerParameterSchema,
    oracle_buy_and_hold: bool,
) -> Result<BTreeMap<String, Box<MakerRebateMmStrategyConfig>>> {
    let mut configs = BTreeMap::new();
    for symbol in VALID_ASSETS {
        let symbol_path = installed_optimizer_symbol_path(dir, symbol, oracle_buy_and_hold);
        let symbol_schema = OptimizerParameterSchema::load_symbol(&symbol_path)?;
        schema.with_symbol_schema_defaults(&symbol_schema, &symbol_path)?;
        let mut config = symbol_schema
            .apply_defaults(base)
            .with_context(|| format!("apply {symbol} optimizer defaults"))?;
        config.max_inventory_per_token = load_symbol_capacity(&symbol_path, symbol)?;
        configs.insert((*symbol).to_string(), Box::new(config));
    }
    Ok(configs)
}

fn load_symbol_capacity(path: &Path, symbol: &str) -> Result<f64> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let root = docs
        .first()
        .and_then(Yaml::as_hash)
        .ok_or_else(|| anyhow::anyhow!("{} must be a non-empty YAML map", path.display()))?;
    let allocation = root
        .get(&Yaml::String("allocation".to_string()))
        .and_then(Yaml::as_hash)
        .with_context(|| format!("{} missing symbol allocation map", path.display()))?;
    for key in allocation.keys() {
        let key = key
            .as_str()
            .context("symbol allocation field name must be string")?;
        anyhow::ensure!(
            key == "max_inventory_per_token",
            "symbol allocation contains unknown field {key}"
        );
    }
    let capacity = yaml_number(
        yaml_child(allocation, "max_inventory_per_token")?,
        "max_inventory_per_token",
    )?;
    anyhow::ensure!(
        capacity.is_finite() && capacity > 0.0,
        "symbol allocation max_inventory_per_token must be finite and > 0 for {symbol}"
    );
    Ok(capacity)
}

fn yaml_child<'a>(hash: &'a yaml_rust2::yaml::Hash, key: &str) -> Result<&'a Yaml> {
    hash.get(&Yaml::String(key.to_string()))
        .with_context(|| format!("missing allocation field {key}"))
}

fn yaml_number(node: &Yaml, field: &str) -> Result<f64> {
    match node {
        Yaml::Real(value) => value.parse().with_context(|| format!("parse {field}")),
        Yaml::Integer(value) => Ok(*value as f64),
        _ => bail!("{field} must be numeric"),
    }
}

fn symbol_from_optimizer_path(path: &Path) -> Result<String> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .with_context(|| format!("invalid symbol optimizer path {}", path.display()))?;
    VALID_ASSETS
        .iter()
        .find(|symbol| file_name == format!("{symbol}.optimization.yml"))
        .map(|symbol| (*symbol).to_string())
        .with_context(|| format!("invalid symbol optimizer filename {file_name}"))
}

pub(super) fn apply_optimizer_parameter(
    cfg: &mut MakerRebateMmStrategyConfig,
    name: &str,
    kind: OptimizerParamKind,
    value: f64,
    typed_value: &OptimizerParameterValue,
) -> Result<()> {
    if kind == OptimizerParamKind::Float && !value.is_finite() {
        bail!("{name}.default must be finite");
    }
    let integer = match (kind, typed_value) {
        (OptimizerParamKind::Int, OptimizerParameterValue::Int(value)) => Some(*value),
        (OptimizerParamKind::Float, OptimizerParameterValue::Float(_)) => None,
        _ => bail!("{name}.default type does not match optimizer schema"),
    };
    match (name, kind) {
        ("entry_cutoff_before_expiry_ms", OptimizerParamKind::Int) => {
            cfg.entry_cutoff_before_expiry_ms =
                value_to_u64(name, integer.expect("validated integer"))?;
        }
        ("base_half_spread", OptimizerParamKind::Float) => {
            cfg.base_half_spread = value;
        }
        ("paired_first_managed_sell_slippage_tolerance", OptimizerParamKind::Float) => {
            cfg.paired_first_managed_sell_slippage_tolerance = value;
        }
        ("paired_first_managed_sell_aggressiveness", OptimizerParamKind::Float) => {
            cfg.paired_first_managed_sell_aggressiveness = value;
        }
        ("oracle_side_noise_weight", OptimizerParamKind::Float) => {
            cfg.oracle_side_noise_weight = value;
        }
        ("oracle_side_flip_hysteresis", OptimizerParamKind::Float) => {
            cfg.oracle_side_flip_hysteresis = value;
        }
        ("oracle_side_flip_noise_weight", OptimizerParamKind::Float) => {
            cfg.oracle_side_flip_noise_weight = value;
        }
        ("oracle_side_min_advantage", OptimizerParamKind::Float) => {
            cfg.oracle_side_min_advantage = value;
        }
        ("oracle_side_cleanup_flip_bump", OptimizerParamKind::Float) => {
            cfg.oracle_side_cleanup_flip_bump = value;
        }
        ("ewma_half_life_ms", OptimizerParamKind::Int) => {
            cfg.ewma_half_life_ms = value_to_u64(name, integer.expect("validated integer"))?;
        }
        ("oracle_market_confirmation_probability_weight", OptimizerParamKind::Float) => {
            cfg.oracle_market_confirmation_probability_weight = value;
        }
        ("oracle_market_confirmation_spread_weight", OptimizerParamKind::Float) => {
            cfg.oracle_market_confirmation_spread_weight = value;
        }
        ("oracle_market_confirmation_depth_weight", OptimizerParamKind::Float) => {
            cfg.oracle_market_confirmation_depth_weight = value;
        }
        ("oracle_market_confirmation_book_weight", OptimizerParamKind::Float) => {
            cfg.oracle_market_confirmation_book_weight = value;
        }
        ("oracle_ta_boundary_scale", OptimizerParamKind::Float) => {
            cfg.oracle_ta_boundary_scale = value;
        }
        ("oracle_ta_boundary_weight", OptimizerParamKind::Float) => {
            cfg.oracle_ta_boundary_weight = value;
        }
        ("oracle_ta_token_agreement_weight", OptimizerParamKind::Float) => {
            cfg.oracle_ta_token_agreement_weight = value;
        }
        ("oracle_ta_component_alignment_weight", OptimizerParamKind::Float) => {
            cfg.oracle_ta_component_alignment_weight = value;
        }
        ("ctf_likely_winner_min_confidence", OptimizerParamKind::Float) => {
            cfg.ctf_likely_winner_min_confidence = value;
        }
        ("min_hold_probability_edge", OptimizerParamKind::Float) => {
            cfg.min_hold_probability_edge = value;
        }
        ("paired_first_managed_sell_book_max_age_ms", OptimizerParamKind::Int) => {
            cfg.paired_first_managed_sell_book_max_age_ms =
                value_to_u64(name, integer.expect("validated integer"))?;
        }
        ("ctf_remaining_leg_liquidity_drawdown_weight", OptimizerParamKind::Float) => {
            cfg.ctf_remaining_leg_liquidity_drawdown_weight = value;
        }
        ("ctf_remaining_leg_time_risk_weight", OptimizerParamKind::Float) => {
            cfg.ctf_remaining_leg_time_risk_weight = value;
        }
        ("ctf_sell_limit_delay_cushion_ticks", OptimizerParamKind::Float) => {
            cfg.ctf_sell_limit_delay_cushion_ticks = value;
        }
        ("ctf_probability_calibration_min_confidence", OptimizerParamKind::Float) => {
            cfg.ctf_probability_calibration_min_confidence = value;
        }
        ("ctf_probability_confidence_market_weight", OptimizerParamKind::Float) => {
            cfg.ctf_probability_confidence_market_weight = value;
        }
        ("ctf_probability_confidence_spread_weight", OptimizerParamKind::Float) => {
            cfg.ctf_probability_confidence_spread_weight = value;
        }
        ("ctf_probability_confidence_depth_weight", OptimizerParamKind::Float) => {
            cfg.ctf_probability_confidence_depth_weight = value;
        }
        ("ctf_probability_model_market_loser_bid_ambiguity_weight", OptimizerParamKind::Float) => {
            cfg.ctf_probability_model_market_loser_bid_ambiguity_weight = value;
        }
        ("live_fair_value_model_micro_logistic_l2_penalty", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.micro_logistic_l2_penalty = value;
        }
        ("live_fair_value_model_market_calibration_l2_penalty", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.market_calibration_l2_penalty = value;
        }
        ("live_fair_value_model_beta_calibration_l2_penalty", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.beta_calibration_l2_penalty = value;
        }
        ("live_fair_value_model_prob_calibration_logit_bias", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.prob_calibration_logit_bias = value;
        }
        ("live_fair_value_model_prob_calibration_logit_scale", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.prob_calibration_logit_scale = value;
        }
        ("live_fair_value_model_depth_pressure_logit_weight", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.depth_pressure_logit_weight = value;
        }
        ("live_fair_value_model_min_forecast_sigma2", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.min_forecast_sigma2 = value;
        }
        ("live_fair_value_model_model_blend_min_weight", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.model_blend_min_weight = value;
        }
        ("live_fair_value_model_model_blend_edge_full", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.model_blend_edge_full = value;
        }
        ("live_fair_value_model_model_blend_toxicity_k", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.model_blend_toxicity_k = value;
        }
        ("live_fair_value_model_market_calibration_enabled", OptimizerParamKind::Int) => {
            cfg.live_fair_value_model.market_calibration_enabled =
                value_to_bool(name, integer.expect("validated integer"))?;
        }
        ("live_fair_value_model_market_calibration_min_samples", OptimizerParamKind::Int) => {
            cfg.live_fair_value_model.market_calibration_min_samples =
                value_to_u64(name, integer.expect("validated integer"))?
                    .try_into()
                    .with_context(|| format!("parse {name}.default"))?;
        }
        ("live_fair_value_model_beta_calibration_enabled", OptimizerParamKind::Int) => {
            cfg.live_fair_value_model.beta_calibration_enabled =
                value_to_bool(name, integer.expect("validated integer"))?;
        }
        ("live_fair_value_model_beta_calibration_min_samples", OptimizerParamKind::Int) => {
            cfg.live_fair_value_model.beta_calibration_min_samples =
                value_to_u64(name, integer.expect("validated integer"))?
                    .try_into()
                    .with_context(|| format!("parse {name}.default"))?;
        }
        ("live_fair_value_model_uncertainty_edge_weight", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.uncertainty_edge_weight = value;
        }
        ("live_fair_value_model_executable_ev_scale", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.executable_ev_scale = value;
        }
        ("live_fair_value_model_adverse_selection_k", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.adverse_selection_k = value;
        }
        ("live_fair_value_model_top_loser_adverse_selection_k", OptimizerParamKind::Float) => {
            cfg.live_fair_value_model.top_loser_adverse_selection_k = value;
        }
        (_, OptimizerParamKind::Int | OptimizerParamKind::Float) => {
            bail!("optimizer param {name} has no MakerRebateMmStrategyConfig mapping");
        }
    }
    Ok(())
}

fn value_to_bool(name: &str, value: i64) -> Result<bool> {
    match value_to_u64(name, value)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => bail!("{name}.default must be 0 or 1"),
    }
}

fn value_to_u64(name: &str, value: i64) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{name}.default must be non-negative"))
}

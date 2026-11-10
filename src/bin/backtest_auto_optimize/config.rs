use super::*;
use std::fs;
use yaml_rust2::{Yaml, YamlLoader};

pub(super) fn load_symbol_schema_from_snapshot(
    snapshot: &Path,
    root_spec: &Path,
) -> Result<OptimizerParameterSchema> {
    OptimizerParameterSchema::load_root(root_spec)?.with_symbol_defaults(snapshot)
}

fn yaml_child<'a>(node: &'a Yaml, key: &str) -> Result<&'a Yaml> {
    node.as_hash()
        .and_then(|hash| hash.get(&Yaml::String(key.to_string())))
        .ok_or_else(|| anyhow!("missing YAML key {key}"))
}

fn yaml_usize(node: &Yaml, key: &str) -> Result<usize> {
    let value = yaml_child(node, key)?
        .as_i64()
        .ok_or_else(|| anyhow!("YAML key {key} must be integer"))?;
    usize::try_from(value).with_context(|| format!("parse {key}"))
}

fn yaml_u64(node: &Yaml, key: &str) -> Result<u64> {
    let value = yaml_child(node, key)?
        .as_i64()
        .ok_or_else(|| anyhow!("YAML key {key} must be integer"))?;
    u64::try_from(value).with_context(|| format!("parse {key}"))
}

fn yaml_selection_mode(optimizer: &Yaml) -> Result<types::SelectionMode> {
    let settings = optimizer
        .as_hash()
        .context("optimizer must be a YAML mapping")?;
    match settings.get(&Yaml::String("selection_mode".to_string())) {
        None => Ok(types::SelectionMode::default()),
        Some(value) => value
            .as_str()
            .context("optimizer.selection_mode must be a string")?
            .parse(),
    }
}

pub(super) fn load_optimizer_selection_mode(path: &Path) -> Result<types::SelectionMode> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let doc = docs
        .first()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))?;
    yaml_selection_mode(yaml_child(doc, "optimizer")?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OptimizerSearchLimits {
    pub(super) max_function_evals: usize,
    pub(super) max_generations_per_run: usize,
    pub(super) max_no_improvement_evals: usize,
}

pub(super) fn load_optimizer_search_limits(path: &Path) -> Result<OptimizerSearchLimits> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let doc = docs
        .first()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))?;
    let auto = yaml_child(yaml_child(doc, "optimizer")?, "auto")?;
    let limits = OptimizerSearchLimits {
        max_function_evals: yaml_usize(auto, "max_function_evals")?,
        max_generations_per_run: yaml_usize(auto, "max_generations_per_run")?,
        max_no_improvement_evals: yaml_usize(auto, "max_no_improvement_evals")?,
    };
    ensure!(
        limits.max_function_evals > 0 && limits.max_generations_per_run > 0,
        "optimizer search ceilings must be > 0"
    );
    Ok(limits)
}

pub(super) fn load_optimizer_settings(path: &Path, args: &Args) -> Result<OptimizerSettings> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let doc = docs
        .first()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))?;
    let optimizer = yaml_child(doc, "optimizer")?;
    let auto = yaml_child(optimizer, "auto")?;
    let limits = load_optimizer_search_limits(path)?;
    let settings = OptimizerSettings {
        selection_mode: yaml_selection_mode(optimizer)?,
        jobs: args
            .jobs
            .or(env_parse("BACKTEST_STRATEGY_AUTO_OPTIMIZE_JOBS")?)
            .unwrap_or(yaml_usize(optimizer, "jobs")?),
        seed: args
            .seed
            .or(env_parse("BACKTEST_STRATEGY_AUTO_OPTIMIZE_SEED")?)
            .unwrap_or(yaml_u64(auto, "seed")?),
        max_function_evals: limits.max_function_evals,
        max_generations_per_run: limits.max_generations_per_run,
        max_no_improvement_evals: limits.max_no_improvement_evals,
        apply_best: !args.no_apply && !env_bool("BACKTEST_STRATEGY_AUTO_OPTIMIZE_NO_APPLY")?,
    };
    Ok(settings)
}

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use yaml_rust2::{Yaml, YamlLoader};

use super::optimizer_knobs::apply_optimizer_parameter;
use super::MakerRebateMmStrategyConfig;

#[path = "optimizer_parameters/candidate_identity.rs"]
mod candidate_identity;
#[path = "optimizer_parameters/grid.rs"]
mod grid;
use grid::{exact_i64, grid_index, schema_sha256};

const OPTIMIZER_PARAMETER_SCHEMA: &[u8; 8] = b"PMOPT001";
const MAX_EXACT_INTEGER_F64: i64 = 1_i64 << 53;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptimizerLearningSelection {
    pub path: PathBuf,
    pub sha256: String,
}

pub fn optimizer_learning_selection(path: &Path) -> Result<Option<OptimizerLearningSelection>> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let root = docs
        .first()
        .and_then(Yaml::as_hash)
        .with_context(|| format!("{} must be a non-empty YAML map", path.display()))?;
    let Some(learning) = root.get(&Yaml::String("learning".to_string())) else {
        return Ok(None);
    };
    let learning = learning
        .as_hash()
        .with_context(|| format!("{} learning selection must be a map", path.display()))?;
    for key in learning.keys() {
        let key = key
            .as_str()
            .context("optimizer learning selection key must be string")?;
        anyhow::ensure!(
            matches!(key, "path" | "sha256"),
            "optimizer learning selection contains unknown field {key}"
        );
    }
    let selected_path = learning.get(&Yaml::String("path".to_string()));
    let selected_sha256 = learning.get(&Yaml::String("sha256".to_string()));
    match (selected_path, selected_sha256) {
        (None, None) => anyhow::bail!("optimizer learning selection is empty"),
        (Some(selected_path), Some(selected_sha256)) => {
            let selected_path = selected_path
                .as_str()
                .context("optimizer learning path must be string")?;
            let selected_sha256 = selected_sha256
                .as_str()
                .context("optimizer learning sha256 must be string")?;
            anyhow::ensure!(
                !selected_path.trim().is_empty(),
                "optimizer learning path is empty"
            );
            anyhow::ensure!(
                selected_sha256.len() == 64
                    && selected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "optimizer learning sha256 is invalid"
            );
            Ok(Some(OptimizerLearningSelection {
                path: PathBuf::from(selected_path),
                sha256: selected_sha256.to_ascii_lowercase(),
            }))
        }
        _ => anyhow::bail!("optimizer learning selection path/hash mismatch"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizerParameterKind {
    Int,
    Float,
}

#[derive(Clone, Debug)]
pub struct OptimizerParameterSpec {
    pub name: String,
    pub kind: OptimizerParameterKind,
    pub low: f64,
    pub high: f64,
    pub step: f64,
    pub default: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum OptimizerParameterValue {
    Int(i64),
    Float(f64),
}

#[derive(Clone, Debug)]
pub struct OptimizerParameterSchema {
    specs: Vec<OptimizerParameterSpec>,
    sha256: [u8; 32],
}

impl OptimizerParameterSpec {
    pub fn grid_size(&self) -> Result<usize> {
        usize::try_from(grid_index(self, self.high)?)
            .context("optimizer parameter grid exceeds usize")?
            .checked_add(1)
            .context("optimizer parameter grid size overflow")
    }

    pub fn default_unit(&self) -> Result<f64> {
        let slots = self.grid_size()?.saturating_sub(1) as f64;
        if slots <= 0.0 {
            Ok(0.5)
        } else {
            Ok(((self.default - self.low) / self.step).round() / slots)
        }
    }

    pub fn value_at_unit(&self, unit: f64) -> Result<OptimizerParameterValue> {
        let max_index = self.grid_size()?.saturating_sub(1) as f64;
        let index = (unit.clamp(0.0, 1.0) * max_index).round();
        let value = round_to_step(self.low + index * self.step, self.step);
        match self.kind {
            OptimizerParameterKind::Int => Ok(OptimizerParameterValue::Int(exact_i64(value)?)),
            OptimizerParameterKind::Float => Ok(OptimizerParameterValue::Float(value)),
        }
    }

    pub fn unit_for_value(&self, value: &OptimizerParameterValue) -> Result<f64> {
        self.validate_value(value)?;
        let index = grid_index(self, value.as_f64())?;
        let max_index = grid_index(self, self.high)?;
        anyhow::ensure!(
            index >= 0 && max_index >= index,
            "optimizer parameter {} grid index outside schema",
            self.name
        );
        let unit = if max_index == 0 {
            0.5
        } else {
            index as f64 / max_index as f64
        };
        anyhow::ensure!(
            self.value_at_unit(unit)? == *value,
            "optimizer parameter {} unit conversion changed value",
            self.name
        );
        Ok(unit)
    }

    pub fn default_value(&self) -> Result<OptimizerParameterValue> {
        match self.kind {
            OptimizerParameterKind::Int => {
                Ok(OptimizerParameterValue::Int(exact_i64(self.default)?))
            }
            OptimizerParameterKind::Float => Ok(OptimizerParameterValue::Float(self.default)),
        }
    }

    fn validate_schema(&self) -> Result<()> {
        anyhow::ensure!(!self.name.is_empty(), "optimizer parameter name is empty");
        anyhow::ensure!(
            self.low.is_finite()
                && self.high.is_finite()
                && self.step.is_finite()
                && self.default.is_finite(),
            "{} has non-finite schema values",
            self.name
        );
        anyhow::ensure!(
            self.high >= self.low && self.step > 0.0,
            "{} requires high >= low and step > 0",
            self.name
        );
        anyhow::ensure!(
            self.default >= self.low && self.default <= self.high,
            "{} default outside bounds",
            self.name
        );
        grid_index(self, self.high)?;
        grid_index(self, self.default)?;
        if self.kind == OptimizerParameterKind::Int {
            for (field, value) in [
                ("low", self.low),
                ("high", self.high),
                ("step", self.step),
                ("default", self.default),
            ] {
                anyhow::ensure!(
                    value.fract() == 0.0
                        && value >= -(MAX_EXACT_INTEGER_F64 as f64)
                        && value <= MAX_EXACT_INTEGER_F64 as f64,
                    "{}.{} must be exactly representable in ordered f64 transport",
                    self.name,
                    field
                );
            }
        }
        Ok(())
    }

    fn validate_value(&self, value: &OptimizerParameterValue) -> Result<()> {
        let numeric = match (self.kind, value) {
            (OptimizerParameterKind::Int, OptimizerParameterValue::Int(value)) => {
                anyhow::ensure!(
                    (-MAX_EXACT_INTEGER_F64..=MAX_EXACT_INTEGER_F64).contains(value),
                    "optimizer parameter {} integer exceeds exact f64 transport",
                    self.name
                );
                *value as f64
            }
            (OptimizerParameterKind::Float, OptimizerParameterValue::Float(value)) => *value,
            (expected, actual) => bail!(
                "optimizer parameter {} type mismatch expected={expected:?} actual={actual:?}",
                self.name
            ),
        };
        anyhow::ensure!(
            numeric.is_finite() && numeric >= self.low && numeric <= self.high,
            "optimizer parameter {} outside bounds value={} low={} high={}",
            self.name,
            numeric,
            self.low,
            self.high
        );
        grid_index(self, numeric)?;
        Ok(())
    }

    fn same_schema(&self, other: &Self) -> bool {
        self.name == other.name
            && self.kind == other.kind
            && self.low.to_bits() == other.low.to_bits()
            && self.high.to_bits() == other.high.to_bits()
            && self.step.to_bits() == other.step.to_bits()
    }
}

impl OptimizerParameterValue {
    pub fn as_f64(&self) -> f64 {
        match self {
            Self::Int(value) => *value as f64,
            Self::Float(value) => *value,
        }
    }
}

impl OptimizerParameterSchema {
    pub fn load_root(path: &Path) -> Result<Self> {
        Self::from_specs(parse_specs(path, true)?)
    }

    pub fn load_symbol(path: &Path) -> Result<Self> {
        Self::from_specs(parse_specs(path, false)?)
    }

    pub fn with_symbol_defaults(&self, path: &Path) -> Result<Self> {
        let symbol_schema = Self::load_symbol(path)?;
        self.with_symbol_schema_defaults(&symbol_schema, path)
    }

    pub(crate) fn with_symbol_schema_defaults(
        &self,
        symbol_schema: &Self,
        path: &Path,
    ) -> Result<Self> {
        let mut specs = self.specs.clone();
        for spec in &mut specs {
            let symbol_spec = symbol_schema
                .specs
                .iter()
                .find(|symbol| symbol.name == spec.name)
                .with_context(|| {
                    format!(
                        "{} missing optimizer parameter {}",
                        path.display(),
                        spec.name
                    )
                })?;
            anyhow::ensure!(
                spec.same_schema(symbol_spec),
                "{}.{} may vary default only",
                path.display(),
                spec.name
            );
            spec.default = symbol_spec.default;
        }
        Self::from_specs(specs)
    }

    pub fn specs(&self) -> &[OptimizerParameterSpec] {
        &self.specs
    }

    pub fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub fn default_map(&self) -> Result<BTreeMap<String, OptimizerParameterValue>> {
        self.specs
            .iter()
            .map(|spec| Ok((spec.name.clone(), spec.default_value()?)))
            .collect()
    }

    pub fn ordered_values(
        &self,
        values: &BTreeMap<String, OptimizerParameterValue>,
    ) -> Result<Vec<OptimizerParameterValue>> {
        anyhow::ensure!(
            values.len() == self.specs.len(),
            "optimizer parameter dimension mismatch expected={} actual={}",
            self.specs.len(),
            values.len()
        );
        self.specs
            .iter()
            .map(|spec| {
                let value = values
                    .get(&spec.name)
                    .with_context(|| format!("missing optimizer parameter {}", spec.name))?;
                spec.validate_value(value)?;
                Ok(value.clone())
            })
            .collect()
    }

    pub fn decode_values(&self, values: &[f64]) -> Result<Vec<OptimizerParameterValue>> {
        anyhow::ensure!(
            values.len() == self.specs.len(),
            "optimizer parameter dimension mismatch expected={} actual={}",
            self.specs.len(),
            values.len()
        );
        self.specs
            .iter()
            .zip(values.iter().copied())
            .map(|(spec, value)| {
                anyhow::ensure!(value.is_finite(), "{} value is nonfinite", spec.name);
                let typed = match spec.kind {
                    OptimizerParameterKind::Int => {
                        anyhow::ensure!(
                            value.fract() == 0.0
                                && value >= -(MAX_EXACT_INTEGER_F64 as f64)
                                && value <= MAX_EXACT_INTEGER_F64 as f64,
                            "{} value exceeds exact ordered f64 integer transport",
                            spec.name
                        );
                        OptimizerParameterValue::Int(exact_i64(value)?)
                    }
                    OptimizerParameterKind::Float => OptimizerParameterValue::Float(value),
                };
                spec.validate_value(&typed)?;
                Ok(typed)
            })
            .collect()
    }

    pub fn apply_map(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &BTreeMap<String, OptimizerParameterValue>,
    ) -> Result<MakerRebateMmStrategyConfig> {
        self.apply_values(base, &self.ordered_values(values)?)
    }

    pub fn apply_symbol_map(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &BTreeMap<String, OptimizerParameterValue>,
        expected_symbol: &str,
    ) -> Result<MakerRebateMmStrategyConfig> {
        self.apply_symbol_values(base, &self.ordered_values(values)?, expected_symbol)
    }

    pub fn apply_defaults(
        &self,
        base: &MakerRebateMmStrategyConfig,
    ) -> Result<MakerRebateMmStrategyConfig> {
        self.apply_values(
            base,
            &self
                .specs
                .iter()
                .map(OptimizerParameterSpec::default_value)
                .collect::<Result<Vec<_>>>()?,
        )
    }

    pub fn apply_values(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &[OptimizerParameterValue],
    ) -> Result<MakerRebateMmStrategyConfig> {
        let config = self.apply_values_unvalidated(base, values)?;
        config.validate_runtime()?;
        Ok(config)
    }

    pub(crate) fn apply_symbol_values(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &[OptimizerParameterValue],
        expected_symbol: &str,
    ) -> Result<MakerRebateMmStrategyConfig> {
        let config = self.apply_values_unvalidated(base, values)?;
        config.validate_symbol_runtime(expected_symbol)?;
        Ok(config)
    }

    fn apply_values_unvalidated(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &[OptimizerParameterValue],
    ) -> Result<MakerRebateMmStrategyConfig> {
        anyhow::ensure!(
            values.len() == self.specs.len(),
            "optimizer parameter dimension mismatch expected={} actual={}",
            self.specs.len(),
            values.len()
        );
        let mut config = base.clone();
        config.optimizer_symbol_configs.clear();
        let mut model_blend_weight_span = None;
        for (spec, value) in self.specs.iter().zip(values) {
            spec.validate_value(value)?;
            let numeric = match value {
                OptimizerParameterValue::Int(_) => 0.0,
                OptimizerParameterValue::Float(value) => *value,
            };
            if spec.name == "live_fair_value_model_model_blend_weight_span_fraction" {
                model_blend_weight_span = Some((numeric, spec.step));
                continue;
            }
            apply_optimizer_parameter(&mut config, &spec.name, spec.kind, numeric, value)
                .with_context(|| format!("apply optimizer parameter {}", spec.name))?;
        }
        if let Some((span_fraction, step)) = model_blend_weight_span {
            let min_weight = config.live_fair_value_model.model_blend_min_weight;
            config.live_fair_value_model.model_blend_max_weight =
                round_to_step(min_weight + span_fraction * (1.0 - min_weight), step);
        }
        Ok(config)
    }

    fn from_specs(mut specs: Vec<OptimizerParameterSpec>) -> Result<Self> {
        anyhow::ensure!(!specs.is_empty(), "optimizer parameter schema is empty");
        specs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        for pair in specs.windows(2) {
            anyhow::ensure!(
                pair[0].name != pair[1].name,
                "duplicate optimizer parameter {}",
                pair[0].name
            );
        }
        for spec in &specs {
            spec.validate_schema()?;
        }
        let sha256 = schema_sha256(&specs)?;
        Ok(Self { specs, sha256 })
    }
}

fn parse_specs(path: &Path, allow_optimizer_section: bool) -> Result<Vec<OptimizerParameterSpec>> {
    let content = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let docs =
        YamlLoader::load_from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let hash = docs
        .first()
        .and_then(Yaml::as_hash)
        .ok_or_else(|| anyhow!("{} must be a non-empty YAML map", path.display()))?;
    let mut specs = Vec::new();
    for (key, node) in hash {
        let name = key
            .as_str()
            .ok_or_else(|| anyhow!("{} parameter key must be string", path.display()))?;
        if name == "optimizer" {
            anyhow::ensure!(
                allow_optimizer_section,
                "{} symbol configuration contains root optimizer settings",
                path.display()
            );
            continue;
        }
        if name == "allocation" {
            anyhow::ensure!(
                !allow_optimizer_section,
                "{} root optimizer schema contains live allocation",
                path.display()
            );
            continue;
        }
        if name == "learning" {
            anyhow::ensure!(
                !allow_optimizer_section,
                "{} root optimizer schema contains symbol learning selection",
                path.display()
            );
            continue;
        }
        let kind = match yaml_string(node, "type")?.as_str() {
            "int" => OptimizerParameterKind::Int,
            "float" => OptimizerParameterKind::Float,
            other => bail!("{name}.type must be int or float, got {other}"),
        };
        let node_hash = node
            .as_hash()
            .with_context(|| format!("{name} optimizer parameter must be a map"))?;
        for key in node_hash.keys() {
            let key = key
                .as_str()
                .with_context(|| format!("{name} optimizer field must be string"))?;
            anyhow::ensure!(
                matches!(key, "type" | "low" | "high" | "step" | "default"),
                "{name} contains unknown optimizer field {key}"
            );
        }
        if kind == OptimizerParameterKind::Int {
            for field in ["low", "high", "step", "default"] {
                anyhow::ensure!(
                    matches!(yaml_child(node, field)?, Yaml::Integer(_)),
                    "{name}.{field} must be an integer scalar"
                );
            }
        }
        specs.push(OptimizerParameterSpec {
            name: name.to_string(),
            kind,
            low: yaml_f64(node, "low")?,
            high: yaml_f64(node, "high")?,
            step: yaml_f64(node, "step")?,
            default: yaml_f64(node, "default")?,
        });
    }
    Ok(specs)
}

fn yaml_child<'a>(node: &'a Yaml, key: &str) -> Result<&'a Yaml> {
    node.as_hash()
        .and_then(|hash| hash.get(&Yaml::String(key.to_string())))
        .ok_or_else(|| anyhow!("missing YAML key {key}"))
}

fn yaml_string(node: &Yaml, key: &str) -> Result<String> {
    yaml_child(node, key)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("YAML key {key} must be string"))
}

fn yaml_f64(node: &Yaml, key: &str) -> Result<f64> {
    match yaml_child(node, key)? {
        Yaml::Real(value) => value.parse().with_context(|| format!("parse {key}")),
        Yaml::Integer(value) => {
            anyhow::ensure!(
                (-MAX_EXACT_INTEGER_F64..=MAX_EXACT_INTEGER_F64).contains(value),
                "YAML key {key} exceeds exact ordered f64 integer transport"
            );
            Ok(*value as f64)
        }
        _ => bail!("YAML key {key} must be number"),
    }
}

fn round_to_step(value: f64, step: f64) -> f64 {
    let text = format!("{step:.12}");
    let decimals = text
        .trim_end_matches('0')
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

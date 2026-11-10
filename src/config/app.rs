use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::watch;

use super::{
    apply_optimizer_defaults, apply_optimizer_symbol_defaults, apply_runtime_symbol_defaults,
    BbwickOracleConfig, MakerRebateMmStrategyConfig, ObservabilityConfig, PolymarketConfig,
    RiskConfig,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub symbols: Vec<String>,
    pub max_order_qty: f64,
    pub strategy: StrategyConfig,
    pub polymarket: PolymarketConfig,
    pub bbwick_oracle: BbwickOracleConfig,
    pub risk: RiskConfig,
    pub observability: ObservabilityConfig,
    pub capture: CaptureConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureConfig {
    pub enabled: bool,
    pub base_dir: String,
    pub max_duration_ms: u64,
    pub min_save_duration_ms: u64,
    pub max_sessions: usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct StrategyConfig {
    pub maker_rebate_mm: MakerRebateMmStrategyConfig,
}

/// Shared, hot-reloadable strategy config. The strategy reads from this on
/// each tick; configuration reload writes to it.
pub type SharedStrategyConfig = watch::Sender<MakerRebateMmStrategyConfig>;

impl AppConfig {
    pub fn load() -> Result<Self> {
        // Runtime settings come from config/main.toml, then PMHFT__ env
        // overrides loaded from ../.env / process env.
        let mut environment_values = ::config::Source::collect(
            &::config::Environment::with_prefix("PMHFT")
                .separator("__")
                .source(Some(std::env::vars().collect()))
                .try_parsing(true),
        )
        .context("failed to build config sources")?;
        if let Some(value) = environment_values.remove("polymarket.execution.pause") {
            environment_values.insert("polymarket.execution.PAUSE".to_owned(), value);
        }
        let mut environment_config = ::config::Config::default();
        environment_config.cache =
            ::config::Value::new(None, ::config::ValueKind::Table(environment_values));

        let builder = ::config::Config::builder()
            .add_source(::config::File::from(PathBuf::from("config/main.toml")).required(true))
            .add_source(environment_config);

        let mut cfg = builder
            .build()
            .context("failed to build config sources")?
            .try_deserialize::<Self>()
            .context("failed to deserialize app config")?;
        apply_runtime_symbol_defaults(&mut cfg.strategy.maker_rebate_mm, Path::new("."))
            .context("failed to validate and load runtime symbol configurations")?;
        cfg.validate().context("config validation failed")?;
        Ok(cfg)
    }

    pub fn load_from_path_with_optimizer_path(
        path: impl AsRef<Path>,
        optimizer_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read app config {}", path.display()))?;
        Self::load_from_bytes_with_optimizer_path(&bytes, optimizer_path)
            .with_context(|| format!("failed to load app config {}", path.display()))
    }

    pub fn load_from_bytes_with_optimizer_path(
        bytes: &[u8],
        optimizer_path: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::parse_from_bytes(bytes)?.with_optimizer_path(optimizer_path)
    }

    pub fn parse_from_bytes(bytes: &[u8]) -> Result<Self> {
        let raw = std::str::from_utf8(bytes).context("app config is not valid UTF-8")?;
        let value = raw
            .parse::<toml::Value>()
            .context("failed to parse app config")?;
        value
            .try_into::<Self>()
            .context("failed to deserialize app config")
    }

    pub fn with_optimizer_path(self, optimizer_path: impl AsRef<Path>) -> Result<Self> {
        self.with_optimizer_profile(optimizer_path.as_ref(), false)
    }

    pub fn with_installed_optimizer_path(self, optimizer_path: impl AsRef<Path>) -> Result<Self> {
        let oracle_buy_and_hold = self.strategy.maker_rebate_mm.oracle_buy_and_hold;
        self.with_optimizer_profile(optimizer_path.as_ref(), oracle_buy_and_hold)
    }

    fn with_optimizer_profile(
        mut self,
        optimizer_path: &Path,
        oracle_buy_and_hold: bool,
    ) -> Result<Self> {
        apply_optimizer_defaults(
            &mut self.strategy.maker_rebate_mm,
            optimizer_path,
            oracle_buy_and_hold,
        )
        .with_context(|| {
            format!(
                "failed to validate optimizer schema and load symbol configurations from {}",
                optimizer_path.display()
            )
        })?;
        self.validate().context("app config validation failed")?;
        Ok(self)
    }

    pub fn load_from_path_with_symbol_optimizer_snapshot(
        path: impl AsRef<Path>,
        root_optimizer_path: impl AsRef<Path>,
        symbol_optimizer_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let root_optimizer_path = root_optimizer_path.as_ref();
        let symbol_optimizer_path = symbol_optimizer_path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read app config {}", path.display()))?;
        let value = raw
            .parse::<toml::Value>()
            .with_context(|| format!("failed to parse app config {}", path.display()))?;
        let mut cfg = value
            .try_into::<Self>()
            .with_context(|| format!("failed to deserialize app config {}", path.display()))?;
        apply_optimizer_symbol_defaults(
            &mut cfg.strategy.maker_rebate_mm,
            root_optimizer_path,
            symbol_optimizer_path,
        )
        .with_context(|| {
            format!(
                "failed to validate root optimizer schema and apply symbol configuration root={} symbol={}",
                root_optimizer_path.display(),
                symbol_optimizer_path.display()
            )
        })?;
        cfg.validate()
            .with_context(|| format!("config validation failed {}", path.display()))?;
        Ok(cfg)
    }
}

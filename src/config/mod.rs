mod app;
mod bbwick;
mod defaults;
mod observability;
mod optimizer_knobs;
mod optimizer_parameters;
mod polymarket;
mod risk;
mod strategy;
mod validation;
mod validation_bbwick;
mod validation_runtime;

pub use app::{AppConfig, CaptureConfig, SharedStrategyConfig, StrategyConfig};
pub(crate) use bbwick::canonical_oracle_symbol;
pub use bbwick::{
    BbwickOracleConfig, BbwickOracleHistoryConfig, BbwickOracleRuntimeConfig,
    BbwickOracleTransportConfig,
};
pub use observability::{
    LogFormat, ObservabilityConfig, ObservabilityDashboardConfig, ObservabilityLoggingConfig,
};
pub use optimizer_knobs::installed_optimizer_symbol_path;
pub(crate) use optimizer_knobs::{
    apply_optimizer_defaults, apply_optimizer_symbol_defaults, apply_runtime_symbol_defaults,
};
pub use optimizer_parameters::{
    optimizer_learning_selection, OptimizerLearningSelection, OptimizerParameterKind,
    OptimizerParameterSchema, OptimizerParameterSpec, OptimizerParameterValue,
};
pub use polymarket::{
    PolymarketConfig, PolymarketDiscoveryConfig, PolymarketExecutionConfig,
    PolymarketExecutionMode, PolymarketMarketDataMode, PolymarketOrderType,
};
pub use risk::RiskConfig;
pub(crate) use risk::POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS;
pub use strategy::VALID_ASSETS;
pub use strategy::{CurrentBlendedFairValueConfig, MakerRebateMmStrategyConfig};

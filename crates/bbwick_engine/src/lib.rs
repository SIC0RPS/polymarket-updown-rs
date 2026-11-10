#![deny(dead_code)]

pub mod config;
mod ids;
pub mod ingest_event;
pub mod live_lanes;
pub mod market_data;
pub mod metrics;
pub mod oracle;
mod oracle_history;
mod oracle_ingest;
mod oracle_runtime;
pub mod oracle_state_updates;
pub mod polymarket_ws;
pub mod provider_ws;
pub mod state;
pub mod wire;

pub use ingest_event::IngestEvent;
pub use market_data::{CanonicalTopOfBook, SourceTsKind, TopOfBookIngressFrame};
pub use oracle::{
    OracleEngine, OracleEngineConfig, OracleHistoryConfig, OracleRuntimeTask, OracleTick,
    PolymarketDiscoveryController,
};
pub use oracle_history::{
    OracleCandleWindow, OracleHistory, OracleHistoryView, OracleTradeFlowCoverage,
    OracleTradeFlowSnapshot,
};
pub use oracle_runtime::ensure_engine_history_ready;
pub use state::{DiscoverySnapshot, MarketSelectionSnapshot, PolymarketOddsState};
pub use wire::StepFrame;

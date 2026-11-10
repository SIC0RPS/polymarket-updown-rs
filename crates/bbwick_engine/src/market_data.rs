use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceTsKind {
    Unavailable,
    Exchange,
    ProviderEvent,
    #[default]
    EngineIngestWallclock,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CanonicalTopOfBook {
    pub symbol: String,
    pub bid_px: f64,
    pub ask_px: f64,
    pub bid_sz: f64,
    pub ask_sz: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_imbalance_l5: Option<f64>,
    pub source_ts_ns: u64,
    pub source_ts_kind: SourceTsKind,
    pub ingest_mono_ns: u64,
    pub seq_no: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopOfBookIngressFrame {
    pub symbol: String,
    pub source_ts_ms: u64,
    pub source_ts_kind: SourceTsKind,
    pub bid_px: f64,
    pub ask_px: f64,
    pub bid_sz: f64,
    pub ask_sz: f64,
    pub queue_imbalance_l5: Option<f64>,
}

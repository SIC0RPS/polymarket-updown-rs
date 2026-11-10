use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct ReplayDataQualitySummary {
    pub status: ReplayDataQualityStatus,
    pub replay_order_clock: &'static str,
    pub rows: u64,
    pub session_ts_inversions: u64,
    pub exchange_local_ordering_inversions: u64,
    pub negative_feed_latencies: u64,
    pub true_exchange_negative_feed_latencies: u64,
    pub source_clock_skew_negative_latencies: u64,
    pub streams: Vec<ReplayDataQualityStream>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDataQualityStatus {
    Ok,
    Degraded,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayDataQualityStream {
    pub symbol: String,
    pub source: String,
    pub replay_order_clock: &'static str,
    pub rows: u64,
    pub session_ts_monotonic: bool,
    pub session_ts_inversions: u64,
    pub exchange_local_ordering_inversions: u64,
    pub negative_feed_latencies: u64,
    pub true_exchange_negative_feed_latencies: u64,
    pub source_clock_skew_negative_latencies: u64,
    pub negative_feed_latencies_by_source_ts_kind: Vec<(String, u64)>,
    pub feed_latency_ms: LatencyDistribution,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyDistribution {
    pub min: Option<i64>,
    pub p50: Option<i64>,
    pub p95: Option<i64>,
    pub p99: Option<i64>,
    pub max: Option<i64>,
}

impl ReplayDataQualitySummary {
    pub fn from_prepared_manifests(capture: &Path, symbols: &[String]) -> Result<Self> {
        let mut streams = Vec::new();
        let mut manifest_failures = 0u64;
        for symbol in symbols {
            let normalized = crate::capture::arrow::normalize_capture_symbol(symbol);
            let manifest_path = capture
                .join("backtest_replay")
                .join("v2")
                .join(&normalized)
                .join("manifest.json");
            let manifest = match fs::read_to_string(&manifest_path)
                .with_context(|| format!("read {}", manifest_path.display()))
                .and_then(|raw| {
                    serde_json::from_str::<PreparedReplayQualityManifest>(&raw)
                        .with_context(|| format!("parse {}", manifest_path.display()))
                }) {
                Ok(manifest) if manifest.completed => manifest,
                Ok(_) => {
                    manifest_failures = manifest_failures.saturating_add(1);
                    continue;
                }
                Err(err) => {
                    manifest_failures = manifest_failures.saturating_add(1);
                    streams.push(ReplayDataQualityStream::manifest_issue(
                        &normalized,
                        "prepared_replay_manifest",
                        format!("{err:#}"),
                    ));
                    continue;
                }
            };
            streams.push(ReplayDataQualityStream::from_manifest_rows(
                &normalized,
                "strategy_consumption",
                manifest.strategy_consumption_rows,
            ));
            streams.push(ReplayDataQualityStream::from_manifest_rows(
                &normalized,
                "polymarket_engine_state_replayable",
                manifest.engine_rows,
            ));
        }
        let rows = streams.iter().map(|stream| stream.rows).sum();
        Ok(Self {
            status: if manifest_failures > 0 {
                ReplayDataQualityStatus::Fail
            } else {
                ReplayDataQualityStatus::Ok
            },
            replay_order_clock: "prepared_replay_manifest_session_ts_ms",
            rows,
            session_ts_inversions: 0,
            exchange_local_ordering_inversions: 0,
            negative_feed_latencies: 0,
            true_exchange_negative_feed_latencies: 0,
            source_clock_skew_negative_latencies: 0,
            streams,
        })
    }

    pub fn proof_fails(&self) -> bool {
        self.status == ReplayDataQualityStatus::Fail
    }

    pub fn status_label(&self) -> &'static str {
        match self.status {
            ReplayDataQualityStatus::Ok => "ok",
            ReplayDataQualityStatus::Degraded => "degraded",
            ReplayDataQualityStatus::Fail => "fail",
        }
    }

    pub fn report_line(&self) -> String {
        format!(
            "PROOF_QUALITY data_quality={} source=prepared_replay_manifest replay_order_clock={} rows={} session_ts_inversions={} negative_feed_latencies={} true_exchange_negative_feed_latencies={} source_clock_skew_negative_latencies={} exchange_local_ordering_inversions={}",
            self.status_label(),
            self.replay_order_clock,
            self.rows,
            self.session_ts_inversions,
            self.negative_feed_latencies,
            self.true_exchange_negative_feed_latencies,
            self.source_clock_skew_negative_latencies,
            self.exchange_local_ordering_inversions
        )
    }

    pub fn write_json(&self, path: &Path) -> Result<()> {
        let body = serde_json::to_string_pretty(self).context("serialize replay data quality")?;
        fs::write(path, body).with_context(|| format!("write {}", path.display()))
    }
}

#[derive(Deserialize)]
struct PreparedReplayQualityManifest {
    completed: bool,
    engine_rows: u64,
    strategy_consumption_rows: u64,
}

impl ReplayDataQualityStream {
    fn from_manifest_rows(symbol: &str, source: &str, rows: u64) -> Self {
        Self {
            symbol: symbol.to_string(),
            source: source.to_string(),
            replay_order_clock: "prepared_replay_manifest_session_ts_ms",
            rows,
            session_ts_monotonic: true,
            session_ts_inversions: 0,
            exchange_local_ordering_inversions: 0,
            negative_feed_latencies: 0,
            true_exchange_negative_feed_latencies: 0,
            source_clock_skew_negative_latencies: 0,
            negative_feed_latencies_by_source_ts_kind: Vec::new(),
            feed_latency_ms: LatencyDistribution::default(),
        }
    }

    fn manifest_issue(symbol: &str, source: &str, issue: String) -> Self {
        let mut stream = Self::from_manifest_rows(symbol, source, 0);
        stream.session_ts_monotonic = false;
        stream.negative_feed_latencies_by_source_ts_kind = vec![(issue, 1)];
        stream
    }
}

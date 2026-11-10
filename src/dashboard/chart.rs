use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use canonical_ohlc_arrow::{read_all, read_tail};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::model::now_ms;
use super::DashboardState;
use crate::config::VALID_ASSETS;
use crate::event_ledger::{
    append_event, current_run_id, history_state_snapshot, make_event_id, CanonicalEventRecord,
    CanonicalEventSource, CanonicalEventType,
};
use crate::risk::{RiskHandle, RiskSnapshot};

const PRICE_SERIES_LIMIT: usize = 1_000;
const EQUITY_HISTORY_RESPONSE_LIMIT: usize = 1_000;
const LIVE_WINDOW_MS: u64 = 5 * 60 * 1_000;

#[derive(Debug, Deserialize)]
pub(super) struct PriceSeriesQuery {
    asset: Option<String>,
    range: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct PriceSeriesPoint {
    time: u64,
    value: f64,
}

#[derive(Debug, PartialEq, Serialize)]
pub(super) struct PriceSeriesResponse {
    asset: String,
    range: String,
    points: Vec<PriceSeriesPoint>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EquityHistoryQuery {
    range: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct EquityHistoryPoint {
    timestamp: u64,
    equity: f64,
}

#[derive(Debug, PartialEq, Serialize)]
pub(super) struct EquityHistoryResponse {
    range: String,
    points: Vec<EquityHistoryPoint>,
    downsampled: bool,
    coverage_start_ms: Option<u64>,
    coverage_end_ms: Option<u64>,
    history_complete: bool,
}

#[derive(Debug)]
pub(super) struct EquityHistory {
    last_observation_timestamp_ms: Option<u64>,
    points: VecDeque<EquityHistoryPoint>,
}

impl EquityHistory {
    pub(super) fn from_canonical_events() -> Self {
        let mut points_by_timestamp = BTreeMap::new();
        for event in history_state_snapshot().rows.iter() {
            if event.event_type != CanonicalEventType::EquitySnapshot
                || event
                    .context
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    != Some("live_snapshot")
                || event.ts_ms == 0
            {
                continue;
            }
            let Some(equity) = event
                .metrics
                .get("equity")
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            points_by_timestamp.insert(
                event.ts_ms,
                EquityHistoryPoint {
                    timestamp: event.ts_ms,
                    equity,
                },
            );
        }
        let points = points_by_timestamp.into_values().collect::<VecDeque<_>>();
        Self {
            last_observation_timestamp_ms: points.back().map(|point| point.timestamp),
            points,
        }
    }

    fn accepts(&self, point: &EquityHistoryPoint) -> bool {
        point.timestamp > 0
            && point.equity.is_finite()
            && self
                .last_observation_timestamp_ms
                .is_none_or(|last| point.timestamp > last)
    }

    fn record(&mut self, point: EquityHistoryPoint) {
        if !self.accepts(&point) {
            return;
        }
        self.last_observation_timestamp_ms = Some(point.timestamp);
        self.points.push_back(point);
    }

    fn response(&self, range: &str, backend_now_ms: u64) -> EquityHistoryResponse {
        let cutoff_ms = range_cutoff_ms(range, backend_now_ms);
        let filtered = self
            .points
            .iter()
            .filter(|point| cutoff_ms.is_none_or(|cutoff| point.timestamp >= cutoff))
            .cloned()
            .collect::<Vec<_>>();
        let coverage_start_ms = filtered.first().map(|point| point.timestamp);
        let coverage_end_ms = filtered.last().map(|point| point.timestamp);
        let (points, downsampled) = downsample_equity_points(filtered);
        EquityHistoryResponse {
            range: range.to_string(),
            points,
            downsampled,
            coverage_start_ms,
            coverage_end_ms,
            history_complete: false,
        }
    }
}

fn downsample_equity_points(points: Vec<EquityHistoryPoint>) -> (Vec<EquityHistoryPoint>, bool) {
    if points.len() <= EQUITY_HISTORY_RESPONSE_LIMIT {
        return (points, false);
    }
    let last_index = points.len() - 1;
    let target_last_index = EQUITY_HISTORY_RESPONSE_LIMIT - 1;
    let mut sampled = Vec::with_capacity(EQUITY_HISTORY_RESPONSE_LIMIT);
    for index in 0..EQUITY_HISTORY_RESPONSE_LIMIT {
        let source_index = (index * last_index + target_last_index / 2) / target_last_index;
        sampled.push(points[source_index].clone());
    }
    (sampled, true)
}

fn equity_observation(snapshot: &RiskSnapshot) -> Option<EquityHistoryPoint> {
    let timestamp = snapshot
        .account_equity_updated_at_ms
        .filter(|value| *value > 0)?;
    let equity = snapshot
        .current_wallet_equity
        .filter(|value| value.is_finite())?;
    Some(EquityHistoryPoint { timestamp, equity })
}

pub(super) fn spawn_equity_history_collector(
    risk: Option<RiskHandle>,
    history: Arc<Mutex<EquityHistory>>,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    let risk = risk?;
    Some(tokio::spawn(async move {
        let mut snapshots = risk.snapshot();
        loop {
            let snapshot = snapshots.borrow().clone();
            if let Some(point) = equity_observation(&snapshot) {
                let accepted = history
                    .lock()
                    .expect("dashboard equity history mutex poisoned")
                    .accepts(&point);
                if accepted {
                    let timestamp = point.timestamp;
                    let equity = point.equity;
                    let persisted = tokio::task::spawn_blocking(move || -> Result<()> {
                        let run_id = current_run_id()?;
                        append_event(&CanonicalEventRecord {
                            event_id: make_event_id(
                                CanonicalEventType::EquitySnapshot,
                                timestamp,
                                run_id.clone(),
                            ),
                            run_id,
                            ts_ms: timestamp,
                            event_type: CanonicalEventType::EquitySnapshot,
                            source: CanonicalEventSource::Runtime,
                            subject: "account_equity".to_string(),
                            refs: serde_json::json!({}),
                            metrics: serde_json::json!({ "equity": equity }),
                            context: serde_json::json!({ "kind": "live_snapshot" }),
                        })
                    })
                    .await;
                    match persisted {
                        Ok(Ok(())) => history
                            .lock()
                            .expect("dashboard equity history mutex poisoned")
                            .record(point),
                        Ok(Err(error)) => tracing::warn!(
                            target: "dashboard.equity_history",
                            timestamp_ms = timestamp,
                            error = %error,
                            "equity_snapshot_append_failed"
                        ),
                        Err(error) => tracing::warn!(
                            target: "dashboard.equity_history",
                            timestamp_ms = timestamp,
                            error = %error,
                            "equity_snapshot_append_task_failed"
                        ),
                    }
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }
    }))
}

pub(super) async fn price_series(
    State(state): State<DashboardState>,
    Query(query): Query<PriceSeriesQuery>,
) -> Result<Json<PriceSeriesResponse>, (StatusCode, String)> {
    let asset = parse_asset(query.asset.as_deref())?;
    let range = parse_range(query.range.as_deref())?;
    let points = if range == "live" {
        live_price_points(&state, asset).map_err(internal_error)?
    } else {
        recorded_price_points(&state, asset, range, now_ms()).map_err(internal_error)?
    };
    Ok(Json(PriceSeriesResponse {
        asset: asset.to_string(),
        range: range.to_string(),
        points,
    }))
}

pub(super) async fn equity_history(
    State(state): State<DashboardState>,
    Query(query): Query<EquityHistoryQuery>,
) -> Result<Json<EquityHistoryResponse>, (StatusCode, String)> {
    let range = parse_range(query.range.as_deref())?;
    let response = state
        .equity_history
        .lock()
        .expect("dashboard equity history mutex poisoned")
        .response(range, now_ms());
    Ok(Json(response))
}

fn parse_asset(raw: Option<&str>) -> Result<&'static str, (StatusCode, String)> {
    let normalized = raw.unwrap_or_default().trim().to_ascii_uppercase();
    VALID_ASSETS
        .iter()
        .copied()
        .find(|asset| *asset == normalized)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "asset must be one of BTC, ETH, SOL, XRP".to_string(),
            )
        })
}

fn parse_range(raw: Option<&str>) -> Result<&'static str, (StatusCode, String)> {
    let normalized = raw.unwrap_or_default().trim().to_ascii_lowercase();
    ["live", "15m", "1h", "all"]
        .into_iter()
        .find(|range| *range == normalized)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                "range must be one of live, 15m, 1h, all".to_string(),
            )
        })
}

fn live_price_points(state: &DashboardState, asset: &str) -> Result<Vec<PriceSeriesPoint>> {
    let ui = state.observability.ui_state().borrow().clone();
    let mut points_by_source_timestamp = BTreeMap::new();
    for (symbol, rows) in &ui.oracle_tick_history {
        if !symbol.eq_ignore_ascii_case(asset) {
            continue;
        }
        for row in rows {
            anyhow::ensure!(
                row.px.is_finite() && row.px > 0.0,
                "UI tick history contains an invalid price asset={asset} timestamp_ms={}",
                row.ts_ms
            );
            points_by_source_timestamp.insert(row.ts_ms, row.px);
        }
    }
    Ok(points_by_source_timestamp
        .into_iter()
        .map(|(timestamp_ms, value)| PriceSeriesPoint {
            time: timestamp_ms / 1_000,
            value,
        })
        .collect())
}

fn recorded_price_points(
    state: &DashboardState,
    asset: &str,
    range: &str,
    backend_now_ms: u64,
) -> Result<Vec<PriceSeriesPoint>> {
    let symbol = state
        .price_symbols
        .get(asset)
        .with_context(|| format!("configured price symbol is unavailable for asset={asset}"))?;
    let path = state
        .price_history_root
        .join(symbol)
        .join("1m")
        .join("ohlc.arrow");
    let bars = if range == "all" {
        read_all(&path)
    } else {
        read_tail(&path, PRICE_SERIES_LIMIT)
    }
    .with_context(|| format!("failed to read recorded price history {}", path.display()))?;
    let cutoff_ms = range_cutoff_ms(range, backend_now_ms);
    let points = bars
        .into_iter()
        .filter_map(|bar| {
            let timestamp_ms = match u64::try_from(bar.ts_ms) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(error) => return Some(Err(error.into())),
            };
            if cutoff_ms.is_some_and(|cutoff| timestamp_ms < cutoff) {
                return None;
            }
            Some((|| {
                anyhow::ensure!(
                    bar.close.is_finite() && bar.close > 0.0,
                    "recorded price history contains an invalid close asset={asset} timestamp_ms={timestamp_ms}"
                );
                Ok(PriceSeriesPoint {
                    time: timestamp_ms / 1_000,
                    value: bar.close,
                })
            })())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(downsample_price_points(points))
}

fn downsample_price_points(points: Vec<PriceSeriesPoint>) -> Vec<PriceSeriesPoint> {
    if points.len() <= PRICE_SERIES_LIMIT {
        return points;
    }
    let last_index = points.len() - 1;
    let target_last_index = PRICE_SERIES_LIMIT - 1;
    let mut sampled = Vec::with_capacity(PRICE_SERIES_LIMIT);
    for index in 0..PRICE_SERIES_LIMIT {
        let source_index = (index * last_index + target_last_index / 2) / target_last_index;
        sampled.push(points[source_index].clone());
    }
    sampled
}

fn range_cutoff_ms(range: &str, backend_now_ms: u64) -> Option<u64> {
    match range {
        "live" => Some(backend_now_ms.saturating_sub(LIVE_WINDOW_MS)),
        "15m" => Some(backend_now_ms.saturating_sub(15 * 60 * 1_000)),
        "1h" => Some(backend_now_ms.saturating_sub(60 * 60 * 1_000)),
        "all" => None,
        _ => unreachable!("range is validated before cutoff selection"),
    }
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}"))
}

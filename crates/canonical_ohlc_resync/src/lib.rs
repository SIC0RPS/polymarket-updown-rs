use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use canonical_ohlc_arrow::{read_all, read_tail, OhlcBar, OhlcFileTransaction};
use reqwest_middleware::ClientWithMiddleware;
use serde::Serialize;
use tracing::{debug, info};

mod binance_rest;

pub use binance_rest::{
    build_resync_http_client, fetch_binance_klines_range, fetch_binance_server_time_ms,
    repair_gaps, validate_continuity, validate_range_bars, ResyncClient,
};
use binance_rest::{repair_gaps_at_server_time, RESYNC_RETRY_MAX_MS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncErrorKind {
    InvalidInput,
    Io,
    Http,
    Validation,
    Parse,
    Internal,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResyncError {
    pub kind: ResyncErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ResyncError {
    fn new(kind: ResyncErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after_ms: None,
        }
    }

    fn with_retry_after(mut self, retry_after_ms: Option<u64>) -> Self {
        self.retry_after_ms = retry_after_ms;
        self
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ResyncErrorKind::InvalidInput, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ResyncErrorKind::Io, message)
    }

    pub fn http(message: impl Into<String>, retry_after_ms: Option<u64>) -> Self {
        Self::new(ResyncErrorKind::Http, message).with_retry_after(retry_after_ms)
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ResyncErrorKind::Validation, message)
    }

    pub fn parse(message: impl Into<String>) -> Self {
        Self::new(ResyncErrorKind::Parse, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ResyncErrorKind::Internal, message)
    }
}

impl std::fmt::Display for ResyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ResyncError {}

#[derive(Debug, Clone, Serialize)]
pub struct GapInfo {
    pub gap_start_ts_ms: i64,
    pub gap_end_ts_ms: i64,
    pub prev_ts_ms: i64,
    pub next_ts_ms: i64,
    pub delta_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditSummary {
    pub interval_ms: i64,
    pub window: Option<usize>,
    pub count: usize,
    pub expected_count: usize,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
    pub first_ts: Option<i64>,
    pub last_ts: Option<i64>,
}

impl AuditSummary {
    fn empty(interval_ms: i64, window: Option<usize>) -> Self {
        Self {
            interval_ms,
            window,
            count: 0,
            expected_count: 0,
            min_ts: None,
            max_ts: None,
            first_ts: None,
            last_ts: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditStatus {
    Ok,
    Gap,
    Empty,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditReport {
    pub status: AuditStatus,
    pub summary: AuditSummary,
    pub gap: Option<GapInfo>,
}

#[derive(Debug, Clone)]
pub struct EnsureConfig<'a> {
    pub arrow_path: &'a Path,
    pub symbol: &'a str,
    pub timeframe: &'a str,
    pub interval_ms: i64,
    pub rest_base_url: &'a str,
    pub window: Option<usize>,
    pub latest_closed_ts_ms: Option<i64>,
    pub fetch_bars: Option<usize>,
    pub required_bars: Option<usize>,
    pub max_gap_repairs: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EnsureStatus {
    Ready,
    InsufficientHistory {
        available: usize,
        required: usize,
    },
    PausedForResync {
        reason: String,
        next_retry_at_ms: i64,
        error_kind: ResyncErrorKind,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct EnsureOutcome {
    pub status: EnsureStatus,
    pub summary: AuditSummary,
    pub latest_closed_ts_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairOutcome {
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub expected_count: usize,
    pub received_count: usize,
    pub duration_ms: u64,
    pub new_total_count: usize,
}

pub fn parse_interval_ms(interval: &str) -> Option<i64> {
    if interval.is_empty() {
        return None;
    }
    let unit_start = interval.char_indices().next_back()?.0;
    let (num_part, unit) = interval.split_at(unit_start);
    let value: i64 = num_part.parse().ok()?;
    let unit_ms = match unit {
        "m" | "M" => 60_000,
        "h" | "H" => 3_600_000,
        "d" | "D" => 86_400_000,
        "w" | "W" => 7 * 86_400_000,
        _ => return None,
    };
    value
        .checked_mul(unit_ms)
        .filter(|interval_ms| *interval_ms > 0)
}

pub fn latest_closed_open_ts(now_ms: i64, interval_ms: i64) -> i64 {
    if interval_ms <= 0 || now_ms <= 0 {
        return 0;
    }
    let aligned = (now_ms / interval_ms) * interval_ms;
    aligned.saturating_sub(interval_ms)
}

fn expected_bar_count(start_ts_ms: i64, end_ts_ms: i64, interval_ms: i64) -> usize {
    if interval_ms <= 0 || end_ts_ms < start_ts_ms {
        return 0;
    }
    ((end_ts_ms - start_ts_ms) / interval_ms + 1).max(0) as usize
}

fn start_ts_for_count(end_ts_ms: i64, interval_ms: i64, count: usize) -> i64 {
    if interval_ms <= 0 || count == 0 {
        return end_ts_ms;
    }
    end_ts_ms.saturating_sub(interval_ms.saturating_mul(count.saturating_sub(1) as i64))
}

fn now_ms() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}

pub fn audit_continuity(
    path: &Path,
    interval_ms: i64,
    window: Option<usize>,
) -> Result<AuditReport, ResyncError> {
    if interval_ms <= 0 {
        return Err(ResyncError::invalid_input("interval_ms must be > 0"));
    }
    let bars = if let Some(limit) = window {
        read_tail(path, limit).map_err(|err| ResyncError::io(format!("read_tail failed: {err}")))?
    } else {
        read_all(path).map_err(|err| ResyncError::io(format!("read_all failed: {err}")))?
    };
    if bars.is_empty() {
        return Ok(AuditReport {
            status: AuditStatus::Empty,
            summary: AuditSummary::empty(interval_ms, window),
            gap: None,
        });
    }
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for bar in &bars {
        min_ts = min_ts.min(bar.ts_ms);
        max_ts = max_ts.max(bar.ts_ms);
    }
    let count = bars.len();
    let expected_count = expected_bar_count(min_ts, max_ts, interval_ms);
    let summary = AuditSummary {
        interval_ms,
        window,
        count,
        expected_count,
        min_ts: Some(min_ts),
        max_ts: Some(max_ts),
        first_ts: Some(bars.first().map(|bar| bar.ts_ms).unwrap_or(min_ts)),
        last_ts: Some(bars.last().map(|bar| bar.ts_ms).unwrap_or(max_ts)),
    };

    let mut expected = bars[0].ts_ms;
    for bar in &bars {
        if bar.ts_ms != expected {
            if bar.ts_ms < expected {
                return Err(ResyncError::validation(format!(
                    "non-monotonic bar sequence at ts_ms={} (expected={})",
                    bar.ts_ms, expected
                )));
            }
            let gap_start = expected;
            let gap_end = bar.ts_ms.saturating_sub(interval_ms);
            return Ok(AuditReport {
                status: AuditStatus::Gap,
                summary,
                gap: Some(GapInfo {
                    gap_start_ts_ms: gap_start,
                    gap_end_ts_ms: gap_end,
                    prev_ts_ms: expected.saturating_sub(interval_ms),
                    next_ts_ms: bar.ts_ms,
                    delta_ms: bar.ts_ms.saturating_sub(expected),
                }),
            });
        }
        expected = expected.saturating_add(interval_ms);
    }
    Ok(AuditReport {
        status: AuditStatus::Ok,
        summary,
        gap: None,
    })
}

pub async fn ensure_window(
    client: &ClientWithMiddleware,
    config: EnsureConfig<'_>,
) -> Result<EnsureOutcome, ResyncError> {
    if config.interval_ms <= 0 {
        return Err(ResyncError::invalid_input("interval_ms must be > 0"));
    }
    if config.symbol.is_empty() || config.timeframe.is_empty() {
        return Err(ResyncError::invalid_input("symbol/timeframe required"));
    }
    if config.rest_base_url.trim().is_empty() {
        return Err(ResyncError::invalid_input("rest_base_url required"));
    }
    let server_time_ms = fetch_binance_server_time_ms(client, config.rest_base_url).await?;
    let server_latest_closed = latest_closed_open_ts(server_time_ms, config.interval_ms);
    let latest_closed = config.latest_closed_ts_ms.unwrap_or(server_latest_closed);
    if latest_closed <= 0 {
        return Err(ResyncError::invalid_input("latest_closed_ts_ms invalid"));
    }
    if latest_closed > server_latest_closed {
        return Err(ResyncError::invalid_input(format!(
            "latest_closed_ts_ms exceeds Binance server closure latest_closed_ts_ms={latest_closed} server_latest_closed_ts_ms={server_latest_closed}",
        )));
    }
    let mut summary = AuditSummary::empty(config.interval_ms, config.window);
    let mut needs_initial = !config.arrow_path.exists();
    if !needs_initial {
        let tail = read_tail(config.arrow_path, 1)
            .map_err(|err| ResyncError::io(format!("read_tail failed: {err}")))?;
        if tail.is_empty() {
            needs_initial = true;
        }
    }
    if needs_initial {
        let target_bars = config
            .fetch_bars
            .or(config.required_bars)
            .or(config.window)
            .ok_or_else(|| {
                ResyncError::invalid_input("fetch_bars or window required for initial build")
            })?;
        let start_ts = start_ts_for_count(latest_closed, config.interval_ms, target_bars);
        if start_ts > latest_closed {
            return Ok(pause_outcome(
                summary,
                latest_closed,
                ResyncError::invalid_input(format!(
                    "initial build start_ts_ms > latest_closed_ts_ms start_ts_ms={start_ts} latest_closed_ts_ms={latest_closed}",
                )),
            ));
        }
        info!(
            symbol = %config.symbol,
            timeframe = %config.timeframe,
            start_ts_ms = start_ts,
            end_ts_ms = latest_closed,
            interval_ms = config.interval_ms,
            target_bars,
            "REST initial build fetch"
        );
        let bars = match fetch_binance_klines_range(
            client,
            config.rest_base_url,
            config.symbol,
            config.timeframe,
            start_ts,
            latest_closed,
            config.interval_ms,
            server_time_ms,
        )
        .await
        {
            Ok(bars) => bars,
            Err(err) => {
                return Ok(pause_outcome(summary, latest_closed, err));
            }
        };
        if bars.is_empty() {
            return Ok(pause_outcome(
                summary,
                latest_closed,
                ResyncError::validation("no bars returned for initial build"),
            ));
        }
        validate_continuity(&bars, config.interval_ms)?;
        merge_fetched_bars(config.arrow_path, bars, config.interval_ms, true).await?;
    }

    let mut audit = match audit_continuity(config.arrow_path, config.interval_ms, config.window) {
        Ok(report) => report,
        Err(err) => return Ok(pause_outcome(summary, latest_closed, err)),
    };
    summary = audit.summary.clone();
    let mut repairs = 0;
    while matches!(audit.status, AuditStatus::Gap) && repairs < config.max_gap_repairs {
        let gap = audit
            .gap
            .clone()
            .ok_or_else(|| ResyncError::internal("gap status without gap info"))?;
        info!(
            symbol = %config.symbol,
            timeframe = %config.timeframe,
            gap_start_ts_ms = gap.gap_start_ts_ms,
            gap_end_ts_ms = gap.gap_end_ts_ms,
            interval_ms = config.interval_ms,
            "REST gap repair fetch"
        );
        match repair_gaps_at_server_time(
            client,
            config.arrow_path,
            config.symbol,
            config.timeframe,
            config.rest_base_url,
            config.interval_ms,
            gap.gap_start_ts_ms,
            gap.gap_end_ts_ms,
            server_time_ms,
        )
        .await
        {
            Ok(_) => {}
            Err(err) => return Ok(pause_outcome(summary, latest_closed, err)),
        }
        repairs += 1;
        audit = match audit_continuity(config.arrow_path, config.interval_ms, config.window) {
            Ok(report) => report,
            Err(err) => return Ok(pause_outcome(summary, latest_closed, err)),
        };
        summary = audit.summary.clone();
    }
    if matches!(audit.status, AuditStatus::Gap) {
        return Ok(pause_outcome(
            summary,
            latest_closed,
            ResyncError::validation("gap remains after repair attempts"),
        ));
    }
    if let Some(last_ts) = audit.summary.last_ts {
        if last_ts < latest_closed {
            let start_ts = last_ts
                .checked_add(config.interval_ms)
                .ok_or_else(|| ResyncError::validation("REST top-up start timestamp overflow"))?;
            let end_ts = latest_closed;
            // If the store is already beyond the closed target, treat as no-op; do not issue REST.
            if start_ts > end_ts {
                return Ok(EnsureOutcome {
                    status: EnsureStatus::Ready,
                    summary,
                    latest_closed_ts_ms: latest_closed,
                });
            }
            let expected_count = expected_bar_count(start_ts, end_ts, config.interval_ms);
            let started = Instant::now();
            debug!(
                symbol = %config.symbol,
                timeframe = %config.timeframe,
                start_ts_ms = start_ts,
                end_ts_ms = end_ts,
                interval_ms = config.interval_ms,
                expected_count,
                "REST top-up fetch"
            );
            let bars = match fetch_binance_klines_range(
                client,
                config.rest_base_url,
                config.symbol,
                config.timeframe,
                start_ts,
                end_ts,
                config.interval_ms,
                server_time_ms,
            )
            .await
            {
                Ok(bars) => bars,
                Err(err) => return Ok(pause_outcome(summary, latest_closed, err)),
            };
            let received_count = bars.len();
            if expected_count != received_count {
                return Ok(pause_outcome(
                    summary,
                    latest_closed,
                    ResyncError::validation(format!(
                        "REST top-up count mismatch expected={expected_count} received={received_count}",
                    )),
                ));
            }
            validate_range_bars(
                &bars,
                start_ts,
                latest_closed,
                config.interval_ms,
                expected_count,
            )?;
            merge_fetched_bars(config.arrow_path, bars, config.interval_ms, false).await?;
            debug!(
                symbol = %config.symbol,
                timeframe = %config.timeframe,
                start_ts_ms = start_ts,
                end_ts_ms = latest_closed,
                expected_count,
                received_count,
                duration_ms = started.elapsed().as_millis() as u64,
                "REST top-up complete"
            );
            audit = match audit_continuity(config.arrow_path, config.interval_ms, config.window) {
                Ok(report) => report,
                Err(err) => return Ok(pause_outcome(summary, latest_closed, err)),
            };
            summary = audit.summary.clone();
        }
    }

    let mut status = EnsureStatus::Ready;
    if let Some(required) = config.required_bars {
        let available = if config.window.is_some() {
            read_all(config.arrow_path)
                .map_err(|err| ResyncError::io(format!("read_all failed: {err}")))?
                .len()
        } else {
            summary.count
        };
        if available < required {
            status = EnsureStatus::InsufficientHistory {
                available,
                required,
            };
        }
    }
    Ok(EnsureOutcome {
        status,
        summary,
        latest_closed_ts_ms: latest_closed,
    })
}

async fn merge_fetched_bars(
    path: &Path,
    bars: Vec<OhlcBar>,
    interval_ms: i64,
    initialize: bool,
) -> Result<(), ResyncError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut transaction = OhlcFileTransaction::open(&path)
            .map_err(|err| ResyncError::io(format!("open OHLC transaction failed: {err}")))?;
        let mut existing = if initialize && !path.exists() {
            Vec::new()
        } else {
            transaction
                .read_all()
                .map_err(|err| ResyncError::io(format!("read_all failed: {err}")))?
        };
        // Stable sorting retains current rows when the fetched range overlaps.
        existing.extend(bars);
        existing.sort_by_key(|bar| bar.ts_ms);
        existing.dedup_by_key(|bar| bar.ts_ms);
        validate_continuity(&existing, interval_ms)?;
        transaction
            .write(&existing)
            .map_err(|err| ResyncError::io(format!("write_ohlc_bars failed: {err}")))?;
        Ok(())
    })
    .await
    .map_err(|err| ResyncError::internal(format!("OHLC transaction task failed: {err}")))?
}

fn pause_outcome(summary: AuditSummary, latest_closed: i64, err: ResyncError) -> EnsureOutcome {
    let backoff_ms = err.retry_after_ms.unwrap_or(RESYNC_RETRY_MAX_MS);
    let next_retry_at_ms = now_ms().saturating_add(backoff_ms as i64);
    EnsureOutcome {
        status: EnsureStatus::PausedForResync {
            reason: err.message,
            next_retry_at_ms,
            error_kind: err.kind,
        },
        summary,
        latest_closed_ts_ms: latest_closed,
    }
}

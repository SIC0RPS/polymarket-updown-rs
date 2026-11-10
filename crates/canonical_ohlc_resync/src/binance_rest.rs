use std::cmp::Ordering;
use std::path::Path;
use std::time::{Duration, Instant};

use canonical_ohlc_arrow::{OhlcBar, OhlcFileTransaction};
use reqwest::{Client as HttpClient, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    default_on_request_failure, policies::ExponentialBackoff, RetryTransientMiddleware, Retryable,
    RetryableStrategy,
};
use retry_policies::Jitter;
use serde_json::Value;
use tracing::debug;

use super::{expected_bar_count, RepairOutcome, ResyncError};

const BINANCE_KLINES_LIMIT: usize = 1_000;
const RESYNC_RETRY_MIN_MS: u64 = 500;
pub(super) const RESYNC_RETRY_MAX_MS: u64 = 10_000;
const RESYNC_RETRY_MAX_ATTEMPTS: u32 = 5;

#[derive(Clone, Copy, Debug)]
struct RestResyncRetryStrategy;

impl RetryableStrategy for RestResyncRetryStrategy {
    fn handle(
        &self,
        res: &Result<reqwest_middleware::reqwest::Response, reqwest_middleware::Error>,
    ) -> Option<Retryable> {
        match res {
            Ok(response) => {
                let status = response.status();
                if status == StatusCode::TOO_MANY_REQUESTS
                    || status == StatusCode::REQUEST_TIMEOUT
                    || status.is_server_error()
                {
                    Some(Retryable::Transient)
                } else if status.is_client_error() {
                    Some(Retryable::Fatal)
                } else {
                    None
                }
            }
            Err(error) => default_on_request_failure(error),
        }
    }
}

pub fn build_resync_http_client() -> ClientWithMiddleware {
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(
            Duration::from_millis(RESYNC_RETRY_MIN_MS),
            Duration::from_millis(RESYNC_RETRY_MAX_MS),
        )
        .jitter(Jitter::Full)
        .build_with_max_retries(RESYNC_RETRY_MAX_ATTEMPTS);
    ClientBuilder::new(HttpClient::new())
        .with(RetryTransientMiddleware::new_with_policy_and_strategy(
            retry_policy,
            RestResyncRetryStrategy,
        ))
        .build()
}

pub type ResyncClient = ClientWithMiddleware;

pub async fn repair_gaps(
    client: &ClientWithMiddleware,
    arrow_path: &Path,
    symbol: &str,
    timeframe: &str,
    rest_base_url: &str,
    interval_ms: i64,
    gap_start_ts_ms: i64,
    gap_end_ts_ms: i64,
) -> Result<RepairOutcome, ResyncError> {
    let server_time_ms = fetch_binance_server_time_ms(client, rest_base_url).await?;
    repair_gaps_at_server_time(
        client,
        arrow_path,
        symbol,
        timeframe,
        rest_base_url,
        interval_ms,
        gap_start_ts_ms,
        gap_end_ts_ms,
        server_time_ms,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical repair operation identity"
)]
pub(super) async fn repair_gaps_at_server_time(
    client: &ClientWithMiddleware,
    arrow_path: &Path,
    symbol: &str,
    timeframe: &str,
    rest_base_url: &str,
    interval_ms: i64,
    gap_start_ts_ms: i64,
    gap_end_ts_ms: i64,
    server_time_ms: i64,
) -> Result<RepairOutcome, ResyncError> {
    if interval_ms <= 0 {
        return Err(ResyncError::invalid_input("interval_ms must be > 0"));
    }
    if gap_start_ts_ms > gap_end_ts_ms {
        return Err(ResyncError::invalid_input(
            "gap_start_ts_ms must be <= gap_end_ts_ms",
        ));
    }
    if !arrow_path.exists() {
        return Err(ResyncError::io(format!(
            "canonical arrow missing for {symbol}:{timeframe}"
        )));
    }
    let expected_count = expected_bar_count(gap_start_ts_ms, gap_end_ts_ms, interval_ms);
    let started = Instant::now();
    let bars = fetch_binance_klines_range(
        client,
        rest_base_url,
        symbol,
        timeframe,
        gap_start_ts_ms,
        gap_end_ts_ms,
        interval_ms,
        server_time_ms,
    )
    .await?;
    let received_count = bars.len();
    validate_range_bars(
        &bars,
        gap_start_ts_ms,
        gap_end_ts_ms,
        interval_ms,
        expected_count,
    )?;
    let path = arrow_path.to_path_buf();
    let new_total_count = tokio::task::spawn_blocking(move || -> Result<usize, ResyncError> {
        let mut transaction = OhlcFileTransaction::open(&path)
            .map_err(|err| ResyncError::io(format!("open OHLC transaction failed: {err}")))?;
        let mut existing = transaction
            .read_all()
            .map_err(|err| ResyncError::io(format!("read_all failed: {err}")))?;
        existing.extend(bars);
        existing.sort_by_key(|bar| bar.ts_ms);
        validate_continuity(&existing, interval_ms)?;
        transaction
            .write(&existing)
            .map_err(|err| ResyncError::io(format!("write_ohlc_bars failed: {err}")))?;
        Ok(existing.len())
    })
    .await
    .map_err(|err| ResyncError::internal(format!("OHLC repair task failed: {err}")))??;
    Ok(RepairOutcome {
        start_ts_ms: gap_start_ts_ms,
        end_ts_ms: gap_end_ts_ms,
        expected_count,
        received_count,
        duration_ms: started.elapsed().as_millis() as u64,
        new_total_count,
    })
}

pub async fn fetch_binance_server_time_ms(
    client: &ClientWithMiddleware,
    rest_base_url: &str,
) -> Result<i64, ResyncError> {
    let base = rest_base_url.trim_end_matches('/');
    let response = client
        .get(format!("{base}{}", server_time_path(rest_base_url)))
        .send()
        .await
        .map_err(|err| {
            ResyncError::http(
                format!("provider REST server-time request failed: {err}"),
                None,
            )
        })?;
    let status = response.status();
    if !status.is_success() {
        let retry_after = retry_after_ms(response.headers());
        let body = response.text().await.unwrap_or_default();
        return Err(ResyncError::http(
            format!("provider REST server-time failed (status={status}) {body}"),
            retry_after,
        ));
    }
    let body: Value = response.json().await.map_err(|err| {
        ResyncError::parse(format!(
            "provider REST server-time JSON decode failed: {err}"
        ))
    })?;
    let server_time_ms = body
        .get("serverTime")
        .and_then(parse_i64_value)
        .ok_or_else(|| ResyncError::parse("provider REST serverTime missing"))?;
    if server_time_ms <= 0 {
        return Err(ResyncError::validation(
            "provider REST serverTime must be positive",
        ));
    }
    Ok(server_time_ms)
}

fn parse_f64(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::String(s)) => s.parse::<f64>().unwrap_or(f64::NAN),
        Some(Value::Number(num)) => num.as_f64().unwrap_or(f64::NAN),
        _ => f64::NAN,
    }
}

fn parse_i64_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num
            .as_i64()
            .or_else(|| num.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
}

fn all_finite(values: &[f64]) -> bool {
    values.iter().all(|v| v.is_finite())
}

fn parse_binance_kline_row(
    row: &Value,
    interval_ms: i64,
    server_time_ms: i64,
) -> Result<(OhlcBar, i64), ResyncError> {
    let fields = row
        .as_array()
        .ok_or_else(|| ResyncError::parse("Binance REST kline row is not an array"))?;
    if fields.len() < 7 {
        return Err(ResyncError::parse(
            "Binance REST kline row has insufficient fields",
        ));
    }
    let open_time =
        parse_i64_value(&fields[0]).ok_or_else(|| ResyncError::parse("missing kline open time"))?;
    if open_time > server_time_ms {
        return Err(ResyncError::validation("REST kline not yet closed"));
    }
    let close_time = parse_i64_value(&fields[6])
        .ok_or_else(|| ResyncError::parse("missing kline close time"))?;
    let open = parse_f64(fields.get(1));
    let high = parse_f64(fields.get(2));
    let low = parse_f64(fields.get(3));
    let close = parse_f64(fields.get(4));
    let volume = parse_f64(fields.get(5));
    if !all_finite(&[open, high, low, close, volume]) {
        return Err(ResyncError::validation("non-finite OHLCV in REST kline"));
    }
    if interval_ms > 0 && open_time % interval_ms != 0 {
        return Err(ResyncError::validation(
            "REST kline open_time not aligned to interval",
        ));
    }
    if close_time < open_time {
        return Err(ResyncError::validation(
            "REST kline close_time precedes open_time",
        ));
    }
    if close_time > server_time_ms {
        return Err(ResyncError::validation("REST kline not yet closed"));
    }
    Ok((
        OhlcBar {
            ts_ms: open_time,
            open,
            high,
            low,
            close,
            volume,
        },
        close_time,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical Binance range identity"
)]
pub async fn fetch_binance_klines_range(
    client: &ClientWithMiddleware,
    rest_base_url: &str,
    symbol: &str,
    interval: &str,
    start_ts_ms: i64,
    end_ts_ms: i64,
    interval_ms: i64,
    server_time_ms: i64,
) -> Result<Vec<OhlcBar>, ResyncError> {
    if start_ts_ms > end_ts_ms {
        return Err(ResyncError::invalid_input(format!(
            "start_ts_ms > end_ts_ms start_ts_ms={start_ts_ms} end_ts_ms={end_ts_ms}",
        )));
    }
    let base = rest_base_url.trim_end_matches('/');
    let klines_path = klines_path(rest_base_url);
    let mut out = Vec::new();
    let mut next_start = start_ts_ms;
    while next_start <= end_ts_ms {
        debug!(
            symbol = %symbol,
            timeframe = %interval,
            start_ts_ms = next_start,
            end_ts_ms,
            "REST klines fetch"
        );
        let response = client
            .get(format!("{base}{klines_path}"))
            .query(&[
                ("symbol", symbol),
                ("interval", interval),
                ("startTime", &next_start.to_string()),
                ("limit", &BINANCE_KLINES_LIMIT.to_string()),
            ])
            .send()
            .await
            .map_err(|err| {
                ResyncError::http(format!("provider REST klines request failed: {err}"), None)
            })?;
        let status = response.status();
        log_weight_headers(symbol, interval, &response);
        if !status.is_success() {
            let retry_after = retry_after_ms(response.headers());
            let body = response.text().await.unwrap_or_default();
            if status == StatusCode::BAD_REQUEST {
                if let Ok(value) = serde_json::from_slice::<Value>(body.as_bytes()) {
                    if value.get("code").and_then(|code| code.as_i64()) == Some(-1023) {
                        return Err(ResyncError::validation(format!(
                            "provider REST klines invalid window (status={status}) {body}"
                        )));
                    }
                }
            }
            return Err(ResyncError::http(
                format!("provider REST klines failed (status={status}) {body}"),
                retry_after,
            ));
        }
        let rows: Vec<Value> = response.json().await.map_err(|err| {
            ResyncError::parse(format!("provider REST klines JSON decode failed: {err}"))
        })?;
        if rows.is_empty() {
            break;
        }
        let mut last_open = None;
        for row in &rows {
            let Some(open_time) = row
                .as_array()
                .and_then(|fields| fields.first())
                .and_then(parse_i64_value)
            else {
                return Err(ResyncError::parse("missing kline open time"));
            };
            if open_time > end_ts_ms {
                return Ok(out);
            }
            let (bar, _) = parse_binance_kline_row(row, interval_ms, server_time_ms)?;
            if bar.ts_ms < start_ts_ms {
                continue;
            }
            last_open = Some(bar.ts_ms);
            out.push(bar);
        }
        let Some(last_open_ts) = last_open else {
            break;
        };
        if rows.len() < BINANCE_KLINES_LIMIT {
            break;
        }
        let next = last_open_ts
            .checked_add(interval_ms)
            .ok_or_else(|| ResyncError::validation("REST kline pagination overflow"))?;
        if next <= next_start {
            return Err(ResyncError::validation(
                "REST kline pagination did not advance",
            ));
        }
        next_start = next;
    }
    Ok(out)
}

fn is_usdm_host(rest_base_url: &str) -> bool {
    rest_base_url.contains("fapi.binance.com")
}

fn server_time_path(rest_base_url: &str) -> &'static str {
    if is_usdm_host(rest_base_url) {
        "/fapi/v1/time"
    } else {
        "/api/v3/time"
    }
}

fn klines_path(rest_base_url: &str) -> &'static str {
    if is_usdm_host(rest_base_url) {
        "/fapi/v1/klines"
    } else {
        "/api/v3/klines"
    }
}

pub fn validate_range_bars(
    bars: &[OhlcBar],
    start_ts_ms: i64,
    end_ts_ms: i64,
    interval_ms: i64,
    expected_count: usize,
) -> Result<(), ResyncError> {
    if bars.len() != expected_count {
        return Err(ResyncError::validation(format!(
            "REST resync count mismatch expected={} received={}",
            expected_count,
            bars.len()
        )));
    }
    if bars.is_empty() {
        return Ok(());
    }
    let mut expected = start_ts_ms;
    for bar in bars {
        if bar.ts_ms != expected {
            return Err(ResyncError::validation(format!(
                "REST resync timestamp mismatch expected={} received={}",
                expected, bar.ts_ms
            )));
        }
        expected = expected
            .checked_add(interval_ms)
            .ok_or_else(|| ResyncError::validation("REST range timestamp overflow"))?;
    }
    let intervals = i64::try_from(bars.len().saturating_sub(1))
        .map_err(|_| ResyncError::validation("REST range length exceeds i64"))?;
    let last_expected = interval_ms
        .checked_mul(intervals)
        .and_then(|delta| start_ts_ms.checked_add(delta))
        .ok_or_else(|| ResyncError::validation("REST range end timestamp overflow"))?;
    if last_expected != end_ts_ms {
        return Err(ResyncError::validation(
            "REST resync end timestamp mismatch",
        ));
    }
    Ok(())
}

pub fn validate_continuity(bars: &[OhlcBar], interval_ms: i64) -> Result<(), ResyncError> {
    if bars.is_empty() {
        return Ok(());
    }
    let mut expected = bars[0].ts_ms;
    for bar in bars {
        match bar.ts_ms.cmp(&expected) {
            Ordering::Equal => {}
            Ordering::Greater => {
                return Err(ResyncError::validation(format!(
                    "gap detected while validating continuity (expected={}, got={})",
                    expected, bar.ts_ms
                )));
            }
            Ordering::Less => {
                return Err(ResyncError::validation(format!(
                    "non-monotonic bar sequence (expected={}, got={})",
                    expected, bar.ts_ms
                )));
            }
        }
        expected = expected
            .checked_add(interval_ms)
            .ok_or_else(|| ResyncError::validation("REST continuity timestamp overflow"))?;
    }
    Ok(())
}

fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers.get("retry-after").and_then(|value| {
        value
            .to_str()
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .and_then(|seconds| seconds.checked_mul(1_000))
    })
}

fn log_weight_headers(
    symbol: &str,
    interval: &str,
    response: &reqwest_middleware::reqwest::Response,
) {
    let headers = response.headers();
    let used_weight = headers
        .get("x-mbx-used-weight-1m")
        .and_then(|value| value.to_str().ok());
    let used_weight_1s = headers
        .get("x-mbx-used-weight-1s")
        .and_then(|value| value.to_str().ok());
    let order_count = headers
        .get("x-mbx-order-count-1m")
        .and_then(|value| value.to_str().ok());
    if used_weight.is_some() || used_weight_1s.is_some() || order_count.is_some() {
        debug!(
            symbol = %symbol,
            timeframe = %interval,
            used_weight_1m = used_weight.unwrap_or(""),
            used_weight_1s = used_weight_1s.unwrap_or(""),
            order_count_1m = order_count.unwrap_or(""),
            "REST weight headers"
        );
    } else {
        debug!(
            symbol = %symbol,
            timeframe = %interval,
            "REST weight headers missing"
        );
    }
}

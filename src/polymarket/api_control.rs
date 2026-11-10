use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use polymarket_client_sdk::error::{Error as PolymarketClientError, Status as PolymarketStatus};
use reqwest::header::HeaderMap;

const SDK_READ_RETRY_ATTEMPTS: usize = 3;
const SDK_READ_RETRY_BASE_BACKOFF_MS: u64 = 500;
const SDK_READ_RETRY_MAX_BACKOFF_MS: u64 = 5_000;

static JITTER_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) fn capped_full_jitter_backoff_ms(
    attempt: usize,
    base_ms: u64,
    max_backoff_ms: u64,
) -> u64 {
    let shift = attempt.saturating_sub(1).min(16) as u32;
    let cap = base_ms.saturating_mul(1_u64 << shift).min(max_backoff_ms);
    full_jitter_ms(cap).max(1)
}

fn full_jitter_ms(cap_ms: u64) -> u64 {
    if cap_ms == 0 {
        return 0;
    }
    let counter = JITTER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(counter);
    let mixed = nanos
        ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (&counter as *const u64 as usize as u64);
    mixed % cap_ms.saturating_add(1)
}

pub(crate) fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    raw.parse::<u64>()
        .ok()
        .map(|seconds| seconds.saturating_mul(1_000))
}

pub(crate) fn is_retryable_sdk_read_error(err: &PolymarketClientError) -> bool {
    if err
        .downcast_ref::<PolymarketStatus>()
        .map(|status| status.status_code.as_u16())
        .is_some_and(|status| matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504))
    {
        return true;
    }
    err.downcast_ref::<reqwest::Error>()
        .is_some_and(|err| err.is_timeout() || err.is_connect())
}

pub(crate) async fn retry_sdk_read<T, F, Fut>(mut request: F) -> polymarket_client_sdk::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = polymarket_client_sdk::Result<T>>,
{
    for attempt in 1..=SDK_READ_RETRY_ATTEMPTS {
        match request().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < SDK_READ_RETRY_ATTEMPTS && is_retryable_sdk_read_error(&err) => {
                let backoff_ms = capped_full_jitter_backoff_ms(
                    attempt,
                    SDK_READ_RETRY_BASE_BACKOFF_MS,
                    SDK_READ_RETRY_MAX_BACKOFF_MS,
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop returns on success or final error")
}

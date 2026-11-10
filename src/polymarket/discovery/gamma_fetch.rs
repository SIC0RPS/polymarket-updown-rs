use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures_util::future::join_all;

use super::{discovery_query_underlyings, DiscoveryGammaMarket};
use crate::config::PolymarketConfig;
use crate::polymarket::api_control::retry_after_ms;

const DISCOVERY_LOOKBACK_WINDOWS: i64 = 1;
const DISCOVERY_LOOKAHEAD_WINDOWS: i64 = 4;
const GAMMA_MARKETS_SLUG_RETRY_ATTEMPTS: usize = 1;
const GAMMA_MARKETS_SLUG_ATTEMPT_TIMEOUT_MS: u64 = 1_000;
const GAMMA_MARKETS_RETRY_BASE_BACKOFF_MS: u64 = 250;
const GAMMA_MARKETS_RETRY_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Debug)]
struct GammaStatusError {
    status: reqwest::StatusCode,
    body: String,
    retry_after_ms: Option<u64>,
}

impl fmt::Display for GammaStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "gamma markets request returned status {}: {}",
            self.status, self.body
        )
    }
}

impl Error for GammaStatusError {}

pub(super) async fn fetch_open_markets(
    http: &reqwest::Client,
    cfg: &PolymarketConfig,
    enabled_assets: &[String],
) -> Result<Vec<DiscoveryGammaMarket>> {
    let mut out = Vec::new();
    let mut seen_ids = HashSet::new();
    let window_secs = (cfg.discovery.window_ms / 1_000).max(1) as i64;
    let now_secs = Utc::now().timestamp();
    let current_window_start = now_secs - now_secs.rem_euclid(window_secs);
    let underlyings = discovery_query_underlyings(enabled_assets, cfg);

    let mut slugs =
        Vec::with_capacity(underlyings.len().saturating_mul(
            (DISCOVERY_LOOKBACK_WINDOWS + DISCOVERY_LOOKAHEAD_WINDOWS + 1) as usize,
        ));
    for underlying in underlyings {
        for window_offset in -DISCOVERY_LOOKBACK_WINDOWS..=DISCOVERY_LOOKAHEAD_WINDOWS {
            let window_start = current_window_start + window_offset.saturating_mul(window_secs);
            if window_start <= 0 {
                continue;
            }
            slugs.push(format!("{underlying}-updown-5m-{window_start}"));
        }
    }
    let requests = slugs.into_iter().map(|slug| async move {
        let result = fetch_open_markets_slug(http, cfg, &slug).await;
        (slug, result)
    });
    let mut succeeded = false;
    let mut last_error = None;
    for (slug, result) in join_all(requests).await {
        match result {
            Ok(markets) => {
                succeeded = true;
                for market in markets {
                    if seen_ids.insert(market.id.clone()) {
                        out.push(market);
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "polymarket.discovery",
                    slug,
                    error = %err,
                    "gamma_markets_slug_skipped"
                );
                last_error = Some(err);
            }
        }
    }

    if !succeeded {
        if let Some(err) = last_error {
            return Err(err.context("all gamma markets slug requests failed"));
        }
    }
    Ok(out)
}

async fn fetch_open_markets_slug(
    http: &reqwest::Client,
    cfg: &PolymarketConfig,
    slug: &str,
) -> Result<Vec<DiscoveryGammaMarket>> {
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=GAMMA_MARKETS_SLUG_RETRY_ATTEMPTS {
        match fetch_open_markets_slug_once(http, cfg, slug).await {
            Ok(markets) => return Ok(markets),
            Err(err)
                if attempt < GAMMA_MARKETS_SLUG_RETRY_ATTEMPTS
                    && is_retryable_gamma_error(&err) =>
            {
                let backoff_ms = gamma_retry_after_ms(&err).unwrap_or_else(|| {
                    gamma_retry_backoff_ms(attempt, Utc::now().timestamp_millis())
                });
                tracing::warn!(
                    target: "polymarket.discovery",
                    slug,
                    attempt,
                    retry_in_ms = backoff_ms,
                    error = %err,
                    "gamma_markets_slug_retry"
                );
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("gamma markets slug request failed without error")))
}

async fn fetch_open_markets_slug_once(
    http: &reqwest::Client,
    cfg: &PolymarketConfig,
    slug: &str,
) -> Result<Vec<DiscoveryGammaMarket>> {
    let url = format!("{}/markets", cfg.gamma_http_url.trim_end_matches('/'));
    let request = http.get(url).query(&[
        ("active", "true"),
        ("closed", "false"),
        ("slug", slug),
        ("limit", "1"),
    ]);

    let response = tokio::time::timeout(
        Duration::from_millis(GAMMA_MARKETS_SLUG_ATTEMPT_TIMEOUT_MS),
        request.send(),
    )
    .await
    .context("gamma markets slug request timed out")?
    .context("gamma markets slug request failed")?;
    let status = response.status();
    if !status.is_success() {
        let retry_after = retry_after_ms(response.headers());
        let mut body = response.text().await.unwrap_or_default();
        body.truncate(512);
        return Err(GammaStatusError {
            status,
            body,
            retry_after_ms: retry_after,
        }
        .into());
    }

    tokio::time::timeout(
        Duration::from_millis(GAMMA_MARKETS_SLUG_ATTEMPT_TIMEOUT_MS),
        response.json::<Vec<DiscoveryGammaMarket>>(),
    )
    .await
    .context("gamma markets slug decode timed out")?
    .context("gamma markets slug response decode failed")
}

fn is_retryable_gamma_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<GammaStatusError>()
            .is_some_and(|err| matches!(err.status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504))
            || cause
                .downcast_ref::<tokio::time::error::Elapsed>()
                .is_some()
            || cause
                .downcast_ref::<reqwest::Error>()
                .is_some_and(|err| err.is_timeout() || err.is_connect())
    })
}

fn gamma_retry_after_ms(err: &anyhow::Error) -> Option<u64> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<GammaStatusError>()?.retry_after_ms)
}

fn gamma_retry_backoff_ms(attempt: usize, now_ms: i64) -> u64 {
    let shift = attempt.saturating_sub(1).min(4) as u32;
    let base = GAMMA_MARKETS_RETRY_BASE_BACKOFF_MS
        .saturating_mul(1_u64 << shift)
        .min(GAMMA_MARKETS_RETRY_MAX_BACKOFF_MS);
    let jitter_window = (base / 2).max(50);
    let jitter_seed = (now_ms as u64)
        .wrapping_mul(1_103_515_245)
        .wrapping_add((attempt as u64).wrapping_mul(12_345));
    base.saturating_add(jitter_seed % jitter_window)
}

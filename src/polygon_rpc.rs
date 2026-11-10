use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::polymarket::api_control::{capped_full_jitter_backoff_ms, retry_after_ms};

const POLYGON_RPC_RETRY_ATTEMPTS: usize = 3;
const POLYGON_RPC_RETRY_BASE_BACKOFF_MS: u64 = 250;
const POLYGON_RPC_RETRY_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedBlockIdentity {
    pub number: u64,
    pub hash: String,
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorPayload {
    code: i64,
}

fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn is_retryable_request_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
}

pub fn preferred_url(configured_rpc_url: &str) -> String {
    configured_rpc_url.trim().to_string()
}

fn candidate_urls(configured_rpc_url: &str) -> Vec<String> {
    let mut candidates = Vec::with_capacity(2);
    if let Ok(key) = std::env::var("ALCHEMY_API_KEY") {
        let key = key.trim();
        if !key.is_empty() {
            candidates.push(format!("https://polygon-mainnet.g.alchemy.com/v2/{key}"));
        }
    }

    let configured_rpc_url = configured_rpc_url.trim();
    if !configured_rpc_url.is_empty()
        && !candidates
            .iter()
            .any(|candidate| candidate == configured_rpc_url)
    {
        candidates.push(configured_rpc_url.to_string());
    }
    candidates
}

pub fn endpoint_label(rpc_url: &str) -> &'static str {
    if rpc_url.contains("polygon-mainnet.g.alchemy.com") {
        "alchemy_polygon_mainnet"
    } else if rpc_url.contains("polygon-bor-rpc.publicnode.com") {
        "polygon_publicnode"
    } else {
        "configured_polygon_rpc"
    }
}

pub async fn result_value(
    http: &reqwest::Client,
    rpc_url: &str,
    method: &str,
    params: Value,
) -> Result<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });
    let candidates = candidate_urls(rpc_url);
    anyhow::ensure!(
        !candidates.is_empty(),
        "configured_polygon_rpc: no endpoint configured"
    );
    let mut last_error = None;

    for candidate_url in &candidates {
        for attempt in 1..=POLYGON_RPC_RETRY_ATTEMPTS {
            match http.post(candidate_url).json(&body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        let retry_after = retry_after_ms(resp.headers());
                        let err_text = format!(
                            "{}: http {}",
                            endpoint_label(candidate_url),
                            status.as_u16()
                        );
                        if attempt < POLYGON_RPC_RETRY_ATTEMPTS && is_retryable_http_status(status)
                        {
                            let backoff_ms = retry_after.unwrap_or_else(|| {
                                capped_full_jitter_backoff_ms(
                                    attempt,
                                    POLYGON_RPC_RETRY_BASE_BACKOFF_MS,
                                    POLYGON_RPC_RETRY_MAX_BACKOFF_MS,
                                )
                            });
                            tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                            continue;
                        }
                        last_error = Some(err_text);
                        break;
                    }

                    match resp.json::<Value>().await {
                        Ok(json) => {
                            if let Some(error_value) = json.get("error") {
                                let error = serde_json::from_value::<JsonRpcErrorPayload>(
                                    error_value.clone(),
                                )
                                .unwrap_or(JsonRpcErrorPayload { code: 0 });
                                last_error = Some(format!(
                                    "{}: json_rpc_code={}",
                                    endpoint_label(candidate_url),
                                    error.code
                                ));
                                break;
                            }
                            if let Some(result) = json.get("result") {
                                return Ok(result.clone());
                            }
                            last_error = Some(format!(
                                "{}: missing result field",
                                endpoint_label(candidate_url)
                            ));
                            break;
                        }
                        Err(_) => {
                            let err_text =
                                format!("{}: decode failed", endpoint_label(candidate_url));
                            if attempt < POLYGON_RPC_RETRY_ATTEMPTS {
                                let backoff_ms = capped_full_jitter_backoff_ms(
                                    attempt,
                                    POLYGON_RPC_RETRY_BASE_BACKOFF_MS,
                                    POLYGON_RPC_RETRY_MAX_BACKOFF_MS,
                                );
                                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms))
                                    .await;
                                continue;
                            }
                            last_error = Some(err_text);
                            break;
                        }
                    }
                }
                Err(err) => {
                    let err_text = format!("{}: request failed", endpoint_label(candidate_url));
                    if attempt < POLYGON_RPC_RETRY_ATTEMPTS && is_retryable_request_error(&err) {
                        let backoff_ms = capped_full_jitter_backoff_ms(
                            attempt,
                            POLYGON_RPC_RETRY_BASE_BACKOFF_MS,
                            POLYGON_RPC_RETRY_MAX_BACKOFF_MS,
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    last_error = Some(err_text);
                    break;
                }
            }
        }
    }

    anyhow::bail!(
        "{}",
        last_error.unwrap_or_else(|| "configured_polygon_rpc: request failed".to_string())
    )
}

pub async fn eth_call(
    http: &reqwest::Client,
    rpc_url: &str,
    to: &str,
    data: &str,
) -> Result<Option<String>> {
    let result = result_value(
        http,
        rpc_url,
        "eth_call",
        json!([{ "to": to, "data": data }, "latest"]),
    )
    .await
    .with_context(|| {
        format!(
            "Polygon RPC eth_call failed for to={to} via {}",
            endpoint_label(rpc_url)
        )
    })?;
    Ok(result.as_str().map(ToOwned::to_owned))
}

pub async fn finalized_block(
    http: &reqwest::Client,
    rpc_url: &str,
) -> Result<FinalizedBlockIdentity> {
    let value = result_value(
        http,
        rpc_url,
        "eth_getBlockByNumber",
        json!(["finalized", false]),
    )
    .await
    .context("Polygon finalized block fetch failed")?;
    let number_hex = value
        .get("number")
        .and_then(Value::as_str)
        .context("Polygon finalized block has no number")?;
    let hash = value
        .get("hash")
        .and_then(Value::as_str)
        .context("Polygon finalized block has no hash")?;
    let number = u64::from_str_radix(number_hex.strip_prefix("0x").unwrap_or(number_hex), 16)
        .context("Polygon finalized block number is invalid")?;
    anyhow::ensure!(
        number > 0 && !hash.trim().is_empty(),
        "Polygon finalized block identity is empty"
    );
    Ok(FinalizedBlockIdentity {
        number,
        hash: hash.to_string(),
    })
}

pub async fn eth_call_at_block(
    http: &reqwest::Client,
    rpc_url: &str,
    to: &str,
    data: &str,
    block_number: u64,
) -> Result<Option<String>> {
    anyhow::ensure!(block_number > 0, "Polygon eth_call block is zero");
    let result = result_value(
        http,
        rpc_url,
        "eth_call",
        json!([{ "to": to, "data": data }, format!("0x{block_number:x}")]),
    )
    .await
    .with_context(|| {
        format!(
            "Polygon RPC eth_call failed for to={to} block={block_number} via {}",
            endpoint_label(rpc_url)
        )
    })?;
    Ok(result.as_str().map(ToOwned::to_owned))
}

use super::*;
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use hmac::{Hmac, Mac as _};
use polymarket_client_sdk::auth::ExposeSecret as _;
use reqwest::header::{HeaderValue, ACCEPT, CONNECTION, CONTENT_TYPE, USER_AGENT};
use sha2::Sha256;
use std::fmt;
use std::time::{Duration, Instant};

pub const HTTP_READ_RETRY_ATTEMPTS_DEFAULT: usize = 3;
pub const HTTP_READ_RETRY_BASE_BACKOFF_MS_DEFAULT: u64 = 500;
pub const HTTP_READ_RETRY_MAX_BACKOFF_MS_DEFAULT: u64 = 5_000;
pub(crate) const ROUTED_POST_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTED_POST_DIAGNOSTIC_MAX_BYTES: usize = 2_048;
const PIA_SOCKS_ENV_PATH: &str = "config/pia-socks.env";

#[derive(Debug)]
pub(crate) struct RoutedPostOutcomeUnknown {
    detail: String,
}

impl fmt::Display for RoutedPostOutcomeUnknown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for RoutedPostOutcomeUnknown {}

fn routed_post_outcome_unknown(detail: String) -> PolymarketClientError {
    PolymarketClientError::with_source(
        polymarket_client_sdk::error::Kind::Internal,
        RoutedPostOutcomeUnknown { detail },
    )
}

pub(crate) fn is_routed_post_outcome_unknown(err: &PolymarketClientError) -> bool {
    err.downcast_ref::<RoutedPostOutcomeUnknown>().is_some()
}

fn bounded_post_diagnostic(value: &[u8]) -> String {
    let end = value.len().min(ROUTED_POST_DIAGNOSTIC_MAX_BYTES);
    String::from_utf8_lossy(&value[..end]).trim().to_string()
}

fn json_value_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn push_bounded_structure(output: &mut String, fragment: &str) -> bool {
    let remaining = ROUTED_POST_DIAGNOSTIC_MAX_BYTES.saturating_sub(output.len());
    if fragment.len() <= remaining {
        output.push_str(fragment);
        return true;
    }
    let mut end = remaining;
    while !fragment.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    output.push_str(&fragment[..end]);
    false
}

fn write_redacted_json_structure(value: &serde_json::Value, output: &mut String) {
    if output.len() >= ROUTED_POST_DIAGNOSTIC_MAX_BYTES {
        return;
    }
    match value {
        serde_json::Value::Array(entries) => {
            if !push_bounded_structure(output, &format!("array(len={})[", entries.len())) {
                return;
            }
            for (index, entry) in entries.iter().enumerate() {
                if index > 0 && !push_bounded_structure(output, ",") {
                    return;
                }
                if !push_bounded_structure(output, &format!("{index}:")) {
                    return;
                }
                write_redacted_json_structure(entry, output);
                if output.len() >= ROUTED_POST_DIAGNOSTIC_MAX_BYTES {
                    return;
                }
            }
            let _ = push_bounded_structure(output, "]");
        }
        serde_json::Value::Object(fields) => {
            let mut known_fields = fields
                .iter()
                .filter(|(name, _)| SAFE_API_NAMES.contains(&name.as_str()))
                .map(|(name, value)| format!("{name}:{}", json_value_kind(value)))
                .collect::<Vec<_>>();
            known_fields.sort_unstable();
            let unknown_key_count = fields.len().saturating_sub(known_fields.len());
            let _ = push_bounded_structure(output, "object{");
            for (index, field) in known_fields.iter().enumerate() {
                if index > 0 && !push_bounded_structure(output, ",") {
                    return;
                }
                if !push_bounded_structure(output, field) {
                    return;
                }
            }
            if unknown_key_count > 0 {
                if !known_fields.is_empty() && !push_bounded_structure(output, ",") {
                    return;
                }
                if !push_bounded_structure(output, &format!("<redacted-keys>:{unknown_key_count}"))
                {
                    return;
                }
            }
            let _ = push_bounded_structure(output, "}");
        }
        value => {
            let _ = push_bounded_structure(output, json_value_kind(value));
        }
    }
}

fn redacted_response_evidence(value: &serde_json::Value) -> String {
    let mut evidence = String::with_capacity(ROUTED_POST_DIAGNOSTIC_MAX_BYTES);
    write_redacted_json_structure(value, &mut evidence);
    evidence
}

const SAFE_API_NAMES: &[&str] = &[
    "errorMsg",
    "makingAmount",
    "takingAmount",
    "orderID",
    "status",
    "success",
    "transactionsHashes",
    "tradeIDs",
];

fn redacted_serde_detail(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(value.len());
    let mut index = 0usize;
    while index < chars.len() {
        let delimiter = chars[index];
        if delimiter != '\'' && delimiter != '"' && delimiter != '`' {
            output.push(delimiter);
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        let mut escaped = false;
        while end < chars.len() {
            if escaped {
                escaped = false;
            } else if chars[end] == '\\' {
                escaped = true;
            } else if chars[end] == delimiter {
                break;
            }
            end = end.saturating_add(1);
        }
        output.push(delimiter);
        output.push_str("<redacted>");
        if end == chars.len() {
            break;
        }
        output.push(delimiter);
        index = end + 1;
    }
    if let Some(expected_start) = output.find(", expected") {
        for marker in ["boolean ", "integer ", "floating point "] {
            if let Some(value_start) = output[..expected_start].rfind(marker) {
                let value_start = value_start + marker.len();
                output.replace_range(value_start..expected_start, "<redacted>");
                break;
            }
        }
    }
    output
}

fn templated_safe_field(message: &str, kind: &str) -> &'static str {
    let prefix = match kind {
        "missing_field" => "missing field ",
        "unknown_field" => "unknown field ",
        _ => return "-",
    };
    let Some(remainder) = message.strip_prefix(prefix) else {
        return "-";
    };
    let Some(delimiter) = remainder.chars().next() else {
        return "-";
    };
    if delimiter != '\'' && delimiter != '"' && delimiter != '`' {
        return "-";
    }
    let remainder = &remainder[delimiter.len_utf8()..];
    let Some(end) = remainder.find(delimiter) else {
        return "-";
    };
    let field = &remainder[..end];
    SAFE_API_NAMES
        .iter()
        .find(|name| **name == field)
        .map_or("-", |name| *name)
}

fn serde_interpretation_error(scope: &str, error: &serde_json::Error) -> String {
    let message = error.to_string();
    let kind = if message.starts_with("missing field") {
        "missing_field"
    } else if message.starts_with("unknown field") {
        "unknown_field"
    } else if message.starts_with("invalid type") {
        "invalid_type"
    } else if message.starts_with("invalid value") {
        "invalid_value"
    } else if message.starts_with("unknown variant") {
        "unknown_variant"
    } else if message.starts_with("trailing characters") {
        "trailing_characters"
    } else {
        match error.classify() {
            serde_json::error::Category::Io => "io",
            serde_json::error::Category::Syntax => "syntax",
            serde_json::error::Category::Data => "data",
            serde_json::error::Category::Eof => "eof",
        }
    };
    let field = templated_safe_field(&message, kind);
    format!(
        "{scope}: kind={kind} field={field} line={} column={} detail={}",
        error.line(),
        error.column(),
        redacted_serde_detail(&message)
    )
}

#[derive(Debug, Clone)]
pub(crate) struct RoutedPostTiming {
    pub(crate) server_time_request_started_at: Option<Instant>,
    pub(crate) server_time_request_completed_at: Option<Instant>,
    pub(crate) invocation_instant: Instant,
    pub(crate) terminal_instant: Instant,
    pub(crate) invocation_elapsed_us: u64,
    pub(crate) response_headers_ts_ms: Option<u64>,
    pub(crate) response_headers_elapsed_us: Option<u64>,
    pub(crate) response_body_elapsed_us: Option<u64>,
    pub(crate) http_status: Option<u16>,
    pub(crate) attempt_result: &'static str,
    pub(crate) interpretation_error: Option<String>,
    pub(crate) response_evidence: Option<String>,
}

#[derive(Debug)]
pub(crate) enum RoutedPostEntry {
    Decoded(PostOrderResponse),
    Undecodable { error: String },
}

pub(crate) struct RoutedPostAttempt {
    pub(crate) result: polymarket_client_sdk::Result<Vec<RoutedPostEntry>>,
    pub(crate) timing: RoutedPostTiming,
}

fn elapsed_us(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn routed_post_timing(
    started: Instant,
    server_time_request: Option<(Instant, Instant)>,
    response_headers_ts_ms: Option<u64>,
    response_headers_elapsed_us: Option<u64>,
    response_body_elapsed_us: Option<u64>,
    http_status: Option<u16>,
    attempt_result: &'static str,
    interpretation_error: Option<String>,
    response_evidence: Option<String>,
) -> RoutedPostTiming {
    let terminal_instant = Instant::now();
    RoutedPostTiming {
        server_time_request_started_at: server_time_request.map(|(started, _)| started),
        server_time_request_completed_at: server_time_request.map(|(_, completed)| completed),
        invocation_instant: started,
        terminal_instant,
        invocation_elapsed_us: elapsed_us(terminal_instant.duration_since(started)),
        response_headers_ts_ms,
        response_headers_elapsed_us,
        response_body_elapsed_us,
        http_status,
        attempt_result,
        interpretation_error,
        response_evidence,
    }
}

fn reqwest_error_class(error: &reqwest::Error) -> String {
    format!(
        "timeout={} connect={} request={} body={} decode={}",
        error.is_timeout(),
        error.is_connect(),
        error.is_request(),
        error.is_body(),
        error.is_decode()
    )
}

pub(crate) fn build_order_post_client() -> Result<reqwest::Client> {
    let entries = dotenvy::from_path_iter(PIA_SOCKS_ENV_PATH)
        .map_err(|_| anyhow::anyhow!("failed to read required PIA SOCKS credential file"))?;
    let mut proxy_url = None;
    let mut username = None;
    let mut password = None;
    for entry in entries {
        let (key, value) = entry
            .map_err(|_| anyhow::anyhow!("failed to parse required PIA SOCKS credential file"))?;
        match key.as_str() {
            "PIA_SOCKS_PROXY" => proxy_url = Some(value),
            "PIA_SOCKS_USER" => username = Some(value),
            "PIA_SOCKS_PASS" => password = Some(value),
            _ => {}
        }
    }
    let proxy_url = proxy_url
        .filter(|value| !value.trim().is_empty())
        .context("PIA_SOCKS_PROXY is missing or empty")?;
    let username = username
        .filter(|value| !value.is_empty())
        .context("PIA_SOCKS_USER is missing or empty")?;
    let password = password
        .filter(|value| !value.is_empty())
        .context("PIA_SOCKS_PASS is missing or empty")?;
    let proxy_url = reqwest::Url::parse(proxy_url.trim())
        .map_err(|_| anyhow::anyhow!("PIA_SOCKS_PROXY is not a valid URL"))?;
    ensure!(
        matches!(proxy_url.scheme(), "socks5" | "socks5h"),
        "PIA_SOCKS_PROXY must use SOCKS5"
    );
    ensure!(
        proxy_url.username().is_empty() && proxy_url.password().is_none(),
        "PIA SOCKS credentials must use the dedicated credential variables"
    );
    let proxy = reqwest::Proxy::all(proxy_url)
        .map_err(|_| anyhow::anyhow!("failed to configure PIA SOCKS proxy"))?
        .basic_auth(&username, &password);
    reqwest::Client::builder()
        .proxy(proxy)
        .timeout(ROUTED_POST_TIMEOUT)
        .build()
        .map_err(|_| anyhow::anyhow!("failed to build PIA SOCKS order POST client"))
}

#[derive(Debug, Clone, Copy)]
pub struct ReadRetryState {
    pub retries: usize,
    pub backoff_ms: u64,
}

impl Default for ReadRetryState {
    fn default() -> Self {
        Self {
            retries: 0,
            backoff_ms: HTTP_READ_RETRY_BASE_BACKOFF_MS_DEFAULT,
        }
    }
}

pub fn http_status_code(err: &PolymarketClientError) -> Option<u16> {
    err.downcast_ref::<PolymarketStatus>()
        .map(|status| status.status_code.as_u16())
}

pub fn is_http_status(err: &PolymarketClientError, expected_status: u16) -> bool {
    http_status_code(err).is_some_and(|status| status == expected_status)
}

pub fn is_http_425(err: &PolymarketClientError) -> bool {
    is_http_status(err, 425)
}

pub fn is_http_429(err: &PolymarketClientError) -> bool {
    is_http_status(err, 429)
}

impl PolymarketExecutionLive {
    pub(crate) async fn post_orders_routed(
        &self,
        orders: Vec<SignedOrder>,
    ) -> polymarket_client_sdk::Result<RoutedPostAttempt> {
        let body = serde_json::to_vec(&orders)?;
        let endpoint = dispatch_client!(&self.client, |client| {
            reqwest::Url::parse(&format!("{}orders", client.host()))
        })?;
        let mut request = reqwest::Request::new(reqwest::Method::POST, endpoint);
        *request.body_mut() = Some(body.clone().into());
        let headers = request.headers_mut();
        headers.insert(USER_AGENT, HeaderValue::from_static("rs_clob_client"));
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let (timestamp, server_time_request) = if self.use_server_time {
            let started = Instant::now();
            let timestamp = dispatch_client!(&self.client, |client| client.server_time().await)?;
            let completed = Instant::now();
            (timestamp, Some((started, completed)))
        } else {
            (Utc::now().timestamp(), None)
        };
        let normalized_body = String::from_utf8_lossy(&body).replace('\'', "\"");
        let message = format!(
            "{}{}{}{}",
            timestamp,
            request.method(),
            request.url().path(),
            normalized_body
        );
        let credentials = self.client.credentials();
        let secret = URL_SAFE.decode(credentials.secret().expose_secret())?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&secret)?;
        mac.update(message.as_bytes());
        let signature = URL_SAFE.encode(mac.finalize().into_bytes());
        let headers = request.headers_mut();
        headers.insert(
            "POLY_ADDRESS",
            self.client.address().to_checksum(None).parse()?,
        );
        headers.insert("POLY_API_KEY", credentials.key().to_string().parse()?);
        headers.insert(
            "POLY_PASSPHRASE",
            credentials.passphrase().expose_secret().parse()?,
        );
        headers.insert("POLY_SIGNATURE", signature.parse()?);
        headers.insert("POLY_TIMESTAMP", timestamp.to_string().parse()?);

        let method = request.method().clone();
        let path = request.url().path().to_owned();
        let invocation_started = Instant::now();
        let response = match self.order_post_client.execute(request).await {
            Ok(response) => response,
            Err(error) => {
                let detail = reqwest_error_class(&error);
                return Ok(RoutedPostAttempt {
                    result: Err(routed_post_outcome_unknown(format!(
                        "PIA SOCKS POST /orders outcome unknown after request invocation: {detail}"
                    ))),
                    timing: routed_post_timing(
                        invocation_started,
                        server_time_request,
                        None,
                        None,
                        None,
                        None,
                        "transport_unknown",
                        Some(format!("request invocation failed: {detail}")),
                        None,
                    ),
                });
            }
        };
        let response_headers_instant = Instant::now();
        let response_headers_ts_ms = Some(now_ms());
        let response_headers_elapsed_us = Some(elapsed_us(
            response_headers_instant.duration_since(invocation_started),
        ));
        let status = response.status();
        let status_code = Some(status.as_u16());
        let response_body = match response.bytes().await {
            Ok(response_body) => response_body,
            Err(error) => {
                let detail = reqwest_error_class(&error);
                return Ok(RoutedPostAttempt {
                    result: Err(routed_post_outcome_unknown(format!(
                        "PIA SOCKS POST /orders outcome unknown while reading response body: {detail}"
                    ))),
                    timing: routed_post_timing(
                        invocation_started,
                        server_time_request,
                        response_headers_ts_ms,
                        response_headers_elapsed_us,
                        None,
                        status_code,
                        "transport_unknown",
                        Some(format!("response body read failed: {detail}")),
                        None,
                    ),
                });
            }
        };
        let response_body_elapsed_us = Some(elapsed_us(invocation_started.elapsed()));
        if !status.is_success() {
            let message = bounded_post_diagnostic(&response_body);
            let response_evidence = Some(
                serde_json::from_slice::<serde_json::Value>(&response_body).map_or_else(
                    |_| format!("bytes={};json=malformed", response_body.len()),
                    |value| redacted_response_evidence(&value),
                ),
            );
            if message.contains("order_version_mismatch") {
                tracing::error!(
                    target: "execution.submit",
                    "routed POST /orders version mismatch; restart required before submission can resume"
                );
            }
            if status.is_server_error() {
                return Ok(RoutedPostAttempt {
                    result: Err(routed_post_outcome_unknown(format!(
                        "PIA SOCKS POST /orders outcome unknown after HTTP {}; body={message:?}",
                        status.as_u16()
                    ))),
                    timing: routed_post_timing(
                        invocation_started,
                        server_time_request,
                        response_headers_ts_ms,
                        response_headers_elapsed_us,
                        response_body_elapsed_us,
                        status_code,
                        "http_server_error",
                        Some(format!("HTTP {}", status.as_u16())),
                        response_evidence,
                    ),
                });
            }
            return Ok(RoutedPostAttempt {
                result: Err(PolymarketClientError::status(status, method, path, message)),
                timing: routed_post_timing(
                    invocation_started,
                    server_time_request,
                    response_headers_ts_ms,
                    response_headers_elapsed_us,
                    response_body_elapsed_us,
                    status_code,
                    "http_error",
                    Some(format!("HTTP {}", status.as_u16())),
                    response_evidence,
                ),
            });
        }

        let expected_count = orders.len();
        let value = match serde_json::from_slice::<serde_json::Value>(&response_body) {
            Ok(value) => value,
            Err(error) => {
                let detail = serde_interpretation_error("malformed JSON response", &error);
                return Ok(RoutedPostAttempt {
                    result: Err(routed_post_outcome_unknown(format!(
                        "PIA SOCKS POST /orders outcome unknown: {detail}"
                    ))),
                    timing: routed_post_timing(
                        invocation_started,
                        server_time_request,
                        response_headers_ts_ms,
                        response_headers_elapsed_us,
                        response_body_elapsed_us,
                        status_code,
                        "response_unassociable",
                        Some(detail),
                        Some(format!("bytes={};json=malformed", response_body.len())),
                    ),
                });
            }
        };
        let response_evidence = Some(redacted_response_evidence(&value));
        let serde_json::Value::Array(entries) = value else {
            let detail = format!(
                "expected JSON array response, received {}",
                json_value_kind(&value)
            );
            return Ok(RoutedPostAttempt {
                result: Err(routed_post_outcome_unknown(format!(
                    "PIA SOCKS POST /orders outcome unknown: {detail}"
                ))),
                timing: routed_post_timing(
                    invocation_started,
                    server_time_request,
                    response_headers_ts_ms,
                    response_headers_elapsed_us,
                    response_body_elapsed_us,
                    status_code,
                    "response_unassociable",
                    Some(detail),
                    response_evidence,
                ),
            });
        };
        if entries.len() != expected_count {
            let detail = format!(
                "response count mismatch: expected {expected_count}, received {}",
                entries.len()
            );
            return Ok(RoutedPostAttempt {
                result: Err(routed_post_outcome_unknown(format!(
                    "PIA SOCKS POST /orders outcome unknown: {detail}"
                ))),
                timing: routed_post_timing(
                    invocation_started,
                    server_time_request,
                    response_headers_ts_ms,
                    response_headers_elapsed_us,
                    response_body_elapsed_us,
                    status_code,
                    "response_unassociable",
                    Some(detail),
                    response_evidence,
                ),
            });
        }

        let mut interpretation_errors = Vec::new();
        let responses = entries
            .into_iter()
            .enumerate()
            .map(
                |(index, entry)| match serde_json::from_value::<PostOrderResponse>(entry) {
                    Ok(response) => RoutedPostEntry::Decoded(response),
                    Err(error) => {
                        let error = serde_interpretation_error(
                            &format!("response[{index}] SDK decode failed"),
                            &error,
                        );
                        interpretation_errors.push(error.clone());
                        RoutedPostEntry::Undecodable { error }
                    }
                },
            )
            .collect();
        let attempt_result = if interpretation_errors.is_empty() {
            "response_decoded"
        } else {
            "response_partially_decoded"
        };
        let interpretation_error =
            (!interpretation_errors.is_empty()).then(|| interpretation_errors.join("; "));
        let timing = routed_post_timing(
            invocation_started,
            server_time_request,
            response_headers_ts_ms,
            response_headers_elapsed_us,
            response_body_elapsed_us,
            status_code,
            attempt_result,
            interpretation_error,
            response_evidence,
        );
        Ok(RoutedPostAttempt {
            result: Ok(responses),
            timing,
        })
    }
}

pub fn is_too_early_service_not_ready_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<PolymarketClientError>()
            .is_some_and(is_http_425)
    })
}

pub fn is_retryable_read_http_status(err: &PolymarketClientError) -> bool {
    http_status_code(err).is_some_and(|status| matches!(status, 408 | 500 | 502 | 503 | 504))
}

pub fn next_retryable_read_backoff_ms(
    err: &PolymarketClientError,
    state: &mut ReadRetryState,
) -> Option<u64> {
    if !is_retryable_read_http_status(err) || state.retries >= HTTP_READ_RETRY_ATTEMPTS_DEFAULT {
        return None;
    }
    state.retries = state.retries.saturating_add(1);
    let backoff_ms = advance_capped_full_jitter_backoff_ms(
        state.retries,
        &mut state.backoff_ms,
        HTTP_READ_RETRY_MAX_BACKOFF_MS_DEFAULT,
    );
    Some(backoff_ms)
}

pub fn advance_capped_full_jitter_backoff_ms(
    attempt: usize,
    backoff_ms: &mut u64,
    max_backoff_ms: u64,
) -> u64 {
    let current_backoff_ms = (*backoff_ms).min(max_backoff_ms);
    *backoff_ms = current_backoff_ms.saturating_mul(2).min(max_backoff_ms);
    crate::polymarket::api_control::capped_full_jitter_backoff_ms(
        attempt,
        current_backoff_ms,
        current_backoff_ms,
    )
    .min(max_backoff_ms)
}

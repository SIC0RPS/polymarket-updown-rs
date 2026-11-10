mod gamma_fetch;
mod market_parse;

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use polymarket_client_sdk::types::{Decimal, B256, U256};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONNECTION, CONTENT_TYPE, USER_AGENT};
use serde::{Deserialize, Serialize};

use crate::config::PolymarketConfig;
use crate::polymarket::identity::{MarketId, MarketIdentity};
use gamma_fetch::fetch_open_markets;
use market_parse::{
    canonical_underlying, choose_up_down_tokens, decimal_opt_to_f64,
    deserialize_json_string_or_seq, market_underlying, market_window_bounds_ms,
    normalize_underlying_allowlist, normalized_market_order_min_size, normalized_market_tick_size,
    normalized_slug_terms, parse_query_terms, query_matches_market, strip_underlying_terms,
    text_contains_term,
};

const CLOB_MARKET_INFO_ATTEMPT_TIMEOUT_MS: u64 = 3_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoveryGammaMarket {
    id: String,
    slug: Option<String>,
    question: Option<String>,
    description: Option<String>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_json_string_or_seq")]
    clob_token_ids: Vec<U256>,
    #[serde(default, deserialize_with = "deserialize_json_string_or_seq")]
    outcomes: Vec<String>,
    condition_id: Option<B256>,
    maker_base_fee: Option<i32>,
    rewards_min_size: Option<Decimal>,
    rewards_max_spread: Option<Decimal>,
    resolution_source: Option<String>,
    neg_risk: Option<bool>,
    itode: Option<bool>,
    order_price_min_tick_size: Option<Decimal>,
    order_min_size: Option<Decimal>,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TokenLifecycleState {
    Active,
    InvalidAfter404,
    Resolved,
    #[default]
    PendingRevalidation,
}

impl TokenLifecycleState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::InvalidAfter404 => "invalid_after_404",
            Self::Resolved => "resolved",
            Self::PendingRevalidation => "pending_revalidation",
        }
    }

    pub const fn ctf_inventory_eligible(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketSelection {
    pub identity: MarketIdentity,
    pub market_id: MarketId,
    pub condition_id_b256: Option<B256>,
    pub condition_id: MarketId,
    pub gamma_market_id: String,
    pub underlying: String,
    pub up_token: U256,
    pub down_token: U256,
    pub up_token_id: String,
    pub down_token_id: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub maker_base_fee_bps: Option<i32>,
    pub taker_base_fee_bps: Option<i32>,
    pub minimum_tick_size: Option<f64>,
    pub minimum_order_size: Option<f64>,
    pub rewards_min_size: Option<f64>,
    pub rewards_max_spread: Option<f64>,
    pub resolution_source: Option<String>,
    pub neg_risk: Option<bool>,
    pub itode: Option<bool>,
    pub standard_binary_ctf: bool,
    pub question: Option<String>,
    pub slug: Option<String>,
    pub up_lifecycle: TokenLifecycleState,
    pub down_lifecycle: TokenLifecycleState,
}

impl MarketSelection {
    pub fn market_id(&self) -> &MarketId {
        &self.identity.id
    }

    pub fn gamma_market_id(&self) -> &str {
        self.identity.gamma_market_id.as_str()
    }

    pub fn condition_id_b256(&self) -> Option<B256> {
        self.identity.condition_id_b256
    }

    pub fn up_token_id(&self) -> &str {
        self.up_token_id.as_str()
    }

    pub fn down_token_id(&self) -> &str {
        self.down_token_id.as_str()
    }

    pub const fn ctf_inventory_eligible(&self) -> bool {
        self.up_lifecycle.ctf_inventory_eligible() && self.down_lifecycle.ctf_inventory_eligible()
    }

    pub const fn standard_binary_ctf(&self) -> bool {
        self.standard_binary_ctf
    }
}

pub async fn discover_enabled_updown_markets(
    cfg: &PolymarketConfig,
    enabled_assets: &[String],
    http: &reqwest::Client,
) -> Result<Vec<MarketSelection>> {
    ensure!(
        cfg.discovery.enabled,
        "polymarket.discovery.enabled must be true to use discovery"
    );

    let now_ms = Utc::now().timestamp_millis();
    let query_terms_raw = parse_query_terms(&cfg.discovery.query);
    let query_terms = strip_underlying_terms(&query_terms_raw);
    let slug_terms = normalized_slug_terms(&cfg.discovery.slug_must_contain);
    let allowlist = normalize_underlying_allowlist(&cfg.discovery.underlyings_allowlist);
    let candidates = fetch_open_markets(http, cfg, enabled_assets).await?;

    let mut selected = Vec::new();
    let mut seen_market_ids = HashSet::new();
    for market in candidates {
        let Some(underlying) = market_underlying(&market) else {
            continue;
        };
        if !allowlist.is_empty() && !allowlist.contains(underlying.as_str()) {
            continue;
        }

        if !query_matches_market(&market, &query_terms) {
            continue;
        }

        if !slug_terms.is_empty() {
            let Some(slug) = market.slug.as_deref() else {
                continue;
            };
            let mut ok = true;
            for needle in &slug_terms {
                if !text_contains_term(Some(slug), needle) {
                    ok = false;
                    break;
                }
            }
            if !ok {
                continue;
            }
        }

        // Gamma intermittently omits these bools; only reject when the API
        // explicitly reports "false" so discovery does not stall on nulls.
        if cfg.discovery.require_order_book && market.enable_order_book == Some(false) {
            continue;
        }
        if cfg.discovery.require_accepting_orders && market.accepting_orders == Some(false) {
            continue;
        }
        if market.active == Some(false)
            || market.closed == Some(true)
            || market.archived == Some(true)
        {
            continue;
        }
        let Some(minimum_tick_size) = normalized_market_tick_size(&market) else {
            continue;
        };
        let Some(minimum_order_size) = normalized_market_order_min_size(&market) else {
            continue;
        };

        let expected = cfg.discovery.window_ms as i64;
        let tol = cfg.discovery.window_tolerance_ms as i64;
        let Some((start_ms, end_ms)) = market_window_bounds_ms(&market, expected, tol) else {
            continue;
        };

        let time_left = end_ms.saturating_sub(now_ms);
        if time_left < (cfg.discovery.min_time_left_ms as i64)
            || time_left > (cfg.discovery.max_time_left_ms as i64)
        {
            continue;
        }

        let token_ids = &market.clob_token_ids;
        if token_ids.len() < 2 {
            continue;
        }

        let Some((up_token, down_token)) = choose_up_down_tokens(&market.outcomes, token_ids)
        else {
            continue;
        };

        let Some(condition_id_b256) = market.condition_id else {
            continue;
        };
        let condition_id = MarketId::new(condition_id_b256.to_string());

        let selection = MarketSelection {
            identity: MarketIdentity::new(
                condition_id.clone(),
                market.id.clone(),
                Some(condition_id_b256),
            ),
            market_id: condition_id.clone(),
            condition_id_b256: Some(condition_id_b256),
            condition_id: condition_id.clone(),
            gamma_market_id: market.id,
            underlying: underlying.clone(),
            up_token,
            down_token,
            up_token_id: up_token.to_string(),
            down_token_id: down_token.to_string(),
            window_start_ms: start_ms.max(0) as u64,
            window_end_ms: end_ms.max(0) as u64,
            maker_base_fee_bps: market.maker_base_fee,
            taker_base_fee_bps: None,
            minimum_tick_size: Some(minimum_tick_size),
            minimum_order_size: Some(minimum_order_size),
            rewards_min_size: decimal_opt_to_f64(market.rewards_min_size),
            rewards_max_spread: decimal_opt_to_f64(market.rewards_max_spread),
            standard_binary_ctf: market.condition_id.is_some()
                && market.neg_risk == Some(false)
                && market.outcomes.len() == 2
                && token_ids.len() == 2,
            resolution_source: market.resolution_source,
            neg_risk: market.neg_risk,
            itode: market.itode,
            question: market.question,
            slug: market.slug,
            up_lifecycle: TokenLifecycleState::PendingRevalidation,
            down_lifecycle: TokenLifecycleState::PendingRevalidation,
        };

        if now_ms < end_ms {
            push_unique_market_candidate(&mut selected, &mut seen_market_ids, selection);
        }
    }

    order_market_selections(&mut selected, now_ms.max(0) as u64);
    hydrate_clob_market_metadata(http, cfg, &mut selected).await;
    Ok(selected)
}

pub(super) fn order_market_selections(markets: &mut [MarketSelection], now_ms: u64) {
    let order_key = |market: &MarketSelection| {
        if market.window_start_ms <= now_ms && now_ms < market.window_end_ms {
            (0, market.window_end_ms)
        } else if market.window_start_ms > now_ms {
            (1, market.window_start_ms)
        } else {
            (2, market.window_end_ms)
        }
    };
    markets.sort_by(|a, b| {
        order_key(a)
            .cmp(&order_key(b))
            .then_with(|| a.underlying.cmp(&b.underlying))
            .then_with(|| a.market_id().cmp(b.market_id()))
    });
}

#[derive(Debug, Deserialize)]
struct ClobMarketInfoMetadata {
    #[serde(default)]
    itode: Option<bool>,
    #[serde(rename = "fd", default)]
    fee_details: Option<ClobFeeDetailsMetadata>,
}

#[derive(Debug, Deserialize)]
struct ClobFeeDetailsMetadata {
    #[serde(rename = "r")]
    rate: Decimal,
    #[serde(rename = "e")]
    exponent: u32,
    #[serde(rename = "to")]
    taker_only: bool,
}

async fn hydrate_clob_market_metadata(
    http: &reqwest::Client,
    cfg: &PolymarketConfig,
    selections: &mut [MarketSelection],
) {
    let lookups = selections
        .iter()
        .enumerate()
        .filter(|(_, selection)| {
            selection.itode.is_none() || selection.taker_base_fee_bps.is_none()
        })
        .map(|(index, selection)| {
            let market_id = selection.market_id().clone();
            let condition_id = selection.condition_id.clone();
            async move {
                let result =
                    fetch_clob_market_metadata_once(http, cfg, condition_id.as_str()).await;
                (index, market_id, condition_id, result)
            }
        });
    for (index, market_id, condition_id, result) in join_all(lookups).await {
        match result {
            Ok(metadata) => {
                if selections[index].itode.is_none() {
                    selections[index].itode = Some(metadata.itode.unwrap_or(false));
                }
                if selections[index].taker_base_fee_bps.is_none() {
                    selections[index].taker_base_fee_bps =
                        normalized_v2_taker_fee_bps(metadata.fee_details.as_ref());
                }
            }
            Err(err) => {
                tracing::warn!(
                    target: "polymarket.discovery",
                    market_id = %market_id,
                    condition_id = %condition_id,
                    error = %err,
                    "clob_market_metadata_lookup_failed"
                );
            }
        }
    }
}

fn normalized_v2_taker_fee_bps(fee_details: Option<&ClobFeeDetailsMetadata>) -> Option<i32> {
    let fee_details = fee_details?;
    if fee_details.exponent != 1
        || !fee_details.taker_only
        || fee_details.rate.is_sign_negative()
        || fee_details.rate > Decimal::ONE
    {
        return None;
    }
    if fee_details.rate == Decimal::ZERO {
        return Some(0);
    }
    let fee_bps = fee_details
        .rate
        .checked_mul(Decimal::from(10_000_u32))?
        .normalize();
    if fee_bps.scale() != 0 {
        return None;
    }
    i32::try_from(fee_bps.mantissa()).ok()
}

async fn fetch_clob_market_metadata_once(
    http: &reqwest::Client,
    cfg: &PolymarketConfig,
    condition_id: &str,
) -> Result<ClobMarketInfoMetadata> {
    let url = format!(
        "{}/clob-markets/{}",
        cfg.clob_http_url.trim_end_matches('/'),
        condition_id
    );
    let response = tokio::time::timeout(
        Duration::from_millis(CLOB_MARKET_INFO_ATTEMPT_TIMEOUT_MS),
        http.get(url).send(),
    )
    .await
    .context("clob market info request timed out")?
    .context("clob market info request failed")?;
    let status = response.status();
    if !status.is_success() {
        let mut body = response.text().await.unwrap_or_default();
        body.truncate(512);
        return Err(anyhow!("clob market info returned status {status}: {body}"));
    }
    let info = tokio::time::timeout(
        Duration::from_millis(CLOB_MARKET_INFO_ATTEMPT_TIMEOUT_MS),
        response.json::<ClobMarketInfoMetadata>(),
    )
    .await
    .context("clob market info decode timed out")?
    .context("clob market info response decode failed")?;
    Ok(info)
}

fn push_unique_market_candidate(
    out: &mut Vec<MarketSelection>,
    seen_market_ids: &mut HashSet<MarketId>,
    sel: MarketSelection,
) {
    if seen_market_ids.insert(sel.market_id().clone()) {
        out.push(sel);
    }
}

pub fn build_gamma_discovery_http_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("rs_clob_client"));
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .context("failed to build Gamma discovery HTTP client")
}

pub(super) fn discovery_query_underlyings(
    enabled_assets: &[String],
    cfg: &PolymarketConfig,
) -> Vec<String> {
    let mut out: Vec<String> = enabled_assets
        .iter()
        .filter_map(|asset| canonical_underlying(asset))
        .collect();
    if out.is_empty() {
        out = normalize_underlying_allowlist(&cfg.discovery.underlyings_allowlist)
            .into_iter()
            .collect();
    }
    out.sort();
    out.dedup();
    out
}

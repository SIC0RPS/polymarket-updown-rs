use super::*;

pub(super) fn choose_up_down_tokens(
    outcomes: &[String],
    token_ids: &[U256],
) -> Option<(U256, U256)> {
    if outcomes.len() != 2 || token_ids.len() != 2 {
        return None;
    }
    let (up_index, down_index) = if let (Some(up), Some(down)) = (
        find_outcome_index(outcomes, "up"),
        find_outcome_index(outcomes, "down"),
    ) {
        (up, down)
    } else if let (Some(yes), Some(no)) = (
        find_outcome_index(outcomes, "yes"),
        find_outcome_index(outcomes, "no"),
    ) {
        (yes, no)
    } else {
        return None;
    };
    Some((token_ids[up_index], token_ids[down_index]))
}

pub(super) fn parse_query_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .filter_map(|term| {
            let cleaned = term.trim_matches(|c: char| !c.is_ascii_alphanumeric());
            if cleaned.is_empty() {
                None
            } else {
                Some(cleaned.to_ascii_lowercase())
            }
        })
        .collect()
}

fn normalize_known_underlying_alias(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "btc" | "bitcoin" => Some("btc"),
        "eth" | "ethereum" => Some("eth"),
        "sol" | "solana" => Some("sol"),
        "xrp" | "ripple" => Some("xrp"),
        "bnb" | "binancecoin" | "binance-coin" => Some("bnb"),
        _ => None,
    }
}

pub(super) fn canonical_underlying(raw: &str) -> Option<String> {
    if let Some(alias) = normalize_known_underlying_alias(raw) {
        return Some(alias.to_string());
    }
    let cleaned = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if cleaned.is_empty() {
        return None;
    }
    Some(cleaned)
}

pub(super) fn normalize_underlying_allowlist(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .filter_map(|v| canonical_underlying(v))
        .collect()
}

pub(super) fn strip_underlying_terms(terms: &[String]) -> Vec<String> {
    terms
        .iter()
        .filter(|term| normalize_known_underlying_alias(term).is_none())
        .cloned()
        .collect()
}

pub(super) fn normalized_slug_terms(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

pub(super) fn market_underlying(market: &DiscoveryGammaMarket) -> Option<String> {
    if let Some(slug) = market.slug.as_deref() {
        if let Some(underlying) = underlying_from_slug(slug) {
            return Some(underlying);
        }
    }

    for source in [
        market.slug.as_deref(),
        market.question.as_deref(),
        market.description.as_deref(),
    ] {
        let Some(source) = source else {
            continue;
        };
        for word in source.split(|c: char| !c.is_ascii_alphanumeric()) {
            if word.is_empty() {
                continue;
            }
            if let Some(alias) = normalize_known_underlying_alias(word) {
                return Some(alias.to_string());
            }
        }
    }

    None
}

fn underlying_from_slug(slug: &str) -> Option<String> {
    let parts: Vec<String> = slug
        .split('-')
        .filter(|p| !p.is_empty())
        .map(|p| p.to_ascii_lowercase())
        .collect();
    let idx = parts.iter().position(|p| p == "updown")?;
    if idx == 0 {
        return None;
    }
    canonical_underlying(&parts[idx - 1])
}

pub(super) fn decimal_opt_to_f64(
    value: Option<polymarket_client_sdk::types::Decimal>,
) -> Option<f64> {
    let value = value?;
    let repr = value.normalize().to_string();
    repr.parse::<f64>().ok().filter(|v| v.is_finite())
}

pub(super) fn normalized_market_tick_size(market: &DiscoveryGammaMarket) -> Option<f64> {
    decimal_opt_to_f64(market.order_price_min_tick_size).filter(|tick| *tick > 0.0)
}

pub(super) fn normalized_market_order_min_size(market: &DiscoveryGammaMarket) -> Option<f64> {
    decimal_opt_to_f64(market.order_min_size).filter(|size| *size > 0.0)
}

pub(super) fn market_window_bounds_ms(
    market: &DiscoveryGammaMarket,
    expected_ms: i64,
    tolerance_ms: i64,
) -> Option<(i64, i64)> {
    if let (Some(start), Some(end)) = (market.start_date, market.end_date) {
        let start_ms = start.timestamp_millis();
        let end_ms = end.timestamp_millis();
        let duration_ms = end_ms.saturating_sub(start_ms);
        let min_dur = expected_ms.saturating_sub(tolerance_ms);
        let max_dur = expected_ms.saturating_add(tolerance_ms);
        if duration_ms >= min_dur && duration_ms <= max_dur {
            return Some((start_ms, end_ms));
        }
    }

    // Gamma currently exposes BTC up/down windows with a trailing unix timestamp in slug,
    // while start/end dates can span longer than the actual trading window.
    // For these slugs, the numeric suffix is the window *start* timestamp.
    let slug = market.slug.as_deref()?;
    let start_ms = parse_window_start_ms_from_slug(slug)?;
    let end_ms = start_ms.saturating_add(expected_ms.max(1));
    Some((start_ms, end_ms))
}

fn parse_window_start_ms_from_slug(slug: &str) -> Option<i64> {
    let tail = slug.rsplit('-').next()?;
    if tail.is_empty() || !tail.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let raw = tail.parse::<i64>().ok()?;
    match tail.len() {
        10 => raw.checked_mul(1000),
        13 => Some(raw),
        _ => None,
    }
}

pub(super) fn query_matches_market(market: &DiscoveryGammaMarket, terms: &[String]) -> bool {
    if terms.is_empty() {
        return true;
    }
    query_matches_text(
        terms,
        market.slug.as_deref(),
        market.question.as_deref(),
        market.description.as_deref(),
    )
}

fn query_matches_text(
    terms: &[String],
    slug: Option<&str>,
    question: Option<&str>,
    description: Option<&str>,
) -> bool {
    terms.iter().all(|term| match term.as_str() {
        // Gamma markets commonly use "btc" in slug while user query uses "bitcoin".
        "bitcoin" | "btc" => {
            text_contains_term(slug, "bitcoin")
                || text_contains_term(question, "bitcoin")
                || text_contains_term(description, "bitcoin")
                || text_contains_term(slug, "btc")
                || text_contains_term(question, "btc")
                || text_contains_term(description, "btc")
        }
        _ => {
            text_contains_term(slug, term)
                || text_contains_term(question, term)
                || text_contains_term(description, term)
        }
    })
}

pub(super) fn text_contains_term(source: Option<&str>, needle: &str) -> bool {
    let Some(source) = source else {
        return false;
    };
    let source = source.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() || source.len() < needle.len() {
        return false;
    }

    for start in 0..=source.len() - needle.len() {
        if start > 0 && source[start - 1].is_ascii_alphanumeric() {
            continue;
        }
        let end = start + needle.len();
        if end < source.len() && source[end].is_ascii_alphanumeric() {
            continue;
        }
        if source[start..end].eq_ignore_ascii_case(needle) {
            return true;
        }
    }
    false
}

pub(super) fn deserialize_json_string_or_seq<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum JsonStringOrSeq<T> {
        Seq(Vec<T>),
        JsonString(String),
        Null,
    }

    match JsonStringOrSeq::deserialize(deserializer)? {
        JsonStringOrSeq::Seq(items) => Ok(items),
        JsonStringOrSeq::JsonString(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(Vec::new())
            } else {
                serde_json::from_slice(trimmed.as_bytes()).map_err(serde::de::Error::custom)
            }
        }
        JsonStringOrSeq::Null => Ok(Vec::new()),
    }
}

fn find_outcome_index(outcomes: &[String], needle: &str) -> Option<usize> {
    let needle = needle.to_ascii_lowercase();
    outcomes
        .iter()
        .position(|o| o.to_ascii_lowercase() == needle)
}

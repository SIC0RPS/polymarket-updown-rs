use super::*;

pub(crate) fn parse_recent_exit_signal(
    state: &UiState,
    ts_ms: u64,
    side: &str,
    signal_reason: &str,
) -> Option<RecentExitExecutionSnapshot> {
    if !side.eq_ignore_ascii_case("sell") {
        return None;
    }
    if let Some(rest) = signal_reason.strip_prefix("paired_first_managed_sell ") {
        let market_id = parse_kv_token(rest, "market=").map(MarketId::new);
        let token = parse_kv_token(rest, "token=")?.to_string();
        let qty = parse_kv_token(rest, "qty=").and_then(|value| value.parse::<f64>().ok());
        let px = parse_kv_token(rest, "px=").and_then(|value| value.parse::<f64>().ok());
        let reason = parse_kv_token(rest, "reason=").map(str::to_string);
        return Some(RecentExitExecutionSnapshot {
            ts_ms,
            market_id,
            token,
            side: side.to_string(),
            close_hint: "paired_first_managed_sell".to_string(),
            reason,
            signal_reason: signal_reason.to_string(),
            intent_px: px,
            intent_qty: qty,
            ack_observed: false,
            ack_ts_ms: None,
            fill_observed: false,
            fill_ts_ms: None,
            fill_px: None,
            fill_qty: None,
        });
    }
    if let Some(rest) = signal_reason.strip_prefix("paired_post_proceeds_sell ") {
        let market_id = parse_kv_token(rest, "market=").map(MarketId::new);
        let token = parse_kv_token(rest, "token=")?.to_string();
        let qty = parse_kv_token(rest, "qty=").and_then(|value| value.parse::<f64>().ok());
        let px = parse_kv_token(rest, "px=").and_then(|value| value.parse::<f64>().ok());
        let reason = parse_kv_token(rest, "reason=").map(str::to_string);
        return Some(RecentExitExecutionSnapshot {
            ts_ms,
            market_id,
            token,
            side: side.to_string(),
            close_hint: "paired_post_proceeds_sell".to_string(),
            reason,
            signal_reason: signal_reason.to_string(),
            intent_px: px,
            intent_qty: qty,
            ack_observed: false,
            ack_ts_ms: None,
            fill_observed: false,
            fill_ts_ms: None,
            fill_px: None,
            fill_qty: None,
        });
    }
    if let Some(rest) = signal_reason.strip_prefix("predictive_post_proceeds_sell ") {
        let market_id = parse_kv_token(rest, "market=").map(MarketId::new);
        let token = parse_kv_token(rest, "token=")?.to_string();
        let qty = parse_kv_token(rest, "qty=").and_then(|value| value.parse::<f64>().ok());
        let px = parse_kv_token(rest, "px=").and_then(|value| value.parse::<f64>().ok());
        let reason = parse_kv_token(rest, "reason=").map(str::to_string);
        return Some(RecentExitExecutionSnapshot {
            ts_ms,
            market_id,
            token,
            side: side.to_string(),
            close_hint: "predictive_post_proceeds_sell".to_string(),
            reason,
            signal_reason: signal_reason.to_string(),
            intent_px: px,
            intent_qty: qty,
            ack_observed: false,
            ack_ts_ms: None,
            fill_observed: false,
            fill_ts_ms: None,
            fill_px: None,
            fill_qty: None,
        });
    }
    let rest = signal_reason.strip_prefix("exit_")?;
    let (mode, details) = rest.split_once(' ')?;
    let token = parse_kv_token(details, "token=")?.to_string();
    let market_id = state.token_to_condition_id.get(&token).cloned();
    let px = parse_kv_token(details, "px=").and_then(|value| value.parse::<f64>().ok());
    let qty = parse_kv_token(details, "inv=").and_then(|value| value.parse::<f64>().ok());
    Some(RecentExitExecutionSnapshot {
        ts_ms,
        market_id,
        token,
        side: side.to_string(),
        close_hint: mode.to_string(),
        reason: Some(mode.to_string()),
        signal_reason: signal_reason.to_string(),
        intent_px: px,
        intent_qty: qty,
        ack_observed: false,
        ack_ts_ms: None,
        fill_observed: false,
        fill_ts_ms: None,
        fill_px: None,
        fill_qty: None,
    })
}

pub(crate) fn parse_recent_risk_block(
    ts_ms: u64,
    rule: &str,
    details: &str,
) -> RecentRiskBlockSnapshot {
    let detail = parse_detail_pairs(details).into_iter().collect();
    RecentRiskBlockSnapshot {
        ts_ms,
        rule: rule.to_string(),
        detail,
        raw: details.to_string(),
    }
}

pub(crate) fn parse_kv_token<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let start = text.find(key)?;
    let value = &text[start + key.len()..];
    Some(value.split_whitespace().next().unwrap_or(value))
}

fn parse_detail_pairs(text: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut chars = text.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        let Some(eq_rel) = text[start..].find('=') else {
            break;
        };
        let eq = start + eq_rel;
        let key = &text[start..eq];
        let value_start = eq + 1;
        let value_end = find_detail_value_end(text, value_start);
        let value = clean_detail_value(&text[value_start..value_end]).to_string();
        pairs.push((key.to_string(), value));
        while let Some((idx, ch)) = chars.peek().copied() {
            if idx < value_end {
                chars.next();
                continue;
            }
            if ch.is_whitespace() || ch == ',' {
                chars.next();
                continue;
            }
            break;
        }
    }
    pairs
}

fn clean_detail_value(value: &str) -> &str {
    value.trim_matches(|c| c == ',' || c == '"' || c == '\'')
}

fn find_detail_value_end(text: &str, value_start: usize) -> usize {
    let bytes = text.as_bytes();
    if value_start >= bytes.len() {
        return value_start;
    }
    let quote = match bytes[value_start] {
        b'"' => Some(b'"'),
        b'\'' => Some(b'\''),
        _ => None,
    };
    if let Some(quote) = quote {
        let mut idx = value_start + 1;
        while idx < bytes.len() {
            if bytes[idx] == quote {
                return idx + 1;
            }
            idx += 1;
        }
        return bytes.len();
    }
    let mut idx = value_start;
    while idx < bytes.len() && !bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

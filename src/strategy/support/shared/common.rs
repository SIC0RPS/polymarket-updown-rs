use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) use bbwick_engine::state::is_valid_polymarket_tick_size as is_valid_tick_size;

pub(crate) fn round_to_tick(px: f64, tick: f64, ceil: bool) -> f64 {
    if !(px.is_finite() && tick.is_finite() && tick > 0.0) {
        return px;
    }
    if ceil {
        (px / tick).ceil() * tick
    } else {
        (px / tick).floor() * tick
    }
}

pub(crate) fn canonical_symbol(raw: &str) -> Option<Cow<'_, str>> {
    let trimmed = raw.trim();
    if trimmed.len() < 3 {
        return None;
    }
    if trimmed
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() && !byte.is_ascii_lowercase())
    {
        return Some(Cow::Borrowed(trimmed));
    }
    let symbol = trimmed
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    if symbol.len() < 3 {
        return None;
    }
    Some(Cow::Owned(symbol))
}

pub(crate) fn ts_ms_from_ns(ts_ns: i64) -> Option<u64> {
    if ts_ns <= 0 {
        return None;
    }
    Some((ts_ns as u64) / 1_000_000)
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn ingest_age_ms(now_ns: u64, then_ns: u64) -> Option<u64> {
    if now_ns == 0 || then_ns == 0 || now_ns < then_ns {
        return None;
    }
    Some(now_ns.saturating_sub(then_ns).saturating_add(999_999) / 1_000_000)
}

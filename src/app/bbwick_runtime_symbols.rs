use std::collections::HashMap;

use polymarket_hft::capture::scope::configured_capture_underlyings;
use polymarket_hft::config::{AppConfig, MakerRebateMmStrategyConfig};

pub(crate) fn required_bbwick_symbols_for_strategy_cfg(
    cfg: &AppConfig,
    strategy_cfg: &MakerRebateMmStrategyConfig,
) -> Vec<String> {
    let mut required: Vec<String> = cfg.bbwick_oracle.active_symbols();
    for underlying in &strategy_cfg.enabled_assets {
        required.push(cfg.bbwick_oracle.resolve_symbol_for_underlying(underlying));
    }
    if cfg.capture.enabled {
        for underlying in configured_capture_underlyings(cfg) {
            required.push(cfg.bbwick_oracle.resolve_symbol_for_underlying(&underlying));
        }
    }
    required.sort();
    required.dedup();
    required
}

pub(crate) fn cached_canonical_bbwick_symbol<'a>(
    cache: &'a mut HashMap<String, Option<String>>,
    raw: &str,
) -> Option<&'a str> {
    if cache.contains_key(raw) {
        return cache.get(raw).and_then(Option::as_deref);
    }
    let cleaned = raw
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    let canonical = (cleaned.len() >= 3).then_some(cleaned);
    cache.insert(raw.to_string(), canonical);
    cache.get(raw).and_then(Option::as_deref)
}

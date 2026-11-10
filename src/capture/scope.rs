use std::collections::HashSet;

use crate::config::AppConfig;

const SUPPORTED_CAPTURE_UNDERLYINGS: &[&str] = &["BTC", "ETH", "SOL", "XRP"];
pub const REQUIRED_CHAINLINK_PRICE_SYMBOLS: &[&str] = &["btc/usd", "eth/usd", "sol/usd", "xrp/usd"];

pub fn configured_capture_underlyings(cfg: &AppConfig) -> Vec<String> {
    let discovery_allowed = cfg
        .polymarket
        .discovery
        .underlyings_allowlist
        .iter()
        .filter_map(|asset| normalize_supported_capture_underlying(asset))
        .collect::<std::collections::BTreeSet<_>>();

    if discovery_allowed.is_empty() {
        SUPPORTED_CAPTURE_UNDERLYINGS
            .iter()
            .map(|symbol| (*symbol).to_string())
            .collect()
    } else {
        discovery_allowed.into_iter().collect()
    }
}

pub fn replay_required_underlyings_for_capture(cfg: &AppConfig) -> Vec<String> {
    let enabled_assets = cfg
        .strategy
        .maker_rebate_mm
        .enabled_assets
        .iter()
        .filter_map(|asset| normalize_supported_capture_underlying(asset))
        .collect::<std::collections::BTreeSet<_>>();

    if enabled_assets.is_empty() {
        SUPPORTED_CAPTURE_UNDERLYINGS
            .iter()
            .map(|symbol| (*symbol).to_string())
            .collect()
    } else {
        enabled_assets.into_iter().collect()
    }
}

pub fn required_bbwick_symbols_for_capture(cfg: &AppConfig) -> Vec<String> {
    let mut required: Vec<String> = cfg.bbwick_oracle.active_symbols();
    for underlying in &cfg.strategy.maker_rebate_mm.enabled_assets {
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

pub(crate) fn capture_underlying_from_text(value: &str) -> Option<String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "btc" | "btcusdt" | "btc-usd" | "polymarket-btc-usd" => Some("btc".to_string()),
        "eth" | "ethusdt" | "eth-usd" | "polymarket-eth-usd" => Some("eth".to_string()),
        "sol" | "solusdt" | "sol-usd" | "polymarket-sol-usd" => Some("sol".to_string()),
        "xrp" | "xrpusdt" | "xrp-usd" | "polymarket-xrp-usd" => Some("xrp".to_string()),
        _ => None,
    }
}

pub(crate) fn capture_symbol_allowed(allowlist: &[String], symbol: &str) -> bool {
    if allowlist.is_empty() {
        return true;
    }
    let Some(underlying) = capture_underlying_from_text(symbol) else {
        return false;
    };
    allowlist
        .iter()
        .filter_map(|allowed| capture_underlying_from_text(allowed))
        .any(|allowed| allowed == underlying)
}

pub(crate) fn normalize_capture_underlying(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "btc" | "eth" | "sol" | "xrp" => Some(value),
        _ => None,
    }
}

fn normalize_supported_capture_underlying(raw: &str) -> Option<String> {
    let upper = raw.trim().to_ascii_uppercase();
    SUPPORTED_CAPTURE_UNDERLYINGS
        .iter()
        .find(|symbol| **symbol == upper)
        .map(|symbol| (*symbol).to_string())
}

const MAX_CAPTURE_LAST_TRADES: usize = 64;

pub(crate) fn capture_filtered_polymarket_state(
    state: &bbwick_engine::PolymarketOddsState,
    symbols_allowlist: &[String],
    _wall_ms: u64,
) -> bbwick_engine::PolymarketOddsState {
    if symbols_allowlist.is_empty() {
        return state.clone();
    }

    let is_allowed = |underlying: &str| {
        symbols_allowlist
            .iter()
            .any(|symbol| symbol.eq_ignore_ascii_case(underlying))
    };
    let mut markets = Vec::new();
    let mut kept_tokens = HashSet::new();
    for market in &state.markets {
        if is_allowed(&market.underlying) {
            kept_tokens.insert(market.up_token);
            kept_tokens.insert(market.down_token);
            markets.push(market.clone());
        }
    }
    let selection = state
        .selection
        .as_ref()
        .filter(|market| is_allowed(&market.underlying));
    let next_selection = state
        .next_selection
        .as_ref()
        .filter(|market| is_allowed(&market.underlying));

    if let Some(selection) = selection {
        kept_tokens.insert(selection.up_token);
        kept_tokens.insert(selection.down_token);
    }
    if let Some(selection) = next_selection {
        kept_tokens.insert(selection.up_token);
        kept_tokens.insert(selection.down_token);
    }

    let resolved_market_winners = state
        .resolved_market_winners
        .iter()
        .filter(|(_, winner)| kept_tokens.contains(winner))
        .map(|(market, winner)| (*market, *winner))
        .collect::<std::collections::HashMap<_, _>>();
    let mut last_trades = std::collections::VecDeque::new();
    for trade in state.last_trades.iter().rev() {
        if kept_tokens.contains(&trade.token_id) {
            last_trades.push_front(trade.clone());
            if last_trades.len() == MAX_CAPTURE_LAST_TRADES {
                break;
            }
        }
    }
    let next_last_trade_seq = last_trades.back().map(|trade| trade.seq).unwrap_or(0);

    bbwick_engine::PolymarketOddsState {
        updated_at_ms: state.updated_at_ms,
        revision: state.revision,
        trigger_fingerprint: state.trigger_fingerprint.clone(),
        selection_updated_at_ms: state.selection_updated_at_ms,
        next_selection_updated_at_ms: state.next_selection_updated_at_ms,
        token_lifecycle: state
            .token_lifecycle
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, lifecycle)| (*token, lifecycle.clone()))
            .collect(),
        resolved_market_contexts: state
            .resolved_market_contexts
            .iter()
            .filter(|(market, _)| resolved_market_winners.contains_key(*market))
            .map(|(market, context)| (*market, context.clone()))
            .collect(),
        resolved_market_winners,
        polymarket_lifecycle: state.polymarket_lifecycle.clone(),
        selection: selection.cloned(),
        next_selection: next_selection.cloned(),
        markets,
        up: selection.and_then(|selection| state.books.get(&selection.up_token).cloned()),
        down: selection.and_then(|selection| state.books.get(&selection.down_token).cloned()),
        next_up: next_selection.and_then(|selection| state.books.get(&selection.up_token).cloned()),
        next_down: next_selection
            .and_then(|selection| state.books.get(&selection.down_token).cloned()),
        books: state
            .books
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, book)| (*token, book.clone()))
            .collect(),
        depths: state
            .depths
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, depth)| (*token, depth.clone()))
            .collect(),
        last_canonical_bids: state.last_canonical_bids.clone(),
        last_trades,
        next_last_trade_seq,
    }
}

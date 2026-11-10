use anyhow::{ensure, Result};
use std::path::Component;

use super::AppConfig;
use crate::config::bbwick::{
    canonical_oracle_symbol, canonical_quote_asset, canonical_underlying_key,
};

pub(super) fn validate_bbwick_oracle_config(cfg: &AppConfig) -> Result<()> {
    let active_symbols = cfg.bbwick_oracle.active_symbols();
    ensure!(
        !active_symbols.is_empty(),
        "bbwick_oracle.symbols must configure at least one enabled symbol"
    );
    ensure!(
        !canonical_quote_asset(&cfg.bbwick_oracle.quote_asset).is_empty(),
        "bbwick_oracle.quote_asset must not be empty"
    );
    ensure!(
        cfg.bbwick_oracle
            .symbols
            .keys()
            .all(|s| canonical_oracle_symbol(s).is_some()),
        "bbwick_oracle.symbols keys must be valid symbol strings (ASCII alnum, at least 3 chars)"
    );
    ensure!(
        cfg.bbwick_oracle
            .underlying_symbol_map
            .keys()
            .all(|u| canonical_underlying_key(u).is_some()),
        "bbwick_oracle.underlying_symbol_map keys must be non-empty underlying names"
    );
    ensure!(
        cfg.bbwick_oracle
            .underlying_symbol_map
            .values()
            .all(|s| canonical_oracle_symbol(s).is_some()),
        "bbwick_oracle.underlying_symbol_map values must be valid symbol strings"
    );
    ensure!(
        cfg.bbwick_oracle.runtime.ingest_channel_capacity > 0,
        "bbwick_oracle.runtime.ingest_channel_capacity must be > 0"
    );
    ensure!(
        cfg.bbwick_oracle.runtime.tick_channel_capacity > 0,
        "bbwick_oracle.runtime.tick_channel_capacity must be > 0"
    );
    ensure!(
        cfg.bbwick_oracle.history.max_bars > 0,
        "bbwick_oracle.history.max_bars must be > 0"
    );
    ensure!(
        !cfg.bbwick_oracle.history.data_root.as_os_str().is_empty(),
        "bbwick_oracle.history.data_root must not be empty"
    );
    ensure!(
        {
            let path = &cfg.bbwick_oracle.history.data_root;
            !path.as_os_str().is_empty()
                && !path.is_absolute()
                && path.components().all(|component| match component {
                    Component::CurDir | Component::Normal(_) => true,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_) => false,
                })
        },
        "bbwick_oracle.history.data_root must stay inside the Polymarket repo"
    );

    if !cfg.bbwick_oracle.enabled {
        return Ok(());
    }

    ensure!(
        cfg.bbwick_oracle.history.enabled,
        "bbwick_oracle.history.enabled must be true when bbwick_oracle.enabled is true"
    );

    let ws_url = cfg.bbwick_oracle.transport.ws_url.trim();
    let rest_base_url = cfg.bbwick_oracle.transport.rest_base_url.trim();
    let polymarket_ws_url = cfg.bbwick_oracle.transport.polymarket_ws_url.trim();
    let polymarket_clob_http_url = cfg.bbwick_oracle.transport.polymarket_clob_http_url.trim();
    let step_timeframes = &cfg.bbwick_oracle.transport.step_timeframes;
    ensure!(
        !ws_url.is_empty(),
        "bbwick_oracle.transport.ws_url must be set when enabled"
    );
    ensure!(
        !rest_base_url.is_empty(),
        "bbwick_oracle.transport.rest_base_url must be set when enabled"
    );
    ensure!(
        is_binance_spot_ws_url(ws_url),
        "bbwick_oracle.transport.ws_url must use Binance spot WebSocket host stream.binance.com"
    );
    ensure!(
        is_binance_spot_rest_url(rest_base_url),
        "bbwick_oracle.transport.rest_base_url must use Binance spot REST host api.binance.com"
    );
    ensure!(
        !polymarket_ws_url.is_empty(),
        "bbwick_oracle.transport.polymarket_ws_url must be set when enabled"
    );
    ensure!(
        !polymarket_clob_http_url.is_empty(),
        "bbwick_oracle.transport.polymarket_clob_http_url must be set when enabled"
    );
    ensure!(
        !step_timeframes.is_empty(),
        "bbwick_oracle.transport.step_timeframes must be set when enabled"
    );
    ensure!(
        step_timeframes
            .iter()
            .all(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1m" | "5m")),
        "bbwick_oracle.transport.step_timeframes must only contain 1m or 5m"
    );
    Ok(())
}

fn is_binance_spot_ws_url(raw: &str) -> bool {
    let Some((scheme, host)) = url_scheme_host(raw) else {
        return false;
    };
    matches!(scheme, "wss" | "ws") && host.eq_ignore_ascii_case("stream.binance.com")
}

fn is_binance_spot_rest_url(raw: &str) -> bool {
    let Some((scheme, host)) = url_scheme_host(raw) else {
        return false;
    };
    matches!(scheme, "https" | "http") && host.eq_ignore_ascii_case("api.binance.com")
}

fn url_scheme_host(raw: &str) -> Option<(&str, &str)> {
    let trimmed = raw.trim();
    let (scheme, rest) = trimmed.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?.trim();
    if scheme.is_empty() || authority.is_empty() {
        return None;
    }
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority)
        .split(':')
        .next()?
        .trim_matches(['[', ']']);
    (!host.is_empty()).then_some((scheme, host))
}

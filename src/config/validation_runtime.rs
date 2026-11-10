use anyhow::{ensure, Result};

use super::{AppConfig, PolymarketExecutionMode, PolymarketMarketDataMode};
use crate::config::bbwick::canonical_underlying_key;

pub(super) fn validate_runtime_config(cfg: &AppConfig) -> Result<()> {
    ensure!(
        !cfg.observability.enabled || !cfg.observability.logging.level.trim().is_empty(),
        "observability.logging.level must not be empty when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled || !cfg.observability.logging.dir.trim().is_empty(),
        "observability.logging.dir must not be empty when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled || cfg.observability.logging.action_log_flush_every > 0,
        "observability.logging.action_log_flush_every must be > 0 when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled || cfg.observability.logging.runtime_log_max_bytes > 0,
        "observability.logging.runtime_log_max_bytes must be > 0 when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || cfg.observability.logging.runtime_log_buffered_lines_limit > 0,
        "observability.logging.runtime_log_buffered_lines_limit must be > 0 when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled || cfg.observability.logging.action_log_max_bytes > 0,
        "observability.logging.action_log_max_bytes must be > 0 when observability.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || !cfg.observability.logging.flood_control_enabled
            || cfg.observability.logging.flood_summary_interval_secs > 0,
        "observability.logging.flood_summary_interval_secs must be > 0 when flood_control_enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || !cfg.observability.logging.flood_control_enabled
            || cfg.observability.logging.flood_recovery_timeout_secs > 0,
        "observability.logging.flood_recovery_timeout_secs must be > 0 when flood_control_enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || !cfg.observability.logging.flood_control_enabled
            || cfg.observability.logging.flood_max_tracked_signatures > 0,
        "observability.logging.flood_max_tracked_signatures must be > 0 when flood_control_enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || !cfg.observability.dashboard.enabled
            || !cfg.observability.dashboard.bind_host.trim().is_empty(),
        "observability.dashboard.bind_host must not be empty when observability.dashboard.enabled = true"
    );
    ensure!(
        !cfg.observability.enabled
            || !cfg.observability.dashboard.enabled
            || cfg.observability.dashboard.port > 0,
        "observability.dashboard.port must be > 0 when observability.dashboard.enabled = true"
    );

    if cfg.polymarket.market_data_enabled || cfg.polymarket.execution.mode.is_some() {
        validate_polymarket_integration(cfg)?;
    }
    ensure!(
        !cfg.polymarket.gamma_http_url.trim().is_empty(),
        "polymarket.gamma_http_url must not be empty"
    );
    ensure!(
        !cfg.polymarket.data_http_url.trim().is_empty(),
        "polymarket.data_http_url must not be empty"
    );
    if cfg.polymarket.discovery.enabled {
        validate_polymarket_discovery(cfg)?;
    }
    validate_polymarket_execution(cfg)?;
    validate_risk_config(cfg)?;
    validate_capture_config(cfg)?;
    Ok(())
}

fn validate_polymarket_integration(cfg: &AppConfig) -> Result<()> {
    ensure!(
        !cfg.polymarket.resolved_account_rpc_url().trim().is_empty(),
        "polymarket.account_rpc_url must not be empty when polymarket integration is enabled"
    );
    ensure!(
        !cfg.polymarket.clob_http_url.trim().is_empty(),
        "polymarket.clob_http_url must not be empty when polymarket integration is enabled"
    );
    let token_id = cfg.polymarket.token_id.as_deref().unwrap_or("").trim();

    if cfg.polymarket.market_data_enabled {
        match cfg.polymarket.market_data_mode {
            PolymarketMarketDataMode::Ws => {
                ensure!(
                    !cfg.polymarket.clob_ws_url.trim().is_empty(),
                    "polymarket.clob_ws_url must not be empty when polymarket.market_data_mode = \"ws\""
                );

                if !cfg.polymarket.discovery.enabled {
                    ensure!(
                        !token_id.is_empty(),
                        "polymarket.token_id must be set when discovery is disabled"
                    );
                }
            }
        }
    } else if cfg.polymarket.execution.mode.is_some() && !cfg.polymarket.discovery.enabled {
        ensure!(
            !token_id.is_empty(),
            "polymarket.token_id must be set when execution is enabled and discovery is disabled"
        );
    }
    Ok(())
}

fn validate_polymarket_discovery(cfg: &AppConfig) -> Result<()> {
    ensure!(
        !cfg.polymarket.discovery.query.trim().is_empty(),
        "polymarket.discovery.query must not be empty when discovery is enabled"
    );
    ensure!(
        cfg.polymarket.discovery.poll_interval_ms > 0,
        "polymarket.discovery.poll_interval_ms must be > 0 when discovery is enabled"
    );
    ensure!(
        cfg.polymarket.discovery.timeout_ms > 0,
        "polymarket.discovery.timeout_ms must be > 0 when discovery is enabled"
    );
    ensure!(
        cfg.polymarket.discovery.window_ms > 0,
        "polymarket.discovery.window_ms must be > 0 when discovery is enabled"
    );
    ensure!(
        cfg.polymarket.discovery.window_tolerance_ms <= cfg.polymarket.discovery.window_ms,
        "polymarket.discovery.window_tolerance_ms must be <= polymarket.discovery.window_ms"
    );
    ensure!(
        cfg.polymarket
            .discovery
            .slug_must_contain
            .iter()
            .all(|s| !s.trim().is_empty()),
        "polymarket.discovery.slug_must_contain entries must not be empty"
    );
    ensure!(
        cfg.polymarket
            .discovery
            .underlyings_allowlist
            .iter()
            .all(|s| !s.trim().is_empty()),
        "polymarket.discovery.underlyings_allowlist entries must not be empty"
    );
    ensure!(
        cfg.polymarket
            .discovery
            .underlyings_allowlist
            .iter()
            .all(|s| canonical_underlying_key(s).is_some()),
        "polymarket.discovery.underlyings_allowlist entries must be non-empty underlying identifiers"
    );
    ensure!(
        cfg.polymarket.discovery.min_time_left_ms <= cfg.polymarket.discovery.max_time_left_ms,
        "polymarket.discovery.min_time_left_ms must be <= polymarket.discovery.max_time_left_ms"
    );
    Ok(())
}

fn validate_polymarket_execution(cfg: &AppConfig) -> Result<()> {
    ensure!(
        cfg.polymarket.execution.mode == Some(PolymarketExecutionMode::Live),
        "polymarket.execution.mode must be set to \"live\""
    );
    ensure!(
        cfg.polymarket.chain_id > 0,
        "polymarket.chain_id must be > 0 when execution is enabled"
    );
    ensure!(
        !cfg.polymarket.private_key_env.trim().is_empty(),
        "polymarket.private_key_env must be set when execution mode is live"
    );
    ensure!(
        !cfg.polymarket
            .execution
            .order_submit_pia_region
            .trim()
            .is_empty(),
        "polymarket.execution.order_submit_pia_region must be set when execution mode is live"
    );
    Ok(())
}

fn validate_risk_config(cfg: &AppConfig) -> Result<()> {
    if cfg.risk.enabled {
        ensure!(
            cfg.risk.startup_cooldown_ms <= 10 * 60 * 1000,
            "risk.startup_cooldown_ms must be <= 600000ms (10 minutes)"
        );
        ensure!(
            cfg.risk.max_window_cost >= 0.0,
            "risk.max_window_cost must be >= 0"
        );
        ensure!(
            cfg.risk.max_market_equity_pct.is_finite()
                && (0.0..=1.0).contains(&cfg.risk.max_market_equity_pct),
            "risk.max_market_equity_pct must be finite and in [0, 1]"
        );
        ensure!(
            cfg.risk.max_drawdown_usd.is_finite() && cfg.risk.max_drawdown_usd >= 0.0,
            "risk.max_drawdown_usd must be finite and >= 0"
        );
        ensure!(
            cfg.risk.order_timeout_ms > 0,
            "risk.order_timeout_ms must be > 0 when risk.enabled = true"
        );
    }

    if cfg.observability.enabled && cfg.observability.logging.action_log_enabled {
        ensure!(
            !cfg.observability.logging.action_log_path.trim().is_empty(),
            "observability.logging.action_log_path must be set when action_log_enabled = true"
        );
    }
    Ok(())
}

fn validate_capture_config(cfg: &AppConfig) -> Result<()> {
    if cfg.capture.enabled {
        ensure!(
            cfg.capture.max_duration_ms > 0,
            "capture.max_duration_ms must be > 0"
        );
        ensure!(
            cfg.capture.min_save_duration_ms > 0,
            "capture.min_save_duration_ms must be > 0"
        );
        ensure!(
            cfg.capture.min_save_duration_ms <= cfg.capture.max_duration_ms,
            "capture.min_save_duration_ms must be <= capture.max_duration_ms"
        );
        ensure!(
            cfg.capture.max_sessions > 0,
            "capture.max_sessions must be > 0"
        );
    }
    Ok(())
}

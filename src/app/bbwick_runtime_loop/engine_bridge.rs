use std::sync::Arc;

use anyhow::Result;
use bbwick_engine::OracleEngine;
use polymarket_hft::capture::events::{signed_delay_ms, PolymarketMarketWsIn};
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::{AppConfig, SharedStrategyConfig};
use polymarket_hft::observability::Observability;
use polymarket_hft::polymarket::engine_bridge::EnginePolymarketBridge;
use polymarket_hft::portfolio::PortfolioState;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(super) fn spawn_engine_state_forwarding(
    started: &OracleEngine,
    engine_state_tx: Option<watch::Sender<Arc<bbwick_engine::PolymarketOddsState>>>,
    market_projection_tx: Option<
        watch::Sender<polymarket_hft::polymarket::state::MarketProjection>,
    >,
    obs: &Observability,
    capture: &MaybeCapture,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    if engine_state_tx.is_none() && market_projection_tx.is_none() {
        return None;
    }
    let mut engine_state_rx = started.subscribe_polymarket_state();
    let observed_at_ms = polymarket_hft::event_ledger::now_ms();
    let initial_state = Arc::new(engine_state_rx.borrow().clone());
    let initial_projection =
        polymarket_hft::polymarket::state::MarketProjection::from_odds(initial_state.as_ref());
    if let Some(market_projection_tx) = market_projection_tx.clone() {
        market_projection_tx.send_replace(initial_projection.clone());
    }
    if let Some(engine_state_tx) = engine_state_tx.clone() {
        engine_state_tx.send_replace(Arc::clone(&initial_state));
    }
    obs.set_market_universe(initial_projection.markets());
    emit_engine_state_capture(capture, initial_state, observed_at_ms);
    let obs_forward = obs.clone();
    let capture_forward = capture.clone();
    let engine_state_tx_forward = engine_state_tx.clone();
    let market_projection_tx_forward = market_projection_tx.clone();
    Some(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = engine_state_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let observed_at_ms = polymarket_hft::event_ledger::now_ms();
                    let next_state = Arc::new(engine_state_rx.borrow_and_update().clone());
                    let next_projection =
                        polymarket_hft::polymarket::state::MarketProjection::from_odds(
                            next_state.as_ref(),
                        );
                    if let Some(market_projection_tx) = market_projection_tx_forward.as_ref() {
                        market_projection_tx.send_replace(next_projection.clone());
                    }
                    if let Some(engine_state_tx) = engine_state_tx_forward.as_ref() {
                        engine_state_tx.send_replace(Arc::clone(&next_state));
                    }
                    obs_forward.set_market_universe(next_projection.markets());
                    emit_engine_state_capture(&capture_forward, next_state, observed_at_ms);
                }
            }
        }
    }))
}

fn emit_engine_state_capture(
    capture: &MaybeCapture,
    state: Arc<bbwick_engine::PolymarketOddsState>,
    observed_at_ms: u64,
) {
    let event_ts_ms = if state.updated_at_ms == 0 {
        observed_at_ms
    } else {
        state.updated_at_ms
    };
    capture.emit_engine_state(PolymarketMarketWsIn {
        session_ts_ms: observed_at_ms,
        channel: "engine_state".to_string(),
        asset_id: String::new(),
        event_ts_ms,
        recv_ts_ms: observed_at_ms,
        ingest_delay_ms: signed_delay_ms(observed_at_ms, event_ts_ms),
        payload: state,
    });
}

pub(super) fn start_polymarket_bridge(
    cfg: &AppConfig,
    capture_underlyings: &[String],
    shared_strategy_cfg: SharedStrategyConfig,
    obs: Observability,
    shutdown: CancellationToken,
    started: &OracleEngine,
    portfolio_rx: watch::Receiver<PortfolioState>,
    discovery_http: reqwest::Client,
    capture: MaybeCapture,
) -> Result<EnginePolymarketBridge> {
    let mut polymarket_cfg = cfg.polymarket.clone();
    if cfg.capture.enabled {
        polymarket_cfg.discovery.underlyings_allowlist = capture_underlyings
            .iter()
            .map(|symbol| symbol.to_ascii_lowercase())
            .collect();
    }
    EnginePolymarketBridge::start(
        polymarket_cfg,
        shared_strategy_cfg,
        obs,
        shutdown,
        started.discovery_controller(),
        started.subscribe_polymarket_state(),
        portfolio_rx,
        discovery_http,
        capture,
    )
}

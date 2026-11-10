use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;
use std::collections::HashMap;
use tokio::sync::{broadcast, mpsc, watch};

use crate::live_lanes::{OracleTapeLane, PolymarketStateLane, ProviderQuoteLane};
use crate::market_data::CanonicalTopOfBook;
use crate::metrics::MetricsState;
use crate::oracle::{
    OracleEngineConfig, OracleEngineParts, OracleTick,
    PolymarketDiscoveryController,
};
use crate::oracle_history::{
    ensure_history_windows, spawn_history_runtime, HistoryRuntimeConfig, OracleHistory,
};
use crate::oracle_ingest::{spawn_ingest_runtime, OracleIngestRuntime};
use crate::polymarket_ws::{spawn_polymarket_ws_runtime, PolymarketWsRuntimeConfig};
use crate::provider_ws::{spawn_provider_ws_runtime, ProviderWsRuntimeConfig};
use crate::state::{DiscoverySnapshot, LastCanonicalBid, PolymarketStateStore};
use crate::wire::StepFrame;

pub(crate) fn start_engine_runtime(
    config: OracleEngineConfig,
    retained_bids: HashMap<U256, LastCanonicalBid>,
    history: Arc<OracleHistory>,
) -> Result<OracleEngineParts> {
    config
        .transport
        .validate()
        .context("bbwick engine transport config invalid")?;
    anyhow::ensure!(
        config.history.enabled,
        "bbwick engine history must be enabled"
    );

    let active_symbols = canonical_active_symbols(config.active_symbols)?;
    let market_data_ws_url = required_url(
        config.transport.market_data_ws_url(),
        "bbwick market-data ws_url missing",
    )?;
    let polymarket_ws_url = required_url(
        config.transport.polymarket_ws_url(),
        "bbwick polymarket ws_url missing",
    )?;
    let polymarket_clob_http_url = required_url(
        config.transport.polymarket_clob_http_url(),
        "bbwick polymarket clob_http_url missing",
    )?;
    let market_data_rest_base_url = required_url(
        config.transport.market_data_rest_base_url(),
        "bbwick market-data rest_base_url missing",
    )?;
    let step_timeframes = config.transport.step_timeframes();
    let key_registry = history.key_registry();
    anyhow::ensure!(
        key_registry.symbol_count() == active_symbols.len()
            && active_symbols.iter().all(|symbol| key_registry.symbol_id(symbol).is_some()),
        "retained OHLC symbol scope differs from engine config"
    );
    anyhow::ensure!(
        key_registry.timeframe_count() == step_timeframes.len()
            && step_timeframes.iter().all(|timeframe| key_registry.timeframe_id(timeframe).is_some()),
        "retained OHLC timeframe scope differs from engine config"
    );

    let ingest_capacity = config.ingest_channel_capacity.max(1);
    let tick_capacity = config.tick_channel_capacity.max(1);

    let (ingest_tx, ingest_rx) = mpsc::channel(ingest_capacity);
    let tape_lane = OracleTapeLane::new(ingest_tx);
    let ingest_mono_origin = history.monotonic_origin();

    let (tick_tx, _) = broadcast::channel::<OracleTick>(tick_capacity);
    let (top_of_book_tx, top_of_book_rx) = broadcast::channel::<CanonicalTopOfBook>(tick_capacity);
    let (step_tx, _) = watch::channel(StepFrame::default());
    let (store, initial_state) = PolymarketStateStore::with_retained_canonical_bids(retained_bids);
    let (polymarket_state_tx, _) = watch::channel(initial_state);
    let state_store = Arc::new(Mutex::new(store));
    let provider_quote_lane = ProviderQuoteLane::new(
        key_registry.clone(),
        top_of_book_tx.clone(),
        ingest_mono_origin,
    );
    let polymarket_state_lane =
        PolymarketStateLane::new(state_store.clone(), polymarket_state_tx.clone());

    let provider_task = spawn_provider_ws_runtime(
        ProviderWsRuntimeConfig {
            ws_url: market_data_ws_url,
            key_registry: key_registry.clone(),
            history: history.clone(),
            subscribe_trades: config.transport.provider_subscribe_trades(),
            subscribe_top_of_book: config.transport.provider_subscribe_top_of_book(),
            subscribe_klines: config.transport.provider_subscribe_klines(),
            capture: config.binance_provider_capture.clone(),
        },
        provider_quote_lane,
        tape_lane.clone(),
    );

    let (polymarket_control_tx, polymarket_control_rx) = mpsc::channel::<DiscoverySnapshot>(64);
    let (polymarket_task, polymarket_shutdown_tx) = spawn_polymarket_ws_runtime(
        PolymarketWsRuntimeConfig {
            ws_url: polymarket_ws_url,
            clob_http_url: polymarket_clob_http_url,
            use_server_time: config.transport.polymarket_use_server_time(),
            subscribe_orderbook: config.transport.polymarket_subscribe_orderbook(),
            subscribe_prices: config.transport.polymarket_subscribe_prices(),
            subscribe_tick_size: config.transport.polymarket_subscribe_tick_size(),
            subscribe_last_trade: config.transport.polymarket_subscribe_last_trade(),
            ingress_channel_capacity: ingest_capacity,
            ingress_capture: config.polymarket_ws_ingress_capture.clone(),
            book_admission_capture: config.polymarket_book_admission_capture.clone(),
        },
        polymarket_state_lane.clone(),
        polymarket_control_rx,
    );

    let history_task = Some(spawn_history_runtime(
        HistoryRuntimeConfig {
            data_root: config.history.data_root,
            max_bars: config.history.max_bars.max(1),
            rest_base_url: market_data_rest_base_url,
            active_symbols,
            step_timeframes,
        },
        history.clone(),
        polymarket_shutdown_tx.subscribe(),
    ));

    let metrics = Arc::new(MetricsState::default());
    let polymarket_controller = PolymarketDiscoveryController::new(
        polymarket_control_tx,
        state_store.clone(),
        polymarket_state_tx.clone(),
    );

    let ingest_task = spawn_ingest_runtime(OracleIngestRuntime {
        ingest_rx,
        tick_tx: tick_tx.clone(),
        step_tx: step_tx.clone(),
        polymarket_state_tx: polymarket_state_tx.clone(),
        state_store,
        key_registry,
        history: history.clone(),
    });

    Ok(OracleEngineParts {
        history,
        metrics,
        ingest_mono_origin,
        tick_tx,
        top_of_book_rx,
        step_tx,
        polymarket_state_tx,
        polymarket_controller,
        provider_task,
        polymarket_task,
        polymarket_shutdown_tx,
        ingest_task,
        history_task,
    })
}

pub async fn ensure_engine_history_ready(
    config: &OracleEngineConfig,
) -> Result<Arc<OracleHistory>> {
    config
        .transport
        .validate()
        .context("bbwick engine transport config invalid")?;
    anyhow::ensure!(
        config.history.enabled,
        "bbwick engine history must be enabled"
    );

    let active_symbols = canonical_active_symbols(config.active_symbols.clone())?;
    let market_data_rest_base_url = required_url(
        config.transport.market_data_rest_base_url(),
        "bbwick market-data rest_base_url missing",
    )?;
    let step_timeframes = config.transport.step_timeframes();
    let runtime = HistoryRuntimeConfig {
        data_root: config.history.data_root.clone(),
        max_bars: config.history.max_bars.max(1),
        rest_base_url: market_data_rest_base_url,
        active_symbols,
        step_timeframes,
    };
    ensure_history_windows(&runtime).await
}

fn canonical_active_symbols(mut active_symbols: Vec<String>) -> Result<Vec<String>> {
    active_symbols = active_symbols
        .into_iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect();
    active_symbols.sort();
    active_symbols.dedup();
    if active_symbols.is_empty() {
        anyhow::bail!("bbwick engine requires at least one active symbol");
    }
    Ok(active_symbols)
}

fn required_url(value: Option<&str>, missing_msg: &str) -> Result<String> {
    value
        .map(str::to_string)
        .filter(|next| !next.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!(missing_msg.to_string()))
}

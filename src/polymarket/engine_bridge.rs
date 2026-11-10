mod resolution;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result};
use bbwick_engine::{
    DiscoverySnapshot as EngineDiscoverySnapshot, MarketSelectionSnapshot as EngineSelection,
    PolymarketDiscoveryController,
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use polymarket_client_sdk::types::{B256, U256};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capture::events::PolymarketDiscoverySnapshotCapture;
use crate::capture::session::{CaptureMsg, MaybeCapture};
use crate::config::{PolymarketConfig, SharedStrategyConfig};
use crate::observability::action::ActionEvent;
use crate::observability::Observability;
use crate::polymarket::discovery::{
    discover_enabled_updown_markets, discovery_query_underlyings, order_market_selections,
    MarketSelection, TokenLifecycleState,
};
use crate::polymarket::state::{convert_selection_snapshot, parse_token_lifecycle_state};
use crate::portfolio::PortfolioState;
use resolution::{
    accept_market_resolution, fetch_market_resolution, track_discovered_markets,
    TrackedResolutionMarket,
};

#[derive(Debug)]
pub struct EnginePolymarketBridge {
    discovery_task: JoinHandle<()>,
    discovery_shutdown: CancellationToken,
    exit_rx: watch::Receiver<Option<String>>,
}

impl EnginePolymarketBridge {
    pub fn start(
        cfg: PolymarketConfig,
        shared_strategy_cfg: SharedStrategyConfig,
        obs: Observability,
        shutdown: CancellationToken,
        controller: PolymarketDiscoveryController,
        engine_rx: watch::Receiver<bbwick_engine::PolymarketOddsState>,
        portfolio_rx: watch::Receiver<PortfolioState>,
        discovery_http: reqwest::Client,
        capture: MaybeCapture,
    ) -> Result<Self> {
        let mut discovery_engine_rx = engine_rx.clone();
        let mut discovery_portfolio_rx = portfolio_rx;
        let (exit_tx, exit_rx) = watch::channel(None);
        let discovery_shutdown = shutdown.child_token();

        let discovery_task = tokio::spawn({
            let obs = obs.clone();
            let shutdown = discovery_shutdown.clone();
            async move {
                let exit_error = match run_discovery_bridge(
                    cfg,
                    shared_strategy_cfg,
                    obs,
                    controller,
                    discovery_http,
                    &mut discovery_engine_rx,
                    &mut discovery_portfolio_rx,
                    capture,
                    shutdown.clone(),
                )
                .await
                {
                    Ok(()) => None,
                    Err(err) => Some(err.to_string()),
                };
                if let Some(err) = exit_error {
                    tracing::error!(
                        target: "polymarket.engine_bridge",
                        error = %err,
                        "bbwick_polymarket_discovery_bridge_exited"
                    );
                    let _ = exit_tx.send(Some(err));
                }
            }
        });

        Ok(Self {
            discovery_task,
            discovery_shutdown,
            exit_rx,
        })
    }

    pub async fn shutdown(self) {
        self.discovery_shutdown.cancel();
        let _ = self.discovery_task.await;
    }

    pub async fn exit_changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.exit_rx.changed().await
    }

    pub fn exit_error(&self) -> Option<String> {
        self.exit_rx.borrow().clone()
    }
}

async fn run_discovery_bridge(
    cfg: PolymarketConfig,
    shared_strategy_cfg: SharedStrategyConfig,
    obs: Observability,
    controller: PolymarketDiscoveryController,
    discovery_http: reqwest::Client,
    engine_rx: &mut watch::Receiver<bbwick_engine::PolymarketOddsState>,
    portfolio_rx: &mut watch::Receiver<PortfolioState>,
    capture: MaybeCapture,
    shutdown: CancellationToken,
) -> Result<()> {
    if !cfg.discovery.enabled {
        let asset_ids = static_asset_ids(&cfg)?;
        let snapshot = EngineDiscoverySnapshot {
            asset_ids,
            ..Default::default()
        };
        capture.emit(CaptureMsg::PolymarketDiscoverySnapshot(
            PolymarketDiscoverySnapshotCapture {
                session_ts_ms: now_ms(),
                snapshot: snapshot.clone(),
            },
        ));
        controller.update(snapshot).await?;
        shutdown.cancelled().await;
        return Ok(());
    }

    let mut tick = tokio::time::interval(Duration::from_millis(
        cfg.discovery.poll_interval_ms.max(150),
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut resolution_tick = tokio::time::interval(Duration::from_secs(1));
    resolution_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pending_resolutions = BTreeMap::<B256, TrackedResolutionMarket>::new();
    let mut discovery_requests = FuturesUnordered::new();
    let mut active_discoveries = HashSet::new();
    let mut resolution_requests = FuturesUnordered::new();
    let mut active_resolutions = HashSet::new();
    let mut last_selection: Option<String> = None;
    let mut last_next_selection: Option<String> = None;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = resolution_tick.tick() => {
                let ts = now_ms();
                for tracked in pending_resolutions.values() {
                    if ts < tracked.window_end_ms
                        || !active_resolutions.insert(tracked.condition_id)
                    {
                        continue;
                    }
                    resolution_requests.push(fetch_market_resolution(
                        tracked.clone(),
                        &cfg.clob_http_url,
                        cfg.discovery.timeout_ms,
                        &discovery_http,
                    ));
                }
            }
            Some((tracked, response)) = resolution_requests.next(), if !resolution_requests.is_empty() => {
                active_resolutions.remove(&tracked.condition_id);
                accept_market_resolution(
                    &mut pending_resolutions,
                    tracked,
                    response,
                    &controller,
                    &capture,
                )?;
            }
            _ = tick.tick() => {
                let enabled_assets = shared_strategy_cfg.borrow().enabled_assets.clone();
                for underlying in discovery_query_underlyings(&enabled_assets, &cfg) {
                    if !active_discoveries.insert(underlying.clone()) {
                        continue;
                    }
                    let cfg = &cfg;
                    let http = &discovery_http;
                    discovery_requests.push(async move {
                        let result = tokio::time::timeout(
                            Duration::from_millis(cfg.discovery.timeout_ms.max(1_000)),
                            discover_enabled_updown_markets(
                                cfg,
                                std::slice::from_ref(&underlying),
                                http,
                            ),
                        )
                        .await;
                        (underlying, result)
                    });
                }
            }
            Some((underlying, result)) = discovery_requests.next(), if !discovery_requests.is_empty() => {
                active_discoveries.remove(&underlying);
                let enabled_assets = shared_strategy_cfg.borrow().enabled_assets.clone();
                let enabled_underlyings = discovery_query_underlyings(&enabled_assets, &cfg);
                if !enabled_underlyings.contains(&underlying) {
                    continue;
                }
                match result {
                    Ok(Ok(mut discovered_markets)) => {
                        discovered_markets.retain(|market| market.underlying == underlying);
                        track_discovered_markets(
                            &mut pending_resolutions,
                            &discovered_markets,
                        )?;
                        let inventory_tokens = inventory_asset_ids(portfolio_rx);
                        let ts = now_ms();
                        let mut markets = {
                            let engine_state = engine_rx.borrow();
                            discovered_markets.extend(
                                engine_state.markets.iter()
                                    .filter(|market| {
                                        market.underlying != underlying
                                            && enabled_underlyings.contains(&market.underlying)
                                            && ts < market.window_end_ms
                                    })
                                    .map(convert_selection_snapshot),
                            );
                            let mut markets = annotate_markets_with_lifecycle(
                                discovered_markets,
                                &engine_state.token_lifecycle,
                                &inventory_tokens,
                            );
                            append_observation_required_markets(
                                &mut markets,
                                &engine_state,
                                &inventory_tokens,
                                ts,
                            );
                            markets
                        };
                        if let Some(token_id) = inventory_tokens.iter().find(|token_id| {
                            !markets.iter().any(|market| {
                                market.up_token == **token_id || market.down_token == **token_id
                            })
                        }) {
                            tracing::error!(
                                target: "polymarket.engine_bridge",
                                token_id = %token_id,
                                "inventory_token_missing_symbol_mapping"
                            );
                            continue;
                        }
                        order_market_selections(&mut markets, ts);
                        let engine_snapshot = build_engine_discovery_snapshot(
                            &markets,
                            desired_asset_ids(&markets, &inventory_tokens),
                            ts,
                        );
                        let selection = engine_snapshot.selection.clone();
                        let next_selection = engine_snapshot.next_selection.clone();
                        // Capture the snapshot exactly as published: replay cannot admit a
                        // book frame without the token to market mapping this carries.
                        capture.emit(CaptureMsg::PolymarketDiscoverySnapshot(
                            PolymarketDiscoverySnapshotCapture {
                                session_ts_ms: now_ms(),
                                snapshot: engine_snapshot.clone(),
                            },
                        ));
                        controller.update(engine_snapshot).await?;
                        if markets.is_empty() {
                            tracing::info!(target: "polymarket.engine_bridge", "idle_no_markets");
                        }

                        if let Some(sel) = selection.as_ref() {
                            let changed = last_selection.as_deref() != Some(sel.market_id.as_str());
                            if changed {
                                last_selection = Some(sel.market_id.clone());
                                obs.emit_action(ActionEvent::MarketSelected {
                                    ts_ms: now_ms(),
                                    condition_id: sel.condition_id.clone().into(),
                                    up_token: sel.up_token.to_string(),
                                    down_token: sel.down_token.to_string(),
                                    window_start_ms: sel.window_start_ms,
                                    window_end_ms: sel.window_end_ms,
                                    question: sel.question.clone(),
                                    slug: sel.slug.clone(),
                                    underlying: Some(sel.underlying.clone()),
                                });
                            }
                        }
                        if let Some(sel) = next_selection.as_ref() {
                            let changed =
                                last_next_selection.as_deref() != Some(sel.market_id.as_str());
                            if changed {
                                last_next_selection = Some(sel.market_id.clone());
                                obs.emit_action(ActionEvent::NextMarketSelected {
                                    ts_ms: now_ms(),
                                    condition_id: sel.condition_id.clone().into(),
                                    up_token: sel.up_token.to_string(),
                                    down_token: sel.down_token.to_string(),
                                    window_start_ms: sel.window_start_ms,
                                    window_end_ms: sel.window_end_ms,
                                    question: sel.question.clone(),
                                    slug: sel.slug.clone(),
                                    underlying: Some(sel.underlying.clone()),
                                });
                            }
                        } else if last_next_selection.take().is_some() {
                            obs.emit_action(ActionEvent::NextMarketCleared { ts_ms: now_ms() });
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::error!(target: "polymarket.engine_bridge", underlying, error = %err, "discovery_failed");
                    }
                    Err(_) => {
                        tracing::error!(
                            target: "polymarket.engine_bridge",
                            underlying,
                            timeout_ms = cfg.discovery.timeout_ms.max(1_000),
                            "discovery_timeout"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn desired_asset_ids(markets: &[MarketSelection], inventory_tokens: &[U256]) -> Vec<U256> {
    let mut ids = Vec::with_capacity(markets.len().saturating_mul(2));
    for market in markets {
        ids.push(market.up_token);
        ids.push(market.down_token);
    }
    for token_id in inventory_tokens {
        ids.push(*token_id);
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn inventory_asset_ids(portfolio_rx: &watch::Receiver<PortfolioState>) -> Vec<U256> {
    let mut ids = Vec::new();
    for token_str in portfolio_rx.borrow().positions.keys() {
        if let Ok(token_id) = U256::from_str(token_str) {
            ids.push(token_id);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn annotate_markets_with_lifecycle(
    markets: Vec<MarketSelection>,
    token_lifecycle: &HashMap<U256, String>,
    inventory_tokens: &[U256],
) -> Vec<MarketSelection> {
    let inventory_set = inventory_tokens.iter().copied().collect::<HashSet<_>>();
    markets
        .into_iter()
        .filter_map(|mut market| {
            market.up_lifecycle = lifecycle_state_for(token_lifecycle, market.up_token);
            market.down_lifecycle = lifecycle_state_for(token_lifecycle, market.down_token);
            let has_inventory = inventory_set.contains(&market.up_token)
                || inventory_set.contains(&market.down_token);
            if market_is_quarantined(&market) && !has_inventory {
                return None;
            }
            Some(market)
        })
        .collect()
}

fn market_is_quarantined(market: &MarketSelection) -> bool {
    matches!(
        market.up_lifecycle,
        TokenLifecycleState::InvalidAfter404 | TokenLifecycleState::Resolved
    ) || matches!(
        market.down_lifecycle,
        TokenLifecycleState::InvalidAfter404 | TokenLifecycleState::Resolved
    )
}

fn append_observation_required_markets(
    markets: &mut Vec<MarketSelection>,
    state: &bbwick_engine::PolymarketOddsState,
    inventory_tokens: &[U256],
    now_ms: u64,
) {
    let inventory_set = inventory_tokens.iter().copied().collect::<HashSet<_>>();
    let mut seen_market_ids = markets
        .iter()
        .map(|market| market.market_id.to_string())
        .collect::<HashSet<_>>();

    for snapshot in state
        .markets
        .iter()
        .chain(state.selection.iter())
        .chain(state.next_selection.iter())
    {
        let mut market = convert_selection_snapshot(snapshot);
        market.up_lifecycle = lifecycle_state_for(&state.token_lifecycle, market.up_token);
        market.down_lifecycle = lifecycle_state_for(&state.token_lifecycle, market.down_token);
        let has_inventory =
            inventory_set.contains(&market.up_token) || inventory_set.contains(&market.down_token);
        if now_ms >= market.window_end_ms && !has_inventory {
            continue;
        }
        let observation_required = market.window_start_ms <= now_ms;
        if !has_inventory && (!observation_required || market_is_quarantined(&market)) {
            continue;
        }
        if !seen_market_ids.insert(market.market_id.to_string()) {
            continue;
        }
        markets.push(market);
    }
}

fn static_asset_ids(cfg: &PolymarketConfig) -> Result<Vec<U256>> {
    let token_id_raw = cfg.token_id.as_deref().unwrap_or("").trim();
    if token_id_raw.is_empty() {
        return Ok(Vec::new());
    }
    let token_id = U256::from_str(token_id_raw)
        .with_context(|| format!("invalid polymarket.token_id value `{token_id_raw}`"))?;
    Ok(vec![token_id])
}

fn lifecycle_state_for(
    token_lifecycle: &HashMap<U256, String>,
    token_id: U256,
) -> TokenLifecycleState {
    token_lifecycle
        .get(&token_id)
        .map(|raw| parse_token_lifecycle_state(raw))
        .unwrap_or(TokenLifecycleState::PendingRevalidation)
}

fn to_engine_selection(selection: &MarketSelection) -> EngineSelection {
    EngineSelection {
        market_id: selection.market_id.to_string(),
        condition_id: selection.condition_id.to_string(),
        gamma_market_id: selection.gamma_market_id.clone(),
        condition_id_b256: selection.condition_id_b256.map(|v| v.to_string()),
        underlying: selection.underlying.clone(),
        up_token: selection.up_token,
        down_token: selection.down_token,
        window_start_ms: selection.window_start_ms,
        window_end_ms: selection.window_end_ms,
        maker_base_fee_bps: selection.maker_base_fee_bps,
        taker_base_fee_bps: selection.taker_base_fee_bps,
        minimum_tick_size: selection.minimum_tick_size,
        minimum_order_size: selection.minimum_order_size,
        rewards_min_size: selection.rewards_min_size,
        rewards_max_spread: selection.rewards_max_spread,
        resolution_source: selection.resolution_source.clone(),
        neg_risk: selection.neg_risk,
        itode: selection.itode,
        standard_binary_ctf: selection.standard_binary_ctf,
        question: selection.question.clone(),
        slug: selection.slug.clone(),
        up_lifecycle: selection.up_lifecycle.as_str().to_string(),
        down_lifecycle: selection.down_lifecycle.as_str().to_string(),
        market_frame_received_at: None,
        market_state_published_at: None,
        market_connection_session_id: None,
        market_frame_sequence: None,
        market_state_revision: None,
    }
}

fn build_engine_discovery_snapshot(
    markets: &[MarketSelection],
    asset_ids: Vec<U256>,
    ts: u64,
) -> EngineDiscoverySnapshot {
    let sel_idx = markets
        .iter()
        .position(|market| market.window_start_ms <= ts && ts < market.window_end_ms)
        .or_else(|| markets.iter().position(|market| ts < market.window_end_ms));
    let next_idx = markets
        .iter()
        .enumerate()
        .filter(|(_, market)| market.window_start_ms > ts)
        .min_by_key(|(_, market)| market.window_start_ms)
        .map(|x| x.0);
    let markets: Vec<EngineSelection> = markets.iter().map(to_engine_selection).collect();
    EngineDiscoverySnapshot {
        selection: sel_idx.and_then(|idx| markets.get(idx).cloned()),
        next_selection: next_idx.and_then(|idx| markets.get(idx).cloned()),
        markets,
        asset_ids,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

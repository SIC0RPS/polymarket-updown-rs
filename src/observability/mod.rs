pub mod action;
mod action_log;
mod event_filter;
#[path = "../dashboard/mod.rs"]
pub(crate) mod operator_surface_ui;
mod parsers;
mod runtime;
mod runtime_state;
pub mod strategy_status;
pub mod ui;
mod ui_projection;

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::ObservabilityConfig;
use crate::event_ledger::now_ms;
use crate::polymarket::identity::MarketId;
use crate::telemetry::{self, FloodControlSnapshot, TelemetryGuards};

use self::action::ActionEvent;
use self::event_filter::should_log_action_event;
pub use self::runtime::{ObservabilityOperatorSurfaceStart, ObservabilityRuntime};
pub use self::runtime_state::RuntimeState;
use self::ui::{
    CexQuoteSnapshot, ErrorSnapshot, LocalOrderBlockSnapshot, MarketSnapshot, OptimizationSnapshot,
    OracleSnapshot, OrderAckSnapshot, OrderIntentSnapshot, PostOnlyRejectStatusSnapshot,
    RecentExitExecutionSnapshot, RecentRiskBlockSnapshot, SignalSnapshot, StrategyStatusSnapshot,
    UiState,
};
use self::ui_projection::{asset_label_for_closed_trade, prune_ui_market_cache};

#[derive(Clone)]
pub struct Observability {
    actions_tx: mpsc::Sender<ActionEvent>,
    ui_state_tx: watch::Sender<UiState>,
    runtime_state_tx: watch::Sender<RuntimeState>,
    live_account_refresh_seq_tx: watch::Sender<u64>,
    /// Monotonically increasing sequence number; PortfolioRuntime watches this
    /// and resets its state whenever the value changes.
    portfolio_reset_tx: watch::Sender<u64>,
    shutdown: CancellationToken,
    filter_reload: tracing_subscriber::reload::Handle<
        tracing_subscriber::EnvFilter,
        tracing_subscriber::Registry,
    >,
    flood_control: telemetry::FloodControlHandle,
    action_log_enabled: bool,
    action_log_log_ticks: bool,
}

impl Observability {
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn ui_state(&self) -> watch::Receiver<UiState> {
        self.ui_state_tx.subscribe()
    }

    pub fn runtime_state(&self) -> watch::Receiver<RuntimeState> {
        self.runtime_state_tx.subscribe()
    }

    /// Subscribe to portfolio reset signals. PortfolioRuntime watches this
    /// and clears its state whenever the sequence number advances.
    pub fn portfolio_reset_signal(&self) -> watch::Receiver<u64> {
        self.portfolio_reset_tx.subscribe()
    }

    pub fn live_account_refresh_signal(&self) -> watch::Receiver<u64> {
        self.live_account_refresh_seq_tx.subscribe()
    }

    pub fn set_market_data_source(&self, source: impl Into<String>) {
        let source = source.into();
        self.ui_state_tx
            .send_modify(|s| s.market_data_source = source.clone());
        self.runtime_state_tx
            .send_modify(|s| s.market_data_source = source.clone());
    }

    pub fn set_execution_backend(&self, backend: impl Into<String>) {
        let backend = backend.into();
        self.ui_state_tx
            .send_modify(|s| s.execution_backend = backend.clone());
        self.runtime_state_tx
            .send_modify(|s| s.execution_backend = backend.clone());
    }

    pub fn flood_control_snapshot(&self) -> FloodControlSnapshot {
        self.flood_control.snapshot()
    }

    pub fn set_portfolio(
        &self,
        portfolio: crate::portfolio::PortfolioState,
        account: Option<&crate::risk::AccountEquitySnapshot>,
        account_updated_at_ms: Option<u64>,
    ) {
        let runtime_portfolio = portfolio.clone();
        self.ui_state_tx.send_modify(|s| {
            // Incrementally update per-asset stats from new closed trades.
            let last_seq = s.last_asset_stats_seq;
            for trade in portfolio.closed_trades.iter() {
                if trade.seq <= last_seq {
                    continue;
                }
                let label = asset_label_for_closed_trade(s, trade);
                let entry = s.asset_stats.entry(label).or_default();
                entry.trades += 1;
                s.last_asset_stats_seq = trade.seq;
            }
            s.risk.wallet_usdc_balance = account
                .and_then(|account| account.wallet_usdc_balance)
                .map(|balance| balance.as_f64());
            s.risk.wallet_matic_balance = account.and_then(|account| account.wallet_matic_balance);
            s.risk.marked_positions_value = account
                .and_then(|account| account.marked_positions_value)
                .map(|value| value.as_f64());
            s.risk.current_wallet_equity = account
                .and_then(|account| account.current_wallet_equity)
                .map(|equity| equity.as_f64());
            s.risk.trading_collateral_balance = account
                .and_then(|account| account.trading_collateral_balance)
                .map(|balance| balance.as_f64());
            s.risk.account_equity_updated_at_ms = account_updated_at_ms;
            s.wallet_observed_at_ms = account.map(|account| account.source_ts_ms);
            s.portfolio = portfolio;
        });
        self.runtime_state_tx
            .send_modify(|s| s.portfolio = runtime_portfolio);
    }

    pub fn set_optimization_snapshot(&self, snapshot: OptimizationSnapshot) {
        self.ui_state_tx
            .send_modify(|s| s.optimization = Some(snapshot.clone()));
    }

    pub fn set_enabled_assets(&self, assets: Vec<String>) {
        self.ui_state_tx.send_modify(|s| s.enabled_assets = assets);
    }

    pub fn set_open_orders(&self, open_orders: crate::orders::OpenOrdersState) {
        self.ui_state_tx
            .send_modify(|s| s.open_orders = open_orders.clone());
        self.runtime_state_tx
            .send_modify(|s| s.open_orders = open_orders);
    }

    pub fn set_runtime_managed_paired_tokens(&self, tokens: std::collections::HashSet<String>) {
        self.runtime_state_tx
            .send_modify(|s| s.runtime_managed_paired_tokens = tokens);
    }

    pub fn set_risk(&self, risk: crate::risk::RiskSnapshot) {
        self.ui_state_tx.send_modify(|s| {
            let mut ui_risk = risk.clone();
            ui_risk.wallet_usdc_balance = s.risk.wallet_usdc_balance;
            ui_risk.wallet_matic_balance = s.risk.wallet_matic_balance;
            ui_risk.marked_positions_value = s.risk.marked_positions_value;
            ui_risk.current_wallet_equity = s.risk.current_wallet_equity;
            ui_risk.trading_collateral_balance = s.risk.trading_collateral_balance;
            ui_risk.account_equity_updated_at_ms = s.risk.account_equity_updated_at_ms;
            s.risk = ui_risk;
        });
        self.runtime_state_tx.send_modify(|s| s.risk = risk);
    }

    pub fn set_post_only_reject_status(&self, status: Option<PostOnlyRejectStatusSnapshot>) {
        self.ui_state_tx
            .send_modify(|s| s.post_only_reject_status = status.clone());
        self.runtime_state_tx
            .send_modify(|s| s.post_only_reject_status = status);
    }

    /// Populate market_snapshots for all markets in the discovery universe.
    /// Ensures positions from any market (not just current/next) can resolve metadata.
    pub fn set_market_universe(&self, markets: &[crate::polymarket::discovery::MarketSelection]) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let mut active_market_ids: HashSet<MarketId> = HashSet::with_capacity(markets.len());
        for m in markets {
            active_market_ids.insert(m.condition_id.clone());
        }
        self.ui_state_tx.send_modify(|s| {
            for m in markets {
                let name = m.question.as_ref().or(m.slug.as_ref());
                if let Some(name) = name {
                    s.market_names
                        .entry(m.condition_id.clone())
                        .or_insert_with(|| name.clone());
                }
                let up_str = m.up_token.to_string();
                let down_str = m.down_token.to_string();
                s.token_to_condition_id
                    .insert(up_str.clone(), m.condition_id.clone());
                s.token_to_condition_id
                    .insert(down_str.clone(), m.condition_id.clone());
                s.market_snapshots.insert(
                    m.condition_id.clone(),
                    MarketSnapshot {
                        ts_ms: now,
                        condition_id: m.condition_id.clone(),
                        up_token: up_str,
                        down_token: down_str,
                        window_start_ms: m.window_start_ms,
                        window_end_ms: m.window_end_ms,
                        question: m.question.clone(),
                        slug: m.slug.clone(),
                        underlying: Some(m.underlying.clone()),
                    },
                );
            }
            prune_ui_market_cache(s, &active_market_ids, now);
        });
    }

    pub fn emit_ui(&self, event: ActionEvent) {
        self.apply_to_ui(&event);
        self.apply_to_runtime_state(&event);
        let is_tick = matches!(
            &event,
            ActionEvent::OracleTick { .. }
                | ActionEvent::CexQuote { .. }
                | ActionEvent::StrategyStatus { .. }
        );
        if !is_tick {
            self.emit_runtime_signals(&event);
        }
    }

    pub fn emit_action(&self, event: ActionEvent) {
        if self.action_log_enabled && should_log_action_event(&event, self.action_log_log_ticks) {
            // Do not block trading paths; drop if congested.
            let _ = self.actions_tx.try_send(event.clone());
        }
        self.emit_ui(event);
    }

    pub async fn emit_action_critical(&self, event: ActionEvent) -> Result<()> {
        if self.action_log_enabled && should_log_action_event(&event, self.action_log_log_ticks) {
            self.actions_tx
                .send(event.clone())
                .await
                .context("action log queue is closed")?;
        }
        self.emit_ui(event);
        Ok(())
    }

    /// Create a lightweight slot-local `Observability` with fresh per-slot
    /// channels but shared action log.  Used by the autotuner to isolate each
    /// parallel test slot without duplicating log infrastructure.
    pub fn new_slot(parent: &Self, shutdown: CancellationToken) -> Self {
        let (ui_state_tx, _) = watch::channel(UiState::new(now_ms(), "autotuner_slot".into()));
        let (runtime_state_tx, _) =
            watch::channel(RuntimeState::new(now_ms(), "autotuner_slot".into()));
        let (live_account_refresh_seq_tx, _) = watch::channel::<u64>(0);
        let (portfolio_reset_tx, _) = watch::channel::<u64>(0);
        Self {
            actions_tx: parent.actions_tx.clone(),
            ui_state_tx,
            runtime_state_tx,
            live_account_refresh_seq_tx,
            portfolio_reset_tx,
            shutdown,
            filter_reload: parent.filter_reload.clone(),
            flood_control: parent.flood_control.clone(),
            action_log_enabled: parent.action_log_enabled,
            action_log_log_ticks: false,
        }
    }

    /// Reset accumulated trading stats without touching live positions/markets.
    /// Reset accumulated trading stats AND signal the PortfolioRuntime to
    /// clear all positions/lots/fills.  Used by the autotuner between config
    /// test runs so each config starts with a clean slate.
    pub fn reset_stats(&self) {
        // Clear UI-side stats and counters, reset run start time.
        self.ui_state_tx.send_modify(|s| {
            s.started_at_ms = now_ms();
            s.portfolio = crate::portfolio::PortfolioState::default();
            s.asset_stats.clear();
            s.last_asset_stats_seq = 0;
            s.counters = Default::default();
        });
        // Bump the reset sequence so PortfolioRuntime clears its state.
        self.portfolio_reset_tx.send_modify(|seq| *seq += 1);
    }

    fn emit_runtime_signals(&self, event: &ActionEvent) {
        if matches!(
            event,
            ActionEvent::Fill { .. } | ActionEvent::ReclaimResolved { .. }
        ) {
            self.live_account_refresh_seq_tx
                .send_modify(|seq| *seq = seq.saturating_add(1));
        }
    }
}

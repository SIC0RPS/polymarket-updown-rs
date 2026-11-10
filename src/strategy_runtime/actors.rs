use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use bbwick_engine::{OracleCandleWindow, OracleHistory};
use tokio::sync::watch;

use crate::capture::events::StrategyConsumptionKind;
use crate::config::{BbwickOracleConfig, MakerRebateMmStrategyConfig, SharedStrategyConfig};
use crate::ctf_inventory::CtfInventoryRuntime;
use crate::observability::strategy_status::StrategyStatusState;
use crate::orders::OpenOrdersState;
use crate::polymarket::state::{
    odds_filtered_for_underlying, MarketProjection, PolymarketOddsState,
};
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::portfolio::PortfolioState;
use crate::risk::{RiskBudgetSnapshot, RiskHandle, RiskSnapshot};
use crate::strategy::{MakerRebateMmStrategy, OracleLearningSnapshot, StrategyFactSink};
use crate::strategy_runtime::recompute::next_strategy_deadline_ms;
use crate::trade_session_ledger::{live_accounting_receiver, LiveAccountingGeneration};
use crate::types::MarketTick;

use super::state::StrategyRuntimeState;

#[path = "actors/views.rs"]
mod views;
pub(crate) use views::{bounded_active_tokens, localized_odds_view};

#[path = "actors/symbol.rs"]
mod symbol;
pub(crate) use symbol::{MakerRebateMmSymbolActorInit, MakerRebateMmSymbolViewUpdate};

#[path = "actors/admission.rs"]
mod admission;
pub use admission::{CentralAdmissionReservation, CentralCycleAdmission};

#[path = "actors/ownership.rs"]
mod ownership;

pub const SYMBOL_ACTOR_ORDER: [&str; 4] = ["BTC", "ETH", "SOL", "XRP"];

pub struct MakerRebateMmActor {
    pub symbol: &'static str,
    pub oracle_symbol: String,
    pub strategy: MakerRebateMmStrategy,
    pub fact_sink: StrategyFactSink,
    pub runtime_state: StrategyRuntimeState,
    pub status_state: StrategyStatusState,
    pub cancel_only: bool,
    last_consumed_odds_revision: Option<u64>,
    last_captured_candle_state: [Option<Arc<[Arc<OracleCandleWindow>]>>; 2],
    ownership: SymbolOwnershipIndex,
    portfolio_tx: watch::Sender<PortfolioState>,
    open_orders_tx: watch::Sender<OpenOrdersState>,
    risk_tx: watch::Sender<RiskBudgetSnapshot>,
    blocked_markets_tx: watch::Sender<HashSet<String>>,
    shared_cfg: SharedStrategyConfig,
    oracle_learning_continuation_path: Option<PathBuf>,
    oracle_learning_capture_identities: Vec<String>,
}

#[derive(Clone)]
pub struct MakerRebateMmActorScope {
    symbol: &'static str,
    ownership: SymbolOwnershipIndex,
}

#[derive(Clone)]
pub struct CentralSymbolCommandOwnership {
    ownership: SymbolOwnershipIndex,
}

pub struct MakerRebateMmActorSetInit {
    pub cfg: MakerRebateMmStrategyConfig,
    pub equity_baseline_usdt: f64,
    pub risk: RiskHandle,
    pub risk_rx: watch::Receiver<RiskSnapshot>,
    pub blocked_markets_rx: watch::Receiver<HashSet<String>>,
    pub open_orders_rx: watch::Receiver<OpenOrdersState>,
    pub bbwick_oracle_cfg: BbwickOracleConfig,
    pub oracle_history: Arc<OracleHistory>,
    pub odds_rx: watch::Receiver<Arc<PolymarketOddsState>>,
    pub market_projection_rx: watch::Receiver<MarketProjection>,
    pub portfolio_rx: watch::Receiver<PortfolioState>,
    pub shared_cfg: SharedStrategyConfig,
    pub ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
    pub selected_oracle_learning_to_validate: BTreeMap<String, OracleLearningSnapshot>,
    pub initial_oracle_learning: BTreeMap<String, OracleLearningSnapshot>,
    pub oracle_learning_continuation_paths: BTreeMap<String, PathBuf>,
    pub oracle_learning_capture_identities: BTreeMap<String, Vec<String>>,
}

pub struct MakerRebateMmActorSet {
    actors: Vec<MakerRebateMmActor>,
    risk: RiskHandle,
    risk_rx: watch::Receiver<RiskSnapshot>,
    blocked_markets_rx: watch::Receiver<HashSet<String>>,
    open_orders_rx: watch::Receiver<OpenOrdersState>,
    odds_rx: watch::Receiver<Arc<PolymarketOddsState>>,
    market_projection_rx: watch::Receiver<MarketProjection>,
    portfolio_rx: watch::Receiver<PortfolioState>,
    accounting_rx: watch::Receiver<Arc<LiveAccountingGeneration>>,
    shared_cfg: SharedStrategyConfig,
    ownership: SymbolOwnershipIndex,
    cycle_admission: CentralCycleAdmission,
    ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
}

impl MakerRebateMmActor {
    pub fn changed_consumed_candle_state(
        &self,
        kind: StrategyConsumptionKind,
    ) -> anyhow::Result<Option<Arc<[Arc<OracleCandleWindow>]>>> {
        let windows = self
            .strategy
            .consumed_candle_view()
            .window_arcs_for_symbol(&self.oracle_symbol)
            .context("consumed candle view is missing actor symbol")?;
        let previous = self.last_captured_candle_state[kind as usize].as_deref();
        if previous.is_some_and(|previous| {
            previous.len() == windows.len()
                && previous
                    .iter()
                    .zip(windows)
                    .all(|(previous, current)| Arc::ptr_eq(previous, current))
        }) {
            return Ok(None);
        }
        Ok(Some(Arc::from(windows)))
    }

    pub fn acknowledge_captured_candle_state(
        &mut self,
        kind: StrategyConsumptionKind,
        state: Option<Arc<[Arc<OracleCandleWindow>]>>,
    ) {
        if let Some(state) = state {
            self.last_captured_candle_state[kind as usize] = Some(state);
        }
    }

    pub fn next_strategy_deadline_ms(&self) -> Option<u64> {
        next_strategy_deadline_ms(&self.runtime_state.control)
    }

    pub fn persist_oracle_learning(&self, knowledge_cutoff_ms: u64) -> anyhow::Result<()> {
        let Some(path) = self.oracle_learning_continuation_path.as_ref() else {
            return Ok(());
        };
        let learning = self.strategy.oracle_learning(
            self.oracle_learning_capture_identities.clone(),
            knowledge_cutoff_ms,
        )?;
        let by_symbol = BTreeMap::from([(self.symbol.to_string(), learning)]);
        let bytes = rmp_serde::to_vec_named(&by_symbol)?;
        crate::runtime_files::write_bytes_atomic(path, &bytes)
    }
}

impl MakerRebateMmActorSet {
    pub fn new(init: MakerRebateMmActorSetInit) -> anyhow::Result<Self> {
        let MakerRebateMmActorSetInit {
            cfg,
            equity_baseline_usdt,
            risk: risk_handle,
            risk_rx,
            blocked_markets_rx,
            open_orders_rx,
            bbwick_oracle_cfg,
            oracle_history,
            odds_rx,
            market_projection_rx,
            portfolio_rx,
            shared_cfg,
            ctf_inventory_runtime,
            mut selected_oracle_learning_to_validate,
            mut initial_oracle_learning,
            mut oracle_learning_continuation_paths,
            mut oracle_learning_capture_identities,
        } = init;
        let full_projection = market_projection_rx.borrow().clone();
        let full_odds = odds_rx.borrow().clone();
        let mut reserved_inventory = [0_u128; SYMBOL_ACTOR_ORDER.len()];
        for (index, symbol) in SYMBOL_ACTOR_ORDER.into_iter().enumerate() {
            reserved_inventory[index] = reserved_inventory_units_for_symbol(
                &ctf_inventory_runtime,
                &risk_handle,
                &full_projection,
                symbol,
            )?;
        }
        let accounting_rx = live_accounting_receiver();
        let accounting = Arc::clone(&accounting_rx.borrow());
        let mut full_portfolio = portfolio_rx.borrow().clone();
        full_portfolio.project_canonical_buy_basis(accounting.financial.financial_view());
        let full_open_orders = open_orders_rx.borrow().clone();
        let full_risk = risk_rx.borrow().clone();
        let full_blocked_markets = blocked_markets_rx.borrow().clone();
        let initial_now_ms = full_risk.updated_at_ms.max(full_portfolio.updated_at_ms);
        let cycle_admission = CentralCycleAdmission::new(cfg.max_orders_per_cycle);
        let ownership = SymbolOwnershipIndex::from_projection(&full_projection)?;
        let actors = SYMBOL_ACTOR_ORDER
            .into_iter()
            .enumerate()
            .map(|(index, symbol)| -> anyhow::Result<MakerRebateMmActor> {
                let actor_market_projection = full_projection.filtered_for_underlying(symbol);
                let actor_ownership =
                    SymbolOwnershipIndex::from_projection(&actor_market_projection)?;
                let portfolio = full_portfolio.filtered_for_underlying(symbol, &actor_ownership);
                let open_orders =
                    full_open_orders.filtered_for_underlying(symbol, &actor_ownership);
                let retained_tokens = bounded_active_tokens(&portfolio, &open_orders)?;
                let odds =
                    odds_filtered_for_underlying(full_odds.as_ref(), symbol, &retained_tokens);
                let symbol_cfg = cfg.config_for_symbol(symbol);
                let mut risk = full_risk.budget_for_underlying(
                    symbol,
                    &portfolio,
                    &open_orders,
                    &actor_market_projection,
                    symbol_cfg.max_inventory_total,
                    initial_now_ms,
                )?;
                risk.subtract_reserved_inventory_units_6dp(reserved_inventory[index])?;
                let blocked = filtered_blocked_markets(
                    &full_blocked_markets,
                    symbol,
                    &actor_market_projection,
                );
                let mut actor = MakerRebateMmActor::new_symbol(MakerRebateMmSymbolActorInit {
                    symbol,
                    cfg: isolated_symbol_cfg(&cfg, symbol),
                    equity_baseline_usdt,
                    oracle_cfg: bbwick_oracle_cfg.isolated_for_underlying(symbol),
                    oracle_history: oracle_history.clone(),
                    odds,
                    portfolio,
                    open_orders,
                    risk,
                    blocked_markets: blocked,
                    ownership: actor_ownership,
                })?;
                if let Some(selected) = selected_oracle_learning_to_validate.remove(symbol) {
                    actor.strategy.restore_oracle_learning(selected)?;
                }
                if let Some(learning) = initial_oracle_learning.remove(symbol) {
                    actor.strategy.restore_oracle_learning(learning)?;
                }
                actor.oracle_learning_continuation_path =
                    oracle_learning_continuation_paths.remove(symbol);
                actor.oracle_learning_capture_identities = oracle_learning_capture_identities
                    .remove(symbol)
                    .unwrap_or_default();
                Ok(actor)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !actors.is_empty(),
            "maker_rebate_mm actor set requires at least one enabled symbol"
        );
        anyhow::ensure!(
            initial_oracle_learning.is_empty(),
            "oracle learning contains a disabled symbol"
        );
        anyhow::ensure!(
            selected_oracle_learning_to_validate.is_empty()
                && oracle_learning_continuation_paths.is_empty()
                && oracle_learning_capture_identities.is_empty(),
            "live oracle learning contains a disabled symbol"
        );
        let mut oracle_symbols = HashSet::new();
        for actor in &actors {
            anyhow::ensure!(
                actor.strategy.bbwick_oracle_cfg.active_symbols().len() == 1,
                "symbol actor must have exactly one active oracle symbol symbol={}",
                actor.symbol
            );
            anyhow::ensure!(
                oracle_symbols.insert(actor.oracle_symbol.to_ascii_lowercase()),
                "duplicate provider oracle symbol across actors oracle_symbol={}",
                actor.oracle_symbol
            );
        }
        Ok(Self {
            actors,
            risk: risk_handle,
            risk_rx,
            blocked_markets_rx,
            open_orders_rx,
            odds_rx,
            market_projection_rx,
            portfolio_rx,
            accounting_rx,
            shared_cfg,
            ownership,
            cycle_admission,
            ctf_inventory_runtime,
        })
    }

    pub fn refresh_views(&mut self, now_ms: u64) -> anyhow::Result<()> {
        let projection = self.market_projection_rx.borrow().clone();
        self.ownership.register_projection(&projection)?;
        let odds = self.odds_rx.borrow().clone();
        let mut reserved_inventory = [0_u128; SYMBOL_ACTOR_ORDER.len()];
        for (index, actor) in self.actors.iter().enumerate() {
            reserved_inventory[index] = reserved_inventory_units_for_symbol(
                &self.ctf_inventory_runtime,
                &self.risk,
                &projection,
                actor.symbol,
            )?;
        }
        let accounting = Arc::clone(&self.accounting_rx.borrow());
        let mut portfolio = self.portfolio_rx.borrow().clone();
        portfolio.project_canonical_buy_basis(accounting.financial.financial_view());
        let open_orders = self.open_orders_rx.borrow().clone();
        let risk = self.risk_rx.borrow().clone();
        let blocked_markets = self.blocked_markets_rx.borrow().clone();
        let base_cfg = self.shared_cfg.borrow().clone();
        for (index, actor) in self.actors.iter_mut().enumerate() {
            let actor_projection = projection.filtered_for_underlying(actor.symbol);
            actor.ownership.register_projection(&actor_projection)?;
            let actor_portfolio = portfolio.filtered_for_underlying(actor.symbol, &actor.ownership);
            let actor_open_orders =
                open_orders.filtered_for_underlying(actor.symbol, &actor.ownership);
            let retained_tokens = bounded_active_tokens(&actor_portfolio, &actor_open_orders)?;
            let external_view = if let Some(symbol_odds) = localized_odds_view(
                actor.strategy.odds.as_ref(),
                odds.as_ref(),
                actor.symbol,
                &retained_tokens,
            )? {
                let symbol_odds = Arc::new(symbol_odds);
                let symbol_projection = MarketProjection::from_odds(symbol_odds.as_ref());
                actor.ownership.register_projection(&symbol_projection)?;
                Some((symbol_odds, symbol_projection))
            } else {
                None
            };
            let mut actor_risk = risk.budget_for_underlying(
                actor.symbol,
                &actor_portfolio,
                &actor_open_orders,
                &actor_projection,
                base_cfg.config_for_symbol(actor.symbol).max_inventory_total,
                now_ms,
            )?;
            actor_risk.subtract_reserved_inventory_units_6dp(reserved_inventory[index])?;
            let actor_blocked_markets =
                filtered_blocked_markets(&blocked_markets, actor.symbol, &actor_projection);
            actor.apply_symbol_views_with_retained_tokens(
                MakerRebateMmSymbolViewUpdate {
                    external_view,
                    portfolio: actor_portfolio,
                    open_orders: actor_open_orders,
                    risk: actor_risk,
                    blocked_markets: actor_blocked_markets,
                    cfg: Some(isolated_symbol_cfg(&base_cfg, actor.symbol)),
                },
                &retained_tokens,
            )?;
        }
        Ok(())
    }

    pub async fn odds_changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.odds_rx.changed().await
    }

    pub fn actor_symbols(&self) -> Vec<&'static str> {
        self.actors.iter().map(|actor| actor.symbol).collect()
    }

    pub fn next_strategy_deadline_ms(
        &self,
        accepted_ticks: &std::collections::BTreeMap<String, MarketTick>,
    ) -> Option<u64> {
        self.actors
            .iter()
            .filter(|actor| accepted_ticks.contains_key(actor.symbol))
            .filter_map(MakerRebateMmActor::next_strategy_deadline_ms)
            .min()
    }

    pub fn refresh_symbol_risk_view(&mut self, symbol: &str, now_ms: u64) -> anyhow::Result<()> {
        let projection = self.market_projection_rx.borrow().clone();
        let reserved_inventory = reserved_inventory_units_for_symbol(
            &self.ctf_inventory_runtime,
            &self.risk,
            &projection,
            symbol,
        )?;
        let portfolio = self.portfolio_rx.borrow().clone();
        let open_orders = self.open_orders_rx.borrow().clone();
        let risk = self.risk_rx.borrow().clone();
        let max_inventory_total = self
            .shared_cfg
            .borrow()
            .config_for_symbol(symbol)
            .max_inventory_total;
        let actor_projection = projection.filtered_for_underlying(symbol);
        let actor = self
            .actors
            .iter_mut()
            .find(|actor| actor.symbol.eq_ignore_ascii_case(symbol))
            .ok_or_else(|| anyhow::anyhow!("unknown strategy actor symbol={symbol}"))?;
        let actor_portfolio = portfolio.filtered_for_underlying(symbol, &actor.ownership);
        let actor_open_orders = open_orders.filtered_for_underlying(symbol, &actor.ownership);
        let mut actor_risk = risk.budget_for_underlying(
            symbol,
            &actor_portfolio,
            &actor_open_orders,
            &actor_projection,
            max_inventory_total,
            now_ms,
        )?;
        actor_risk.subtract_reserved_inventory_units_6dp(reserved_inventory)?;
        actor.risk_tx.send_replace(actor_risk);
        Ok(())
    }

    pub fn cycle_admission(&self) -> CentralCycleAdmission {
        self.cycle_admission
            .reconcile(self.shared_cfg.borrow().max_orders_per_cycle);
        self.cycle_admission.clone()
    }

    pub fn command_ownership(&self) -> CentralSymbolCommandOwnership {
        CentralSymbolCommandOwnership {
            ownership: self.ownership.clone(),
        }
    }

    pub fn reset_oracle_epoch_state(&mut self) {
        for actor in &mut self.actors {
            actor.strategy.reset_oracle_epoch_state();
        }
    }

    pub fn actor_for_underlying_mut(
        &mut self,
        underlying: &str,
    ) -> Option<&mut MakerRebateMmActor> {
        self.actors
            .iter_mut()
            .find(|actor| actor.symbol.eq_ignore_ascii_case(underlying))
    }

    pub fn actor_for_oracle_symbol_mut(
        &mut self,
        oracle_symbol: &str,
    ) -> Option<&mut MakerRebateMmActor> {
        self.actors
            .iter_mut()
            .find(|actor| actor.oracle_symbol.eq_ignore_ascii_case(oracle_symbol))
    }

    pub fn actor_for_chainlink_symbol_mut(
        &mut self,
        chainlink_symbol: &str,
    ) -> anyhow::Result<&mut MakerRebateMmActor> {
        let canonical = crate::oracle_resolution::canonical_chainlink_symbol(chainlink_symbol)?;
        self.actors
            .iter_mut()
            .find(|actor| {
                crate::oracle_resolution::canonical_chainlink_symbol(actor.symbol)
                    .is_ok_and(|candidate| candidate == canonical)
            })
            .ok_or_else(|| {
                anyhow::anyhow!("Chainlink price has no symbol actor: {chainlink_symbol}")
            })
    }
}

fn reserved_inventory_units_for_symbol(
    runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    risk: &RiskHandle,
    projection: &MarketProjection,
    symbol: &str,
) -> anyhow::Result<u128> {
    let conditions = projection
        .markets()
        .iter()
        .filter(|market| market.underlying.eq_ignore_ascii_case(symbol))
        .map(|market| {
            market.condition_id_b256().ok_or_else(|| {
                anyhow::anyhow!(
                    "symbol risk projection has invalid condition identity market_id={}",
                    market.market_id
                )
            })
        })
        .collect::<anyhow::Result<HashSet<_>>>()?;
    let split_units = runtime
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .unmaterialized_split_gross_inventory_units_6dp_for_conditions(
            conditions.iter().copied().map(Into::into),
        )?;
    split_units
        .checked_add(risk.unresolved_direct_buy_qty_units_6dp_for_conditions(&conditions)?)
        .context("symbol reserved inventory quantity overflow")
}

fn isolated_symbol_cfg(
    base: &MakerRebateMmStrategyConfig,
    symbol: &str,
) -> MakerRebateMmStrategyConfig {
    let mut cfg = base.config_for_symbol(symbol).clone();
    cfg.optimizer_symbol_configs.clear();
    cfg.enabled_assets = base.enabled_assets.clone();
    cfg
}

fn filtered_blocked_markets(
    blocked: &HashSet<String>,
    symbol: &str,
    projection: &MarketProjection,
) -> HashSet<String> {
    projection
        .markets()
        .iter()
        .filter(|market| market.underlying.eq_ignore_ascii_case(symbol))
        .flat_map(|market| {
            [market.market_id.as_str(), market.condition_id.as_str()]
                .into_iter()
                .filter(|id| blocked.contains(*id))
                .map(str::to_string)
        })
        .collect()
}

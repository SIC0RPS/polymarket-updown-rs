use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{ensure, Result};
use bbwick_engine::OracleHistory;
use tokio::sync::watch;

use crate::config::{BbwickOracleConfig, MakerRebateMmStrategyConfig, VALID_ASSETS};
use crate::observability::strategy_status::StrategyStatusState;
use crate::orders::OpenOrdersState;
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::portfolio::PortfolioState;
use crate::risk::RiskBudgetSnapshot;
use crate::strategy::{MakerRebateMmStrategy, MakerRebateMmStrategyInit, StrategyFactSink};

use super::{bounded_active_tokens, MakerRebateMmActor, StrategyRuntimeState};

pub(crate) struct MakerRebateMmSymbolActorInit {
    pub symbol: &'static str,
    pub cfg: MakerRebateMmStrategyConfig,
    pub equity_baseline_usdt: f64,
    pub oracle_cfg: BbwickOracleConfig,
    pub oracle_history: Arc<OracleHistory>,
    pub odds: PolymarketOddsState,
    pub portfolio: PortfolioState,
    pub open_orders: OpenOrdersState,
    pub risk: RiskBudgetSnapshot,
    pub blocked_markets: HashSet<String>,
    pub ownership: SymbolOwnershipIndex,
}

pub(crate) struct MakerRebateMmSymbolViewUpdate {
    pub external_view: Option<(Arc<PolymarketOddsState>, MarketProjection)>,
    pub portfolio: PortfolioState,
    pub open_orders: OpenOrdersState,
    pub risk: RiskBudgetSnapshot,
    pub blocked_markets: HashSet<String>,
    pub cfg: Option<MakerRebateMmStrategyConfig>,
}

impl MakerRebateMmActor {
    pub(crate) fn new_symbol(init: MakerRebateMmSymbolActorInit) -> Result<Self> {
        let MakerRebateMmSymbolActorInit {
            symbol,
            cfg,
            equity_baseline_usdt,
            oracle_cfg,
            oracle_history,
            odds,
            portfolio,
            open_orders,
            risk,
            blocked_markets,
            ownership,
        } = init;
        validate_symbol(symbol)?;
        validate_symbol_cfg(symbol, &cfg)?;
        validate_oracle_cfg(symbol, &oracle_cfg)?;
        let odds = Arc::new(odds);
        let market_projection = MarketProjection::from_odds(odds.as_ref());
        ownership.register_projection(&market_projection)?;
        let retained_tokens = bounded_active_tokens(&portfolio, &open_orders)?;
        validate_symbol_views(
            symbol,
            Some(&market_projection),
            &retained_tokens,
            &risk,
            &blocked_markets,
            &ownership,
        )?;
        let shared_cfg = watch::channel(cfg.clone()).0;
        let oracle_symbol = oracle_cfg.resolve_symbol_for_underlying(symbol);
        let (portfolio_tx, portfolio_rx) = watch::channel(portfolio);
        let (open_orders_tx, open_orders_rx) = watch::channel(open_orders);
        let (risk_tx, risk_rx) = watch::channel(risk);
        let (blocked_markets_tx, blocked_markets_rx) = watch::channel(blocked_markets);
        let strategy = MakerRebateMmStrategy::new(MakerRebateMmStrategyInit {
            cfg,
            oracle_symbol: oracle_symbol.clone(),
            equity_baseline_usdt,
            risk_rx: Some(risk_rx),
            blocked_markets_rx: Some(blocked_markets_rx),
            open_orders_rx: Some(open_orders_rx),
            bbwick_oracle_cfg: oracle_cfg,
            oracle_history,
            odds,
            market_projection,
            portfolio_rx,
            shared_cfg: shared_cfg.clone(),
        })?;
        Ok(Self {
            symbol,
            oracle_symbol,
            strategy,
            fact_sink: StrategyFactSink::default(),
            runtime_state: StrategyRuntimeState::default(),
            status_state: StrategyStatusState::default(),
            cancel_only: false,
            last_consumed_odds_revision: None,
            last_captured_candle_state: [None, None],
            ownership,
            portfolio_tx,
            open_orders_tx,
            risk_tx,
            blocked_markets_tx,
            shared_cfg,
            oracle_learning_continuation_path: None,
            oracle_learning_capture_identities: Vec::new(),
        })
    }

    pub fn odds_snapshot(&self) -> Arc<PolymarketOddsState> {
        Arc::clone(&self.strategy.odds)
    }

    pub fn odds_revision(&self) -> u64 {
        self.strategy.odds.revision
    }

    pub fn has_unconsumed_odds(&self) -> bool {
        self.last_consumed_odds_revision != Some(self.odds_revision())
    }

    pub fn acknowledge_odds_revision(&mut self, revision: u64) {
        debug_assert_eq!(revision, self.odds_revision());
        self.last_consumed_odds_revision = Some(revision);
    }

    pub(crate) fn apply_symbol_views(
        &mut self,
        update: MakerRebateMmSymbolViewUpdate,
    ) -> Result<()> {
        let retained_tokens = bounded_active_tokens(&update.portfolio, &update.open_orders)?;
        self.apply_symbol_views_with_retained_tokens(update, &retained_tokens)
    }

    pub(crate) fn apply_symbol_views_with_retained_tokens(
        &mut self,
        update: MakerRebateMmSymbolViewUpdate,
        retained_tokens: &HashSet<polymarket_client_sdk::types::U256>,
    ) -> Result<()> {
        let MakerRebateMmSymbolViewUpdate {
            external_view,
            portfolio,
            mut open_orders,
            risk,
            blocked_markets,
            cfg,
        } = update;
        if let Some(symbol_cfg) = cfg.as_ref() {
            validate_symbol_cfg(self.symbol, symbol_cfg)?;
        }
        validate_symbol_views(
            self.symbol,
            external_view.as_ref().map(|(_, projection)| projection),
            retained_tokens,
            &risk,
            &blocked_markets,
            &self.ownership,
        )?;
        if let Some((odds, projection)) = external_view {
            self.strategy.odds = odds;
            self.strategy.market_projection = projection;
        }
        let portfolio_changed = {
            let prior = self.portfolio_tx.borrow();
            portfolio.positions_fingerprint != prior.positions_fingerprint
                || portfolio.revision != prior.revision
                || portfolio.positions.iter().any(|(token, position)| {
                    prior.positions.get(token).is_none_or(|prior_position| {
                        prior_position.remaining_basis_units_6dp
                            != position.remaining_basis_units_6dp
                    })
                })
        };
        if portfolio_changed {
            self.portfolio_tx.send_replace(portfolio);
        }
        let (prior_orders_fingerprint, prior_orders_revision) = {
            let prior = self.open_orders_tx.borrow();
            (prior.orders_fingerprint, prior.revision)
        };
        if open_orders.orders_fingerprint != prior_orders_fingerprint {
            open_orders.revision = prior_orders_revision.saturating_add(1);
            self.open_orders_tx.send_replace(open_orders);
        }
        self.risk_tx.send_replace(risk);
        self.blocked_markets_tx.send_replace(blocked_markets);
        if let Some(symbol_cfg) = cfg {
            self.shared_cfg.send_replace(symbol_cfg);
        }
        Ok(())
    }

    pub(crate) fn apply_symbol_odds(
        &mut self,
        odds: Arc<PolymarketOddsState>,
        projection: MarketProjection,
    ) -> Result<()> {
        validate_external_view(self.symbol, &projection, &self.ownership)?;
        self.strategy.odds = odds;
        self.strategy.market_projection = projection;
        Ok(())
    }
}

fn validate_symbol(symbol: &str) -> Result<()> {
    ensure!(
        VALID_ASSETS.contains(&symbol),
        "invalid strategy actor symbol={symbol}"
    );
    Ok(())
}

fn validate_symbol_cfg(symbol: &str, cfg: &MakerRebateMmStrategyConfig) -> Result<()> {
    ensure!(
        cfg.optimizer_symbol_configs.is_empty(),
        "strategy actor config is not isolated symbol={symbol} enabled_assets={:?} nested_symbol_configs={}",
        cfg.enabled_assets,
        cfg.optimizer_symbol_configs.len()
    );
    Ok(())
}

fn validate_oracle_cfg(symbol: &str, cfg: &BbwickOracleConfig) -> Result<()> {
    let active = cfg.active_symbols();
    let expected = cfg.resolve_symbol_for_underlying(symbol);
    let isolated = cfg.isolated_for_underlying(symbol);
    ensure!(
        active.len() == 1
            && active[0].eq_ignore_ascii_case(&expected)
            && cfg.symbols == isolated.symbols
            && cfg.underlying_symbol_map == isolated.underlying_symbol_map,
        "strategy actor oracle is not isolated symbol={symbol} expected={expected} active={active:?}"
    );
    Ok(())
}

fn validate_symbol_views(
    symbol: &str,
    projection: Option<&MarketProjection>,
    retained_tokens: &HashSet<polymarket_client_sdk::types::U256>,
    risk: &RiskBudgetSnapshot,
    blocked_markets: &HashSet<String>,
    ownership: &SymbolOwnershipIndex,
) -> Result<()> {
    for foreign in VALID_ASSETS
        .iter()
        .copied()
        .filter(|candidate| !candidate.eq_ignore_ascii_case(symbol))
    {
        ensure!(
            ownership.markets_for_symbol(foreign).is_empty()
                && ownership.tokens_for_symbol(foreign).is_empty(),
            "strategy actor ownership contains foreign symbol={foreign} actor={symbol}"
        );
    }
    ensure!(
        retained_tokens
            .iter()
            .all(|token| ownership.token_belongs(&token.to_string(), symbol)),
        "strategy actor account token ownership mismatch symbol={symbol}"
    );
    if let Some(projection) = projection {
        let missing_tokens = retained_tokens
            .iter()
            .filter(|token| !projection.contains_token(&token.to_string()))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        ensure!(
            missing_tokens.is_empty(),
            "strategy actor retained token has no market metadata symbol={symbol} missing_tokens={missing_tokens:?} projected_markets={:?}",
            projection
                .markets()
                .iter()
                .map(|market| market.market_id.as_str())
                .collect::<Vec<_>>()
        );
        validate_external_view(symbol, projection, ownership)?;
        ensure!(
            blocked_markets.iter().all(|blocked| {
                projection.markets().iter().any(|market| {
                    market.market_id.as_str() == blocked || market.condition_id.as_str() == blocked
                })
            }),
            "strategy actor blocked markets are not isolated symbol={symbol}"
        );
    } else {
        ensure!(
            blocked_markets
                .iter()
                .all(|blocked| ownership.market_belongs(blocked, symbol)),
            "strategy actor blocked markets are not owned symbol={symbol}"
        );
    }
    ensure!(
        risk.market_budgets.iter().all(|(market, budget)| {
            market == &budget.market_id
                && ownership.market_belongs(market.as_str(), symbol)
                && ownership.market_belongs(budget.market_id.as_str(), symbol)
        }),
        "strategy actor risk is not isolated symbol={symbol}"
    );
    Ok(())
}

fn validate_external_view(
    symbol: &str,
    projection: &MarketProjection,
    ownership: &SymbolOwnershipIndex,
) -> Result<()> {
    ensure!(
        projection.markets().iter().all(|market| {
            market.underlying.eq_ignore_ascii_case(symbol)
                && ownership.market_belongs(market.market_id.as_str(), symbol)
                && ownership.token_belongs(market.up_token_id(), symbol)
                && ownership.token_belongs(market.down_token_id(), symbol)
        }),
        "strategy actor odds ownership mismatch symbol={symbol}"
    );
    Ok(())
}

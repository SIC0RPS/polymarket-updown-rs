use std::collections::HashMap;

use anyhow::Result;

use crate::polymarket::identity::MarketId;

use super::market_budget::{
    effective_market_exposure_units, inflight_buy_cost_units, local_open_cohort_costs,
    open_buy_order_notional_units, validate_authoritative_market_exposure_identities,
    validate_market_id,
};
use super::RiskFixed6;

const EXPIRED_POSITION_INVENTORY_CAP_GRACE_MS: u64 = 5_000;

#[derive(Clone)]
pub struct RiskBlock {
    pub rule: String,
    pub details: String,
}

#[derive(Debug, Clone, Default)]
pub struct RiskSnapshot {
    pub updated_at_ms: u64,

    pub enabled: bool,
    pub trading_allowed: bool,
    pub freeze_level: u8,
    pub frozen_rule: Option<String>,
    pub frozen_details: Option<String>,

    pub last_recon_at_ms: Option<u64>,
    /// Trading/admission availability from Polymarket balance-allowance.
    pub usdc_balance: Option<f64>,
    pub wallet_usdc_balance: Option<f64>,
    pub wallet_matic_balance: Option<f64>,
    pub marked_positions_value: Option<f64>,
    pub current_wallet_equity: Option<f64>,
    pub trading_collateral_balance: Option<f64>,
    pub account_equity_updated_at_ms: Option<u64>,
    pub exchange_allowance: Option<f64>,

    pub rest_open_orders: Option<usize>,
    pub rest_positions: Option<usize>,
    pub peak_equity: f64,
    pub current_equity: f64,
    pub last_positive_equity: Option<f64>,
    pub max_order_qty: f64,
    pub max_window_cost: f64,
    pub max_market_equity_pct: f64,
    pub authoritative_market_exposures: HashMap<MarketId, AuthoritativeMarketExposure>,
    pub inflight_buy_notional_by_market: HashMap<MarketId, f64>,
}

#[derive(Clone, Default)]
pub struct RiskBudgetSnapshot {
    pub updated_at_ms: u64,
    pub enabled: bool,
    pub trading_allowed: bool,
    pub freeze_level: u8,
    pub frozen_rule: Option<String>,
    pub frozen_details: Option<String>,
    pub max_order_qty: RiskFixed6,
    pub available_buy_budget_usdt: RiskFixed6,
    pub max_entry_notional_usdt: RiskFixed6,
    pub max_inventory_total: RiskFixed6,
    pub remaining_total_inventory_qty: RiskFixed6,
    pub market_budgets: HashMap<MarketId, MarketBudgetView>,
}

impl RiskBudgetSnapshot {
    pub fn subtract_reserved_inventory_units_6dp(&mut self, units_6dp: u128) -> Result<()> {
        let reserved = RiskFixed6::from_units(units_6dp, "reserved total inventory quantity")?;
        self.remaining_total_inventory_qty =
            self.remaining_total_inventory_qty.saturating_sub(reserved);
        Ok(())
    }
}

#[derive(Clone)]
pub struct MarketBudgetView {
    pub market_id: MarketId,
    pub cap_equity_usdt: Option<RiskFixed6>,
    pub pct_cap_notional: Option<RiskFixed6>,
    pub effective_market_cap_notional: Option<RiskFixed6>,
    pub effective_market_exposure: RiskFixed6,
    pub remaining_market_headroom_usdt: RiskFixed6,
    pub binding_rule: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AuthoritativeMarketExposure {
    pub market_id: MarketId,
    pub open_cohort_committed_outlay_units_6dp: u128,
    pub open_buy_notional: f64,
    pub snapshot_ts_ms: u64,
}

pub(super) struct MarketBudgetLimits {
    cap_equity_usdt: Option<RiskFixed6>,
    pct_cap_notional: Option<RiskFixed6>,
    effective_market_cap_notional: Option<RiskFixed6>,
    binding_rule: Option<&'static str>,
}

pub(super) fn market_budget_limits(snap: &RiskSnapshot) -> Result<MarketBudgetLimits> {
    anyhow::ensure!(
        snap.max_window_cost.is_finite()
            && snap.max_window_cost >= 0.0
            && !snap.max_window_cost.is_sign_negative(),
        "maximum window cost is invalid"
    );
    anyhow::ensure!(
        snap.max_market_equity_pct.is_finite()
            && snap.max_market_equity_pct >= 0.0
            && !snap.max_market_equity_pct.is_sign_negative(),
        "maximum market equity percentage is invalid"
    );
    let cap_equity = if snap.max_market_equity_pct > 0.0 {
        match snap.current_wallet_equity {
            Some(equity) => {
                anyhow::ensure!(
                    equity.is_finite() && equity >= 0.0 && !equity.is_sign_negative(),
                    "current central account equity is invalid"
                );
                Some(equity)
            }
            None => None,
        }
    } else {
        None
    };
    let cap_equity_usdt = cap_equity
        .map(|value| RiskFixed6::capacity(value, "market cap equity"))
        .transpose()?;
    let pct_cap_notional = if snap.max_market_equity_pct > 0.0 {
        cap_equity
            .map(|equity| {
                RiskFixed6::capacity_product(
                    equity,
                    snap.max_market_equity_pct,
                    "market percentage cap",
                )
            })
            .transpose()?
    } else {
        None
    };
    let absolute_cap_notional = if snap.max_window_cost > 0.0 {
        Some(RiskFixed6::capacity(
            snap.max_window_cost,
            "maximum window cost",
        )?)
    } else {
        None
    };
    let effective_market_cap_notional = match (absolute_cap_notional, pct_cap_notional) {
        (Some(absolute), Some(percentage)) => Some(if absolute <= percentage {
            absolute
        } else {
            percentage
        }),
        (Some(absolute), None) => Some(absolute),
        (None, Some(percentage)) => Some(percentage),
        (None, None) => None,
    };
    let binding_rule = match (absolute_cap_notional, pct_cap_notional) {
        (Some(abs_cap), Some(pct_cap)) => Some(if abs_cap <= pct_cap {
            "max_window_cost"
        } else {
            "max_market_equity_pct"
        }),
        (Some(_), None) => Some("max_window_cost"),
        (None, Some(_)) => Some("max_market_equity_pct"),
        (None, None) => None,
    };

    Ok(MarketBudgetLimits {
        cap_equity_usdt,
        pct_cap_notional,
        effective_market_cap_notional,
        binding_rule,
    })
}

pub(super) fn market_budget_view_from_inputs(
    market_id: MarketId,
    limits: &MarketBudgetLimits,
    effective_market_exposure: RiskFixed6,
) -> MarketBudgetView {
    let remaining_market_headroom_usdt = limits
        .effective_market_cap_notional
        .map(|cap| cap.saturating_sub(effective_market_exposure))
        .unwrap_or_default();
    MarketBudgetView {
        market_id,
        cap_equity_usdt: limits.cap_equity_usdt,
        pct_cap_notional: limits.pct_cap_notional,
        effective_market_cap_notional: limits.effective_market_cap_notional,
        effective_market_exposure,
        remaining_market_headroom_usdt,
        binding_rule: limits.binding_rule.map(str::to_string),
    }
}

impl RiskSnapshot {
    pub fn budget_for_underlying(
        &self,
        underlying: &str,
        portfolio: &crate::portfolio::PortfolioState,
        open_orders: &crate::orders::OpenOrdersState,
        projection: &crate::polymarket::state::MarketProjection,
        max_inventory_total: f64,
        now_ms: u64,
    ) -> Result<RiskBudgetSnapshot> {
        let symbol_markets = projection
            .markets()
            .iter()
            .filter(|market| market.underlying.eq_ignore_ascii_case(underlying))
            .collect::<Vec<_>>();
        let symbol_market_ids = symbol_markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut symbol_snapshot = self.clone();
        symbol_snapshot
            .authoritative_market_exposures
            .retain(|market_id, _| symbol_market_ids.contains(market_id));
        symbol_snapshot
            .inflight_buy_notional_by_market
            .retain(|market_id, _| symbol_market_ids.contains(market_id));
        validate_authoritative_market_exposure_identities(
            &symbol_snapshot.authoritative_market_exposures,
        )?;
        let inflight_total =
            inflight_buy_cost_units(&symbol_snapshot.inflight_buy_notional_by_market)?;
        let has_symbol_markets = !symbol_markets.is_empty();
        let local_cohort_by_market = if has_symbol_markets {
            local_open_cohort_costs(portfolio, now_ms)?
        } else {
            HashMap::new()
        };
        let mut open_buy_notional = RiskFixed6::default();
        let mut open_buy_by_token = HashMap::<String, RiskFixed6>::new();
        for order in open_orders.orders.values() {
            let Some(notional) = open_buy_order_notional_units(order)? else {
                continue;
            };
            open_buy_notional =
                open_buy_notional.checked_add(notional, "aggregate open buy notional")?;
            if has_symbol_markets {
                let token_cost = open_buy_by_token
                    .get(&order.token)
                    .copied()
                    .unwrap_or_default()
                    .checked_add(notional, "local token open buy cost")?;
                open_buy_by_token.insert(order.token.clone(), token_cost);
            }
        }
        let mut market_budgets = HashMap::new();
        if has_symbol_markets {
            let limits = market_budget_limits(&symbol_snapshot)?;
            for market in symbol_markets {
                validate_market_id(&market.market_id, "market budget selection")?;
                let mut local_market_exposure = local_cohort_by_market
                    .get(&market.market_id)
                    .copied()
                    .unwrap_or_default();
                if let Some(notional) = open_buy_by_token.get(market.up_token_id()).copied() {
                    local_market_exposure =
                        local_market_exposure.checked_add(notional, "local market exposure")?;
                }
                if market.down_token_id() != market.up_token_id() {
                    if let Some(notional) = open_buy_by_token.get(market.down_token_id()).copied() {
                        local_market_exposure =
                            local_market_exposure.checked_add(notional, "local market exposure")?;
                    }
                }
                let effective_market_exposure = effective_market_exposure_units(
                    &symbol_snapshot,
                    &market.market_id,
                    local_market_exposure,
                )?;
                let budget = market_budget_view_from_inputs(
                    market.market_id.clone(),
                    &limits,
                    effective_market_exposure,
                );
                market_budgets.insert(market.market_id.clone(), budget);
            }
        }
        let max_entry_notional_usdt = market_budgets
            .values()
            .filter_map(|budget| budget.effective_market_cap_notional)
            .fold(RiskFixed6::default(), RiskFixed6::max);
        let cash = self.trading_collateral_balance.unwrap_or(0.0);
        anyhow::ensure!(
            cash.is_finite() && cash >= 0.0 && !cash.is_sign_negative(),
            "risk pUSD collateral capacity contains an invalid balance"
        );
        let available_collateral = RiskFixed6::capacity(cash, "available pUSD collateral")?;
        let aggregate_inventory_qty = aggregate_actionable_inventory_units(portfolio, now_ms)?;
        let max_inventory_total =
            RiskFixed6::capacity(max_inventory_total, "maximum total inventory quantity")?;
        let remaining_total_inventory_qty =
            max_inventory_total.saturating_sub(aggregate_inventory_qty);
        let available_buy_budget_usdt = available_collateral
            .saturating_sub(open_buy_notional)
            .saturating_sub(inflight_total);
        Ok(RiskBudgetSnapshot {
            updated_at_ms: self.updated_at_ms,
            enabled: self.enabled,
            trading_allowed: self.trading_allowed,
            freeze_level: self.freeze_level,
            frozen_rule: self.frozen_rule.clone(),
            frozen_details: self.frozen_details.clone(),
            max_order_qty: RiskFixed6::capacity(self.max_order_qty, "maximum order quantity")?,
            available_buy_budget_usdt,
            max_entry_notional_usdt,
            max_inventory_total,
            remaining_total_inventory_qty,
            market_budgets,
        })
    }

    pub fn frozen_reason(&self) -> Option<String> {
        match (&self.frozen_rule, &self.frozen_details) {
            (Some(a), Some(b)) => Some(format!("{a}: {b}")),
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (None, None) => None,
        }
    }
}

fn aggregate_actionable_inventory_units(
    portfolio: &crate::portfolio::PortfolioState,
    now_ms: u64,
) -> Result<RiskFixed6> {
    let mut total = RiskFixed6::default();
    for position in portfolio.positions.values().filter(|position| {
        !position.resolved_redeemable
            && position.end_time_ms.is_none_or(|end_time_ms| {
                end_time_ms.saturating_add(EXPIRED_POSITION_INVENTORY_CAP_GRACE_MS) > now_ms
            })
    }) {
        total = total.checked_add(
            RiskFixed6::from_units(
                position.qty_units_6dp,
                &format!("actionable inventory quantity token={}", position.token),
            )?,
            "aggregate actionable inventory quantity",
        )?;
    }
    Ok(total)
}

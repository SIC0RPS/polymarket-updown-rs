use std::collections::HashMap;

use crate::orders::OpenOrdersState;
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::polymarket::state::PolymarketOddsState;
use crate::portfolio::PortfolioState;
use crate::types::{DirectBuyAuthorization, OrderIntent, Side};

use super::market_budget::{
    aggregate_inflight_buy_notional_by_market, direct_buy_order_requirement,
    inflight_buy_cost_units, market_budget_view_at, market_is_active, open_buy_cost_units,
    order_is_active, reserved_direct_buy_order_coverage,
    validate_authoritative_market_exposure_identities,
};
use super::runtime::now_ms;
use super::snapshot::{AuthoritativeMarketExposure, RiskBlock};
use super::{RiskFixed6, RiskHandle};

#[path = "admission/sell_inventory.rs"]
mod sell_inventory;

use sell_inventory::InflightSellReservation;

#[derive(Debug, Clone)]
pub(super) struct InflightBuyReservation {
    pub(super) market_id: MarketId,
    pub(super) notional: f64,
    pub(super) order_id: Option<String>,
    pub(super) authorization: Option<DirectBuyAuthorization>,
    pub(super) limit_px_units_8dp: u64,
    pub(super) confirmed_qty_units_6dp: u128,
    pub(super) terminal_matched_qty_units_6dp: Option<u128>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AdmissionExposureState {
    pub(super) markets: HashMap<MarketId, AuthoritativeMarketExposure>,
    pub(super) inflight_buy_reservations: HashMap<String, InflightBuyReservation>,
    pub(super) inflight_sell_reservations: HashMap<String, InflightSellReservation>,
}

impl AdmissionExposureState {
    pub(super) fn from_captured_snapshot(
        markets: HashMap<MarketId, AuthoritativeMarketExposure>,
    ) -> anyhow::Result<Self> {
        validate_authoritative_market_exposure_identities(&markets)?;
        Ok(Self {
            markets,
            inflight_buy_reservations: HashMap::new(),
            inflight_sell_reservations: HashMap::new(),
        })
    }
}

pub(super) fn invalid_risk_fact(error: anyhow::Error) -> RiskBlock {
    RiskBlock {
        rule: "invalid_risk_fact".to_string(),
        details: format!("{error:#}"),
    }
}

impl RiskHandle {
    pub(super) fn publish_admission_snapshot(
        &self,
        markets: HashMap<MarketId, AuthoritativeMarketExposure>,
        inflight_buy_notional_by_market: HashMap<MarketId, f64>,
    ) {
        self.state_tx.send_modify(|s| {
            s.updated_at_ms = now_ms();
            s.authoritative_market_exposures = markets.clone();
            s.inflight_buy_notional_by_market = inflight_buy_notional_by_market.clone();
        });
    }

    pub fn bind_inflight_buy_order(&self, reservation_id: &str, order_id: impl Into<String>) {
        self.bind_inflight_order(reservation_id, order_id);
    }

    pub fn reserve_inflight_order(
        &self,
        intent: &OrderIntent,
        market_projection: Option<&crate::polymarket::state::MarketProjection>,
    ) -> std::result::Result<String, RiskBlock> {
        let intent_id = intent
            .execution_intent_id
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                invalid_risk_fact(anyhow::anyhow!(
                    "inflight order is missing execution intent identity"
                ))
            })?;
        let reservation_id = format!("execution_intent:{intent_id}");
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        if let Some(existing) = admission.inflight_buy_reservations.get(&reservation_id) {
            let (authorization, _) =
                direct_buy_order_requirement(intent).map_err(invalid_risk_fact)?;
            if existing.authorization == Some(authorization)
                && existing.limit_px_units_8dp == intent.execution_instruction_limit_px_units_8dp
            {
                return Ok(reservation_id);
            }
            return Err(invalid_risk_fact(anyhow::anyhow!(
                "BUY reservation differs from original authorization"
            )));
        }
        if admission
            .inflight_sell_reservations
            .contains_key(&reservation_id)
        {
            return Err(invalid_risk_fact(anyhow::anyhow!(
                "duplicate inflight order reservation execution_intent_id={intent_id}"
            )));
        }
        match intent.side() {
            Side::Buy => {
                let market_id = intent
                    .market_id
                    .as_deref()
                    .map(MarketId::new)
                    .filter(|market| !market.is_empty())
                    .ok_or_else(|| {
                        invalid_risk_fact(anyhow::anyhow!(
                            "inflight BUY is missing market identity execution_intent_id={intent_id}"
                        ))
                    })?;
                let (authorization, requirement) =
                    direct_buy_order_requirement(intent).map_err(invalid_risk_fact)?;
                if self.runtime_config().cfg.enabled {
                    let snap = self.state_tx.borrow();
                    let capacity =
                        snap.usdc_balance
                            .zip(snap.exchange_allowance)
                            .ok_or_else(|| {
                                invalid_risk_fact(anyhow::anyhow!(
                                    "BUY reservation collateral capacity is unavailable"
                                ))
                            })?;
                    let venue = capacity.0.min(capacity.1);
                    let available = RiskFixed6::capacity(
                        snap.trading_collateral_balance
                            .map_or(venue, |cash| cash.min(venue)),
                        "BUY reservation collateral capacity",
                    )
                    .map_err(invalid_risk_fact)?;
                    let required = admission
                        .inflight_buy_reservations
                        .values()
                        .try_fold(requirement, |total, reservation| {
                            total.checked_add(
                                RiskFixed6::consumption(
                                    reservation.notional,
                                    "retained BUY liability",
                                )?,
                                "aggregate BUY reservation liability",
                            )
                        })
                        .map_err(invalid_risk_fact)?;
                    if required > available {
                        return Err(RiskBlock {
                            rule: "balance_exceeded".into(),
                            details: format!(
                                "reserved={:.6} available={:.6}",
                                required.as_f64(),
                                available.as_f64()
                            ),
                        });
                    }
                }
                let notional = requirement.as_f64();
                admission.inflight_buy_reservations.insert(
                    reservation_id.clone(),
                    InflightBuyReservation {
                        market_id,
                        notional,
                        order_id: None,
                        authorization: Some(authorization),
                        limit_px_units_8dp: intent.execution_instruction_limit_px_units_8dp,
                        confirmed_qty_units_6dp: 0,
                        terminal_matched_qty_units_6dp: None,
                    },
                );
            }
            Side::Sell => {
                admission.inflight_sell_reservations.insert(
                    reservation_id.clone(),
                    InflightSellReservation::from_intent(intent, intent_id, market_projection)?,
                );
            }
        }
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
        Ok(reservation_id)
    }

    pub fn bind_inflight_order(&self, reservation_id: &str, order_id: impl Into<String>) {
        let order_id = order_id.into();
        if reservation_id.is_empty() || order_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        if let Some(reservation) = admission.inflight_buy_reservations.get_mut(reservation_id) {
            if reservation.authorization.is_some()
                && reservation
                    .order_id
                    .as_deref()
                    .is_some_and(|existing| existing != order_id)
            {
                tracing::error!(target: "risk.admission", reservation_id, order_id,
                    "direct_buy_order_binding_conflict");
                return;
            }
            reservation.order_id = Some(order_id.clone());
        }
        if let Some(reservation) = admission.inflight_sell_reservations.get_mut(reservation_id) {
            reservation.order_id = Some(order_id);
        }
    }

    pub fn release_inflight_buy_notional(&self, reservation_id: &str) {
        if reservation_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        if admission
            .inflight_buy_reservations
            .get(reservation_id)
            .is_some_and(|reservation| reservation.authorization.is_none())
        {
            admission.inflight_buy_reservations.remove(reservation_id);
        }
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
    }

    pub fn release_inflight_order(&self, reservation_id: &str) {
        if reservation_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        if admission
            .inflight_buy_reservations
            .get(reservation_id)
            .is_some_and(|reservation| {
                reservation.authorization.is_none() || reservation.order_id.is_none()
            })
        {
            admission.inflight_buy_reservations.remove(reservation_id);
        }
        admission.inflight_sell_reservations.remove(reservation_id);
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
    }

    pub fn release_inflight_buy_order(&self, order_id: &str) {
        if order_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        admission
            .inflight_buy_reservations
            .retain(|_, reservation| {
                reservation.authorization.is_some()
                    || reservation.order_id.as_deref() != Some(order_id)
            });
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
    }

    pub fn release_inflight_order_by_order_id(&self, order_id: &str) {
        if order_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        let remove_keys: Vec<String> = admission
            .inflight_buy_reservations
            .iter()
            .filter_map(|(key, reservation)| {
                (reservation.authorization.is_none()
                    && reservation.order_id.as_deref() == Some(order_id))
                .then_some(key.clone())
            })
            .collect();
        for key in remove_keys {
            admission.inflight_buy_reservations.remove(&key);
        }
        admission
            .inflight_sell_reservations
            .retain(|_, reservation| reservation.order_id.as_deref() != Some(order_id));
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
    }

    pub fn release_market_inflight_buy_notional(&self, market_id: &MarketId) {
        if market_id.is_empty() {
            return;
        }
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        admission
            .inflight_buy_reservations
            .retain(|_, reservation| {
                reservation.authorization.is_some() || reservation.market_id != *market_id
            });
        let inflight_buy_notional_by_market =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight_buy_notional_by_market);
        drop(admission);
    }

    pub fn check_order(
        &self,
        intent: &OrderIntent,
        portfolio: &PortfolioState,
        open_orders: &OpenOrdersState,
        selection: Option<&MarketSelection>,
        odds: Option<&PolymarketOddsState>,
        now_ms: u64,
    ) -> Result<(), RiskBlock> {
        let runtime = self.runtime_config();
        let cfg = runtime.cfg;
        let max_order_qty = runtime.max_order_qty;

        if !(intent.qty.is_finite() && intent.qty > 0.0) {
            return Err(RiskBlock {
                rule: "invalid_qty".to_string(),
                details: format!("qty={}", intent.qty),
            });
        }
        let intent_qty = RiskFixed6::consumption(intent.qty, "requested order quantity")
            .map_err(invalid_risk_fact)?;
        if !(max_order_qty.is_finite() && max_order_qty >= 0.0 && !max_order_qty.is_sign_negative())
        {
            return Err(RiskBlock {
                rule: "invalid_risk_fact".to_string(),
                details: format!("maximum order quantity is invalid max={max_order_qty}"),
            });
        }
        if max_order_qty > 0.0 {
            let max_order_qty_units = RiskFixed6::capacity(max_order_qty, "maximum order quantity")
                .map_err(invalid_risk_fact)?;
            if intent_qty > max_order_qty_units {
                return Err(RiskBlock {
                    rule: "max_order_qty".to_string(),
                    details: format!("qty={} max={}", intent.qty, max_order_qty),
                });
            }
        }
        if !(intent.limit_px.is_finite() && intent.limit_px > 0.0) {
            return Err(RiskBlock {
                rule: "invalid_limit_px".to_string(),
                details: format!("limit_px={}", intent.limit_px),
            });
        }

        if !cfg.enabled {
            return Ok(());
        }

        let mut snap = self.state_tx.borrow().clone();
        if !snap.trading_allowed {
            return Err(RiskBlock {
                rule: "circuit_breaker".to_string(),
                details: snap.frozen_reason().unwrap_or_else(|| "frozen".to_string()),
            });
        }
        let (open_count, inflight_count, covered_projected_cash, covered_projected_market) = {
            let admission = self
                .admission
                .read()
                .expect("risk admission exposure lock poisoned");
            let own_id = intent
                .execution_intent_id
                .map(|id| format!("execution_intent:{id}"));
            let own = own_id
                .as_ref()
                .and_then(|id| admission.inflight_buy_reservations.get(id));
            if let Some(own) = own {
                let (authorization, _) =
                    direct_buy_order_requirement(intent).map_err(invalid_risk_fact)?;
                if own.authorization != Some(authorization)
                    || own.limit_px_units_8dp != intent.execution_instruction_limit_px_units_8dp
                {
                    return Err(invalid_risk_fact(anyhow::anyhow!(
                        "BUY risk check authorization changed"
                    )));
                }
            }
            snap.inflight_buy_notional_by_market = aggregate_inflight_buy_notional_by_market(
                admission
                    .inflight_buy_reservations
                    .iter()
                    .filter(|(id, _)| own_id.as_ref() != Some(*id))
                    .map(|(_, value)| value),
            );
            let (covered, cash, market) = reserved_direct_buy_order_coverage(
                &admission,
                open_orders,
                portfolio,
                intent.market_id.as_ref(),
                selection,
                odds,
                now_ms,
            )
            .map_err(invalid_risk_fact)?;
            let active_buys = admission
                .inflight_buy_reservations
                .iter()
                .filter(|(id, reservation)| {
                    own_id.as_ref() != Some(*id)
                        && market_is_active(
                            reservation.authorization.map(|auth| auth.market_end_ms),
                            now_ms,
                        )
                })
                .count();
            let active_sells = admission.active_sell_count(now_ms);
            (
                open_orders
                    .orders
                    .values()
                    .filter(|order| {
                        order_is_active(&admission, order, portfolio, selection, odds, now_ms)
                    })
                    .count()
                    .saturating_sub(covered),
                active_buys + active_sells,
                cash,
                market,
            )
        };
        if open_count.saturating_add(inflight_count) >= cfg.max_open_orders {
            return Err(RiskBlock {
                rule: "max_open_orders".to_string(),
                details: format!(
                    "open_orders={open_count} inflight_orders={inflight_count} max={}",
                    cfg.max_open_orders
                ),
            });
        }

        let order_cost = match intent.side() {
            Side::Buy => {
                direct_buy_order_requirement(intent)
                    .map_err(invalid_risk_fact)?
                    .1
            }
            Side::Sell => RiskFixed6::default(),
        };

        if intent.side() == Side::Buy {
            for (name, value) in [
                ("minimum collateral balance", cfg.min_usdc_balance),
                ("maximum market cost", cfg.max_window_cost),
                (
                    "maximum market equity percentage",
                    cfg.max_market_equity_pct,
                ),
            ] {
                if !(value.is_finite() && value >= 0.0 && !value.is_sign_negative()) {
                    return Err(RiskBlock {
                        rule: "invalid_risk_fact".to_string(),
                        details: format!("{name} is invalid value={value}"),
                    });
                }
            }
            let balance_value = snap.usdc_balance.ok_or_else(|| RiskBlock {
                rule: "balance_unavailable".to_string(),
                details: "current CLOB collateral balance is unavailable".to_string(),
            })?;
            let allowance_value = snap.exchange_allowance.ok_or_else(|| RiskBlock {
                rule: "allowance_unavailable".to_string(),
                details: "current CLOB exchange allowance is unavailable".to_string(),
            })?;
            if !(allowance_value.is_finite()
                && allowance_value >= 0.0
                && !allowance_value.is_sign_negative())
            {
                return Err(RiskBlock {
                    rule: "invalid_risk_fact".to_string(),
                    details: format!(
                        "current CLOB exchange allowance is invalid allowance={allowance_value}"
                    ),
                });
            }
            let balance = RiskFixed6::capacity(balance_value, "current CLOB collateral balance")
                .map_err(invalid_risk_fact)?;
            let available_collateral = RiskFixed6::capacity(
                balance_value.min(allowance_value),
                "current CLOB collateral capacity",
            )
            .map_err(invalid_risk_fact)?;
            if cfg.min_usdc_balance > 0.0 {
                let minimum_balance =
                    RiskFixed6::consumption(cfg.min_usdc_balance, "minimum collateral balance")
                        .map_err(invalid_risk_fact)?;
                if balance < minimum_balance {
                    return Err(RiskBlock {
                        rule: "insufficient_balance".into(),
                        details: format!(
                            "balance={:.6} min={:.6}",
                            balance.as_f64(),
                            minimum_balance.as_f64()
                        ),
                    });
                }
            }
            let projected = open_buy_cost_units(open_orders)
                .and_then(|current| {
                    current.saturating_sub(covered_projected_cash).checked_add(
                        inflight_buy_cost_units(&snap.inflight_buy_notional_by_market)?,
                        "projected collateral consumption",
                    )
                })
                .and_then(|current| {
                    current.checked_add(order_cost, "projected collateral consumption")
                })
                .map_err(invalid_risk_fact)?;
            if projected > available_collateral {
                return Err(RiskBlock {
                    rule: "balance_exceeded".into(),
                    details: format!(
                        "projected={:.6} balance={:.6} allowance={:.6} available={:.6}",
                        projected.as_f64(),
                        balance.as_f64(),
                        allowance_value,
                        available_collateral.as_f64()
                    ),
                });
            }
        }

        self.check_sell_inventory(intent, portfolio, open_orders)?;

        if intent.side() == Side::Buy
            && (cfg.max_window_cost > 0.0 || cfg.max_market_equity_pct > 0.0)
        {
            let resolved_selection = selection
                .filter(|market| {
                    market.up_token_id() == intent.instrument
                        || market.down_token_id() == intent.instrument
                })
                .cloned()
                .or_else(|| {
                    let odds = odds?;
                    odds.markets
                        .iter()
                        .chain(odds.selection.iter())
                        .chain(odds.next_selection.iter())
                        .find(|market| {
                            market.up_token.to_string() == intent.instrument
                                || market.down_token.to_string() == intent.instrument
                        })
                        .map(crate::polymarket::state::convert_selection_snapshot)
                })
                .ok_or_else(|| RiskBlock {
                    rule: "market_exposure_cap".to_string(),
                    details: format!(
                        "token={} missing_market_resolution max_window_cost={:.6} max_market_equity_pct={:.6}",
                        intent.instrument, cfg.max_window_cost, cfg.max_market_equity_pct
                    ),
                })?;
            let mut risk_view = snap.clone();
            risk_view.max_window_cost = cfg.max_window_cost;
            risk_view.max_market_equity_pct = cfg.max_market_equity_pct;
            let budget = market_budget_view_at(
                &risk_view,
                portfolio,
                open_orders,
                &resolved_selection,
                covered_projected_market,
                now_ms,
            )
            .map_err(invalid_risk_fact)?;
            let effective_cap_notional =
                budget
                    .effective_market_cap_notional
                    .ok_or_else(|| RiskBlock {
                        rule: "invalid_risk_fact".to_string(),
                        details: format!(
                            "market cap is unavailable market_id={}",
                            budget.market_id
                        ),
                    })?;
            let market_notional = budget
                .effective_market_exposure
                .checked_add(order_cost, "projected market exposure")
                .map_err(invalid_risk_fact)?;
            if market_notional > effective_cap_notional {
                return Err(RiskBlock {
                    rule: "market_exposure_cap".to_string(),
                    details: format!(
                        "market_id={} effective_market_exposure={:.6} order_notional={:.6} market_notional={:.6} effective_cap_notional={:.6} binding_rule={}",
                        budget.market_id,
                        budget.effective_market_exposure.as_f64(),
                        order_cost.as_f64(),
                        market_notional.as_f64(),
                        effective_cap_notional.as_f64(),
                        budget.binding_rule.as_deref().unwrap_or("unavailable")
                    ),
                });
            }
        }

        Ok(())
    }
}

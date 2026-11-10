use std::collections::HashMap;

use crate::orders::{OpenOrderState, OpenOrdersState};
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::portfolio::PortfolioState;
use crate::types::{DirectBuyAuthorization, OrderIntent, Side};
use anyhow::Result;

use super::admission::InflightBuyReservation;
use super::snapshot::{
    market_budget_limits, market_budget_view_from_inputs, AuthoritativeMarketExposure,
    MarketBudgetView, RiskSnapshot,
};
use super::RiskFixed6;

pub(super) fn validate_market_id(market_id: &MarketId, owner: &str) -> Result<()> {
    let raw = market_id.as_str();
    anyhow::ensure!(
        !raw.is_empty() && raw != "-" && raw.trim() == raw,
        "{owner} has invalid market identity market_id={raw:?}"
    );
    Ok(())
}

pub(super) fn validate_authoritative_market_exposure_identities(
    exposures: &HashMap<MarketId, AuthoritativeMarketExposure>,
) -> Result<()> {
    for (map_key, exposure) in exposures {
        validate_market_id(map_key, "authoritative market exposure map key")?;
        validate_market_id(
            &exposure.market_id,
            "authoritative market exposure embedded identity",
        )?;
        anyhow::ensure!(
            exposure.market_id == *map_key,
            "authoritative market exposure identity conflict map_key={} exposure_market_id={}",
            map_key,
            exposure.market_id
        );
    }
    Ok(())
}

pub(super) fn validate_inflight_market_identities(
    inflight_buy_notional_by_market: &HashMap<MarketId, f64>,
) -> Result<()> {
    for market_id in inflight_buy_notional_by_market.keys() {
        validate_market_id(market_id, "inflight buy exposure map key")?;
    }
    Ok(())
}

pub(super) fn is_reservation_owned_direct_buy(order: &OpenOrderState) -> bool {
    order.status != "PROJECTED" && has_canonical_direct_buy_identity(order)
}

fn has_canonical_direct_buy_identity(order: &OpenOrderState) -> bool {
    if order.side != Side::Buy || order.order_id.is_empty() {
        return false;
    }
    let Some(intent_id) = order.execution_intent_id.filter(|id| *id > 0) else {
        return false;
    };
    let Ok(condition) = order.market.parse::<polymarket_client_sdk::types::B256>() else {
        return false;
    };
    let Ok(token) = order.token.parse::<polymarket_client_sdk::types::U256>() else {
        return false;
    };
    token != polymarket_client_sdk::types::U256::ZERO
        && crate::ctf_inventory::confirmed_direct_buy_cohort_id(condition, intent_id)
            .is_ok_and(|cohort| order.lifecycle_cohort.as_deref() == Some(cohort.as_str()))
}

pub(super) fn direct_buy_order_requirement(
    intent: &OrderIntent,
) -> Result<(DirectBuyAuthorization, RiskFixed6)> {
    let authorization = intent
        .buy_authorization()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("BUY has no central authorization"))?;
    authorization.validate()?;
    let qty = crate::ctf_accounting::qty_to_units_6dp(intent.qty)?;
    anyhow::ensure!(
        intent.execution_intent_id == Some(authorization.intent_id)
            && intent.market_id.as_ref().is_some_and(|market| {
                market.as_str() == authorization.condition_id.to_string()
            })
            && intent.instrument == authorization.selected_token().to_string()
            && intent.lifecycle_cohort.as_deref()
                == Some(
                    crate::ctf_inventory::confirmed_direct_buy_cohort_id(
                        authorization.condition_id,
                        authorization.intent_id,
                    )?
                    .as_str()
                )
            && qty == authorization.target_qty_units_6dp
            && qty == intent.execution_instruction_qty_units_6dp
            && crate::polymarket::fixed_math::price_to_units_8dp(intent.limit_px)
                == Some(intent.execution_instruction_limit_px_units_8dp),
        "BUY instruction differs from central authorization"
    );
    let cash = crate::polymarket::fills::direct_buy_cash_requirement_units_6dp(
        qty,
        intent.execution_instruction_limit_px_units_8dp,
        authorization.fee_rate_bps(),
    )?;
    Ok((
        authorization,
        RiskFixed6::from_units(cash, "authorized BUY cash requirement")?,
    ))
}

pub(super) fn market_is_active(market_end_ms: Option<u64>, now_ms: u64) -> bool {
    market_end_ms
        .filter(|end| *end > 0)
        .is_none_or(|end| now_ms < end)
}

pub(super) fn order_is_active(
    admission: &super::admission::AdmissionExposureState,
    order: &OpenOrderState,
    portfolio: &PortfolioState,
    selection: Option<&MarketSelection>,
    odds: Option<&crate::polymarket::state::PolymarketOddsState>,
    now_ms: u64,
) -> bool {
    let end = admission
        .order_market_end_ms(order, portfolio)
        .or_else(|| {
            selection
                .filter(|market| {
                    market.market_id.as_str() == order.market
                        && (market.up_token_id() == order.token
                            || market.down_token_id() == order.token)
                })
                .map(|market| market.window_end_ms)
        })
        .or_else(|| {
            let odds = odds?;
            odds.markets
                .iter()
                .chain(odds.selection.iter())
                .chain(odds.next_selection.iter())
                .find(|market| {
                    market.market_id == order.market
                        && (market.up_token.to_string() == order.token
                            || market.down_token.to_string() == order.token)
                })
                .map(|market| market.window_end_ms)
        });
    market_is_active(end, now_ms)
}

pub(super) fn reserved_direct_buy_order_coverage(
    admission: &super::admission::AdmissionExposureState,
    orders: &OpenOrdersState,
    portfolio: &PortfolioState,
    market: Option<&MarketId>,
    selection: Option<&MarketSelection>,
    odds: Option<&crate::polymarket::state::PolymarketOddsState>,
    now_ms: u64,
) -> Result<(usize, RiskFixed6, RiskFixed6)> {
    let mut count = 0;
    let mut projected_cash = RiskFixed6::default();
    let mut projected_market = RiskFixed6::default();
    for order in orders
        .orders
        .values()
        .filter(|order| has_canonical_direct_buy_identity(order))
    {
        let active = usize::from(order_is_active(
            admission, order, portfolio, selection, odds, now_ms,
        ));
        let projected = order.status == "PROJECTED";
        let reservation = admission
            .inflight_buy_reservations
            .values()
            .find(|reservation| {
                reservation
                    .authorization
                    .is_some_and(|auth| Some(auth.intent_id) == order.execution_intent_id)
            });
        let Some(reservation) = reservation else {
            if !projected
                && portfolio.confirmed_direct_buy_receipts().any(|fact| {
                    fact.order_id == order.order_id
                        && fact.token == order.token
                        && fact.market_id.as_str() == order.market
                        && Some(fact.authorization.intent_id) == order.execution_intent_id
                        && crate::ctf_accounting::qty_to_units_6dp(order.original_size)
                            .is_ok_and(|qty| qty == fact.authorization.target_qty_units_6dp)
                        && portfolio.confirmed_direct_buy_qty_units_6dp(&fact.cohort_id)
                            == fact.authorization.target_qty_units_6dp
                })
            {
                count += active;
                continue;
            }
            anyhow::ensure!(projected, "open BUY has no retained reservation");
            continue;
        };
        let authorization = reservation
            .authorization
            .ok_or_else(|| anyhow::anyhow!("open BUY reservation has no authorization"))?;
        anyhow::ensure!(
            (projected || reservation.order_id.as_deref() == Some(order.order_id.as_str()))
                && reservation.market_id.as_str() == order.market
                && authorization.selected_token().to_string() == order.token
                && crate::ctf_accounting::qty_to_units_6dp(order.original_size)?
                    == authorization.target_qty_units_6dp
                && crate::polymarket::fixed_math::price_to_units_8dp(order.px)
                    == Some(reservation.limit_px_units_8dp),
            "open BUY differs from retained reservation"
        );
        if projected {
            let cost = open_buy_order_notional_units(order)?.unwrap_or_default();
            projected_cash = projected_cash.checked_add(cost, "reserved projected BUY cost")?;
            if market.is_some_and(|market| market.as_str() == order.market) {
                projected_market =
                    projected_market.checked_add(cost, "reserved projected market cost")?;
            }
        }
        count += active;
    }
    Ok((count, projected_cash, projected_market))
}

pub(crate) fn open_buy_cost_units(open_orders: &OpenOrdersState) -> Result<RiskFixed6> {
    let mut total = RiskFixed6::default();
    for order in open_orders.orders.values() {
        if let Some(notional) = open_buy_order_notional_units(order)? {
            total = total.checked_add(notional, "aggregate open buy notional")?;
        }
    }
    Ok(total)
}

pub(super) fn open_buy_order_notional_units(order: &OpenOrderState) -> Result<Option<RiskFixed6>> {
    if order.side != Side::Buy || is_reservation_owned_direct_buy(order) {
        return Ok(None);
    }
    anyhow::ensure!(
        order.original_size.is_finite()
            && order.original_size >= 0.0
            && !order.original_size.is_sign_negative()
            && order.matched_size.is_finite()
            && order.matched_size >= 0.0
            && !order.matched_size.is_sign_negative()
            && order.matched_size <= order.original_size
            && order.px.is_finite()
            && order.px >= 0.0
            && !order.px.is_sign_negative(),
        "open buy risk fact is invalid order_id={}",
        order.order_id
    );
    let original_notional = RiskFixed6::consumption_product(
        order.original_size,
        order.px,
        &format!("open buy original notional order_id={}", order.order_id),
    )?;
    let matched_notional = RiskFixed6::capacity_product(
        order.matched_size,
        order.px,
        &format!("open buy matched notional order_id={}", order.order_id),
    )?;
    Ok(Some(original_notional.saturating_sub(matched_notional)))
}

pub(crate) fn market_budget_view_at(
    snap: &RiskSnapshot,
    portfolio: &PortfolioState,
    open_orders: &OpenOrdersState,
    selection: &MarketSelection,
    covered_projected_cost: RiskFixed6,
    now_ms: u64,
) -> Result<MarketBudgetView> {
    let market_id = selection.market_id.clone();
    validate_market_id(&market_id, "market budget selection")?;
    validate_authoritative_market_exposure_identities(&snap.authoritative_market_exposures)?;
    validate_inflight_market_identities(&snap.inflight_buy_notional_by_market)?;
    let local_open_cohort_outlays = portfolio.active_cohort_committed_outlay_units_6dp(now_ms)?;
    let mut local_market_exposure = RiskFixed6::from_units(
        local_open_cohort_outlays
            .get(&market_id)
            .copied()
            .unwrap_or(0),
        "local canonical open-cohort committed outlay",
    )?;
    for order in open_orders.orders.values().filter(|order| {
        order.token == selection.up_token_id() || order.token == selection.down_token_id()
    }) {
        if let Some(notional) = open_buy_order_notional_units(order)? {
            local_market_exposure =
                local_market_exposure.checked_add(notional, "local market exposure")?;
        }
    }
    let effective_market_exposure = effective_market_exposure_units(
        snap,
        &market_id,
        local_market_exposure.saturating_sub(covered_projected_cost),
    )?;
    let limits = market_budget_limits(snap)?;
    Ok(market_budget_view_from_inputs(
        market_id,
        &limits,
        effective_market_exposure,
    ))
}

pub(super) fn effective_market_exposure_units(
    snap: &RiskSnapshot,
    market_id: &MarketId,
    local_market_exposure: RiskFixed6,
) -> Result<RiskFixed6> {
    let authoritative = snap
        .authoritative_market_exposures
        .get(market_id)
        .cloned()
        .unwrap_or_else(|| AuthoritativeMarketExposure {
            market_id: market_id.clone(),
            ..Default::default()
        });
    let inflight_buy_notional = snap
        .inflight_buy_notional_by_market
        .get(market_id)
        .copied()
        .unwrap_or(0.0);
    RiskFixed6::from_units(
        authoritative.open_cohort_committed_outlay_units_6dp,
        "canonical open-cohort committed outlay",
    )?
    .checked_add(
        RiskFixed6::consumption(
            authoritative.open_buy_notional,
            "authoritative open buy notional",
        )?,
        "authoritative market exposure",
    )?
    .max(local_market_exposure)
    .checked_add(
        RiskFixed6::consumption(inflight_buy_notional, "inflight buy notional")?,
        "effective market exposure",
    )
}

pub(super) fn local_open_cohort_costs(
    portfolio: &PortfolioState,
    now_ms: u64,
) -> Result<HashMap<MarketId, RiskFixed6>> {
    portfolio
        .active_cohort_committed_outlay_units_6dp(now_ms)?
        .into_iter()
        .map(|(market_id, outlay_units_6dp)| {
            RiskFixed6::from_units(
                outlay_units_6dp,
                &format!("local canonical open-cohort committed outlay market_id={market_id}"),
            )
            .map(|outlay| (market_id, outlay))
        })
        .collect()
}

pub(super) fn aggregate_inflight_buy_notional_by_market<'a>(
    reservations: impl Iterator<Item = &'a InflightBuyReservation>,
) -> HashMap<MarketId, f64> {
    let mut aggregated = HashMap::new();
    for reservation in reservations {
        if !(reservation.notional.is_finite() && reservation.notional > 0.0) {
            continue;
        }
        let entry = aggregated
            .entry(reservation.market_id.clone())
            .or_insert(0.0);
        *entry += reservation.notional;
    }
    aggregated
}

pub(crate) fn inflight_buy_cost_units(
    inflight_buy_notional_by_market: &HashMap<MarketId, f64>,
) -> Result<RiskFixed6> {
    validate_inflight_market_identities(inflight_buy_notional_by_market)?;
    let mut total = RiskFixed6::default();
    for (market_id, value) in inflight_buy_notional_by_market {
        total = total.checked_add(
            RiskFixed6::consumption(
                *value,
                &format!("inflight buy notional market_id={market_id}"),
            )?,
            "aggregate inflight buy notional",
        )?;
    }
    Ok(total)
}

impl super::RiskHandle {
    pub fn confirm_direct_buy_acquisitions(&self, portfolio: &PortfolioState) -> Result<()> {
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        let mut changed = false;
        let result = (|| -> Result<()> {
            let mut receipts = portfolio.confirmed_direct_buy_receipts().peekable();
            while let Some(first) = receipts.next() {
                let mut confirmed = first.qty_units_6dp;
                while receipts
                    .peek()
                    .is_some_and(|next| next.cohort_id == first.cohort_id)
                {
                    let next = receipts.next().expect("peeked BUY receipt");
                    anyhow::ensure!(
                        next.authorization == first.authorization
                            && next.order_id == first.order_id
                            && next.market_id == first.market_id
                            && next.token == first.token,
                        "canonical BUY cohort changes original order identity"
                    );
                    confirmed = confirmed
                        .checked_add(next.qty_units_6dp)
                        .ok_or_else(|| anyhow::anyhow!("confirmed BUY quantity overflow"))?;
                }
                let id = format!("execution_intent:{}", first.authorization.intent_id);
                let Some(reservation) = admission.inflight_buy_reservations.get_mut(&id) else {
                    continue;
                };
                anyhow::ensure!(
                    reservation.authorization == Some(first.authorization)
                        && reservation.market_id == first.market_id
                        && reservation
                            .order_id
                            .as_deref()
                            .is_none_or(|id| id == first.order_id),
                    "confirmed BUY differs from retained reservation"
                );
                anyhow::ensure!(
                    confirmed >= reservation.confirmed_qty_units_6dp,
                    "coherent BUY confirmation quantity regressed"
                );
                if confirmed == reservation.confirmed_qty_units_6dp {
                    continue;
                }
                let remaining = reservation
                    .terminal_matched_qty_units_6dp
                    .unwrap_or(first.authorization.target_qty_units_6dp)
                    .checked_sub(confirmed)
                    .ok_or_else(|| anyhow::anyhow!("confirmed BUY exceeds authorized quantity"))?;
                let cash = crate::polymarket::fills::direct_buy_cash_requirement_units_6dp(
                    remaining,
                    reservation.limit_px_units_8dp,
                    first.authorization.fee_rate_bps(),
                )?;
                let cash = RiskFixed6::from_units(cash, "remaining BUY liability")?;
                reservation.confirmed_qty_units_6dp = confirmed;
                reservation.order_id = Some(first.order_id.clone());
                reservation.notional = cash.as_f64();
                if remaining == 0 {
                    admission.inflight_buy_reservations.remove(&id);
                }
                changed = true;
            }
            Ok(())
        })();
        if changed {
            let inflight = aggregate_inflight_buy_notional_by_market(
                admission.inflight_buy_reservations.values(),
            );
            let markets = admission.markets.clone();
            self.publish_admission_snapshot(markets, inflight);
            drop(admission);
        }
        result
    }

    pub fn unresolved_direct_buy_qty_units_6dp_for_conditions(
        &self,
        conditions: &std::collections::HashSet<polymarket_client_sdk::types::B256>,
    ) -> Result<u128> {
        let admission = self
            .admission
            .read()
            .expect("risk admission exposure lock poisoned");
        admission
            .inflight_buy_reservations
            .values()
            .try_fold(0u128, |total, reservation| {
                let Some(authorization) = reservation.authorization else {
                    return Ok(total);
                };
                if !conditions.contains(&authorization.condition_id) {
                    return Ok(total);
                }
                let pending = reservation
                    .terminal_matched_qty_units_6dp
                    .unwrap_or(authorization.target_qty_units_6dp)
                    .checked_sub(reservation.confirmed_qty_units_6dp)
                    .ok_or_else(|| anyhow::anyhow!("unresolved BUY quantity underflow"))?;
                total
                    .checked_add(pending)
                    .ok_or_else(|| anyhow::anyhow!("unresolved BUY quantity overflow"))
            })
    }

    pub fn abandon_unsubmitted_direct_buy(&self, intent: &OrderIntent) -> Result<bool> {
        if intent.side() != Side::Buy {
            return Ok(false);
        }
        let authorization = intent
            .buy_authorization()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("abandoned BUY has no central authorization"))?;
        authorization.validate()?;
        anyhow::ensure!(
            intent.execution_intent_id == Some(authorization.intent_id)
                && intent.execution_instruction_qty_units_6dp == authorization.target_qty_units_6dp
                && intent.instrument == authorization.selected_token().to_string()
                && intent.market_id.as_ref().is_some_and(|market| {
                    market.as_str() == authorization.condition_id.to_string()
                }),
            "abandoned BUY differs from immutable authorization"
        );
        let id = format!("execution_intent:{}", authorization.intent_id);
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        if let Some(reservation) = admission.inflight_buy_reservations.get(&id) {
            anyhow::ensure!(
                reservation.authorization == Some(authorization)
                    && reservation.limit_px_units_8dp
                        == intent.execution_instruction_limit_px_units_8dp,
                "cannot abandon a conflicting BUY"
            );
            if reservation.order_id.is_some()
                || reservation.confirmed_qty_units_6dp > 0
                || reservation.terminal_matched_qty_units_6dp.is_some()
            {
                return Ok(false);
            }
            admission.inflight_buy_reservations.remove(&id);
            let inflight = aggregate_inflight_buy_notional_by_market(
                admission.inflight_buy_reservations.values(),
            );
            let markets = admission.markets.clone();
            self.publish_admission_snapshot(markets, inflight);
            drop(admission);
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn reconcile_direct_buy_order(
        &self,
        authorization: DirectBuyAuthorization,
        update: Option<&crate::orders::OrderUpdateEvent>,
        pending_matched_qty_units_6dp: u128,
        published_confirmed_qty_units_6dp: u128,
    ) -> Result<bool> {
        authorization.validate()?;
        let known_matched = published_confirmed_qty_units_6dp
            .checked_add(pending_matched_qty_units_6dp)
            .ok_or_else(|| anyhow::anyhow!("BUY matched quantity overflow"))?;
        anyhow::ensure!(
            known_matched <= authorization.target_qty_units_6dp,
            "BUY matched evidence exceeds authorized quantity"
        );
        let id = format!("execution_intent:{}", authorization.intent_id);
        let mut admission = self
            .admission
            .write()
            .expect("risk admission exposure lock poisoned");
        let Some(reservation) = admission.inflight_buy_reservations.get_mut(&id) else {
            anyhow::ensure!(
                pending_matched_qty_units_6dp == 0,
                "unconfirmed BUY match has no reservation"
            );
            return Ok(true);
        };
        anyhow::ensure!(
            reservation.authorization == Some(authorization),
            "BUY terminal evidence differs from original authorization"
        );
        let terminal_matched = if let Some(update) = update.filter(|update| update.is_closed()) {
            anyhow::ensure!(
                update.side == Side::Buy
                    && update.token == authorization.selected_token().to_string()
                    && update.market == authorization.condition_id.to_string()
                    && reservation.order_id.as_deref() == Some(update.order_id.as_str())
                    && update.ts_ms >= authorization.authorized_at_ms,
                "BUY closing update identity differs from reservation"
            );
            let reported = update
                .matched_size
                .ok_or_else(|| anyhow::anyhow!("BUY closing update has no matched quantity"))?;
            let reported = if reported == 0.0 {
                0
            } else {
                crate::ctf_accounting::qty_to_units_6dp(reported)?
            };
            if let Some(original) = update.original_size {
                anyhow::ensure!(
                    crate::ctf_accounting::qty_to_units_6dp(original)?
                        == authorization.target_qty_units_6dp,
                    "BUY closing update original quantity changed"
                );
            }
            let matched = reported
                .max(known_matched)
                .max(reservation.terminal_matched_qty_units_6dp.unwrap_or(0));
            Some(matched)
        } else {
            reservation
                .terminal_matched_qty_units_6dp
                .map(|matched| matched.max(known_matched))
        };
        let Some(terminal_matched) = terminal_matched else {
            return Ok(published_confirmed_qty_units_6dp == authorization.target_qty_units_6dp);
        };
        anyhow::ensure!(
            terminal_matched <= authorization.target_qty_units_6dp,
            "BUY terminal matched quantity exceeds authorization"
        );
        let complete = pending_matched_qty_units_6dp == 0
            && published_confirmed_qty_units_6dp == terminal_matched;
        if reservation.terminal_matched_qty_units_6dp == Some(terminal_matched) {
            return Ok(complete);
        }
        let remaining = terminal_matched
            .checked_sub(reservation.confirmed_qty_units_6dp)
            .ok_or_else(|| anyhow::anyhow!("BUY terminal quantity underflow"))?;
        let cash = crate::polymarket::fills::direct_buy_cash_requirement_units_6dp(
            remaining,
            reservation.limit_px_units_8dp,
            authorization.fee_rate_bps(),
        )?;
        let cash = RiskFixed6::from_units(cash, "terminal BUY liability")?;
        reservation.terminal_matched_qty_units_6dp = Some(terminal_matched);
        reservation.notional = cash.as_f64();
        if remaining == 0 {
            admission.inflight_buy_reservations.remove(&id);
        }
        let inflight =
            aggregate_inflight_buy_notional_by_market(admission.inflight_buy_reservations.values());
        let markets = admission.markets.clone();
        self.publish_admission_snapshot(markets, inflight);
        drop(admission);
        Ok(complete)
    }
}

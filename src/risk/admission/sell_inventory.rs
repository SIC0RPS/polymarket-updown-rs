use super::*;

#[derive(Debug, Clone)]
pub(in crate::risk) struct InflightSellReservation {
    pub(super) token: String,
    pub(super) qty: f64,
    pub(super) order_id: Option<String>,
    pub(super) market_end_ms: Option<u64>,
}

impl InflightSellReservation {
    pub(super) fn from_intent(
        intent: &OrderIntent,
        intent_id: u64,
        market_projection: Option<&crate::polymarket::state::MarketProjection>,
    ) -> Result<Self, RiskBlock> {
        if !(intent.qty.is_finite() && intent.qty > 0.0) {
            return Err(invalid_risk_fact(anyhow::anyhow!(
                "inflight SELL quantity is invalid execution_intent_id={intent_id} qty={}",
                intent.qty
            )));
        }
        let market_end_ms = market_projection
            .zip(intent.market_id.as_ref())
            .and_then(|(projection, market_id)| {
                projection.market_for_token(&intent.instrument, market_id.as_str())
            })
            .map(|market| market.window_end_ms);
        Ok(Self {
            token: intent.instrument.clone(),
            qty: intent.qty,
            order_id: None,
            market_end_ms,
        })
    }
}

impl AdmissionExposureState {
    pub(super) fn active_sell_count(&self, now_ms: u64) -> usize {
        self.inflight_sell_reservations
            .values()
            .filter(|reservation| market_is_active(reservation.market_end_ms, now_ms))
            .count()
    }

    pub(in crate::risk) fn order_market_end_ms(
        &self,
        order: &crate::orders::OpenOrderState,
        portfolio: &PortfolioState,
    ) -> Option<u64> {
        let reservation_end = order.execution_intent_id.and_then(|intent_id| {
            let id = format!("execution_intent:{intent_id}");
            match order.side {
                Side::Buy => self
                    .inflight_buy_reservations
                    .get(&id)
                    .filter(|reservation| {
                        reservation.market_id.as_str() == order.market
                            && (order.status == "PROJECTED"
                                || reservation
                                    .order_id
                                    .as_deref()
                                    .is_none_or(|id| id == order.order_id))
                    })
                    .and_then(|reservation| reservation.authorization)
                    .filter(|authorization| {
                        authorization.selected_token().to_string() == order.token
                    })
                    .map(|authorization| authorization.market_end_ms),
                Side::Sell => self
                    .inflight_sell_reservations
                    .get(&id)
                    .filter(|reservation| {
                        reservation.token == order.token
                            && (order.status == "PROJECTED"
                                || reservation
                                    .order_id
                                    .as_deref()
                                    .is_none_or(|id| id == order.order_id))
                    })
                    .and_then(|reservation| reservation.market_end_ms),
            }
        });
        reservation_end.or_else(|| {
            portfolio
                .confirmed_direct_buy_entries()
                .find(|fact| {
                    fact.market_id.as_str() == order.market
                        && fact.token == order.token
                        && order.lifecycle_cohort.as_deref() == Some(fact.cohort_id.as_str())
                        && (order.side == Side::Sell
                            || (Some(fact.authorization.intent_id) == order.execution_intent_id
                                && fact.order_id == order.order_id))
                })
                .map(|fact| fact.authorization.market_end_ms)
        })
    }
}

impl RiskHandle {
    pub fn check_sell_inventory(
        &self,
        intent: &OrderIntent,
        portfolio: &PortfolioState,
        open_orders: &OpenOrdersState,
    ) -> std::result::Result<(), RiskBlock> {
        if intent.side() != Side::Sell {
            return Ok(());
        }

        if let Some(entry) = portfolio.confirmed_direct_buy_entries().find(|entry| {
            intent.lifecycle_cohort.as_deref() == Some(entry.cohort_id.as_str())
                && intent.market_id.as_ref() == Some(&entry.market_id)
                && intent.instrument == entry.token
        }) {
            let conditions = std::collections::HashSet::from([entry.authorization.condition_id]);
            let unresolved_qty_units_6dp = self
                .unresolved_direct_buy_qty_units_6dp_for_conditions(&conditions)
                .map_err(invalid_risk_fact)?;
            if unresolved_qty_units_6dp > 0 {
                return Err(RiskBlock {
                    rule: "buy_acquisition_unresolved".into(),
                    details: format!(
                        "cohort={} unresolved_qty_units_6dp={unresolved_qty_units_6dp}",
                        entry.cohort_id
                    ),
                });
            }
        }

        let position_qty = portfolio
            .position(&intent.instrument)
            .map(|position| {
                RiskFixed6::from_units(
                    position.sellable_qty_units_6dp,
                    &format!("sellable inventory token={}", intent.instrument),
                )
            })
            .transpose()
            .map_err(invalid_risk_fact)?
            .unwrap_or_default();
        let mut reserved_sell_qty = RiskFixed6::default();
        for order in open_orders
            .orders
            .values()
            .filter(|order| order.side == Side::Sell && order.token == intent.instrument)
        {
            if order.matched_size > order.original_size {
                return Err(invalid_risk_fact(anyhow::anyhow!(
                    "open SELL matched quantity exceeds original order_id={} original={} matched={}",
                    order.order_id,
                    order.original_size,
                    order.matched_size
                )));
            }
            let original_size = RiskFixed6::consumption(
                order.original_size,
                &format!("open SELL original quantity order_id={}", order.order_id),
            )
            .map_err(invalid_risk_fact)?;
            let matched_size = RiskFixed6::capacity(
                order.matched_size,
                &format!("open SELL matched quantity order_id={}", order.order_id),
            )
            .map_err(invalid_risk_fact)?;
            reserved_sell_qty = reserved_sell_qty
                .checked_add(
                    original_size.saturating_sub(matched_size),
                    "reserved open SELL quantity",
                )
                .map_err(invalid_risk_fact)?;
        }
        let admission = self
            .admission
            .read()
            .expect("risk admission exposure lock poisoned");
        for reservation in admission
            .inflight_sell_reservations
            .values()
            .filter(|reservation| reservation.token == intent.instrument)
        {
            reserved_sell_qty = reserved_sell_qty
                .checked_add(
                    RiskFixed6::consumption(
                        reservation.qty,
                        &format!("inflight SELL quantity token={}", intent.instrument),
                    )
                    .map_err(invalid_risk_fact)?,
                    "reserved inflight SELL quantity",
                )
                .map_err(invalid_risk_fact)?;
        }
        drop(admission);
        let available_sell_qty = position_qty.saturating_sub(reserved_sell_qty);
        let requested_sell_qty = if intent.execution_instruction_qty_units_6dp > 0 {
            let actual_units =
                crate::ctf_accounting::qty_to_units_6dp(intent.qty).map_err(invalid_risk_fact)?;
            if actual_units != intent.execution_instruction_qty_units_6dp {
                return Err(invalid_risk_fact(anyhow::anyhow!(
                    "managed SELL quantity differs from canonical instruction owner_units={} actual_units={actual_units}",
                    intent.execution_instruction_qty_units_6dp
                )));
            }
            RiskFixed6::from_units(
                intent.execution_instruction_qty_units_6dp,
                "canonical managed SELL instruction quantity",
            )
            .map_err(invalid_risk_fact)?
        } else {
            RiskFixed6::consumption(intent.qty, "requested SELL quantity")
                .map_err(invalid_risk_fact)?
        };
        if requested_sell_qty > available_sell_qty {
            return Err(RiskBlock {
                rule: "inventory_exceeded".into(),
                details: format!(
                    "token={} qty={:.4} available={:.4} position={:.4} reserved={:.4}",
                    intent.instrument,
                    requested_sell_qty.as_f64(),
                    available_sell_qty.as_f64(),
                    position_qty.as_f64(),
                    reserved_sell_qty.as_f64()
                ),
            });
        }

        Ok(())
    }
}

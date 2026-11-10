use anyhow::Context as _;

use crate::orders::OpenOrdersState;
use crate::portfolio::PortfolioState;
use crate::types::{DirectBuyAuthorization, Side};

use super::OrderLifecycleSupervisor;

pub(super) fn published_buy_quantity(
    portfolio: &PortfolioState,
    authorization: DirectBuyAuthorization,
) -> anyhow::Result<u128> {
    let cohort = crate::ctf_inventory::confirmed_direct_buy_cohort_id(
        authorization.condition_id,
        authorization.intent_id,
    )?;
    Ok(portfolio.confirmed_direct_buy_qty_units_6dp(&cohort))
}

impl OrderLifecycleSupervisor {
    pub(crate) fn reconcile_confirmed_direct_buys(
        &mut self,
        portfolio: &PortfolioState,
        risk: &crate::risk::RiskHandle,
        contexts: &crate::polymarket::order_context::OrderFillContextRegistry,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> anyhow::Result<bool> {
        let mut cohort = None;
        let mut complete = false;
        for fact in portfolio.confirmed_direct_buy_receipts() {
            if cohort != Some(fact.cohort_id.as_str()) {
                cohort = Some(fact.cohort_id.as_str());
                complete = risk.reconcile_direct_buy_order(
                    fact.authorization,
                    None,
                    self.matched_execution
                        .pending_buy_quantity(fact.authorization)?,
                    portfolio.confirmed_direct_buy_qty_units_6dp(&fact.cohort_id),
                )?;
            }
            if complete
                && contexts.direct_buy_authorization(&fact.order_id) == Some(fact.authorization)
            {
                self.note_execution_complete(&fact.order_id);
            }
        }
        let mut corrections = Vec::new();
        for (order_id, order) in &open_orders.orders {
            if order.side != Side::Buy {
                continue;
            }
            let mut receipts = portfolio
                .confirmed_direct_buy_receipts()
                .filter(|fact| fact.order_id == *order_id);
            let Some(first) = receipts.next() else {
                continue;
            };
            let authorization = first.authorization;
            anyhow::ensure!(
                order.order_id == *order_id
                    && order.execution_intent_id == Some(authorization.intent_id)
                    && order.market == first.market_id.as_str()
                    && order.token == first.token
                    && order.lifecycle_cohort.as_deref() == Some(first.cohort_id.as_str())
                    && order.lifecycle_symbol.as_deref() == Some(authorization.symbol.as_str())
                    && crate::ctf_accounting::qty_to_units_6dp(order.original_size)?
                        == authorization.target_qty_units_6dp
                    && contexts
                        .direct_buy_authorization(order_id)
                        .is_none_or(|expected| expected == authorization),
                "resident BUY differs from confirmed order authorization"
            );
            let confirmed = std::iter::once(first).chain(receipts).try_fold(
                0_u128,
                |total, fact| -> anyhow::Result<u128> {
                    anyhow::ensure!(
                        fact.authorization == authorization,
                        "confirmed BUY order has conflicting authorizations"
                    );
                    total
                        .checked_add(fact.qty_units_6dp)
                        .context("confirmed BUY order quantity overflow")
                },
            )?;
            anyhow::ensure!(
                confirmed <= authorization.target_qty_units_6dp,
                "confirmed BUY order exceeds authorized quantity"
            );
            if confirmed == authorization.target_qty_units_6dp {
                corrections.push((order_id.clone(), None));
            } else {
                let confirmed = crate::ctf_accounting::units_6dp_to_f64(
                    i128::try_from(confirmed)
                        .context("confirmed BUY quantity exceeds signed units")?,
                );
                let matched = order.matched_size.max(confirmed);
                if matched > order.matched_size {
                    corrections.push((order_id.clone(), Some(matched)));
                }
            }
        }
        let changed = !corrections.is_empty();
        for (order_id, matched) in corrections {
            if let Some(matched) = matched {
                let order = open_orders
                    .orders
                    .get_mut(&order_id)
                    .expect("validated resident BUY remains present");
                order.matched_size = matched;
                order.last_update_ms = order.last_update_ms.max(now_ms);
            } else {
                open_orders.orders.remove(&order_id);
                self.note_execution_complete(&order_id);
            }
        }
        if changed {
            open_orders.note_runtime_mutation(now_ms);
        }
        Ok(changed)
    }

    pub(crate) fn consume_direct_buy_order_update(
        &mut self,
        update: &crate::orders::OrderUpdateEvent,
        authorization: DirectBuyAuthorization,
        risk: &crate::risk::RiskHandle,
        portfolio: &PortfolioState,
        open_orders: &mut OpenOrdersState,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.matched_execution
            .reconcile(portfolio, open_orders, now_ms);
        if risk.reconcile_direct_buy_order(
            authorization,
            Some(update),
            self.matched_execution.pending_buy_quantity(authorization)?,
            published_buy_quantity(portfolio, authorization)?,
        )? && update.is_closed()
        {
            self.note_execution_complete(&update.order_id);
        }
        Ok(())
    }
}

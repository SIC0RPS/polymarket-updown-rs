use crate::ctf_accounting::units_6dp_to_f64;
use crate::orders::{OpenOrdersState, OrderLifecycleKey};
use crate::portfolio::PortfolioState;
use crate::types::OrderCloseReason;
use crate::types::Side;

use super::PairTokenSide;

#[derive(Debug, Clone, Copy)]
pub(super) struct CtfFirstExecutionSummary {
    pub(super) token_side: PairTokenSide,
    pub(super) confirmed_qty_units_6dp: Option<u128>,
    pub(super) confirmed_net_units_6dp: Option<i128>,
    pub(super) pending_qty_units_6dp: u128,
    pending_net_units_6dp: Option<i128>,
    pub(super) active_release_remaining_ms: u64,
    pub(super) complete: bool,
}

pub(super) fn ctf_first_execution_summary(
    portfolio: &PortfolioState,
    open_orders: Option<&OpenOrdersState>,
    cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
    symbol: &str,
    token_side: PairTokenSide,
    target_qty_units_6dp: u128,
    now_ms: u64,
) -> CtfFirstExecutionSummary {
    let token = match token_side {
        PairTokenSide::Up => cohort.up_token.as_str(),
        PairTokenSide::Down => cohort.down_token.as_str(),
    };
    let confirmed = portfolio
        .confirmed_lifecycle_sell_fills
        .values()
        .filter(|fill| {
            fill.lifecycle_cohort == cohort.quote_group
                && fill.market_id == cohort.market_id
                && fill.token == token
                && fill.close_reason == OrderCloseReason::PairedFirstManagedSell
        })
        .try_fold((0_u128, Some(0_i128)), |(qty, net), fill| {
            let qty = qty.checked_add(fill.qty_units_6dp)?;
            let net = net.and_then(|total| {
                let fee = i128::try_from(fill.fee_units_6dp).ok()?;
                let net = fill.gross_proceeds_units_6dp.checked_sub(fee)?;
                (net >= 0).then_some(())?;
                total.checked_add(net)
            });
            Some((qty, net))
        });
    let confirmed_qty_units_6dp = confirmed.map(|(qty, _)| qty);
    let confirmed_net_units_6dp = confirmed.and_then(|(_, net)| net);
    let active = open_orders.and_then(|orders| {
        orders.active_order_lifecycles.get(&OrderLifecycleKey {
            market: cohort.market_id.to_string(),
            token: token.to_string(),
            side: Side::Sell,
            close_reason: OrderCloseReason::PairedFirstManagedSell,
            cohort: cohort.quote_group.clone(),
            symbol: symbol.to_string(),
        })
    });
    let active_release_remaining_ms = active
        .filter(|active| active.terminal_no_fill)
        .and_then(|active| active.active_until_ms)
        .map_or(0, |until_ms| until_ms.saturating_sub(now_ms));
    let coherent_pending = active.filter(|active| {
        active.matched_execution_qty_units_6dp == 0
            || active.matched_execution_portfolio_revision == portfolio.revision
    });
    let pending_qty_units_6dp =
        coherent_pending.map_or(0, |active| active.matched_execution_qty_units_6dp);
    let pending_net_units_6dp = coherent_pending.and_then(|active| {
        let fee = active.matched_execution_fee_units_6dp?;
        let gross = i128::try_from(active.matched_execution_gross_units_6dp).ok()?;
        let fee = i128::try_from(fee).ok()?;
        let net = gross.checked_sub(fee)?;
        (net >= 0).then_some(net)
    });
    let complete = confirmed_qty_units_6dp
        .and_then(|confirmed| confirmed.checked_add(pending_qty_units_6dp))
        .is_some_and(|total| total == target_qty_units_6dp)
        && (pending_qty_units_6dp == 0 || pending_net_units_6dp.is_some());
    CtfFirstExecutionSummary {
        token_side,
        confirmed_qty_units_6dp,
        confirmed_net_units_6dp,
        pending_qty_units_6dp,
        pending_net_units_6dp,
        active_release_remaining_ms,
        complete,
    }
}

pub(super) fn ctf_entry_prior_net_proceeds(
    portfolio: &PortfolioState,
    cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
    first_execution: Option<CtfFirstExecutionSummary>,
) -> Option<(i128, f64)> {
    let confirmed = portfolio
        .confirmed_lifecycle_sell_fills
        .values()
        .filter(|fill| managed_sell_matches_cohort(fill, cohort))
        .try_fold(0_i128, |total, fill| {
            let fee = i128::try_from(fill.fee_units_6dp).ok()?;
            let net = fill.gross_proceeds_units_6dp.checked_sub(fee)?;
            (net >= 0).then_some(())?;
            total.checked_add(net)
        })?;
    let valued = confirmed.checked_add(
        first_execution
            .and_then(|summary| summary.pending_net_units_6dp)
            .unwrap_or(0),
    )?;
    let value = units_6dp_to_f64(valued);
    (value.is_finite() && value >= 0.0).then_some((confirmed, value))
}

pub(super) fn ctf_entry_closed_qty(
    portfolio: &PortfolioState,
    cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
    side: PairTokenSide,
) -> Option<f64> {
    let token = match side {
        PairTokenSide::Up => cohort.up_token.as_str(),
        PairTokenSide::Down => cohort.down_token.as_str(),
    };
    portfolio
        .confirmed_lifecycle_sell_fills
        .values()
        .filter(|fill| fill.token == token && managed_sell_matches_cohort(fill, cohort))
        .try_fold(0_u128, |total, fill| total.checked_add(fill.qty_units_6dp))
        .map(|units| units as f64 / 1_000_000.0)
        .filter(|value| value.is_finite() && *value >= 0.0)
}

fn managed_sell_matches_cohort(
    fill: &crate::portfolio::ConfirmedLifecycleSellFill,
    cohort: &crate::strategy::pair_lifecycle::AuthoritativePairedCohortView,
) -> bool {
    fill.lifecycle_cohort == cohort.quote_group
        && fill.market_id == cohort.market_id
        && (fill.token == cohort.up_token || fill.token == cohort.down_token)
        && matches!(
            fill.close_reason,
            OrderCloseReason::PairedFirstManagedSell | OrderCloseReason::PairedPostProceedsSell
        )
}

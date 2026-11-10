use polymarket_client_sdk::types::U256;

use crate::polymarket::state::TradeSide;
use crate::trade_session_ledger::ManagedSellReceiptIdentity;
use crate::types::{Side, TimeInForce};

use super::{same_price, ExchangeSim, RestingQueueState, SimFill, VenueDiagnosticKind, VenueOrder};

impl ExchangeSim {
    pub(crate) fn next_gtd_expiry_ms(&self) -> Option<u64> {
        self.resting_orders
            .values()
            .filter_map(|order| order.time_in_force.expiration_ms())
            .min()
    }
}

pub(super) fn process_captured_last_trade(
    exchange: &mut ExchangeSim,
    token_id: U256,
    price: f64,
    size: f64,
    side: Option<TradeSide>,
    ts_ms: u64,
    receipt_identity: ManagedSellReceiptIdentity,
) {
    let (Ok(price_units_6dp), Some(mut trade_remaining_units_6dp), Some(side)) = (
        super::validate_price(price),
        super::fixed_units_6dp(size, false),
        side,
    ) else {
        exchange.record_diagnostic(
            ts_ms,
            None,
            "",
            VenueDiagnosticKind::ProtocolError,
            "invalid_captured_last_trade",
        );
        return;
    };
    expire_orders(exchange, ts_ms);

    let mut orders = exchange
        .resting_orders
        .values()
        .filter(|order| {
            order.token_id == token_id
                && captured_trade_can_execute_order(side, price_units_6dp, order)
        })
        .cloned()
        .collect::<Vec<_>>();
    sort_resting_orders_for_trade(side, &mut orders);

    for order in orders {
        if trade_remaining_units_6dp == 0 {
            break;
        }
        if let Err(reason) = exchange.validated_book_state(order.token_id, ts_ms) {
            exchange.record_diagnostic(
                ts_ms,
                Some(&order),
                &order.order_id,
                VenueDiagnosticKind::ProtocolError,
                reason,
            );
            continue;
        }
        let (fill, consumed_trade_units_6dp) = captured_last_trade_fill(
            exchange,
            &order,
            price_units_6dp,
            trade_remaining_units_6dp,
            ts_ms,
            &receipt_identity,
        );
        trade_remaining_units_6dp =
            trade_remaining_units_6dp.saturating_sub(consumed_trade_units_6dp);
        let Some(fill) = fill else {
            continue;
        };
        apply_resting_fill(exchange, order, fill, ts_ms);
    }
    exchange.invalidate_trade_ladder(token_id, side);
}

pub(super) fn expire_orders(exchange: &mut ExchangeSim, ts_ms: u64) {
    let mut expired = exchange
        .resting_orders
        .values()
        .filter_map(|order| {
            let TimeInForce::Gtd { expiration_ms } = order.time_in_force else {
                return None;
            };
            (ts_ms >= expiration_ms).then(|| (expiration_ms, order.order_id.clone()))
        })
        .collect::<Vec<_>>();
    expired.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    for (_, order_id) in expired {
        let Some(order) = exchange.resting_orders.remove(&order_id) else {
            continue;
        };
        exchange.resting_queues.remove(&order_id);
        exchange.push_order_update(
            &order,
            ts_ms,
            "CANCELED",
            "CANCELLATION",
            Some("gtd_expired"),
        );
    }
}

pub(super) fn displayed_queue_ahead_units_6dp(
    exchange: &ExchangeSim,
    order: &VenueOrder,
    ts_ms: u64,
) -> Result<u128, &'static str> {
    let (book, depth) = exchange.validated_book_state(order.token_id, ts_ms)?;
    let levels = match (depth, order.side) {
        (Some(depth), Side::Buy) => Some(depth.bids.as_slice()),
        (Some(depth), Side::Sell) => Some(depth.asks.as_slice()),
        _ => None,
    };
    if let Some(level) = levels.and_then(|levels| {
        levels
            .iter()
            .find(|level| super::fixed_units_6dp(level.price, false) == Some(order.px_units_6dp))
    }) {
        return super::fixed_units_6dp(level.size, true).ok_or("depth_size_not_exact_fixed_6dp");
    }
    let displayed = match order.side {
        Side::Buy if book.bid.is_some_and(|bid| same_price(bid, order.px())) => {
            book.bid_sz.unwrap_or(0.0)
        }
        Side::Sell if book.ask.is_some_and(|ask| same_price(ask, order.px())) => {
            book.ask_sz.unwrap_or(0.0)
        }
        _ => 0.0,
    };
    super::fixed_units_6dp(displayed, true).ok_or("book_size_not_exact_fixed_6dp")
}

fn captured_last_trade_fill(
    exchange: &mut ExchangeSim,
    order: &VenueOrder,
    trade_price_units_6dp: u128,
    trade_remaining_units_6dp: u128,
    ts_ms: u64,
    receipt_identity: &ManagedSellReceiptIdentity,
) -> (Option<SimFill>, u128) {
    if ts_ms <= order.placed_at_ms || trade_remaining_units_6dp == 0 {
        return (None, 0);
    }
    let queue_ahead_units_6dp = exchange
        .resting_queues
        .get(&order.order_id)
        .map(|state| state.queue_ahead_units_6dp)
        .unwrap_or(0);
    let consumed_ahead_units_6dp = queue_ahead_units_6dp.min(trade_remaining_units_6dp);
    let queue_ahead_units_6dp = queue_ahead_units_6dp - consumed_ahead_units_6dp;
    let after_queue_units_6dp = trade_remaining_units_6dp - consumed_ahead_units_6dp;
    if queue_ahead_units_6dp > 0 || after_queue_units_6dp == 0 {
        exchange.resting_queues.insert(
            order.order_id.clone(),
            RestingQueueState {
                queue_ahead_units_6dp,
            },
        );
        return (None, consumed_ahead_units_6dp);
    }

    let qty_units_6dp = order.remaining_units_6dp().min(after_queue_units_6dp);
    if qty_units_6dp == 0 {
        return (None, consumed_ahead_units_6dp);
    }
    let book_age_ms = exchange
        .book_for(order.token_id)
        .map(|book| ts_ms - book.observed_at_ms)
        .unwrap_or(0);
    let Some(gross_units_6dp) =
        super::exact_level_gross_units_6dp(qty_units_6dp, trade_price_units_6dp)
    else {
        return (None, consumed_ahead_units_6dp);
    };
    (
        Some(SimFill {
            qty_units_6dp,
            px_units_6dp: trade_price_units_6dp,
            book_age_ms,
            order_rest_ms: ts_ms.saturating_sub(order.placed_at_ms),
            depth_source: "captured_last_trade_mbp_queue",
            available_qty: super::units_6dp_to_f64(trade_remaining_units_6dp),
            levels_crossed: 1,
            matched_levels: vec![super::MatchedLevel {
                source: None,
                price_units_6dp: trade_price_units_6dp,
                qty_units_6dp,
            }],
            gross_units_6dp,
            fee_units_6dp: None,
            receipt_identity: Some(receipt_identity.clone()),
        }),
        consumed_ahead_units_6dp + qty_units_6dp,
    )
}

fn apply_resting_fill(
    exchange: &mut ExchangeSim,
    mut order: VenueOrder,
    mut fill: SimFill,
    ts_ms: u64,
) {
    if let Err(reason) = exchange.finalize_fill_economics(&order, &mut fill) {
        exchange.record_diagnostic(
            ts_ms,
            Some(&order),
            &order.order_id,
            VenueDiagnosticKind::ProtocolError,
            reason,
        );
        return;
    }
    let Some(matched_size_units_6dp) = order.matched_size_units_6dp.checked_add(fill.qty_units_6dp)
    else {
        exchange.record_diagnostic(
            ts_ms,
            Some(&order),
            &order.order_id,
            VenueDiagnosticKind::ProtocolError,
            "matched_quantity_overflow",
        );
        return;
    };
    order.matched_size_units_6dp = matched_size_units_6dp.min(order.original_size_units_6dp);
    super::fill_event::emit_fill(exchange, &order, &fill, ts_ms);
    if order.remaining_units_6dp() == 0 {
        exchange.resting_orders.remove(&order.order_id);
        exchange.resting_queues.remove(&order.order_id);
        exchange.push_order_update(&order, ts_ms, "MATCHED", "TRADE", None);
    } else {
        exchange
            .resting_orders
            .insert(order.order_id.clone(), order.clone());
        exchange.push_order_update(&order, ts_ms, "LIVE", "TRADE", None);
    }
}

fn captured_trade_can_execute_order(
    side: TradeSide,
    price_units_6dp: u128,
    order: &VenueOrder,
) -> bool {
    match (side, order.side) {
        (TradeSide::Buy, Side::Sell) => order.px_units_6dp <= price_units_6dp,
        (TradeSide::Sell, Side::Buy) => order.px_units_6dp >= price_units_6dp,
        _ => false,
    }
}

fn sort_resting_orders_for_trade(side: TradeSide, orders: &mut [VenueOrder]) {
    match side {
        TradeSide::Buy => orders.sort_by(|left, right| {
            left.px_units_6dp
                .cmp(&right.px_units_6dp)
                .then(left.placed_at_ms.cmp(&right.placed_at_ms))
                .then(left.order_id.cmp(&right.order_id))
        }),
        TradeSide::Sell => orders.sort_by(|left, right| {
            right
                .px_units_6dp
                .cmp(&left.px_units_6dp)
                .then(left.placed_at_ms.cmp(&right.placed_at_ms))
                .then(left.order_id.cmp(&right.order_id))
        }),
    }
}

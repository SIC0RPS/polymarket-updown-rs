use crate::portfolio::{FillEvent, FillSide};
use crate::types::Side;

use super::{
    CapturedVenueFact, ExchangeSim, SimFill, VenueDiagnostic, VenueDiagnosticKind, VenueOrder,
};

pub(super) fn emit_fill(
    exchange: &mut ExchangeSim,
    order: &VenueOrder,
    fill: &SimFill,
    ts_ms: u64,
) {
    let Some(receipt_identity) = fill.receipt_identity.clone() else {
        exchange.record_diagnostic(
            ts_ms,
            Some(order),
            &order.order_id,
            VenueDiagnosticKind::ProtocolError,
            "captured_fill_receipt_identity_unavailable",
        );
        return;
    };
    let Ok(fill_id) = receipt_identity.fill_id(&order.order_id) else {
        exchange.record_diagnostic(
            ts_ms,
            Some(order),
            &order.order_id,
            VenueDiagnosticKind::ProtocolError,
            "captured_fill_receipt_identity_invalid",
        );
        return;
    };
    let fee = fill.fee_units_6dp.map(super::units_6dp_to_f64);
    exchange.facts.push(CapturedVenueFact::Fill(FillEvent {
        ts_ms,
        trade_id: Some(fill_id),
        order_id: order.order_id.clone(),
        transaction_hash: None,
        token: order.token.clone(),
        market_id: Some(order.market_id.clone()),
        side: match order.side {
            Side::Buy => FillSide::Buy,
            Side::Sell => FillSide::Sell,
        },
        quote_group: order.quote_group.clone(),
        lifecycle_cohort: order.lifecycle_cohort.clone(),
        execution_intent_id: order.execution_intent_id,
        direct_buy_authorization: None,
        trade_observation: None,
        receipt_identity: Some(receipt_identity),
        receipt_qty_units_6dp: Some(fill.qty_units_6dp),
        receipt_gross_proceeds_units_6dp: match order.side {
            Side::Sell => i128::try_from(fill.gross_units_6dp).ok(),
            Side::Buy => None,
        },
        receipt_principal_units_6dp: (order.side == Side::Buy).then_some(fill.gross_units_6dp),
        receipt_approved_direct_cost_units_6dp: (order.side == Side::Buy).then_some(0),
        receipt_collateral_refund_units_6dp: (order.side == Side::Buy).then_some(0),
        receipt_collateral_cash_delta_units_6dp: if order.side == Side::Buy {
            fill.fee_units_6dp
                .and_then(|fee| fill.gross_units_6dp.checked_add(fee))
                .and_then(|debit| i128::try_from(debit).ok())
                .and_then(i128::checked_neg)
        } else {
            None
        },
        receipt_wallet: None,
        receipt_fee_units_6dp: fill.fee_units_6dp,
        receipt_confirmed_at_ms: (order.side == Side::Buy).then_some(ts_ms),
        submitted_order_size: Some(order.original_size()),
        px: super::units_6dp_to_f64(fill.px_units_6dp),
        size: super::units_6dp_to_f64(fill.qty_units_6dp),
        fee,
        at_top_of_book: Some(fill.levels_crossed <= 1),
        queue_ahead_at_fill: None,
        time_to_fill_ms: Some(fill.order_rest_ms),
    }));
    exchange.execution_stats.note_fill(&order.market_id);
    exchange.diagnostics.push(VenueDiagnostic {
        ts_ms,
        order_id: order.order_id.clone(),
        market_id: Some(order.market_id.to_string()),
        token: Some(order.token.clone()),
        kind: VenueDiagnosticKind::Fill,
        reason: "captured_venue_fill",
        book_age_ms: Some(fill.book_age_ms),
        order_rest_ms: Some(fill.order_rest_ms),
        depth_source: Some(fill.depth_source),
        available_qty: Some(fill.available_qty),
        levels_crossed: Some(fill.levels_crossed),
        gross_units_6dp: Some(fill.gross_units_6dp),
        fee_units_6dp: fill.fee_units_6dp,
    });
}

use std::collections::HashMap;

use crate::polymarket::identity::MarketId;
use crate::{orders::OrderUpdateEvent, portfolio::FillEvent};

#[derive(Debug, Clone)]
pub enum CapturedVenueFact {
    OrderUpdate(OrderUpdateEvent),
    Fill(FillEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueDiagnosticKind {
    Fill,
    Rejected,
    NoFill,
    Cancelled,
    ProtocolError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VenueDiagnostic {
    pub ts_ms: u64,
    pub order_id: String,
    pub market_id: Option<String>,
    pub token: Option<String>,
    pub kind: VenueDiagnosticKind,
    pub reason: &'static str,
    pub book_age_ms: Option<u64>,
    pub order_rest_ms: Option<u64>,
    pub depth_source: Option<&'static str>,
    pub available_qty: Option<f64>,
    pub levels_crossed: Option<u16>,
    pub gross_units_6dp: Option<u128>,
    pub fee_units_6dp: Option<u128>,
}

#[derive(Debug, Clone, Default)]
pub struct ExchangeSimExecutionStats {
    pub submitted_orders: u64,
    pub rejected_orders: u64,
    pub emitted_fills: u64,
    by_market: HashMap<MarketId, ExchangeSimExecutionCounters>,
}

#[derive(Debug, Clone, Default)]
struct ExchangeSimExecutionCounters {
    submitted_orders: u64,
    rejected_orders: u64,
    emitted_fills: u64,
}

impl ExchangeSimExecutionStats {
    fn counters_mut(&mut self, market_id: &MarketId) -> &mut ExchangeSimExecutionCounters {
        self.by_market.entry(market_id.clone()).or_default()
    }

    pub(super) fn note_submitted(&mut self, market_id: &MarketId) {
        self.submitted_orders = self.submitted_orders.saturating_add(1);
        let counters = self.counters_mut(market_id);
        counters.submitted_orders = counters.submitted_orders.saturating_add(1);
    }

    pub(super) fn note_rejected(&mut self, market_id: &MarketId) {
        self.rejected_orders = self.rejected_orders.saturating_add(1);
        let counters = self.counters_mut(market_id);
        counters.rejected_orders = counters.rejected_orders.saturating_add(1);
    }

    pub(super) fn note_fill(&mut self, market_id: &MarketId) {
        self.emitted_fills = self.emitted_fills.saturating_add(1);
        let counters = self.counters_mut(market_id);
        counters.emitted_fills = counters.emitted_fills.saturating_add(1);
    }
}

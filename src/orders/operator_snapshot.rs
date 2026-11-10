use serde::Serialize;

use super::OpenOrderState;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct OpenOrderSnapshot {
    pub order_id: String,
    pub market_id: String,
    pub token: String,
    pub side: String,
    pub time_in_force: String,
    pub price: f64,
    pub original_size: f64,
    pub matched_size: f64,
    pub remaining_size: f64,
    pub status: String,
    pub placed_at_ms: u64,
    pub updated_at_ms: u64,
    pub lifecycle_cohort: Option<String>,
    pub lifecycle_symbol: Option<String>,
}

impl From<&OpenOrderState> for OpenOrderSnapshot {
    fn from(order: &OpenOrderState) -> Self {
        Self {
            order_id: order.order_id.clone(),
            market_id: order.market.clone(),
            token: order.token.clone(),
            side: match order.side {
                crate::types::Side::Buy => "buy",
                crate::types::Side::Sell => "sell",
            }
            .to_string(),
            time_in_force: order.time_in_force.as_str().to_string(),
            price: order.px,
            original_size: order.original_size,
            matched_size: order.matched_size,
            remaining_size: order.remaining_size(),
            status: order.status.clone(),
            placed_at_ms: order.placed_at_ms,
            updated_at_ms: order.last_update_ms,
            lifecycle_cohort: order.lifecycle_cohort.clone(),
            lifecycle_symbol: order.lifecycle_symbol.clone(),
        }
    }
}

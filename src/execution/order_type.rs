use crate::config::{PolymarketOrderType, POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS};
use crate::execution::POLYMARKET_GTD_EFFECTIVE_EXPIRY_GUARD_MS;
use crate::types::{OrderIntent, TimeInForce};

pub(crate) fn apply_configured_resting_order_type(
    intent: &mut OrderIntent,
    order_type: PolymarketOrderType,
    resting_order_timeout_ms: u64,
    now_ms: u64,
) {
    if intent.time_in_force.is_immediate() {
        return;
    }
    if intent.time_in_force.expiration_ms().is_some() {
        return;
    }
    intent.common_mut().time_in_force = match order_type {
        PolymarketOrderType::Gtc => TimeInForce::Gtc,
        PolymarketOrderType::Gtd => TimeInForce::Gtd {
            expiration_ms: now_ms.saturating_add(
                resting_order_timeout_ms
                    .max(POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS)
                    .saturating_add(POLYMARKET_GTD_EFFECTIVE_EXPIRY_GUARD_MS),
            ),
        },
        PolymarketOrderType::Fok | PolymarketOrderType::Fak => {
            unreachable!("config validation restricts resting order type to GTC/GTD")
        }
    };
}

use super::*;

pub(crate) fn to_polymarket_side(side: Side) -> PolymarketSide {
    match side {
        Side::Buy => PolymarketSide::Buy,
        Side::Sell => PolymarketSide::Sell,
    }
}

/// Deep-copy a `SignedOrder` for retry/batch repost.
/// V2 SignedOrder owns a signed payload; do not manually copy V1 order fields.
pub(crate) fn clone_signed_order(so: &SignedOrder) -> SignedOrder {
    SignedOrder::builder()
        .payload(so.payload.clone())
        .signature(so.signature.clone())
        .order_type(so.order_type.clone())
        .owner(so.owner)
        .maybe_post_only(so.post_only)
        .maybe_defer_exec(so.defer_exec)
        .build()
}

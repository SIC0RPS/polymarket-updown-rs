use crate::types::OrderCloseReason;

#[path = "paired_action/ctf_routes.rs"]
mod ctf_routes;

#[derive(Debug, Clone)]
pub(crate) enum PairedAction {
    QuoteBoth,
    CancelSibling,
    FirstManagedSell { qty: f64 },
    PostProceedsSell { qty: f64 },
    HoldDirectionalRemainingOwnedLeg,
    HoldPendingProtection,
    HoldProtected,
    HoldForMerge,
    HoldMatchedPair,
    HoldForRedeem,
    FreezeUnknown,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PairedSellRoute {
    pub(crate) qty_units_6dp: u128,
    pub(crate) qty: f64,
    pub(crate) close_reason: OrderCloseReason,
}

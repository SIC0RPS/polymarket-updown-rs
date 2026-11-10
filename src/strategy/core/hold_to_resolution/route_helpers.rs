use crate::types::OrderCloseReason;

use super::{PairedAction, PairedSellRoute};

pub(super) fn paired_sell_route_from_action(
    action: &PairedAction,
    _cohort: Option<&crate::strategy::pair_lifecycle::AuthoritativePairedCohortView>,
) -> Option<PairedSellRoute> {
    match action {
        PairedAction::FirstManagedSell { qty } => Some(PairedSellRoute {
            qty_units_6dp: 0,
            qty: *qty,
            close_reason: OrderCloseReason::PairedFirstManagedSell,
        }),
        PairedAction::PostProceedsSell { qty } => Some(PairedSellRoute {
            qty_units_6dp: 0,
            qty: *qty,
            close_reason: OrderCloseReason::PairedPostProceedsSell,
        }),
        _ => None,
    }
}

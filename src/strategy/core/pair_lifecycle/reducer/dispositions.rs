use super::{CtfOwnedLegDisposition, PairLifecycleReducerAccess, PairLifecycleTruth};

impl PairLifecycleTruth {
    fn observe_ctf_disposition(
        current: &mut Option<CtfOwnedLegDisposition>,
        observed: CtfOwnedLegDisposition,
    ) {
        match (*current, observed) {
            (
                Some(
                    existing @ CtfOwnedLegDisposition::SellNow {
                        close_reason: crate::types::OrderCloseReason::PairedFirstManagedSell,
                        ..
                    },
                ),
                next @ CtfOwnedLegDisposition::SellNow { .. },
            ) => {
                debug_assert_eq!(existing, next, "CTF First SELL_NOW decision changed");
            }
            (
                Some(CtfOwnedLegDisposition::SellNow {
                    close_reason: crate::types::OrderCloseReason::PairedFirstManagedSell,
                    ..
                }),
                _,
            ) => {}
            (_, next) => *current = Some(next),
        }
    }

    pub(in crate::strategy::pair_lifecycle) fn observe_ctf_owned_leg_dispositions(
        &mut self,
        _access: PairLifecycleReducerAccess,
        up: CtfOwnedLegDisposition,
        down: CtfOwnedLegDisposition,
    ) {
        Self::observe_ctf_disposition(&mut self.ctf_up_disposition, up);
        Self::observe_ctf_disposition(&mut self.ctf_down_disposition, down);
    }
}

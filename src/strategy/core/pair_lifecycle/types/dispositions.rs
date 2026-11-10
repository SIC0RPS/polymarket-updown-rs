use super::PairLifecycleState;
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy::pair_lifecycle::reducer::{
    CtfOwnedLegDisposition, PairLifecycleReducerAccess,
};

impl PairLifecycleState {
    pub(crate) const fn ctf_owned_leg_disposition(
        &self,
        side: PairTokenSide,
    ) -> Option<CtfOwnedLegDisposition> {
        self.truth.ctf_owned_leg_disposition(side)
    }

    pub(in crate::strategy::pair_lifecycle) fn observe_ctf_owned_leg_dispositions(
        &mut self,
        access: PairLifecycleReducerAccess,
        up: CtfOwnedLegDisposition,
        down: CtfOwnedLegDisposition,
    ) {
        self.truth
            .observe_ctf_owned_leg_dispositions(access, up, down);
    }
}

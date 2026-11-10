use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::oracle_authority::OracleAuthoritySnapshot;

use super::{is_confirmed_ctf_split_cohort_id, PairLifecycleState};

impl MakerRebateMmStrategy {
    pub(super) fn current_ctf_oracle_authority_snapshot(
        &self,
        state: &PairLifecycleState,
    ) -> Option<OracleAuthoritySnapshot> {
        if !is_confirmed_ctf_split_cohort_id(&state.quote_group)
            || state.resolved_winner().is_some()
        {
            return None;
        }
        self.oracle_authority_by_market
            .get(&state.market_id)
            .copied()
    }
}

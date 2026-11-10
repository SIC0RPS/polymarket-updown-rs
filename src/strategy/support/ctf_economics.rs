use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy::support::oracle_authority::OracleAuthoritySnapshot;

pub(crate) fn side_fair_value(
    snapshot: &OracleAuthoritySnapshot,
    side: PairTokenSide,
) -> Option<f64> {
    let p_fair = snapshot.p_fair?;
    p_fair.is_finite().then_some(match side {
        PairTokenSide::Up => p_fair,
        PairTokenSide::Down => 1.0 - p_fair,
    })
}

use super::*;

pub(super) fn ctf_leg_market_evaluation(
    projection: CtfEntryLedgerProjection,
    quantum: f64,
) -> CtfLegMarketEvaluation {
    // Surface blockers only. Loss-cut / multi-OR salvage is not a disposition reason;
    // post-first sell vs hold is three-reason economics in sell_selection.
    let _ = quantum;
    CtfLegMarketEvaluation { projection }
}

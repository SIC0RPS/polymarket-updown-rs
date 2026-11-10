use crate::strategy::facts::CtfOwnedLegCloseReason;
use crate::trade_session_ledger::ConfirmedOrderSubmissionFact;
use crate::types::TimeInForce;

use super::{
    cohort_projection::order_submission_matches_entry, ctf_summary::checked_increment,
    CanonicalCtfSummaryRow, CanonicalOwnedLegOutcome, CohortEntryIdentity,
};

pub(super) fn validate_and_count(
    entry: &CohortEntryIdentity,
    submission: &ConfirmedOrderSubmissionFact,
    outcome: Option<&CanonicalOwnedLegOutcome>,
    summary: &mut CanonicalCtfSummaryRow,
    violations: &mut Vec<String>,
) -> bool {
    if !order_submission_matches_entry(entry, submission) {
        violations.push(format!(
            "order_submission_split_identity_mismatch:order_id={}:cohort={}",
            submission.order_id, entry.cohort_id
        ));
        return false;
    }
    if submission.buy_authorization.is_some() {
        if let Err(error) = submission.validate() {
            violations.push(format!(
                "invalid_buy_submission:order_id={}:{}",
                submission.order_id, error
            ));
            return false;
        }
        checked_increment(
            &mut summary.submitted_orders,
            violations,
            format!(
                "ctf_symbol_submitted_order_count_overflow:symbol={}",
                entry.underlying
            ),
        );
        return true;
    }
    let Some(outcome) = outcome else {
        violations.push(format!(
            "order_submission_without_sell_now_owner_intent:order_id={}:intent_id={}",
            submission.order_id, submission.intent_id
        ));
        return false;
    };
    if outcome.symbol != entry.underlying
        || outcome.market_id != entry.market_id
        || outcome.cohort_id != entry.cohort_id
        || outcome.token != submission.asset_id
    {
        violations.push(format!(
            "order_submission_owner_intent_identity_mismatch:order_id={}:intent_id={}",
            submission.order_id, submission.intent_id
        ));
        return false;
    }
    let close_reason = match outcome.close_reason {
        Some(close_reason) => close_reason,
        None => {
            violations.push(format!(
                "order_submission_owner_intent_missing_close_reason:order_id={}:intent_id={}",
                submission.order_id, submission.intent_id
            ));
            return false;
        }
    };
    if !matches!(&submission.time_in_force, TimeInForce::Fak) || submission.post_only {
        violations.push(format!(
            "managed_sell_submission_invalid_order_type:order_id={}",
            submission.order_id
        ));
        return false;
    }
    checked_increment(
        &mut summary.submitted_orders,
        violations,
        format!(
            "ctf_symbol_submitted_order_count_overflow:symbol={}",
            entry.underlying
        ),
    );
    let submitted_order_count = match close_reason {
        CtfOwnedLegCloseReason::PairedFirstManagedSell => {
            &mut summary.paired_first_managed_sell_submitted_order_count
        }
        CtfOwnedLegCloseReason::PairedPostProceedsSell => {
            &mut summary.paired_post_proceeds_sell_submitted_order_count
        }
    };
    checked_increment(
        submitted_order_count,
        violations,
        format!(
            "ctf_symbol_close_reason_submitted_order_count_overflow:symbol={}",
            entry.underlying
        ),
    );
    true
}

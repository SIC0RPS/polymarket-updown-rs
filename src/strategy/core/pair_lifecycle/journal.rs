use crate::engine_trade_journal::{
    append as append_engine_trade_journal, EngineTradeJournalRecord,
};

use super::super::hold_to_resolution::{PairTokenSide, ProtectedInventoryTerminalState};
use super::{
    is_confirmed_ctf_split_cohort_id, PairLifecycleState, PairedCohort, ResolvedInventoryOwnerKind,
    SiblingCancelState,
};

fn append_pair_lifecycle_journal(record: EngineTradeJournalRecord) {
    if let Err(err) = append_engine_trade_journal(record) {
        tracing::warn!(
            target: "strategy.engine_trade_journal",
            error = %err,
            "append_pair_lifecycle_journal_failed"
        );
    }
}

pub(super) fn append_post_proceeds_sell_expired_or_unfilled(
    state: &PairLifecycleState,
    token: String,
    order_id: String,
    reason: &'static str,
    paired_cohort_key: &str,
    now_ms: u64,
) {
    let Some(post_proceeds_sell_state) = state.post_proceeds_sell_remaining_position_state() else {
        return;
    };
    let mut row = EngineTradeJournalRecord::new(
        state.quote_group.clone(),
        "post_proceeds_sell_expired_or_unfilled",
        now_ms,
    );
    row.market_id = Some(state.market_id.clone());
    row.tokens = vec![token];
    row.paired_cohort_key = Some(paired_cohort_key.to_string());
    row.hold_state = Some(post_proceeds_sell_state.as_str().to_string());
    let post_proceeds_sell_reason = "paired_post_proceeds_sell";
    row.close_hint = Some(post_proceeds_sell_reason.to_string());
    row.strategy_exit_reason = Some(format!(
        "{post_proceeds_sell_reason} terminal without fill: {reason}"
    ));
    row.local_order_ids.push(order_id);
    append_pair_lifecycle_journal(row);
}

pub(super) fn append_winner_selected(
    state: &PairLifecycleState,
    paired_cohort_key: &str,
    now_ms: u64,
) {
    let mut row =
        EngineTradeJournalRecord::new(state.quote_group.clone(), "winner_selected", now_ms);
    row.market_id = Some(state.market_id.clone());
    row.tokens = vec![state.up_token.clone(), state.down_token.clone()];
    row.paired_cohort_key = Some(paired_cohort_key.to_string());
    row.hold_state = Some(state.owner_kind().as_str().to_string());
    append_pair_lifecycle_journal(row);
}

fn durable_winner_for_journal(state: &PairLifecycleState) -> Option<PairTokenSide> {
    if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
        return None;
    }
    let winner = state.effective_winner()?;
    if state.resolved_winner() == Some(winner) || state.terminal_state().is_some() {
        return Some(winner);
    }
    if state.owner_kind() == ResolvedInventoryOwnerKind::AcceptedPair
        && state.remaining_owned_side() == Some(winner)
        && (state.protection_authorized() || state.protection_eligible_since_ms().is_some())
    {
        return Some(winner);
    }
    None
}

fn should_emit_winner_selected(
    previous: Option<&PairLifecycleState>,
    current: &PairLifecycleState,
) -> bool {
    let Some(current_winner) = durable_winner_for_journal(current) else {
        return false;
    };
    if previous.is_some_and(|previous| {
        previous.effective_winner() == Some(current_winner)
            && previous.owner_kind() == current.owner_kind()
            && previous.terminal_state() == current.terminal_state()
            && previous.resolved_winner() != Some(current_winner)
            && current.resolved_winner() == Some(current_winner)
    }) {
        return false;
    }
    let previous_winner = previous.and_then(durable_winner_for_journal);
    previous_winner != Some(current_winner)
}

pub(super) fn append_lifecycle_state_journal_events(
    previous: Option<&PairLifecycleState>,
    current: &PairLifecycleState,
    paired_cohort_key: &str,
    now_ms: u64,
) {
    if should_emit_winner_selected(previous, current) {
        if current.effective_winner().is_some() {
            append_winner_selected(current, paired_cohort_key, now_ms);
        }
    }
}

pub(super) fn append_protected_winner_hold(
    cohort: &PairedCohort,
    paired_cohort_key: &str,
    now_ms: u64,
) {
    let mut row =
        EngineTradeJournalRecord::new(cohort.quote_group.clone(), "protected_winner_hold", now_ms);
    row.market_id = Some(cohort.market_id.clone());
    row.tokens = vec![cohort.up_token.clone(), cohort.down_token.clone()];
    row.paired_cohort_key = Some(paired_cohort_key.to_string());
    row.hold_state = Some(cohort.owner_kind().as_str().to_string());
    append_pair_lifecycle_journal(row);
}

pub(super) fn sibling_cancel_event_kind(
    owner_kind: ResolvedInventoryOwnerKind,
    sibling_cancel_state: SiblingCancelState,
) -> Option<&'static str> {
    match (owner_kind, sibling_cancel_state) {
        (_, SiblingCancelState::Pending) => Some("loser_cleanup_requested"),
        (_, SiblingCancelState::Acked) => Some("loser_cleanup_confirmed"),
        (_, SiblingCancelState::NotNeeded) => None,
    }
}

pub(super) fn append_sibling_cancel(
    cohort: &PairedCohort,
    paired_cohort_key: &str,
    event_kind: &str,
    now_ms: u64,
) {
    let mut row = EngineTradeJournalRecord::new(cohort.quote_group.clone(), event_kind, now_ms);
    row.market_id = Some(cohort.market_id.clone());
    row.tokens = vec![cohort.up_token.clone(), cohort.down_token.clone()];
    row.paired_cohort_key = Some(paired_cohort_key.to_string());
    row.hold_state = Some(cohort.sibling_cancel_state.as_str().to_string());
    append_pair_lifecycle_journal(row);
}

pub(super) fn append_terminal_release_ready(
    cohort: &PairedCohort,
    paired_cohort_key: &str,
    terminal_state: ProtectedInventoryTerminalState,
    now_ms: u64,
) {
    let mut row =
        EngineTradeJournalRecord::new(cohort.quote_group.clone(), "terminal_release_ready", now_ms);
    row.market_id = Some(cohort.market_id.clone());
    row.tokens = vec![cohort.up_token.clone(), cohort.down_token.clone()];
    row.paired_cohort_key = Some(paired_cohort_key.to_string());
    row.hold_state = Some(terminal_state.as_str().to_string());
    append_pair_lifecycle_journal(row);
}

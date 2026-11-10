use crate::orders::{OpenOrdersState, OrderUpdateEvent};
use crate::types::{OrderCloseReason, Side};

use super::journal::append_post_proceeds_sell_expired_or_unfilled;
use super::logic::inventory_eps;
use super::types::POST_PROCEEDS_SELL_FINAL_ATTEMPT_UNFILLED_REASON;
use super::{
    is_confirmed_ctf_split_cohort_id, PairLifecycleEvent, PairLifecycleReducer, PairLifecycleState,
    PostProceedsSellState,
};

fn observed_post_proceeds_sell_open_order(
    state: &PairLifecycleState,
    open_orders: &OpenOrdersState,
) -> Option<(String, String, &'static str)> {
    if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
        return None;
    }
    open_orders.orders.values().find_map(|order| {
        if order.side != Side::Sell
            || order.quote_group.as_deref() != Some(state.quote_group.as_str())
            || order.order_close_reason != Some(OrderCloseReason::PairedPostProceedsSell)
        {
            return None;
        }
        Some((
            order.token.clone(),
            order.order_id.clone(),
            "paired_post_proceeds_sell_open_order",
        ))
    })
}

fn closed_update_matches_state(
    state: &PairLifecycleState,
    update: &OrderUpdateEvent,
    pending_order_id: &str,
) -> bool {
    update.order_id == pending_order_id
        && update.market == state.market_id.as_str()
        && (update.token == state.up_token || update.token == state.down_token)
        && update.side == Side::Sell
}

fn observed_post_proceeds_sell_closed_without_fill(
    state: &PairLifecycleState,
    open_orders: &OpenOrdersState,
) -> Option<(String, String, &'static str)> {
    if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
        return None;
    }
    let pending_order_id = match state.post_proceeds_sell_remaining_position_state()? {
        PostProceedsSellState::Pending { order_id, .. } => order_id.as_str(),
        _ => return None,
    };
    let update = open_orders.last_update.as_ref()?;
    let metadata = open_orders.last_update_metadata(update.order_id.as_str())?;
    if metadata.order_close_reason != Some(OrderCloseReason::PairedPostProceedsSell) {
        return None;
    }
    let status = update
        .status
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let msg_type = update
        .msg_type
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let is_closed_without_fill =
        matches!(status.as_str(), "CANCELED" | "CANCELLED") || msg_type == "CANCELLATION";
    if !is_closed_without_fill
        || !closed_update_matches_state(state, update, pending_order_id)
        || open_orders.orders.contains_key(update.order_id.as_str())
        || observed_post_proceeds_sell_open_order(state, open_orders).is_some()
        || open_orders.has_active_lifecycle_order_id(update.order_id.as_str(), state.last_update_ms)
        || update.matched_size.unwrap_or(0.0) > inventory_eps()
    {
        return None;
    }
    let reason = if metadata.time_in_force.is_immediate() {
        POST_PROCEEDS_SELL_FINAL_ATTEMPT_UNFILLED_REASON
    } else {
        "post_proceeds_sell_working_closed_without_fill"
    };
    Some((update.token.clone(), update.order_id.clone(), reason))
}

fn expired_or_unfilled_already_recorded(
    state: &PairLifecycleState,
    token: &str,
    order_id: &str,
) -> bool {
    matches!(
        state.post_proceeds_sell_remaining_position_state(),
        Some(PostProceedsSellState::ExpiredOrUnfilled {
            token: existing_token,
            order_id: existing_order_id,
            ..
        }) if existing_token == token && existing_order_id == order_id
    )
}

pub(super) fn apply_post_proceeds_sell_observations(
    state: &mut PairLifecycleState,
    open_orders: &OpenOrdersState,
    paired_cohort_key: &str,
    now_ms: u64,
) {
    if is_confirmed_ctf_split_cohort_id(&state.quote_group) {
        return;
    }
    if let Some((token, order_id, reason)) =
        observed_post_proceeds_sell_open_order(state, open_orders)
    {
        PairLifecycleReducer::apply_state(
            state,
            PairLifecycleEvent::PostProceedsSellOpenOrderObserved {
                token: token.as_str(),
                order_id: order_id.as_str(),
                reason,
            },
        );
    }
    let Some((token, order_id, reason)) =
        observed_post_proceeds_sell_closed_without_fill(state, open_orders)
    else {
        return;
    };
    if expired_or_unfilled_already_recorded(state, &token, &order_id) {
        return;
    }
    PairLifecycleReducer::apply_state(
        state,
        PairLifecycleEvent::PostProceedsSellExpiredOrUnfilled {
            token: token.as_str(),
            order_id: order_id.as_str(),
            reason,
        },
    );
    append_post_proceeds_sell_expired_or_unfilled(
        state,
        token,
        order_id,
        reason,
        paired_cohort_key,
        now_ms,
    );
}

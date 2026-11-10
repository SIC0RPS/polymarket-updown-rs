use super::*;
use crate::strategy::support::shared::common::is_valid_tick_size;

pub(in crate::strategy_runtime::replay_mode) fn apply_market_rows(
    state: &mut PolymarketOddsState,
    rows: &[MarketRow],
) {
    if rows.is_empty() {
        return;
    }
    let underlyings = rows
        .iter()
        .map(|row| row.market.underlying.as_str())
        .collect::<std::collections::HashSet<_>>();
    state.markets.retain(|market| {
        !underlyings
            .iter()
            .any(|underlying| market.underlying.eq_ignore_ascii_case(underlying))
    });
    state.selection = None;
    state.next_selection = None;
    let first = &rows[0];
    state.updated_at_ms = state.updated_at_ms.max(first.updated_at_ms);
    state.revision = state.revision.max(first.revision);
    state.selection_updated_at_ms = state
        .selection_updated_at_ms
        .max(first.selection_updated_at_ms);
    state.next_selection_updated_at_ms = state
        .next_selection_updated_at_ms
        .max(first.next_selection_updated_at_ms);
    for row in rows {
        match row.role.as_str() {
            "market" => state.markets.push(row.market.clone()),
            "selection" => state.selection = Some(row.market.clone()),
            "next_selection" => state.next_selection = Some(row.market.clone()),
            _ => {}
        }
    }
}
pub(in crate::strategy_runtime::replay_mode) enum PreparedBookUpdate {
    Ignore,
    Remove(U256),
    Upsert {
        book: BookTop,
        depth: Option<BookDepth>,
    },
}

impl BookRow {
    pub(in crate::strategy_runtime::replay_mode) fn into_update(
        self,
        observed_at_ms: u64,
    ) -> PreparedBookUpdate {
        if self.remove {
            return self
                .token
                .map(PreparedBookUpdate::Remove)
                .unwrap_or(PreparedBookUpdate::Ignore);
        }
        let Some(token) = self.token else {
            return PreparedBookUpdate::Ignore;
        };
        if !is_valid_tick_size(self.tick_size)
            || !(self.min_order_size.is_finite() && self.min_order_size > 0.0)
        {
            return PreparedBookUpdate::Remove(token);
        }
        PreparedBookUpdate::Upsert {
            book: BookTop {
                token_id: token,
                bid: self.bid,
                ask: self.ask,
                bid_sz: self.bid_sz,
                ask_sz: self.ask_sz,
                tick_size: self.tick_size,
                min_order_size: self.min_order_size,
                ts_ms: self.ts_ms,
                observed_at_ms,
            },
            depth: self.depth.map(|mut depth| {
                depth.observed_at_ms = observed_at_ms;
                depth
            }),
        }
    }
}
impl LifecycleRow {
    pub(in crate::strategy_runtime::replay_mode) fn parsed_token(&self) -> Result<Option<U256>> {
        match self.kind.as_str() {
            "token" => U256::from_str(&self.token_id)
                .context("prepared lifecycle token is invalid")
                .map(Some),
            "status" => Ok(None),
            other => anyhow::bail!("prepared lifecycle row kind is invalid kind={other}"),
        }
    }
}
pub(in crate::strategy_runtime::replay_mode) fn apply_lifecycle_rows(
    state: &mut PolymarketOddsState,
    rows: &[LifecycleRow],
) {
    for row in rows {
        match row.kind.as_str() {
            "token" => {
                if let Ok(token) = U256::from_str(&row.token_id) {
                    state.token_lifecycle.insert(token, row.lifecycle.clone());
                }
            }
            "status" => {
                state.polymarket_lifecycle = PolymarketLifecycleStatus {
                    sync_state: row.sync_state.clone(),
                    sync_epoch: row.sync_epoch,
                    rotation_epoch: row.rotation_epoch,
                    updated_at_ms: row.updated_at_ms,
                    rotation_started_at_ms: row.rotation_started_at_ms,
                    last_error: row.last_error.clone(),
                    last_error_at_ms: row.last_error_at_ms,
                    active_tokens: row.active_tokens,
                    pending_revalidation_tokens: row.pending_revalidation_tokens,
                    pending_subscription_tokens: row.pending_subscription_tokens,
                    invalid_after_404_tokens: row.invalid_after_404_tokens,
                    resolved_tokens: row.resolved_tokens,
                    quarantined_from_entry_tokens: row.quarantined_from_entry_tokens,
                    recent_book_404_total: row.recent_book_404_total,
                    recent_book_404_tokens: row
                        .recent_book_404_tokens
                        .lines()
                        .map(str::to_string)
                        .collect(),
                }
            }
            _ => {}
        }
    }
}
pub(in crate::strategy_runtime::replay_mode) fn refresh_projection_books(
    state: &mut PolymarketOddsState,
) {
    state.up = state
        .selection
        .as_ref()
        .and_then(|m| state.books.get(&m.up_token).cloned());
    state.down = state
        .selection
        .as_ref()
        .and_then(|m| state.books.get(&m.down_token).cloned());
    state.next_up = state
        .next_selection
        .as_ref()
        .and_then(|m| state.books.get(&m.up_token).cloned());
    state.next_down = state
        .next_selection
        .as_ref()
        .and_then(|m| state.books.get(&m.down_token).cloned());
}

use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use crate::orders::OpenOrdersState;
use crate::polymarket::state::{localized_odds_for_underlying, PolymarketOddsState};
use crate::portfolio::PortfolioState;

pub(crate) fn bounded_active_tokens(
    portfolio: &PortfolioState,
    open_orders: &OpenOrdersState,
) -> Result<HashSet<U256>> {
    let mut tokens = HashSet::new();
    for (token, position) in &portfolio.positions {
        if position.qty_units_6dp > 0 {
            insert_token(&mut tokens, token, "positive portfolio position")?;
        }
    }
    for order in open_orders.orders.values() {
        if order.remaining_size() > 0.0 {
            insert_token(&mut tokens, &order.token, "open order")?;
        }
    }
    for (key, lifecycle) in &open_orders.active_order_lifecycles {
        if lifecycle.remaining_qty_units_6dp > 0 {
            insert_token(&mut tokens, &key.token, "active order lifecycle")?;
        }
    }
    Ok(tokens)
}

fn insert_token(tokens: &mut HashSet<U256>, raw: &str, owner: &str) -> Result<()> {
    let token = U256::from_str(raw.trim())
        .with_context(|| format!("{owner} has invalid Polymarket token `{raw}`"))?;
    tokens.insert(token);
    Ok(())
}

pub(crate) fn localized_odds_view(
    previous: &PolymarketOddsState,
    full: &PolymarketOddsState,
    symbol: &str,
    retained_tokens: &HashSet<U256>,
) -> Result<Option<PolymarketOddsState>> {
    let retains_market = |market: &bbwick_engine::state::MarketSelectionSnapshot| {
        retained_tokens.contains(&market.up_token) || retained_tokens.contains(&market.down_token)
    };
    let mut next = localized_odds_for_underlying(previous, full, symbol, retained_tokens);
    for market in &mut next.markets {
        market.underlying = symbol.to_string();
    }
    if let Some(selection) = next.selection.as_mut() {
        selection.underlying = symbol.to_string();
    }
    if let Some(selection) = next.next_selection.as_mut() {
        selection.underlying = symbol.to_string();
    }
    if let Some(previous_selection) = previous.selection.as_ref() {
        if retains_market(previous_selection)
            && next.updated_at_ms < previous_selection.window_end_ms
        {
            if let Some(selection) = next
                .markets
                .iter()
                .find(|market| {
                    market.underlying.eq_ignore_ascii_case(symbol)
                        && market.market_id == previous_selection.market_id
                })
                .cloned()
            {
                next.selection = Some(selection);
            }
        }
    }
    if let Some(previous_next) = previous.next_selection.as_ref() {
        if retains_market(previous_next) && next.updated_at_ms < previous_next.window_start_ms {
            if let Some(selection) = next
                .markets
                .iter()
                .find(|market| {
                    market.underlying.eq_ignore_ascii_case(symbol)
                        && market.market_id == previous_next.market_id
                })
                .cloned()
            {
                next.next_selection = Some(selection);
            }
        }
    }
    next.up = next
        .selection
        .as_ref()
        .and_then(|market| next.books.get(&market.up_token).cloned());
    next.down = next
        .selection
        .as_ref()
        .and_then(|market| next.books.get(&market.down_token).cloned());
    next.next_up = next
        .next_selection
        .as_ref()
        .and_then(|market| next.books.get(&market.up_token).cloned());
    next.next_down = next
        .next_selection
        .as_ref()
        .and_then(|market| next.books.get(&market.down_token).cloned());

    let source_updated_at_ms = next.updated_at_ms;
    next.updated_at_ms = previous.updated_at_ms;
    next.revision = previous.revision;
    next.selection_updated_at_ms = previous.selection_updated_at_ms;
    next.next_selection_updated_at_ms = previous.next_selection_updated_at_ms;
    next.next_last_trade_seq = previous.next_last_trade_seq;
    if &next == previous {
        return Ok(None);
    }
    next.updated_at_ms = source_updated_at_ms;
    next.revision = previous.revision.saturating_add(1);
    next.selection_updated_at_ms = source_updated_at_ms;
    next.next_selection_updated_at_ms = source_updated_at_ms;
    next.next_last_trade_seq = previous.next_last_trade_seq.saturating_add(1);
    Ok(Some(next))
}

use std::collections::HashSet;

use polymarket_client_sdk::types::U256;

use super::PolymarketOddsState;

pub fn odds_filtered_for_underlying(
    state: &PolymarketOddsState,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) -> PolymarketOddsState {
    let mut filtered =
        odds_filtered_for_underlying_unfingerprinted(state, underlying, retained_tokens);
    refresh_trigger_fingerprint(&mut filtered);
    filtered
}

pub(crate) fn odds_filtered_for_underlying_unfingerprinted(
    state: &PolymarketOddsState,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) -> PolymarketOddsState {
    let mut markets = state
        .markets
        .iter()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying))
        .cloned()
        .collect::<Vec<_>>();
    let mut selection = state
        .selection
        .clone()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying));
    let mut next_selection = state
        .next_selection
        .clone()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying));
    let resolved_tokens = resolved_tokens_for_metadata(
        state,
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        underlying,
        None,
    );
    retain_market_metadata(
        &mut markets,
        &mut selection,
        &mut next_selection,
        state.updated_at_ms,
        underlying,
        retained_tokens,
    );
    let kept_tokens = kept_tokens_for_metadata(
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        &resolved_tokens,
        retained_tokens,
    );
    materialize_filtered_state(state, markets, selection, next_selection, &kept_tokens)
}

pub(crate) fn localized_odds_for_underlying(
    previous: &PolymarketOddsState,
    full: &PolymarketOddsState,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) -> PolymarketOddsState {
    let mut markets = full
        .markets
        .iter()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying))
        .cloned()
        .collect::<Vec<_>>();
    let mut selection = full
        .selection
        .clone()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying));
    let mut next_selection = full
        .next_selection
        .clone()
        .filter(|market| market.underlying.eq_ignore_ascii_case(underlying));
    let resolved_tokens = resolved_tokens_for_metadata(
        full,
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        underlying,
        None,
    );
    retain_market_metadata(
        &mut markets,
        &mut selection,
        &mut next_selection,
        full.updated_at_ms,
        underlying,
        retained_tokens,
    );
    let first_kept_tokens = kept_tokens_for_metadata(
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        &resolved_tokens,
        retained_tokens,
    );

    for market in full.selection.iter().chain(full.next_selection.iter()) {
        if market.underlying.eq_ignore_ascii_case(underlying)
            && !markets
                .iter()
                .any(|current| current.market_id == market.market_id)
        {
            markets.push(market.clone());
        }
    }
    for market in previous
        .markets
        .iter()
        .chain(previous.selection.iter())
        .chain(previous.next_selection.iter())
    {
        if market.underlying.eq_ignore_ascii_case(underlying)
            && (market.window_end_ms >= full.updated_at_ms
                || retained_tokens.contains(&market.up_token)
                || retained_tokens.contains(&market.down_token))
            && !markets
                .iter()
                .any(|current| current.market_id == market.market_id)
        {
            markets.push(market.clone());
        }
    }
    let resolved_tokens = resolved_tokens_for_metadata(
        full,
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        underlying,
        Some(&first_kept_tokens),
    );
    retain_market_metadata(
        &mut markets,
        &mut selection,
        &mut next_selection,
        full.updated_at_ms,
        underlying,
        retained_tokens,
    );
    let final_kept_tokens = kept_tokens_for_metadata(
        &markets,
        selection.as_ref(),
        next_selection.as_ref(),
        &resolved_tokens,
        retained_tokens,
    );
    let payload_tokens = first_kept_tokens
        .intersection(&final_kept_tokens)
        .copied()
        .collect::<HashSet<_>>();
    let mut localized =
        materialize_filtered_state(full, markets, selection, next_selection, &payload_tokens);
    refresh_trigger_fingerprint(&mut localized);
    localized
}

pub(crate) fn retain_odds_for_underlying(
    state: &mut PolymarketOddsState,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) {
    retain_odds_for_underlying_without_fingerprint(state, underlying, retained_tokens);
    refresh_trigger_fingerprint(state);
}

fn retain_odds_for_underlying_without_fingerprint(
    state: &mut PolymarketOddsState,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) {
    let resolved_tokens = resolved_tokens_for_underlying(state, underlying);
    let updated_at_ms = state.updated_at_ms;
    let selected_market_id = state
        .selection
        .as_ref()
        .map(|market| market.market_id.clone());
    let next_selected_market_id = state
        .next_selection
        .as_ref()
        .map(|market| market.market_id.clone());
    state.markets.retain(|market| {
        market.underlying.eq_ignore_ascii_case(underlying)
            && (market.window_end_ms >= updated_at_ms
                || retained_tokens.contains(&market.up_token)
                || retained_tokens.contains(&market.down_token)
                || selected_market_id
                    .as_ref()
                    .is_some_and(|selected| selected == &market.market_id)
                || next_selected_market_id
                    .as_ref()
                    .is_some_and(|selected| selected == &market.market_id))
    });
    state.selection = state.selection.take().filter(|market| {
        market.underlying.eq_ignore_ascii_case(underlying) && updated_at_ms < market.window_end_ms
    });
    state.next_selection = state.next_selection.take().filter(|market| {
        market.underlying.eq_ignore_ascii_case(underlying) && updated_at_ms < market.window_start_ms
    });
    if state.selection.is_none() {
        state.selection = state
            .markets
            .iter()
            .find(|market| {
                market.window_start_ms <= updated_at_ms && updated_at_ms < market.window_end_ms
            })
            .or_else(|| {
                state
                    .markets
                    .iter()
                    .find(|market| updated_at_ms < market.window_end_ms)
            })
            .cloned();
    }
    if state.next_selection.is_none() {
        state.next_selection = state
            .markets
            .iter()
            .filter(|market| market.window_start_ms > updated_at_ms)
            .min_by_key(|market| market.window_start_ms)
            .cloned();
    }

    let kept_tokens = state
        .markets
        .iter()
        .flat_map(|market| [market.up_token, market.down_token])
        .chain(
            state
                .selection
                .iter()
                .flat_map(|market| [market.up_token, market.down_token]),
        )
        .chain(
            state
                .next_selection
                .iter()
                .flat_map(|market| [market.up_token, market.down_token]),
        )
        .chain(resolved_tokens.iter().copied())
        .chain(retained_tokens.iter().copied())
        .collect::<HashSet<_>>();
    state.books.retain(|token, _| kept_tokens.contains(token));
    state.depths.retain(|token, _| kept_tokens.contains(token));
    state
        .last_canonical_bids
        .retain(|token, _| kept_tokens.contains(token));
    state
        .token_lifecycle
        .retain(|token, _| kept_tokens.contains(token));
    state
        .resolved_market_winners
        .retain(|_, token| kept_tokens.contains(token));
    let retained_resolutions = &state.resolved_market_winners;
    state
        .resolved_market_contexts
        .retain(|condition, _| retained_resolutions.contains_key(condition));
    state
        .last_trades
        .retain(|trade| kept_tokens.contains(&trade.token_id));
    state.polymarket_lifecycle = Default::default();
    state.up = state
        .selection
        .as_ref()
        .and_then(|market| state.books.get(&market.up_token).cloned());
    state.down = state
        .selection
        .as_ref()
        .and_then(|market| state.books.get(&market.down_token).cloned());
    state.next_up = state
        .next_selection
        .as_ref()
        .and_then(|market| state.books.get(&market.up_token).cloned());
    state.next_down = state
        .next_selection
        .as_ref()
        .and_then(|market| state.books.get(&market.down_token).cloned());
}

fn retain_market_metadata(
    markets: &mut Vec<bbwick_engine::state::MarketSelectionSnapshot>,
    selection: &mut Option<bbwick_engine::state::MarketSelectionSnapshot>,
    next_selection: &mut Option<bbwick_engine::state::MarketSelectionSnapshot>,
    updated_at_ms: u64,
    underlying: &str,
    retained_tokens: &HashSet<U256>,
) {
    let mut metadata = PolymarketOddsState {
        updated_at_ms,
        selection: selection.take(),
        next_selection: next_selection.take(),
        markets: std::mem::take(markets),
        ..Default::default()
    };
    retain_odds_for_underlying(&mut metadata, underlying, retained_tokens);
    *markets = metadata.markets;
    *selection = metadata.selection;
    *next_selection = metadata.next_selection;
}

fn kept_tokens_for_metadata(
    markets: &[bbwick_engine::state::MarketSelectionSnapshot],
    selection: Option<&bbwick_engine::state::MarketSelectionSnapshot>,
    next_selection: Option<&bbwick_engine::state::MarketSelectionSnapshot>,
    resolved_tokens: &HashSet<U256>,
    retained_tokens: &HashSet<U256>,
) -> HashSet<U256> {
    markets
        .iter()
        .flat_map(|market| [market.up_token, market.down_token])
        .chain(
            selection
                .into_iter()
                .flat_map(|market| [market.up_token, market.down_token]),
        )
        .chain(
            next_selection
                .into_iter()
                .flat_map(|market| [market.up_token, market.down_token]),
        )
        .chain(resolved_tokens.iter().copied())
        .chain(retained_tokens.iter().copied())
        .collect()
}

fn materialize_filtered_state(
    state: &PolymarketOddsState,
    markets: Vec<bbwick_engine::state::MarketSelectionSnapshot>,
    selection: Option<bbwick_engine::state::MarketSelectionSnapshot>,
    next_selection: Option<bbwick_engine::state::MarketSelectionSnapshot>,
    kept_tokens: &HashSet<U256>,
) -> PolymarketOddsState {
    let resolved_market_winners = state
        .resolved_market_winners
        .iter()
        .filter(|(_, token)| kept_tokens.contains(token))
        .map(|(condition, token)| (*condition, *token))
        .collect::<std::collections::HashMap<_, _>>();
    let mut filtered = PolymarketOddsState {
        updated_at_ms: state.updated_at_ms,
        revision: state.revision,
        selection_updated_at_ms: state.selection_updated_at_ms,
        next_selection_updated_at_ms: state.next_selection_updated_at_ms,
        token_lifecycle: state
            .token_lifecycle
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, lifecycle)| (*token, lifecycle.clone()))
            .collect(),
        resolved_market_contexts: state
            .resolved_market_contexts
            .iter()
            .filter(|(condition, _)| resolved_market_winners.contains_key(*condition))
            .map(|(condition, context)| (*condition, context.clone()))
            .collect(),
        resolved_market_winners,
        selection,
        next_selection,
        markets,
        last_canonical_bids: state
            .last_canonical_bids
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, mark)| (*token, *mark))
            .collect(),
        books: state
            .books
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, book)| (*token, book.clone()))
            .collect(),
        depths: state
            .depths
            .iter()
            .filter(|(token, _)| kept_tokens.contains(token))
            .map(|(token, depth)| (*token, depth.clone()))
            .collect(),
        last_trades: state
            .last_trades
            .iter()
            .filter(|trade| kept_tokens.contains(&trade.token_id))
            .cloned()
            .collect(),
        next_last_trade_seq: state.next_last_trade_seq,
        ..Default::default()
    };
    filtered.up = filtered
        .selection
        .as_ref()
        .and_then(|market| filtered.books.get(&market.up_token).cloned());
    filtered.down = filtered
        .selection
        .as_ref()
        .and_then(|market| filtered.books.get(&market.down_token).cloned());
    filtered.next_up = filtered
        .next_selection
        .as_ref()
        .and_then(|market| filtered.books.get(&market.up_token).cloned());
    filtered.next_down = filtered
        .next_selection
        .as_ref()
        .and_then(|market| filtered.books.get(&market.down_token).cloned());
    filtered
}

fn refresh_trigger_fingerprint(state: &mut PolymarketOddsState) {
    let mut fingerprint = bbwick_engine::state::Fingerprint::default();
    for market in &state.markets {
        fingerprint.insert_hash(bbwick_engine::state::fingerprint_hash(&(
            market.market_id.as_str(),
            market.up_token,
            market.down_token,
            market.window_start_ms,
            market.window_end_ms,
        )));
    }
    for (token, book) in &state.books {
        fingerprint.insert_hash(bbwick_engine::state::fingerprint_hash(&(
            *token,
            book.bid.map(f64::to_bits),
            book.ask.map(f64::to_bits),
            book.bid_sz.map(f64::to_bits),
            book.ask_sz.map(f64::to_bits),
            book.tick_size.to_bits(),
            book.ts_ms,
            book.observed_at_ms,
        )));
    }
    for (token, depth) in &state.depths {
        fingerprint.insert_hash(bbwick_engine::state::fingerprint_hash(&(
            *token,
            depth.revision,
            depth.observed_at_ms,
        )));
    }
    state.trigger_fingerprint = fingerprint;
}

fn resolved_tokens_for_underlying(state: &PolymarketOddsState, underlying: &str) -> HashSet<U256> {
    resolved_tokens_for_metadata(
        state,
        &state.markets,
        state.selection.as_ref(),
        state.next_selection.as_ref(),
        underlying,
        None,
    )
}

fn resolved_tokens_for_metadata(
    state: &PolymarketOddsState,
    markets: &[bbwick_engine::state::MarketSelectionSnapshot],
    selection: Option<&bbwick_engine::state::MarketSelectionSnapshot>,
    next_selection: Option<&bbwick_engine::state::MarketSelectionSnapshot>,
    underlying: &str,
    allowed_winners: Option<&HashSet<U256>>,
) -> HashSet<U256> {
    let mut conditions = HashSet::new();
    for market in markets.iter().chain(selection).chain(next_selection) {
        if market.underlying.eq_ignore_ascii_case(underlying) {
            if let Some(condition) = market.condition_id_b256.as_deref() {
                conditions.insert(condition.to_string());
            }
            conditions.insert(market.condition_id.clone());
        }
    }
    let mut tokens = HashSet::new();
    for (condition, winner) in &state.resolved_market_winners {
        if !conditions.contains(&condition.to_string())
            || allowed_winners.is_some_and(|allowed| !allowed.contains(winner))
        {
            continue;
        }
        tokens.insert(*winner);
        if let Some(context) = state.resolved_market_contexts.get(condition) {
            tokens.insert(context.up_token);
            tokens.insert(context.down_token);
        }
    }
    tokens
}

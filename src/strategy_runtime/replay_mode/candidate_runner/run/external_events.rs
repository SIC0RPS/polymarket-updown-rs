use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{send_symbol_views, CandidateCentralState, CandidateSlot};
use crate::strategy_runtime::replay_mode::candidate_runner::CandidateExternalInput;
use crate::strategy_runtime::replay_mode::prepared_arrow::PreparedBookUpdate;
use crate::strategy_runtime::replay_mode::PackedReplayTape;
use crate::strategy_runtime::symbol_process::{EventRef, SymbolId};

pub(super) fn apply_external_event(
    input: &CandidateExternalInput,
    event: EventRef,
    central: &mut CandidateCentralState,
) -> Result<bool> {
    use crate::strategy_runtime::replay_mode::prepared_event_stream::{
        EVENT_KIND_CLOCK, EVENT_KIND_LAST_TRADE, EVENT_KIND_WINDOW_BOOTSTRAP,
    };
    let publish_account_state = match input {
        CandidateExternalInput::MarketState {
            odds,
            projection,
            book_updates,
            receipt_identity,
            publish_account_state,
        } => {
            anyhow::ensure!(
                matches!(event.kind, EVENT_KIND_CLOCK | EVENT_KIND_WINDOW_BOOTSTRAP),
                "prepared market state carries invalid event kind={}",
                event.kind
            );
            for update in book_updates.iter() {
                match update {
                    PreparedBookUpdate::Ignore => {}
                    PreparedBookUpdate::Remove(token) => {
                        central.exchange.remove_replay_book(*token)
                    }
                    PreparedBookUpdate::Upsert { book, depth } => {
                        let token_id = book.token_id;
                        central
                            .exchange
                            .ingest_replay_book(book.clone(), receipt_identity.clone());
                        if let Some(depth) = depth {
                            central
                                .exchange
                                .ingest_replay_depth(depth.clone(), receipt_identity.clone());
                        } else {
                            central.exchange.remove_replay_depth(token_id);
                        }
                    }
                }
            }
            central.install_market_state(Arc::clone(odds), projection.as_ref())?;
            *publish_account_state
        }
        CandidateExternalInput::LastTrade {
            trade,
            receipt_identity,
        } => {
            anyhow::ensure!(
                event.kind == EVENT_KIND_LAST_TRADE,
                "prepared last trade carries invalid event kind={}",
                event.kind
            );
            central.exchange.process_captured_last_trade(
                trade.token_id,
                trade.price,
                trade.size,
                trade.side,
                event.timestamp_ms,
                receipt_identity.clone(),
            );
            false
        }
        CandidateExternalInput::ProviderResolution { .. } => {
            anyhow::bail!("provider resolution passed to symbol external owner")
        }
    };
    central.exchange.advance_time(event.timestamp_ms);
    Ok(publish_account_state)
}

pub(super) fn ensure_initial_admission(
    slot: &mut CandidateSlot,
    central: &mut CandidateCentralState,
    symbol: SymbolId,
    timestamp_ms: u64,
) -> Result<bool> {
    if central.admitted.contains(symbol.as_str()) {
        return Ok(false);
    }
    if !central
        .projection
        .markets()
        .iter()
        .any(|market| market.underlying.eq_ignore_ascii_case(symbol.as_str()))
    {
        return Ok(false);
    }
    let source_sequence = central
        .worker_source_sequences
        .get(symbol.as_str())
        .copied()
        .with_context(|| {
            format!(
                "candidate worker source sequence missing symbol={}",
                symbol.as_str()
            )
        })?;
    send_symbol_views(
        slot,
        central,
        symbol,
        source_sequence,
        timestamp_ms,
        true,
        false,
        &[],
    )?;
    central.admitted.insert(symbol.as_str().to_string());
    Ok(true)
}

pub(super) fn tape_for_symbol<'a>(
    tapes: &'a BTreeMap<String, Arc<PackedReplayTape>>,
    symbol: &str,
) -> Result<&'a Arc<PackedReplayTape>> {
    tapes
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
        .map(|(_, tape)| tape)
        .with_context(|| format!("candidate prepared tape missing symbol={symbol}"))
}

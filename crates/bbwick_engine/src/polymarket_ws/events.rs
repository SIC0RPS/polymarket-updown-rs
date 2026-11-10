use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use polymarket_client_sdk::clob::ws::interest::MessageInterest;
use polymarket_client_sdk::clob::ws::types::response::{
    parse_if_interested, PriceChangeBatchEntry, WsMessage,
};
use polymarket_client_sdk::types::U256;

use crate::live_lanes::PolymarketStateLane;
use crate::wire::PolymarketPriceChangeFrame;

use super::frames::{book_frame, last_trade_frame, price_change_entry_frame, tick_size_frame};
use super::PolymarketWsRuntimeConfig;

pub fn forward_raw_market_text(
    payload: &[u8],
    config: &PolymarketWsRuntimeConfig,
    state_lane: &PolymarketStateLane,
    last_err_sent_at: &mut Instant,
    branch_now: Instant,
) -> Result<()> {
    let Some(messages) = parse_market_text(payload, last_err_sent_at, branch_now) else {
        return Ok(());
    };
    let _ = forward_market_messages(messages, config, state_lane, None, None, None, None)?;
    Ok(())
}

pub(super) fn parse_market_text(
    payload: &[u8],
    last_err_sent_at: &mut Instant,
    branch_now: Instant,
) -> Option<Vec<WsMessage>> {
    parse_market_text_with_interest(
        payload,
        &MessageInterest::MARKET,
        last_err_sent_at,
        branch_now,
    )
}

pub(super) fn parse_market_text_with_interest(
    payload: &[u8],
    interest: &MessageInterest,
    last_err_sent_at: &mut Instant,
    branch_now: Instant,
) -> Option<Vec<WsMessage>> {
    match parse_if_interested(payload, interest) {
        Ok(messages) => Some(messages),
        Err(err) => {
            if branch_now.duration_since(*last_err_sent_at) >= Duration::from_secs(1) {
                tracing::warn!(
                    target: "engine.polymarket_ws",
                    error = %err,
                    "bbwick_polymarket_ws_parse_failed"
                );
                *last_err_sent_at = branch_now;
            }
            None
        }
    }
}

pub(super) struct BookProvenance<'a> {
    pub(super) digest: u64,
    pub(super) first_seen: Instant,
    pub(super) expires_at: Instant,
    pub(super) payload: &'a Arc<[u8]>,
}

pub(super) fn forward_market_messages(
    messages: Vec<WsMessage>,
    config: &PolymarketWsRuntimeConfig,
    state_lane: &PolymarketStateLane,
    provenance: Option<&BookProvenance<'_>>,
    frame_received_at: Option<Instant>,
    connection_session_id: Option<u64>,
    frame_sequence: Option<u64>,
) -> Result<Vec<(U256, u64)>> {
    let mut validated_books = Vec::new();

    for message in messages {
        match message {
            WsMessage::Book(event) if config.subscribe_orderbook => {
                let asset_id = event.asset_id;
                let market = event.market;
                let Some(frame) = book_frame(event) else {
                    state_lane.sync_invalid_ws_event(
                        market,
                        asset_id,
                        frame_received_at,
                        connection_session_id,
                        frame_sequence,
                    )?;
                    continue;
                };
                if let Some(provenance) = provenance {
                    if let Some(epoch) = state_lane.sync_book_with_provenance(
                        market,
                        &frame,
                        provenance.digest,
                        provenance.first_seen,
                        provenance.expires_at,
                        provenance.payload,
                        frame_received_at,
                        connection_session_id,
                        frame_sequence,
                    )? {
                        validated_books.push((asset_id, epoch));
                    }
                } else {
                    state_lane.sync_book(
                        market,
                        &frame,
                        frame_received_at,
                        connection_session_id,
                        frame_sequence,
                    )?;
                }
            }
            WsMessage::PriceChange(event) if config.subscribe_prices => {
                let market = event.market;
                let timestamp = event.timestamp;
                for (asset_id, entries) in group_price_changes(event.price_changes) {
                    let entries = entries
                        .into_iter()
                        .map(price_change_entry_frame)
                        .collect::<Option<Vec<_>>>();
                    let Some(entries) = entries else {
                        state_lane.sync_invalid_ws_event(
                            market,
                            asset_id,
                            frame_received_at,
                            connection_session_id,
                            frame_sequence,
                        )?;
                        continue;
                    };
                    state_lane.sync_price_change(
                        market,
                        &PolymarketPriceChangeFrame {
                            timestamp,
                            price_changes: entries,
                        },
                        frame_received_at,
                        connection_session_id,
                        frame_sequence,
                    )?;
                }
            }
            WsMessage::TickSizeChange(event) if config.subscribe_tick_size => {
                let market = event.market;
                let frame = tick_size_frame(event);
                state_lane.sync_tick_size(
                    market,
                    &frame,
                    frame_received_at,
                    connection_session_id,
                    frame_sequence,
                )?;
            }
            WsMessage::LastTradePrice(event) if config.subscribe_last_trade => {
                let asset_id = event.asset_id;
                let market = event.market;
                let Some(frame) = last_trade_frame(event) else {
                    state_lane.sync_invalid_ws_event(
                        market,
                        asset_id,
                        frame_received_at,
                        connection_session_id,
                        frame_sequence,
                    )?;
                    continue;
                };
                state_lane.sync_last_trade(
                    market,
                    &frame,
                    frame_received_at,
                    connection_session_id,
                    frame_sequence,
                )?;
            }
            _ => {}
        }
    }
    Ok(validated_books)
}

fn group_price_changes(
    entries: Vec<PriceChangeBatchEntry>,
) -> Vec<(U256, Vec<PriceChangeBatchEntry>)> {
    let mut grouped: Vec<(U256, Vec<PriceChangeBatchEntry>)> = Vec::new();
    for entry in entries {
        if let Some((_, group)) = grouped
            .iter_mut()
            .find(|(asset_id, _)| *asset_id == entry.asset_id)
        {
            group.push(entry);
        } else {
            grouped.push((entry.asset_id, vec![entry]));
        }
    }
    grouped
}

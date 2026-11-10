use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use polymarket_client_sdk::clob::ws::interest::MessageInterest;
use polymarket_client_sdk::clob::ws::types::response::WsMessage;
use polymarket_client_sdk::types::U256;
use tokio::sync::Notify;

use crate::live_lanes::PolymarketStateLane;
use crate::state::PolymarketOddsState;

use super::super::connection::GenerationHealth;
use super::super::dedupe::{DedupeTicket, EventDedupe};
use super::super::events::{
    forward_market_messages, parse_market_text_with_interest, BookProvenance,
};
use super::super::{PolymarketWsRuntimeConfig, WS_MARKET_LEG_COUNT};
use super::{
    elapsed_ms, log_first_book, log_generation_censored, log_readiness, mono_ns,
    GenerationEvidence, CLOCK_PRECISION_NS, WS_DELIVERY_SLOT_COUNT,
};

mod replacement;

#[derive(Default)]
struct LegDelivery {
    subscribed: Vec<U256>,
    books: Vec<(U256, u64)>,
    achieved_complete: bool,
    evidence: Option<GenerationEvidence>,
    pending_pong: Option<PendingPong>,
    last_processed_sequence: u64,
    health_revision: u64,
}

#[derive(Clone, Copy)]
struct PendingPong {
    generation: u64,
    health_deadline: Option<Instant>,
    hard_deadline: Instant,
}

#[derive(Clone, Copy)]
struct VoluntaryReplacement {
    leg: usize,
    incumbent_slot: usize,
    replacement_slot: Option<usize>,
}

#[derive(Default)]
struct HandoverReadiness {
    candidate_key: Option<(usize, u64)>,
    peer_key: Option<(usize, u64)>,
    candidate: Option<bool>,
    peer: Option<bool>,
}

struct CoverageGap {
    asset: U256,
    observed_open_at: Instant,
    left_censored: bool,
}

struct DeliveryState {
    desired: Vec<U256>,
    legs: [LegDelivery; WS_DELIVERY_SLOT_COUNT],
    serving: [usize; WS_MARKET_LEG_COUNT],
    readiness: [HandoverReadiness; WS_MARKET_LEG_COUNT],
    preparation_budget: Option<Duration>,
    voluntary_replacement: Option<VoluntaryReplacement>,
    coverage_observed: Vec<(U256, bool)>,
    gaps: Vec<CoverageGap>,
    reducer_running: bool,
}

impl Default for DeliveryState {
    fn default() -> Self {
        Self {
            desired: Vec::new(),
            legs: std::array::from_fn(|_| LegDelivery::default()),
            serving: std::array::from_fn(|leg| leg),
            readiness: std::array::from_fn(|_| HandoverReadiness::default()),
            preparation_budget: None,
            voluntary_replacement: None,
            coverage_observed: Vec::new(),
            gaps: Vec::new(),
            reducer_running: true,
        }
    }
}

/// The two-leg owner coordinates delivery evidence, planned rotation, and book invalidation.
pub(in crate::polymarket_ws) struct ServingLegs {
    symbol: &'static str,
    state_lane: PolymarketStateLane,
    state: Mutex<DeliveryState>,
    pub(super) changed: Notify,
    clock_origin: Instant,
}

pub(super) struct FinishedLeg {
    pub(super) remaining: usize,
    pub(super) achieved_complete: bool,
    pub(super) connection_session_id: Option<u64>,
    pub(super) launch_to_end_ms: u64,
    pub(super) connected_to_end_ms: Option<u64>,
    pub(super) end_mono_ns: u64,
}

impl ServingLegs {
    pub(super) fn new(symbol: &'static str, state_lane: PolymarketStateLane) -> Self {
        Self {
            symbol,
            state_lane,
            state: Mutex::new(DeliveryState::default()),
            changed: Notify::new(),
            clock_origin: Instant::now(),
        }
    }

    pub(super) fn start_generation(&self, slot: usize, leg: usize, generation: u64) {
        let now = Instant::now();
        let mut state = self.state.lock().expect("market leg state poisoned");
        state.legs[slot] = LegDelivery {
            evidence: Some(GenerationEvidence {
                leg,
                generation,
                launch_at: now,
                connection_session_id: None,
                connected_at: None,
            }),
            ..LegDelivery::default()
        };
        let serving_slot = state.serving[leg];
        if let Some(replacement) = state.voluntary_replacement.as_mut() {
            if replacement.leg == leg
                && replacement.incumbent_slot == serving_slot
                && slot != serving_slot
            {
                replacement.replacement_slot = Some(slot);
            }
        }
        tracing::info!(
            target: "engine.polymarket_ws",
            symbol = self.symbol,
            leg,
            slot,
            generation,
            launch_mono_ns = mono_ns(self.clock_origin, now),
            clock_precision_ns = CLOCK_PRECISION_NS,
            "bbwick_polymarket_ws_generation_launched"
        );
    }

    pub(in crate::polymarket_ws) fn apply_generation_health(
        &self,
        slot: usize,
        leg: usize,
        generation: u64,
        health: &GenerationHealth,
    ) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if !state.legs[slot]
            .evidence
            .is_some_and(|evidence| evidence.leg == leg && evidence.generation == generation)
        {
            return;
        }
        if health.revision <= state.legs[slot].health_revision {
            return;
        }
        state.legs[slot].health_revision = health.revision;
        if let Some(connected) = health.connected {
            let already_recorded = state.legs[slot]
                .evidence
                .and_then(|evidence| evidence.connection_session_id)
                == Some(connected.connection_session_id);
            if !already_recorded {
                let launch_to_connect_ms = state.legs[slot]
                    .evidence
                    .map(|evidence| elapsed_ms(evidence.launch_at, connected.connected_at));
                if let Some(evidence) = state.legs[slot].evidence.as_mut() {
                    evidence.connection_session_id = Some(connected.connection_session_id);
                    evidence.connected_at = Some(connected.connected_at);
                }
                tracing::info!(
                    target: "engine.polymarket_ws",
                    symbol = self.symbol,
                    leg,
                    slot,
                    generation,
                    connection_session_id = connected.connection_session_id,
                    asset_count = connected.asset_count,
                    connected_mono_ns = mono_ns(self.clock_origin, connected.connected_at),
                    launch_to_connect_ms = ?launch_to_connect_ms,
                    clock_precision_ns = CLOCK_PRECISION_NS,
                    "bbwick_polymarket_ws_connected"
                );
            }
        }
        let assets = health.subscribed.as_ref();
        if state.legs[slot].subscribed.as_slice() != assets {
            let now = health.subscription_observed_at.unwrap_or_else(Instant::now);
            let previous = {
                let delivery = &mut state.legs[slot];
                let previous_books = delivery.books.clone();
                delivery.books.retain(|(asset, _)| assets.contains(asset));
                (
                    std::mem::replace(&mut delivery.subscribed, assets.to_vec()),
                    previous_books,
                )
            };
            let serving = state.serving.contains(&slot);
            if let Err(err) = self.state_lane.with_book_authorities(|store| {
                mark_first_complete(&mut state, slot, |asset| store.book_authority_epoch(asset));
                observe_coverage(
                    &mut state,
                    self.symbol,
                    self.clock_origin,
                    now,
                    "subscription_changed",
                    |asset| store.book_authority_epoch(asset),
                    |asset| {
                        serving
                            && previous.0.contains(&asset)
                            && !assets.contains(&asset)
                            && previous
                                .1
                                .iter()
                                .find(|(book_asset, _)| *book_asset == asset)
                                .is_some_and(|(_, epoch)| {
                                    store.book_authority_epoch(asset) == Some(*epoch)
                                })
                    },
                );
            }) {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    symbol = self.symbol,
                    error = %err,
                    "bbwick_polymarket_coverage_observation_failed"
                );
            }
        }
        state.legs[slot].pending_pong = health.pending_pong.map(|pending| PendingPong {
            generation,
            health_deadline: pending
                .health_allowance
                .and_then(|allowance| pending.ping_started.checked_add(allowance)),
            hard_deadline: pending.hard_deadline,
        });
        drop(state);
        self.changed.notify_waiters();
    }

    pub(super) fn set_desired(
        &self,
        desired: Vec<U256>,
        is_current: impl FnOnce(&PolymarketOddsState) -> bool,
        publish: impl FnOnce(),
    ) -> Result<bool> {
        let now = Instant::now();
        let mut state = self.state.lock().expect("market leg state poisoned");
        let mut accepted = false;
        self.state_lane.sync_state(|store, canonical, _| {
            if !is_current(canonical) {
                return false;
            }
            let previous = std::mem::replace(&mut state.desired, desired);
            observe_coverage(
                &mut state,
                self.symbol,
                self.clock_origin,
                now,
                "desired_assets_changed",
                |asset| store.book_authority_epoch(asset),
                |asset| !previous.contains(&asset),
            );
            publish();
            accepted = true;
            false
        })?;
        if accepted {
            self.changed.notify_waiters();
        }
        Ok(accepted)
    }

    pub(in crate::polymarket_ws) fn acknowledge_frame(
        &self,
        slot: usize,
        generation: u64,
        sequence: u64,
    ) {
        let mut state = self.state.lock().expect("market leg state poisoned");
        if state.legs[slot]
            .evidence
            .is_some_and(|evidence| evidence.generation == generation)
        {
            state.legs[slot].last_processed_sequence = sequence;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    pub(in crate::polymarket_ws) async fn wait_for_processed(
        &self,
        slot: usize,
        generation: u64,
        terminal_sequence: u64,
    ) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Polymarket market leg state poisoned"))?;
                let delivery = &state.legs[slot];
                anyhow::ensure!(
                    delivery
                        .evidence
                        .is_some_and(|evidence| evidence.generation == generation),
                    "Polymarket generation retired before reducer acknowledgement"
                );
                if delivery.last_processed_sequence >= terminal_sequence {
                    return Ok(());
                }
                anyhow::ensure!(state.reducer_running, "Polymarket symbol reducer stopped");
            }
            changed.await;
        }
    }

    pub(in crate::polymarket_ws) fn mark_reducer_stopped(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reducer_running = false;
        self.changed.notify_waiters();
    }

    pub(in crate::polymarket_ws) fn process_market_text(
        &self,
        slot: usize,
        state_lane: &PolymarketStateLane,
        dedupe: &EventDedupe,
        payload: &[u8],
        config: &PolymarketWsRuntimeConfig,
        last_err_sent_at: &mut Instant,
        branch_now: Instant,
        frame_subscribed: &[U256],
        frame_received_at: Instant,
        connection_session_id: u64,
        frame_sequence: u64,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("market leg state poisoned");
        let ticket = dedupe.accept(&payload);
        let interest = if ticket.first {
            MessageInterest::MARKET
        } else {
            MessageInterest::BOOK
        };
        let Some(messages) =
            parse_market_text_with_interest(payload, &interest, last_err_sent_at, branch_now)
        else {
            return Ok(());
        };
        if messages
            .iter()
            .any(|message| matches!(message, WsMessage::Book(_)))
        {
            self.process_book_text(
                &mut state,
                slot,
                state_lane,
                ticket,
                messages,
                payload,
                config,
                frame_subscribed,
                frame_received_at,
                connection_session_id,
                frame_sequence,
            )
        } else if ticket.first {
            let _ = forward_market_messages(
                messages,
                config,
                state_lane,
                None,
                Some(frame_received_at),
                Some(connection_session_id),
                Some(frame_sequence),
            )?;
            Ok(())
        } else {
            Ok(())
        }
    }

    fn process_book_text(
        &self,
        state: &mut DeliveryState,
        slot: usize,
        state_lane: &PolymarketStateLane,
        ticket: DedupeTicket,
        messages: Vec<WsMessage>,
        payload: &[u8],
        config: &PolymarketWsRuntimeConfig,
        frame_subscribed: &[U256],
        frame_received_at: Instant,
        connection_session_id: u64,
        frame_sequence: u64,
    ) -> Result<()> {
        let validated = if ticket.first {
            let retained_payload = Arc::<[u8]>::from(payload);
            let provenance = BookProvenance {
                digest: ticket.digest,
                first_seen: ticket.first_seen,
                expires_at: ticket.expires_at(),
                payload: &retained_payload,
            };
            forward_market_messages(
                messages,
                config,
                state_lane,
                Some(&provenance),
                Some(frame_received_at),
                Some(connection_session_id),
                Some(frame_sequence),
            )?
        } else {
            let assets = messages
                .into_iter()
                .filter_map(|message| match message {
                    WsMessage::Book(book) => Some(book.asset_id),
                    _ => None,
                })
                .collect::<Vec<_>>();
            state_lane.validated_book_epochs(&assets, ticket.digest, ticket.first_seen, payload)?
        };

        let now = Instant::now();
        let changed = state_lane.with_book_authorities(|store| {
            let mut changed = false;
            for (asset, epoch) in validated {
                if !frame_subscribed.contains(&asset)
                    || store.book_authority_epoch(asset) != Some(epoch)
                {
                    continue;
                }
                let first_book = {
                    let delivery = &mut state.legs[slot];
                    if let Some((_, current_epoch)) = delivery
                        .books
                        .iter_mut()
                        .find(|(current_asset, _)| *current_asset == asset)
                    {
                        if *current_epoch == epoch {
                            false
                        } else {
                            *current_epoch = epoch;
                            changed = true;
                            false
                        }
                    } else {
                        delivery.books.push((asset, epoch));
                        changed = true;
                        true
                    }
                };
                if first_book {
                    log_first_book(
                        self.symbol,
                        self.clock_origin,
                        now,
                        state.legs[slot].evidence,
                        asset,
                        epoch,
                    );
                }
            }
            changed |= mark_first_complete(state, slot, |candidate| {
                store.book_authority_epoch(candidate)
            });
            changed |= observe_coverage(
                state,
                self.symbol,
                self.clock_origin,
                now,
                "validated_book_delivered",
                |candidate| store.book_authority_epoch(candidate),
                |_| false,
            );
            changed
        })?;
        if changed {
            self.changed.notify_waiters();
        }
        Ok(())
    }
}

impl Drop for ServingLegs {
    fn drop(&mut self) {
        let now = Instant::now();
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        for delivery in &state.legs {
            if let Some(evidence) = delivery.evidence {
                log_generation_censored(
                    self.symbol,
                    self.clock_origin,
                    now,
                    evidence,
                    "observation_cutoff",
                );
            }
        }
        for gap in &state.gaps {
            log_gap_censored(
                self.symbol,
                self.clock_origin,
                now,
                gap,
                "observation_cutoff",
            );
        }
    }
}

fn mark_first_complete(
    state: &mut DeliveryState,
    slot: usize,
    mut current_epoch: impl FnMut(U256) -> Option<u64>,
) -> bool {
    if state.legs[slot].achieved_complete
        || !delivery_has_current_coverage(&state.legs[slot], &state.desired, &mut current_epoch)
    {
        return false;
    }
    state.legs[slot].achieved_complete = true;
    let Some(evidence) = state.legs[slot].evidence else {
        return true;
    };
    let preparation = Instant::now().saturating_duration_since(evidence.launch_at);
    if state
        .preparation_budget
        .is_none_or(|current| preparation > current)
    {
        state.preparation_budget = Some(preparation);
    }
    true
}

fn delivery_has_current_coverage(
    delivery: &LegDelivery,
    desired: &[U256],
    mut current_epoch: impl FnMut(U256) -> Option<u64>,
) -> bool {
    !desired.is_empty()
        && delivery.subscribed.len() == desired.len()
        && desired.iter().all(|asset| {
            delivery.subscribed.contains(asset)
                && delivery
                    .books
                    .iter()
                    .find(|(seen_asset, _)| seen_asset == asset)
                    .is_some_and(|(_, epoch)| current_epoch(*asset) == Some(*epoch))
        })
}

fn asset_has_current_coverage(
    state: &DeliveryState,
    asset: U256,
    current_epoch: &mut impl FnMut(U256) -> Option<u64>,
) -> bool {
    state.serving.iter().any(|slot| {
        let delivery = &state.legs[*slot];
        delivery.subscribed.contains(&asset)
            && delivery
                .books
                .iter()
                .find(|(seen_asset, _)| *seen_asset == asset)
                .is_some_and(|(_, epoch)| current_epoch(asset) == Some(*epoch))
    })
}

fn observe_coverage(
    state: &mut DeliveryState,
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    reason: &'static str,
    mut current_epoch: impl FnMut(U256) -> Option<u64>,
    mut exact_transition: impl FnMut(U256) -> bool,
) -> bool {
    let mut changed = false;
    let mut gap_index = 0;
    while gap_index < state.gaps.len() {
        if state.desired.contains(&state.gaps[gap_index].asset) {
            gap_index += 1;
            continue;
        }
        let gap = state.gaps.remove(gap_index);
        log_gap_censored(symbol, clock_origin, now, &gap, "desired_asset_removed");
        changed = true;
    }

    for desired_index in 0..state.desired.len() {
        let asset = state.desired[desired_index];
        let covered = asset_has_current_coverage(state, asset, &mut current_epoch);
        let previous = state
            .coverage_observed
            .iter()
            .find(|(observed_asset, _)| *observed_asset == asset)
            .map(|(_, covered)| *covered);
        let open_gap = state.gaps.iter().position(|gap| gap.asset == asset);
        match (covered, open_gap) {
            (true, Some(index)) => {
                let gap = state.gaps.remove(index);
                log_gap_closed(symbol, clock_origin, now, gap, reason);
                changed = true;
            }
            (false, None) => {
                let left_censored = previous != Some(true) || !exact_transition(asset);
                state.gaps.push(CoverageGap {
                    asset,
                    observed_open_at: now,
                    left_censored,
                });
                changed = true;
                tracing::warn!(
                    target: "engine.polymarket_ws",
                    symbol,
                    asset_id = %asset,
                    observed_at_mono_ns = mono_ns(clock_origin, now),
                    clock_precision_ns = CLOCK_PRECISION_NS,
                    left_censored,
                    right_censored = false,
                    previously_covered = ?previous,
                    reason,
                    "bbwick_polymarket_ws_coverage_gap_opened"
                );
            }
            _ => {}
        }
        if let Some((_, observed)) = state
            .coverage_observed
            .iter_mut()
            .find(|(observed_asset, _)| *observed_asset == asset)
        {
            changed |= *observed != covered;
            *observed = covered;
        } else {
            state.coverage_observed.push((asset, covered));
            changed = true;
        }
    }
    let observed_len = state.coverage_observed.len();
    state
        .coverage_observed
        .retain(|(asset, _)| state.desired.contains(asset));
    changed || observed_len != state.coverage_observed.len()
}

#[allow(clippy::too_many_arguments)]
fn observe_readiness(
    state: &mut DeliveryState,
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    rotation_leg: usize,
    candidate_slot: usize,
    peer_slot: usize,
    candidate_ready: bool,
    peer_ready: bool,
) {
    let candidate = state.legs[candidate_slot].evidence;
    let peer = state.legs[peer_slot].evidence;
    let candidate_key = candidate.map(|evidence| (candidate_slot, evidence.generation));
    let peer_key = peer.map(|evidence| (peer_slot, evidence.generation));
    if state.readiness[rotation_leg].candidate_key != candidate_key {
        state.readiness[rotation_leg] = HandoverReadiness {
            candidate_key,
            peer_key: None,
            candidate: None,
            peer: None,
        };
    }
    if state.readiness[rotation_leg].candidate != Some(candidate_ready) {
        log_readiness(
            symbol,
            clock_origin,
            now,
            rotation_leg,
            "candidate",
            candidate,
            candidate_ready,
        );
        state.readiness[rotation_leg].candidate = Some(candidate_ready);
    }
    if state.readiness[rotation_leg].peer_key != peer_key {
        state.readiness[rotation_leg].peer_key = peer_key;
        state.readiness[rotation_leg].peer = None;
    }
    if state.readiness[rotation_leg].peer != Some(peer_ready) {
        log_readiness(
            symbol,
            clock_origin,
            now,
            rotation_leg,
            "peer",
            peer,
            peer_ready,
        );
        state.readiness[rotation_leg].peer = Some(peer_ready);
    }
}

fn log_gap_closed(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    gap: CoverageGap,
    reason: &'static str,
) {
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        asset_id = %gap.asset,
        observed_at_mono_ns = mono_ns(clock_origin, now),
        observed_gap_ms = elapsed_ms(gap.observed_open_at, now),
        clock_precision_ns = CLOCK_PRECISION_NS,
        left_censored = gap.left_censored,
        right_censored = false,
        reason,
        "bbwick_polymarket_ws_coverage_gap_closed"
    );
}

fn log_gap_censored(
    symbol: &'static str,
    clock_origin: Instant,
    now: Instant,
    gap: &CoverageGap,
    reason: &'static str,
) {
    tracing::info!(
        target: "engine.polymarket_ws",
        symbol,
        asset_id = %gap.asset,
        observed_at_mono_ns = mono_ns(clock_origin, now),
        observed_gap_ms = elapsed_ms(gap.observed_open_at, now),
        clock_precision_ns = CLOCK_PRECISION_NS,
        left_censored = gap.left_censored,
        right_censored = true,
        reason,
        "bbwick_polymarket_ws_coverage_gap_censored"
    );
}

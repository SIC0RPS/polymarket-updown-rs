use std::time::{Duration, Instant};

use super::super::super::WS_MARKET_LEG_COUNT;
use super::super::{elapsed_ms, log_generation_censored, log_handover, mono_ns};
use super::{
    asset_has_current_coverage, delivery_has_current_coverage, observe_coverage, observe_readiness,
    FinishedLeg, HandoverReadiness, LegDelivery, PendingPong, ServingLegs, VoluntaryReplacement,
};

impl ServingLegs {
    pub(in crate::polymarket_ws) fn next_voluntary_deadline(
        &self,
        leg: usize,
        incumbent_slot: usize,
        incumbent_generation: u64,
    ) -> Option<Instant> {
        let state = self.state.lock().expect("market leg state poisoned");
        if state.voluntary_replacement.is_some()
            || state.serving.get(leg).copied() != Some(incumbent_slot)
            || !generation_matches(&state.legs[incumbent_slot], leg, incumbent_generation)
        {
            return None;
        }
        let (deadline, warning_leg, warning_slot, warning_generation) = earliest_warning(&state)?;
        (warning_leg == leg
            && warning_slot == incumbent_slot
            && warning_generation == incumbent_generation)
            .then_some(deadline)
    }

    pub(in crate::polymarket_ws) fn claim_voluntary_replacement(
        &self,
        leg: usize,
        incumbent_slot: usize,
        incumbent_generation: u64,
        now: Instant,
    ) -> bool {
        let mut state = self.state.lock().expect("market leg state poisoned");
        if state.voluntary_replacement.is_some()
            || state.serving.get(leg).copied() != Some(incumbent_slot)
            || !generation_matches(&state.legs[incumbent_slot], leg, incumbent_generation)
        {
            return false;
        }
        let Some((deadline, warning_leg, warning_slot, warning_generation)) =
            earliest_warning(&state)
        else {
            return false;
        };
        if deadline > now
            || warning_leg != leg
            || warning_slot != incumbent_slot
            || warning_generation != incumbent_generation
        {
            return false;
        }
        state.voluntary_replacement = Some(VoluntaryReplacement {
            leg,
            incumbent_slot,
            replacement_slot: None,
        });
        drop(state);
        self.changed.notify_waiters();
        true
    }

    pub(in crate::polymarket_ws::runtime) fn handover_ready(
        &self,
        leg: usize,
        incumbent_slot: usize,
        replacement_slot: usize,
    ) -> Option<bool> {
        let now = Instant::now();
        let mut state = self.state.lock().expect("market leg state poisoned");
        self.state_lane
            .with_book_authorities(|store| {
                if state.serving[leg] != incumbent_slot {
                    return None;
                }
                let peer_slot = state.serving[(leg + 1) % WS_MARKET_LEG_COUNT];
                let candidate_ready = delivery_has_current_coverage(
                    &state.legs[replacement_slot],
                    &state.desired,
                    |asset| store.book_authority_epoch(asset),
                );
                let peer_ready = delivery_has_current_coverage(
                    &state.legs[peer_slot],
                    &state.desired,
                    |asset| store.book_authority_epoch(asset),
                );
                observe_readiness(
                    &mut state,
                    self.symbol,
                    self.clock_origin,
                    now,
                    leg,
                    replacement_slot,
                    peer_slot,
                    candidate_ready,
                    peer_ready,
                );
                candidate_ready.then_some(
                    state.legs[incumbent_slot].achieved_complete
                        || state.legs[replacement_slot].achieved_complete,
                )
            })
            .ok()
            .flatten()
    }

    pub(in crate::polymarket_ws::runtime) fn commit_handover(
        &self,
        leg: usize,
        incumbent_slot: usize,
        replacement_slot: usize,
    ) -> Option<bool> {
        let now = Instant::now();
        let mut state = self.state.lock().expect("market leg state poisoned");
        let handover = match self.state_lane.with_book_authorities(|store| {
            if state.serving[leg] != incumbent_slot {
                return None;
            }
            let peer_slot = state.serving[(leg + 1) % WS_MARKET_LEG_COUNT];
            let candidate_ready = delivery_has_current_coverage(
                &state.legs[replacement_slot],
                &state.desired,
                |asset| store.book_authority_epoch(asset),
            );
            if !candidate_ready {
                return None;
            }
            let incumbent = state.legs[incumbent_slot].evidence;
            let replacement = state.legs[replacement_slot].evidence;
            let peer = state.legs[peer_slot].evidence;
            let reset_backoff = state.legs[incumbent_slot].achieved_complete
                || state.legs[replacement_slot].achieved_complete;
            log_handover(
                self.symbol,
                self.clock_origin,
                now,
                leg,
                incumbent,
                replacement,
                peer,
            );
            if let Some(incumbent) = incumbent {
                log_generation_censored(
                    self.symbol,
                    self.clock_origin,
                    now,
                    incumbent,
                    "planned_handover",
                );
            }
            state.legs[incumbent_slot] = LegDelivery::default();
            state.serving[leg] = replacement_slot;
            state.readiness[leg] = HandoverReadiness::default();
            if state
                .voluntary_replacement
                .is_some_and(|active| active.leg == leg)
            {
                state.voluntary_replacement = None;
            }
            observe_coverage(
                &mut state,
                self.symbol,
                self.clock_origin,
                now,
                "planned_handover",
                |asset| store.book_authority_epoch(asset),
                |_| false,
            );
            Some(reset_backoff)
        }) {
            Ok(handover) => handover,
            Err(err) => {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    error = %err,
                    "bbwick_polymarket_rotation_check_failed"
                );
                None
            }
        };
        if handover.is_some() {
            self.changed.notify_waiters();
        }
        handover
    }

    pub(in crate::polymarket_ws::runtime) fn finish(&self, slot: usize) -> FinishedLeg {
        self.finish_generation(slot, None)
    }

    pub(in crate::polymarket_ws::runtime) fn replace_failed_incumbent(
        &self,
        leg: usize,
        incumbent_slot: usize,
        replacement_slot: usize,
    ) -> FinishedLeg {
        self.finish_generation(incumbent_slot, Some((leg, replacement_slot)))
    }

    fn finish_generation(&self, slot: usize, replacement: Option<(usize, usize)>) -> FinishedLeg {
        let now = Instant::now();
        let mut state = self.state.lock().expect("market leg state poisoned");
        let was_serving = state.serving.contains(&slot);
        let ended = std::mem::take(&mut state.legs[slot]);
        let achieved_complete = ended.achieved_complete;
        let evidence = ended.evidence;

        let ended_voluntary_generation = state.voluntary_replacement.is_some_and(|active| {
            active.incumbent_slot == slot || active.replacement_slot == Some(slot)
        });
        if ended_voluntary_generation {
            state.voluntary_replacement = None;
        }

        if let Some((leg, replacement_slot)) = replacement {
            state.serving[leg] = replacement_slot;
            state.readiness[leg] = HandoverReadiness::default();
        }
        let remaining = match self.state_lane.finish_transport(|store| {
            observe_coverage(
                &mut state,
                self.symbol,
                self.clock_origin,
                now,
                "generation_ended",
                |asset| store.book_authority_epoch(asset),
                |asset| {
                    was_serving
                        && ended.subscribed.contains(&asset)
                        && ended
                            .books
                            .iter()
                            .find(|(book_asset, _)| *book_asset == asset)
                            .is_some_and(|(_, epoch)| {
                                store.book_authority_epoch(asset) == Some(*epoch)
                            })
                },
            );
            let remaining = state
                .serving
                .iter()
                .filter(|slot| {
                    let delivery = &state.legs[**slot];
                    delivery.books.iter().any(|(asset, epoch)| {
                        state.desired.contains(asset)
                            && delivery.subscribed.contains(asset)
                            && store.book_authority_epoch(*asset) == Some(*epoch)
                    })
                })
                .count();
            let mut current_epoch = |asset| store.book_authority_epoch(asset);
            let unserved = state
                .desired
                .iter()
                .copied()
                .filter(|asset| !asset_has_current_coverage(&state, *asset, &mut current_epoch))
                .collect();
            (remaining, unserved)
        }) {
            Ok(remaining) => remaining,
            Err(err) => {
                tracing::error!(
                    target: "engine.polymarket_ws",
                    error = %err,
                    "bbwick_polymarket_state_clear_failed"
                );
                0
            }
        };
        if remaining == 0 {
            tracing::error!(
                target: "engine.polymarket_ws",
                symbol = self.symbol,
                "bbwick_polymarket_all_market_legs_down"
            );
        }
        self.changed.notify_waiters();
        FinishedLeg {
            remaining,
            achieved_complete,
            connection_session_id: evidence.and_then(|evidence| evidence.connection_session_id),
            launch_to_end_ms: evidence
                .map(|evidence| elapsed_ms(evidence.launch_at, now))
                .unwrap_or(0),
            connected_to_end_ms: evidence
                .and_then(|evidence| evidence.connected_at)
                .map(|connected_at| elapsed_ms(connected_at, now)),
            end_mono_ns: mono_ns(self.clock_origin, now),
        }
    }
}

fn generation_matches(delivery: &LegDelivery, leg: usize, generation: u64) -> bool {
    delivery
        .evidence
        .is_some_and(|evidence| evidence.leg == leg && evidence.generation == generation)
}

fn earliest_warning(state: &super::DeliveryState) -> Option<(Instant, usize, usize, u64)> {
    state
        .serving
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(leg, slot)| {
            let delivery = &state.legs[slot];
            let evidence = delivery.evidence?;
            let pending = delivery.pending_pong?;
            if evidence.leg != leg || evidence.generation != pending.generation {
                return None;
            }
            let deadline = warning_deadline(pending, state.preparation_budget)?;
            Some((deadline, leg, slot, evidence.generation))
        })
        .min_by_key(|(deadline, leg, _, _)| (*deadline, *leg))
}

fn warning_deadline(pending: PendingPong, preparation_budget: Option<Duration>) -> Option<Instant> {
    let preparation_deadline =
        preparation_budget.and_then(|budget| pending.hard_deadline.checked_sub(budget));
    match (pending.health_deadline, preparation_deadline) {
        (Some(health), Some(preparation)) => Some(health.min(preparation)),
        (Some(health), None) => Some(health),
        (None, Some(preparation)) => Some(preparation),
        (None, None) => None,
    }
}

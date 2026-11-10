use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;
use tokio::sync::watch;

use super::super::connection::{run_market_leg, GenerationHealth, MarketFrame, MarketLegTerminal};
use super::super::dedupe::EventDedupe;
use super::super::generation::next_ws_generation;
use super::super::{
    PolymarketWsRuntimeConfig, WS_MARKET_LEG_COUNT, WS_REBUILD_BACKOFF_JITTER_MS,
    WS_REBUILD_INITIAL_BACKOFF, WS_REBUILD_MAX_BACKOFF,
};
use super::coverage::FinishedLeg;
use super::{LegReaders, ReaderJoin, ServingLegs, CLOCK_PRECISION_NS};

struct RunningGeneration {
    slot: usize,
    generation: u64,
    stop_tx: watch::Sender<bool>,
    health_rx: watch::Receiver<Arc<GenerationHealth>>,
    task: ReaderJoin,
    readers: Arc<LegReaders>,
}

async fn join_reader(task: &ReaderJoin) -> Result<MarketLegTerminal> {
    let mut task = task.lock().await;
    task.as_mut()
        .context("Polymarket market reader task missing")?
        .await
        .context("Polymarket market reader task failed")
}

impl RunningGeneration {
    fn sync_health(&mut self, leg: usize, live_legs: &ServingLegs) {
        let health = self.health_rx.borrow_and_update().clone();
        live_legs.apply_generation_health(self.slot, leg, self.generation, health.as_ref());
    }

    async fn is_finished(&self) -> bool {
        self.task
            .lock()
            .await
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
    }

    async fn stop_and_settle(
        self,
        leg: usize,
        config: &PolymarketWsRuntimeConfig,
        live_legs: &ServingLegs,
    ) -> Result<MarketLegTerminal> {
        self.stop_tx.send_replace(true);
        let joined = join_reader(&self.task).await;
        self.settle_joined(leg, config, live_legs, joined).await
    }

    async fn settle_joined(
        mut self,
        leg: usize,
        config: &PolymarketWsRuntimeConfig,
        live_legs: &ServingLegs,
        joined: Result<MarketLegTerminal>,
    ) -> Result<MarketLegTerminal> {
        self.task.lock().await.take();
        self.readers.remove(self.slot, self.generation);
        self.sync_health(leg, live_legs);
        let terminal = joined?;
        if let (Some(capture), Some(reason)) = (&config.ingress_capture, terminal.loss_reason) {
            capture.mark_loss(reason);
        }
        if terminal.last_enqueued_sequence > 0 {
            live_legs
                .wait_for_processed(self.slot, self.generation, terminal.last_enqueued_sequence)
                .await?;
        }
        anyhow::ensure!(
            !terminal.channel_closed,
            "Polymarket symbol reducer channel closed"
        );
        Ok(terminal)
    }
}

/// Runs one leg for the life of the process.
///
/// A leg is never reconnected in place. Every exit drops the socket and builds a brand new
/// generation that subscribes from scratch.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_leg_forever(
    symbol: &'static str,
    leg: usize,
    config: Arc<PolymarketWsRuntimeConfig>,
    mut assets_rx: watch::Receiver<Vec<U256>>,
    dedupe: Arc<EventDedupe>,
    live_legs: Arc<ServingLegs>,
    frame_tx: tokio::sync::mpsc::Sender<MarketFrame>,
    mut shutdown_rx: watch::Receiver<bool>,
    readers: Arc<LegReaders>,
) -> Result<()> {
    while assets_rx.borrow().is_empty() {
        if *shutdown_rx.borrow() {
            return Ok(());
        }
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow_and_update() {
                    return Ok(());
                }
            }
            changed = assets_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
    }
    if *shutdown_rx.borrow() {
        return Ok(());
    }
    let mut next_generation = 1_u64;
    let mut backoff = WS_REBUILD_INITIAL_BACKOFF;

    let start_generation = |slot: usize, generation: u64| -> RunningGeneration {
        live_legs.start_generation(slot, leg, generation);
        let config = Arc::clone(&config);
        let mut generation_assets_rx = assets_rx.clone();
        let frame_tx = frame_tx.clone();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let initial_health = Arc::new(GenerationHealth::initial());
        let (health_tx, health_rx) = watch::channel(initial_health);
        let task = tokio::spawn(async move {
            match run_market_leg(
                slot,
                generation,
                &config,
                &mut generation_assets_rx,
                &frame_tx,
                &health_tx,
                &mut stop_rx,
            )
            .await
            {
                Ok(terminal) => terminal,
                Err(err) => MarketLegTerminal {
                    last_enqueued_sequence: 0,
                    loss_reason: None,
                    channel_closed: false,
                    outcome: Err(err),
                },
            }
        });
        let task = readers.register(slot, generation, stop_tx.clone(), health_rx.clone(), task);
        RunningGeneration {
            slot,
            generation,
            stop_tx,
            health_rx,
            task,
            readers: Arc::clone(&readers),
        }
    };

    let mut incumbent_slot = leg;
    let mut incumbent_generation = next_ws_generation(&mut next_generation);
    let mut incumbent = start_generation(incumbent_slot, incumbent_generation);

    'serving: loop {
        if *shutdown_rx.borrow() {
            let terminal = incumbent.stop_and_settle(leg, &config, &live_legs).await?;
            let finished = live_legs.finish(incumbent_slot);
            log_abnormal_leg_end(
                symbol,
                leg,
                incumbent_generation,
                &finished,
                terminal.outcome,
            );
            return Ok(());
        }
        incumbent.sync_health(leg, &live_legs);
        // Register before checking the deadline so a concurrent PONG, P update, or
        // generation transition cannot be lost before the next poll.
        let changed = live_legs.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let warning_at =
            live_legs.next_voluntary_deadline(leg, incumbent_slot, incumbent_generation);
        let warning = async {
            if let Some(deadline) = warning_at {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(warning);

        enum ServingEvent {
            Ended(Result<MarketLegTerminal>),
            Changed,
            Warning,
            Shutdown,
            Health,
        }
        let event = tokio::select! {
            biased;
            result = join_reader(&incumbent.task) => ServingEvent::Ended(result),
            result = shutdown_rx.changed() => {
                let _ = result;
                ServingEvent::Shutdown
            },
            result = incumbent.health_rx.changed() => {
                let _ = result;
                ServingEvent::Health
            },
            _ = &mut changed => ServingEvent::Changed,
            _ = &mut warning => ServingEvent::Warning,
        };

        if matches!(event, ServingEvent::Shutdown) && *shutdown_rx.borrow_and_update() {
            let terminal = incumbent.stop_and_settle(leg, &config, &live_legs).await?;
            let finished = live_legs.finish(incumbent_slot);
            log_abnormal_leg_end(
                symbol,
                leg,
                incumbent_generation,
                &finished,
                terminal.outcome,
            );
            return Ok(());
        }
        if matches!(
            event,
            ServingEvent::Changed | ServingEvent::Health | ServingEvent::Shutdown
        ) {
            continue;
        }
        if let ServingEvent::Ended(joined) = event {
            let terminal = incumbent
                .settle_joined(leg, &config, &live_legs, joined)
                .await?;
            let finished = live_legs.finish(incumbent_slot);
            log_abnormal_leg_end(
                symbol,
                leg,
                incumbent_generation,
                &finished,
                terminal.outcome,
            );
            log_arbitration_counters(symbol, leg, &dedupe);
            if finished.achieved_complete {
                backoff = WS_REBUILD_INITIAL_BACKOFF;
            }
            tokio::time::sleep(backoff + rebuild_jitter()).await;
            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
            if *shutdown_rx.borrow() {
                return Ok(());
            }
            incumbent_generation = next_ws_generation(&mut next_generation);
            incumbent = start_generation(incumbent_slot, incumbent_generation);
            continue;
        }

        if !live_legs.claim_voluntary_replacement(
            leg,
            incumbent_slot,
            incumbent_generation,
            Instant::now(),
        ) {
            continue;
        }

        let replacement_slot = if incumbent_slot == leg {
            leg + WS_MARKET_LEG_COUNT
        } else {
            leg
        };
        let mut replacement_generation = next_ws_generation(&mut next_generation);
        let mut replacement = start_generation(replacement_slot, replacement_generation);

        'replacement: loop {
            if *shutdown_rx.borrow() {
                let incumbent_result = incumbent.stop_and_settle(leg, &config, &live_legs).await;
                let replacement_result =
                    replacement.stop_and_settle(leg, &config, &live_legs).await;
                let incumbent_terminal = incumbent_result?;
                let replacement_terminal = replacement_result?;
                let incumbent_finished = live_legs.finish(incumbent_slot);
                log_abnormal_leg_end(
                    symbol,
                    leg,
                    incumbent_generation,
                    &incumbent_finished,
                    incumbent_terminal.outcome,
                );
                let replacement_finished = live_legs.finish(replacement_slot);
                log_abnormal_leg_end(
                    symbol,
                    leg,
                    replacement_generation,
                    &replacement_finished,
                    replacement_terminal.outcome,
                );
                return Ok(());
            }
            // Register before checking readiness so a concurrent admission cannot be lost
            // between the atomic candidate check and the next poll.
            let changed = live_legs.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            incumbent.sync_health(leg, &live_legs);
            replacement.sync_health(leg, &live_legs);
            if !replacement.is_finished().await
                && live_legs
                    .handover_ready(leg, incumbent_slot, replacement_slot)
                    .is_some()
            {
                let stopped_incumbent = incumbent.stop_and_settle(leg, &config, &live_legs).await?;
                let reset_backoff = loop {
                    if !replacement.is_finished().await {
                        if let Some(reset) =
                            live_legs.commit_handover(leg, incumbent_slot, replacement_slot)
                        {
                            break reset;
                        }
                    }
                    tokio::select! {
                        result = join_reader(&replacement.task) => {
                            let terminal = replacement
                                .settle_joined(leg, &config, &live_legs, result)
                                .await?;
                            let replacement_finished = live_legs.finish(replacement_slot);
                            log_abnormal_leg_end(
                                symbol, leg, replacement_generation, &replacement_finished,
                                terminal.outcome,
                            );
                            let incumbent_finished = live_legs.finish(incumbent_slot);
                            log_abnormal_leg_end(
                                symbol, leg, incumbent_generation, &incumbent_finished,
                                stopped_incumbent.outcome,
                            );
                            tokio::time::sleep(backoff + rebuild_jitter()).await;
                            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
                            if *shutdown_rx.borrow() {
                                return Ok(());
                            }
                            incumbent_generation = next_ws_generation(&mut next_generation);
                            incumbent = start_generation(incumbent_slot, incumbent_generation);
                            continue 'serving;
                        }
                        result = shutdown_rx.changed() => {
                            let _ = result;
                            if *shutdown_rx.borrow_and_update() {
                                let terminal = replacement
                                    .stop_and_settle(leg, &config, &live_legs)
                                    .await?;
                                let replacement_finished = live_legs.finish(replacement_slot);
                                log_abnormal_leg_end(
                                    symbol, leg, replacement_generation, &replacement_finished,
                                    terminal.outcome,
                                );
                                let incumbent_finished = live_legs.finish(incumbent_slot);
                                log_abnormal_leg_end(
                                    symbol, leg, incumbent_generation, &incumbent_finished,
                                    stopped_incumbent.outcome,
                                );
                                return Ok(());
                            }
                        }
                        result = replacement.health_rx.changed(), if replacement.health_rx.has_changed().is_ok() => {
                            let _ = result;
                            replacement.sync_health(leg, &live_legs);
                        }
                        _ = live_legs.changed.notified() => {}
                    }
                };
                log_arbitration_counters(symbol, leg, &dedupe);
                if reset_backoff {
                    backoff = WS_REBUILD_INITIAL_BACKOFF;
                }
                incumbent_slot = replacement_slot;
                incumbent_generation = replacement_generation;
                incumbent = replacement;
                continue 'serving;
            }

            tokio::select! {
                biased;
                result = shutdown_rx.changed() => {
                    let _ = result;
                    if *shutdown_rx.borrow_and_update() {
                        let incumbent_result = incumbent
                            .stop_and_settle(leg, &config, &live_legs)
                            .await;
                        let replacement_result = replacement
                            .stop_and_settle(leg, &config, &live_legs)
                            .await;
                        let incumbent_terminal = incumbent_result?;
                        let replacement_terminal = replacement_result?;
                        let incumbent_finished = live_legs.finish(incumbent_slot);
                        log_abnormal_leg_end(
                            symbol, leg, incumbent_generation, &incumbent_finished,
                            incumbent_terminal.outcome,
                        );
                        let replacement_finished = live_legs.finish(replacement_slot);
                        log_abnormal_leg_end(
                            symbol, leg, replacement_generation, &replacement_finished,
                            replacement_terminal.outcome,
                        );
                        return Ok(());
                    }
                }
                result = incumbent.health_rx.changed(), if incumbent.health_rx.has_changed().is_ok() => {
                    let _ = result;
                }
                result = replacement.health_rx.changed(), if replacement.health_rx.has_changed().is_ok() => {
                    let _ = result;
                }
                outcome = join_reader(&incumbent.task) => {
                    let terminal = incumbent
                        .settle_joined(leg, &config, &live_legs, outcome)
                        .await?;
                    let finished = live_legs.replace_failed_incumbent(
                        leg,
                        incumbent_slot,
                        replacement_slot,
                    );
                    log_abnormal_leg_end(
                        symbol,
                        leg,
                        incumbent_generation,
                        &finished,
                        terminal.outcome,
                    );
                    log_arbitration_counters(symbol, leg, &dedupe);
                    if finished.achieved_complete {
                        backoff = WS_REBUILD_INITIAL_BACKOFF;
                    }
                    let delay = tokio::time::sleep(backoff + rebuild_jitter());
                    tokio::pin!(delay);
                    tokio::select! {
                        biased;
                        replacement_outcome = join_reader(&replacement.task) => {
                            let terminal = replacement
                                .settle_joined(leg, &config, &live_legs, replacement_outcome)
                                .await?;
                            let replacement_finished = live_legs.finish(replacement_slot);
                            log_abnormal_leg_end(
                                symbol,
                                leg,
                                replacement_generation,
                                &replacement_finished,
                                terminal.outcome,
                            );
                            log_arbitration_counters(symbol, leg, &dedupe);
                            if replacement_finished.achieved_complete {
                                backoff = WS_REBUILD_INITIAL_BACKOFF;
                            }
                            tokio::time::sleep(backoff + rebuild_jitter()).await;
                            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
                            if *shutdown_rx.borrow() {
                                return Ok(());
                            }
                            incumbent_slot = replacement_slot;
                            incumbent_generation = next_ws_generation(&mut next_generation);
                            incumbent = start_generation(incumbent_slot, incumbent_generation);
                            continue 'serving;
                        }
                        _ = &mut delay => {
                            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
                            incumbent_slot = replacement_slot;
                            incumbent_generation = replacement_generation;
                            incumbent = replacement;
                            continue 'serving;
                        }
                    }
                }
                outcome = join_reader(&replacement.task) => {
                    let terminal = replacement
                        .settle_joined(leg, &config, &live_legs, outcome)
                        .await?;
                    let finished = live_legs.finish(replacement_slot);
                    log_abnormal_leg_end(
                        symbol,
                        leg,
                        replacement_generation,
                        &finished,
                        terminal.outcome,
                    );
                    log_arbitration_counters(symbol, leg, &dedupe);
                    if finished.achieved_complete {
                        backoff = WS_REBUILD_INITIAL_BACKOFF;
                    }
                    let delay = tokio::time::sleep(backoff + rebuild_jitter());
                    tokio::pin!(delay);
                    tokio::select! {
                        biased;
                        outcome = join_reader(&incumbent.task) => {
                            let terminal = incumbent
                                .settle_joined(leg, &config, &live_legs, outcome)
                                .await?;
                            let finished = live_legs.finish(incumbent_slot);
                            log_abnormal_leg_end(
                                symbol,
                                leg,
                                incumbent_generation,
                                &finished,
                                terminal.outcome,
                            );
                            log_arbitration_counters(symbol, leg, &dedupe);
                            if finished.achieved_complete {
                                backoff = WS_REBUILD_INITIAL_BACKOFF;
                            }
                            tokio::time::sleep(backoff + rebuild_jitter()).await;
                            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
                            if *shutdown_rx.borrow() {
                                return Ok(());
                            }
                            incumbent_generation = next_ws_generation(&mut next_generation);
                            incumbent = start_generation(incumbent_slot, incumbent_generation);
                            continue 'serving;
                        }
                        _ = &mut delay => {
                            backoff = (backoff * 2).min(WS_REBUILD_MAX_BACKOFF);
                            if *shutdown_rx.borrow() {
                                let terminal = incumbent
                                    .stop_and_settle(leg, &config, &live_legs)
                                    .await?;
                                let finished = live_legs.finish(incumbent_slot);
                                log_abnormal_leg_end(
                                    symbol,
                                    leg,
                                    incumbent_generation,
                                    &finished,
                                    terminal.outcome,
                                );
                                return Ok(());
                            }
                            if !live_legs.claim_voluntary_replacement(
                                leg,
                                incumbent_slot,
                                incumbent_generation,
                                Instant::now(),
                            ) {
                                continue 'serving;
                            }
                            replacement_generation =
                                next_ws_generation(&mut next_generation);
                            replacement = start_generation(
                                replacement_slot,
                                replacement_generation,
                            );
                            continue 'replacement;
                        }
                    }
                }
                _ = &mut changed => {}
            }
        }
    }
}

fn log_abnormal_leg_end(
    symbol: &'static str,
    leg: usize,
    generation: u64,
    finished: &FinishedLeg,
    outcome: Result<()>,
) {
    match outcome {
        Ok(()) => tracing::warn!(
            target: "engine.polymarket_ws",
            symbol,
            leg,
            generation,
            connection_session_id = ?finished.connection_session_id,
            launch_to_end_ms = finished.launch_to_end_ms,
            connected_to_end_ms = ?finished.connected_to_end_ms,
            end_mono_ns = finished.end_mono_ns,
            clock_precision_ns = CLOCK_PRECISION_NS,
            right_censored = false,
            remaining_legs = finished.remaining,
            "bbwick_polymarket_ws_leg_ended"
        ),
        Err(err) => tracing::warn!(
            target: "engine.polymarket_ws",
            symbol,
            leg,
            generation,
            connection_session_id = ?finished.connection_session_id,
            launch_to_end_ms = finished.launch_to_end_ms,
            connected_to_end_ms = ?finished.connected_to_end_ms,
            end_mono_ns = finished.end_mono_ns,
            clock_precision_ns = CLOCK_PRECISION_NS,
            right_censored = false,
            remaining_legs = finished.remaining,
            error = %format!("{err:#}"),
            "bbwick_polymarket_ws_leg_failed"
        ),
    }
}

fn log_arbitration_counters(symbol: &'static str, leg: usize, dedupe: &EventDedupe) {
    let (accepted, duplicates) = dedupe.counters();
    tracing::debug!(
        target: "engine.polymarket_ws",
        symbol,
        leg,
        accepted,
        duplicates,
        "bbwick_polymarket_ws_arbitration_counters"
    );
}

/// Decorrelates rebuild storms across legs without pulling in a random dependency.
fn rebuild_jitter() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::from(since.subsec_nanos()))
        .unwrap_or(0);
    Duration::from_millis(nanos % WS_REBUILD_BACKOFF_JITTER_MS.max(1))
}

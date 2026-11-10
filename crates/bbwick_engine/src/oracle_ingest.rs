use std::sync::{Arc, Mutex};

use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::sync::{broadcast, mpsc, watch};

use crate::ids::{IdHeartbeatFrame, IdStepFrame, IdTickFrame, MarketDataKeyRegistry};
use crate::ingest_event::IngestEvent;
use crate::oracle::OracleTick;
use crate::oracle_history::OracleHistory;
use crate::state::{PolymarketOddsState, PolymarketStateStore};
use crate::wire::StepFrame;

const INGEST_BATCH_LIMIT: usize = 256;

#[derive(Default)]
struct CoalescedLiveBatch {
    ticks_by_symbol_id: Vec<Option<IdTickFrame>>,
    touched_ticks: Vec<usize>,
    steps_by_key: Vec<Option<IdStepFrame>>,
    touched_steps: Vec<usize>,
}

impl CoalescedLiveBatch {
    fn with_registry(key_registry: &MarketDataKeyRegistry) -> Self {
        Self {
            ticks_by_symbol_id: vec![None; key_registry.symbol_count()],
            touched_ticks: Vec::with_capacity(key_registry.symbol_count()),
            steps_by_key: vec![None; key_registry.step_slot_count()],
            touched_steps: Vec::with_capacity(key_registry.step_slot_count()),
        }
    }

    fn set_tick(&mut self, index: usize, frame: IdTickFrame) {
        if self.ticks_by_symbol_id[index].is_none() {
            self.touched_ticks.push(index);
        }
        self.ticks_by_symbol_id[index] = Some(frame);
    }

    fn set_step(&mut self, index: usize, frame: IdStepFrame) {
        if self.steps_by_key[index].is_none() {
            self.touched_steps.push(index);
        }
        self.steps_by_key[index] = Some(frame);
    }
}

pub(crate) struct OracleIngestRuntime {
    pub ingest_rx: mpsc::Receiver<IngestEvent>,
    pub tick_tx: broadcast::Sender<OracleTick>,
    pub step_tx: watch::Sender<StepFrame>,
    pub polymarket_state_tx: watch::Sender<PolymarketOddsState>,
    pub state_store: Arc<Mutex<PolymarketStateStore>>,
    pub key_registry: Arc<MarketDataKeyRegistry>,
    pub history: Arc<OracleHistory>,
}

pub(crate) fn spawn_ingest_runtime(runtime: OracleIngestRuntime) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move { runtime.run().await })
}

impl OracleIngestRuntime {
    async fn run(mut self) {
        let mut batch = Vec::with_capacity(INGEST_BATCH_LIMIT);
        let mut coalesced = CoalescedLiveBatch::with_registry(&self.key_registry);
        let history = Arc::clone(&self.history);
        let mut active = vec![false; self.key_registry.step_slot_count()];
        let mut pending = FuturesUnordered::new();
        let mut input_closed = false;
        loop {
            tokio::select! {
                count = self.ingest_rx.recv_many(&mut batch, INGEST_BATCH_LIMIT), if !input_closed => {
                    input_closed = count == 0;
                    for event in batch.drain(..) {
                        match event {
                            IngestEvent::TickById(frame) => {
                                coalesced.set_tick(frame.symbol_id.index(), frame);
                            }
                            IngestEvent::Tick(frame) => {
                                if let Some(symbol_id) = self.key_registry.symbol_id(frame.symbol.as_str())
                                {
                                    coalesced.set_tick(
                                        symbol_id.index(),
                                        IdTickFrame {
                                            symbol_id,
                                            ts_ms: frame.ts_ms,
                                            price: frame.price,
                                        },
                                    );
                                }
                            }
                            IngestEvent::HeartbeatById(IdHeartbeatFrame {
                                symbol_id,
                                ts_ms,
                                seq,
                            }) => {
                                tracing::debug!(
                                    symbol = self.key_registry.symbol(symbol_id),
                                    ts_ms = ts_ms,
                                    seq = seq,
                                    "oracle heartbeat"
                                );
                            }
                            IngestEvent::Heartbeat(frame) => {
                                tracing::debug!(
                                    symbol = frame.symbol,
                                    ts_ms = frame.ts_ms,
                                    seq = frame.seq,
                                    "oracle heartbeat"
                                );
                            }
                            IngestEvent::StepById(frame) => {
                                let slot = self
                                    .key_registry
                                    .step_slot(frame.symbol_id, frame.timeframe_id);
                                coalesced.set_step(slot, frame);
                            }
                            IngestEvent::Step(frame) => {
                                if let (Some(symbol_id), Some(timeframe_id)) = (
                                    self.key_registry.symbol_id(frame.symbol.as_str()),
                                    self.key_registry.timeframe_id(frame.timeframe.as_str()),
                                ) {
                                    if let Err(err) = history.admit_step(&frame, frame.received_mono_ns) {
                                        tracing::error!(
                                            target: "engine.oracle",
                                            error = %err,
                                            symbol = %frame.symbol,
                                            timeframe = %frame.timeframe,
                                            ts_ms = frame.bar.ts_ms,
                                            "bbwick_history_step_rejected"
                                        );
                                        continue;
                                    }
                                    let slot = self.key_registry.step_slot(symbol_id, timeframe_id);
                                    let StepFrame {
                                        received_mono_ns,
                                        bar,
                                        overlays,
                                        trades,
                                        seq,
                                        ledger_epoch,
                                        ..
                                    } = frame;
                                    coalesced.set_step(
                                        slot,
                                        IdStepFrame {
                                            symbol_id,
                                            timeframe_id,
                                            received_mono_ns,
                                            bar,
                                            overlays,
                                            trades,
                                            seq,
                                            ledger_epoch,
                                        },
                                    );
                                }
                            }
                            IngestEvent::PolymarketLastTrade(frame) => {
                                let _ = self.apply_polymarket_last_trade(&frame);
                            }
                        }
                    }
                    for index in coalesced.touched_ticks.drain(..) {
                        if let Some(tick) = coalesced.ticks_by_symbol_id[index].take() {
                            let _ = self.tick_tx.send(self.key_registry.materialize_tick(tick));
                        }
                    }
                }
                Some((index, result)) = pending.next(), if !pending.is_empty() => {
                    active[index] = false;
                    match result {
                        Ok((step, Ok(()))) => {
                            self.step_tx.send_replace(step);
                        }
                        Ok((step, Err(err))) => {
                            tracing::error!(
                                target: "engine.oracle",
                                error = %err,
                                symbol = %step.symbol,
                                timeframe = %step.timeframe,
                                ts_ms = step.bar.ts_ms,
                                "bbwick_history_step_rejected"
                            );
                        }
                        Err(err) => {
                            history.mark_persistence_failed();
                            tracing::error!(
                                target: "engine.oracle",
                                error = %err,
                                step_slot = index,
                                "bbwick_history_step_task_failed"
                            );
                        }
                    }
                }
            }
            let CoalescedLiveBatch {
                steps_by_key,
                touched_steps,
                ..
            } = &mut coalesced;
            touched_steps.retain(|&index| {
                if active[index] {
                    return true;
                }
                if let Some(frame) = steps_by_key[index].take() {
                    let step = self.key_registry.materialize_step(&frame);
                    active[index] = true;
                    let history = Arc::clone(&history);
                    pending.push(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            let result = history.persist(&step.symbol, &step.timeframe);
                            (step, result)
                        })
                        .await;
                        (index, result)
                    });
                }
                false
            });
            if input_closed && pending.is_empty() && touched_steps.is_empty() {
                break;
            }
        }
    }

    fn apply_polymarket_last_trade(
        &self,
        frame: &crate::wire::PolymarketLastTradeFrame,
    ) -> Option<bool> {
        let mut store = self.state_store.lock().ok()?;
        let mut changed = false;
        let now = crate::oracle_state_updates::now_ms();
        let _ = self.polymarket_state_tx.send_if_modified(|state| {
            changed |= store.sync_last_trade(state, frame, now);
            changed
        });
        drop(store);
        Some(changed)
    }
}

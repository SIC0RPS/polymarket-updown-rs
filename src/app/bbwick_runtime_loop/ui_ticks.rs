use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use polymarket_hft::config::{AppConfig, VALID_ASSETS};
use polymarket_hft::observability::action::ActionEvent;
use polymarket_hft::observability::Observability;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const PROVIDER: &str = "bbwick_engine";

struct UiTickSlot {
    version: AtomicU64,
    ts_ms: AtomicU64,
    bid_bits: AtomicU64,
    ask_bits: AtomicU64,
    bid_sz_bits: AtomicU64,
    ask_sz_bits: AtomicU64,
}

impl UiTickSlot {
    const fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            ts_ms: AtomicU64::new(0),
            bid_bits: AtomicU64::new(0),
            ask_bits: AtomicU64::new(0),
            bid_sz_bits: AtomicU64::new(0),
            ask_sz_bits: AtomicU64::new(0),
        }
    }

    fn store(&self, ts_ms: u64, bid: f64, ask: f64, bid_sz: f64, ask_sz: f64) {
        self.version.fetch_add(1, Ordering::SeqCst);
        self.ts_ms.store(ts_ms, Ordering::SeqCst);
        self.bid_bits.store(bid.to_bits(), Ordering::SeqCst);
        self.ask_bits.store(ask.to_bits(), Ordering::SeqCst);
        self.bid_sz_bits.store(bid_sz.to_bits(), Ordering::SeqCst);
        self.ask_sz_bits.store(ask_sz.to_bits(), Ordering::SeqCst);
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    fn load(&self) -> Option<(u64, UiTickSnapshot)> {
        for _ in 0..3 {
            let v1 = self.version.load(Ordering::SeqCst);
            if v1 == 0 || v1 % 2 != 0 {
                continue;
            }
            let snapshot = UiTickSnapshot {
                ts_ms: self.ts_ms.load(Ordering::SeqCst),
                bid: f64::from_bits(self.bid_bits.load(Ordering::SeqCst)),
                ask: f64::from_bits(self.ask_bits.load(Ordering::SeqCst)),
                bid_sz: f64::from_bits(self.bid_sz_bits.load(Ordering::SeqCst)),
                ask_sz: f64::from_bits(self.ask_sz_bits.load(Ordering::SeqCst)),
            };
            let v2 = self.version.load(Ordering::SeqCst);
            if v1 == v2 && v2 % 2 == 0 {
                return Some((v2, snapshot));
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
struct UiTickSnapshot {
    ts_ms: u64,
    bid: f64,
    ask: f64,
    bid_sz: f64,
    ask_sz: f64,
}

pub(super) struct UiTickProjection {
    slots: Arc<[UiTickSlot]>,
    source_symbols: Vec<String>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl UiTickProjection {
    pub(super) fn start(obs: Observability, cfg: &AppConfig) -> Self {
        let slots: Arc<[UiTickSlot]> = (0..VALID_ASSETS.len())
            .map(|_| UiTickSlot::new())
            .collect::<Vec<_>>()
            .into();
        let source_by_asset = cfg.bbwick_oracle.resolved_underlying_symbols();
        let source_symbols = VALID_ASSETS
            .iter()
            .map(|asset| source_by_asset[*asset].clone())
            .collect();
        let log_ticks = cfg.observability.logging.log_ticks;
        let task_slots = Arc::clone(&slots);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let period = Duration::from_secs(1);
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut emitted_versions = vec![0_u64; task_slots.len()];
            loop {
                tokio::select! {
                    _ = task_cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        for (index, (symbol, slot)) in VALID_ASSETS.iter().zip(task_slots.iter()).enumerate() {
                            let Some((version, tick)) = slot.load() else {
                                continue;
                            };
                            if emitted_versions[index] == version {
                                continue;
                            }
                            emitted_versions[index] = version;
                            obs.emit_ui(ActionEvent::CexQuote {
                                ts_ms: tick.ts_ms,
                                provider: PROVIDER.to_string(),
                                symbol: (*symbol).to_string(),
                                bid: tick.bid,
                                ask: tick.ask,
                                bid_sz: Some(tick.bid_sz),
                                ask_sz: Some(tick.ask_sz),
                            });
                            let event = ActionEvent::OracleTick {
                                ts_ms: tick.ts_ms,
                                px: (tick.bid + tick.ask) * 0.5,
                                vol: None,
                                symbol: (*symbol).to_string(),
                            };
                            if log_ticks {
                                obs.emit_action(event);
                            } else {
                                obs.emit_ui(event);
                            }
                        }
                    }
                }
            }
        });
        Self {
            slots,
            source_symbols,
            cancellation,
            task,
        }
    }

    pub(super) fn slot_index(&self, symbol: &str) -> Option<usize> {
        self.source_symbols
            .iter()
            .position(|configured| configured == symbol)
    }

    pub(super) fn observe(
        &self,
        index: usize,
        ts_ms: u64,
        bid: f64,
        ask: f64,
        bid_sz: f64,
        ask_sz: f64,
    ) {
        if let Some(slot) = self.slots.get(index) {
            slot.store(ts_ms, bid, ask, bid_sz, ask_sz);
        }
    }

    pub(super) async fn shutdown(self) {
        self.cancellation.cancel();
        if let Err(err) = self.task.await {
            tracing::warn!(
                target: "app.bbwick_runtime",
                error = %err,
                "bbwick_ui_tick_projection_join_failed"
            );
        }
    }
}

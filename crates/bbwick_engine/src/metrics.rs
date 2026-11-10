use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct MetricsState {
    seq_gap_count: AtomicU64,
    resync_count: AtomicU64,
    snapshot_fail_count: AtomicU64,
    pending_drop_count: AtomicU64,
    heartbeat_count: AtomicU64,
    tick_drop_count: AtomicU64,
    ws_lag_count: AtomicU64,
    sse_drop_count: AtomicU64,
    sse_lag_count: AtomicU64,
    stale_tick_count: AtomicU64,
    stale_overlay_count: AtomicU64,
    last_heartbeat_ts_ms: AtomicU64,
    last_sse_send_ts_ms: AtomicU64,
}

impl MetricsState {
    pub fn inc_seq_gap(&self) {
        self.seq_gap_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_resync(&self) {
        self.resync_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_snapshot_fail(&self) {
        self.snapshot_fail_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_pending_drop(&self, count: u64) {
        self.pending_drop_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_heartbeat_count(&self) {
        self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_tick_drop(&self) {
        self.tick_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ws_lag(&self, count: u64) {
        self.ws_lag_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_sse_drop(&self) {
        self.sse_drop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sse_lag(&self, count: u64) {
        self.sse_lag_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_stale_tick(&self) {
        self.stale_tick_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_stale_overlay(&self) {
        self.stale_overlay_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_last_sse_send_ts_ms(&self, value: u64) {
        self.last_sse_send_ts_ms.store(value, Ordering::Relaxed);
    }

    pub fn set_last_heartbeat_ts_ms(&self, value: u64) {
        self.last_heartbeat_ts_ms.store(value, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let sse_lag_count = self.sse_lag_count.load(Ordering::Relaxed);
        let sse_drop_count = self.sse_drop_count.load(Ordering::Relaxed);
        MetricsSnapshot {
            seq_gap_count: self.seq_gap_count.load(Ordering::Relaxed),
            resync_count: self.resync_count.load(Ordering::Relaxed),
            snapshot_fail_count: self.snapshot_fail_count.load(Ordering::Relaxed),
            pending_drop_count: self.pending_drop_count.load(Ordering::Relaxed),
            heartbeat_count: self.heartbeat_count.load(Ordering::Relaxed),
            tick_drop_count: self.tick_drop_count.load(Ordering::Relaxed),
            ws_lag_count: self.ws_lag_count.load(Ordering::Relaxed),
            sse_drop_count,
            sse_lag_count,
            lagged_count: sse_lag_count,
            drop_count: sse_drop_count,
            stale_tick_count: self.stale_tick_count.load(Ordering::Relaxed),
            stale_overlay_count: self.stale_overlay_count.load(Ordering::Relaxed),
            last_heartbeat_ts_ms: self.last_heartbeat_ts_ms.load(Ordering::Relaxed),
            last_sse_send_ts_ms: self.last_sse_send_ts_ms.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub seq_gap_count: u64,
    pub resync_count: u64,
    pub snapshot_fail_count: u64,
    pub pending_drop_count: u64,
    pub heartbeat_count: u64,
    pub tick_drop_count: u64,
    pub ws_lag_count: u64,
    pub sse_drop_count: u64,
    pub sse_lag_count: u64,
    pub lagged_count: u64,
    pub drop_count: u64,
    pub stale_tick_count: u64,
    pub stale_overlay_count: u64,
    pub last_heartbeat_ts_ms: u64,
    pub last_sse_send_ts_ms: u64,
}

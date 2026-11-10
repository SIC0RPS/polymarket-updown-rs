use bbwick_engine::OracleEngine;
use std::time::Duration;

use crate::app::bootstrap::now_ms;

pub(super) const BBWICK_STATUS_INTERVAL_MS: u64 = 250;

#[derive(Clone, Copy)]
pub(super) struct BranchTimestamps {
    pub(super) wall_ms: u64,
    pub(super) ingest_mono_ns: u64,
}

#[derive(Clone, Copy)]
pub(super) struct CachedWallClock {
    wall_ms: u64,
}

impl CachedWallClock {
    pub(super) fn new() -> Self {
        Self { wall_ms: now_ms() }
    }

    fn current_ms(&mut self) -> u64 {
        self.maybe_refresh(now_ms())
    }

    fn maybe_refresh(&mut self, observed_ms: u64) -> u64 {
        self.wall_ms = self.wall_ms.max(observed_ms);
        self.wall_ms
    }
}

pub(super) fn branch_timestamps(
    wall_clock: &mut CachedWallClock,
    oracle: &Option<OracleEngine>,
) -> BranchTimestamps {
    BranchTimestamps {
        wall_ms: wall_clock.current_ms(),
        ingest_mono_ns: oracle
            .as_ref()
            .map(|engine| engine.current_ingest_mono_ns())
            .unwrap_or(0),
    }
}

pub(super) async fn wait_for_strategy_deadline(deadline_ms: Option<u64>, current_wall_ms: u64) {
    let Some(deadline_ms) = deadline_ms else {
        std::future::pending::<()>().await;
        return;
    };
    tokio::time::sleep(Duration::from_millis(
        deadline_ms.saturating_sub(current_wall_ms),
    ))
    .await;
}

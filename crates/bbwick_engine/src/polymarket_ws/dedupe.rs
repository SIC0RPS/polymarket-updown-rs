//! First-dedupe-admission-wins arbitration across redundant market WebSocket legs.
//!
//! Every leg subscribes to the same assets and the venue fans the same events to all of
//! them, so an event delivered twice is the same event, not new information. The first copy
//! to insert its digest while holding the dedupe mutex is applied and later copies are
//! dropped. Recorded socket receipt timestamps are not compared by this arbitration.
//!
//! It elects no authority and scores no leg. Whichever leg first inserts a given event wins
//! that event, and nothing else is inferred from it.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Retention window for seen-event keys.
///
/// Must exceed the worst inter-leg arrival skew or a late duplicate is treated as new.
/// Measured skew against production is +/-2 ms, so this is roughly three orders of
/// magnitude of margin.
const DEDUPE_WINDOW: Duration = Duration::from_secs(10);

/// Entry count that makes a new admission eligible for periodic cleanup.
/// This is a cleanup threshold, not a size cap.
const DEDUPE_CLEANUP_THRESHOLD: usize = 262_144;

#[derive(Debug, Default)]
struct SeenEvents {
    entries: HashMap<u64, Instant>,
    last_cleanup: Option<Instant>,
}

#[derive(Debug, Default)]
pub(super) struct EventDedupe {
    seen: Mutex<SeenEvents>,
    accepted: AtomicU64,
    duplicates: AtomicU64,
}

#[derive(Clone, Copy)]
pub(super) struct DedupeTicket {
    pub(super) first: bool,
    pub(super) digest: u64,
    pub(super) first_seen: Instant,
}

impl DedupeTicket {
    pub(super) fn expires_at(self) -> Instant {
        self.first_seen + DEDUPE_WINDOW
    }
}

impl EventDedupe {
    /// The first mutex-protected insertion is applied; its ticket identifies later copies.
    pub(super) fn accept<K: Hash>(&self, key: &K) -> DedupeTicket {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let digest = hasher.finish();
        let now = Instant::now();
        let mut seen = match self.seen.lock() {
            Ok(seen) => seen,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(first_seen) = seen
            .entries
            .get(&digest)
            .copied()
            .filter(|first_seen| now.duration_since(*first_seen) < DEDUPE_WINDOW)
        {
            drop(seen);
            self.duplicates.fetch_add(1, Ordering::Relaxed);
            return DedupeTicket {
                first: false,
                digest,
                first_seen,
            };
        }
        let cleanup_due = seen.entries.len() >= DEDUPE_CLEANUP_THRESHOLD
            && seen
                .last_cleanup
                .is_none_or(|last_cleanup| now.duration_since(last_cleanup) >= DEDUPE_WINDOW);
        if cleanup_due {
            seen.entries
                .retain(|_, first_seen| now.duration_since(*first_seen) < DEDUPE_WINDOW);
            seen.last_cleanup = Some(now);
        }
        seen.entries.insert(digest, now);
        drop(seen);
        self.accepted.fetch_add(1, Ordering::Relaxed);
        DedupeTicket {
            first: true,
            digest,
            first_seen: now,
        }
    }

    pub(super) fn counters(&self) -> (u64, u64) {
        (
            self.accepted.load(Ordering::Relaxed),
            self.duplicates.load(Ordering::Relaxed),
        )
    }
}

//! Monotonic generation numbering for market WebSocket legs.

pub(super) fn next_ws_generation(next_generation: &mut u64) -> u64 {
    let generation = *next_generation;
    *next_generation = next_generation.saturating_add(1).max(1);
    generation
}

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use super::{PolymarketOddsState, PolymarketStateStore};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Fingerprint {
    pub count: u64,
    pub xor: u64,
    pub sum: u64,
}

impl Fingerprint {
    pub fn insert_hash(&mut self, hash: u64) {
        self.count = self.count.saturating_add(1);
        self.xor ^= hash;
        self.sum = self.sum.wrapping_add(hash);
    }
}

pub fn fingerprint_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

impl PolymarketStateStore {
    fn compute_trigger_fingerprint(&self) -> Fingerprint {
        let mut fingerprint = Fingerprint::default();
        fingerprint.insert_hash(fingerprint_hash(
            &self
                .state
                .selection
                .as_ref()
                .map(|selection| selection.market_id.as_str()),
        ));
        fingerprint.insert_hash(fingerprint_hash(
            &self
                .state
                .next_selection
                .as_ref()
                .map(|selection| selection.market_id.as_str()),
        ));
        for market in &self.state.markets {
            fingerprint.insert_hash(fingerprint_hash(&(
                (
                    market.market_id.as_str(),
                    market.condition_id.as_str(),
                    market.underlying.as_str(),
                    market.up_token,
                    market.down_token,
                    market.window_start_ms,
                    market.window_end_ms,
                ),
                (
                    market.minimum_tick_size.map(f64::to_bits),
                    market.minimum_order_size.map(f64::to_bits),
                    market.rewards_min_size.map(f64::to_bits),
                    market.rewards_max_spread.map(f64::to_bits),
                    market.itode,
                    market.up_lifecycle.as_str(),
                    market.down_lifecycle.as_str(),
                ),
            )));
        }
        for (token_id, book) in &self.state.books {
            fingerprint.insert_hash(fingerprint_hash(&(
                *token_id,
                book.bid.map(f64::to_bits),
                book.ask.map(f64::to_bits),
                book.bid_sz.map(f64::to_bits),
                book.ask_sz.map(f64::to_bits),
                book.tick_size.to_bits(),
                book.min_order_size.to_bits(),
                book.ts_ms,
            )));
        }
        for (token_id, depth) in &self.state.depths {
            fingerprint.insert_hash(fingerprint_hash(&(*token_id, depth.revision)));
        }
        fingerprint
    }

    pub(super) fn refresh_trigger_metadata(&mut self, projected: &mut PolymarketOddsState) {
        self.state.revision = self.state.revision.saturating_add(1);
        self.state.trigger_fingerprint = self.compute_trigger_fingerprint();
        projected.revision = self.state.revision;
        projected.trigger_fingerprint = self.state.trigger_fingerprint;
    }
}

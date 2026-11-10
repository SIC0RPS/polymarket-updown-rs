use super::OpenOrderState;

impl OpenOrderState {
    pub fn remaining_size(&self) -> f64 {
        (self.original_size - self.matched_size).max(0.0)
    }

    pub fn timeout_deadline_ms(&self, default_timeout_ms: u64) -> u64 {
        if self.post_only || self.side == crate::types::Side::Buy {
            if let Some(expires_at_ms) = self.time_in_force.expiration_ms() {
                return expires_at_ms.max(self.placed_at_ms);
            }
        }
        self.placed_at_ms.saturating_add(default_timeout_ms.max(1))
    }
}

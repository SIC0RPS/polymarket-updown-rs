// Strategy support required for behavior: shared quote validation.
use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::support::shared::common::is_valid_tick_size;

impl MakerRebateMmStrategy {
    pub(crate) fn validate_tick_size(
        &mut self,
        _token: &str,
        tick_size: f64,
        _now_ms: u64,
    ) -> Option<f64> {
        if tick_size.is_finite() && tick_size > 0.0 && is_valid_tick_size(tick_size) {
            return Some(tick_size);
        }

        None
    }
}

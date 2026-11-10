pub const FIXED_6DP_SCALE: u128 = 1_000_000;
pub const FIXED_6DP_QUANTUM: f64 = 1.0 / FIXED_6DP_SCALE as f64;
pub const FIXED_8DP_SCALE: u64 = 100_000_000;

pub fn has_fixed_6dp_units(value: f64) -> bool {
    value.is_finite() && value >= FIXED_6DP_QUANTUM
}

pub fn below_one_fixed_6dp_unit(value: f64) -> bool {
    !value.is_finite() || value < FIXED_6DP_QUANTUM
}

pub fn price_to_units_8dp(value: f64) -> Option<u64> {
    if !(value.is_finite() && value > 0.0 && value <= 1.0) {
        return None;
    }
    let scaled = value * FIXED_8DP_SCALE as f64;
    let rounded = scaled.round();
    ((scaled - rounded).abs() <= 0.000_001).then_some(rounded as u64)
}

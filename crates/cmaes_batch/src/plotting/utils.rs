//! Utilities for plotting

/// Applies a small offset to the value to prevent taking the log of zero
pub fn apply_offset(x: f64) -> f64 {
    let offset = 1e-20;
    if x >= 0.0 {
        x + offset
    } else {
        x - offset
    }
}

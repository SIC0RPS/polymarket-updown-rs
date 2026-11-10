pub const VENDOR_LOT_SIZE_DECIMALS: u32 = 2;

pub fn truncate_to_decimals(value: f64, decimals: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let scale = 10f64.powi(decimals as i32);
    // Guard boundary cases like 14.289999999999997 that arise from binary float
    // representation of decimal values; without this, floor-at-decimals can underflow
    // by one step even when the mathematical value is exact at that precision.
    let scaled = value * scale;
    (scaled + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM).floor() / scale
}

pub fn ceil_to_decimals(value: f64, decimals: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let scale = 10f64.powi(decimals as i32);
    // Symmetric boundary guard for values represented just above the decimal step.
    let scaled = value * scale;
    (scaled - crate::polymarket::fixed_math::FIXED_6DP_QUANTUM).ceil() / scale
}

pub fn truncate_to_vendor_qty(qty: f64) -> f64 {
    truncate_to_decimals(qty, VENDOR_LOT_SIZE_DECIMALS)
}

pub fn vendor_lots_to_qty(lots: u64) -> f64 {
    lots as f64 / 10u64.pow(VENDOR_LOT_SIZE_DECIMALS) as f64
}

pub fn normalized_min_order_size(min_order_size: f64) -> f64 {
    if min_order_size.is_finite() && min_order_size > 0.0 {
        ceil_to_decimals(min_order_size, VENDOR_LOT_SIZE_DECIMALS)
    } else {
        0.0
    }
}

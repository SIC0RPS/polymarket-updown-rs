pub fn cap_order_qty(qty: f64, max_order_qty: f64) -> f64 {
    if !(qty.is_finite() && qty > 0.0) {
        return qty;
    }
    if max_order_qty.is_finite() && max_order_qty > 0.0 {
        qty.min(max_order_qty)
    } else {
        qty
    }
}

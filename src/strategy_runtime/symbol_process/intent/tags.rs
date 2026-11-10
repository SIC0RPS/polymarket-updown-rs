use super::*;

pub(super) fn side_tag(side: Option<Side>) -> u8 {
    match side {
        None => SIDE_NONE,
        Some(Side::Buy) => SIDE_BUY,
        Some(Side::Sell) => SIDE_SELL,
    }
}

pub(super) fn close_reason_tag(reason: OrderCloseReason) -> Result<u8> {
    match reason {
        OrderCloseReason::PairedFirstManagedSell => Ok(CLOSE_PAIRED_FIRST_MANAGED_SELL),
        OrderCloseReason::PairedPostProceedsSell => Ok(CLOSE_PAIRED_POST_PROCEEDS_SELL),
        OrderCloseReason::PredictivePostProceedsSell => {
            anyhow::bail!("predictive post-proceeds sell authority is forbidden")
        }
    }
}

pub(super) fn close_reason_from_tag(tag: u8) -> Result<OrderCloseReason> {
    match tag {
        CLOSE_PAIRED_FIRST_MANAGED_SELL => Ok(OrderCloseReason::PairedFirstManagedSell),
        CLOSE_PAIRED_POST_PROCEEDS_SELL => Ok(OrderCloseReason::PairedPostProceedsSell),
        other => anyhow::bail!("invalid managed-sell close reason={other}"),
    }
}

pub(super) fn inventory_operation_from_tag(tag: u8) -> Result<InventoryOperation> {
    match tag {
        INVENTORY_SPLIT => Ok(InventoryOperation::SplitCompleteSet),
        INVENTORY_MERGE => Ok(InventoryOperation::MergeCompleteSet),
        other => anyhow::bail!("invalid inventory operation={other}"),
    }
}

pub(super) fn side_from_tag(tag: u8) -> Result<Option<Side>> {
    match tag {
        SIDE_NONE => Ok(None),
        SIDE_BUY => Ok(Some(Side::Buy)),
        SIDE_SELL => Ok(Some(Side::Sell)),
        other => anyhow::bail!("invalid side={other}"),
    }
}

pub(super) fn time_in_force_tag(time_in_force: &TimeInForce) -> Result<(u8, u64)> {
    match time_in_force {
        TimeInForce::Gtc => Ok((TIF_GTC, 0)),
        TimeInForce::Gtd { expiration_ms } => {
            anyhow::ensure!(*expiration_ms > 0, "GTD expiration is zero");
            Ok((TIF_GTD, *expiration_ms))
        }
        TimeInForce::Fok => Ok((TIF_FOK, 0)),
        TimeInForce::Fak => Ok((TIF_FAK, 0)),
    }
}

pub(super) fn time_in_force_from_tag(tag: u8, expiration_ms: u64) -> Result<TimeInForce> {
    match tag {
        TIF_GTC => {
            anyhow::ensure!(expiration_ms == 0, "GTC carries expiration");
            Ok(TimeInForce::Gtc)
        }
        TIF_GTD => {
            anyhow::ensure!(expiration_ms > 0, "GTD expiration is zero");
            Ok(TimeInForce::Gtd { expiration_ms })
        }
        TIF_FOK => {
            anyhow::ensure!(expiration_ms == 0, "FOK carries expiration");
            Ok(TimeInForce::Fok)
        }
        TIF_FAK => {
            anyhow::ensure!(expiration_ms == 0, "FAK carries expiration");
            Ok(TimeInForce::Fak)
        }
        other => anyhow::bail!("invalid time in force={other}"),
    }
}

pub(super) fn qty_from_units_6dp(units: u128) -> Result<f64> {
    anyhow::ensure!(
        units <= MAX_EXACT_F64_INTEGER,
        "order quantity exceeds exact f64 integer transport"
    );
    let qty = units as f64 / 1_000_000.0;
    anyhow::ensure!(
        qty.is_finite()
            && crate::ctf_accounting::qty_to_units_6dp(qty)
                .is_ok_and(|roundtrip| roundtrip == units),
        "order quantity cannot cross the domain f64 boundary losslessly"
    );
    Ok(qty)
}

pub(super) fn price_from_units_8dp(units: u64) -> Result<f64> {
    let price = units as f64 / PRICE_SCALE_8DP;
    anyhow::ensure!(
        price_to_units_8dp(price)? == units,
        "order price cannot cross the domain f64 boundary losslessly"
    );
    Ok(price)
}

pub(super) fn price_to_units_8dp(value: f64) -> Result<u64> {
    anyhow::ensure!(
        value.is_finite() && value > 0.0 && value <= 1.0,
        "order price is nonfinite or outside probability range"
    );
    let scaled = value * PRICE_SCALE_8DP;
    let rounded = scaled.round();
    anyhow::ensure!(
        rounded <= u64::MAX as f64 && (scaled - rounded).abs() <= FIXED_PRECISION_TOLERANCE,
        "order price exceeds fixed-8dp precision or range"
    );
    Ok(rounded as u64)
}

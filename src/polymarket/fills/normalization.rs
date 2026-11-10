use super::*;

pub fn ts_to_ms(ts: i64) -> u64 {
    if ts <= 0 {
        return now_ms();
    }
    let u = ts as u64;
    if u < 10_000_000_000 {
        u.saturating_mul(1000)
    } else {
        u
    }
}

pub(super) fn normalized_maker_order(
    owner_matches: bool,
    order_id: String,
    token: String,
    side: Option<FillSide>,
    matched_amount: Decimal,
    price: Decimal,
) -> Result<NormalizedMakerOrder> {
    let _ = decimal_to_f64(matched_amount)?;
    let _ = decimal_to_f64(price)?;
    Ok(NormalizedMakerOrder {
        owner_matches,
        order_id,
        token,
        side,
        matched_amount,
        price,
    })
}

pub(super) fn normalized_trader_role(role: Option<&TraderSide>) -> Result<NormalizedTraderRole> {
    match role {
        Some(TraderSide::Maker) => Ok(NormalizedTraderRole::Maker),
        Some(TraderSide::Taker) => Ok(NormalizedTraderRole::Taker),
        Some(TraderSide::Unknown(value)) => {
            anyhow::bail!("unsupported Polymarket trader_side `{value}`")
        }
        Some(_) => anyhow::bail!("unsupported Polymarket trader_side variant"),
        None => anyhow::bail!("confirmed Polymarket trade is missing trader_side"),
    }
}

pub(super) fn normalized_trade_to_fills(trade: NormalizedTrade) -> Result<Vec<FillEvent>> {
    let owned_maker_orders = trade
        .maker_orders
        .iter()
        .filter(|maker| maker.owner_matches)
        .collect::<Vec<_>>();
    if trade.role == NormalizedTraderRole::Maker {
        anyhow::ensure!(
            !owned_maker_orders.is_empty(),
            "confirmed maker trade has no owned maker_orders entry trade_id={}",
            trade.trade_id
        );
        let mut fills = Vec::with_capacity(owned_maker_orders.len());
        for maker in owned_maker_orders {
            let side = maker.side.with_context(|| {
                format!(
                    "maker fill side context unavailable order_id={} token={}",
                    maker.order_id, maker.token
                )
            })?;
            let price = decimal_to_f64(maker.price)?;
            let matched_amount = decimal_to_f64(maker.matched_amount)?;
            anyhow::ensure!(
                valid_vendor_fill_economics(price, matched_amount),
                "invalid confirmed maker fill economics order_id={}",
                maker.order_id
            );
            let exact_qty = decimal_to_units_6dp(maker.matched_amount, "maker matched amount")?;
            anyhow::ensure!(exact_qty > 0, "confirmed maker fill quantity is zero");
            fills.push(fill_from_parts(FillParts {
                ts_ms: trade.ts_ms,
                order_id: maker.order_id.clone(),
                trade_id: Some(trade.trade_id.clone()),
                transaction_hash: trade.transaction_hash.clone(),
                token: maker.token.clone(),
                side,
                px: price,
                size: matched_amount,
                fee_market_state: trade.fee_market_state,
                charges_taker_fee: false,
                receipt_qty_units_6dp: Some(exact_qty),
                receipt_fee_units_6dp: (side == FillSide::Sell).then_some(0),
            }));
        }
        return Ok(fills);
    }

    anyhow::ensure!(
        owned_maker_orders.is_empty(),
        "confirmed taker trade also contains an owned maker order trade_id={}",
        trade.trade_id
    );
    if !trade.owner_matches || !valid_vendor_fill_economics(trade.taker_px, trade.taker_size) {
        return Ok(Vec::new());
    }

    let Some(order_id) = trade
        .taker_order_id
        .as_deref()
        .map(str::trim)
        .filter(|order_id| !order_id.is_empty())
    else {
        tracing::warn!(
            target: "polymarket.fills",
            trade_id = %trade.trade_id,
            "taker_fill_missing_vendor_order_id"
        );
        return Ok(Vec::new());
    };

    Ok(vec![fill_from_parts(FillParts {
        ts_ms: trade.ts_ms,
        order_id: order_id.to_string(),
        trade_id: Some(trade.trade_id),
        transaction_hash: trade.transaction_hash,
        token: trade.token,
        side: trade.taker_side,
        px: trade.taker_px,
        size: trade.taker_size,
        fee_market_state: trade.fee_market_state,
        charges_taker_fee: true,
        receipt_qty_units_6dp: Some(trade.taker_size_units_6dp),
        receipt_fee_units_6dp: None,
    })])
}

fn valid_vendor_fill_economics(px: f64, size: f64) -> bool {
    px.is_finite() && (0.0..=1.0).contains(&px) && size.is_finite() && size > 0.0
}

struct FillParts {
    ts_ms: u64,
    order_id: String,
    trade_id: Option<String>,
    transaction_hash: Option<String>,
    token: String,
    side: FillSide,
    px: f64,
    size: f64,
    fee_market_state: FeeMarketState,
    charges_taker_fee: bool,
    receipt_qty_units_6dp: Option<u128>,
    receipt_fee_units_6dp: Option<u128>,
}

fn fill_from_parts(parts: FillParts) -> FillEvent {
    let fee = fill_fee_usdc(
        parts.px,
        parts.size,
        parts.fee_market_state,
        parts.charges_taker_fee,
    );
    FillEvent {
        ts_ms: parts.ts_ms,
        order_id: parts.order_id,
        trade_id: parts.trade_id,
        transaction_hash: parts.transaction_hash,
        token: parts.token,
        market_id: None,
        side: parts.side,
        quote_group: None,
        lifecycle_cohort: None,
        execution_intent_id: None,
        direct_buy_authorization: None,
        trade_observation: None,
        receipt_identity: None,
        receipt_qty_units_6dp: parts.receipt_qty_units_6dp,
        receipt_gross_proceeds_units_6dp: None,
        receipt_principal_units_6dp: None,
        receipt_approved_direct_cost_units_6dp: None,
        receipt_collateral_refund_units_6dp: None,
        receipt_collateral_cash_delta_units_6dp: None,
        receipt_wallet: None,
        receipt_fee_units_6dp: parts.receipt_fee_units_6dp,
        receipt_confirmed_at_ms: None,
        submitted_order_size: None,
        px: parts.px,
        size: parts.size,
        fee,
        at_top_of_book: None,
        queue_ahead_at_fill: None,
        time_to_fill_ms: None,
    }
}

fn fill_fee_usdc(
    px: f64,
    size: f64,
    fee_market_state: FeeMarketState,
    charges_taker_fee: bool,
) -> Option<f64> {
    if !charges_taker_fee {
        return Some(0.0);
    }
    match fee_market_state {
        FeeMarketState::Enabled { taker_base_fee_bps } => {
            documented_fee_usdc_for_taker_base_fee_bps(px, size, taker_base_fee_bps)
        }
        FeeMarketState::Disabled => Some(0.0),
        FeeMarketState::Unavailable => None,
    }
}

pub(crate) fn direct_buy_cash_requirement_units_6dp(
    qty_units_6dp: u128,
    limit_px_units_8dp: u64,
    taker_base_fee_bps: f64,
) -> Result<u128> {
    anyhow::ensure!(
        limit_px_units_8dp > 0 && limit_px_units_8dp <= 100_000_000,
        "BUY cash requirement price is invalid"
    );
    let rate_micro_bps = direct_buy_fee_rate_micro_bps(taker_base_fee_bps)?;
    let principal = qty_units_6dp
        .checked_mul(u128::from(limit_px_units_8dp))
        .context("BUY principal reservation overflow")?
        .div_ceil(100_000_000);
    // Rounded charged fees are at most twice raw fees; p * (1 - p) <= 1/4.
    let fee_bound = qty_units_6dp
        .checked_mul(rate_micro_bps)
        .context("BUY fee reservation overflow")?
        .div_ceil(20_000_000_000);
    principal
        .checked_add(fee_bound)
        .context("BUY cash reservation overflow")
}

pub(crate) fn direct_buy_affordable_qty_units_6dp(
    cash_units_6dp: u128,
    limit_px_units_8dp: u64,
    taker_base_fee_bps: f64,
) -> Result<u128> {
    let lot_units_6dp = crate::polymarket::fixed_math::FIXED_6DP_SCALE
        / 10_u128.pow(crate::polymarket::order_rules::VENDOR_LOT_SIZE_DECIMALS);
    let price_quantum_units_8dp =
        u128::from(crate::polymarket::fixed_math::FIXED_8DP_SCALE) / lot_units_6dp;
    anyhow::ensure!(
        limit_px_units_8dp > 0
            && limit_px_units_8dp <= crate::polymarket::fixed_math::FIXED_8DP_SCALE
            && u128::from(limit_px_units_8dp) % price_quantum_units_8dp == 0,
        "BUY cash sizing price is outside the native lattice"
    );
    let rate_micro_bps = direct_buy_fee_rate_micro_bps(taker_base_fee_bps)?;
    // Native lots and prices make principal an integer micro-dollar amount.
    // The forward reservation is therefore ceil(lots * (200 * price + rate)
    // / (20_000_000_000 / lot_units_6dp)), including its rounded fee bound.
    let cash_per_lot_numerator = u128::from(limit_px_units_8dp)
        .checked_mul(200)
        .and_then(|principal| principal.checked_add(rate_micro_bps))
        .context("BUY cash sizing denominator overflow")?;
    let lots = cash_units_6dp
        .checked_mul(20_000_000_000 / lot_units_6dp)
        .context("BUY cash sizing numerator overflow")?
        / cash_per_lot_numerator;
    lots.checked_mul(lot_units_6dp)
        .context("BUY cash sizing quantity overflow")
}

fn direct_buy_fee_rate_micro_bps(taker_base_fee_bps: f64) -> Result<u128> {
    anyhow::ensure!(
        taker_base_fee_bps.is_finite()
            && (0.0..=10_000.0).contains(&taker_base_fee_bps)
            && !taker_base_fee_bps.is_sign_negative(),
        "BUY cash requirement fee rate is invalid"
    );
    let bits = taker_base_fee_bps.to_bits();
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (mantissa, exponent) = if exponent_bits == 0 {
        (u128::from(fraction), 1 - 1023 - 52)
    } else {
        (
            u128::from((1_u64 << 52) | fraction),
            exponent_bits - 1023 - 52,
        )
    };
    let scaled_rate = mantissa
        .checked_mul(1_000_000)
        .context("BUY fee-rate scaling overflow")?;
    let shift = exponent.unsigned_abs();
    Ok(if shift >= u128::BITS {
        u128::from(scaled_rate > 0)
    } else {
        scaled_rate.div_ceil(1_u128 << shift)
    })
}

pub(crate) fn documented_fee_usdc_for_taker_base_fee_bps(
    px: f64,
    size: f64,
    taker_base_fee_bps: f64,
) -> Option<f64> {
    if !(taker_base_fee_bps.is_finite() && taker_base_fee_bps >= 0.0) {
        return None;
    }
    let rate = taker_base_fee_bps / 10_000.0;
    if !rate.is_finite() {
        return None;
    }
    documented_fee_usdc_at_rate(px, size, rate)
}

fn documented_fee_usdc_at_rate(px: f64, size: f64, rate: f64) -> Option<f64> {
    if !(px.is_finite() && (0.0..=1.0).contains(&px)) {
        return None;
    }
    if !(size.is_finite() && size > 0.0) {
        return None;
    }
    if !(rate.is_finite() && rate >= 0.0) {
        return None;
    }

    let raw_fee = size * rate * px * (1.0 - px);
    let rounded_fee = round_fee_usdc(raw_fee);
    Some(if rounded_fee >= MIN_NON_ZERO_FEE_USDC {
        rounded_fee
    } else {
        0.0
    })
}

pub(super) fn fee_market_state(value: Option<Decimal>) -> FeeMarketState {
    let Some(raw) = value.and_then(|raw| decimal_to_f64(raw).ok()) else {
        return FeeMarketState::Unavailable;
    };
    if !raw.is_finite() || raw < 0.0 {
        FeeMarketState::Unavailable
    } else if raw > 0.0 {
        FeeMarketState::Enabled {
            taker_base_fee_bps: raw,
        }
    } else {
        FeeMarketState::Disabled
    }
}

fn round_fee_usdc(raw_fee: f64) -> f64 {
    let raw_fee = raw_fee.max(0.0);
    if raw_fee <= 0.0 {
        return 0.0;
    }
    ((raw_fee * FEE_PRECISION_SCALE).round() / FEE_PRECISION_SCALE).max(0.0)
}

pub(super) fn fill_side_from_polymarket_side(side: PolymarketSide) -> Option<FillSide> {
    match side {
        PolymarketSide::Buy => Some(FillSide::Buy),
        PolymarketSide::Sell => Some(FillSide::Sell),
        _ => None,
    }
}

pub(super) fn ws_trade_status(status: &TradeMessageStatus) -> Result<CtfTradeStatus> {
    match status {
        TradeMessageStatus::Matched => Ok(CtfTradeStatus::Matched),
        TradeMessageStatus::Mined => Ok(CtfTradeStatus::Mined),
        TradeMessageStatus::Confirmed => Ok(CtfTradeStatus::Confirmed),
        TradeMessageStatus::Retrying => Ok(CtfTradeStatus::Retrying),
        TradeMessageStatus::Failed => Ok(CtfTradeStatus::Failed),
        TradeMessageStatus::Unknown(value) => {
            anyhow::bail!("unsupported CLOB trade status `{value}`")
        }
        _ => anyhow::bail!("unsupported CLOB trade status variant"),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

use super::*;

impl TradeObservationBase {
    fn for_owned_order(
        &self,
        owned_order_id: String,
        asset_id: String,
        side: FillSide,
        owned_price_units_6dp: u128,
        owned_size_units_6dp: u128,
        owned_gross_proceeds_units_6dp: Option<u128>,
        owned_fee_units_6dp: Option<u128>,
    ) -> UnboundCtfTradeObservation {
        UnboundCtfTradeObservation {
            trade_id: self.trade_id.clone(),
            market_id: self.market_id.clone(),
            trade_asset_id: self.trade_asset_id.clone(),
            asset_id,
            owned_order_id,
            side,
            taker_order_id: self.taker_order_id.clone(),
            maker_order_ids: self.maker_order_ids.clone(),
            status: self.status,
            price_units_6dp: self.price_units_6dp,
            size_units_6dp: self.size_units_6dp,
            matched_at_ms: self.matched_at_ms,
            observed_at_ms: self.observed_at_ms,
            owned_price_units_6dp,
            owned_size_units_6dp,
            owned_gross_proceeds_units_6dp,
            owned_fee_units_6dp,
        }
    }
}

fn execution_component_economics(
    price: Decimal,
    size: Decimal,
    fee_market_state: FeeMarketState,
    charges_taker_fee: bool,
) -> Result<(u128, u128, Option<u128>)> {
    anyhow::ensure!(
        price >= Decimal::ZERO,
        "execution price is negative: {price}"
    );
    let qty_units = decimal_to_units_6dp(size, "execution quantity")?;
    let mut gross = price
        .checked_mul(size)
        .context("execution gross proceeds overflow")?;
    gross.rescale(6);
    let gross_units = decimal_to_units_6dp(gross, "execution gross proceeds")?;
    let fee = if !charges_taker_fee {
        Some(0.0)
    } else {
        match fee_market_state {
            FeeMarketState::Enabled { taker_base_fee_bps } => {
                documented_fee_usdc_for_taker_base_fee_bps(
                    decimal_to_f64(price)?,
                    decimal_to_f64(size)?,
                    taker_base_fee_bps,
                )
            }
            FeeMarketState::Disabled => Some(0.0),
            FeeMarketState::Unavailable => None,
        }
    };
    let fee_units = fee
        .map(|fee| {
            let scaled = fee * crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64;
            let rounded = scaled.round();
            anyhow::ensure!(
                scaled.is_finite() && rounded >= 0.0 && rounded <= u128::MAX as f64,
                "execution fee estimate is invalid"
            );
            Ok(rounded as u128)
        })
        .transpose()?;
    Ok((qty_units, gross_units, fee_units))
}

fn taker_execution_economics<'a>(
    trade_asset_id: &str,
    expected_qty_units_6dp: u128,
    components: impl Iterator<Item = (&'a str, Decimal, Decimal)>,
    fee_market_state: FeeMarketState,
) -> Option<(u128, Option<u128>)> {
    let mut qty_units = 0_u128;
    let mut gross_units = 0_u128;
    let mut fee_units = Some(0_u128);
    let mut count = 0_usize;
    for (asset_id, price, size) in components {
        if asset_id != trade_asset_id {
            return None;
        }
        let (component_qty, component_gross, component_fee) =
            execution_component_economics(price, size, fee_market_state, true).ok()?;
        qty_units = qty_units.checked_add(component_qty)?;
        gross_units = gross_units.checked_add(component_gross)?;
        fee_units = fee_units
            .zip(component_fee)
            .and_then(|(total, component)| total.checked_add(component));
        count = count.saturating_add(1);
    }
    (count > 0 && qty_units == expected_qty_units_6dp).then_some((gross_units, fee_units))
}

impl UnboundCtfTradeObservation {
    pub(crate) const fn status(&self) -> CtfTradeStatus {
        self.status
    }

    pub(crate) fn order_id(&self) -> &str {
        &self.owned_order_id
    }

    pub(crate) fn trade_id(&self) -> &str {
        &self.trade_id
    }

    pub(crate) fn market_id(&self) -> &MarketId {
        &self.market_id
    }

    pub(crate) fn asset_id(&self) -> &str {
        &self.asset_id
    }

    pub(crate) const fn side(&self) -> FillSide {
        self.side
    }

    pub(crate) const fn owned_price_units_6dp(&self) -> u128 {
        self.owned_price_units_6dp
    }

    pub(crate) const fn owned_size_units_6dp(&self) -> u128 {
        self.owned_size_units_6dp
    }

    pub(crate) const fn owned_gross_proceeds_units_6dp(&self) -> Option<u128> {
        self.owned_gross_proceeds_units_6dp
    }

    pub(crate) const fn owned_fee_units_6dp(&self) -> Option<u128> {
        self.owned_fee_units_6dp
    }

    pub(crate) fn matches_recorded_fact(&self, fact: &ObservedCtfTradeFact) -> bool {
        fact.trade_id == self.trade_id
            && fact.market_id == self.market_id
            && fact.trade_asset_id == self.trade_asset_id
            && fact.asset_id == self.asset_id
            && fact.order_id == self.owned_order_id
            && fact.taker_order_id == self.taker_order_id
            && fact.maker_order_ids == self.maker_order_ids
            && fact.status == self.status
            && fact.price_units_6dp == self.price_units_6dp
            && fact.size_units_6dp == self.size_units_6dp
            && fact.owned_price_units_6dp == self.owned_price_units_6dp
            && fact.owned_size_units_6dp == self.owned_size_units_6dp
            && fact.matched_at_ms == self.matched_at_ms
            && fact.observed_at_ms == self.observed_at_ms
    }

    pub(crate) fn bind(
        &self,
        market_id: &MarketId,
        cohort_id: &str,
        token: &str,
    ) -> Result<ObservedCtfTradeFact> {
        anyhow::ensure!(
            market_id == &self.market_id && token == self.asset_id,
            "trade observation differs from canonical order market or asset identity"
        );
        ObservedCtfTradeFact::from_vendor(
            self.trade_id.clone(),
            self.market_id.clone(),
            cohort_id.to_string(),
            self.trade_asset_id.clone(),
            self.asset_id.clone(),
            self.owned_order_id.clone(),
            self.taker_order_id.clone(),
            self.maker_order_ids.clone(),
            self.status,
            self.price_units_6dp,
            self.size_units_6dp,
            self.owned_price_units_6dp,
            self.owned_size_units_6dp,
            self.matched_at_ms,
            self.observed_at_ms,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WsTradeUpdate {
    pub(crate) fills: Vec<FillEvent>,
}

pub(crate) fn ws_trade_observations(
    trade: &TradeMessage,
    own_api_key: ApiKey,
) -> Result<Vec<UnboundCtfTradeObservation>> {
    let status = ws_trade_status(&trade.status)?;
    let matched_at = trade
        .matchtime
        .filter(|value| *value > 0)
        .context("CLOB trade update is missing match_time")?;
    let matched_at_ms = ts_to_ms(matched_at);
    let observed_at = trade
        .last_update
        .filter(|value| *value > 0)
        .context("CLOB trade update is missing last_update")?;
    let observed_at_ms = ts_to_ms(observed_at);
    anyhow::ensure!(
        observed_at_ms >= matched_at_ms,
        "CLOB last_update precedes match_time"
    );
    let role = normalized_trader_role(trade.trader_side.as_ref())?;
    let owner_matches = trade.owner == Some(own_api_key) || trade.trade_owner == Some(own_api_key);
    anyhow::ensure!(
        role == NormalizedTraderRole::Maker || owner_matches,
        "taker trade update does not belong to authenticated owner"
    );
    let top_price_units_6dp = decimal_price_to_units_6dp(trade.price, "trade price")?;
    let top_size_units_6dp = decimal_to_units_6dp(trade.size, "trade size")?;
    let execution_fee_market_state = fee_market_state(trade.fee_rate_bps);
    let mut maker_order_ids = trade
        .maker_orders
        .iter()
        .map(|maker| maker.order_id.clone())
        .collect::<Vec<_>>();
    maker_order_ids.sort();
    let observation_base = TradeObservationBase {
        trade_id: trade.id.clone(),
        market_id: MarketId::new(trade.market.to_string()),
        trade_asset_id: trade.asset_id.to_string(),
        taker_order_id: trade.taker_order_id.clone(),
        maker_order_ids,
        status,
        price_units_6dp: top_price_units_6dp,
        size_units_6dp: top_size_units_6dp,
        matched_at_ms,
        observed_at_ms,
    };
    let taker_side = fill_side_from_polymarket_side(trade.side)
        .context("owned WS trade has unsupported side")?;
    let mut observations = if role == NormalizedTraderRole::Maker {
        trade
            .maker_orders
            .iter()
            .filter(|maker| maker.owner == own_api_key)
            .map(|maker| {
                let owned_side = match (maker.asset_id == trade.asset_id, taker_side) {
                    (true, FillSide::Buy) => FillSide::Sell,
                    (true, FillSide::Sell) => FillSide::Buy,
                    (false, side) => side,
                };
                let (owned_size_units_6dp, owned_gross_proceeds_units_6dp, owned_fee_units_6dp) =
                    execution_component_economics(
                        maker.price,
                        maker.matched_amount,
                        execution_fee_market_state,
                        false,
                    )?;
                Ok(observation_base.for_owned_order(
                    maker.order_id.clone(),
                    maker.asset_id.to_string(),
                    owned_side,
                    decimal_price_to_units_6dp(maker.price, "maker trade price")?,
                    owned_size_units_6dp,
                    Some(owned_gross_proceeds_units_6dp),
                    owned_fee_units_6dp,
                ))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        let order_id = trade
            .taker_order_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("owned taker trade is missing taker_order_id")?;
        let trade_asset_id = trade.asset_id.to_string();
        let components = trade
            .maker_orders
            .iter()
            .map(|maker| {
                (
                    maker.asset_id.to_string(),
                    maker.price,
                    maker.matched_amount,
                )
            })
            .collect::<Vec<_>>();
        let economics = taker_execution_economics(
            trade_asset_id.as_str(),
            top_size_units_6dp,
            components
                .iter()
                .map(|(asset_id, price, size)| (asset_id.as_str(), *price, *size)),
            execution_fee_market_state,
        );
        vec![observation_base.for_owned_order(
            order_id.to_string(),
            trade_asset_id,
            taker_side,
            top_price_units_6dp,
            top_size_units_6dp,
            economics.map(|(gross, _)| gross),
            economics.and_then(|(_, fee)| fee),
        )]
    };
    anyhow::ensure!(
        !observations.is_empty(),
        "owned CLOB trade update has no owned order"
    );
    observations.sort_by(|left, right| left.order_id().cmp(right.order_id()));
    Ok(observations)
}

pub(crate) fn ws_trade_to_update(
    trade: &TradeMessage,
    observations: Vec<UnboundCtfTradeObservation>,
) -> Result<WsTradeUpdate> {
    anyhow::ensure!(
        observations
            .windows(2)
            .all(|pair| pair[0].order_id() < pair[1].order_id()),
        "authorized trade observations are not sorted unique"
    );
    let status = ws_trade_status(&trade.status)?;
    let role = normalized_trader_role(trade.trader_side.as_ref())?;
    let owner_matches = role == NormalizedTraderRole::Taker && !observations.is_empty();
    let top_size_units_6dp = decimal_to_units_6dp(trade.size, "trade size")?;
    let taker_side = fill_side_from_polymarket_side(trade.side)
        .context("owned WS trade has unsupported side")?;
    let mut fills = if status == CtfTradeStatus::Confirmed {
        let Some(ts) = [trade.last_update, trade.timestamp]
            .into_iter()
            .flatten()
            .find(|ts| *ts > 0)
        else {
            anyhow::bail!("confirmed WS trade is missing vendor update timestamp")
        };
        normalized_trade_to_fills(NormalizedTrade {
            trade_id: trade.id.clone(),
            transaction_hash: trade.transaction_hash.map(|hash| hash.to_string()),
            token: trade.asset_id.to_string(),
            ts_ms: ts_to_ms(ts),
            taker_side,
            taker_px: decimal_to_f64(trade.price)?,
            taker_size: decimal_to_f64(trade.size)?,
            taker_size_units_6dp: top_size_units_6dp,
            taker_order_id: trade.taker_order_id.clone(),
            fee_market_state: fee_market_state(trade.fee_rate_bps),
            owner_matches,
            role,
            maker_orders: trade
                .maker_orders
                .iter()
                .map(|maker| {
                    let owned_observation = observations
                        .binary_search_by(|observation| {
                            observation.order_id().cmp(maker.order_id.as_str())
                        })
                        .ok()
                        .map(|index| &observations[index]);
                    normalized_maker_order(
                        owned_observation.is_some(),
                        maker.order_id.clone(),
                        maker.asset_id.to_string(),
                        owned_observation.map(|observation| observation.side()),
                        maker.matched_amount,
                        maker.price,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        })?
    } else {
        Vec::new()
    };
    for fill in &mut fills {
        let index = observations
            .binary_search_by(|observation| observation.order_id().cmp(fill.order_id.as_str()))
            .ok()
            .context("confirmed WS fill has no exact trade observation")?;
        fill.trade_observation = Some(observations[index].clone());
    }
    Ok(WsTradeUpdate { fills })
}

pub fn rest_trade_to_fills(trade: &TradeResponse, own_api_key: ApiKey) -> Result<Vec<FillEvent>> {
    if trade.status != TradeStatusType::Confirmed {
        return Ok(Vec::new());
    }
    let taker_side = match fill_side_from_polymarket_side(trade.side) {
        Some(side) => side,
        None => return Ok(Vec::new()),
    };
    let role = normalized_trader_role(Some(&trade.trader_side))?;
    let top_price_units_6dp = decimal_price_to_units_6dp(trade.price, "trade price")?;
    let top_size_units_6dp = decimal_to_units_6dp(trade.size, "trade size")?;
    let execution_fee_market_state = fee_market_state(Some(trade.fee_rate_bps));
    let matched_at_ms = ts_to_ms(trade.match_time.timestamp_millis());
    let observed_at_ms = ts_to_ms(trade.last_update.timestamp_millis());
    anyhow::ensure!(
        observed_at_ms >= matched_at_ms,
        "REST trade observation precedes match_time"
    );
    let mut maker_order_ids = trade
        .maker_orders
        .iter()
        .map(|maker| maker.order_id.clone())
        .collect::<Vec<_>>();
    maker_order_ids.sort();
    let observation_base = TradeObservationBase {
        trade_id: trade.id.clone(),
        market_id: MarketId::new(trade.market.to_string()),
        trade_asset_id: trade.asset_id.to_string(),
        taker_order_id: Some(trade.taker_order_id.clone()),
        maker_order_ids,
        status: CtfTradeStatus::Confirmed,
        price_units_6dp: top_price_units_6dp,
        size_units_6dp: top_size_units_6dp,
        matched_at_ms,
        observed_at_ms,
    };
    let observations = if role == NormalizedTraderRole::Maker {
        trade
            .maker_orders
            .iter()
            .filter(|maker| maker.owner == own_api_key)
            .map(|maker| {
                let owned_side = fill_side_from_polymarket_side(maker.side)
                    .context("owned REST maker trade has unsupported side")?;
                let (owned_size_units_6dp, owned_gross_proceeds_units_6dp, owned_fee_units_6dp) =
                    execution_component_economics(
                        maker.price,
                        maker.matched_amount,
                        execution_fee_market_state,
                        false,
                    )?;
                Ok(observation_base.for_owned_order(
                    maker.order_id.clone(),
                    maker.asset_id.to_string(),
                    owned_side,
                    decimal_price_to_units_6dp(maker.price, "maker trade price")?,
                    owned_size_units_6dp,
                    Some(owned_gross_proceeds_units_6dp),
                    owned_fee_units_6dp,
                ))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        anyhow::ensure!(
            !trade.taker_order_id.trim().is_empty(),
            "owned REST taker trade is missing taker_order_id"
        );
        let trade_asset_id = trade.asset_id.to_string();
        let components = trade
            .maker_orders
            .iter()
            .map(|maker| {
                (
                    maker.asset_id.to_string(),
                    maker.price,
                    maker.matched_amount,
                )
            })
            .collect::<Vec<_>>();
        let economics = taker_execution_economics(
            trade_asset_id.as_str(),
            top_size_units_6dp,
            components
                .iter()
                .map(|(asset_id, price, size)| (asset_id.as_str(), *price, *size)),
            execution_fee_market_state,
        );
        vec![observation_base.for_owned_order(
            trade.taker_order_id.clone(),
            trade_asset_id,
            taker_side,
            top_price_units_6dp,
            top_size_units_6dp,
            economics.map(|(gross, _)| gross),
            economics.and_then(|(_, fee)| fee),
        )]
    };
    anyhow::ensure!(
        !observations.is_empty(),
        "owned REST trade has no owned order"
    );
    let mut fills = normalized_trade_to_fills(NormalizedTrade {
        trade_id: trade.id.clone(),
        transaction_hash: Some(trade.transaction_hash.to_string()),
        token: trade.asset_id.to_string(),
        ts_ms: ts_to_ms(trade.last_update.timestamp_millis()),
        taker_side,
        taker_px: decimal_to_f64(trade.price)?,
        taker_size: decimal_to_f64(trade.size)?,
        taker_size_units_6dp: top_size_units_6dp,
        taker_order_id: Some(trade.taker_order_id.clone()),
        fee_market_state: fee_market_state(Some(trade.fee_rate_bps)),
        owner_matches: trade.owner == own_api_key,
        role,
        maker_orders: trade
            .maker_orders
            .iter()
            .map(|maker| {
                normalized_maker_order(
                    maker.owner == own_api_key,
                    maker.order_id.clone(),
                    maker.asset_id.to_string(),
                    fill_side_from_polymarket_side(maker.side),
                    maker.matched_amount,
                    maker.price,
                )
            })
            .collect::<Result<Vec<_>>>()?,
    })?;
    for fill in &mut fills {
        let observation = observations
            .iter()
            .find(|observation| observation.order_id() == fill.order_id)
            .context("confirmed REST fill has no exact trade observation")?;
        fill.trade_observation = Some(observation.clone());
    }
    Ok(fills)
}

pub fn decimal_to_f64(value: Decimal) -> Result<f64> {
    let repr = value.normalize().to_string();
    let parsed = repr
        .parse::<f64>()
        .with_context(|| format!("failed to parse decimal `{repr}` as f64"))?;
    Ok(parsed)
}

fn decimal_price_to_units_6dp(mut value: Decimal, field: &str) -> Result<u128> {
    anyhow::ensure!(value >= Decimal::ZERO, "{field} is negative: {value}");
    value.rescale(6);
    decimal_to_units_6dp(value, field)
}

pub(super) fn decimal_to_units_6dp(value: Decimal, field: &str) -> Result<u128> {
    let repr = value.normalize().to_string();
    anyhow::ensure!(!repr.starts_with('-'), "{field} is negative: {repr}");
    let (whole, fractional) = repr.split_once('.').unwrap_or((&repr, ""));
    anyhow::ensure!(
        fractional.len() <= 6,
        "{field} exceeds six decimal places: {repr}"
    );
    let whole = whole
        .parse::<u128>()
        .with_context(|| format!("{field} whole units are invalid: {repr}"))?;
    let fractional = if fractional.is_empty() {
        0
    } else {
        fractional
            .parse::<u128>()
            .with_context(|| format!("{field} fractional units are invalid: {repr}"))?
            .checked_mul(10_u128.pow((6 - fractional.len()) as u32))
            .with_context(|| format!("{field} fractional units overflow: {repr}"))?
    };
    whole
        .checked_mul(1_000_000)
        .and_then(|units| units.checked_add(fractional))
        .with_context(|| format!("{field} overflows fixed-6dp units: {repr}"))
}

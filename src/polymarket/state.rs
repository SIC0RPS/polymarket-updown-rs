use std::str::FromStr;
use std::sync::Arc;

use polymarket_client_sdk::types::B256;

use crate::polymarket::discovery::{MarketSelection, TokenLifecycleState};
use crate::polymarket::identity::{MarketId, MarketIdentity};
use crate::types::Side;

pub use bbwick_engine::state::{
    BookDepth, BookTop, LastTrade, PolymarketLifecycleStatus, PolymarketOddsState, PriceLevel,
    TradeSide,
};

#[path = "state/odds_filter.rs"]
mod odds_filter;
pub use odds_filter::odds_filtered_for_underlying;
pub(crate) use odds_filter::{
    localized_odds_for_underlying, odds_filtered_for_underlying_unfingerprinted,
    retain_odds_for_underlying,
};

pub fn book_for_token(
    odds: &PolymarketOddsState,
    token_id: polymarket_client_sdk::types::U256,
) -> Option<&BookTop> {
    odds.books.get(&token_id)
}

pub fn depth_for_token(
    odds: &PolymarketOddsState,
    token_id: polymarket_client_sdk::types::U256,
) -> Option<&BookDepth> {
    odds.depths.get(&token_id)
}

pub(crate) fn book_level_price_is_l1_coherent(
    book: &BookTop,
    side: Side,
    level_price: f64,
) -> Option<bool> {
    let l1_price = match side {
        Side::Buy => book.ask,
        Side::Sell => book.bid,
    }?;
    let units_6dp = |value: f64| {
        if !(value.is_finite() && value > 0.0) {
            return None;
        }
        let scaled = value * crate::polymarket::fixed_math::FIXED_6DP_SCALE as f64;
        let rounded = scaled.round();
        (rounded >= 1.0 && rounded <= u128::MAX as f64 && (scaled - rounded).abs() <= 0.000_001)
            .then_some(rounded as u128)
    };
    let l1_units_6dp = units_6dp(l1_price)?;
    let level_units_6dp = units_6dp(level_price)?;
    Some(match side {
        Side::Buy => level_units_6dp >= l1_units_6dp,
        Side::Sell => level_units_6dp <= l1_units_6dp,
    })
}

#[derive(Debug, Clone, Default)]
pub struct MarketProjection {
    source_revision: u64,
    source_updated_at_ms: u64,
    markets: Arc<[MarketSelection]>,
    market_index_by_id: Arc<std::collections::HashMap<String, usize>>,
    market_index_by_token: Arc<std::collections::HashMap<String, usize>>,
    current_selection_market_id: Option<String>,
    next_selection_market_id: Option<String>,
}

impl MarketProjection {
    pub fn from_odds(odds: &PolymarketOddsState) -> Self {
        let mut market_rows = Vec::with_capacity(odds.markets.len().saturating_add(2));
        let mut market_index_by_id = std::collections::HashMap::new();
        let mut market_index_by_token = std::collections::HashMap::new();
        let mut push_unique =
            |selection: &bbwick_engine::state::MarketSelectionSnapshot| -> Option<String> {
                let market = convert_selection_snapshot(selection);
                let market_id = market.market_id.to_string();
                if market_index_by_id.contains_key(&market_id) {
                    return Some(market_id);
                }
                let idx = market_rows.len();
                market_index_by_token.insert(market.up_token_id.clone(), idx);
                market_index_by_token.insert(market.down_token_id.clone(), idx);
                market_index_by_id.insert(market_id.clone(), idx);
                market_rows.push(market);
                Some(market_id)
            };

        for market in &odds.markets {
            let _ = push_unique(market);
        }
        let current_selection_market_id = odds.selection.as_ref().and_then(&mut push_unique);
        let next_selection_market_id = odds.next_selection.as_ref().and_then(&mut push_unique);

        Self {
            source_revision: odds.revision,
            source_updated_at_ms: odds.updated_at_ms,
            markets: Arc::<[MarketSelection]>::from(market_rows),
            market_index_by_id: Arc::new(market_index_by_id),
            market_index_by_token: Arc::new(market_index_by_token),
            current_selection_market_id,
            next_selection_market_id,
        }
    }

    pub fn source_revision(&self) -> u64 {
        self.source_revision
    }

    pub fn source_updated_at_ms(&self) -> u64 {
        self.source_updated_at_ms
    }

    pub fn markets(&self) -> &[MarketSelection] {
        &self.markets
    }

    pub fn market_rows(&self) -> Arc<[MarketSelection]> {
        self.markets.clone()
    }

    pub fn market_by_id(&self, market_id: &str) -> Option<&MarketSelection> {
        self.market_index_by_id
            .get(market_id)
            .and_then(|&idx| self.markets.get(idx))
    }

    pub fn market_by_token(&self, token: &str) -> Option<&MarketSelection> {
        self.market_index_by_token
            .get(token)
            .and_then(|&idx| self.markets.get(idx))
    }

    pub fn market_for_token(&self, token: &str, market_id: &str) -> Option<&MarketSelection> {
        self.market_by_token(token)
            .filter(|selection| selection.market_id.as_str() == market_id)
            .or_else(|| {
                self.markets.iter().find(|selection| {
                    selection.market_id.as_str() == market_id
                        && (selection.up_token_id() == token || selection.down_token_id() == token)
                })
            })
    }

    pub fn contains_token(&self, token: &str) -> bool {
        self.market_index_by_token.contains_key(token)
    }

    pub fn current_selection(&self) -> Option<&MarketSelection> {
        self.current_selection_market_id
            .as_deref()
            .and_then(|market_id| self.market_by_id(market_id))
    }

    pub fn next_selection(&self) -> Option<&MarketSelection> {
        self.next_selection_market_id
            .as_deref()
            .and_then(|market_id| self.market_by_id(market_id))
    }

    pub fn filtered_for_underlying(&self, underlying: &str) -> Self {
        let underlying = underlying.trim();
        let markets = self
            .markets
            .iter()
            .filter(|market| market.underlying.eq_ignore_ascii_case(underlying))
            .cloned()
            .collect::<Vec<_>>();
        let mut market_index_by_id = std::collections::HashMap::new();
        let mut market_index_by_token = std::collections::HashMap::new();
        for (idx, market) in markets.iter().enumerate() {
            market_index_by_id.insert(market.market_id.to_string(), idx);
            market_index_by_token.insert(market.up_token_id.clone(), idx);
            market_index_by_token.insert(market.down_token_id.clone(), idx);
        }
        let keep_market = |market_id: &Option<String>, keep: fn(&MarketSelection, u64) -> bool| {
            market_id
                .as_ref()
                .and_then(|market_id| {
                    market_index_by_id
                        .get(market_id.as_str())
                        .and_then(|index| markets.get(*index))
                })
                .filter(|market| keep(market, self.source_updated_at_ms))
                .map(|market| market.market_id.to_string())
        };
        let current_selection_market_id =
            keep_market(&self.current_selection_market_id, current_or_future_market).or_else(
                || {
                    markets
                        .iter()
                        .find(|market| {
                            market.window_start_ms <= self.source_updated_at_ms
                                && self.source_updated_at_ms < market.window_end_ms
                        })
                        .or_else(|| {
                            markets
                                .iter()
                                .find(|market| self.source_updated_at_ms < market.window_end_ms)
                        })
                        .map(|market| market.market_id.to_string())
                },
            );
        let next_selection_market_id = keep_market(&self.next_selection_market_id, future_market)
            .or_else(|| {
                markets
                    .iter()
                    .filter(|market| market.window_start_ms > self.source_updated_at_ms)
                    .min_by_key(|market| market.window_start_ms)
                    .map(|market| market.market_id.to_string())
            });
        Self {
            source_revision: self.source_revision,
            source_updated_at_ms: self.source_updated_at_ms,
            markets: Arc::from(markets),
            market_index_by_id: Arc::new(market_index_by_id),
            market_index_by_token: Arc::new(market_index_by_token),
            current_selection_market_id,
            next_selection_market_id,
        }
    }
}

fn current_or_future_market(market: &MarketSelection, ts_ms: u64) -> bool {
    ts_ms < market.window_end_ms
}

fn future_market(market: &MarketSelection, ts_ms: u64) -> bool {
    ts_ms < market.window_start_ms
}

pub fn convert_selection_snapshot(
    selection: &bbwick_engine::state::MarketSelectionSnapshot,
) -> MarketSelection {
    let condition_id_b256 = selection
        .condition_id_b256
        .as_deref()
        .and_then(|raw| B256::from_str(raw).ok());
    MarketSelection {
        identity: MarketIdentity::new(
            MarketId::new(selection.market_id.clone()),
            selection.gamma_market_id.clone(),
            condition_id_b256,
        ),
        market_id: MarketId::new(selection.market_id.clone()),
        condition_id_b256,
        condition_id: MarketId::new(selection.condition_id.clone()),
        gamma_market_id: selection.gamma_market_id.clone(),
        underlying: selection.underlying.clone(),
        up_token: selection.up_token,
        down_token: selection.down_token,
        up_token_id: selection.up_token.to_string(),
        down_token_id: selection.down_token.to_string(),
        window_start_ms: selection.window_start_ms,
        window_end_ms: selection.window_end_ms,
        maker_base_fee_bps: selection.maker_base_fee_bps,
        taker_base_fee_bps: selection.taker_base_fee_bps,
        minimum_tick_size: selection.minimum_tick_size,
        minimum_order_size: selection.minimum_order_size,
        rewards_min_size: selection.rewards_min_size,
        rewards_max_spread: selection.rewards_max_spread,
        resolution_source: selection.resolution_source.clone(),
        neg_risk: selection.neg_risk,
        itode: selection.itode,
        standard_binary_ctf: selection.standard_binary_ctf,
        question: selection.question.clone(),
        slug: selection.slug.clone(),
        up_lifecycle: parse_token_lifecycle_state(&selection.up_lifecycle),
        down_lifecycle: parse_token_lifecycle_state(&selection.down_lifecycle),
    }
}

pub(crate) fn parse_token_lifecycle_state(raw: &str) -> TokenLifecycleState {
    match raw.trim() {
        "active" => TokenLifecycleState::Active,
        "invalid_after_404" => TokenLifecycleState::InvalidAfter404,
        "resolved" => TokenLifecycleState::Resolved,
        _ => TokenLifecycleState::PendingRevalidation,
    }
}

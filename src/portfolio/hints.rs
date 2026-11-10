use std::collections::HashMap;

use crate::polymarket::identity::MarketId;
use crate::polymarket::state::PolymarketOddsState;

use super::fills::FillEvent;
use super::state::PortfolioState;

pub(in crate::portfolio) const TOKEN_MARKET_HINT_GRACE_MS: u64 = 10 * 60 * 1000;

impl PortfolioState {
    pub(in crate::portfolio) fn market_id_for_fill(&self, fill: &FillEvent) -> Option<MarketId> {
        fill.market_id
            .as_ref()
            .map(|market_id| market_id.as_str().trim())
            .filter(|market_id| !market_id.is_empty() && *market_id != "-")
            .map(MarketId::new)
            .or_else(|| self.token_market_hint.get(&fill.token).cloned())
    }

    pub(in crate::portfolio) fn prune_token_market_hints(&mut self, now: u64) {
        self.token_market_hint.retain(|token, _| {
            self.positions.contains_key(token)
                || self.open_lots.contains_key(token)
                || self
                    .token_expiry_ms
                    .get(token)
                    .is_some_and(|expiry| now < expiry.saturating_add(TOKEN_MARKET_HINT_GRACE_MS))
        });
    }
}

pub(in crate::portfolio) fn build_token_expiry_map(
    odds: &PolymarketOddsState,
) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for market in &odds.markets {
        map.insert(market.up_token.to_string(), market.window_end_ms);
        map.insert(market.down_token.to_string(), market.window_end_ms);
    }
    if let Some(sel) = &odds.selection {
        map.insert(sel.up_token.to_string(), sel.window_end_ms);
        map.insert(sel.down_token.to_string(), sel.window_end_ms);
    }
    if let Some(sel) = &odds.next_selection {
        map.insert(sel.up_token.to_string(), sel.window_end_ms);
        map.insert(sel.down_token.to_string(), sel.window_end_ms);
    }
    map
}

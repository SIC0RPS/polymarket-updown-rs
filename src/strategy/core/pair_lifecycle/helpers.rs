use crate::polymarket::identity::MarketId;
use crate::polymarket::order_rules::truncate_to_vendor_qty;
use crate::polymarket::state::PolymarketOddsState;
use crate::strategy::paired_maker::{MakerRebateMmStrategy, PAIRED_RELEASE_QTY_EPS};
use crate::strategy::{CancelQuotesRequest, StrategyAction, StrategyDecision};
use crate::types::Side;

const RECENT_CANCEL_QUOTES_GUARD_MS: u64 = 60_000;

impl MakerRebateMmStrategy {
    pub(crate) fn pair_material_asymmetry_qty(
        &self,
        odds: &PolymarketOddsState,
        up_token: &str,
        down_token: &str,
    ) -> f64 {
        let up_min_qty = up_token
            .parse::<polymarket_client_sdk::types::U256>()
            .ok()
            .and_then(|token| self.current_book_for_token(odds, token))
            .map(|book| self.venue_min_size(&book))
            .unwrap_or(0.0);
        let down_min_qty = down_token
            .parse::<polymarket_client_sdk::types::U256>()
            .ok()
            .and_then(|token| self.current_book_for_token(odds, token))
            .map(|book| self.venue_min_size(&book))
            .unwrap_or(0.0);
        truncate_to_vendor_qty(up_min_qty.max(down_min_qty)).max(PAIRED_RELEASE_QTY_EPS)
    }

    fn pending_cancel_quotes_for_group(&self, quote_group: &str, side: Option<Side>) -> bool {
        self.pending.iter().any(|decision| {
            matches!(
                &decision.action,
                StrategyAction::CancelQuotes(request)
                    if request.quote_group == quote_group && request.side == side
            )
        })
    }

    fn cancel_quote_guard_key(
        market_id: &MarketId,
        quote_group: &str,
        side: Option<Side>,
    ) -> String {
        format!("{market_id}:{quote_group}:{side:?}")
    }

    fn cancel_quote_guard_active(
        &mut self,
        market_id: &MarketId,
        quote_group: &str,
        side: Option<Side>,
        now_ms: u64,
    ) -> bool {
        let key = Self::cancel_quote_guard_key(market_id, quote_group, side);
        let Some(emitted_at_ms) = self.cancel_quote_guard_by_key.get(&key).copied() else {
            return false;
        };
        if now_ms.saturating_sub(emitted_at_ms) >= RECENT_CANCEL_QUOTES_GUARD_MS {
            self.cancel_quote_guard_by_key.remove(&key);
            return false;
        }
        true
    }

    pub(crate) fn queue_cancel_quotes_for_group(
        &mut self,
        market_id: &MarketId,
        quote_group: &str,
        side: Option<Side>,
        now_ms: u64,
        reason: impl Into<String>,
    ) {
        if quote_group.trim().is_empty()
            || self.pending_cancel_quotes_for_group(quote_group, side)
            || self.cancel_quote_guard_active(market_id, quote_group, side, now_ms)
        {
            return;
        }
        self.cancel_quote_guard_by_key.insert(
            Self::cancel_quote_guard_key(market_id, quote_group, side),
            now_ms,
        );
        self.pending.push_back(StrategyDecision {
            action: StrategyAction::CancelQuotes(CancelQuotesRequest {
                market_id: Some(market_id.clone()),
                quote_group: quote_group.to_string(),
                side,
            }),
            context: None,
            p_up: None,
            edge: None,
            reason: reason.into(),
        });
    }
}

// Strategy support required for behavior: sell/market blocking policy used by
// paired cleanup and managed exit routing.
use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::discovery::TokenLifecycleState;
use crate::polymarket::state::BookTop;

use crate::strategy::paired_maker::MakerRebateMmStrategy;
use crate::strategy::StrategyFactSink;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SellBlockReason {
    InvalidAfter404,
    ResolvedOrQuarantined,
    MissingBook,
    StaleOrInvalidBook,
    InvalidTick,
    InvalidSellPrice,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SellSurfaceState {
    pub token_lifecycle: TokenLifecycleState,
    pub market_quarantined: bool,
    pub book_present: bool,
    pub book_current: bool,
    pub book_valid: bool,
    pub valid_tick: bool,
    pub sell_price_valid: bool,
}

impl SellSurfaceState {
    pub(crate) fn sell_block_reason(self) -> Option<SellBlockReason> {
        if self.token_lifecycle == TokenLifecycleState::InvalidAfter404 {
            Some(SellBlockReason::InvalidAfter404)
        } else if self.market_quarantined || self.token_lifecycle == TokenLifecycleState::Resolved {
            Some(SellBlockReason::ResolvedOrQuarantined)
        } else if !self.book_present {
            Some(SellBlockReason::MissingBook)
        } else if !self.book_valid || !self.book_current {
            Some(SellBlockReason::StaleOrInvalidBook)
        } else if !self.valid_tick {
            Some(SellBlockReason::InvalidTick)
        } else if !self.sell_price_valid {
            Some(SellBlockReason::InvalidSellPrice)
        } else {
            None
        }
    }
}

pub(crate) fn sell_block_reason(
    _market_id: &str,
    _token: &str,
    book: Option<&BookTop>,
    state: &SellSurfaceState,
) -> Option<SellBlockReason> {
    if book.is_none() {
        Some(SellBlockReason::MissingBook)
    } else {
        state.sell_block_reason()
    }
}

impl MakerRebateMmStrategy {
    pub(super) fn token_lifecycle_for_selection(
        selection: Option<&MarketSelection>,
        token: &str,
    ) -> TokenLifecycleState {
        selection
            .map(|market| {
                if market.up_token_id() == token {
                    market.up_lifecycle
                } else {
                    market.down_lifecycle
                }
            })
            .unwrap_or(TokenLifecycleState::PendingRevalidation)
    }

    pub(super) fn market_selection_is_quarantined(selection: Option<&MarketSelection>) -> bool {
        selection.is_some_and(|market| {
            matches!(
                market.up_lifecycle,
                TokenLifecycleState::InvalidAfter404 | TokenLifecycleState::Resolved
            ) || matches!(
                market.down_lifecycle,
                TokenLifecycleState::InvalidAfter404 | TokenLifecycleState::Resolved
            )
        })
    }

    pub(super) fn market_quarantined_from_exit_routing(&self, market: &MarketSelection) -> bool {
        Self::market_selection_is_quarantined(Some(market))
    }

    pub(super) fn sell_surface_state_for_selection(
        &self,
        selection: Option<&MarketSelection>,
        token: &str,
        book: Option<&BookTop>,
        book_valid: bool,
        book_current: bool,
        valid_tick: bool,
        sell_price_valid: bool,
    ) -> SellSurfaceState {
        SellSurfaceState {
            token_lifecycle: Self::token_lifecycle_for_selection(selection, token),
            market_quarantined: Self::market_selection_is_quarantined(selection),
            book_present: book.is_some(),
            book_current,
            book_valid,
            valid_tick,
            sell_price_valid,
        }
    }

    pub(super) fn note_sell_guard_block(
        &mut self,
        fact_buffer: &mut StrategyFactSink,
        market_id: &str,
        token: &str,
        close_hint: &str,
        book: Option<&BookTop>,
        state: &SellSurfaceState,
        now_ms: u64,
    ) {
        let _ = (
            fact_buffer,
            market_id,
            token,
            close_hint,
            book,
            state,
            now_ms,
            sell_block_reason(market_id, token, book, state),
        );
    }
}

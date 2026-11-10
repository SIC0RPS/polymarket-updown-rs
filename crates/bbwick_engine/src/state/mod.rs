mod book_state;
mod book_validation;
mod discovery_state;
mod fingerprint;
mod lifecycle_state;
mod resolution_state;
mod store;
mod trade_state;

pub use crate::wire::TradeSide;
pub use book_state::{
    is_valid_polymarket_tick_size, polymarket_tick_size_from_f64, BookConstraints, BookDepth,
    BookTop, PriceLevel,
};
pub(crate) use book_validation::strict_ladder_is_valid;
pub use discovery_state::{DiscoverySnapshot, MarketSelectionSnapshot};
pub use fingerprint::{fingerprint_hash, Fingerprint};
pub use lifecycle_state::PolymarketLifecycleStatus;
pub use resolution_state::CapturedMarketResolutionContext;
pub(crate) use store::PriceChangeApplyOutcome;
pub use store::{
    normalize_ws_ts_ms, LastCanonicalBid, LifecycleSyncRequest, PolymarketOddsState,
    PolymarketStateStore,
};
pub use trade_state::LastTrade;

use crate::polymarket::identity::MarketId;

use super::state::PortfolioState;

#[derive(Debug, Clone)]
pub struct ClosedTrade {
    pub seq: u64,
    pub market_id: MarketId,
    pub token: String,
    pub engine_trade_id: Option<String>,
    pub close_reason: String,
    pub exit_fee: Option<f64>,
    pub exit_order_id: Option<String>,
    pub exit_fill_id: Option<String>,
    pub exit_tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedeemCloseRequest {
    pub market_id: MarketId,
    pub token: String,
    pub outcome_index: Option<u8>,
    pub qty_units_6dp: u128,
    pub close_reason: String,
    pub closed_at_ms: u64,
    pub exit_tx_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::portfolio) struct TerminalCloseKey {
    pub market_id: MarketId,
    pub token: String,
    pub close_reason: String,
    pub exit_tx_hash: String,
}

impl TerminalCloseKey {
    pub(in crate::portfolio) fn from_request(request: &RedeemCloseRequest) -> Option<Self> {
        let exit_tx_hash = request.exit_tx_hash.as_deref()?.trim();
        if exit_tx_hash.is_empty() {
            return None;
        }
        Some(Self {
            market_id: request.market_id.clone(),
            token: request.token.clone(),
            close_reason: request.close_reason.clone(),
            exit_tx_hash: exit_tx_hash.to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementInventoryClearance {
    pub market_id: MarketId,
    pub token: String,
    pub outcome_index: Option<u8>,
    pub engine_trade_id: Option<String>,
    pub qty_units_6dp: u128,
    pub closed_at_ms: u64,
    pub settlement_tx_hash: Option<String>,
}

pub(in crate::portfolio) const MAX_CLOSED_TRADES: usize = 1_024;

impl PortfolioState {
    pub(in crate::portfolio) fn record_closed_trade(&mut self, trade: ClosedTrade) {
        self.closed_trades.push_back(trade);
        while self.closed_trades.len() > MAX_CLOSED_TRADES {
            let _ = self.closed_trades.pop_front();
        }
    }
}

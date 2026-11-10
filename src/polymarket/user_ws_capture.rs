use polymarket_client_sdk::auth::ApiKey;
use polymarket_client_sdk::clob::ws::types::response::{
    OrderMessage, OrderMessageType, TradeMessage, TradeMessageStatus, TradeMessageType,
};
use polymarket_client_sdk::types::Decimal;

use crate::capture::events::{
    PolymarketUserOrderCapture, PolymarketUserTradeCapture, PolymarketUserTradeMakerOrderCapture,
};
use crate::polymarket::fills::ts_to_ms;

pub(super) fn user_order_capture(
    order: &OrderMessage,
    recv_ts_ms: u64,
    event_ts: i64,
) -> PolymarketUserOrderCapture {
    PolymarketUserOrderCapture {
        session_ts_ms: recv_ts_ms,
        recv_ts_ms,
        event_ts_ms: ts_to_ms(event_ts),
        order_id: order.id.clone(),
        market: order.market.to_string(),
        token: order.asset_id.to_string(),
        side: clob_side_string(order.side),
        price: decimal_string(order.price),
        msg_type: order.msg_type.as_ref().map(order_msg_type_string),
        outcome: order.outcome.clone(),
        owner: order.owner.map(|value| value.to_string()),
        order_owner: order.order_owner.map(|value| value.to_string()),
        original_size: order.original_size.map(decimal_string),
        size_matched: order.size_matched.map(decimal_string),
        timestamp: order.timestamp,
        associate_trades: order.associate_trades.clone(),
        status: order.status.as_ref().map(ToString::to_string),
    }
}

pub(super) fn user_trade_capture(
    trade: &TradeMessage,
    recv_ts_ms: u64,
    event_ts: i64,
) -> PolymarketUserTradeCapture {
    PolymarketUserTradeCapture {
        session_ts_ms: recv_ts_ms,
        recv_ts_ms,
        event_ts_ms: ts_to_ms(event_ts),
        trade_id: trade.id.clone(),
        market: trade.market.to_string(),
        token: trade.asset_id.to_string(),
        side: clob_side_string(trade.side),
        size: decimal_string(trade.size),
        price: decimal_string(trade.price),
        status: trade_status_string(&trade.status),
        msg_type: trade.msg_type.as_ref().map(trade_msg_type_string),
        last_update: trade.last_update,
        matchtime: trade.matchtime,
        timestamp: trade.timestamp,
        outcome: trade.outcome.clone(),
        owner: trade.owner.map(|value| value.to_string()),
        trade_owner: trade.trade_owner.map(|value| value.to_string()),
        taker_order_id: trade.taker_order_id.clone(),
        fee_rate_bps: trade.fee_rate_bps.map(decimal_string),
        transaction_hash: trade.transaction_hash.map(|value| value.to_string()),
        trader_side: trade.trader_side.as_ref().map(|value| format!("{value:?}")),
        maker_orders: trade
            .maker_orders
            .iter()
            .map(|maker| PolymarketUserTradeMakerOrderCapture {
                order_id: maker.order_id.clone(),
                owner: maker.owner.to_string(),
                token: maker.asset_id.to_string(),
                matched_amount: decimal_string(maker.matched_amount),
                price: decimal_string(maker.price),
                outcome: maker.outcome.clone(),
            })
            .collect(),
    }
}

pub(super) fn user_trade_belongs_to_us(trade: &TradeMessage, own_api_key: ApiKey) -> bool {
    trade.owner == Some(own_api_key)
        || trade.trade_owner == Some(own_api_key)
        || trade
            .maker_orders
            .iter()
            .any(|maker| maker.owner == own_api_key)
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn clob_side_string(side: polymarket_client_sdk::clob::types::Side) -> String {
    match side {
        polymarket_client_sdk::clob::types::Side::Buy => "BUY".to_string(),
        polymarket_client_sdk::clob::types::Side::Sell => "SELL".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn order_msg_type_string(value: &OrderMessageType) -> String {
    match value {
        OrderMessageType::Placement => "PLACEMENT".to_string(),
        OrderMessageType::Update => "UPDATE".to_string(),
        OrderMessageType::Cancellation => "CANCELLATION".to_string(),
        OrderMessageType::Unknown(value) => value.clone(),
        _ => "UNKNOWN".to_string(),
    }
}

fn trade_msg_type_string(value: &TradeMessageType) -> String {
    match value {
        TradeMessageType::Trade => "TRADE".to_string(),
        TradeMessageType::Unknown(value) => value.clone(),
        _ => "UNKNOWN".to_string(),
    }
}

fn trade_status_string(value: &TradeMessageStatus) -> String {
    match value {
        TradeMessageStatus::Matched => "MATCHED".to_string(),
        TradeMessageStatus::Mined => "MINED".to_string(),
        TradeMessageStatus::Confirmed => "CONFIRMED".to_string(),
        TradeMessageStatus::Retrying => "RETRYING".to_string(),
        TradeMessageStatus::Failed => "FAILED".to_string(),
        TradeMessageStatus::Unknown(value) => value.clone(),
        _ => "UNKNOWN".to_string(),
    }
}

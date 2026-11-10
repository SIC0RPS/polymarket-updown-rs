use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::rpc::types::TransactionReceipt;
use alloy::sol;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use polymarket_client_sdk::auth::ApiKey;
use polymarket_client_sdk::clob::types::response::TradeResponse;
use polymarket_client_sdk::clob::types::{Side as PolymarketSide, TradeStatusType, TraderSide};
use polymarket_client_sdk::clob::ws::types::response::{TradeMessage, TradeMessageStatus};
use polymarket_client_sdk::types::{Address, Decimal, B256, U256};

use crate::polymarket::contracts_v2;
use crate::polymarket::identity::MarketId;
use crate::portfolio::{FillEvent, FillSide};
use crate::trade_session_ledger::{
    CtfTradeStatus, ManagedSellReceiptIdentity, ObservedCtfTradeFact,
};

#[path = "fills/normalization.rs"]
mod normalization;
#[path = "fills/ws_trade.rs"]
mod ws_trade;

pub use normalization::ts_to_ms;
pub(crate) use normalization::{
    direct_buy_affordable_qty_units_6dp, direct_buy_cash_requirement_units_6dp,
    documented_fee_usdc_for_taker_base_fee_bps,
};
use normalization::{
    fee_market_state, fill_side_from_polymarket_side, normalized_maker_order,
    normalized_trade_to_fills, normalized_trader_role, ws_trade_status,
};
use ws_trade::decimal_to_units_6dp;
pub use ws_trade::{decimal_to_f64, rest_trade_to_fills};
#[allow(unused_imports)]
pub(crate) use ws_trade::{ws_trade_observations, ws_trade_to_update, WsTradeUpdate};

sol! {
    interface ICtfExchangeV2Events {
        event OrderFilled(
            bytes32 indexed orderHash,
            address indexed maker,
            address indexed taker,
            uint8 side,
            uint256 tokenId,
            uint256 makerAmountFilled,
            uint256 takerAmountFilled,
            uint256 fee,
            bytes32 builder,
            bytes32 metadata
        );
        event OrdersMatched(
            bytes32 indexed takerOrderHash,
            address indexed takerOrderMaker,
            uint8 side,
            uint256 tokenId,
            uint256 makerAmountFilled,
            uint256 takerAmountFilled
        );
        event FeeCharged(address indexed receiver, uint256 amount);
    }
    interface ICollateralReceiptEvents {
        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

const FEE_PRECISION_SCALE: f64 = 100_000.0;
const MIN_NON_ZERO_FEE_USDC: f64 = 0.00001;

#[derive(Debug, Clone)]
struct NormalizedTrade {
    trade_id: String,
    transaction_hash: Option<String>,
    token: String,
    ts_ms: u64,
    taker_side: FillSide,
    taker_px: f64,
    taker_size: f64,
    taker_size_units_6dp: u128,
    taker_order_id: Option<String>,
    fee_market_state: FeeMarketState,
    owner_matches: bool,
    role: NormalizedTraderRole,
    maker_orders: Vec<NormalizedMakerOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NormalizedTraderRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy)]
enum FeeMarketState {
    Enabled { taker_base_fee_bps: f64 },
    Disabled,
    Unavailable,
}

#[derive(Debug, Clone)]
struct NormalizedMakerOrder {
    owner_matches: bool,
    order_id: String,
    token: String,
    side: Option<FillSide>,
    matched_amount: Decimal,
    price: Decimal,
}

#[derive(Debug, Clone)]
pub struct UnboundCtfTradeObservation {
    trade_id: String,
    market_id: MarketId,
    trade_asset_id: String,
    asset_id: String,
    owned_order_id: String,
    side: FillSide,
    taker_order_id: Option<String>,
    maker_order_ids: Vec<String>,
    status: CtfTradeStatus,
    price_units_6dp: u128,
    size_units_6dp: u128,
    matched_at_ms: u64,
    observed_at_ms: u64,
    owned_price_units_6dp: u128,
    owned_size_units_6dp: u128,
    owned_gross_proceeds_units_6dp: Option<u128>,
    owned_fee_units_6dp: Option<u128>,
}

#[derive(Debug, Clone)]
struct TradeObservationBase {
    trade_id: String,
    market_id: MarketId,
    trade_asset_id: String,
    taker_order_id: Option<String>,
    maker_order_ids: Vec<String>,
    status: CtfTradeStatus,
    price_units_6dp: u128,
    size_units_6dp: u128,
    matched_at_ms: u64,
    observed_at_ms: u64,
}

pub(crate) fn apply_finalized_v2_order_receipt(
    fill: &mut FillEvent,
    receipt: &TransactionReceipt,
    expected_chain_id: u64,
    expected_maker: Address,
) -> Result<()> {
    anyhow::ensure!(
        expected_chain_id > 0,
        "exact sell receipt chain identity is zero"
    );
    let is_buy = fill.side == FillSide::Buy;
    if is_buy {
        let authorization = fill
            .direct_buy_authorization
            .context("exact BUY receipt has no original authorization")?;
        authorization.validate()?;
        anyhow::ensure!(
            fill.token == authorization.selected_token().to_string()
                && expected_maker != Address::ZERO,
            "exact BUY receipt token or wallet differs from authorization"
        );
    }
    let fill_id = FillEvent::canonical_id(fill.trade_id.as_deref(), &fill.order_id)
        .context("exact sell receipt is missing vendor trade/order identity")?;
    let venue_trade_id = fill
        .trade_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("exact sell receipt is missing venue trade identity")?
        .to_string();
    let expected_order_hash = B256::from_str(fill.order_id.trim())
        .with_context(|| format!("sell order identity is not bytes32 fill_id={fill_id}"))?;
    let expected_transaction_hash = fill
        .transaction_hash
        .as_deref()
        .context("exact sell receipt is missing transaction identity")?
        .parse::<B256>()
        .with_context(|| format!("sell fill transaction identity is invalid fill_id={fill_id}"))?;
    anyhow::ensure!(
        expected_transaction_hash != B256::ZERO
            && receipt.transaction_hash == expected_transaction_hash,
        "sell fill transaction identity mismatch fill_id={fill_id}"
    );
    anyhow::ensure!(
        receipt.status(),
        "sell fill transaction reverted fill_id={fill_id}"
    );
    let receipt_block_number = receipt
        .block_number
        .context("finalized sell receipt is missing block number")?;
    let receipt_block_hash = receipt
        .block_hash
        .context("finalized sell receipt is missing block hash")?;
    let expected_token = U256::from_str(fill.token.trim())
        .with_context(|| format!("sell fill token identity is invalid fill_id={fill_id}"))?;
    let expected_qty = fill
        .receipt_qty_units_6dp
        .context("confirmed sell trade is missing exact quantity")?;
    anyhow::ensure!(
        expected_qty > 0,
        "confirmed sell trade exact quantity is zero fill_id={fill_id}"
    );

    let mut matching = Vec::new();
    for log in receipt.logs().iter().filter(|log| {
        !log.removed
            && (log.address() == contracts_v2::CTF_EXCHANGE_V2
                || log.address() == contracts_v2::NEG_RISK_CTF_EXCHANGE_V2)
            && log.topics().first().copied()
                == Some(ICtfExchangeV2Events::OrderFilled::SIGNATURE_HASH)
    }) {
        let decoded = log
            .log_decode_validate::<ICtfExchangeV2Events::OrderFilled>()
            .with_context(|| {
                format!("invalid V2 OrderFilled log transaction={expected_transaction_hash}")
            })?;
        if decoded.inner.data.orderHash == expected_order_hash {
            matching.push((log, decoded.inner.data));
        }
    }
    let [(log, event)] = matching.as_slice() else {
        anyhow::bail!(
            "expected exactly one finalized V2 sell OrderFilled log fill_id={fill_id} count={}",
            matching.len()
        );
    };
    anyhow::ensure!(
        event.maker == expected_maker
            && event.side == if is_buy { 0 } else { 1 }
            && event.tokenId == expected_token
            && (if is_buy {
                event.takerAmountFilled
            } else {
                event.makerAmountFilled
            }) == U256::from(expected_qty)
            && event.taker != Address::ZERO
            && (is_buy || event.fee <= event.takerAmountFilled),
        "finalized V2 OrderFilled identity or sell economics mismatch fill_id={fill_id}"
    );
    anyhow::ensure!(
        log.transaction_hash == Some(expected_transaction_hash)
            && log.block_number == Some(receipt_block_number)
            && log.block_hash == Some(receipt_block_hash)
            && log.log_index.is_some(),
        "finalized V2 OrderFilled log identity is incomplete fill_id={fill_id}"
    );
    let gross_units_u128 = u128::try_from(event.takerAmountFilled)
        .with_context(|| format!("sell gross proceeds overflow u128 fill_id={fill_id}"))?;
    let gross_units = i128::try_from(gross_units_u128)
        .with_context(|| format!("sell gross proceeds overflow i128 fill_id={fill_id}"))?;
    let fee_units = u128::try_from(event.fee)
        .with_context(|| format!("sell fee overflow u128 fill_id={fill_id}"))?;
    anyhow::ensure!(
        gross_units > 0,
        "sell gross proceeds are zero fill_id={fill_id}"
    );

    let buy_economics = if is_buy {
        let principal =
            u128::try_from(event.makerAmountFilled).context("BUY principal exceeds exact range")?;
        anyhow::ensure!(principal > 0, "BUY principal is zero");
        let refund = finalized_buy_refund_units_6dp(
            receipt,
            log.address(),
            log.log_index.context("BUY log index is absent")?,
            event,
        )?;
        let debit = principal
            .checked_add(fee_units)
            .and_then(|value| i128::try_from(value).ok())
            .context("BUY cash debit exceeds exact range")?;
        let delta = i128::try_from(refund)?
            .checked_sub(debit)
            .context("BUY cash delta overflow")?;
        Some((principal, refund, delta))
    } else {
        None
    };

    fill.receipt_identity = Some(ManagedSellReceiptIdentity::ExchangeV2 {
        venue_trade_id,
        order_hash: expected_order_hash.to_string(),
        chain_id: expected_chain_id,
        exchange: log.address().to_string(),
        transaction_hash: expected_transaction_hash.to_string(),
        block_number: receipt_block_number,
        block_hash: receipt_block_hash.to_string(),
        log_index: log
            .log_index
            .context("finalized V2 OrderFilled log index is missing")?,
    });
    fill.transaction_hash = Some(expected_transaction_hash.to_string());
    fill.receipt_qty_units_6dp = Some(expected_qty);
    if let Some((principal, refund, delta)) = buy_economics {
        fill.receipt_gross_proceeds_units_6dp = None;
        fill.receipt_principal_units_6dp = Some(principal);
        fill.receipt_approved_direct_cost_units_6dp = Some(0);
        fill.receipt_collateral_refund_units_6dp = Some(refund);
        fill.receipt_collateral_cash_delta_units_6dp = Some(delta);
        fill.receipt_wallet = Some(expected_maker.to_string());
    } else {
        fill.receipt_gross_proceeds_units_6dp = Some(gross_units);
    }
    fill.receipt_fee_units_6dp = Some(fee_units);
    fill.fee = Some(fee_units as f64 / 1_000_000.0);
    // The CLOB CONFIRMED status update owns this observation time. The
    // finalized transaction receipt itself contains no timestamp.
    fill.receipt_confirmed_at_ms = Some(fill.ts_ms);
    Ok(())
}

fn finalized_buy_refund_units_6dp(
    receipt: &TransactionReceipt,
    exchange: Address,
    target_index: u64,
    target: &ICtfExchangeV2Events::OrderFilled,
) -> Result<u128> {
    let mut previous_match = None;
    let mut closing_match = None;
    for log in receipt.logs().iter().filter(|log| {
        log.address() == exchange
            && log.topics().first().copied()
                == Some(ICtfExchangeV2Events::OrdersMatched::SIGNATURE_HASH)
    }) {
        let index = finalized_log_index(receipt, log)?;
        if index < target_index {
            previous_match = Some(previous_match.map_or(index, |old: u64| old.max(index)));
        } else if closing_match.as_ref().is_none_or(|(old, _)| index < *old) {
            let event = log.log_decode_validate::<ICtfExchangeV2Events::OrdersMatched>()?;
            closing_match = Some((index, event.inner.data));
        }
    }
    let (closing_index, matched) = closing_match.context("BUY match segment is incomplete")?;
    anyhow::ensure!(
        closing_index > target_index,
        "BUY match event ordering is invalid"
    );
    if matched.takerOrderHash != target.orderHash {
        return Ok(0);
    }
    anyhow::ensure!(
        matched.takerOrderMaker == target.maker
            && matched.side == 0
            && matched.tokenId == target.tokenId
            && matched.makerAmountFilled == target.makerAmountFilled
            && matched.takerAmountFilled == target.takerAmountFilled,
        "BUY taker match summary differs from its fill"
    );
    let mut last_maker_index = None;
    let mut same_side_maker = false;
    let mut wallet_is_fee_recipient = false;
    for log in receipt
        .logs()
        .iter()
        .filter(|log| log.address() == exchange)
    {
        let index = finalized_log_index(receipt, log)?;
        if !previous_match.is_none_or(|previous| index > previous) || index >= target_index {
            continue;
        }
        match log.topics().first().copied() {
            Some(topic) if topic == ICtfExchangeV2Events::OrderFilled::SIGNATURE_HASH => {
                let event = log.log_decode_validate::<ICtfExchangeV2Events::OrderFilled>()?;
                last_maker_index = Some(last_maker_index.map_or(index, |old: u64| old.max(index)));
                same_side_maker |= event.inner.data.side == 0;
            }
            Some(topic) if topic == ICtfExchangeV2Events::FeeCharged::SIGNATURE_HASH => {
                let event = log.log_decode_validate::<ICtfExchangeV2Events::FeeCharged>()?;
                wallet_is_fee_recipient |= event.inner.data.receiver == target.maker;
            }
            _ => {}
        }
    }
    let last_maker_index = last_maker_index.context("BUY match segment has no maker fill")?;
    if !same_side_maker {
        return Ok(0);
    }
    anyhow::ensure!(
        !wallet_is_fee_recipient,
        "BUY refund overlaps fee-recipient collateral"
    );
    let gross_pull = target
        .makerAmountFilled
        .checked_add(target.fee)
        .context("BUY gross collateral pull overflow")?;
    let mut pull_count = 0;
    let mut refund = None;
    for log in receipt.logs().iter().filter(|log| {
        log.address() == contracts_v2::PUSD_COLLATERAL
            && log.topics().first().copied()
                == Some(ICollateralReceiptEvents::Transfer::SIGNATURE_HASH)
    }) {
        let index = finalized_log_index(receipt, log)?;
        if !previous_match.is_none_or(|previous| index > previous) || index >= target_index {
            continue;
        }
        let event = log
            .log_decode_validate::<ICollateralReceiptEvents::Transfer>()?
            .inner
            .data;
        if event.from == target.maker && event.to == exchange && event.value == gross_pull {
            pull_count += 1;
        }
        if index > last_maker_index && event.from == exchange && event.to == target.maker {
            anyhow::ensure!(refund.is_none(), "BUY match segment has ambiguous refunds");
            refund = Some(u128::try_from(event.value)?);
        }
    }
    anyhow::ensure!(
        pull_count == 1,
        "BUY match segment has ambiguous gross collateral pull"
    );
    Ok(refund.unwrap_or(0))
}

fn finalized_log_index(receipt: &TransactionReceipt, log: &alloy::rpc::types::Log) -> Result<u64> {
    anyhow::ensure!(
        !log.removed
            && log.transaction_hash == Some(receipt.transaction_hash)
            && log.block_number == receipt.block_number
            && log.block_hash == receipt.block_hash,
        "finalized BUY match log provenance is incomplete"
    );
    log.log_index
        .context("finalized BUY match log index is absent")
}

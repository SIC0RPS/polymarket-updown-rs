use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::protocol::{CohortIdentityRecord, IntentRecord, SymbolId};
use crate::strategy::{InventoryIntent, InventoryOperation, StrategyAction};
use crate::strategy_runtime::market_catalog::{MarketCatalog, MarketCatalogRecord};
use crate::types::{OrderCloseReason, OrderIntent, OrderIntentFields, Side, TimeInForce};

#[path = "intent/split_context.rs"]
mod split_context;
pub(super) use split_context::{decision_context_record, record_to_journal};
#[path = "intent/tags.rs"]
mod tags;
use tags::*;

const ACTION_ORDER: u8 = 1;
const ACTION_INVENTORY: u8 = 2;
const ACTION_CANCEL_MARKET: u8 = 3;
const ACTION_CANCEL_QUOTES: u8 = 4;

const SIDE_NONE: u8 = 0;
const SIDE_BUY: u8 = 1;
const SIDE_SELL: u8 = 2;

const INVENTORY_SPLIT: u8 = 1;
const INVENTORY_MERGE: u8 = 2;

const TIF_NONE: u8 = 0;
const TIF_GTC: u8 = 1;
const TIF_GTD: u8 = 2;
const TIF_FOK: u8 = 3;
const TIF_FAK: u8 = 4;

const CLOSE_NONE: u8 = 0;
const CLOSE_PAIRED_FIRST_MANAGED_SELL: u8 = 1;
const CLOSE_PAIRED_POST_PROCEEDS_SELL: u8 = 2;

const FLAG_POST_ONLY: u16 = 1;
const PRICE_SCALE_8DP: f64 = 100_000_000.0;
const FIXED_PRECISION_TOLERANCE: f64 = 0.000_001;
const MAX_EXACT_F64_INTEGER: u128 = 9_007_199_254_740_992;

pub(crate) struct DecodedIntent {
    pub(crate) intent_id: u64,
    pub(crate) source_symbol: SymbolId,
    pub(crate) worker_epoch: u32,
    pub(crate) action: StrategyAction,
    pub(crate) entry_context: Option<DecodedEntryContext>,
}

pub(crate) enum DecodedEntryContext {
    Split(crate::engine_trade_journal::CtfSplitEntryJournal),
    DirectBuy(crate::types::DirectBuyAuthorization),
}

/// Converts one strategy-owned action to the fixed symbol-process record.
/// `intent_id` remains zero until `WorkerChannel` assigns its monotonic sequence.
pub(crate) fn strategy_action_to_intent_record(
    action: &StrategyAction,
    catalog: &MarketCatalog,
) -> Result<IntentRecord> {
    let record = match action {
        StrategyAction::Order(order) => order_record(order, catalog)?,
        StrategyAction::Inventory(intent) => inventory_record(intent, catalog)?,
        StrategyAction::CancelMarket(request) => {
            let market = resolve_market(catalog, request.market_id.as_str())?;
            ensure_gamma_market_id(request.gamma_market_id.as_deref(), market, "market cancel")?;
            IntentRecord {
                action_kind: ACTION_CANCEL_MARKET,
                market_id: market.market_handle,
                ..IntentRecord::default()
            }
        }
        StrategyAction::CancelQuotes(request) => {
            let market_id = request
                .market_id
                .as_ref()
                .context("quote cancel market identity is missing")?;
            let market = resolve_market(catalog, market_id.as_str())?;
            let token =
                canonical_quote_cancel_token(request.quote_group.as_str(), market_id.as_str())?;
            let token_handle = resolve_market_token(market, token, "quote cancel")?;
            IntentRecord {
                action_kind: ACTION_CANCEL_QUOTES,
                side_or_operation: side_tag(request.side),
                market_id: market.market_handle,
                token_or_up_id: token_handle,
                ..IntentRecord::default()
            }
        }
    };
    validate_unsequenced(record, catalog)
}

/// Restores the domain action accepted by the central live owners. Identity is
/// resolved only through the frozen catalog; the final encoder round trip makes
/// this the inverse of `strategy_action_to_intent_record`, not a second policy.
pub(crate) fn intent_record_to_strategy_action(
    record: IntentRecord,
    catalog: &MarketCatalog,
    source_symbol: SymbolId,
    worker_epoch: u32,
) -> Result<DecodedIntent> {
    anyhow::ensure!(worker_epoch > 0, "decoded intent worker epoch is zero");
    anyhow::ensure!(
        catalog.symbol() == source_symbol.as_str(),
        "decoded intent source symbol differs from frozen catalog"
    );
    let record = record.validate()?.validate_catalog(catalog)?;
    let market = catalog
        .record(record.market_id)
        .context("intent market handle is absent from frozen symbol catalog")?;
    let market_id = B256::from(market.condition_id).to_string();
    let action = match record.action_kind {
        ACTION_ORDER => {
            let token = token_for_handle(market, record.token_or_up_id, "order")?;
            let qty = qty_from_units_6dp(record.qty_or_amount_units_6dp)?;
            let limit_px = price_from_units_8dp(record.limit_px_units_8dp)?;
            let close_reason = (record.side_or_operation == SIDE_SELL)
                .then(|| close_reason_from_tag(record.close_reason))
                .transpose()?;
            let route = close_reason.map_or("oracle_buy_and_hold", |reason| reason.as_str());
            let instrument = U256::from_be_bytes(token).to_string();
            let lifecycle_cohort = record.cohort_id.optional_str()?.map(str::to_owned);
            let fields = OrderIntentFields {
                execution_intent_id: Some(record.intent_id),
                market_frame_received_at: None,
                market_state_published_at: None,
                market_connection_session_id: None,
                market_frame_sequence: None,
                market_state_revision: None,
                strategy_consumption_sequence: None,
                strategy_input_kind: None,
                decision_at: None,
                execution_evidence_revision: record.execution_evidence_revision,
                execution_instruction_qty_units_6dp: record.qty_or_amount_units_6dp,
                execution_instruction_limit_px_units_8dp: record.limit_px_units_8dp,
                instrument: instrument.clone(),
                market_id: Some(market_id.clone().into()),
                qty,
                limit_px,
                time_in_force: time_in_force_from_tag(record.time_in_force, record.expiration_ms)?,
                post_only: record.flags & FLAG_POST_ONLY != 0,
                quote_level: Some(record.quote_level),
                quote_group: Some(format!("{route}:{market_id}:{instrument}")),
                lifecycle_cohort,
                lifecycle_symbol: Some(catalog.symbol().to_string()),
            };
            StrategyAction::Order(match close_reason {
                Some(reason) => OrderIntent::sell(fields, reason),
                None => OrderIntent::buy(fields),
            })
        }
        ACTION_INVENTORY => StrategyAction::Inventory(InventoryIntent {
            operation: inventory_operation_from_tag(record.side_or_operation)?,
            cohort_id: record.cohort_id.optional_str()?.map(str::to_owned),
            market_id: market_id.clone().into(),
            gamma_market_id: Some(market.gamma_market_id.to_string()),
            condition_id: B256::from(market.condition_id),
            up_token: U256::from_be_bytes(market.up_token).to_string(),
            down_token: U256::from_be_bytes(market.down_token).to_string(),
            amount_units_6dp: record.qty_or_amount_units_6dp,
        }),
        ACTION_CANCEL_MARKET => {
            StrategyAction::CancelMarket(crate::strategy::CancelMarketRequest {
                market_id: market_id.clone().into(),
                gamma_market_id: Some(market.gamma_market_id.to_string()),
            })
        }
        ACTION_CANCEL_QUOTES => {
            let token = token_for_handle(market, record.token_or_up_id, "quote cancel")?;
            StrategyAction::CancelQuotes(crate::strategy::CancelQuotesRequest {
                market_id: Some(market_id.clone().into()),
                quote_group: format!("ctf_inventory:{market_id}:{}", U256::from_be_bytes(token)),
                side: side_from_tag(record.side_or_operation)?,
            })
        }
        _ => unreachable!("validated intent action kind"),
    };

    let mut encoded = strategy_action_to_intent_record(&action, catalog)?;
    encoded.intent_id = record.intent_id;
    anyhow::ensure!(
        encoded == record,
        "decoded intent does not round-trip through the fixed authority boundary"
    );
    Ok(DecodedIntent {
        intent_id: record.intent_id,
        source_symbol,
        worker_epoch,
        action,
        entry_context: None,
    })
}

fn order_record(order: &OrderIntent, catalog: &MarketCatalog) -> Result<IntentRecord> {
    let fields = order.common();
    let market_id = fields
        .market_id
        .as_ref()
        .context("order market identity is missing")?;
    let market = resolve_market(catalog, market_id.as_str())?;
    anyhow::ensure!(
        fields.lifecycle_symbol.as_deref() == Some(catalog.symbol()),
        "order lifecycle symbol is missing, noncanonical, or foreign"
    );
    anyhow::ensure!(fields.quote_level.is_some(), "order quote level is missing");
    let token = parse_token(fields.instrument.as_str(), "order token")?;
    let token_handle = resolve_market_token(market, token, "order")?;
    let qty_units_6dp = crate::ctf_accounting::qty_to_units_6dp(fields.qty)
        .context("order quantity is not exact positive 6dp")?;
    anyhow::ensure!(
        qty_units_6dp <= MAX_EXACT_F64_INTEGER,
        "order quantity exceeds exact f64 integer transport"
    );
    let limit_px_units_8dp = price_to_units_8dp(fields.limit_px)?;
    anyhow::ensure!(
        fields.execution_instruction_qty_units_6dp == qty_units_6dp
            && fields.execution_instruction_limit_px_units_8dp == limit_px_units_8dp,
        "managed sell execution instruction identity differs from order fields"
    );
    let (time_in_force, expiration_ms) = time_in_force_tag(&fields.time_in_force)?;
    anyhow::ensure!(
        !fields.post_only
            || matches!(
                fields.time_in_force,
                TimeInForce::Gtc | TimeInForce::Gtd { .. }
            ),
        "post-only order requires GTC or GTD"
    );

    let (side_or_operation, close_reason, cohort_id) = match order {
        OrderIntent::Buy(_) => {
            let expected_quote_group =
                format!("oracle_buy_and_hold:{market_id}:{}", fields.instrument);
            anyhow::ensure!(
                fields.lifecycle_cohort.is_none()
                    && fields.quote_group.as_deref() == Some(expected_quote_group.as_str()),
                "BUY entry carries noncanonical ownership fields"
            );
            (SIDE_BUY, CLOSE_NONE, CohortIdentityRecord::default())
        }
        OrderIntent::Sell(intent) => {
            let close_reason = close_reason_tag(intent.close_reason)?;
            let cohort = fields
                .lifecycle_cohort
                .as_deref()
                .context("managed sell lifecycle cohort is missing")?;
            let cohort_id = CohortIdentityRecord::required(cohort)?;
            let route = match intent.close_reason {
                OrderCloseReason::PairedFirstManagedSell => "paired_first_managed_sell",
                OrderCloseReason::PairedPostProceedsSell => "paired_post_proceeds_sell",
                OrderCloseReason::PredictivePostProceedsSell => unreachable!("rejected above"),
            };
            let expected_quote_group = format!("{route}:{market_id}:{}", fields.instrument);
            anyhow::ensure!(
                fields.quote_group.as_deref() == Some(expected_quote_group.as_str()),
                "managed sell quote group is missing or noncanonical"
            );
            (SIDE_SELL, close_reason, cohort_id)
        }
    };

    Ok(IntentRecord {
        action_kind: ACTION_ORDER,
        side_or_operation,
        time_in_force,
        close_reason,
        flags: u16::from(fields.post_only) * FLAG_POST_ONLY,
        quote_level: fields.quote_level.unwrap_or(0),
        market_id: market.market_handle,
        token_or_up_id: token_handle,
        cohort_id,
        qty_or_amount_units_6dp: qty_units_6dp,
        limit_px_units_8dp,
        execution_evidence_revision: fields.execution_evidence_revision,
        expiration_ms,
        ..IntentRecord::default()
    })
}

fn inventory_record(intent: &InventoryIntent, catalog: &MarketCatalog) -> Result<IntentRecord> {
    let market = resolve_market(catalog, intent.market_id.as_str())?;
    ensure_gamma_market_id(intent.gamma_market_id.as_deref(), market, "inventory")?;
    let condition = b256_bytes(intent.condition_id);
    anyhow::ensure!(
        condition == market.condition_id,
        "inventory condition identity is foreign to market"
    );
    let up_token = parse_token(intent.up_token.as_str(), "inventory UP token")?;
    let down_token = parse_token(intent.down_token.as_str(), "inventory DOWN token")?;
    anyhow::ensure!(
        up_token == market.up_token && down_token == market.down_token,
        "inventory token identity or orientation is foreign to market"
    );
    anyhow::ensure!(intent.amount_units_6dp > 0, "inventory amount is zero");
    let operation = match intent.operation {
        InventoryOperation::SplitCompleteSet => INVENTORY_SPLIT,
        InventoryOperation::MergeCompleteSet => INVENTORY_MERGE,
    };
    Ok(IntentRecord {
        action_kind: ACTION_INVENTORY,
        side_or_operation: operation,
        time_in_force: TIF_NONE,
        close_reason: CLOSE_NONE,
        market_id: market.market_handle,
        condition_id: market.market_handle,
        token_or_up_id: market.up_token_handle,
        down_token_id: market.down_token_handle,
        cohort_id: CohortIdentityRecord::optional(intent.cohort_id.as_deref())?,
        qty_or_amount_units_6dp: intent.amount_units_6dp,
        ..IntentRecord::default()
    })
}

fn resolve_market<'a>(
    catalog: &'a MarketCatalog,
    market_id: &str,
) -> Result<&'a MarketCatalogRecord> {
    let condition = parse_condition(market_id, "market")?;
    anyhow::ensure!(
        market_id == B256::from(condition).to_string(),
        "market identity is not canonical"
    );
    let handle = catalog
        .market_handle_for_condition(condition)
        .context("market identity is absent from frozen symbol catalog")?;
    catalog
        .record(handle)
        .context("market handle is absent from frozen symbol catalog")
}

fn resolve_market_token(market: &MarketCatalogRecord, token: [u8; 32], owner: &str) -> Result<u32> {
    if token == market.up_token {
        Ok(market.up_token_handle)
    } else if token == market.down_token {
        Ok(market.down_token_handle)
    } else {
        anyhow::bail!("{owner} token identity is foreign to market")
    }
}

fn ensure_gamma_market_id(
    actual: Option<&str>,
    market: &MarketCatalogRecord,
    owner: &str,
) -> Result<()> {
    let expected = market.gamma_market_id.to_string();
    anyhow::ensure!(
        actual == Some(expected.as_str()),
        "{owner} gamma market identity is missing, noncanonical, or foreign"
    );
    Ok(())
}

fn token_for_handle(
    market: &MarketCatalogRecord,
    token_handle: u32,
    owner: &str,
) -> Result<[u8; 32]> {
    if token_handle == market.up_token_handle {
        Ok(market.up_token)
    } else if token_handle == market.down_token_handle {
        Ok(market.down_token)
    } else {
        anyhow::bail!("{owner} token handle is foreign to market")
    }
}

fn canonical_quote_cancel_token(quote_group: &str, market_id: &str) -> Result<[u8; 32]> {
    let prefix = format!("ctf_inventory:{market_id}:");
    let raw_token = quote_group
        .strip_prefix(prefix.as_str())
        .context("quote cancel group is not canonical token-scoped CTF inventory")?;
    anyhow::ensure!(
        !raw_token.is_empty() && !raw_token.contains(':'),
        "quote cancel group token identity is missing or malformed"
    );
    let token = U256::from_str(raw_token).context("quote cancel token identity is invalid")?;
    anyhow::ensure!(
        token.to_string() == raw_token,
        "quote cancel token identity is not canonical decimal"
    );
    Ok(token.to_be_bytes::<32>())
}

fn parse_condition(raw: &str, owner: &str) -> Result<[u8; 32]> {
    let condition = B256::from_str(raw.trim())
        .with_context(|| format!("{owner} condition identity is invalid"))?;
    Ok(b256_bytes(condition))
}

fn parse_token(raw: &str, owner: &str) -> Result<[u8; 32]> {
    let token = U256::from_str(raw).with_context(|| format!("{owner} identity is invalid"))?;
    anyhow::ensure!(
        token.to_string() == raw,
        "{owner} identity is not canonical"
    );
    Ok(token.to_be_bytes::<32>())
}

fn b256_bytes(value: B256) -> [u8; 32] {
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(value.as_slice());
    bytes
}

fn validate_unsequenced(mut record: IntentRecord, catalog: &MarketCatalog) -> Result<IntentRecord> {
    record.intent_id = 1;
    record.validate()?.validate_catalog(catalog)?;
    record.intent_id = 0;
    Ok(record)
}

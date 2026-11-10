use anyhow::{Context, Result};
use polymarket_client_sdk::data::{
    types::{request::ActivityRequest, response::Activity, ActivityType},
    Client as DataClient,
};
use polymarket_client_sdk::types::{Address, Decimal};
use serde_json::{json, Value};

use polymarket_hft::polymarket::PUSD_COLLATERAL;
use polymarket_hft::trade_session_ledger::{
    append_session_receipt, run_start_ts_ms, MakerRebateReceipt, SessionReceipt,
    SessionReceiptAppendOutcome,
};

const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ACTIVITY_PAGE_SIZE: i32 = 500;

pub(super) async fn ingest_current_session_maker_rebate_receipts(
    http: &reqwest::Client,
    data: &DataClient,
    rpc_url: &str,
    wallet: Address,
) -> Result<()> {
    let run_id = polymarket_hft::event_ledger::current_run_id()
        .context("maker rebate ingestion requires an active run identity")?;
    let start_s = run_start_ts_ms(&run_id)
        .map(|ts_ms| ts_ms / 1000)
        .context("maker rebate ingestion requires a canonical live run identity")?;
    let mut request = ActivityRequest::builder()
        .user(wallet)
        .activity_types(vec![ActivityType::MakerRebate])
        .start(start_s)
        .limit(ACTIVITY_PAGE_SIZE)
        .context("invalid maker rebate activity page size")?
        .build();
    let mut first_item_error = None;
    loop {
        let page = data
            .activity(&request)
            .await
            .context("maker rebate activity request failed")?;
        let next_cursor = page
            .pagination
            .into_next_cursor()
            .context("maker rebate activity pagination is invalid")?;
        for activity in page.data {
            let receipt = match maker_rebate_receipt(
                http, rpc_url, wallet, &run_id, start_s, &activity,
            )
            .await
            {
                Ok(Some(receipt)) => receipt,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        target: "app.maker_rebate_receipts",
                        tx_hash = %activity.transaction_hash,
                        error = %error,
                        "maker_rebate_receipt_item_failed"
                    );
                    if first_item_error.is_none() {
                        first_item_error = Some(error);
                    }
                    continue;
                }
            };
            let log_index = receipt.log_index;
            anyhow::ensure!(
                append_session_receipt(SessionReceipt::MakerRebate(receipt))?
                    != SessionReceiptAppendOutcome::Conflict,
                "maker rebate canonical receipt conflict tx_hash={} log_index={}",
                activity.transaction_hash,
                log_index
            );
        }
        match next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => {
                tracing::debug!(
                    target: "app.maker_rebate_receipts",
                    run_id = %run_id,
                    receipt_item_error = first_item_error.is_some(),
                    "maker_rebate_activity_scan_completed"
                );
                return first_item_error.map_or(Ok(()), Err);
            }
        }
    }
}

async fn maker_rebate_receipt(
    http: &reqwest::Client,
    rpc_url: &str,
    wallet: Address,
    run_id: &str,
    start_s: u64,
    activity: &Activity,
) -> Result<Option<MakerRebateReceipt>> {
    anyhow::ensure!(
        activity.activity_type == ActivityType::MakerRebate,
        "maker rebate activity type mismatch actual={}",
        activity.activity_type
    );
    anyhow::ensure!(
        activity.proxy_wallet == wallet,
        "maker rebate activity wallet mismatch expected={} actual={}",
        wallet,
        activity.proxy_wallet
    );
    let amount_units_6dp = decimal_units_6dp(
        activity
            .usdc_size
            .context("maker rebate activity cash amount is unavailable")?,
    )?;
    let tx_hash = activity.transaction_hash.to_string();
    let timestamp_s =
        u64::try_from(activity.timestamp).context("maker rebate activity timestamp is invalid")?;
    anyhow::ensure!(
        timestamp_s >= start_s,
        "maker rebate activity predates current run start activity_ts={} run_start={}",
        timestamp_s,
        start_s
    );
    let timestamp_ms = timestamp_s
        .checked_mul(1000)
        .context("maker rebate activity timestamp overflows milliseconds")?;
    let Some((log_index, amount_units_6dp)) =
        confirmed_inbound_pusd_transfer(http, rpc_url, &tx_hash, wallet, amount_units_6dp).await?
    else {
        return Ok(None);
    };
    Ok(Some(MakerRebateReceipt {
        run_id: run_id.to_string(),
        ledger_seq: 0,
        ts_ms: timestamp_ms,
        tx_hash,
        log_index,
        token_contract: PUSD_COLLATERAL.to_string(),
        recipient: wallet.to_string(),
        amount_units_6dp,
        activity_amount_units_6dp: amount_units_6dp,
    }))
}

async fn confirmed_inbound_pusd_transfer(
    http: &reqwest::Client,
    rpc_url: &str,
    tx_hash: &str,
    wallet: Address,
    expected_amount_units_6dp: u64,
) -> Result<Option<(u64, u64)>> {
    let receipt = polymarket_hft::polygon_rpc::result_value(
        http,
        rpc_url,
        "eth_getTransactionReceipt",
        json!([tx_hash]),
    )
    .await
    .with_context(|| format!("Polygon RPC receipt lookup failed tx_hash={tx_hash}"))?;
    anyhow::ensure!(
        receipt
            .get("transactionHash")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(tx_hash)),
        "maker rebate receipt transaction identity mismatch expected={tx_hash}"
    );
    if receipt.get("status").and_then(Value::as_str) != Some("0x1") {
        return Ok(None);
    }
    let wallet_topic = format!("0x{:0>64}", wallet.to_string().trim_start_matches("0x"));
    let contract = PUSD_COLLATERAL.to_string();
    let mut matches = Vec::new();
    for log in receipt
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if log.get("removed").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let same_contract = log
            .get("address")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(&contract));
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        let topic_matches = topics.len() >= 3
            && topics[0]
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC))
            && topics[2]
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(&wallet_topic));
        let amount = log
            .get("data")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64);
        let log_index = log
            .get("logIndex")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64);
        if same_contract && topic_matches && amount == Some(expected_amount_units_6dp) {
            if let Some(log_index) = log_index {
                matches.push((log_index, expected_amount_units_6dp));
            }
        }
    }
    Ok((matches.len() == 1).then(|| matches[0]))
}

fn decimal_units_6dp(value: Decimal) -> Result<u64> {
    let normalized = value.normalize().to_string();
    let (whole, fractional) = normalized.split_once('.').unwrap_or((&normalized, ""));
    let whole = whole
        .parse::<u64>()
        .context("maker rebate whole amount is invalid")?;
    anyhow::ensure!(
        fractional.len() <= 6,
        "maker rebate exceeds 6 decimal places"
    );
    let fractional = format!("{fractional:0<6}")
        .parse::<u64>()
        .context("maker rebate fractional amount is invalid")?;
    let units = whole
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_add(fractional))
        .context("maker rebate amount overflows 6dp units")?;
    anyhow::ensure!(units > 0, "maker rebate amount is zero");
    Ok(units)
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

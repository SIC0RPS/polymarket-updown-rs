use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_client_sdk::clob::types::response::BalanceAllowanceResponse;
use polymarket_client_sdk::types::{Address, Decimal, U256};

use crate::execution::ExecutionClient;
use crate::polygon_rpc;
use crate::polymarket::contracts_v2;

pub(super) const RISK_RECON_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct WalletCashBalance {
    pub block_number: u64,
    pub block_hash: String,
    pub usdc_units_6dp: u128,
    pub pusd_units_6dp: u128,
    pub observed_at_ms: u64,
}

pub fn build_recon_http_client(context: &'static str) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(RISK_RECON_HTTP_TIMEOUT)
        .build()
        .context(context)
}

pub async fn fetch_wallet_cash_balances(
    http: &reqwest::Client,
    rpc_url: &str,
    address: Address,
) -> Result<WalletCashBalance> {
    let tracked_wallet = address.to_string();
    let block = polygon_rpc::finalized_block(http, rpc_url)
        .await
        .context("finalized Polygon block is unavailable")?;
    let usdc_contract = contracts_v2::USDC_E_COLLATERAL.to_string();
    let pusd_contract = contracts_v2::PUSD_COLLATERAL.to_string();
    let (usdc_units_6dp, pusd_units_6dp) = tokio::try_join!(
        fetch_onchain_erc20_balance_units(
            http,
            &usdc_contract,
            &tracked_wallet,
            rpc_url,
            block.number,
        ),
        fetch_onchain_erc20_balance_units(
            http,
            &pusd_contract,
            &tracked_wallet,
            rpc_url,
            block.number,
        )
    )
    .context("finalized Polygon wallet balance reads failed")?;
    Ok(WalletCashBalance {
        block_number: block.number,
        block_hash: block.hash,
        usdc_units_6dp,
        pusd_units_6dp,
        observed_at_ms: now_ms(),
    })
}

pub async fn fetch_clob_balance_allowance(exec: &ExecutionClient) -> Result<(f64, f64)> {
    let balance_allowance = exec
        .polymarket_balance_allowance()
        .await
        .context("current CLOB balance and allowance fetch failed")?;
    let (balance_units_6dp, allowance_units_6dp) = balance_allowance_units(balance_allowance)?;
    Ok((
        fixed_6dp_units_to_f64(balance_units_6dp)?,
        fixed_6dp_u256_units_to_f64(allowance_units_6dp)?,
    ))
}

fn balance_allowance_units(res: BalanceAllowanceResponse) -> Result<(u128, U256)> {
    let balance_units_6dp = integral_decimal_units(res.balance, "current CLOB collateral balance")?;
    let allowance_units = exchange_allowance_units(&res.allowances)
        .context("current CLOB response is missing a valid V2 exchange allowance")?;
    Ok((balance_units_6dp, allowance_units))
}

fn fixed_6dp_units_to_f64(units: u128) -> Result<f64> {
    Ok(crate::risk::RiskFixed6::from_units(units, "account collateral")?.as_f64())
}

fn fixed_6dp_u256_units_to_f64(units: U256) -> Result<f64> {
    let value = units
        .to_string()
        .parse::<f64>()
        .context("account allowance cannot be projected as f64")?
        / 1_000_000.0;
    anyhow::ensure!(
        value.is_finite() && value >= 0.0 && !value.is_sign_negative(),
        "account allowance projection is invalid"
    );
    Ok(value)
}

fn integral_decimal_units(value: Decimal, field: &str) -> Result<u128> {
    let normalized = value.normalize();
    anyhow::ensure!(
        !normalized.is_sign_negative() && normalized.scale() == 0,
        "{field} is not a nonnegative integer base-unit value: {normalized}"
    );
    u128::try_from(normalized.mantissa())
        .with_context(|| format!("{field} exceeds exact u128 range: {normalized}"))
}

fn exchange_allowance_units(allowances: &HashMap<Address, String>) -> Option<U256> {
    let values = v2_exchange_spenders()
        .iter()
        .map(|spender| {
            allowances
                .get(spender)
                .and_then(|raw| parse_allowance_units(raw))
        })
        .collect::<Option<Vec<_>>>()?;
    values.into_iter().min()
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn fetch_onchain_erc20_balance_units(
    http: &reqwest::Client,
    token: &str,
    wallet: &str,
    rpc_url: &str,
    block_number: u64,
) -> Result<u128> {
    let addr_clean = wallet.strip_prefix("0x").unwrap_or(wallet);
    let data = format!("0x70a08231{addr_clean:0>64}");
    let hex = polygon_rpc::eth_call_at_block(http, rpc_url, token, &data, block_number)
        .await
        .with_context(|| format!("ERC-20 balanceOf failed token={token} wallet={wallet}"))?
        .with_context(|| {
            format!("ERC-20 balanceOf returned no data token={token} wallet={wallet}")
        })?;
    let raw =
        u128::from_str_radix(hex.strip_prefix("0x").unwrap_or(&hex), 16).with_context(|| {
            format!("invalid ERC-20 balanceOf result token={token} wallet={wallet} hex={hex}")
        })?;
    Ok(raw)
}

fn v2_exchange_spenders() -> [Address; 2] {
    [
        contracts_v2::CTF_EXCHANGE_V2,
        contracts_v2::NEG_RISK_CTF_EXCHANGE_V2,
    ]
}

fn parse_allowance_units(raw: &str) -> Option<U256> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    raw.parse::<U256>().ok()
}

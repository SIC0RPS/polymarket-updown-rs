use std::{env, str::FromStr, time::Duration};

use alloy::primitives::U256;
use alloy::providers::ProviderBuilder;
use alloy::signers::Signer as _;
use alloy::sol;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use polymarket_client_sdk::types::Address;

use crate::config::AppConfig;
use crate::polymarket::contracts_v2;

const APPROVAL_SEND_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_RECEIPT_TIMEOUT: Duration = Duration::from_secs(120);

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 value) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IERC1155 {
        function setApprovalForAll(address operator, bool approved) external;
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

pub async fn run_approve(cfg: &AppConfig) -> Result<()> {
    let key_var = cfg.polymarket.private_key_env.trim();
    let private_key = env::var(key_var)
        .with_context(|| format!("missing private key env var `{key_var}` for approvals"))?;
    let signer = PrivateKeySigner::from_str(private_key.trim())
        .context("failed to parse Polymarket private key for approvals")?
        .with_chain_id(Some(cfg.polymarket.chain_id));

    let rpc_url = cfg.polymarket.resolved_account_rpc_url();
    let provider = ProviderBuilder::new()
        .wallet(signer.clone())
        .connect(&rpc_url)
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "failed to connect provider for approvals via {}",
                crate::polygon_rpc::endpoint_label(&rpc_url)
            )
        })?;

    let owner = signer.address();
    let pusd = IERC20::new(contracts_v2::PUSD_COLLATERAL, provider.clone());
    let ctf = IERC1155::new(contracts_v2::CONDITIONAL_TOKENS, provider);

    ensure_erc20_allowance(
        &pusd,
        owner,
        contracts_v2::CTF_EXCHANGE_V2,
        "pUSD",
        "CTF Exchange V2",
    )
    .await?;
    ensure_erc20_allowance(
        &pusd,
        owner,
        contracts_v2::NEG_RISK_CTF_EXCHANGE_V2,
        "pUSD",
        "Neg Risk CTF Exchange V2",
    )
    .await?;
    ensure_erc20_allowance(
        &pusd,
        owner,
        contracts_v2::CTF_COLLATERAL_ADAPTER,
        "pUSD",
        "CTF Collateral Adapter",
    )
    .await?;
    ensure_erc20_allowance(
        &pusd,
        owner,
        contracts_v2::NEG_RISK_CTF_COLLATERAL_ADAPTER,
        "pUSD",
        "Neg Risk CTF Collateral Adapter",
    )
    .await?;
    ensure_erc1155_operator(
        &ctf,
        owner,
        contracts_v2::CTF_EXCHANGE_V2,
        "CTF Exchange V2",
    )
    .await?;
    ensure_erc1155_operator(
        &ctf,
        owner,
        contracts_v2::NEG_RISK_CTF_EXCHANGE_V2,
        "Neg Risk CTF Exchange V2",
    )
    .await?;
    ensure_erc1155_operator(
        &ctf,
        owner,
        contracts_v2::CTF_COLLATERAL_ADAPTER,
        "CTF Collateral Adapter",
    )
    .await?;
    ensure_erc1155_operator(
        &ctf,
        owner,
        contracts_v2::NEG_RISK_CTF_COLLATERAL_ADAPTER,
        "Neg Risk CTF Collateral Adapter",
    )
    .await?;

    Ok(())
}

async fn ensure_erc20_allowance<P: alloy::providers::Provider>(
    token: &IERC20::IERC20Instance<P>,
    owner: Address,
    spender: Address,
    token_label: &str,
    label: &str,
) -> Result<()> {
    let allowance = token
        .allowance(owner, spender)
        .call()
        .await
        .with_context(|| format!("failed to query {token_label} allowance for {label}"))?;
    if allowance >= U256::MAX / U256::from(2u8) {
        tracing::info!(owner = %owner, spender = %spender, token = token_label, label, "approval already set");
        return Ok(());
    }

    let approve_call = token.approve(spender, U256::MAX);
    let pending = tokio::time::timeout(APPROVAL_SEND_TIMEOUT, approve_call.send())
        .await
        .with_context(|| format!("{token_label} approve send timed out for {label}"))?
        .with_context(|| format!("failed to send {token_label} approve for {label}"))?;
    let tx_hash = pending.tx_hash().to_string();
    let _ = tokio::time::timeout(APPROVAL_RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .with_context(|| format!("{token_label} approve receipt timed out for {label}"))?
        .with_context(|| format!("failed to confirm {token_label} approve for {label}"))?;
    tracing::info!(owner = %owner, spender = %spender, token = token_label, label, tx_hash = %tx_hash, "approval confirmed");
    Ok(())
}

async fn ensure_erc1155_operator<P: alloy::providers::Provider>(
    ctf: &IERC1155::IERC1155Instance<P>,
    owner: Address,
    operator: Address,
    label: &str,
) -> Result<()> {
    let approved = ctf
        .isApprovedForAll(owner, operator)
        .call()
        .await
        .with_context(|| format!("failed to query CTF approval for {label}"))?;
    if approved {
        tracing::info!(owner = %owner, operator = %operator, label, "approval already set");
        return Ok(());
    }

    let approval_call = ctf.setApprovalForAll(operator, true);
    let pending = tokio::time::timeout(APPROVAL_SEND_TIMEOUT, approval_call.send())
        .await
        .with_context(|| format!("CTF setApprovalForAll send timed out for {label}"))?
        .with_context(|| format!("failed to send CTF setApprovalForAll for {label}"))?;
    let tx_hash = pending.tx_hash().to_string();
    let _ = tokio::time::timeout(APPROVAL_RECEIPT_TIMEOUT, pending.get_receipt())
        .await
        .with_context(|| format!("CTF setApprovalForAll receipt timed out for {label}"))?
        .with_context(|| format!("failed to confirm CTF setApprovalForAll for {label}"))?;
    tracing::info!(owner = %owner, operator = %operator, label, tx_hash = %tx_hash, "approval confirmed");
    Ok(())
}

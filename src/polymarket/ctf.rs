use std::collections::HashMap;
use std::str::FromStr;

use alloy::primitives::{Address, U256};
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result};

use crate::polygon_rpc;

pub const POLYGON_CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

sol! {
    #[sol(rpc)]
    interface IERC1155BalanceReader {
        function balanceOfBatch(address[] calldata accounts, uint256[] calldata ids)
            external
            view
            returns (uint256[] memory);
    }
}

pub async fn fetch_ctf_position_balances(
    http: &reqwest::Client,
    rpc_url: &str,
    wallet: &str,
    asset_ids: &[String],
) -> Result<HashMap<String, u128>> {
    let wallet =
        Address::from_str(wallet).context("invalid wallet address for ctf balance check")?;
    let mut parsed_assets: Vec<(String, U256)> = Vec::new();
    for asset in asset_ids {
        let asset = asset.trim();
        if asset.is_empty() {
            continue;
        }
        let token_id = U256::from_str(asset)
            .with_context(|| format!("invalid ctf asset id `{asset}` from live positions"))?;
        parsed_assets.push((asset.to_string(), token_id));
    }
    if parsed_assets.is_empty() {
        return Ok(HashMap::new());
    }

    let accounts: Vec<Address> = vec![wallet; parsed_assets.len()];
    let ids: Vec<U256> = parsed_assets.iter().map(|(_, id)| *id).collect();
    let call = IERC1155BalanceReader::balanceOfBatchCall { accounts, ids };
    let calldata = encode_hex(&SolCall::abi_encode(&call));

    let balance_call = polygon_rpc::eth_call(http, rpc_url, POLYGON_CTF_CONTRACT, &calldata)
        .await
        .context("ctf balanceOfBatch request failed")?;
    let hex = balance_call
        .as_deref()
        .context("ctf balanceOfBatch missing result field")?;
    let balances = decode_uint256_array_abi(hex)
        .with_context(|| format!("ctf balanceOfBatch decode failed result={hex}"))?;
    anyhow::ensure!(
        balances.len() == parsed_assets.len(),
        "ctf balanceOfBatch length mismatch expected={} got={}",
        parsed_assets.len(),
        balances.len()
    );

    let out = parsed_assets
        .into_iter()
        .zip(balances.into_iter())
        .map(|((asset, _), raw)| -> Result<(String, u128)> {
            let raw_units = raw
                .to_string()
                .parse::<u128>()
                .with_context(|| format!("ctf balance units overflow asset={asset}"))?;
            Ok((asset, raw_units))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    Ok(out)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_uint256_array_abi(hex: &str) -> Result<Vec<U256>> {
    let clean = hex.strip_prefix("0x").unwrap_or(hex);
    anyhow::ensure!(!clean.is_empty(), "empty abi payload");
    anyhow::ensure!(clean.len() % 64 == 0, "invalid abi payload length");

    let word = |idx: usize| -> Result<&str> {
        let start = idx * 64;
        let end = start + 64;
        clean
            .get(start..end)
            .context("abi word index out of bounds")
    };

    let offset_bytes = usize::from_str_radix(&word(0)?[48..], 16).context("invalid abi offset")?;
    anyhow::ensure!(offset_bytes % 32 == 0, "abi offset not word aligned");
    let len_index = offset_bytes / 32;
    let len =
        usize::from_str_radix(&word(len_index)?[48..], 16).context("invalid abi array len")?;

    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let raw = word(len_index + 1 + i)?;
        out.push(U256::from_str_radix(raw, 16).context("invalid abi uint256 word")?);
    }
    Ok(out)
}

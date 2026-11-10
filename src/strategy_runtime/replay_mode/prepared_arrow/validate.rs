use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use super::PreparedReplayData;

impl PreparedReplayData {
    pub(in crate::strategy_runtime::replay_mode) fn validate_symbol_scope(
        &self,
        expected_symbol: &str,
    ) -> Result<()> {
        let mut tokens = HashSet::new();
        for row in &self.markets {
            anyhow::ensure!(
                row.market
                    .underlying
                    .trim()
                    .eq_ignore_ascii_case(expected_symbol),
                "prepared market row has foreign symbol expected={} actual={}",
                expected_symbol,
                row.market.underlying
            );
            crate::oracle_resolution::chainlink_resolution_feed(
                &row.market.underlying,
                row.market.resolution_source.as_deref().unwrap_or("missing"),
            )?;
            tokens.insert(row.market.up_token);
            tokens.insert(row.market.down_token);
        }
        anyhow::ensure!(
            !tokens.is_empty(),
            "prepared replay has no market token identity symbol={expected_symbol}"
        );
        self.books.validate_tokens(&tokens)?;
        for row in &self.lifecycles {
            if row.kind == "token" {
                let token =
                    U256::from_str(&row.token_id).context("invalid lifecycle_delta token_id")?;
                anyhow::ensure!(
                    tokens.contains(&token),
                    "lifecycle_delta references foreign token {token}"
                );
            }
        }
        for row in &self.last_trades {
            anyhow::ensure!(
                tokens.contains(&row.token_id),
                "last_trade_delta references foreign token {}",
                row.token_id
            );
        }
        for row in &self.resolutions {
            row.validate(&self.markets, &tokens)?;
        }
        Ok(())
    }
}

use std::str::FromStr;

use alloy::hex;
use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::central_receipts::apply_simulated_resolution_redemption;
use super::{
    converge_central_consequences, send_admitted_views, CandidateCentralState,
    CandidateRunnerInputs, CandidateSlot,
};
use crate::polymarket::state::PolymarketOddsState;
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::replay_mode::candidate_runner::CandidateExternalInput;
use crate::strategy_runtime::replay_mode::prepared_arrow::ResolutionRow;
use crate::strategy_runtime::symbol_process::{EventRef, SymbolId};

pub(super) async fn apply_provider_resolution(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    symbol: SymbolId,
    event: EventRef,
    input: &CandidateExternalInput,
) -> Result<()> {
    let CandidateExternalInput::ProviderResolution {
        resolution,
        changed,
        odds,
        projection,
    } = input
    else {
        anyhow::bail!("central resolution event has foreign prepared input");
    };
    anyhow::ensure!(
        event.kind
            == crate::strategy_runtime::replay_mode::prepared_event_stream::EVENT_KIND_PROVIDER_RESOLUTION,
        "prepared provider resolution carries invalid event kind={} ",
        event.kind
    );
    if *changed {
        let odds = odds
            .as_ref()
            .context("changed provider resolution has no prepared odds snapshot")?;
        let projection = projection
            .as_ref()
            .context("changed provider resolution has no prepared market projection")?;
        central.install_market_state(odds.clone(), Some(projection))?;
        if let Some(redeemed_symbol) =
            apply_simulated_resolution_redemption(central, resolution, event.timestamp_ms)?
        {
            anyhow::ensure!(
                redeemed_symbol == symbol,
                "captured provider resolution redeemed a foreign symbol"
            );
            central.portfolio_tx.send_replace(central.portfolio.clone());
        }
        let intents = send_admitted_views(slot, central, event.timestamp_ms, [symbol])?;
        converge_central_consequences(slot, inputs, central, event.timestamp_ms, intents).await?;
    }
    Ok(())
}

pub(in crate::strategy_runtime::replay_mode::candidate_runner) fn prepare_provider_resolution_fact(
    odds: &mut PolymarketOddsState,
    catalog: &MarketCatalog,
    resolution: &ResolutionRow,
    applied_at_ms: u64,
) -> Result<bool> {
    anyhow::ensure!(
        resolution.observed_at_ms <= applied_at_ms,
        "captured provider resolution is applied before observation"
    );
    let condition = B256::from_str(&resolution.condition_id)
        .context("captured provider resolution condition is invalid")?;
    let winner = U256::from_str(&resolution.winning_asset_id)
        .context("captured provider resolution winner is invalid")?;
    let market_handle = catalog
        .market_handle_for_condition(condition.into())
        .context("captured provider resolution condition is foreign to symbol catalog")?;
    let market = catalog
        .record(market_handle)
        .context("captured provider resolution market handle is unavailable")?;
    let expected_up = U256::from_be_bytes(market.up_token);
    let expected_down = U256::from_be_bytes(market.down_token);
    anyhow::ensure!(
        winner == expected_up || winner == expected_down,
        "captured provider winner is foreign to market token pair"
    );
    anyhow::ensure!(
        market.window_end_ms <= resolution.observed_at_ms,
        "captured provider resolution predates market close"
    );
    validate_provider_context(resolution, expected_up, expected_down)?;

    let digest = hex::decode(&resolution.raw_response_sha256)
        .context("captured provider resolution digest is invalid")?;
    let digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| anyhow::anyhow!("captured provider resolution digest length is invalid"))?;
    let changed = odds.retain_market_resolution(
        condition,
        expected_up,
        expected_down,
        winner,
        resolution.observed_at_ms,
        digest,
    )?;
    if changed {
        odds.revision = odds
            .revision
            .checked_add(1)
            .context("candidate odds revision overflow")?;
    }
    Ok(changed)
}

fn validate_provider_context(
    resolution: &ResolutionRow,
    expected_up: U256,
    expected_down: U256,
) -> Result<()> {
    let actual_up = U256::from_str(&resolution.up_token)?;
    let actual_down = U256::from_str(&resolution.down_token)?;
    anyhow::ensure!(
        actual_up == expected_up && actual_down == expected_down,
        "captured provider resolution pair is foreign"
    );
    Ok(())
}

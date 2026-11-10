use std::collections::HashSet;

use crate::ctf_inventory::CtfInventoryRuntime;
use crate::execution::live_ops::RedeemClaimSnapshot;
use crate::execution::ConditionPayoutVector;
use crate::polymarket::identity::MarketId;
use crate::portfolio::{PortfolioState, RedeemCloseRequest};
use anyhow::{Context, Result};
use polymarket_client_sdk::data::types::response::Position as VendorPosition;
use polymarket_client_sdk::types::U256;

use super::*;

fn portfolio_qty_units_6dp(portfolio: &PortfolioState, token: &str) -> u128 {
    portfolio
        .position(token)
        .map(|position| position.qty_units_6dp)
        .unwrap_or(0)
}

fn position_payout_index(position: &VendorPosition) -> Option<usize> {
    usize::try_from(position.outcome_index).ok()
}

fn validate_redeem_payout_index(
    position: &VendorPosition,
    payout: &ConditionPayoutVector,
) -> Result<u8> {
    let outcome_index = position_payout_index(position)
        .with_context(|| format!("invalid outcome index {}", position.outcome_index))?;
    anyhow::ensure!(
        position.condition_id == payout.condition_id,
        "redeem candidate payout condition mismatch vendor_condition_id={} payout_condition_id={}",
        position.condition_id,
        payout.condition_id
    );
    anyhow::ensure!(
        matches!(outcome_index, 0 | 1),
        "redeem outcome index is outside exact binary payout condition_id={} outcome_index={outcome_index}",
        payout.condition_id
    );
    u8::try_from(outcome_index).context("redeem outcome index exceeds terminal binary range")
}

pub(super) async fn prepare_redeem_for_condition(
    exec: &ExecutionClient,
    condition_id: B256,
    redeemable_positions: &[VendorPosition],
    payout: &ConditionPayoutVector,
    portfolio: &PortfolioState,
) -> Result<(Vec<RedeemClaimSnapshot>, Vec<RedeemCloseRequest>)> {
    let mut positions = redeemable_positions
        .iter()
        .filter(|position| position.condition_id == condition_id);
    let Some(first) = positions.next() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let first_outcome_index = validate_redeem_payout_index(first, payout)?;
    anyhow::ensure!(
        first.asset != first.opposite_asset,
        "redeem binary candidate assets are identical condition_id={condition_id}"
    );
    let assets = if first_outcome_index == 0 {
        [first.asset, first.opposite_asset]
    } else {
        [first.opposite_asset, first.asset]
    };
    let mut seen_assets = HashSet::new();
    for position in std::iter::once(first).chain(positions) {
        anyhow::ensure!(
            position.redeemable,
            "redeem candidate is not vendor-redeemable condition_id={condition_id} token={}",
            position.asset
        );
        let outcome_index = usize::from(validate_redeem_payout_index(position, payout)?);
        anyhow::ensure!(
            seen_assets.insert(position.asset),
            "duplicate redeem candidate asset condition_id={condition_id} asset={}",
            position.asset
        );
        anyhow::ensure!(
            position.asset == assets[outcome_index]
                && position.opposite_asset == assets[1 - outcome_index],
            "redeem candidate binary token identity mismatch condition_id={condition_id} token={}",
            position.asset
        );
    }
    let payout_numerators = [
        payout.outcome_slot_0_numerator,
        payout.outcome_slot_1_numerator,
    ];
    anyhow::ensure!(
        payout.denominator > 0
            && payout_numerators
                .iter()
                .all(|numerator| *numerator == 0 || *numerator == payout.denominator),
        "redeem candidate payout is not exact binary condition_id={condition_id}"
    );
    let tokens = assets.map(|asset| asset.to_string());
    let condition_id_text = condition_id.to_string();
    let current_condition_tokens = portfolio
        .positions
        .iter()
        .filter(|(token, position)| {
            position.qty_units_6dp > 0
                && portfolio
                    .token_market_hint
                    .get(*token)
                    .is_some_and(|market_id| market_id.as_str() == condition_id_text)
        })
        .map(|(token, _)| token.as_str())
        .collect::<HashSet<_>>();
    let has_current_session_inventory = !current_condition_tokens.is_empty()
        || tokens
            .iter()
            .any(|token| portfolio_qty_units_6dp(portfolio, token) > 0);
    if !has_current_session_inventory {
        return Ok((Vec::new(), Vec::new()));
    }
    anyhow::ensure!(
        current_condition_tokens
            .iter()
            .all(|token| tokens.iter().any(|candidate| candidate.as_str() == *token)),
        "redeem current-session condition inventory is absent from vendor candidates condition_id={condition_id}"
    );
    let balances = exec.ctf_position_balances(&tokens).await?;
    let mut claims = Vec::with_capacity(2);
    let mut requests = Vec::with_capacity(2);
    for (outcome_index, token) in [0_u8, 1].into_iter().zip(tokens.iter()) {
        let quantity_units_6dp = portfolio_qty_units_6dp(portfolio, token);
        let chain_units_6dp = *balances.get(token).with_context(|| {
            format!("redeem native balance is absent condition_id={condition_id} token={token}")
        })?;
        anyhow::ensure!(
            chain_units_6dp == quantity_units_6dp,
            "redeem mixed current-session/native exact quantity mismatch condition_id={condition_id} token={token} chain_units={chain_units_6dp} portfolio_units={quantity_units_6dp}"
        );
        if quantity_units_6dp == 0 {
            continue;
        }
        let payout_numerator = payout_numerators[usize::from(outcome_index)];
        let claim = RedeemClaimSnapshot {
            condition_id: MarketId::new(condition_id_text.clone()),
            token: token.clone(),
            title: first.title.clone(),
            outcome: if outcome_index == first_outcome_index {
                first.outcome.clone()
            } else {
                first.opposite_outcome.clone()
            },
            outcome_index,
            quantity_units_6dp,
            payout_numerator,
            payout_denominator: payout.denominator,
            claim_value_units_6dp: if payout_numerator == payout.denominator {
                quantity_units_6dp
            } else {
                0
            },
        };
        requests.push(RedeemCloseRequest {
            market_id: claim.condition_id.clone(),
            token: claim.token.clone(),
            outcome_index: Some(claim.outcome_index),
            qty_units_6dp: claim.quantity_units_6dp,
            close_reason: "redeem".to_string(),
            closed_at_ms: 0,
            exit_tx_hash: None,
        });
        claims.push(claim);
    }
    Ok((claims, requests))
}

pub(super) fn terminal_condition_ids(
    mergeable_positions: &[VendorPosition],
    redeemable_positions: &[VendorPosition],
) -> Vec<B256> {
    let mut condition_ids = mergeable_positions
        .iter()
        .filter(|position| !position.size.is_zero())
        .chain(redeemable_positions.iter())
        .map(|position| position.condition_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    condition_ids.sort_unstable();
    condition_ids
}

fn dedup_positions_for_condition(
    positions: &[VendorPosition],
    condition_id: B256,
) -> Vec<VendorPosition> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for position in positions {
        if position.condition_id != condition_id || position.size.is_zero() {
            continue;
        }
        let token = position.asset.to_string();
        if seen.insert(token) {
            deduped.push(position.clone());
        }
    }
    deduped
}

fn ensure_unique_nonzero_assets(
    positions: &[VendorPosition],
    condition_id: B256,
    operation: &str,
) -> Result<()> {
    let mut seen_assets = HashSet::new();
    for position in positions
        .iter()
        .filter(|position| position.condition_id == condition_id && !position.size.is_zero())
    {
        anyhow::ensure!(
            seen_assets.insert(position.asset),
            "duplicate {operation} candidate asset condition_id={condition_id} asset={}",
            position.asset
        );
    }
    Ok(())
}

fn merge_candidates_for_condition(
    condition_id: B256,
    mergeable_positions: &[VendorPosition],
    portfolio: &PortfolioState,
) -> Result<Vec<(VendorPosition, u128)>> {
    ensure_unique_nonzero_assets(mergeable_positions, condition_id, "merge")?;
    let candidates = dedup_positions_for_condition(mergeable_positions, condition_id)
        .into_iter()
        .map(|position| {
            let token = position.asset.to_string();
            let local_qty_units_6dp = portfolio_qty_units_6dp(portfolio, &token);
            let vendor_qty_units_6dp = crate::strategy::full_set_qty_to_units_6dp(position.size)
                .with_context(|| {
                    format!(
                        "merge candidate quantity is not positive exact fixed-6dp condition_id={condition_id} token={token}"
                    )
                })?;
            anyhow::ensure!(
                vendor_qty_units_6dp == local_qty_units_6dp,
                "merge vendor/portfolio exact quantity mismatch condition_id={condition_id} token={token} vendor_units={vendor_qty_units_6dp} portfolio_units={local_qty_units_6dp}"
            );
            Ok((position, vendor_qty_units_6dp))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut outcome_indices = candidates
        .iter()
        .filter_map(|(position, _)| position_payout_index(position))
        .collect::<Vec<_>>();
    outcome_indices.sort_unstable();
    if candidates.is_empty() {
        return Ok(candidates);
    }
    anyhow::ensure!(
        candidates.len() == 2 && outcome_indices == [0, 1],
        "merge candidates are not one complete binary outcome set condition_id={condition_id} candidate_count={} outcome_indices={outcome_indices:?}",
        candidates.len()
    );
    Ok(candidates)
}

pub(super) fn candidate_positions_map_to_payout(
    condition_id: B256,
    mergeable_positions: &[VendorPosition],
    redeemable_positions: &[VendorPosition],
    _portfolio: &PortfolioState,
    payout: &ConditionPayoutVector,
) -> bool {
    payout.condition_id == condition_id
        && mergeable_positions
            .iter()
            .filter(|position| !position.size.is_zero())
            .chain(redeemable_positions.iter())
            .filter(|position| position.condition_id == condition_id)
            .all(|position| {
                position_payout_index(position).is_some_and(|index| matches!(index, 0 | 1))
            })
}
fn validate_merge_command_candidates(
    command: &CtfTerminalMergeCommand,
    candidates: &[(VendorPosition, u128)],
) -> Result<()> {
    command.validate()?;
    let candidate_tokens = candidates
        .iter()
        .map(|(position, _)| position.asset.to_string())
        .collect::<HashSet<_>>();
    let command_tokens = [
        command.intent.up_token.clone(),
        command.intent.down_token.clone(),
    ]
    .into_iter()
    .collect::<HashSet<_>>();
    anyhow::ensure!(
        candidate_tokens == command_tokens,
        "merge command tokens differ from exact vendor binary assets condition_id={}",
        command.intent.condition_id
    );
    Ok(())
}

pub(super) fn merge_amount_units_6dp(
    command: &CtfTerminalMergeCommand,
    mergeable_positions: &[VendorPosition],
    portfolio: &PortfolioState,
    ctf_inventory: &CtfInventoryRuntime,
) -> Result<Option<U256>> {
    command.validate()?;
    let condition_id = command.intent.condition_id;
    let candidates = merge_candidates_for_condition(condition_id, mergeable_positions, portfolio)?;
    if candidates.is_empty() {
        return Ok(None);
    }
    validate_merge_command_candidates(command, &candidates)?;
    let merge_qty_units_6dp = candidates
        .iter()
        .map(|(_, qty_units_6dp)| *qty_units_6dp)
        .min()
        .expect("two positive merge candidates checked");
    let cohort = command
        .intent
        .cohort_id
        .as_deref()
        .context("terminal merge command cohort identity is missing")?;
    let (
        split_market_id,
        split_condition_id,
        split_up_token,
        split_down_token,
        split_amount_units_6dp,
    ) = ctf_inventory.confirmed_split_identity_for_cohort(cohort)?;
    anyhow::ensure!(
        split_market_id == command.intent.market_id
            && split_condition_id == command.intent.condition_id
            && split_up_token == command.intent.up_token
            && split_down_token == command.intent.down_token,
        "merge command identity does not match current-session confirmed split condition_id={condition_id} cohort={cohort}"
    );
    let cohort_up_units_6dp =
        portfolio.confirmed_ctf_cohort_qty_units_6dp(&split_up_token, cohort)?;
    let cohort_down_units_6dp =
        portfolio.confirmed_ctf_cohort_qty_units_6dp(&split_down_token, cohort)?;
    anyhow::ensure!(
        portfolio_qty_units_6dp(portfolio, &split_up_token) == cohort_up_units_6dp
            && portfolio_qty_units_6dp(portfolio, &split_down_token) == cohort_down_units_6dp,
        "condition-global merge portfolio quantities are not owned by one current-session cohort condition_id={condition_id} cohort={cohort}"
    );
    anyhow::ensure!(
        merge_qty_units_6dp == command.intent.amount_units_6dp
            && merge_qty_units_6dp <= cohort_up_units_6dp
            && merge_qty_units_6dp <= cohort_down_units_6dp
            && cohort_up_units_6dp <= split_amount_units_6dp
            && cohort_down_units_6dp <= split_amount_units_6dp,
        "merge command amount does not equal exact executable current-session quantity condition_id={condition_id} cohort={cohort} command_units={} executable_units={merge_qty_units_6dp}",
        command.intent.amount_units_6dp
    );
    anyhow::ensure!(
        candidates.iter().all(|(_, qty_units_6dp)| {
            *qty_units_6dp >= command.intent.amount_units_6dp
        }),
        "merge command amount exceeds exact vendor/portfolio quantity condition_id={condition_id} cohort={cohort}"
    );
    Ok(Some(U256::from(merge_qty_units_6dp)))
}

fn close_request_from_position(
    position: &VendorPosition,
    outcome_index: Option<u8>,
    qty_units_6dp: u128,
    close_reason: &str,
    closed_at_ms: u64,
    tx_hash: Option<&str>,
) -> RedeemCloseRequest {
    RedeemCloseRequest {
        market_id: MarketId::new(position.condition_id.to_string()),
        token: position.asset.to_string(),
        outcome_index,
        qty_units_6dp,
        close_reason: close_reason.to_string(),
        closed_at_ms,
        exit_tx_hash: tx_hash.map(ToString::to_string),
    }
}

pub(super) fn attach_close_confirmation(
    requests: &mut [RedeemCloseRequest],
    closed_at_ms: u64,
    tx_hash: &str,
) {
    for request in requests {
        request.closed_at_ms = closed_at_ms;
        request.exit_tx_hash = Some(tx_hash.to_string());
    }
}

pub(super) fn merge_close_requests_for_command(
    command: &CtfTerminalMergeCommand,
    mergeable_positions: &[VendorPosition],
    portfolio: &PortfolioState,
    ctf_inventory: &CtfInventoryRuntime,
    closed_at_ms: u64,
    tx_hash: Option<&str>,
) -> Result<Vec<RedeemCloseRequest>> {
    let Some(merge_amount) =
        merge_amount_units_6dp(command, mergeable_positions, portfolio, ctf_inventory)?
    else {
        return Ok(Vec::new());
    };
    let merge_qty_units_6dp = merge_amount.to::<u128>();
    let condition_id = command.intent.condition_id;

    let candidates = merge_candidates_for_condition(condition_id, mergeable_positions, portfolio)?;
    validate_merge_command_candidates(command, &candidates)?;
    candidates
        .into_iter()
        .map(|(position, _)| {
            Ok(close_request_from_position(
                &position,
                None,
                merge_qty_units_6dp,
                "merge",
                closed_at_ms,
                tx_hash,
            ))
        })
        .collect()
}

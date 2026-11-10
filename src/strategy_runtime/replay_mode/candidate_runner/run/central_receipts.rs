use std::collections::BTreeMap;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::{
    converge_central_consequences, send_admitted_views, CandidateCentralState,
    CandidateRunnerInputs, CandidateSlot,
};
use crate::engine_trade_journal::{
    build_resolution_observed_record, ConfirmedCtfTerminalClearance,
};
use crate::portfolio::{PortfolioState, RedeemCloseRequest, SettlementInventoryClearance};
use crate::reclaim_merge::CtfTerminalMergeCommand;
use crate::strategy::InventoryIntent;
use crate::strategy_runtime::replay_mode::prepared_arrow::ResolutionRow;
use crate::strategy_runtime::symbol_process::EventRef;
use crate::strategy_runtime::symbol_process::SymbolId;
use crate::trade_session_ledger::{
    CanonicalUnresolvedCohortState, ConfirmedCtfTerminalFact, ConfirmedSettlementReceiptLineage,
    CtfTerminalOperation, DirectEoaOperation,
};

#[derive(Debug, Clone)]
struct PendingMergeCommand {
    requested_at_ms: u64,
    command: CtfTerminalMergeCommand,
}

#[derive(Debug, Clone, Default)]
pub(super) struct CandidateTerminalSettlementOwner {
    pending_merges: BTreeMap<String, PendingMergeCommand>,
}

impl CandidateTerminalSettlementOwner {
    pub(super) fn retain_merge_command(
        &mut self,
        symbol: SymbolId,
        intent_id: u64,
        requested_at_ms: u64,
        intent: InventoryIntent,
        portfolio: &PortfolioState,
    ) -> Result<bool> {
        anyhow::ensure!(
            intent_id > 0 && requested_at_ms > 0,
            "candidate terminal merge command identity or timestamp is zero"
        );
        let command = CtfTerminalMergeCommand::new(symbol, intent)?;
        let amount = command.intent.amount_units_6dp;
        anyhow::ensure!(
            portfolio_qty(portfolio, &command.intent.up_token) >= amount
                && portfolio_qty(portfolio, &command.intent.down_token) >= amount,
            "candidate terminal merge command exceeds exact central inventory condition_id={}",
            command.intent.condition_id
        );
        let key = command.intent.condition_id.to_string();
        let pending = PendingMergeCommand {
            requested_at_ms,
            command,
        };
        if let Some(existing) = self.pending_merges.get(&key) {
            anyhow::ensure!(
                same_pending_merge(existing, &pending),
                "candidate terminal merge command conflicts with pending identity condition_id={key}"
            );
            return Ok(false);
        }
        self.pending_merges.insert(key, pending);
        Ok(true)
    }
}

pub(super) fn apply_simulated_merge(
    central: &mut CandidateCentralState,
    submitted: &crate::execution::SimSubmittedCtfOperation,
    confirmed_at_ms: u64,
) -> Result<SymbolId> {
    anyhow::ensure!(
        submitted.kind == crate::execution::SimSubmittedCtfOperationKind::Merge
            && submitted.condition_ids.len() == 1
            && submitted.amount_units_6dp.len() == 1,
        "candidate simulated merge submission shape is invalid"
    );
    let condition_id = submitted.condition_ids[0];
    let key = condition_id.to_string();
    let pending = central
        .terminal_settlements
        .pending_merges
        .get(&key)
        .cloned()
        .context("candidate simulated merge has no pending command")?;
    let intent = &pending.command.intent;
    anyhow::ensure!(
        pending.requested_at_ms == confirmed_at_ms
            && intent.condition_id == condition_id
            && submitted.amount_units_6dp.as_slice()
                == [(condition_id, U256::from(intent.amount_units_6dp))],
        "candidate simulated merge submission differs from pending command"
    );
    let (split, run_id) = {
        let audit = central
            .audit
            .as_ref()
            .context("candidate audit ledger is unavailable")?;
        let (split, split_confirmed_at_ms) = audit
            .confirmed_split_for_terminal(&key)?
            .context("candidate simulated merge has no confirmed split")?;
        anyhow::ensure!(
            split_confirmed_at_ms <= confirmed_at_ms,
            "candidate simulated merge predates its confirmed split"
        );
        (split, audit.run_id().to_string())
    };
    anyhow::ensure!(
        pending.command.symbol.as_str() == split.underlying
            && intent.market_id == split.market_id
            && intent.up_token == split.up_token
            && intent.down_token == split.down_token,
        "candidate simulated merge identity differs from confirmed split"
    );
    let amount = intent.amount_units_6dp;
    anyhow::ensure!(
        terminal_inventory_precedes_submission(
            central,
            [&split.up_token, &split.down_token],
            portfolio_qty(&central.portfolio, &split.up_token),
            portfolio_qty(&central.portfolio, &split.down_token),
            confirmed_at_ms,
        )? && portfolio_qty(&central.portfolio, &split.up_token) >= amount
            && portfolio_qty(&central.portfolio, &split.down_token) >= amount,
        "candidate simulated merge exceeds exact central inventory"
    );
    let clearance = ConfirmedCtfTerminalClearance::confirmed_merge(
        split.market_id.clone(),
        split.condition_id.clone(),
        split.cohort_id.clone(),
        split.up_token.clone(),
        split.down_token.clone(),
        amount,
        amount,
        confirmed_at_ms,
    );
    let record = clearance.build_simulated_merge_record(&run_id, &submitted.tx_hash, amount)?;
    let mut next_portfolio = central.portfolio.clone();
    let requests = terminal_close_requests(
        &split.market_id,
        [&split.up_token, &split.down_token],
        amount,
        amount,
        None,
        None,
        confirmed_at_ms,
        CtfTerminalOperation::Merge,
        &submitted.tx_hash,
    )?;
    let clearances = next_portfolio.close_redeemed_positions(&requests)?;
    validate_clearances(
        &clearances,
        &split.market_id,
        &split.cohort_id,
        [&split.up_token, &split.down_token],
        amount,
        amount,
        &submitted.tx_hash,
    )?;
    let mut next_ctf_chain = central.ctf_chain.clone();
    next_ctf_chain.apply_terminal_settlement(
        &submitted.tx_hash,
        &split.up_token,
        amount,
        &split.down_token,
        amount,
        confirmed_at_ms,
    )?;
    central.ingest_audit_record(&record)?;
    central.portfolio = next_portfolio;
    central.ctf_chain = next_ctf_chain;
    central.terminal_settlements.pending_merges.remove(&key);
    SymbolId::parse(&split.underlying)
}

pub(super) fn apply_simulated_resolution_redemption(
    central: &mut CandidateCentralState,
    resolution: &ResolutionRow,
    applied_at_ms: u64,
) -> Result<Option<SymbolId>> {
    let condition = resolution.condition_id.parse::<B256>()?;
    let accounting = central.accounting.current();
    let mut matching = accounting
        .financial
        .unresolved_cohorts
        .iter()
        .filter(|cohort| cohort.market_id.as_str() == resolution.condition_id);
    let Some(cohort) = matching.next() else {
        return Ok(None);
    };
    anyhow::ensure!(
        matching.next().is_none(),
        "candidate redemption has multiple condition entries"
    );
    let cashflow = match &cohort.state {
        CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => cashflow,
        CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { detail } => {
            anyhow::bail!("candidate redemption acquisition receipts are unresolved: {detail}")
        }
    };
    if cohort.last_ts_ms > applied_at_ms {
        return Ok(None);
    }
    anyhow::ensure!(
        resolution.observed_at_ms <= applied_at_ms
            && cashflow.condition_id == resolution.condition_id
            && cashflow.up_token == resolution.up_token
            && cashflow.down_token == resolution.down_token
            && (resolution.winning_asset_id == cashflow.up_token
                || resolution.winning_asset_id == cashflow.down_token),
        "candidate simulated redemption differs from captured resolution"
    );
    let up_qty = portfolio_qty(&central.portfolio, &cashflow.up_token);
    let down_qty = portfolio_qty(&central.portfolio, &cashflow.down_token);
    if up_qty == 0 && down_qty == 0 {
        return Ok(None);
    }
    anyhow::ensure!(
        up_qty == cashflow.up_remaining_qty_units_6dp
            && down_qty == cashflow.down_remaining_qty_units_6dp,
        "candidate terminal inventory differs from canonical acquisition receipts"
    );
    if !terminal_inventory_precedes_submission(
        central,
        [&cashflow.up_token, &cashflow.down_token],
        up_qty,
        down_qty,
        applied_at_ms,
    )? {
        return Ok(None);
    }

    central.execution_sim.submit_replay_redeem(condition);
    let submitted = central.execution_sim.take_submitted_ctf_ops_delta()?;
    anyhow::ensure!(
        submitted.len() == 1
            && submitted[0].kind == crate::execution::SimSubmittedCtfOperationKind::Redeem
            && submitted[0].condition_ids.as_slice() == [condition]
            && submitted[0].amount_units_6dp.is_empty(),
        "candidate simulated redemption produced invalid operation evidence"
    );
    let operation_id = &submitted[0].tx_hash;
    let clearance = ConfirmedCtfTerminalClearance::simulated_resolution_redeem(
        cohort.market_id.clone(),
        cashflow.condition_id.clone(),
        cohort.paired_cohort_key.clone(),
        cashflow.up_token.clone(),
        cashflow.down_token.clone(),
        up_qty,
        down_qty,
        applied_at_ms,
    );
    let record = clearance.build_simulated_resolution_redemption_record(
        &cashflow.run_id,
        operation_id,
        &resolution.winning_asset_id,
        resolution.observed_at_ms,
        &resolution.raw_response_sha256,
    )?;
    let mut next_portfolio = central.portfolio.clone();
    let requests = terminal_close_requests(
        &cohort.market_id,
        [&cashflow.up_token, &cashflow.down_token],
        up_qty,
        down_qty,
        None,
        None,
        applied_at_ms,
        CtfTerminalOperation::Redeem,
        operation_id,
    )?;
    let clearances = next_portfolio.close_redeemed_positions(&requests)?;
    validate_clearances(
        &clearances,
        &cohort.market_id,
        &cohort.paired_cohort_key,
        [&cashflow.up_token, &cashflow.down_token],
        up_qty,
        down_qty,
        operation_id,
    )?;
    let mut next_ctf_chain = central.ctf_chain.clone();
    next_ctf_chain.apply_terminal_settlement(
        operation_id,
        &cashflow.up_token,
        up_qty,
        &cashflow.down_token,
        down_qty,
        applied_at_ms,
    )?;
    central.ingest_audit_record(&record)?;
    central.portfolio = next_portfolio;
    central.ctf_chain = next_ctf_chain;
    Ok(Some(SymbolId::parse(&cohort.symbol)?))
}

pub(super) fn symbol_for_inventory_intent(
    inputs: &CandidateRunnerInputs<'_>,
    central: &CandidateCentralState,
    intent: &InventoryIntent,
) -> Result<SymbolId> {
    symbol_for_condition(inputs, central, intent.condition_id.to_string().as_str())
}

pub(super) fn symbol_for_condition(
    inputs: &CandidateRunnerInputs<'_>,
    central: &CandidateCentralState,
    raw_condition_id: &str,
) -> Result<SymbolId> {
    let condition_id: [u8; 32] = raw_condition_id
        .parse::<polymarket_client_sdk::types::B256>()
        .context("candidate condition identity is invalid")?
        .into();
    let mut owner = None;
    for symbol in [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp] {
        if !inputs
            .symbols
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(symbol.as_str()))
        {
            continue;
        }
        let Some(catalog) = central.catalogs.get(symbol.as_str()) else {
            continue;
        };
        if catalog.market_handle_for_condition(condition_id).is_none() {
            continue;
        }
        anyhow::ensure!(
            owner.replace(symbol).is_none(),
            "candidate inventory condition belongs to multiple symbol catalogs"
        );
    }
    owner.context("candidate condition is foreign to all symbol catalogs")
}

pub(super) async fn apply_central_event(
    candidate_run_id: &str,
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    event: EventRef,
) -> Result<()> {
    if let Some(symbol) = apply_central_receipt(candidate_run_id, inputs, central, event)? {
        central.portfolio_tx.send_replace(central.portfolio.clone());
        let intents = send_admitted_views(slot, central, event.timestamp_ms, [symbol])?;
        converge_central_consequences(slot, inputs, central, event.timestamp_ms, intents).await?;
    }
    Ok(())
}

fn apply_central_receipt(
    candidate_run_id: &str,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    event: EventRef,
) -> Result<Option<SymbolId>> {
    use crate::strategy_runtime::replay_mode::prepared_event_stream::{
        EVENT_KIND_CONDITION_RESOLUTION, EVENT_KIND_SETTLEMENT_COLLATERAL,
    };
    match event.kind {
        EVENT_KIND_CONDITION_RESOLUTION => {
            let receipt = inputs.receipt_tape.finalized_resolution(event)?;
            let record = build_resolution_observed_record(candidate_run_id, receipt)?;
            central.ingest_audit_record(&record)?;
            Ok(None)
        }
        EVENT_KIND_SETTLEMENT_COLLATERAL => {
            let settlement = inputs.receipt_tape.settlement_lineage(event)?;
            retain_captured_live_settlement_evidence(
                central,
                &settlement.lineage,
                &settlement.terminal,
                event.timestamp_ms,
            )?;
            Ok(None)
        }
        other => anyhow::bail!("candidate central receipt event kind is invalid kind={other}"),
    }
}

fn retain_captured_live_settlement_evidence(
    central: &mut CandidateCentralState,
    lineage: &ConfirmedSettlementReceiptLineage,
    source_terminal: &ConfirmedCtfTerminalFact,
    captured_at_ms: u64,
) -> Result<()> {
    let receipt = &lineage.collateral;
    let (receipt_id, transaction_id) = source_terminal
        .confirmed_receipt_identity()
        .context("prepared terminal mapping is not receipt-backed")?;
    anyhow::ensure!(
        receipt_id == receipt.accounting_receipt_id()
            && transaction_id == receipt.tx_hash
            && source_terminal.condition_id == receipt.condition_id
            && receipt.ts_ms <= captured_at_ms,
        "prepared live settlement evidence identity is inconsistent"
    );
    if receipt.operation == DirectEoaOperation::Redeem {
        let resolution = central
            .audit
            .as_ref()
            .context("candidate audit ledger is unavailable")?
            .finalized_resolution_for_terminal(&receipt.condition_id)
            .context("prepared live redemption evidence has no finalized resolution")?;
        anyhow::ensure!(
            resolution.observed_at_ms <= lineage.submission.ts_ms,
            "prepared live redemption resolution follows its submission"
        );
    }
    central.ingest_settlement_receipt(receipt.clone())
}

fn terminal_inventory_precedes_submission(
    central: &CandidateCentralState,
    tokens: [&str; 2],
    up_qty: u128,
    down_qty: u128,
    submission_ts_ms: u64,
) -> Result<bool> {
    for (token, expected_qty) in [(tokens[0], up_qty), (tokens[1], down_qty)] {
        let Some(position) = central.portfolio.position(token) else {
            if expected_qty == 0 && central.ctf_chain.position_state_units_6dp(token).is_none() {
                continue;
            }
            return Ok(false);
        };
        let Some((chain_qty, chain_last_mutation_ms)) =
            central.ctf_chain.position_state_units_6dp(token)
        else {
            return Ok(false);
        };
        if position.qty_units_6dp != expected_qty
            || chain_qty != expected_qty
            || position.last_update_ms > submission_ts_ms
            || chain_last_mutation_ms > submission_ts_ms
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn terminal_close_requests(
    market_id: &crate::polymarket::identity::MarketId,
    tokens: [&str; 2],
    up_qty: u128,
    down_qty: u128,
    up_outcome_index: Option<u8>,
    down_outcome_index: Option<u8>,
    confirmed_at_ms: u64,
    operation: CtfTerminalOperation,
    operation_id: &str,
) -> Result<Vec<RedeemCloseRequest>> {
    [
        (tokens[0], up_qty, up_outcome_index),
        (tokens[1], down_qty, down_outcome_index),
    ]
    .into_iter()
    .filter(|(_, qty, _)| *qty > 0)
    .map(|(token, qty, outcome_index)| {
        Ok(RedeemCloseRequest {
            market_id: market_id.clone(),
            token: token.to_string(),
            outcome_index,
            qty_units_6dp: qty,
            close_reason: match operation {
                CtfTerminalOperation::Merge => "merge",
                CtfTerminalOperation::Redeem => "redeem",
            }
            .to_string(),
            closed_at_ms: confirmed_at_ms,
            exit_tx_hash: Some(operation_id.to_string()),
        })
    })
    .collect()
}

fn validate_clearances(
    clearances: &[SettlementInventoryClearance],
    market_id: &crate::polymarket::identity::MarketId,
    cohort_id: &str,
    tokens: [&str; 2],
    expected_up: u128,
    expected_down: u128,
    tx_hash: &str,
) -> Result<()> {
    let mut up = 0u128;
    let mut down = 0u128;
    for clearance in clearances {
        anyhow::ensure!(
            &clearance.market_id == market_id
                && clearance.engine_trade_id.as_deref() == Some(cohort_id)
                && clearance.settlement_tx_hash.as_deref() == Some(tx_hash),
            "candidate terminal portfolio clearance identity mismatch"
        );
        let total = if clearance.token == tokens[0] {
            &mut up
        } else if clearance.token == tokens[1] {
            &mut down
        } else {
            anyhow::bail!("candidate terminal clearance token is foreign to acquisition cohort")
        };
        *total = total
            .checked_add(clearance.qty_units_6dp)
            .context("candidate terminal clearance quantity overflow")?;
    }
    anyhow::ensure!(
        up == expected_up && down == expected_down,
        "candidate terminal portfolio clearance quantities mismatch expected_up={expected_up} actual_up={up} expected_down={expected_down} actual_down={down}"
    );
    Ok(())
}

fn portfolio_qty(portfolio: &PortfolioState, token: &str) -> u128 {
    portfolio
        .position(token)
        .map(|position| position.qty_units_6dp)
        .unwrap_or(0)
}

fn same_pending_merge(left: &PendingMergeCommand, right: &PendingMergeCommand) -> bool {
    let left_intent = &left.command.intent;
    let right_intent = &right.command.intent;
    left.command.symbol == right.command.symbol
        && left_intent.operation == right_intent.operation
        && left_intent.market_id == right_intent.market_id
        && left_intent.gamma_market_id == right_intent.gamma_market_id
        && left_intent.condition_id == right_intent.condition_id
        && left_intent.up_token == right_intent.up_token
        && left_intent.down_token == right_intent.down_token
        && left_intent.amount_units_6dp == right_intent.amount_units_6dp
}

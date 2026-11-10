use anyhow::Result;
use polymarket_client_sdk::types::U256;

use super::central_receipts::apply_simulated_merge;
use super::{symbol_for_inventory_intent, CandidateCentralState};
use crate::ctf_inventory::{
    submit_inventory_operation, CtfInventoryAction, CtfInventoryReceipt, CtfInventoryWait,
};
use crate::execution::{SimSubmittedCtfOperation, SimSubmittedCtfOperationKind};
use crate::strategy::{InventoryIntent, InventoryOperation};
use crate::strategy_runtime::replay_mode::candidate_runner::CandidateRunnerInputs;
use crate::strategy_runtime::symbol_process::DecodedEntryContext;

pub(super) async fn schedule_ctf_inventory_intent(
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    intent_id: u64,
    timestamp_ms: u64,
    intent: InventoryIntent,
    entry_context: Option<DecodedEntryContext>,
) -> Result<Option<crate::strategy_runtime::symbol_process::SymbolId>> {
    let symbol = symbol_for_inventory_intent(inputs, central, &intent)?;
    if intent.operation == InventoryOperation::MergeCompleteSet {
        anyhow::ensure!(
            entry_context.is_none(),
            "candidate merge intent carries split decision context"
        );
        let retained = central.terminal_settlements.retain_merge_command(
            symbol,
            intent_id,
            timestamp_ms,
            intent.clone(),
            &central.portfolio,
        )?;
        if !retained {
            return Ok(None);
        }
        central
            .execution_sim
            .submit_replay_merge(intent.condition_id, intent.amount_units_6dp)?;
        let submitted = central.execution_sim.take_submitted_ctf_ops_delta()?;
        anyhow::ensure!(
            submitted.len() == 1,
            "candidate merge submission produced invalid operation count actual={}",
            submitted.len()
        );
        let applied = apply_simulated_merge(central, &submitted[0], timestamp_ms)?;
        anyhow::ensure!(
            applied == symbol,
            "candidate simulated merge applied to foreign symbol"
        );
        central.portfolio_tx.send_replace(central.portfolio.clone());
        return Ok(Some(symbol));
    }
    let entry_journal = match entry_context {
        Some(DecodedEntryContext::Split(journal)) => journal,
        _ => anyhow::bail!("candidate split intent lost mandatory split decision context"),
    };
    let action = central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .next_action(&intent, timestamp_ms)?;
    match action {
        CtfInventoryAction::Execute => {
            let reserved = match reserve_ctf_split_intent(central, symbol, timestamp_ms, &intent) {
                Ok(reserved) => reserved,
                Err(error) => {
                    tracing::warn!(
                        target: "strategy_runtime.candidate.central",
                        symbol = symbol.as_str(),
                        market_id = %intent.market_id,
                        condition_id = %intent.condition_id,
                        error = %error,
                        "candidate_ctf_split_admission_blocked"
                    );
                    return Ok(None);
                }
            };
            anyhow::ensure!(
                reserved,
                "candidate executable split already had a central reservation"
            );
        }
        CtfInventoryAction::BeginSubmission => {}
        CtfInventoryAction::Wait(CtfInventoryWait::Pending)
        | CtfInventoryAction::Wait(CtfInventoryWait::Confirmed { .. }) => return Ok(None),
        CtfInventoryAction::PollReceipt { .. }
        | CtfInventoryAction::Materialize { .. }
        | CtfInventoryAction::PublishConfirmed(_)
        | CtfInventoryAction::Wait(CtfInventoryWait::FailedCooldown { .. })
        | CtfInventoryAction::Wait(CtfInventoryWait::ReconciliationRequired { .. }) => {
            anyhow::bail!("candidate split intent reached an invalid CTF inventory phase")
        }
    }
    central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .begin_reserved_split_submission(&intent, timestamp_ms)?;
    let receipt = match submit_inventory_operation(
        &central.execution,
        symbol.as_str(),
        &intent,
        &entry_journal,
    )
    .await
    {
        Ok(receipt) => receipt,
        Err(error) => {
            let detail = format!("{error:#}");
            central
                .ctf_inventory
                .lock()
                .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                .record_submission_failure(&intent, timestamp_ms, detail.clone())?;
            return Err(anyhow::anyhow!(detail));
        }
    };
    let submitted = central.execution_sim.take_submitted_ctf_ops_delta()?;
    anyhow::ensure!(
        submitted.len() == 1,
        "candidate split submission produced invalid operation count actual={}",
        submitted.len()
    );
    validate_submitted_split(&submitted[0], &intent, &receipt)?;
    central.ctf_chain.schedule_confirmed(
        symbol,
        intent_id,
        timestamp_ms,
        intent,
        receipt,
        entry_journal,
    )?;
    Ok(Some(symbol))
}

pub(super) fn reserve_ctf_split_intent(
    central: &mut CandidateCentralState,
    symbol: crate::strategy_runtime::symbol_process::SymbolId,
    timestamp_ms: u64,
    intent: &InventoryIntent,
) -> Result<bool> {
    anyhow::ensure!(
        intent.operation == InventoryOperation::SplitCompleteSet,
        "candidate split reservation received non-split operation"
    );
    central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .reserve_split_for_symbol(&central.projection, symbol, intent, timestamp_ms)
}

fn validate_submitted_split(
    submitted: &SimSubmittedCtfOperation,
    intent: &InventoryIntent,
    receipt: &CtfInventoryReceipt,
) -> Result<()> {
    let tx_hash = match receipt {
        CtfInventoryReceipt::Confirmed { tx_hash } => tx_hash,
        CtfInventoryReceipt::Pending { .. }
        | CtfInventoryReceipt::Reverted { .. }
        | CtfInventoryReceipt::ReconciliationRequired { .. } => {
            anyhow::bail!("candidate split simulation did not return a confirmed receipt")
        }
    };
    anyhow::ensure!(
        submitted.kind == SimSubmittedCtfOperationKind::Split,
        "candidate CTF submission kind is not split"
    );
    anyhow::ensure!(
        submitted.tx_hash == *tx_hash,
        "candidate split submission/receipt transaction identity mismatch"
    );
    anyhow::ensure!(
        submitted.condition_ids.as_slice() == [intent.condition_id],
        "candidate split submitted foreign condition identity"
    );
    anyhow::ensure!(
        submitted.amount_units_6dp.as_slice()
            == [(intent.condition_id, U256::from(intent.amount_units_6dp))],
        "candidate split submitted foreign amount identity"
    );
    Ok(())
}

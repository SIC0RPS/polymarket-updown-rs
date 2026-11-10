use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use polymarket_hft::capture::events::CtfInventoryOperationLatencyCapture;
use polymarket_hft::capture::session::{CaptureMsg, MaybeCapture};
use polymarket_hft::config::SharedStrategyConfig;
use polymarket_hft::ctf_inventory::{
    expected_ctf_positions_for_confirmed_split, split_receipt, submit_inventory_operation,
    ConfirmedCtfInventoryOperation, CtfInventoryAction, CtfInventoryReceipt, CtfInventoryRuntime,
    CtfInventoryWait,
};
use polymarket_hft::engine_trade_journal::{
    append_prepared_ctf_split_entry_accepted, prepare_ctf_split_entry_accepted,
};
use polymarket_hft::execution::{ExecutionClient, SplitReceiptStatus};
use polymarket_hft::polymarket::state::MarketProjection;
use polymarket_hft::portfolio::PortfolioHydrationHandle;
use polymarket_hft::runtime_control::RuntimeConfigState;
use polymarket_hft::strategy::{InventoryIntent, InventoryOperation};
use polymarket_hft::trade_session_ledger::ConfirmedCtfSplitFact;
use tokio::sync::watch;

pub(crate) mod risk_reservation;

mod task;
pub(crate) use task::{spawn_ctf_inventory_task, CtfSplitCommand};

enum CtfInventoryWork {
    Submit,
    PollReceipt { tx_hash: String },
    Retry,
}

enum CtfInventoryObservation {
    Submission(std::result::Result<Option<CtfInventoryReceipt>, String>),
    Receipt(CtfReceiptPollObservation),
}

struct CtfReceiptPollObservation {
    tx_hash: String,
    status: std::result::Result<Option<SplitReceiptStatus>, String>,
}

async fn submit_inventory_command(
    exec: &ExecutionClient,
    runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    shared_strategy_cfg: &SharedStrategyConfig,
    paused: bool,
    runtime_control_rx: &watch::Receiver<RuntimeConfigState>,
    market_projection_rx: &watch::Receiver<MarketProjection>,
    command: &CtfSplitCommand,
) -> Result<Option<CtfInventoryReceipt>> {
    let intent = &command.intent;
    let now_ms = polymarket_hft::event_ledger::now_ms();
    let underlying = command
        .entry_journal
        .decisions
        .first()
        .and_then(|decision| decision.market_underlying.as_deref())
        .context("split submission missing original underlying identity")?;
    if paused
        || !shared_strategy_cfg.borrow().is_asset_enabled(underlying)
        || market_projection_rx
            .borrow()
            .market_by_id(intent.market_id.as_str())
            .is_none_or(|market| {
                !runtime_control_rx
                    .borrow()
                    .allows_new_market_entry(market.window_start_ms)
            })
    {
        runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
            .release_reserved_split(intent, now_ms)?;
        return Ok(None);
    }
    runtime
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .begin_reserved_split_submission(intent, now_ms)?;
    exec.live_ops().record_inventory_pending(
        intent.market_id.as_str(),
        &intent.condition_id.to_string(),
        intent.operation.as_str(),
    );
    submit_inventory_operation(exec, underlying, intent, &command.entry_journal)
        .await
        .map(Some)
}

fn execute_inventory_intent(
    exec: &ExecutionClient,
    portfolio_hydration: &PortfolioHydrationHandle,
    paused: bool,
    runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    capture: &MaybeCapture,
    command: &CtfSplitCommand,
    observation: Option<CtfInventoryObservation>,
    retrying: bool,
) -> Result<Option<CtfInventoryWork>> {
    let intent = &command.intent;
    anyhow::ensure!(
        intent.operation == InventoryOperation::SplitCompleteSet,
        "split owner received non-split inventory command"
    );
    let mut receipt_observation = match observation {
        Some(CtfInventoryObservation::Submission(result)) => {
            let receipt = match result {
                Ok(Some(receipt)) => receipt,
                Ok(None) => return Ok(None),
                Err(detail) => {
                    let failed_at_ms = polymarket_hft::event_ledger::now_ms();
                    runtime
                        .lock()
                        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                        .record_submission_failure(intent, failed_at_ms, detail.clone())?;
                    exec.live_ops().record_inventory_failed(
                        intent.market_id.as_str(),
                        &intent.condition_id.to_string(),
                        intent.operation.as_str(),
                        None,
                        detail.clone(),
                    );
                    return Err(anyhow::anyhow!(detail));
                }
            };
            let receipt_at_ms = polymarket_hft::event_ledger::now_ms();
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                .record_receipt(intent, receipt_at_ms, receipt.clone())?;
            match receipt {
                CtfInventoryReceipt::Pending { tx_hash } => {
                    tracing::info!(
                        target: "app.ctf_inventory",
                        market_id = %intent.market_id,
                        condition_id = %intent.condition_id,
                        operation = intent.operation.as_str(),
                        tx_hash = %tx_hash,
                        "ctf_inventory_split_submission_pending"
                    );
                }
                CtfInventoryReceipt::Confirmed { .. } => {}
                CtfInventoryReceipt::Reverted { tx_hash } => {
                    let error = format!("CTF inventory transaction reverted tx_hash={tx_hash}");
                    exec.live_ops().record_inventory_failed(
                        intent.market_id.as_str(),
                        &intent.condition_id.to_string(),
                        intent.operation.as_str(),
                        Some(tx_hash),
                        error.clone(),
                    );
                    return Err(anyhow::anyhow!(error));
                }
                CtfInventoryReceipt::ReconciliationRequired { tx_hash, error } => {
                    exec.live_ops().record_inventory_ambiguous(
                        intent.market_id.as_str(),
                        &intent.condition_id.to_string(),
                        intent.operation.as_str(),
                        Some(tx_hash.clone()),
                        error.clone(),
                    );
                    tracing::error!(
                        target: "app.ctf_inventory",
                        market_id = %intent.market_id,
                        condition_id = %intent.condition_id,
                        operation = intent.operation.as_str(),
                        tx_hash = %tx_hash,
                        error = %error,
                        "ctf_inventory_operation_reconciliation_required"
                    );
                    return Ok(None);
                }
            }
            None
        }
        Some(CtfInventoryObservation::Receipt(observation)) => Some(observation),
        None => None,
    };

    loop {
        let now_ms = polymarket_hft::event_ledger::now_ms();
        let action = runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
            .next_action(intent, now_ms)?;
        if !retrying
            && matches!(
                &action,
                CtfInventoryAction::PollReceipt { .. }
                    | CtfInventoryAction::Materialize { .. }
                    | CtfInventoryAction::PublishConfirmed(_)
            )
        {
            return Ok(None);
        }
        match action {
            CtfInventoryAction::BeginSubmission => {
                if paused {
                    runtime
                        .lock()
                        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                        .release_reserved_split(intent, now_ms)?;
                    tracing::debug!(
                        target: "app.ctf_inventory",
                        market_id = %intent.market_id,
                        condition_id = %intent.condition_id,
                        "ctf_split_blocked_by_pause"
                    );
                    return Ok(None);
                }
                if let Err(error) = command
                    .entry_journal
                    .decisions
                    .first()
                    .and_then(|decision| decision.market_underlying.as_deref())
                    .context("split submission missing original underlying identity")
                {
                    runtime
                        .lock()
                        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                        .release_reserved_split(intent, now_ms)?;
                    return Err(error);
                }
                return Ok(Some(CtfInventoryWork::Submit));
            }
            CtfInventoryAction::Execute => {
                anyhow::ensure!(
                    retrying,
                    "split task received an unreserved CTF inventory command"
                );
                let reserved = runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                    .reserve_split(intent, now_ms)?;
                anyhow::ensure!(
                    reserved,
                    "split retry failed to replace the released runtime reservation"
                );
            }
            CtfInventoryAction::PollReceipt {
                tx_hash,
                elapsed_ms,
            } => {
                let Some(observation) = receipt_observation.take() else {
                    return Ok(Some(CtfInventoryWork::PollReceipt { tx_hash }));
                };
                anyhow::ensure!(
                    observation.tx_hash == tx_hash,
                    "CTF split receipt observation transaction identity changed"
                );
                match observation.status {
                    Ok(Some(status)) => {
                        let receipt = split_receipt(&tx_hash, status);
                        let observed_at_ms = polymarket_hft::event_ledger::now_ms();
                        runtime
                            .lock()
                            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                            .record_receipt(intent, observed_at_ms, receipt.clone())?;
                        match receipt {
                            CtfInventoryReceipt::Pending { .. } => {
                                return Ok(Some(CtfInventoryWork::Retry));
                            }
                            CtfInventoryReceipt::Confirmed { .. } => continue,
                            CtfInventoryReceipt::Reverted { tx_hash } => {
                                let error =
                                    format!("CTF inventory transaction reverted tx_hash={tx_hash}");
                                exec.live_ops().record_inventory_failed(
                                    intent.market_id.as_str(),
                                    &intent.condition_id.to_string(),
                                    intent.operation.as_str(),
                                    Some(tx_hash),
                                    error.clone(),
                                );
                                return Err(anyhow::anyhow!(error));
                            }
                            CtfInventoryReceipt::ReconciliationRequired { tx_hash, error } => {
                                exec.live_ops().record_inventory_ambiguous(
                                    intent.market_id.as_str(),
                                    &intent.condition_id.to_string(),
                                    intent.operation.as_str(),
                                    Some(tx_hash),
                                    error,
                                );
                                return Ok(None);
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::info!(
                            target: "app.ctf_inventory",
                            market_id = %intent.market_id,
                            condition_id = %intent.condition_id,
                            operation = intent.operation.as_str(),
                            tx_hash = %tx_hash,
                            elapsed_ms,
                            "ctf_inventory_split_receipt_pending"
                        );
                        return Ok(Some(CtfInventoryWork::Retry));
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "app.ctf_inventory",
                            market_id = %intent.market_id,
                            condition_id = %intent.condition_id,
                            operation = intent.operation.as_str(),
                            tx_hash = %tx_hash,
                            error = %err,
                            "ctf_inventory_split_receipt_query_failed"
                        );
                        return Ok(Some(CtfInventoryWork::Retry));
                    }
                }
            }
            CtfInventoryAction::Materialize { tx_hash } => {
                let materialized_at_ms = polymarket_hft::event_ledger::now_ms();
                runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                    .record_materialized(intent, materialized_at_ms, &tx_hash)?;
            }
            CtfInventoryAction::PublishConfirmed(confirmed) => {
                anyhow::ensure!(
                    confirmed.operation == InventoryOperation::SplitCompleteSet,
                    "split owner published non-split confirmation"
                );
                let (record, split) = prepare_ctf_split_entry_accepted(&confirmed)
                    .context("confirmed split audit persistence failed")?;
                let mut persistence_failed = false;
                if let Err(err) = materialize_confirmed_split(
                    portfolio_hydration,
                    &split,
                    confirmed.materialized_ts_ms,
                    || {
                        let result = append_prepared_ctf_split_entry_accepted(record);
                        persistence_failed = result.is_err();
                        result
                    },
                ) {
                    if persistence_failed {
                        return Err(err).context("confirmed split audit persistence failed");
                    }
                    exec.live_ops().record_inventory_refresh_failed(
                        confirmed.market_id.as_str(),
                        &confirmed.condition_id.to_string(),
                        confirmed.operation.as_str(),
                        Some(confirmed.tx_hash.clone()),
                        err.to_string(),
                    );
                    return Err(err).context("post-CTF operation portfolio refresh failed");
                }
                emit_inventory_latency(capture, &confirmed);
                exec.live_ops().record_inventory_confirmed(
                    confirmed.market_id.as_str(),
                    &confirmed.condition_id.to_string(),
                    confirmed.operation.as_str(),
                    Some(confirmed.tx_hash.clone()),
                );
                runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                    .acknowledge_confirmed(&confirmed, polymarket_hft::event_ledger::now_ms())?;
                tracing::info!(
                    target: "app.ctf_inventory",
                    market_id = %confirmed.market_id,
                    condition_id = %confirmed.condition_id,
                    operation = confirmed.operation.as_str(),
                    tx_hash = %confirmed.tx_hash,
                    "ctf_inventory_operation_confirmed"
                );
                return Ok(None);
            }
            CtfInventoryAction::Wait(CtfInventoryWait::Pending) => {
                tracing::info!(
                    target: "app.ctf_inventory",
                    market_id = %intent.market_id,
                    condition_id = %intent.condition_id,
                    operation = intent.operation.as_str(),
                    "ctf_inventory_operation_dedup_pending"
                );
                return Ok(None);
            }
            CtfInventoryAction::Wait(CtfInventoryWait::Confirmed { tx_hash }) => {
                tracing::info!(
                    target: "app.ctf_inventory",
                    market_id = %intent.market_id,
                    condition_id = %intent.condition_id,
                    operation = intent.operation.as_str(),
                    tx_hash = %tx_hash,
                    "ctf_inventory_operation_dedup_confirmed"
                );
                return Ok(None);
            }
            CtfInventoryAction::Wait(CtfInventoryWait::FailedCooldown {
                remaining_ms,
                error,
            }) => {
                tracing::info!(
                    target: "app.ctf_inventory",
                    market_id = %intent.market_id,
                    condition_id = %intent.condition_id,
                    operation = intent.operation.as_str(),
                    remaining_ms,
                    error = %error,
                    "ctf_inventory_operation_dedup_failed_cooldown"
                );
                return Ok(Some(CtfInventoryWork::Retry));
            }
            CtfInventoryAction::Wait(CtfInventoryWait::ReconciliationRequired {
                tx_hash,
                error,
            }) => {
                tracing::error!(
                    target: "app.ctf_inventory",
                    market_id = %intent.market_id,
                    condition_id = %intent.condition_id,
                    operation = intent.operation.as_str(),
                    tx_hash = %tx_hash,
                    error = %error,
                    "ctf_inventory_operation_dedup_reconciliation_required"
                );
                return Ok(None);
            }
        }
    }
}

fn emit_inventory_latency(capture: &MaybeCapture, confirmed: &ConfirmedCtfInventoryOperation) {
    capture.emit(CaptureMsg::CtfInventoryOperationLatency(
        CtfInventoryOperationLatencyCapture {
            session_ts_ms: confirmed.materialized_ts_ms,
            market_id: confirmed.market_id.clone(),
            condition_id: confirmed.condition_id.to_string(),
            operation: confirmed.operation.as_str().to_string(),
            status: "materialized".to_string(),
            amount_units_6dp: confirmed.amount_units_6dp,
            pending_ts_ms: confirmed.pending_ts_ms,
            materialized_ts_ms: confirmed.materialized_ts_ms,
            latency_ms: confirmed
                .materialized_ts_ms
                .saturating_sub(confirmed.pending_ts_ms),
            materialization_source: "receipt_confirmed_current_session_split".to_string(),
            tx_hash: Some(confirmed.tx_hash.clone()),
            error: None,
        },
    ));
}

fn materialize_confirmed_split(
    portfolio_hydration: &PortfolioHydrationHandle,
    split: &ConfirmedCtfSplitFact,
    materialized_ts_ms: u64,
    persist: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let expected_positions = expected_ctf_positions_for_confirmed_split(split, materialized_ts_ms)?;
    let exact_balances = expected_positions
        .iter()
        .map(|position| (position.token.clone(), split.collateral_debit_units_6dp))
        .collect::<HashMap<_, _>>();
    portfolio_hydration
        .hydrate_expected_ctf_balances_with_persist(&expected_positions, &exact_balances, persist)
        .context("failed to materialize confirmed current-session CTF split")?;
    Ok(())
}

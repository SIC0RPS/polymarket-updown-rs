use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};

use crate::config::AppConfig;
use crate::ctf_inventory::CtfInventoryRuntime;
use crate::execution::{CtfSettlementConfirmation, ExecutionClient};
use crate::observability::action::{ActionEvent, ReclaimActionKind};
use crate::polymarket::data_positions::{fetch_complete_positions, PositionScope};
use crate::portfolio::PortfolioCloseHandle;
use polymarket_client_sdk::data::types::response::Position;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::types::B256;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod close_accounting;
mod command;
mod journal;
mod merge_processing;
mod payout_vector;
mod tx_clearance;

pub use command::CtfTerminalMergeCommand;

use command::{
    condition_has_pending_submission, retain_terminal_merge_commands, PendingConfirmedAccounting,
    ReclaimAttemptKey, ReclaimKind, ReclaimOperationCompletion, ReclaimOperationFuture,
    TerminalSubmission,
};

use close_accounting::{
    attach_close_confirmation, prepare_redeem_for_condition, terminal_condition_ids,
};
use journal::append_portfolio_close_receipts_to_journal;
use merge_processing::process_pending_merge_commands;
use payout_vector::validated_payout_vector_for_condition;
use tx_clearance::{reclaim_retry_remaining, schedule_reclaim_retry};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn process_confirmed_accounting(
    key: ReclaimAttemptKey,
    terminal_submissions: &mut HashMap<ReclaimAttemptKey, TerminalSubmission>,
    obs: &crate::observability::Observability,
    portfolio_close: &PortfolioCloseHandle,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    settlement_account_refresh_tx: &mpsc::Sender<()>,
) -> bool {
    let accounting = match terminal_submissions.remove(&key) {
        Some(TerminalSubmission::Accounting(accounting)) => accounting,
        Some(submission) => {
            terminal_submissions.insert(key, submission);
            return true;
        }
        None => return true,
    };
    let receipt = &accounting.receipt;
    let tx = receipt.tx_hash.clone();
    let condition_id_str = key.condition_id.to_string();
    if accounting.requests.is_empty() {
        if key.kind == ReclaimKind::Merge {
            tracing::error!(
                target: "app.reclaim_merge",
                condition_id = %condition_id_str,
                "merge_terminal_portfolio_clearance_empty"
            );
            terminal_submissions.insert(key, TerminalSubmission::Accounting(accounting));
            return false;
        }
        match settlement_account_refresh_tx.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => {}
            Err(mpsc::error::TrySendError::Closed(())) => {
                tracing::error!(
                    target: "app.reclaim_merge",
                    condition_id = %condition_id_str,
                    "settlement_account_projection_channel_closed"
                );
                terminal_submissions.insert(key, TerminalSubmission::Accounting(accounting));
                return false;
            }
        }
    } else {
        let portfolio_snapshot = portfolio_close.snapshot();
        let mut preview = portfolio_snapshot.clone();
        let expected_revision = preview.revision;
        let preview_clearances = match preview.close_redeemed_positions(&accounting.requests) {
            Ok(clearances) => clearances,
            Err(err) => {
                tracing::error!(
                    target: "app.reclaim_merge",
                    closed_at_ms = accounting.closed_at_ms,
                    error = %err,
                    "terminal_portfolio_clearance_preflight_failed"
                );
                terminal_submissions.insert(key, TerminalSubmission::Accounting(accounting));
                return false;
            }
        };
        if let Err(err) = portfolio_close.close_redeemed_positions_if_revision(
            &accounting.requests,
            expected_revision,
            &preview_clearances,
            |clearances| {
                let ctf_inventory = ctf_inventory_runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?;
                append_portfolio_close_receipts_to_journal(
                    clearances,
                    receipt,
                    accounting.payout_vector.as_ref(),
                    &portfolio_snapshot,
                    &ctf_inventory,
                )
            },
        ) {
            tracing::error!(
                target: "app.reclaim_merge",
                closed_at_ms = accounting.closed_at_ms,
                error = %err,
                "terminal_portfolio_clearance_failed"
            );
            terminal_submissions.insert(key, TerminalSubmission::Accounting(accounting));
            return false;
        }
    }
    terminal_submissions.insert(key, TerminalSubmission::Complete);
    match key.kind {
        ReclaimKind::Merge => tracing::info!(
            target: "app.reclaim_merge",
            closed_at_ms = accounting.closed_at_ms,
            condition_id = %condition_id_str,
            tx = %tx,
            "merge_resolved"
        ),
        ReclaimKind::Redeem => tracing::info!(
            target: "app.reclaim_merge",
            closed_at_ms = accounting.closed_at_ms,
            condition_id = %condition_id_str,
            tx = %tx,
            "redeem_resolved"
        ),
    }
    obs.emit_action(ActionEvent::ReclaimResolved {
        ts_ms: accounting.closed_at_ms,
        kind: match key.kind {
            ReclaimKind::Merge => ReclaimActionKind::Merge,
            ReclaimKind::Redeem => ReclaimActionKind::Redeem,
        },
        conditions: 1,
        tx: Some(tx),
    });
    true
}

pub fn spawn_reclaim_task(
    cfg: &AppConfig,
    exec: ExecutionClient,
    obs: crate::observability::Observability,
    data: DataClient,
    portfolio_close: PortfolioCloseHandle,
    ctf_inventory_runtime: Arc<Mutex<CtfInventoryRuntime>>,
    mut merge_commands_rx: mpsc::Receiver<Vec<CtfTerminalMergeCommand>>,
    settlement_account_refresh_tx: mpsc::Sender<()>,
    shutdown: CancellationToken,
) -> JoinHandle<Result<()>> {
    let user = exec.polymarket_tracked_wallet();
    let retry_cooldown = Duration::from_secs(cfg.polymarket.execution.reclaim_retry_cooldown_secs);
    let periodic_wake = Duration::from_secs(60);
    let live_ops = exec.reclaim_ops_handle();
    let reclaim_route = exec.reclaim_route_label().unwrap_or("direct_redeem");
    live_ops.record_redeem_enabled(reclaim_route);
    let reclaim_trigger = exec.reclaim_trigger();

    tokio::spawn(async move {
        let mut last_attempt_at: HashMap<ReclaimAttemptKey, Instant> = HashMap::new();
        let mut pending_merge_commands = Vec::<CtfTerminalMergeCommand>::new();
        let mut terminal_submissions = HashMap::new();
        let mut reconciliation_error = None;
        let mut merge_commands_open = true;
        let mut draining = shutdown.is_cancelled();
        let mut refresh_requested = !draining;
        let mut merge_processing_requested = false;
        let mut redeem_processing_requested = false;
        let mut mergeable_lookup_pending = false;
        let mut redeemable_lookup_pending = false;
        let mut mergeable_positions = Arc::new(Vec::<Position>::new());
        let mut redeemable_positions = Arc::new(Vec::<Position>::new());
        let mut discovery_work =
            FuturesUnordered::<BoxFuture<'static, (PositionScope, Result<Vec<Position>>)>>::new();
        let mut active_work = FuturesUnordered::<ReclaimOperationFuture>::new();
        if draining {
            merge_commands_rx.close();
        }
        loop {
            let pending_accounting_keys = terminal_submissions
                .iter()
                .filter_map(|(key, submission)| {
                    matches!(submission, TerminalSubmission::Accounting(_)).then_some(*key)
                })
                .collect::<Vec<_>>();
            for key in pending_accounting_keys {
                process_confirmed_accounting(
                    key,
                    &mut terminal_submissions,
                    &obs,
                    &portfolio_close,
                    &ctf_inventory_runtime,
                    &settlement_account_refresh_tx,
                );
            }
            if merge_processing_requested {
                merge_processing_requested = false;
                process_pending_merge_commands(
                    &mut pending_merge_commands,
                    &mut terminal_submissions,
                    &last_attempt_at,
                    retry_cooldown,
                    &reclaim_trigger,
                    reclaim_route,
                    &live_ops,
                    &exec,
                    &portfolio_close,
                    &ctf_inventory_runtime,
                    &mergeable_positions,
                    &active_work,
                );
            }
            if redeem_processing_requested && !draining {
                redeem_processing_requested = false;
                for condition_id in terminal_condition_ids(&[], &redeemable_positions) {
                    let key = ReclaimAttemptKey {
                        condition_id,
                        kind: ReclaimKind::Redeem,
                    };
                    if terminal_submissions.contains_key(&key)
                        || condition_has_pending_submission(&terminal_submissions, condition_id)
                        || (mergeable_lookup_pending
                            && pending_merge_commands
                                .iter()
                                .any(|command| command.intent.condition_id == condition_id))
                    {
                        continue;
                    }
                    if let Some(retry_in) =
                        reclaim_retry_remaining(last_attempt_at.get(&key), retry_cooldown)
                    {
                        schedule_reclaim_retry(&reclaim_trigger, retry_in);
                        continue;
                    }
                    let merged_condition = terminal_submissions.contains_key(&ReclaimAttemptKey {
                        condition_id,
                        kind: ReclaimKind::Merge,
                    });
                    terminal_submissions.insert(key, TerminalSubmission::Pending);
                    let exec = exec.clone();
                    let live_ops = live_ops.clone();
                    let portfolio_close = portfolio_close.clone();
                    let mergeable_positions = mergeable_positions.clone();
                    let redeemable_positions = redeemable_positions.clone();
                    active_work.push(Box::pin(async move {
                        let mut completion = ReclaimOperationCompletion {
                            key,
                            command: None,
                            requests: Vec::new(),
                            payout_vector: None,
                            attempted_at: None,
                            result: Ok(None),
                        };
                        let condition_id_str = condition_id.to_string();
                        let preparation: Result<Option<_>> = async {
                            let payout_snapshot = portfolio_close.snapshot();
                            let Some(payout_vector) = validated_payout_vector_for_condition(
                                &exec,
                                condition_id,
                                &mergeable_positions,
                                &redeemable_positions,
                                &payout_snapshot,
                            )
                            .await?
                            else {
                                return Ok(None);
                            };
                            let portfolio_snapshot = portfolio_close.snapshot();
                            let (redeem_claims, requests) = prepare_redeem_for_condition(
                                &exec,
                                condition_id,
                                &redeemable_positions,
                                &payout_vector,
                                &portfolio_snapshot,
                            )
                            .await?;
                            live_ops.record_redeem_claims(
                                &condition_id_str,
                                reclaim_route,
                                redeem_claims,
                            );
                            if merged_condition && requests.is_empty() {
                                return Ok(None);
                            }
                            let mut preview = portfolio_snapshot.clone();
                            preview.close_redeemed_positions(&requests)?;
                            Ok(Some((requests, payout_vector)))
                        }
                        .await;
                        match preparation {
                            Ok(Some((requests, payout_vector))) => {
                                completion.requests = requests;
                                completion.payout_vector = Some(payout_vector);
                            }
                            Ok(None) => return completion,
                            Err(error) => {
                                completion.result = Err(error);
                                return completion;
                            }
                        }
                        completion.attempted_at = Some(Instant::now());
                        live_ops.record_redeem_pending(&condition_id_str, reclaim_route);
                        live_ops.record_redeem_tx_submit_attempt(&condition_id_str);
                        completion.result = exec.redeem_binary_conditions(&[condition_id]).await;
                        completion
                    }));
                }
            }
            if refresh_requested {
                for (scope, lookup_pending) in [
                    (PositionScope::Mergeable, &mut mergeable_lookup_pending),
                    (PositionScope::Redeemable, &mut redeemable_lookup_pending),
                ] {
                    if *lookup_pending || (draining && matches!(scope, PositionScope::Redeemable)) {
                        continue;
                    }
                    let data = data.clone();
                    discovery_work.push(Box::pin(async move {
                        (scope, fetch_complete_positions(&data, user, scope).await)
                    }));
                    *lookup_pending = true;
                }
                refresh_requested = false;
            }
            if draining
                && !merge_commands_open
                && discovery_work.is_empty()
                && active_work.is_empty()
            {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled(), if !draining => {
                    draining = true;
                    merge_commands_rx.close();
                    refresh_requested = !pending_merge_commands.is_empty();
                }
                _ = reclaim_trigger.notified(), if !draining => {
                    refresh_requested = true;
                }
                _ = tokio::time::sleep(periodic_wake), if !draining => {
                    refresh_requested = true;
                }
                next = merge_commands_rx.recv(), if merge_commands_open => {
                    match next {
                        Some(commands) => {
                            if let Err(error) = retain_terminal_merge_commands(
                                &mut pending_merge_commands,
                                &terminal_submissions,
                                commands,
                            ) {
                                tracing::error!(
                                    target: "app.reclaim_merge",
                                    error = %error,
                                    "terminal_merge_command_rejected"
                                );
                                reconciliation_error = Some(error);
                            }
                            refresh_requested = true;
                        }
                        None => {
                            if !draining {
                                tracing::error!(
                                    target: "app.reclaim_merge",
                                    "terminal_merge_command_channel_closed"
                                );
                            }
                            merge_commands_open = false;
                        }
                    }
                }
                Some(completion) = active_work.next(), if !active_work.is_empty() => {
                    let ReclaimOperationCompletion {
                        key,
                        command,
                        mut requests,
                        payout_vector,
                        attempted_at,
                        result,
                    } = completion;
                    if let Some(attempted_at) = attempted_at {
                        last_attempt_at.insert(key, attempted_at);
                    }
                    let condition_id_str = key.condition_id.to_string();
                    let operation = match key.kind {
                        ReclaimKind::Merge => "merge",
                        ReclaimKind::Redeem => "redeem",
                    };
                    match result {
                        Ok(Some(CtfSettlementConfirmation::Confirmed { receipt })) => {
                            let tx = receipt.tx_hash.clone();
                            let closed_at_ms = now_ms();
                            attach_close_confirmation(&mut requests, closed_at_ms, &tx);
                            terminal_submissions.insert(
                                key,
                                TerminalSubmission::Accounting(PendingConfirmedAccounting {
                                    receipt,
                                    requests,
                                    payout_vector,
                                    closed_at_ms,
                                }),
                            );
                            live_ops.record_redeem_tx_status(
                                &condition_id_str,
                                reclaim_route,
                                "confirmed",
                                Some(tx),
                            );
                            let admitted = process_confirmed_accounting(
                                key,
                                &mut terminal_submissions,
                                &obs,
                                &portfolio_close,
                                &ctf_inventory_runtime,
                                &settlement_account_refresh_tx,
                            );
                            if !admitted || key.kind == ReclaimKind::Merge {
                                schedule_reclaim_retry(
                                    &reclaim_trigger,
                                    Duration::from_millis(1),
                                );
                            }
                        }
                        Ok(Some(CtfSettlementConfirmation::ReconciliationRequired {
                            tx_hash,
                            error,
                        })) => {
                            reconciliation_error = Some(anyhow::anyhow!(
                                "{operation} reconciliation required condition_id={condition_id_str} tx_hash={tx_hash}: {error}"
                            ));
                            live_ops.record_redeem_tx_status(
                                &condition_id_str,
                                reclaim_route,
                                "reconciliation_required",
                                Some(tx_hash.clone()),
                            );
                            tracing::error!(
                                target: "app.reclaim_merge",
                                condition_id = %condition_id_str,
                                operation,
                                tx_hash = %tx_hash,
                                error = %error,
                                "terminal_reclaim_reconciliation_required"
                            );
                        }
                        result => {
                            terminal_submissions.remove(&key);
                            if let Some(command) = command {
                                pending_merge_commands.push(command);
                            }
                            if let Err(error) = result {
                                let err_detail = format!("{error:#}");
                                if attempted_at.is_some() {
                                    schedule_reclaim_retry(&reclaim_trigger, retry_cooldown);
                                    live_ops.record_redeem_failed(
                                        &condition_id_str,
                                        reclaim_route,
                                        err_detail.clone(),
                                    );
                                }
                                tracing::error!(
                                    target: "app.reclaim_merge",
                                    condition_id = %condition_id_str,
                                    operation,
                                    error = %err_detail,
                                    "terminal_reclaim_failed"
                                );
                            }
                        }
                    }
                    if key.kind == ReclaimKind::Redeem
                        && !condition_has_pending_submission(&terminal_submissions, key.condition_id)
                        && pending_merge_commands.iter().any(|command| {
                            command.intent.condition_id == key.condition_id
                        })
                    {
                        merge_processing_requested = true;
                    }
                }
                Some((scope, result)) = discovery_work.next(), if !discovery_work.is_empty() => {
                    match scope {
                        PositionScope::Mergeable => mergeable_lookup_pending = false,
                        PositionScope::Redeemable => redeemable_lookup_pending = false,
                    }
                    let positions = match result {
                        Ok(positions) => Arc::new(positions),
                        Err(error) => {
                            let scope_label = match scope {
                                PositionScope::Mergeable => "mergeable",
                                PositionScope::Redeemable => "redeemable",
                            };
                            live_ops.record_redeem_poll_error(format!(
                                "{scope_label}_positions_fetch_failed err={error}"
                            ));
                            tracing::error!(
                                target: "app.reclaim_merge",
                                scope = scope_label,
                                error = %error,
                                "reclaim_positions_fetch_failed"
                            );
                            if !draining {
                                schedule_reclaim_retry(&reclaim_trigger, retry_cooldown);
                            }
                            continue;
                        }
                    };
                    match scope {
                        PositionScope::Mergeable => {
                            mergeable_positions = positions;
                            merge_processing_requested = true;
                        }
                        PositionScope::Redeemable => {
                            live_ops.record_redeem_poll_ok();
                            redeemable_positions = positions;
                        }
                    }
                    redeem_processing_requested = !redeemable_lookup_pending;
                }
            }
        }
        let pending_accounting = terminal_submissions
            .values()
            .filter(|value| matches!(value, TerminalSubmission::Accounting(_)))
            .count();
        let pending_operations = terminal_submissions
            .values()
            .filter(|value| matches!(value, TerminalSubmission::Pending))
            .count();
        anyhow::ensure!(
            pending_merge_commands.is_empty()
                && pending_accounting == 0
                && pending_operations == 0
                && reconciliation_error.is_none(),
            "reclaim shutdown has unresolved obligations merge_commands={} accounting={} operations={} reconciliation={:?}",
            pending_merge_commands.len(),
            pending_accounting,
            pending_operations,
            reconciliation_error
        );
        Ok(())
    })
}

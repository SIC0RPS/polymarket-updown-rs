use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::stream::FuturesUnordered;
use polymarket_client_sdk::data::types::response::Position;

use crate::ctf_inventory::CtfInventoryRuntime;
use crate::execution::{ExecutionClient, LiveOpsHandle};
use crate::portfolio::PortfolioCloseHandle;

use super::close_accounting::{merge_amount_units_6dp, merge_close_requests_for_command};
use super::command::{
    condition_has_pending_submission, CtfTerminalMergeCommand, ReclaimAttemptKey,
    ReclaimOperationCompletion, ReclaimOperationFuture, TerminalSubmission,
};
use super::tx_clearance::{reclaim_retry_remaining, schedule_reclaim_retry};

pub(super) fn process_pending_merge_commands(
    pending: &mut Vec<CtfTerminalMergeCommand>,
    terminal_submissions: &mut HashMap<ReclaimAttemptKey, TerminalSubmission>,
    last_attempt_at: &HashMap<ReclaimAttemptKey, Instant>,
    retry_cooldown: Duration,
    reclaim_trigger: &Arc<tokio::sync::Notify>,
    reclaim_route: &str,
    live_ops: &LiveOpsHandle,
    exec: &ExecutionClient,
    portfolio_close: &PortfolioCloseHandle,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    mergeable_positions: &[Position],
    active_work: &FuturesUnordered<ReclaimOperationFuture>,
) {
    for command in std::mem::take(pending) {
        let merge_key = command.attempt_key();
        let condition_id = command.intent.condition_id;
        let condition_id_str = condition_id.to_string();
        if terminal_submissions.contains_key(&merge_key) {
            continue;
        }
        if condition_has_pending_submission(terminal_submissions, condition_id) {
            pending.push(command);
            continue;
        }
        if let Some(retry_in) =
            reclaim_retry_remaining(last_attempt_at.get(&merge_key), retry_cooldown)
        {
            schedule_reclaim_retry(reclaim_trigger, retry_in);
            pending.push(command);
            continue;
        }
        let portfolio_snapshot = portfolio_close.snapshot();
        let preparation = ctf_inventory_runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))
            .and_then(|ctf_inventory| {
                let Some(amount) = merge_amount_units_6dp(
                    &command,
                    mergeable_positions,
                    &portfolio_snapshot,
                    &ctf_inventory,
                )?
                else {
                    return Ok(None);
                };
                let requests = merge_close_requests_for_command(
                    &command,
                    mergeable_positions,
                    &portfolio_snapshot,
                    &ctf_inventory,
                    0,
                    None,
                )?;
                let mut preview = portfolio_snapshot.clone();
                preview.close_redeemed_positions(&requests)?;
                Ok(Some((amount, requests)))
            });
        let (amount_units_6dp, requests) = match preparation {
            Ok(Some(prepared)) => prepared,
            Ok(None) => {
                pending.push(command);
                continue;
            }
            Err(err) => {
                tracing::error!(
                    target: "app.reclaim_merge",
                    symbol = command.symbol.as_str(),
                    condition_id = %condition_id_str,
                    error = %err,
                    "merge_command_verification_failed"
                );
                pending.push(command);
                continue;
            }
        };
        terminal_submissions.insert(merge_key, TerminalSubmission::Pending);
        let exec = exec.clone();
        let live_ops = live_ops.clone();
        let reclaim_route = reclaim_route.to_string();
        active_work.push(Box::pin(async move {
            let attempted_at = Some(Instant::now());
            live_ops.record_redeem_pending(&condition_id_str, &reclaim_route);
            live_ops.record_redeem_tx_submit_attempt(&condition_id_str);
            let result = exec
                .merge_binary_conditions(&[(condition_id, amount_units_6dp)])
                .await;
            ReclaimOperationCompletion {
                key: merge_key,
                command: Some(command),
                requests,
                payout_vector: None,
                attempted_at,
                result,
            }
        }));
    }
}

use crate::app::ctf_inventory::CtfSplitCommand;
use anyhow::Result;
use polymarket_hft::ctf_inventory::CtfInventoryRuntime;
use polymarket_hft::execution::supervisor::ExecutionIntentBatch;
use polymarket_hft::reclaim_merge::CtfTerminalMergeCommand;
use polymarket_hft::types::OrderIntent;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueueDispatchOutcome {
    HandedOff,
    ReceiverClosed,
}

pub(super) enum IntentBatchDispatchOutcome {
    HandedOff,
    ReceiverClosed(Vec<OrderIntent>),
}

pub(super) async fn dispatch_intent_batch(
    intents_tx: &mpsc::UnboundedSender<ExecutionIntentBatch>,
    batch: Vec<OrderIntent>,
    decision_ts_ms: u64,
    decision_instant: Instant,
) -> IntentBatchDispatchOutcome {
    let handoff_instant = Instant::now();
    match intents_tx.send(ExecutionIntentBatch {
        decision_ts_ms,
        decision_instant,
        handoff_instant,
        intents: batch,
    }) {
        Ok(()) => IntentBatchDispatchOutcome::HandedOff,
        Err(error) => IntentBatchDispatchOutcome::ReceiverClosed(error.0.intents),
    }
}

pub(super) async fn dispatch_cancel_requests(
    cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    cancel_requests: Vec<polymarket_hft::strategy::CancelRequest>,
) -> QueueDispatchOutcome {
    match cancel_requests_tx.send(cancel_requests) {
        Ok(()) => QueueDispatchOutcome::HandedOff,
        Err(_) => QueueDispatchOutcome::ReceiverClosed,
    }
}

pub(super) async fn dispatch_split_commands(
    split_tx: &mpsc::Sender<Vec<CtfSplitCommand>>,
    commands: Vec<CtfSplitCommand>,
) -> QueueDispatchOutcome {
    match split_tx.try_send(commands) {
        Ok(()) => QueueDispatchOutcome::HandedOff,
        Err(TrySendError::Closed(_)) => QueueDispatchOutcome::ReceiverClosed,
        Err(TrySendError::Full(commands)) => {
            tracing::warn!(
                target: "app.bbwick_runtime",
                batch_size = commands.len(),
                "bbwick_split_queue_congested"
            );
            match split_tx.send(commands).await {
                Ok(()) => QueueDispatchOutcome::HandedOff,
                Err(_) => QueueDispatchOutcome::ReceiverClosed,
            }
        }
    }
}

pub(super) fn release_reserved_splits(
    runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    commands: &[CtfSplitCommand],
) -> Result<()> {
    let now_ms = polymarket_hft::event_ledger::now_ms();
    let mut runtime = runtime
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?;
    for command in commands {
        runtime.release_reserved_split(&command.intent, now_ms)?;
    }
    Ok(())
}

pub(super) async fn dispatch_terminal_merge_commands(
    merge_tx: &mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    commands: Vec<CtfTerminalMergeCommand>,
) -> QueueDispatchOutcome {
    match merge_tx.try_send(commands) {
        Ok(()) => QueueDispatchOutcome::HandedOff,
        Err(TrySendError::Closed(_)) => QueueDispatchOutcome::ReceiverClosed,
        Err(TrySendError::Full(commands)) => {
            tracing::warn!(
                target: "app.bbwick_runtime",
                batch_size = commands.len(),
                "bbwick_terminal_merge_queue_congested"
            );
            match merge_tx.send(commands).await {
                Ok(()) => QueueDispatchOutcome::HandedOff,
                Err(_) => QueueDispatchOutcome::ReceiverClosed,
            }
        }
    }
}

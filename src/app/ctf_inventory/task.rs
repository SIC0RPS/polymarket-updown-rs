use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use polymarket_hft::engine_trade_journal::CtfSplitEntryJournal;
use polymarket_hft::runtime_control::SharedRuntimeConfigState;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::*;

const CTF_INVENTORY_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(1);

type CtfInventoryFuture =
    Pin<Box<dyn Future<Output = (CtfSplitCommand, CtfInventoryObservation)> + Send>>;

#[allow(clippy::too_many_arguments)]
fn start_inventory_work(
    exec: ExecutionClient,
    runtime: Arc<Mutex<CtfInventoryRuntime>>,
    pause_rx: watch::Receiver<bool>,
    runtime_control_rx: watch::Receiver<RuntimeConfigState>,
    market_projection_rx: watch::Receiver<MarketProjection>,
    shared_strategy_cfg: SharedStrategyConfig,
    shutdown: CancellationToken,
    command: CtfSplitCommand,
    work: CtfInventoryWork,
) -> CtfInventoryFuture {
    Box::pin(async move {
        let observation = match work {
            CtfInventoryWork::Submit => {
                let paused = *pause_rx.borrow() || shutdown.is_cancelled();
                let result = submit_inventory_command(
                    &exec,
                    &runtime,
                    &shared_strategy_cfg,
                    paused,
                    &runtime_control_rx,
                    &market_projection_rx,
                    &command,
                )
                .await
                .map_err(|error| format!("{error:#}"));
                CtfInventoryObservation::Submission(result)
            }
            CtfInventoryWork::PollReceipt { tx_hash } => {
                let status = exec
                    .split_transaction_receipt_status(command.intent.condition_id, &tx_hash)
                    .await
                    .map_err(|error| format!("{error:#}"));
                CtfInventoryObservation::Receipt(CtfReceiptPollObservation { tx_hash, status })
            }
            CtfInventoryWork::Retry => unreachable!("CTF retry work remains in command storage"),
        };
        (command, observation)
    })
}

fn release_unstarted_split(
    runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    intent: &InventoryIntent,
) -> Result<bool> {
    let now_ms = polymarket_hft::event_ledger::now_ms();
    let mut runtime = runtime
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?;
    if runtime.next_action(intent, now_ms)? != CtfInventoryAction::BeginSubmission {
        return Ok(false);
    }
    runtime.release_reserved_split(intent, now_ms)?;
    Ok(true)
}

#[derive(Debug, Clone)]
pub(crate) struct CtfSplitCommand {
    pub(crate) intent: InventoryIntent,
    pub(crate) entry_journal: CtfSplitEntryJournal,
}

impl CtfSplitCommand {
    pub(crate) fn new(
        intent: InventoryIntent,
        entry_journal: CtfSplitEntryJournal,
    ) -> Result<Self> {
        entry_journal.validate_split_intent(&intent)?;
        Ok(Self {
            intent,
            entry_journal,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_ctf_inventory_task(
    exec: ExecutionClient,
    portfolio_hydration: PortfolioHydrationHandle,
    runtime: Arc<Mutex<CtfInventoryRuntime>>,
    runtime_config_state: SharedRuntimeConfigState,
    market_projection_rx: watch::Receiver<MarketProjection>,
    shared_strategy_cfg: SharedStrategyConfig,
    mut intents_rx: mpsc::Receiver<Vec<CtfSplitCommand>>,
    capture: MaybeCapture,
    shutdown: CancellationToken,
) -> Result<JoinHandle<Result<()>>> {
    Ok(tokio::spawn(async move {
        let pause_rx = runtime_config_state.subscribe_pause();
        let runtime_control_rx = runtime_config_state.subscribe();
        let mut retry_commands: HashMap<(InventoryOperation, String), CtfSplitCommand> =
            HashMap::new();
        let mut receipt_poll = tokio::time::interval(CTF_INVENTORY_REFRESH_RETRY_INTERVAL);
        receipt_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut active_work = FuturesUnordered::<CtfInventoryFuture>::new();
        let advance =
            |mut command: CtfSplitCommand,
             mut observation: Option<CtfInventoryObservation>,
             mut retrying: bool,
             draining: bool,
             active_work: &FuturesUnordered<CtfInventoryFuture>,
             retry_commands: &mut HashMap<(InventoryOperation, String), CtfSplitCommand>|
             -> Result<()> {
                let key = (
                    command.intent.operation,
                    command.intent.condition_id.to_string(),
                );
                if !retrying && retry_commands.contains_key(&key) {
                    return Ok(());
                }
                loop {
                    let paused = draining || *pause_rx.borrow() || shutdown.is_cancelled();
                    match execute_inventory_intent(
                        &exec,
                        &portfolio_hydration,
                        paused,
                        &runtime,
                        &capture,
                        &command,
                        observation.take(),
                        retrying,
                    ) {
                        Ok(Some(CtfInventoryWork::Retry)) => {
                            retry_commands.insert(key, command);
                            return Ok(());
                        }
                        Ok(Some(work)) => {
                            let mut future = start_inventory_work(
                                exec.clone(),
                                runtime.clone(),
                                pause_rx.clone(),
                                runtime_control_rx.clone(),
                                market_projection_rx.clone(),
                                shared_strategy_cfg.clone(),
                                shutdown.clone(),
                                command,
                                work,
                            );
                            // Start the reservation before accepting another command for this split.
                            match future.as_mut().now_or_never() {
                                Some((completed_command, completed_observation)) => {
                                    command = completed_command;
                                    observation = Some(completed_observation);
                                    retrying = true;
                                }
                                None => {
                                    active_work.push(future);
                                    return Ok(());
                                }
                            }
                        }
                        Ok(None) => return Ok(()),
                        Err(error) => {
                            tracing::warn!(
                                target: "app.ctf_inventory",
                                market_id = %command.intent.market_id,
                                condition_id = %command.intent.condition_id,
                                operation = command.intent.operation.as_str(),
                                error = %error,
                                "ctf_inventory_operation_failed"
                            );
                            if retrying {
                                retry_commands.insert(key, command);
                            } else if let Err(release_error) =
                                release_unstarted_split(&runtime, &command.intent)
                            {
                                retry_commands.insert(key, command);
                                return Err(error.context(format!(
                                    "failed to release unstarted split: {release_error:#}"
                                )));
                            }
                            return Err(error);
                        }
                    }
                }
            };
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                Some((command, observation)) = active_work.next(), if !active_work.is_empty() => {
                    let _ = advance(
                        command,
                        Some(observation),
                        true,
                        false,
                        &active_work,
                        &mut retry_commands,
                    );
                }
                next = intents_rx.recv() => {
                    let Some(batch) = next else { break };
                    for command in batch {
                        let _ = advance(
                            command,
                            None,
                            false,
                            false,
                            &active_work,
                            &mut retry_commands,
                        );
                    }
                }
                _ = receipt_poll.tick(), if !retry_commands.is_empty() => {
                    for (_, command) in std::mem::take(&mut retry_commands) {
                        let _ = advance(
                            command,
                            None,
                            true,
                            false,
                            &active_work,
                            &mut retry_commands,
                        );
                    }
                }
            }
        }
        intents_rx.close();
        let mut failures = Vec::new();
        let mut queued_commands = true;
        while queued_commands || !active_work.is_empty() || !retry_commands.is_empty() {
            tokio::select! {
                next = intents_rx.recv(), if queued_commands => {
                    match next {
                        Some(batch) => {
                            for command in batch {
                                if let Err(error) = advance(
                                    command,
                                    None,
                                    false,
                                    true,
                                    &active_work,
                                    &mut retry_commands,
                                ) {
                                    failures.push(format!("release queued split: {error:#}"));
                                }
                            }
                        }
                        None => queued_commands = false,
                    }
                }
                Some((command, observation)) = active_work.next(), if !active_work.is_empty() => {
                    if let Err(error) = advance(
                        command,
                        Some(observation),
                        true,
                        true,
                        &active_work,
                        &mut retry_commands,
                    ) {
                        failures.push(format!("finish active split work: {error:#}"));
                    }
                }
                _ = receipt_poll.tick(), if !retry_commands.is_empty() => {
                    for (_, command) in std::mem::take(&mut retry_commands) {
                        if let Err(error) = advance(
                            command,
                            None,
                            true,
                            true,
                            &active_work,
                            &mut retry_commands,
                        ) {
                            failures.push(format!("finish split financial admission: {error:#}"));
                        }
                    }
                }
            }
        }
        let unmaterialized = runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
            .unmaterialized_split_gross_inventory_units_6dp()?;
        if unmaterialized != 0 {
            failures.push(format!(
                "CTF shutdown has unresolved gross inventory units_6dp={unmaterialized}"
            ));
        }
        anyhow::ensure!(
            failures.is_empty(),
            "CTF shutdown failed: {}",
            failures.join("; ")
        );
        Ok(())
    }))
}

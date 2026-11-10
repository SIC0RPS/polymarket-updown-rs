use anyhow::{Context, Result};
use polymarket_hft::capture::events::{
    StrategyConsumptionInput, StrategyConsumptionKind, StrategyConsumptionRecord,
    STRATEGY_CONSUMPTION_SCHEMA_VERSION,
};
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::oracle_resolution::ChainlinkResolutionPrice;
use polymarket_hft::polymarket::rtds_chainlink::{
    spawn_rtds_chainlink_runtime, RtdsChainlinkRuntime,
};
use polymarket_hft::strategy_runtime::actors::MakerRebateMmActorSet;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(super) struct ChainlinkCaptureRuntime {
    shutdown: CancellationToken,
    runtime_task: JoinHandle<()>,
    receiver: mpsc::Receiver<ChainlinkResolutionPrice>,
    receiver_open: bool,
}

impl ChainlinkCaptureRuntime {
    pub(super) fn start(shutdown: CancellationToken) -> Self {
        let chainlink_shutdown = shutdown.child_token();
        let RtdsChainlinkRuntime {
            receiver,
            task: runtime_task,
        } = spawn_rtds_chainlink_runtime(chainlink_shutdown.clone());
        Self {
            shutdown: chainlink_shutdown,
            runtime_task,
            receiver,
            receiver_open: true,
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.receiver_open
    }

    pub(super) async fn recv(&mut self) -> Option<ChainlinkResolutionPrice> {
        self.receiver.recv().await
    }

    pub(super) fn observe(
        &mut self,
        run_id: &str,
        received: Option<ChainlinkResolutionPrice>,
        actors: &mut MakerRebateMmActorSet,
        cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
        capture: &MaybeCapture,
        consumption_sequence: &mut u64,
    ) -> Result<()> {
        let Some(price) = received else {
            self.receiver_open = false;
            tracing::error!(
                target: "polymarket.rtds_chainlink",
                "rtds_chainlink_receiver_closed"
            );
            return Ok(());
        };
        actors.refresh_views(price.observed_ts_ms)?;
        let actor = match actors.actor_for_chainlink_symbol_mut(&price.symbol) {
            Ok(actor) => actor,
            Err(err) => {
                tracing::error!(
                    target: "oracle.resolution_label",
                    symbol = %price.symbol,
                    error = %err,
                    "chainlink_resolution_missing_symbol_actor"
                );
                self.receiver_open = false;
                self.shutdown.cancel();
                return Ok(());
            }
        };
        let actor_symbol = polymarket_hft::strategy_runtime::SymbolId::parse(actor.symbol)?;
        let learning_applied_ts_ms = crate::app::bootstrap::now_ms();
        *consumption_sequence = consumption_sequence
            .checked_add(1)
            .context("strategy consumption sequence overflow")?;
        let current_consumption_sequence = *consumption_sequence;
        let consumed_at_ms = crate::app::bootstrap::now_ms();
        let consumption = capture.reserve_strategy_consumption(
            actor_symbol,
            StrategyConsumptionKind::Chainlink,
            current_consumption_sequence,
            consumed_at_ms,
        ).map(|permit| (permit, actor.odds_snapshot()));
        let observe_result = actor
            .strategy
            .observe_chainlink_resolution_price(&price, learning_applied_ts_ms);
        let observe_result = match consumption {
            Some((permit, odds)) => match observe_result.and_then(|labels| {
                actor.changed_consumed_candle_state(StrategyConsumptionKind::Chainlink)
                    .map(|state| (labels, state))
            }) {
                Ok((labels, candle_state)) => {
                    permit.send(StrategyConsumptionRecord {
                        schema_version: STRATEGY_CONSUMPTION_SCHEMA_VERSION,
                        consumption_sequence: current_consumption_sequence,
                        consumed_at_ms,
                        consumed_mono_ns: actor.strategy.consumed_candle_mono_ns(),
                        trade_flow: actor.strategy.consumed_trade_flow_snapshot(),
                        candle_state: candle_state.clone(),
                        actor_symbol,
                        odds,
                        input: StrategyConsumptionInput::Chainlink {
                            price,
                            learning_applied_ts_ms,
                        },
                    });
                    actor.acknowledge_captured_candle_state(
                        StrategyConsumptionKind::Chainlink, candle_state,
                    );
                    capture.finish_required_capture(false);
                    Ok(labels)
                }
                Err(error) => {
                    drop(permit);
                    capture.finish_required_capture(true);
                    Err(error)
                }
            },
            None => observe_result,
        };
        let observe_result = observe_result.and_then(|labels| {
            polymarket_hft::trade_session_ledger::append_oracle_resolved_labels(run_id, &labels)?;
            Ok(labels)
        });
        match observe_result {
            Ok(labels) => {
                if !labels.is_empty() {
                    if let Err(err) = actor.persist_oracle_learning(learning_applied_ts_ms) {
                        tracing::error!(
                            target: "oracle.resolution_label",
                            symbol = actor.symbol,
                            error = %err,
                            "oracle_learning_continuation_write_failed"
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "oracle.resolution_label",
                    error = %err,
                    "chainlink_resolution_label_failed_closed"
                );
                actor.cancel_only = true;
                let cancel_requests = actor.fault_cancel_requests();
                if !cancel_requests.is_empty() {
                    cancel_requests_tx
                        .send(cancel_requests)
                        .map_err(|_| anyhow::anyhow!("symbol actor fault cancel queue closed"))?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn join(self) {
        let Self {
            shutdown,
            runtime_task,
            receiver,
            ..
        } = self;
        shutdown.cancel();
        drop(receiver);
        let _ = runtime_task.await;
    }
}

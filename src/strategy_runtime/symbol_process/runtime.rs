use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::B256;
use sha2::{Digest, Sha256};

use super::protocol::OracleResolvedLabelRecord;
use super::{
    strategy_action_to_intent_record, Drive, DriveKind, EntryContextRecord, Finalize, IntentRecord,
    OwnerAuditRecord, ResultIdentity,
};
use crate::capture::events::StrategyConsumptionInput;
use crate::config::OptimizerParameterSchema;
use crate::orders::OpenOrdersState;
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::portfolio::PortfolioState;
use crate::risk::RiskBudgetSnapshot;
use crate::strategy::facts::StrategyFact;
use crate::strategy_runtime::actors::MakerRebateMmActor;
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::replay_mode::PackedReplayTape;
use crate::strategy_runtime::runtime_driver::{RuntimeDriver, RuntimeInput};

#[path = "runtime/candidate.rs"]
mod candidate;
#[path = "runtime/direct.rs"]
mod direct;
#[path = "runtime/helpers.rs"]
mod helpers;
#[path = "runtime/output.rs"]
mod output;
#[path = "runtime/prediction.rs"]
mod prediction;
#[path = "runtime/replay_resolution.rs"]
mod replay_resolution;
#[path = "runtime/views.rs"]
mod views;

use helpers::owner_audit_record_from_fact;

const MAX_DRIVER_DECISIONS: usize = 512;

pub(crate) struct CandidateSymbolRuntime {
    candidate_slot: u32,
    mode: super::ProtocolMode,
    symbol: super::SymbolId,
    base: super::WorkerBaseSnapshot,
    base_sha256: [u8; 32],
    parameter_schema: OptimizerParameterSchema,
    tape: Arc<PackedReplayTape>,
    catalog: Arc<MarketCatalog>,
    ownership: SymbolOwnershipIndex,
    candidate: Option<CandidateState>,
    worker_epoch: u32,
    next_intent_id: u64,
    next_owner_audit_seq: u64,
    last_global_sequence: Option<u64>,
    last_central_state_seq: u64,
    local_event_count: u64,
}

struct CandidateState {
    candidate_sha256: [u8; 32],
    actor: MakerRebateMmActor,
    prediction: Option<prediction::PredictionCollector>,
    portfolio: PortfolioState,
    open_orders: OpenOrdersState,
    risk: RiskBudgetSnapshot,
    pending_risk: Option<RiskBudgetSnapshot>,
    last_tick: Option<crate::types::MarketTick>,
    position_revision_by_token: HashMap<u32, u64>,
    order_revision_by_handle: HashMap<u64, u64>,
    lifecycle_revision_by_identity: HashMap<(u32, u32, u8, u8), u64>,
    split_reason_target_per_side: f64,
}

#[derive(Default)]
struct WorkerRuntimeOutput {
    intents: Vec<IntentRecord>,
    entry_contexts: Vec<EntryContextRecord>,
    owner_audits: Vec<OwnerAuditRecord>,
    oracle_labels: Vec<OracleResolvedLabelRecord>,
}

impl CandidateSymbolRuntime {
    fn process_consumption_output(
        &mut self,
        input: &StrategyConsumptionInput,
    ) -> Result<WorkerRuntimeOutput> {
        let output = match input {
            StrategyConsumptionInput::Runtime(input) => {
                let allow_not_due_heartbeat = matches!(input, RuntimeInput::Heartbeat { .. });
                self.drive_runtime(input.clone(), allow_not_due_heartbeat)
            }
            StrategyConsumptionInput::Chainlink {
                price,
                learning_applied_ts_ms,
            } => self.process_consumed_chainlink(price, *learning_applied_ts_ms),
        }?;
        if tracing::enabled!(
            target: "strategy.oracle_probability",
            tracing::Level::TRACE
        ) {
            self.trace_oracle_resolved_labels(&output.oracle_labels);
        }
        Ok(output)
    }

    fn trace_oracle_resolved_labels(&self, labels: &[OracleResolvedLabelRecord]) {
        for label in labels {
            let Some(market) = self.catalog.record(label.market_id) else {
                continue;
            };
            tracing::trace!(
                target: "strategy.oracle_probability",
                event = "oracle_resolved_label",
                market_id = %B256::from(market.condition_id),
                winner_up = label.winner == 1,
                window_start_ms = label.window_start_ms,
                window_end_ms = label.window_end_ms,
                release_ts_ms = label.release_ts_ms,
                open_price_e18 = %label.open_price_e18,
                open_source_ts_ms = label.open_source_ts_ms,
                open_observed_ts_ms = label.open_observed_ts_ms,
                open_capture_sequence = label.open_capture_sequence,
                close_price_e18 = %label.close_price_e18,
                close_source_ts_ms = label.close_source_ts_ms,
                close_observed_ts_ms = label.close_observed_ts_ms,
                close_capture_sequence = label.close_capture_sequence,
            );
        }
    }

    fn process_drive_output(&mut self, drive: Drive, now_ms: u64) -> Result<WorkerRuntimeOutput> {
        let Some(last_tick) = self
            .candidate
            .as_ref()
            .context("worker has no candidate state")?
            .last_tick
            .clone()
        else {
            return Ok(WorkerRuntimeOutput::default());
        };
        let last_tick_ms = u64::try_from(last_tick.ts_ns)
            .unwrap_or(0)
            .saturating_div(1_000_000);
        let now_ingest_mono_ns = last_tick.ingest_mono_ns.saturating_add(
            now_ms
                .saturating_sub(last_tick_ms)
                .saturating_mul(1_000_000),
        );
        let input = match drive.kind {
            DriveKind::PortfolioChanged => RuntimeInput::PortfolioChanged {
                last_tick,
                now_ms,
                now_ingest_mono_ns,
            },
            DriveKind::LiveStep => anyhow::bail!(
                "live step requires a worker-local live feed event; no live feed owner is bound"
            ),
        };
        self.drive_runtime(input, false)
    }

    fn drive_runtime(
        &mut self,
        mut input: RuntimeInput,
        allow_not_due_heartbeat: bool,
    ) -> Result<WorkerRuntimeOutput> {
        let catalog = self.catalog.clone();
        let source_symbol = self.symbol;
        let symbol = self.symbol.as_str();
        let oracle_symbol = self
            .base
            .oracle_config
            .resolve_symbol_for_underlying(symbol);
        let candidate = self.candidate_mut()?;
        anyhow::ensure!(
            candidate.pending_risk.is_none(),
            "worker risk view is not decision-ready"
        );
        candidate.last_tick = Some(input.tick().clone());
        let split_reason_target_per_side = candidate.split_reason_target_per_side;
        let actor = &mut candidate.actor;
        let outcome = RuntimeDriver::process(
            &mut actor.strategy,
            &mut actor.fact_sink,
            &mut actor.runtime_state,
            &mut actor.status_state,
            &mut input,
            MAX_DRIVER_DECISIONS,
        )?;
        let Some(mut outcome) = outcome else {
            anyhow::ensure!(
                allow_not_due_heartbeat,
                "accepted symbol runtime input produced no output"
            );
            return Ok(WorkerRuntimeOutput::default());
        };
        actor.cancel_only |= actor.runtime_state.control.tick_kill_switch;
        let scope = actor.command_scope();
        anyhow::ensure!(
            outcome
                .decisions
                .iter()
                .all(|decision| scope.action_belongs(&decision.action)),
            "symbol actor emitted a foreign action"
        );
        if actor.cancel_only {
            outcome
                .decisions
                .retain(|decision| scope.allowed_while_cancel_only(&decision.action));
            let mut cancellations = actor.fault_cancel_decisions();
            cancellations.append(&mut outcome.decisions);
            outcome.decisions = cancellations;
        }
        let intents = outcome
            .decisions
            .iter()
            .map(|decision| strategy_action_to_intent_record(&decision.action, &catalog))
            .collect::<Result<Vec<_>>>()?;
        let entry_contexts = outcome
            .decisions
            .iter()
            .enumerate()
            .filter_map(|(index, decision)| {
                let is_buy = matches!(
                    &decision.action,
                    crate::strategy::StrategyAction::Order(crate::types::OrderIntent::Buy(_))
                );
                let split = decision
                    .context
                    .as_ref()
                    .and_then(crate::engine_trade_journal::ctf_split_entry_decision_context);
                if !is_buy && split.is_none() {
                    return None;
                }
                Some((|| -> Result<EntryContextRecord> {
                    let ordinal = u64::try_from(index)?
                        .checked_add(1)
                        .context("worker entry intent ordinal overflow")?;
                    let mut intent = intents[index];
                    intent.intent_id = ordinal;
                    let context = if let Some(context) = split {
                        EntryContextRecord::Split(super::intent::decision_context_record(
                            &context,
                            decision.reason.as_str(),
                            split_reason_target_per_side,
                            false,
                            oracle_symbol.as_str(),
                            intents[index],
                            ordinal,
                            &catalog,
                        )?)
                    } else {
                        let crate::strategy::StrategyAction::Order(order) = &decision.action else {
                            unreachable!("BUY action checked above")
                        };
                        let mut authorization = *order
                            .buy_authorization()
                            .context("worker BUY proposal lost entry authorization")?;
                        anyhow::ensure!(
                            authorization.intent_id == 0 && authorization.authorized_at_ms == 0,
                            "strategy BUY proposal assigned central authorization"
                        );
                        authorization.intent_id = ordinal;
                        EntryContextRecord::DirectBuy(authorization)
                    };
                    context.validate_intent(intent, &catalog, source_symbol)
                })())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut owner_audits = outcome
            .facts
            .iter()
            .map(|fact| owner_audit_record_from_fact(fact, &catalog, 0))
            .collect::<Result<Vec<_>>>()?;
        for (decision_index, decision) in outcome.decisions.iter().enumerate() {
            let Some(crate::strategy::StrategyDecisionContext::CtfOwnedLegSell(fact)) =
                decision.context.as_ref()
            else {
                continue;
            };
            let execution_intent_ordinal = u64::try_from(decision_index)
                .context("worker execution intent ordinal overflow")?
                .checked_add(1)
                .context("worker execution intent ordinal overflow")?;
            actor
                .fact_sink
                .push(StrategyFact::CtfOwnedLegOutcome(fact.clone()));
            for published_fact in actor.fact_sink.drain() {
                owner_audits.push(owner_audit_record_from_fact(
                    &published_fact,
                    &catalog,
                    execution_intent_ordinal,
                )?);
            }
        }
        Ok(WorkerRuntimeOutput {
            intents,
            entry_contexts,
            owner_audits,
            oracle_labels: Vec::new(),
        })
    }

    fn finalize_identity(&mut self, finalize: Finalize) -> Result<ResultIdentity> {
        let candidate = self
            .candidate
            .as_ref()
            .context("worker has no active candidate state")?;
        anyhow::ensure!(
            candidate.pending_risk.is_none(),
            "worker risk view is not decision-ready at finalize"
        );
        let candidate_sha256 = candidate.candidate_sha256;
        anyhow::ensure!(
            candidate_sha256 == finalize.candidate_sha256,
            "worker finalize candidate state mismatch"
        );
        let mut artifact = Vec::with_capacity(149);
        artifact.extend_from_slice(b"PMWORKER-RESULT-1");
        artifact.extend_from_slice(self.symbol.as_str().as_bytes());
        artifact.extend_from_slice(&candidate_sha256);
        artifact.extend_from_slice(&finalize.last_global_sequence.to_le_bytes());
        artifact.extend_from_slice(&finalize.last_central_state_seq.to_le_bytes());
        artifact.extend_from_slice(&finalize.local_event_count.to_le_bytes());
        artifact.extend_from_slice(&self.base_sha256);
        artifact.extend_from_slice(&self.parameter_schema.sha256());
        artifact.extend_from_slice(&self.tape.prepared_payload_sha256());
        artifact.extend_from_slice(&self.catalog.sha256());
        Ok(ResultIdentity {
            candidate_sha256: finalize.candidate_sha256,
            last_global_sequence: finalize.last_global_sequence,
            last_central_state_seq: finalize.last_central_state_seq,
            local_event_count: finalize.local_event_count,
            artifact_sha256: Sha256::digest(&artifact).into(),
            artifact_size: u64::try_from(artifact.len())
                .context("worker result identity overflow")?,
            status: 1,
        })
    }

    fn candidate_mut(&mut self) -> Result<&mut CandidateState> {
        self.candidate
            .as_mut()
            .context("worker has no active candidate state")
    }
}

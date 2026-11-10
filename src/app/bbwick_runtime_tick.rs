use super::bbwick_runtime_dispatch::{
    dispatch_cancel_requests, dispatch_intent_batch, dispatch_split_commands,
    dispatch_terminal_merge_commands, release_reserved_splits, IntentBatchDispatchOutcome,
    QueueDispatchOutcome,
};
use crate::app::ctf_inventory::risk_reservation::CentralCtfRiskViews;
use crate::app::ctf_inventory::CtfSplitCommand;
use anyhow::{Context, Result};
use polymarket_hft::capture::events::{
    StrategyConsumptionInput, StrategyConsumptionKind, StrategyConsumptionRecord,
    STRATEGY_CONSUMPTION_SCHEMA_VERSION,
};
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::AppConfig;
use polymarket_hft::ctf_inventory::{CtfInventoryAction, CtfInventoryRuntime, CtfInventoryWait};
use polymarket_hft::engine_trade_journal::{
    build_order_terminal_record, ctf_split_entry_decision_context, CtfSplitEntryJournal,
};
use polymarket_hft::execution::supervisor::ExecutionIntentBatch;
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::observability::action::ActionEvent;
use polymarket_hft::orders::OrderTerminalProvenance;
use polymarket_hft::reclaim_merge::CtfTerminalMergeCommand;
use polymarket_hft::strategy::{
    facts::StrategyFact, InventoryOperation, StrategyAction, StrategyDecisionContext,
};
use polymarket_hft::strategy_runtime::actors::MakerRebateMmActor;
use polymarket_hft::strategy_runtime::driver::strategy_status_emit_sig;
use polymarket_hft::strategy_runtime::runtime_driver::{RuntimeDriver, RuntimeInput};
use polymarket_hft::strategy_runtime::SymbolId;
use polymarket_hft::types::OrderIntent;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Default)]
pub(super) struct StrategyStatusEmitState {
    last_sig: Option<u64>,
    last_emit_ms: u64,
}

pub(super) async fn handle_tick(
    cfg: &AppConfig,
    run_id: &str,
    actor: &mut MakerRebateMmActor,
    central_command_ownership: &polymarket_hft::strategy_runtime::actors::CentralSymbolCommandOwnership,
    _exec: &ExecutionClient,
    intents_tx: &mpsc::UnboundedSender<ExecutionIntentBatch>,
    split_tx: &mpsc::Sender<Vec<CtfSplitCommand>>,
    ctf_inventory_runtime: &Arc<Mutex<CtfInventoryRuntime>>,
    ctf_risk_views: &CentralCtfRiskViews,
    terminal_merge_tx: &mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    cancel_requests_tx: &mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    pause: bool,
    mut input: RuntimeInput,
    obs: &polymarket_hft::observability::Observability,
    capture: &MaybeCapture,
    strategy_status_emit_state: &mut StrategyStatusEmitState,
    managed_tokens_by_symbol: &mut std::collections::BTreeMap<
        String,
        std::collections::HashSet<String>,
    >,
    next_execution_intent_id: &mut u64,
    consumption_sequence: &mut u64,
    cycle_admission: &mut polymarket_hft::strategy_runtime::actors::CentralCycleAdmission,
    wall_ms: u64,
) -> Result<Option<String>> {
    let actor_symbol = actor.symbol.to_string();
    let actor_symbol_id = SymbolId::parse(&actor_symbol)?;
    let fault_cancel_requests = actor.fault_cancel_requests();
    let actor_scope = actor.command_scope();
    *consumption_sequence = consumption_sequence
        .checked_add(1)
        .context("strategy consumption sequence overflow")?;
    let current_consumption_sequence = *consumption_sequence;
    let consumed_at_ms = polymarket_hft::event_ledger::now_ms();
    let consumption_permit = capture.reserve_strategy_consumption(
        actor_symbol_id,
        StrategyConsumptionKind::Runtime,
        current_consumption_sequence,
        consumed_at_ms,
    );
    let consumed_odds = actor.odds_snapshot();
    let consumed_odds_revision = actor.odds_revision();
    let strategy_input_kind = match &input {
        RuntimeInput::AcceptedTick { .. } => "accepted_tick",
        RuntimeInput::Heartbeat { .. } => "heartbeat",
        RuntimeInput::PortfolioChanged { .. } => "portfolio_changed",
        RuntimeInput::PolymarketDataChanged { .. } => "polymarket_data_changed",
    };
    let driven = {
        let MakerRebateMmActor {
            strategy,
            fact_sink: strategy_fact_sink,
            runtime_state: strategy_runtime_state,
            status_state: strategy_status_state,
            ..
        } = actor;
        RuntimeDriver::process(
            strategy,
            strategy_fact_sink,
            strategy_runtime_state,
            strategy_status_state,
            &mut input,
            512,
        )
    };
    let driven = match driven {
        Ok(driven) => driven,
        Err(error) => {
            if let Some(permit) = consumption_permit {
                drop(permit);
                capture.finish_required_capture(true);
            }
            return Err(error);
        }
    };
    let decision_instant = Instant::now();
    if driven.is_some() {
        actor.acknowledge_odds_revision(consumed_odds_revision);
    }
    if let Some(permit) = consumption_permit {
        let candle_state =
            match actor.changed_consumed_candle_state(StrategyConsumptionKind::Runtime) {
                Ok(state) => state,
                Err(error) => {
                    drop(permit);
                    capture.finish_required_capture(true);
                    return Err(error);
                }
            };
        permit.send(StrategyConsumptionRecord {
            schema_version: STRATEGY_CONSUMPTION_SCHEMA_VERSION,
            consumption_sequence: current_consumption_sequence,
            consumed_at_ms,
            consumed_mono_ns: actor.strategy.consumed_candle_mono_ns(),
            trade_flow: actor.strategy.consumed_trade_flow_snapshot(),
            candle_state: candle_state.clone(),
            actor_symbol: actor_symbol_id,
            odds: Arc::clone(&consumed_odds),
            input: StrategyConsumptionInput::Runtime(input),
        });
        actor.acknowledge_captured_candle_state(StrategyConsumptionKind::Runtime, candle_state);
        capture.finish_required_capture(false);
    }
    let Some(driven) = driven else {
        return Ok(None);
    };
    let MakerRebateMmActor {
        fact_sink: strategy_fact_sink,
        runtime_state: strategy_runtime_state,
        cancel_only,
        ..
    } = actor;
    polymarket_hft::trade_session_ledger::append_owned_leg_outcome_facts(
        run_id,
        &actor_symbol,
        &driven.facts,
        None,
    )?;
    *cancel_only |= strategy_runtime_state.control.tick_kill_switch;
    if !driven.has_runtime_output() {
        return Ok(None);
    }
    managed_tokens_by_symbol.insert(actor_symbol.clone(), driven.runtime_managed_paired_tokens);
    obs.set_runtime_managed_paired_tokens(
        managed_tokens_by_symbol
            .values()
            .flat_map(|tokens| tokens.iter().cloned())
            .collect(),
    );
    let mut drained = driven.decisions;
    let foreign_action = drained.iter().fold(false, |foreign, decision| {
        let belongs = actor_scope.action_belongs(&decision.action)
            && central_command_ownership.action_belongs(&actor_symbol, &decision.action);
        foreign || !belongs
    });
    if foreign_action {
        *cancel_only = true;
        tracing::error!(
            target: "app.bbwick_runtime",
            symbol = %actor_symbol,
            "symbol_actor_emitted_foreign_action"
        );
        drained.retain(|decision| {
            actor_scope.action_belongs(&decision.action)
                && central_command_ownership.action_belongs(&actor_symbol, &decision.action)
                && actor_scope.allowed_while_cancel_only(&decision.action)
        });
    }

    if let Some(st) = driven.status {
        let status_sig = strategy_status_emit_sig(&st);
        let should_emit = strategy_status_emit_state.last_sig != Some(status_sig)
            || st
                .ts_ms
                .saturating_sub(strategy_status_emit_state.last_emit_ms)
                >= 1_000;
        if should_emit {
            strategy_status_emit_state.last_sig = Some(status_sig);
            strategy_status_emit_state.last_emit_ms = st.ts_ms;
            obs.emit_ui(ActionEvent::StrategyStatus {
                ts_ms: st.ts_ms,
                kind: st.kind,
                status: st.msg,
                p_up: st.p_up,
                edge: st.edge,
                gate_drop_oracle: st.gate_drops_cycle.oracle,
                gate_drop_book: st.gate_drops_cycle.book,
                gate_drop_stale: st.gate_drops_cycle.stale,
                gate_drop_open_delay: st.gate_drops_cycle.open_delay,
                gate_drop_open_price: st.gate_drops_cycle.open_price,
                gate_drop_entry_time: st.gate_drops_cycle.entry_time,
                gate_drop_blocked_market: st.gate_drops_cycle.blocked_market,
                gate_drop_markout_toxicity: st.gate_drops_cycle.markout_toxicity,
                gate_drop_oracle_total: st.gate_drops_total.oracle,
                gate_drop_book_total: st.gate_drops_total.book,
                gate_drop_stale_total: st.gate_drops_total.stale,
                gate_drop_open_delay_total: st.gate_drops_total.open_delay,
                gate_drop_open_price_total: st.gate_drops_total.open_price,
                gate_drop_entry_time_total: st.gate_drops_total.entry_time,
                gate_drop_blocked_market_total: st.gate_drops_total.blocked_market,
                gate_drop_markout_toxicity_total: st.gate_drops_total.markout_toxicity,
                selection_status: st.selection_status,
                activity: st.activity,
                recompute_reason: st.recompute_reason,
                queued: st.queued,
                drained: st.drained,
                ranked: st.ranked,
                managed_exits: st.managed_exits,
                quote_blocks: st.quote_blocks,
                primary_block_detail: st.primary_block_detail,
                shock_toxicity: st.shock_toxicity,
                markout_toxicity: st.markout_toxicity,
                markout_samples: st.markout_samples,
                oracle_wait_reason: st.oracle_wait_reason,
            });
        }
    }
    for event in driven.action_events {
        obs.emit_action(event);
    }

    let mut malformed_output = drained.iter().any(|decision| {
        matches!(
            (&decision.action, &decision.context),
            (StrategyAction::Order(OrderIntent::Buy(buy)), context)
                if context.is_some() || buy.authorization.is_none()
        ) || matches!(
            (&decision.action, &decision.context),
            (StrategyAction::Order(OrderIntent::Sell(_)), context)
                if !matches!(context, Some(StrategyDecisionContext::CtfOwnedLegSell(_)))
        ) || matches!(
            (&decision.action, &decision.context),
            (action, Some(StrategyDecisionContext::CtfOwnedLegSell(_)))
                if !matches!(action, StrategyAction::Order(OrderIntent::Sell(_)))
        )
    });
    let mut split_commands = Vec::new();
    let mut terminal_merge_commands = Vec::new();
    drained = drained
        .into_iter()
        .filter_map(|decision| {
            let polymarket_hft::strategy::StrategyDecision {
                action: StrategyAction::Inventory(intent),
                context,
                p_up,
                edge,
                reason,
            } = decision
            else {
                return Some(decision);
            };
            let blocked =
                *cancel_only || (pause && intent.operation == InventoryOperation::SplitCompleteSet);
            if !blocked && !malformed_output {
                obs.emit_action(ActionEvent::Signal {
                    ts_ms: wall_ms,
                    side: intent.operation.as_str().to_string(),
                    p_up,
                    edge,
                    reason: reason.clone(),
                });
                tracing::info!(
                    target: "app.bbwick_runtime",
                    market_id = %intent.market_id,
                    condition_id = %intent.condition_id,
                    operation = intent.operation.as_str(),
                    amount_units_6dp = intent.amount_units_6dp,
                    "ctf_inventory_intent"
                );
            }
            let prepared = (|| -> Result<()> {
                match intent.operation {
                    InventoryOperation::SplitCompleteSet => {
                        let decision_context = context
                            .as_ref()
                            .and_then(ctf_split_entry_decision_context)
                            .context("mandatory CTF split decision context is missing")?;
                        let mut entry = CtfSplitEntryJournal {
                            market_id: Some(intent.market_id.to_string()),
                            ..CtfSplitEntryJournal::default()
                        };
                        entry.tokens.push(intent.up_token.clone());
                        entry.tokens.push(intent.down_token.clone());
                        entry.reasons.push(reason);
                        entry.decisions.push(decision_context);
                        split_commands.push(CtfSplitCommand::new(intent, entry)?);
                    }
                    InventoryOperation::MergeCompleteSet => {
                        terminal_merge_commands
                            .push(CtfTerminalMergeCommand::new(actor_symbol_id, intent)?);
                    }
                }
                Ok(())
            })();
            if let Err(error) = prepared {
                malformed_output = true;
                tracing::error!(
                    target: "app.bbwick_runtime",
                    symbol = %actor_symbol,
                    error = %error,
                    "symbol_actor_emitted_malformed_inventory_command"
                );
            }
            None
        })
        .collect();
    if malformed_output {
        *cancel_only = true;
        tracing::error!(
            target: "app.bbwick_runtime",
            symbol = %actor_symbol,
            "symbol_actor_emitted_malformed_output"
        );
    }
    if *cancel_only {
        drained.retain(|decision| actor_scope.allowed_while_cancel_only(&decision.action));
        split_commands.clear();
        terminal_merge_commands.clear();
    } else if pause {
        split_commands.clear();
    }

    let mut batch: Vec<OrderIntent> = Vec::with_capacity(drained.len());
    let mut cancel_requests = if *cancel_only {
        fault_cancel_requests.clone()
    } else {
        Vec::new()
    };
    for decision in drained {
        let polymarket_hft::strategy::StrategyDecision {
            mut action,
            context,
            p_up,
            edge,
            reason,
        } = decision;
        debug_assert!(actor_scope.action_belongs(&action));
        if (*cancel_only && !actor_scope.allowed_while_cancel_only(&action))
            || (pause && matches!(&action, StrategyAction::Order(OrderIntent::Buy(_))))
        {
            continue;
        }
        let admission_reservation = match &action {
            StrategyAction::Inventory(_) => None,
            _ => match cycle_admission.reserve(&action) {
                Ok(reservation) => reservation,
                Err(()) => continue,
            },
        };
        let side = match &action {
            StrategyAction::Order(order) => match order.side() {
                polymarket_hft::types::Side::Buy => "buy",
                polymarket_hft::types::Side::Sell => "sell",
            },
            StrategyAction::Inventory(intent) => intent.operation.as_str(),
            StrategyAction::CancelMarket(_) | StrategyAction::CancelQuotes(_) => "cancel",
        };

        obs.emit_action(ActionEvent::Signal {
            ts_ms: wall_ms,
            side: side.to_string(),
            p_up,
            edge,
            reason: reason.clone(),
        });

        let execution_intent_id = *next_execution_intent_id;
        if let StrategyAction::Order(order) = &mut action {
            anyhow::ensure!(
                order.common().execution_intent_id.is_none(),
                "symbol strategy assigned central execution intent identity"
            );
            *next_execution_intent_id = execution_intent_id
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("live execution intent identity overflow"))?;
            order.common_mut().execution_intent_id = Some(execution_intent_id);
        }
        if matches!(&action, StrategyAction::Order(OrderIntent::Buy(_)))
            && !ctf_risk_views.reserve_new_direct_buy(
                actor_symbol_id,
                &mut action,
                ctf_inventory_runtime,
                polymarket_hft::event_ledger::now_ms(),
                &actor_scope,
                central_command_ownership,
            )?
        {
            if let Some(reservation) = admission_reservation.as_ref() {
                reservation.release();
            }
            continue;
        }

        match action {
            StrategyAction::Order(mut order) => {
                let fact = match (&order, context) {
                    (
                        OrderIntent::Sell(_),
                        Some(StrategyDecisionContext::CtfOwnedLegSell(fact)),
                    ) => Some(fact),
                    (OrderIntent::Buy(_), None) => None,
                    _ => unreachable!("order context validated before admission"),
                };
                let source_market = order.market_id.as_ref().and_then(|market_id| {
                    consumed_odds
                        .markets
                        .iter()
                        .chain(consumed_odds.selection.iter())
                        .chain(consumed_odds.next_selection.iter())
                        .find(|market| market.market_id == market_id.as_str())
                });
                let fields = order.common_mut();
                fields.market_frame_received_at =
                    source_market.and_then(|market| market.market_frame_received_at);
                fields.market_state_published_at =
                    source_market.and_then(|market| market.market_state_published_at);
                fields.market_connection_session_id =
                    source_market.and_then(|market| market.market_connection_session_id);
                fields.market_frame_sequence =
                    source_market.and_then(|market| market.market_frame_sequence);
                fields.market_state_revision =
                    source_market.and_then(|market| market.market_state_revision);
                fields.strategy_consumption_sequence = Some(current_consumption_sequence);
                fields.strategy_input_kind = Some(strategy_input_kind);
                fields.decision_at = Some(decision_instant);
                if let Some(fact) = fact {
                    strategy_fact_sink.push(StrategyFact::CtfOwnedLegOutcome(fact));
                    polymarket_hft::trade_session_ledger::append_owned_leg_outcome_facts(
                        run_id,
                        &actor_symbol,
                        &strategy_fact_sink.drain(),
                        Some(execution_intent_id),
                    )?;
                }
                let token = cfg.polymarket.token_id.as_deref().unwrap_or("").trim();
                let token = if !token.is_empty() {
                    token.to_string()
                } else {
                    order.instrument.clone()
                };
                obs.emit_action(ActionEvent::OrderIntent {
                    ts_ms: wall_ms,
                    side: side.to_string(),
                    token: token.clone(),
                    px: order.limit_px,
                    size: order.qty,
                    time_in_force: order.time_in_force.as_str().to_string(),
                    post_only: order.post_only,
                    market_id: order.market_id.clone(),
                    close_hint: order.order_close_reason_str().map(str::to_string),
                    quote_group: order.quote_group.clone(),
                    lifecycle_cohort: order.lifecycle_cohort.clone(),
                });
                batch.push(order);
            }
            StrategyAction::Inventory(_) => {
                unreachable!("inventory commands prepared before admission")
            }
            StrategyAction::CancelMarket(request) => {
                cancel_requests.push(polymarket_hft::strategy::CancelRequest::Market(request))
            }
            StrategyAction::CancelQuotes(request) => {
                cancel_requests.push(polymarket_hft::strategy::CancelRequest::Quotes(request))
            }
        }
    }

    if !batch.is_empty() {
        if let IntentBatchDispatchOutcome::ReceiverClosed(failed_batch) =
            dispatch_intent_batch(intents_tx, batch, wall_ms, decision_instant).await
        {
            let observed_at_ms = polymarket_hft::event_ledger::now_ms();
            for intent in failed_batch {
                ctf_risk_views.abandon_unsubmitted_direct_buy(&intent, ctf_inventory_runtime)?;
                if let Some(fact) = polymarket_hft::execution::build_order_terminal_fact(
                    &intent,
                    None,
                    OrderTerminalProvenance::PreSubmissionAbandoned,
                    None,
                    None,
                    Some("intent_channel_closed"),
                    Some("app_intent_dispatch"),
                    Some("intent_channel_closed"),
                    observed_at_ms,
                )? {
                    let mut record = build_order_terminal_record(fact)?;
                    record.run_id = run_id.to_string();
                    polymarket_hft::engine_trade_journal::append(record)?;
                }
            }
            return Ok(Some("bbwick_intent_queue_closed".to_string()));
        }
    }
    if !cancel_requests.is_empty()
        && dispatch_cancel_requests(cancel_requests_tx, cancel_requests).await
            == QueueDispatchOutcome::ReceiverClosed
    {
        return Ok(Some("bbwick_cancel_queue_closed".to_string()));
    }
    if !split_commands.is_empty() {
        let mut handoff_commands = Vec::with_capacity(split_commands.len());
        let mut newly_reserved_commands = Vec::new();
        for command in split_commands {
            let now_ms = polymarket_hft::event_ledger::now_ms();
            let mut runtime = ctf_inventory_runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?;
            let action = match runtime.next_action(&command.intent, now_ms) {
                Ok(action) => action,
                Err(error) => {
                    drop(runtime);
                    release_reserved_splits(ctf_inventory_runtime, &newly_reserved_commands)?;
                    return Err(error);
                }
            };
            match action {
                CtfInventoryAction::Execute => {
                    match ctf_risk_views.reserve_new_split(
                        actor_symbol_id,
                        &command.intent,
                        &mut runtime,
                        now_ms,
                    ) {
                        Ok(true) => {
                            newly_reserved_commands.push(command.clone());
                            handoff_commands.push(command);
                        }
                        Ok(false) => {
                            tracing::debug!(
                                target: "app.bbwick_runtime",
                                market_id = %command.intent.market_id,
                                condition_id = %command.intent.condition_id,
                                "ctf_split_already_reserved"
                            );
                        }
                        Err(error) => {
                            drop(runtime);
                            release_reserved_splits(
                                ctf_inventory_runtime,
                                &newly_reserved_commands,
                            )?;
                            return Err(error);
                        }
                    }
                }
                CtfInventoryAction::PollReceipt { .. }
                | CtfInventoryAction::Materialize { .. }
                | CtfInventoryAction::PublishConfirmed(_) => {
                    tracing::debug!(
                        target: "app.bbwick_runtime",
                        market_id = %command.intent.market_id,
                        condition_id = %command.intent.condition_id,
                        "ctf_split_original_journal_retained_by_inventory_owner"
                    );
                }
                CtfInventoryAction::BeginSubmission
                | CtfInventoryAction::Wait(CtfInventoryWait::Pending)
                | CtfInventoryAction::Wait(CtfInventoryWait::Confirmed { .. })
                | CtfInventoryAction::Wait(CtfInventoryWait::FailedCooldown { .. })
                | CtfInventoryAction::Wait(CtfInventoryWait::ReconciliationRequired { .. }) => {
                    tracing::debug!(
                        target: "app.bbwick_runtime",
                        market_id = %command.intent.market_id,
                        condition_id = %command.intent.condition_id,
                        "ctf_split_handoff_deduplicated"
                    );
                }
            }
        }
        if !handoff_commands.is_empty()
            && dispatch_split_commands(split_tx, handoff_commands).await
                == QueueDispatchOutcome::ReceiverClosed
        {
            release_reserved_splits(ctf_inventory_runtime, &newly_reserved_commands)?;
            return Ok(Some("bbwick_split_queue_closed".to_string()));
        }
    }
    if !terminal_merge_commands.is_empty() {
        if dispatch_terminal_merge_commands(terminal_merge_tx, terminal_merge_commands).await
            == QueueDispatchOutcome::ReceiverClosed
        {
            return Ok(Some("bbwick_terminal_merge_queue_closed".to_string()));
        }
    }
    Ok(None)
}

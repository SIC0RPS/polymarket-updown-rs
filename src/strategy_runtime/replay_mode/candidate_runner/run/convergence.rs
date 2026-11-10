use super::super::clob_clock::DueClobEvent;
use super::*;
use crate::strategy_runtime::symbol_process::DecodedEntryContext;

pub(super) async fn converge_central_consequences(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    timestamp_ms: u64,
    mut pending_intents: Vec<DecodedIntent>,
) -> Result<()> {
    for _ in 0..FIXED_POINT_LIMIT {
        let mut order_intents = Vec::new();
        let mut cancel_requests = Vec::new();
        let mut affected_symbols = HashSet::new();
        for decoded in pending_intents.drain(..) {
            let DecodedIntent {
                intent_id,
                source_symbol,
                worker_epoch,
                action,
                entry_context,
            } = decoded;
            match action {
                StrategyAction::Order(order) => {
                    anyhow::ensure!(
                        intent_id > 0,
                        "candidate symbol-local execution intent identity is zero"
                    );
                    anyhow::ensure!(
                        worker_epoch > 0
                            && order.common().lifecycle_symbol.as_deref()
                                == Some(source_symbol.as_str()),
                        "candidate order source identity is foreign or incomplete"
                    );
                    anyhow::ensure!(
                        order
                            .common()
                            .execution_intent_id
                            .is_some_and(|intent_id| intent_id > 0),
                        "candidate central command owner did not assign execution intent identity"
                    );
                    match entry_context {
                        Some(DecodedEntryContext::DirectBuy(authorization)) => {
                            anyhow::ensure!(
                                order.buy_authorization() == Some(&authorization),
                                "candidate BUY order differs from central entry authorization"
                            );
                            let admitted = central
                                .ctf_inventory
                                .lock()
                                .map_err(|_| {
                                    anyhow::anyhow!("CTF inventory runtime lock poisoned")
                                })?
                                .reserve_direct_buy_for_symbol(
                                    &central.projection,
                                    source_symbol,
                                    &authorization,
                                    timestamp_ms,
                                )?;
                            if !admitted {
                                continue;
                            }
                            if let Err(block) = central.risk.reserve_inflight_order(&order, None) {
                                central
                                    .ctf_inventory
                                    .lock()
                                    .map_err(|_| {
                                        anyhow::anyhow!("CTF inventory runtime lock poisoned")
                                    })?
                                    .release_reserved_direct_buy(&authorization);
                                tracing::warn!(
                                    target: "strategy_runtime.candidate.central",
                                    symbol = source_symbol.as_str(),
                                    rule = %block.rule,
                                    details = %block.details,
                                    "candidate_direct_buy_admission_blocked"
                                );
                                continue;
                            }
                            central
                                .symbol_projections
                                .get(source_symbol.as_str())
                                .context("BUY central symbol projection is absent")?
                                .claim_cohort(
                                    order
                                        .lifecycle_cohort
                                        .as_deref()
                                        .context("candidate BUY order has no bound cohort")?,
                                )?;
                        }
                        None => anyhow::ensure!(
                            matches!(&order, crate::types::OrderIntent::Sell(_)),
                            "candidate BUY order has no central entry authorization"
                        ),
                        Some(DecodedEntryContext::Split(_)) => {
                            anyhow::bail!("candidate order carries split entry context");
                        }
                    }
                    order_intents.push(order);
                }
                StrategyAction::CancelMarket(request) => {
                    cancel_requests.push(CancelRequest::Market(request));
                }
                StrategyAction::CancelQuotes(request) => {
                    cancel_requests.push(CancelRequest::Quotes(request));
                }
                StrategyAction::Inventory(intent) => {
                    if let Some(symbol) = schedule_ctf_inventory_intent(
                        inputs,
                        central,
                        intent_id,
                        timestamp_ms,
                        intent,
                        entry_context,
                    )
                    .await?
                    {
                        affected_symbols.insert(symbol);
                    }
                }
            }
        }
        let had_commands = !order_intents.is_empty() || !cancel_requests.is_empty();
        if had_commands {
            central
                .process_supervisor_batch(
                    timestamp_ms,
                    Vec::new(),
                    Vec::new(),
                    cancel_requests,
                    order_intents,
                )
                .await?;
        }
        let submitted = central.execution_sim.take_submitted_orders_delta()?;
        let canceled = central.execution_sim.take_canceled_orders_delta()?;
        let submitted_ctf = central.execution_sim.take_submitted_ctf_ops_delta()?;
        anyhow::ensure!(
            submitted_ctf.is_empty(),
            "candidate supervisor submitted an inventory operation through the order path"
        );
        for order in submitted {
            affected_symbols.insert(SymbolId::parse(
                order
                    .intent
                    .lifecycle_symbol
                    .as_deref()
                    .context("submitted candidate order has no lifecycle symbol")?,
            )?);
            let market_itode = reserve_submitted_order_handle(inputs, central, &order)?;
            central
                .clob_submit_clock
                .schedule(order, timestamp_ms, market_itode, &central.odds)?;
        }
        let visible_cancellations = central.clob_submit_clock.cancel_pending(&canceled)?;
        for order_id in &canceled {
            let mut owners = central
                .symbol_projections
                .iter()
                .filter(|(_, projection)| projection.order_handle(order_id).is_ok())
                .map(|(symbol, _)| symbol);
            let symbol = owners.next().with_context(|| {
                format!("candidate canceled order has no projected owner order_id={order_id}")
            })?;
            anyhow::ensure!(
                owners.next().is_none(),
                "candidate canceled order has multiple projected owners order_id={order_id}"
            );
            affected_symbols.insert(SymbolId::parse(symbol)?);
        }
        if !visible_cancellations.is_empty() {
            central
                .exchange
                .cancel_orders(&visible_cancellations, timestamp_ms);
        }
        central.exchange.advance_time(timestamp_ms);

        affected_symbols.extend(publish_venue_facts(inputs, central, timestamp_ms, &[]).await?);
        let refresh_views = !affected_symbols.is_empty();
        if refresh_views {
            pending_intents.extend(send_admitted_views(
                slot,
                central,
                timestamp_ms,
                affected_symbols,
            )?);
        }
        if pending_intents.is_empty() && !refresh_views {
            return Ok(());
        }
    }
    anyhow::bail!(
        "candidate central consequence fixed point exceeded iterations={FIXED_POINT_LIMIT}"
    )
}

async fn publish_venue_facts(
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    timestamp_ms: u64,
    receipt_deadlines: &[(String, u64)],
) -> Result<HashSet<SymbolId>> {
    let mut affected_symbols = HashSet::new();
    let facts = central.exchange.drain_facts();
    let mut updates = Vec::new();
    let mut fills = Vec::new();
    for fact in facts {
        match fact {
            CapturedVenueFact::OrderUpdate(update) => updates.push(update),
            CapturedVenueFact::Fill(fill) => fills.push(fill),
        }
    }
    if updates.is_empty() && fills.is_empty() {
        return Ok(affected_symbols);
    }
    for update in &updates {
        affected_symbols.insert(symbol_for_condition(
            inputs,
            central,
            update.market.as_str(),
        )?);
    }
    let rejected_ctf_order_ids: Vec<String> = updates
        .iter()
        .filter(|update| {
            update.terminal_reason.as_deref() == Some(CONFIRMED_CTF_COHORT_INVENTORY_MISMATCH)
        })
        .map(|update| update.order_id.clone())
        .collect();
    let mut matched_fills = Vec::with_capacity(fills.len());
    for mut fill in fills {
        affected_symbols.insert(symbol_for_condition(
            inputs,
            central,
            fill.market_id
                .as_ref()
                .context("candidate venue fill has no market identity")?
                .as_str(),
        )?);
        if let Some((_, receipt_due_ts_ms)) = receipt_deadlines
            .iter()
            .find(|(order_id, _)| *order_id == fill.order_id)
        {
            let mut matched = fill.clone();
            matched.receipt_confirmed_at_ms = None;
            central
                .clob_submit_clock
                .retain_financial_receipt(fill, *receipt_due_ts_ms)?;
            matched_fills.push(matched);
        } else {
            fill.receipt_confirmed_at_ms = Some(timestamp_ms);
            apply_confirmed_fills(central, std::slice::from_ref(&fill), timestamp_ms)?;
            matched_fills.push(fill);
        }
    }
    central.portfolio_tx.send_replace(central.portfolio.clone());
    central
        .process_supervisor_batch(timestamp_ms, updates, matched_fills, Vec::new(), Vec::new())
        .await?;
    for order_id in rejected_ctf_order_ids {
        central.order_fill_context.mark_audit_complete(&order_id);
        central
            .order_fill_context
            .mark_execution_complete(&order_id);
    }
    Ok(affected_symbols)
}

pub(super) async fn drain_next_due_clob_batch(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    due_ts_ms: u64,
) -> Result<()> {
    let DueClobSubmissions {
        due_ts_ms: actual_due_ts_ms,
        events,
    } = central
        .clob_submit_clock
        .pop_next_due(due_ts_ms)?
        .context("candidate CLOB due clock changed while draining")?;
    anyhow::ensure!(
        actual_due_ts_ms == due_ts_ms,
        "candidate CLOB due timestamp changed while draining"
    );
    slot.observe_central_timestamp(due_ts_ms);
    let mut receipt_deadlines = Vec::new();
    let mut affected_symbols = HashSet::new();
    for event in events {
        match event {
            DueClobEvent::Submission {
                order,
                receipt_due_ts_ms,
            } => {
                let reject_for_inventory = is_ctf_market_entry_sell_intent(&order.intent)
                    && order
                        .intent
                        .lifecycle_cohort
                        .as_deref()
                        .is_none_or(|cohort| {
                            !central
                                .portfolio
                                .confirmed_ctf_cohort_qty_units_6dp(
                                    order.intent.instrument.as_str(),
                                    cohort,
                                )
                                .is_ok_and(|owned_qty_units_6dp| {
                                    order.intent.execution_instruction_qty_units_6dp > 0
                                        && order.intent.execution_instruction_qty_units_6dp
                                            <= owned_qty_units_6dp
                                })
                        });
                let consequence = if reject_for_inventory {
                    central.exchange.reject_order(
                        order.clone(),
                        due_ts_ms,
                        CONFIRMED_CTF_COHORT_INVENTORY_MISMATCH,
                    )
                } else {
                    central.exchange.submit_order(order.clone(), due_ts_ms)
                };
                central.apply_venue_submission_consequence(&order, consequence, due_ts_ms)?;
                if let Some(receipt_due_ts_ms) = receipt_due_ts_ms {
                    receipt_deadlines.push((order.order_id, receipt_due_ts_ms));
                }
            }
            DueClobEvent::FinancialReceipt(fill) => {
                affected_symbols.extend(
                    publish_venue_facts(inputs, central, due_ts_ms, &receipt_deadlines).await?,
                );
                receipt_deadlines.clear();
                affected_symbols.insert(symbol_for_condition(
                    inputs,
                    central,
                    fill.market_id
                        .as_ref()
                        .context("candidate financial receipt has no market identity")?
                        .as_str(),
                )?);
                apply_confirmed_fills(central, std::slice::from_ref(&fill), due_ts_ms)?;
                central.portfolio_tx.send_replace(central.portfolio.clone());
                central
                    .process_supervisor_batch(
                        due_ts_ms,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                    )
                    .await?;
            }
        }
    }
    central.exchange.advance_time(due_ts_ms);
    affected_symbols
        .extend(publish_venue_facts(inputs, central, due_ts_ms, &receipt_deadlines).await?);
    let intents = send_admitted_views(slot, central, due_ts_ms, affected_symbols)?;
    converge_central_consequences(slot, inputs, central, due_ts_ms, intents).await
}

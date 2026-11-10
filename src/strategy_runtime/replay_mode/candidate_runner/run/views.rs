use super::*;
use crate::ctf_inventory::CtfInventoryAction;
use crate::strategy_runtime::symbol_process::InventoryReceiptRecord;

pub(super) fn send_admitted_views(
    slot: &mut CandidateSlot,
    central: &mut CandidateCentralState,
    timestamp_ms: u64,
    affected_symbols: impl IntoIterator<Item = SymbolId>,
) -> Result<Vec<DecodedIntent>> {
    let affected_symbols = affected_symbols.into_iter().collect::<HashSet<_>>();
    let mut intents = Vec::new();
    for symbol in [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp] {
        if affected_symbols.contains(&symbol) && central.admitted.contains(symbol.as_str()) {
            let source_sequence = central.worker_source_sequences[symbol.as_str()];
            send_symbol_views(
                slot,
                central,
                symbol,
                source_sequence,
                timestamp_ms,
                true,
                true,
                &[],
            )?;
            let central_state_seq = central.central_sequences[symbol.as_str()];
            let output = slot.process_mut(symbol)?.drive(
                Drive {
                    source_sequence,
                    central_state_seq,
                    kind: DriveKind::PortfolioChanged,
                    flags: 0,
                },
                timestamp_ms,
            )?;
            let symbol_intents = central.accept_worker_output(output)?;
            for decoded in symbol_intents {
                let retain = match &decoded.action {
                    StrategyAction::Inventory(intent)
                        if intent.operation
                            == crate::strategy::InventoryOperation::SplitCompleteSet =>
                    {
                        let action = central
                            .ctf_inventory
                            .lock()
                            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                            .next_action(intent, timestamp_ms)?;
                        match action {
                            CtfInventoryAction::Execute => {
                                match reserve_ctf_split_intent(
                                    central,
                                    symbol,
                                    timestamp_ms,
                                    intent,
                                ) {
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
                                        false
                                    }
                                }
                            }
                            CtfInventoryAction::BeginSubmission
                            | CtfInventoryAction::PollReceipt { .. }
                            | CtfInventoryAction::Materialize { .. }
                            | CtfInventoryAction::PublishConfirmed(_)
                            | CtfInventoryAction::Wait(_) => false,
                        }
                    }
                    _ => true,
                };
                if retain {
                    intents.push(decoded);
                }
            }
        }
    }
    Ok(intents)
}

pub(super) fn send_symbol_views(
    slot: &mut CandidateSlot,
    central: &mut CandidateCentralState,
    symbol: SymbolId,
    source_sequence: u64,
    timestamp_ms: u64,
    publish_account_state: bool,
    include_receipts: bool,
    inventory_receipts: &[InventoryReceiptRecord],
) -> Result<()> {
    let symbol_name = symbol.as_str();
    anyhow::ensure!(
        publish_account_state || (!include_receipts && inventory_receipts.is_empty()),
        "receipts require a complete account view symbol={symbol_name}"
    );
    if !publish_account_state {
        return Ok(());
    }
    let open_orders_rx = central.supervisor()?.open_orders();
    let open_orders = open_orders_rx.borrow();
    let deltas = central
        .symbol_projections
        .get_mut(symbol_name)
        .with_context(|| format!("central projection missing symbol={symbol_name}"))?
        .project(&central.portfolio, &open_orders)?;
    drop(open_orders);
    if !deltas.positions.is_empty() {
        let sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
        slot.process_mut(symbol)?.apply_position_deltas(
            source_sequence,
            sequence,
            timestamp_ms,
            &deltas.positions,
        )?;
    }
    if !deltas.open_orders.is_empty() {
        let sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
        slot.process_mut(symbol)?.apply_open_order_deltas(
            source_sequence,
            sequence,
            timestamp_ms,
            &deltas.open_orders,
        )?;
    }
    if !deltas.active_lifecycles.is_empty() {
        let sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
        slot.process_mut(symbol)?.apply_active_lifecycle_deltas(
            source_sequence,
            sequence,
            timestamp_ms,
            &deltas.active_lifecycles,
        )?;
    }
    if include_receipts {
        send_managed_sell_receipts(slot, central, symbol, source_sequence, timestamp_ms)?;
    }
    if !inventory_receipts.is_empty() {
        let sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
        slot.process_mut(symbol)?.apply_inventory_receipts(
            source_sequence,
            sequence,
            timestamp_ms,
            inventory_receipts,
        )?;
    }
    let max_inventory_total = *central
        .max_inventory_total_by_symbol
        .get(symbol_name)
        .with_context(|| format!("normal capacity missing symbol={symbol_name}"))?;
    let symbol_projection = central
        .symbol_projections
        .get(symbol_name)
        .context("central symbol projection disappeared")?;
    let tape_catalog = central
        .catalogs
        .get(symbol_name)
        .context("central symbol catalog disappeared")?;
    let conditions = tape_catalog
        .records()
        .iter()
        .map(|record| B256::from(record.condition_id))
        .collect::<HashSet<_>>();
    let reserved_inventory_units_6dp = {
        let runtime = central
            .ctf_inventory
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?;
        symbol_projection.reserved_inventory_units_6dp(&runtime)?
    };
    let reserved_inventory_units_6dp = reserved_inventory_units_6dp
        .checked_add(
            central
                .risk
                .unresolved_direct_buy_qty_units_6dp_for_conditions(&conditions)?,
        )
        .context("candidate reserved inventory quantity overflow")?;
    central.refresh_account_equity(timestamp_ms)?;
    let mut budget = central.risk.current_snapshot().budget_for_underlying(
        symbol_name,
        symbol_projection.portfolio(),
        symbol_projection.open_orders(),
        &central.projection,
        max_inventory_total,
        timestamp_ms,
    )?;
    budget.subtract_reserved_inventory_units_6dp(reserved_inventory_units_6dp)?;
    let risk_sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
    let (risk, market_budgets) =
        transport::risk_records(&budget, tape_catalog, source_sequence, risk_sequence)?;
    slot.process_mut(symbol)?.apply_risk_budget(risk)?;
    let market_sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
    slot.process_mut(symbol)?.apply_market_budgets(
        source_sequence,
        market_sequence,
        timestamp_ms,
        &market_budgets,
    )?;
    let accounting = central.accounting.current();
    slot.process_mut(symbol)?.commit_views(
        source_sequence,
        timestamp_ms,
        symbol_projection.portfolio(),
        accounting.financial.financial_view(),
    )?;
    Ok(())
}

fn send_managed_sell_receipts(
    slot: &mut CandidateSlot,
    central: &mut CandidateCentralState,
    symbol: SymbolId,
    source_sequence: u64,
    timestamp_ms: u64,
) -> Result<()> {
    let symbol_name = symbol.as_str();
    let projection = central
        .symbol_projections
        .get(symbol_name)
        .context("managed-sell central projection missing")?;
    let catalog = central
        .catalogs
        .get(symbol_name)
        .context("managed-sell central catalog missing")?
        .clone();
    let mut records = Vec::new();
    let mut sent = Vec::new();
    for fill in central.portfolio.confirmed_lifecycle_sell_fills.values() {
        if central.sent_managed_receipts.contains(&fill.fill_id) {
            continue;
        }
        let condition = fill
            .market_id
            .as_str()
            .parse::<B256>()
            .context("managed-sell market identity is invalid")?;
        if catalog
            .market_handle_for_condition(condition.into())
            .is_none()
        {
            continue;
        }
        let order_handle = projection.order_handle(&fill.order_id)?;
        let fill_handle = central.next_fill_handle;
        central.next_fill_handle = fill_handle
            .checked_add(1)
            .context("candidate fill handle overflow")?;
        records.push(transport::managed_sell_receipt(
            fill,
            &catalog,
            order_handle,
            fill_handle,
        )?);
        sent.push(fill.fill_id.clone());
    }
    if records.is_empty() {
        return Ok(());
    }
    let sequence = next_central_sequence(&mut central.central_sequences, symbol)?;
    slot.process_mut(symbol)?.apply_managed_sell_receipts(
        source_sequence,
        sequence,
        timestamp_ms,
        &records,
    )?;
    central.sent_managed_receipts.extend(sent);
    Ok(())
}

pub(super) fn next_central_sequence(
    sequences: &mut BTreeMap<String, u64>,
    symbol: SymbolId,
) -> Result<u64> {
    let current = sequences
        .get_mut(symbol.as_str())
        .with_context(|| format!("central sequence missing symbol={}", symbol.as_str()))?;
    *current = current
        .checked_add(1)
        .context("candidate central-state sequence overflow")?;
    Ok(*current)
}

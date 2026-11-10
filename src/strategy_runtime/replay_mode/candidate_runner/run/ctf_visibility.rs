use super::*;
use crate::ctf_inventory::{CtfInventoryAction, CtfInventoryReceipt};

pub(super) async fn drain_next_due_ctf_split(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    due_ts_ms: u64,
) -> Result<()> {
    let visibility = central
        .ctf_chain
        .pop_next_due(due_ts_ms)?
        .context("candidate CTF due clock changed while draining")?;
    anyhow::ensure!(
        visibility.due_ts_ms == due_ts_ms,
        "candidate CTF due timestamp changed while draining"
    );
    slot.observe_central_timestamp(due_ts_ms);
    let source_sequence = central
        .worker_source_sequences
        .get(visibility.symbol.as_str())
        .copied()
        .with_context(|| {
            format!(
                "candidate worker source sequence missing symbol={}",
                visibility.symbol.as_str()
            )
        })?;
    let intents = materialize_due_ctf_split(slot, central, source_sequence, visibility)?;
    if !intents.is_empty() {
        converge_central_consequences(slot, inputs, central, due_ts_ms, intents).await?;
    }
    Ok(())
}

fn materialize_due_ctf_split(
    slot: &mut CandidateSlot,
    central: &mut CandidateCentralState,
    source_sequence: u64,
    visibility: CapturedCtfSplitVisibility,
) -> Result<Vec<DecodedIntent>> {
    let CapturedCtfSplitVisibility {
        symbol,
        intent_id,
        pending_ts_ms,
        due_ts_ms,
        intent,
        receipt,
        entry_journal,
    } = visibility;
    let tx_hash = match &receipt {
        CtfInventoryReceipt::Confirmed { tx_hash } => tx_hash.clone(),
        _ => unreachable!("captured CTF chain schedules confirmed receipts only"),
    };
    anyhow::ensure!(
        pending_ts_ms < due_ts_ms,
        "candidate split materialization does not follow submission"
    );
    central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .record_receipt(&intent, due_ts_ms, receipt)?;
    let action = central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .next_action(&intent, due_ts_ms)?;
    match action {
        CtfInventoryAction::Materialize {
            tx_hash: materialize_tx_hash,
        } => anyhow::ensure!(
            materialize_tx_hash == tx_hash,
            "candidate split materialization transaction identity mismatch"
        ),
        _ => anyhow::bail!("candidate split receipt did not reach materialization"),
    }

    let catalog = central.catalogs.get(symbol.as_str()).with_context(|| {
        format!(
            "candidate symbol catalog missing symbol={}",
            symbol.as_str()
        )
    })?;
    let condition_id: [u8; 32] = intent.condition_id.into();
    let market_handle = catalog
        .market_handle_for_condition(condition_id)
        .context("candidate split condition is missing from frozen symbol catalog")?;
    let market = catalog
        .record(market_handle)
        .context("candidate split market handle is missing from frozen symbol catalog")?;
    anyhow::ensure!(
        intent.market_id.as_str() == B256::from(market.condition_id).to_string()
            && intent.up_token == U256::from_be_bytes(market.up_token).to_string()
            && intent.down_token == U256::from_be_bytes(market.down_token).to_string(),
        "candidate split intent does not match frozen symbol catalog"
    );
    let market_window_end_ms = market.window_end_ms;
    let confirmed = central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .record_materialized(&intent, due_ts_ms, &tx_hash)?;
    let audit_record =
        build_confirmed_ctf_split_entry_record(&confirmed, symbol.as_str(), &entry_journal)?;
    let split = match audit_record.ctf_event.as_ref() {
        Some(crate::trade_session_ledger::CanonicalCtfEvent::SplitConfirmed(split)) => split,
        _ => anyhow::bail!("candidate confirmed split record is missing canonical split fact"),
    };
    central.ingest_audit_record(&audit_record)?;
    central
        .symbol_projections
        .get(symbol.as_str())
        .with_context(|| format!("central projection missing symbol={}", symbol.as_str()))?
        .claim_cohort(split.cohort_id.as_str())?;
    let mut expected =
        expected_ctf_positions_for_confirmed_split(split, confirmed.materialized_ts_ms)?;
    for position in &mut expected {
        position.end_time_ms = Some(market_window_end_ms);
    }
    let exact_balances = central
        .ctf_chain
        .split_position_balances_units_6dp(&intent)?;
    let onchain_balances = exact_balances.into_iter().collect::<HashMap<_, _>>();
    central
        .portfolio
        .hydrate_expected_ctf_balances(&expected, &onchain_balances, due_ts_ms)?;
    central
        .ctf_inventory
        .lock()
        .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
        .acknowledge_confirmed(&confirmed, due_ts_ms)?;
    central.portfolio_tx.send_replace(central.portfolio.clone());
    let receipt_handle = central.next_inventory_receipt_handle;
    central.next_inventory_receipt_handle = receipt_handle
        .checked_add(1)
        .context("candidate inventory receipt handle overflow")?;
    let transaction_handle = central.next_inventory_transaction_handle;
    central.next_inventory_transaction_handle = transaction_handle
        .checked_add(1)
        .context("candidate inventory transaction handle overflow")?;
    let catalog = central
        .catalogs
        .get(symbol.as_str())
        .cloned()
        .with_context(|| {
            format!(
                "candidate symbol catalog missing symbol={}",
                symbol.as_str()
            )
        })?;
    let record = transport::inventory_receipt(
        &confirmed,
        &expected,
        &catalog,
        receipt_handle,
        transaction_handle,
        intent_id,
    )?;
    send_symbol_views(
        slot,
        central,
        symbol,
        source_sequence,
        due_ts_ms,
        true,
        false,
        &[record],
    )?;
    let central_state_seq = central
        .central_sequences
        .get(symbol.as_str())
        .copied()
        .with_context(|| {
            format!(
                "candidate central-state sequence missing symbol={}",
                symbol.as_str()
            )
        })?;
    let output = slot.process_mut(symbol)?.drive(
        Drive {
            source_sequence,
            central_state_seq,
            kind: DriveKind::PortfolioChanged,
            flags: 0,
        },
        due_ts_ms,
    )?;
    central.accept_worker_output(output)
}

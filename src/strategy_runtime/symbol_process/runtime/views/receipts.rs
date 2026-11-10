use super::*;

impl CandidateSymbolRuntime {
    pub(in crate::strategy_runtime::symbol_process::runtime) fn ingest_inventory_receipts(
        &mut self,
        records: &[InventoryReceiptRecord],
        updated_at_ms: u64,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let ownership = self.ownership.clone();
        let symbol = self.symbol.as_str();
        let candidate = self.candidate_mut()?;
        for record in records {
            anyhow::ensure!(
                record.confirmed_at_ms <= updated_at_ms,
                "inventory receipt confirmation exceeds frame time confirmed_at_ms={} frame_timestamp_ms={updated_at_ms}",
                record.confirmed_at_ms
            );
            if record.operation != InventoryReceiptOperation::Split {
                continue;
            }
            anyhow::ensure!(
                record.collateral_debit_units_6dp == record.qty_units_6dp,
                "confirmed split principal differs from created complete-set quantity"
            );
            let market = catalog
                .record(record.market_id)
                .context("split receipt market handle is missing")?;
            let market_id = MarketId::new(B256::from(market.condition_id).to_string());
            let up_token = U256::from_be_bytes(market.up_token).to_string();
            let down_token = U256::from_be_bytes(market.down_token).to_string();
            let cohort = record.cohort_id.required_str()?.to_string();
            anyhow::ensure!(
                crate::ctf_inventory::is_confirmed_ctf_split_cohort_id(&cohort),
                "split receipt cohort identity is not confirmation-owned"
            );
            ownership.claim_cohort(&cohort, symbol)?;
            let entry_outlay_units_6dp = record
                .collateral_debit_units_6dp
                .checked_add(record.fee_units_6dp)
                .and_then(|value| value.checked_add(record.direct_cost_units_6dp))
                .context("confirmed split entry outlay overflow")?;
            let transaction = format!("central-transaction-{}", record.transaction_handle);
            let expected = [
                ExpectedCtfPosition {
                    token: up_token.clone(),
                    market_id: market_id.clone(),
                    end_time_ms: Some(market.window_end_ms),
                    confirmed_entry_outlay_units_6dp: Some(entry_outlay_units_6dp),
                    confirmed_materialized_ts_ms: Some(record.confirmed_at_ms),
                    confirmed_token_created_qty_units_6dp: Some(record.qty_units_6dp),
                    confirmed_token_principal_units_6dp: Some(record.up_principal_units_6dp),
                    confirmed_token_entry_fee_units_6dp: Some(record.up_entry_fee_units_6dp),
                    confirmed_token_approved_direct_cost_units_6dp: Some(
                        record.up_direct_cost_units_6dp,
                    ),
                    engine_trade_id: Some(cohort.clone()),
                    confirmed_entry_tx_hash: Some(transaction.clone()),
                    require_positive_balance: true,
                    min_qty_units_6dp: Some(record.qty_units_6dp),
                    portfolio_actionable: true,
                },
                ExpectedCtfPosition {
                    token: down_token.clone(),
                    market_id: market_id.clone(),
                    end_time_ms: Some(market.window_end_ms),
                    confirmed_entry_outlay_units_6dp: Some(entry_outlay_units_6dp),
                    confirmed_materialized_ts_ms: Some(record.confirmed_at_ms),
                    confirmed_token_created_qty_units_6dp: Some(record.qty_units_6dp),
                    confirmed_token_principal_units_6dp: Some(record.down_principal_units_6dp),
                    confirmed_token_entry_fee_units_6dp: Some(record.down_entry_fee_units_6dp),
                    confirmed_token_approved_direct_cost_units_6dp: Some(
                        record.down_direct_cost_units_6dp,
                    ),
                    engine_trade_id: Some(cohort),
                    confirmed_entry_tx_hash: Some(transaction),
                    require_positive_balance: true,
                    min_qty_units_6dp: Some(record.qty_units_6dp),
                    portfolio_actionable: true,
                },
            ];
            let balances = [up_token, down_token]
                .into_iter()
                .map(|token| {
                    let qty_units_6dp = candidate
                        .portfolio
                        .positions
                        .get(&token)
                        .with_context(|| {
                            format!("confirmed split position delta missing token={token}")
                        })?
                        .qty_units_6dp;
                    Ok((token, qty_units_6dp))
                })
                .collect::<Result<std::collections::HashMap<_, _>>>()?;
            candidate.portfolio.hydrate_expected_ctf_balances(
                &expected,
                &balances,
                updated_at_ms,
            )?;
        }
        Ok(())
    }

    pub(in crate::strategy_runtime::symbol_process::runtime) fn ingest_managed_sell_receipts(
        &mut self,
        records: &[ManagedSellReceiptRecord],
        updated_at_ms: u64,
    ) -> Result<()> {
        let catalog = self.catalog.clone();
        let candidate = self.candidate_mut()?;
        for record in records {
            anyhow::ensure!(
                record.confirmed_at_ms <= updated_at_ms,
                "managed-sell receipt confirmation exceeds frame time confirmed_at_ms={} frame_timestamp_ms={updated_at_ms}",
                record.confirmed_at_ms
            );
            anyhow::ensure!(
                record.flags == 0,
                "managed-sell receipt flags have no defined worker-view semantics flags={}",
                record.flags
            );
            let market = catalog
                .record(record.market_id)
                .context("managed-sell market handle is missing")?;
            let market_id = MarketId::new(B256::from(market.condition_id).to_string());
            let (_, token) = resolve_token(&catalog, record.token_id)?;
            let cohort = record.cohort_id.required_str()?.to_string();
            let fill_id = format!(
                "receipt-sha256:{}",
                alloy::hex::encode(record.receipt_identity_sha256)
            );
            let close_reason = close_reason(record.close_reason)?;
            let key = (cohort.clone(), fill_id.clone());
            anyhow::ensure!(
                !candidate
                    .portfolio
                    .confirmed_lifecycle_sell_fills
                    .contains_key(&key),
                "managed-sell receipt identity was consumed twice fill_handle={}",
                record.fill_handle
            );
            candidate.portfolio.reduce_confirmed_ctf_cohort_lot(
                &token,
                &cohort,
                record.qty_units_6dp,
            )?;
            candidate.portfolio.confirmed_lifecycle_sell_fills.insert(
                key,
                ConfirmedLifecycleSellFill {
                    fill_id,
                    receipt_identity: None,
                    order_id: format!("central-order-{}", record.order_handle),
                    lifecycle_cohort: cohort,
                    market_id,
                    token,
                    intent_id: record.intent_id,
                    qty_units_6dp: record.qty_units_6dp,
                    gross_proceeds_units_6dp: record.gross_proceeds_units_6dp,
                    fee_units_6dp: record.fee_units_6dp,
                    confirmed_at_ms: record.confirmed_at_ms,
                    close_reason,
                },
            );
        }
        candidate.portfolio.note_runtime_mutation(updated_at_ms);
        Ok(())
    }
}

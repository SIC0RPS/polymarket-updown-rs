use super::*;

impl ExecutionClient {
    pub(crate) async fn ctf_position_balances(
        &self,
        asset_ids: &[String],
    ) -> Result<HashMap<String, u128>> {
        let live = self.live_backend();
        let rpc_url = live.rpc_url();
        crate::polymarket::ctf::fetch_ctf_position_balances(
            &live.finality_http,
            &rpc_url,
            &self.polymarket_tracked_wallet().to_string(),
            asset_ids,
        )
        .await
    }

    pub async fn redeem_binary_conditions(
        &self,
        condition_ids: &[B256],
    ) -> Result<Option<CtfSettlementConfirmation>> {
        if condition_ids.is_empty() {
            return Ok(None);
        }
        match &self.backend {
            ExecutionBackend::Live(live) => {
                Ok(Some(live.redeem_binary_conditions(condition_ids).await?))
            }
            ExecutionBackend::Sim(sim) => sim.redeem_binary_conditions(condition_ids).await,
        }
    }

    pub async fn redeem_binary_condition(
        &self,
        condition_id: B256,
    ) -> Result<Option<CtfSettlementConfirmation>> {
        self.redeem_binary_conditions(&[condition_id]).await
    }

    pub async fn merge_binary_conditions(
        &self,
        merges: &[(B256, U256)],
    ) -> Result<Option<CtfSettlementConfirmation>> {
        if merges.is_empty() {
            return Ok(None);
        }
        match &self.backend {
            ExecutionBackend::Live(live) => Ok(Some(live.merge_binary_conditions(merges).await?)),
            ExecutionBackend::Sim(sim) => sim.merge_binary_conditions(merges).await,
        }
    }

    pub async fn split_binary_condition(
        &self,
        split_identity: &crate::trade_session_ledger::DirectEoaSplitIdentity,
        entry_journal: &crate::engine_trade_journal::CtfSplitEntryJournal,
    ) -> Result<Option<SplitConfirmation>> {
        split_identity.validate_entry_journal(entry_journal)?;
        match &self.backend {
            ExecutionBackend::Live(live) => Ok(Some(
                live.split_binary_condition(split_identity, entry_journal)
                    .await?,
            )),
            ExecutionBackend::Sim(sim) => {
                sim.split_binary_condition(split_identity, entry_journal)
                    .await
            }
        }
    }

    pub async fn split_transaction_receipt_status(
        &self,
        condition_id: B256,
        tx_hash: &str,
    ) -> Result<Option<SplitReceiptStatus>> {
        match &self.backend {
            ExecutionBackend::Live(live) => {
                live.split_transaction_receipt_status(condition_id, tx_hash)
                    .await
            }
            ExecutionBackend::Sim(_) => Ok(None),
        }
    }

    pub async fn condition_payout_vector(
        &self,
        condition_id: B256,
    ) -> Result<Option<ConditionPayoutVector>> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.condition_payout_vector(condition_id).await,
            ExecutionBackend::Sim(sim) => sim.condition_payout_vector(condition_id).await,
        }
    }
}

impl ExecutionSimClient {
    async fn redeem_binary_conditions(
        &self,
        condition_ids: &[B256],
    ) -> Result<Option<CtfSettlementConfirmation>> {
        let tx_hash = self.record_ctf_operation(
            SimSubmittedCtfOperationKind::Redeem,
            condition_ids.to_vec(),
            Vec::new(),
        );
        Ok(Some(CtfSettlementConfirmation::ReconciliationRequired {
            tx_hash,
            error: "simulated redeem has no finalized collateral receipt".to_string(),
        }))
    }

    async fn merge_binary_conditions(
        &self,
        merges: &[(B256, U256)],
    ) -> Result<Option<CtfSettlementConfirmation>> {
        let condition_ids = merges
            .iter()
            .map(|(condition_id, _)| *condition_id)
            .collect::<Vec<_>>();
        let amounts = merges.to_vec();
        let tx_hash =
            self.record_ctf_operation(SimSubmittedCtfOperationKind::Merge, condition_ids, amounts);
        Ok(Some(CtfSettlementConfirmation::ReconciliationRequired {
            tx_hash,
            error: "simulated merge has no finalized collateral receipt".to_string(),
        }))
    }

    async fn split_binary_condition(
        &self,
        split_identity: &crate::trade_session_ledger::DirectEoaSplitIdentity,
        entry_journal: &crate::engine_trade_journal::CtfSplitEntryJournal,
    ) -> Result<Option<SplitConfirmation>> {
        split_identity.validate_entry_journal(entry_journal)?;
        let condition_id = split_identity
            .condition_id
            .parse::<B256>()
            .context("simulated split condition identity is invalid")?;
        let amount = U256::from(split_identity.amount_units_6dp);
        let tx_hash = self.record_ctf_operation(
            SimSubmittedCtfOperationKind::Split,
            vec![condition_id],
            vec![(condition_id, amount)],
        );
        let receipt = self
            .state
            .state
            .lock()
            .expect("execution sim lock poisoned")
            .next_split_receipt
            .take()
            .unwrap_or(SplitReceiptStatus::Confirmed);
        Ok(Some(SplitConfirmation { tx_hash, receipt }))
    }

    async fn condition_payout_vector(
        &self,
        _condition_id: B256,
    ) -> Result<Option<ConditionPayoutVector>> {
        Ok(None)
    }

    fn record_ctf_operation(
        &self,
        kind: SimSubmittedCtfOperationKind,
        condition_ids: Vec<B256>,
        amount_units_6dp: Vec<(B256, U256)>,
    ) -> String {
        let mut state = self
            .state
            .state
            .lock()
            .expect("execution sim lock poisoned");
        state.next_ctf_seq = state.next_ctf_seq.saturating_add(1);
        let tx_hash = format!(
            "sim-ctf-{}-{}",
            match kind {
                SimSubmittedCtfOperationKind::Split => "split",
                SimSubmittedCtfOperationKind::Merge => "merge",
                SimSubmittedCtfOperationKind::Redeem => "redeem",
            },
            state.next_ctf_seq
        );
        state.submitted_ctf_ops.push(SimSubmittedCtfOperation {
            tx_hash: tx_hash.clone(),
            kind,
            condition_ids,
            amount_units_6dp,
        });
        tx_hash
    }
}

impl ExecutionSimHandle {
    pub(crate) fn submit_replay_redeem(&self, condition_id: B256) {
        ExecutionSimClient {
            state: self.clone(),
        }
        .record_ctf_operation(
            SimSubmittedCtfOperationKind::Redeem,
            vec![condition_id],
            Vec::new(),
        );
    }

    pub(crate) fn submit_replay_merge(
        &self,
        condition_id: B256,
        amount_units_6dp: u128,
    ) -> Result<()> {
        ensure!(
            amount_units_6dp > 0,
            "simulated replay merge amount is zero"
        );
        ExecutionSimClient {
            state: self.clone(),
        }
        .record_ctf_operation(
            SimSubmittedCtfOperationKind::Merge,
            vec![condition_id],
            vec![(condition_id, U256::from(amount_units_6dp))],
        );
        Ok(())
    }
}

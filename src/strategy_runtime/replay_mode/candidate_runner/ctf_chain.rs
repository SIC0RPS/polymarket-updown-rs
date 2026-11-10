use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::ctf_inventory::CtfInventoryReceipt;
use crate::engine_trade_journal::CtfSplitEntryJournal;
use crate::strategy::{InventoryIntent, InventoryOperation};
use crate::strategy_runtime::backtest_live_latency::{
    sample_observation_index, stable_latency_seed, ReplayCtfTimingObservation,
    ReplayExecutionTimingModel,
};
use crate::strategy_runtime::backtest_matrix_report::BacktestCtfTimingDiagnostic;
use crate::strategy_runtime::symbol_process::SymbolId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedCtfChainModelIdentity {
    pub(crate) model_version: String,
    pub(crate) artifact_sha256: [u8; 32],
    pub(crate) captured_rows: u64,
    /// Representative applied delay for reports (avg of active profile).
    pub(crate) applied_materialization_latency_ms: u64,
    pub(crate) latency_min_ms: u64,
    pub(crate) latency_max_ms: u64,
    pub(crate) source_capture: Option<String>,
    pub(crate) source_config_sha256: Option<String>,
    pub(crate) journal_path: Option<String>,
    pub(crate) journal_sha256: Option<String>,
    pub(crate) endpoint: String,
    pub(crate) unit: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedCtfChainModel {
    identity: CapturedCtfChainModelIdentity,
    observations: Vec<ReplayCtfTimingObservation>,
}

impl CapturedCtfChainModel {
    pub(crate) fn from_execution_timing(model: &ReplayExecutionTimingModel) -> Result<Self> {
        let artifact = serde_json::to_vec(model).context("serialize CTF timing model")?;
        let artifact_sha256 = Sha256::digest(&artifact).into();
        let observations = model
            .ctf
            .as_ref()
            .map(|source| source.observations.clone())
            .unwrap_or_default();
        let values = observations
            .iter()
            .map(|sample| sample.latency_ms)
            .collect::<Vec<_>>();
        let profile = (!values.is_empty())
            .then(|| crate::strategy_runtime::backtest_live_latency::distribution(&values))
            .transpose()?;
        Ok(Self {
            identity: CapturedCtfChainModelIdentity {
                model_version: model.model_version.clone(),
                artifact_sha256,
                captured_rows: observations.len() as u64,
                applied_materialization_latency_ms: profile
                    .as_ref()
                    .and_then(|value| value.avg)
                    .unwrap_or(0),
                latency_min_ms: profile.as_ref().and_then(|value| value.min).unwrap_or(0),
                latency_max_ms: profile.as_ref().and_then(|value| value.max).unwrap_or(0),
                source_capture: model
                    .ctf
                    .as_ref()
                    .map(|source| source.source_capture.clone()),
                source_config_sha256: model
                    .ctf
                    .as_ref()
                    .and_then(|source| source.source_config_sha256.clone()),
                journal_path: model.ctf.as_ref().map(|source| source.journal_path.clone()),
                journal_sha256: model
                    .ctf
                    .as_ref()
                    .map(|source| source.journal_sha256.clone()),
                endpoint: "confirmed_pending_to_materialized_inventory".to_string(),
                unit: "milliseconds".to_string(),
            },
            observations,
        })
    }

    pub(crate) fn clock(&self) -> CapturedCtfChainClock {
        CapturedCtfChainClock {
            identity: self.identity.clone(),
            pending: BTreeMap::new(),
            seen_operation_ids: BTreeSet::new(),
            seen_fill_receipts: BTreeSet::new(),
            position_balances_units_6dp: BTreeMap::new(),
            position_last_mutation_ms: BTreeMap::new(),
            observations: self.observations.clone(),
            diagnostics: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedCtfSplitVisibility {
    pub(crate) symbol: SymbolId,
    pub(crate) intent_id: u64,
    pub(crate) pending_ts_ms: u64,
    pub(crate) due_ts_ms: u64,
    pub(crate) intent: InventoryIntent,
    pub(crate) receipt: CtfInventoryReceipt,
    pub(crate) entry_journal: CtfSplitEntryJournal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PendingKey {
    due_ts_ms: u64,
    symbol_rank: u8,
    intent_id: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedCtfChainClock {
    identity: CapturedCtfChainModelIdentity,
    pending: BTreeMap<PendingKey, CapturedCtfSplitVisibility>,
    seen_operation_ids: BTreeSet<String>,
    seen_fill_receipts: BTreeSet<String>,
    position_balances_units_6dp: BTreeMap<String, u128>,
    position_last_mutation_ms: BTreeMap<String, u64>,
    observations: Vec<ReplayCtfTimingObservation>,
    diagnostics: Vec<BacktestCtfTimingDiagnostic>,
}

impl CapturedCtfChainClock {
    pub(crate) fn identity(&self) -> &CapturedCtfChainModelIdentity {
        &self.identity
    }

    pub(crate) fn next_due_ts(&self) -> Option<u64> {
        self.pending.first_key_value().map(|(key, _)| key.due_ts_ms)
    }

    pub(crate) fn diagnostics(&self) -> &[BacktestCtfTimingDiagnostic] {
        &self.diagnostics
    }

    pub(crate) fn schedule_confirmed(
        &mut self,
        symbol: SymbolId,
        intent_id: u64,
        pending_ts_ms: u64,
        intent: InventoryIntent,
        receipt: CtfInventoryReceipt,
        entry_journal: CtfSplitEntryJournal,
    ) -> Result<u64> {
        anyhow::ensure!(intent_id > 0, "captured CTF split intent identity is zero");
        anyhow::ensure!(
            pending_ts_ms > 0,
            "captured CTF split pending timestamp is zero"
        );
        anyhow::ensure!(
            intent.operation == InventoryOperation::SplitCompleteSet,
            "captured CTF chain timing accepts split operations only"
        );
        let tx_hash = match &receipt {
            CtfInventoryReceipt::Confirmed { tx_hash } => tx_hash,
            _ => anyhow::bail!("captured CTF chain timing requires a confirmed receipt"),
        };
        anyhow::ensure!(
            !tx_hash.is_empty() && tx_hash.trim() == tx_hash,
            "captured CTF split transaction identity is empty or noncanonical"
        );
        let seed = stable_latency_seed(
            &[
                symbol.as_str(),
                &intent.condition_id.to_string(),
                "ctf_split",
            ],
            0,
        );
        let sample_index = sample_observation_index(self.observations.len(), seed)
            .context("CTF timing is uncalibrated; materialization was not simulated")?;
        let materialization_latency_ms = self.observations[sample_index].latency_ms;
        let due_ts_ms = pending_ts_ms
            .checked_add(materialization_latency_ms)
            .context("captured CTF split due timestamp overflow")?;
        if self.diagnostics.len() < 16 {
            self.diagnostics.push(BacktestCtfTimingDiagnostic {
                tx_hash: tx_hash.clone(),
                intent_id,
                pending_ts_ms,
                sample_index,
                sampled_pending_to_materialized_ms: materialization_latency_ms,
                due_ts_ms,
            });
        }
        let key = PendingKey {
            due_ts_ms,
            symbol_rank: symbol as u8,
            intent_id,
        };
        anyhow::ensure!(
            !self.pending.contains_key(&key),
            "duplicate captured CTF split visibility key symbol={} intent_id={intent_id}",
            symbol.as_str()
        );
        anyhow::ensure!(
            self.seen_operation_ids.insert(tx_hash.clone()),
            "duplicate captured CTF split transaction identity tx_hash={tx_hash}"
        );
        self.pending.insert(
            key,
            CapturedCtfSplitVisibility {
                symbol,
                intent_id,
                pending_ts_ms,
                due_ts_ms,
                intent,
                receipt,
                entry_journal,
            },
        );
        Ok(due_ts_ms)
    }

    pub(crate) fn pop_next_due(
        &mut self,
        up_to_ts_ms: u64,
    ) -> Result<Option<CapturedCtfSplitVisibility>> {
        if !self
            .pending
            .first_key_value()
            .is_some_and(|(key, _)| key.due_ts_ms <= up_to_ts_ms)
        {
            return Ok(None);
        }
        let (_, visibility) = self
            .pending
            .pop_first()
            .context("captured CTF pending queue changed while draining")?;
        anyhow::ensure!(
            matches!(
                &visibility.receipt,
                CtfInventoryReceipt::Confirmed { tx_hash }
                    if self.seen_operation_ids.contains(tx_hash)
            ),
            "captured CTF transaction identity disappeared before materialization"
        );
        anyhow::ensure!(
            visibility.intent.up_token != visibility.intent.down_token,
            "captured CTF split tokens are identical"
        );
        for token in [
            visibility.intent.up_token.as_str(),
            visibility.intent.down_token.as_str(),
        ] {
            let current = self
                .position_balances_units_6dp
                .get(token)
                .copied()
                .unwrap_or(0);
            let next = current
                .checked_add(visibility.intent.amount_units_6dp)
                .context("captured CTF position balance overflow")?;
            self.position_balances_units_6dp
                .insert(token.to_string(), next);
            self.position_last_mutation_ms
                .insert(token.to_string(), visibility.due_ts_ms);
        }
        Ok(Some(visibility))
    }

    pub(crate) fn split_position_balances_units_6dp(
        &self,
        intent: &InventoryIntent,
    ) -> Result<[(String, u128); 2]> {
        anyhow::ensure!(
            intent.operation == InventoryOperation::SplitCompleteSet,
            "captured CTF balance query accepts split operations only"
        );
        let up = self
            .position_balances_units_6dp
            .get(&intent.up_token)
            .copied()
            .context("captured CTF up-token balance is unavailable")?;
        let down = self
            .position_balances_units_6dp
            .get(&intent.down_token)
            .copied()
            .context("captured CTF down-token balance is unavailable")?;
        Ok([
            (intent.up_token.clone(), up),
            (intent.down_token.clone(), down),
        ])
    }

    pub(crate) fn apply_confirmed_token_fill(
        &mut self,
        fill_id: &str,
        token: &str,
        qty_units_6dp: u128,
        is_buy: bool,
        confirmed_at_ms: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            !fill_id.is_empty() && fill_id.trim() == fill_id,
            "captured CTF fill receipt identity is empty or noncanonical"
        );
        anyhow::ensure!(
            !token.is_empty() && token.trim() == token,
            "captured CTF fill token identity is empty or noncanonical"
        );
        anyhow::ensure!(
            qty_units_6dp > 0,
            "captured CTF fill quantity is not positive"
        );
        anyhow::ensure!(
            confirmed_at_ms > 0,
            "captured CTF fill confirmation timestamp is zero"
        );
        anyhow::ensure!(
            self.seen_fill_receipts.insert(fill_id.to_string()),
            "duplicate captured CTF fill receipt identity fill_id={fill_id}"
        );
        let current = self
            .position_balances_units_6dp
            .get(token)
            .copied()
            .unwrap_or(0);
        let next = if is_buy {
            current
                .checked_add(qty_units_6dp)
                .context("captured CTF token balance overflow after buy fill")?
        } else {
            current
                .checked_sub(qty_units_6dp)
                .context("captured CTF sell fill exceeds simulated chain balance")?
        };
        if next == 0 {
            self.position_balances_units_6dp.remove(token);
            self.position_last_mutation_ms.remove(token);
        } else {
            self.position_balances_units_6dp
                .insert(token.to_string(), next);
            self.position_last_mutation_ms
                .insert(token.to_string(), confirmed_at_ms);
        }
        Ok(())
    }

    pub(crate) fn position_state_units_6dp(&self, token: &str) -> Option<(u128, u64)> {
        self.position_balances_units_6dp
            .get(token)
            .copied()
            .zip(self.position_last_mutation_ms.get(token).copied())
    }

    pub(crate) fn apply_terminal_settlement(
        &mut self,
        settlement_id: &str,
        up_token: &str,
        up_qty_units_6dp: u128,
        down_token: &str,
        down_qty_units_6dp: u128,
        confirmed_at_ms: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            !settlement_id.is_empty() && settlement_id.trim() == settlement_id,
            "captured CTF terminal settlement identity is empty or noncanonical"
        );
        anyhow::ensure!(
            up_token != down_token && !up_token.is_empty() && !down_token.is_empty(),
            "captured CTF terminal token identities are missing or duplicate"
        );
        anyhow::ensure!(
            up_qty_units_6dp > 0 || down_qty_units_6dp > 0,
            "captured CTF terminal quantity is zero"
        );
        anyhow::ensure!(
            confirmed_at_ms > 0,
            "captured CTF terminal confirmation timestamp is zero"
        );
        anyhow::ensure!(
            !self.seen_operation_ids.contains(settlement_id),
            "duplicate captured CTF terminal settlement identity settlement_id={settlement_id}"
        );
        for (token, qty) in [
            (up_token, up_qty_units_6dp),
            (down_token, down_qty_units_6dp),
        ] {
            if qty == 0 {
                continue;
            }
            let balance = self
                .position_balances_units_6dp
                .get(token)
                .copied()
                .with_context(|| {
                    format!("captured CTF terminal position balance is unavailable token={token}")
                })?;
            anyhow::ensure!(
                qty <= balance,
                "captured CTF terminal quantity exceeds position balance token={token} quantity={qty} balance={balance}"
            );
        }
        self.seen_operation_ids.insert(settlement_id.to_string());
        for (token, qty) in [
            (up_token, up_qty_units_6dp),
            (down_token, down_qty_units_6dp),
        ] {
            if qty == 0 {
                continue;
            }
            let balance = self
                .position_balances_units_6dp
                .get_mut(token)
                .expect("captured terminal balance preflight checked");
            *balance = balance
                .checked_sub(qty)
                .expect("captured terminal quantity preflight checked");
            if *balance == 0 {
                self.position_balances_units_6dp.remove(token);
                self.position_last_mutation_ms.remove(token);
            } else {
                self.position_last_mutation_ms
                    .insert(token.to_string(), confirmed_at_ms);
            }
        }
        Ok(())
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn reset(&mut self) {
        self.pending.clear();
        self.seen_operation_ids.clear();
        self.seen_fill_receipts.clear();
        self.position_balances_units_6dp.clear();
        self.position_last_mutation_ms.clear();
        self.diagnostics.clear();
    }
}

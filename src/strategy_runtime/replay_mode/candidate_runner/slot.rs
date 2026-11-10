use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use super::{
    CapturedClobSubmitModel, CapturedCtfChainModel, MergedEventSchedule, PackedReplayTape,
};
use crate::config::{
    AppConfig, MakerRebateMmStrategyConfig, OptimizerParameterSchema, OptimizerParameterValue,
};
use crate::strategy_runtime::backtest_matrix::BacktestMatrixCandidate;
use crate::strategy_runtime::prediction_metrics::PredictionMetrics;
use crate::strategy_runtime::symbol_process::{
    CandidateReset, CandidateSymbolRuntime, Finalize, ProtocolMode, SymbolId, WorkerBaseSnapshot,
    WorkerReplayWindow,
};

pub(crate) struct CandidateSlot {
    schedule: MergedEventSchedule,
    clob_submit_model: Arc<CapturedClobSubmitModel>,
    ctf_chain_model: Arc<CapturedCtfChainModel>,
    workers: BTreeMap<String, CandidateWorker>,
    last_timestamp_ms: u64,
    first_timestamp_ms: u64,
    capture_identity: String,
}

struct CandidateWorker {
    symbol: SymbolId,
    runtime: CandidateSymbolRuntime,
    schema: OptimizerParameterSchema,
    base_config: MakerRebateMmStrategyConfig,
    base_config_sha256: [u8; 32],
    active_candidate_sha256: Option<[u8; 32]>,
    active_capture_identities: Vec<String>,
}

impl CandidateSlot {
    #[allow(
        clippy::too_many_arguments,
        reason = "fixed candidate runtime identity"
    )]
    pub(crate) fn spawn(
        slot: u32,
        mode: ProtocolMode,
        cfg: &AppConfig,
        symbols: &[String],
        tapes: &BTreeMap<String, Arc<PackedReplayTape>>,
        schema: &OptimizerParameterSchema,
        schedule: MergedEventSchedule,
        clob_submit_model: Arc<CapturedClobSubmitModel>,
        ctf_chain_model: Arc<CapturedCtfChainModel>,
        replay_window: WorkerReplayWindow,
        equity_baseline_usdt: f64,
        first_timestamp_ms: u64,
        capture_identity: String,
    ) -> Result<Self> {
        anyhow::ensure!(
            matches!(mode, ProtocolMode::Replay | ProtocolMode::Optimize),
            "candidate slot requires replay or optimizer mode"
        );
        let mut workers = BTreeMap::new();
        for symbol in symbols {
            let symbol_id = SymbolId::parse(symbol)?;
            let symbol_name = symbol_id.as_str();
            anyhow::ensure!(
                !workers.contains_key(symbol_name),
                "candidate slot contains duplicate symbol={symbol_name}"
            );
            let tape = tapes
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol_name))
                .map(|(_, tape)| tape)
                .with_context(|| format!("candidate slot tape missing symbol={symbol_name}"))?;
            let mut base_config = cfg
                .strategy
                .maker_rebate_mm
                .config_for_symbol(symbol_name)
                .clone();
            base_config.optimizer_symbol_configs.clear();
            base_config.enabled_assets = vec![symbol_name.to_string()];
            base_config.validate_symbol_runtime(symbol_name)?;
            let snapshot = WorkerBaseSnapshot::new(
                symbol_id,
                base_config.clone(),
                cfg.bbwick_oracle.isolated_for_underlying(symbol_name),
                equity_baseline_usdt,
                Some(replay_window),
            )?;
            let snapshot_bytes = snapshot.encode_json(symbol_id)?;
            let base_config_sha256: [u8; 32] = Sha256::digest(&snapshot_bytes).into();
            let runtime = CandidateSymbolRuntime::new(
                slot,
                mode,
                symbol_id,
                snapshot,
                base_config_sha256,
                schema.clone(),
                Arc::clone(tape),
            )?;
            workers.insert(
                symbol_name.to_string(),
                CandidateWorker {
                    symbol: symbol_id,
                    runtime,
                    schema: schema.clone(),
                    base_config,
                    base_config_sha256,
                    active_candidate_sha256: None,
                    active_capture_identities: Vec::new(),
                },
            );
        }
        anyhow::ensure!(
            workers.len() == symbols.len(),
            "candidate slot worker coverage mismatch workers={} symbols={}",
            workers.len(),
            symbols.len()
        );
        Ok(Self {
            last_timestamp_ms: schedule.last_timestamp_ms(),
            first_timestamp_ms,
            capture_identity,
            schedule,
            clob_submit_model,
            ctf_chain_model,
            workers,
        })
    }

    pub(crate) fn reset_workers(&mut self, candidate: &BacktestMatrixCandidate) -> Result<()> {
        for worker in self.workers.values_mut() {
            let symbol = worker.symbol.as_str();
            let values = candidate
                .per_symbol_parameter_values
                .get(symbol)
                .with_context(|| format!("candidate parameter map missing symbol={symbol}"))?;
            validate_candidate_config(
                candidate,
                symbol,
                &worker.schema,
                &worker.base_config,
                values,
            )?;
            let candidate_sha256 = worker
                .schema
                .candidate_sha256(&worker.base_config, values)?;
            let parameter_values = worker
                .schema
                .ordered_values(values)?
                .iter()
                .map(OptimizerParameterValue::as_f64)
                .collect::<Vec<_>>();
            let reset = CandidateReset::new(
                candidate_sha256,
                worker.base_config_sha256,
                worker.schema.sha256(),
                0,
                &parameter_values,
            )?;
            let initial_oracle_learning = candidate.initial_oracle_learning.get(symbol).cloned();
            if let Some(learning) = initial_oracle_learning.as_ref() {
                anyhow::ensure!(
                    learning.knowledge_cutoff_ms() < self.first_timestamp_ms,
                    "oracle learning cutoff does not precede earliest replay input symbol={symbol}"
                );
                anyhow::ensure!(
                    !learning
                        .capture_identities()
                        .iter()
                        .any(|identity| identity == &self.capture_identity),
                    "oracle learning includes current capture symbol={symbol}"
                );
            }
            worker.active_capture_identities = initial_oracle_learning
                .as_ref()
                .map(|learning| learning.capture_identities().to_vec())
                .unwrap_or_default();
            let initial_sequence = self.schedule.initial_sequence(worker.symbol);
            worker.runtime.reset_candidate(
                &reset,
                initial_sequence,
                initial_oracle_learning,
                candidate.oracle_direct_market_research,
                candidate.collect_prediction_metrics,
            )?;
            worker.active_candidate_sha256 = Some(candidate_sha256);
        }
        Ok(())
    }

    pub(crate) fn process_mut(&mut self, symbol: SymbolId) -> Result<&mut CandidateSymbolRuntime> {
        self.workers
            .get_mut(symbol.as_str())
            .map(|worker| &mut worker.runtime)
            .with_context(|| {
                format!(
                    "candidate symbol runtime unavailable symbol={}",
                    symbol.as_str()
                )
            })
    }

    pub(crate) fn schedule(&self) -> &MergedEventSchedule {
        &self.schedule
    }

    pub(crate) fn clob_submit_model(&self) -> &Arc<CapturedClobSubmitModel> {
        &self.clob_submit_model
    }

    pub(crate) fn ctf_chain_model(&self) -> &Arc<CapturedCtfChainModel> {
        &self.ctf_chain_model
    }

    pub(crate) fn observe_central_timestamp(&mut self, timestamp_ms: u64) {
        self.last_timestamp_ms = self.last_timestamp_ms.max(timestamp_ms);
    }

    pub(crate) fn finalize_workers(
        &mut self,
        central_state_sequences: &BTreeMap<String, u64>,
        knowledge_cutoff_ms: u64,
    ) -> Result<(
        BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
        BTreeMap<String, PredictionMetrics>,
    )> {
        let mut learning_by_symbol = BTreeMap::new();
        let mut prediction_by_symbol = BTreeMap::new();
        for symbol in [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp] {
            let Some(worker) = self.workers.get_mut(symbol.as_str()) else {
                continue;
            };
            let candidate_sha256 = worker
                .active_candidate_sha256
                .context("candidate worker finalized without active identity")?;
            let last_central_state_seq = central_state_sequences
                .get(symbol.as_str())
                .copied()
                .with_context(|| {
                    format!(
                        "candidate central-state sequence missing symbol={}",
                        symbol.as_str()
                    )
                })?;
            let local_event_count = self.schedule.local_event_count(symbol)?;
            let last_global_sequence = self.schedule.last_sequence(symbol);
            let mut capture_identities = std::mem::take(&mut worker.active_capture_identities);
            capture_identities.push(self.capture_identity.clone());
            let (_, learning, prediction) = worker.runtime.finalize(
                Finalize {
                    candidate_sha256,
                    last_global_sequence,
                    last_central_state_seq,
                    local_event_count,
                },
                self.last_timestamp_ms,
                capture_identities,
                knowledge_cutoff_ms,
            )?;
            anyhow::ensure!(
                learning_by_symbol
                    .insert(symbol.as_str().to_string(), learning)
                    .is_none(),
                "duplicate finalized oracle learning symbol={}",
                symbol.as_str()
            );
            if let Some(metrics) = prediction {
                metrics.validate()?;
                anyhow::ensure!(
                    prediction_by_symbol
                        .insert(symbol.as_str().to_string(), metrics)
                        .is_none(),
                    "duplicate finalized prediction metrics symbol={}",
                    symbol.as_str()
                );
            }
            worker.active_candidate_sha256 = None;
        }
        anyhow::ensure!(
            prediction_by_symbol.is_empty()
                || prediction_by_symbol.len() == learning_by_symbol.len(),
            "finalized prediction symbol coverage mismatch"
        );
        Ok((learning_by_symbol, prediction_by_symbol))
    }

    pub(crate) fn shutdown(mut self) -> Result<()> {
        for worker in self.workers.values_mut() {
            worker.runtime.abort_candidate();
        }
        Ok(())
    }
}

fn validate_candidate_config(
    candidate: &BacktestMatrixCandidate,
    symbol: &str,
    schema: &OptimizerParameterSchema,
    expected: &MakerRebateMmStrategyConfig,
    values: &BTreeMap<String, OptimizerParameterValue>,
) -> Result<()> {
    let mut actual = candidate.strategy_config.config_for_symbol(symbol).clone();
    actual.optimizer_symbol_configs.clear();
    actual.enabled_assets = vec![symbol.to_string()];
    let actual = serde_json::to_vec(&actual).context("serialize candidate base strategy config")?;
    let expected_bytes =
        serde_json::to_vec(expected).context("serialize frozen worker base strategy config")?;
    anyhow::ensure!(
        actual == expected_bytes,
        "candidate base strategy config changed after worker launch symbol={symbol}"
    );
    if let Some(provided) = candidate.per_symbol_strategy_config.get(symbol) {
        let applied = schema.apply_symbol_map(expected, values, symbol)?;
        let provided = serde_json::to_vec(provided)
            .context("serialize candidate provided symbol strategy config")?;
        let applied = serde_json::to_vec(&applied)
            .context("serialize candidate schema-applied symbol strategy config")?;
        anyhow::ensure!(
            provided == applied,
            "candidate per-symbol strategy config disagrees with typed parameter map symbol={symbol}"
        );
    }
    Ok(())
}

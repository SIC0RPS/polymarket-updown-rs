use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rayon::prelude::*;

use super::super::{capture_replay_interval, PackedReplayTape, PreparedReceiptTape};
use super::{
    abort_candidate, advance_candidate, finalize_candidate, initialize_candidate,
    CandidateInputDecoder, CandidateReplayResult, CandidateRun, CandidateRunnerInputs,
    CandidateSlot, CapturedCentralBaseline, CapturedClobSubmitModel, CapturedCtfChainModel,
    MergedEventSchedule, ScheduledEventTarget,
};
use crate::capture::finalized_capture_identity;
use crate::config::{AppConfig, OptimizerParameterSchema};
use crate::observability::{Observability, ObservabilityRuntime};
use crate::strategy_runtime::backtest_live_latency::ReplayExecutionTimingModel;
use crate::strategy_runtime::backtest_matrix::BacktestMatrixCandidate;
use crate::strategy_runtime::symbol_process::{ProtocolMode, WorkerReplayWindow};
use anyhow::{Context, Result};

pub(crate) struct OwnedCandidateInputs {
    symbols: Vec<String>,
    tapes: BTreeMap<String, Arc<PackedReplayTape>>,
    receipt_tape: Arc<PreparedReceiptTape>,
    window: (u64, u64),
    baseline: CapturedCentralBaseline,
    schedule: MergedEventSchedule,
    first_timestamp_ms: u64,
    capture_identity: String,
    clob_submit_model: Arc<CapturedClobSubmitModel>,
    ctf_chain_model: Arc<CapturedCtfChainModel>,
}

impl OwnedCandidateInputs {
    pub(crate) fn load(
        capture: &Path,
        symbols: &[String],
        tapes: BTreeMap<String, Arc<PackedReplayTape>>,
        execution_timing: &ReplayExecutionTimingModel,
    ) -> Result<Self> {
        let baseline = CapturedCentralBaseline::load(capture)?;
        let interval = capture_replay_interval(capture)?;
        let window = interval
            .window()
            .context("candidate replay interval is unavailable")?;
        let receipt_tape = PreparedReceiptTape::load(capture, symbols, &tapes)?;
        let finalized_capture = finalized_capture_identity(capture)?;
        let capture_identity = format!(
            "{}:{}:{}:{}",
            finalized_capture.session_id,
            finalized_capture.run_id,
            finalized_capture.start_ts_ms,
            finalized_capture.end_ts_ms
        );
        let schedule = CandidateRunnerInputs {
            symbols,
            tapes: &tapes,
            receipt_tape: &receipt_tape,
            window: Some(window),
        }
        .merged_schedule()?;
        let first_timestamp_ms =
            {
                let mut consumption_cursors = tapes
                    .iter()
                    .map(|(symbol, tape)| Ok((symbol.clone(), tape.consumption_cursor()?)))
                    .collect::<Result<BTreeMap<_, _>>>()?;
                schedule.events().iter().try_fold(
                    baseline.session_ts_ms(),
                    |earliest, scheduled| match scheduled.target {
                        ScheduledEventTarget::Symbol(symbol) => {
                            let (tape_key, tape) = tapes
                                .iter()
                                .find(|(candidate, _)| {
                                    candidate.eq_ignore_ascii_case(symbol.as_str())
                                })
                                .with_context(|| {
                                    format!("candidate tape missing symbol={}", symbol.as_str())
                                })?;
                            let cursor =
                                consumption_cursors.get_mut(tape_key).with_context(|| {
                                    format!(
                                        "candidate consumption cursor missing symbol={}",
                                        symbol.as_str()
                                    )
                                })?;
                            Ok::<_, anyhow::Error>(
                                earliest
                                    .min(tape.earliest_consumed_input_ms(cursor, scheduled.event)?),
                            )
                        }
                        ScheduledEventTarget::Central
                        | ScheduledEventTarget::CentralResolution(_) => {
                            Ok(earliest.min(scheduled.event.timestamp_ms))
                        }
                    },
                )?
            };
        let clob_submit_model = Arc::new(CapturedClobSubmitModel::from_execution_timing(
            execution_timing,
        )?);
        let ctf_chain_model = Arc::new(CapturedCtfChainModel::from_execution_timing(
            execution_timing,
        )?);
        Ok(Self {
            symbols: symbols.to_vec(),
            tapes,
            receipt_tape,
            window,
            baseline,
            schedule,
            first_timestamp_ms,
            capture_identity,
            clob_submit_model,
            ctf_chain_model,
        })
    }
}

pub(crate) struct PersistentCandidatePool {
    slots: Vec<Mutex<Option<CandidateSlot>>>,
    observability: Option<ObservabilityRuntime>,
    obs: Observability,
    inputs: OwnedCandidateInputs,
    worker_pool: rayon::ThreadPool,
    poisoned: AtomicBool,
}

impl PersistentCandidatePool {
    pub(crate) async fn spawn(
        cfg: &AppConfig,
        capture: &Path,
        symbols: &[String],
        tapes: &BTreeMap<String, Arc<PackedReplayTape>>,
        parameter_schema: &OptimizerParameterSchema,
        requested_slots: usize,
        worker_threads: usize,
        mode: ProtocolMode,
        shared_observability: Option<&Observability>,
        execution_timing: &ReplayExecutionTimingModel,
    ) -> Result<Self> {
        anyhow::ensure!(requested_slots > 0, "candidate pool has no slots");
        anyhow::ensure!(worker_threads > 0, "candidate pool has no CPU workers");
        let inputs = OwnedCandidateInputs::load(capture, symbols, tapes.clone(), execution_timing)?;
        let worker_window = WorkerReplayWindow {
            start_ms: inputs.window.0,
            end_ms: inputs.window.1,
        };
        let (observability, obs) = match shared_observability {
            Some(parent) => (
                None,
                Observability::new_slot(parent, parent.shutdown_token()),
            ),
            None => {
                let mut observability_cfg = cfg.observability.clone();
                observability_cfg.enabled = false;
                observability_cfg.logging.action_log_enabled = false;
                observability_cfg.dashboard.enabled = false;
                let runtime =
                    ObservabilityRuntime::start(&observability_cfg, "candidate_pool").await?;
                let obs = runtime.handle();
                (Some(runtime), obs)
            }
        };

        let worker_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_threads)
            .thread_name(|index| format!("candidate-pool-{index}"))
            .build()
            .context("build persistent candidate Rayon pool")?;
        let mut replay_cfg = cfg.clone();
        replay_cfg.bbwick_oracle.history.data_root = capture.join("bbwick_ohlc");
        let mut slots = Vec::with_capacity(requested_slots);
        for index in 0..requested_slots {
            let slot = u32::try_from(index).context("candidate slot index exceeds u32")?;
            slots.push(Mutex::new(Some(CandidateSlot::spawn(
                slot,
                mode,
                &replay_cfg,
                &inputs.symbols,
                &inputs.tapes,
                parameter_schema,
                inputs.schedule.clone(),
                Arc::clone(&inputs.clob_submit_model),
                Arc::clone(&inputs.ctf_chain_model),
                worker_window,
                inputs.baseline.equity_baseline_usdt(),
                inputs.first_timestamp_ms,
                inputs.capture_identity.clone(),
            )?)));
        }

        Ok(Self {
            slots,
            observability,
            obs,
            inputs,
            worker_pool,
            poisoned: AtomicBool::new(false),
        })
    }

    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn observability(&self) -> Observability {
        self.obs.clone()
    }

    pub(crate) fn slot(&self, index: usize) -> Result<&Mutex<Option<CandidateSlot>>> {
        self.slots
            .get(index)
            .with_context(|| format!("candidate pool slot is unavailable index={index}"))
    }

    pub(crate) fn evaluate_candidates(
        &self,
        cfg: &AppConfig,
        candidates: &[BacktestMatrixCandidate],
    ) -> Result<Vec<CandidateReplayResult>> {
        anyhow::ensure!(
            !self.poisoned.load(Ordering::Acquire),
            "candidate pool is poisoned by a prior failed candidate"
        );
        let mut results = Vec::with_capacity(candidates.len());
        for group in candidates.chunks(self.slot_count()) {
            results.extend(self.evaluate_group(cfg, group)?);
        }
        results.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        Ok(results)
    }

    fn evaluate_group(
        &self,
        cfg: &AppConfig,
        candidates: &[BacktestMatrixCandidate],
    ) -> Result<Vec<CandidateReplayResult>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let runtime = tokio::runtime::Handle::current();
        let inputs = CandidateRunnerInputs {
            symbols: &self.inputs.symbols,
            tapes: &self.inputs.tapes,
            receipt_tape: &self.inputs.receipt_tape,
            window: Some(self.inputs.window),
        };
        let runs = (0..candidates.len())
            .map(|_| Mutex::new(None::<CandidateRun>))
            .collect::<Vec<_>>();
        let started = (0..candidates.len())
            .map(|_| Instant::now())
            .collect::<Vec<_>>();

        let initialized = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.worker_pool.install(|| {
                (0..candidates.len())
                    .into_par_iter()
                    .map(|index| {
                        let slot_mutex = self.slot(index)?;
                        let mut slot_guard = slot_mutex
                            .lock()
                            .map_err(|_| anyhow::anyhow!("candidate slot mutex poisoned"))?;
                        let slot = slot_guard
                            .as_mut()
                            .context("candidate slot was shut down after a prior failure")?;
                        let run = runtime.block_on(initialize_candidate(
                            slot,
                            cfg,
                            &candidates[index],
                            &inputs,
                            &self.inputs.baseline,
                            self.observability(),
                        ))?;
                        *runs[index]
                            .lock()
                            .map_err(|_| anyhow::anyhow!("candidate run mutex poisoned"))? =
                            Some(run);
                        Ok::<_, anyhow::Error>(())
                    })
                    .collect::<Vec<_>>()
            })
        }));
        if let Err(error) = phase_result(initialized) {
            return Err(self.fail_group(&runtime, &runs, error));
        }

        let mut input_decoder = match CandidateInputDecoder::new(&self.inputs.tapes) {
            Ok(decoder) => decoder,
            Err(error) => return Err(self.fail_group(&runtime, &runs, error)),
        };
        let mut cursor = 0usize;
        while cursor < self.inputs.schedule.events().len() {
            let block = match input_decoder.decode_block(
                &self.inputs.schedule,
                cursor,
                &self.worker_pool,
            ) {
                Ok(block) => block,
                Err(error) => return Err(self.fail_group(&runtime, &runs, error)),
            };
            let advanced = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.worker_pool.install(|| {
                    (0..candidates.len())
                        .into_par_iter()
                        .map(|index| {
                            let slot_mutex = self.slot(index)?;
                            let mut slot_guard = slot_mutex
                                .lock()
                                .map_err(|_| anyhow::anyhow!("candidate slot mutex poisoned"))?;
                            let slot = slot_guard
                                .as_mut()
                                .context("candidate slot was shut down after a prior failure")?;
                            let mut run_guard = runs[index]
                                .lock()
                                .map_err(|_| anyhow::anyhow!("candidate run mutex poisoned"))?;
                            let run = run_guard
                                .as_mut()
                                .context("candidate run state is unavailable")?;
                            runtime.block_on(advance_candidate(slot, &inputs, run, &block))
                        })
                        .collect::<Vec<_>>()
                })
            }));
            if let Err(error) = phase_result(advanced) {
                return Err(self.fail_group(&runtime, &runs, error));
            }
            cursor = cursor
                .checked_add(block.events.len())
                .context("candidate input block cursor overflow")?;
            let events = block.events;
            self.worker_pool
                .install(|| events.into_par_iter().for_each(drop));
        }

        let finalized = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.worker_pool.install(|| {
                (0..candidates.len())
                    .into_par_iter()
                    .map(|index| {
                        let slot_mutex = self.slot(index)?;
                        let mut slot_guard = slot_mutex
                            .lock()
                            .map_err(|_| anyhow::anyhow!("candidate slot mutex poisoned"))?;
                        let slot = slot_guard
                            .as_mut()
                            .context("candidate slot was shut down after a prior failure")?;
                        let mut run_guard = runs[index]
                            .lock()
                            .map_err(|_| anyhow::anyhow!("candidate run mutex poisoned"))?;
                        let run = run_guard
                            .as_mut()
                            .context("candidate run state is unavailable")?;
                        let mut result =
                            runtime.block_on(finalize_candidate(slot, &inputs, run))?;
                        result.elapsed_ms = started[index].elapsed().as_millis();
                        *run_guard = None;
                        Ok::<_, anyhow::Error>(result)
                    })
                    .collect::<Vec<_>>()
            })
        }));
        match phase_result(finalized) {
            Ok(results) => Ok(results),
            Err(error) => Err(self.fail_group(&runtime, &runs, error)),
        }
    }

    fn fail_group(
        &self,
        runtime: &tokio::runtime::Handle,
        runs: &[Mutex<Option<CandidateRun>>],
        error: anyhow::Error,
    ) -> anyhow::Error {
        self.poisoned.store(true, Ordering::Release);
        let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.worker_pool.install(|| {
                let mut cleanup_errors = Vec::new();
                for run in runs {
                    let run = match run.lock() {
                        Ok(mut guard) => guard.take(),
                        Err(poisoned) => poisoned.into_inner().take(),
                    };
                    let Some(run) = run else {
                        continue;
                    };
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        runtime.block_on(abort_candidate(run))
                    })) {
                        Ok(Ok(())) => {}
                        Ok(Err(cleanup_error)) => {
                            cleanup_errors.push(format!("{cleanup_error:#}"));
                        }
                        Err(panic) => cleanup_errors.push(format!(
                            "candidate supervisor cleanup panicked: {}",
                            panic_message(panic.as_ref())
                        )),
                    }
                }
                for slot in &self.slots {
                    let slot = match slot.lock() {
                        Ok(mut guard) => guard.take(),
                        Err(poisoned) => poisoned.into_inner().take(),
                    };
                    let Some(slot) = slot else {
                        continue;
                    };
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| slot.shutdown()))
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(cleanup_error)) => {
                            cleanup_errors.push(format!("{cleanup_error:#}"));
                        }
                        Err(panic) => cleanup_errors.push(format!(
                            "candidate slot cleanup panicked: {}",
                            panic_message(panic.as_ref())
                        )),
                    }
                }
                cleanup_errors
            })
        }));
        let cleanup_errors = match cleanup {
            Ok(cleanup_errors) => cleanup_errors,
            Err(panic) => vec![format!(
                "candidate group cleanup worker panicked: {}",
                panic_message(panic.as_ref())
            )],
        };
        if cleanup_errors.is_empty() {
            error
        } else {
            error.context(format!(
                "candidate group cleanup also failed: {}",
                cleanup_errors.join("; ")
            ))
        }
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        let slots = std::mem::take(&mut self.slots)
            .into_iter()
            .map(|slot| match slot.into_inner() {
                Ok(slot) => slot,
                Err(poisoned) => poisoned.into_inner(),
            })
            .collect::<Vec<_>>();
        let shutdown = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.worker_pool.install(|| {
                slots
                    .into_par_iter()
                    .flatten()
                    .map(CandidateSlot::shutdown)
                    .collect::<Vec<_>>()
            })
        }));
        if let Some(observability) = self.observability.take() {
            observability.shutdown().await;
        }
        phase_result(shutdown).map(|_| ())
    }
}

fn phase_result<T>(phase: std::thread::Result<Vec<Result<T>>>) -> Result<Vec<T>> {
    match phase {
        Ok(results) => results.into_iter().collect(),
        Err(panic) => Err(anyhow::anyhow!(
            "candidate evaluator worker panicked: {}",
            panic_message(panic.as_ref())
        )),
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

impl Drop for PersistentCandidatePool {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            if let Ok(slot) = slot.get_mut() {
                let _ = slot.take();
            }
        }
        self.observability.take();
    }
}

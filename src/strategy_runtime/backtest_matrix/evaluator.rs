use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::{
    installed_optimizer_symbol_path, AppConfig, MakerRebateMmStrategyConfig,
    OptimizerParameterSchema, VALID_ASSETS,
};
use crate::observability::Observability;
use crate::strategy_runtime::backtest_config_truth::summarize_replay_config_truth;
use crate::strategy_runtime::backtest_data_quality::ReplayDataQualitySummary;
use crate::strategy_runtime::backtest_live_latency::{
    frozen_replay_live_latency, select_replay_execution_timing,
    validate_frozen_replay_execution_timing, ReplayExecutionTimingModel,
};
use crate::strategy_runtime::backtest_matrix_report::{
    build_report_artifact, BacktestMatrixReplayRuntimeArtifact,
};
use crate::strategy_runtime::backtest_ohlc_manifest::summarize_capture_ohlc_manifest;
use crate::strategy_runtime::backtest_run_identity::build_backtest_run_identity;
use crate::strategy_runtime::symbol_process::ProtocolMode;

use super::super::replay_mode::{
    capture_replay_interval, capture_replay_risk_timeline_summary, frozen_ctf_split_latency,
    prepare_backtest_replay_for_capture, resolve_capture_session_for_replay, CandidateReplayResult,
    PackedReplayTape, PersistentCandidatePool, RecordedMarketPopulation,
};
use super::reporting::{matrix_diagnostics, render_report};
use super::{
    BacktestMatrixCandidate, BacktestMatrixCandidateMetricsSummary, BacktestMatrixCandidateSummary,
    BacktestMatrixEvaluator, DEFAULT_SYMBOLS,
};

type CandidateResult = CandidateReplayResult;

impl BacktestMatrixCandidate {
    pub(crate) fn strategy_config_for_symbols(
        &self,
        symbols: &[String],
    ) -> Result<MakerRebateMmStrategyConfig> {
        let mut config = self.strategy_config.clone();
        config.enabled_assets = symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect();
        if self.per_symbol_strategy_config.is_empty() {
            return Ok(config);
        }
        let expected = symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<BTreeSet<_>>();
        let actual = self
            .per_symbol_strategy_config
            .keys()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            actual == expected,
            "candidate symbol config coverage mismatch candidate={} expected={expected:?} actual={actual:?}",
            self.id
        );
        config.optimizer_symbol_configs = self
            .per_symbol_strategy_config
            .iter()
            .map(|(symbol, symbol_config)| {
                let mut symbol_config = symbol_config.clone();
                symbol_config.optimizer_symbol_configs.clear();
                (symbol.to_ascii_uppercase(), Box::new(symbol_config))
            })
            .collect();
        Ok(config)
    }
}

impl BacktestMatrixEvaluator {
    pub fn replay_window_for_capture(capture: &Path) -> Result<(u64, u64)> {
        capture_replay_interval(capture)?
            .window()
            .context("capture replay interval is invalid")
    }

    pub fn prepare(cfg: &AppConfig, capture_session: &Path) -> Result<Self> {
        let requested_capture = capture_session.to_path_buf();
        let capture = resolve_capture_session_for_replay(cfg, Some(&requested_capture))?;
        let symbols = DEFAULT_SYMBOLS
            .iter()
            .map(|symbol| (*symbol).to_string())
            .collect::<Vec<_>>();
        let (parameter_schema, config_input_paths) = runtime_symbol_schema(cfg, &symbols)?;
        Self::prepare_resolved_symbols(
            capture,
            symbols,
            parameter_schema,
            config_input_paths,
            true,
            cfg,
            None,
        )
    }

    /// Same engine as full prepare, restricted to requested symbols (debug loop).
    pub fn prepare_for_symbols(
        cfg: &AppConfig,
        capture_session: &Path,
        symbols: &[String],
    ) -> Result<Self> {
        let (capture, selected) = resolve_selected_symbols(cfg, capture_session, symbols)?;
        let (parameter_schema, config_input_paths) = runtime_symbol_schema(cfg, &selected)?;
        Self::prepare_resolved_symbols(
            capture,
            selected,
            parameter_schema,
            config_input_paths,
            true,
            cfg,
            None,
        )
    }

    pub fn prepare_for_symbols_with_optimizer_path(
        cfg: &AppConfig,
        capture_session: &Path,
        symbols: &[String],
        optimizer_spec_path: &Path,
    ) -> Result<Self> {
        let (capture, selected) = resolve_selected_symbols(cfg, capture_session, symbols)?;
        let parameter_schema = OptimizerParameterSchema::load_root(optimizer_spec_path)?;
        let config_input_paths = optimizer_config_input_paths(optimizer_spec_path);
        Self::prepare_resolved_symbols(
            capture,
            selected,
            parameter_schema,
            config_input_paths,
            true,
            cfg,
            None,
        )
    }

    pub fn load_prepared_for_symbols_with_optimizer_path(
        cfg: &AppConfig,
        capture_session: &Path,
        symbols: &[String],
        optimizer_spec_path: &Path,
        frozen_execution_timing: Option<&ReplayExecutionTimingModel>,
    ) -> Result<Self> {
        let (capture, selected) = resolve_selected_symbols(cfg, capture_session, symbols)?;
        let parameter_schema = OptimizerParameterSchema::load_root(optimizer_spec_path)?;
        let config_input_paths = optimizer_config_input_paths(optimizer_spec_path);
        Self::prepare_resolved_symbols(
            capture,
            selected,
            parameter_schema,
            config_input_paths,
            false,
            cfg,
            frozen_execution_timing,
        )
    }

    fn prepare_resolved_symbols(
        capture: PathBuf,
        symbols: Vec<String>,
        parameter_schema: OptimizerParameterSchema,
        config_input_paths: Vec<PathBuf>,
        prepare_artifacts: bool,
        cfg: &AppConfig,
        frozen_execution_timing: Option<&ReplayExecutionTimingModel>,
    ) -> Result<Self> {
        let execution_timing = if let Some(frozen) = frozen_execution_timing {
            validate_frozen_replay_execution_timing(
                &capture,
                &cfg.polymarket.execution,
                cfg.polymarket.use_server_time,
                frozen,
                true,
            )?;
            frozen.clone()
        } else {
            select_replay_execution_timing(
                &capture,
                &cfg.polymarket.execution,
                cfg.polymarket.use_server_time,
            )?
        };
        let force_prepare = prepare_artifacts
            && std::env::var("BACKTEST_STRATEGY_PREPARE_FORCE")
                .ok()
                .is_some_and(|value| {
                    matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES")
                });
        let symbols = prepare_backtest_replay_for_capture(
            &capture,
            &symbols,
            &execution_timing,
            prepare_artifacts,
            force_prepare,
        )?;
        anyhow::ensure!(
            !symbols.is_empty(),
            "capture unavailable: no qualified requested replay symbols at {}",
            capture.display()
        );
        let mut tapes = BTreeMap::new();
        for symbol in &symbols {
            let tape = PackedReplayTape::load(&capture, symbol)
                .with_context(|| format!("load prepared replay tape symbol={symbol}"))?;
            tapes.insert(symbol.clone(), tape);
        }
        let (initial_oracle_learning, initial_oracle_learning_sha256) =
            match crate::capture::session::captured_initial_oracle_learning_bytes(&capture)? {
                Some(bytes) => {
                    let mut learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot> =
                        rmp_serde::from_slice(&bytes)
                            .context("decode captured initial oracle learning")?;
                    for symbol in learning.keys() {
                        anyhow::ensure!(
                            VALID_ASSETS
                                .iter()
                                .any(|expected| expected.eq_ignore_ascii_case(symbol)),
                            "captured initial oracle learning contains unknown capture symbol={symbol}"
                        );
                    }
                    learning.retain(|symbol, _| {
                        symbols
                            .iter()
                            .any(|expected| expected.eq_ignore_ascii_case(symbol))
                    });
                    (learning, Some(super::bytes_sha256(&bytes)))
                }
                None => (BTreeMap::new(), None),
            };
        Ok(Self {
            execution_timing,
            capture,
            symbols,
            tapes,
            initial_oracle_learning,
            initial_oracle_learning_sha256,
            parameter_schema,
            config_input_paths,
            candidate_pool_capacity: 1,
            candidate_pool_mode: ProtocolMode::Replay,
            optimizer_observability: None,
            candidate_pool: tokio::sync::Mutex::new(None),
        })
    }

    pub fn configure_candidate_pool(
        &mut self,
        capacity: usize,
        optimize: bool,
        observability: Observability,
    ) -> Result<()> {
        anyhow::ensure!(
            capacity > 0,
            "optimizer candidate pool capacity must be > 0"
        );
        anyhow::ensure!(
            self.candidate_pool.get_mut().is_none(),
            "optimizer candidate pool was configured after startup"
        );
        self.candidate_pool_capacity = capacity;
        self.candidate_pool_mode = if optimize {
            ProtocolMode::Optimize
        } else {
            ProtocolMode::Replay
        };
        self.optimizer_observability = Some(observability);
        Ok(())
    }

    pub async fn evaluate_candidate_metrics(
        &self,
        cfg: &AppConfig,
        candidates: &[BacktestMatrixCandidate],
        worker_threads: usize,
    ) -> Result<Vec<BacktestMatrixCandidateMetricsSummary>> {
        self.run_cpu_phase(cfg, candidates, worker_threads)
            .await
            .map(|results| {
                results
                    .into_iter()
                    .map(|result| {
                        let failed = candidate_runtime_failed(
                            &self.symbols,
                            &result.rows,
                            &result.canonical_session_stats,
                            &result.portfolio_accounting,
                        );
                        BacktestMatrixCandidateMetricsSummary {
                            candidate_id: result.candidate_id,
                            elapsed_ms: result.elapsed_ms,
                            canonical_session_stats: result.canonical_session_stats,
                            portfolio_accounting: result.portfolio_accounting,
                            oracle_learning: result.oracle_learning,
                            prediction_metrics: result.prediction_metrics,
                            failed,
                        }
                    })
                    .collect()
            })
    }

    pub(super) async fn evaluate_candidates_with_root(
        &self,
        cfg: &AppConfig,
        candidates: &[BacktestMatrixCandidate],
        worker_threads: usize,
        report_root: &Path,
    ) -> Result<Vec<BacktestMatrixCandidateSummary>> {
        fs::create_dir_all(report_root)
            .with_context(|| format!("create {}", report_root.display()))?;
        let results = self.run_cpu_phase(cfg, candidates, worker_threads).await?;
        let data_quality =
            ReplayDataQualitySummary::from_prepared_manifests(&self.capture, &self.symbols)?;
        data_quality.write_json(&report_root.join("replay_data_quality.json"))?;
        let config_truth = summarize_replay_config_truth(&self.capture);
        config_truth.write_json(&report_root.join("replay_config_truth.json"))?;
        let ohlc_manifest = summarize_capture_ohlc_manifest(&self.capture, cfg, &self.symbols)?;
        ohlc_manifest.write_json(&report_root.join("replay_ohlc_manifest.json"))?;
        let live_latency = frozen_replay_live_latency(&self.execution_timing)?;
        live_latency.write_json(&report_root.join("replay_live_latency.json"))?;
        let ctf_split_latency = frozen_ctf_split_latency(&self.capture, &self.execution_timing)?;
        ctf_split_latency.write_json(&report_root.join("replay_ctf_split_latency.json"))?;
        let replay_interval = capture_replay_interval(&self.capture)?;
        replay_interval.write_json(&report_root.join("replay_interval.json"))?;
        let risk_timeline = capture_replay_risk_timeline_summary(&self.capture)?;
        risk_timeline.write_json(&report_root.join("replay_risk_timeline.json"))?;
        let run_identity = build_backtest_run_identity(
            &self
                .capture
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("capture")
                .to_string(),
            &self.capture,
            &self.config_input_paths,
            None,
        )?;
        let recorded_market_population = self.recorded_market_population()?;
        let mut summaries = Vec::new();
        for result in results {
            let replay_runtime = replay_runtime_artifact(&result);
            let candidate_id = result.candidate_id;
            let mut rows = result.rows;
            rows.sort_by(|left, right| left.symbol.cmp(&right.symbol));
            let diagnostics = matrix_diagnostics(&rows);
            let recorded_market_coverage = recorded_market_population.project_entered(
                result
                    .canonical_session_stats
                    .canonical
                    .ctf_exit_cohorts
                    .iter()
                    .map(|row| (row.symbol.as_str(), row.market_id.as_str())),
            );
            let failed = candidate_runtime_failed(
                &self.symbols,
                &rows,
                &result.canonical_session_stats,
                &result.portfolio_accounting,
            );
            let report = render_report(
                &self.capture,
                report_root,
                &rows,
                &result.canonical_session_stats,
                &result.portfolio_accounting,
                failed,
            );
            let report_json = build_report_artifact(
                &candidate_id,
                &self.capture,
                report_root,
                failed,
                run_identity.clone(),
                ohlc_manifest.clone(),
                &live_latency,
                &ctf_split_latency,
                &self.execution_timing,
                replay_runtime,
                diagnostics,
                result.canonical_session_stats,
                result.portfolio_accounting,
                Some(recorded_market_coverage),
                &rows,
            );
            summaries.push(BacktestMatrixCandidateSummary {
                report,
                report_json,
                failed,
                oracle_learning: result.oracle_learning,
            });
        }
        Ok(summaries)
    }

    async fn run_cpu_phase(
        &self,
        cfg: &AppConfig,
        candidates: &[BacktestMatrixCandidate],
        worker_threads: usize,
    ) -> Result<Vec<CandidateResult>> {
        anyhow::ensure!(
            worker_threads > 0,
            "candidate evaluator worker count must be > 0"
        );
        let mut pool = self.candidate_pool.lock().await;
        if pool.is_none() {
            if let Err(error) = writeln!(
                std::io::stdout().lock(),
                "BACKTEST_MATRIX pool_spawn_begin capture={} symbols={} capacity={}",
                self.capture.display(),
                self.symbols.join(","),
                self.candidate_pool_capacity
            )
            .and_then(|()| std::io::stdout().flush())
            {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "BACKTEST_MATRIX progress_output_failed phase=pool_spawn_begin reason={error}"
                );
            }
            *pool = Some(
                PersistentCandidatePool::spawn(
                    cfg,
                    &self.capture,
                    &self.symbols,
                    &self.tapes,
                    &self.parameter_schema,
                    self.candidate_pool_capacity,
                    worker_threads,
                    self.candidate_pool_mode,
                    self.optimizer_observability.as_ref(),
                    &self.execution_timing,
                )
                .await?,
            );
            if let Err(error) = writeln!(
                std::io::stdout().lock(),
                "BACKTEST_MATRIX pool_spawn_done capture={} symbols={} capacity={}",
                self.capture.display(),
                self.symbols.join(","),
                self.candidate_pool_capacity
            )
            .and_then(|()| std::io::stdout().flush())
            {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "BACKTEST_MATRIX progress_output_failed phase=pool_spawn_done reason={error}"
                );
            }
        }
        pool.as_ref()
            .context("candidate pool initialization failed")?
            .evaluate_candidates(cfg, candidates)
    }

    pub fn capture(&self) -> &Path {
        &self.capture
    }

    pub fn replay_window(&self) -> Result<(u64, u64)> {
        capture_replay_interval(&self.capture)?
            .window()
            .context("candidate replay interval is unavailable")
    }

    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    pub fn prepared_market_lifecycles(&self) -> Vec<(String, String, u64, u64)> {
        let mut lifecycles = self
            .tapes
            .iter()
            .flat_map(|(symbol, tape)| {
                tape.market_catalog_records().iter().map(move |record| {
                    (
                        symbol.to_ascii_uppercase(),
                        digest_hex(&record.condition_id),
                        record.window_start_ms,
                        record.window_end_ms,
                    )
                })
            })
            .collect::<Vec<_>>();
        lifecycles.sort();
        lifecycles.dedup();
        lifecycles
    }

    fn recorded_market_population(&self) -> Result<RecordedMarketPopulation> {
        let mut population = RecordedMarketPopulation::default();
        for tape in self.tapes.values() {
            population.merge(tape.recorded_market_population().clone())?;
        }
        Ok(population)
    }

    pub fn captured_initial_oracle_learning(
        &self,
    ) -> &BTreeMap<String, crate::strategy::OracleLearningSnapshot> {
        &self.initial_oracle_learning
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(pool) = self.candidate_pool.get_mut().take() {
            pool.shutdown().await?;
        }
        self.tapes.clear();
        Ok(())
    }
}

fn runtime_symbol_schema(
    cfg: &AppConfig,
    symbols: &[String],
) -> Result<(OptimizerParameterSchema, Vec<PathBuf>)> {
    let first_symbol = symbols
        .first()
        .context("runtime symbol configuration list is empty")?;
    let oracle_buy_and_hold = cfg.strategy.maker_rebate_mm.oracle_buy_and_hold;
    let first_path =
        installed_optimizer_symbol_path(Path::new("."), first_symbol, oracle_buy_and_hold);
    let config_input_paths = VALID_ASSETS
        .iter()
        .map(|symbol| installed_optimizer_symbol_path(Path::new("."), symbol, oracle_buy_and_hold))
        .collect::<Vec<_>>();
    let parameter_schema = OptimizerParameterSchema::load_symbol(&first_path)?;
    for path in &config_input_paths {
        if path == &first_path {
            continue;
        }
        parameter_schema.with_symbol_defaults(path)?;
    }
    Ok((parameter_schema, config_input_paths))
}

fn optimizer_config_input_paths(optimizer_spec_path: &Path) -> Vec<PathBuf> {
    let dir = optimizer_spec_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    std::iter::once(optimizer_spec_path.to_path_buf())
        .chain(
            VALID_ASSETS
                .iter()
                .map(|symbol| dir.join(format!("{symbol}.optimization.yml"))),
        )
        .collect()
}

fn resolve_selected_symbols(
    cfg: &AppConfig,
    capture_session: &Path,
    symbols: &[String],
) -> Result<(PathBuf, Vec<String>)> {
    let requested_capture = capture_session.to_path_buf();
    let capture = resolve_capture_session_for_replay(cfg, Some(&requested_capture))?;
    let mut selected = symbols
        .iter()
        .map(|symbol| symbol.to_ascii_lowercase())
        .collect::<Vec<_>>();
    selected.sort();
    selected.dedup();
    Ok((capture, selected))
}

fn replay_runtime_artifact(result: &CandidateResult) -> BacktestMatrixReplayRuntimeArtifact {
    let has_admission_timing = result.clob_submit_model.model_version
        == crate::strategy_runtime::backtest_live_latency::EXECUTION_TIMING_MODEL_VERSION_V5;
    BacktestMatrixReplayRuntimeArtifact {
        clob_model_version: result.clob_submit_model.model_version.clone(),
        clob_model_sha256: digest_hex(&result.clob_submit_model.artifact_sha256),
        clob_captured_rows: result.clob_submit_model.captured_rows,
        clob_submit_latency_ms: result.clob_submit_model.applied_submit_latency_ms,
        clob_itode_delay_ms: result.clob_submit_model.applied_itode_delay_ms,
        pending_clob_submissions: result.pending_clob_submissions,
        pending_clob_receipts: has_admission_timing.then_some(result.pending_clob_receipts),
        clob_supported_admission_submissions: has_admission_timing
            .then_some(result.clob_supported_admission_submissions),
        clob_approximate_admission_submissions: has_admission_timing
            .then_some(result.clob_approximate_admission_submissions),
        clob_source_capture: result.clob_submit_model.source_capture.clone(),
        clob_source_config_sha256: result.clob_submit_model.source_config_sha256.clone(),
        clob_journal_path: result.clob_submit_model.journal_path.clone(),
        clob_journal_sha256: result.clob_submit_model.journal_sha256.clone(),
        clob_endpoint: result.clob_submit_model.endpoint.clone(),
        clob_unit: result.clob_submit_model.unit.clone(),
        clob_current_rule_source: result.clob_submit_model.current_rule_source.clone(),
        clob_timing_diagnostics: result.clob_timing_diagnostics.clone(),
        ctf_model_version: result.ctf_chain_model.model_version.clone(),
        ctf_model_sha256: digest_hex(&result.ctf_chain_model.artifact_sha256),
        ctf_captured_rows: result.ctf_chain_model.captured_rows,
        ctf_materialization_latency_ms: result.ctf_chain_model.applied_materialization_latency_ms,
        pending_ctf_split_receipts: result.pending_ctf_split_receipts,
        ctf_source_capture: result.ctf_chain_model.source_capture.clone(),
        ctf_source_config_sha256: result.ctf_chain_model.source_config_sha256.clone(),
        ctf_journal_path: result.ctf_chain_model.journal_path.clone(),
        ctf_journal_sha256: result.ctf_chain_model.journal_sha256.clone(),
        ctf_endpoint: result.ctf_chain_model.endpoint.clone(),
        ctf_unit: result.ctf_chain_model.unit.clone(),
        ctf_timing_diagnostics: result.ctf_timing_diagnostics.clone(),
    }
}

fn digest_hex(bytes: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn candidate_runtime_failed(
    expected_symbols: &[String],
    rows: &[super::SymbolRow],
    stats: &crate::trade_session_ledger::CanonicalSessionStats,
    portfolio_accounting: &crate::portfolio::PortfolioAccountingProjectionProof,
) -> bool {
    let expected_symbols = expected_symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let expected = expected_symbols.iter().cloned().collect::<BTreeSet<_>>();
    let actual = rows
        .iter()
        .map(|row| row.symbol.to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    actual != expected
        || rows.iter().any(|row| row.status != "PASS")
        || !stats
            .proof_for_eligible_markets(portfolio_accounting)
            .is_complete()
}

pub(super) fn matrix_worker_threads() -> Result<usize> {
    if let Some(raw) = std::env::var("BACKTEST_STRATEGY_JOBS")
        .ok()
        .filter(|raw| !raw.trim().is_empty())
    {
        let parsed = raw
            .parse::<usize>()
            .with_context(|| format!("parse BACKTEST_STRATEGY_JOBS={raw}"))?;
        anyhow::ensure!(parsed > 0, "BACKTEST_STRATEGY_JOBS must be > 0");
        return Ok(parsed);
    }
    Ok(std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(1))
}

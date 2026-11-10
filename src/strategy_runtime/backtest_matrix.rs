// Backtest data boundary:
// - Backtest uses captured symbol-scoped Arrow for tick and Polymarket venue state.
// - Backtest uses capture-local bbwick_ohlc for TA OHLC.
// - Replay must not fall back to mutable live crates/bbwick_engine/mData.

use std::collections::BTreeMap;
use std::fs;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::replay_mode::{
    finalized_capture_identity, resolve_capture_session_for_replay, PackedReplayTape,
};
use crate::config::{
    AppConfig, MakerRebateMmStrategyConfig, OptimizerParameterSchema, OptimizerParameterValue,
    VALID_ASSETS,
};
use crate::observability::Observability;
use crate::report_markdown::write_backtest_report;
use crate::strategy_runtime::backtest_matrix_report::{
    finalize_oracle_research_artifact, BacktestMatrixReportArtifact,
};
use crate::strategy_runtime::prediction_metrics::PredictionMetrics;
use crate::trade_session_ledger::CanonicalSessionStats;

pub(super) mod oracle_research;
mod reporting;
mod rows;
pub use oracle_research::BacktestOracleResearchVariant;
use oracle_research::{OracleResearchRun, PreparedOracleResearchSeed};
pub(crate) use rows::SymbolRow;

mod evaluator;
use evaluator::matrix_worker_threads;

const DEFAULT_SYMBOLS: [&str; 4] = ["btc", "eth", "sol", "xrp"];
const OFFLINE_RUN_LANE_FD_ENV: &str = "POLYMARKET_OFFLINE_RUN_LANE_FD";
const OFFLINE_RUN_LANE_LOCK_PATH: &str = "data/runtime/.retained_run_lane.lock";

static OFFLINE_RUN_LANE_LOCK: Mutex<Option<fs::File>> = Mutex::new(None);

pub fn acquire_offline_run_lane() -> Result<()> {
    let mut guard = OFFLINE_RUN_LANE_LOCK
        .lock()
        .map_err(|_| anyhow!("offline retained-run lane lock poisoned"))?;
    if guard.is_some() {
        return Ok(());
    }

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(OFFLINE_RUN_LANE_LOCK_PATH);
    let parent = path
        .parent()
        .context("offline retained-run lane lock has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;

    let file = match std::env::var(OFFLINE_RUN_LANE_FD_ENV) {
        Ok(raw_fd) => {
            let raw_fd = raw_fd
                .parse::<i32>()
                .with_context(|| format!("parse {OFFLINE_RUN_LANE_FD_ENV}"))?;
            ensure!(
                raw_fd >= 3,
                "invalid inherited retained-run lane fd {raw_fd}"
            );
            // SAFETY: `fcntl(F_GETFD)` validates that the inherited descriptor is open.
            let descriptor_flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
            if descriptor_flags == -1 {
                return Err(std::io::Error::last_os_error())
                    .context("validate inherited retained-run lane fd");
            }
            // SAFETY: the wrapper transfers sole descriptor ownership through `exec`; the
            // process-lifetime guard below retains and closes it exactly once.
            let file = unsafe { fs::File::from_raw_fd(raw_fd) };
            let inherited = file
                .metadata()
                .context("read inherited retained-run lane metadata")?;
            let canonical = fs::metadata(&path)
                .with_context(|| format!("read retained-run lane metadata {}", path.display()))?;
            ensure!(
                inherited.dev() == canonical.dev() && inherited.ino() == canonical.ino(),
                "inherited retained-run lane fd does not own {}",
                path.display()
            );
            file
        }
        Err(std::env::VarError::NotPresent) => fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open retained-run lane {}", path.display()))?,
        Err(err) => return Err(err).context("read inherited retained-run lane fd"),
    };

    // SAFETY: `file` owns a valid descriptor retained by the process-lifetime guard.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "another backtest or optimizer owns retained-run lane {}",
                path.display()
            )
        });
    }
    *guard = Some(file);
    Ok(())
}

pub fn require_inherited_offline_run_lane() -> Result<()> {
    ensure!(
        std::env::var_os(OFFLINE_RUN_LANE_FD_ENV).is_some(),
        "optimizer child missing inherited retained-run lane"
    );
    acquire_offline_run_lane()
}

pub fn inherit_offline_run_lane(command: &mut std::process::Command) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let guard = OFFLINE_RUN_LANE_LOCK
        .lock()
        .map_err(|_| anyhow!("offline retained-run lane lock poisoned"))?;
    let file = guard
        .as_ref()
        .context("offline retained-run lane is not acquired")?;
    let raw_fd = file.as_raw_fd();
    command.env(OFFLINE_RUN_LANE_FD_ENV, raw_fd.to_string());
    // SAFETY: the hook performs only async-signal-safe `fcntl` calls after fork. It clears
    // close-on-exec in this child only, preserving the parent's descriptor flags.
    unsafe {
        command.pre_exec(move || {
            let descriptor_flags = libc::fcntl(raw_fd, libc::F_GETFD);
            if descriptor_flags == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::fcntl(raw_fd, libc::F_SETFD, descriptor_flags & !libc::FD_CLOEXEC) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

pub struct BacktestMatrixArgs {
    pub capture_session: PathBuf,
    /// Empty = full four-symbol retained proof. Non-empty = same engine, subset only
    /// (debug / structural loop; not an optimized PnL score).
    pub symbols: Vec<String>,
    pub oracle_learning_seed: Option<PathBuf>,
    pub oracle_learning_seed_sha256: Option<String>,
    pub oracle_research_variant: Option<BacktestOracleResearchVariant>,
    pub oracle_research_root: Option<PathBuf>,
}

#[derive(Clone)]
pub struct BacktestMatrixCandidate {
    pub id: String,
    pub strategy_config: MakerRebateMmStrategyConfig,
    pub per_symbol_strategy_config: BTreeMap<String, MakerRebateMmStrategyConfig>,
    pub per_symbol_parameter_values: BTreeMap<String, BTreeMap<String, OptimizerParameterValue>>,
    pub initial_oracle_learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
    pub oracle_direct_market_research: bool,
    pub collect_prediction_metrics: bool,
}

pub struct BacktestMatrixEvaluator {
    capture: PathBuf,
    symbols: Vec<String>,
    tapes: BTreeMap<String, Arc<PackedReplayTape>>,
    initial_oracle_learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
    initial_oracle_learning_sha256: Option<String>,
    parameter_schema: OptimizerParameterSchema,
    config_input_paths: Vec<PathBuf>,
    candidate_pool_capacity: usize,
    candidate_pool_mode: crate::strategy_runtime::symbol_process::ProtocolMode,
    optimizer_observability: Option<Observability>,
    execution_timing: crate::strategy_runtime::backtest_live_latency::ReplayExecutionTimingModel,
    candidate_pool:
        tokio::sync::Mutex<Option<crate::strategy_runtime::replay_mode::PersistentCandidatePool>>,
}

#[derive(Clone, Debug)]
struct BacktestMatrixCandidateSummary {
    report: String,
    report_json: BacktestMatrixReportArtifact,
    failed: bool,
    oracle_learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
}

pub struct BacktestMatrixCandidateMetricsSummary {
    pub candidate_id: String,
    pub elapsed_ms: u128,
    pub canonical_session_stats: CanonicalSessionStats,
    pub portfolio_accounting: crate::portfolio::PortfolioAccountingProjectionProof,
    pub oracle_learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
    pub prediction_metrics: BTreeMap<String, PredictionMetrics>,
    pub failed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BacktestSymbolDiagnostics {
    pub symbol: String,
    pub status: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BacktestMatrixDiagnostics {
    pub runnable: u64,
    pub passed: u64,
    pub skipped: u64,
    pub symbols: Vec<BacktestSymbolDiagnostics>,
}

impl Default for BacktestMatrixArgs {
    fn default() -> Self {
        Self {
            capture_session: PathBuf::from("latest"),
            symbols: Vec::new(),
            oracle_learning_seed: None,
            oracle_learning_seed_sha256: None,
            oracle_research_variant: None,
            oracle_research_root: None,
        }
    }
}

fn capture_report_root(capture: &Path, symbol_subset: bool) -> PathBuf {
    // Subset runs write under debug/ so they never clobber the full four-symbol matrix proof.
    if symbol_subset {
        capture.join("backtest_reports").join("debug")
    } else {
        capture.join("backtest_reports").join("matrix")
    }
}

pub fn load_backtest_config_for_capture(capture: &Path) -> Result<AppConfig> {
    let cfg = AppConfig::load().context(
        "load clean current backtest config; capture config_snapshot is diagnostic-only",
    )?;
    finalized_capture_identity(capture)?;
    Ok(cfg)
}

pub fn load_backtest_config_for_capture_with_optimizer_path(
    capture: &Path,
    config_path: &Path,
    optimizer_path: &Path,
) -> Result<AppConfig> {
    let cfg = AppConfig::load_from_path_with_optimizer_path(config_path, optimizer_path)
        .with_context(|| {
            format!(
                "load backtest config from frozen app config {} and optimizer path {}",
                config_path.display(),
                optimizer_path.display()
            )
        })?;
    finalized_capture_identity(capture)?;
    Ok(cfg)
}

fn apply_backtest_symbol_universe(cfg: &mut AppConfig, symbols: &[String]) {
    let mut enabled_assets = symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    enabled_assets.sort();
    enabled_assets.dedup();
    cfg.strategy.maker_rebate_mm.enabled_assets = enabled_assets;
    for symbol_cfg in cfg
        .strategy
        .maker_rebate_mm
        .optimizer_symbol_configs
        .values_mut()
    {
        symbol_cfg.enabled_assets = cfg.strategy.maker_rebate_mm.enabled_assets.clone();
    }
    for symbol in symbols {
        let oracle_symbol = cfg.bbwick_oracle.resolve_symbol_for_underlying(symbol);
        cfg.bbwick_oracle.symbols.insert(oracle_symbol, true);
    }
}

pub async fn run_backtest_strategy_matrix(
    cfg: &AppConfig,
    args: BacktestMatrixArgs,
) -> Result<i32> {
    acquire_offline_run_lane()?;
    let symbol_subset = !args.symbols.is_empty();
    let research_request = match (
        args.oracle_research_variant,
        args.oracle_research_root.as_deref(),
    ) {
        (Some(variant), Some(root)) => Some((variant, root)),
        (None, None) => None,
        _ => anyhow::bail!("oracle research variant/root must be supplied together"),
    };
    let research_symbols = if args.symbols.is_empty() {
        DEFAULT_SYMBOLS
            .iter()
            .map(|symbol| (*symbol).to_string())
            .collect::<Vec<_>>()
    } else {
        args.symbols.clone()
    };
    let (mut evaluator, backtest_cfg, mut research_run) = if let Some((variant, research_root)) =
        research_request
    {
        let capture = resolve_capture_session_for_replay(cfg, Some(&args.capture_session))?;
        let run = OracleResearchRun::prepare(variant, research_root, &capture, &research_symbols)?;
        let mut frozen_cfg = load_backtest_config_for_capture_with_optimizer_path(
            &capture,
            run.frozen_config_path(),
            run.frozen_optimizer_path(),
        )?;
        variant.apply_trade_flow_setting(&mut frozen_cfg.strategy.maker_rebate_mm);
        let frozen_execution_timing = run.frozen_execution_timing()?;
        let evaluator = BacktestMatrixEvaluator::load_prepared_for_symbols_with_optimizer_path(
            &frozen_cfg,
            &capture,
            run.symbols(),
            run.frozen_optimizer_path(),
            frozen_execution_timing.as_ref(),
        )?;
        apply_backtest_symbol_universe(&mut frozen_cfg, evaluator.symbols());
        (evaluator, frozen_cfg, Some(run))
    } else {
        let evaluator = if symbol_subset {
            BacktestMatrixEvaluator::prepare_for_symbols(cfg, &args.capture_session, &args.symbols)?
        } else {
            BacktestMatrixEvaluator::prepare(cfg, &args.capture_session)?
        };
        let mut backtest_cfg = load_backtest_config_for_capture(evaluator.capture())?;
        apply_backtest_symbol_universe(&mut backtest_cfg, evaluator.symbols());
        (evaluator, backtest_cfg, None)
    };
    let execution_timing_sha256 = if research_run.is_some() {
        Some(evaluator.execution_timing.sha256()?)
    } else {
        None
    };
    let explicit_learning_seed = match (
        args.oracle_learning_seed.as_deref(),
        args.oracle_learning_seed_sha256.as_deref(),
    ) {
        (Some(path), Some(expected_sha256)) => {
            let bytes = fs::read(path).with_context(|| {
                format!("read research oracle learning seed {}", path.display())
            })?;
            let actual_sha256 = bytes_sha256(&bytes);
            ensure!(
                actual_sha256 == expected_sha256.to_ascii_lowercase(),
                "research oracle learning seed content changed"
            );
            Some((bytes, actual_sha256, path.display().to_string()))
        }
        (None, None) => None,
        _ => anyhow::bail!("research oracle learning seed path/hash mismatch"),
    };
    let explicit_seed = explicit_learning_seed.is_some();
    let (mut source_learning, source_name, source_sha256) = match explicit_learning_seed.as_ref() {
        Some((bytes, actual_sha256, source_path)) => {
            let learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot> =
                rmp_serde::from_slice(bytes).with_context(|| {
                    format!("decode research oracle learning seed {source_path}")
                })?;
            (learning, "explicit_research", Some(actual_sha256.clone()))
        }
        None if evaluator.initial_oracle_learning_sha256.is_some() => (
            evaluator.initial_oracle_learning.clone(),
            "captured",
            evaluator.initial_oracle_learning_sha256.clone(),
        ),
        None => (BTreeMap::new(), "empty", None),
    };
    for symbol in source_learning.keys() {
        ensure!(
            VALID_ASSETS
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(symbol)),
            "research oracle learning seed contains unknown symbol={symbol}"
        );
    }
    source_learning.retain(|symbol, _| {
        evaluator
            .symbols
            .iter()
            .any(|expected| expected.eq_ignore_ascii_case(symbol))
    });
    let (initial_oracle_learning, initial_learning_source, initial_learning_sha256, research_seed) =
        if let Some(run) = research_run.as_ref() {
            let source_sha256 = source_sha256
                .as_deref()
                .context("frozen oracle research capture has no learning seed")?;
            let prepared = run.prepare_seed(
                evaluator.capture(),
                source_learning,
                source_sha256,
                explicit_seed,
            )?;
            (
                prepared.learning.clone(),
                prepared.source.to_string(),
                Some(prepared.sha256.clone()),
                Some(prepared),
            )
        } else {
            (
                source_learning,
                source_name.to_string(),
                source_sha256,
                None::<PreparedOracleResearchSeed>,
            )
        };

    let worker_threads = matrix_worker_threads()?;
    let per_symbol_parameter_values = evaluator
        .symbols
        .iter()
        .map(|symbol| {
            let symbol_path = match research_run.as_ref() {
                Some(run) => run.frozen_symbol_optimizer_path(symbol)?.to_path_buf(),
                None => crate::config::installed_optimizer_symbol_path(
                    Path::new("."),
                    symbol,
                    backtest_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
                ),
            };
            let values = evaluator
                .parameter_schema
                .with_symbol_defaults(&symbol_path)?
                .default_map()?;
            Ok((symbol.to_ascii_uppercase(), values))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let root = research_run
        .as_ref()
        .map(|run| run.output_root().to_path_buf())
        .unwrap_or_else(|| capture_report_root(evaluator.capture(), symbol_subset));
    if research_run.is_none() {
        let preserved_optimizer_sidecars = preserve_optimizer_report_sidecars(&root)?;
        if root.exists() {
            fs::remove_dir_all(&root).with_context(|| format!("remove {}", root.display()))?;
        }
        fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
        restore_optimizer_report_sidecars(&root, preserved_optimizer_sidecars)?;
    }

    let candidates = vec![BacktestMatrixCandidate {
        id: research_run.as_ref().map_or_else(
            || {
                if symbol_subset {
                    "debug".to_string()
                } else {
                    "default".to_string()
                }
            },
            |run| run.variant().as_str().to_string(),
        ),
        strategy_config: backtest_cfg.strategy.maker_rebate_mm.clone(),
        per_symbol_strategy_config: BTreeMap::new(),
        per_symbol_parameter_values,
        initial_oracle_learning: initial_oracle_learning.clone(),
        oracle_direct_market_research: matches!(
            research_run.as_ref().map(|run| run.variant()),
            Some(BacktestOracleResearchVariant::DirectMarket)
        ),
        collect_prediction_metrics: false,
    }];
    if let (Some(run), Some(seed)) = (research_run.as_mut(), research_seed.as_ref()) {
        let execution_timing_sha256 = execution_timing_sha256
            .as_deref()
            .context("oracle research execution timing digest is absent")?;
        let observability = run.start_observability(&backtest_cfg).await?;
        evaluator.configure_candidate_pool(1, false, observability)?;
        run.trace_start(seed, &backtest_cfg.strategy.maker_rebate_mm)?;
        tracing::trace!(
            target: "strategy.oracle_probability",
            event = "oracle_research_execution_timing",
            execution_timing_sha256 = %execution_timing_sha256,
        );
    } else if symbol_subset {
        println!(
            "BACKTEST_DEBUG symbols={} report_root={} note=structural_only_not_optimized_pnl_score",
            evaluator.symbols.join(","),
            root.display()
        );
    }

    let mut evaluator = Some(evaluator);
    let mut summaries = evaluator
        .as_ref()
        .context("backtest evaluator is absent")?
        .evaluate_candidates_with_root(&backtest_cfg, &candidates, worker_threads, &root)
        .await?;
    let mut summary = summaries
        .pop()
        .context("default backtest candidate missing summary")?;
    let research_flood_control = if let Some(run) = research_run.as_mut() {
        evaluator
            .take()
            .context("oracle research evaluator is absent")?
            .shutdown()
            .await?;
        run.trace_end(summary.failed);
        Some(run.shutdown_observability().await?)
    } else {
        None
    };

    let final_learning_bytes = rmp_serde::to_vec_named(&summary.oracle_learning)?;
    let final_learning_sha256 = bytes_sha256(&final_learning_bytes);
    let final_learning_path = root.join("oracle_learning_final.rmp");
    crate::runtime_files::write_bytes_atomic(&final_learning_path, &final_learning_bytes)?;
    summary.report_json.oracle_learning_initial_source = initial_learning_source.clone();
    summary.report_json.oracle_learning_initial_sha256 = initial_learning_sha256.clone();
    summary.report_json.oracle_learning_initial_cutoffs_ms = initial_oracle_learning
        .iter()
        .map(|(symbol, learning)| (symbol.clone(), learning.knowledge_cutoff_ms()))
        .collect();
    summary.report_json.oracle_learning_initial_metrics = initial_oracle_learning
        .iter()
        .map(|(symbol, learning)| (symbol.clone(), learning.metrics()))
        .collect();
    summary.report_json.oracle_learning_final_path = final_learning_path.display().to_string();
    summary.report_json.oracle_learning_final_sha256 = final_learning_sha256.clone();
    summary.report_json.oracle_learning_final_cutoffs_ms = summary
        .oracle_learning
        .iter()
        .map(|(symbol, learning)| (symbol.clone(), learning.knowledge_cutoff_ms()))
        .collect();
    summary.report_json.oracle_learning_final_metrics = summary
        .oracle_learning
        .iter()
        .map(|(symbol, learning)| (symbol.clone(), learning.metrics()))
        .collect();
    if let (Some(run), Some(seed), Some(flood_control)) = (
        research_run.as_ref(),
        research_seed.as_ref(),
        research_flood_control.as_ref(),
    ) {
        finalize_oracle_research_artifact(
            &mut summary.report_json,
            run,
            seed,
            &final_learning_sha256,
            &summary.oracle_learning,
            execution_timing_sha256
                .as_deref()
                .context("oracle research execution timing digest is absent")?,
            flood_control,
        )?;
    }

    let body = summary.report;
    let report_path = root.join("backtest_strategy_report.txt");
    fs::write(&report_path, &body).with_context(|| format!("write {}", report_path.display()))?;
    let report_json_path = root.join("backtest_strategy_report.json");
    fs::write(
        &report_json_path,
        serde_json::to_vec_pretty(&summary.report_json)?,
    )
    .with_context(|| format!("write {}", report_json_path.display()))?;
    let report_md_path = root.join("backtest_strategy_report.md");
    write_backtest_report(&report_md_path, &summary.report_json)
        .with_context(|| format!("write {}", report_md_path.display()))?;
    if research_run.is_none() && !symbol_subset {
        let monitor_report_md_path = Path::new("logs").join("monitors").join("backtest_stats.md");
        write_backtest_report(&monitor_report_md_path, &summary.report_json)
            .with_context(|| format!("write {}", monitor_report_md_path.display()))?;
        println!("REPORT_MONITOR_MD {}", monitor_report_md_path.display());
    }
    print!("{body}");
    println!("REPORT {}", report_path.display());
    println!("REPORT_JSON {}", report_json_path.display());
    println!("REPORT_MD {}", report_md_path.display());
    println!(
        "ORACLE_LEARNING_INPUT source={} sha256={} cutoffs={:?}",
        initial_learning_source,
        initial_learning_sha256.as_deref().unwrap_or("none"),
        initial_oracle_learning
            .iter()
            .map(|(symbol, learning)| (symbol, learning.knowledge_cutoff_ms()))
            .collect::<BTreeMap<_, _>>()
    );
    println!(
        "ORACLE_LEARNING_FINAL path={} sha256={} cutoffs={:?}",
        final_learning_path.display(),
        final_learning_sha256,
        summary
            .oracle_learning
            .iter()
            .map(|(symbol, learning)| (symbol, learning.knowledge_cutoff_ms()))
            .collect::<BTreeMap<_, _>>()
    );
    let exit_code = if summary.failed { 1 } else { 0 };
    if let Some(evaluator) = evaluator {
        evaluator.shutdown().await?;
    }
    Ok(exit_code)
}

fn bytes_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn preserve_optimizer_report_sidecars(root: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    const OPTIMIZER_REPORT_SIDECARS: [&str; 2] =
        ["optimizer_status.json", "optimizer_summary.json"];
    let mut sidecars = Vec::new();
    for file_name in OPTIMIZER_REPORT_SIDECARS {
        let path = root.join(file_name);
        if path.exists() {
            sidecars.push((
                file_name.to_string(),
                fs::read(&path).with_context(|| format!("read {}", path.display()))?,
            ));
        }
    }
    Ok(sidecars)
}

fn restore_optimizer_report_sidecars(root: &Path, sidecars: Vec<(String, Vec<u8>)>) -> Result<()> {
    for (file_name, bytes) in sidecars {
        let path = root.join(file_name);
        fs::write(&path, bytes).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

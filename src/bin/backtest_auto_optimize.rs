use anyhow::{anyhow, bail, ensure, Context, Result};
use cmaes::DVector;
use polymarket_hft::config::{
    AppConfig, MakerRebateMmStrategyConfig, OptimizerParameterSchema,
    OptimizerParameterSpec as ParamSpec, OptimizerParameterValue as ParamValue, VALID_ASSETS,
};
use polymarket_hft::observability::ObservabilityRuntime;
use polymarket_hft::portfolio::{PortfolioAccountingPnlRow, PortfolioAccountingProjectionProof};
use polymarket_hft::strategy::OracleLearningSnapshot;
use polymarket_hft::strategy_runtime::backtest_matrix::{
    acquire_offline_run_lane, require_inherited_offline_run_lane, BacktestMatrixCandidate,
    BacktestMatrixEvaluator,
};
use polymarket_hft::trade_session_ledger::CanonicalSessionStats;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "backtest_auto_optimize/apply.rs"]
mod apply;
#[path = "backtest_auto_optimize/archive_contenders.rs"]
mod archive_contenders;
#[path = "backtest_auto_optimize/candidate_proof.rs"]
mod candidate_proof;
#[path = "backtest_auto_optimize/capture_inputs.rs"]
mod capture_inputs;
#[path = "backtest_auto_optimize/child_artifact.rs"]
mod child_artifact;
#[path = "backtest_auto_optimize/cli.rs"]
mod cli;
#[path = "backtest_auto_optimize/config.rs"]
mod config;
#[path = "backtest_auto_optimize/default_replay_guard.rs"]
mod default_replay_guard;
#[path = "backtest_auto_optimize/evaluate.rs"]
mod evaluate;
#[path = "backtest_auto_optimize/holdout.rs"]
mod holdout;
#[path = "backtest_auto_optimize/objective_profile.rs"]
mod objective_profile;
#[path = "backtest_auto_optimize/params.rs"]
mod params;
#[path = "backtest_auto_optimize/population.rs"]
mod population;
#[path = "backtest_auto_optimize/process_parent.rs"]
mod process_parent;
#[path = "backtest_auto_optimize/resource_resolver.rs"]
mod resource_resolver;
#[path = "backtest_auto_optimize/scoring.rs"]
mod scoring;
#[path = "backtest_auto_optimize/search.rs"]
mod search;
#[path = "backtest_auto_optimize/summary.rs"]
mod summary;
#[path = "backtest_auto_optimize/types.rs"]
mod types;

use apply::{apply_joint_candidate, export_selected_oracle_learning};
use archive_contenders::evaluate_archived_contenders;
use cli::{env_bool, env_parse, parse_args};
use config::{load_optimizer_settings, load_symbol_schema_from_snapshot};
use default_replay_guard::assert_default_replay_stable;
use evaluate::evaluate_params;
use objective_profile::ObjectiveProfile;
use params::{default_params, params_from_vector};
pub(crate) use population::{
    candidate_id_for_params, default_params_by_symbol, evaluate_population,
    initial_mean_for_joint_params, strategy_configs_for_params,
};
use resource_resolver::resolve_resource_policy;
use scoring::endpoint_joint_score;
use search::{run_optimizer_search, select_search_incumbent, select_symbol_winner};
use summary::{append_trial, write_optimizer_status};
pub(crate) use types::{
    Args, CandidateRequest, OptimizerSettings, RetainedCaptureProof, RetainedPackScore, RunContext,
    SelectionMode, TrialOrigin, TrialRecord,
};

const OPTIMIZE_RUNS_DIR: &str = "optimizer_runs";

fn main() -> Result<()> {
    let args = parse_args()?;
    if args.default_spec_snapshot.is_none() {
        acquire_offline_run_lane()?;
        if args.proof_only_run_root.is_some() {
            return process_parent::run_proof_only(args);
        }
        if args.finalize_retained_run_root.is_some() {
            return process_parent::run_retained_finalization(args);
        }
        return process_parent::run(args);
    }
    require_inherited_offline_run_lane()?;
    run_symbol_child(args)
}

fn run_symbol_child(args: Args) -> Result<()> {
    let snapshot = args
        .default_spec_snapshot
        .as_deref()
        .context("symbol child missing optimizer snapshot")?;
    let symbol = child_symbol_from_snapshot(snapshot)?;
    ensure!(
        symbol == args.symbol,
        "symbol child authority mismatch argument={} snapshot={symbol}",
        args.symbol
    );
    let root_spec_sha256 = args
        .root_spec_sha256
        .as_deref()
        .context("symbol child root spec hash missing")?;
    let symbol_spec_sha256 = args
        .default_spec_sha256
        .as_deref()
        .context("symbol child symbol spec hash missing")?;
    let config_snapshot = args
        .spec
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("config.main.toml");
    ensure!(
        candidate_proof::file_sha256(snapshot)? == symbol_spec_sha256,
        "symbol child optimizer snapshot changed symbol={symbol}"
    );
    ensure!(
        candidate_proof::file_sha256(&args.spec)? == root_spec_sha256,
        "symbol child root optimizer snapshot changed symbol={symbol}"
    );
    let objective_profile = ObjectiveProfile::load(
        &args
            .spec
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("objective_profile.yml"),
    )?;
    ensure!(
        Some(objective_profile.sha256()) == args.objective_profile_sha256.as_deref(),
        "symbol child objective profile changed symbol={symbol}"
    );

    let mut settings = load_optimizer_settings(&args.spec, &args)?;
    settings.apply_best = false;
    let resource_policy = resolve_resource_policy(settings.jobs)?;
    settings.jobs = resource_policy.effective_jobs;
    let cfg = AppConfig::load_from_path_with_symbol_optimizer_snapshot(
        &config_snapshot,
        &args.spec,
        snapshot,
    )?;
    let symbols = vec![symbol.clone()];
    let frozen = capture_inputs::FrozenCaptureInputs::load_symbol_snapshot(
        snapshot,
        &symbol,
        args.frozen_capture_inputs_sha256
            .as_deref()
            .context("symbol child frozen input hash missing")?,
        &cfg,
        &args.spec,
        objective_profile.sha256(),
    )?;
    ensure!(
        frozen.holdout.is_empty(),
        "symbol child search must not evaluate validation capture B"
    );
    let frozen_capture = &frozen
        .selection
        .first()
        .context("symbol child frozen selection capture missing")?
        .capture;
    ensure!(
        args.selection_captures
            .first()
            .is_some_and(|capture| Path::new(capture) == frozen_capture)
            && Path::new(&args.capture_session) == frozen_capture,
        "symbol child capture arguments differ from frozen parent input symbol={symbol}"
    );
    let run_root = args
        .parent_run_root
        .as_ref()
        .context("symbol child parent run root missing")?
        .join("children")
        .join(&symbol);
    fs::create_dir_all(&run_root)?;
    let parameter_schema = load_symbol_schema_from_snapshot(snapshot, &args.spec)?;
    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.jobs.max(1))
        .enable_all()
        .build()
        .context("build optimizer child async runtime")?;
    let mut observability_cfg = cfg.observability.clone();
    observability_cfg.enabled = false;
    observability_cfg.logging.dir = run_root.display().to_string();
    observability_cfg.logging.action_log_enabled = false;
    observability_cfg.dashboard.enabled = false;
    let optimizer_observability = async_runtime.block_on(ObservabilityRuntime::start(
        &observability_cfg,
        "optimizer_symbol",
    ))?;
    let ctx = RunContext {
        selection_mode: settings.selection_mode,
        run_root: run_root.clone(),
        objective_profile,
        cfg,
        selection_inputs: frozen.selection.clone(),
        holdout_inputs: Vec::new(),
        optimizer_spec_path: args.spec.clone(),
        async_runtime,
        optimizer_observability,
        candidate_slots: settings.jobs.max(1),
        symbols: symbols.clone(),
        parameter_schemas: BTreeMap::from([(symbol.clone(), parameter_schema)]),
        selection_evaluator: Mutex::new(None),
        trial_counter: AtomicUsize::new(0),
        trials: Mutex::new(Vec::new()),
    };

    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE symbol_search_begin symbol={} root={} jobs={} max_function_evals={} max_no_improvement_evals={} seed={} selection_mode={}",
        symbol,
        run_root.display(),
        settings.jobs,
        settings.max_function_evals,
        settings.max_no_improvement_evals,
        settings.seed,
        settings.selection_mode.as_str()
    ));
    let work_result = (|| -> Result<()> {
        evaluate_default_candidate(&ctx, TrialOrigin::Default)?;
        let default_recheck_index = assert_default_replay_stable(&ctx)?;
        evaluate_archived_contenders(&ctx)?;
        let incumbent = {
            let trials = ctx
                .trials
                .lock()
                .map_err(|_| anyhow!("trial mutex poisoned"))?;
            select_search_incumbent(&trials)?
        };
        let search = run_optimizer_search(&ctx, &settings, &incumbent)?;
        let winner = {
            let trials = ctx
                .trials
                .lock()
                .map_err(|_| anyhow!("trial mutex poisoned"))?;
            select_symbol_winner(&trials)?.context("symbol search winner missing")?
        };
        let default_id = {
            let trials = ctx
                .trials
                .lock()
                .map_err(|_| anyhow!("trial mutex poisoned"))?;
            trials
                .first()
                .context("symbol default trial missing")?
                .candidate_id
                .clone()
        };
        let recheck_index = if winner.candidate_id == default_id {
            default_recheck_index
        } else {
            recheck_winner(&ctx, &winner)?
        };
        child_artifact::write_child_artifact(
            &ctx,
            &frozen,
            &settings,
            &search,
            winner.trial_index,
            recheck_index,
            snapshot,
            symbol_spec_sha256,
            root_spec_sha256,
            &config_snapshot,
        )?;
        summary::write_optimizer_selected_status(&ctx, "symbol_complete", recheck_index)?;
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE symbol_search_done symbol={} function_evals={} selected_candidate_id={}",
            symbol, search.function_evals, winner.candidate_id
        ));
        Ok(())
    })();
    shutdown_run_context(ctx, work_result)
}

fn recheck_winner(ctx: &RunContext, winner: &search::SymbolWinner) -> Result<usize> {
    let (params, score, selection, packs, learning) = {
        let trials = ctx
            .trials
            .lock()
            .map_err(|_| anyhow!("trial mutex poisoned"))?;
        let source = trials
            .iter()
            .find(|trial| trial.index == winner.trial_index)
            .context("symbol winner source trial missing")?;
        (
            source.params_by_symbol.clone(),
            source.selection_score,
            scoring::trial_selection_score(source)?,
            source.retained_pack_scores.clone(),
            source.oracle_learning_sha256.clone(),
        )
    };
    evaluate_params(
        ctx,
        CandidateRequest::new(params.clone(), TrialOrigin::ChampionRecheck),
        true,
    )?;
    let trials = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?;
    let rechecked = trials.last().context("symbol winner recheck missing")?;
    ensure!(
        rechecked.selection_mode == ctx.selection_mode
            && rechecked.candidate_id == winner.candidate_id
            && rechecked.params_by_symbol == params
            && rechecked.selection_score == score
            && scoring::trial_selection_score(rechecked)? == selection
            && rechecked.retained_pack_scores == packs
            && rechecked.oracle_learning_sha256 == learning,
        "symbol winner changed during frozen A recheck candidate_id={}",
        winner.candidate_id
    );
    Ok(rechecked.index)
}

fn child_symbol_from_snapshot(snapshot: &Path) -> Result<String> {
    let name = snapshot
        .file_name()
        .and_then(|value| value.to_str())
        .with_context(|| format!("invalid symbol snapshot {}", snapshot.display()))?;
    VALID_ASSETS
        .iter()
        .find(|symbol| name == format!("{symbol}.optimization.yml"))
        .map(|symbol| (*symbol).to_string())
        .with_context(|| format!("invalid symbol snapshot filename {name}"))
}

fn shutdown_run_context(ctx: RunContext, work_result: Result<()>) -> Result<()> {
    let selection_shutdown = evaluate::shutdown_selection_evaluator(&ctx);
    let RunContext {
        async_runtime,
        optimizer_observability,
        ..
    } = ctx;
    async_runtime.block_on(optimizer_observability.shutdown());
    drop(async_runtime);
    match (work_result, selection_shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(shutdown_error)) => Err(error.context(format!(
            "selection evaluator shutdown also failed: {shutdown_error:#}"
        ))),
    }
}

fn evaluate_default_candidate(ctx: &RunContext, origin: TrialOrigin) -> Result<f64> {
    evaluate_params(
        ctx,
        CandidateRequest::new(default_params_by_symbol(ctx)?, origin),
        true,
    )
}

fn timestamp_run_id() -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(format!("{}_{:09}", now.as_secs(), now.subsec_nanos()))
}

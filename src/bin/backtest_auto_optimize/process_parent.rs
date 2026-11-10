use super::*;
use crate::capture_inputs::FrozenCaptureInputs;

#[path = "process_parent/combined_replay.rs"]
mod combined_replay;
#[path = "process_parent/inputs.rs"]
pub(super) mod inputs;
#[path = "process_parent/proof_only.rs"]
mod proof_only;
#[path = "process_parent/replay_proof.rs"]
mod replay_proof;
#[path = "process_parent/report.rs"]
mod report;
#[path = "process_parent/retained_finalization.rs"]
mod retained_finalization;

pub(super) use proof_only::run_proof_only;
pub(super) use retained_finalization::run_retained_finalization;

pub(crate) const RETAINED_FINALIZATION_RUN_ROOT: &str =
    "data/captures/20261008T144229Z_live_2715964/backtest_reports/optimizer_runs/1792444644_853221286";
const RETAINED_VALIDATION_CAPTURE: &str = "data/captures/20261007T224506Z_live_2507692";

pub(super) fn run(mut args: Args) -> Result<()> {
    let config_path = Path::new("config/main.toml");
    let config_bytes =
        fs::read(config_path).with_context(|| format!("read {}", config_path.display()))?;
    let parsed_cfg = AppConfig::parse_from_bytes(&config_bytes)?;
    if !args.spec_explicit {
        args.spec = PathBuf::from(if parsed_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold {
            "optimization.buy.yml"
        } else {
            "optimization.yml"
        });
    }
    let root_spec_bytes =
        fs::read(&args.spec).with_context(|| format!("read {}", args.spec.display()))?;
    let objective_profile = ObjectiveProfile::load(Path::new("objective_profile.yml"))?;
    let mut settings = load_optimizer_settings(&args.spec, &args)?;
    let resource_policy = resolve_resource_policy(settings.jobs)?;
    settings.jobs = resource_policy.effective_jobs;
    let symbols = VALID_ASSETS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect::<Vec<_>>();
    let oracle_buy_and_hold = parsed_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold;
    let locator_cfg = parsed_cfg.with_installed_optimizer_path(&args.spec)?;
    let frozen_captures = FrozenCaptureInputs::freeze(
        &locator_cfg,
        &args,
        &symbols,
        &args.spec,
        objective_profile.sha256(),
        settings.jobs,
    )?;
    let Some(selection_input) = frozen_captures.selection.first() else {
        summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE finished outcome=selection_unavailable selection_captures=0 searched=false applied=false"
        ));
        return Ok(());
    };
    let participating_symbols = frozen_captures.symbols()?;
    let capture = selection_input.capture.clone();
    let run_root = capture
        .join("backtest_reports")
        .join(OPTIMIZE_RUNS_DIR)
        .join(timestamp_run_id()?);
    let defaults_root = run_root.join("defaults");
    fs::create_dir_all(&defaults_root)?;

    let config_snapshot = defaults_root.join("config.main.toml");
    fs::write(&config_snapshot, &config_bytes)?;
    let app_config_sha256 = format!("{:x}", Sha256::digest(&config_bytes));
    let root_spec_snapshot = defaults_root.join("optimization.yml");
    fs::write(&root_spec_snapshot, &root_spec_bytes)?;
    let root_spec_sha256 = format!("{:x}", Sha256::digest(&root_spec_bytes));
    let frozen_selection_mode = config::load_optimizer_selection_mode(&root_spec_snapshot)?;
    ensure!(
        frozen_selection_mode == settings.selection_mode,
        "optimizer selection mode changed while freezing root settings"
    );
    inputs::snapshot_objective_profile(&defaults_root, &objective_profile)?;
    frozen_captures.write_symbol_snapshot(&root_spec_snapshot)?;

    let mut symbol_files = BTreeMap::new();
    let mut symbol_snapshots = BTreeMap::new();
    let mut symbol_sha256s = BTreeMap::new();
    let mut parameter_schemas = BTreeMap::new();
    let mut parameter_schema_sha256s = BTreeMap::new();
    for symbol in &symbols {
        let source = inputs::symbol_spec_path(&args.spec, symbol, oracle_buy_and_hold);
        let bytes = fs::read(&source).with_context(|| format!("read {}", source.display()))?;
        let snapshot = defaults_root.join(format!("{symbol}.optimization.yml"));
        fs::write(&snapshot, &bytes)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let schema = load_symbol_schema_from_snapshot(&snapshot, &root_spec_snapshot)?;
        parameter_schema_sha256s
            .insert(symbol.clone(), child_artifact::hex_bytes(&schema.sha256()));
        parameter_schemas.insert(symbol.clone(), schema);
        symbol_files.insert(symbol.clone(), bytes);
        symbol_snapshots.insert(symbol.clone(), snapshot);
        symbol_sha256s.insert(symbol.clone(), sha256);
    }
    inputs::verify_frozen_inputs(
        &args.spec,
        oracle_buy_and_hold,
        &root_spec_bytes,
        &config_snapshot,
        &app_config_sha256,
        &root_spec_snapshot,
        &root_spec_sha256,
        objective_profile.sha256(),
        &symbol_files,
        &symbol_snapshots,
        &symbol_sha256s,
    )?;

    let frozen_cfg = polymarket_hft::strategy_runtime::backtest_matrix::load_backtest_config_for_capture_with_optimizer_path(
        &capture,
        &config_snapshot,
        &root_spec_snapshot,
    )?;
    let mut initial_oracle_learning =
        match polymarket_hft::capture::session::captured_initial_oracle_learning_bytes(&capture)? {
            Some(bytes) => {
                rmp_serde::from_slice::<BTreeMap<String, OracleLearningSnapshot>>(&bytes)
                    .context("decode frozen capture initial oracle learning")?
            }
            None => BTreeMap::new(),
        };
    initial_oracle_learning.retain(|symbol, _| participating_symbols.contains(symbol));
    let initial_oracle_learning_sha256 =
        candidate_proof::oracle_learning_sha256(&initial_oracle_learning)?;
    ensure!(
        initial_oracle_learning_sha256 == selection_input.initial_oracle_learning_sha256,
        "frozen selection learner identity changed"
    );

    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.jobs.max(1))
        .enable_all()
        .build()
        .context("build optimizer parent async runtime")?;
    let mut observability_cfg = frozen_cfg.observability.clone();
    observability_cfg.enabled = false;
    observability_cfg.logging.dir = run_root.display().to_string();
    observability_cfg.logging.action_log_enabled = false;
    observability_cfg.dashboard.enabled = false;
    let optimizer_observability = async_runtime.block_on(ObservabilityRuntime::start(
        &observability_cfg,
        "optimizer_parent",
    ))?;
    let ctx = RunContext {
        selection_mode: frozen_selection_mode,
        run_root: run_root.clone(),
        objective_profile,
        cfg: frozen_cfg,
        selection_inputs: frozen_captures.selection.clone(),
        holdout_inputs: frozen_captures.holdout.clone(),
        optimizer_spec_path: root_spec_snapshot.clone(),
        async_runtime,
        optimizer_observability,
        candidate_slots: settings.jobs.max(1),
        symbols: participating_symbols,
        parameter_schemas,
        selection_evaluator: Mutex::new(None),
        trial_counter: AtomicUsize::new(0),
        trials: Mutex::new(Vec::new()),
    };

    summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE init root={} jobs={} configured_jobs={} online_cpus={} dimensions={} configured_search_evals={} no_apply={} selection_mode={}",
        run_root.display(),
        settings.jobs,
        resource_policy.configured_jobs,
        resource_policy.online_cpus,
        ctx.parameter_count(),
        settings.max_function_evals,
        !settings.apply_best,
        ctx.selection_mode.as_str()
    ));

    let work_result = run_sequential(
        &ctx,
        &args,
        &settings,
        &frozen_captures,
        &root_spec_bytes,
        &config_snapshot,
        &app_config_sha256,
        &root_spec_snapshot,
        &root_spec_sha256,
        &symbol_files,
        &symbol_snapshots,
        &symbol_sha256s,
        &parameter_schema_sha256s,
    );
    shutdown_run_context(ctx, work_result)
}

#[allow(clippy::too_many_arguments)]
fn run_sequential(
    ctx: &RunContext,
    args: &Args,
    settings: &OptimizerSettings,
    frozen_captures: &FrozenCaptureInputs,
    root_spec_bytes: &[u8],
    config_snapshot: &Path,
    app_config_sha256: &str,
    root_spec_snapshot: &Path,
    root_spec_sha256: &str,
    symbol_files: &BTreeMap<String, Vec<u8>>,
    symbol_snapshots: &BTreeMap<String, PathBuf>,
    symbol_sha256s: &BTreeMap<String, String>,
    parameter_schema_sha256s: &BTreeMap<String, String>,
) -> Result<()> {
    let searches = replay_proof::run_symbol_searches(
        ctx,
        settings,
        frozen_captures,
        config_snapshot,
        app_config_sha256,
        root_spec_snapshot,
        root_spec_sha256,
        symbol_snapshots,
        symbol_sha256s,
        parameter_schema_sha256s,
    )?;
    inputs::verify_frozen_inputs(
        &args.spec,
        ctx.cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
        root_spec_bytes,
        config_snapshot,
        app_config_sha256,
        root_spec_snapshot,
        root_spec_sha256,
        ctx.objective_profile.sha256(),
        symbol_files,
        symbol_snapshots,
        symbol_sha256s,
    )?;
    combined_replay::finalize_sequential_searches(
        ctx,
        args,
        settings,
        frozen_captures,
        &searches,
        root_spec_bytes,
        config_snapshot,
        app_config_sha256,
        root_spec_snapshot,
        root_spec_sha256,
        symbol_files,
        symbol_snapshots,
        symbol_sha256s,
        parameter_schema_sha256s,
    )
}

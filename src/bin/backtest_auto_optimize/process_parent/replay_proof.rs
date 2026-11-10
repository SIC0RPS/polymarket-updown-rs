use super::*;
use polymarket_hft::optimizer_report::OptimizerSymbolSearchReport;
use polymarket_hft::strategy_runtime::backtest_matrix::inherit_offline_run_lane;
use std::process::{Command, ExitStatus};

#[cfg(target_os = "linux")]
fn require_parent_lifetime(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    let expected_parent_pid = std::process::id() as libc::pid_t;
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != expected_parent_pid {
                return Err(std::io::Error::from_raw_os_error(libc::ECHILD));
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
fn require_parent_lifetime(_command: &mut Command) {
    compile_error!("optimizer child process containment requires Linux PR_SET_PDEATHSIG");
}

pub(super) struct ValidatedSymbolSearch {
    pub(super) artifact: child_artifact::SymbolChildArtifact,
    pub(super) artifact_sha256: String,
}

pub(super) struct SequentialSearches {
    pub(super) by_symbol: BTreeMap<String, ValidatedSymbolSearch>,
    pub(super) reports: Vec<OptimizerSymbolSearchReport>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_symbol_searches(
    ctx: &RunContext,
    settings: &OptimizerSettings,
    frozen: &capture_inputs::FrozenCaptureInputs,
    config_snapshot: &Path,
    app_config_sha256: &str,
    root_spec_snapshot: &Path,
    root_spec_sha256: &str,
    symbol_snapshots: &BTreeMap<String, PathBuf>,
    symbol_sha256s: &BTreeMap<String, String>,
    parameter_schema_sha256s: &BTreeMap<String, String>,
) -> Result<SequentialSearches> {
    ensure!(
        ctx.symbols == frozen.symbols()? && ctx.selection_mode == settings.selection_mode,
        "optimizer parent symbol order is not canonical"
    );
    let executable = std::env::current_exe().context("resolve optimizer executable")?;
    let capture = frozen
        .selection
        .first()
        .context("frozen selection capture missing")?
        .capture
        .clone();
    let mut by_symbol = BTreeMap::new();
    let mut reports = Vec::new();
    report::write_parent_status(
        ctx,
        frozen,
        &reports,
        &by_symbol,
        None,
        &format!("{}_search_pending", ctx.symbols[0].to_ascii_lowercase()),
    )?;
    for (scope_index, symbol) in ctx.symbols.iter().enumerate() {
        let canonical_index = VALID_ASSETS
            .iter()
            .position(|asset| *asset == symbol)
            .context("optimizer symbol is not canonical")?;
        let child_frozen = frozen.for_symbol_selection(symbol)?;
        ensure!(
            child_frozen.selection.len() == 1 && child_frozen.holdout.is_empty(),
            "optimizer child scope must contain only frozen A symbol={symbol}"
        );
        let child_frozen_sha256 = child_frozen.sha256()?;
        let snapshot = symbol_snapshots
            .get(symbol)
            .with_context(|| format!("symbol snapshot missing {symbol}"))?;
        let symbol_sha256 = symbol_sha256s
            .get(symbol)
            .with_context(|| format!("symbol snapshot hash missing {symbol}"))?;
        ensure!(
            child_frozen.write_symbol_snapshot(snapshot)? == child_frozen_sha256,
            "optimizer child frozen snapshot digest mismatch symbol={symbol}"
        );
        let status = dispatch_symbol_search(
            &executable,
            &ctx.run_root,
            settings,
            symbol,
            canonical_index,
            &capture,
            snapshot,
            symbol_sha256,
            root_spec_snapshot,
            root_spec_sha256,
            ctx.objective_profile.sha256(),
            &child_frozen_sha256,
        )?;
        let validated = validate_current_child(
            status,
            symbol,
            canonical_index,
            ctx,
            settings,
            &child_frozen_sha256,
            config_snapshot,
            app_config_sha256,
            root_spec_sha256,
            symbol_sha256,
            parameter_schema_sha256s
                .get(symbol)
                .with_context(|| format!("parameter schema hash missing {symbol}"))?,
        )?;
        reports.push(child_artifact::search_report(&validated.artifact));
        by_symbol.insert(symbol.clone(), validated);
        let next_phase = ctx
            .symbols
            .get(scope_index + 1)
            .map(|next| format!("{}_search_pending", next.to_ascii_lowercase()))
            .unwrap_or_else(|| "symbol_searches_complete".to_string());
        report::write_parent_status(ctx, frozen, &reports, &by_symbol, None, &next_phase)?;
    }
    ensure!(
        by_symbol.len() == ctx.symbols.len()
            && ctx
                .symbols
                .iter()
                .all(|symbol| by_symbol.contains_key(symbol)),
        "sequential optimizer child coverage is incomplete"
    );
    Ok(SequentialSearches { by_symbol, reports })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn dispatch_symbol_search(
    executable: &Path,
    run_root: &Path,
    settings: &OptimizerSettings,
    symbol: &str,
    canonical_index: usize,
    capture: &Path,
    snapshot: &Path,
    symbol_sha256: &str,
    root_spec_snapshot: &Path,
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
    child_frozen_sha256: &str,
) -> Result<ExitStatus> {
    ensure!(
        VALID_ASSETS.get(canonical_index).copied() == Some(symbol),
        "optimizer child dispatch symbol order differs"
    );
    ensure!(
        candidate_proof::file_sha256(&FrozenCaptureInputs::snapshot_path(snapshot))?
            == child_frozen_sha256
            && config::load_optimizer_selection_mode(root_spec_snapshot)?
                == settings.selection_mode,
        "optimizer child frozen snapshot changed before dispatch symbol={symbol}"
    );
    let seed = settings.seed.saturating_add(canonical_index as u64);
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE symbol_search_dispatch symbol={} canonical_index={} seed={} max_function_evals={} max_no_improvement_evals={}",
        symbol,
        canonical_index,
        seed,
        settings.max_function_evals,
        settings.max_no_improvement_evals
    ));
    let mut command = Command::new(executable);
    command
        .arg("--spec")
        .arg(root_spec_snapshot)
        .arg("--capture-session")
        .arg(capture)
        .arg("--selection-capture")
        .arg(capture)
        .arg("--symbol")
        .arg(symbol)
        .arg("--default-spec-snapshot")
        .arg(snapshot)
        .arg("--default-spec-sha256")
        .arg(symbol_sha256)
        .arg("--root-spec-sha256")
        .arg(root_spec_sha256)
        .arg("--objective-profile-sha256")
        .arg(objective_profile_sha256)
        .arg("--parent-run-root")
        .arg(run_root)
        .arg("--jobs")
        .arg(settings.jobs.to_string())
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--frozen-capture-inputs-sha256")
        .arg(child_frozen_sha256);
    inherit_offline_run_lane(&mut command)?;
    require_parent_lifetime(&mut command);
    command
        .status()
        .with_context(|| format!("run optimizer child symbol={symbol}"))
}

#[allow(clippy::too_many_arguments)]
fn validate_current_child(
    status: ExitStatus,
    symbol: &str,
    canonical_index: usize,
    ctx: &RunContext,
    settings: &OptimizerSettings,
    expected_frozen_sha256: &str,
    config_snapshot: &Path,
    app_config_sha256: &str,
    root_spec_sha256: &str,
    symbol_spec_sha256: &str,
    parameter_schema_sha256: &str,
) -> Result<ValidatedSymbolSearch> {
    ensure!(
        status.success(),
        "optimizer child exited unsuccessfully symbol={symbol} status={status}"
    );
    let expected_root = ctx.run_root.join("children").join(symbol);
    let artifact_path = expected_root.join("child_result.json");
    let artifact = child_artifact::validate::load_validated_child(&ctx.run_root, symbol)?;
    let expected_seed = settings.seed.saturating_add(canonical_index as u64);
    ensure!(
        artifact.symbol == symbol
            && artifact.selection_mode == ctx.selection_mode
            && artifact.selection_mode == settings.selection_mode
            && artifact.run_root == expected_root.display().to_string()
            && artifact.frozen_capture_inputs_sha256 == expected_frozen_sha256
            && artifact.frozen_capture_inputs.holdout.is_empty()
            && artifact.app_config_sha256 == app_config_sha256
            && artifact.root_spec_sha256 == root_spec_sha256
            && artifact.symbol_spec_sha256 == symbol_spec_sha256
            && artifact.objective_profile_sha256 == ctx.objective_profile.sha256()
            && artifact.parameter_schema_sha256 == parameter_schema_sha256
            && artifact.seed == expected_seed,
        "optimizer child immutable identity mismatch symbol={symbol}"
    );
    ensure!(
        artifact.configured_max_function_evals == settings.max_function_evals
            && artifact.configured_max_generations_per_run == settings.max_generations_per_run
            && artifact.configured_max_no_improvement_evals == settings.max_no_improvement_evals,
        "optimizer child search budget mismatch symbol={symbol}"
    );
    ensure!(
        child_artifact::file_sha256(config_snapshot)? == app_config_sha256,
        "optimizer app config snapshot changed while child ran symbol={symbol}"
    );
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE symbol_search_retained symbol={} status={} search_evaluations={} no_improvement_evaluations={} selected_candidate_id={}",
        symbol,
        artifact.search_status,
        artifact.search_evaluations,
        artifact.no_improvement_evals,
        artifact.selected_candidate.candidate_id
    ));
    Ok(ValidatedSymbolSearch {
        artifact_sha256: child_artifact::file_sha256(&artifact_path)?,
        artifact,
    })
}

pub(super) fn bind_retained_selection(
    searches: &SequentialSearches,
    cfg: &AppConfig,
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
    original_optimizer_binary_sha256: &str,
) -> Result<capture_inputs::FrozenCaptureInput> {
    let first_child = &searches
        .by_symbol
        .first_key_value()
        .context("retained first child missing")?
        .1
        .artifact;
    let first = first_child.frozen_capture_inputs.selection[0].clone();
    let timing = first
        .execution_timing
        .as_ref()
        .context("retained A timing missing")?;
    ensure!(
        timing
            .clob
            .as_ref()
            .is_some_and(|source| !source.observations.is_empty())
            && timing
                .ctf
                .as_ref()
                .is_some_and(|source| !source.observations.is_empty()),
        "retained A requires its original nonempty timing"
    );
    let mut selection = first.clone();
    selection.market_lifecycles.clear();
    selection.prepared_proof_identities_by_symbol.clear();
    selection.initial_oracle_learning_sha256_by_symbol.clear();
    selection.initial_oracle_learning_cutoffs_ms.clear();
    let mut population =
        polymarket_hft::strategy_runtime::replay_mode::RecordedMarketPopulation::default();
    for symbol in VALID_ASSETS
        .iter()
        .filter(|symbol| searches.by_symbol.contains_key(**symbol))
    {
        let child = &searches
            .by_symbol
            .get(*symbol)
            .with_context(|| format!("retained child missing symbol={symbol}"))?
            .artifact;
        let input = &child.frozen_capture_inputs.selection[0];
        ensure!(
            child.selection_mode == first_child.selection_mode
                && input.role == first.role
                && input.capture == first.capture
                && input.session_id == first.session_id
                && input.run_id == first.run_id
                && input.capture_start_ms == first.capture_start_ms
                && input.capture_end_ms == first.capture_end_ms
                && input.replay_start_ms == first.replay_start_ms
                && input.replay_end_ms == first.replay_end_ms
                && input.execution_timing == first.execution_timing
                && input
                    .prepared_proof_identities_by_symbol
                    .get(*symbol)
                    .is_some_and(|identity| identity.optimizer_binary_sha256()
                        == original_optimizer_binary_sha256),
            "retained children disagree on original A evidence symbol={symbol}"
        );
        selection
            .market_lifecycles
            .extend(input.market_lifecycles.clone());
        selection
            .prepared_proof_identities_by_symbol
            .extend(input.prepared_proof_identities_by_symbol.clone());
        selection
            .initial_oracle_learning_sha256_by_symbol
            .extend(input.initial_oracle_learning_sha256_by_symbol.clone());
        selection
            .initial_oracle_learning_cutoffs_ms
            .extend(input.initial_oracle_learning_cutoffs_ms.clone());
        population.merge(
            input
                .recorded_market_population
                .clone()
                .context("retained child recorded market population missing")?,
        )?;
    }
    selection.market_lifecycles.sort();
    selection.market_lifecycles.dedup();
    selection.recorded_market_population = Some(population);
    let mut learning: BTreeMap<String, OracleLearningSnapshot> =
        match polymarket_hft::capture::session::captured_initial_oracle_learning_bytes(
            &selection.capture,
        )? {
            Some(bytes) => {
                rmp_serde::from_slice(&bytes).context("decode retained A captured learners")?
            }
            None => BTreeMap::new(),
        };
    learning.retain(|symbol, _| searches.by_symbol.contains_key(symbol));
    ensure!(
        learning
            .iter()
            .map(|(symbol, state)| (symbol.clone(), state.knowledge_cutoff_ms()))
            .collect::<BTreeMap<_, _>>()
            == selection.initial_oracle_learning_cutoffs_ms,
        "retained A captured learner cutoffs differ from children"
    );
    selection.initial_oracle_learning_sha256 = candidate_proof::oracle_learning_sha256(&learning)?;
    let original = FrozenCaptureInputs {
        selection: vec![selection.clone()],
        holdout: Vec::new(),
    };
    for (symbol, current) in candidate_proof::build_symbol_proof_identities(
        &selection.symbols()?,
        &selection.capture,
        cfg,
        root_spec_sha256,
        objective_profile_sha256,
    )? {
        let child = &searches.by_symbol[&symbol].artifact;
        ensure!(
            original.for_symbol_selection(&symbol)? == child.frozen_capture_inputs,
            "retained A union differs from sealed child projection symbol={symbol}"
        );
        ensure!(
            selection.prepared_proof_identities_by_symbol[&symbol]
                .same_capture_content_ignoring_optimizer_binary(&current),
            "retained A content changed before binding finalizer executable symbol={symbol}"
        );
        selection
            .prepared_proof_identities_by_symbol
            .insert(symbol, current);
    }
    selection.validate_frozen_execution_timing(cfg)?;
    Ok(selection)
}

pub(super) fn validate_retained_inputs(
    original: &FrozenCaptureInputs,
    current: &FrozenCaptureInputs,
) -> Result<()> {
    let mut original = original.clone();
    ensure!(
        original.selection.len() == current.selection.len()
            && original.holdout.len() == current.holdout.len(),
        "retained frozen capture roles changed"
    );
    for (original, current) in original
        .selection
        .iter_mut()
        .chain(&mut original.holdout)
        .zip(current.all())
    {
        ensure!(
            original.prepared_proof_identities_by_symbol.len()
                == current.prepared_proof_identities_by_symbol.len()
                && original
                    .prepared_proof_identities_by_symbol
                    .iter()
                    .all(|(symbol, identity)| current
                        .prepared_proof_identities_by_symbol
                        .get(symbol)
                        .is_some_and(|current| identity
                            .same_capture_content_ignoring_optimizer_binary(current))),
            "retained parent capture content changed"
        );
        original.prepared_proof_identities_by_symbol =
            current.prepared_proof_identities_by_symbol.clone();
    }
    ensure!(
        original == *current,
        "retained parent frozen A/B facts changed"
    );
    Ok(())
}

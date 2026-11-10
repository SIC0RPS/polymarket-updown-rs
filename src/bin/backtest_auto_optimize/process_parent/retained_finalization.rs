use super::*;

pub(crate) fn run_retained_finalization(mut args: Args) -> Result<()> {
    const HISTORICAL_RUN_ROOT: &str =
        "data/captures/20261027T124909Z_live_1731613/backtest_reports/optimizer_runs/1793129051_218088822";
    let requested = args
        .finalize_retained_run_root
        .as_ref()
        .context("retained finalization run root missing")?;
    let requested_root = fs::canonicalize(requested)?;
    let historical = requested_root.ends_with("1793129051_218088822");
    let archive_root = Path::new(
        "data/backtest_reports/optimizer_archive/20261030T211455Z_live_1455599/1793655687_999246658",
    );
    let archived_parent = if requested_root.ends_with("1793655687_999246658") {
        let parent = candidate_proof::load_candidate_result(archive_root)?;
        ensure!(
            parent.artifact_identity
                == "888b05d05a9089d0dccbc50fba25ad2464ae46d1781bf6098bdc98bff1849235"
                && parent.schema_version == 19
                && !parent.applied
                && !parent.structural_pass
                && parent.application_reason == "structural_gate_failed"
                && parent.selected_learning_bindings.is_empty()
                && fs::canonicalize(&parent.run_root)? == requested_root
                && parent.search_trials == 1606
                && parent.exercised_trials == 1618,
            "completed retained parent differs from its original archived seal"
        );
        ensure!(
            candidate_proof::file_sha256(&requested_root.join("candidate_result.json"))?
                == candidate_proof::file_sha256(&archive_root.join("candidate_result.json"))?,
            "current retained parent differs from the original archived artifact"
        );
        Some(parent)
    } else {
        None
    };
    let retained_status: Option<polymarket_hft::optimizer_report::OptimizerStatusArtifact> =
        if archived_parent.is_some() {
            None
        } else {
            let status_path = requested_root.join(if historical {
                "optimizer_status.interrupted.json"
            } else {
                "optimizer_status.json"
            });
            Some(serde_json::from_slice(&fs::read(&status_path)?)?)
        };
    let run_root = PathBuf::from(
        archived_parent
            .as_ref()
            .map(|parent| &parent.run_root)
            .or_else(|| retained_status.as_ref().map(|status| &status.run_root))
            .context("retained run identity missing")?,
    );
    ensure!(
        fs::canonicalize(&run_root)? == requested_root,
        "retained run-root identity conflicts"
    );
    if let Some(status) = &retained_status {
        ensure!(
            status.schema_version == 19 && !status.applied,
            "retained parent status conflicts"
        );
        ensure!(
            !run_root.join("candidate_result.json").try_exists()?,
            "retained finalization conflicts with an unapproved completed parent"
        );
    }
    let recorded_provenance = if let Some(parent) = &archived_parent {
        summary::provenance_report(
            parent.selection_mode,
            &parent.frozen_capture_inputs.selection,
            &parent.frozen_capture_inputs.holdout,
            &parent.app_config_sha256,
            &parent.root_spec_sha256,
            None,
            None,
            None,
        )?
    } else {
        retained_status
            .as_ref()
            .and_then(|status| status.provenance.clone())
            .context("retained original capture learner provenance missing")?
    };
    let recorded_captures = if let Some(parent) = &archived_parent {
        parent
            .frozen_capture_inputs
            .selection
            .iter()
            .map(|input| summary::capture_report(input, "selection"))
            .chain(
                parent
                    .frozen_capture_inputs
                    .holdout
                    .iter()
                    .map(|input| summary::capture_report(input, "validation")),
            )
            .collect::<Vec<_>>()
    } else {
        retained_status
            .as_ref()
            .context("retained capture reports missing")?
            .captures
            .clone()
    };
    let frozen_path =
        FrozenCaptureInputs::snapshot_path(&run_root.join("defaults/optimization.yml"));
    let retained_frozen: Option<FrozenCaptureInputs> = if frozen_path.try_exists()? {
        Some(serde_json::from_slice(&fs::read(&frozen_path)?)?)
    } else {
        None
    };
    let all_symbols = VALID_ASSETS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect::<Vec<_>>();
    let symbols = if let Some(frozen) = &retained_frozen {
        frozen.symbols()?
    } else if let Some(parent) = &archived_parent {
        parent.frozen_capture_inputs.symbols()?
    } else {
        ensure!(
            recorded_provenance
                .capture_learners
                .iter()
                .all(
                    |capture| capture.initial_oracle_learning_sha256_by_symbol.len()
                        == VALID_ASSETS.len()
                        && VALID_ASSETS.iter().all(|symbol| capture
                            .initial_oracle_learning_sha256_by_symbol
                            .contains_key(*symbol))
                ),
            "retained scoped run requires its original full frozen input snapshot"
        );
        all_symbols.clone()
    };
    let mut searches = replay_proof::SequentialSearches {
        by_symbol: BTreeMap::new(),
        reports: Vec::new(),
    };
    for symbol in &symbols {
        let child_root = run_root.join("children").join(symbol);
        let artifact_path = child_root.join("child_result.json");
        if artifact_path.try_exists()? {
            searches.by_symbol.insert(
                symbol.clone(),
                replay_proof::ValidatedSymbolSearch {
                    artifact: child_artifact::validate::load_validated_child(&run_root, symbol)?,
                    artifact_sha256: candidate_proof::file_sha256(&artifact_path)?,
                },
            );
        } else {
            ensure!(
                !child_root.try_exists()?,
                "retained search has unsealed partial evidence symbol={symbol}"
            );
            if historical {
                ensure!(
                    symbol == "XRP"
                        && run_root
                            .join("children/XRP.interrupted/trials.jsonl")
                            .is_file(),
                    "historical missing XRP evidence changed"
                );
            }
        }
    }
    let (first_symbol, first_search) = searches
        .by_symbol
        .first_key_value()
        .context("retained finalization requires a sealed child search")?;
    let first = &first_search.artifact;
    let first_index = VALID_ASSETS
        .iter()
        .position(|symbol| *symbol == first_symbol)
        .context("retained child symbol is not canonical")?;
    let original_binary_sha256 = first.frozen_capture_inputs.selection[0]
        .prepared_proof_identities_by_symbol
        .get(first_symbol)
        .context("retained child executable identity missing")?
        .optimizer_binary_sha256()
        .to_string();
    ensure!(
        !original_binary_sha256.is_empty(),
        "retained child executable identity is empty"
    );
    let original_timing = first.frozen_capture_inputs.selection[0]
        .execution_timing
        .clone();
    let defaults = run_root.join("defaults");
    let config_snapshot = defaults.join("config.main.toml");
    let root_spec_snapshot = defaults.join("optimization.yml");
    let root_spec_bytes = fs::read(&root_spec_snapshot)?;
    let app_config_sha256 = candidate_proof::file_sha256(&config_snapshot)?;
    let root_spec_sha256 = candidate_proof::file_sha256(&root_spec_snapshot)?;
    let objective_profile = ObjectiveProfile::load(&defaults.join("objective_profile.yml"))?;
    let frozen_cfg =
        AppConfig::load_from_path_with_optimizer_path(&config_snapshot, &root_spec_snapshot)?;
    if !args.spec_explicit {
        args.spec = PathBuf::from(if frozen_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold {
            "optimization.buy.yml"
        } else {
            "optimization.yml"
        });
    }
    let mut settings = load_optimizer_settings(&root_spec_snapshot, &args)?;
    ensure!(
        settings.apply_best
            && settings.jobs == 0
            && recorded_provenance.selection_mode == settings.selection_mode.as_str()
            && settings.max_function_evals == first.configured_max_function_evals
            && settings.max_generations_per_run == first.configured_max_generations_per_run
            && settings.max_no_improvement_evals == first.configured_max_no_improvement_evals
            && retained_status.as_ref().is_none_or(|status| {
                status.selection_mode == settings.selection_mode.as_str()
                    && status.objective_profile
                        == summary::objective_profile_report(&objective_profile)
            })
            && recorded_provenance.app_config_sha256 == app_config_sha256
            && recorded_provenance.root_spec_sha256 == root_spec_sha256,
        "retained configuration, objective, application or search ceilings changed"
    );
    settings.seed = first
        .seed
        .checked_sub(first_index as u64)
        .context("retained child seed precedes its canonical index")?;
    if historical {
        let retained_status = retained_status
            .as_ref()
            .context("historical status missing")?;
        ensure!(
            run_root == Path::new(HISTORICAL_RUN_ROOT)
                && retained_status.phase == "xrp_search_pending"
                && retained_status.search_trials == 1962
                && retained_status.exercised_trials == 3270
                && retained_status
                    .symbol_searches
                    .iter()
                    .map(|row| row.symbol.as_str())
                    .eq(VALID_ASSETS[..3].iter().copied())
                && original_binary_sha256
                    == "0bae195b6065511b403d65ccf2927771d3931d618e8ec407677f4bcd857429ce"
                && settings.seed == 20261027191420
                && settings.max_function_evals == 1024
                && settings.max_generations_per_run == 64
                && settings.max_no_improvement_evals == 240,
            "historical retained search evidence changed"
        );
    }
    settings.jobs = resolve_resource_policy(0)?.effective_jobs;
    let mut symbol_files = BTreeMap::new();
    let mut symbol_snapshots = BTreeMap::new();
    let mut symbol_sha256s = BTreeMap::new();
    let mut parameter_schemas = BTreeMap::new();
    let mut parameter_schema_sha256s = BTreeMap::new();
    for symbol in &all_symbols {
        let snapshot = defaults.join(format!("{symbol}.optimization.yml"));
        let installed = fs::read(inputs::symbol_spec_path(
            &args.spec,
            symbol,
            frozen_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
        ))?;
        ensure!(
            historical || installed == fs::read(&snapshot)?,
            "retained application baseline changed symbol={symbol}"
        );
        let schema = load_symbol_schema_from_snapshot(&snapshot, &root_spec_snapshot)?;
        parameter_schema_sha256s
            .insert(symbol.clone(), child_artifact::hex_bytes(&schema.sha256()));
        parameter_schemas.insert(symbol.clone(), schema);
        symbol_files.insert(symbol.clone(), installed);
        symbol_sha256s.insert(symbol.clone(), candidate_proof::file_sha256(&snapshot)?);
        symbol_snapshots.insert(symbol.clone(), snapshot);
    }
    inputs::verify_frozen_inputs(
        &args.spec,
        frozen_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
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
    if historical {
        let incumbent_params = parameter_schemas
            .iter()
            .map(|(symbol, schema)| Ok((symbol.clone(), schema.default_map()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        ensure!(
            population::joint_candidate_id(
                &frozen_cfg,
                &parameter_schemas,
                &symbols,
                &incumbent_params,
            )? == "721ef51b906465a8ae4111f03986e106f70310a9f4d4ee927f110af7564c4eac",
            "retained original incumbent changed"
        );
    }
    let validate_child =
        |child: &child_artifact::SymbolChildArtifact, index: usize| -> Result<()> {
            let symbol = &child.symbol;
            child_artifact::validate::validate_original_inputs(&run_root, child)?;
            ensure!(
            child.app_config_sha256 == app_config_sha256
                && child.root_spec_sha256 == root_spec_sha256
                && child.objective_profile_sha256 == objective_profile.sha256()
                && child.symbol_spec_sha256 == symbol_sha256s[symbol]
                && child.parameter_schema_sha256 == parameter_schema_sha256s[symbol]
                && child.seed == settings.seed.saturating_add(index as u64)
                && child.configured_max_function_evals == settings.max_function_evals
                && child.configured_max_generations_per_run == settings.max_generations_per_run
                && child.configured_max_no_improvement_evals == settings.max_no_improvement_evals
                && child.frozen_capture_inputs.selection[0]
                    .prepared_proof_identities_by_symbol
                    .get(symbol)
                    .is_some_and(|identity| identity.optimizer_binary_sha256()
                        == original_binary_sha256),
            "retained child differs from original parent inputs or search evidence symbol={symbol}"
        );
            Ok(())
        };
    for symbol in &symbols {
        let index = VALID_ASSETS
            .iter()
            .position(|asset| *asset == symbol)
            .context("retained symbol is not canonical")?;
        if let Some(search) = searches.by_symbol.get(symbol) {
            validate_child(&search.artifact, index)?;
        }
    }
    if let Some(retained_status) = &retained_status {
        let recorded_searches = &retained_status.symbol_searches;
        let mut recorded_symbols = BTreeSet::new();
        for report in recorded_searches {
            ensure!(
                recorded_symbols.insert(&report.symbol)
                    && searches
                        .by_symbol
                        .get(&report.symbol)
                        .is_some_and(|search| {
                            *report == child_artifact::search_report(&search.artifact)
                        }),
                "retained parent report differs from sealed child symbol={}",
                report.symbol
            );
        }
        ensure!(
            retained_status.search_trials
                == recorded_searches
                    .iter()
                    .map(|row| row.search_evaluations)
                    .sum::<usize>()
                && retained_status.exercised_trials
                    == recorded_searches
                        .iter()
                        .map(|row| row.exercised_trials)
                        .sum::<usize>(),
            "retained parent counts differ from recorded child reports"
        );
    }
    if let Some(parent) = &archived_parent {
        combined_replay::validate_retained_parent(
            &run_root,
            archive_root,
            parent,
            &searches,
            &frozen_cfg,
            &parameter_schemas,
        )?;
    }
    let executable = Path::new("target/release/backtest_auto_optimize");
    if searches.by_symbol.len() < symbols.len() {
        ensure!(
            candidate_proof::file_sha256(executable)? == original_binary_sha256,
            "original retained optimizer executable changed"
        );
    }
    ensure!(
        recorded_captures.len() == 2,
        "retained capture roles are incomplete"
    );
    let mut frozen = FrozenCaptureInputs {
        selection: Vec::new(),
        holdout: Vec::new(),
    };
    for (role, target, capture_role) in [
        (
            "selection",
            &mut frozen.selection,
            capture_inputs::CaptureRole::Selection,
        ),
        (
            "validation",
            &mut frozen.holdout,
            capture_inputs::CaptureRole::Holdout,
        ),
    ] {
        let recorded = recorded_captures
            .iter()
            .find(|capture| capture.role == role)
            .with_context(|| format!("retained capture role missing role={role}"))?;
        *target = FrozenCaptureInputs::freeze_retained_capture(
            &frozen_cfg,
            Path::new(&recorded.capture),
            capture_role,
            &symbols,
            &root_spec_snapshot,
            objective_profile.sha256(),
            settings.jobs,
        )?;
        ensure!(
            target.len() == 1,
            "retained named capture missing role={role}"
        );
        if capture_role == capture_inputs::CaptureRole::Selection {
            target[0].execution_timing = original_timing.clone();
        }
        ensure!(
            target[0].symbols()? == symbols,
            "retained capture participating scope changed"
        );
        target[0].validate_frozen_execution_timing(&frozen_cfg)?;
        summary::verify_capture_report(recorded, &target[0], role)?;
    }
    if historical {
        ensure!(
            frozen.selection[0].capture == Path::new("data/captures/20261027T124909Z_live_1731613")
                && frozen.holdout[0].capture
                    == Path::new("data/captures/20261026T143054Z_live_720834")
                && frozen.holdout[0].capture_end_ms < frozen.selection[0].capture_start_ms,
            "historical retained A/B pair changed"
        );
        let mut expected_b_timing = frozen.selection[0]
            .execution_timing
            .clone()
            .context("retained A timing missing")?;
        expected_b_timing.selected_capture = frozen.holdout[0].capture.display().to_string();
        expected_b_timing.selected_config_sha256 = Some(app_config_sha256.clone());
        ensure!(
            frozen.holdout[0].execution_timing.as_ref() == Some(&expected_b_timing),
            "retained B timing differs from original shared donor evidence"
        );
    }
    let (earlier, later) =
        if frozen.selection[0].capture_start_ms < frozen.holdout[0].capture_start_ms {
            (&frozen.selection[0], &frozen.holdout[0])
        } else {
            (&frozen.holdout[0], &frozen.selection[0])
        };
    ensure!(
        earlier.capture != later.capture
            && earlier.capture_end_ms < later.capture_start_ms
            && earlier
                .market_lifecycles
                .iter()
                .map(|market| market.window_end_ms)
                .max()
                <= later
                    .market_lifecycles
                    .iter()
                    .map(|market| market.window_start_ms)
                    .min(),
        "retained A/B capture or lifecycle intervals conflict"
    );
    let fresh_provenance = summary::provenance_report(
        settings.selection_mode,
        &frozen.selection,
        &frozen.holdout,
        &app_config_sha256,
        &root_spec_sha256,
        None,
        None,
        None,
    )?;
    ensure!(
        recorded_provenance.capture_learners == fresh_provenance.capture_learners,
        "retained A/B identities or captured learner seeds changed"
    );
    for (symbol, identity) in &mut frozen.selection[0].prepared_proof_identities_by_symbol {
        if let Some(search) = searches.by_symbol.get(symbol) {
            let original = &search.artifact.frozen_capture_inputs.selection[0]
                .prepared_proof_identities_by_symbol[symbol];
            ensure!(
                identity.same_capture_content_ignoring_optimizer_binary(original),
                "retained A content changed during preflight symbol={symbol}"
            );
            *identity = original.clone();
        } else {
            identity.bind_child_executable(executable, &original_binary_sha256)?;
        }
    }
    for symbol in &symbols {
        let frozen_path = FrozenCaptureInputs::snapshot_path(&symbol_snapshots[symbol]);
        if frozen_path.try_exists()? {
            ensure!(
                candidate_proof::file_sha256(&frozen_path)?
                    == frozen.for_symbol_selection(symbol)?.sha256()?,
                "retained frozen A differs from named capture facts symbol={symbol}"
            );
        } else {
            ensure!(
                !searches.by_symbol.contains_key(symbol),
                "sealed child snapshot missing symbol={symbol}"
            );
        }
    }
    if let Some(original_inputs) = retained_frozen.as_ref().or_else(|| {
        archived_parent
            .as_ref()
            .map(|parent| &parent.frozen_capture_inputs)
    }) {
        replay_proof::validate_retained_inputs(original_inputs, &frozen)?;
    }
    summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE retained_finalization_begin run={} optimizer_binary_sha256={} child_optimizer_binary_sha256={} validation_capture={}",
        run_root.display(),
        fresh_provenance.optimizer_binary_sha256,
        original_binary_sha256,
        frozen.holdout[0].capture.display()
    ));
    for symbol in &symbols {
        let index = VALID_ASSETS
            .iter()
            .position(|asset| *asset == symbol)
            .context("retained symbol is not canonical")?;
        if searches.by_symbol.contains_key(symbol) {
            continue;
        }
        let snapshot = &symbol_snapshots[symbol];
        let child_frozen = frozen.for_symbol_selection(symbol)?;
        let frozen_sha256 = child_frozen.sha256()?;
        if !FrozenCaptureInputs::snapshot_path(snapshot).try_exists()? {
            ensure!(
                child_frozen.write_symbol_snapshot(snapshot)? == frozen_sha256,
                "retained new child snapshot digest mismatch symbol={symbol}"
            );
        }
        ensure!(
            candidate_proof::file_sha256(executable)? == original_binary_sha256,
            "original retained optimizer executable changed before dispatch"
        );
        let status = replay_proof::dispatch_symbol_search(
            executable,
            &run_root,
            &settings,
            symbol,
            index,
            &frozen.selection[0].capture,
            snapshot,
            &symbol_sha256s[symbol],
            &root_spec_snapshot,
            &root_spec_sha256,
            objective_profile.sha256(),
            &frozen_sha256,
        )?;
        ensure!(
            status.success(),
            "retained child failed symbol={symbol} status={status}"
        );
        let child = child_artifact::validate::load_validated_child(&run_root, symbol)?;
        validate_child(&child, index)?;
        ensure!(
            child.frozen_capture_inputs == child_frozen,
            "retained dispatched child frozen inputs changed symbol={symbol}"
        );
        searches.by_symbol.insert(
            symbol.clone(),
            replay_proof::ValidatedSymbolSearch {
                artifact_sha256: candidate_proof::file_sha256(
                    &run_root
                        .join("children")
                        .join(symbol)
                        .join("child_result.json"),
                )?,
                artifact: child,
            },
        );
    }
    searches.reports = symbols
        .iter()
        .map(|symbol| child_artifact::search_report(&searches.by_symbol[symbol].artifact))
        .collect();
    inputs::verify_frozen_inputs(
        &args.spec,
        frozen_cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
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
    frozen.selection = vec![replay_proof::bind_retained_selection(
        &searches,
        &frozen_cfg,
        &root_spec_sha256,
        objective_profile.sha256(),
        &original_binary_sha256,
    )?];
    let frozen_cfg = polymarket_hft::strategy_runtime::backtest_matrix::load_backtest_config_for_capture_with_optimizer_path(
        &frozen.selection[0].capture,
        &config_snapshot,
        &root_spec_snapshot,
    )?;
    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.jobs.max(1))
        .enable_all()
        .build()
        .context("build retained optimizer finalization runtime")?;
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
        selection_mode: settings.selection_mode,
        run_root,
        objective_profile,
        cfg: frozen_cfg,
        selection_inputs: frozen.selection.clone(),
        holdout_inputs: frozen.holdout.clone(),
        optimizer_spec_path: root_spec_snapshot.clone(),
        async_runtime,
        optimizer_observability,
        candidate_slots: settings.jobs.max(1),
        symbols,
        parameter_schemas,
        selection_evaluator: Mutex::new(None),
        trial_counter: AtomicUsize::new(0),
        trials: Mutex::new(Vec::new()),
    };
    let result = combined_replay::finalize_sequential_searches(
        &ctx,
        &args,
        &settings,
        &frozen,
        &searches,
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
    shutdown_run_context(ctx, result)
}

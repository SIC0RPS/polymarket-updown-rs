use super::*;
use crate::process_parent::replay_proof::SequentialSearches;

#[allow(clippy::too_many_arguments)]
pub(super) fn finalize_sequential_searches(
    ctx: &RunContext,
    args: &Args,
    settings: &OptimizerSettings,
    frozen: &capture_inputs::FrozenCaptureInputs,
    searches: &SequentialSearches,
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
    let default_params = params_from_children(searches, false)?;
    let selected_params = params_from_children(searches, true)?;
    validate_child_bindings(
        &ctx.run_root,
        searches,
        frozen,
        &default_params,
        &selected_params,
    )?;
    report::write_parent_status(
        ctx,
        frozen,
        &searches.reports,
        &searches.by_symbol,
        None,
        "final_selection_integration",
    )?;

    let mut selection_results =
        holdout::evaluate_frozen_pair(ctx, &frozen.selection, &default_params, &selected_params)
            .context("evaluate final frozen A integration")?;
    ensure!(
        selection_results.len() == 1,
        "final frozen A integration result count mismatch"
    );
    let selection = selection_results
        .pop()
        .context("final frozen A integration result missing")?;
    let selected_symbols = ctx.symbols.iter().cloned().collect::<BTreeSet<_>>();
    let mut learning_bindings = export_selected_oracle_learning(
        &args.spec,
        &selection.candidate.final_oracle_learning,
        &selected_symbols,
    )
    .context("export final A oracle learning")?;

    let holdout = if frozen.holdout.is_empty() {
        None
    } else {
        report::write_parent_status(
            ctx,
            frozen,
            &searches.reports,
            &searches.by_symbol,
            Some(&selection),
            "cross_capture_validation",
        )?;
        validate_child_bindings(
            &ctx.run_root,
            searches,
            frozen,
            &default_params,
            &selected_params,
        )?;
        let mut results =
            holdout::evaluate_frozen_pair(ctx, &frozen.holdout, &default_params, &selected_params)
                .context("evaluate paired B validation")?;
        ensure!(
            results.len() == 1,
            "paired B validation result count mismatch"
        );
        results.pop()
    };

    let selection_incumbent = candidate_proof::holdout_replay_proof(&selection.incumbent);
    let selection_candidate = candidate_proof::holdout_replay_proof(&selection.candidate);
    let selection_economics =
        candidate_proof::selection_economics(&selection_incumbent, &selection_candidate)?;
    let selection_pass = selection_economics.len() == VALID_ASSETS.len()
        && selection_economics
            .values()
            .all(|row| row.candidate_profitable);
    let validation_economics = holdout
        .as_ref()
        .map(|result| result.economics_by_symbol.clone())
        .unwrap_or_default();
    let validation_pass = validation_economics.len() == VALID_ASSETS.len()
        && validation_economics.values().all(|row| row.passed);
    let eligible_symbols = candidate_proof::eligible_symbols_from_economics(
        &selection_economics,
        &validation_economics,
    );
    let structural_pass = selection.structural_pass
        && holdout
            .as_ref()
            .is_some_and(|result| result.structural_pass);
    let all_four_economic_pass = selection_pass && validation_pass;
    let validation_error = holdout.as_ref().and_then(|result| result.error.clone());

    validate_child_bindings(
        &ctx.run_root,
        searches,
        frozen,
        &default_params,
        &selected_params,
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

    let base_reason = if frozen.holdout.is_empty() {
        "validation_unavailable"
    } else if validation_error.is_some() || !structural_pass {
        "structural_gate_failed"
    } else if eligible_symbols.is_empty() {
        "no_symbol_economic_gate_passed"
    } else if !settings.apply_best {
        "application_disabled"
    } else {
        "eligible"
    };
    let build_artifact =
        |applied: bool,
         application_reason: &str,
         learning_bindings: BTreeMap<String, candidate_proof::SelectedLearningBindingProof>|
         -> Result<candidate_proof::CandidateResultArtifact> {
            let search_trials_by_symbol = searches
                .by_symbol
                .iter()
                .map(|(symbol, child)| (symbol.clone(), child.artifact.search_evaluations))
                .collect::<BTreeMap<_, _>>();
            let symbol_search_artifact_sha256s = searches
                .by_symbol
                .iter()
                .map(|(symbol, child)| (symbol.clone(), child.artifact_sha256.clone()))
                .collect::<BTreeMap<_, _>>();
            let mut history_hasher = Sha256::new();
            for symbol in &ctx.symbols {
                history_hasher.update(
                    searches.by_symbol[symbol]
                        .artifact
                        .trial_history_sha256
                        .as_bytes(),
                );
            }
            Ok(candidate_proof::CandidateResultArtifact {
                selection_mode: ctx.selection_mode,
                schema_version: 19,
                artifact_identity: String::new(),
                run_root: ctx.run_root.display().to_string(),
                frozen_capture_inputs: frozen.clone(),
                frozen_capture_inputs_sha256: frozen.sha256()?,
                app_config_sha256: app_config_sha256.to_string(),
                root_spec_sha256: root_spec_sha256.to_string(),
                objective_profile_sha256: ctx.objective_profile.sha256().to_string(),
                symbol_spec_sha256s: symbol_sha256s.clone(),
                parameter_schema_sha256s: parameter_schema_sha256s.clone(),
                search_trials: search_trials_by_symbol.values().sum(),
                search_trials_by_symbol,
                symbol_search_artifact_sha256s,
                exercised_trials: searches
                    .by_symbol
                    .values()
                    .map(|child| child.artifact.exercised_trials)
                    .sum(),
                trial_history_sha256: format!("{:x}", history_hasher.finalize()),
                selected_source_trial_index: None,
                selected_recheck_trial_index: None,
                selection_incumbent: selection_incumbent.clone(),
                selection_candidate: Some(selection_candidate.clone()),
                validation_incumbent: holdout
                    .as_ref()
                    .map(|result| candidate_proof::holdout_replay_proof(&result.incumbent)),
                validation_candidate: holdout
                    .as_ref()
                    .map(|result| candidate_proof::holdout_replay_proof(&result.candidate)),
                selection_economics_by_symbol: selection_economics.clone(),
                validation_economics_by_symbol: validation_economics.clone(),
                validation_error: validation_error.clone(),
                structural_pass,
                all_four_economic_pass,
                applied,
                application_reason: application_reason.to_string(),
                selected_learning_bindings: learning_bindings,
            })
        };

    if base_reason == "eligible" {
        learning_bindings.retain(|symbol, _| eligible_symbols.contains(symbol));
        let prospective = build_artifact(
            true,
            "applied",
            candidate_proof::selected_learning_binding_proofs(&learning_bindings),
        )?;
        let application_symbol_sha256s = symbol_files
            .iter()
            .map(|(symbol, bytes)| (symbol.clone(), format!("{:x}", Sha256::digest(bytes))))
            .collect::<BTreeMap<_, _>>();
        let mut applied_artifact = None;
        let apply_result = apply_joint_candidate(
            &args.spec,
            ctx.cfg.strategy.maker_rebate_mm.oracle_buy_and_hold,
            root_spec_snapshot,
            &selected_params,
            &learning_bindings,
            symbol_files,
            &application_symbol_sha256s,
            || {
                applied_artifact = Some(candidate_proof::write_candidate_result(
                    &ctx.run_root,
                    prospective,
                )?);
                Ok(())
            },
        );
        if apply_result.is_ok() {
            let artifact = applied_artifact.context("applied candidate artifact missing")?;
            report::write_parent_summary(
                ctx,
                frozen,
                &searches.reports,
                &selection,
                holdout.as_ref(),
                &artifact,
            )?;
            return Ok(());
        }
        let error = apply_result.expect_err("checked apply failure");
        let artifact = candidate_proof::write_candidate_result(
            &ctx.run_root,
            build_artifact(false, "apply_failed", BTreeMap::new())?,
        )?;
        report::write_parent_summary(
            ctx,
            frozen,
            &searches.reports,
            &selection,
            holdout.as_ref(),
            &artifact,
        )?;
        return Err(error).context("atomic optimizer application failed");
    }

    let artifact = candidate_proof::write_candidate_result(
        &ctx.run_root,
        build_artifact(false, base_reason, BTreeMap::new())?,
    )?;
    report::write_parent_summary(
        ctx,
        frozen,
        &searches.reports,
        &selection,
        holdout.as_ref(),
        &artifact,
    )
}

fn params_from_children(
    searches: &SequentialSearches,
    selected: bool,
) -> Result<BTreeMap<String, BTreeMap<String, ParamValue>>> {
    searches
        .by_symbol
        .iter()
        .map(|(symbol, child)| {
            let proof = if selected {
                &child.artifact.selected_candidate
            } else {
                &child.artifact.default_candidate
            };
            let params = proof
                .params_by_symbol
                .get(symbol)
                .with_context(|| format!("symbol search parameters missing {symbol}"))?;
            Ok((symbol.clone(), params.clone()))
        })
        .collect()
}

fn validate_child_bindings(
    parent_run_root: &Path,
    searches: &SequentialSearches,
    frozen: &capture_inputs::FrozenCaptureInputs,
    default_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    selected_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<()> {
    ensure!(
        searches.by_symbol.keys().eq(default_params.keys())
            && default_params.keys().eq(selected_params.keys())
            && frozen
                .symbols()?
                .iter()
                .all(|symbol| searches.by_symbol.contains_key(symbol))
            && searches.by_symbol.len() == frozen.symbols()?.len(),
        "final integration requires every participating sealed child"
    );
    for symbol in default_params.keys() {
        let child = searches
            .by_symbol
            .get(symbol)
            .with_context(|| format!("final integration child missing symbol={symbol}"))?;
        child_artifact::validate::validate_parent_bindings(
            &child.artifact,
            parent_run_root,
            frozen,
            default_params,
            selected_params,
        )?;
    }
    Ok(())
}

pub(super) fn validate_retained_parent(
    run_root: &Path,
    archive_root: &Path,
    parent: &candidate_proof::CandidateResultArtifact,
    searches: &SequentialSearches,
    cfg: &AppConfig,
    schemas: &BTreeMap<String, OptimizerParameterSchema>,
) -> Result<()> {
    let defaults = run_root.join("defaults");
    ensure!(
        parent.selection_mode
            == config::load_optimizer_selection_mode(&defaults.join("optimization.yml"))?
            && parent.app_config_sha256
                == candidate_proof::file_sha256(&defaults.join("config.main.toml"))?
            && parent.root_spec_sha256
                == candidate_proof::file_sha256(&defaults.join("optimization.yml"))?
            && parent.objective_profile_sha256
                == ObjectiveProfile::load(&defaults.join("objective_profile.yml"))?.sha256()
            && parent.parameter_schema_sha256s
                == schemas
                    .iter()
                    .map(|(symbol, schema)| {
                        (symbol.clone(), child_artifact::hex_bytes(&schema.sha256()))
                    })
                    .collect::<BTreeMap<_, _>>()
            && parent.symbol_spec_sha256s.len() == VALID_ASSETS.len()
            && VALID_ASSETS.iter().all(|symbol| {
                parent
                    .symbol_spec_sha256s
                    .get(*symbol)
                    .is_some_and(|expected| {
                        candidate_proof::file_sha256(
                            &defaults.join(format!("{symbol}.optimization.yml")),
                        )
                        .is_ok_and(|current| current == *expected)
                    })
            }),
        "retained parent frozen configuration or parameter schemas changed"
    );
    let selected = parent
        .selection_candidate
        .as_ref()
        .context("retained selected proof missing")?;
    validate_child_bindings(
        run_root,
        searches,
        &parent.frozen_capture_inputs,
        &parent.selection_incumbent.params_by_symbol,
        &selected.params_by_symbol,
    )?;
    let mut history = Sha256::new();
    let mut exercised_trials = 0usize;
    let symbols = parent.frozen_capture_inputs.symbols()?;
    for symbol in &symbols {
        child_artifact::validate::load_archived_symbol_candidates(
            archive_root,
            parent,
            symbol,
            &parent.parameter_schema_sha256s[symbol],
        )?;
        let child = &searches.by_symbol[symbol];
        ensure!(
            parent.symbol_search_artifact_sha256s.get(symbol) == Some(&child.artifact_sha256)
                && parent.symbol_spec_sha256s.get(symbol)
                    == Some(&child.artifact.symbol_spec_sha256)
                && parent.search_trials_by_symbol.get(symbol)
                    == Some(&child.artifact.search_evaluations),
            "retained parent differs from sealed child symbol={symbol}"
        );
        history.update(child.artifact.trial_history_sha256.as_bytes());
        exercised_trials = exercised_trials
            .checked_add(child.artifact.exercised_trials)
            .context("retained exercised trial count overflow")?;
    }
    ensure!(
        format!("{:x}", history.finalize()) == parent.trial_history_sha256
            && exercised_trials == parent.exercised_trials,
        "retained parent combined trial history changed"
    );
    for proof in [
        Some(&parent.selection_incumbent),
        parent.selection_candidate.as_ref(),
        parent.validation_incumbent.as_ref(),
        parent.validation_candidate.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        ensure!(
            population::joint_candidate_id(cfg, schemas, &symbols, &proof.params_by_symbol)?
                == proof.candidate_id,
            "retained parent original candidate identity changed"
        );
    }
    Ok(())
}

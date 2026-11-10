use super::*;

pub(crate) fn run_proof_only(args: Args) -> Result<()> {
    ensure!(args.no_apply, "proof-only mode requires --no-apply");
    let retained_run_root = args
        .proof_only_run_root
        .clone()
        .context("retained optimizer run root missing")?;
    let artifact = candidate_proof::load_candidate_result(&retained_run_root)?;
    ensure!(
        artifact.run_root == retained_run_root.display().to_string(),
        "retained joint proof run-root identity mismatch"
    );
    if artifact.schema_version == 18 {
        ensure!(
            candidate_proof::file_sha256(&retained_run_root.join("trials.jsonl"))?
                == artifact.trial_history_sha256,
            "retained joint trial history changed"
        );
    }
    let defaults_root = retained_run_root.join("defaults");
    let config_snapshot = defaults_root.join("config.main.toml");
    let root_spec_snapshot = defaults_root.join("optimization.yml");
    ensure!(
        candidate_proof::file_sha256(&config_snapshot)? == artifact.app_config_sha256,
        "retained optimizer app-config snapshot changed"
    );
    ensure!(
        candidate_proof::file_sha256(&root_spec_snapshot)? == artifact.root_spec_sha256
            && config::load_optimizer_selection_mode(&root_spec_snapshot)?
                == artifact.selection_mode,
        "retained optimizer root-spec snapshot changed"
    );
    let objective_profile = ObjectiveProfile::load(&defaults_root.join("objective_profile.yml"))?;
    ensure!(
        objective_profile.sha256() == artifact.objective_profile_sha256,
        "retained optimizer objective profile changed"
    );
    let retained_summary = if artifact.schema_version == 19 {
        Some(verify_native_report(
            &retained_run_root,
            &artifact,
            &objective_profile,
        )?)
    } else {
        None
    };
    let frozen_cfg =
        AppConfig::load_from_path_with_optimizer_path(&config_snapshot, &root_spec_snapshot)?;
    let symbols = artifact.frozen_capture_inputs.symbols()?;
    let all_symbols = VALID_ASSETS
        .iter()
        .map(|symbol| (*symbol).to_string())
        .collect::<Vec<_>>();

    let mut schemas = BTreeMap::new();
    for symbol in &all_symbols {
        let snapshot = defaults_root.join(format!("{symbol}.optimization.yml"));
        let expected_file_sha = artifact
            .symbol_spec_sha256s
            .get(symbol)
            .with_context(|| format!("retained symbol hash missing symbol={symbol}"))?;
        ensure!(
            candidate_proof::file_sha256(&snapshot)? == *expected_file_sha,
            "retained symbol snapshot changed symbol={symbol}"
        );
        let schema = load_symbol_schema_from_snapshot(&snapshot, &root_spec_snapshot)?;
        ensure!(
            child_artifact::hex_bytes(&schema.sha256())
                == *artifact
                    .parameter_schema_sha256s
                    .get(symbol)
                    .with_context(|| format!("retained schema hash missing symbol={symbol}"))?,
            "retained parameter schema changed symbol={symbol}"
        );
        schemas.insert(symbol.clone(), schema);
    }
    if let Some(retained_summary) = &retained_summary {
        let mut history_hasher = Sha256::new();
        let mut exercised_trials = 0usize;
        for symbol in &symbols {
            let child_path = retained_run_root
                .join("children")
                .join(symbol)
                .join("child_result.json");
            ensure!(
                candidate_proof::file_sha256(&child_path)?
                    == *artifact
                        .symbol_search_artifact_sha256s
                        .get(symbol)
                        .with_context(|| format!("retained child hash missing symbol={symbol}"))?,
                "retained child artifact changed symbol={symbol}"
            );
            let child = child_artifact::load_child_artifact(&child_path)?;
            child_artifact::validate::load_archived_symbol_candidates(
                &retained_run_root,
                &artifact,
                symbol,
                artifact
                    .parameter_schema_sha256s
                    .get(symbol)
                    .with_context(|| format!("retained schema hash missing symbol={symbol}"))?,
            )?;
            ensure!(
                retained_summary
                    .symbol_searches
                    .iter()
                    .find(|search| search.symbol == *symbol)
                    == Some(&child_artifact::search_report(&child)),
                "retained symbol search report differs from sealed child symbol={symbol}"
            );
            let selected = artifact
                .selection_candidate
                .as_ref()
                .context("retained parent selected proof missing")?;
            child_artifact::validate::validate_parent_bindings(
                &child,
                &retained_run_root,
                &artifact.frozen_capture_inputs,
                &artifact.selection_incumbent.params_by_symbol,
                &selected.params_by_symbol,
            )?;
            ensure!(
                artifact.symbol_spec_sha256s.get(symbol) == Some(&child.symbol_spec_sha256),
                "retained parent symbol defaults differ from sealed child symbol={symbol}"
            );
            if retained_run_root == Path::new(RETAINED_FINALIZATION_RUN_ROOT) {
                child_artifact::validate::validate_original_inputs(&retained_run_root, &child)?;
            }
            exercised_trials = exercised_trials
                .checked_add(child.exercised_trials)
                .context("retained parent exercised trial count overflow")?;
            history_hasher.update(child.trial_history_sha256.as_bytes());
        }
        ensure!(
            format!("{:x}", history_hasher.finalize()) == artifact.trial_history_sha256
                && exercised_trials == artifact.exercised_trials,
            "retained combined symbol-search history changed"
        );
    }

    if retained_run_root == Path::new(RETAINED_FINALIZATION_RUN_ROOT) {
        ensure!(
            artifact.frozen_capture_inputs.holdout.len() == 1
                && artifact.frozen_capture_inputs.holdout[0].capture
                    == Path::new(RETAINED_VALIDATION_CAPTURE)
                && artifact.search_trials == 990
                && artifact.exercised_trials == 1002,
            "retained completion proof changed the original validation capture or search counts"
        );
    }
    verify_capture_inputs(
        &artifact.frozen_capture_inputs,
        &frozen_cfg,
        &artifact.root_spec_sha256,
        &artifact.objective_profile_sha256,
        &symbols,
    )?;

    let mut proofs = vec![&artifact.selection_incumbent];
    if let Some(proof) = &artifact.selection_candidate {
        proofs.push(proof);
    }
    if let Some(proof) = &artifact.validation_incumbent {
        proofs.push(proof);
    }
    if let Some(proof) = &artifact.validation_candidate {
        proofs.push(proof);
    }
    for proof in proofs {
        ensure!(
            population::joint_candidate_id(
                &frozen_cfg,
                &schemas,
                &symbols,
                &proof.params_by_symbol,
            )? == proof.candidate_id,
            "retained joint candidate identity changed candidate_id={}",
            proof.candidate_id
        );
    }
    if artifact.schema_version == 18 {
        ensure!(
            !artifact.applied || artifact.selected_learning_bindings.len() == VALID_ASSETS.len(),
            "applied retained proof lacks all learning bindings"
        );
    } else {
        let promoted_symbols = artifact
            .selected_learning_bindings
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected_promoted_symbols = if artifact.applied {
            artifact.eligible_symbols()
        } else {
            BTreeSet::new()
        };
        ensure!(
            promoted_symbols == expected_promoted_symbols,
            "retained learning bindings differ from the sealed eligible symbol set"
        );
    }
    for (symbol, binding) in &artifact.selected_learning_bindings {
        ensure!(
            symbols.contains(symbol),
            "retained learning binding has invalid symbol={symbol}"
        );
        let path = args
            .spec
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&binding.path);
        ensure!(
            candidate_proof::file_sha256(&path)? == binding.sha256,
            "retained selected learning artifact changed symbol={symbol}"
        );
    }
    if let Some(retained_summary) = retained_summary.filter(|summary| summary.provenance.is_none())
    {
        let corrected_summary = corrected_historical_summary(retained_summary, &artifact)?;
        print!(
            "{}",
            polymarket_hft::report_markdown::render_optimizer_summary_correction(
                &corrected_summary
            )
        );
    }
    println!(
        "CMAES_AUTO_OPTIMIZE proof_only_done candidate={} selection={} validation={} structural={} economic={} applied={}",
        artifact.artifact_identity,
        artifact
            .selection_candidate
            .as_ref()
            .map_or("missing", |proof| proof.candidate_id.as_str()),
        artifact
            .validation_candidate
            .as_ref()
            .map_or("unavailable", |proof| proof.candidate_id.as_str()),
        artifact.structural_pass,
        artifact.all_four_economic_pass,
        artifact.applied,
    );
    Ok(())
}

fn verify_native_report(
    retained_run_root: &Path,
    artifact: &candidate_proof::CandidateResultArtifact,
    objective_profile: &ObjectiveProfile,
) -> Result<polymarket_hft::optimizer_report::OptimizerSummaryArtifact> {
    let summary_json_path = retained_run_root.join("summary.json");
    let summary: polymarket_hft::optimizer_report::OptimizerSummaryArtifact =
        serde_json::from_slice(
            &fs::read(&summary_json_path)
                .with_context(|| format!("read {}", summary_json_path.display()))?,
        )?;
    let retained_markdown_path = retained_run_root.join("summary.md");
    let retained_markdown = fs::read_to_string(&retained_markdown_path)
        .with_context(|| format!("read {}", retained_markdown_path.display()))?;
    ensure!(
        polymarket_hft::report_markdown::optimizer_summary_report_matches(
            &summary,
            &retained_markdown,
        ),
        "retained optimizer Markdown differs from its paired native JSON"
    );
    ensure!(
        summary.schema_version == artifact.schema_version
            && summary.run_root == artifact.run_root
            && Path::new(&summary.candidate_result_path)
                == retained_run_root.join("candidate_result.json")
            && summary.search_trials == artifact.search_trials
            && summary.exercised_trials == artifact.exercised_trials
            && summary.objective_profile == summary::objective_profile_report(objective_profile)
            && summary.application_reason == artifact.application_reason
            && summary.applied == artifact.applied,
        "retained optimizer summary identity or application differs from sealed proof"
    );
    verify_publication_mode(&summary)?;
    summary::verify_native_summary_core(&summary, artifact, objective_profile)?;
    report::verify_structural_summary(&summary, artifact, objective_profile)?;
    let expected_provenance = provenance_from_artifact(artifact)?;
    if let Some(provenance) = &summary.provenance {
        let mut comparable_expected = expected_provenance;
        comparable_expected.selected_learning_bindings =
            provenance.selected_learning_bindings.clone();
        ensure!(
            provenance == &comparable_expected,
            "retained optimizer provenance differs from sealed proof"
        );
        if let Some(projected) = &provenance.selected_learning_bindings {
            ensure!(
                projected
                    == &summary::selected_learning_binding_reports(
                        &artifact.selected_learning_bindings,
                    ),
                "retained optimizer learning-binding projection differs from sealed proof"
            );
        }
    }
    Ok(summary)
}

fn verify_publication_mode(
    summary: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
) -> Result<()> {
    let validation_capture = summary
        .validation
        .iter()
        .filter_map(|validation| validation.capture.as_ref());
    let structural_captures = summary
        .structural_recheck
        .iter()
        .flat_map(|structural| structural.capture_replays.iter())
        .map(|replay| &replay.capture);
    let captures = summary
        .captures
        .iter()
        .chain(validation_capture)
        .chain(structural_captures)
        .collect::<Vec<_>>();
    let all_legacy = captures.iter().all(|capture| {
        capture.capture_duration_ms.is_none() && capture.replay_duration_ms.is_none()
    });
    let all_scoped = captures.iter().all(|capture| {
        capture.duration_ms == 0
            && capture.capture_duration_ms.is_some()
            && capture.replay_duration_ms.is_some()
    });
    ensure!(
        all_legacy || all_scoped,
        "retained optimizer report mixes legacy and scoped duration evidence"
    );
    ensure!(
        summary.provenance.is_none() == all_legacy,
        "retained optimizer provenance and duration publication modes differ"
    );
    Ok(())
}

fn provenance_from_artifact(
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<polymarket_hft::optimizer_report::OptimizerRunProvenanceReport> {
    summary::provenance_report(
        artifact.selection_mode,
        &artifact.frozen_capture_inputs.selection,
        &artifact.frozen_capture_inputs.holdout,
        &artifact.app_config_sha256,
        &artifact.root_spec_sha256,
        artifact
            .selection_candidate
            .as_ref()
            .map(|proof| proof.final_oracle_learning_sha256.as_str()),
        artifact
            .validation_candidate
            .as_ref()
            .map(|proof| proof.final_oracle_learning_sha256.as_str()),
        Some(&artifact.selected_learning_bindings),
    )
}

fn corrected_historical_summary(
    mut summary: polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<polymarket_hft::optimizer_report::OptimizerSummaryArtifact> {
    summary.provenance = Some(provenance_from_artifact(artifact)?);
    for capture in &mut summary.captures {
        apply_scoped_duration(capture, &artifact.frozen_capture_inputs)?;
    }
    if let Some(structural) = &mut summary.structural_recheck {
        for replay in &mut structural.capture_replays {
            apply_scoped_duration(&mut replay.capture, &artifact.frozen_capture_inputs)?;
        }
    }
    Ok(summary)
}

fn apply_scoped_duration(
    report: &mut polymarket_hft::optimizer_report::OptimizerCaptureReport,
    inputs: &FrozenCaptureInputs,
) -> Result<()> {
    let input = inputs
        .selection
        .iter()
        .chain(&inputs.holdout)
        .find(|input| input.capture.display().to_string() == report.capture)
        .with_context(|| format!("sealed capture missing path={}", report.capture))?;
    report.duration_ms = 0;
    report.capture_duration_ms = Some(input.capture_end_ms.saturating_sub(input.capture_start_ms));
    report.replay_duration_ms = Some(input.replay_end_ms.saturating_sub(input.replay_start_ms));
    Ok(())
}

fn verify_capture_inputs(
    retained: &FrozenCaptureInputs,
    cfg: &AppConfig,
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
    symbols: &[String],
) -> Result<()> {
    ensure!(
        retained.selection.len() == 1 && retained.holdout.len() <= 1,
        "retained joint capture-role coverage changed"
    );
    retained.validate_frozen_execution_timing(cfg)?;
    for input in retained.all() {
        ensure!(
            input.prepared_proof_identities_by_symbol.len() == symbols.len(),
            "retained prepared proof coverage changed capture={}",
            input.capture.display()
        );
        for (symbol, current) in candidate_proof::build_symbol_proof_identities(
            symbols,
            &input.capture,
            cfg,
            root_spec_sha256,
            objective_profile_sha256,
        )? {
            let retained = input
                .prepared_proof_identities_by_symbol
                .get(&symbol)
                .with_context(|| format!("retained prepared proof missing symbol={symbol}"))?;
            ensure!(
                retained.same_capture_content_ignoring_optimizer_binary(&current),
                "retained prepared capture content changed symbol={symbol} capture={}",
                input.capture.display()
            );
        }
        let mut current_learning =
            match polymarket_hft::capture::session::captured_initial_oracle_learning_bytes(
                &input.capture,
            )? {
                Some(bytes) => {
                    rmp_serde::from_slice::<BTreeMap<String, OracleLearningSnapshot>>(&bytes)
                        .context("decode retained capture initial oracle learning")?
                }
                None => BTreeMap::new(),
            };
        current_learning.retain(|symbol, _| {
            input
                .prepared_proof_identities_by_symbol
                .contains_key(symbol)
        });
        ensure!(
            candidate_proof::oracle_learning_sha256(&current_learning)?
                == input.initial_oracle_learning_sha256
                && current_learning
                    .iter()
                    .map(|(symbol, learning)| { (symbol.clone(), learning.knowledge_cutoff_ms()) })
                    .collect::<BTreeMap<_, _>>()
                    == input.initial_oracle_learning_cutoffs_ms,
            "retained capture learner seed changed capture={}",
            input.capture.display()
        );
    }
    Ok(())
}

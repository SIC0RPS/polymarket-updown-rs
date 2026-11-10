use super::*;

#[derive(Clone, Debug)]
pub(crate) struct ArchivedSymbolCandidate {
    pub(crate) params: BTreeMap<String, ParamValue>,
    pub(crate) origin_id: String,
    pub(crate) search_trial: bool,
}

#[derive(Deserialize)]
struct ArchivedChildTrial {
    #[serde(default)]
    selection_mode: SelectionMode,
    index: usize,
    candidate_id: String,
    params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    origin: TrialOrigin,
    origin_id: Option<String>,
    complete_coverage: bool,
    selection_score: Option<scoring::FeasibilityScore>,
    retained_pack_scores: Vec<RetainedPackScore>,
}

pub(crate) fn load_archived_symbol_candidates(
    run_root: &Path,
    parent: &candidate_proof::CandidateResultArtifact,
    symbol: &str,
    expected_parameter_schema_sha256: &str,
) -> Result<(Vec<ArchivedSymbolCandidate>, Option<String>)> {
    let child_root = run_root.join("children").join(symbol);
    let artifact_path = child_root.join("child_result.json");
    let (artifact, trials) = load_validated_child_trials(run_root, symbol)?;
    let expected_artifact_sha256 = parent
        .symbol_search_artifact_sha256s
        .get(symbol)
        .with_context(|| format!("parent child-artifact hash missing symbol={symbol}"))?;
    ensure!(
        file_sha256(&artifact_path)? == *expected_artifact_sha256
            && artifact.symbol == symbol
            && artifact.selection_mode == parent.selection_mode
            && artifact.parameter_schema_sha256 == expected_parameter_schema_sha256
            && artifact.root_spec_sha256 == parent.root_spec_sha256
            && artifact.app_config_sha256 == parent.app_config_sha256
            && artifact.objective_profile_sha256 == parent.objective_profile_sha256,
        "archived symbol child provenance mismatch symbol={symbol}"
    );
    ensure!(
        artifact.search_evaluations
            == parent
                .search_trials_by_symbol
                .get(symbol)
                .copied()
                .with_context(|| format!("parent search count missing symbol={symbol}"))?,
        "archived symbol child search count mismatch symbol={symbol}"
    );

    ensure!(
        !parent.run_root.is_empty()
            && parent.symbol_spec_sha256s.get(symbol) == Some(&artifact.symbol_spec_sha256)
            && parent.parameter_schema_sha256s.get(symbol)
                == Some(&artifact.parameter_schema_sha256),
        "archived symbol original configuration provenance mismatch symbol={symbol}"
    );
    let original_root = Path::new(&parent.run_root);
    let original_child = original_root
        .join("children")
        .join(symbol)
        .join("child_result.json");
    ensure!(
        file_sha256(&original_child)? == *expected_artifact_sha256,
        "archived symbol original child changed symbol={symbol}"
    );
    let (original_cfg, original_schema) = load_original_configuration(original_root, &artifact)?;
    let schemas = BTreeMap::from([(symbol.to_string(), original_schema)]);
    let symbols = [symbol.to_string()];
    for trial in &trials {
        ensure!(
            population::joint_candidate_id(
                &original_cfg,
                &schemas,
                &symbols,
                &trial.params_by_symbol,
            )? == trial.candidate_id,
            "archived original joint candidate identity mismatch symbol={symbol} index={}",
            trial.index
        );
    }

    let trial_path = child_root.join("trials.jsonl");
    let mut candidates = Vec::new();
    for trial in trials
        .into_iter()
        .filter(|trial| trial.origin == TrialOrigin::Search && trial.complete_coverage)
    {
        candidates.push(ArchivedSymbolCandidate {
            params: trial.params_by_symbol[symbol].clone(),
            origin_id: format!(
                "{}#{}:{}:{}",
                trial_path.display(),
                artifact.artifact_identity,
                trial.index,
                trial.candidate_id
            ),
            search_trial: true,
        });
    }
    candidates.push(ArchivedSymbolCandidate {
        params: artifact.selected_candidate.params_by_symbol[symbol].clone(),
        origin_id: format!(
            "{}#{}",
            artifact_path.display(),
            artifact.selected_candidate.candidate_id
        ),
        search_trial: false,
    });
    let optimizer_binary_sha256 = artifact.frozen_capture_inputs.selection[0]
        .prepared_proof_identities_by_symbol
        .get(symbol)
        .map(|identity| identity.optimizer_binary_sha256().to_string());
    Ok((candidates, optimizer_binary_sha256))
}

pub(crate) fn load_validated_child(run_root: &Path, symbol: &str) -> Result<SymbolChildArtifact> {
    load_validated_child_trials(run_root, symbol).map(|(artifact, _)| artifact)
}

fn load_validated_child_trials(
    run_root: &Path,
    symbol: &str,
) -> Result<(SymbolChildArtifact, Vec<ArchivedChildTrial>)> {
    let child_root = run_root.join("children").join(symbol);
    let artifact = load_child_artifact(&child_root.join("child_result.json"))?;
    ensure!(
        artifact.symbol == symbol,
        "retained child symbol mismatch symbol={symbol}"
    );
    let trial_path = child_root.join("trials.jsonl");
    let bytes = fs::read(&trial_path).with_context(|| format!("read {}", trial_path.display()))?;
    ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == artifact.trial_history_sha256,
        "retained symbol trial history digest mismatch symbol={symbol}"
    );
    let contents =
        std::str::from_utf8(&bytes).with_context(|| format!("read {}", trial_path.display()))?;
    let mut trials = Vec::new();
    let mut search_trials = 0usize;
    let capture = &artifact.default_candidate.capture_path;
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let trial: ArchivedChildTrial = serde_json::from_str(line)
            .with_context(|| format!("parse {}", trial_path.display()))?;
        ensure!(
            trial.index == trials.len() + 1
                && trial.params_by_symbol.len() == 1
                && trial.params_by_symbol.contains_key(symbol),
            "retained child trial index or symbol scope mismatch symbol={symbol}"
        );
        let pack = trial
            .retained_pack_scores
            .first()
            .context("retained child trial pack missing")?;
        ensure!(
            trial.selection_mode == artifact.selection_mode
                && pack.selection_mode == trial.selection_mode
                && trial.retained_pack_scores.len() == 1
                && pack.capture == *capture
                && trial.complete_coverage == pack.is_eligible()
                && trial.selection_score
                    == pack
                        .candidate_selection_score
                        .filter(|_| trial.complete_coverage),
            "retained child trial selection evidence mismatch symbol={symbol} index={}",
            trial.index
        );
        scoring::validate_prediction_evidence(
            trial.selection_mode,
            &[symbol.to_string()],
            &pack.default_prediction_metrics,
        )?;
        if trial.selection_mode.is_profit() || trial.complete_coverage {
            scoring::validate_prediction_evidence(
                trial.selection_mode,
                &[symbol.to_string()],
                &pack.candidate_prediction_metrics,
            )?;
        } else {
            for (key, metrics) in &pack.candidate_prediction_metrics {
                ensure!(
                    key == symbol,
                    "retained rejected prediction symbol mismatch"
                );
                metrics.validate()?;
            }
        }
        if trial.complete_coverage {
            scoring::selection_score(
                trial.selection_mode,
                trial
                    .selection_score
                    .context("retained child trial score missing")?,
                &pack.candidate_prediction_metrics,
                &pack.default_prediction_metrics,
            )?;
        }
        if trial.origin == TrialOrigin::Search {
            search_trials = search_trials
                .checked_add(1)
                .context("retained child search count overflow")?;
            ensure!(
                trial.origin_id.is_none()
                    && trial.retained_pack_scores.len() == 1
                    && trial.retained_pack_scores[0].capture == *capture
                    && trial.complete_coverage == trial.retained_pack_scores[0].is_eligible()
                    && trial.selection_score
                        == trial.retained_pack_scores[0]
                            .candidate_selection_score
                            .filter(|_| trial.complete_coverage),
                "retained child search evidence is invalid symbol={symbol} index={}",
                trial.index
            );
        }
        trials.push(trial);
    }
    ensure!(
        trials.len() == artifact.exercised_trials
            && search_trials == artifact.search_evaluations
            && search_trials <= artifact.configured_max_function_evals,
        "retained child trial coverage mismatch symbol={symbol}"
    );
    let default = trials
        .first()
        .context("retained child default trial missing")?;
    ensure!(
        default.origin == TrialOrigin::Default && default.origin_id.is_none(),
        "retained child default origin mismatch symbol={symbol}"
    );
    validate_trial_proof(default, &artifact.default_candidate, symbol)?;
    let default_predictions = &default.retained_pack_scores[0].candidate_prediction_metrics;
    ensure!(
        trials.iter().all(
            |trial| trial.retained_pack_scores[0].default_prediction_metrics
                == *default_predictions
        ),
        "retained child prediction reference differs from frozen default symbol={symbol}"
    );
    let source = trials
        .iter()
        .find(|trial| trial.index == artifact.selected_source_trial_index)
        .context("retained child selected source missing")?;
    let rechecked = trials
        .iter()
        .find(|trial| trial.index == artifact.selected_recheck_trial_index)
        .context("retained child selected recheck missing")?;
    ensure!(
        source.origin.is_champion_eligible()
            && source.complete_coverage
            && matches!(
                rechecked.origin,
                TrialOrigin::ChampionRecheck | TrialOrigin::DefaultStabilityRecheck
            )
            && rechecked.origin_id.is_none()
            && source.candidate_id == rechecked.candidate_id
            && source.params_by_symbol == rechecked.params_by_symbol
            && source.selection_score == rechecked.selection_score
            && source.retained_pack_scores == rechecked.retained_pack_scores,
        "retained child selected source and recheck differ symbol={symbol}"
    );
    validate_trial_proof(rechecked, &artifact.selected_candidate, symbol)?;
    let default_rechecks = trials
        .iter()
        .filter(|trial| trial.origin == TrialOrigin::DefaultStabilityRecheck)
        .collect::<Vec<_>>();
    ensure!(
        default_rechecks.len() == 1
            && default_rechecks[0].candidate_id == default.candidate_id
            && default_rechecks[0].params_by_symbol == default.params_by_symbol
            && default_rechecks[0].selection_score == default.selection_score
            && default_rechecks[0].retained_pack_scores == default.retained_pack_scores,
        "retained child default stability evidence differs symbol={symbol}"
    );
    Ok((artifact, trials))
}

fn validate_trial_proof(
    trial: &ArchivedChildTrial,
    proof: &candidate_proof::CandidateReplayProof,
    symbol: &str,
) -> Result<()> {
    let pack = trial
        .retained_pack_scores
        .first()
        .context("retained child proof pack missing")?;
    ensure!(
        trial.complete_coverage
            && trial.selection_mode == proof.selection_mode
            && pack.selection_mode == proof.selection_mode
            && pack.candidate_prediction_metrics == proof.prediction_metrics
            && trial.retained_pack_scores.len() == 1
            && pack.is_eligible()
            && trial.candidate_id == proof.candidate_id
            && trial.params_by_symbol == proof.params_by_symbol
            && trial.selection_score == proof.selection_score
            && pack.capture == proof.capture_path
            && pack.candidate_selection_score == proof.selection_score
            && pack.initial_oracle_learning_sha256 == proof.initial_oracle_learning_sha256
            && pack.initial_oracle_learning_cutoffs_ms == proof.initial_oracle_learning_cutoffs_ms
            && pack.final_oracle_learning_sha256.as_ref()
                == Some(&proof.final_oracle_learning_sha256)
            && pack.accounting_coverage == proof.accounting_coverage
            && pack
                .candidate_pnl_by_symbol
                .as_ref()
                .and_then(|rows| rows.get(symbol))
                == proof.portfolio_accounting.row_for_symbol(symbol),
        "retained child journal differs from sealed replay proof symbol={symbol}"
    );
    Ok(())
}

fn load_original_configuration(
    run_root: &Path,
    artifact: &SymbolChildArtifact,
) -> Result<(AppConfig, OptimizerParameterSchema)> {
    let symbol = &artifact.symbol;
    ensure!(
        artifact.run_root == run_root.join("children").join(symbol).display().to_string(),
        "retained child original run-root mismatch symbol={symbol}"
    );
    let defaults = run_root.join("defaults");
    let config_path = defaults.join("config.main.toml");
    let root_spec = defaults.join("optimization.yml");
    let symbol_spec = defaults.join(format!("{symbol}.optimization.yml"));
    let objective_path = defaults.join("objective_profile.yml");
    ensure!(
        file_sha256(&config_path)? == artifact.app_config_sha256
            && file_sha256(&root_spec)? == artifact.root_spec_sha256
            && file_sha256(&symbol_spec)? == artifact.symbol_spec_sha256
            && file_sha256(&objective_path)? == artifact.objective_profile_sha256
            && config::load_optimizer_selection_mode(&root_spec)? == artifact.selection_mode,
        "retained child frozen configuration changed symbol={symbol}"
    );
    let schema = load_symbol_schema_from_snapshot(&symbol_spec, &root_spec)?;
    ensure!(
        hex_bytes(&schema.sha256()) == artifact.parameter_schema_sha256
            && schema.default_map()? == artifact.default_candidate.params_by_symbol[symbol],
        "retained child frozen schema or defaults changed symbol={symbol}"
    );
    schema.ordered_values(&artifact.selected_candidate.params_by_symbol[symbol])?;
    let cfg = AppConfig::load_from_path_with_symbol_optimizer_snapshot(
        &config_path,
        &root_spec,
        &symbol_spec,
    )?;
    Ok((cfg, schema))
}

pub(crate) fn validate_original_inputs(
    run_root: &Path,
    artifact: &SymbolChildArtifact,
) -> Result<()> {
    let symbol = &artifact.symbol;
    let (cfg, _) = load_original_configuration(run_root, artifact)?;
    let symbol_spec = run_root
        .join("defaults")
        .join(format!("{symbol}.optimization.yml"));
    let frozen_path = capture_inputs::FrozenCaptureInputs::snapshot_path(&symbol_spec);
    let frozen_bytes =
        fs::read(&frozen_path).with_context(|| format!("read {}", frozen_path.display()))?;
    let snapshot: capture_inputs::FrozenCaptureInputs = serde_json::from_slice(&frozen_bytes)?;
    ensure!(
        format!("{:x}", Sha256::digest(&frozen_bytes)) == artifact.frozen_capture_inputs_sha256
            && snapshot == artifact.frozen_capture_inputs,
        "retained child frozen capture snapshot changed symbol={symbol}"
    );
    let input = &snapshot.selection[0];
    input.validate_frozen_execution_timing(&cfg)?;
    polymarket_hft::strategy_runtime::replay_mode::validate_prepared_replay_sources_for_optimizer(
        &input.capture,
        std::slice::from_ref(symbol),
    )?;
    ensure!(
        BacktestMatrixEvaluator::replay_window_for_capture(&input.capture)?
            == (input.replay_start_ms, input.replay_end_ms)
            && input.prepared_proof_identities_by_symbol.len() == 1
            && input.initial_oracle_learning_sha256_by_symbol.len() == 1
            && input
                .market_lifecycles
                .iter()
                .all(|market| market.symbol == *symbol),
        "retained child capture scope or window changed symbol={symbol}"
    );
    let (markets, population) =
        polymarket_hft::strategy_runtime::replay_mode::prepared_market_inputs_for_capture(
            &input.capture,
            symbol,
        )?;
    let mut markets = markets
        .into_iter()
        .map(|(symbol, condition_id, window_start_ms, window_end_ms)| {
            capture_inputs::FrozenMarketLifecycle {
                symbol,
                condition_id,
                window_start_ms,
                window_end_ms,
            }
        })
        .collect::<Vec<_>>();
    markets.sort();
    markets.dedup();
    ensure!(
        input.role == capture_inputs::CaptureRole::Selection
            && !markets.is_empty()
            && markets == input.market_lifecycles
            && input
                .recorded_market_population
                .as_ref()
                .is_none_or(|retained| retained == &population),
        "retained child prepared market facts changed symbol={symbol}"
    );
    let current = candidate_proof::build_symbol_proof_identities(
        std::slice::from_ref(symbol),
        &input.capture,
        &cfg,
        &artifact.root_spec_sha256,
        &artifact.objective_profile_sha256,
    )?;
    ensure!(
        input
            .prepared_proof_identities_by_symbol
            .get(symbol)
            .is_some_and(|retained| retained
                .same_capture_content_ignoring_optimizer_binary(&current[symbol])),
        "retained child capture content changed symbol={symbol}"
    );
    let learning: BTreeMap<String, OracleLearningSnapshot> =
        match polymarket_hft::capture::session::captured_initial_oracle_learning_bytes(
            &input.capture,
        )? {
            Some(bytes) => {
                rmp_serde::from_slice(&bytes).context("decode retained child captured learners")?
            }
            None => BTreeMap::new(),
        };
    let learning = learning
        .into_iter()
        .filter(|(key, _)| key == symbol)
        .collect::<BTreeMap<_, _>>();
    let digest = candidate_proof::oracle_learning_sha256(&learning)?;
    let cutoffs = learning
        .iter()
        .map(|(key, state)| (key.clone(), state.knowledge_cutoff_ms()))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        digest == input.initial_oracle_learning_sha256
            && input.initial_oracle_learning_sha256_by_symbol.get(symbol) == Some(&digest)
            && cutoffs == input.initial_oracle_learning_cutoffs_ms
            && artifact.default_candidate.initial_oracle_learning_sha256 == digest
            && artifact.selected_candidate.initial_oracle_learning_sha256 == digest
            && artifact
                .default_candidate
                .initial_oracle_learning_cutoffs_ms
                == cutoffs
            && artifact
                .selected_candidate
                .initial_oracle_learning_cutoffs_ms
                == cutoffs,
        "retained child captured learner identity changed symbol={symbol}"
    );
    Ok(())
}

pub(crate) fn validate_parent_bindings(
    child: &SymbolChildArtifact,
    parent_run_root: &Path,
    frozen: &capture_inputs::FrozenCaptureInputs,
    default_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    selected_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<()> {
    let symbol = &child.symbol;
    ensure!(
        config::load_optimizer_selection_mode(&parent_run_root.join("defaults/optimization.yml"))?
            == child.selection_mode,
        "parent frozen selection mode differs from sealed child symbol={symbol}"
    );
    ensure!(
        frozen.selection.first().is_some_and(|input| default_params
            .keys()
            .eq(input.prepared_proof_identities_by_symbol.keys()))
            && default_params.keys().eq(selected_params.keys())
            && default_params.get(symbol) == child.default_candidate.params_by_symbol.get(symbol)
            && selected_params.get(symbol) == child.selected_candidate.params_by_symbol.get(symbol),
        "parent parameter maps differ from sealed child symbol={symbol}"
    );
    let mut projected = frozen.for_symbol_selection(symbol)?;
    if projected != child.frozen_capture_inputs {
        ensure!(
            Path::new(&child.run_root) == parent_run_root.join("children").join(symbol),
            "retained child run-root differs from parent symbol={symbol}"
        );
        let parent_identity = projected.selection[0]
            .prepared_proof_identities_by_symbol
            .get(symbol)
            .context("parent frozen A proof identity missing")?;
        let child_identity = child.frozen_capture_inputs.selection[0]
            .prepared_proof_identities_by_symbol
            .get(symbol)
            .context("child frozen A proof identity missing")?;
        ensure!(
            child_identity.same_capture_content_ignoring_optimizer_binary(parent_identity),
            "retained parent and child A content differ symbol={symbol}"
        );
        projected.selection[0]
            .prepared_proof_identities_by_symbol
            .insert(symbol.clone(), child_identity.clone());
    }
    ensure!(
        projected == child.frozen_capture_inputs,
        "parent frozen A facts differ from sealed child symbol={symbol}"
    );
    Ok(())
}

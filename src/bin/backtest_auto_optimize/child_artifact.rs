use super::*;
use sha2::{Digest, Sha256};

#[path = "child_artifact/validate.rs"]
pub(super) mod validate;

pub(super) const CHILD_ARTIFACT_SCHEMA: u32 = 35;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct SymbolChildArtifact {
    #[serde(default, skip_serializing_if = "SelectionMode::is_profit")]
    pub(super) selection_mode: SelectionMode,
    pub(super) schema_version: u32,
    pub(super) artifact_identity: String,
    pub(super) symbol: String,
    pub(super) run_root: String,
    pub(super) frozen_capture_inputs: capture_inputs::FrozenCaptureInputs,
    pub(super) frozen_capture_inputs_sha256: String,
    pub(super) app_config_sha256: String,
    pub(super) root_spec_sha256: String,
    pub(super) symbol_spec_sha256: String,
    pub(super) objective_profile_sha256: String,
    pub(super) parameter_schema_sha256: String,
    pub(super) seed: u64,
    pub(super) configured_max_function_evals: usize,
    pub(super) configured_max_generations_per_run: usize,
    pub(super) configured_max_no_improvement_evals: usize,
    pub(super) search_status: String,
    pub(super) restart_termination_reason: String,
    pub(super) run_termination_reasons: Vec<Vec<String>>,
    pub(super) search_runs: usize,
    pub(super) search_evaluations: usize,
    pub(super) no_improvement_evals: usize,
    pub(super) exercised_trials: usize,
    pub(super) trial_history_sha256: String,
    pub(super) default_candidate: candidate_proof::CandidateReplayProof,
    pub(super) selected_source_candidate_id: String,
    pub(super) selected_source_trial_index: usize,
    pub(super) selected_recheck_trial_index: usize,
    pub(super) selected_candidate: candidate_proof::CandidateReplayProof,
}

impl SymbolChildArtifact {
    pub(super) fn seal(mut self) -> Result<Self> {
        self.artifact_identity.clear();
        self.artifact_identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&self)?));
        Ok(self)
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == CHILD_ARTIFACT_SCHEMA,
            "unsupported symbol child schema"
        );
        let mut unsealed = self.clone();
        let identity = std::mem::take(&mut unsealed.artifact_identity);
        ensure!(
            identity == format!("{:x}", Sha256::digest(serde_json::to_vec(&unsealed)?)),
            "symbol child artifact identity mismatch"
        );
        ensure!(
            VALID_ASSETS.contains(&self.symbol.as_str())
                && self.frozen_capture_inputs.selection.len() == 1
                && self.frozen_capture_inputs.holdout.is_empty(),
            "symbol child scope is invalid"
        );
        ensure!(
            self.frozen_capture_inputs.sha256()? == self.frozen_capture_inputs_sha256,
            "symbol child frozen A digest mismatch"
        );
        ensure!(
            self.default_candidate.params_by_symbol.len() == 1
                && self
                    .default_candidate
                    .params_by_symbol
                    .contains_key(&self.symbol)
                && self.selected_candidate.params_by_symbol.len() == 1
                && self
                    .selected_candidate
                    .params_by_symbol
                    .contains_key(&self.symbol),
            "symbol child candidate map scope mismatch"
        );
        ensure!(
            self.default_candidate.capture_path
                == self.frozen_capture_inputs.selection[0]
                    .capture
                    .display()
                    .to_string()
                && self.selected_candidate.capture_path == self.default_candidate.capture_path,
            "symbol child proof capture mismatch"
        );
        ensure!(
            self.default_candidate.accounting_coverage.is_complete()
                && self.selected_candidate.accounting_coverage.is_complete()
                && self.default_candidate.selection_score.is_some()
                && self.selected_candidate.selection_score.is_some(),
            "symbol child proof is incomplete"
        );
        let symbols = vec![self.symbol.clone()];
        for proof in [&self.default_candidate, &self.selected_candidate] {
            ensure!(
                proof.selection_mode == self.selection_mode,
                "symbol child selection mode mismatch"
            );
            scoring::validate_prediction_evidence(
                self.selection_mode,
                &symbols,
                &proof.prediction_metrics,
            )?;
        }
        let default_score = scoring::selection_score(
            self.selection_mode,
            self.default_candidate
                .selection_score
                .context("symbol child default score missing")?,
            &self.default_candidate.prediction_metrics,
            &self.default_candidate.prediction_metrics,
        )?;
        let selected_score = scoring::selection_score(
            self.selection_mode,
            self.selected_candidate
                .selection_score
                .context("symbol child selected score missing")?,
            &self.selected_candidate.prediction_metrics,
            &self.default_candidate.prediction_metrics,
        )?;
        ensure!(
            !selected_score.compare(default_score)?.is_lt(),
            "symbol child selected score is worse than its default"
        );
        ensure!(
            self.selected_candidate.candidate_id == self.selected_source_candidate_id,
            "symbol child recheck identity differs from selected source"
        );
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_child_artifact(
    ctx: &RunContext,
    frozen: &capture_inputs::FrozenCaptureInputs,
    settings: &OptimizerSettings,
    search: &search::SearchOutcome,
    selected_source_trial_index: usize,
    selected_recheck_trial_index: usize,
    symbol_snapshot: &Path,
    symbol_spec_sha256: &str,
    root_spec_sha256: &str,
    config_snapshot: &Path,
) -> Result<SymbolChildArtifact> {
    ensure!(ctx.symbols.len() == 1, "child artifact requires one symbol");
    let symbol = ctx.symbols[0].clone();
    ensure!(
        file_sha256(symbol_snapshot)? == symbol_spec_sha256
            && file_sha256(&ctx.optimizer_spec_path)? == root_spec_sha256,
        "symbol child frozen optimizer inputs changed"
    );
    ensure!(
        config::load_optimizer_selection_mode(&ctx.optimizer_spec_path)? == ctx.selection_mode
            && settings.selection_mode == ctx.selection_mode,
        "symbol child selection mode differs from frozen root settings"
    );
    let trials = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?;
    let default = trials
        .first()
        .filter(|trial| trial.origin == TrialOrigin::Default)
        .context("symbol child default trial missing")?;
    let source = trials
        .iter()
        .find(|trial| trial.index == selected_source_trial_index)
        .context("symbol child selected source trial missing")?;
    let rechecked = trials
        .iter()
        .find(|trial| trial.index == selected_recheck_trial_index)
        .context("symbol child selected recheck trial missing")?;
    ensure!(
        default.selection_mode == ctx.selection_mode
            && source.selection_mode == ctx.selection_mode
            && rechecked.selection_mode == ctx.selection_mode
            && source.candidate_id == rechecked.candidate_id
            && source.params_by_symbol == rechecked.params_by_symbol
            && source.selection_score == rechecked.selection_score
            && source.retained_pack_scores == rechecked.retained_pack_scores
            && scoring::trial_selection_score(source)?
                == scoring::trial_selection_score(rechecked)?
            && source.oracle_learning_sha256 == rechecked.oracle_learning_sha256,
        "symbol child selected result changed during A recheck"
    );
    let capture = ctx.primary_capture().display().to_string();
    let artifact = SymbolChildArtifact {
        selection_mode: ctx.selection_mode,
        schema_version: CHILD_ARTIFACT_SCHEMA,
        artifact_identity: String::new(),
        symbol: symbol.clone(),
        run_root: ctx.run_root.display().to_string(),
        frozen_capture_inputs: frozen.clone(),
        frozen_capture_inputs_sha256: frozen.sha256()?,
        app_config_sha256: file_sha256(config_snapshot)?,
        root_spec_sha256: root_spec_sha256.to_string(),
        symbol_spec_sha256: symbol_spec_sha256.to_string(),
        objective_profile_sha256: ctx.objective_profile.sha256().to_string(),
        parameter_schema_sha256: hex_bytes(
            &ctx.parameter_schemas
                .get(&symbol)
                .context("symbol child schema missing")?
                .sha256(),
        ),
        seed: settings.seed,
        configured_max_function_evals: settings.max_function_evals,
        configured_max_generations_per_run: settings.max_generations_per_run,
        configured_max_no_improvement_evals: settings.max_no_improvement_evals,
        search_status: search.search_status.clone(),
        restart_termination_reason: search.restart_termination_reason.clone(),
        run_termination_reasons: search.run_termination_reasons.clone(),
        search_runs: search.runs,
        search_evaluations: search.function_evals,
        no_improvement_evals: search.no_improvement_evals,
        exercised_trials: trials.len(),
        trial_history_sha256: file_sha256(&ctx.run_root.join("trials.jsonl"))?,
        default_candidate: candidate_proof::selection_replay_proof(default, &capture)?,
        selected_source_candidate_id: source.candidate_id.clone(),
        selected_source_trial_index,
        selected_recheck_trial_index,
        selected_candidate: candidate_proof::selection_replay_proof(rechecked, &capture)?,
    }
    .seal()?;
    artifact.validate()?;
    polymarket_hft::runtime_files::write_bytes_atomic(
        &ctx.run_root.join("child_result.json"),
        &serde_json::to_vec_pretty(&artifact)?,
    )?;
    Ok(artifact)
}

pub(super) fn load_child_artifact(path: &Path) -> Result<SymbolChildArtifact> {
    let artifact: SymbolChildArtifact = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )?;
    artifact.validate()?;
    Ok(artifact)
}

pub(super) fn search_report(
    artifact: &SymbolChildArtifact,
) -> polymarket_hft::optimizer_report::OptimizerSymbolSearchReport {
    polymarket_hft::optimizer_report::OptimizerSymbolSearchReport {
        selection_mode: artifact.selection_mode.as_str().to_string(),
        symbol: artifact.symbol.clone(),
        status: artifact.search_status.clone(),
        seed: artifact.seed,
        configured_max_function_evals: artifact.configured_max_function_evals,
        configured_max_generations_per_run: artifact.configured_max_generations_per_run,
        configured_max_no_improvement_evals: artifact.configured_max_no_improvement_evals,
        search_evaluations: artifact.search_evaluations,
        no_improvement_evaluations: artifact.no_improvement_evals,
        search_runs: artifact.search_runs,
        exercised_trials: artifact.exercised_trials,
        default_candidate_id: artifact.default_candidate.candidate_id.clone(),
        selected_candidate_id: artifact.selected_candidate.candidate_id.clone(),
        selected_params: artifact.selected_candidate.params_by_symbol[&artifact.symbol]
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().into()))
            .collect(),
        artifact_identity: artifact.artifact_identity.clone(),
        error: None,
    }
}

pub(super) fn file_sha256(path: &Path) -> Result<String> {
    candidate_proof::file_sha256(path)
}

pub(crate) fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("write hex");
    }
    encoded
}

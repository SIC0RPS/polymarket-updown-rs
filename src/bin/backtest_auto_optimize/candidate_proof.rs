use super::*;
use polymarket_hft::strategy_runtime::backtest_ohlc_manifest::summarize_capture_ohlc_manifest;
use polymarket_hft::strategy_runtime::prediction_metrics::PredictionMetrics;
use polymarket_hft::trade_session_ledger::{CanonicalCoverageProof, CAPTURED_SESSION_LEDGER_FILE};
use sha2::{Digest, Sha256};
use std::io::Read;

#[path = "candidate_proof/validation.rs"]
mod validation;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SymbolChildProofIdentity {
    symbol: String,
    capture_path: String,
    capture_config_snapshot_sha256: Option<String>,
    session_meta_sha256: String,
    replay_risk_snapshot_sha256: String,
    canonical_session_ledger_sha256: String,
    prepared_manifest_sha256: String,
    bbwick_ohlc_sha256: BTreeMap<String, String>,
    root_spec_sha256: String,
    objective_profile_sha256: String,
    optimizer_binary_sha256: String,
}

impl SymbolChildProofIdentity {
    pub(crate) fn optimizer_binary_sha256(&self) -> &str {
        &self.optimizer_binary_sha256
    }

    pub(crate) fn bind_child_executable(
        &mut self,
        executable: &Path,
        expected_sha256: &str,
    ) -> Result<()> {
        let sha256 = file_sha256(executable)?;
        ensure!(
            sha256 == expected_sha256,
            "retained child executable identity changed"
        );
        self.optimizer_binary_sha256 = sha256;
        Ok(())
    }

    pub(crate) fn same_capture_content_ignoring_optimizer_binary(&self, other: &Self) -> bool {
        self.symbol == other.symbol
            && self.capture_path == other.capture_path
            && self.capture_config_snapshot_sha256 == other.capture_config_snapshot_sha256
            && self.session_meta_sha256 == other.session_meta_sha256
            && self.replay_risk_snapshot_sha256 == other.replay_risk_snapshot_sha256
            && self.canonical_session_ledger_sha256 == other.canonical_session_ledger_sha256
            && self.prepared_manifest_sha256 == other.prepared_manifest_sha256
            && self.bbwick_ohlc_sha256 == other.bbwick_ohlc_sha256
            && self.root_spec_sha256 == other.root_spec_sha256
            && self.objective_profile_sha256 == other.objective_profile_sha256
    }
}

pub(crate) fn file_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).with_context(|| format!("read {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn optional_file_sha256(path: &Path) -> Result<Option<String>> {
    path.is_file().then(|| file_sha256(path)).transpose()
}

pub(crate) fn build_symbol_proof_identities(
    symbols: &[impl AsRef<str>],
    capture: &Path,
    cfg: &AppConfig,
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
) -> Result<BTreeMap<String, SymbolChildProofIdentity>> {
    let optimizer_binary = std::env::current_exe().context("resolve optimizer executable")?;
    let capture_path = capture.display().to_string();
    let capture_config_snapshot_sha256 =
        optional_file_sha256(&capture.join("config_snapshot.toml"))?;
    let session_meta_sha256 = file_sha256(&capture.join("session_meta.json"))?;
    let replay_risk_snapshot_sha256 = file_sha256(&capture.join("replay_risk_snapshot.jsonl"))?;
    let canonical_session_ledger_sha256 = file_sha256(&capture.join(CAPTURED_SESSION_LEDGER_FILE))?;
    let optimizer_binary_sha256 = file_sha256(&optimizer_binary)?;
    let mut identities = BTreeMap::new();
    for requested_symbol in symbols {
        let requested_symbol = requested_symbol.as_ref();
        let symbol = requested_symbol.to_ascii_uppercase();
        let prepared_manifest = capture
            .join("backtest_replay/v2")
            .join(symbol.to_ascii_lowercase())
            .join("manifest.json");
        let bbwick_ohlc_sha256 =
            summarize_capture_ohlc_manifest(capture, cfg, std::slice::from_ref(&symbol))?
                .hash_by_key();
        identities.insert(
            requested_symbol.to_string(),
            SymbolChildProofIdentity {
                symbol,
                capture_path: capture_path.clone(),
                capture_config_snapshot_sha256: capture_config_snapshot_sha256.clone(),
                session_meta_sha256: session_meta_sha256.clone(),
                replay_risk_snapshot_sha256: replay_risk_snapshot_sha256.clone(),
                canonical_session_ledger_sha256: canonical_session_ledger_sha256.clone(),
                prepared_manifest_sha256: file_sha256(&prepared_manifest)?,
                bbwick_ohlc_sha256,
                root_spec_sha256: root_spec_sha256.to_string(),
                objective_profile_sha256: objective_profile_sha256.to_string(),
                optimizer_binary_sha256: optimizer_binary_sha256.clone(),
            },
        );
    }
    Ok(identities)
}

pub(crate) fn oracle_learning_sha256(
    learning: &BTreeMap<String, polymarket_hft::strategy::OracleLearningSnapshot>,
) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(rmp_serde::to_vec_named(learning)?)
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CandidateReplayProof {
    #[serde(default, skip_serializing_if = "SelectionMode::is_profit")]
    pub(crate) selection_mode: SelectionMode,
    pub(crate) capture_path: String,
    pub(crate) candidate_id: String,
    pub(crate) params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    pub(crate) canonical_session_stats: CanonicalSessionStats,
    pub(crate) portfolio_accounting: PortfolioAccountingProjectionProof,
    pub(crate) initial_oracle_learning_sha256: String,
    pub(crate) initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    pub(crate) final_oracle_learning_sha256: String,
    pub(crate) selection_score: Option<scoring::FeasibilityScore>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) prediction_metrics: BTreeMap<String, PredictionMetrics>,
    pub(crate) accounting_coverage: CanonicalCoverageProof,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SelectionEconomicProof {
    pub(crate) symbol: String,
    pub(crate) candidate_net_pnl_units_6dp: i128,
    pub(crate) incumbent_net_pnl_units_6dp: i128,
    pub(crate) candidate_profitable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SelectedLearningBindingProof {
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
}

pub(crate) fn selected_learning_binding_proofs(
    bindings: &BTreeMap<String, polymarket_hft::config::OptimizerLearningSelection>,
) -> BTreeMap<String, SelectedLearningBindingProof> {
    bindings
        .iter()
        .map(|(symbol, binding)| {
            (
                symbol.clone(),
                SelectedLearningBindingProof {
                    path: binding.path.clone(),
                    sha256: binding.sha256.clone(),
                },
            )
        })
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct CandidateResultArtifact {
    #[serde(default, skip_serializing_if = "SelectionMode::is_profit")]
    pub(crate) selection_mode: SelectionMode,
    pub(crate) schema_version: u32,
    pub(crate) artifact_identity: String,
    pub(crate) run_root: String,
    pub(crate) frozen_capture_inputs: capture_inputs::FrozenCaptureInputs,
    pub(crate) frozen_capture_inputs_sha256: String,
    pub(crate) app_config_sha256: String,
    pub(crate) root_spec_sha256: String,
    pub(crate) objective_profile_sha256: String,
    pub(crate) symbol_spec_sha256s: BTreeMap<String, String>,
    pub(crate) parameter_schema_sha256s: BTreeMap<String, String>,
    pub(crate) search_trials: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) search_trials_by_symbol: BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) symbol_search_artifact_sha256s: BTreeMap<String, String>,
    pub(crate) exercised_trials: usize,
    pub(crate) trial_history_sha256: String,
    pub(crate) selected_source_trial_index: Option<usize>,
    pub(crate) selected_recheck_trial_index: Option<usize>,
    pub(crate) selection_incumbent: CandidateReplayProof,
    pub(crate) selection_candidate: Option<CandidateReplayProof>,
    pub(crate) validation_incumbent: Option<CandidateReplayProof>,
    pub(crate) validation_candidate: Option<CandidateReplayProof>,
    pub(crate) selection_economics_by_symbol: BTreeMap<String, SelectionEconomicProof>,
    pub(crate) validation_economics_by_symbol: BTreeMap<String, holdout::SymbolEconomicResult>,
    pub(crate) validation_error: Option<String>,
    pub(crate) structural_pass: bool,
    pub(crate) all_four_economic_pass: bool,
    pub(crate) applied: bool,
    pub(crate) application_reason: String,
    pub(crate) selected_learning_bindings: BTreeMap<String, SelectedLearningBindingProof>,
}

impl CandidateResultArtifact {
    pub(crate) fn eligible_symbols(&self) -> BTreeSet<String> {
        eligible_symbols_from_economics(
            &self.selection_economics_by_symbol,
            &self.validation_economics_by_symbol,
        )
    }

    pub(crate) fn seal(mut self) -> Result<Self> {
        self.artifact_identity.clear();
        self.artifact_identity = format!("{:x}", Sha256::digest(serde_json::to_vec(&self)?));
        Ok(self)
    }
}

pub(crate) fn eligible_symbols_from_economics(
    selection: &BTreeMap<String, SelectionEconomicProof>,
    validation: &BTreeMap<String, holdout::SymbolEconomicResult>,
) -> BTreeSet<String> {
    VALID_ASSETS
        .iter()
        .filter(|symbol| {
            selection
                .get(**symbol)
                .is_some_and(|row| row.candidate_profitable)
                && validation.get(**symbol).is_some_and(|row| row.passed)
        })
        .map(|symbol| (*symbol).to_string())
        .collect()
}

pub(crate) fn selection_replay_proof(
    trial: &TrialRecord,
    capture_path: &str,
) -> Result<CandidateReplayProof> {
    let retained = trial
        .retained_capture_proofs
        .iter()
        .find(|proof| proof.capture == capture_path)
        .with_context(|| format!("selection replay proof missing capture={capture_path}"))?;
    let canonical_session_stats = trial
        .canonical_session_stats
        .clone()
        .context("selection canonical proof missing")?;
    let portfolio_accounting = trial
        .portfolio_accounting
        .clone()
        .context("selection accounting proof missing")?;
    let accounting_coverage =
        canonical_session_stats.proof_for_eligible_markets(&portfolio_accounting);
    let pack = trial
        .retained_pack_scores
        .iter()
        .find(|pack| pack.capture == capture_path)
        .context("selection prediction evidence missing")?;
    Ok(CandidateReplayProof {
        selection_mode: trial.selection_mode,
        capture_path: capture_path.to_string(),
        candidate_id: trial.candidate_id.clone(),
        params_by_symbol: trial.params_by_symbol.clone(),
        canonical_session_stats,
        portfolio_accounting,
        initial_oracle_learning_sha256: retained.initial_oracle_learning_sha256.clone(),
        initial_oracle_learning_cutoffs_ms: retained.initial_oracle_learning_cutoffs_ms.clone(),
        final_oracle_learning_sha256: retained.final_oracle_learning_sha256.clone(),
        selection_score: trial.selection_score,
        prediction_metrics: pack.candidate_prediction_metrics.clone(),
        accounting_coverage,
    })
}

pub(crate) fn holdout_replay_proof(proof: &holdout::HoldoutReplayProof) -> CandidateReplayProof {
    CandidateReplayProof {
        selection_mode: proof.selection_mode,
        capture_path: proof.capture_path.clone(),
        candidate_id: proof.candidate_id.clone(),
        params_by_symbol: proof.params_by_symbol.clone(),
        canonical_session_stats: proof.canonical_session_stats.clone(),
        portfolio_accounting: proof.portfolio_accounting.clone(),
        initial_oracle_learning_sha256: proof.initial_oracle_learning_sha256.clone(),
        initial_oracle_learning_cutoffs_ms: proof.initial_oracle_learning_cutoffs_ms.clone(),
        final_oracle_learning_sha256: proof.final_oracle_learning_sha256.clone(),
        selection_score: proof.selection_score,
        prediction_metrics: proof.prediction_metrics.clone(),
        accounting_coverage: proof.accounting_coverage.clone(),
    }
}

pub(crate) fn selection_economics(
    incumbent: &CandidateReplayProof,
    candidate: &CandidateReplayProof,
) -> Result<BTreeMap<String, SelectionEconomicProof>> {
    ensure!(
        incumbent
            .params_by_symbol
            .keys()
            .eq(candidate.params_by_symbol.keys()),
        "economic proof symbol coverage differs"
    );
    let mut rows = BTreeMap::new();
    for symbol in candidate.params_by_symbol.keys() {
        let incumbent_pnl = receipt_realized_pnl(&incumbent.portfolio_accounting, symbol)?;
        let candidate_pnl = receipt_realized_pnl(&candidate.portfolio_accounting, symbol)?;
        rows.insert(
            (*symbol).to_string(),
            SelectionEconomicProof {
                symbol: (*symbol).to_string(),
                candidate_net_pnl_units_6dp: candidate_pnl,
                incumbent_net_pnl_units_6dp: incumbent_pnl,
                candidate_profitable: candidate_pnl > 0,
            },
        );
    }
    Ok(rows)
}

pub(crate) fn write_candidate_result(
    run_root: &Path,
    artifact: CandidateResultArtifact,
) -> Result<CandidateResultArtifact> {
    ensure!(
        artifact.schema_version == 19
            && artifact.frozen_capture_inputs.all().all(|input| {
                input.execution_timing.as_ref().is_some_and(|timing| {
                    timing
                        .clob
                        .as_ref()
                        .is_some_and(|source| !source.observations.is_empty())
                        && timing
                            .ctf
                            .as_ref()
                            .is_some_and(|source| !source.observations.is_empty())
                })
            }),
        "current joint candidate proof requires nonempty frozen CLOB/CTF timing"
    );
    let artifact = artifact.seal()?;
    artifact.validate()?;
    let path = run_root.join("candidate_result.json");
    polymarket_hft::runtime_files::write_bytes_atomic(
        &path,
        &serde_json::to_vec_pretty(&artifact)?,
    )?;
    Ok(artifact)
}

pub(crate) fn load_candidate_result(run_root: &Path) -> Result<CandidateResultArtifact> {
    let path = run_root.join("candidate_result.json");
    let artifact: CandidateResultArtifact = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )?;
    artifact.validate()?;
    Ok(artifact)
}

fn receipt_realized_pnl(
    accounting: &PortfolioAccountingProjectionProof,
    symbol: &str,
) -> Result<i128> {
    accounting
        .row_for_symbol(symbol)
        .with_context(|| format!("accounting row missing symbol={symbol}"))?
        .receipt_realized_pnl_units_6dp
        .with_context(|| format!("receipt-realized PnL missing symbol={symbol}"))
}

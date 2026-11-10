use super::*;
use polymarket_hft::trade_session_ledger::{CanonicalCoverageGap, CanonicalCoverageGapKind};

impl CandidateResultArtifact {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.schema_version, 18 | 19),
            "unsupported joint candidate proof schema"
        );
        let mut unsealed = self.clone();
        let identity = std::mem::take(&mut unsealed.artifact_identity);
        ensure!(
            identity == format!("{:x}", Sha256::digest(serde_json::to_vec(&unsealed)?)),
            "joint candidate proof identity mismatch"
        );
        ensure!(
            self.frozen_capture_inputs.sha256()? == self.frozen_capture_inputs_sha256,
            "joint candidate proof frozen capture digest mismatch"
        );
        ensure!(
            !self.trial_history_sha256.is_empty(),
            "joint candidate proof trial history digest is missing"
        );
        let symbols = self.frozen_capture_inputs.symbols()?;
        for proof in std::iter::once(&self.selection_incumbent)
            .chain(self.selection_candidate.iter())
            .chain(self.validation_incumbent.iter())
            .chain(self.validation_candidate.iter())
        {
            ensure!(
                proof.selection_mode == self.selection_mode,
                "candidate proof selection mode mismatch"
            );
        }
        ensure!(
            self.symbol_spec_sha256s.len() == VALID_ASSETS.len()
                && self.parameter_schema_sha256s.len() == VALID_ASSETS.len()
                && VALID_ASSETS
                    .iter()
                    .all(|symbol| self.symbol_spec_sha256s.contains_key(*symbol)
                        && self.parameter_schema_sha256s.contains_key(*symbol))
                && (self.schema_version != 18 || symbols.len() == VALID_ASSETS.len()),
            "candidate proof frozen configuration coverage is incomplete"
        );
        if self.schema_version == 19 {
            ensure!(
                self.search_trials_by_symbol.len() == symbols.len()
                    && symbols
                        .iter()
                        .all(|symbol| self.search_trials_by_symbol.contains_key(symbol))
                    && self.search_trials_by_symbol.values().sum::<usize>() == self.search_trials,
                "candidate proof per-symbol search counts are incomplete"
            );
            ensure!(
                self.symbol_search_artifact_sha256s.len() == symbols.len()
                    && symbols.iter().all(|symbol| self
                        .symbol_search_artifact_sha256s
                        .get(symbol)
                        .is_some_and(|sha256| !sha256.is_empty())),
                "candidate proof per-symbol search artifacts are incomplete"
            );
        }
        ensure!(
            self.frozen_capture_inputs.selection.len() == 1
                && self.frozen_capture_inputs.holdout.len() <= 1,
            "joint candidate proof capture roles are invalid"
        );
        let selection_input = self
            .frozen_capture_inputs
            .selection
            .first()
            .context("joint candidate proof selection input missing")?;
        validate_replay_proof(&self.selection_incumbent, selection_input)?;
        if let Some(candidate) = &self.selection_candidate {
            validate_replay_proof(candidate, selection_input)?;
        }
        ensure!(
            self.validation_incumbent.is_some() == self.validation_candidate.is_some(),
            "joint candidate proof validation pair is incomplete"
        );
        let holdout_input = self.frozen_capture_inputs.holdout.first();
        if let (Some(incumbent), Some(candidate)) =
            (&self.validation_incumbent, &self.validation_candidate)
        {
            let input = holdout_input
                .context("joint candidate proof contains validation without frozen B")?;
            validate_replay_proof(incumbent, input)?;
            validate_replay_proof(candidate, input)?;
            ensure!(
                incumbent.candidate_id == self.selection_incumbent.candidate_id
                    && incumbent.params_by_symbol == self.selection_incumbent.params_by_symbol,
                "joint candidate proof B incumbent differs from frozen A incumbent"
            );
            let selected = self
                .selection_candidate
                .as_ref()
                .context("joint candidate proof validates B without a selected A candidate")?;
            ensure!(
                candidate.candidate_id == selected.candidate_id
                    && candidate.params_by_symbol == selected.params_by_symbol,
                "joint candidate proof B candidate differs from frozen A candidate"
            );
            ensure!(
                validation_economics(incumbent, candidate)? == self.validation_economics_by_symbol,
                "joint candidate validation economics mismatch"
            );
        } else {
            ensure!(
                self.validation_economics_by_symbol.is_empty(),
                "joint candidate proof has B economics without paired B proofs"
            );
        }
        ensure!(
            holdout_input.is_some() || self.validation_error.is_none(),
            "candidate proof has a B validation error without frozen B"
        );
        let candidate = self
            .selection_candidate
            .as_ref()
            .unwrap_or(&self.selection_incumbent);
        ensure!(
            selection_economics(&self.selection_incumbent, candidate)?
                == self.selection_economics_by_symbol,
            "joint candidate selection economics mismatch"
        );
        let selection_pass = self.selection_economics_by_symbol.len() == VALID_ASSETS.len()
            && self
                .selection_economics_by_symbol
                .values()
                .all(|row| row.candidate_profitable);
        let validation_pass = self.validation_economics_by_symbol.len() == VALID_ASSETS.len()
            && self
                .validation_economics_by_symbol
                .values()
                .all(|row| row.passed);
        let computed_structural_pass = (self.schema_version == 18
            || self.validation_error.is_none())
            && self.selection_candidate.as_ref().is_some_and(|proof| {
                proof.accounting_coverage.is_complete()
                    && proof.selection_score.is_some_and(|score| {
                        scoring::selection_score(
                            self.selection_mode,
                            score,
                            &proof.prediction_metrics,
                            &self.selection_incumbent.prediction_metrics,
                        )
                        .is_ok()
                    })
            })
            && self
                .validation_incumbent
                .as_ref()
                .zip(self.validation_candidate.as_ref())
                .is_some_and(|(incumbent, candidate)| {
                    incumbent.accounting_coverage.is_complete()
                        && candidate.accounting_coverage.is_complete()
                        && incumbent.selection_score.is_some()
                        && candidate.selection_score.is_some_and(|score| {
                            scoring::selection_score(
                                self.selection_mode,
                                score,
                                &candidate.prediction_metrics,
                                &incumbent.prediction_metrics,
                            )
                            .is_ok()
                        })
                });
        ensure!(
            self.structural_pass == computed_structural_pass,
            "joint candidate structural gate mismatch"
        );
        ensure!(
            self.all_four_economic_pass == (selection_pass && validation_pass),
            "joint candidate all-four economic gate mismatch"
        );
        let eligible_symbols = if self.schema_version == 18 {
            VALID_ASSETS
                .iter()
                .map(|symbol| (*symbol).to_string())
                .collect()
        } else {
            self.eligible_symbols()
        };
        ensure!(
            !self.applied
                || (self.structural_pass
                    && (self.schema_version != 18 || self.all_four_economic_pass)
                    && self.selection_candidate.is_some()
                    && self.validation_incumbent.is_some()
                    && self.validation_candidate.is_some()
                    && self.validation_error.is_none()
                    && !eligible_symbols.is_empty()
                    && self
                        .selected_learning_bindings
                        .keys()
                        .eq(eligible_symbols.iter())
                    && self.application_reason == "applied"),
            "joint candidate application differs from the eligible symbol set"
        );
        Ok(())
    }
}

fn validate_replay_proof(
    proof: &CandidateReplayProof,
    input: &capture_inputs::FrozenCaptureInput,
) -> Result<()> {
    ensure!(
        proof
            .params_by_symbol
            .keys()
            .eq(input.prepared_proof_identities_by_symbol.keys()),
        "joint candidate replay proof differs from frozen participating symbols"
    );
    scoring::validate_prediction_evidence(
        proof.selection_mode,
        &proof.params_by_symbol.keys().cloned().collect::<Vec<_>>(),
        &proof.prediction_metrics,
    )?;
    let mut accounting_coverage = proof
        .canonical_session_stats
        .proof_for_eligible_markets(&proof.portfolio_accounting);
    if proof.accounting_coverage != accounting_coverage
        && proof
            .canonical_session_stats
            .entry_condition_ids_by_symbol()
            .values()
            .any(BTreeSet::is_empty)
        && !accounting_coverage
            .gaps
            .iter()
            .any(|gap| gap.kind == CanonicalCoverageGapKind::InvalidSymbolScope)
    {
        // Derive the old rejection without changing the sealed proof or its decision.
        accounting_coverage.gaps.insert(
            0,
            CanonicalCoverageGap {
                symbol: "*".to_string(),
                kind: CanonicalCoverageGapKind::InvalidSymbolScope,
            },
        );
    }
    ensure!(
        proof.accounting_coverage == accounting_coverage,
        "joint candidate replay accounting coverage mismatch"
    );
    ensure!(
        proof.capture_path == input.capture.display().to_string()
            && proof.initial_oracle_learning_sha256 == input.initial_oracle_learning_sha256
            && proof.initial_oracle_learning_cutoffs_ms == input.initial_oracle_learning_cutoffs_ms,
        "joint candidate replay proof differs from frozen capture or learner"
    );
    Ok(())
}

fn validation_economics(
    incumbent: &CandidateReplayProof,
    candidate: &CandidateReplayProof,
) -> Result<BTreeMap<String, holdout::SymbolEconomicResult>> {
    ensure!(
        incumbent
            .params_by_symbol
            .keys()
            .eq(candidate.params_by_symbol.keys()),
        "validation economic proof symbol coverage differs"
    );
    let mut rows = BTreeMap::new();
    for symbol in candidate.params_by_symbol.keys() {
        let incumbent_pnl = receipt_realized_pnl(&incumbent.portfolio_accounting, symbol)?;
        let candidate_pnl = receipt_realized_pnl(&candidate.portfolio_accounting, symbol)?;
        rows.insert(
            (*symbol).to_string(),
            holdout::symbol_economic_result(
                candidate.selection_mode,
                symbol,
                incumbent_pnl,
                candidate_pnl,
                incumbent.prediction_metrics.get(symbol),
                candidate.prediction_metrics.get(symbol),
            )?,
        );
    }
    Ok(rows)
}

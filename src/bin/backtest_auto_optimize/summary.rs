use super::*;
use polymarket_hft::optimizer_report::{
    OptimizerCandidateReport, OptimizerCaptureLearnerReport, OptimizerCaptureReport,
    OptimizerObjectiveProfileReport, OptimizerObjectiveScoreReport, OptimizerRunProvenanceReport,
    OptimizerSelectedLearningBindingReport, OptimizerStatusArtifact, OptimizerValidationReport,
};
use polymarket_hft::report_markdown::write_optimizer_status_report;
use std::fs::OpenOptions;

#[path = "summary/verification.rs"]
mod verification;

pub(crate) use verification::{
    verify_capture_report, verify_native_summary_core, verify_structural_summary,
};

#[derive(Serialize)]
struct PersistedTrial<'a> {
    #[serde(skip_serializing_if = "SelectionMode::is_profit")]
    selection_mode: SelectionMode,
    index: usize,
    candidate_id: &'a str,
    elapsed_ms: u128,
    params_by_symbol: &'a BTreeMap<String, BTreeMap<String, ParamValue>>,
    pnl_by_symbol: Option<&'a BTreeMap<String, PortfolioAccountingPnlRow>>,
    entry_count: Option<u64>,
    profitable_market_count: Option<u64>,
    selection_score: Option<scoring::FeasibilityScore>,
    retained_pack_scores: &'a [RetainedPackScore],
    origin: TrialOrigin,
    origin_id: Option<&'a str>,
    complete_coverage: bool,
    feasible: bool,
    accounting_coverage: Option<&'a polymarket_hft::trade_session_ledger::CanonicalCoverageProof>,
}

pub(crate) fn publish_progress(message: std::fmt::Arguments<'_>) {
    let result = {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{message}").and_then(|()| stdout.flush())
    };
    if let Err(error) = result {
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(
            stderr,
            "CMAES_AUTO_OPTIMIZE progress_output_failed reason={error}"
        )
        .and_then(|()| stderr.flush());
    }
}

pub(super) fn append_trial(ctx: &RunContext, record: TrialRecord) -> Result<()> {
    ensure!(
        record.selection_mode == ctx.selection_mode,
        "trial selection mode differs from frozen context"
    );
    let path = ctx.run_root.join("trials.jsonl");
    let persisted = PersistedTrial {
        selection_mode: record.selection_mode,
        index: record.index,
        candidate_id: &record.candidate_id,
        elapsed_ms: record.elapsed_ms,
        params_by_symbol: &record.params_by_symbol,
        pnl_by_symbol: record
            .requested_metrics
            .as_ref()
            .map(|metrics| &metrics.pnl_by_symbol),
        entry_count: record
            .retained_pack_scores
            .first()
            .and_then(|pack| pack.candidate_entry_count),
        profitable_market_count: record
            .retained_pack_scores
            .first()
            .and_then(|pack| pack.candidate_profitable_market_count),
        selection_score: record.selection_score,
        retained_pack_scores: &record.retained_pack_scores,
        origin: record.origin,
        origin_id: record.origin_id.as_deref(),
        complete_coverage: record.complete_coverage(),
        feasible: record.is_feasible(),
        accounting_coverage: record
            .requested_metrics
            .as_ref()
            .map(|metrics| &metrics.accounting_coverage),
    };
    let line = serde_json::to_string(&persisted)? + "\n";
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?
        .write_all(line.as_bytes())?;

    publish_progress(format_args!(
        "TRIAL {:05} origin={:?} complete_coverage={} feasible={} pnl_by_symbol={:?} selection_score={:?} candidate_id={} params={} selection_mode={}",
        record.index,
        record.origin,
        record.complete_coverage(),
        record.is_feasible(),
        record
            .requested_metrics
            .as_ref()
            .map(|metrics| &metrics.pnl_by_symbol),
        record.selection_score,
        record.candidate_id,
        serde_json::to_string(&record.params_by_symbol)?,
        record.selection_mode.as_str()
    ));
    ctx.trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?
        .push(record);
    Ok(())
}

pub(super) fn write_optimizer_status(ctx: &RunContext, phase: &str) -> Result<()> {
    write_status(ctx, phase, None)
}

pub(super) fn write_optimizer_selected_status(
    ctx: &RunContext,
    phase: &str,
    selected_recheck_index: usize,
) -> Result<()> {
    write_status(ctx, phase, Some(selected_recheck_index))
}

fn write_status(ctx: &RunContext, phase: &str, selected_index: Option<usize>) -> Result<()> {
    let status = match build_status(ctx, phase, selected_index) {
        Ok(status) => status,
        Err(error) => {
            let _ = writeln!(
                std::io::stderr().lock(),
                "CMAES_AUTO_OPTIMIZE status_build_failed root={} phase={phase} reason={error:#}",
                ctx.run_root.display()
            );
            return Ok(());
        }
    };
    let status_path = ctx.run_root.join("optimizer_status.json");
    let markdown_path = ctx.run_root.join("optimizer_status.md");
    for (path, result) in [
        (
            &status_path,
            serde_json::to_vec_pretty(&status)
                .context("serialize optimizer status JSON")
                .and_then(|bytes| {
                    fs::write(&status_path, bytes).context("write optimizer status JSON")
                }),
        ),
        (
            &markdown_path,
            write_optimizer_status_report(&markdown_path, &status),
        ),
    ] {
        if let Err(error) = result {
            let _ = writeln!(
                std::io::stderr().lock(),
                "CMAES_AUTO_OPTIMIZE status_output_failed path={} phase={phase} reason={error:#}",
                path.display()
            );
        }
    }
    Ok(())
}

fn build_status(
    ctx: &RunContext,
    phase: &str,
    selected_index: Option<usize>,
) -> Result<OptimizerStatusArtifact> {
    let trials = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?;
    let incumbent = trials
        .iter()
        .find(|trial| trial.origin == TrialOrigin::Default && trial.requested_metrics.is_some())
        .map(|trial| candidate_report(ctx, trial))
        .transpose()?;
    let selected = selected_index
        .map(|index| {
            candidate_report(
                ctx,
                trials
                    .iter()
                    .find(|trial| trial.index == index)
                    .context("selected status recheck missing")?,
            )
        })
        .transpose()?;
    let frozen_defaults = ctx
        .optimizer_spec_path
        .parent()
        .context("frozen optimizer spec has no defaults directory")?;
    let selected_learning_bindings = BTreeMap::new();
    Ok(OptimizerStatusArtifact {
        selection_mode: ctx.selection_mode.as_str().to_string(),
        schema_version: 19,
        objective_profile: objective_profile_report(&ctx.objective_profile),
        provenance: Some(provenance_report(
            ctx.selection_mode,
            &ctx.selection_inputs,
            &ctx.holdout_inputs,
            &candidate_proof::file_sha256(&frozen_defaults.join("config.main.toml"))?,
            &candidate_proof::file_sha256(&ctx.optimizer_spec_path)?,
            None,
            None,
            Some(&selected_learning_bindings),
        )?),
        phase: phase.to_string(),
        run_root: ctx.run_root.display().to_string(),
        captures: capture_reports(ctx),
        symbol_searches: Vec::new(),
        capture_replays: Vec::new(),
        search_trials: trials
            .iter()
            .filter(|trial| trial.origin == TrialOrigin::Search)
            .count(),
        exercised_trials: trials.len(),
        selection_status: format!("single_symbol_{phase}"),
        validation_status: "not_run".to_string(),
        structural_status: "not_run".to_string(),
        economic_status: "not_run".to_string(),
        application_status: "not_run".to_string(),
        application_reason: if phase == "archive" {
            "single_symbol_archive_evaluation_in_progress"
        } else {
            "single_symbol_search_in_progress"
        }
        .to_string(),
        incumbent_candidate: incumbent,
        selected_candidate: selected.clone(),
        rechecked_candidate: selected,
        validation: None,
        applied: false,
    })
}

pub(crate) fn candidate_report(
    ctx: &RunContext,
    trial: &TrialRecord,
) -> Result<OptimizerCandidateReport> {
    let metrics = trial
        .requested_metrics
        .as_ref()
        .context("reported joint candidate has no A metrics")?;
    ensure!(
        trial.selection_mode == ctx.selection_mode,
        "reported candidate selection mode differs from frozen context"
    );
    let pack = trial
        .retained_pack_scores
        .first()
        .context("reported candidate prediction evidence missing")?;
    let score = if trial.selection_mode.is_profit() {
        trial.selection_score
    } else {
        pack.candidate_selection_score
    };
    let (canonical_ctf_exit_summary, canonical_ctf_exit_cohorts) = trial
        .canonical_session_stats
        .as_ref()
        .map(|stats| {
            (
                stats.canonical.ctf_exit_summary.clone(),
                stats.canonical.ctf_exit_cohorts.clone(),
            )
        })
        .unwrap_or_default();
    let entry_count = trial
        .retained_pack_scores
        .first()
        .and_then(|pack| pack.candidate_entry_count)
        .context("reported joint candidate objective entry count missing")?;
    let profitable_market_count = trial
        .retained_pack_scores
        .first()
        .and_then(|pack| pack.candidate_profitable_market_count)
        .context("reported joint candidate profitable market count missing")?;
    let all_symbols_profitable = ctx.symbols.iter().all(|symbol| {
        metrics
            .pnl_by_symbol
            .get(symbol)
            .and_then(|row| row.receipt_realized_pnl_units_6dp)
            .is_some_and(|pnl| pnl > 0)
    });
    let recorded_market_coverage = ctx
        .selection_inputs
        .first()
        .and_then(|input| input.recorded_market_population.as_ref())
        .map(|population| {
            population.project_entered(
                canonical_ctf_exit_cohorts
                    .iter()
                    .map(|row| (row.symbol.as_str(), row.market_id.as_str())),
            )
        });
    Ok(OptimizerCandidateReport {
        selection_mode: trial.selection_mode.as_str().to_string(),
        prediction_metrics: pack.candidate_prediction_metrics.clone(),
        candidate_id: trial.candidate_id.clone(),
        origin: format!("{:?}", trial.origin).to_ascii_lowercase(),
        origin_id: trial.origin_id.clone(),
        params_by_symbol: trial.params_by_symbol.clone(),
        exercised: true,
        pnl_by_symbol: metrics.pnl_by_symbol.clone(),
        objective_score: score.map(|score| OptimizerObjectiveScoreReport {
            numerator: score.objective_score.numerator,
            denominator: score.objective_score.denominator,
        }),
        shortfall_units_6dp: score.map_or(u128::MAX, |score| score.shortfall_units_6dp),
        all_symbols_profitable,
        entry_count,
        profitable_market_count,
        split_entries: metrics.split_entries,
        submitted_orders: metrics.submitted_orders,
        managed_sell_fills: metrics.managed_sell_fills,
        wrong_side_first_managed_sell: metrics.wrong_side_first_sells,
        evaluation_scope: ctx.evaluation_scope().to_string(),
        accounting_coverage: Some(metrics.accounting_coverage.clone()),
        canonical_ctf_exit_summary,
        canonical_ctf_exit_cohorts,
        recorded_market_coverage,
    })
}

pub(crate) fn objective_profile_report(
    profile: &ObjectiveProfile,
) -> OptimizerObjectiveProfileReport {
    OptimizerObjectiveProfileReport {
        formula_type: profile.formula_type().to_string(),
        expression: profile.expression().to_string(),
        sha256: profile.sha256().to_string(),
    }
}

pub(crate) fn capture_reports(ctx: &RunContext) -> Vec<OptimizerCaptureReport> {
    ctx.selection_inputs
        .iter()
        .map(|input| capture_report(input, "selection"))
        .chain(
            ctx.holdout_inputs
                .iter()
                .map(|input| capture_report(input, "validation")),
        )
        .collect()
}

pub(crate) fn capture_report(
    input: &capture_inputs::FrozenCaptureInput,
    role: &str,
) -> OptimizerCaptureReport {
    OptimizerCaptureReport {
        capture: input.capture.display().to_string(),
        role: role.to_string(),
        session_id: input.session_id.clone(),
        run_id: input.run_id.clone(),
        duration_ms: 0,
        capture_duration_ms: Some(input.capture_end_ms.saturating_sub(input.capture_start_ms)),
        replay_duration_ms: Some(input.replay_end_ms.saturating_sub(input.replay_start_ms)),
        recorded_market_population: input.recorded_market_population.clone(),
    }
}

pub(crate) fn provenance_report(
    mode: SelectionMode,
    selection_inputs: &[capture_inputs::FrozenCaptureInput],
    holdout_inputs: &[capture_inputs::FrozenCaptureInput],
    app_config_sha256: &str,
    root_spec_sha256: &str,
    selection_final_oracle_learning_sha256: Option<&str>,
    validation_final_oracle_learning_sha256: Option<&str>,
    selected_learning_bindings: Option<
        &BTreeMap<String, candidate_proof::SelectedLearningBindingProof>,
    >,
) -> Result<OptimizerRunProvenanceReport> {
    let inputs = selection_inputs.iter().chain(holdout_inputs);
    let optimizer_binary_sha256s = inputs
        .clone()
        .flat_map(|input| input.prepared_proof_identities_by_symbol.values())
        .map(candidate_proof::SymbolChildProofIdentity::optimizer_binary_sha256)
        .collect::<BTreeSet<_>>();
    ensure!(
        optimizer_binary_sha256s.len() == 1,
        "optimizer report requires one sealed binary identity"
    );
    let optimizer_binary_sha256 = optimizer_binary_sha256s
        .into_iter()
        .next()
        .context("optimizer report binary identity missing")?
        .to_string();
    let capture_learners = selection_inputs
        .iter()
        .map(|input| (input, "selection"))
        .chain(holdout_inputs.iter().map(|input| (input, "validation")))
        .map(|(input, role)| OptimizerCaptureLearnerReport {
            role: role.to_string(),
            capture: input.capture.display().to_string(),
            initial_oracle_learning_sha256: input.initial_oracle_learning_sha256.clone(),
            initial_oracle_learning_sha256_by_symbol: input
                .initial_oracle_learning_sha256_by_symbol
                .clone(),
            initial_oracle_learning_cutoffs_ms: input.initial_oracle_learning_cutoffs_ms.clone(),
        })
        .collect();
    Ok(OptimizerRunProvenanceReport {
        selection_mode: mode.as_str().to_string(),
        optimizer_binary_sha256,
        app_config_sha256: app_config_sha256.to_string(),
        root_spec_sha256: root_spec_sha256.to_string(),
        selection_final_oracle_learning_sha256: selection_final_oracle_learning_sha256
            .map(str::to_string),
        validation_final_oracle_learning_sha256: validation_final_oracle_learning_sha256
            .map(str::to_string),
        selected_learning_bindings: selected_learning_bindings
            .map(selected_learning_binding_reports),
        capture_learners,
    })
}

pub(crate) fn selected_learning_binding_reports(
    bindings: &BTreeMap<String, candidate_proof::SelectedLearningBindingProof>,
) -> BTreeMap<String, OptimizerSelectedLearningBindingReport> {
    bindings
        .iter()
        .map(|(symbol, binding)| {
            (
                symbol.clone(),
                OptimizerSelectedLearningBindingReport {
                    path: binding.path.clone(),
                    sha256: binding.sha256.clone(),
                },
            )
        })
        .collect()
}

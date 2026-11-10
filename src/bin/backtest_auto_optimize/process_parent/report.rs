use super::*;
use polymarket_hft::optimizer_report::{
    OptimizerCandidateReport, OptimizerCaptureReplayReport, OptimizerStatusArtifact,
    OptimizerStructuralReport, OptimizerSummaryArtifact, OptimizerSymbolEconomicReport,
    OptimizerSymbolSearchReport, OptimizerValidationReport,
};
use polymarket_hft::report_markdown::{
    write_optimizer_status_report, write_optimizer_summary_report,
};

#[path = "report/validation.rs"]
mod validation;

use validation::{structural_report, validation_report};

pub(super) use crate::summary::verify_structural_summary;

pub(super) fn write_parent_status(
    ctx: &RunContext,
    frozen: &capture_inputs::FrozenCaptureInputs,
    searches: &[OptimizerSymbolSearchReport],
    children: &BTreeMap<String, replay_proof::ValidatedSymbolSearch>,
    selection: Option<&holdout::HoldoutResult>,
    phase: &str,
) -> Result<()> {
    if let Some(status) = optional_output(
        &ctx.run_root,
        build_parent_status(ctx, frozen, searches, children, selection, phase),
    ) {
        write_status_files(ctx, &status);
    }
    Ok(())
}

fn build_parent_status(
    ctx: &RunContext,
    frozen: &capture_inputs::FrozenCaptureInputs,
    searches: &[OptimizerSymbolSearchReport],
    children: &BTreeMap<String, replay_proof::ValidatedSymbolSearch>,
    selection: Option<&holdout::HoldoutResult>,
    phase: &str,
) -> Result<OptimizerStatusArtifact> {
    let search_trials = searches
        .iter()
        .map(|search| search.search_evaluations)
        .sum();
    let exercised_trials = searches.iter().map(|search| search.exercised_trials).sum();
    let selected_learning_bindings = BTreeMap::new();
    let mut capture_replays = Vec::with_capacity(children.len() * 2);
    for child in children.values() {
        let input = child
            .artifact
            .frozen_capture_inputs
            .selection
            .first()
            .context("child status selection input missing")?;
        for (origin, proof) in [
            ("retained_child_default", &child.artifact.default_candidate),
            (
                "retained_child_selected_recheck",
                &child.artifact.selected_candidate,
            ),
        ] {
            capture_replays.push(OptimizerCaptureReplayReport {
                capture: summary::capture_report(input, "selection"),
                candidate: Some(sealed_candidate_report(
                    ctx,
                    &proof.candidate_id,
                    &proof.params_by_symbol,
                    &proof.canonical_session_stats,
                    &proof.portfolio_accounting,
                    proof.selection_score,
                    &proof.prediction_metrics,
                    &proof.accounting_coverage,
                    Some(input),
                    origin,
                    "single_capture_single_symbol_selection",
                )?),
                structural_validation_status: "not_run".to_string(),
                structural_validation_issues: Vec::new(),
                error: None,
            });
        }
    }
    let incumbent_candidate = selection
        .map(|result| {
            replay_candidate_report(
                ctx,
                &result.incumbent,
                "frozen_default_integration",
                "single_capture_symbol_integration",
            )
        })
        .transpose()?;
    let selected_candidate = selection
        .map(|result| {
            replay_candidate_report(
                ctx,
                &result.candidate,
                "selected_symbol_maps",
                "single_capture_symbol_integration",
            )
        })
        .transpose()?;
    let status = OptimizerStatusArtifact {
        selection_mode: ctx.selection_mode.as_str().to_string(),
        schema_version: 19,
        objective_profile: summary::objective_profile_report(&ctx.objective_profile),
        provenance: Some(summary::provenance_report(
            ctx.selection_mode,
            &frozen.selection,
            &frozen.holdout,
            &candidate_proof::file_sha256(&ctx.run_root.join("defaults/config.main.toml"))?,
            &candidate_proof::file_sha256(&ctx.optimizer_spec_path)?,
            None,
            None,
            Some(&selected_learning_bindings),
        )?),
        phase: phase.to_string(),
        run_root: ctx.run_root.display().to_string(),
        captures: summary::capture_reports(ctx),
        symbol_searches: searches.to_vec(),
        capture_replays,
        search_trials,
        exercised_trials,
        selection_status: if searches.len() == ctx.symbols.len() {
            "symbol_searches_complete"
        } else {
            "symbol_searches_in_progress"
        }
        .to_string(),
        validation_status: "not_run".to_string(),
        structural_status: "not_run".to_string(),
        economic_status: "not_run".to_string(),
        application_status: "not_run".to_string(),
        application_reason: format!("application_not_run_phase_{phase}"),
        incumbent_candidate,
        rechecked_candidate: selected_candidate.clone(),
        selected_candidate,
        validation: None,
        applied: false,
    };
    Ok(status)
}

pub(super) fn write_parent_summary(
    ctx: &RunContext,
    frozen: &capture_inputs::FrozenCaptureInputs,
    searches: &[OptimizerSymbolSearchReport],
    selection: &holdout::HoldoutResult,
    holdout: Option<&holdout::HoldoutResult>,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<()> {
    let reports = optional_output(
        &ctx.run_root,
        build_parent_summary(ctx, frozen, searches, selection, holdout, artifact),
    );
    if let Some((summary, status)) = &reports {
        write_status_files(ctx, status);
        let matrix_root = ctx.primary_capture().join("backtest_reports/matrix");
        let _ = optional_output(
            &matrix_root,
            fs::create_dir_all(&matrix_root).map_err(Into::into),
        );
        for (json, markdown) in [
            (
                ctx.run_root.join("summary.json"),
                ctx.run_root.join("summary.md"),
            ),
            (
                matrix_root.join("optimizer_summary.json"),
                matrix_root.join("optimizer_summary.md"),
            ),
        ] {
            write_json(&json, summary);
            let _ = optional_output(
                &markdown,
                write_optimizer_summary_report(&markdown, summary),
            );
        }
        let _ = optional_output(
            &ctx.run_root,
            writeln!(
                std::io::stdout().lock(),
                "SUMMARY publication_attempted root={}",
                ctx.run_root.display()
            )
            .map_err(Into::into),
        );
    }
    let _ = optional_output(
        &ctx.run_root,
        archive_run(ctx, reports.as_ref().map(|(summary, _)| summary)),
    );
    Ok(())
}

fn build_parent_summary(
    ctx: &RunContext,
    frozen: &capture_inputs::FrozenCaptureInputs,
    searches: &[OptimizerSymbolSearchReport],
    selection: &holdout::HoldoutResult,
    holdout: Option<&holdout::HoldoutResult>,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<(OptimizerSummaryArtifact, OptimizerStatusArtifact)> {
    artifact.validate()?;
    ensure!(
        artifact.selection_mode == ctx.selection_mode,
        "report selection mode differs from frozen proof"
    );
    ensure!(
        searches.len() == ctx.symbols.len()
            && ctx
                .symbols
                .iter()
                .zip(searches)
                .all(|(expected, actual)| *expected == actual.symbol),
        "native report symbol-search order or coverage mismatch"
    );
    let incumbent_candidate = Some(replay_candidate_report(
        ctx,
        &selection.incumbent,
        "frozen_default_integration",
        "single_capture_symbol_integration",
    )?);
    let selected_candidate = Some(replay_candidate_report(
        ctx,
        &selection.candidate,
        "selected_symbol_maps",
        "single_capture_symbol_integration",
    )?);
    let rechecked_candidate = selected_candidate.clone();
    let validation = validation_report(ctx, artifact, holdout)?;
    let structural_recheck = structural_report(
        ctx,
        artifact,
        selection,
        holdout,
        rechecked_candidate.clone(),
    )?;
    let validation_status = validation.status.clone();
    let structural_status = if frozen.holdout.is_empty() {
        "validation_unavailable"
    } else if artifact.structural_pass {
        "structural_pass"
    } else {
        "structural_failed"
    }
    .to_string();
    let economic_status = validation.economic_status();
    let status = if artifact.application_reason == "apply_failed" {
        "failed"
    } else {
        "complete"
    }
    .to_string();
    let provenance = summary::provenance_report(
        ctx.selection_mode,
        &ctx.selection_inputs,
        &ctx.holdout_inputs,
        &artifact.app_config_sha256,
        &artifact.root_spec_sha256,
        Some(
            &artifact
                .selection_candidate
                .as_ref()
                .context("final selection candidate proof missing")?
                .final_oracle_learning_sha256,
        ),
        artifact
            .validation_candidate
            .as_ref()
            .map(|proof| proof.final_oracle_learning_sha256.as_str()),
        Some(&artifact.selected_learning_bindings),
    )?;
    let summary = OptimizerSummaryArtifact {
        selection_mode: ctx.selection_mode.as_str().to_string(),
        schema_version: 19,
        objective_profile: summary::objective_profile_report(&ctx.objective_profile),
        provenance: Some(provenance.clone()),
        status,
        run_root: ctx.run_root.display().to_string(),
        captures: summary::capture_reports(ctx),
        symbol_searches: searches.to_vec(),
        search_trials: artifact.search_trials,
        exercised_trials: artifact.exercised_trials,
        selection_status: "symbol_maps_frozen_and_integrated".to_string(),
        validation_status: validation_status.clone(),
        structural_status: structural_status.clone(),
        economic_status: economic_status.clone(),
        application_status: if artifact.applied {
            "applied"
        } else {
            "not_applied"
        }
        .to_string(),
        application_reason: artifact.application_reason.clone(),
        incumbent_candidate: incumbent_candidate.clone(),
        selected_candidate: selected_candidate.clone(),
        rechecked_candidate: rechecked_candidate.clone(),
        validation: Some(validation.clone()),
        structural_recheck: Some(structural_recheck),
        candidate_result_path: ctx
            .run_root
            .join("candidate_result.json")
            .display()
            .to_string(),
        applied: artifact.applied,
    };
    let status_artifact = OptimizerStatusArtifact {
        selection_mode: ctx.selection_mode.as_str().to_string(),
        schema_version: summary.schema_version,
        objective_profile: summary.objective_profile.clone(),
        provenance: Some(provenance),
        phase: summary.status.clone(),
        run_root: summary.run_root.clone(),
        captures: summary.captures.clone(),
        symbol_searches: summary.symbol_searches.clone(),
        capture_replays: Vec::new(),
        search_trials: summary.search_trials,
        exercised_trials: summary.exercised_trials,
        selection_status: summary.selection_status.clone(),
        validation_status,
        structural_status,
        economic_status,
        application_status: summary.application_status.clone(),
        application_reason: summary.application_reason.clone(),
        incumbent_candidate,
        selected_candidate,
        rechecked_candidate,
        validation: Some(validation),
        applied: summary.applied,
    };

    Ok((summary, status_artifact))
}

fn write_status_files(ctx: &RunContext, status: &OptimizerStatusArtifact) {
    write_json(&ctx.run_root.join("optimizer_status.json"), status);
    let path = ctx.run_root.join("optimizer_status.md");
    let _ = optional_output(&path, write_optimizer_status_report(&path, status));
}

fn write_json(path: &Path, value: &impl Serialize) {
    let _ = optional_output(
        path,
        serde_json::to_vec_pretty(value)
            .context("serialize optimizer report")
            .and_then(|bytes| polymarket_hft::runtime_files::write_bytes_atomic(path, &bytes)),
    );
}

fn optional_output<T>(path: &Path, result: Result<T>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            let _ = writeln!(
                std::io::stderr().lock(),
                "CMAES_AUTO_OPTIMIZE report_unavailable path={} reason={error:#}",
                path.display()
            );
            None
        }
    }
}

fn replay_candidate_report(
    ctx: &RunContext,
    proof: &holdout::HoldoutReplayProof,
    origin: &str,
    evaluation_scope: &str,
) -> Result<OptimizerCandidateReport> {
    let input = ctx
        .selection_inputs
        .iter()
        .chain(&ctx.holdout_inputs)
        .find(|input| input.capture == Path::new(&proof.capture_path));
    sealed_candidate_report(
        ctx,
        &proof.candidate_id,
        &proof.params_by_symbol,
        &proof.canonical_session_stats,
        &proof.portfolio_accounting,
        proof.selection_score,
        &proof.prediction_metrics,
        &proof.accounting_coverage,
        input,
        origin,
        evaluation_scope,
    )
}

#[allow(clippy::too_many_arguments)]
fn sealed_candidate_report(
    ctx: &RunContext,
    candidate_id: &str,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    canonical_session_stats: &CanonicalSessionStats,
    portfolio_accounting: &PortfolioAccountingProjectionProof,
    score: Option<scoring::FeasibilityScore>,
    prediction_metrics: &BTreeMap<
        String,
        polymarket_hft::strategy_runtime::prediction_metrics::PredictionMetrics,
    >,
    accounting_coverage: &polymarket_hft::trade_session_ledger::CanonicalCoverageProof,
    input: Option<&capture_inputs::FrozenCaptureInput>,
    origin: &str,
    evaluation_scope: &str,
) -> Result<OptimizerCandidateReport> {
    let symbols = params_by_symbol.keys().cloned().collect::<Vec<_>>();
    scoring::validate_prediction_evidence(ctx.selection_mode, &symbols, prediction_metrics)?;
    let endpoint = endpoint_joint_score(
        &ctx.objective_profile,
        &symbols,
        canonical_session_stats,
        portfolio_accounting,
    )?;
    let total = canonical_session_stats.canonical.total_summary();
    let recorded_market_coverage = input
        .and_then(|input| input.recorded_market_population.as_ref())
        .map(|population| {
            population.project_entered(
                canonical_session_stats
                    .canonical
                    .ctf_exit_cohorts
                    .iter()
                    .map(|row| (row.symbol.as_str(), row.market_id.as_str())),
            )
        });
    Ok(OptimizerCandidateReport {
        selection_mode: ctx.selection_mode.as_str().to_string(),
        prediction_metrics: prediction_metrics.clone(),
        candidate_id: candidate_id.to_string(),
        origin: origin.to_string(),
        origin_id: None,
        params_by_symbol: params_by_symbol.clone(),
        exercised: true,
        pnl_by_symbol: endpoint.pnl_by_symbol,
        objective_score: score.map(|score| {
            polymarket_hft::optimizer_report::OptimizerObjectiveScoreReport {
                numerator: score.objective_score.numerator,
                denominator: score.objective_score.denominator,
            }
        }),
        shortfall_units_6dp: score.map_or(u128::MAX, |score| score.shortfall_units_6dp),
        all_symbols_profitable: symbols.iter().all(|symbol| {
            portfolio_accounting
                .row_for_symbol(symbol)
                .and_then(|row| row.receipt_realized_pnl_units_6dp)
                .is_some_and(|pnl| pnl > 0)
        }),
        entry_count: endpoint.entry_count,
        profitable_market_count: endpoint.profitable_market_count,
        split_entries: total.split_entries,
        submitted_orders: total.submitted_orders,
        managed_sell_fills: total.confirmed_managed_sell_fill_count,
        wrong_side_first_managed_sell: canonical_session_stats
            .wrong_side_first_sells
            .unwrap_or_default(),
        evaluation_scope: evaluation_scope.to_string(),
        accounting_coverage: Some(accounting_coverage.clone()),
        canonical_ctf_exit_summary: canonical_session_stats.canonical.ctf_exit_summary.clone(),
        canonical_ctf_exit_cohorts: canonical_session_stats.canonical.ctf_exit_cohorts.clone(),
        recorded_market_coverage,
    })
}

fn archive_run(ctx: &RunContext, summary: Option<&OptimizerSummaryArtifact>) -> Result<()> {
    let capture_id = ctx
        .primary_capture()
        .file_name()
        .context("optimizer capture identity missing")?;
    let run_id = ctx
        .run_root
        .file_name()
        .context("optimizer run identity missing")?;
    let archive_root = Path::new("data/backtest_reports/optimizer_archive")
        .join(capture_id)
        .join(run_id);
    let _ = optional_output(
        &archive_root,
        fs::create_dir_all(&archive_root).map_err(Into::into),
    );
    if let Some(summary) = summary {
        let path = archive_root.join("summary.json");
        let _ = optional_output(
            &path,
            serde_json::to_vec_pretty(summary)
                .context("serialize archived optimizer summary")
                .and_then(|bytes| write_archive_member(&path, bytes.as_slice())),
        );
        let path = archive_root.join("summary.md");
        let rendered = polymarket_hft::report_markdown::render_optimizer_summary_report(summary);
        let _ = optional_output(&path, write_archive_member(&path, rendered.as_bytes()));
    }
    let destination = archive_root.join("candidate_result.json");
    let _ = optional_output(
        &destination,
        fs::File::open(ctx.run_root.join("candidate_result.json"))
            .map_err(Into::into)
            .and_then(|source| write_archive_member(&destination, source)),
    );
    for symbol in &ctx.symbols {
        let source = ctx.run_root.join("children").join(symbol);
        let destination = archive_root.join("children").join(symbol);
        let _ = optional_output(
            &destination,
            fs::create_dir_all(&destination).map_err(Into::into),
        );
        for name in ["child_result.json", "trials.jsonl"] {
            let target = destination.join(name);
            let _ = optional_output(
                &target,
                fs::File::open(source.join(name))
                    .map_err(Into::into)
                    .and_then(|source| write_archive_member(&target, source)),
            );
        }
    }
    Ok(())
}

fn write_archive_member(path: &Path, mut source: impl std::io::Read) -> Result<()> {
    let mut destination = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("create archive member {}", path.display()))
        }
    };
    std::io::copy(&mut source, &mut destination)
        .with_context(|| format!("write archive member {}", path.display()))?;
    Ok(())
}

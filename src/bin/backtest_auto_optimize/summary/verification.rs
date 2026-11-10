use super::*;

pub(crate) fn verify_native_summary_core(
    report: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
    objective_profile: &ObjectiveProfile,
) -> Result<()> {
    ensure!(
        report.selection_mode == artifact.selection_mode.as_str()
            && report
                .symbol_searches
                .iter()
                .all(|search| search.selection_mode == artifact.selection_mode.as_str())
            && report
                .provenance
                .as_ref()
                .map_or(artifact.selection_mode.is_profit(), |provenance| {
                    provenance.selection_mode == artifact.selection_mode.as_str()
                }),
        "retained report selection mode differs from sealed proof"
    );
    verify_statuses(report, artifact)?;
    verify_captures(&report.captures, &artifact.frozen_capture_inputs)?;
    verify_candidate_report(
        report.incumbent_candidate.as_ref(),
        Some(&artifact.selection_incumbent),
        objective_profile,
        "single_capture_four_symbol_integration",
        "selection incumbent",
    )?;
    verify_candidate_report(
        report.rechecked_candidate.as_ref(),
        artifact.selection_candidate.as_ref(),
        objective_profile,
        "single_capture_four_symbol_integration",
        "selection candidate",
    )?;
    ensure!(
        report.selected_candidate.is_some() == artifact.selection_candidate.is_some()
            && report.rechecked_candidate.is_some() == artifact.selection_candidate.is_some(),
        "retained integrated candidate presence differs from sealed proof"
    );
    if let Some(selected) = &report.selected_candidate {
        let proof = artifact
            .selection_candidate
            .as_ref()
            .context("sealed selected candidate proof missing")?;
        ensure!(
            selected.selection_mode == proof.selection_mode.as_str()
                && selected.prediction_metrics == proof.prediction_metrics
                && selected.candidate_id == proof.candidate_id
                && selected.params_by_symbol == proof.params_by_symbol,
            "retained selected candidate identity differs from sealed proof"
        );
    }
    let validation = report
        .validation
        .as_ref()
        .context("retained optimizer summary validation section missing")?;
    verify_candidate_report(
        validation.incumbent.as_ref(),
        artifact.validation_incumbent.as_ref(),
        objective_profile,
        "single_capture_four_symbol_validation",
        "validation incumbent",
    )?;
    verify_candidate_report(
        validation.candidate.as_ref(),
        artifact.validation_candidate.as_ref(),
        objective_profile,
        "single_capture_four_symbol_validation",
        "validation candidate",
    )?;
    ensure!(
        validation.all_symbols_pass
            == artifact
                .validation_candidate
                .as_ref()
                .map(|_| artifact.all_four_economic_pass)
            && validation.error == artifact.validation_error,
        "retained validation conclusion differs from sealed proof"
    );
    match (
        validation.capture.as_ref(),
        artifact.frozen_capture_inputs.holdout.first(),
    ) {
        (Some(capture), Some(input)) => verify_capture_report(capture, input, "validation")?,
        (None, None) => {}
        _ => bail!("retained validation capture presence differs from sealed proof"),
    }
    verify_economic_table(validation, artifact)
}

fn verify_statuses(
    report: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<()> {
    let selected = artifact.selection_candidate.is_some();
    let validation = !artifact.frozen_capture_inputs.holdout.is_empty();
    let selection_status = if selected {
        "four_symbol_maps_frozen_and_integrated"
    } else {
        "no_all_four_profitable_candidate"
    };
    let validation_status = if !validation {
        "unavailable"
    } else if !selected {
        "not_run_no_selection_candidate"
    } else if artifact.validation_error.is_some() {
        "failed"
    } else {
        "complete"
    };
    let structural_status = if !selected {
        "not_run"
    } else if !validation {
        "validation_unavailable"
    } else if artifact.structural_pass {
        "structural_pass"
    } else {
        "structural_failed"
    };
    let historical_economic_status = if !selected {
        "selection_failed"
    } else if !validation {
        "validation_unavailable"
    } else if artifact.all_four_economic_pass {
        "all_four_pass"
    } else {
        "all_four_failed"
    };
    ensure!(
        report.status
            == if matches!(artifact.application_reason.as_str(), "apply_failed") {
                "failed"
            } else {
                "complete"
            }
            && (report.selection_status == selection_status
                || (selected && report.selection_status == "symbol_maps_frozen_and_integrated"))
            && report.validation_status == validation_status
            && report.structural_status == structural_status
            && (report.economic_status == historical_economic_status
                || report.validation.as_ref().is_some_and(|validation| {
                    validation.status == validation_status
                        && report.economic_status == validation.economic_status()
                }))
            && report.application_status
                == if artifact.applied {
                    "applied"
                } else {
                    "not_applied"
                },
        "retained optimizer status conclusions differ from sealed proof"
    );
    Ok(())
}

fn verify_captures(
    reports: &[OptimizerCaptureReport],
    inputs: &capture_inputs::FrozenCaptureInputs,
) -> Result<()> {
    ensure!(
        reports.len() == inputs.selection.len() + inputs.holdout.len(),
        "retained optimizer capture count differs from sealed proof"
    );
    for (input, role) in inputs
        .selection
        .iter()
        .map(|input| (input, "selection"))
        .chain(inputs.holdout.iter().map(|input| (input, "validation")))
    {
        let report = reports
            .iter()
            .find(|report| {
                report.role == role && report.capture == input.capture.display().to_string()
            })
            .with_context(|| format!("retained optimizer capture missing role={role}"))?;
        verify_capture_report(report, input, role)?;
    }
    Ok(())
}

pub(crate) fn verify_capture_report(
    report: &OptimizerCaptureReport,
    input: &capture_inputs::FrozenCaptureInput,
    role: &str,
) -> Result<()> {
    let capture_duration_ms = input.capture_end_ms.saturating_sub(input.capture_start_ms);
    let replay_duration_ms = input.replay_end_ms.saturating_sub(input.replay_start_ms);
    ensure!(
        report.role == role
            && report.capture == input.capture.display().to_string()
            && report.session_id == input.session_id
            && report.run_id == input.run_id
            && report.recorded_market_population == input.recorded_market_population
            && ((report.duration_ms == replay_duration_ms
                && report.capture_duration_ms.is_none()
                && report.replay_duration_ms.is_none())
                || (report.duration_ms == 0
                    && report.capture_duration_ms == Some(capture_duration_ms)
                    && report.replay_duration_ms == Some(replay_duration_ms))),
        "retained optimizer capture identity or duration differs from sealed proof role={role}"
    );
    Ok(())
}

pub(crate) fn verify_candidate_report(
    report: Option<&OptimizerCandidateReport>,
    proof: Option<&candidate_proof::CandidateReplayProof>,
    objective_profile: &ObjectiveProfile,
    evaluation_scope: &str,
    label: &str,
) -> Result<()> {
    ensure!(
        report.is_some() == proof.is_some(),
        "retained {label} presence differs from sealed proof"
    );
    let Some((report, proof)) = report.zip(proof) else {
        return Ok(());
    };
    scoring::validate_prediction_evidence(
        proof.selection_mode,
        &proof.params_by_symbol.keys().cloned().collect::<Vec<_>>(),
        &proof.prediction_metrics,
    )?;
    let endpoint = endpoint_joint_score(
        objective_profile,
        &proof.params_by_symbol.keys().cloned().collect::<Vec<_>>(),
        &proof.canonical_session_stats,
        &proof.portfolio_accounting,
    )?;
    let total = proof.canonical_session_stats.canonical.total_summary();
    let objective_score = proof
        .selection_score
        .map(|score| OptimizerObjectiveScoreReport {
            numerator: score.objective_score.numerator,
            denominator: score.objective_score.denominator,
        });
    ensure!(
        report.selection_mode == proof.selection_mode.as_str()
            && report.prediction_metrics == proof.prediction_metrics
            && report.candidate_id == proof.candidate_id
            && report.params_by_symbol == proof.params_by_symbol
            && report.pnl_by_symbol == endpoint.pnl_by_symbol
            && report.objective_score == objective_score
            && report.shortfall_units_6dp
                == proof
                    .selection_score
                    .map_or(u128::MAX, |score| score.shortfall_units_6dp)
            && report.all_symbols_profitable
                == endpoint.pnl_by_symbol.values().all(|row| {
                    row.receipt_realized_pnl_units_6dp
                        .is_some_and(|pnl| pnl > 0)
                })
            && report.entry_count == endpoint.entry_count
            && report.profitable_market_count == endpoint.profitable_market_count
            && report.split_entries == total.split_entries
            && report.submitted_orders == total.submitted_orders
            && report.managed_sell_fills == total.confirmed_managed_sell_fill_count
            && report.wrong_side_first_managed_sell
                == proof
                    .canonical_session_stats
                    .wrong_side_first_sells
                    .unwrap_or_default()
            && (report.evaluation_scope == evaluation_scope
                || report.evaluation_scope
                    == evaluation_scope.replace("_four_symbol_", "_symbol_"))
            && report.accounting_coverage.as_ref() == Some(&proof.accounting_coverage)
            && report.canonical_ctf_exit_summary
                == proof.canonical_session_stats.canonical.ctf_exit_summary
            && report.canonical_ctf_exit_cohorts
                == proof.canonical_session_stats.canonical.ctf_exit_cohorts,
        "retained {label} accounting, score, or CTF evidence differs from sealed proof"
    );
    Ok(())
}

fn verify_economic_table(
    validation: &OptimizerValidationReport,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<()> {
    ensure!(
        validation.per_symbol.len() == artifact.frozen_capture_inputs.symbols()?.len(),
        "retained economic row count differs from sealed proof"
    );
    let selected = artifact
        .selection_candidate
        .as_ref()
        .unwrap_or(&artifact.selection_incumbent);
    for symbol in &artifact.frozen_capture_inputs.symbols()? {
        let selection = artifact
            .selection_economics_by_symbol
            .get(symbol)
            .with_context(|| format!("sealed selection economics missing symbol={symbol}"))?;
        let later = artifact.validation_economics_by_symbol.get(symbol);
        let selection_ctf = selected
            .canonical_session_stats
            .canonical
            .summary_for_symbol(symbol)
            .with_context(|| format!("sealed selection CTF row missing symbol={symbol}"))?;
        let validation_ctf = artifact
            .validation_candidate
            .as_ref()
            .map(|proof| {
                proof
                    .canonical_session_stats
                    .canonical
                    .summary_for_symbol(symbol)
                    .with_context(|| format!("sealed validation CTF row missing symbol={symbol}"))
            })
            .transpose()?;
        let expected = polymarket_hft::optimizer_report::OptimizerSymbolEconomicReport {
            symbol: (*symbol).to_string(),
            selection_candidate_pnl: selected
                .portfolio_accounting
                .row_for_symbol(symbol)
                .cloned(),
            selection_incumbent_pnl: artifact
                .selection_incumbent
                .portfolio_accounting
                .row_for_symbol(symbol)
                .cloned(),
            validation_candidate_pnl: artifact
                .validation_candidate
                .as_ref()
                .and_then(|proof| proof.portfolio_accounting.row_for_symbol(symbol))
                .cloned(),
            validation_incumbent_pnl: artifact
                .validation_incumbent
                .as_ref()
                .and_then(|proof| proof.portfolio_accounting.row_for_symbol(symbol))
                .cloned(),
            validation_delta_units_6dp: later.map(|row| row.delta_units_6dp),
            selection_candidate_entries: Some(selection_ctf.confirmed_entry_market_count),
            selection_candidate_cohorts: Some(
                selection_ctf.closed_cohort_count + selection_ctf.open_cohort_count,
            ),
            validation_candidate_entries: validation_ctf
                .map(|row| row.confirmed_entry_market_count),
            validation_candidate_cohorts: validation_ctf
                .map(|row| row.closed_cohort_count + row.open_cohort_count),
            selection_profitable: selection.candidate_profitable,
            validation_profitable: later.map(|row| row.candidate_profitable),
            beats_validation_incumbent: later.map(|row| row.beats_incumbent),
            economic_pass: later.map(|row| selection.candidate_profitable && row.passed),
            selection_candidate_brier: artifact
                .selection_candidate
                .as_ref()
                .and_then(|proof| proof.prediction_metrics.get(symbol))
                .and_then(|metric| metric.brier),
            selection_incumbent_brier: artifact
                .selection_incumbent
                .prediction_metrics
                .get(symbol)
                .and_then(|metric| metric.brier),
            validation_candidate_brier: artifact
                .validation_candidate
                .as_ref()
                .and_then(|proof| proof.prediction_metrics.get(symbol))
                .and_then(|metric| metric.brier),
            validation_incumbent_brier: artifact
                .validation_incumbent
                .as_ref()
                .and_then(|proof| proof.prediction_metrics.get(symbol))
                .and_then(|metric| metric.brier),
            prediction_comparable: later.and_then(|row| row.prediction_comparable),
            prediction_improved: later.and_then(|row| row.prediction_improved),
        };
        ensure!(
            validation
                .per_symbol
                .iter()
                .find(|row| row.symbol == *symbol)
                == Some(&expected),
            "retained all-four economic row differs from sealed proof symbol={symbol}"
        );
    }
    Ok(())
}

pub(crate) fn verify_structural_summary(
    report: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
    objective_profile: &ObjectiveProfile,
) -> Result<()> {
    let structural = report
        .structural_recheck
        .as_ref()
        .context("retained structural report missing")?;
    let expected_status = if artifact.structural_pass {
        "structural_pass"
    } else if artifact.frozen_capture_inputs.holdout.is_empty() {
        "validation_unavailable"
    } else {
        "structural_failed"
    };
    ensure!(
        structural.validation_status == expected_status
            && structural.error == artifact.validation_error
            && structural.capture_replays.len()
                == 1 + usize::from(artifact.validation_candidate.is_some()),
        "retained structural conclusion differs from sealed proof"
    );
    let selection = structural
        .capture_replays
        .iter()
        .find(|replay| replay.capture.role == "selection")
        .context("retained selection structural replay missing")?;
    verify_capture_report(
        &selection.capture,
        artifact
            .frozen_capture_inputs
            .selection
            .first()
            .context("sealed selection input missing")?,
        "selection",
    )?;
    verify_candidate_report(
        selection.candidate.as_ref(),
        artifact.selection_candidate.as_ref(),
        objective_profile,
        "single_capture_four_symbol_integration",
        "selection structural candidate",
    )?;
    if let Some(proof) = &artifact.validation_candidate {
        let validation = structural
            .capture_replays
            .iter()
            .find(|replay| replay.capture.role == "validation")
            .context("retained validation structural replay missing")?;
        verify_capture_report(
            &validation.capture,
            artifact
                .frozen_capture_inputs
                .holdout
                .first()
                .context("sealed validation input missing")?,
            "validation",
        )?;
        verify_candidate_report(
            validation.candidate.as_ref(),
            Some(proof),
            objective_profile,
            "single_capture_four_symbol_validation",
            "validation structural candidate",
        )?;
    }
    Ok(())
}

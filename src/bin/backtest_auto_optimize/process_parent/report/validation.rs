use super::*;

pub(super) fn validation_report(
    ctx: &RunContext,
    artifact: &candidate_proof::CandidateResultArtifact,
    holdout: Option<&holdout::HoldoutResult>,
) -> Result<OptimizerValidationReport> {
    let mut per_symbol = Vec::with_capacity(ctx.symbols.len());
    for symbol in &ctx.symbols {
        let selection = artifact
            .selection_economics_by_symbol
            .get(symbol)
            .with_context(|| format!("selection economics missing symbol={symbol}"))?;
        let validation = holdout.and_then(|result| result.economics_by_symbol.get(symbol));
        let selection_summary = artifact
            .selection_candidate
            .as_ref()
            .context("selection candidate proof missing")?
            .canonical_session_stats
            .canonical
            .summary_for_symbol(symbol)
            .with_context(|| format!("selection canonical row missing symbol={symbol}"))?;
        let validation_summary = holdout
            .map(|result| {
                result
                    .candidate
                    .canonical_session_stats
                    .canonical
                    .summary_for_symbol(symbol)
                    .with_context(|| format!("validation canonical row missing symbol={symbol}"))
            })
            .transpose()?;
        per_symbol.push(OptimizerSymbolEconomicReport {
            symbol: (*symbol).to_string(),
            selection_candidate_pnl: artifact
                .selection_candidate
                .as_ref()
                .and_then(|proof| proof.portfolio_accounting.row_for_symbol(symbol))
                .cloned(),
            selection_incumbent_pnl: artifact
                .selection_incumbent
                .portfolio_accounting
                .row_for_symbol(symbol)
                .cloned(),
            validation_candidate_pnl: holdout
                .and_then(|result| result.candidate.portfolio_accounting.row_for_symbol(symbol))
                .cloned(),
            validation_incumbent_pnl: holdout
                .and_then(|result| result.incumbent.portfolio_accounting.row_for_symbol(symbol))
                .cloned(),
            validation_delta_units_6dp: validation.map(|row| row.delta_units_6dp),
            selection_candidate_entries: Some(selection_summary.confirmed_entry_market_count),
            selection_candidate_cohorts: Some(
                selection_summary.closed_cohort_count + selection_summary.open_cohort_count,
            ),
            validation_candidate_entries: validation_summary
                .map(|row| row.confirmed_entry_market_count),
            validation_candidate_cohorts: validation_summary
                .map(|row| row.closed_cohort_count + row.open_cohort_count),
            selection_profitable: selection.candidate_profitable,
            validation_profitable: validation.map(|row| row.candidate_profitable),
            beats_validation_incumbent: validation.map(|row| row.beats_incumbent),
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
            validation_candidate_brier: holdout
                .and_then(|result| result.candidate.prediction_metrics.get(symbol))
                .and_then(|metric| metric.brier),
            validation_incumbent_brier: holdout
                .and_then(|result| result.incumbent.prediction_metrics.get(symbol))
                .and_then(|metric| metric.brier),
            prediction_comparable: validation.and_then(|row| row.prediction_comparable),
            prediction_improved: validation.and_then(|row| row.prediction_improved),
            economic_pass: validation.map(|row| selection.candidate_profitable && row.passed),
        });
    }
    let status = if ctx.holdout_inputs.is_empty() {
        "unavailable"
    } else if artifact.validation_error.is_some() {
        "failed"
    } else {
        "complete"
    };
    Ok(OptimizerValidationReport {
        status: status.to_string(),
        capture: ctx
            .holdout_inputs
            .first()
            .map(|input| summary::capture_report(input, "validation")),
        candidate: holdout
            .map(|result| {
                replay_candidate_report(
                    ctx,
                    &result.candidate,
                    "frozen_candidate",
                    "single_capture_symbol_validation",
                )
            })
            .transpose()?,
        incumbent: holdout
            .map(|result| {
                replay_candidate_report(
                    ctx,
                    &result.incumbent,
                    "frozen_incumbent",
                    "single_capture_symbol_validation",
                )
            })
            .transpose()?,
        per_symbol,
        all_symbols_pass: holdout.map(|_| artifact.all_four_economic_pass),
        error: artifact.validation_error.clone(),
    })
}

pub(super) fn structural_report(
    ctx: &RunContext,
    artifact: &candidate_proof::CandidateResultArtifact,
    selection: &holdout::HoldoutResult,
    holdout: Option<&holdout::HoldoutResult>,
    selection_candidate: Option<OptimizerCandidateReport>,
) -> Result<OptimizerStructuralReport> {
    let mut issues = Vec::new();
    if !selection.structural_pass {
        issues.push(
            selection
                .error
                .clone()
                .unwrap_or_else(|| "selection_structural_failure".to_string()),
        );
    }
    let mut capture_replays = vec![OptimizerCaptureReplayReport {
        capture: summary::capture_report(
            ctx.selection_inputs
                .first()
                .context("selection input missing")?,
            "selection",
        ),
        candidate: selection_candidate,
        structural_validation_status: if selection.structural_pass {
            "structural_pass"
        } else {
            "structural_failed"
        }
        .to_string(),
        structural_validation_issues: issues.clone(),
        error: selection.error.clone(),
    }];
    if let Some(result) = holdout {
        let holdout_issues = result
            .error
            .as_deref()
            .map(|error| vec![error.to_string()])
            .unwrap_or_default();
        issues.extend(holdout_issues.clone());
        capture_replays.push(OptimizerCaptureReplayReport {
            capture: summary::capture_report(
                ctx.holdout_inputs
                    .first()
                    .context("validation input missing")?,
                "validation",
            ),
            candidate: Some(replay_candidate_report(
                ctx,
                &result.candidate,
                "frozen_candidate",
                "single_capture_symbol_validation",
            )?),
            structural_validation_status: if result.structural_pass {
                "structural_pass"
            } else {
                "structural_failed"
            }
            .to_string(),
            structural_validation_issues: holdout_issues,
            error: result.error.clone(),
        });
    }
    Ok(OptimizerStructuralReport {
        validation_status: if artifact.structural_pass {
            "structural_pass"
        } else if ctx.holdout_inputs.is_empty() {
            "validation_unavailable"
        } else {
            "structural_failed"
        }
        .to_string(),
        validation_issues: issues,
        capture_replays,
        error: artifact.validation_error.clone(),
    })
}

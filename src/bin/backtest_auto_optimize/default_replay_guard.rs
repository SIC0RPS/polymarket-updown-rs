use anyhow::{anyhow, ensure, Context, Result};

use super::{endpoint_joint_score, evaluate_default_candidate, scoring, RunContext, TrialOrigin};

pub(super) fn assert_default_replay_stable(ctx: &RunContext) -> Result<usize> {
    let (
        initial_candidate_id,
        initial_params,
        initial_pnl,
        initial_coverage,
        initial_score,
        initial_selection,
        initial_pack_scores,
        initial_learning,
        before_count,
    ) = {
        let trials = ctx
            .trials
            .lock()
            .map_err(|_| anyhow!("trial mutex poisoned"))?;
        let default = trials
            .first()
            .filter(|trial| trial.index == 1 && trial.origin == TrialOrigin::Default)
            .context("default optimizer trial missing")?;
        ensure!(
            default.selection_mode == ctx.selection_mode,
            "default selection mode differs from frozen context"
        );
        let metrics = default
            .requested_metrics
            .as_ref()
            .context("default optimizer anchor-capture metrics missing")?;
        (
            default.candidate_id.clone(),
            default.params_by_symbol.clone(),
            metrics.pnl_by_symbol.clone(),
            metrics.accounting_coverage.clone(),
            default
                .selection_score
                .context("default selection score missing")?,
            scoring::trial_selection_score(default)?,
            default.retained_pack_scores.clone(),
            default.oracle_learning_sha256.clone(),
            trials.len(),
        )
    };

    evaluate_default_candidate(ctx, TrialOrigin::DefaultStabilityRecheck)
        .context("re-evaluate optimizer default candidate")?;

    let trials = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?;
    ensure!(
        trials.len() == before_count + 1,
        "default replay stability check appended an unexpected trial count"
    );
    let rechecked = trials
        .last()
        .context("default replay stability trial missing")?;
    ensure!(
        rechecked.origin == TrialOrigin::DefaultStabilityRecheck,
        "default replay stability trial has wrong origin"
    );
    let metrics = rechecked
        .requested_metrics
        .as_ref()
        .context("default replay stability anchor-capture metrics missing")?;
    let canonical_proof = rechecked
        .canonical_session_stats
        .as_ref()
        .context("default replay stability canonical proof missing")?;
    let portfolio_accounting = rechecked
        .portfolio_accounting
        .as_ref()
        .context("default replay stability endpoint proof missing")?;
    let endpoint = endpoint_joint_score(
        &ctx.objective_profile,
        &ctx.symbols,
        canonical_proof,
        portfolio_accounting,
    )?;
    ensure!(
        rechecked.selection_mode == ctx.selection_mode
            && rechecked.candidate_id == initial_candidate_id
            && rechecked.params_by_symbol == initial_params
            && metrics.pnl_by_symbol == initial_pnl
            && endpoint.pnl_by_symbol == initial_pnl
            && metrics.accounting_coverage == initial_coverage
            && endpoint.accounting_coverage == initial_coverage
            && rechecked.selection_score == Some(initial_score)
            && endpoint.selection_score == initial_score
            && scoring::trial_selection_score(rechecked)? == initial_selection
            && rechecked.retained_pack_scores == initial_pack_scores
            && rechecked.oracle_learning_sha256 == initial_learning,
        "proof invalid: joint optimizer default replay drift candidate_id={}",
        initial_candidate_id
    );
    Ok(rechecked.index)
}

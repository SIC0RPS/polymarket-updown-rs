use super::*;
use cmaes::restart::{RestartOptions, RestartStrategy, RestartTerminationReason};
use cmaes::Mode;

#[derive(Clone, Debug)]
pub(super) struct SearchOutcome {
    pub(super) search_status: String,
    pub(super) restart_termination_reason: String,
    pub(super) run_termination_reasons: Vec<Vec<String>>,
    pub(super) function_evals: usize,
    pub(super) runs: usize,
    pub(super) no_improvement_evals: usize,
}

#[derive(Clone, Debug)]
pub(super) struct SearchIncumbent {
    pub(super) selection_score: scoring::SelectionScore,
}

#[derive(Clone, Debug)]
pub(super) struct SymbolWinner {
    pub(super) trial_index: usize,
    pub(super) candidate_id: String,
}

pub(super) fn compare_trials(
    left: &TrialRecord,
    right: &TrialRecord,
) -> Result<std::cmp::Ordering> {
    let ordering =
        scoring::trial_selection_score(left)?.compare(scoring::trial_selection_score(right)?)?;
    Ok(ordering
        .then_with(|| right.index.cmp(&left.index))
        .then_with(|| right.candidate_id.cmp(&left.candidate_id)))
}

pub(super) fn select_search_incumbent(trials: &[TrialRecord]) -> Result<SearchIncumbent> {
    let mut incumbent = None;
    for trial in trials.iter().filter(|trial| {
        trial.complete_coverage()
            && matches!(
                trial.origin,
                TrialOrigin::Default | TrialOrigin::ArchivedContender
            )
    }) {
        let replaces = match incumbent {
            Some(current) => compare_trials(trial, current)?.is_gt(),
            None => true,
        };
        if replaces {
            incumbent = Some(trial);
        }
    }
    let incumbent = incumbent.context("default/archive search incumbent missing")?;
    Ok(SearchIncumbent {
        selection_score: scoring::trial_selection_score(incumbent)?,
    })
}

pub(super) fn select_symbol_winner(trials: &[TrialRecord]) -> Result<Option<SymbolWinner>> {
    let mut winner = None;
    for trial in trials
        .iter()
        .filter(|trial| trial.origin.is_champion_eligible() && trial.complete_coverage())
    {
        let replaces = match winner {
            Some(current) => compare_trials(trial, current)?.is_gt(),
            None => true,
        };
        if replaces {
            winner = Some(trial);
        }
    }
    Ok(winner.map(|trial| SymbolWinner {
        trial_index: trial.index,
        candidate_id: trial.candidate_id.clone(),
    }))
}

pub(super) fn run_optimizer_search(
    ctx: &RunContext,
    settings: &OptimizerSettings,
    incumbent: &SearchIncumbent,
) -> Result<SearchOutcome> {
    let defaults = default_params_by_symbol(ctx)?;
    let first_run_initial_mean = initial_mean_for_joint_params(ctx, &defaults)?;
    let switches = [(0, 0), (0, 1), (1, 0), (1, 1)];
    let starter_count = switches.len();
    let remaining_function_evals = settings
        .max_function_evals
        .checked_sub(starter_count)
        .context("search evaluation allowance must cover all four switch starters")?;
    let mut starters = Vec::with_capacity(starter_count);
    for (market, beta) in switches {
        let mut params_by_symbol = defaults.clone();
        for params in params_by_symbol.values_mut() {
            for (name, value) in [
                ("live_fair_value_model_market_calibration_enabled", market),
                ("live_fair_value_model_beta_calibration_enabled", beta),
            ] {
                *params
                    .get_mut(name)
                    .with_context(|| format!("switch starter parameter missing: {name}"))? =
                    ParamValue::Int(value);
            }
        }
        starters.push(CandidateRequest::new(params_by_symbol, TrialOrigin::Search));
    }

    let mut best_selection_score = incumbent.selection_score;
    let mut no_improvement_evals = 0usize;
    let mut record_population = |first_trial: usize, population_size: usize| -> Result<bool> {
        let improved = {
            let trials = ctx
                .trials
                .lock()
                .map_err(|_| anyhow!("trial mutex poisoned"))?;
            let population = trials
                .get(first_trial..)
                .context("search population trials missing")?;
            ensure!(
                population.len() == population_size
                    && population
                        .iter()
                        .all(|trial| trial.origin == TrialOrigin::Search),
                "search population trial evidence mismatch"
            );
            let mut improved = false;
            for trial in population {
                if trial.complete_coverage() {
                    let score = scoring::trial_selection_score(trial)?;
                    if score.compare(best_selection_score)?.is_gt() {
                        best_selection_score = score;
                        improved = true;
                    }
                }
            }
            improved
        };
        no_improvement_evals = if improved {
            0
        } else {
            no_improvement_evals
                .checked_add(population_size)
                .context("no-improvement evaluation count overflow")?
        };
        let stop = settings.max_no_improvement_evals > 0
            && no_improvement_evals >= settings.max_no_improvement_evals;
        write_optimizer_status(ctx, "population")?;
        Ok(stop)
    };
    let first_trial = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?
        .len();
    evaluate::evaluate_params_batch(ctx, starters, false)?;
    let stop = record_population(first_trial, starter_count)?;
    let (reason, runs, function_evals, run_termination_reasons) = if stop {
        (
            RestartTerminationReason::NoImprovement,
            0,
            starter_count,
            Vec::new(),
        )
    } else {
        let results = RestartOptions::new(
            ctx.parameter_count(),
            0.0..=1.0,
            RestartStrategy::BIPOP(Default::default()),
        )
        .mode(Mode::Minimize)
        .minimum_population_size(settings.jobs)
        .max_function_evals(remaining_function_evals)
        .max_generations_per_run(settings.max_generations_per_run)
        .parallel_update(false)
        .seed(settings.seed)
        .first_run_initial_mean(first_run_initial_mean)
        .build()
        .map_err(|err| anyhow!("build BIPOP restart optimizer: {err:?}"))?
        .run_batch_with_reuse(|vectors| {
            let first_trial = ctx
                .trials
                .lock()
                .map_err(|_| anyhow!("trial mutex poisoned"))?
                .len();
            let scores = evaluate_population(ctx, vectors)?;
            let stop = record_population(first_trial, vectors.len())?;
            Ok::<(Vec<f64>, bool), anyhow::Error>((
                scores.into_iter().map(|score| -score).collect(),
                stop,
            ))
        })
        .map_err(|err| anyhow!("run BIPOP batch optimizer: {err}"))?;
        (
            results.reason,
            results.runs,
            results
                .function_evals
                .checked_add(starter_count)
                .context("search evaluation count overflow")?,
            results.run_termination_reasons,
        )
    };
    let search_status = match reason {
        RestartTerminationReason::MaxFunctionEvals | RestartTerminationReason::MaxTime => {
            "budget_limited"
        }
        RestartTerminationReason::FunTarget => "target_reached",
        RestartTerminationReason::NoImprovement => "no_improvement",
        RestartTerminationReason::MaxRuns => "restart_limit_reached",
        RestartTerminationReason::InvalidFunctionValue => {
            bail!("BIPOP restart optimizer terminated with invalid function value")
        }
    };
    let restart_termination_reason = reason.to_string();
    let run_termination_reasons = run_termination_reasons
        .into_iter()
        .map(|reasons| {
            reasons
                .into_iter()
                .map(|reason| reason.to_string())
                .collect()
        })
        .collect::<Vec<Vec<String>>>();
    let outcome = SearchOutcome {
        search_status: search_status.to_string(),
        restart_termination_reason,
        run_termination_reasons,
        function_evals,
        runs,
        no_improvement_evals,
    };
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE search_done status={} restart_reason={} runs={} function_evals={} no_improvement_evals={} run_reasons={:?}",
        search_status,
        outcome.restart_termination_reason,
        outcome.runs,
        outcome.function_evals,
        no_improvement_evals,
        outcome.run_termination_reasons
    ));
    write_optimizer_status(ctx, "search_done")?;
    Ok(outcome)
}

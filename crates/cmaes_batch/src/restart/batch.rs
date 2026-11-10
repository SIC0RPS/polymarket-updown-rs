use std::error::Error;
use std::fmt;

use nalgebra::DVector;

use super::{RestartResults, RestartTerminationReason, Restarter};
use crate::{BatchAskTellError, TerminationData, TerminationReason, CMAES};

#[derive(Debug)]
pub enum BatchRestartError<E> {
    Objective(E),
    AskTell(BatchAskTellError),
}

impl<E: fmt::Display> fmt::Display for BatchRestartError<E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Objective(error) => write!(fmt, "batch objective failed: {error}"),
            Self::AskTell(error) => write!(fmt, "batch CMA-ES failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for BatchRestartError<E> {}

impl Restarter {
    pub fn run_batch_with_reuse<F, E>(
        self,
        mut objective: F,
    ) -> Result<RestartResults, BatchRestartError<E>>
    where
        F: FnMut(&[DVector<f64>]) -> Result<(Vec<f64>, bool), E>,
    {
        let max_function_evals = self.max_function_evals;
        let mut completed_function_evals = 0usize;
        let mut global_budget_exhausted = false;
        let mut batch_error = None;
        let mut results = self.run_internal(
            || (),
            false,
            |state| loop {
                if max_function_evals.is_some_and(|maximum| {
                    state.parameters().lambda() > maximum.saturating_sub(completed_function_evals)
                }) {
                    global_budget_exhausted = true;
                    return (max_function_evals_termination(state), true);
                }
                let points = match state.ask_population() {
                    Ok(points) => points,
                    Err(error) => {
                        batch_error = Some(BatchRestartError::AskTell(error));
                        return (error_termination(state), false);
                    }
                };
                let (values, stop_after_update) = match objective(&points) {
                    Ok(result) => result,
                    Err(error) => {
                        batch_error = Some(BatchRestartError::Objective(error));
                        return (error_termination(state), false);
                    }
                };
                if values.len() != points.len() {
                    batch_error = Some(BatchRestartError::AskTell(
                        BatchAskTellError::PopulationSize {
                            expected: points.len(),
                            actual: values.len(),
                        },
                    ));
                    return (error_termination(state), false);
                }
                let evaluated = points.into_iter().zip(values).collect();
                match state.tell_population(evaluated) {
                    Ok(result) => {
                        completed_function_evals =
                            completed_function_evals.saturating_add(state.parameters().lambda());
                        match result {
                            Some(result) => return (result, stop_after_update),
                            None if stop_after_update => {
                                return (requested_termination(state), true)
                            }
                            None => {}
                        }
                    }
                    Err(error) => {
                        batch_error = Some(BatchRestartError::AskTell(error));
                        return (error_termination(state), false);
                    }
                }
            },
        );
        if global_budget_exhausted {
            results.reason = RestartTerminationReason::MaxFunctionEvals;
        }
        match batch_error {
            Some(error) => Err(error),
            None => Ok(results),
        }
    }
}

fn requested_termination<F>(state: &CMAES<F>) -> TerminationData {
    TerminationData {
        current_best: state.current_best_individual().cloned(),
        overall_best: state.overall_best_individual().cloned(),
        final_mean: state.mean().clone(),
        reasons: Vec::new(),
    }
}

fn max_function_evals_termination<F>(state: &CMAES<F>) -> TerminationData {
    let mut termination = requested_termination(state);
    termination
        .reasons
        .push(TerminationReason::MaxFunctionEvals);
    termination
}

fn error_termination<F>(state: &CMAES<F>) -> TerminationData {
    TerminationData {
        current_best: state.current_best_individual().cloned(),
        overall_best: state.overall_best_individual().cloned(),
        final_mean: state.mean().clone(),
        reasons: vec![TerminationReason::InvalidFunctionValue],
    }
}

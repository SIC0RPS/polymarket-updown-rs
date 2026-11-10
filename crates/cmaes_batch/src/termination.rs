//! Algorithm termination handling. See [`TerminationReason`] for full documentation.

use statrs::statistics::{Data, Median};

use std::collections::VecDeque;
use std::fmt::{self, Debug};
use std::time::Instant;

use crate::history::History;
use crate::parameters::Parameters;
use crate::sampling::EvaluatedPoint;
use crate::state::State;
use crate::{utils, MAX_HISTORY_LENGTH};

/// Represents a reason for the algorithm terminating. Most of these are for preventing numerical
/// instability, while `Tol*` are problem-dependent parameters and `Max*` are for bounding
/// iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TerminationReason {
    /// The maximum number of objective function evaluations has been reached.
    MaxFunctionEvals,
    /// The maximum number of generations has been reached.
    MaxGenerations,
    /// The algorithm has been running for longer than the time limit.
    MaxTime,
    /// The target objective function value has been reached.
    FunTarget,
    /// The range of function values of the latest generation and the range of the best function
    /// values of many consecutive generations lie below `tol_fun`. Indicates that the function
    /// value has stopped changing significantly and that the function value spread of each
    /// generation is equally insignificant.
    TolFun,
    /// Like `TolFun`, but the range is `tol_fun_rel * (first_median - best_median)` (i.e. it is
    /// relative to the overall improvement in the median objective function value).
    TolFunRel,
    /// The range of best function values in many consecutive generations is lower than
    /// `tol_fun_hist` (i.e. little to no improvement or change is occurring).
    TolFunHist,
    /// The standard deviation of the distribution is smaller than `tol_x` in every coordinate and
    /// the mean has not moved much recently. Indicates that the algorithm has converged.
    TolX,
    /// The best and median function values have not improved over the past 20% of all generations,
    /// clamped to the range `[tol_stagnation, MAX_HISTORY_LENGTH]`. Setting `tol_stagnation` to be
    /// greater than `MAX_HISTORY_LENGTH` effectively disables this termination criterion.
    TolStagnation,
    /// The maximum standard deviation across all distribution axes increased by a factor of more
    /// than `tol_x_up`. This is likely due to the function diverging or the initial step size being
    /// set far too small. In the latter case a restart with a larger step size may be useful.
    TolXUp,
    /// The standard deviation in any principal axis in the distribution is too small to perform any
    /// meaningful calculations.
    NoEffectAxis,
    /// The standard deviation in any coordinate axis in the distribution is too small to perform
    /// any meaningful calculations.
    NoEffectCoord,
    /// The condition number of the covariance matrix exceeds `tol_condition_cov` or is non-normal.
    TolConditionCov,
    /// The objective function has returned an invalid value (`NAN` or `-NAN`).
    InvalidFunctionValue,
    /// The covariance matrix is not positive definite. If this is returned frequently, it probably
    /// indicates a bug in the library and can be reported [here][0]. Using
    /// [`Weights::Positive`][crate::parameters::Weights::Positive] should prevent this entirely in
    /// the meantime.
    ///
    /// [0]: https://github.com/pengowen123/cmaes/issues/
    PosDefCov,
}

impl fmt::Display for TerminationReason {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, fmt)
    }
}

/// Stores parameters of the termination check
pub(crate) struct TerminationCheck<'a> {
    pub current_function_evals: usize,
    /// The time at which the `CMAES` was created
    pub time_created: Instant,
    pub parameters: &'a Parameters,
    pub state: &'a State,
    pub history: &'a History,
    /// The current generation of individuals
    pub individuals: &'a [EvaluatedPoint],
}

impl<'a> TerminationCheck<'a> {
    /// Checks whether any termination criteria are met based on the stored parameters
    pub(crate) fn check_termination_criteria(self) -> Vec<TerminationReason> {
        let mut result = Vec::new();

        let mode = self.parameters.mode();
        let dim = self.parameters.dim();
        let lambda = self.parameters.lambda();
        let initial_sigma = self.parameters.initial_sigma();
        let tol_fun = self.parameters.tol_fun();
        let tol_fun_rel_option = self.parameters.tol_fun_rel();
        let tol_fun_hist = self.parameters.tol_fun_hist();
        let tol_x = self.parameters.tol_x();
        let tol_stagnation_option = self.parameters.tol_stagnation();
        let tol_x_up = self.parameters.tol_x_up();
        let tol_condition_cov = self.parameters.tol_condition_cov();

        let mean = self.state.mean();
        let cov = self.state.cov();
        let cov_eigenvectors = self.state.cov_eigenvectors();
        let cov_sqrt_eigenvalues = self.state.cov_sqrt_eigenvalues();
        let sigma = self.state.sigma();
        let path_c = self.state.path_c();

        // Check TerminationReason::MaxFunctionEvals
        if let Some(max_function_evals) = self.parameters.max_function_evals() {
            if self.current_function_evals >= max_function_evals {
                result.push(TerminationReason::MaxFunctionEvals);
            }
        }

        // Check TerminationReason::MaxGenerations
        if let Some(max_generations) = self.parameters.max_generations() {
            if self.state.generation() >= max_generations {
                result.push(TerminationReason::MaxGenerations);
            }
        }

        // Check TerminationReason::MaxTime
        if let Some(max_time) = self.parameters.max_time() {
            if self.time_created.elapsed() >= max_time {
                result.push(TerminationReason::MaxTime);
            }
        }

        // Check TerminationReason::FunTarget
        if let Some(fun_target) = self.parameters.fun_target() {
            if self
                .individuals
                .iter()
                .any(|ind| mode.is_better(ind.value(), fun_target))
            {
                result.push(TerminationReason::FunTarget);
            }
        }

        // Check TerminationReason::TolFun*
        let past_generations_a = 10 + (30.0 * dim as f64 / lambda as f64).ceil() as usize;

        if self.history.best_function_values().len() >= past_generations_a {
            let range_history = utils::range(
                self.history
                    .best_function_values()
                    .iter()
                    .take(past_generations_a)
                    .cloned(),
            )
            .unwrap();

            let range_current = utils::range(self.individuals.iter().map(|p| p.value())).unwrap();

            if range_history < tol_fun_hist {
                result.push(TerminationReason::TolFunHist);
            }

            if range_history < tol_fun && range_current < tol_fun {
                result.push(TerminationReason::TolFun);
            }

            if let (Some(first_median_value), Some(best_median_value)) = (
                self.history.first_median_function_value(),
                self.history.best_median_function_value(),
            ) {
                let tol_fun_rel_range =
                    tol_fun_rel_option * (first_median_value - best_median_value).abs();

                if range_history < tol_fun_rel_range && range_current < tol_fun_rel_range {
                    result.push(TerminationReason::TolFunRel);
                }
            }
        }

        // Check TerminationReason::TolX
        if (0..dim).all(|i| (sigma * cov[(i, i)]).abs() < tol_x)
            && path_c.iter().all(|x| (sigma * *x).abs() < tol_x)
        {
            result.push(TerminationReason::TolX);
        }

        // Check TerminationReason::TolConditionCov
        let cond = self.state.axis_ratio().powi(2);

        if !cond.is_normal() || cond > tol_condition_cov {
            result.push(TerminationReason::TolConditionCov);
        }

        // Check TerminationReason::NoEffectAxis
        // Cycles from 0 to n-1 to avoid checking every column every iteration
        let index_to_check = self.state.generation() % dim;

        let no_effect_axis_check = 0.1
            * sigma
            * cov_sqrt_eigenvalues[(index_to_check, index_to_check)]
            * cov_eigenvectors.column(index_to_check);

        if mean == &(mean + no_effect_axis_check) {
            result.push(TerminationReason::NoEffectAxis);
        }

        // Check TerminationReason::NoEffectCoord
        if (0..dim).any(|i| mean[i] == mean[i] + 0.2 * sigma * cov[(i, i)]) {
            result.push(TerminationReason::NoEffectCoord);
        }

        // Check TerminationReason::TolStagnation
        let tol_stagnation_generations =
            get_tol_stagnation_generations(tol_stagnation_option, self.state.generation());

        if let Some(tol_stagnation_generations) = tol_stagnation_generations {
            if self.history.best_function_values().len() >= tol_stagnation_generations
                && self.history.median_function_values().len() >= tol_stagnation_generations
            {
                // Checks whether the median of the values has regressed over the past
                // `tol_stagnation_generations` generations
                // Returns true if the values became worse
                let did_values_regress = |values: &VecDeque<f64>| {
                    // Note that TolStagnation is effectively disabled if tol_stagnation_generations
                    // is < 4, enforcing an effective minimum bound on tol_stagnation
                    let subrange_length = (tol_stagnation_generations as f64 * 0.3) as usize;

                    // Most recent `subrange_length `values within the past
                    // `tol_stagnation_generations` generations
                    let first_values = values
                        .iter()
                        .take(tol_stagnation_generations)
                        .take(subrange_length)
                        .cloned()
                        .collect::<Vec<_>>();

                    // Least recent `subrange_length` values within the past
                    // tol_stagnation_generations` generations
                    let last_values = values
                        .iter()
                        .take(tol_stagnation_generations)
                        .skip(tol_stagnation_generations - subrange_length)
                        .cloned()
                        .collect::<Vec<_>>();

                    mode.is_better(
                        Data::new(last_values).median(),
                        Data::new(first_values).median(),
                    )
                };

                if did_values_regress(self.history.best_function_values())
                    && did_values_regress(self.history.median_function_values())
                {
                    result.push(TerminationReason::TolStagnation);
                }
            }
        }

        // Check TerminationReason::TolXUp
        let max_standard_deviation = sigma
            * cov_sqrt_eigenvalues
                .diagonal()
                .iter()
                .max_by(|a, b| utils::partial_cmp(**a, **b))
                .unwrap();

        if max_standard_deviation / initial_sigma > tol_x_up {
            result.push(TerminationReason::TolXUp);
        }

        result
    }
}

/// Returns the default value for the `tol_stagnation` option (which is the lower bound for
/// `TolStagnation`)
pub(crate) fn get_default_tol_stagnation_option(dim: usize, lambda: usize) -> usize {
    100 + (100.0 * (dim as f64).powf(1.5) / lambda as f64).ceil() as usize
}

/// Returns the number of generations over which to check `TolStagnation`
///
/// Returns `None` if the history isn't long enough to perform the check
fn get_tol_stagnation_generations(
    tol_stagnation_option: usize,
    current_generation: usize,
) -> Option<usize> {
    // 20% of past generations
    let generations = (current_generation / 5)
        // At most the max history length
        .min(MAX_HISTORY_LENGTH);

    // Don't check TolStagnation if `generations` is below the lower bound
    if generations < tol_stagnation_option {
        None
    } else {
        Some(generations)
    }
}

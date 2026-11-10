//! Types for automatic restarting of CMA-ES. Useful for solving complex problems and improving the
//! robustness of the algorithm in general.
//!
//! To use automatic restarts, a [`RestartOptions`] can be created and built. Then, a `run_*`
//! method on [`Restarter`] should be called to execute the algorithm.
//!
//! Top-level configuration of the restarts is done through [`RestartOptions`], while configuration
//! of specific restart algorithms is done through their respective types (see [`RestartStrategy`]).
//!
//! For examples and complete usage documentation, see [`RestartOptions`] and [`Restarter`].

mod batch;
mod bipop;
mod ipop;
mod local;
pub mod options;
mod strategy;

pub use batch::BatchRestartError;
pub use bipop::BIPOP;
pub use ipop::IPOP;
pub use local::Local;
pub use options::RestartOptions;
pub use strategy::RestartStrategy;

use nalgebra::DVector;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use std::fmt::{self, Debug};
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use crate::parameters::Parameters;
use crate::{
    utils, CMAESOptions, Individual, Mode, ObjectiveFunction, ParallelObjectiveFunction,
    TerminationData, TerminationReason, CMAES,
};
use options::{InvalidRestartOptionsError, InvalidRestartStrategyOptionsError};
use strategy::{RestartControl, Strategy};

/// The default initial step size for runs performed by `Restarter`
/// This value should generally be overridden by individual restart strategies
const DEFAULT_INITIAL_STEP_SIZE: f64 = 0.5;

/// Represents the reason for the termination of the restart strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RestartTerminationReason {
    /// The maximum number of runs has been reached. The exact value is specific to each restart
    /// strategy.
    MaxRuns,
    /// The target objective function value has been reached.
    FunTarget,
    /// The maximum number of objective function evaluations has been reached.
    MaxFunctionEvals,
    /// The time limit has been reached.
    MaxTime,
    /// The objective function returned an invalid value (`NAN` or `-NAN`).
    InvalidFunctionValue,
    /// The objective requested termination after a completed population did not improve the
    /// whole-search incumbent within its configured evaluation allowance.
    NoImprovement,
}

impl fmt::Display for RestartTerminationReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

/// The results from executing a restart strategy.
#[derive(Clone, Debug)]
pub struct RestartResults {
    /// The best individual found. Always `Some` unless CMA-ES terminated with
    /// [`InvalidFunctionValue`][crate::TerminationReason::InvalidFunctionValue] or the
    /// [`Restarter`] terminated immediately (due to `max_runs`, `max_generations`, or `max_time`
    /// being set to zero).
    pub best: Option<Individual>,
    /// The reason for the restart strategy terminating.
    pub reason: RestartTerminationReason,
    /// The total number of times the objective function has been evaluated across all runs.
    pub function_evals: usize,
    /// The number of runs performed.
    pub runs: usize,
    /// Native CMA-ES termination reasons for each completed restart run.
    pub run_termination_reasons: Vec<Vec<TerminationReason>>,
}

impl RestartResults {
    /// Prints the results of the `Restarter` run.
    fn print_results(&self) {
        println!(
            "Terminated in {} f-evals with reason: `{}`",
            self.function_evals, self.reason
        );
        if let Some(ref best) = self.best {
            println!("Best function value: {:e}", best.value);
            println!("Best point: {}", best.point);
        }
    }
}

/// A type that handles automatic restarting of CMA-ES. Use [`RestartOptions`] to build a
/// `Restarter`.
///
/// # Examples
///
/// A good default choice for most problems is [`BIPOP`]:
///
/// ```no_run
/// use cmaes::restart::{RestartOptions, RestartStrategy};
/// use cmaes::DVector;
///
/// let function = |x: &DVector<f64>| x.magnitude();
///
/// let strategy = RestartStrategy::BIPOP(Default::default());
/// let dim = 10;
/// let restarter = RestartOptions::new(dim, -5.0..=5.0, strategy)
///     .fun_target(1e-10)
///     .enable_printing(true)
///     .build()
///     .unwrap();
///
/// let results = restarter.run(|| function);
/// ```
///
/// Use [`run_parallel`][Self::run_parallel] for expensive objectives. The `with_reuse` variants
/// retain objective-function state across restarts.
#[derive(Clone, Debug)]
pub struct Restarter {
    /// The strategy to use in performing the restarts
    strategy: RestartStrategy,
    /// The number of dimensions to search
    dimensions: usize,
    /// The minimum base population size before the restart strategy adjusts it
    minimum_population_size: Option<usize>,
    /// The optimization mode
    mode: Mode,
    /// Whether to perform state updates in parallel
    parallel_update: bool,
    /// The range in which to generate the initial mean for each run
    search_range: RangeInclusive<f64>,
    /// The initial mean used by the first run only
    first_run_initial_mean: Option<DVector<f64>>,
    /// The target objective function value
    fun_target: Option<f64>,
    /// The maximum number of objective function evaluations allowed across all runs
    max_function_evals: Option<usize>,
    /// The time limit across all runs
    max_time: Option<Duration>,
    /// The maximum number of objective function evaluations allowed for each run
    max_function_evals_per_run: Option<usize>,
    /// The maximum number of generations allowed for each run
    max_generations_per_run: Option<usize>,
    /// Whether to print info about each run
    print_info: bool,
    /// Seed for the RNG
    seed: u64,
    /// Used to generate numbers specifically relevant to performing restarts in addition to
    /// generating seeds for the runs themselves
    rng: ChaChaRng,
    /// The best individual found so far
    overall_best: Option<Individual>,
}

impl Restarter {
    /// Returns a new `Restarter` using the provided options. [`RestartOptions::build`] should
    /// generally be used instead.
    pub fn new(options: RestartOptions) -> Result<Self, InvalidRestartOptionsError> {
        let seed = options.seed.unwrap_or_else(rand::random);

        if options.dimensions == 0 {
            Err(InvalidRestartOptionsError::Dimensions)
        } else if options.search_range.end() - options.search_range.start() == 0.0 {
            Err(InvalidRestartOptionsError::SearchRange)
        } else {
            if let Some(mean) = options.first_run_initial_mean.as_ref() {
                if mean.len() != options.dimensions {
                    return Err(InvalidRestartOptionsError::InitialMeanDimensions);
                }
                if mean.iter().any(|value| !value.is_finite()) {
                    return Err(InvalidRestartOptionsError::InitialMeanNotFinite);
                }
                if mean
                    .iter()
                    .any(|value| !options.search_range.contains(value))
                {
                    return Err(InvalidRestartOptionsError::InitialMeanOutOfRange);
                }
            }
            Ok(Self {
                strategy: options.strategy,
                dimensions: options.dimensions,
                minimum_population_size: options.minimum_population_size,
                mode: options.mode,
                parallel_update: options.parallel_update,
                search_range: options.search_range,
                first_run_initial_mean: options.first_run_initial_mean,
                fun_target: options.fun_target,
                max_function_evals: options.max_function_evals,
                max_time: options.max_time,
                max_function_evals_per_run: options.max_function_evals_per_run,
                max_generations_per_run: options.max_generations_per_run,
                print_info: options.enable_printing,
                seed,
                rng: ChaChaRng::seed_from_u64(seed),
                overall_best: None,
            })
        }
    }

    /// Returns the seed for the RNG.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Executes the restart strategy. Uses the objective function returned by
    /// `get_objective_function`, which is called once for each run (`F` can be a reference but not
    /// a mutable one). [`run_with_reuse`][Self::run_with_reuse] can be used to reuse the same
    /// objective function for each run instead (allows a mutable reference).
    pub fn run<F, G>(self, get_objective_function: G) -> RestartResults
    where
        F: ObjectiveFunction,
        G: FnMut() -> F,
    {
        self.run_internal(get_objective_function, false, |state| (state.run(), false))
    }

    /// Like [`run`][Self::run], but reuses `objective_function` instead of initializing one for every
    /// run.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cmaes::restart::{RestartOptions, RestartStrategy};
    /// use cmaes::DVector;
    ///
    /// let mut counter = 0;
    /// let function = |x: &DVector<f64>| {
    ///     counter += 1;
    ///     x.magnitude()
    /// };
    ///
    /// let strategy = RestartStrategy::BIPOP(Default::default());
    /// let results = RestartOptions::new(10, -1.0..=1.0, strategy)
    ///     .build()
    ///     .unwrap()
    ///     // `function` is passed by-value here, but a mutable reference can be used as well
    ///     .run_with_reuse(function);
    ///
    /// // `counter` is updated by all runs because `function` is reused
    /// assert_eq!(results.function_evals, counter);
    /// ```
    pub fn run_with_reuse<F: ObjectiveFunction>(self, objective_function: F) -> RestartResults {
        let mut function = Some(objective_function);
        self.run_internal(
            || function.take().unwrap(),
            true,
            |state| (state.run(), false),
        )
    }

    /// Like [`run`][Self::run], but executes the objective function in parallel using multiple
    /// threads. Requires that `F` implements
    /// [`ParallelObjectiveFunction`][crate::objective_function::ParallelObjectiveFunction].
    /// [`run_parallel_with_reuse`][Self::run_parallel_with_reuse] can be used to reuse the same
    /// objective function instead of initializing one for each run.
    pub fn run_parallel<F, G>(self, get_objective_function: G) -> RestartResults
    where
        F: ParallelObjectiveFunction,
        G: FnMut() -> F,
    {
        self.run_internal(get_objective_function, false, |state| {
            (state.run_parallel(), false)
        })
    }

    /// Like [`run_parallel`][Self::run_parallel], but reuses `objective_function` instead of
    /// initializing one for every run. See [`run_with_reuse`][Self::run_with_reuse].
    pub fn run_parallel_with_reuse<F: ParallelObjectiveFunction>(
        self,
        objective_function: F,
    ) -> RestartResults {
        let mut function = Some(objective_function);
        self.run_internal(
            || function.take().unwrap(),
            true,
            |state| (state.run_parallel(), false),
        )
    }

    /// Shared logic between `Self::run_*`
    fn run_internal<F, G, R>(
        mut self,
        mut get_objective_function: G,
        reuse_objective_function: bool,
        mut run: R,
    ) -> RestartResults
    where
        G: FnMut() -> F,
        R: FnMut(&mut CMAES<F>) -> (TerminationData, bool),
    {
        // Print parameters before any runs
        if self.print_info {
            self.print_initial_info();
        }

        // Initialize results
        let time_started = Instant::now();
        let reason;
        let mut function_evals = 0;
        let mut runs = 0;
        let mut run_termination_reasons = Vec::new();
        // For storing the objective function if it's being reused
        let mut objective_function = None;

        // Repeatedly restart CMA-ES
        loop {
            // Check if the `max_runs` setting of the restart strategy is zero to avoid always
            // running it once to find out
            if self.strategy.has_zero_max_runs() {
                reason = RestartTerminationReason::MaxRuns;
                break;
            }

            // Check RestartTerminationReason::MaxFunctionEvals
            if let Some(max_function_evals) = self.max_function_evals {
                if function_evals >= max_function_evals {
                    reason = RestartTerminationReason::MaxFunctionEvals;
                    break;
                }
            }

            // Check RestartTerminationReason::MaxTime if enabled
            if let Some(max_time) = self.max_time {
                if time_started.elapsed() >= max_time {
                    reason = RestartTerminationReason::MaxTime;
                    break;
                }
            }

            // Generate new parameters
            let initial_mean = self
                .first_run_initial_mean
                .take()
                .unwrap_or_else(|| self.generate_initial_mean());
            let seed = self.rng.gen();

            // Apply default configuration (may be overridden by individual restart strategies)
            let mut options = CMAESOptions::new(initial_mean, DEFAULT_INITIAL_STEP_SIZE)
                .mode(self.mode)
                .parallel_update(self.parallel_update)
                .seed(seed);
            if let Some(minimum_population_size) = self.minimum_population_size {
                options.population_size = options.population_size.max(minimum_population_size);
            }
            let remaining_function_evals = self
                .max_function_evals
                .map(|maximum| maximum.saturating_sub(function_evals));
            options.max_function_evals =
                match (self.max_function_evals_per_run, remaining_function_evals) {
                    (Some(per_run), Some(remaining)) => Some(per_run.min(remaining)),
                    (Some(per_run), None) => Some(per_run),
                    (None, Some(remaining)) => Some(remaining),
                    (None, None) => None,
                };
            options.max_generations = self.max_generations_per_run;
            options.fun_target = self.fun_target;

            // Run CMA-ES
            let search_range_size = (self.search_range.end() - self.search_range.start()).abs();
            // Reuse the objective function if it's stored or get a fresh one otherwise
            let function = objective_function
                .take()
                .unwrap_or_else(&mut get_objective_function);
            let mut stop_after_run = false;
            let run_with_print = |cmaes: &mut CMAES<F>| {
                // Print initial parameters of the run
                if self.print_info {
                    print_run_info(runs + 1, cmaes.parameters());
                }

                let (termination, stop) = run(cmaes);
                stop_after_run = stop;
                termination
            };
            let (final_state, reasons, control) = self.strategy.next_run(
                options,
                search_range_size,
                function,
                run_with_print,
                &mut self.rng,
            );

            // Print results of the run
            if self.print_info {
                print_run_results(runs + 1, &final_state, &reasons);
            }

            // Update results
            function_evals += final_state.function_evals();
            runs += 1;
            run_termination_reasons.push(reasons.clone());

            // Check RestartTerminationReason::InvalidFunctionValue
            if reasons
                .iter()
                .any(|&r| r == TerminationReason::InvalidFunctionValue)
            {
                reason = RestartTerminationReason::InvalidFunctionValue;
                break;
            }

            if let Some(best) = final_state.overall_best_individual().cloned() {
                self.update_best_individual(best);
            }

            // Check RestartTerminationReason::FunTarget if enabled
            if reasons.iter().any(|&r| r == TerminationReason::FunTarget) {
                reason = RestartTerminationReason::FunTarget;
                break;
            }

            if stop_after_run {
                reason = RestartTerminationReason::NoImprovement;
                break;
            }

            // Check RestartTerminationReason::MaxRuns
            match control {
                RestartControl::Continue => (),
                RestartControl::MaxRunsReached => {
                    reason = RestartTerminationReason::MaxRuns;
                    break;
                }
            }

            // Reuse the objective function if enabled
            if reuse_objective_function {
                objective_function = Some(final_state.into_objective_function());
            }
        }

        let results = RestartResults {
            best: self.overall_best,
            reason,
            function_evals,
            runs,
            run_termination_reasons,
        };

        // Print overall results
        if self.print_info {
            results.print_results();
        }

        results
    }

    /// Updates the overall best individual
    fn update_best_individual(&mut self, individual: Individual) {
        match self.overall_best {
            Some(ref mut current_best) => {
                if self.mode.is_better(individual.value, current_best.value) {
                    *current_best = individual;
                }
            }
            None => self.overall_best = Some(individual),
        }
    }

    /// Prints various parameters of the `Restarter` and its `RestartStrategy`
    fn print_initial_info(&self) {
        let algorithm_name = self.strategy.get_algorithm_name();
        let parameters_str = self
            .strategy
            .get_parameters_as_strings()
            .iter()
            .map(|(name, value)| format!("{}={}", name, value))
            .collect::<Vec<_>>()
            .join(", ");
        let search_range_str = format!(
            "[{}, {}]",
            self.search_range.start(),
            self.search_range.end()
        );

        println!(
            "{} with dimension={}, search_range={}, {}, seed={}",
            algorithm_name, self.dimensions, search_range_str, parameters_str, self.seed
        );
    }

    /// Generates a random initial mean in the search range and returns it
    fn generate_initial_mean(&mut self) -> DVector<f64> {
        DVector::from_iterator(
            self.dimensions,
            (0..self.dimensions).map(|_| self.rng.gen_range(self.search_range.clone())),
        )
    }
}

/// Prints various initial parameters of a run
fn print_run_info(run: usize, params: &Parameters) {
    println!(
        "Run {} parameters: lambda={}, sigma0={}, seed={}",
        run,
        params.lambda(),
        utils::format_num(params.initial_sigma(), 12),
        params.seed()
    );
}

/// Prints the results of a run
fn print_run_results<F>(run: usize, cmaes: &CMAES<F>, termination_reasons: &[TerminationReason]) {
    let best_value_str = cmaes
        .overall_best_individual()
        .map(|ind| utils::format_num(ind.value, 19))
        .unwrap_or_else(|| "None".into());
    let reasons_str = termination_reasons
        .iter()
        .map(|r| format!("`{}`", r))
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "Run {} results: best f-val={}, termination_reasons=[{}], f-evals={}",
        run,
        best_value_str,
        reasons_str,
        cmaes.function_evals()
    );
}

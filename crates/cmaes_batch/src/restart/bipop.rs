//! A BIPOP restart strategy, performing both local restarts and restarts with increasing population
//! size

use rand::Rng;
use rand_chacha::ChaChaRng;

use super::options::InvalidRestartStrategyOptionsError;
use super::strategy::{RestartControl, Strategy};
use crate::{options, CMAESOptions, TerminationData, TerminationReason, CMAES};

/// The maximum number of IPOP runs allowed
const MAX_IPOP_RUNS: usize = 10;
/// The default population size increase factor for IPOP runs
const DEFAULT_IPOP_INCREASE_FACTOR: usize = 2;
/// The default factor for calculating the initial step size for LR runs
const DEFAULT_LR_INITIAL_STEP_SIZE_FACTOR: f64 = 0.2;

/// A BIPOP restart strategy, performing both local restarts and restarts with increasing population
/// size. Also called `BIPOP-aCMA-ES`.
///
/// Useful for most multimodal functions regardless of the existence of an exploitable global
/// structure due to combining the benefits of both approaches, but [`IPOP`][crate::restart::IPOP]
/// will likely outperform it in cases where the local restarts are not useful, and likewise for
/// [`Local`][crate::restart::Local] in cases where increasing the population size is not useful.
#[derive(Clone, Debug)]
pub struct BIPOP {
    /// The number of IPOP runs performed so far
    ipop_runs: usize,
    /// The number of function evaluations performed across all IPOP runs so far
    ipop_function_evals: usize,
    /// The current multiplier for the population size for IPOP runs
    /// The first run is always with the default population size
    ipop_current_multiplier: usize,
    /// The factor by which to increase the IPOP population size each restart
    ipop_increase_factor: usize,
    /// The number of function evaluations performed in the most recent IPOP run
    last_ipop_function_evals_used: usize,
    /// The population size multiplier used in the most recent IPOP run
    last_ipop_multiplier: usize,
    /// The factor for the search range size used to calculate the initial step size for LR runs
    lr_initial_step_size_factor: f64,
    /// The number of function evaluations performed across all IPOP runs so far
    lr_function_evals: usize,
}

impl BIPOP {
    /// Returns a new `BIPOP` with the provided parameters.
    ///
    /// - `lr_initial_step_size_factor` is the factor multiplied by the search range size `B - A`
    /// and `10^(-2 * rand(0, 1))` to calculate the initial step size for local restarts. If `None`,
    /// the default value of `0.2` is used.
    /// - `ipop_increase_factor` is the factor by which to increase the population size for each
    /// IPOP restart. If `None`, the default value of `2` is used.
    pub fn new(
        lr_initial_step_size_factor: Option<f64>,
        ipop_increase_factor: Option<usize>,
    ) -> Result<Self, InvalidRestartStrategyOptionsError> {
        if let Some(step_size_factor) = lr_initial_step_size_factor {
            // FIXME: This doesn't catch every invalid case; if set to a tiny value (maybe 1e-150),
            // the 10^-2 factor makes it round to zero and cause a panic when calling
            // CMAESOptions::build (also affects Local)
            if !options::is_initial_step_size_valid(step_size_factor) {
                return Err(InvalidRestartStrategyOptionsError::InitialStepSize);
            }
        }

        if let Some(increase_factor) = ipop_increase_factor {
            if increase_factor == 0 {
                return Err(InvalidRestartStrategyOptionsError::PopulationSize);
            }
        }

        Ok(Self {
            ipop_runs: 0,
            ipop_function_evals: 0,
            ipop_current_multiplier: 1,
            ipop_increase_factor: ipop_increase_factor.unwrap_or(DEFAULT_IPOP_INCREASE_FACTOR),
            last_ipop_function_evals_used: 0,
            last_ipop_multiplier: 1,
            lr_initial_step_size_factor: lr_initial_step_size_factor
                .unwrap_or(DEFAULT_LR_INITIAL_STEP_SIZE_FACTOR),
            lr_function_evals: 0,
        })
    }

    /// Returns the initial step size to use for LR restarts
    fn get_initial_step_size_lr(&self, search_range_size: f64, rng: &mut ChaChaRng) -> f64 {
        search_range_size * self.lr_initial_step_size_factor * 10f64.powf(-2.0 * rng.gen::<f64>())
    }

    /// Returns the population size to use for LR restarts
    fn get_population_size_lr(&self, default_population_size: usize, rng: &mut ChaChaRng) -> usize {
        // Scale the default population size to lie in the range
        // [default_popsize, min(default_popsize, last_ipop_popsize / 2)]
        let multiplier = (self.last_ipop_multiplier as f64 / 2.0)
            .powf(rng.gen::<f64>().powi(2))
            .max(1.0);
        (default_population_size as f64 * multiplier).floor() as usize
    }

    /// Returns the initial step size to use for IPOP restarts
    fn get_initial_step_size_ipop(&self, search_range_size: f64) -> f64 {
        search_range_size / 5.0
    }

    /// Executes a LR run
    fn next_run_lr<F, R: FnOnce(&mut CMAES<F>) -> TerminationData>(
        &mut self,
        mut options: CMAESOptions,
        search_range_size: f64,
        objective_function: F,
        run: R,
        rng: &mut ChaChaRng,
    ) -> (CMAES<F>, Vec<TerminationReason>, RestartControl) {
        // Configure the run
        options.initial_step_size = self.get_initial_step_size_lr(search_range_size, rng);
        options.population_size = self.get_population_size_lr(options.population_size, rng);
        // Half the most recent IPOP run's function evals is used as a limit for LR (if the
        // max_function_evals option isn't stricter)
        options.max_function_evals = options
            .max_function_evals
            .map(|max_fevals| max_fevals.min(self.last_ipop_function_evals_used / 2));

        // Execute the run
        let mut cmaes_state = options.build(objective_function).unwrap();
        let results = run(&mut cmaes_state);

        // Update internal state
        self.lr_function_evals += cmaes_state.function_evals();

        (cmaes_state, results.reasons, RestartControl::Continue)
    }

    /// Executes an IPOP run
    fn next_run_ipop<F, R: FnOnce(&mut CMAES<F>) -> TerminationData>(
        &mut self,
        mut options: CMAESOptions,
        search_range_size: f64,
        objective_function: F,
        run: R,
    ) -> (CMAES<F>, Vec<TerminationReason>, RestartControl) {
        // Configure the run
        options.initial_step_size = self.get_initial_step_size_ipop(search_range_size);
        options.population_size *= self.ipop_current_multiplier;

        // Execute the run
        let mut cmaes_state = options.build(objective_function).unwrap();
        let results = run(&mut cmaes_state);

        // Update internal state
        self.ipop_runs += 1;
        self.ipop_function_evals += cmaes_state.function_evals();
        self.last_ipop_function_evals_used = cmaes_state.function_evals();
        self.last_ipop_multiplier = self.ipop_current_multiplier;
        self.ipop_current_multiplier *= self.ipop_increase_factor;

        let control = if self.ipop_runs >= MAX_IPOP_RUNS {
            RestartControl::MaxRunsReached
        } else {
            RestartControl::Continue
        };

        (cmaes_state, results.reasons, control)
    }
}

impl Default for BIPOP {
    fn default() -> Self {
        Self::new(None, None).unwrap()
    }
}

impl Strategy for BIPOP {
    fn has_zero_max_runs(&self) -> bool {
        false
    }

    fn next_run<F, R: FnOnce(&mut CMAES<F>) -> TerminationData>(
        &mut self,
        options: CMAESOptions,
        search_range_size: f64,
        objective_function: F,
        run: R,
        rng: &mut ChaChaRng,
    ) -> (CMAES<F>, Vec<TerminationReason>, RestartControl) {
        if self.ipop_runs != 0 && self.lr_function_evals < self.ipop_function_evals {
            // Use LR if it has used less function evals so far and this is not the first run
            self.next_run_lr(options, search_range_size, objective_function, run, rng)
        } else {
            // Use IPOP otherwise
            self.next_run_ipop(options, search_range_size, objective_function, run)
        }
    }

    fn get_algorithm_name(&self) -> &'static str {
        "BIPOP-aCMA-ES"
    }

    fn get_parameters_as_strings(&self) -> Vec<(String, String)> {
        [
            (
                "lr_sigma0_factor".to_string(),
                format!("{}", self.lr_initial_step_size_factor),
            ),
            (
                "ipop_increase_factor".to_string(),
                format!("{}", self.ipop_increase_factor),
            ),
        ]
        .into()
    }
}

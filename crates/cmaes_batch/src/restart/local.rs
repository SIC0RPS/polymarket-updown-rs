//! A local restart strategy, keeping the population size constant

use rand_chacha::ChaChaRng;

use super::strategy::{RestartControl, Strategy};
use super::InvalidRestartStrategyOptionsError;
use crate::{options, CMAESOptions, TerminationData, TerminationReason, CMAES};

/// The default factor for calculating the initial step size
const DEFAULT_INITIAL_STEP_SIZE_FACTOR: f64 = 0.5;

/// A local restart strategy, keeping the population size constant and using a small initial step
/// size while only adjusting the initial mean each restart. Also called `LR-aCMA-ES`.
///
/// Useful for multimodal functions with no exploitable global structure, although
/// [`BIPOP`][crate::restart::BIPOP] may still perform better for these cases.
/// Both [`BIPOP`][crate::restart::BIPOP] and [`IPOP`][crate::restart::IPOP] will likely perform
/// better for multimodal functions with a global structure.
#[derive(Clone, Debug)]
pub struct Local {
    /// The number of runs performed so far
    runs: usize,
    /// The factor for the search range size used to calculate the initial step size
    initial_step_size_factor: f64,
    /// The maximum number of runs allowed
    max_runs: usize,
}

impl Local {
    /// Returns a new `Local` with the provided parameters.
    ///
    /// - `max_runs` is the maximum number of runs allowed.
    /// - `initial_step_size_factor` is the factor multiplied by the search range size `B - A` and
    /// `10^-2` to calculate the initial step size. If `None`, the default value of `0.5` is used.
    pub fn new(
        max_runs: usize,
        initial_step_size_factor: Option<f64>,
    ) -> Result<Self, InvalidRestartStrategyOptionsError> {
        if let Some(factor) = initial_step_size_factor {
            if !options::is_initial_step_size_valid(factor) {
                return Err(InvalidRestartStrategyOptionsError::InitialStepSize);
            }
        }

        Ok(Self {
            runs: 0,
            initial_step_size_factor: initial_step_size_factor
                .unwrap_or(DEFAULT_INITIAL_STEP_SIZE_FACTOR),
            max_runs,
        })
    }

    /// Returns the initial step size to use for the given search range size
    fn get_initial_step_size(&self, search_range_size: f64) -> f64 {
        search_range_size * self.initial_step_size_factor * 1e-2
    }
}

impl Strategy for Local {
    fn has_zero_max_runs(&self) -> bool {
        self.max_runs == 0
    }

    fn next_run<F, R: FnOnce(&mut CMAES<F>) -> TerminationData>(
        &mut self,
        mut options: CMAESOptions,
        search_range_size: f64,
        objective_function: F,
        run: R,
        _: &mut ChaChaRng,
    ) -> (CMAES<F>, Vec<TerminationReason>, RestartControl) {
        // Configure the run
        options.initial_step_size = self.get_initial_step_size(search_range_size);

        // Execute the run
        let mut cmaes_state = options.build(objective_function).unwrap();
        let results = run(&mut cmaes_state);

        // Update internal state
        self.runs += 1;

        let control = if self.runs >= self.max_runs {
            RestartControl::MaxRunsReached
        } else {
            RestartControl::Continue
        };

        (cmaes_state, results.reasons, control)
    }

    fn get_algorithm_name(&self) -> &'static str {
        "LR-aCMA-ES"
    }

    fn get_parameters_as_strings(&self) -> Vec<(String, String)> {
        [(
            "sigma0_factor".to_string(),
            format!("{}", self.initial_step_size_factor),
        )]
        .into()
    }
}

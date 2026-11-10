//! Objective function value history tracking.

use std::collections::VecDeque;

use crate::mode::Mode;
use crate::sampling::EvaluatedPoint;
use crate::Individual;

/// The maximum number of elements to store in the objective function value histories.
pub const MAX_HISTORY_LENGTH: usize = 20_000;

/// A type that tracks various histories of the objective function value, as well as the current
/// and overall best points.
pub struct History {
    /// A history of the best function values (values at the front are from more recent generations)
    best_function_values: VecDeque<f64>,
    /// A history of the median function values (values at the front are from more recent
    /// generations)
    median_function_values: VecDeque<f64>,
    /// The best individual of the latest generation
    current_best_individual: Option<Individual>,
    /// The best individual of any generation
    overall_best_individual: Option<Individual>,
    /// The median function value of the first generation
    first_median_function_value: Option<f64>,
    /// The best median function value of any generation
    best_median_function_value: Option<f64>,
}

impl History {
    pub fn new() -> Self {
        Self {
            best_function_values: VecDeque::new(),
            median_function_values: VecDeque::new(),
            current_best_individual: None,
            overall_best_individual: None,
            first_median_function_value: None,
            best_median_function_value: None,
        }
    }

    pub fn best_function_values(&self) -> &VecDeque<f64> {
        &self.best_function_values
    }

    pub fn median_function_values(&self) -> &VecDeque<f64> {
        &self.median_function_values
    }

    /// Always `Some` if `Self::update` has been called at least once
    pub fn current_best_individual(&self) -> Option<&Individual> {
        self.current_best_individual.as_ref()
    }

    /// Always `Some` if `Self::update` has been called at least once
    pub fn overall_best_individual(&self) -> Option<&Individual> {
        self.overall_best_individual.as_ref()
    }

    /// Always `Some` if `Self::update` has been called at least once
    #[cfg(feature = "plotters")]
    pub fn current_median_function_value(&self) -> Option<f64> {
        self.median_function_values.get(0).cloned()
    }

    /// Always `Some` if `Self::update` has been called at least once
    pub fn first_median_function_value(&self) -> Option<f64> {
        self.first_median_function_value
    }

    /// Always `Some` if `Self::update` has been called at least once
    pub fn best_median_function_value(&self) -> Option<f64> {
        self.best_median_function_value
    }

    /// Updates the histories based on the current generation of individuals. Assumes that
    /// `current_generation` is already sorted by objective function value.
    pub fn update(&mut self, mode: Mode, current_generation: &[EvaluatedPoint]) {
        let best = &current_generation[0];

        self.best_function_values.push_front(best.value());
        if self.best_function_values.len() > MAX_HISTORY_LENGTH {
            self.best_function_values.pop_back();
        }

        let median_value = get_median_value(current_generation);
        self.median_function_values.push_front(median_value);
        if self.median_function_values.len() > MAX_HISTORY_LENGTH {
            self.median_function_values.pop_back();
        }

        self.first_median_function_value = self.first_median_function_value.or(Some(median_value));

        match self.best_median_function_value {
            Some(ref mut value) => *value = mode.choose_best(*value, median_value),
            None => self.best_median_function_value = Some(median_value),
        }

        self.update_best_individuals(mode, Individual::new(best.point().clone(), best.value()));
    }

    /// Updates the current and overall best individuals
    fn update_best_individuals(&mut self, mode: Mode, current_best: Individual) {
        self.current_best_individual = Some(current_best.clone());

        match &mut self.overall_best_individual {
            Some(ref mut overall) => {
                if mode.is_better(current_best.value, overall.value) {
                    *overall = current_best;
                }
            }
            None => self.overall_best_individual = Some(current_best),
        }
    }
}

/// Returns the median function value of the generation
/// Assumes that `individuals` is already sorted by function value
fn get_median_value(individuals: &[EvaluatedPoint]) -> f64 {
    if individuals.len() % 2 == 0 {
        (individuals[individuals.len() / 2 - 1].value()
            + individuals[individuals.len() / 2].value())
            / 2.0
    } else {
        individuals[individuals.len() / 2].value()
    }
}

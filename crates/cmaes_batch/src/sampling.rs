//! Types related to sampling points from the distribution

use nalgebra::DVector;
use rand::distributions::Distribution;
use rand::SeedableRng;
use rand_chacha::ChaCha12Rng;
use rayon::prelude::*;
use statrs::distribution::Normal;

use crate::mode::Mode;
use crate::state::State;
use crate::{ObjectiveFunction, ParallelObjectiveFunction};

/// A type for sampling and evaluating points from the distribution for each generation
pub struct Sampler<F> {
    /// Number of dimensions to sample from
    dim: usize,
    /// Number of points to sample each generation
    population_size: usize,
    /// RNG from which all random numbers are sourced
    rng: ChaCha12Rng,
    /// The objective function to optimize, used to evaluate points
    objective_function: F,
    /// The number of times the objective function has been evaluated
    function_evals: usize,
}

impl<F> Sampler<F> {
    pub fn new(dim: usize, population_size: usize, objective_function: F, rng_seed: u64) -> Self {
        Self {
            dim,
            population_size,
            rng: ChaCha12Rng::seed_from_u64(rng_seed),
            objective_function,
            function_evals: 0,
        }
    }

    /// Shared logic between `sample` and `sample_parallel`
    fn sample_internal<
        P: Fn(Vec<DVector<f64>>, &mut F) -> Result<Vec<EvaluatedPoint>, InvalidFunctionValueError>,
    >(
        &mut self,
        state: &State,
        mode: Mode,
        parallel_update: bool,
        evaluate_points: P,
    ) -> Result<Vec<EvaluatedPoint>, InvalidFunctionValueError> {
        let normal = Normal::new(0.0, 1.0).unwrap();

        // Random steps in the distribution N(0, I)
        let z = (0..self.population_size)
            .map(|_| {
                DVector::from_iterator(
                    self.dim,
                    (0..self.dim).map(|_| normal.sample(&mut self.rng)),
                )
            })
            .collect::<Vec<_>>();
        let transform = |zk| state.cov_transform() * zk;
        let y = if parallel_update {
            z.into_par_iter().map(transform).collect()
        } else {
            z.into_iter().map(transform).collect()
        };

        // Evaluate and rank points
        let mut points = evaluate_points(y, &mut self.objective_function)?;

        self.function_evals += points.len();

        points.sort_by(|a, b| mode.sort_cmp(a.value, b.value));
        Ok(points)
    }

    pub fn function_evals(&self) -> usize {
        self.function_evals
    }

    /// Samples a generation without evaluating it. Returned vectors are unscaled steps from the
    /// current mean in the distribution N(0, cov). The caller must convert them into evaluated
    /// points before updating the CMA-ES state.
    pub fn ask_population(&mut self, state: &State, parallel_update: bool) -> Vec<DVector<f64>> {
        let normal = Normal::new(0.0, 1.0).unwrap();
        let z = (0..self.population_size)
            .map(|_| {
                DVector::from_iterator(
                    self.dim,
                    (0..self.dim).map(|_| normal.sample(&mut self.rng)),
                )
            })
            .collect::<Vec<_>>();
        let transform = |zk| state.cov_transform() * zk;
        if parallel_update {
            z.into_par_iter().map(transform).collect()
        } else {
            z.into_iter().map(transform).collect()
        }
    }

    pub fn add_function_evals(&mut self, count: usize) {
        self.function_evals += count;
    }

    /// Consumes `self` and returns the objective function
    pub fn into_objective_function(self) -> F {
        self.objective_function
    }
}

impl<F: ObjectiveFunction> Sampler<F> {
    /// Samples and returns a new generation of points, sorted in ascending order by their
    /// corresponding objective function values
    ///
    /// Returns Err if the objective function returned an invalid value
    pub fn sample(
        &mut self,
        state: &State,
        mode: Mode,
        parallel_update: bool,
    ) -> Result<Vec<EvaluatedPoint>, InvalidFunctionValueError> {
        self.sample_internal(state, mode, parallel_update, |y, objective_function| {
            y.into_iter()
                .map(|yk| {
                    EvaluatedPoint::new(yk, state.mean(), state.sigma(), |x| {
                        objective_function.evaluate(x)
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
    }
}

impl<F: ParallelObjectiveFunction> Sampler<F> {
    /// Like `sample`, but evaluates the sampled points using multiple threads
    pub fn sample_parallel(
        &mut self,
        state: &State,
        mode: Mode,
        parallel_update: bool,
    ) -> Result<Vec<EvaluatedPoint>, InvalidFunctionValueError> {
        self.sample_internal(state, mode, parallel_update, |y, objective_function| {
            y.into_par_iter()
                .map(|yk| {
                    EvaluatedPoint::new(yk, state.mean(), state.sigma(), |x| {
                        objective_function.evaluate_parallel(x)
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
    }
}

/// A point from the distribution that has been evaluated by the objective function
#[derive(Clone, Debug)]
pub struct EvaluatedPoint {
    /// The evaluated point
    point: DVector<f64>,
    /// The step from the mean of the point before scaling by sigma
    /// In the distribution N(0, cov)
    unscaled_step: DVector<f64>,
    /// The objective value at the point
    value: f64,
}

impl EvaluatedPoint {
    /// Returns a new `EvaluatedPoint` from the unscaled step from the mean, the mean, and the step
    /// size
    ///
    /// Returns `Err` if the objective function returned an invalid value
    pub fn new<F: FnMut(&DVector<f64>) -> f64>(
        unscaled_step: DVector<f64>,
        mean: &DVector<f64>,
        sigma: f64,
        mut objective_function: F,
    ) -> Result<Self, InvalidFunctionValueError> {
        let point = mean + sigma * &unscaled_step;
        let value = objective_function(&point);

        if value.is_nan() {
            Err(InvalidFunctionValueError)
        } else {
            Ok(Self {
                point,
                unscaled_step,
                value,
            })
        }
    }

    pub fn point(&self) -> &DVector<f64> {
        &self.point
    }

    pub fn unscaled_step(&self) -> &DVector<f64> {
        &self.unscaled_step
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub(crate) fn from_parts(point: DVector<f64>, unscaled_step: DVector<f64>, value: f64) -> Self {
        Self {
            point,
            unscaled_step,
            value,
        }
    }
}

/// The objective function returned an invalid value
#[derive(Clone, Debug)]
pub struct InvalidFunctionValueError;

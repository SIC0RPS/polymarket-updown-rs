use std::fmt;

use crate::sampling::EvaluatedPoint;
use crate::{DVector, TerminationData, CMAES};

/// Error returned by the batch ask/tell interface.
#[derive(Clone, Debug)]
pub enum BatchAskTellError {
    PendingPopulation,
    MissingPopulation,
    PopulationSize { expected: usize, actual: usize },
    PointDimension { expected: usize, actual: usize },
    InvalidFunctionValue,
}

impl fmt::Display for BatchAskTellError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BatchAskTellError::PendingPopulation => {
                write!(fmt, "previous CMA-ES population has not been told")
            }
            BatchAskTellError::MissingPopulation => write!(fmt, "no pending CMA-ES population"),
            BatchAskTellError::PopulationSize { expected, actual } => write!(
                fmt,
                "CMA-ES population size mismatch: expected {expected}, got {actual}"
            ),
            BatchAskTellError::PointDimension { expected, actual } => write!(
                fmt,
                "CMA-ES point dimension mismatch: expected {expected}, got {actual}"
            ),
            BatchAskTellError::InvalidFunctionValue => {
                write!(fmt, "CMA-ES objective returned NaN")
            }
        }
    }
}

impl std::error::Error for BatchAskTellError {}

impl<F> CMAES<F> {
    /// Samples one full CMA-ES generation and returns candidate vectors without evaluating the
    /// objective function.
    pub fn ask_population(&mut self) -> Result<Vec<DVector<f64>>, BatchAskTellError> {
        if self.pending_batch.is_some() {
            return Err(BatchAskTellError::PendingPopulation);
        }
        let unscaled = self
            .sampler
            .ask_population(&self.state, self.parameters.parallel_update());
        let points = unscaled
            .iter()
            .map(|step| self.state.mean() + self.state.sigma() * step)
            .collect::<Vec<_>>();
        self.pending_batch = Some(unscaled);
        Ok(points)
    }

    /// Updates CMA-ES with the fitness values for the most recent population returned by
    /// [`ask_population`][Self::ask_population]. Lower values are better in minimize mode.
    pub fn tell_population(
        &mut self,
        evaluated: Vec<(DVector<f64>, f64)>,
    ) -> Result<Option<TerminationData>, BatchAskTellError> {
        let unscaled = self
            .pending_batch
            .take()
            .ok_or(BatchAskTellError::MissingPopulation)?;
        if evaluated.len() != unscaled.len() {
            return Err(BatchAskTellError::PopulationSize {
                expected: unscaled.len(),
                actual: evaluated.len(),
            });
        }
        let dim = self.parameters.dim();
        let mut individuals = unscaled
            .into_iter()
            .zip(evaluated)
            .map(|(unscaled_step, (point, value))| {
                if point.len() != dim {
                    return Err(BatchAskTellError::PointDimension {
                        expected: dim,
                        actual: point.len(),
                    });
                }
                if value.is_nan() {
                    return Err(BatchAskTellError::InvalidFunctionValue);
                }
                Ok(EvaluatedPoint::from_parts(point, unscaled_step, value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.sampler.add_function_evals(individuals.len());
        individuals.sort_by(|a, b| self.parameters.mode().sort_cmp(a.value(), b.value()));
        self.sample_internal(&individuals);
        Ok(self.next_internal(&individuals))
    }
}

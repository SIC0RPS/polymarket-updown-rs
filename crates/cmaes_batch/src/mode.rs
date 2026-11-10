//! Handling of different optimization modes (minimize and maximize)

use std::cmp::Ordering;

use crate::utils;

/// The mode to use when optimizing a function.
///
/// The default value is `Minimize`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Minimize the value of the function.
    Minimize,
    /// Maximize the value of the function.
    Maximize,
}

impl Default for Mode {
    fn default() -> Self {
        Self::Minimize
    }
}

impl Mode {
    /// Compares the two values
    /// For use in sorting
    pub(crate) fn sort_cmp(&self, a: f64, b: f64) -> Ordering {
        match self {
            Mode::Minimize => utils::partial_cmp(a, b),
            Mode::Maximize => utils::partial_cmp(b, a),
        }
    }

    /// Returns whether `a` is a better function value than `b`
    pub(crate) fn is_better(&self, a: f64, b: f64) -> bool {
        match self {
            Mode::Minimize => a < b,
            Mode::Maximize => a > b,
        }
    }

    pub(crate) fn choose_best(&self, a: f64, b: f64) -> f64 {
        match self {
            Mode::Minimize => a.min(b),
            Mode::Maximize => a.max(b),
        }
    }
}

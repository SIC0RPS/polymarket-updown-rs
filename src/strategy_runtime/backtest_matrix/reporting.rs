use super::*;

#[path = "reporting/metrics.rs"]
mod metrics;
#[path = "reporting/render.rs"]
mod render;

pub(super) use metrics::matrix_diagnostics;
pub(crate) use render::render_report;

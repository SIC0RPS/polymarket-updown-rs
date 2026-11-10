use std::path::Path;

use anyhow::Result;

use crate::optimizer_report::{OptimizerStatusArtifact, OptimizerSummaryArtifact};
use crate::strategy_runtime::backtest_matrix_report::BacktestMatrixReportArtifact;
use crate::trade_session_ledger::LiveSessionReportArtifact;

mod backtest;
mod ctf_exit;
mod live;
mod optimizer;
mod optimizer_objective;
mod table;

pub(crate) use ctf_exit::{aggregate as aggregate_exit_type_pnl, SymbolExitAgg};

pub fn write_live_report(path: &Path, report: &LiveSessionReportArtifact) -> Result<()> {
    table::write(path, &live::render(report))
}

pub fn write_backtest_report(path: &Path, report: &BacktestMatrixReportArtifact) -> Result<()> {
    table::write(path, &backtest::render(report))
}

pub fn write_optimizer_status_report(path: &Path, report: &OptimizerStatusArtifact) -> Result<()> {
    table::write(path, &optimizer::render_status(report))
}

pub fn write_optimizer_summary_report(
    path: &Path,
    report: &OptimizerSummaryArtifact,
) -> Result<()> {
    table::write(path, &render_optimizer_summary_report(report))
}

pub fn render_optimizer_summary_report(report: &OptimizerSummaryArtifact) -> String {
    optimizer::render_summary(report, false, false)
}

pub fn optimizer_summary_report_matches(
    report: &OptimizerSummaryArtifact,
    retained_markdown: &str,
) -> bool {
    retained_markdown == render_optimizer_summary_report(report)
        || retained_markdown == optimizer::render_summary(report, true, false)
        || retained_markdown == optimizer::render_summary(report, true, true)
}

pub fn render_optimizer_summary_correction(report: &OptimizerSummaryArtifact) -> String {
    optimizer::render_correction(report)
}

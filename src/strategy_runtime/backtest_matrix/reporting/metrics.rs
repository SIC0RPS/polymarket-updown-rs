use super::*;

pub(in crate::strategy_runtime::backtest_matrix) fn matrix_diagnostics(
    rows: &[SymbolRow],
) -> BacktestMatrixDiagnostics {
    let mut diagnostics = BacktestMatrixDiagnostics::default();
    for row in rows {
        diagnostics.symbols.push(BacktestSymbolDiagnostics {
            symbol: row.symbol.clone(),
            status: row.status.clone(),
        });
        match row.status.as_str() {
            "PASS" => {
                diagnostics.runnable = diagnostics.runnable.saturating_add(1);
                diagnostics.passed = diagnostics.passed.saturating_add(1);
            }
            "SKIP" => diagnostics.skipped = diagnostics.skipped.saturating_add(1),
            _ => diagnostics.runnable = diagnostics.runnable.saturating_add(1),
        }
    }
    diagnostics
}

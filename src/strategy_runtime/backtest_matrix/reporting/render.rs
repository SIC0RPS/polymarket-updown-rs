use super::*;

pub(crate) fn render_report(
    capture: &Path,
    root: &Path,
    rows: &[SymbolRow],
    canonical_session_stats: &CanonicalSessionStats,
    portfolio_accounting: &crate::portfolio::PortfolioAccountingProjectionProof,
    failed: bool,
) -> String {
    let diagnostics = matrix_diagnostics(rows);
    let mut lines = vec![format!(
        "BACKTEST capture={} out={} failed={failed}",
        capture
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("capture"),
        root.display()
    )];
    for row in rows {
        lines.push(format!(
            "{} runtime_status={}{}",
            row.symbol,
            row.status,
            row.error
                .as_deref()
                .map(|error| format!(" error={error}"))
                .unwrap_or_default()
        ));
    }
    let total = canonical_session_stats.canonical.total_summary();
    lines.push(format!(
        "CANONICAL_SESSION_STATS schema_version={} revision={} closed_cohorts={} open_cohorts={} closed_principal={} open_principal={} open_shares={} open_sell_proceeds={} open_terminal_receipts={} open_fees={} open_direct_costs={} open_unrecovered_cash_status={} open_unrecovered_cash={} total_realized_pnl_status={} total_realized_pnl={} split_entries={} submitted_orders={} resolved_decisions={} emitted_side_decisions={} emitted_side_correct={} abstained_decisions={} wrong_side_first_sells={} violations={}",
        canonical_session_stats.schema_version,
        canonical_session_stats.canonical.revision,
        total.closed_cohort_count,
        total.open_cohort_count,
        fixed_u128_6dp(total.closed_cohort_principal_units_6dp),
        fixed_u128_6dp(total.open_cohort_principal_units_6dp),
        fixed_u128_6dp(total.open_share_quantity_units_6dp),
        fixed_i128_6dp(total.open_confirmed_sell_proceeds_units_6dp),
        fixed_i128_6dp(total.open_confirmed_terminal_receipts_units_6dp),
        fixed_u128_6dp(total.open_confirmed_fee_units_6dp),
        fixed_u128_6dp(total.open_confirmed_approved_direct_cost_units_6dp),
        total.open_unrecovered_cash.status(),
        optional_i128_6dp(total.open_unrecovered_cash.units_6dp()),
        total.realized_pnl.status(),
        optional_i128_6dp(total.realized_pnl.units_6dp()),
        total.split_entries,
        total.submitted_orders,
        optional_u64(canonical_session_stats.resolved_decisions),
        optional_u64(canonical_session_stats.emitted_side_decisions),
        optional_u64(canonical_session_stats.emitted_side_correct),
        optional_u64(canonical_session_stats.abstained_decisions),
        optional_u64(canonical_session_stats.wrong_side_first_sells),
        canonical_session_stats.violations.len(),
    ));
    let endpoint = portfolio_accounting.row_for_symbol("TOTAL");
    lines.push(format!(
        "ENDPOINT_ACCOUNTING complete={} marked_remaining_value={} receipt_realized_pnl={} open_pnl={} total_pnl={} portfolio_revision={} canonical_revision={} odds_revision={} valued_at_ms={} gaps={}",
        portfolio_accounting.is_complete(),
        optional_u128_6dp(endpoint.and_then(|row| row.marked_remaining_value_units_6dp)),
        optional_i128_6dp(endpoint.and_then(|row| row.receipt_realized_pnl_units_6dp)),
        optional_i128_6dp(endpoint.and_then(|row| row.open_pnl_units_6dp)),
        optional_i128_6dp(endpoint.and_then(|row| row.total_pnl_units_6dp)),
        portfolio_accounting.portfolio_revision,
        optional_u64(portfolio_accounting.canonical_revision),
        optional_u64(portfolio_accounting.odds_revision),
        portfolio_accounting.valued_at_ms,
        portfolio_accounting.gaps.len(),
    ));
    lines.push(format!(
        "RUNTIME_TOTAL runnable={} passed={} skipped={}",
        diagnostics.runnable, diagnostics.passed, diagnostics.skipped
    ));
    lines.join("\n") + "\n"
}

fn optional_i128_6dp(value: Option<i128>) -> String {
    value
        .map(fixed_i128_6dp)
        .unwrap_or_else(|| "NA".to_string())
}

fn optional_u128_6dp(value: Option<u128>) -> String {
    value
        .map(fixed_u128_6dp)
        .unwrap_or_else(|| "NA".to_string())
}

fn fixed_i128_6dp(value: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs();
    format!(
        "{sign}{}.{:06}",
        magnitude / 1_000_000,
        magnitude % 1_000_000
    )
}

fn fixed_u128_6dp(value: u128) -> String {
    format!("{}.{:06}", value / 1_000_000, value % 1_000_000)
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NA".to_string())
}

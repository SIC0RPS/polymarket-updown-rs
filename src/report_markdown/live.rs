use crate::trade_session_ledger::LiveSessionReportArtifact;

use super::table::{document, section, table};

pub(super) fn render(report: &LiveSessionReportArtifact) -> String {
    let mut body = document("Live Session Report");
    section(&mut body, "Session Summary");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["schema_version".into(), report.schema_version.to_string()],
            vec!["run_id".into(), report.run_id.clone()],
            vec!["projection_source".into(), report.projection_source.clone()],
            vec![
                "projection_version".into(),
                report.projection_version.to_string(),
            ],
            vec!["market_count".into(), report.stats.market_count.to_string()],
            vec!["trade_count".into(), report.stats.trade_count.to_string()],
            vec!["event_count".into(), report.stats.event_count.to_string()],
            vec![
                "stats_revision".into(),
                report.stats.canonical.revision.to_string(),
            ],
            vec![
                "accounting_generation".into(),
                report.accounting_generation.to_string(),
            ],
        ],
    );
    super::ctf_exit::render(
        &mut body,
        &report.stats.canonical.ctf_exit_summary,
        &report.stats.canonical.ctf_exit_cohorts,
        Some(&report.stats),
        None,
    );
    section(&mut body, "Violations");
    table(
        &mut body,
        &["violation"],
        report
            .stats
            .violations
            .iter()
            .map(|violation| vec![violation.clone()])
            .collect(),
    );
    body
}

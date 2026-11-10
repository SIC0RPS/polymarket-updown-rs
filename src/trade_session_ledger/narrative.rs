use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{trade_session_ledger_path_for_run, trade_session_ledger_run_dir};
use super::{
    CanonicalClosedCohortStats, CanonicalCohortCashflow, CanonicalFinancialProjection,
    CanonicalSessionStats, CanonicalUnresolvedCohortState, CanonicalUnresolvedCohortStats,
};
use crate::event_ledger::current_run_id;
use crate::report_markdown::write_live_report;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LiveSessionReportArtifact {
    pub schema_version: u32,
    pub projection_source: String,
    pub projection_version: u64,
    pub run_id: String,
    pub accounting_generation: u64,
    pub stats: CanonicalSessionStats,
}

static LIVE_SESSION_REPORT: OnceLock<watch::Sender<Option<LiveSessionReportArtifact>>> =
    OnceLock::new();
static SESSION_REPORT_PROJECTION_LOCK: Mutex<()> = Mutex::new(());

fn live_session_report_sender() -> &'static watch::Sender<Option<LiveSessionReportArtifact>> {
    LIVE_SESSION_REPORT.get_or_init(|| watch::channel(None).0)
}

pub fn live_session_report_receiver() -> watch::Receiver<Option<LiveSessionReportArtifact>> {
    live_session_report_sender().subscribe()
}

fn publish_live_session_report(artifact: LiveSessionReportArtifact) -> Result<()> {
    let mut pending = Some(artifact);
    let mut invalid_update = None;
    live_session_report_sender().send_if_modified(|current| {
        let candidate = pending.as_ref().expect("pending canonical session report");
        let replace = match current.as_ref() {
            None => true,
            Some(existing) if existing.run_id != candidate.run_id => true,
            Some(existing) => match (
                candidate.stats.canonical.revision,
                candidate.accounting_generation,
            )
                .cmp(&(
                    existing.stats.canonical.revision,
                    existing.accounting_generation,
                ))
            {
                Ordering::Greater => true,
                Ordering::Equal if existing == candidate => false,
                Ordering::Equal => {
                    let mut enriched = existing.clone();
                    let preserves_known_outcomes = enriched
                        .stats
                        .canonical
                        .ctf_exit_cohorts
                        .iter_mut()
                        .zip(&candidate.stats.canonical.ctf_exit_cohorts)
                        .all(|(retained, updated)| {
                            if retained.venue_winner_side.as_ref().is_some_and(|side| {
                                side.is_some()
                                    && updated.venue_winner_side.as_ref() != Some(side)
                            }) {
                                return false;
                            }
                            retained.venue_winner_side = updated.venue_winner_side.clone();
                            retained.venue_resolution_observed_at_ms =
                                updated.venue_resolution_observed_at_ms;
                            retained.venue_resolution_provenance =
                                updated.venue_resolution_provenance.clone();
                            true
                        });
                    if preserves_known_outcomes && enriched == *candidate {
                        return {
                            *current = pending.take();
                            true
                        };
                    }
                    invalid_update = Some(format!(
                        "conflicting canonical session report revision run_id={} revision={}",
                        candidate.run_id, candidate.stats.canonical.revision
                    ));
                    false
                }
                Ordering::Less => {
                    invalid_update = Some(format!(
                        "canonical session report revision regressed run_id={} current={} candidate={}",
                        candidate.run_id,
                        existing.stats.canonical.revision,
                        candidate.stats.canonical.revision
                    ));
                    false
                }
            },
        };
        if replace {
            *current = pending.take();
        }
        replace
    });
    ensure!(
        invalid_update.is_none(),
        "{}",
        invalid_update.unwrap_or_default()
    );
    Ok(())
}

impl LiveSessionReportArtifact {
    fn with_stats(
        projection_source: String,
        projection_version: u64,
        run_id: String,
        accounting_generation: u64,
        stats: CanonicalSessionStats,
    ) -> Self {
        Self {
            schema_version: 10,
            projection_source,
            projection_version,
            run_id,
            accounting_generation,
            stats,
        }
    }
}

pub fn write_trade_ledger_stats_sidecars(run_id: Option<&str>) -> Result<Option<PathBuf>> {
    let Some(artifact) = snapshot_live_session_report(run_id, None)? else {
        return Ok(None);
    };
    write_live_session_report_artifact(&artifact).map(Some)
}

pub(crate) fn snapshot_live_session_report(
    run_id: Option<&str>,
    odds: Option<&crate::polymarket::state::PolymarketOddsState>,
) -> Result<Option<LiveSessionReportArtifact>> {
    let _projection_guard = SESSION_REPORT_PROJECTION_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("session report projection lock poisoned"))?;
    let Some(path) = select_session_path(run_id)? else {
        return Ok(None);
    };
    let resolved_run_id = path_run_id(&path)
        .context("selected trade ledger session path has no canonical run identity")?;
    ensure!(
        current_run_id().is_ok_and(|active_run_id| active_run_id == resolved_run_id),
        "session report requires the resident current run run_id={resolved_run_id}"
    );
    let snapshot = super::live_accounting::snapshot_live_accounting(&resolved_run_id)?;
    let projected = super::live_accounting::project_live_accounting_snapshot(snapshot)?;
    let generation = projected.generation;
    let mut stats = projected.canonical;
    merge_resident_financial_projection(&mut stats, generation.financial.as_ref())?;
    if let Some(odds) = odds {
        stats.project_venue_outcomes(None, Some(odds));
    }
    let artifact = LiveSessionReportArtifact::with_stats(
        generation.projection_source.clone(),
        generation.projection_version,
        generation.run_id.clone(),
        generation.generation,
        stats,
    );
    publish_live_session_report(artifact.clone())?;
    Ok(Some(artifact))
}

fn merge_resident_financial_projection(
    stats: &mut CanonicalSessionStats,
    resident: &CanonicalFinancialProjection,
) -> Result<()> {
    ensure!(
        stats.canonical.revision == resident.revision,
        "resident financial revision differs from canonical report revision"
    );
    ensure!(
        stats.canonical.ctf_summary.as_slice() == resident.ctf_summary.as_ref(),
        "resident financial summary differs from canonical report summary"
    );

    let mut resident_closed = BTreeMap::new();
    for cohort in resident.closed_cohorts.iter() {
        ensure!(
            resident_closed
                .insert(cohort.paired_cohort_key.as_str(), cohort.as_ref())
                .is_none(),
            "resident closed cohort identity repeated cohort_id={}",
            cohort.paired_cohort_key
        );
    }
    for canonical in &mut stats.closed_cohorts {
        let cohort_id = canonical.paired_cohort_key.as_str();
        let resident = resident_closed
            .remove(cohort_id)
            .with_context(|| format!("resident closed cohort missing cohort_id={cohort_id}"))?;
        merge_closed_financial_values(canonical, resident)?;
    }
    ensure!(
        resident_closed.is_empty(),
        "resident financial projection has unreported closed cohorts"
    );

    let mut resident_unresolved = BTreeMap::new();
    for cohort in resident.unresolved_cohorts.iter() {
        ensure!(
            resident_unresolved
                .insert(cohort.paired_cohort_key.as_str(), cohort.as_ref())
                .is_none(),
            "resident unresolved cohort identity repeated cohort_id={}",
            cohort.paired_cohort_key
        );
    }
    for canonical in &mut stats.unresolved_cohorts {
        let cohort_id = canonical.paired_cohort_key.as_str();
        let resident = resident_unresolved
            .remove(cohort_id)
            .with_context(|| format!("resident unresolved cohort missing cohort_id={cohort_id}"))?;
        merge_unresolved_financial_values(canonical, resident)?;
    }
    ensure!(
        resident_unresolved.is_empty(),
        "resident financial projection has unreported unresolved cohorts"
    );

    stats.canonical.ctf_summary = resident.ctf_summary.to_vec();
    Ok(())
}

fn merge_closed_financial_values(
    canonical: &mut CanonicalClosedCohortStats,
    resident: &CanonicalClosedCohortStats,
) -> Result<()> {
    let cohort_id = canonical.paired_cohort_key.as_str();
    ensure!(
        canonical.symbol == resident.symbol
            && canonical.market_id == resident.market_id
            && canonical.paired_cohort_key == resident.paired_cohort_key
            && canonical.closed_outcome_share_quantity_units_6dp
                == resident.closed_outcome_share_quantity_units_6dp
            && canonical.realized_pnl_units_6dp == resident.realized_pnl_units_6dp,
        "resident closed financial values differ from canonical report cohort_id={cohort_id}"
    );
    merge_cashflow_financial_values(&mut canonical.cashflow, &resident.cashflow, cohort_id)?;
    canonical.closed_outcome_share_quantity_units_6dp =
        resident.closed_outcome_share_quantity_units_6dp;
    canonical.realized_pnl_units_6dp = resident.realized_pnl_units_6dp;
    Ok(())
}

fn merge_unresolved_financial_values(
    canonical: &mut CanonicalUnresolvedCohortStats,
    resident: &CanonicalUnresolvedCohortStats,
) -> Result<()> {
    let cohort_id = canonical.paired_cohort_key.as_str();
    ensure!(
        canonical.symbol == resident.symbol
            && canonical.market_id == resident.market_id
            && canonical.paired_cohort_key == resident.paired_cohort_key,
        "resident unresolved financial identity differs from canonical report cohort_id={cohort_id}"
    );
    let both_invalid = matches!(
        (&canonical.state, &resident.state),
        (
            CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. },
            CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. }
        )
    );
    if both_invalid {
        return Ok(());
    }
    match (&mut canonical.state, &resident.state) {
        (
            CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                cashflow: canonical,
            },
            CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                cashflow: resident,
            },
        ) => merge_cashflow_financial_values(canonical, resident, cohort_id),
        _ => anyhow::bail!(
            "resident unresolved financial state differs from canonical report cohort_id={cohort_id}"
        ),
    }
}

fn merge_cashflow_financial_values(
    canonical: &mut CanonicalCohortCashflow,
    resident: &CanonicalCohortCashflow,
    cohort_id: &str,
) -> Result<()> {
    let audit_events = std::mem::take(&mut canonical.events);
    let accepted_orders = std::mem::take(&mut canonical.order_submissions);
    let mut published = resident.clone();
    published.events.clear();
    published.order_submissions.clear();
    ensure!(
        *canonical == published,
        "resident cashflow differs from canonical report cohort_id={cohort_id}"
    );
    published.events = audit_events;
    published.order_submissions = accepted_orders;
    *canonical = published;
    Ok(())
}

pub(crate) fn write_live_session_report_artifact(
    artifact: &LiveSessionReportArtifact,
) -> Result<PathBuf> {
    let run_dir = trade_session_ledger_run_dir(&artifact.run_id);
    fs::create_dir_all(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let stats_path = run_dir.join("session_stats.json");
    write_json_atomic(&stats_path, artifact)?;
    let md_path = run_dir.join("session_stats.md");
    write_live_report(&md_path, artifact)
        .with_context(|| format!("write {}", md_path.display()))?;
    Ok(md_path)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| format!("replace {}", path.display()))
}

fn select_session_path(run_id: Option<&str>) -> Result<Option<PathBuf>> {
    let Ok(active_run_id) = current_run_id() else {
        return Ok(None);
    };
    if let Some(requested) = run_id.map(str::trim).filter(|value| !value.is_empty()) {
        ensure!(
            requested == active_run_id,
            "session report cannot reconstruct a prior run in process requested={requested} active={active_run_id}"
        );
    }
    let path = trade_session_ledger_path_for_run(&active_run_id);
    Ok(path.exists().then_some(path))
}

fn path_run_id(path: &Path) -> Option<String> {
    path.parent()
        .and_then(|value| value.file_name())
        .and_then(|value| value.to_str())
        .map(str::to_string)
}

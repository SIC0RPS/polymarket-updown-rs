use crate::optimizer_report::{
    OptimizerCandidateReport, OptimizerCaptureReport, OptimizerStatusArtifact,
    OptimizerStructuralReport, OptimizerSummaryArtifact, OptimizerSymbolSearchReport,
    OptimizerValidationReport,
};

use super::optimizer_objective::{
    candidate_table, economic_table, has_partial_symbol_scope, objective_table,
};
use super::table::{display_option, document, section, table};

pub(super) fn render_status(report: &OptimizerStatusArtifact) -> String {
    let mut body = document("Optimizer Status");
    let selected_learning_bindings = report
        .provenance
        .as_ref()
        .and_then(|provenance| provenance.selected_learning_bindings.as_ref());
    section(&mut body, "Status");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["schema_version".into(), report.schema_version.to_string()],
            vec!["phase".into(), report.phase.clone()],
            vec!["run_root".into(), report.run_root.clone()],
            vec!["search_trials".into(), report.search_trials.to_string()],
            vec![
                "exercised_trials".into(),
                report.exercised_trials.to_string(),
            ],
            vec!["selection_status".into(), report.selection_status.clone()],
            vec!["validation_status".into(), report.validation_status.clone()],
            vec!["structural_status".into(), report.structural_status.clone()],
            vec!["economic_status".into(), report.economic_status.clone()],
            vec![
                "application_status".into(),
                report.application_status.clone(),
            ],
            vec![
                "application_reason".into(),
                report.application_reason.clone(),
            ],
            vec!["applied".into(), report.applied.to_string()],
        ],
    );
    objective_table(
        &mut body,
        &report.objective_profile,
        report.provenance.as_ref(),
        false,
        false,
    );
    provenance_tables(&mut body, report.provenance.as_ref());
    capture_table(&mut body, &report.captures);
    symbol_search_table(&mut body, &report.symbol_searches);
    candidate_table(
        &mut body,
        "Retained Candidate Results",
        [
            ("frozen_incumbent", report.incumbent_candidate.as_ref()),
            ("selected_candidate", report.selected_candidate.as_ref()),
            ("rechecked_candidate", report.rechecked_candidate.as_ref()),
        ],
    );
    if let Some(validation) = &report.validation {
        let aggregate = report.economic_status != validation.economic_status();
        validation_status_table(&mut body, validation, false, aggregate);
        economic_table(&mut body, validation, selected_learning_bindings, aggregate);
    }
    for replay in &report.capture_replays {
        render_status_ctf_scope(
            &mut body,
            Some(&replay.capture),
            &report.phase,
            "retained_child",
            replay.candidate.as_ref(),
        );
    }
    let selection_capture = report
        .captures
        .iter()
        .find(|capture| capture.role == "selection");
    for (role, candidate) in [
        ("selection_incumbent", report.incumbent_candidate.as_ref()),
        (
            "selection_candidate",
            report
                .rechecked_candidate
                .as_ref()
                .or(report.selected_candidate.as_ref()),
        ),
    ] {
        render_status_ctf_scope(&mut body, selection_capture, &report.phase, role, candidate);
    }
    if let Some(validation) = &report.validation {
        for (role, candidate) in [
            ("validation_incumbent", validation.incumbent.as_ref()),
            ("validation_candidate", validation.candidate.as_ref()),
        ] {
            render_status_ctf_scope(
                &mut body,
                validation.capture.as_ref(),
                &report.phase,
                role,
                candidate,
            );
        }
    }
    body
}

pub(super) fn render_summary(
    report: &OptimizerSummaryArtifact,
    original_wording: bool,
    later_validation_wording: bool,
) -> String {
    let mut body = document("Automatic Optimization Report");
    let selected_learning_bindings = report
        .provenance
        .as_ref()
        .and_then(|provenance| provenance.selected_learning_bindings.as_ref());
    section(&mut body, "Summary");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["schema_version".into(), report.schema_version.to_string()],
            vec!["status".into(), report.status.clone()],
            vec!["run_root".into(), report.run_root.clone()],
            vec![
                "candidate_result_path".into(),
                report.candidate_result_path.clone(),
            ],
            vec!["search_trials".into(), report.search_trials.to_string()],
            vec![
                "exercised_trials".into(),
                report.exercised_trials.to_string(),
            ],
            vec!["selection_status".into(), report.selection_status.clone()],
            vec!["validation_status".into(), report.validation_status.clone()],
            vec!["structural_status".into(), report.structural_status.clone()],
            vec!["economic_status".into(), report.economic_status.clone()],
            vec![
                "application_status".into(),
                report.application_status.clone(),
            ],
            vec![
                "application_reason".into(),
                report.application_reason.clone(),
            ],
            vec!["applied".into(), report.applied.to_string()],
        ],
    );
    objective_table(
        &mut body,
        &report.objective_profile,
        report.provenance.as_ref(),
        original_wording,
        later_validation_wording,
    );
    provenance_tables(&mut body, report.provenance.as_ref());
    capture_table(&mut body, &report.captures);
    symbol_search_table(&mut body, &report.symbol_searches);
    candidate_table(
        &mut body,
        "Final Candidate Integration",
        [
            ("frozen_incumbent", report.incumbent_candidate.as_ref()),
            ("selected_candidate", report.selected_candidate.as_ref()),
            ("rechecked_candidate", report.rechecked_candidate.as_ref()),
        ],
    );
    if let Some(validation) = &report.validation {
        let aggregate = report.economic_status != validation.economic_status();
        validation_status_table(&mut body, validation, later_validation_wording, aggregate);
        economic_table(&mut body, validation, selected_learning_bindings, aggregate);
    }
    if let Some(structural) = &report.structural_recheck {
        structural_table(&mut body, structural);
    }
    render_ctf_scope(
        &mut body,
        report
            .captures
            .iter()
            .find(|capture| capture.role == "selection"),
        "selection_incumbent",
        report.incumbent_candidate.as_ref(),
    );
    render_ctf_scope(
        &mut body,
        report
            .captures
            .iter()
            .find(|capture| capture.role == "selection"),
        "selection_candidate",
        report
            .rechecked_candidate
            .as_ref()
            .or(report.selected_candidate.as_ref()),
    );
    if let Some(validation) = &report.validation {
        render_ctf_scope(
            &mut body,
            validation.capture.as_ref(),
            "validation_incumbent",
            validation.incumbent.as_ref(),
        );
        render_ctf_scope(
            &mut body,
            validation.capture.as_ref(),
            "validation_candidate",
            validation.candidate.as_ref(),
        );
    }
    body
}

pub(super) fn render_correction(report: &OptimizerSummaryArtifact) -> String {
    let mut body = document("Historical Optimizer Report Correction");
    section(&mut body, "Correction Scope");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["run_root".into(), report.run_root.clone()],
            vec![
                "source".into(),
                "sealed candidate proof and original paired summary".into(),
            ],
            vec![
                "original_artifacts".into(),
                "preserved; this projection is not persisted".into(),
            ],
        ],
    );
    provenance_tables(&mut body, report.provenance.as_ref());
    capture_table(&mut body, &report.captures);
    if let Some(structural) = &report.structural_recheck {
        structural_capture_table(&mut body, structural);
    }
    body
}

fn provenance_tables(
    body: &mut String,
    provenance: Option<&crate::optimizer_report::OptimizerRunProvenanceReport>,
) {
    let Some(provenance) = provenance else {
        return;
    };
    section(body, "Run Provenance");
    let mut run_rows = vec![
        vec![
            "optimizer_binary_sha256".into(),
            provenance.optimizer_binary_sha256.clone(),
        ],
        vec![
            "app_config_sha256".into(),
            provenance.app_config_sha256.clone(),
        ],
        vec![
            "root_spec_sha256".into(),
            provenance.root_spec_sha256.clone(),
        ],
    ];
    if provenance.selected_learning_bindings.is_none() {
        run_rows.extend([
            vec![
                "selection_final_oracle_learning_sha256".into(),
                display_option(provenance.selection_final_oracle_learning_sha256.as_deref()),
            ],
            vec![
                "validation_final_oracle_learning_sha256".into(),
                display_option(
                    provenance
                        .validation_final_oracle_learning_sha256
                        .as_deref(),
                ),
            ],
        ]);
    }
    table(body, &["field", "value"], run_rows);
    section(body, "Capture-Local Starting Learners");
    table(
        body,
        &[
            "role",
            "capture",
            "bundle_sha256",
            "symbol",
            "learner_sha256",
            "knowledge_cutoff_ms",
        ],
        provenance
            .capture_learners
            .iter()
            .flat_map(|capture| {
                capture.initial_oracle_learning_sha256_by_symbol.iter().map(
                    move |(symbol, sha256)| {
                        vec![
                            capture.role.clone(),
                            capture.capture.clone(),
                            capture.initial_oracle_learning_sha256.clone(),
                            symbol.clone(),
                            sha256.clone(),
                            capture
                                .initial_oracle_learning_cutoffs_ms
                                .get(symbol)
                                .map(u64::to_string)
                                .unwrap_or_else(|| "NA".into()),
                        ]
                    },
                )
            })
            .collect(),
    );
    let Some(selected_learning_bindings) = &provenance.selected_learning_bindings else {
        return;
    };
    section(body, "Replay-Final Learner Bundles");
    table(
        body,
        &["role", "capture", "bundle_sha256"],
        [
            (
                "selection",
                provenance.selection_final_oracle_learning_sha256.as_ref(),
            ),
            (
                "validation",
                provenance.validation_final_oracle_learning_sha256.as_ref(),
            ),
        ]
        .into_iter()
        .filter_map(|(role, sha256)| {
            sha256.map(|sha256| {
                vec![
                    role.to_string(),
                    provenance
                        .capture_learners
                        .iter()
                        .find(|capture| capture.role == role)
                        .map(|capture| capture.capture.clone())
                        .unwrap_or_else(|| "NA".into()),
                    sha256.clone(),
                ]
            })
        })
        .collect(),
    );
    section(body, "Learner Promotion Identity");
    table(
        body,
        &["field", "value"],
        vec![
            vec![
                "validation_replay_seed".into(),
                "capture B uses B's captured initial learner seed".into(),
            ],
            vec![
                "promotion_export".into(),
                "promoted symbols export capture A replay-final learners".into(),
            ],
            vec![
                "identity_relation".into(),
                "A replay-final and B captured-initial identities are separate; no equivalence is asserted"
                    .into(),
            ],
            vec![
                "selected_candidate_scope".into(),
                if has_partial_symbol_scope(Some(provenance)) {
                    "selected candidate and CTF PnL describe the frozen participating-symbol replay; deployment may promote a subset"
                } else {
                    "selected candidate and CTF PnL describe the full frozen four-map replay, not the deployed subset outcome"
                }.into(),
            ],
        ],
    );
    section(body, "Promoted A-Final Learner Bindings");
    table(
        body,
        &["symbol", "path", "sha256"],
        selected_learning_bindings
            .iter()
            .map(|(symbol, binding)| {
                vec![
                    symbol.clone(),
                    binding.path.display().to_string(),
                    binding.sha256.clone(),
                ]
            })
            .collect(),
    );
}

fn capture_table(body: &mut String, captures: &[OptimizerCaptureReport]) {
    section(body, "Frozen Capture Inputs");
    let scoped = captures.iter().any(|capture| {
        capture.capture_duration_ms.is_some() || capture.replay_duration_ms.is_some()
    });
    let columns = if scoped {
        vec![
            "role",
            "capture",
            "session_id",
            "run_id",
            "capture_duration_ms",
            "replay_duration_ms",
        ]
    } else {
        vec!["role", "capture", "session_id", "run_id", "duration_ms"]
    };
    table(
        body,
        &columns,
        captures
            .iter()
            .map(|capture| {
                let mut row = vec![
                    capture.role.clone(),
                    capture.capture.clone(),
                    capture.session_id.clone(),
                    capture.run_id.clone(),
                ];
                if scoped {
                    row.push(
                        capture
                            .capture_duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "NA".into()),
                    );
                    row.push(
                        capture
                            .replay_duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "NA".into()),
                    );
                } else {
                    row.push(capture.duration_ms.to_string());
                }
                row
            })
            .collect(),
    );
}

fn symbol_search_table(body: &mut String, searches: &[OptimizerSymbolSearchReport]) {
    if searches.is_empty() {
        return;
    }
    section(body, "Sequential Symbol Searches");
    table(
        body,
        &[
            "symbol",
            "status",
            "seed",
            "configured_evaluations",
            "configured_no_improvement",
            "search_evaluations",
            "no_improvement_evaluations",
            "search_runs",
            "exercised_trials",
            "default_candidate",
            "selected_candidate",
            "artifact_identity",
            "error",
        ],
        searches
            .iter()
            .map(|search| {
                vec![
                    search.symbol.clone(),
                    search.status.clone(),
                    search.seed.to_string(),
                    search.configured_max_function_evals.to_string(),
                    search.configured_max_no_improvement_evals.to_string(),
                    search.search_evaluations.to_string(),
                    search.no_improvement_evaluations.to_string(),
                    search.search_runs.to_string(),
                    search.exercised_trials.to_string(),
                    search.default_candidate_id.clone(),
                    search.selected_candidate_id.clone(),
                    search.artifact_identity.clone(),
                    display_option(search.error.as_deref()),
                ]
            })
            .collect(),
    );
}

fn validation_status_table(
    body: &mut String,
    validation: &OptimizerValidationReport,
    later_validation_wording: bool,
    aggregate: bool,
) {
    section(
        body,
        if later_validation_wording {
            "Later Capture Validation"
        } else {
            "Cross-Capture Validation"
        },
    );
    table(
        body,
        &["field", "value"],
        vec![
            vec!["status".into(), validation.status.clone()],
            vec![
                "capture".into(),
                validation
                    .capture
                    .as_ref()
                    .map(|capture| capture.capture.clone())
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec![
                "incumbent_candidate_id".into(),
                validation
                    .incumbent
                    .as_ref()
                    .map(|candidate| candidate.candidate_id.clone())
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec![
                "selected_candidate_id".into(),
                validation
                    .candidate
                    .as_ref()
                    .map(|candidate| candidate.candidate_id.clone())
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec![
                "all_symbols_pass".into(),
                validation
                    .all_symbols_pass
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec!["error".into(), display_option(validation.error.as_deref())],
        ]
        .into_iter()
        .filter(|row| aggregate || row[0] != "all_symbols_pass")
        .collect(),
    );
}

fn structural_table(body: &mut String, structural: &OptimizerStructuralReport) {
    section(body, "Structural Recheck");
    table(
        body,
        &["field", "value"],
        vec![
            vec![
                "validation_status".into(),
                structural.validation_status.clone(),
            ],
            vec![
                "validation_issues".into(),
                structural.validation_issues.join(","),
            ],
            vec!["error".into(), display_option(structural.error.as_deref())],
        ],
    );
    structural_capture_table(body, structural);
}

fn structural_capture_table(body: &mut String, structural: &OptimizerStructuralReport) {
    section(body, "Per-Capture Parent Proofs");
    let scoped = structural.capture_replays.iter().any(|proof| {
        proof.capture.capture_duration_ms.is_some() || proof.capture.replay_duration_ms.is_some()
    });
    let columns = if scoped {
        vec![
            "capture",
            "role",
            "session_id",
            "run_id",
            "capture_duration_ms",
            "replay_duration_ms",
            "candidate_id",
            "structural_status",
            "structural_issues",
            "error",
        ]
    } else {
        vec![
            "capture",
            "role",
            "session_id",
            "run_id",
            "duration_ms",
            "candidate_id",
            "structural_status",
            "structural_issues",
            "error",
        ]
    };
    table(
        body,
        &columns,
        structural
            .capture_replays
            .iter()
            .map(|proof| {
                let mut row = vec![
                    proof.capture.capture.clone(),
                    proof.capture.role.clone(),
                    proof.capture.session_id.clone(),
                    proof.capture.run_id.clone(),
                ];
                if scoped {
                    row.push(
                        proof
                            .capture
                            .capture_duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "NA".into()),
                    );
                    row.push(
                        proof
                            .capture
                            .replay_duration_ms
                            .map(|duration| duration.to_string())
                            .unwrap_or_else(|| "NA".into()),
                    );
                } else {
                    row.push(proof.capture.duration_ms.to_string());
                }
                row.extend([
                    proof
                        .candidate
                        .as_ref()
                        .map(|candidate| candidate.candidate_id.clone())
                        .unwrap_or_else(|| "NA".into()),
                    proof.structural_validation_status.clone(),
                    proof.structural_validation_issues.join(","),
                    display_option(proof.error.as_deref()),
                ]);
                row
            })
            .collect(),
    );
}

fn render_status_ctf_scope(
    body: &mut String,
    capture: Option<&OptimizerCaptureReport>,
    phase: &str,
    role: &str,
    candidate: Option<&OptimizerCandidateReport>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    let symbols = candidate
        .params_by_symbol
        .keys()
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    let scope = format!(
        "{phase}/{role}/symbols={symbols}/origin={}",
        candidate.origin
    );
    render_ctf_scope(body, capture, &scope, Some(candidate));
}

fn render_ctf_scope(
    body: &mut String,
    capture: Option<&OptimizerCaptureReport>,
    role: &str,
    candidate: Option<&OptimizerCandidateReport>,
) {
    let Some(candidate) = candidate else {
        return;
    };
    let recorded_market_coverage = capture
        .and_then(|capture| capture.recorded_market_population.as_ref())
        .map(|population| {
            population.project_entered(
                candidate
                    .canonical_ctf_exit_cohorts
                    .iter()
                    .map(|row| (row.symbol.as_str(), row.market_id.as_str())),
            )
        });
    section(body, "Canonical CTF Table Scope");
    table(
        body,
        &[
            "capture",
            "capture_role",
            "candidate_role",
            "candidate_id",
            "evaluation_scope",
        ],
        vec![vec![
            capture
                .map(|capture| capture.capture.clone())
                .unwrap_or_else(|| "NA".into()),
            capture
                .map(|capture| capture.role.clone())
                .unwrap_or_else(|| "NA".into()),
            role.to_string(),
            candidate.candidate_id.clone(),
            candidate.evaluation_scope.clone(),
        ]],
    );
    super::ctf_exit::render(
        body,
        &candidate.canonical_ctf_exit_summary,
        &candidate.canonical_ctf_exit_cohorts,
        None,
        recorded_market_coverage.as_ref(),
    );
}

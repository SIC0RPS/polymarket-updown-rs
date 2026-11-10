use crate::strategy_runtime::backtest_live_latency::{
    EXECUTION_TIMING_MODEL_VERSION_V4, EXECUTION_TIMING_MODEL_VERSION_V5,
};
use crate::strategy_runtime::backtest_matrix_report::BacktestMatrixReportArtifact;

use super::table::{
    display_option, display_quantity_units_6dp, display_units_6dp, document, section, table,
};

mod oracle_research;

pub(super) fn render(report: &BacktestMatrixReportArtifact) -> String {
    let mut body = document("Backtest Strategy Report");
    section(&mut body, "Run");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["schema_version".into(), report.schema_version.to_string()],
            vec!["candidate_id".into(), report.candidate_id.clone()],
            vec!["capture_session".into(), report.capture_session.clone()],
            vec!["failed".into(), report.failed.to_string()],
            vec![
                "equity_baseline_usdt".into(),
                format!("{:.6}", report.replay_assumptions.equity_baseline_usdt),
            ],
            vec![
                "equity_baseline_source".into(),
                report.replay_assumptions.equity_baseline_source.into(),
            ],
            vec![
                "git_head".into(),
                display_option(report.run_identity.git_head.as_deref()),
            ],
            vec![
                "matrix_binary_sha256".into(),
                report.run_identity.matrix_binary_sha256.clone(),
            ],
        ],
    );

    let assumptions = &report.replay_assumptions;
    let timing = &assumptions.execution_timing;
    section(
        &mut body,
        if matches!(
            timing.model_version.as_str(),
            EXECUTION_TIMING_MODEL_VERSION_V4 | EXECUTION_TIMING_MODEL_VERSION_V5
        ) {
            "recorded execution conditions"
        } else {
            "Execution Timing"
        },
    );
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["model_version".into(), timing.model_version.clone()],
            vec!["selected_capture".into(), timing.selected_capture.clone()],
            vec![
                "selected_config_sha256".into(),
                display_option(timing.selected_config_sha256.as_deref()),
            ],
            vec![
                "current_execution_config_sha256".into(),
                timing.current_execution_config_sha256.clone(),
            ],
            vec![
                "current_clob_protocol".into(),
                timing.current_clob_protocol.clone(),
            ],
            vec![
                "synchronous_response_approximation".into(),
                assumptions.synchronous_response_approximation.into(),
            ],
            vec![
                "exchange_entry_measurement".into(),
                assumptions.exchange_entry_measurement.into(),
            ],
            vec![
                "timestamp_quantization".into(),
                assumptions.timestamp_quantization.into(),
            ],
            vec![
                "ctf_visibility_approximation".into(),
                assumptions.ctf_visibility_approximation.into(),
            ],
        ],
    );
    let sources = [
        (
            "CLOB",
            timing.clob.as_ref().map(|source| {
                (
                    source.source_capture.as_str(),
                    source.source_run_id.as_str(),
                    source.source_config_sha256.as_deref(),
                    source.journal_path.as_str(),
                    source.journal_sha256.as_str(),
                    source.observations.len(),
                    source.endpoint.as_str(),
                    source.unit.as_str(),
                )
            }),
        ),
        (
            "CTF",
            timing.ctf.as_ref().map(|source| {
                (
                    source.source_capture.as_str(),
                    source.source_run_id.as_str(),
                    source.source_config_sha256.as_deref(),
                    source.journal_path.as_str(),
                    source.journal_sha256.as_str(),
                    source.observations.len(),
                    source.endpoint.as_str(),
                    source.unit.as_str(),
                )
            }),
        ),
    ];
    table(
        &mut body,
        &[
            "operation",
            "source_capture",
            "source_run_id",
            "source_config_sha256",
            "journal_path",
            "journal_sha256",
            "observations",
            "endpoint",
            "unit",
        ],
        sources
            .into_iter()
            .map(|(operation, source)| match source {
                Some((capture, run, config, journal, hash, count, endpoint, unit)) => vec![
                    operation.into(),
                    capture.into(),
                    run.into(),
                    display_option(config),
                    journal.into(),
                    hash.into(),
                    count.to_string(),
                    endpoint.into(),
                    unit.into(),
                ],
                None => vec![
                    operation.into(),
                    "NA".into(),
                    "NA".into(),
                    "NA".into(),
                    "NA".into(),
                    "NA".into(),
                    "0".into(),
                    "NA".into(),
                    "NA".into(),
                ],
            })
            .collect(),
    );

    let admission_assumptions = [
        (
            "canonical_receipt_admission_measurement",
            assumptions.canonical_receipt_admission_measurement,
        ),
        (
            "canonical_receipt_admission_scheduling",
            assumptions.canonical_receipt_admission_scheduling,
        ),
        (
            "receipt_admission_approximation",
            assumptions.receipt_admission_approximation,
        ),
    ]
    .into_iter()
    .filter_map(|(field, value)| value.map(|value| vec![field.into(), value.into()]))
    .collect::<Vec<_>>();
    if !admission_assumptions.is_empty() {
        table(&mut body, &["field", "value"], admission_assumptions);
    }
    if let Some(source) = &timing.clob {
        if source.canonical_journal_path.is_some() || source.canonical_journal_sha256.is_some() {
            table(
                &mut body,
                &[
                    "operation",
                    "canonical_journal_path",
                    "canonical_journal_sha256",
                ],
                vec![vec![
                    "CLOB".into(),
                    display_option(source.canonical_journal_path.as_deref()),
                    display_option(source.canonical_journal_sha256.as_deref()),
                ]],
            );
        }
        let associations = source
            .observations
            .iter()
            .enumerate()
            .filter_map(|(sample_index, observation)| {
                observation.receipt_admission.as_ref().map(|admission| {
                    vec![
                        sample_index.to_string(),
                        observation.attempt.to_string(),
                        admission.order_id.clone(),
                        admission.canonical_receipt_id.clone(),
                        admission.execution_intent_id.to_string(),
                        admission.input_index.to_string(),
                        admission.response_to_canonical_append_us.to_string(),
                    ]
                })
            })
            .collect::<Vec<_>>();
        if !associations.is_empty() {
            table(
                &mut body,
                &[
                    "sample_index",
                    "attempt",
                    "source_order_id",
                    "canonical_receipt_id",
                    "execution_intent_id",
                    "input_index",
                    "response_terminal_to_canonical_append_us",
                ],
                associations,
            );
        }
    }

    let runtime = &report.replay_runtime;
    if runtime.pending_clob_receipts.is_some()
        || runtime.clob_supported_admission_submissions.is_some()
        || runtime.clob_approximate_admission_submissions.is_some()
    {
        section(&mut body, "Replay Receipt Admission Coverage");
        table(
            &mut body,
            &["field", "value"],
            vec![
                vec![
                    "clob_supported_admission_submissions".into(),
                    runtime
                        .clob_supported_admission_submissions
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "NA".into()),
                ],
                vec![
                    "clob_approximate_admission_submissions".into(),
                    runtime
                        .clob_approximate_admission_submissions
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "NA".into()),
                ],
                vec![
                    "pending_clob_submissions_at_replay_end".into(),
                    runtime.pending_clob_submissions.to_string(),
                ],
                vec![
                    "pending_clob_receipts_at_replay_end".into(),
                    runtime
                        .pending_clob_receipts
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "NA".into()),
                ],
                vec![
                    "pending_ctf_split_receipts_at_replay_end".into(),
                    runtime.pending_ctf_split_receipts.to_string(),
                ],
                vec![
                    "timing_diagnostics_scope".into(),
                    "first_16_submissions_not_aggregate_coverage".into(),
                ],
            ],
        );
    }
    if runtime
        .clob_timing_diagnostics
        .iter()
        .any(|diagnostic| diagnostic.admission_timing.is_some())
    {
        section(&mut body, "Replay CLOB Timing Diagnostics");
        table(
            &mut body,
            &[
                "order_id",
                "sample_index",
                "accepted_ts_ms",
                "simulated_request_send_us_S",
                "sampled_response_duration_us_R",
                "qualifying_current_venue_hold_us_H",
                "response_derived_matching_due_us_M",
                "response_derived_matching_due_ts_ms",
                "admission_timing",
                "response_terminal_to_canonical_append_us_A",
                "canonical_receipt_due_us_C",
                "canonical_receipt_due_ts_ms",
            ],
            runtime
                .clob_timing_diagnostics
                .iter()
                .map(|diagnostic| {
                    vec![
                        diagnostic.order_id.clone(),
                        diagnostic.sample_index.to_string(),
                        diagnostic.accepted_ts_ms.to_string(),
                        diagnostic.simulated_request_send_us.to_string(),
                        diagnostic.sampled_response_duration_us.to_string(),
                        diagnostic.qualifying_current_venue_hold_us.to_string(),
                        diagnostic.absolute_due_us.to_string(),
                        diagnostic.due_ts_ms.to_string(),
                        display_option(diagnostic.admission_timing.as_deref()),
                        diagnostic
                            .response_to_canonical_append_us
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "NA".into()),
                        diagnostic
                            .absolute_receipt_due_us
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "NA".into()),
                        diagnostic
                            .receipt_due_ts_ms
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "NA".into()),
                    ]
                })
                .collect(),
        );
    }

    section(&mut body, "Oracle Learning");
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec![
                "initial_source".into(),
                report.oracle_learning_initial_source.clone(),
            ],
            vec![
                "initial_sha256".into(),
                display_option(report.oracle_learning_initial_sha256.as_deref()),
            ],
            vec![
                "initial_cutoffs_ms".into(),
                serde_json::to_string(&report.oracle_learning_initial_cutoffs_ms)
                    .unwrap_or_else(|_| "invalid".to_string()),
            ],
            vec![
                "final_path".into(),
                report.oracle_learning_final_path.clone(),
            ],
            vec![
                "final_sha256".into(),
                report.oracle_learning_final_sha256.clone(),
            ],
            vec![
                "final_cutoffs_ms".into(),
                serde_json::to_string(&report.oracle_learning_final_cutoffs_ms)
                    .unwrap_or_else(|_| "invalid".to_string()),
            ],
        ],
    );
    table(
        &mut body,
        &[
            "stage",
            "symbol",
            "trainer",
            "retained_windows",
            "candidate_or_calibrated_brier",
            "prior_or_raw_brier",
            "candidate_or_calibrated_log_loss",
            "prior_or_raw_log_loss",
        ],
        report
            .oracle_learning_initial_metrics
            .iter()
            .map(|(symbol, metrics)| ("initial", symbol, metrics))
            .chain(
                report
                    .oracle_learning_final_metrics
                    .iter()
                    .map(|(symbol, metrics)| ("final", symbol, metrics)),
            )
            .flat_map(|(stage, symbol, metrics)| {
                [
                    vec![
                        stage.to_string(),
                        symbol.clone(),
                        "logistic".to_string(),
                        metrics.logistic_resolved_windows.to_string(),
                        display_loss(metrics.logistic_candidate_brier_loss),
                        display_loss(metrics.logistic_prior_brier_loss),
                        display_loss(metrics.logistic_candidate_log_loss),
                        display_loss(metrics.logistic_prior_log_loss),
                    ],
                    vec![
                        stage.to_string(),
                        symbol.clone(),
                        "market_calibration".to_string(),
                        metrics.market_calibration_resolved_windows.to_string(),
                        display_loss(metrics.market_calibration_brier_loss),
                        display_loss(metrics.market_raw_brier_loss),
                        display_loss(metrics.market_calibration_log_loss),
                        display_loss(metrics.market_raw_log_loss),
                    ],
                    vec![
                        stage.to_string(),
                        symbol.clone(),
                        "beta_calibration".to_string(),
                        metrics.beta_calibration_resolved_windows.to_string(),
                        display_loss(metrics.beta_calibration_brier_loss),
                        display_loss(metrics.beta_raw_brier_loss),
                        display_loss(metrics.beta_calibration_log_loss),
                        display_loss(metrics.beta_raw_log_loss),
                    ],
                ]
            })
            .collect(),
    );
    if let Some(research) = &report.oracle_research {
        oracle_research::render(&mut body, research);
    }

    let stats = &report.canonical_session_stats;
    section(&mut body, "Symbol Runtime");
    table(
        &mut body,
        &["symbol", "status", "error"],
        report
            .symbols
            .iter()
            .map(|symbol| {
                vec![
                    symbol.symbol.clone(),
                    symbol.status.clone(),
                    display_option(symbol.error.as_deref()),
                ]
            })
            .collect(),
    );

    let endpoint = &report.portfolio_accounting;
    section(&mut body, "Endpoint Accounting");
    table(
        &mut body,
        &[
            "symbol",
            "marked_remaining_value",
            "receipt_realized_pnl",
            "open_pnl",
            "total_pnl",
        ],
        endpoint
            .rows
            .iter()
            .map(|row| {
                vec![
                    row.symbol.clone(),
                    row.marked_remaining_value_units_6dp
                        .map(display_quantity_units_6dp)
                        .unwrap_or_else(|| "NA".to_string()),
                    row.receipt_realized_pnl_units_6dp
                        .map(display_units_6dp)
                        .unwrap_or_else(|| "NA".to_string()),
                    row.open_pnl_units_6dp
                        .map(display_units_6dp)
                        .unwrap_or_else(|| "NA".to_string()),
                    row.total_pnl_units_6dp
                        .map(display_units_6dp)
                        .unwrap_or_else(|| "NA".to_string()),
                ]
            })
            .collect(),
    );
    table(
        &mut body,
        &["field", "value"],
        vec![
            vec!["complete".into(), endpoint.is_complete().to_string()],
            vec![
                "portfolio_revision".into(),
                endpoint.portfolio_revision.to_string(),
            ],
            vec![
                "canonical_revision".into(),
                endpoint
                    .canonical_revision
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "NA".to_string()),
            ],
            vec![
                "odds_revision".into(),
                endpoint
                    .odds_revision
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "NA".to_string()),
            ],
            vec!["valued_at_ms".into(), endpoint.valued_at_ms.to_string()],
            vec!["gap_count".into(), endpoint.gaps.len().to_string()],
        ],
    );

    super::ctf_exit::render(
        &mut body,
        &stats.canonical.ctf_exit_summary,
        &stats.canonical.ctf_exit_cohorts,
        Some(stats),
        report.recorded_market_coverage.as_ref(),
    );
    section(&mut body, "Violations");
    table(
        &mut body,
        &["violation"],
        stats
            .violations
            .iter()
            .map(|violation| vec![violation.clone()])
            .collect(),
    );
    body
}

fn display_loss(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.8}"))
        .unwrap_or_else(|| "NA".into())
}

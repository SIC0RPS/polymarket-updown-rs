use crate::strategy_runtime::backtest_matrix_report::{
    OracleResearchArtifact, OracleResearchCoverage, OracleResearchDirectMarketCoverage,
    OracleResearchModelScores, OracleResearchSignalScores, OracleResearchSymbolMetrics,
};

use super::super::table::{display_units_6dp, section, table};
use super::display_loss;

mod first_decision;

pub(super) fn render(body: &mut String, research: &OracleResearchArtifact) {
    section(body, "Oracle Research");
    table(
        body,
        &["field", "value"],
        vec![
            vec!["variant".into(), research.variant.clone()],
            vec!["capture".into(), research.capture.clone()],
            vec!["capture_run_id".into(), research.capture_run_id.clone()],
            vec![
                "capture_window_ms".into(),
                format!(
                    "{}..{}",
                    research.capture_start_ts_ms, research.capture_end_ts_ms
                ),
            ],
            vec![
                "frozen_manifest_sha256".into(),
                research.frozen_manifest_sha256.clone(),
            ],
            vec![
                "common_input_identity_sha256".into(),
                research.common_input_identity_sha256.clone(),
            ],
            vec![
                "execution_timing_sha256".into(),
                research.execution_timing_sha256.clone(),
            ],
            vec![
                "frozen_input_sha256".into(),
                json(&research.frozen_input_sha256),
            ],
            vec![
                "prepared_payload_sha256".into(),
                json(&research.prepared_payload_sha256),
            ],
            vec!["seed_source".into(), research.seed_source.clone()],
            vec![
                "seed_initial_sha256".into(),
                research.seed_initial_sha256.clone(),
            ],
            vec![
                "seed_final_sha256".into(),
                research.seed_final_sha256.clone(),
            ],
            vec![
                "seed_transformed_once".into(),
                research.seed_transformed_once.to_string(),
            ],
            vec![
                "application_policy".into(),
                research
                    .application_policy
                    .clone()
                    .unwrap_or_else(|| "legacy".into()),
            ],
            vec!["runtime_log_path".into(), research.runtime_log_path.clone()],
            vec![
                "candidate_run_identity".into(),
                research
                    .comparison_identity
                    .as_ref()
                    .map(|identity| json(&identity.candidate_run_identity))
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec![
                "reference_run_identity".into(),
                research
                    .comparison_identity
                    .as_ref()
                    .map(|identity| json(&identity.reference_run_identity))
                    .unwrap_or_else(|| "NA".into()),
            ],
            vec![
                "log_loss_probability_clamp".into(),
                "[0.000001, 0.999999]".into(),
            ],
        ],
    );

    first_decision::render(body, research);

    table(
        body,
        &[
            "variant",
            "symbol",
            "policy",
            "population",
            "observations/label_unavailable/label_conflict",
            "eligible/unavailable/ineligible_with_facts",
            "policy available/missing/mismatch/field_absent/eligible_missing",
            "phase available/missing/unrecognized/field_absent/identity_mismatch/unverifiable",
            "incoming valid/missing/invalid/field_absent",
            "prospective valid/missing/invalid/field_absent",
            "support available/missing/field_absent",
            "trust true/false/missing/field_absent",
            "actual_applied/contributing",
        ],
        comparison_symbols(research)
            .filter_map(|(variant, symbol)| {
                let diagnostics = symbol.phase_trust.as_ref()?;
                Some(vec![
                    variant.to_string(),
                    symbol.symbol.clone(),
                    diagnostics.application_policy.clone(),
                    diagnostics.population.clone(),
                    format!(
                        "{}/{}/{}",
                        diagnostics.observations,
                        diagnostics.label_unavailable_observations,
                        diagnostics.label_conflict_observations
                    ),
                    format!(
                        "{}/{}/{}",
                        diagnostics.eligible_observations,
                        diagnostics.unavailable_observations,
                        diagnostics.ineligible_with_phase_facts
                    ),
                    format!(
                        "{}/{}/{}/{}/{}",
                        diagnostics.policy_available,
                        diagnostics.policy_missing,
                        diagnostics.policy_mismatch,
                        diagnostics.policy_field_absent,
                        diagnostics.eligible_policy_missing
                    ),
                    format!(
                        "{}/{}/{}/{}/{}/{}",
                        diagnostics.phase_available,
                        diagnostics.phase_missing,
                        diagnostics.phase_unrecognized,
                        diagnostics.phase_field_absent,
                        diagnostics.phase_identity_mismatch,
                        diagnostics.phase_identity_unverifiable
                    ),
                    format!(
                        "{}/{}/{}/{}",
                        diagnostics.incoming_valid,
                        diagnostics.incoming_missing,
                        diagnostics.incoming_invalid,
                        diagnostics.incoming_field_absent
                    ),
                    format!(
                        "{}/{}/{}/{}",
                        diagnostics.prospective_valid,
                        diagnostics.prospective_missing,
                        diagnostics.prospective_invalid,
                        diagnostics.prospective_field_absent
                    ),
                    format!(
                        "{}/{}/{}",
                        diagnostics.support_count_available,
                        diagnostics.support_count_missing,
                        diagnostics.support_count_field_absent
                    ),
                    format!(
                        "{}/{}/{}/{}",
                        diagnostics.trusted,
                        diagnostics.untrusted,
                        diagnostics.trust_missing,
                        diagnostics.trust_field_absent
                    ),
                    format!(
                        "{}/{}",
                        diagnostics.actual_applied, diagnostics.actually_contributing
                    ),
                ])
            })
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "population",
            "phase",
            "eligible/valid_pairs/scored_pairs",
            "incoming missing/invalid",
            "prospective missing/invalid",
            "support available/missing/min/max",
            "trust true/false/missing",
            "actual_applied/contributing",
            "incoming brier/log_loss/direction",
            "prospective brier/log_loss/direction",
            "prospective corrected/damaged",
        ],
        comparison_symbols(research)
            .flat_map(|(variant, symbol)| {
                symbol.phase_trust.iter().flat_map(move |diagnostics| {
                    diagnostics.phases.iter().map(move |phase| {
                        vec![
                            variant.to_string(),
                            symbol.symbol.clone(),
                            diagnostics.population.clone(),
                            phase.phase.clone(),
                            format!(
                                "{}/{}/{}",
                                phase.observations, phase.valid_pairs, phase.scored_pairs
                            ),
                            format!("{}/{}", phase.incoming_missing, phase.incoming_invalid),
                            format!(
                                "{}/{}",
                                phase.prospective_missing, phase.prospective_invalid
                            ),
                            format!(
                                "{}/{}/{}/{}",
                                phase.support_count_available,
                                phase.support_count_missing,
                                phase
                                    .support_count_min
                                    .map_or_else(|| "NA".into(), |value| value.to_string()),
                                phase
                                    .support_count_max
                                    .map_or_else(|| "NA".into(), |value| value.to_string())
                            ),
                            format!(
                                "{}/{}/{}",
                                phase.trusted, phase.untrusted, phase.trust_missing
                            ),
                            format!("{}/{}", phase.actual_applied, phase.actually_contributing),
                            compact_scores(&phase.incoming),
                            compact_scores(&phase.prospective),
                            format!(
                                "{}/{}",
                                phase.prospective_corrected, phase.prospective_damaged
                            ),
                        ]
                    })
                })
            })
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "complete",
            "log_bytes/capacity",
            "lines/malformed",
            "start/end/timing",
            "probability/unavailable/labels",
            "first_authorizations",
            "scope/duplicate/identity",
            "first_join/unobserved/unexpected/latched",
            "missing_features/reference_failures",
            "common_variants/paired",
            "flood/suppressed/dropped",
            "direct_selected/materialization/predicate_unknown/predicate_fail/selected_unknown",
            "direct_mode_market/cleanup/unknown",
            "direct_open/closed/unknown",
            "direct_learning_on/off/unknown",
            "direct_fit/missing/unknown",
            "direct_prediction_valid/invalid/unknown/disagreement",
            "direct_trust_false/true/unknown",
            "direct_market_probability_valid/invalid/unknown",
            "direct_interval_unavailable/field_absent",
            "direct_mean/interval/beta/confidence/beta_transform/materialization_fail",
            "incomplete_reasons",
        ],
        research
            .completed_variant_comparison
            .iter()
            .map(|variant| coverage_row(&variant.variant, &variant.coverage))
            .collect(),
    );

    let mut score_rows = Vec::new();
    let mut final_rows = Vec::new();
    for (variant, symbol) in comparison_symbols(research) {
        let mut add_score = |model: &str, scores: &OracleResearchModelScores| {
            score_rows.push(score_row(variant, &symbol.symbol, model, scores));
        };
        for (model, scores) in [
            ("learner", &symbol.learner),
            ("market_prior", &symbol.market_prior),
            ("market_raw", &symbol.market_raw),
            ("original_base", &symbol.original_base),
        ] {
            add_score(model, scores);
        }
        for (model, scores) in [
            ("paired_learner", symbol.paired_learner.as_ref()),
            ("paired_market_prior", symbol.paired_market_prior.as_ref()),
        ] {
            if let Some(scores) = scores {
                add_score(model, scores);
            }
        }
        if let Some(scores) = &symbol.direct_market_scores {
            for (population, scores) in [
                ("paired", &scores.paired_population),
                ("affected", &scores.affected_population),
                ("first_minute", &scores.first_minute),
                ("affected_first_minute", &scores.affected_first_minute),
                ("actual_first", &scores.actual_first),
            ] {
                add_score(population, scores);
            }
        }
        let Some(output) = &symbol.final_output else {
            continue;
        };
        for phase in &output.phases {
            add_score(&format!("retained_{}_F0", phase.phase), &phase.f0.scores);
            add_score(&format!("retained_{}_F1", phase.phase), &phase.f1.scores);
            final_rows.push(vec![
                variant.into(),
                symbol.symbol.clone(),
                "retained".into(),
                phase.phase.clone(),
                phase.observations.to_string(),
                format!(
                    "{}/{}/{}",
                    phase.runtime_join_unique,
                    phase.runtime_join_missing,
                    phase.runtime_join_ambiguous
                ),
                format!(
                    "{}/{}",
                    phase.selected_published_equal, phase.selected_published_mismatch
                ),
                format!("{}/{}", phase.branch_applied, phase.actually_contributing),
                "NA".into(),
            ]);
        }
        for row in &output.published {
            add_score(
                &format!("published_{}_{}", row.population, row.phase),
                &row.final_probability.scores,
            );
            final_rows.push(vec![
                variant.into(),
                symbol.symbol.clone(),
                row.population.clone(),
                row.phase.clone(),
                row.observations.to_string(),
                "NA".into(),
                "NA".into(),
                "NA".into(),
                signal_counts(&row.emitted_signal),
            ]);
        }
    }
    table(
        body,
        &[
            "variant",
            "symbol",
            "model",
            "observations",
            "markets",
            "brier",
            "log_loss",
            "direction_accuracy",
            "direction_observations",
            "direction_ties",
        ],
        score_rows,
    );
    table(
        body,
        &[
            "variant",
            "symbol",
            "population",
            "phase",
            "observations",
            "join unique/missing/ambiguous",
            "selected equal/mismatch",
            "applied/contributing",
            "signal known/field_absent/abstain/accuracy",
        ],
        final_rows,
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "labeled_markets/observations/common",
            "missing_features/ties",
            "fit/predicted/unavailable/trusted/untrusted/unknown",
            "attempted/branch_applied/contributing",
            "learner_corrected/damaged",
            "calibration_corrected/damaged",
            "final_corrected/damaged",
            "probability_calibration_attempted/effective",
            "beta_attempted/applied",
            "uncertainty learner+market/learner_only/market_only/neither/omitted/invalid",
            "direct_market_coverage",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| stage_row(variant, symbol))
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "canonical_cohort_pnl",
            "operational_baseline_pnl",
            "comparison_baseline",
            "comparison_baseline_pnl",
            "canonical_delta",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| {
                vec![
                    variant.to_string(),
                    symbol.symbol.clone(),
                    display_money(symbol.canonical_cohort_pnl_units_6dp),
                    display_money(symbol.operational_baseline_pnl_units_6dp),
                    symbol.comparison_baseline.clone(),
                    display_money(symbol.comparison_baseline_pnl_units_6dp),
                    display_money(symbol.canonical_pnl_delta_units_6dp),
                ]
            })
            .collect(),
    );
}

pub(super) fn comparison_symbols(
    research: &OracleResearchArtifact,
) -> impl Iterator<Item = (&str, &OracleResearchSymbolMetrics)> {
    research
        .completed_variant_comparison
        .iter()
        .flat_map(|variant| {
            variant
                .symbols
                .iter()
                .map(move |symbol| (variant.variant.as_str(), symbol))
        })
}

fn coverage_row(variant: &str, coverage: &OracleResearchCoverage) -> Vec<String> {
    vec![
        variant.to_string(),
        coverage.complete.to_string(),
        format!(
            "{}/{}",
            coverage.runtime_log_bytes, coverage.runtime_log_capacity_bytes
        ),
        format!("{}/{}", coverage.parsed_lines, coverage.malformed_lines),
        format!(
            "{}/{}/{}",
            coverage.run_start_markers, coverage.run_end_markers, coverage.timing_identity_markers
        ),
        format!(
            "{}/{}/{}",
            coverage.probability_events,
            coverage.unavailable_probability_events,
            coverage.resolved_label_events
        ),
        coverage.first_authorization_events.to_string(),
        format!(
            "{}/{}/{}",
            coverage.missing_scope_events, coverage.duplicate_conflicts, coverage.identity_failures
        ),
        format!(
            "{}/{}/{}/{}",
            coverage.missing_first_snapshot_joins,
            coverage.unobserved_initial_first_boundaries,
            coverage.unexpected_initial_first_boundaries,
            coverage.latched_first_without_original_snapshot
        ),
        format!(
            "{}/{}",
            coverage.missing_required_features, coverage.reference_provenance_failures
        ),
        format!(
            "{}/{}",
            coverage.common_variant_count, coverage.common_paired_observations
        ),
        format!(
            "{}/{}/{}",
            coverage.flood_control_enabled,
            coverage.flood_suppressed_events,
            coverage.sink_dropped_lines
        ),
        coverage
            .direct_market
            .as_ref()
            .map(|direct| {
                format!(
                    "{}/{}/{}/{}/{}",
                    direct.selected_events,
                    direct.materialization_selected_events,
                    direct.unknown_predicate_events,
                    direct.predicate_violations,
                    direct.selected_unknown_predicate_events
                )
            })
            .unwrap_or_else(|| "NA".into()),
        direct_counts(coverage, |direct| {
            (
                direct.market_scope_events,
                direct.cleanup_scope_events,
                direct.mode_unavailable_events,
            )
        }),
        direct_counts(coverage, |direct| {
            (
                direct.open_scope_events,
                direct.closed_scope_events,
                direct.close_scope_unavailable_events,
            )
        }),
        direct_counts(coverage, |direct| {
            (
                direct.learning_enabled_events,
                direct.learning_disabled_events,
                direct.learning_unavailable_events,
            )
        }),
        direct_counts(coverage, |direct| {
            (
                direct.fit_available_events,
                direct.fit_missing_events,
                direct.fit_unavailable_events,
            )
        }),
        coverage
            .direct_market
            .as_ref()
            .map(|direct| {
                format!(
                    "{}/{}/{}/{}",
                    direct.prediction_valid_events,
                    direct.prediction_invalid_events,
                    direct.prediction_validity_unavailable_events,
                    direct.prediction_validity_disagreements
                )
            })
            .unwrap_or_else(|| "NA".into()),
        direct_counts(coverage, |direct| {
            (
                direct.trust_false_events,
                direct.trust_true_events,
                direct.trust_unavailable_events,
            )
        }),
        direct_counts(coverage, |direct| {
            (
                direct.market_probability_valid_events,
                direct.market_probability_invalid_events,
                direct.market_probability_unavailable_events,
            )
        }),
        coverage
            .direct_market
            .as_ref()
            .map(|direct| {
                format!(
                    "{}/{}",
                    direct.interval_unavailable_events, direct.interval_field_absent_events
                )
            })
            .unwrap_or_else(|| "NA".into()),
        coverage
            .direct_market
            .as_ref()
            .map(|direct| {
                format!(
                    "{}/{}/{}/{}/{}/{}",
                    direct.mean_identity_failures,
                    direct.interval_identity_failures,
                    direct.beta_observation_identity_failures,
                    direct.confidence_transform_violations,
                    direct.beta_transform_violations,
                    direct.materialization_identity_failures
                )
            })
            .unwrap_or_else(|| "NA".into()),
        coverage.incomplete_reasons.join(", "),
    ]
}

pub(super) fn score_row(
    variant: &str,
    symbol: &str,
    model: &str,
    scores: &OracleResearchModelScores,
) -> Vec<String> {
    vec![
        variant.to_string(),
        symbol.to_string(),
        model.to_string(),
        scores.observations.to_string(),
        scores.markets.to_string(),
        display_loss(scores.brier),
        display_loss(scores.log_loss),
        display_loss(scores.direction_accuracy),
        scores
            .direction_observations
            .map_or_else(|| "NA".into(), |value| value.to_string()),
        scores
            .direction_ties
            .map_or_else(|| "NA".into(), |value| value.to_string()),
    ]
}

fn direct_counts(
    coverage: &OracleResearchCoverage,
    counts: impl FnOnce(&OracleResearchDirectMarketCoverage) -> (u64, u64, u64),
) -> String {
    coverage
        .direct_market
        .as_ref()
        .map(|direct| {
            let (first, second, third) = counts(direct);
            format!("{first}/{second}/{third}")
        })
        .unwrap_or_else(|| "NA".into())
}

fn stage_row(variant: &str, symbol: &OracleResearchSymbolMetrics) -> Vec<String> {
    vec![
        variant.to_string(),
        symbol.symbol.clone(),
        format!(
            "{}/{}/{}",
            symbol.labeled_markets, symbol.labeled_observations, symbol.common_paired_observations
        ),
        format!("{}/{}", symbol.missing_features, symbol.ties),
        format!(
            "{}/{}/{}/{}/{}/{}",
            symbol.fit_available,
            symbol.prediction_available,
            symbol
                .prediction_unavailable
                .map_or_else(|| "NA".into(), |value| value.to_string()),
            symbol.prequentially_trusted,
            symbol
                .prequentially_untrusted
                .map_or_else(|| "NA".into(), |value| value.to_string()),
            symbol
                .prequential_trust_unavailable
                .map_or_else(|| "NA".into(), |value| value.to_string())
        ),
        format!(
            "{}/{}/{}",
            symbol.application_attempted, symbol.branch_applied, symbol.actually_contributing
        ),
        format!("{}/{}", symbol.learner_corrected, symbol.learner_damaged),
        format!(
            "{}/{}",
            symbol.calibration_corrected, symbol.calibration_damaged
        ),
        format!("{}/{}", symbol.final_corrected, symbol.final_damaged),
        format!(
            "{}/{}",
            symbol.probability_calibration_attempted, symbol.probability_calibration_effective
        ),
        format!(
            "{}/{}",
            symbol.beta_calibration_attempted, symbol.beta_calibration_applied
        ),
        symbol
            .uncertainty_availability
            .as_ref()
            .map(|availability| {
                format!(
                    "{}/{}/{}/{}/{}/{}",
                    availability.learner_and_market,
                    availability.learner_only,
                    availability.market_only,
                    availability.neither,
                    availability.omitted_fields,
                    availability.invalid_fields
                )
            })
            .unwrap_or_else(|| "NA".into()),
        symbol
            .direct_market
            .as_ref()
            .map(json)
            .unwrap_or_else(|| "NA".into()),
    ]
}

fn display_money(value: Option<i128>) -> String {
    value
        .map(display_units_6dp)
        .unwrap_or_else(|| "NA".to_string())
}

fn compact_scores(scores: &OracleResearchModelScores) -> String {
    format!(
        "{}/{}/{}",
        display_loss(scores.brier),
        display_loss(scores.log_loss),
        display_loss(scores.direction_accuracy)
    )
}

fn signal_counts(scores: &OracleResearchSignalScores) -> String {
    format!(
        "{}/{}/{}/{}",
        scores.direction_up + scores.direction_down,
        scores.field_absent,
        scores.abstained,
        display_loss(scores.accuracy.equal_market_weight_accuracy)
    )
}

fn json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "invalid".to_string())
}

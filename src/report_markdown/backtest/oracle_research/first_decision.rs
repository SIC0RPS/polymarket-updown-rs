use crate::strategy_runtime::backtest_matrix_report::{
    OracleResearchAccuracy, OracleResearchArtifact, OracleResearchSymbolMetrics,
};

use super::super::super::table::table;
use super::super::display_loss;
use super::{comparison_symbols, score_row};

pub(super) fn render(body: &mut String, research: &OracleResearchArtifact) {
    table(
        body,
        &[
            "variant",
            "symbol",
            "population",
            "pairs",
            "label available/unavailable/conflict",
            "probability ties",
            "brier",
            "log_loss",
            "direction_accuracy",
            "direction_observations",
            "authorizations/attributed/missing_snapshot",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| {
                let scores = symbol.actual_first_scores.as_ref();
                vec![
                    variant.to_string(),
                    symbol.symbol.clone(),
                    "final_first_consumed".to_string(),
                    scores.map_or_else(|| "NA".into(), |row| row.observations.to_string()),
                    format!(
                        "{}/{}/{}",
                        count(symbol.actual_first_label_available),
                        count(symbol.actual_first_label_unavailable),
                        count(symbol.actual_first_label_conflict)
                    ),
                    scores
                        .and_then(|row| row.direction_ties)
                        .map_or_else(|| "NA".into(), |value| value.to_string()),
                    display_loss(scores.and_then(|row| row.brier)),
                    display_loss(scores.and_then(|row| row.log_loss)),
                    display_loss(scores.and_then(|row| row.direction_accuracy)),
                    scores
                        .and_then(|row| row.direction_observations)
                        .map_or_else(|| "NA".into(), |value| value.to_string()),
                    format!(
                        "{}/{}/{}",
                        symbol.actual_first_authorizations,
                        symbol.actual_first_attributed,
                        symbol.actual_first_missing_original_snapshot
                    ),
                ]
            })
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "population",
            "observations",
            "markets",
            "brier",
            "log_loss",
            "direction_accuracy",
            "direction_observations",
            "direction_ties",
        ],
        comparison_symbols(research)
            .flat_map(|(variant, symbol)| {
                let mut rows = vec![score_row(
                    variant,
                    &symbol.symbol,
                    "common_decision_overall_final",
                    &symbol.final_probability,
                )];
                if let Some(scores) = &symbol.first_minute_final_scores {
                    rows.push(score_row(
                        variant,
                        &symbol.symbol,
                        "first_minute_final",
                        scores,
                    ));
                }
                rows
            })
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "horizon",
            "observations",
            "markets",
            "accuracy",
            "unobserved/unexpected_initial",
        ],
        comparison_symbols(research)
            .flat_map(|(variant, symbol)| {
                [
                    ("first_minute", &symbol.first_minute_final_accuracy),
                    ("actual_first", &symbol.actual_first_accuracy),
                    ("later", &symbol.later_horizon_final_accuracy),
                ]
                .map(|(horizon, accuracy)| accuracy_row(variant, symbol, horizon, accuracy))
            })
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "evidence",
            "policy",
            "original/retention_events/windows/marker",
            "sample available/unavailable",
            "unavailable snapshot/observation/contribution/capacity/unrecognized",
            "field_absent/identity_failures",
            "repeated_authorizations",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| retention_row(variant, symbol))
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "evidence",
            "expected/resolved/windows/marker",
            "sample available/unavailable",
            "retained_pairs/mismatches/unresolved",
            "merged/added",
            "field_absent/fit_failures/chronology_failures",
            "first_support_min/max",
            "fit_sample_min/max",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| resolution_row(variant, symbol))
            .collect(),
    );

    table(
        body,
        &[
            "variant",
            "symbol",
            "evidence",
            "evaluations/eligible",
            "trust source first/phase/missing/unrecognized",
            "field_absent/identity_failures",
            "trust_count available/missing",
            "first_count available/missing",
            "fit_count available/missing",
        ],
        comparison_symbols(research)
            .map(|(variant, symbol)| trust_row(variant, symbol))
            .collect(),
    );
}

fn retention_row(variant: &str, symbol: &OracleResearchSymbolMetrics) -> Vec<String> {
    let Some(row) = symbol.first_decision.as_ref() else {
        return unavailable_row(variant, symbol, 9);
    };
    vec![
        variant.to_string(),
        symbol.symbol.clone(),
        "candidate_native".into(),
        row.application_policy.clone(),
        format!(
            "{}/{}/{}/{}",
            row.original_first_markers,
            row.retention_events,
            row.retained_windows,
            row.marker_retained
        ),
        format!(
            "{}/{}",
            row.retention_sample_available, row.retention_sample_unavailable
        ),
        format!(
            "{}/{}/{}/{}/{}",
            row.unavailable_missing_snapshot_timestamp,
            row.unavailable_exact_observation,
            row.unavailable_prospective_contribution,
            row.unavailable_pending_capacity,
            row.unavailable_reason_unrecognized
        ),
        format!(
            "{}/{}",
            row.retention_field_absent, row.retention_identity_failures
        ),
        row.repeated_authorization_attempts.to_string(),
    ]
}

fn resolution_row(variant: &str, symbol: &OracleResearchSymbolMetrics) -> Vec<String> {
    let Some(row) = symbol.first_decision.as_ref() else {
        return unavailable_row(variant, symbol, 10);
    };
    vec![
        variant.to_string(),
        symbol.symbol.clone(),
        "candidate_native".into(),
        format!(
            "{}/{}/{}/{}",
            row.expected_resolution_events,
            row.resolution_events,
            row.resolved_windows,
            row.resolution_marker_available
        ),
        format!(
            "{}/{}",
            row.resolution_sample_available, row.resolution_sample_unavailable
        ),
        format!(
            "{}/{}/{}",
            row.retained_resolution_pairs,
            row.retained_resolution_mismatches,
            row.retained_without_resolution
        ),
        format!("{}/{}", row.samples_merged, row.first_fit_rows_added),
        format!(
            "{}/{}/{}",
            row.resolution_field_absent,
            row.fitting_identity_failures,
            row.resolution_chronology_failures
        ),
        range(row.first_support_count_min, row.first_support_count_max),
        range(row.fit_sample_count_min, row.fit_sample_count_max),
    ]
}

fn trust_row(variant: &str, symbol: &OracleResearchSymbolMetrics) -> Vec<String> {
    let Some(row) = symbol.first_decision.as_ref() else {
        return unavailable_row(variant, symbol, 9);
    };
    vec![
        variant.to_string(),
        symbol.symbol.clone(),
        "candidate_native".into(),
        format!(
            "{}/{}",
            row.evaluation_observations, row.eligible_trust_observations
        ),
        format!(
            "{}/{}/{}/{}",
            row.trust_source_first,
            row.trust_source_phase,
            row.trust_source_missing,
            row.trust_source_unrecognized
        ),
        format!("{}/{}", row.trust_field_absent, row.trust_identity_failures),
        format!(
            "{}/{}",
            row.trust_sample_count_available, row.trust_sample_count_missing
        ),
        format!(
            "{}/{}",
            row.first_sample_count_available, row.first_sample_count_missing
        ),
        format!(
            "{}/{}",
            row.evaluation_fit_sample_count_available, row.evaluation_fit_sample_count_missing
        ),
    ]
}

fn unavailable_row(
    variant: &str,
    symbol: &OracleResearchSymbolMetrics,
    columns: usize,
) -> Vec<String> {
    let evidence = if variant == "market_offset_phase_trust" {
        "historical_unavailable"
    } else {
        "not_applicable"
    };
    let mut row = vec![
        variant.to_string(),
        symbol.symbol.clone(),
        evidence.to_string(),
    ];
    row.resize(columns, "NA".to_string());
    row
}

fn accuracy_row(
    variant: &str,
    symbol: &OracleResearchSymbolMetrics,
    horizon: &str,
    accuracy: &OracleResearchAccuracy,
) -> Vec<String> {
    vec![
        variant.to_string(),
        symbol.symbol.clone(),
        horizon.to_string(),
        accuracy.observations.to_string(),
        accuracy.markets.to_string(),
        display_loss(accuracy.equal_market_weight_accuracy),
        format!(
            "{}/{}",
            symbol.unobserved_initial_first_boundaries, symbol.unexpected_initial_first_boundaries
        ),
    ]
}

fn range(min: Option<u64>, max: Option<u64>) -> String {
    format!(
        "{}/{}",
        min.map_or_else(|| "NA".into(), |value| value.to_string()),
        max.map_or_else(|| "NA".into(), |value| value.to_string())
    )
}

fn count(value: Option<u64>) -> String {
    value.map_or_else(|| "NA".into(), |value| value.to_string())
}

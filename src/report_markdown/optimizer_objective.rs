use crate::optimizer_report::{
    OptimizerCandidateReport, OptimizerObjectiveProfileReport, OptimizerObjectiveScoreReport,
    OptimizerRunProvenanceReport, OptimizerSelectedLearningBindingReport,
    OptimizerValidationReport,
};
use crate::strategy_runtime::prediction_metrics::{
    PredictionPhaseMetrics, PREDICTION_METRIC_DEFINITION,
};
use std::collections::BTreeMap;

use super::table::{display_option, display_units_6dp, section, table};

pub(super) fn objective_table(
    body: &mut String,
    profile: &OptimizerObjectiveProfileReport,
    provenance: Option<&OptimizerRunProvenanceReport>,
    original_wording: bool,
    later_validation_wording: bool,
) {
    let selection_mode = provenance.map_or("profit", |value| value.selection_mode.as_str());
    let prediction = selection_mode == "prediction";
    let independent_promotion = provenance
        .and_then(|value| value.selected_learning_bindings.as_ref())
        .is_some();
    let (scope, feasibility, validation) = if has_partial_symbol_scope(provenance) {
        (
            "qualified symbols search independently in BTC, ETH, SOL, XRP order on each symbol's frozen selection-capture A inputs; the participating maps stay frozen and replay together on A",
            "economic eligibility requires strictly positive receipt-realized net PnL for the symbol on A; each symbol's own A result controls only that symbol's eligibility",
            "capture B covers the same participating symbols as frozen A and replays their candidate and incumbent maps without search or retuning; each symbol's own positive B result and incumbent comparison controls only that symbol's promotion",
        )
    } else if independent_promotion {
        (
            "BTC, ETH, SOL, then XRP search independently on only that symbol's frozen selection-capture A inputs; the selected maps remain one frozen four-map candidate replayed together on A",
            if original_wording {
                "a search candidate must have strictly positive receipt-realized net PnL for its symbol on A; each symbol's own A result controls only that symbol's eligibility"
            } else {
                "economic eligibility requires strictly positive receipt-realized net PnL for the symbol on A; each symbol's own A result controls only that symbol's eligibility"
            },
            "capture B replays the frozen four-map candidate and incumbent without search or retuning; each symbol's own positive B result and incumbent comparison controls only that symbol's promotion",
        )
    } else {
        (
            "BTC, ETH, SOL, then XRP search independently on only that symbol's frozen selection-capture A inputs; the selected maps are then frozen and replayed together on A",
            if original_wording {
                "a search candidate must have strictly positive receipt-realized net PnL for its symbol on A; the frozen four-map replay must be strictly positive for every symbol"
            } else {
                "economic eligibility requires strictly positive receipt-realized net PnL for the symbol on A; the frozen four-map replay must be strictly positive for every symbol"
            },
            if later_validation_wording {
                "later capture B replays the frozen four-map candidate and incumbent without search or retuning; every symbol must be positive and beat its B incumbent"
            } else {
                "capture B replays the frozen four-map candidate and incumbent without search or retuning; every symbol must be positive and beat its B incumbent"
            },
        )
    };
    let feasibility = if prediction {
        "economic promotion requires strictly positive receipt-realized net PnL for the symbol on A; candidate ranking remains independent of PnL sign"
    } else {
        feasibility
    };
    section(body, "Optimization Objective");
    if prediction {
        table(body, &["field", "value"], vec![
            vec!["selection_mode".into(), selection_mode.into()],
            vec!["prediction_metric_definition".into(), PREDICTION_METRIC_DEFINITION.into()],
            vec!["financial_objective_role".into(), "the configured financial objective, shortfall and profitability remain financial diagnostics; prediction selection ranks lower Brier independently of PnL sign".into()],
            vec!["observation_population".into(), "current published final UP probability after each original recorded runtime-input consumption within the market window; synthetic portfolio drives are excluded; expected and valid identities and per-market phase counts must match".into()],
            vec!["prediction_promotion".into(), "strictly lower B Brier than the symbol's own incumbent on comparable coverage, plus every existing independent A/B financial and structural gate; B stays frozen and promotion exports A's final learner".into()],
        ]);
    }
    table(
        body,
        &["field", "value"],
        vec![
            vec!["formula_type".into(), profile.formula_type.clone()],
            vec!["expression".into(), profile.expression.clone()],
            vec!["profile_sha256".into(), profile.sha256.clone()],
            vec![
                "P".into(),
                "canonical TOTAL PnL (6 decimal dollar units)".into(),
            ],
            vec![
                "A".into(),
                "receipt-realized PnL / entered markets".into(),
            ],
            vec!["W".into(), "profitable entered markets".into()],
            vec!["N".into(), "entered markets".into()],
            vec!["scope".into(), scope.into()],
            vec!["feasibility".into(), feasibility.into()],
            vec![
                "ranking".into(),
                if prediction {
                    "admissible comparable symbol candidates rank by lower equal-market Brier of the published final UP probability, independently of PnL sign; log loss and direction are separate diagnostics"
                } else if original_wording {
                    "feasible symbol candidates use the unchanged objective on that symbol's canonical TOTAL; infeasible candidates cannot win"
                } else {
                    "winner selection ranks complete-coverage symbol candidates: feasible positive candidates rank first by the unchanged objective on that symbol's canonical TOTAL, then losing candidates rank by lower shortfall; selection alone does not establish economic eligibility or application"
                }
                .into(),
            ],
            vec!["validation".into(), validation.into()],
        ],
    );
}

pub(super) fn economic_table(
    body: &mut String,
    validation: &OptimizerValidationReport,
    selected_learning_bindings: Option<&BTreeMap<String, OptimizerSelectedLearningBindingReport>>,
    aggregate: bool,
) {
    let independent_promotion = selected_learning_bindings.is_some();
    let prediction = validation.per_symbol.iter().any(|row| {
        row.prediction_comparable.is_some()
            || row.prediction_improved.is_some()
            || row.selection_candidate_brier.is_some()
            || row.selection_incumbent_brier.is_some()
            || row.validation_candidate_brier.is_some()
            || row.validation_incumbent_brier.is_some()
    });
    section(
        body,
        if independent_promotion {
            "Per-Symbol Promotion Decisions"
        } else {
            "All-Four Economic Gate"
        },
    );
    if independent_promotion {
        table(
            body,
            &["field", "value"],
            vec![
                vec![
                    "all_four_economic_pass_diagnostic".into(),
                    display_bool(validation.all_symbols_pass),
                ],
                vec![
                    "promotion_policy".into(),
                    "each symbol decision is independent".into(),
                ],
            ]
            .into_iter()
            .filter(|row| aggregate || row[0] != "all_four_economic_pass_diagnostic")
            .collect(),
        );
    }
    let mut columns = vec!["symbol"];
    if independent_promotion {
        columns.push("promotion_decision");
    }
    columns.extend([
        "candidate_A_receipt_realized_net_pnl",
        "incumbent_A_receipt_realized_net_pnl",
        "candidate_A_entries",
        "candidate_A_cohorts",
        "candidate_B_receipt_realized_net_pnl",
        "incumbent_B_receipt_realized_net_pnl",
        "candidate_B_minus_incumbent_B",
        "candidate_B_entries",
        "candidate_B_cohorts",
        "A_positive",
        "B_positive",
        "B_beats_incumbent",
        "economic_pass",
    ]);
    if prediction {
        columns.extend([
            "candidate_A_brier",
            "incumbent_A_brier",
            "candidate_B_brier",
            "incumbent_B_brier",
            "B_prediction_comparable",
            "B_prediction_improved",
        ]);
    }
    table(
        body,
        &columns,
        validation
            .per_symbol
            .iter()
            .map(|row| {
                let mut cells = vec![row.symbol.clone()];
                if let Some(bindings) = selected_learning_bindings {
                    cells.push(
                        if bindings.contains_key(&row.symbol) {
                            "promoted"
                        } else {
                            "not_promoted"
                        }
                        .into(),
                    );
                }
                cells.extend([
                    pnl_component(row.selection_candidate_pnl.as_ref(), |pnl| {
                        pnl.receipt_realized_pnl_units_6dp
                    }),
                    pnl_component(row.selection_incumbent_pnl.as_ref(), |pnl| {
                        pnl.receipt_realized_pnl_units_6dp
                    }),
                    display_count(row.selection_candidate_entries),
                    display_count(row.selection_candidate_cohorts),
                    pnl_component(row.validation_candidate_pnl.as_ref(), |pnl| {
                        pnl.receipt_realized_pnl_units_6dp
                    }),
                    pnl_component(row.validation_incumbent_pnl.as_ref(), |pnl| {
                        pnl.receipt_realized_pnl_units_6dp
                    }),
                    row.validation_delta_units_6dp
                        .map(display_units_6dp)
                        .unwrap_or_else(|| "NA".into()),
                    display_count(row.validation_candidate_entries),
                    display_count(row.validation_candidate_cohorts),
                    row.selection_profitable.to_string(),
                    display_bool(row.validation_profitable),
                    display_bool(row.beats_validation_incumbent),
                    display_bool(row.economic_pass),
                ]);
                if prediction {
                    cells.extend([
                        display_metric(row.selection_candidate_brier),
                        display_metric(row.selection_incumbent_brier),
                        display_metric(row.validation_candidate_brier),
                        display_metric(row.validation_incumbent_brier),
                        display_bool(row.prediction_comparable),
                        display_bool(row.prediction_improved),
                    ]);
                }
                cells
            })
            .collect(),
    );
}

pub(super) fn display_objective_score(score: Option<&OptimizerObjectiveScoreReport>) -> String {
    score
        .map(|score| format!("{}/{}", score.numerator, score.denominator))
        .unwrap_or_else(|| "NA".into())
}

fn display_count(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NA".into())
}

fn display_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NA".into())
}

pub(super) fn has_partial_symbol_scope(provenance: Option<&OptimizerRunProvenanceReport>) -> bool {
    provenance
        .and_then(|value| {
            value
                .capture_learners
                .iter()
                .find(|capture| capture.role == "selection")
        })
        .is_some_and(|capture| {
            capture.initial_oracle_learning_sha256_by_symbol.len()
                < crate::config::VALID_ASSETS.len()
        })
}

pub(super) fn candidate_table<'a>(
    body: &mut String,
    title: &str,
    candidates: impl IntoIterator<Item = (&'a str, Option<&'a OptimizerCandidateReport>)>,
) {
    let candidates = candidates
        .into_iter()
        .filter_map(|(role, candidate)| candidate.map(|candidate| (role, candidate)))
        .collect::<Vec<_>>();
    section(body, title);
    table(
        body,
        &[
            "role",
            "candidate_id",
            "origin",
            "origin_id",
            "symbols",
            "exercised",
            "objective_score_exact",
            "shortfall_units_6dp",
            "all_symbols_profitable",
            "entered_markets",
            "profitable_markets",
            "split_entries",
            "submitted_orders",
            "managed_sell_fills",
            "wrong_side_first_managed_sell",
            "evaluation_scope",
            "accounting_coverage",
        ],
        candidates
            .iter()
            .map(|(role, candidate)| {
                vec![
                    role.to_string(),
                    candidate.candidate_id.clone(),
                    candidate.origin.clone(),
                    display_option(candidate.origin_id.as_deref()),
                    candidate
                        .params_by_symbol
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","),
                    candidate.exercised.to_string(),
                    display_objective_score(candidate.objective_score.as_ref()),
                    candidate.shortfall_units_6dp.to_string(),
                    candidate.all_symbols_profitable.to_string(),
                    candidate.entry_count.to_string(),
                    candidate.profitable_market_count.to_string(),
                    candidate.split_entries.to_string(),
                    candidate.submitted_orders.to_string(),
                    candidate.managed_sell_fills.to_string(),
                    candidate.wrong_side_first_managed_sell.to_string(),
                    candidate.evaluation_scope.clone(),
                    coverage(candidate),
                ]
            })
            .collect(),
    );
    for (role, candidate) in candidates {
        prediction_tables(body, title, role, candidate);
    }
}

fn prediction_tables(
    body: &mut String,
    title: &str,
    role: &str,
    candidate: &OptimizerCandidateReport,
) {
    if candidate.prediction_metrics.is_empty() {
        return;
    }
    section(
        body,
        &format!("{title}: {role} Published Prediction Evidence"),
    );
    table(
        body,
        &[
            "symbol",
            "selection_mode",
            "metric_definition",
            "equal_market_brier",
            "equal_market_log_loss",
            "probability_direction_accuracy",
            "emitted_direction_accuracy",
            "expected",
            "valid",
            "unavailable",
            "invalid",
            "future_dated",
            "missing_labels",
            "probability_up",
            "probability_down",
            "probability_ties",
            "emitted_up",
            "emitted_down",
            "abstained",
            "snapshot_reuses",
        ],
        candidate
            .prediction_metrics
            .iter()
            .map(|(symbol, metrics)| {
                vec![
                    symbol.clone(),
                    candidate.selection_mode.clone(),
                    metrics.metric_definition.clone(),
                    display_metric(metrics.brier),
                    display_metric(metrics.log_loss),
                    display_metric(metrics.direction_accuracy),
                    display_metric(metrics.emitted_direction_accuracy),
                    metrics.expected_observations.to_string(),
                    metrics.valid_predictions.to_string(),
                    metrics.unavailable_predictions.to_string(),
                    metrics.invalid_predictions.to_string(),
                    metrics.future_dated_forecasts.to_string(),
                    metrics.missing_labels.to_string(),
                    metrics.direction_up.to_string(),
                    metrics.direction_down.to_string(),
                    metrics.direction_ties.to_string(),
                    metrics.emitted_up.to_string(),
                    metrics.emitted_down.to_string(),
                    metrics.abstained.to_string(),
                    metrics.snapshot_reuses.to_string(),
                ]
            })
            .collect(),
    );
    table(
        body,
        &[
            "symbol",
            "market",
            "window_start_ms",
            "window_end_ms",
            "winner_up",
            "recorded_label_identity",
            "scored_observations",
            "brier",
            "log_loss",
        ],
        candidate
            .prediction_metrics
            .iter()
            .flat_map(|(symbol, metrics)| {
                metrics.markets.iter().map(move |(market, row)| {
                    vec![
                        symbol.clone(),
                        market.clone(),
                        row.window_start_ms.to_string(),
                        row.window_end_ms.to_string(),
                        display_bool(row.winner_up),
                        row.label_identity.clone(),
                        row.scores.observations.to_string(),
                        display_metric(row.scores.brier),
                        display_metric(row.scores.log_loss),
                    ]
                })
            })
            .collect(),
    );
    table(
        body,
        &[
            "symbol",
            "market_scope",
            "phase_0_to_4",
            "expected",
            "valid",
            "unavailable",
            "invalid",
            "future_dated",
            "expected_observation_identity",
            "valid_prediction_identity",
            "brier",
            "log_loss",
            "probability_direction_accuracy",
            "emitted_direction_accuracy",
            "probability_up",
            "probability_down",
            "probability_ties",
            "emitted_up",
            "emitted_down",
            "abstained",
            "snapshot_reuses",
        ],
        candidate
            .prediction_metrics
            .iter()
            .flat_map(|(symbol, metrics)| {
                metrics
                    .phases
                    .iter()
                    .enumerate()
                    .map(move |(index, phase)| {
                        prediction_phase_row(symbol, "all_markets", index, phase)
                    })
                    .chain(metrics.markets.iter().flat_map(move |(market, row)| {
                        row.phases.iter().enumerate().map(move |(index, phase)| {
                            prediction_phase_row(symbol, market, index, phase)
                        })
                    }))
            })
            .collect(),
    );
}

fn prediction_phase_row(
    symbol: &str,
    market: &str,
    phase_index: usize,
    phase: &PredictionPhaseMetrics,
) -> Vec<String> {
    vec![
        symbol.into(),
        market.into(),
        phase_index.to_string(),
        phase.expected_observations.to_string(),
        phase.valid_predictions.to_string(),
        phase.unavailable_predictions.to_string(),
        phase.invalid_predictions.to_string(),
        phase.future_dated_forecasts.to_string(),
        phase.expected_observation_identity.clone(),
        phase.valid_prediction_identity.clone(),
        display_metric(phase.scores.brier),
        display_metric(phase.scores.log_loss),
        display_metric(phase.scores.direction_accuracy),
        display_metric(phase.emitted_direction_accuracy),
        phase.direction_up.to_string(),
        phase.direction_down.to_string(),
        phase.direction_ties.to_string(),
        phase.emitted_up.to_string(),
        phase.emitted_down.to_string(),
        phase.abstained.to_string(),
        phase.snapshot_reuses.to_string(),
    ]
}

fn display_metric(value: Option<f64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "NA".into())
}

fn coverage(candidate: &OptimizerCandidateReport) -> String {
    candidate
        .accounting_coverage
        .as_ref()
        .map(|coverage| {
            if coverage.is_complete() {
                "complete".to_string()
            } else {
                format!("gaps={:?}", coverage.gaps)
            }
        })
        .unwrap_or_else(|| "NA".into())
}

pub(super) fn pnl_component(
    pnl: Option<&crate::portfolio::PortfolioAccountingPnlRow>,
    field: impl FnOnce(&crate::portfolio::PortfolioAccountingPnlRow) -> Option<i128>,
) -> String {
    pnl.and_then(field)
        .map(display_units_6dp)
        .unwrap_or_else(|| "NA".into())
}

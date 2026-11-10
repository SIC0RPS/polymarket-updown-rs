use super::*;
use polymarket_hft::strategy_runtime::prediction_metrics::PredictionMetrics;
use polymarket_hft::trade_session_ledger::CanonicalCoverageProof;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct SymbolEconomicResult {
    pub(super) symbol: String,
    pub(super) candidate_net_pnl_units_6dp: i128,
    pub(super) incumbent_net_pnl_units_6dp: i128,
    pub(super) delta_units_6dp: i128,
    pub(super) candidate_profitable: bool,
    pub(super) beats_incumbent: bool,
    pub(super) passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) prediction_comparable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) prediction_improved: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct HoldoutReplayProof {
    #[serde(default, skip_serializing_if = "SelectionMode::is_profit")]
    pub(super) selection_mode: SelectionMode,
    pub(super) capture_path: String,
    pub(super) candidate_id: String,
    pub(super) params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    pub(super) canonical_session_stats: CanonicalSessionStats,
    pub(super) portfolio_accounting: PortfolioAccountingProjectionProof,
    pub(super) initial_oracle_learning_sha256: String,
    pub(super) initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    pub(super) final_oracle_learning_sha256: String,
    pub(super) final_oracle_learning_sha256_by_symbol: BTreeMap<String, String>,
    pub(super) final_oracle_learning: BTreeMap<String, OracleLearningSnapshot>,
    pub(super) selection_score: Option<scoring::FeasibilityScore>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(super) prediction_metrics: BTreeMap<String, PredictionMetrics>,
    pub(super) accounting_coverage: CanonicalCoverageProof,
    pub(super) failed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct HoldoutResult {
    pub(super) capture: String,
    pub(super) incumbent: HoldoutReplayProof,
    pub(super) candidate: HoldoutReplayProof,
    pub(super) economics_by_symbol: BTreeMap<String, SymbolEconomicResult>,
    pub(super) structural_pass: bool,
    pub(super) economic_pass: bool,
    pub(super) error: Option<String>,
}

pub(super) fn evaluate_frozen_pair(
    ctx: &RunContext,
    inputs: &[capture_inputs::FrozenCaptureInput],
    incumbent_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    winner_params: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<Vec<HoldoutResult>> {
    let incumbent_id = candidate_id_for_params(ctx, incumbent_params)?;
    let winner_id = candidate_id_for_params(ctx, winner_params)?;
    let (incumbent_config, incumbent_symbol_configs) =
        strategy_configs_for_params(ctx, incumbent_params)?;
    let (winner_config, winner_symbol_configs) = strategy_configs_for_params(ctx, winner_params)?;
    let root_spec_sha256 = candidate_proof::file_sha256(&ctx.optimizer_spec_path)?;
    let mut results = Vec::with_capacity(inputs.len());
    for input in inputs {
        let mut evaluator = BacktestMatrixEvaluator::load_prepared_for_symbols_with_optimizer_path(
            &ctx.cfg,
            &input.capture,
            &ctx.symbols,
            &ctx.optimizer_spec_path,
            Some(
                input
                    .execution_timing
                    .as_ref()
                    .context("frozen validation execution timing missing")?,
            ),
        )?;
        ensure!(
            evaluator.replay_window()? == (input.replay_start_ms, input.replay_end_ms),
            "validation frozen replay window changed capture={}",
            input.capture.display()
        );
        for (symbol, proof) in candidate_proof::build_symbol_proof_identities(
            &ctx.symbols,
            &input.capture,
            &ctx.cfg,
            &root_spec_sha256,
            ctx.objective_profile.sha256(),
        )? {
            ensure!(
                input.prepared_proof_identities_by_symbol.get(&symbol) == Some(&proof),
                "validation frozen prepared proof changed symbol={symbol} capture={}",
                input.capture.display()
            );
        }
        let initial_learning = evaluator.captured_initial_oracle_learning().clone();
        let initial_sha256 = candidate_proof::oracle_learning_sha256(&initial_learning)?;
        let initial_cutoffs = initial_learning
            .iter()
            .map(|(symbol, learning)| (symbol.clone(), learning.knowledge_cutoff_ms()))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            initial_sha256 == input.initial_oracle_learning_sha256
                && initial_cutoffs == input.initial_oracle_learning_cutoffs_ms,
            "validation frozen initial learner changed capture={}",
            input.capture.display()
        );
        let mut candidates = vec![BacktestMatrixCandidate {
            id: incumbent_id.clone(),
            strategy_config: incumbent_config.clone(),
            per_symbol_strategy_config: incumbent_symbol_configs.clone(),
            per_symbol_parameter_values: incumbent_params.clone(),
            initial_oracle_learning: initial_learning.clone(),
            oracle_direct_market_research: false,
            collect_prediction_metrics: ctx.selection_mode.collect_prediction_metrics(),
        }];
        if winner_id != incumbent_id {
            candidates.push(BacktestMatrixCandidate {
                id: winner_id.clone(),
                strategy_config: winner_config.clone(),
                per_symbol_strategy_config: winner_symbol_configs.clone(),
                per_symbol_parameter_values: winner_params.clone(),
                initial_oracle_learning: initial_learning,
                oracle_direct_market_research: false,
                collect_prediction_metrics: ctx.selection_mode.collect_prediction_metrics(),
            });
        }
        evaluator.configure_candidate_pool(
            candidates.len().min(ctx.candidate_slots),
            true,
            ctx.optimizer_observability.handle(),
        )?;
        let summaries_result = ctx
            .async_runtime
            .block_on(evaluator.evaluate_candidate_metrics(
                &ctx.cfg,
                &candidates,
                ctx.candidate_slots,
            ));
        let shutdown_result = ctx.async_runtime.block_on(evaluator.shutdown());
        let summaries = match (summaries_result, shutdown_result) {
            (Ok(summaries), Ok(())) => summaries,
            (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
            (Err(run_error), Err(shutdown_error)) => bail!(
                "validation evaluation failed capture={}: {run_error:#}; shutdown failed: {shutdown_error:#}",
                input.capture.display()
            ),
        };
        ensure!(
            summaries.len() == candidates.len(),
            "validation result count mismatch"
        );
        let mut summaries_by_id = summaries
            .into_iter()
            .map(|summary| (summary.candidate_id.clone(), summary))
            .collect::<BTreeMap<_, _>>();
        let incumbent_summary = summaries_by_id
            .remove(&incumbent_id)
            .context("validation incumbent result missing")?;
        let incumbent = replay_proof(
            ctx,
            input,
            incumbent_id.clone(),
            incumbent_params.clone(),
            &initial_sha256,
            &initial_cutoffs,
            incumbent_summary,
        )?;
        let mut candidate = if winner_id == incumbent_id {
            let mut same = incumbent.clone();
            same.candidate_id = winner_id.clone();
            same.params_by_symbol = winner_params.clone();
            same
        } else {
            let winner_summary = summaries_by_id
                .remove(&winner_id)
                .context("validation winner result missing")?;
            replay_proof(
                ctx,
                input,
                winner_id.clone(),
                winner_params.clone(),
                &initial_sha256,
                &initial_cutoffs,
                winner_summary,
            )?
        };
        ensure!(
            summaries_by_id.is_empty(),
            "unconsumed validation candidate results"
        );
        let mut errors = Vec::new();
        for (role, proof) in [("incumbent", &incumbent), ("candidate", &candidate)] {
            if let Err(error) = scoring::validate_prediction_evidence(
                ctx.selection_mode,
                &ctx.symbols,
                &proof.prediction_metrics,
            ) {
                errors.push(format!("{role}_invalid_prediction_evidence:{error:#}"));
            }
            if ctx.selection_mode.collect_prediction_metrics()
                && !proof
                    .prediction_metrics
                    .values()
                    .all(PredictionMetrics::is_admissible)
            {
                errors.push(format!("{role}_inadmissible_prediction_evidence"));
            }
        }
        if ctx.selection_mode.collect_prediction_metrics()
            && !scoring::prediction_coverage_matches(
                &candidate.prediction_metrics,
                &incumbent.prediction_metrics,
            )
        {
            candidate.selection_score = None;
            errors.push("incomparable_prediction_coverage".to_string());
        }
        if incumbent.failed {
            errors.push("incumbent_summary_failed".to_string());
        }
        if candidate.failed {
            errors.push("candidate_summary_failed".to_string());
        }
        if !incumbent.accounting_coverage.is_complete() {
            errors.push(format!(
                "incumbent_incomplete_accounting_coverage:{:?}",
                incumbent.accounting_coverage.gaps
            ));
        }
        if !candidate.accounting_coverage.is_complete() {
            errors.push(format!(
                "candidate_incomplete_accounting_coverage:{:?}",
                candidate.accounting_coverage.gaps
            ));
        }
        let economics_by_symbol = economic_results(ctx, &incumbent, &candidate, &mut errors)?;
        let structural_pass = errors.is_empty();
        let economic_pass = economics_by_symbol.len() == ctx.symbols.len()
            && economics_by_symbol.values().all(|row| row.passed);
        let error = (!errors.is_empty()).then(|| errors.join("|"));
        results.push(HoldoutResult {
            capture: input.capture.display().to_string(),
            incumbent,
            candidate,
            economics_by_symbol,
            structural_pass,
            economic_pass,
            error,
        });
    }
    Ok(results)
}

fn replay_proof(
    ctx: &RunContext,
    input: &capture_inputs::FrozenCaptureInput,
    candidate_id: String,
    params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    initial_sha256: &str,
    initial_cutoffs: &BTreeMap<String, u64>,
    summary: polymarket_hft::strategy_runtime::backtest_matrix::BacktestMatrixCandidateMetricsSummary,
) -> Result<HoldoutReplayProof> {
    let accounting_coverage = summary
        .canonical_session_stats
        .proof_for_eligible_markets(&summary.portfolio_accounting);
    let selection_score = endpoint_joint_score(
        &ctx.objective_profile,
        &ctx.symbols,
        &summary.canonical_session_stats,
        &summary.portfolio_accounting,
    )?
    .selection_score;
    let prediction_metrics = summary.prediction_metrics;
    let prediction_valid = scoring::validate_prediction_evidence(
        ctx.selection_mode,
        &ctx.symbols,
        &prediction_metrics,
    )
    .is_ok()
        && (ctx.selection_mode.is_profit()
            || prediction_metrics
                .values()
                .all(PredictionMetrics::is_admissible));
    let selection_score =
        (accounting_coverage.is_complete() && !summary.failed && prediction_valid)
            .then_some(selection_score);
    let final_oracle_learning_sha256 =
        candidate_proof::oracle_learning_sha256(&summary.oracle_learning)?;
    let final_oracle_learning_sha256_by_symbol = summary
        .oracle_learning
        .iter()
        .map(|(symbol, learning)| {
            Ok((
                symbol.clone(),
                candidate_proof::oracle_learning_sha256(&BTreeMap::from([(
                    symbol.clone(),
                    learning.clone(),
                )]))?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(HoldoutReplayProof {
        selection_mode: ctx.selection_mode,
        capture_path: input.capture.display().to_string(),
        candidate_id,
        params_by_symbol,
        canonical_session_stats: summary.canonical_session_stats,
        portfolio_accounting: summary.portfolio_accounting,
        initial_oracle_learning_sha256: initial_sha256.to_string(),
        initial_oracle_learning_cutoffs_ms: initial_cutoffs.clone(),
        final_oracle_learning_sha256,
        final_oracle_learning_sha256_by_symbol,
        final_oracle_learning: summary.oracle_learning,
        selection_score,
        prediction_metrics,
        accounting_coverage,
        failed: summary.failed,
    })
}

fn economic_results(
    ctx: &RunContext,
    incumbent: &HoldoutReplayProof,
    candidate: &HoldoutReplayProof,
    errors: &mut Vec<String>,
) -> Result<BTreeMap<String, SymbolEconomicResult>> {
    let mut rows = BTreeMap::new();
    for symbol in &ctx.symbols {
        let incumbent_pnl = incumbent
            .portfolio_accounting
            .row_for_symbol(symbol)
            .and_then(|row| row.receipt_realized_pnl_units_6dp);
        let candidate_pnl = candidate
            .portfolio_accounting
            .row_for_symbol(symbol)
            .and_then(|row| row.receipt_realized_pnl_units_6dp);
        let (Some(incumbent_pnl), Some(candidate_pnl)) = (incumbent_pnl, candidate_pnl) else {
            errors.push(format!("validation_realized_pnl_unavailable:{symbol}"));
            continue;
        };
        rows.insert(
            symbol.clone(),
            symbol_economic_result(
                ctx.selection_mode,
                symbol,
                incumbent_pnl,
                candidate_pnl,
                incumbent.prediction_metrics.get(symbol),
                candidate.prediction_metrics.get(symbol),
            )?,
        );
    }
    Ok(rows)
}

pub(super) fn symbol_economic_result(
    mode: SelectionMode,
    symbol: &str,
    incumbent_pnl: i128,
    candidate_pnl: i128,
    incumbent_prediction: Option<&PredictionMetrics>,
    candidate_prediction: Option<&PredictionMetrics>,
) -> Result<SymbolEconomicResult> {
    let delta = candidate_pnl
        .checked_sub(incumbent_pnl)
        .with_context(|| format!("validation PnL delta overflow symbol={symbol}"))?;
    let candidate_profitable = candidate_pnl > 0;
    let beats_incumbent = delta > 0;
    let (prediction_comparable, prediction_improved) = if mode.is_profit() {
        ensure!(
            incumbent_prediction.is_none() && candidate_prediction.is_none(),
            "profit validation contains prediction evidence"
        );
        (None, None)
    } else {
        match (candidate_prediction, incumbent_prediction) {
            (Some(candidate), Some(incumbent)) => (
                Some(candidate.comparable_coverage(incumbent)),
                Some(scoring::prediction_improves(candidate, incumbent)?),
            ),
            _ => (Some(false), Some(false)),
        }
    };
    Ok(SymbolEconomicResult {
        symbol: symbol.to_string(),
        candidate_net_pnl_units_6dp: candidate_pnl,
        incumbent_net_pnl_units_6dp: incumbent_pnl,
        delta_units_6dp: delta,
        candidate_profitable,
        beats_incumbent,
        passed: candidate_profitable && beats_incumbent && prediction_improved.unwrap_or(true),
        prediction_comparable,
        prediction_improved,
    })
}

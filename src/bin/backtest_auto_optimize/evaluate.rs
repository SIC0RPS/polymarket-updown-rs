use super::*;
use polymarket_hft::strategy_runtime::prediction_metrics::PredictionMetrics;
use polymarket_hft::trade_session_ledger::CanonicalCoverageProof;

struct RawPackScore {
    capture: String,
    candidate_pnl_by_symbol: Option<BTreeMap<String, PortfolioAccountingPnlRow>>,
    candidate_entry_count: Option<u64>,
    candidate_profitable_market_count: Option<u64>,
    candidate_selection_score: Option<scoring::FeasibilityScore>,
    candidate_prediction_metrics: BTreeMap<String, PredictionMetrics>,
    initial_oracle_learning_sha256: String,
    initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    final_oracle_learning_sha256: Option<String>,
    accounting_coverage: CanonicalCoverageProof,
    error: Option<String>,
}

struct PendingEvaluation {
    index: usize,
    candidate_id: String,
    params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    origin: TrialOrigin,
    origin_id: Option<String>,
    elapsed_ms: u128,
    requested_stats: Option<CanonicalSessionStats>,
    requested_portfolio_accounting: Option<PortfolioAccountingProjectionProof>,
    pack_scores: Vec<RawPackScore>,
    capture_proofs: Vec<RetainedCaptureProof>,
}

type DefaultPackEvidence = (
    BTreeMap<String, PortfolioAccountingPnlRow>,
    u64,
    u64,
    scoring::FeasibilityScore,
    BTreeMap<String, PredictionMetrics>,
);

pub(super) fn shutdown_selection_evaluator(ctx: &RunContext) -> Result<()> {
    let resident = ctx
        .selection_evaluator
        .lock()
        .map_err(|_| anyhow!("selection evaluator mutex poisoned"))?
        .take();
    if let Some((capture, evaluator)) = resident {
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE selection_shutdown_begin capture={capture}"
        ));
        ctx.async_runtime
            .block_on(evaluator.shutdown())
            .with_context(|| format!("shutdown selection evaluator capture={capture}"))?;
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE selection_shutdown_done capture={capture}"
        ));
    }
    Ok(())
}

pub(super) fn evaluate_params(
    ctx: &RunContext,
    request: CandidateRequest,
    retain_canonical_proof: bool,
) -> Result<f64> {
    evaluate_params_batch(ctx, vec![request], retain_canonical_proof)?
        .pop()
        .context("single optimizer candidate score missing")
}

pub(super) fn evaluate_params_batch(
    ctx: &RunContext,
    requests: Vec<CandidateRequest>,
    retain_canonical_proof: bool,
) -> Result<Vec<f64>> {
    fs::create_dir_all(&ctx.run_root)
        .with_context(|| format!("create {}", ctx.run_root.display()))?;
    let mut candidates = Vec::with_capacity(requests.len());
    let mut pending = Vec::with_capacity(requests.len());
    for request in requests {
        let CandidateRequest {
            params_by_symbol,
            origin,
            origin_id,
        } = request;
        let index = ctx.trial_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let candidate_id = candidate_id_for_params(ctx, &params_by_symbol)?;
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE candidate_origin={origin:?} trial_index={index} candidate_id={candidate_id} origin_id={}",
            origin_id.as_deref().unwrap_or("none")
        ));
        let (strategy_config, per_symbol_strategy_config) =
            strategy_configs_for_params(ctx, &params_by_symbol)?;
        candidates.push(BacktestMatrixCandidate {
            id: candidate_id.clone(),
            strategy_config,
            per_symbol_strategy_config,
            per_symbol_parameter_values: params_by_symbol.clone(),
            initial_oracle_learning: BTreeMap::new(),
            oracle_direct_market_research: false,
            collect_prediction_metrics: ctx.selection_mode.collect_prediction_metrics(),
        });
        pending.push(PendingEvaluation {
            index,
            candidate_id,
            params_by_symbol,
            origin,
            origin_id,
            elapsed_ms: 0,
            requested_stats: None,
            requested_portfolio_accounting: None,
            pack_scores: Vec::with_capacity(ctx.retained_pack_count()),
            capture_proofs: Vec::with_capacity(ctx.retained_pack_count()),
        });
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let anchor_capture = ctx.primary_capture().display().to_string();
    let capture_order = ctx
        .selection_inputs
        .iter()
        .enumerate()
        .map(|(index, input)| (input.capture.display().to_string(), index))
        .collect::<BTreeMap<_, _>>();
    let resident_index = ctx
        .selection_evaluator
        .lock()
        .map_err(|_| anyhow!("selection evaluator mutex poisoned"))?
        .as_ref()
        .and_then(|(capture, _)| capture_order.get(capture).copied())
        .unwrap_or(0);
    for offset in 0..ctx.selection_inputs.len() {
        let input = &ctx.selection_inputs[(resident_index + offset) % ctx.selection_inputs.len()];
        let capture = input.capture.display().to_string();
        let mut active = ctx
            .selection_evaluator
            .lock()
            .map_err(|_| anyhow!("selection evaluator mutex poisoned"))?;
        if active
            .as_ref()
            .is_some_and(|(resident_capture, _)| resident_capture != &capture)
        {
            let (previous_capture, previous_evaluator) = active
                .take()
                .context("resident selection evaluator missing")?;
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_shutdown_begin capture={previous_capture}"
            ));
            ctx.async_runtime
                .block_on(previous_evaluator.shutdown())
                .with_context(|| {
                    format!("shutdown selection evaluator capture={previous_capture}")
                })?;
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_shutdown_done capture={previous_capture}"
            ));
        }
        let loading_capture = active.is_none();
        if loading_capture {
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_load_begin capture={capture}"
            ));
            let mut evaluator =
                BacktestMatrixEvaluator::load_prepared_for_symbols_with_optimizer_path(
                    &ctx.cfg,
                    &input.capture,
                    &ctx.symbols,
                    &ctx.optimizer_spec_path,
                    Some(
                        input
                            .execution_timing
                            .as_ref()
                            .context("frozen selection execution timing missing")?,
                    ),
                )?;
            evaluator.configure_candidate_pool(
                ctx.candidate_slots,
                true,
                ctx.optimizer_observability.handle(),
            )?;
            *active = Some((capture.clone(), evaluator));
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_load_done capture={capture}"
            ));
        }
        let evaluator = &active
            .as_ref()
            .context("selection evaluator missing after load")?
            .1;
        ensure!(
            evaluator.replay_window()? == (input.replay_start_ms, input.replay_end_ms),
            "optimizer frozen replay window changed capture={}",
            input.capture.display()
        );
        if loading_capture {
            let root_spec_sha256 = candidate_proof::file_sha256(&ctx.optimizer_spec_path)?;
            for (symbol, prepared_proof_identity) in candidate_proof::build_symbol_proof_identities(
                &ctx.symbols,
                &input.capture,
                &ctx.cfg,
                &root_spec_sha256,
                ctx.objective_profile.sha256(),
            )? {
                ensure!(
                    input.prepared_proof_identities_by_symbol.get(&symbol)
                        == Some(&prepared_proof_identity),
                    "optimizer frozen prepared proof changed symbol={symbol} capture={}",
                    input.capture.display()
                );
            }
        }
        let initial_oracle_learning = evaluator.captured_initial_oracle_learning().clone();
        let initial_oracle_learning_sha256 =
            candidate_proof::oracle_learning_sha256(&initial_oracle_learning)?;
        ensure!(
            initial_oracle_learning_sha256 == input.initial_oracle_learning_sha256,
            "optimizer frozen initial learner changed capture={}",
            input.capture.display()
        );
        let initial_oracle_learning_cutoffs_ms = initial_oracle_learning
            .iter()
            .map(|(symbol, learning)| (symbol.clone(), learning.knowledge_cutoff_ms()))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            initial_oracle_learning_cutoffs_ms == input.initial_oracle_learning_cutoffs_ms,
            "optimizer frozen initial learner cutoff changed capture={}",
            input.capture.display()
        );
        for candidate in &mut candidates {
            candidate.initial_oracle_learning = initial_oracle_learning.clone();
        }
        let mut score_by_candidate = BTreeMap::new();
        for (candidate_group, pending_group) in candidates
            .chunks(ctx.candidate_slots)
            .zip(pending.chunks_mut(ctx.candidate_slots))
        {
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_replay_begin capture={capture} candidates={}",
                candidate_group.len()
            ));
            let summaries = ctx
                .async_runtime
                .block_on(evaluator.evaluate_candidate_metrics(
                    &ctx.cfg,
                    candidate_group,
                    ctx.candidate_slots,
                ))
                .with_context(|| format!("run retained prepared pack {capture}"))?;
            crate::summary::publish_progress(format_args!(
                "CMAES_AUTO_OPTIMIZE selection_replay_done capture={capture} candidates={}",
                summaries.len()
            ));
            ensure!(
                summaries.len() == pending_group.len(),
                "optimizer candidate result count mismatch capture={} expected={} actual={}",
                capture,
                pending_group.len(),
                summaries.len()
            );
            let mut summaries_by_candidate = BTreeMap::<_, Vec<_>>::new();
            for summary in summaries {
                summaries_by_candidate
                    .entry(summary.candidate_id.clone())
                    .or_default()
                    .push(summary);
            }
            for item in pending_group {
                let summary = summaries_by_candidate
                    .get_mut(&item.candidate_id)
                    .and_then(Vec::pop)
                    .with_context(|| {
                        format!(
                            "optimizer candidate result missing capture={} candidate_id={}",
                            capture, item.candidate_id
                        )
                    })?;
                item.elapsed_ms = item
                    .elapsed_ms
                    .checked_add(summary.elapsed_ms)
                    .context("optimizer multi-pack elapsed time overflow")?;
                let oracle_learning_sha256 =
                    candidate_proof::oracle_learning_sha256(&summary.oracle_learning)?;
                let stats = summary.canonical_session_stats;
                let portfolio_accounting = summary.portfolio_accounting;
                let candidate_prediction_metrics = summary.prediction_metrics;
                let score_result = endpoint_joint_score(
                    &ctx.objective_profile,
                    &ctx.symbols,
                    &stats,
                    &portfolio_accounting,
                );
                let (
                    candidate_pnl_by_symbol,
                    candidate_entry_count,
                    candidate_profitable_market_count,
                    mut candidate_selection_score,
                    accounting_coverage,
                    score_error,
                ) = match score_result {
                    Ok(score) => (
                        Some(score.pnl_by_symbol),
                        Some(score.entry_count),
                        Some(score.profitable_market_count),
                        Some(score.selection_score),
                        score.accounting_coverage,
                        None,
                    ),
                    Err(error) => (
                        None,
                        None,
                        None,
                        None,
                        stats.proof_for_eligible_markets(&portfolio_accounting),
                        Some(format!("endpoint_score_unavailable:{error:#}")),
                    ),
                };
                let mut errors = Vec::new();
                if let Err(error) = scoring::validate_prediction_evidence(
                    ctx.selection_mode,
                    &ctx.symbols,
                    &candidate_prediction_metrics,
                ) {
                    errors.push(format!("invalid_prediction_evidence:{error:#}"));
                }
                if ctx.selection_mode.collect_prediction_metrics()
                    && !candidate_prediction_metrics
                        .values()
                        .all(PredictionMetrics::is_admissible)
                {
                    errors.push("inadmissible_prediction_evidence".to_string());
                }
                if summary.failed {
                    errors.push("candidate_summary_failed".to_string());
                }
                if !accounting_coverage.is_complete() {
                    candidate_selection_score = None;
                    errors.push(format!(
                        "incomplete_accounting_coverage:{:?}",
                        accounting_coverage.gaps
                    ));
                }
                if let Some(error) = score_error {
                    errors.push(error);
                }
                let error = (!errors.is_empty()).then(|| errors.join("|"));
                let fingerprint = (
                    candidate_pnl_by_symbol.clone(),
                    candidate_entry_count,
                    candidate_profitable_market_count,
                    candidate_selection_score,
                    accounting_coverage.clone(),
                    error.clone(),
                    oracle_learning_sha256,
                    candidate_prediction_metrics.clone(),
                );
                if let Some(previous) =
                    score_by_candidate.insert(item.candidate_id.clone(), fingerprint.clone())
                {
                    ensure!(
                    previous == fingerprint,
                    "duplicate optimizer candidate produced nondeterministic pack score capture={} candidate_id={}",
                    capture,
                    item.candidate_id
                );
                }
                if retain_canonical_proof {
                    item.capture_proofs.push(RetainedCaptureProof {
                        capture: capture.clone(),
                        canonical_session_stats: stats.clone(),
                        portfolio_accounting: portfolio_accounting.clone(),
                        initial_oracle_learning_sha256: initial_oracle_learning_sha256.clone(),
                        initial_oracle_learning_cutoffs_ms: initial_oracle_learning_cutoffs_ms
                            .clone(),
                        final_oracle_learning_sha256: fingerprint.6.clone(),
                    });
                }
                if capture == anchor_capture {
                    ensure!(
                        item.requested_stats.replace(stats).is_none(),
                        "optimizer anchor capture produced duplicate stats"
                    );
                    ensure!(
                        item.requested_portfolio_accounting
                            .replace(portfolio_accounting)
                            .is_none(),
                        "optimizer anchor capture produced duplicate endpoint accounting"
                    );
                }
                item.pack_scores.push(RawPackScore {
                    capture: capture.clone(),
                    candidate_pnl_by_symbol,
                    candidate_entry_count,
                    candidate_profitable_market_count,
                    candidate_selection_score,
                    candidate_prediction_metrics,
                    initial_oracle_learning_sha256: initial_oracle_learning_sha256.clone(),
                    initial_oracle_learning_cutoffs_ms: initial_oracle_learning_cutoffs_ms.clone(),
                    final_oracle_learning_sha256: Some(fingerprint.6.clone()),
                    accounting_coverage,
                    error,
                });
            }
            ensure!(
                summaries_by_candidate.values().all(Vec::is_empty),
                "optimizer produced unconsumed candidate results capture={capture}"
            );
        }
        drop(active);
    }

    finish_evaluations(
        ctx,
        pending,
        &capture_order,
        &anchor_capture,
        retain_canonical_proof,
    )
}

fn default_pack_evidence(
    ctx: &RunContext,
    pending: &[PendingEvaluation],
) -> Result<BTreeMap<String, DefaultPackEvidence>> {
    let trials = ctx
        .trials
        .lock()
        .map_err(|_| anyhow!("trial mutex poisoned"))?;
    let mut defaults = BTreeMap::new();
    if let Some(default) = trials.first() {
        ensure!(
            default.index == 1
                && default.origin == TrialOrigin::Default
                && default.selection_mode == ctx.selection_mode
                && default.complete_coverage(),
            "optimizer applied-default trial is invalid"
        );
        for pack in &default.retained_pack_scores {
            ensure!(
                pack.candidate_selection_score == Some(pack.default_selection_score)
                    && pack.candidate_prediction_metrics == pack.default_prediction_metrics,
                "optimizer applied-default selection identity changed"
            );
            let pnl = pack
                .candidate_pnl_by_symbol
                .clone()
                .context("optimizer applied-default retained-pack PnL missing")?;
            ensure!(
                defaults
                    .insert(
                        pack.capture.clone(),
                        (
                            pnl,
                            pack.default_entry_count,
                            pack.default_profitable_market_count,
                            pack.default_selection_score,
                            pack.default_prediction_metrics.clone(),
                        ),
                    )
                    .is_none(),
                "duplicate retained pack identity {}",
                pack.capture
            );
        }
    } else {
        ensure!(
            pending.len() == 1 && pending[0].origin == TrialOrigin::Default,
            "optimizer applied-default pack scores were not established first"
        );
        for pack in &pending[0].pack_scores {
            ensure!(
                pack.error.is_none() && pack.accounting_coverage.is_complete(),
                "applied default is invalid on retained pack {} error={:?} gaps={:?} prediction_metrics={:?}",
                pack.capture,
                pack.error,
                pack.accounting_coverage.gaps,
                pack.candidate_prediction_metrics
            );
            let pnl = pack.candidate_pnl_by_symbol.clone().with_context(|| {
                format!(
                    "applied default PnL missing on retained pack {}",
                    pack.capture
                )
            })?;
            let selection_score = pack
                .candidate_selection_score
                .context("applied default selection score missing")?;
            ensure!(
                defaults
                    .insert(
                        pack.capture.clone(),
                        (
                            pnl,
                            pack.candidate_entry_count
                                .context("applied default entry count missing")?,
                            pack.candidate_profitable_market_count
                                .context("applied default profitable market count missing")?,
                            selection_score,
                            pack.candidate_prediction_metrics.clone(),
                        ),
                    )
                    .is_none(),
                "duplicate retained pack identity {}",
                pack.capture
            );
        }
    }
    Ok(defaults)
}

fn finish_evaluations(
    ctx: &RunContext,
    pending: Vec<PendingEvaluation>,
    capture_order: &BTreeMap<String, usize>,
    anchor_capture: &str,
    retain_canonical_proof: bool,
) -> Result<Vec<f64>> {
    let default_pack_evidence = default_pack_evidence(ctx, &pending)?;
    let mut scores = Vec::with_capacity(pending.len());
    for mut item in pending {
        item.pack_scores.sort_by_key(|raw| {
            capture_order
                .get(&raw.capture)
                .copied()
                .unwrap_or(usize::MAX)
        });
        item.capture_proofs.sort_by_key(|proof| {
            capture_order
                .get(&proof.capture)
                .copied()
                .unwrap_or(usize::MAX)
        });
        ensure!(
            item.pack_scores.len() == default_pack_evidence.len(),
            "optimizer retained-pack score coverage mismatch candidate_id={}",
            item.candidate_id
        );
        let mut pack_scores = Vec::with_capacity(item.pack_scores.len());
        for raw in item.pack_scores {
            let (
                default_pnl_by_symbol,
                default_entry_count,
                default_profitable_market_count,
                default_selection_score,
                default_prediction_metrics,
            ) = default_pack_evidence
                .get(&raw.capture)
                .with_context(|| format!("default score missing retained pack {}", raw.capture))?
                .clone();
            let mut pack = RetainedPackScore {
                selection_mode: ctx.selection_mode,
                capture: raw.capture,
                candidate_pnl_by_symbol: raw.candidate_pnl_by_symbol,
                default_pnl_by_symbol,
                candidate_entry_count: raw.candidate_entry_count,
                default_entry_count,
                candidate_profitable_market_count: raw.candidate_profitable_market_count,
                default_profitable_market_count,
                candidate_selection_score: raw.candidate_selection_score,
                default_selection_score,
                candidate_prediction_metrics: raw.candidate_prediction_metrics,
                default_prediction_metrics,
                initial_oracle_learning_sha256: raw.initial_oracle_learning_sha256,
                initial_oracle_learning_cutoffs_ms: raw.initial_oracle_learning_cutoffs_ms,
                final_oracle_learning_sha256: raw.final_oracle_learning_sha256,
                accounting_coverage: raw.accounting_coverage,
                error: raw.error,
            };
            if !pack.prediction_coverage_complete() {
                pack.error = Some(match pack.error.take() {
                    Some(error) => format!("{error}|incomparable_prediction_coverage"),
                    None => "incomparable_prediction_coverage".to_string(),
                });
            }
            pack_scores.push(pack);
        }
        let selection_score = pack_scores
            .iter()
            .all(RetainedPackScore::is_eligible)
            .then(|| {
                pack_scores
                    .first()
                    .and_then(|pack| pack.candidate_selection_score)
            })
            .flatten();
        let requested_stats = item
            .requested_stats
            .context("optimizer anchor-capture stats missing")?;
        let requested_portfolio_accounting = item
            .requested_portfolio_accounting
            .context("optimizer anchor-capture endpoint accounting missing")?;
        let oracle_learning_sha256 = pack_scores
            .first()
            .and_then(|pack| pack.final_oracle_learning_sha256.clone())
            .context("anchor final learner digest missing")?;
        let record = TrialRecord::from_pack_results(
            &ctx.symbols,
            anchor_capture,
            item.index,
            item.candidate_id,
            item.elapsed_ms,
            item.params_by_symbol,
            requested_stats,
            requested_portfolio_accounting,
            retain_canonical_proof,
            item.origin,
            item.origin_id,
            selection_score,
            pack_scores,
            item.capture_proofs,
            oracle_learning_sha256,
        )?;
        let transported = if record.complete_coverage() {
            scoring::trial_selection_score(&record)?.cma_transport()?
        } else {
            f64::MIN
        };
        ensure!(
            transported.is_finite(),
            "optimizer transport score is not finite"
        );
        append_trial(ctx, record)?;
        scores.push(transported);
    }
    Ok(scores)
}

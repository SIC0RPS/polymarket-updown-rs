use super::*;

pub(in crate::execution::supervisor) struct ActiveSubmissionCompletion {
    pub(in crate::execution::supervisor) decision_ts_ms: u64,
    pub(in crate::execution::supervisor) supervisor_admission_ts_ms: u64,
    pub(in crate::execution::supervisor) decision_instant: Option<Instant>,
    pub(in crate::execution::supervisor) handoff_instant: Option<Instant>,
    pub(in crate::execution::supervisor) oldest_pending_intent_age_us: Option<u64>,
    pub(in crate::execution::supervisor) intents: Vec<OrderIntent>,
    pub(in crate::execution::supervisor) reservation_ids: Vec<String>,
    pub(in crate::execution::supervisor) outcome: ExecutionBatchSendOutcome,
}

pub(in crate::execution::supervisor) struct ActiveSubmissions {
    futures: FuturesUnordered<BoxFuture<'static, ActiveSubmissionCompletion>>,
    pending_since: HashMap<u64, Instant>,
    pending_scopes: HashMap<ScopedOrderFenceKey, usize>,
}

impl ActiveSubmissions {
    pub(in crate::execution::supervisor) fn new() -> Self {
        Self {
            futures: FuturesUnordered::new(),
            pending_since: HashMap::new(),
            pending_scopes: HashMap::new(),
        }
    }

    pub(in crate::execution::supervisor) fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    pub(in crate::execution::supervisor) fn push(
        &mut self,
        future: BoxFuture<'static, ActiveSubmissionCompletion>,
        intents: &[OrderIntent],
        decision_instant: Option<Instant>,
    ) {
        for intent in intents {
            if let (Some(intent_id), Some(started)) = (intent.execution_intent_id, decision_instant)
            {
                self.pending_since.insert(intent_id, started);
            }
            if let Some(market) = intent_market_id(intent) {
                let scope = ScopedOrderFenceKey {
                    market: market.to_string(),
                    token: intent.instrument.clone(),
                };
                *self.pending_scopes.entry(scope).or_default() += 1;
            }
        }
        self.futures.push(future);
    }

    pub(in crate::execution::supervisor) fn has_pending_scope(
        &self,
        scope: &ScopedOrderFenceKey,
    ) -> bool {
        self.pending_scopes.contains_key(scope)
    }

    pub(in crate::execution::supervisor) fn has_pending_market(&self, market: &str) -> bool {
        self.pending_scopes
            .keys()
            .any(|scope| scope.market == market)
    }

    pub(in crate::execution::supervisor) async fn next_completion(
        &mut self,
    ) -> Option<ActiveSubmissionCompletion> {
        let mut completion = self.futures.next().await?;
        let owner_now = Instant::now();
        completion.oldest_pending_intent_age_us = self
            .pending_since
            .values()
            .copied()
            .min()
            .and_then(|started| owner_now.checked_duration_since(started))
            .map(duration_to_us);
        for intent in &completion.intents {
            if let Some(intent_id) = intent.execution_intent_id {
                self.pending_since.remove(&intent_id);
            }
            if let Some(market) = intent_market_id(intent) {
                let scope = ScopedOrderFenceKey {
                    market: market.to_string(),
                    token: intent.instrument.clone(),
                };
                if let Some(pending) = self.pending_scopes.get_mut(&scope) {
                    *pending -= 1;
                    if *pending == 0 {
                        self.pending_scopes.remove(&scope);
                    }
                }
            }
        }
        Some(completion)
    }
}

#[path = "submit/completion.rs"]
mod completion;
pub(in crate::execution::supervisor) use completion::apply_submission_completion;

#[allow(clippy::too_many_arguments, reason = "execution/risk/context plumbing")]
pub(in crate::execution::supervisor) async fn submit_ready_orders(
    now_ms: u64,
    decision_ts_ms: u64,
    decision_instant: Option<Instant>,
    handoff_instant: Option<Instant>,
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_fill_ctx: &OrderFillContextRegistry,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
    market_projection: Option<&crate::polymarket::state::MarketProjection>,
    open_orders: &mut OpenOrdersState,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    live_quote_cancel_index: &mut HashMap<QuoteCancelKey, HashSet<String>>,
    seen_quote_cancel_keys: &mut HashSet<QuoteCancelKey>,
    fenced_scopes: &HashSet<ScopedOrderFenceKey>,
    post_only_rejects: &mut HashMap<String, PostOnlyRejectState>,
    obs: &Observability,
    capture: &MaybeCapture,
    open_orders_tx: &watch::Sender<OpenOrdersState>,
    hard_submit_block: &mut Option<String>,
    execution_order_type: PolymarketOrderType,
    resting_order_timeout_ms: u64,
    mut to_place: Vec<OrderIntent>,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
    active_submissions: Option<&mut ActiveSubmissions>,
    submission_recovery: &mut SubmissionRecovery,
    pause: bool,
    runtime_control_rx: Option<&watch::Receiver<RuntimeConfigState>>,
    portfolio_rx: Option<&watch::Receiver<PortfolioState>>,
    active_cancellations: Option<&mut ActiveCancellations>,
) -> Result<()> {
    let mut scope_error = None;
    to_place.retain(|intent| {
        if scope_error.is_some() {
            return true;
        }
        let disposition = if hard_submit_block.is_some() {
            Some("hard_submit_blocked")
        } else if intent.side() == Side::Buy
            && (pause
                || intent.buy_authorization().is_some_and(|authorization| {
                    !strategy_cfg.is_asset_enabled(authorization.symbol.as_str())
                })
                || runtime_control_rx.is_some_and(|rx| {
                    market_selection_for_intent(intent, market_projection).is_none_or(|market| {
                        !rx.borrow().allows_new_market_entry(market.window_start_ms)
                    })
                }))
        {
            Some("final_local_admission_block")
        } else {
            None
        };
        if let Some(disposition) = disposition {
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block {disposition} token={} side={:?}",
                    intent.instrument,
                    intent.side()
                ),
            );
            scope_error = record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                intent,
                disposition,
                now_ms,
                execution_audit_records,
            )
            .err();
            return false;
        }
        let Some(market) = intent_market_id(intent) else {
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block missing_market_context token={} side={:?}",
                    intent.instrument,
                    intent.side()
                ),
            );
            scope_error = record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                intent,
                "missing_market_context",
                now_ms,
                execution_audit_records,
            )
            .err();
            return false;
        };
        let scope = ScopedOrderFenceKey {
            market: market.to_string(),
            token: intent.instrument.clone(),
        };
        if fenced_scopes.contains(&scope)
            || active_cancellations
                .as_deref()
                .is_some_and(|pending| pending.blocks_scope(&scope))
        {
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block cancel_scope_reconciling market={} token={} side={:?}",
                    scope.market,
                    scope.token,
                    intent.side()
                ),
            );
            scope_error = record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                intent,
                "cancel_scope_reconciling",
                now_ms,
                execution_audit_records,
            )
            .err();
            return false;
        }
        true
    });
    if let Some(error) = scope_error {
        record_batch_pre_submission_abandoned(
            risk,
            ctf_inventory_runtime,
            submission_recovery,
            &to_place,
            "batch_admission_failed",
            now_ms,
            execution_audit_records,
        )?;
        return Err(error);
    }
    if to_place.is_empty() {
        return Ok(());
    }
    let latest_odds_state = odds_rx.map(|rx| rx.borrow().clone());
    let current_portfolio = portfolio_rx.map(|rx| rx.borrow().clone());
    let mut projected_open_orders = current_portfolio.as_ref().map(|_| open_orders.clone());
    let selection = market_projection.and_then(|projection| projection.current_selection());
    let mut latest_safe_to_place: Vec<OrderIntent> = Vec::with_capacity(to_place.len());
    for mut intent in to_place {
        apply_configured_resting_order_type(
            &mut intent,
            execution_order_type,
            resting_order_timeout_ms,
            now_ms,
        );
        let mut block_reason: Option<String> = None;
        if let Some(reason) = order_lifecycle.block_reason(&intent, open_orders, now_ms) {
            emit_local_order_block(obs, capture, now_ms, reason.clone());
            block_reason = Some(reason);
        }
        if let Some(reason) =
            active_post_only_reject_cooldown_reason(post_only_rejects, &intent, now_ms)
        {
            emit_local_order_block(obs, capture, now_ms, reason.clone());
            block_reason = Some(reason);
        }
        let extra_buffer_ticks =
            active_post_only_extra_buffer_ticks(post_only_rejects, &intent, strategy_cfg, now_ms);
        let effective_buffer_ticks =
            effective_admission_buffer_ticks(strategy_cfg, extra_buffer_ticks);
        let final_send_buffer_ticks =
            effective_buffer_ticks.saturating_add(POST_ONLY_FINAL_SEND_EXTRA_BUFFER_TICKS);
        let original_limit_px = intent.limit_px;
        enforce_post_only_non_crossing(
            &mut intent,
            latest_odds_state.as_deref(),
            final_send_buffer_ticks,
        );
        if normalize_order_price_for_vendor(&mut intent, latest_odds_state.as_deref()).is_none() {
            let reason = format!(
                "local_order_block missing_verified_tick token={} side={:?}",
                intent.instrument,
                intent.side()
            );
            emit_local_order_block(obs, capture, now_ms, reason.clone());
            block_reason = Some(reason);
        }
        if block_reason.is_none() {
            if let Some(reason) = order_lifecycle.normalized_instruction_block_reason(&intent) {
                emit_local_order_block(obs, capture, now_ms, reason.clone());
                block_reason = Some(reason);
            }
        }
        if intent.post_only
            && (intent.limit_px - original_limit_px).abs()
                > crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
        {
            tracing::info!(
                target: "execution.supervisor_batch",
                token = %intent.instrument,
                side = ?intent.side(),
                old_px = original_limit_px,
                new_px = intent.limit_px,
                buffer_ticks = final_send_buffer_ticks,
                "local_order_reprice_post_only_latest_book"
            );
        }
        if block_reason.is_none() {
            if let Some(reason) = local_venue_order_block_reason(
                &intent,
                latest_odds_state.as_deref(),
                order_book_max_age_ms(&intent, strategy_cfg),
                final_send_buffer_ticks,
                now_ms,
            ) {
                emit_local_order_block(obs, capture, now_ms, reason.clone());
                block_reason = Some(reason);
            }
        }
        if block_reason.is_some() && !order_lifecycle.retains_execution_intent(&intent, open_orders)
        {
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "final_local_admission_block",
                now_ms,
                execution_audit_records,
            )?;
        }
        if block_reason.is_none() {
            if let (Some(portfolio), Some(projected)) =
                (current_portfolio.as_ref(), projected_open_orders.as_mut())
            {
                let admission = if intent.side() == Side::Sell {
                    risk.check_sell_inventory(&intent, portfolio, projected)
                } else {
                    risk.check_order(
                        &intent,
                        portfolio,
                        projected,
                        selection,
                        latest_odds_state.as_deref(),
                        now_ms,
                    )
                };
                if let Err(block) = admission {
                    emit_risk_admission_block(obs, capture, now_ms, block, &intent);
                    record_pre_submission_abandoned(
                        risk,
                        ctf_inventory_runtime,
                        submission_recovery,
                        &intent,
                        if intent.side() == Side::Sell {
                            "sell_inventory_block"
                        } else {
                            "risk_admission_block"
                        },
                        now_ms,
                        execution_audit_records,
                    )?;
                    continue;
                }
                seed_projected_open_order(now_ms, projected, &intent);
            }
            order_lifecycle.record_pending(open_orders, &intent, now_ms);
            latest_safe_to_place.push(intent);
        }
    }
    if latest_safe_to_place.is_empty() {
        return Ok(());
    }
    exec.prime_order_build_metadata(
        &latest_safe_to_place,
        latest_odds_state.as_deref(),
        market_projection,
    );

    let to_place = latest_safe_to_place;
    publish_open_orders(open_orders_tx, obs, open_orders);
    let mut reservation_ids = Vec::with_capacity(to_place.len());
    for intent in &to_place {
        match risk.reserve_inflight_order(intent, market_projection) {
            Ok(reservation_id) => reservation_ids.push(reservation_id),
            Err(block) => {
                for (pending, reservation_id) in to_place.iter().zip(&reservation_ids) {
                    if pending.side() == Side::Sell
                        && !submission_recovery.retains_execution_intent(pending)
                    {
                        risk.release_inflight_order(reservation_id);
                    }
                }
                let abandoned = record_batch_pre_submission_abandoned(
                    risk,
                    ctf_inventory_runtime,
                    submission_recovery,
                    &to_place,
                    "risk_reservation_failed",
                    now_ms,
                    execution_audit_records,
                );
                for pending in &to_place {
                    if !submission_recovery.retains_execution_intent(pending) {
                        order_lifecycle.record_terminal(open_orders, pending, now_ms);
                    }
                }
                abandoned?;
                emit_risk_admission_block(obs, capture, now_ms, block, intent);
                publish_open_orders(open_orders_tx, obs, open_orders);
                return Ok(());
            }
        }
    }
    if let Err(error) = submission_recovery.register_inflight(&to_place, &reservation_ids) {
        let abandoned = record_batch_pre_submission_abandoned(
            risk,
            ctf_inventory_runtime,
            submission_recovery,
            &to_place,
            "submission_owner_registration_failed",
            now_ms,
            execution_audit_records,
        );
        for (intent, reservation_id) in to_place.iter().zip(&reservation_ids) {
            if !submission_recovery.retains_execution_intent(intent) {
                if intent.side() == Side::Sell {
                    risk.release_inflight_order(reservation_id);
                }
                order_lifecycle.record_terminal(open_orders, intent, now_ms);
            }
        }
        abandoned?;
        return Err(error);
    }
    let pre_send_ids: HashSet<String> = open_orders.orders.keys().cloned().collect();
    let send_exec = exec.clone();
    let send_intents = to_place.clone();
    let completion = async move {
        let outcome = send_exec
            .send_batch(&send_intents, Some(&pre_send_ids))
            .await;
        ActiveSubmissionCompletion {
            decision_ts_ms,
            supervisor_admission_ts_ms: now_ms,
            decision_instant,
            handoff_instant,
            oldest_pending_intent_age_us: None,
            intents: send_intents,
            reservation_ids,
            outcome,
        }
    }
    .boxed();
    if let Some(active_submissions) = active_submissions {
        active_submissions.push(completion, &to_place, decision_instant);
        return Ok(());
    }
    let completion = completion.await;
    apply_submission_completion(
        clock.now_ms(),
        risk,
        ctf_inventory_runtime,
        order_fill_ctx,
        strategy_cfg,
        market_projection,
        open_orders,
        order_lifecycle,
        live_quote_cancel_index,
        seen_quote_cancel_keys,
        post_only_rejects,
        obs,
        capture,
        hard_submit_block,
        execution_audit_records,
        submission_recovery,
        completion,
    )
}

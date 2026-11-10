use super::*;

#[allow(clippy::too_many_arguments, reason = "shared batch admission owners")]
pub(super) fn prepare_admitted_intents(
    now_ms: u64,
    pause: bool,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    order_lifecycle: &mut OrderLifecycleSupervisor,
    post_only_rejects: &HashMap<String, PostOnlyRejectState>,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    portfolio: &PortfolioState,
    odds_state: Option<&PolymarketOddsState>,
    market_projection: Option<&MarketProjection>,
    open_orders: &mut OpenOrdersState,
    fenced_scopes: &HashSet<ScopedOrderFenceKey>,
    execution_order_type: PolymarketOrderType,
    resting_order_timeout_ms: u64,
    obs: &Observability,
    capture: &MaybeCapture,
    intents: Vec<OrderIntent>,
    execution_audit_records: &mut Vec<EngineTradeJournalRecord>,
    submission_recovery: &SubmissionRecovery,
) -> Result<(Vec<OrderIntent>, HashSet<QuoteKey>, HashSet<QuoteKey>)> {
    let selection =
        market_projection.and_then(|projection| projection.current_selection().cloned());
    let mut prepared: Vec<OrderIntent> = Vec::new();
    let mut replacement_quote_order_ids: HashMap<QuoteKey, String> = HashMap::new();
    let mut preserved_quote_keys: HashSet<QuoteKey> = HashSet::new();
    let mut cancel_quote_keys: HashSet<QuoteKey> = HashSet::new();
    let now = now_ms;
    for mut intent in intents {
        normalize_order_qty_for_vendor(&mut intent, risk);
        let post_only_size_multiplier =
            active_post_only_size_multiplier(post_only_rejects, &intent, strategy_cfg, now);
        if intent.post_only && post_only_size_multiplier < 1.0 {
            intent.qty = truncate_to_vendor_qty(intent.qty * post_only_size_multiplier);
        }
        if is_unsafe_buy_without_close_hint(&intent) {
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "unauthorized_buy",
                now_ms,
                execution_audit_records,
            )?;
            continue;
        }
        if is_ctf_inventory_quote_group(&intent) {
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block ctf_inventory_sell_authority_disabled token={} side={} quote_group={}",
                    intent.instrument,
                    format!("{:?}", intent.side()),
                    intent.quote_group.as_deref().unwrap_or("-")
                ),
            );
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "ctf_inventory_quote_block",
                now_ms,
                execution_audit_records,
            )?;
            continue;
        }
        let extra_buffer_ticks =
            active_post_only_extra_buffer_ticks(post_only_rejects, &intent, strategy_cfg, now);
        enforce_post_only_non_crossing(
            &mut intent,
            odds_state.as_deref(),
            effective_admission_buffer_ticks(strategy_cfg, extra_buffer_ticks),
        );
        apply_configured_resting_order_type(
            &mut intent,
            execution_order_type,
            resting_order_timeout_ms,
            now_ms,
        );
        let Some(tick) = normalize_order_price_for_vendor(&mut intent, odds_state.as_deref())
        else {
            if let Some(key) = quote_key_from_intent(&intent) {
                cancel_quote_keys.insert(key);
            }
            emit_local_order_block(
                obs,
                capture,
                now_ms,
                format!(
                    "local_order_block missing_verified_tick token={} side={:?}",
                    intent.instrument,
                    intent.side()
                ),
            );
            record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                "missing_verified_tick",
                now_ms,
                execution_audit_records,
            )?;
            continue;
        };
        let replacement_order_id =
            match working_close_quote_replacement_for_open_orders(open_orders, &intent, tick) {
                Some((existing, WorkingCloseQuoteReplacementDecision::PreserveExisting)) => {
                    if let Some(key) = quote_key_from_intent(&intent) {
                        preserved_quote_keys.insert(key);
                    }
                    if existing.execution_intent_id != intent.execution_intent_id {
                        record_pre_submission_abandoned(
                            risk,
                            ctf_inventory_runtime,
                            submission_recovery,
                            &intent,
                            "existing_order_preserved",
                            now_ms,
                            execution_audit_records,
                        )?;
                    }
                    continue;
                }
                Some((existing, WorkingCloseQuoteReplacementDecision::ReplaceExisting)) => {
                    Some(existing.order_id)
                }
                Some((_, WorkingCloseQuoteReplacementDecision::NotApplicable)) | None => None,
            };
        if replacement_order_id.is_none() {
            pin_close_intent_to_existing_order(&mut intent, open_orders);
        }
        if replacement_order_id.is_none() {
            if let Some(reason) = order_lifecycle.block_reason(&intent, open_orders, now_ms) {
                emit_local_order_block(obs, capture, now_ms, reason);
                if !order_lifecycle.retains_execution_intent(&intent, open_orders) {
                    record_pre_submission_abandoned(
                        risk,
                        ctf_inventory_runtime,
                        submission_recovery,
                        &intent,
                        "order_lifecycle_block",
                        now_ms,
                        execution_audit_records,
                    )?;
                }
                continue;
            }
        }
        if let (Some(key), Some(order_id)) = (quote_key_from_intent(&intent), replacement_order_id)
        {
            replacement_quote_order_ids.insert(key, order_id);
        }
        prepared.push(intent);
    }
    let mut projected_open_orders = open_orders.clone();
    let mut passed: Vec<OrderIntent> = Vec::new();
    if !prepared.is_empty() {
        let (coalesced, superseded) = coalesce_latest_intents(prepared);
        prepared = coalesced;
        if !superseded.is_empty() {
            tracing::info!(
                target: "execution.supervisor_batch",
                dropped = superseded.len(),
                "intent_batch_coalesced"
            );
            record_batch_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &superseded,
                "superseded",
                now_ms,
                execution_audit_records,
            )?;
        }
        if !prepared.is_empty() {
            let (pause_filtered, pause_drops) = filter_buy_entry_intents(
                prepared,
                pause,
                "manual_pause",
                now_ms,
                risk,
                ctf_inventory_runtime,
                submission_recovery,
            )?;
            prepared = pause_filtered;
            if pause_drops > 0 {
                tracing::info!(
                    target: "execution.supervisor_batch",
                    blocked_buys = pause_drops,
                    "PAUSE_drop"
                );
            }
        }
        let mut scope_error = None;
        prepared.retain(|intent| {
            if scope_error.is_some() {
                return true;
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
            if fenced_scopes.contains(&scope) {
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
                &prepared,
                "batch_admission_failed",
                now_ms,
                execution_audit_records,
            )?;
            return Err(error);
        }
        for intent in prepared {
            let replacement_order_id = quote_key_from_intent(&intent)
                .and_then(|key| replacement_quote_order_ids.get(&key).cloned());
            if replacement_order_id.is_none() {
                if let Some(reason) =
                    order_lifecycle.block_reason(&intent, &mut projected_open_orders, now_ms)
                {
                    emit_local_order_block(obs, capture, now_ms, reason);
                    if !order_lifecycle.retains_execution_intent(&intent, &projected_open_orders) {
                        record_pre_submission_abandoned(
                            risk,
                            ctf_inventory_runtime,
                            submission_recovery,
                            &intent,
                            "order_lifecycle_block",
                            now_ms,
                            execution_audit_records,
                        )?;
                    }
                    continue;
                }
            }
            if let Some(reason) =
                active_post_only_reject_cooldown_reason(post_only_rejects, &intent, now_ms)
            {
                tracing::info!(target: "execution.supervisor_batch", reason, "post_only_reject_cooldown");
                record_pre_submission_abandoned(
                    risk,
                    ctf_inventory_runtime,
                    submission_recovery,
                    &intent,
                    "post_only_reject_cooldown",
                    now_ms,
                    execution_audit_records,
                )?;
                continue;
            }
            let extra_buffer_ticks = active_post_only_extra_buffer_ticks(
                post_only_rejects,
                &intent,
                strategy_cfg,
                now_ms,
            );
            if let Some(reason) = local_venue_order_block_reason(
                &intent,
                odds_state.as_deref(),
                order_book_max_age_ms(&intent, strategy_cfg),
                effective_admission_buffer_ticks(strategy_cfg, extra_buffer_ticks),
                now_ms,
            ) {
                emit_local_order_block(obs, capture, now_ms, reason);
                record_pre_submission_abandoned(
                    risk,
                    ctf_inventory_runtime,
                    submission_recovery,
                    &intent,
                    "local_venue_order_block",
                    now_ms,
                    execution_audit_records,
                )?;
                continue;
            }
            if intent.side() == Side::Sell {
                if let Some(order_id) = replacement_order_id.as_deref() {
                    projected_open_orders.orders.remove(order_id);
                }
                if let Err(block) =
                    risk.check_sell_inventory(&intent, portfolio, &projected_open_orders)
                {
                    emit_risk_admission_block(obs, capture, now_ms, block, &intent);
                    record_pre_submission_abandoned(
                        risk,
                        ctf_inventory_runtime,
                        submission_recovery,
                        &intent,
                        "sell_inventory_block",
                        now_ms,
                        execution_audit_records,
                    )?;
                    continue;
                }
            }
            // Close/exit sells bypass risk gates. These are risk-reducing orders
            // that must NEVER be blocked by circuit breaker, drawdown, or
            // daily-loss limits.
            // Blocking exits causes a death spiral: losses -> freeze -> more losses.
            let is_close_order = intent.side() == Side::Sell;
            if !is_close_order {
                if let Err(block) = risk.check_order(
                    &intent,
                    portfolio,
                    &projected_open_orders,
                    selection.as_ref(),
                    odds_state.as_deref(),
                    now_ms,
                ) {
                    emit_risk_admission_block(obs, capture, now_ms, block, &intent);
                    record_pre_submission_abandoned(
                        risk,
                        ctf_inventory_runtime,
                        submission_recovery,
                        &intent,
                        "risk_admission_block",
                        now_ms,
                        execution_audit_records,
                    )?;
                    continue;
                }
            }
            seed_projected_open_order(now_ms, &mut projected_open_orders, &intent);
            passed.push(intent);
        }
    }
    Ok((passed, preserved_quote_keys, cancel_quote_keys))
}

fn parse_kv_fields(text: &str) -> BTreeMap<String, String> {
    text.split_whitespace()
        .filter_map(|part| part.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
pub(super) fn emit_local_order_block(
    obs: &Observability,
    capture: &MaybeCapture,
    ts_ms: u64,
    raw: String,
) {
    let rest = raw
        .strip_prefix("local_order_block ")
        .unwrap_or(raw.as_str());
    let mut parts = rest.split_whitespace();
    let Some(reason) = parts.next() else {
        tracing::info!(target: "execution.supervisor_batch", raw, "local_order_block");
        return;
    };
    let detail_text = parts.collect::<Vec<_>>().join(" ");
    let detail = parse_kv_fields(&detail_text);
    let reason = reason.to_string();
    let token = detail.get("token").cloned();
    let side = detail.get("side").cloned();
    obs.emit_action(ActionEvent::LocalOrderBlock {
        ts_ms,
        reason: reason.clone(),
        token: token.clone(),
        side: side.clone(),
        detail: detail.clone(),
        raw: raw.clone(),
    });
    capture.emit(CaptureMsg::LocalOrderBlock(LocalOrderBlockCapture {
        session_ts_ms: ts_ms,
        reason,
        token,
        side,
        detail,
        raw,
    }));
}
pub(super) fn emit_risk_admission_block(
    obs: &Observability,
    capture: &MaybeCapture,
    ts_ms: u64,
    block: RiskBlock,
    intent: &OrderIntent,
) {
    emit_risk_block(ts_ms, obs, block.clone());
    capture.emit(CaptureMsg::RiskBlock(RiskBlockCapture {
        session_ts_ms: ts_ms,
        rule: block.rule,
        details: block.details,
        market_id: intent_market_id(intent).map(|market| market.to_string()),
        token: intent.instrument.clone(),
        side: match intent.side() {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
        .to_string(),
        close_hint: intent.order_close_reason_str().map(str::to_string),
        qty: intent.qty,
        limit_px: intent.limit_px,
    }));
}

pub(in crate::execution::supervisor) fn record_pre_submission_abandoned(
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    submission_recovery: &SubmissionRecovery,
    intent: &OrderIntent,
    kind: &str,
    observed_at_ms: u64,
    records: &mut Vec<EngineTradeJournalRecord>,
) -> Result<()> {
    if intent.side() == Side::Buy {
        if let Some(authorization) = intent.buy_authorization() {
            if !submission_recovery.retains_execution_intent(intent)
                && risk.abandon_unsubmitted_direct_buy(intent)?
            {
                ctf_inventory_runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                    .release_reserved_direct_buy(authorization);
            }
        }
        return Ok(());
    }
    if let Some(fact) = build_order_terminal_fact(
        intent,
        None,
        OrderTerminalProvenance::PreSubmissionAbandoned,
        None,
        None,
        Some(kind),
        Some("execution_pre_submission"),
        Some(kind),
        observed_at_ms,
    )? {
        records.push(build_order_terminal_record(fact)?);
    }
    Ok(())
}
pub(in crate::execution::supervisor) fn record_batch_pre_submission_abandoned(
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    submission_recovery: &SubmissionRecovery,
    intents: &[OrderIntent],
    kind: &str,
    observed_at_ms: u64,
    records: &mut Vec<EngineTradeJournalRecord>,
) -> Result<()> {
    let mut first_error = None;
    for intent in intents {
        if let Err(error) = record_pre_submission_abandoned(
            risk,
            ctf_inventory_runtime,
            submission_recovery,
            intent,
            kind,
            observed_at_ms,
            records,
        ) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

pub(in crate::execution::supervisor) fn current_post_only_reject_status(
    rejects: &HashMap<String, PostOnlyRejectState>,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    now_ms: u64,
) -> Option<crate::observability::ui::PostOnlyRejectStatusSnapshot> {
    let mut tokens: Vec<crate::observability::ui::PostOnlyRejectTokenSnapshot> = rejects
        .iter()
        .filter(|(_, state)| state.streak > 0 && state.cooldown_until_ms > now_ms)
        .map(
            |(token, state)| crate::observability::ui::PostOnlyRejectTokenSnapshot {
                token: token.clone(),
                streak: state.streak,
                cooldown_until_ms: state.cooldown_until_ms,
                extra_buffer_ticks: state
                    .streak
                    .min(strategy_cfg.post_only_reject_max_extra_buffer_ticks),
            },
        )
        .collect();
    if tokens.is_empty() {
        return None;
    }
    tokens.sort_by(|a, b| {
        b.cooldown_until_ms
            .cmp(&a.cooldown_until_ms)
            .then_with(|| b.streak.cmp(&a.streak))
            .then_with(|| a.token.cmp(&b.token))
    });
    Some(crate::observability::ui::PostOnlyRejectStatusSnapshot {
        active_count: tokens.len(),
        tokens,
    })
}
pub(super) fn hard_submit_reject_reason(err: &anyhow::Error) -> Option<&'static str> {
    let mut text = String::new();
    for cause in err.chain() {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(&cause.to_string());
    }
    let lower = text.to_ascii_lowercase();
    if lower.contains("order_version_mismatch") {
        Some("order_version_mismatch")
    } else if lower.contains("invalid signature") {
        Some("invalid_signature")
    } else if lower.contains("unauthorized") || lower.contains("invalid api key") {
        Some("auth_rejected")
    } else {
        None
    }
}
pub(super) fn reject_kind_label(kind: RejectKind) -> &'static str {
    match kind {
        RejectKind::AggressiveNoMatch => "aggressive_no_match",
        RejectKind::Http425ServiceNotReady => "http_425_service_not_ready",
        RejectKind::MarketNotReady => "market_not_ready",
        RejectKind::NoOrderbook => "no_orderbook",
        RejectKind::FeeRate => "fee_rate",
        RejectKind::TickSize => "tick_size",
        RejectKind::Balance => "balance",
        RejectKind::PostOnly => "post_only",
        RejectKind::InvalidPrice => "invalid_price",
        RejectKind::Other => "other",
    }
}

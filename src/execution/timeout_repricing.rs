use super::*;

pub(super) async fn handle_timeouts(
    now_ms: u64,
    clock: &ExecutionSupervisorClock,
    exec: &ExecutionClient,
    risk: &RiskHandle,
    open_orders: &mut OpenOrdersState,
    fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_inflight: &mut HashSet<ScopedOrderFenceKey>,
    scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    _obs: &Observability,
    active_cancellations: Option<&mut ActiveCancellations>,
) -> Result<()> {
    let risk_cfg = risk.cfg();
    if !risk_cfg.enabled {
        return Ok(());
    }
    let timeout_ms = risk_cfg.order_timeout_ms.max(1);
    let now = now_ms;

    let mut to_cancel: HashSet<String> = HashSet::new();
    for (id, ord) in &open_orders.orders {
        let cancel_deadline_ms = ord.timeout_deadline_ms(timeout_ms);
        if now >= cancel_deadline_ms {
            to_cancel.insert(id.clone());
        }
    }

    if to_cancel.is_empty() {
        return Ok(());
    }

    let ids: Vec<String> = to_cancel.into_iter().collect();
    if let Some(active) = active_cancellations {
        active.schedule_orders(
            exec,
            ActiveCancellations::targets_for_ids(open_orders, &ids),
            "order_timeout_cancel",
            "TIMEOUT_CANCEL_OK",
        );
        return Ok(());
    }
    let cancel_result = exec.cancel_orders(&ids).await;
    let receipt_now_ms = clock.now_ms();
    match cancel_result {
        Ok(ack) => {
            let mut failed_ids = Vec::new();
            let mut mutated = false;
            for id in ack.canceled {
                risk.release_inflight_buy_order(id.as_str());
                tracing::info!(
                    target: "execution.timeout_repricing",
                    order_id = %id,
                    "order_timeout_cancel"
                );
                mutated |=
                    open_orders.record_cancel_terminal(&id, receipt_now_ms, "TIMEOUT_CANCEL_OK");
            }
            for (id, reason) in ack.not_canceled {
                failed_ids.push(id.clone());
                tracing::error!(
                    target: "execution.timeout_repricing",
                    order_id = %id,
                    reason = %reason,
                    "order_timeout_cancel_failed"
                );
            }
            if mutated {
                open_orders.note_runtime_mutation(receipt_now_ms);
            }
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &failed_ids,
                "order_timeout_cancel",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
        }
        Err(err) => {
            tracing::error!(
                target: "execution.timeout_repricing",
                error = %err,
                "order_timeout_cancel_error"
            );
            enqueue_failed_cancel_scope_heals(
                open_orders,
                &ids,
                "order_timeout_cancel",
                fenced_scopes,
                scoped_heal_inflight,
                scoped_heal_tx,
            )?;
        }
    }
    Ok(())
}

pub(super) fn publish_open_orders(
    tx: &watch::Sender<OpenOrdersState>,
    obs: &Observability,
    state: &OpenOrdersState,
) {
    let _ = tx.send_if_modified(|current| {
        if current.revision == state.revision
            && current.orders_fingerprint == state.orders_fingerprint
            && current.updated_at_ms == state.updated_at_ms
        {
            return false;
        }
        *current = state.clone();
        true
    });
    obs.set_open_orders(state.clone());
}

pub(super) fn emit_risk_block(now_ms: u64, obs: &Observability, block: RiskBlock) {
    obs.emit_action(ActionEvent::RiskBlock {
        ts_ms: now_ms,
        rule: block.rule,
        details: block.details,
    });
}

pub(super) fn pin_close_intent_to_existing_order(
    intent: &mut OrderIntent,
    open_orders: &OpenOrdersState,
) {
    if intent.side() != Side::Sell || !intent.post_only {
        return;
    }
    let Some(key) = quote_key_from_intent(intent) else {
        return;
    };

    let Some(existing) = open_orders
        .orders
        .values()
        .filter(|ord| quote_key_from_order(ord).is_some_and(|ord_key| ord_key == key))
        .filter(|ord| ord.remaining_size() > CLOSE_ORDER_QTY_EPS)
        .min_by_key(|ord| ord.placed_at_ms)
    else {
        return;
    };

    intent.limit_px = existing.px;
    intent.qty = existing.remaining_size();
}

pub(super) fn quote_key_from_intent(intent: &OrderIntent) -> Option<QuoteKey> {
    Some(QuoteKey {
        token: intent.instrument.clone(),
        side: intent.side(),
        quote_level: intent.quote_level?,
        quote_group: intent.quote_group.clone()?,
    })
}

pub(super) fn quote_key_from_order(ord: &OpenOrderState) -> Option<QuoteKey> {
    Some(QuoteKey {
        token: ord.token.clone(),
        side: ord.side,
        quote_level: ord.quote_level?,
        quote_group: ord.quote_group.clone()?,
    })
}

pub(super) fn coalesce_latest_intents(
    intents: Vec<OrderIntent>,
) -> (Vec<OrderIntent>, Vec<OrderIntent>) {
    let mut coalesced: Vec<OrderIntent> = Vec::with_capacity(intents.len());
    let mut index_by_key: HashMap<QuoteKey, usize> = HashMap::new();
    let mut superseded = Vec::new();

    for intent in intents {
        if let Some(key) = quote_key_from_intent(&intent) {
            if let Some(idx) = index_by_key.get(&key).copied() {
                let previous = std::mem::replace(&mut coalesced[idx], intent);
                if previous.execution_intent_id != coalesced[idx].execution_intent_id {
                    superseded.push(previous);
                }
            } else {
                index_by_key.insert(key, coalesced.len());
                coalesced.push(intent);
            }
        } else {
            coalesced.push(intent);
        }
    }

    (coalesced, superseded)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkingCloseQuoteReplacementDecision {
    NotApplicable,
    PreserveExisting,
    ReplaceExisting,
}

pub(crate) fn working_close_quote_replacement_decision(
    existing: &OpenOrderState,
    desired: &OrderIntent,
    tick: f64,
) -> WorkingCloseQuoteReplacementDecision {
    if desired.side() != Side::Sell || existing.side != Side::Sell {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    }
    if quote_key_from_order(existing) != quote_key_from_intent(desired) {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    }
    let Some(desired_close_reason) = desired.order_close_reason() else {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    };
    if !desired.post_only
        || !existing.post_only
        || desired.time_in_force.is_immediate()
        || existing.time_in_force.is_immediate()
    {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    }
    if existing.order_close_reason != Some(desired_close_reason)
        || existing.lifecycle_cohort.as_deref() != desired.lifecycle_cohort.as_deref()
        || existing.lifecycle_symbol.as_deref() != desired.lifecycle_symbol.as_deref()
        || !existing.px.is_finite()
        || !desired.limit_px.is_finite()
        || !desired.qty.is_finite()
    {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    }
    if !tick.is_finite() || tick <= 0.0 {
        return WorkingCloseQuoteReplacementDecision::NotApplicable;
    }
    let same_price = same_tick_index(existing.px, desired.limit_px, tick);
    let same_qty = (existing.remaining_size() - desired.qty).abs()
        <= crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
    let same_time_in_force = existing.time_in_force == desired.time_in_force;
    if same_price && same_qty && same_time_in_force {
        WorkingCloseQuoteReplacementDecision::PreserveExisting
    } else {
        WorkingCloseQuoteReplacementDecision::ReplaceExisting
    }
}

fn same_tick_index(left: f64, right: f64, tick: f64) -> bool {
    if !(left.is_finite() && right.is_finite() && tick.is_finite() && tick > 0.0) {
        return false;
    }
    (left / tick).round() as i64 == (right / tick).round() as i64
}

pub(crate) fn working_close_quote_replacement_for_open_orders(
    open_orders: &OpenOrdersState,
    desired: &OrderIntent,
    tick: f64,
) -> Option<(OpenOrderState, WorkingCloseQuoteReplacementDecision)> {
    open_orders
        .orders
        .values()
        .filter_map(|order| {
            let decision = working_close_quote_replacement_decision(order, desired, tick);
            (decision != WorkingCloseQuoteReplacementDecision::NotApplicable)
                .then_some((order, decision))
        })
        .min_by_key(|(order, _)| order.placed_at_ms)
        .map(|(order, decision)| (order.clone(), decision))
}

pub(super) fn seed_projected_open_order(
    now_ms: u64,
    open_orders: &mut OpenOrdersState,
    intent: &OrderIntent,
) {
    let seed_idx = open_orders.orders.len();
    let seed_id = format!(
        "projected:{}:{}:{:?}:{seed_idx}",
        intent.instrument,
        intent.limit_px,
        intent.side()
    );
    let Some(market) = intent_market_id(intent) else {
        tracing::warn!(
            target: "execution.timeout_repricing",
            token = %intent.instrument,
            "projected_open_order_missing_market_context"
        );
        return;
    };
    let now = now_ms;
    open_orders.orders.insert(
        seed_id.clone(),
        OpenOrderState {
            execution_intent_id: intent.execution_intent_id,
            order_id: seed_id,
            market: market.to_string(),
            token: intent.instrument.clone(),
            side: intent.side(),
            time_in_force: intent.time_in_force.clone(),
            px: intent.limit_px,
            original_size: intent.qty.max(0.0),
            matched_size: 0.0,
            placed_at_ms: now,
            last_update_ms: now,
            status: "PROJECTED".to_string(),
            post_only: intent.post_only,
            quote_level: intent.quote_level,
            quote_group: intent.quote_group.clone(),
            lifecycle_cohort: intent.lifecycle_cohort.clone(),
            lifecycle_symbol: intent.lifecycle_symbol.clone(),
            client_tag: intent
                .quote_group
                .as_ref()
                .map(|g| format!("{g}:{}", intent.quote_level.unwrap_or(255))),
            order_close_reason: intent.order_close_reason(),
        },
    );
    open_orders.note_runtime_mutation(now);
}

pub(super) fn should_keep_existing_quote(
    now_ms: u64,
    existing: &OpenOrderState,
    desired: &OrderIntent,
    odds_state: Option<&PolymarketOddsState>,
    strategy_cfg: &MakerRebateMmStrategyConfig,
    admission_buffer_ticks: u8,
) -> bool {
    let tick = resolve_tick_size(desired, odds_state).filter(|tick| is_valid_tick_size(*tick));
    match working_close_quote_replacement_decision(existing, desired, tick.unwrap_or(f64::NAN)) {
        WorkingCloseQuoteReplacementDecision::PreserveExisting => return true,
        WorkingCloseQuoteReplacementDecision::ReplaceExisting => return false,
        WorkingCloseQuoteReplacementDecision::NotApplicable => {}
    }
    let Some(tick) = tick else {
        return true;
    };
    let same_px =
        (existing.px - desired.limit_px).abs() <= crate::polymarket::fixed_math::FIXED_6DP_QUANTUM;
    let same_qty = (existing.remaining_size() - desired.qty).abs() <= 1e-6;
    if same_px && same_qty {
        return true;
    }
    if !desired.post_only {
        return false;
    }

    let price_delta_ticks = ((existing.px - desired.limit_px).abs() / tick).abs();
    if price_delta_ticks
        > QUOTE_REPLACE_PRICE_TOLERANCE_TICKS + crate::polymarket::fixed_math::FIXED_6DP_QUANTUM
    {
        return false;
    }

    let qty_tolerance = (desired.qty.abs() * QUOTE_REPLACE_QTY_REL_TOLERANCE).clamp(
        QUOTE_REPLACE_QTY_ABS_TOLERANCE_MIN,
        QUOTE_REPLACE_QTY_ABS_TOLERANCE_MAX,
    );
    if (existing.remaining_size() - desired.qty).abs() > qty_tolerance {
        return false;
    }

    let existing_fields = crate::types::OrderIntentFields {
        execution_intent_id: desired.execution_intent_id,
        market_frame_received_at: desired.market_frame_received_at,
        market_state_published_at: desired.market_state_published_at,
        market_connection_session_id: desired.market_connection_session_id,
        market_frame_sequence: desired.market_frame_sequence,
        market_state_revision: desired.market_state_revision,
        strategy_consumption_sequence: desired.strategy_consumption_sequence,
        strategy_input_kind: desired.strategy_input_kind,
        decision_at: desired.decision_at,
        execution_evidence_revision: desired.execution_evidence_revision,
        execution_instruction_qty_units_6dp: desired.execution_instruction_qty_units_6dp,
        execution_instruction_limit_px_units_8dp: desired.execution_instruction_limit_px_units_8dp,
        market_id: Some(MarketId::new(existing.market.clone())),
        instrument: existing.token.clone(),
        qty: existing.remaining_size(),
        limit_px: existing.px,
        time_in_force: existing.time_in_force.clone(),
        post_only: desired.post_only,
        quote_level: desired.quote_level,
        quote_group: desired.quote_group.clone(),
        lifecycle_cohort: desired.lifecycle_cohort.clone(),
        lifecycle_symbol: desired.lifecycle_symbol.clone(),
    };
    let existing_intent = match existing.side {
        Side::Buy => OrderIntent::buy(existing_fields),
        Side::Sell => OrderIntent::sell(
            existing_fields,
            desired
                .order_close_reason()
                .expect("existing sell quote comparison requires typed close reason"),
        ),
    };
    let now = now_ms;
    let min_rest_ms = strategy_cfg.passive_min_rest_ms;
    if min_rest_ms > 0 {
        let rest_age_ms = now.saturating_sub(existing.placed_at_ms);
        let expiry_soon = desired
            .time_in_force
            .expiration_ms()
            .map(|expiry| expiry <= now.saturating_add(min_rest_ms))
            .unwrap_or(false);
        if rest_age_ms < min_rest_ms {
            if expiry_soon || desired.side() == Side::Sell {
                return false;
            }
            return true;
        }
    }
    local_venue_order_block_reason(
        &existing_intent,
        odds_state,
        order_book_max_age_ms(&existing_intent, strategy_cfg),
        admission_buffer_ticks,
        now_ms,
    )
    .is_none()
}

pub(super) fn resolve_tick_size(
    intent: &OrderIntent,
    odds_state: Option<&PolymarketOddsState>,
) -> Option<f64> {
    let odds = odds_state?;
    let token_id = U256::from_str(intent.instrument.trim()).ok()?;
    crate::polymarket::state::book_for_token(odds, token_id).map(|top| top.tick_size)
}

pub(super) fn tick_decimal_places(tick: f64) -> Option<u32> {
    if !(tick.is_finite() && tick > 0.0) {
        return None;
    }
    for decimals in 0..=VENDOR_MAX_PRICE_DECIMALS {
        let scale = 10f64.powi(decimals as i32);
        let scaled = tick * scale;
        if (scaled - scaled.round()).abs() <= crate::polymarket::fixed_math::FIXED_6DP_QUANTUM {
            return Some(decimals);
        }
    }
    None
}

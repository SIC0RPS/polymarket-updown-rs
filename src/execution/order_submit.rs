use super::client::ExecutionBatchSendOutcome;
use super::http_support::{is_http_425, is_routed_post_outcome_unknown};
use super::*;
use crate::config::POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS;
use std::time::Instant;

mod response;

impl PolymarketExecutionLive {
    pub(crate) async fn send(&self, intent: &OrderIntent) -> Result<ExecutionAck> {
        match self.send_batch(std::slice::from_ref(intent), None).await {
            ExecutionBatchSendOutcome::Complete { mut known, .. } => known
                .pop()
                .map(|result| result.ack)
                .context("missing single-order ack from Polymarket batch submission"),
            ExecutionBatchSendOutcome::Unknown { error, .. }
            | ExecutionBatchSendOutcome::DefinitiveFailure { error, .. } => {
                Err(error.context("failed to post Polymarket completion order"))
            }
        }
    }

    pub(crate) async fn build_signed_order_with_type(
        &self,
        intent: &OrderIntent,
        order_type: OrderType,
    ) -> Result<SignedOrder> {
        ensure!(intent.qty.is_finite(), "order qty must be finite");
        ensure!(intent.limit_px.is_finite(), "order limit_px must be finite");
        ensure!(intent.qty > 0.0, "order qty must be > 0");
        ensure!(intent.limit_px > 0.0, "order limit_px must be > 0");
        ensure!(
            !intent.post_only || !intent.time_in_force.is_immediate(),
            "post_only_incompatible_time_in_force tif={}",
            intent.time_in_force.as_str()
        );

        let token_id = resolve_token_id(intent)?;
        let side = to_polymarket_side(intent.side());
        let price = decimal_from_f64("limit_px", intent.limit_px)?;
        let signer = &*self.signer;
        let market_buy_user_usdc_balance = if matches!(order_type, OrderType::FAK | OrderType::FOK)
            && matches!(intent.side(), Side::Buy)
        {
            Some(self.market_buy_user_usdc_balance().await?)
        } else {
            None
        };

        dispatch_client!(&self.client, |c| {
            let order = if matches!(order_type, OrderType::FAK | OrderType::FOK) {
                let amount = market_amount_from_intent(intent)?;
                let mut builder = c
                    .market_order()
                    .token_id(token_id)
                    .order_type(order_type)
                    .side(side)
                    .price(price)
                    .amount(amount);
                if let Some(balance) = market_buy_user_usdc_balance {
                    builder = builder.user_usdc_balance(balance);
                }
                builder
                    .build()
                    .await
                    .context("failed to build Polymarket market order")?
            } else {
                let size = decimal_from_f64("qty", intent.qty)?;
                let mut builder = c
                    .limit_order()
                    .token_id(token_id)
                    .order_type(order_type.clone())
                    .side(side)
                    .price(price)
                    .size(size);
                if matches!(order_type, OrderType::GTD) {
                    if let Some(expiration) = gtd_expiration_datetime(intent)? {
                        builder = builder.expiration(expiration);
                    }
                }
                if intent.post_only {
                    builder = builder.post_only(true);
                }
                builder
                    .build()
                    .await
                    .context("failed to build Polymarket limit order")?
            };
            let signed = c
                .sign(signer, order)
                .await
                .context("failed to sign Polymarket order")?;
            Ok(signed)
        })
    }

    async fn market_buy_user_usdc_balance(&self) -> Result<Decimal> {
        let balance = self
            .balance_allowance()
            .await
            .context("failed to fetch pUSD balance before Polymarket market BUY order")?
            .balance;
        Ok(balance / Decimal::from(1_000_000u32))
    }

    pub(crate) async fn send_batch(
        &self,
        intents: &[OrderIntent],
        pre_send_ids_override: Option<&HashSet<String>>,
    ) -> ExecutionBatchSendOutcome {
        if intents.is_empty() {
            return ExecutionBatchSendOutcome::Complete {
                known: Vec::new(),
                post_timings: Vec::new(),
            };
        }

        let mut out = Vec::with_capacity(intents.len());
        let mut post_timings = Vec::new();
        let mut input_offset = 0usize;
        for tif_group in batched_intents_by_tif(intents) {
            for chunk in tif_group.chunks(MAX_BATCH_ORDERS) {
                let chunk_start = input_offset;
                let effective_order_type = venue_order_type_for_tif(&chunk[0].time_in_force);
                let pre_send_ids: HashSet<String> =
                    pre_send_ids_override.cloned().unwrap_or_else(HashSet::new);

                // Sign once AFTER the pre-send snapshot and BEFORE the retry loop.
                // Re-signing across ambiguous retries would produce a new
                // salt/signature and the exchange would treat it as a new order.
                let mut signed_orders: Vec<SignedOrder> = Vec::with_capacity(chunk.len());
                let mut signed_order_completed_at = Vec::with_capacity(chunk.len());
                for intent in chunk {
                    let signed = self
                        .build_signed_order_with_type(intent, effective_order_type.clone())
                        .await
                        .with_context(|| {
                            format!(
                                "build_signed_order_failed token={} side={:?} qty={} limit_px={} post_only={} tif={}",
                                intent.instrument,
                                intent.side(),
                                intent.qty,
                                intent.limit_px,
                                intent.post_only,
                                intent.time_in_force.as_str()
                            )
                        });
                    match signed {
                        Ok(signed) => {
                            let completed_at = Instant::now();
                            signed_orders.push(signed);
                            signed_order_completed_at.push(completed_at);
                        }
                        Err(error) => {
                            return ExecutionBatchSendOutcome::DefinitiveFailure {
                                known: out,
                                failed: chunk_start..chunk_start,
                                unsent: chunk_start..intents.len(),
                                post_timings,
                                error,
                            };
                        }
                    }
                }
                let signed_order_completed_at: Arc<[Instant]> = signed_order_completed_at.into();

                let policy = ApiRetryPolicy {
                    endpoint: "POST /orders",
                    method: "post_orders",
                    allow_read_retry: false,
                    retry_425_log: Some("post_orders_http_425_retry"),
                    retry_429_log: Some("Rate limited by CLOB API, backing off"),
                };
                let mut state = ApiRetryState::new();
                let mut dedup_resolved = false;
                let (responses, request_ts_ms, response_ts_ms) = loop {
                    state.attempt += 1;
                    let request_ts_ms = now_ms();
                    let post_attempt = self
                        .post_orders_routed(signed_orders.iter().map(clone_signed_order).collect())
                        .await;
                    let response_ts_ms = now_ms();
                    let post_res = match post_attempt {
                        Ok(attempt) => {
                            post_timings.push(super::client::ExecutionPostTiming {
                                input_range: chunk_start..chunk_start + chunk.len(),
                                signed_order_completed_at: Arc::clone(&signed_order_completed_at),
                                accepted_order_ids: vec![None; chunk.len()],
                                attempt: state.attempt,
                                server_time_request_started_at: attempt
                                    .timing
                                    .server_time_request_started_at,
                                server_time_request_completed_at: attempt
                                    .timing
                                    .server_time_request_completed_at,
                                invocation_instant: attempt.timing.invocation_instant,
                                terminal_instant: attempt.timing.terminal_instant,
                                request_ts_ms,
                                response_headers_ts_ms: attempt.timing.response_headers_ts_ms,
                                terminal_ts_ms: response_ts_ms,
                                invocation_elapsed_us: attempt.timing.invocation_elapsed_us,
                                response_headers_elapsed_us: attempt
                                    .timing
                                    .response_headers_elapsed_us,
                                response_body_elapsed_us: attempt.timing.response_body_elapsed_us,
                                http_status: attempt.timing.http_status,
                                attempt_result: attempt.timing.attempt_result.to_string(),
                                interpretation_error: attempt.timing.interpretation_error,
                                response_evidence: attempt.timing.response_evidence,
                            });
                            attempt.result
                        }
                        Err(error) => {
                            return ExecutionBatchSendOutcome::DefinitiveFailure {
                                known: out,
                                failed: chunk_start..chunk_start,
                                unsent: chunk_start..intents.len(),
                                post_timings,
                                error: anyhow::Error::new(error).context(
                                    "failed to prepare Polymarket order batch before POST invocation",
                                ),
                            };
                        }
                    };

                    match self.run_api_attempt(&policy, &mut state, post_res).await {
                        ApiRetryOutcome::Success(responses) => {
                            break (responses, request_ts_ms, response_ts_ms);
                        }
                        ApiRetryOutcome::Retry(ApiRetryKind::Http425 | ApiRetryKind::Http429) => {
                            if let Some(acks) = self.check_dedup_scoped(chunk, &pre_send_ids).await
                            {
                                tracing::info!(
                                    count = acks.len(),
                                    "dedup_resolved: orders already placed, skipping retry"
                                );
                                out.extend(acks.into_iter().enumerate().map(|(offset, ack)| {
                                    super::client::IndexedExecutionAck {
                                        input_index: chunk_start.saturating_add(offset),
                                        ack,
                                    }
                                }));
                                dedup_resolved = true;
                                break (Vec::new(), request_ts_ms, response_ts_ms);
                            }
                        }
                        ApiRetryOutcome::Retry(ApiRetryKind::ReadTransient) => {}
                        ApiRetryOutcome::Error(err) => {
                            if is_http_425(&err) {
                                let error_msg = format!("{err}");
                                for offset in 0..chunk.len() {
                                    out.push(super::client::IndexedExecutionAck {
                                        input_index: chunk_start.saturating_add(offset),
                                        ack: ExecutionAck {
                                            status: Some("http_425".to_string()),
                                            order_id: None,
                                            accepted: false,
                                            error_msg: Some(error_msg.clone()),
                                            reject_kind: Some(RejectKind::Http425ServiceNotReady),
                                            request_ts_ms: Some(request_ts_ms),
                                            response_ts_ms: Some(response_ts_ms),
                                        },
                                    });
                                }
                                dedup_resolved = true;
                                break (Vec::new(), request_ts_ms, response_ts_ms);
                            }
                            let unknown_outcome = is_routed_post_outcome_unknown(&err);
                            let error = anyhow::Error::new(err)
                                .context("failed to post Polymarket order batch");
                            let submitted_end = chunk_start.saturating_add(chunk.len());
                            if unknown_outcome {
                                return ExecutionBatchSendOutcome::Unknown {
                                    known: out,
                                    unknown: (chunk_start..submitted_end).collect(),
                                    unsent: submitted_end..intents.len(),
                                    post_timings,
                                    error,
                                };
                            }
                            return ExecutionBatchSendOutcome::DefinitiveFailure {
                                known: out,
                                failed: chunk_start..submitted_end,
                                unsent: submitted_end..intents.len(),
                                post_timings,
                                error,
                            };
                        }
                    }
                };

                if dedup_resolved {
                    input_offset = input_offset.saturating_add(chunk.len());
                    continue;
                }

                let mut reduced = response::reduce_batch_responses(
                    out,
                    responses,
                    chunk,
                    chunk_start,
                    intents.len(),
                    request_ts_ms,
                    response_ts_ms,
                    post_timings,
                );
                stamp_response_accepted_order_ids(
                    &mut reduced,
                    chunk_start..chunk_start.saturating_add(chunk.len()),
                );
                match reduced {
                    ExecutionBatchSendOutcome::Complete {
                        known,
                        post_timings: timings,
                    } => {
                        out = known;
                        post_timings = timings;
                    }
                    outcome => return outcome,
                }
                input_offset = input_offset.saturating_add(chunk.len());
            }
        }

        ExecutionBatchSendOutcome::Complete {
            known: out,
            post_timings,
        }
    }
}

fn stamp_response_accepted_order_ids(
    outcome: &mut ExecutionBatchSendOutcome,
    input_range: std::ops::Range<usize>,
) {
    let (known, post_timings) = match outcome {
        ExecutionBatchSendOutcome::Complete {
            known,
            post_timings,
        }
        | ExecutionBatchSendOutcome::Unknown {
            known,
            post_timings,
            ..
        }
        | ExecutionBatchSendOutcome::DefinitiveFailure {
            known,
            post_timings,
            ..
        } => (known, post_timings),
    };
    let Some(timing) = post_timings
        .iter_mut()
        .rev()
        .find(|timing| timing.input_range == input_range)
    else {
        return;
    };
    for result in known
        .iter()
        .filter(|result| input_range.contains(&result.input_index))
    {
        let Some(order_id) = result
            .ack
            .accepted
            .then_some(result.ack.order_id.as_ref())
            .flatten()
        else {
            continue;
        };
        let offset = result.input_index.saturating_sub(input_range.start);
        if let Some(slot) = timing.accepted_order_ids.get_mut(offset) {
            *slot = Some(order_id.clone());
        }
    }
}

pub(crate) fn venue_order_type_for_tif(time_in_force: &TimeInForce) -> OrderType {
    match time_in_force {
        TimeInForce::Gtc => OrderType::GTC,
        TimeInForce::Gtd { .. } => OrderType::GTD,
        TimeInForce::Fok => OrderType::FOK,
        TimeInForce::Fak => OrderType::FAK,
    }
}

pub(crate) fn batched_intents_by_tif(
    intents: &[OrderIntent],
) -> impl Iterator<Item = &[OrderIntent]> {
    let mut batches: Vec<&[OrderIntent]> = Vec::new();
    let mut start = 0usize;
    while start < intents.len() {
        let mut end = start + 1;
        while end < intents.len() && intents[end].time_in_force == intents[start].time_in_force {
            end += 1;
        }
        batches.push(&intents[start..end]);
        start = end;
    }
    batches.into_iter()
}

pub(crate) fn gtd_expiration_datetime(
    intent: &OrderIntent,
) -> Result<Option<chrono::DateTime<Utc>>> {
    let Some(effective_expiration_ms) = intent.time_in_force.expiration_ms() else {
        return Ok(None);
    };
    let min_effective_expiration_ms =
        now_ms().saturating_add(POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS);
    ensure!(
        effective_expiration_ms >= min_effective_expiration_ms,
        "gtd_effective_expiration_too_close effective_expiration_ms={effective_expiration_ms} min_effective_lifetime_ms={POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS}"
    );
    let venue_expiration_ms =
        effective_expiration_ms.saturating_add(POLYMARKET_GTD_SECURITY_BUFFER_MS);
    Utc.timestamp_millis_opt(venue_expiration_ms as i64)
        .single()
        .map(Some)
        .context("invalid_gtd_expiration_ms")
}

pub(crate) fn decimal_from_f64(name: &str, value: f64) -> Result<Decimal> {
    ensure!(value.is_finite(), "{name} must be finite");
    ensure!(value > 0.0, "{name} must be > 0");

    let repr = format!("{value:.8}");
    let repr = repr.trim_end_matches('0').trim_end_matches('.');
    let repr = if repr.is_empty() { "0" } else { repr };

    Decimal::from_str(repr).with_context(|| format!("failed to parse {name} `{repr}`"))
}

pub(crate) fn decimal_from_f64_with_scale(name: &str, value: f64, scale: u32) -> Result<Decimal> {
    ensure!(value.is_finite(), "{name} must be finite");
    ensure!(value > 0.0, "{name} must be > 0");

    let value = truncate_to_decimals(value, scale);
    let repr = format!("{value:.scale$}", scale = scale as usize);
    let repr = repr.trim_end_matches('0').trim_end_matches('.');
    let repr = if repr.is_empty() { "0" } else { repr };

    Decimal::from_str(repr).with_context(|| format!("failed to parse {name} `{repr}`"))
}

pub(crate) fn market_amount_from_intent(intent: &OrderIntent) -> Result<Amount> {
    match intent.side() {
        Side::Buy => {
            let spend_usdc = intent.qty * intent.limit_px;
            let spend = decimal_from_f64_with_scale("market_buy_spend_usdc", spend_usdc, 6)?;
            Amount::usdc(spend).context("failed to build Polymarket market BUY amount")
        }
        Side::Sell => {
            let shares = decimal_from_f64_with_scale(
                "market_sell_shares",
                intent.qty,
                VENDOR_LOT_SIZE_DECIMALS,
            )?;
            Amount::shares(shares).context("failed to build Polymarket market SELL amount")
        }
    }
}

pub(crate) fn resolve_token_id(intent: &OrderIntent) -> Result<U256> {
    let instrument = intent.instrument.trim();
    if instrument.is_empty() {
        anyhow::bail!("missing token id: OrderIntent.instrument must be the CLOB token id");
    }
    U256::from_str(instrument).with_context(|| format!("invalid U256 value `{instrument}`"))
}

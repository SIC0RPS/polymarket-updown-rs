use super::*;

mod scoring;

impl PolymarketExecutionLive {
    const QUERY_PAGINATION_MAX_PAGES: usize = 100;

    async fn retry_http_status(
        &self,
        policy: &ApiRetryPolicy<'_>,
        err: &PolymarketClientError,
        retry_count: usize,
        max_attempts: usize,
        backoff_ms: &mut u64,
        max_backoff_ms: u64,
        retry_log: Option<&'static str>,
        retry_kind: ApiRetryKind,
    ) -> ApiRetryKind {
        self.emit_api_call(
            policy.endpoint,
            policy.method,
            *backoff_ms,
            http_status_code(err),
            false,
        );
        if let Some(message) = retry_log {
            tracing::warn!(
                target: "execution.query",
                attempt = retry_count,
                max_attempts,
                backoff_ms = *backoff_ms,
                message = %message,
                "execution_query_retry"
            );
        }
        let sleep_backoff_ms =
            advance_capped_full_jitter_backoff_ms(retry_count, backoff_ms, max_backoff_ms);
        tokio::time::sleep(Duration::from_millis(sleep_backoff_ms)).await;
        retry_kind
    }

    pub(super) async fn run_api_attempt<T>(
        &self,
        policy: &ApiRetryPolicy<'_>,
        state: &mut ApiRetryState,
        result: std::result::Result<T, PolymarketClientError>,
    ) -> ApiRetryOutcome<T> {
        match result {
            Ok(value) => {
                self.emit_api_call(policy.endpoint, policy.method, 0, Some(200), true);
                ApiRetryOutcome::Success(value)
            }
            Err(err) if is_http_425(&err) && state.attempt < HTTP_425_RETRY_ATTEMPTS => {
                ApiRetryOutcome::Retry(
                    self.retry_http_status(
                        policy,
                        &err,
                        state.attempt,
                        HTTP_425_RETRY_ATTEMPTS,
                        &mut state.backoff_ms,
                        HTTP_425_RETRY_MAX_BACKOFF_MS,
                        policy.retry_425_log,
                        ApiRetryKind::Http425,
                    )
                    .await,
                )
            }
            Err(err) if is_http_429(&err) && state.rl_retries < HTTP_429_RETRY_ATTEMPTS => {
                state.rl_retries = state.rl_retries.saturating_add(1);
                ApiRetryOutcome::Retry(
                    self.retry_http_status(
                        policy,
                        &err,
                        state.rl_retries,
                        HTTP_429_RETRY_ATTEMPTS,
                        &mut state.rl_backoff_ms,
                        HTTP_429_RETRY_MAX_BACKOFF_MS,
                        policy.retry_429_log,
                        ApiRetryKind::Http429,
                    )
                    .await,
                )
            }
            Err(err) => {
                if policy.allow_read_retry {
                    if let Some(backoff_ms) =
                        next_retryable_read_backoff_ms(&err, &mut state.read_retry)
                    {
                        self.emit_api_call(
                            policy.endpoint,
                            policy.method,
                            backoff_ms,
                            http_status_code(&err),
                            false,
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        return ApiRetryOutcome::Retry(ApiRetryKind::ReadTransient);
                    }
                }
                self.emit_api_call(
                    policy.endpoint,
                    policy.method,
                    0,
                    http_status_code(&err),
                    false,
                );
                ApiRetryOutcome::Error(err)
            }
        }
    }

    pub(crate) async fn balance_allowance(&self) -> Result<BalanceAllowanceResponse> {
        // Refresh the CLOB server's cached on-chain balance before querying.
        // Without this the server may return a stale zero balance (e.g. after
        // first deposit or when the cache entry has expired).
        let update_req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        dispatch_client!(&self.client, |c| c
            .update_balance_allowance(update_req)
            .await)
        .map_err(anyhow::Error::new)
        .context("failed to refresh Polymarket balance/allowance cache")?;

        let req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        let policy = ApiRetryPolicy {
            endpoint: "GET /balance-allowance",
            method: "balance_allowance",
            allow_read_retry: true,
            retry_425_log: None,
            retry_429_log: None,
        };
        let mut state = ApiRetryState::new();

        loop {
            state.attempt += 1;
            let res = dispatch_client!(&self.client, |c| c.balance_allowance(req.clone()).await);
            match self.run_api_attempt(&policy, &mut state, res).await {
                ApiRetryOutcome::Success(res) => return Ok(res),
                ApiRetryOutcome::Retry(_) => {}
                ApiRetryOutcome::Error(err) => {
                    return Err(anyhow::Error::new(err)
                        .context("failed to fetch polymarket balance/allowance"));
                }
            }
        }
    }

    pub(crate) async fn fetch_open_orders(&self, method: &str) -> Result<Vec<OpenOrderResponse>> {
        let req = OrdersRequest::builder().build();
        self.fetch_open_orders_for_request(req, method).await
    }

    pub(crate) async fn fetch_open_orders_filtered(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
        method: &str,
    ) -> Result<Vec<OpenOrderResponse>> {
        let market = market
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(B256::from_str)
            .transpose()
            .context("invalid orders scope market id")?;
        let asset_id = asset_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(U256::from_str)
            .transpose()
            .context("invalid orders scope asset id")?;
        let req = match (market, asset_id) {
            (Some(market), Some(asset_id)) => OrdersRequest::builder()
                .market(market)
                .asset_id(asset_id)
                .build(),
            (Some(market), None) => OrdersRequest::builder().market(market).build(),
            (None, Some(asset_id)) => OrdersRequest::builder().asset_id(asset_id).build(),
            (None, None) => OrdersRequest::builder().build(),
        };
        self.fetch_open_orders_for_request(req, method).await
    }

    pub(crate) async fn fetch_open_orders_by_id(
        &self,
        order_id: &str,
        method: &str,
    ) -> Result<Vec<OpenOrderResponse>> {
        let order_id = order_id.trim();
        ensure!(!order_id.is_empty(), "open_order_id_query_empty");
        let req = OrdersRequest::builder()
            .order_id(order_id.to_string())
            .build();
        self.fetch_open_orders_for_request(req, method).await
    }

    pub(crate) async fn fetch_open_orders_for_request(
        &self,
        req: OrdersRequest,
        method: &str,
    ) -> Result<Vec<OpenOrderResponse>> {
        let mut next_cursor: Option<String> = None;
        let mut out: Vec<OpenOrderResponse> = Vec::new();
        let mut page_count = 0usize;
        loop {
            page_count = page_count.saturating_add(1);
            let request_cursor = next_cursor.clone();
            let policy = ApiRetryPolicy {
                endpoint: "GET /orders",
                method,
                allow_read_retry: true,
                retry_425_log: None,
                retry_429_log: None,
            };
            let mut state = ApiRetryState::new();

            let page = loop {
                state.attempt += 1;
                let page_res = dispatch_client!(&self.client, |c| c
                    .orders(&req, request_cursor.clone())
                    .await);
                match self.run_api_attempt(&policy, &mut state, page_res).await {
                    ApiRetryOutcome::Success(page) => break page,
                    ApiRetryOutcome::Retry(_) => {}
                    ApiRetryOutcome::Error(err) => {
                        return Err(anyhow::Error::new(err)
                            .context("failed to fetch polymarket open orders"));
                    }
                }
            };

            out.extend(page.data);

            let cursor = page.next_cursor.trim().to_string();
            // "LTE=" is base64 for -1 — Polymarket's end-of-pagination sentinel.
            if cursor.is_empty() || cursor == "LTE=" || page.count == 0 {
                break;
            }
            if page_count >= Self::QUERY_PAGINATION_MAX_PAGES {
                return Err(anyhow::anyhow!(
                    "polymarket open orders pagination exceeded {} pages without terminal cursor method={} cursor={}",
                    Self::QUERY_PAGINATION_MAX_PAGES,
                    method,
                    cursor
                ));
            }
            next_cursor = Some(cursor);
        }

        Ok(out)
    }

    pub(crate) async fn fetch_detected_orders_for_intents(
        &self,
        intents: &[OrderIntent],
        method: &str,
    ) -> Result<Vec<DetectedOrder>> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }

        let mut scopes = HashSet::new();
        let mut out: Vec<DetectedOrder> = Vec::new();
        for intent in intents {
            let market = intent.market_id.as_ref().map(|market| market.to_string());
            let token = intent.instrument.clone();
            if !scopes.insert((market.clone(), token.clone())) {
                continue;
            }
            for o in self
                .fetch_open_orders_filtered(market.as_deref(), Some(token.as_str()), method)
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch scoped open orders for dedup market={} token={}",
                        market.as_deref().unwrap_or("-"),
                        token
                    )
                })?
            {
                out.push(DetectedOrder {
                    id: o.id,
                    asset_id: o.asset_id,
                    side: o.side,
                    price: o.price,
                    original_size: o.original_size,
                });
            }
        }

        Ok(out)
    }

    const DEDUP_CONFIRMATION_ATTEMPTS: usize = 4;
    const DEDUP_CONFIRMATION_BACKOFF_MS: u64 = 25;

    /// Check whether the intents have already been placed by comparing current
    /// open orders against the pre-send snapshot. Returns `Some(acks)` only when
    /// ALL intents match newly-appeared orders. On ambiguous transport paths,
    /// give the venue a brief chance to surface newly accepted orders before
    /// failing open.
    pub(crate) async fn check_dedup_scoped(
        &self,
        intents: &[OrderIntent],
        pre_send_ids: &HashSet<String>,
    ) -> Option<Vec<ExecutionAck>> {
        for attempt in 0..Self::DEDUP_CONFIRMATION_ATTEMPTS {
            let current = match self
                .fetch_detected_orders_for_intents(intents, "orders_dedup_scoped")
                .await
            {
                Ok(orders) => orders,
                Err(err) => {
                    tracing::warn!(
                        target: "execution.query",
                        attempt = attempt + 1,
                        max_attempts = Self::DEDUP_CONFIRMATION_ATTEMPTS,
                        error = %err,
                        "dedup_fetch_failed_allowing_retry"
                    );
                    if attempt + 1 < Self::DEDUP_CONFIRMATION_ATTEMPTS {
                        tokio::time::sleep(Duration::from_millis(
                            Self::DEDUP_CONFIRMATION_BACKOFF_MS,
                        ))
                        .await;
                    }
                    continue;
                }
            };
            if let Some(acks) = dedup_acks_from_current_orders(intents, pre_send_ids, &current) {
                return Some(acks);
            }
            if attempt + 1 < Self::DEDUP_CONFIRMATION_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(Self::DEDUP_CONFIRMATION_BACKOFF_MS))
                    .await;
            }
        }
        None
    }
}

fn dedup_acks_from_current_orders(
    intents: &[OrderIntent],
    pre_send_ids: &HashSet<String>,
    current: &[DetectedOrder],
) -> Option<Vec<ExecutionAck>> {
    let new_orders: Vec<&DetectedOrder> = current
        .iter()
        .filter(|o| !pre_send_ids.contains(&o.id))
        .collect();
    match_intents_to_new_orders(intents, &new_orders)
}

pub(crate) fn match_intents_to_new_orders(
    intents: &[OrderIntent],
    new_orders: &[&DetectedOrder],
) -> Option<Vec<ExecutionAck>> {
    if intents.is_empty() {
        return Some(Vec::new());
    }

    let mut used: Vec<bool> = vec![false; new_orders.len()];
    let mut acks: Vec<ExecutionAck> = Vec::with_capacity(intents.len());

    for intent in intents {
        let token_id = match resolve_token_id(intent) {
            Ok(id) => id,
            Err(_) => return None,
        };
        let side = to_polymarket_side(intent.side());
        let price = match decimal_from_f64("price", intent.limit_px) {
            Ok(p) => p,
            Err(_) => return None,
        };
        let size = match decimal_from_f64("size", intent.qty) {
            Ok(s) => s,
            Err(_) => return None,
        };

        let mut matched = false;
        for (i, order) in new_orders.iter().enumerate() {
            if used[i] {
                continue;
            }
            if order.asset_id == token_id
                && order.side == side
                && order.price == price
                && order.original_size == size
            {
                used[i] = true;
                acks.push(ExecutionAck {
                    order_id: Some(order.id.clone()),
                    accepted: true,
                    status: Some("LIVE".to_string()),
                    error_msg: None,
                    reject_kind: None,
                    request_ts_ms: None,
                    response_ts_ms: None,
                });
                matched = true;
                break;
            }
        }

        if !matched {
            return None;
        }
    }

    Some(acks)
}

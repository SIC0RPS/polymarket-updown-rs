use super::*;
use crate::execution::order_submit::batched_intents_by_tif;
use crate::polymarket::state::{book_for_token, MarketProjection, PolymarketOddsState};
use polymarket_client_sdk::clob::types::OrderPayload;

#[path = "client/ctf_operations.rs"]
mod ctf_operations;

#[derive(Debug, Clone)]
pub(crate) struct ExecutionPostTiming {
    pub(crate) input_range: std::ops::Range<usize>,
    pub(crate) signed_order_completed_at: Arc<[std::time::Instant]>,
    pub(crate) accepted_order_ids: Vec<Option<String>>,
    pub(crate) attempt: usize,
    pub(crate) server_time_request_started_at: Option<std::time::Instant>,
    pub(crate) server_time_request_completed_at: Option<std::time::Instant>,
    pub(crate) invocation_instant: std::time::Instant,
    pub(crate) terminal_instant: std::time::Instant,
    pub(crate) request_ts_ms: u64,
    pub(crate) response_headers_ts_ms: Option<u64>,
    pub(crate) terminal_ts_ms: u64,
    pub(crate) invocation_elapsed_us: u64,
    pub(crate) response_headers_elapsed_us: Option<u64>,
    pub(crate) response_body_elapsed_us: Option<u64>,
    pub(crate) http_status: Option<u16>,
    pub(crate) attempt_result: String,
    pub(crate) interpretation_error: Option<String>,
    pub(crate) response_evidence: Option<String>,
}

#[derive(Debug)]
pub(crate) struct IndexedExecutionAck {
    pub(crate) input_index: usize,
    pub(crate) ack: ExecutionAck,
}

#[derive(Debug)]
pub(crate) enum ExecutionBatchSendOutcome {
    Complete {
        known: Vec<IndexedExecutionAck>,
        post_timings: Vec<ExecutionPostTiming>,
    },
    Unknown {
        known: Vec<IndexedExecutionAck>,
        unknown: Vec<usize>,
        unsent: std::ops::Range<usize>,
        post_timings: Vec<ExecutionPostTiming>,
        error: anyhow::Error,
    },
    DefinitiveFailure {
        known: Vec<IndexedExecutionAck>,
        failed: std::ops::Range<usize>,
        unsent: std::ops::Range<usize>,
        post_timings: Vec<ExecutionPostTiming>,
        error: anyhow::Error,
    },
}

impl ExecutionClient {
    pub async fn inspect_signed_order(
        &self,
        intent: &OrderIntent,
    ) -> Result<(U256, U256, u64, u8)> {
        let live = self.live_backend();
        let signed = live
            .build_signed_order_with_type(intent, OrderType::GTC)
            .await?;
        let OrderPayload::V2(payload) = &signed.payload else {
            anyhow::bail!("expected V2 signed order");
        };
        let order = &payload.order;
        Ok((
            order.makerAmount,
            order.takerAmount,
            live.chain_id,
            order.signatureType,
        ))
    }

    pub async fn from_config(cfg: &AppConfig) -> Result<Self> {
        let Some(PolymarketExecutionMode::Live) = cfg.polymarket.execution.mode else {
            anyhow::bail!("polymarket.execution.mode must be set to \"live\"");
        };

        let key_var = cfg.polymarket.private_key_env.trim();
        let private_key = env::var(key_var).with_context(|| {
            format!("missing private key env var `{key_var}` for polymarket execution")
        })?;
        let signer = PrivateKeySigner::from_str(private_key.trim())
            .context("failed to parse Polymarket private key")?
            .with_chain_id(Some(cfg.polymarket.chain_id));

        for name in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            ensure!(
                env::var_os(name).is_none(),
                "{name} must be unset for direct Polymarket live execution"
            );
        }

        let clob_cfg = ClobConfig::builder()
            .use_server_time(cfg.polymarket.use_server_time)
            .build();
        let client =
            ClobClient::new(&cfg.polymarket.clob_http_url, clob_cfg).with_context(|| {
                format!(
                    "failed to create Polymarket CLOB client for {}",
                    cfg.polymarket.clob_http_url
                )
            })?;
        let auth_builder = client.authentication_builder(&signer);
        let funder = None;

        tracing::info!(
            signer = %signer.address(),
            signature_type = ?SignatureType::Eoa,
            is_proxy = false,
            is_safe = false,
            "configuring wallet for CLOB auth"
        );

        let normal_client = auth_builder
            .authenticate()
            .await
            .context("failed to authenticate Polymarket CLOB client")?;
        let version = normal_client
            .version()
            .await
            .context("failed to read Polymarket CLOB version")?;
        ensure!(version == 2, "CLOB backend version {version}, expected 2");
        tracing::info!(
            heartbeats_active = normal_client.heartbeats_active(),
            clob_version = version,
            "polymarket Normal mode ready"
        );
        let live_client = LiveClobClient::Normal(normal_client);
        let order_post_client = build_order_post_client()?;
        let account_rpc_url = cfg.polymarket.resolved_account_rpc_url();
        let finality_rpc_url = crate::polygon_rpc::preferred_url(&account_rpc_url);
        let finality_provider = RootProvider::new_http(
            finality_rpc_url
                .parse()
                .with_context(|| format!("invalid Polygon RPC URL {finality_rpc_url}"))?,
        );

        Ok(Self {
            backend: ExecutionBackend::Live(PolymarketExecutionLive {
                signer: Arc::new(signer),
                cached_nonce_manager: CachedNonceManager::default(),
                client: live_client,
                order_post_client,
                use_server_time: cfg.polymarket.use_server_time,
                chain_id: cfg.polymarket.chain_id,
                account_rpc_url,
                finality_http: reqwest::Client::new(),
                finality_provider,
                funder,
                live_ops: LiveOpsHandle::default(),
                reclaim_trigger: Arc::new(Notify::new()),
            }),
        })
    }

    pub fn simulated(heartbeats_active: bool) -> (Self, ExecutionSimHandle) {
        let handle = ExecutionSimHandle {
            state: Arc::new(Mutex::new(ExecutionSimState {
                heartbeats_active,
                cancel_delay_ms: 0,
                next_order_seq: 0,
                next_ctf_seq: 0,
                submitted: Vec::new(),
                consumed_submitted: 0,
                submitted_ctf_ops: Vec::new(),
                consumed_submitted_ctf_ops: 0,
                next_split_receipt: None,
                canceled: Vec::new(),
                consumed_canceled: 0,
                live_ops: LiveOpsHandle::default(),
                reclaim_trigger: Arc::new(Notify::new()),
            })),
        };
        (
            Self {
                backend: ExecutionBackend::Sim(ExecutionSimClient {
                    state: handle.clone(),
                }),
            },
            handle,
        )
    }

    pub fn backend_name(&self) -> &'static str {
        match &self.backend {
            ExecutionBackend::Live(_) => "polymarket_live",
            ExecutionBackend::Sim(_) => "polymarket_sim",
        }
    }

    pub fn polymarket_user_ws_auth(&self) -> PolymarketUserWsAuth {
        let live = self.live_backend();
        PolymarketUserWsAuth {
            address: live.client.address(),
            credentials: live.client.credentials().clone(),
        }
    }

    pub fn polymarket_address(&self) -> Address {
        match &self.backend {
            ExecutionBackend::Live(live) => live.client.address(),
            ExecutionBackend::Sim(_) => Address::ZERO,
        }
    }

    pub fn polymarket_tracked_wallet(&self) -> Address {
        match &self.backend {
            ExecutionBackend::Live(live) => live.funder.unwrap_or(live.client.address()),
            ExecutionBackend::Sim(_) => Address::ZERO,
        }
    }

    pub fn polymarket_wallet_kind(&self) -> &'static str {
        match &self.backend {
            ExecutionBackend::Live(live) => live.wallet_kind(),
            ExecutionBackend::Sim(_) => "simulated",
        }
    }

    pub fn prime_order_build_metadata(
        &self,
        intents: &[OrderIntent],
        odds_state: Option<&PolymarketOddsState>,
        market_projection: Option<&MarketProjection>,
    ) {
        let Some(market_projection) = market_projection else {
            return;
        };

        for intent in intents {
            let Ok(token_id) = U256::from_str(intent.instrument.trim()) else {
                continue;
            };
            let Some(selection) = market_projection.market_by_token(intent.instrument.trim())
            else {
                continue;
            };
            let live_tick_size = odds_state
                .and_then(|odds| book_for_token(odds, token_id))
                .map(|book| book.tick_size);
            if let ExecutionBackend::Live(live) = &self.backend {
                if let Some(neg_risk) = selection.neg_risk {
                    live.client.set_neg_risk(token_id, neg_risk);
                }
                if let Some(tick_size) =
                    live_tick_size.and_then(bbwick_engine::state::polymarket_tick_size_from_f64)
                {
                    live.client.set_tick_size(token_id, tick_size);
                }
            }
        }
    }

    pub fn heartbeats_active(&self) -> bool {
        match &self.backend {
            ExecutionBackend::Live(live) => live.client.heartbeats_active(),
            ExecutionBackend::Sim(sim) => sim.state.heartbeats_active(),
        }
    }

    pub fn live_backend(&self) -> &PolymarketExecutionLive {
        match &self.backend {
            ExecutionBackend::Live(live) => live,
            ExecutionBackend::Sim(_) => {
                panic!("live Polymarket execution backend requested from simulated client")
            }
        }
    }

    pub fn live_ops(&self) -> live_ops::LiveOpsHandle {
        match &self.backend {
            ExecutionBackend::Live(live) => live.live_ops.clone(),
            ExecutionBackend::Sim(sim) => sim.state.live_ops(),
        }
    }

    pub fn reclaim_ops_handle(&self) -> live_ops::LiveOpsHandle {
        match &self.backend {
            ExecutionBackend::Live(live) => live.live_ops.clone(),
            ExecutionBackend::Sim(sim) => sim.state.live_ops(),
        }
    }

    pub fn reclaim_route_label(&self) -> Result<&'static str> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.redeem_route_label(),
            ExecutionBackend::Sim(_) => Ok("simulated_reclaim"),
        }
    }

    pub fn reclaim_precheck(&self) -> Result<()> {
        match &self.backend {
            ExecutionBackend::Live(live) => {
                live.redeem_precheck()?;
                live.merge_precheck()
            }
            ExecutionBackend::Sim(_) => Ok(()),
        }
    }

    pub fn reclaim_trigger(&self) -> Arc<Notify> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.reclaim_trigger(),
            ExecutionBackend::Sim(sim) => sim.state.reclaim_trigger(),
        }
    }

    pub fn trigger_redeem_now(&self) -> Result<()> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.trigger_redeem_now(),
            ExecutionBackend::Sim(_) => {
                self.reclaim_precheck()?;
                self.reclaim_trigger().notify_one();
                Ok(())
            }
        }
    }

    pub async fn polymarket_balance_allowance(&self) -> Result<BalanceAllowanceResponse> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.balance_allowance().await,
            ExecutionBackend::Sim(_) => Ok(BalanceAllowanceResponse::default()),
        }
    }

    pub async fn polymarket_open_orders(&self) -> Result<Vec<OpenOrderResponse>> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.fetch_open_orders("orders").await,
            ExecutionBackend::Sim(_) => Ok(Vec::new()),
        }
    }

    pub async fn open_order_ids_in_scope(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<Vec<String>> {
        match &self.backend {
            ExecutionBackend::Live(live) => Ok(live
                .fetch_open_orders_filtered(market, asset_id, "orders_scope_verify")
                .await?
                .into_iter()
                .map(|order| order.id)
                .collect()),
            ExecutionBackend::Sim(sim) => Ok(sim
                .state
                .submitted_orders()
                .into_iter()
                .filter(|order| {
                    market.is_none_or(|market| {
                        order.intent.market_id.as_ref().map(|id| id.as_str()) == Some(market)
                    }) && asset_id.is_none_or(|asset| order.intent.instrument == asset)
                })
                .map(|order| order.order_id)
                .collect()),
        }
    }

    pub async fn open_order_ids_by_id(&self, order_id: &str) -> Result<Vec<String>> {
        match &self.backend {
            ExecutionBackend::Live(live) => Ok(live
                .fetch_open_orders_by_id(order_id, "orders_scope_verify_by_id")
                .await?
                .into_iter()
                .map(|order| order.id)
                .collect()),
            ExecutionBackend::Sim(sim) => Ok(sim
                .state
                .submitted_orders()
                .into_iter()
                .filter(|order| order.order_id == order_id)
                .map(|order| order.order_id)
                .collect()),
        }
    }

    pub(crate) fn finalized_sell_chain_id(&self) -> u64 {
        self.live_backend().chain_id
    }

    pub(crate) async fn observe_finalized_sell_transaction(
        &self,
        transaction_hash: B256,
    ) -> Result<Option<alloy::rpc::types::TransactionReceipt>> {
        let live = self.live_backend();
        live.observe_polygon_finality_once(transaction_hash).await
    }

    pub(crate) fn apply_finalized_sell_receipt(
        &self,
        fill: &mut crate::portfolio::FillEvent,
        receipt: &alloy::rpc::types::TransactionReceipt,
    ) -> Result<()> {
        let live = self.live_backend();
        crate::polymarket::fills::apply_finalized_v2_order_receipt(
            fill,
            receipt,
            live.chain_id,
            live.funder.unwrap_or_else(|| live.client.address()),
        )
    }

    pub async fn are_orders_scoring(&self, order_ids: &[String]) -> Result<HashMap<String, bool>> {
        if order_ids.is_empty() {
            return Ok(HashMap::new());
        }
        match &self.backend {
            ExecutionBackend::Live(live) => live.are_orders_scoring(order_ids).await,
            ExecutionBackend::Sim(_) => Ok(order_ids
                .iter()
                .cloned()
                .map(|order_id| (order_id, true))
                .collect()),
        }
    }

    pub async fn send(&self, intent: &OrderIntent) -> Result<ExecutionAck> {
        match &self.backend {
            ExecutionBackend::Live(live) => live.send(intent).await,
            ExecutionBackend::Sim(sim) => sim.send(intent).await,
        }
    }

    pub(crate) async fn send_batch(
        &self,
        intents: &[OrderIntent],
        known_open_order_ids: Option<&HashSet<String>>,
    ) -> ExecutionBatchSendOutcome {
        if intents.is_empty() {
            return ExecutionBatchSendOutcome::Complete {
                known: Vec::new(),
                post_timings: Vec::new(),
            };
        }

        let mut out = Vec::with_capacity(intents.len());
        let mut all_post_timings = Vec::new();
        let mut input_offset = 0usize;
        for chunk in batched_intents_by_tif(intents) {
            let outcome = match &self.backend {
                ExecutionBackend::Live(live) => live.send_batch(chunk, known_open_order_ids).await,
                ExecutionBackend::Sim(sim) => match sim.send_batch(chunk).await {
                    Ok(acks) => ExecutionBatchSendOutcome::Complete {
                        known: acks
                            .into_iter()
                            .enumerate()
                            .map(|(input_index, ack)| IndexedExecutionAck { input_index, ack })
                            .collect(),
                        post_timings: Vec::new(),
                    },
                    Err(error) => ExecutionBatchSendOutcome::DefinitiveFailure {
                        known: Vec::new(),
                        failed: 0..0,
                        unsent: 0..chunk.len(),
                        post_timings: Vec::new(),
                        error,
                    },
                },
            };
            match outcome {
                ExecutionBatchSendOutcome::Complete {
                    mut known,
                    mut post_timings,
                } => {
                    for result in &mut known {
                        result.input_index = input_offset.saturating_add(result.input_index);
                    }
                    for timing in &mut post_timings {
                        timing.input_range = input_offset + timing.input_range.start
                            ..input_offset + timing.input_range.end;
                    }
                    out.extend(known);
                    all_post_timings.extend(post_timings);
                }
                ExecutionBatchSendOutcome::Unknown {
                    mut known,
                    unknown,
                    unsent,
                    mut post_timings,
                    error,
                } => {
                    for result in &mut known {
                        result.input_index = input_offset.saturating_add(result.input_index);
                    }
                    for timing in &mut post_timings {
                        timing.input_range = input_offset + timing.input_range.start
                            ..input_offset + timing.input_range.end;
                    }
                    out.extend(known);
                    all_post_timings.extend(post_timings);
                    return ExecutionBatchSendOutcome::Unknown {
                        known: out,
                        unknown: unknown
                            .into_iter()
                            .map(|index| input_offset.saturating_add(index))
                            .collect(),
                        unsent: input_offset.saturating_add(unsent.start)..intents.len(),
                        post_timings: all_post_timings,
                        error,
                    };
                }
                ExecutionBatchSendOutcome::DefinitiveFailure {
                    mut known,
                    failed,
                    unsent,
                    mut post_timings,
                    error,
                } => {
                    for result in &mut known {
                        result.input_index = input_offset.saturating_add(result.input_index);
                    }
                    for timing in &mut post_timings {
                        timing.input_range = input_offset + timing.input_range.start
                            ..input_offset + timing.input_range.end;
                    }
                    out.extend(known);
                    all_post_timings.extend(post_timings);
                    return ExecutionBatchSendOutcome::DefinitiveFailure {
                        known: out,
                        failed: input_offset + failed.start..input_offset + failed.end,
                        unsent: input_offset.saturating_add(unsent.start)..intents.len(),
                        post_timings: all_post_timings,
                        error,
                    };
                }
            }
            input_offset = input_offset.saturating_add(chunk.len());
        }
        ExecutionBatchSendOutcome::Complete {
            known: out,
            post_timings: all_post_timings,
        }
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelAck> {
        self.cancel_orders(&[order_id.to_string()]).await
    }

    pub async fn cancel_orders(&self, order_ids: &[String]) -> Result<CancelAck> {
        if order_ids.is_empty() {
            return Ok(CancelAck {
                canceled: Vec::new(),
                not_canceled: Vec::new(),
            });
        }
        match &self.backend {
            ExecutionBackend::Live(live) => live.cancel_orders(order_ids).await,
            ExecutionBackend::Sim(sim) => sim.cancel_orders(order_ids).await,
        }
    }

    pub async fn cancel_all_orders(&self) -> Result<CancelAck> {
        self.live_backend().cancel_all_orders().await
    }

    pub async fn cancel_market_orders(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<CancelAck> {
        if market.is_none() && asset_id.is_none() {
            return Ok(CancelAck {
                canceled: Vec::new(),
                not_canceled: Vec::new(),
            });
        }
        self.live_backend()
            .cancel_market_orders(market, asset_id)
            .await
    }
}

impl ExecutionSimClient {
    async fn send(&self, intent: &OrderIntent) -> Result<ExecutionAck> {
        let mut acks = self.send_batch(std::slice::from_ref(intent)).await?;
        Ok(acks
            .pop()
            .expect("sim send_batch returns one ack for one intent"))
    }

    async fn send_batch(&self, intents: &[OrderIntent]) -> Result<Vec<ExecutionAck>> {
        let mut state = self
            .state
            .state
            .lock()
            .expect("execution sim lock poisoned");
        let mut acks = Vec::with_capacity(intents.len());
        for intent in intents {
            state.next_order_seq = state.next_order_seq.saturating_add(1);
            let order_id = format!("sim-exec-order-{}", state.next_order_seq);
            state.submitted.push(SimSubmittedOrder {
                order_id: order_id.clone(),
                intent: intent.clone(),
            });
            acks.push(ExecutionAck {
                order_id: Some(order_id),
                accepted: true,
                status: None,
                error_msg: None,
                reject_kind: None,
                request_ts_ms: None,
                response_ts_ms: None,
            });
        }
        Ok(acks)
    }

    async fn cancel_orders(&self, order_ids: &[String]) -> Result<CancelAck> {
        let delay_ms = self.state.cancel_delay_ms();
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let mut state = self
            .state
            .state
            .lock()
            .expect("execution sim lock poisoned");
        state.canceled.extend(order_ids.iter().cloned());
        Ok(CancelAck {
            canceled: order_ids.to_vec(),
            not_canceled: Vec::new(),
        })
    }
}

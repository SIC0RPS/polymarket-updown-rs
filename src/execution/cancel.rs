use super::*;

const MAX_CANCEL_ORDER_IDS: usize = 1000;

impl PolymarketExecutionLive {
    pub(crate) async fn cancel_orders(&self, order_ids: &[String]) -> Result<CancelAck> {
        if order_ids.is_empty() {
            return Ok(CancelAck {
                canceled: Vec::new(),
                not_canceled: Vec::new(),
            });
        }

        let mut canceled = Vec::new();
        let mut not_canceled = Vec::new();

        for chunk in order_ids.chunks(MAX_CANCEL_ORDER_IDS) {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            let response = self
                .run_cancel_request(
                    ApiRetryPolicy {
                        endpoint: "DELETE /orders",
                        method: "cancel_orders",
                        allow_read_retry: false,
                        retry_425_log: Some("cancel_orders_http_425_retry"),
                        retry_429_log: Some("Rate limited by CLOB API, backing off"),
                    },
                    "failed to cancel Polymarket order batch",
                    || async { dispatch_client!(&self.client, |c| c.cancel_orders(&refs).await) },
                )
                .await?;
            let ack = to_cancel_ack(response);
            canceled.extend(ack.canceled);
            not_canceled.extend(ack.not_canceled);
        }

        Ok(CancelAck {
            canceled,
            not_canceled,
        })
    }

    pub(crate) async fn cancel_all_orders(&self) -> Result<CancelAck> {
        let response = self
            .run_cancel_request(
                ApiRetryPolicy {
                    endpoint: "DELETE /cancel-all",
                    method: "cancel_all_orders",
                    allow_read_retry: false,
                    retry_425_log: Some("cancel_all_http_425_retry"),
                    retry_429_log: Some("Rate limited by CLOB API, backing off"),
                },
                "failed to cancel all Polymarket orders",
                || async { dispatch_client!(&self.client, |c| c.cancel_all_orders().await) },
            )
            .await?;
        Ok(to_cancel_ack(response))
    }

    pub(crate) async fn cancel_market_orders(
        &self,
        market: Option<&str>,
        asset_id: Option<&str>,
    ) -> Result<CancelAck> {
        let market = market
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(B256::from_str)
            .transpose()
            .context("invalid cancel-market-orders market id")?;
        let asset_id = asset_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(U256::from_str)
            .transpose()
            .context("invalid cancel-market-orders asset id")?;

        if market.is_none() && asset_id.is_none() {
            return Ok(CancelAck {
                canceled: Vec::new(),
                not_canceled: Vec::new(),
            });
        }

        let request = match (market, asset_id) {
            (Some(market), Some(asset_id)) => CancelMarketOrderRequest::builder()
                .market(market)
                .asset_id(asset_id)
                .build(),
            (Some(market), None) => CancelMarketOrderRequest::builder().market(market).build(),
            (None, Some(asset_id)) => CancelMarketOrderRequest::builder()
                .asset_id(asset_id)
                .build(),
            (None, None) => CancelMarketOrderRequest::builder().build(),
        };
        let response = self
            .run_cancel_request(
                ApiRetryPolicy {
                    endpoint: "DELETE /cancel-market-orders",
                    method: "cancel_market_orders",
                    allow_read_retry: false,
                    retry_425_log: Some("cancel_market_orders_http_425_retry"),
                    retry_429_log: Some("Rate limited by CLOB API, backing off"),
                },
                "failed to cancel Polymarket market orders",
                || async {
                    dispatch_client!(&self.client, |c| c.cancel_market_orders(&request).await)
                },
            )
            .await?;

        Ok(to_cancel_ack(response))
    }

    async fn run_cancel_request<F, Fut>(
        &self,
        policy: ApiRetryPolicy<'_>,
        error_context: &'static str,
        mut request_fn: F,
    ) -> Result<CancelOrdersResponse>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<
            Output = std::result::Result<CancelOrdersResponse, PolymarketClientError>,
        >,
    {
        let mut state = ApiRetryState::new();
        loop {
            state.attempt += 1;
            let cancel_res = request_fn().await;
            match self.run_api_attempt(&policy, &mut state, cancel_res).await {
                ApiRetryOutcome::Success(response) => return Ok(response),
                ApiRetryOutcome::Retry(_) => {}
                ApiRetryOutcome::Error(err) => {
                    return Err(anyhow::Error::new(err).context(error_context));
                }
            }
        }
    }
}

pub(crate) fn to_cancel_ack(response: CancelOrdersResponse) -> CancelAck {
    CancelAck {
        canceled: response.canceled,
        not_canceled: response.not_canceled.into_iter().collect(),
    }
}

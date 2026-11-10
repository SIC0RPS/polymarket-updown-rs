use super::*;

impl PolymarketExecutionLive {
    pub(crate) async fn are_orders_scoring(
        &self,
        order_ids: &[String],
    ) -> Result<HashMap<String, bool>> {
        if order_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
        let policy = ApiRetryPolicy {
            endpoint: "POST /orders-scoring",
            method: "are_orders_scoring",
            allow_read_retry: true,
            retry_425_log: None,
            retry_429_log: None,
        };
        let mut state = ApiRetryState::new();

        loop {
            state.attempt += 1;
            let res = dispatch_client!(&self.client, |c| c.are_orders_scoring(&refs).await);
            match self.run_api_attempt(&policy, &mut state, res).await {
                ApiRetryOutcome::Success(res) => return Ok(res),
                ApiRetryOutcome::Retry(_) => {}
                ApiRetryOutcome::Error(err) => {
                    return Err(anyhow::Error::new(err).context(format!(
                        "failed to query order scoring order_ids_count={} attempts={} transient_retries={}",
                        order_ids.len(),
                        state.attempt,
                        state.read_retry.retries
                    )));
                }
            }
        }
    }
}

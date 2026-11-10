use super::*;

impl PolymarketExecutionLive {
    pub(crate) fn wallet_kind(&self) -> &'static str {
        "eoa"
    }

    pub(crate) fn emit_api_call(
        &self,
        endpoint: &str,
        method: &str,
        backoff_ms: u64,
        http_status: Option<u16>,
        ok: bool,
    ) {
        self.live_ops
            .record_http_result(endpoint, method, http_status, ok, backoff_ms);
    }
}

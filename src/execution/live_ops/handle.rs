use super::*;

impl LiveOpsHandle {
    pub fn snapshot(&self) -> LiveOpsSnapshot {
        self.with_lock(|state| state.snapshot())
    }

    pub fn record_heartbeat_sample(&self, active: Option<bool>) {
        let now = now_ms();
        self.with_lock(|state| state.record_heartbeat_sample(now, active));
    }

    pub fn record_http_result(
        &self,
        endpoint: &str,
        method: &str,
        http_status: Option<u16>,
        ok: bool,
        backoff_ms: u64,
    ) {
        let now = now_ms();
        self.with_lock(|state| {
            state.record_http_result(now, endpoint, method, http_status, ok, backoff_ms)
        });
    }

    pub fn record_redeem_disabled(&self, reason: impl Into<String>) {
        let now = now_ms();
        let reason = reason.into();
        self.with_lock(|state| state.record_redeem_disabled(now, reason));
    }

    pub fn record_redeem_enabled(&self, route: &str) {
        let now = now_ms();
        self.with_lock(|state| state.record_redeem_enabled(now, route));
    }

    pub fn record_redeem_poll_ok(&self) {
        let now = now_ms();
        self.with_lock(|state| state.record_redeem_poll_ok(now));
    }

    pub fn record_redeem_poll_error(&self, error: impl Into<String>) {
        let now = now_ms();
        let error = error.into();
        self.with_lock(|state| state.record_redeem_poll_error(now, error));
    }

    pub fn record_redeem_tx_submit_attempt(&self, condition_id: &str) {
        let now = now_ms();
        self.with_lock(|state| state.record_redeem_tx_submit_attempt(now, condition_id));
    }

    pub fn record_redeem_pending(&self, condition_id: &str, route: &str) {
        let now = now_ms();
        self.with_lock(|state| {
            state.record_redeem_status(now, condition_id, route, "pending", None, None)
        });
    }

    pub fn record_redeem_claims(
        &self,
        condition_id: &str,
        route: &str,
        claims: Vec<RedeemClaimSnapshot>,
    ) {
        let now = now_ms();
        self.with_lock(|state| state.record_redeem_claims(now, condition_id, route, claims));
    }

    pub fn record_redeem_tx_status(
        &self,
        condition_id: &str,
        route: &str,
        status: &str,
        tx_hash: Option<String>,
    ) {
        let now = now_ms();
        self.with_lock(|state| {
            state.record_redeem_status(now, condition_id, route, status, tx_hash, None)
        });
    }

    pub fn record_redeem_failed(&self, condition_id: &str, route: &str, error: impl Into<String>) {
        let now = now_ms();
        let error = error.into();
        self.with_lock(|state| {
            state.record_redeem_status(now, condition_id, route, "failed", None, Some(error))
        });
    }

    pub fn record_inventory_pending(&self, market_id: &str, condition_id: &str, operation: &str) {
        let now = now_ms();
        self.with_lock(|state| {
            state.record_inventory_status(
                now,
                market_id,
                condition_id,
                operation,
                "pending",
                None,
                None,
            )
        });
    }

    pub fn record_inventory_confirmed(
        &self,
        market_id: &str,
        condition_id: &str,
        operation: &str,
        tx_ref: Option<String>,
    ) {
        let now = now_ms();
        self.with_lock(|state| {
            state.record_inventory_status(
                now,
                market_id,
                condition_id,
                operation,
                "confirmed",
                tx_ref,
                None,
            )
        });
    }

    pub fn record_inventory_ambiguous(
        &self,
        market_id: &str,
        condition_id: &str,
        operation: &str,
        tx_ref: Option<String>,
        error: impl Into<String>,
    ) {
        let now = now_ms();
        let error = error.into();
        self.with_lock(|state| {
            state.record_inventory_status(
                now,
                market_id,
                condition_id,
                operation,
                "ambiguous",
                tx_ref,
                Some(error),
            )
        });
    }

    pub fn record_inventory_failed(
        &self,
        market_id: &str,
        condition_id: &str,
        operation: &str,
        tx_ref: Option<String>,
        error: impl Into<String>,
    ) {
        let now = now_ms();
        let error = error.into();
        self.with_lock(|state| {
            state.record_inventory_status(
                now,
                market_id,
                condition_id,
                operation,
                "failed",
                tx_ref,
                Some(error),
            )
        });
    }

    pub fn record_inventory_refresh_failed(
        &self,
        market_id: &str,
        condition_id: &str,
        operation: &str,
        tx_ref: Option<String>,
        error: impl Into<String>,
    ) {
        let now = now_ms();
        let error = error.into();
        self.with_lock(|state| {
            state.record_inventory_status(
                now,
                market_id,
                condition_id,
                operation,
                "refresh_failed",
                tx_ref,
                Some(error),
            )
        });
    }

    fn with_lock<T>(&self, f: impl FnOnce(&mut LiveOpsState) -> T) -> T {
        let mut guard = self.inner.lock().expect("live ops mutex poisoned");
        f(&mut guard)
    }
}

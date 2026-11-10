use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::config::RiskConfig;

use super::account_equity::AccountEquitySnapshot;
use super::admission::AdmissionExposureState;
use super::hard_stops::apply_hard_stop_limits;
use super::snapshot::RiskSnapshot;

#[derive(Clone)]
pub struct RiskHandle {
    pub(super) runtime: Arc<RwLock<RiskRuntimeConfig>>,
    pub(super) state_tx: watch::Sender<RiskSnapshot>,
    pub(super) admission: Arc<RwLock<AdmissionExposureState>>,
    account: Arc<RwLock<Option<AccountEquitySnapshot>>>,
}

#[derive(Clone)]
pub(super) struct RiskRuntimeConfig {
    pub(super) cfg: RiskConfig,
    pub(super) max_order_qty: f64,
}

impl RiskHandle {
    pub fn new(cfg: RiskConfig, max_order_qty: f64) -> (Self, watch::Receiver<RiskSnapshot>) {
        let initial = RiskSnapshot {
            updated_at_ms: now_ms(),
            enabled: cfg.enabled,
            trading_allowed: true,
            max_order_qty,
            max_window_cost: cfg.max_window_cost,
            max_market_equity_pct: cfg.max_market_equity_pct,
            ..Default::default()
        };
        let (state_tx, state_rx) = watch::channel(initial);
        (
            Self {
                runtime: Arc::new(RwLock::new(RiskRuntimeConfig { cfg, max_order_qty })),
                state_tx,
                admission: Arc::new(RwLock::new(AdmissionExposureState::default())),
                account: Arc::new(RwLock::new(None)),
            },
            state_rx,
        )
    }

    pub fn from_captured_snapshot(
        cfg: RiskConfig,
        snapshot: RiskSnapshot,
    ) -> anyhow::Result<(Self, watch::Receiver<RiskSnapshot>)> {
        let admission = captured_admission_state(&snapshot)?;
        let mut runtime = RiskRuntimeConfig {
            cfg,
            max_order_qty: snapshot.max_order_qty,
        };
        apply_captured_runtime_fields(&mut runtime, &snapshot);
        let (state_tx, state_rx) = watch::channel(snapshot);
        Ok((
            Self {
                runtime: Arc::new(RwLock::new(runtime)),
                state_tx,
                admission: Arc::new(RwLock::new(admission)),
                account: Arc::new(RwLock::new(None)),
            },
            state_rx,
        ))
    }

    pub(super) fn runtime_config(&self) -> RiskRuntimeConfig {
        self.runtime
            .read()
            .expect("risk runtime lock poisoned")
            .clone()
    }

    pub fn cfg(&self) -> RiskConfig {
        self.runtime_config().cfg
    }

    pub fn update_runtime_config(&self, cfg: RiskConfig, max_order_qty: f64) {
        let mut runtime = self.runtime.write().expect("risk runtime lock poisoned");
        runtime.cfg = cfg.clone();
        runtime.max_order_qty = max_order_qty;

        self.state_tx.send_modify(|s| {
            s.updated_at_ms = now_ms();
            s.enabled = cfg.enabled;
            s.max_order_qty = max_order_qty;
            s.max_window_cost = cfg.max_window_cost;
            s.max_market_equity_pct = cfg.max_market_equity_pct;

            if !cfg.enabled {
                s.trading_allowed = true;
                s.freeze_level = 0;
                s.frozen_rule = None;
                s.frozen_details = None;
                return;
            }

            if !s.trading_allowed {
                let rule = s.frozen_rule.as_deref().unwrap_or("");
                let is_balance_freeze =
                    rule == "insufficient_balance" || rule == "startup_insufficient_balance";
                if is_balance_freeze {
                    if let Some(bal) = s.usdc_balance {
                        if bal >= cfg.min_usdc_balance {
                            s.trading_allowed = true;
                            s.freeze_level = 0;
                            s.frozen_rule = None;
                            s.frozen_details = None;
                        }
                    }
                }
            }

            apply_hard_stop_limits(s, &cfg);
        });
        drop(runtime);
    }

    pub fn cap_order_qty(&self, qty: f64) -> f64 {
        if !(qty.is_finite() && qty > 0.0) {
            return qty;
        }
        let runtime = self.runtime_config();
        crate::execution::sizing::cap_order_qty(qty, runtime.max_order_qty)
    }

    pub fn snapshot(&self) -> watch::Receiver<RiskSnapshot> {
        self.state_tx.subscribe()
    }

    pub fn current_snapshot(&self) -> RiskSnapshot {
        self.state_tx.borrow().clone()
    }

    pub fn account_source_ts_ms(&self) -> Option<u64> {
        self.account
            .read()
            .expect("risk account lock poisoned")
            .as_ref()
            .map(|account| account.source_ts_ms)
    }

    pub fn freeze(&self, rule: impl Into<String>, details: impl Into<String>) {
        let runtime = self.runtime_config();
        let rule = rule.into();
        let details = details.into();
        self.state_tx.send_modify(|s| {
            s.updated_at_ms = now_ms();
            s.enabled = runtime.cfg.enabled;
            s.trading_allowed = false;
            s.freeze_level = 2;
            s.frozen_rule = Some(rule.clone());
            s.frozen_details = Some(details.clone());
        });
    }

    pub fn unfreeze(&self) {
        let runtime = self.runtime_config();
        self.state_tx.send_modify(|s| {
            s.updated_at_ms = now_ms();
            s.enabled = runtime.cfg.enabled;
            s.trading_allowed = true;
            s.freeze_level = 0;
            s.frozen_rule = None;
            s.frozen_details = None;
            s.peak_equity = 0.0;
            s.current_equity = 0.0;
            s.last_positive_equity = None;
            apply_hard_stop_limits(s, &runtime.cfg);
        });
    }

    pub fn set_account_equity_snapshot(&self, account: &AccountEquitySnapshot) {
        let runtime = self.runtime_config();
        *self.account.write().expect("risk account lock poisoned") = Some(account.clone());
        self.state_tx.send_if_modified(|s| {
            let mut next = s.clone();
            next.wallet_usdc_balance = account.wallet_usdc_balance.map(|value| value.as_f64());
            next.wallet_matic_balance = account.wallet_matic_balance;
            next.marked_positions_value =
                account.marked_positions_value.map(|value| value.as_f64());
            next.current_wallet_equity = account.current_wallet_equity.map(|value| value.as_f64());
            next.trading_collateral_balance = account
                .trading_collateral_balance
                .map(|value| value.as_f64());
            next.account_equity_updated_at_ms = Some(account.source_ts_ms);
            if let Some(equity) = account.current_wallet_equity {
                update_equity_fields(&mut next, equity.as_f64());
            }
            apply_hard_stop_limits(&mut next, &runtime.cfg);
            if account_equity_state_matches(s, &next) {
                return false;
            }
            next.updated_at_ms = now_ms();
            *s = next;
            true
        });
    }

    pub fn set_balance_snapshot(&self, usdc_balance: f64, exchange_allowance: f64) {
        let runtime = self.runtime_config();
        self.state_tx.send_if_modified(|s| {
            let mut next = s.clone();
            next.enabled = runtime.cfg.enabled;
            next.usdc_balance = Some(usdc_balance);
            next.exchange_allowance = Some(exchange_allowance);

            if !next.trading_allowed {
                let rule = next.frozen_rule.as_deref().unwrap_or("");
                let is_balance_freeze =
                    rule == "insufficient_balance" || rule == "startup_insufficient_balance";
                if is_balance_freeze && usdc_balance >= runtime.cfg.min_usdc_balance {
                    next.trading_allowed = true;
                    next.freeze_level = 0;
                    next.frozen_rule = None;
                    next.frozen_details = None;
                }
            }

            apply_hard_stop_limits(&mut next, &runtime.cfg);
            if balance_state_matches(s, &next) {
                return false;
            }
            next.updated_at_ms = now_ms();
            *s = next;
            true
        });
    }
}

fn captured_admission_state(snapshot: &RiskSnapshot) -> anyhow::Result<AdmissionExposureState> {
    anyhow::ensure!(
        snapshot.inflight_buy_notional_by_market.is_empty(),
        "captured risk snapshot contains aggregate inflight buy exposure without reservation identities"
    );
    AdmissionExposureState::from_captured_snapshot(snapshot.authoritative_market_exposures.clone())
}

fn apply_captured_runtime_fields(runtime: &mut RiskRuntimeConfig, snapshot: &RiskSnapshot) {
    runtime.cfg.enabled = snapshot.enabled;
    runtime.cfg.max_window_cost = snapshot.max_window_cost;
    runtime.cfg.max_market_equity_pct = snapshot.max_market_equity_pct;
    runtime.max_order_qty = snapshot.max_order_qty;
}

fn account_equity_state_matches(left: &RiskSnapshot, right: &RiskSnapshot) -> bool {
    left.wallet_usdc_balance == right.wallet_usdc_balance
        && left.wallet_matic_balance == right.wallet_matic_balance
        && left.marked_positions_value == right.marked_positions_value
        && left.current_wallet_equity == right.current_wallet_equity
        && left.trading_collateral_balance == right.trading_collateral_balance
        && left.account_equity_updated_at_ms == right.account_equity_updated_at_ms
        && left.peak_equity == right.peak_equity
        && left.current_equity == right.current_equity
        && left.last_positive_equity == right.last_positive_equity
        && hard_stop_state_matches(left, right)
}

fn balance_state_matches(left: &RiskSnapshot, right: &RiskSnapshot) -> bool {
    left.enabled == right.enabled
        && left.usdc_balance == right.usdc_balance
        && left.exchange_allowance == right.exchange_allowance
        && hard_stop_state_matches(left, right)
}

fn hard_stop_state_matches(left: &RiskSnapshot, right: &RiskSnapshot) -> bool {
    left.trading_allowed == right.trading_allowed
        && left.freeze_level == right.freeze_level
        && left.frozen_rule == right.frozen_rule
        && left.frozen_details == right.frozen_details
}

fn update_equity_fields(s: &mut RiskSnapshot, equity: f64) {
    s.current_equity = equity;
    if equity.is_finite() && equity > 0.0 {
        s.last_positive_equity = Some(equity);
    }
    if equity > s.peak_equity {
        s.peak_equity = equity;
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

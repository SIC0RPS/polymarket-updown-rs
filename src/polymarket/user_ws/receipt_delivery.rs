use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr as _;
use std::time::{Duration, Instant};

use alloy::primitives::B256;
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use futures_util::FutureExt as _;
use tokio::sync::mpsc;

use crate::capture::session::MaybeCapture;
use crate::execution::ExecutionClient;
use crate::observability::action::ActionEvent;
use crate::observability::Observability;
use crate::polymarket::order_context::{FillClassification, OrderFillContext};
use crate::portfolio::{FillEvent, NormalizedStrategyFill};
use crate::trade_session_ledger::ObservedCtfTradeFact;

const MAX_RETAINED_SELL_RECEIPTS: usize = 1_024;
const FINALITY_RETRY_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TransactionKey {
    chain_id: u64,
    transaction_hash: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingNotificationIdentity {
    transaction_hash: B256,
    quantity: u128,
    gross_proceeds: Option<u128>,
    fee: Option<u128>,
    matched_trade: ObservedCtfTradeFact,
}

struct ReceiptObligation {
    fill_key: (String, String),
    fill: FillEvent,
    context: OrderFillContext,
    matched_trade: ObservedCtfTradeFact,
}

struct TransactionGroup {
    obligations: Vec<ReceiptObligation>,
    next_attempt_at: Option<Instant>,
}

struct ReadyFill {
    normalized: NormalizedStrategyFill,
    action: ActionEvent,
    receipt_key: Option<(String, String)>,
}

struct TransferOutcome {
    action: ActionEvent,
    receipt_key: Option<(String, String)>,
    result: Result<(), String>,
}

type ActiveAttempt = BoxFuture<
    'static,
    (
        TransactionKey,
        anyhow::Result<Option<alloy::rpc::types::TransactionReceipt>>,
    ),
>;
type ActiveTransfer = BoxFuture<'static, TransferOutcome>;

pub(super) enum ReceiptAdmission {
    Admitted,
    IdenticalPending,
    Rejected(String),
}

pub(super) struct ReceiptDelivery {
    exec: ExecutionClient,
    fills_tx: mpsc::Sender<NormalizedStrategyFill>,
    obs: Observability,
    capture: MaybeCapture,
    groups: HashMap<TransactionKey, TransactionGroup>,
    active_keys: HashSet<TransactionKey>,
    attempts: FuturesUnordered<ActiveAttempt>,
    pending_identities: HashMap<(String, String), PendingNotificationIdentity>,
    ready_fills: VecDeque<ReadyFill>,
    transfers: FuturesUnordered<ActiveTransfer>,
    retained_receipts: usize,
    max_transfers: usize,
    delivery_failure: Option<String>,
}

impl ReceiptDelivery {
    pub(super) fn new(
        exec: ExecutionClient,
        fills_tx: mpsc::Sender<NormalizedStrategyFill>,
        obs: Observability,
        capture: MaybeCapture,
    ) -> Self {
        let max_transfers = fills_tx.max_capacity();
        Self {
            exec,
            fills_tx,
            obs,
            capture,
            groups: HashMap::new(),
            active_keys: HashSet::new(),
            attempts: FuturesUnordered::new(),
            pending_identities: HashMap::new(),
            ready_fills: VecDeque::new(),
            transfers: FuturesUnordered::new(),
            retained_receipts: 0,
            max_transfers,
            delivery_failure: None,
        }
    }

    pub(super) fn has_work(&self) -> bool {
        !self.groups.is_empty()
            || !self.attempts.is_empty()
            || !self.ready_fills.is_empty()
            || !self.transfers.is_empty()
    }

    pub(super) async fn finish(&mut self) -> anyhow::Result<()> {
        for group in self.groups.values_mut() {
            if group.next_attempt_at.is_some() {
                group.next_attempt_at = Some(Instant::now());
            }
        }
        self.start_ready_attempts();
        loop {
            self.start_ready_transfers();
            if self.attempts.is_empty() && self.transfers.is_empty() {
                break;
            }
            tokio::select! {
                attempt = self.attempts.next(), if !self.attempts.is_empty() => {
                    if let Some((key, result)) = attempt {
                        self.finish_attempt(key, result);
                    }
                }
                transfer = self.transfers.next(), if !self.transfers.is_empty() => {
                    if let Some(outcome) = transfer {
                        self.finish_transfer(outcome);
                    }
                }
            }
        }
        anyhow::ensure!(
            !self.has_work() && self.pending_identities.is_empty()
                && self.delivery_failure.is_none(),
            "authenticated receipt delivery unresolved: transactions={} receipts={} queued_fills={} transfer_failure={:?}",
            self.groups.len(), self.retained_receipts, self.ready_fills.len(), self.delivery_failure,
        );
        Ok(())
    }

    pub(super) fn can_admit_receipt(&self) -> bool {
        self.retained_receipts < MAX_RETAINED_SELL_RECEIPTS
    }

    pub(super) fn can_admit_direct(&self) -> bool {
        self.ready_fills.len() + self.transfers.len() < self.max_transfers
    }

    pub(super) fn admit_receipt(
        &mut self,
        mut fill: FillEvent,
        context: OrderFillContext,
        matched_trade: ObservedCtfTradeFact,
    ) -> ReceiptAdmission {
        let authorization = context.direct_buy_authorization();
        if fill.direct_buy_authorization.is_some() && fill.direct_buy_authorization != authorization
        {
            return ReceiptAdmission::Rejected("BUY receipt authorization conflict".to_string());
        }
        fill.direct_buy_authorization = authorization;
        if !self.can_admit_receipt() {
            return ReceiptAdmission::Rejected(
                "confirmed sell receipt capacity unavailable".to_string(),
            );
        }
        let venue_trade_id = match fill
            .trade_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(value) => value.to_string(),
            None => {
                return ReceiptAdmission::Rejected(
                    "confirmed sell receipt has no venue trade identity".to_string(),
                )
            }
        };
        let order_id = fill.order_id.trim().to_string();
        if order_id.is_empty() {
            return ReceiptAdmission::Rejected(
                "confirmed sell receipt has no order identity".to_string(),
            );
        }
        let transaction_hash = match fill
            .transaction_hash
            .as_deref()
            .map(str::trim)
            .and_then(|value| B256::from_str(value).ok())
            .filter(|value| *value != B256::ZERO)
        {
            Some(value) => value,
            None => {
                return ReceiptAdmission::Rejected(
                    "confirmed sell receipt transaction identity is invalid".to_string(),
                )
            }
        };
        let quantity = match fill.receipt_qty_units_6dp.filter(|value| *value > 0) {
            Some(value) => value,
            None => {
                return ReceiptAdmission::Rejected(
                    "confirmed sell receipt quantity is invalid".to_string(),
                )
            }
        };
        let (gross_proceeds, fee) = match fill.trade_observation.as_ref() {
            Some(observation) => (
                observation.owned_gross_proceeds_units_6dp(),
                observation.owned_fee_units_6dp(),
            ),
            None => {
                return ReceiptAdmission::Rejected(
                    "confirmed sell receipt has no trade observation".to_string(),
                )
            }
        };
        let fill_key = (venue_trade_id, order_id);
        let notification = PendingNotificationIdentity {
            transaction_hash,
            quantity,
            gross_proceeds,
            fee,
            matched_trade: matched_trade.clone(),
        };
        if let Some(existing) = self.pending_identities.get(&fill_key) {
            return if existing == &notification {
                ReceiptAdmission::IdenticalPending
            } else {
                ReceiptAdmission::Rejected(
                    "conflicting retained venue trade/order notification".to_string(),
                )
            };
        }
        let key = TransactionKey {
            chain_id: self.exec.finalized_sell_chain_id(),
            transaction_hash,
        };
        self.pending_identities
            .insert(fill_key.clone(), notification);
        self.groups
            .entry(key)
            .and_modify(|group| {
                if group.next_attempt_at.is_none() {
                    group.next_attempt_at = Some(Instant::now());
                }
            })
            .or_insert_with(|| TransactionGroup {
                obligations: Vec::new(),
                next_attempt_at: Some(Instant::now()),
            })
            .obligations
            .push(ReceiptObligation {
                fill_key,
                fill,
                context,
                matched_trade,
            });
        self.retained_receipts += 1;
        ReceiptAdmission::Admitted
    }

    pub(super) fn admit_direct(
        &mut self,
        fill: FillEvent,
        context: OrderFillContext,
        matched_trade: ObservedCtfTradeFact,
    ) {
        match context.normalize_fill_with_matched_trade(fill, matched_trade) {
            FillClassification::Normalized(fill) => self.queue_fill(fill, context, None),
            FillClassification::Quarantined(quarantined) => {
                tracing::warn!(
                    target: "polymarket.user_ws",
                    reason = quarantined.reason,
                    order_id = %quarantined.fill.order_id,
                    "quarantined_unmanaged_fill"
                );
                self.delivery_failure.get_or_insert_with(|| {
                    format!(
                        "accepted fill was quarantined order_id={} reason={}",
                        quarantined.fill.order_id, quarantined.reason,
                    )
                });
            }
        }
    }

    pub(super) async fn drive(&mut self) {
        self.start_ready_attempts();
        self.start_ready_transfers();
        let retry_deadline = self
            .groups
            .iter()
            .filter(|(key, _)| !self.active_keys.contains(key))
            .filter_map(|(_, group)| group.next_attempt_at)
            .min();
        tokio::select! {
            attempt = self.attempts.next(), if !self.attempts.is_empty() => {
                if let Some((key, result)) = attempt {
                    self.finish_attempt(key, result);
                }
            }
            transfer = self.transfers.next(), if !self.transfers.is_empty() => {
                if let Some(outcome) = transfer {
                    self.finish_transfer(outcome);
                }
            }
            _ = async {
                if let Some(deadline) = retry_deadline {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                }
            }, if retry_deadline.is_some() => {},
            else => std::future::pending::<()>().await,
        }
    }

    fn start_ready_attempts(&mut self) {
        let now = Instant::now();
        let exec = &self.exec;
        let active_keys = &mut self.active_keys;
        let attempts = &mut self.attempts;
        for (&key, group) in &self.groups {
            if group
                .next_attempt_at
                .is_some_and(|deadline| deadline <= now)
                && active_keys.insert(key)
            {
                let exec = exec.clone();
                attempts.push(
                    async move {
                        let result = exec
                            .observe_finalized_sell_transaction(key.transaction_hash)
                            .await;
                        (key, result)
                    }
                    .boxed(),
                );
            }
        }
    }

    fn finish_attempt(
        &mut self,
        key: TransactionKey,
        result: anyhow::Result<Option<alloy::rpc::types::TransactionReceipt>>,
    ) {
        self.active_keys.remove(&key);
        match result {
            Ok(None) => {
                if let Some(group) = self.groups.get_mut(&key) {
                    group.next_attempt_at = Some(Instant::now() + FINALITY_RETRY_INTERVAL);
                }
            }
            Ok(Some(receipt)) => {
                let Some(group) = self.groups.remove(&key) else {
                    return;
                };
                let mut unresolved = Vec::new();
                for mut obligation in group.obligations {
                    if let Err(error) = self
                        .exec
                        .apply_finalized_sell_receipt(&mut obligation.fill, &receipt)
                    {
                        tracing::error!(
                            target: "polymarket.user_ws",
                            order_id = %obligation.fill.order_id,
                            error = %error,
                            "confirmed_sell_receipt_rejected"
                        );
                        self.delivery_failure.get_or_insert_with(|| {
                            format!(
                                "accepted sell receipt rejected order_id={}: {error:#}",
                                obligation.fill.order_id,
                            )
                        });
                        if obligation.fill.side == crate::portfolio::FillSide::Buy {
                            unresolved.push(obligation);
                        } else {
                            self.finish_receipt_key(&obligation.fill_key);
                        }
                        continue;
                    }
                    match obligation.context.normalize_fill_with_matched_trade(
                        obligation.fill,
                        obligation.matched_trade.clone(),
                    ) {
                        FillClassification::Normalized(fill) => {
                            self.queue_fill(fill, obligation.context, Some(obligation.fill_key))
                        }
                        FillClassification::Quarantined(quarantined) => {
                            tracing::warn!(
                                target: "polymarket.user_ws",
                                reason = quarantined.reason,
                                order_id = %quarantined.fill.order_id,
                                "quarantined_unmanaged_fill"
                            );
                            self.delivery_failure.get_or_insert_with(|| {
                                format!(
                                    "accepted sell fill was quarantined order_id={} reason={}",
                                    quarantined.fill.order_id, quarantined.reason,
                                )
                            });
                            if quarantined.fill.side == crate::portfolio::FillSide::Buy {
                                unresolved.push(ReceiptObligation {
                                    fill_key: obligation.fill_key,
                                    fill: quarantined.fill,
                                    context: obligation.context,
                                    matched_trade: obligation.matched_trade,
                                });
                            } else {
                                self.finish_receipt_key(&obligation.fill_key);
                            }
                        }
                    }
                }
                if !unresolved.is_empty() {
                    self.groups.insert(
                        key,
                        TransactionGroup {
                            obligations: unresolved,
                            next_attempt_at: None,
                        },
                    );
                }
            }
            Err(error) => {
                let Some(group) = self.groups.remove(&key) else {
                    return;
                };
                let mut unresolved = Vec::new();
                for obligation in group.obligations {
                    tracing::error!(
                        target: "polymarket.user_ws",
                        order_id = %obligation.fill.order_id,
                        error = %error,
                        "confirmed_sell_receipt_definitively_rejected"
                    );
                    if obligation.fill.side == crate::portfolio::FillSide::Buy {
                        self.delivery_failure.get_or_insert_with(|| {
                            format!("confirmed BUY receipt remains unresolved: {error:#}")
                        });
                        unresolved.push(obligation);
                    } else {
                        self.finish_receipt_key(&obligation.fill_key);
                    }
                }
                if !unresolved.is_empty() {
                    self.groups.insert(
                        key,
                        TransactionGroup {
                            obligations: unresolved,
                            next_attempt_at: None,
                        },
                    );
                }
            }
        }
    }

    fn queue_fill(
        &mut self,
        normalized: NormalizedStrategyFill,
        context: OrderFillContext,
        receipt_key: Option<(String, String)>,
    ) {
        let normalized = normalized
            .with_timing_capture(self.capture.clone())
            .with_order_context_owner(context);
        let fill = normalized.fill();
        if fill.canonical_fill_id().is_none() {
            tracing::error!(
                target: "polymarket.user_ws",
                order_id = %fill.order_id,
                "canonical_fill_delivery_rejected"
            );
            self.delivery_failure.get_or_insert_with(|| {
                format!(
                    "accepted fill has no canonical delivery identity order_id={}",
                    fill.order_id,
                )
            });
            if let Some(key) = receipt_key.as_ref() {
                self.finish_receipt_key(key);
            }
            return;
        }
        let action = ActionEvent::Fill {
            ts_ms: fill.ts_ms,
            order_id: fill.order_id.clone(),
            trade_id: fill.trade_id.clone(),
            transaction_hash: fill.transaction_hash.clone(),
            token: fill.token.clone(),
            side: fill.side.as_str().to_string(),
            px: fill.px,
            size: fill.size,
            fee: fill.fee,
        };
        let ready = ReadyFill {
            normalized,
            action,
            receipt_key,
        };
        if self.transfers.len() < self.max_transfers {
            self.start_transfer(ready);
        } else {
            self.ready_fills.push_back(ready);
        }
    }

    fn start_ready_transfers(&mut self) {
        while self.transfers.len() < self.max_transfers {
            let Some(ready) = self.ready_fills.pop_front() else {
                break;
            };
            self.start_transfer(ready);
        }
    }

    fn start_transfer(&mut self, ready: ReadyFill) {
        match self.fills_tx.try_send(ready.normalized) {
            Ok(()) => self.finish_transfer(TransferOutcome {
                action: ready.action,
                receipt_key: ready.receipt_key,
                result: Ok(()),
            }),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.finish_transfer(TransferOutcome {
                    action: ready.action,
                    receipt_key: ready.receipt_key,
                    result: Err("portfolio fill input is closed".to_string()),
                });
            }
            Err(mpsc::error::TrySendError::Full(normalized)) => {
                let fills_tx = self.fills_tx.clone();
                self.transfers.push(
                    async move {
                        let result = match fills_tx.reserve_owned().await {
                            Ok(permit) => {
                                permit.send(normalized);
                                Ok(())
                            }
                            Err(_) => Err("portfolio fill input is closed".to_string()),
                        };
                        TransferOutcome {
                            action: ready.action,
                            receipt_key: ready.receipt_key,
                            result,
                        }
                    }
                    .boxed(),
                );
            }
        }
    }

    fn finish_transfer(&mut self, outcome: TransferOutcome) {
        match outcome.result {
            Ok(()) => self.obs.emit_action(outcome.action),
            Err(error) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    error,
                    "fill_handoff_failed"
                );
                self.delivery_failure.get_or_insert(error);
            }
        }
        if let Some(key) = outcome.receipt_key.as_ref() {
            self.finish_receipt_key(key);
        }
    }

    fn finish_receipt_key(&mut self, key: &(String, String)) {
        self.pending_identities.remove(key);
        self.retained_receipts = self.retained_receipts.saturating_sub(1);
    }
}

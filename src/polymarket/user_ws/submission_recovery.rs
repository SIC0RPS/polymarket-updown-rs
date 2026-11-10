use alloy::primitives::B256;
use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt as _;
use polymarket_client_sdk::clob::ws::types::response::TradeMessage;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::execution::supervisor::ExecutionOrderEvent;
use crate::orders::OrderUpdateEvent;
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::order_context::OrderFillContext;
use crate::portfolio::append_observed_trade_event;
use crate::portfolio::FillSide;
use crate::trade_session_ledger::{
    canonical_buy_by_venue_order, canonical_managed_sell_by_venue_order, CanonicalCtfEvent,
    CtfTradeStatus, FinancialAdmissionDisposition, ManagedSellReceiptIdentity,
    ObservedCtfTradeFact,
};

pub(super) type ActiveTradeAuthorizations =
    FuturesUnordered<BoxFuture<'static, AuthorizedTradeMessage>>;
pub(super) type ActiveOrderHandoffs =
    FuturesUnordered<BoxFuture<'static, (String, Result<(), String>)>>;

pub(super) struct AuthorizedTradeObservation {
    pub(super) observation: UnboundCtfTradeObservation,
    pub(super) context: OrderFillContext,
    pub(super) matched_trade: ObservedCtfTradeFact,
}

pub(super) struct AuthorizedTradeMessage {
    pub(super) trade: TradeMessage,
    pub(super) observations: Vec<AuthorizedTradeObservation>,
    pub(super) failure: Option<String>,
}

pub(super) fn authorize_trade_message(
    trade: TradeMessage,
    observations: Vec<UnboundCtfTradeObservation>,
    chain_id: u64,
    orders_tx: mpsc::Sender<ExecutionOrderEvent>,
    shutdown: CancellationToken,
) -> BoxFuture<'static, AuthorizedTradeMessage> {
    async move {
        let transaction_hash = trade.transaction_hash;
        let (observations, failure) = authorize_trade_observations(
            observations,
            chain_id,
            transaction_hash,
            &orders_tx,
            &shutdown,
        )
        .await;
        AuthorizedTradeMessage {
            trade,
            observations,
            failure,
        }
    }
    .boxed()
}

async fn authorize_trade_observations(
    observations: Vec<UnboundCtfTradeObservation>,
    chain_id: u64,
    transaction_hash: Option<B256>,
    orders_tx: &mpsc::Sender<ExecutionOrderEvent>,
    shutdown: &CancellationToken,
) -> (Vec<AuthorizedTradeObservation>, Option<String>) {
    let mut failure = None;
    let mut authorized = Vec::with_capacity(observations.len());
    'observations: for observation in observations {
        if observation.status() == CtfTradeStatus::Confirmed {
            loop {
                match recorded_observation_disposition(chain_id, transaction_hash, &observation) {
                    Ok(Some(FinancialAdmissionDisposition::Admitted)) => continue 'observations,
                    Ok(Some(FinancialAdmissionDisposition::DefinitivelyRejected)) => {
                        tracing::error!(
                            target: "polymarket.user_ws",
                            order_id = observation.order_id(),
                            trade_id = observation.trade_id(),
                            "recorded_sell_receipt_definitively_rejected"
                        );
                        continue 'observations;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::error!(
                            target: "polymarket.user_ws",
                            order_id = observation.order_id(),
                            trade_id = observation.trade_id(),
                            error = %error,
                            "canonical_sell_receipt_lookup_unresolved"
                        );
                        if shutdown.is_cancelled() {
                            failure.get_or_insert_with(|| format!("canonical sell receipt lookup unresolved at shutdown: {error:#}"));
                            continue 'observations;
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }
        let (ack, wait) = oneshot::channel();
        let order_id = observation.order_id().to_string();
        if let Err(error) = send_execution_event(
            ExecutionOrderEvent::UnboundTrade {
                observation: observation.clone(),
                ack,
            },
            orders_tx,
        )
        .await
        {
            tracing::error!(
                target: "polymarket.user_ws",
                order_id,
                error,
                "trade_submission_identity_handoff_failed"
            );
            failure.get_or_insert(error);
            continue;
        }
        let context = match wait.await {
            Ok(Ok(context)) => context,
            Ok(Err(error)) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    order_id,
                    error,
                    "trade_submission_identity_rejected"
                );
                failure.get_or_insert(error);
                continue;
            }
            Err(_) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    order_id,
                    "execution_supervisor_trade_recovery_ack_closed"
                );
                failure.get_or_insert_with(|| {
                    "execution supervisor trade authorization acknowledgment closed".to_string()
                });
                continue;
            }
        };
        let matched_trade = match context.bind_trade_observation(&observation) {
            Ok(fact) => fact,
            Err(error) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    order_id,
                    error = %error,
                    "trade_observation_identity_rejected"
                );
                continue;
            }
        };
        if observation.status() == CtfTradeStatus::Confirmed {
            authorized.push(AuthorizedTradeObservation {
                observation,
                context,
                matched_trade,
            });
            continue;
        }
        let event = match CanonicalCtfEvent::from_observed_trade(matched_trade) {
            Ok(event) => event,
            Err(error) => {
                tracing::error!(
                    target: "polymarket.user_ws",
                    order_id,
                    error = %error,
                    "trade_observation_status_rejected"
                );
                continue;
            }
        };
        loop {
            match append_observed_trade_event(event.clone()) {
                Ok(()) => break,
                Err(error) => {
                    tracing::error!(
                        target: "polymarket.user_ws",
                        order_id,
                        error = %error,
                        "trade_observation_persistence_failed_retained"
                    );
                    if shutdown.is_cancelled() {
                        failure.get_or_insert_with(|| {
                            format!(
                                "trade observation persistence unresolved at shutdown: {error:#}"
                            )
                        });
                        continue 'observations;
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }
    (authorized, failure)
}

fn recorded_observation_disposition(
    chain_id: u64,
    transaction_hash: Option<B256>,
    observation: &UnboundCtfTradeObservation,
) -> Result<Option<FinancialAdmissionDisposition>> {
    if observation.side() == FillSide::Buy {
        let Some(recorded) =
            canonical_buy_by_venue_order(observation.trade_id(), observation.order_id())?
        else {
            return Ok(None);
        };
        let ManagedSellReceiptIdentity::ExchangeV2 {
            chain_id: recorded_chain_id,
            transaction_hash: recorded_transaction_hash,
            ..
        } = &recorded.fact.receipt_identity
        else {
            return Ok(None);
        };
        let exact = *recorded_chain_id == chain_id
            && transaction_hash.is_some()
            && transaction_hash == recorded_transaction_hash.parse::<B256>().ok()
            && recorded.fact.qty_units_6dp == observation.owned_size_units_6dp()
            && recorded.fact.matched_trade.as_ref().is_some_and(|fact| {
                observation.matches_recorded_fact(fact)
                    && recorded.fact.confirmed_at_ms == fact.observed_at_ms
            });
        return Ok(exact.then_some(recorded.disposition));
    }
    let Some(recorded) =
        canonical_managed_sell_by_venue_order(observation.trade_id(), observation.order_id())?
    else {
        return Ok(None);
    };
    let ManagedSellReceiptIdentity::ExchangeV2 {
        chain_id: recorded_chain_id,
        transaction_hash: recorded_transaction_hash,
        ..
    } = &recorded.fact.receipt_identity
    else {
        return Ok(None);
    };
    let recorded_transaction_hash = recorded_transaction_hash.parse::<B256>().ok();
    let exact = *recorded_chain_id == chain_id
        && transaction_hash.is_some()
        && transaction_hash == recorded_transaction_hash
        && recorded.fact.qty_units_6dp == observation.owned_size_units_6dp()
        && observation
            .owned_gross_proceeds_units_6dp()
            .and_then(|gross| i128::try_from(gross).ok())
            == Some(recorded.fact.gross_proceeds_units_6dp)
        && observation.owned_fee_units_6dp() == Some(recorded.fact.fee_units_6dp)
        && recorded.fact.matched_trade.as_ref().is_some_and(|fact| {
            observation.matches_recorded_fact(fact)
                && recorded.fact.confirmed_at_ms == fact.observed_at_ms
        });
    Ok(exact.then_some(recorded.disposition))
}

pub(super) fn handoff_order_update(
    update: OrderUpdateEvent,
    context: Option<OrderFillContext>,
    orders_tx: &mpsc::Sender<ExecutionOrderEvent>,
) -> Option<BoxFuture<'static, (String, Result<(), String>)>> {
    let order_id = update.order_id.clone();
    let event = ExecutionOrderEvent::Update { update, context };
    match orders_tx.try_send(event) {
        Ok(()) => None,
        Err(mpsc::error::TrySendError::Closed(_)) => Some(
            async move {
                (
                    order_id,
                    Err("execution supervisor order input is closed".to_string()),
                )
            }
            .boxed(),
        ),
        Err(mpsc::error::TrySendError::Full(event)) => {
            let orders_tx = orders_tx.clone();
            Some(
                async move {
                    let result = match orders_tx.reserve_owned().await {
                        Ok(permit) => {
                            permit.send(event);
                            Ok(())
                        }
                        Err(_) => Err("execution supervisor order input is closed".to_string()),
                    };
                    (order_id, result)
                }
                .boxed(),
            )
        }
    }
}

async fn send_execution_event(
    event: ExecutionOrderEvent,
    orders_tx: &mpsc::Sender<ExecutionOrderEvent>,
) -> std::result::Result<(), String> {
    match orders_tx.try_send(event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err("execution supervisor order input is closed".to_string())
        }
        Err(mpsc::error::TrySendError::Full(event)) => {
            let permit = orders_tx
                .clone()
                .reserve_owned()
                .await
                .map_err(|_| "execution supervisor order input is closed".to_string())?;
            permit.send(event);
            Ok(())
        }
    }
}

use std::time::Duration;

use polymarket_hft::execution::http_support::advance_capped_full_jitter_backoff_ms;
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::risk::RiskSnapshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const FREEZE_CANCEL_RETRY_BASE_BACKOFF_MS: u64 = 1_000;
const FREEZE_CANCEL_RETRY_MAX_BACKOFF_MS: u64 = 30_000;

pub(crate) fn install_signal_incident_channel() -> anyhow::Result<watch::Receiver<u64>> {
    let (incident_tx, incident_rx) = watch::channel(0_u64);
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::spawn(async move {
        let mut incident = 0_u64;
        loop {
            #[cfg(unix)]
            let signal = tokio::select! {
                _ = interrupt.recv() => "sigint",
                _ = terminate.recv() => "sigterm",
            };
            #[cfg(not(unix))]
            let signal = wait_for_shutdown_signal().await;
            incident = incident.saturating_add(1);
            tracing::warn!(target: "app.shutdown", signal, incident, "live_signal_contained");
            incident_tx.send_replace(incident);
        }
    });
    Ok(incident_rx)
}

pub(crate) fn spawn_signal_task(
    mut incident_rx: watch::Receiver<u64>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = incident_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    incident_rx.borrow_and_update();
                }
            }
        }
    })
}

pub(crate) fn spawn_freeze_cancel_task(
    exec: ExecutionClient,
    mut freeze_rx: watch::Receiver<RiskSnapshot>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut prev_trading_allowed = freeze_rx.borrow().trading_allowed;
        if !prev_trading_allowed {
            let reason = freeze_rx
                .borrow()
                .frozen_reason()
                .unwrap_or_else(|| "frozen".to_string());
            cancel_all_deterministic(&exec, format!("risk_freeze_initial reason={reason}")).await;
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = freeze_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let snapshot = freeze_rx.borrow().clone();
                    if prev_trading_allowed && !snapshot.trading_allowed {
                        let reason = snapshot
                            .frozen_reason()
                            .unwrap_or_else(|| "frozen".to_string());
                        cancel_all_deterministic(
                            &exec,
                            format!("risk_freeze_transition reason={reason}"),
                        )
                        .await;
                    }
                    prev_trading_allowed = snapshot.trading_allowed;
                }
            }
        }
    })
}

pub(crate) async fn cancel_all_deterministic(exec: &ExecutionClient, trigger: String) {
    let mut backoff_ms = FREEZE_CANCEL_RETRY_BASE_BACKOFF_MS;
    let mut attempt = 0_usize;
    loop {
        attempt = attempt.saturating_add(1);
        match exec.cancel_all_orders().await {
            Ok(ack) => {
                let canceled_count = ack.canceled.len();
                let pending_count = ack.not_canceled.len();
                tracing::info!(
                    target: "app.shutdown",
                    trigger,
                    attempt,
                    canceled = canceled_count,
                    not_canceled = pending_count,
                    "cancel_all_ok"
                );

                for (order_id, reason) in &ack.not_canceled {
                    tracing::warn!(
                        target: "app.shutdown",
                        trigger,
                        order_id,
                        reason,
                        "cancel_all_pending"
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    target: "app.shutdown",
                    trigger,
                    attempt,
                    error = %err,
                    "cancel_all_err"
                );
            }
        }

        match exec.polymarket_open_orders().await {
            Ok(open_orders) if open_orders.is_empty() => {
                tracing::info!(target: "app.shutdown", trigger, attempt, "remote_orders_clear");
                return;
            }
            Ok(open_orders) => tracing::warn!(
                target: "app.shutdown",
                trigger,
                attempt,
                open_orders = open_orders.len(),
                "remote_orders_remain"
            ),
            Err(err) => tracing::error!(
                target: "app.shutdown",
                trigger,
                attempt,
                error = %err,
                "remote_orders_query_failed"
            ),
        }

        let sleep_backoff_ms = advance_capped_full_jitter_backoff_ms(
            attempt,
            &mut backoff_ms,
            FREEZE_CANCEL_RETRY_MAX_BACKOFF_MS,
        )
        .max(FREEZE_CANCEL_RETRY_BASE_BACKOFF_MS);
        tokio::time::sleep(Duration::from_millis(sleep_backoff_ms)).await;
    }
}

#[cfg(not(unix))]
pub(crate) async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut term = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return "ctrl_c";
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => "ctrl_c",
            _ = term.recv() => "sigterm",
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "ctrl_c"
    }
}

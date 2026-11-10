use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::observability::Observability;
use crate::polymarket::state::PolymarketOddsState;

use super::closed_trades::{RedeemCloseRequest, SettlementInventoryClearance};
use super::fills::{FillAdmission, NormalizedStrategyFill};
use super::hints::build_token_expiry_map;
use super::state::{now_ms, ExpectedCtfPosition, PortfolioState};

#[derive(Clone)]
pub struct PortfolioCloseHandle {
    state_tx: watch::Sender<PortfolioState>,
    state_rx: watch::Receiver<PortfolioState>,
}

#[derive(Clone)]
pub struct PortfolioHydrationHandle {
    state_tx: watch::Sender<PortfolioState>,
    state_rx: watch::Receiver<PortfolioState>,
    odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
}

impl PortfolioHydrationHandle {
    pub fn snapshot(&self) -> PortfolioState {
        self.state_rx.borrow().clone()
    }

    pub fn hydrate_expected_ctf_balances_with_persist<F>(
        &self,
        expected_positions: &[ExpectedCtfPosition],
        onchain_position_balances: &HashMap<String, u128>,
        persist: F,
    ) -> Result<usize>
    where
        F: FnOnce() -> Result<()>,
    {
        let observed_at_ms = now_ms();
        let token_expiry = match self.odds_rx.as_ref() {
            Some(rx) => build_token_expiry_map(rx.borrow().as_ref()),
            None => HashMap::new(),
        };
        let mut persist = Some(persist);
        let mut hydrated: Result<usize> = Err(anyhow::anyhow!(
            "confirmed split portfolio mutation did not execute"
        ));
        self.state_tx.send_modify(|state| {
            hydrated = (|| {
                let mut next = state.clone();
                if !token_expiry.is_empty() {
                    next.ingest_expiry(&token_expiry);
                }
                let count = next.hydrate_expected_ctf_balances(
                    expected_positions,
                    onchain_position_balances,
                    observed_at_ms,
                )?;
                persist
                    .take()
                    .expect("confirmed split persistence callback consumed")()?;
                *state = next;
                Ok(count)
            })();
        });
        hydrated
    }
}

impl PortfolioCloseHandle {
    pub fn snapshot(&self) -> PortfolioState {
        self.state_rx.borrow().clone()
    }

    pub fn close_redeemed_positions_if_revision<F>(
        &self,
        requests: &[RedeemCloseRequest],
        expected_revision: u64,
        expected_clearances: &[SettlementInventoryClearance],
        persist: F,
    ) -> Result<(PortfolioState, Vec<SettlementInventoryClearance>)>
    where
        F: FnOnce(&[SettlementInventoryClearance]) -> Result<()>,
    {
        let mut persist = Some(persist);
        let mut result = Err(anyhow::anyhow!(
            "terminal portfolio conditional mutation did not execute"
        ));
        self.state_tx.send_modify(|state| {
            result = (|| {
                anyhow::ensure!(
                    state.revision == expected_revision,
                    "terminal portfolio revision changed expected={} actual={}",
                    expected_revision,
                    state.revision
                );
                let mut next = state.clone();
                let clearances = next.close_redeemed_positions(requests)?;
                anyhow::ensure!(
                    clearances == expected_clearances,
                    "terminal exact clearances changed at conditional mutation"
                );
                persist
                    .take()
                    .expect("terminal persistence callback consumed")(&clearances)?;
                *state = next;
                Ok(clearances)
            })();
        });
        Ok((self.state_rx.borrow().clone(), result?))
    }
}

pub struct PortfolioRuntime {
    state_tx: watch::Sender<PortfolioState>,
    state_rx: watch::Receiver<PortfolioState>,
    odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
    task: JoinHandle<Result<()>>,
}

const INITIAL_FINANCIAL_RETRY_DELAY: Duration = Duration::from_millis(250);
const MAX_FINANCIAL_RETRY_DELAY: Duration = Duration::from_secs(5);

struct PendingFillAdmission {
    fill: NormalizedStrategyFill,
    retry_at: tokio::time::Instant,
    retry_delay: Duration,
}

impl PendingFillAdmission {
    fn immediate(fill: NormalizedStrategyFill) -> Self {
        Self {
            fill,
            retry_at: tokio::time::Instant::now(),
            retry_delay: INITIAL_FINANCIAL_RETRY_DELAY,
        }
    }
}

fn apply_one_pending_fill(
    state_tx: &watch::Sender<PortfolioState>,
    pending: &mut Option<PendingFillAdmission>,
) {
    let Some(attempt) = pending.take() else {
        return;
    };
    let PendingFillAdmission {
        fill, retry_delay, ..
    } = attempt;
    let mut outcome = None;
    let _ = state_tx.send_if_modified(|state| {
        let prior_revision = state.revision;
        let prior_fill_sequence = state.next_fill_seq;
        outcome = Some(state.apply_fill(fill));
        state.revision != prior_revision || state.next_fill_seq != prior_fill_sequence
    });
    match outcome.expect("portfolio fill admission callback did not execute") {
        FillAdmission::Admitted
        | FillAdmission::DirectBuyAdmitted(_)
        | FillAdmission::DefinitivelyRejected => {}
        FillAdmission::Unresolved { fill, error } => {
            tracing::error!(
                target: "portfolio.runtime",
                error = %error,
                order_id = %fill.fill().order_id,
                "portfolio_financial_admission_unresolved"
            );
            let retry_at = tokio::time::Instant::now() + retry_delay;
            let retry_delay = retry_delay.saturating_mul(2).min(MAX_FINANCIAL_RETRY_DELAY);
            *pending = Some(PendingFillAdmission {
                fill,
                retry_at,
                retry_delay,
            });
        }
    }
}

impl PortfolioRuntime {
    pub fn start(
        obs: Observability,
        mut fills_rx: mpsc::Receiver<NormalizedStrategyFill>,
        odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
        shutdown: CancellationToken,
    ) -> Self {
        let (state_tx, state_rx) = watch::channel(PortfolioState::default());

        let mut reset_rx = obs.portfolio_reset_signal();

        let state_tx_task = state_tx.clone();
        let task = tokio::spawn(async move {
            let mut pending_fill: Option<PendingFillAdmission> = None;
            loop {
                let retry_at = pending_fill.as_ref().map(|pending| pending.retry_at);
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        fills_rx.close();
                        break;
                    },
                    _ = async {
                        match retry_at {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {
                        apply_one_pending_fill(&state_tx_task, &mut pending_fill);
                    }
                    next = fills_rx.recv(), if pending_fill.is_none() => {
                        let Some(fill) = next else { break };
                        pending_fill = Some(PendingFillAdmission::immediate(fill));
                        apply_one_pending_fill(&state_tx_task, &mut pending_fill);
                    }
                    Ok(()) = reset_rx.changed() => {
                        while fills_rx.try_recv().is_ok() {}
                        state_tx_task.send_modify(|s| {
                            *s = PortfolioState::default();
                        });
                        tracing::info!("portfolio reset by autotuner signal");
                    }
                }
            }
            if let Some(pending) = &pending_fill {
                tokio::time::sleep_until(pending.retry_at).await;
                apply_one_pending_fill(&state_tx_task, &mut pending_fill);
            }
            loop {
                anyhow::ensure!(
                    pending_fill.is_none(),
                    "portfolio shutdown has unresolved financial admission order_id={} buffered_fills={}",
                    pending_fill.as_ref().map(|pending| pending.fill.fill().order_id.as_str()).unwrap_or(""),
                    fills_rx.len()
                );
                let Some(fill) = fills_rx.recv().await else {
                    break;
                };
                pending_fill = Some(PendingFillAdmission::immediate(fill));
                apply_one_pending_fill(&state_tx_task, &mut pending_fill);
            }
            Ok(())
        });

        Self {
            state_tx,
            state_rx,
            odds_rx,
            task,
        }
    }

    pub fn state(&self) -> watch::Receiver<PortfolioState> {
        self.state_rx.clone()
    }

    pub fn close_handle(&self) -> PortfolioCloseHandle {
        PortfolioCloseHandle {
            state_tx: self.state_tx.clone(),
            state_rx: self.state_rx.clone(),
        }
    }

    pub fn hydration_handle(&self) -> PortfolioHydrationHandle {
        PortfolioHydrationHandle {
            state_tx: self.state_tx.clone(),
            state_rx: self.state_rx.clone(),
            odds_rx: self.odds_rx.clone(),
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        self.task.await.context("join portfolio runtime")?
    }
}

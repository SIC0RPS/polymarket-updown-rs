use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use polymarket_hft::capture::events::{
    ExternalAccountAuditCapture, ExternalClobAuditOutcome, ExternalWalletAuditOutcome,
    ReplayRiskSnapshotCapture,
};
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::observability::Observability;
use polymarket_hft::portfolio::PortfolioState;
use polymarket_hft::risk::recon::{
    fetch_clob_balance_allowance, fetch_wallet_cash_balances, WalletCashBalance,
};
use polymarket_hft::risk::{AccountEquitySnapshot, RiskHandle, RiskSnapshot};
use polymarket_hft::trade_session_ledger::{
    live_accounting_receiver, reconcile_live_accounting, LiveAccountingGeneration,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::app::bootstrap::now_ms;

const ACCOUNT_AUDIT_INTERVAL: Duration = Duration::from_secs(15);

type ExternalAccountAuditFuture = Pin<Box<dyn Future<Output = ExternalAccountAuditResult> + Send>>;

struct ExternalAccountAuditResult {
    trigger: &'static str,
    wallet_cash: anyhow::Result<WalletCashBalance>,
    clob_admission: anyhow::Result<(f64, f64)>,
}

pub(super) async fn spawn_capture_forwarding_tasks(
    capture: MaybeCapture,
    obs: Observability,
    risk: RiskHandle,
    risk_rx: watch::Receiver<RiskSnapshot>,
    portfolio_rx: watch::Receiver<PortfolioState>,
    exec: ExecutionClient,
    account_http: reqwest::Client,
    account_rpc_url: String,
    odds_rx: Option<watch::Receiver<Arc<bbwick_engine::PolymarketOddsState>>>,
    shutdown: CancellationToken,
) -> anyhow::Result<(
    JoinHandle<()>,
    JoinHandle<anyhow::Result<()>>,
    mpsc::Sender<()>,
)> {
    let (settlement_refresh_tx, settlement_refresh_rx) = mpsc::channel(1);
    let accounting_rx = live_accounting_receiver();
    let initial_audit = start_external_account_audit(
        capture.clone(),
        risk.clone(),
        portfolio_rx.clone(),
        exec.clone(),
        account_http.clone(),
        account_rpc_url.clone(),
        "startup_audit",
    )
    .await;
    anyhow::ensure!(
        complete_external_account_projection(&risk, &portfolio_rx, initial_audit),
        "initial complete account observation failed"
    );
    anyhow::ensure!(
        publish_current_accounting_generation(
            &obs,
            &risk,
            &portfolio_rx,
            &accounting_rx,
            odds_rx.as_ref(),
        ),
        "initial coherent account valuation unavailable"
    );
    publish_coherent_risk_capture(&capture, &risk);
    let risk_fwd_task = tokio::spawn({
        let obs = obs.clone();
        let shutdown = shutdown.clone();
        let mut risk_rx = risk_rx;
        async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    changed = risk_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        obs.set_risk(risk_rx.borrow().clone());
                    }
                }
            }
        }
    });

    let portfolio_fwd_task = tokio::spawn({
        let capture = capture.clone();
        let shutdown = shutdown.clone();
        let mut portfolio_rx = portfolio_rx;
        let mut odds_rx = odds_rx;
        let mut accounting_rx = accounting_rx;
        let mut settlement_refresh_rx = settlement_refresh_rx;
        async move {
            let mut account_audit = tokio::time::interval_at(
                tokio::time::Instant::now() + ACCOUNT_AUDIT_INTERVAL,
                ACCOUNT_AUDIT_INTERVAL,
            );
            account_audit.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut external_audit: Option<ExternalAccountAuditFuture> = None;
            let mut settlement_refresh_open = true;
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    changed = portfolio_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if publish_current_accounting_generation(
                            &obs,
                            &risk,
                            &portfolio_rx,
                            &accounting_rx,
                            odds_rx.as_ref(),
                        ) {
                            publish_coherent_risk_capture(&capture, &risk);
                        }
                    }
                    changed = accounting_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        if publish_current_accounting_generation(
                            &obs,
                            &risk,
                            &portfolio_rx,
                            &accounting_rx,
                            odds_rx.as_ref(),
                        ) {
                            publish_coherent_risk_capture(&capture, &risk);
                        }
                    }
                    changed = async {
                        match odds_rx.as_mut() {
                            Some(receiver) => receiver.changed().await,
                            None => std::future::pending::<Result<(), watch::error::RecvError>>().await,
                        }
                    } => {
                        match changed {
                            Ok(()) => {
                                if publish_current_accounting_generation(
                                    &obs,
                                    &risk,
                                    &portfolio_rx,
                                    &accounting_rx,
                                    odds_rx.as_ref(),
                                ) {
                                    publish_coherent_risk_capture(&capture, &risk);
                                }
                            }
                            Err(_) => odds_rx = None,
                        }
                    }
                    audit = async {
                        match external_audit.as_mut() {
                            Some(audit) => audit.as_mut().await,
                            None => std::future::pending::<ExternalAccountAuditResult>().await,
                        }
                    } => {
                        external_audit = None;
                        let _ = complete_external_account_projection(&risk, &portfolio_rx, audit);
                    }
                    _ = account_audit.tick(), if external_audit.is_none() => {
                        external_audit = Some(start_external_account_audit(
                            capture.clone(),
                            risk.clone(),
                            portfolio_rx.clone(),
                            exec.clone(),
                            account_http.clone(),
                            account_rpc_url.clone(),
                            "periodic_audit",
                        ));
                    }
                    refresh = settlement_refresh_rx.recv(), if settlement_refresh_open && external_audit.is_none() => {
                        let Some(()) = refresh else {
                            if !shutdown.is_cancelled() {
                                tracing::error!(
                                    target: "app.live_runtime.account_projection",
                                    "settlement_account_projection_channel_closed"
                                );
                            }
                            settlement_refresh_open = false;
                            continue;
                        };
                        external_audit = Some(start_external_account_audit(
                            capture.clone(),
                            risk.clone(),
                            portfolio_rx.clone(),
                            exec.clone(),
                            account_http.clone(),
                            account_rpc_url.clone(),
                            "confirmed_prior_session_settlement",
                        ));
                    }
                }
            }
            let mut completion = Ok(());
            if let Some(audit) = external_audit.take() {
                if !complete_external_account_projection(&risk, &portfolio_rx, audit.await) {
                    completion = Err(anyhow::anyhow!(
                        "owned external account audit unresolved at shutdown"
                    ));
                }
            }
            settlement_refresh_rx.close();
            while let Some(()) = settlement_refresh_rx.recv().await {
                let audit = start_external_account_audit(
                    capture.clone(),
                    risk.clone(),
                    portfolio_rx.clone(),
                    exec.clone(),
                    account_http.clone(),
                    account_rpc_url.clone(),
                    "confirmed_prior_session_settlement",
                )
                .await;
                if !complete_external_account_projection(&risk, &portfolio_rx, audit) {
                    completion = Err(anyhow::anyhow!(
                        "settlement account audit unresolved at shutdown"
                    ));
                }
            }
            if !publish_current_accounting_generation(
                &obs,
                &risk,
                &portfolio_rx,
                &accounting_rx,
                odds_rx.as_ref(),
            ) {
                completion = Err(anyhow::anyhow!(
                    "final coherent financial publication unavailable"
                ));
            }
            publish_coherent_risk_capture(&capture, &risk);
            obs.set_risk(risk.current_snapshot());
            completion
        }
    });

    Ok((risk_fwd_task, portfolio_fwd_task, settlement_refresh_tx))
}

fn publish_current_accounting_generation(
    obs: &Observability,
    risk: &RiskHandle,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    accounting_rx: &watch::Receiver<Arc<LiveAccountingGeneration>>,
    odds_rx: Option<&watch::Receiver<Arc<bbwick_engine::PolymarketOddsState>>>,
) -> bool {
    let portfolio_guard = portfolio_rx.borrow();
    let accounting_guard = accounting_rx.borrow();
    let portfolio = (*portfolio_guard).clone();
    let accounting = Arc::clone(&*accounting_guard);
    drop(accounting_guard);
    drop(portfolio_guard);
    let odds = odds_rx.map(|rx| rx.borrow().clone());
    let valued_at_ms = now_ms();
    publish_accounting_generation(
        obs,
        risk,
        portfolio,
        accounting.as_ref(),
        odds,
        valued_at_ms,
    )
}

fn publish_accounting_generation(
    obs: &Observability,
    risk: &RiskHandle,
    portfolio: PortfolioState,
    accounting: &LiveAccountingGeneration,
    odds: Option<Arc<bbwick_engine::PolymarketOddsState>>,
    valued_at_ms: u64,
) -> bool {
    let projection = portfolio.canonical_accounting_projection(
        Some(accounting.financial.financial_view()),
        Some(accounting.confirmed_portfolio_receipts.as_ref()),
        odds.as_deref(),
        valued_at_ms,
    );
    if !projection.is_complete() {
        tracing::error!(
            target: "app.live_runtime.account_projection",
            portfolio_revision = projection.portfolio_revision,
            canonical_revision = ?projection.canonical_revision,
            odds_revision = ?projection.odds_revision,
            accounting_generation = accounting.generation,
            portfolio_updated_at_ms = projection.portfolio_updated_at_ms,
            odds_updated_at_ms = ?projection.odds_updated_at_ms,
            valued_at_ms = projection.valued_at_ms,
            gap_count = projection.gaps.len(),
            gaps = ?projection.gaps,
            "account_equity_mark_projection_incomplete"
        );
        return false;
    }
    let Some(marked_positions_value) = projection
        .row_for_symbol("TOTAL")
        .and_then(|row| row.marked_remaining_value_units_6dp)
        .and_then(|units| u128::try_from(units).ok())
    else {
        tracing::error!(
            target: "app.live_runtime.account_projection",
            portfolio_revision = projection.portfolio_revision,
            accounting_generation = accounting.generation,
            "account_equity_total_mark_missing"
        );
        return false;
    };
    let portfolio = projection.portfolio;
    let Some(wallet) = accounting.wallet.as_ref() else {
        return false;
    };
    let account = match AccountEquitySnapshot::from_units(
        Some(wallet.reconciled_usdc_units_6dp),
        risk.current_snapshot().wallet_matic_balance,
        Some(marked_positions_value),
        Some(wallet.projected_pusd_units_6dp),
        wallet.source_ts_ms,
    ) {
        Ok(account) => account,
        Err(error) => {
            tracing::error!(
                target: "app.live_runtime.account_projection",
                accounting_generation = accounting.generation,
                error = %error,
                "account_equity_projection_failed"
            );
            return false;
        }
    };
    risk.set_account_equity_snapshot(&account);
    if let Err(error) = risk.confirm_direct_buy_acquisitions(&portfolio) {
        tracing::error!(
            target: "app.live_runtime.account_projection",
            accounting_generation = accounting.generation,
            error = %error,
            "account_equity_projection_failed"
        );
        return false;
    }
    let account_updated_at_ms = risk.current_snapshot().account_equity_updated_at_ms;
    obs.set_portfolio(portfolio, Some(&account), account_updated_at_ms);
    true
}

fn publish_coherent_risk_capture(capture: &MaybeCapture, risk: &RiskHandle) {
    let row = ReplayRiskSnapshotCapture::from_snapshot(now_ms(), &risk.current_snapshot());
    if capture.emit_replay_risk_snapshot(row).is_err() {
        tracing::error!(
            target: "app.live_runtime.account_projection",
            "coherent_accounting_capture_enqueue_failed"
        );
    }
}

fn start_external_account_audit(
    capture: MaybeCapture,
    risk: RiskHandle,
    portfolio_rx: watch::Receiver<PortfolioState>,
    exec: ExecutionClient,
    account_http: reqwest::Client,
    account_rpc_url: String,
    trigger: &'static str,
) -> ExternalAccountAuditFuture {
    Box::pin(async move {
        let wallet = exec.polymarket_tracked_wallet();
        let requested_at_ms = now_ms();
        let (wallet_cash, clob_admission) = tokio::join!(
            fetch_wallet_cash_balances(&account_http, &account_rpc_url, wallet),
            fetch_clob_balance_allowance(&exec),
        );
        let completed_at_ms = now_ms();
        let wallet_outcome = match &wallet_cash {
            Ok(wallet) => ExternalWalletAuditOutcome::Success {
                usdc_balance: wallet.usdc_units_6dp as f64 / 1_000_000.0,
                pusd_balance: wallet.pusd_units_6dp as f64 / 1_000_000.0,
            },
            Err(_) => ExternalWalletAuditOutcome::Error {
                message: "wallet cash balance refresh failed".to_string(),
            },
        };
        let clob_outcome = match &clob_admission {
            Ok((balance, allowance)) => ExternalClobAuditOutcome::Success {
                balance: *balance,
                allowance: *allowance,
            },
            Err(_) => ExternalClobAuditOutcome::Error {
                message: "CLOB balance and allowance refresh failed".to_string(),
            },
        };
        let audit = ExternalAccountAuditCapture {
            requested_at_ms,
            completed_at_ms,
            trigger: trigger.to_string(),
            wallet: wallet_outcome,
            clob: clob_outcome,
        };
        let audit_row = ReplayRiskSnapshotCapture::from_snapshot_with_external_account_audit(
            completed_at_ms,
            &risk.current_snapshot(),
            audit,
        );
        if capture.emit_replay_risk_snapshot(audit_row).is_err() {
            tracing::error!(
                target: "app.live_runtime.account_projection",
                trigger,
                portfolio_revision = portfolio_rx.borrow().revision,
                "external_account_audit_enqueue_failed"
            );
        }
        ExternalAccountAuditResult {
            trigger,
            wallet_cash,
            clob_admission,
        }
    })
}

fn complete_external_account_projection(
    risk: &RiskHandle,
    portfolio_rx: &watch::Receiver<PortfolioState>,
    audit_result: ExternalAccountAuditResult,
) -> bool {
    let ExternalAccountAuditResult {
        trigger,
        wallet_cash,
        clob_admission,
    } = audit_result;
    match clob_admission {
        Ok((balance, allowance)) => risk.set_balance_snapshot(balance, allowance),
        Err(_) => {
            tracing::error!(
                target: "app.live_runtime.account_projection",
                trigger,
                portfolio_revision = portfolio_rx.borrow().revision,
                "clob_admission_refresh_failed"
            );
        }
    }
    let wallet_current = match wallet_cash {
        Ok(wallet) => {
            match reconcile_live_accounting(
                wallet.block_number,
                wallet.block_hash,
                wallet.usdc_units_6dp,
                wallet.pusd_units_6dp,
                wallet.observed_at_ms,
            ) {
                Ok(()) => true,
                Err(error) => {
                    tracing::error!(
                        target: "app.live_runtime.account_projection",
                        trigger,
                        portfolio_revision = portfolio_rx.borrow().revision,
                        error = %error,
                        "wallet_cash_projection_refresh_failed"
                    );
                    false
                }
            }
        }
        Err(_) => {
            tracing::error!(
                target: "app.live_runtime.account_projection",
                trigger,
                portfolio_revision = portfolio_rx.borrow().revision,
                "wallet_cash_balance_audit_failed"
            );
            false
        }
    };
    wallet_current
}

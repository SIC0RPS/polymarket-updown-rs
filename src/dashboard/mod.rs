mod chart;
mod commands;
mod http;
mod model;
mod ws;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::execution::ExecutionClient;
use crate::observability::Observability;
use crate::risk::RiskHandle;
use crate::runtime_control::SharedRuntimeConfigState;
use crate::trade_session_ledger::LiveSessionReportArtifact;

pub struct OperatorUiSurface {
    runtime: Option<OperatorUiRuntime>,
}

struct OperatorUiRuntime {
    task: JoinHandle<Result<()>>,
    equity_history_task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub(super) struct DashboardState {
    pub(super) observability: Observability,
    pub(super) shutdown_request: tokio_util::sync::CancellationToken,
    pub(super) execution: ExecutionClient,
    pub(super) risk: Option<RiskHandle>,
    pub(super) runtime_config_state: SharedRuntimeConfigState,
    pub(super) canonical_session_report: watch::Receiver<Option<LiveSessionReportArtifact>>,
    pub(super) price_history_root: PathBuf,
    pub(super) price_symbols: BTreeMap<String, String>,
    equity_history: Arc<Mutex<chart::EquityHistory>>,
    sequence: Arc<AtomicU64>,
}

impl DashboardState {
    pub(super) fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }
}

pub async fn start_operator_surface(
    args: crate::observability::ObservabilityOperatorSurfaceStart,
) -> Result<OperatorUiSurface> {
    let crate::observability::ObservabilityOperatorSurfaceStart {
        cfg,
        obs,
        exec,
        shutdown,
        runtime_config_state,
        risk,
        price_history_root,
        price_symbols,
        ..
    } = args;
    if !cfg.enabled {
        return Ok(OperatorUiSurface { runtime: None });
    }

    let bind_host = cfg.bind_host.trim();
    let addr: SocketAddr = format!("{}:{}", bind_host, cfg.port)
        .parse()
        .with_context(|| format!("invalid dashboard bind addr {bind_host}:{}", cfg.port))?;
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "dashboard command surface must bind to loopback, got {addr}"
    );
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind dashboard server on {addr}"))?;
    let equity_history = Arc::new(Mutex::new(
        tokio::task::spawn_blocking(chart::EquityHistory::from_canonical_events)
            .await
            .context("failed to hydrate dashboard equity history")?,
    ));
    let equity_history_task = chart::spawn_equity_history_collector(
        risk.clone(),
        Arc::clone(&equity_history),
        shutdown.clone(),
    );
    let state = DashboardState {
        observability: obs,
        shutdown_request: shutdown.clone(),
        execution: exec,
        risk,
        runtime_config_state,
        canonical_session_report: crate::trade_session_ledger::live_session_report_receiver(),
        price_history_root,
        price_symbols,
        equity_history,
        sequence: Arc::new(AtomicU64::new(1)),
    };
    let app = http::build_router(state);
    let task = tokio::spawn(async move {
        tracing::info!(target: "dashboard", %addr, "server_start");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .context("dashboard server stopped with error")
    });
    Ok(OperatorUiSurface {
        runtime: Some(OperatorUiRuntime {
            task,
            equity_history_task,
        }),
    })
}

impl OperatorUiSurface {
    pub async fn shutdown(self) -> Result<()> {
        if let Some(runtime) = self.runtime {
            let server_result = runtime
                .task
                .await
                .context("dashboard server task failed")
                .and_then(|result| result);
            let mut result = server_result;
            if let Some(task) = runtime.equity_history_task {
                if let Err(error) = task.await {
                    result = Err(match result {
                        Ok(()) => anyhow::anyhow!("dashboard equity history task failed: {error}"),
                        Err(prior) => prior.context(format!(
                            "dashboard equity history task also failed: {error}"
                        )),
                    });
                }
            }
            result?;
        }
        Ok(())
    }
}

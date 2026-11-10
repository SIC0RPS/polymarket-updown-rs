use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::capture::session::MaybeCapture;
use crate::event_ledger::{ensure_runtime_layout, EVENTS_JSONL_PATH};
use crate::observability::Observability;
use crate::polymarket::state::PolymarketOddsState;
use crate::runtime_control::SharedRuntimeConfigState;

mod compact;
mod json_output;
mod pause;
mod projection;
mod schema;

pub use json_output::write_bytes_atomic;

pub const DEBUG_STATE_JSON_PATH: &str = "data/runtime/debug_state.json";
pub const MANIFEST_JSON_PATH: &str = "data/runtime/manifest.json";
pub const PAUSE_JSONL_PATH: &str = "data/runtime/PAUSE.jsonl";

const FILE_PUBLISH_INTERVAL_MS: u64 = 1_000;

pub fn write_pause_control_state_file(
    enabled: bool,
    actor: &str,
    reason: &str,
    comment: &str,
) -> Result<()> {
    pause::write_pause_state_file(enabled, actor, reason, comment)
}

pub struct RuntimeFileRuntime {
    shutdown: CancellationToken,
    publisher_thread: thread::JoinHandle<Result<()>>,
    pause_task: JoinHandle<()>,
    report_runtime: ReportRuntime,
}

struct ReportQueue {
    pending: Option<crate::trade_session_ledger::LiveSessionReportArtifact>,
    projection_done: bool,
}

struct ReportRuntime {
    stop: Arc<AtomicBool>,
    queue: Arc<(Mutex<ReportQueue>, Condvar)>,
    projection_thread: thread::JoinHandle<Result<()>>,
    writer_thread: thread::JoinHandle<Result<()>>,
}

pub struct RuntimeFileStart {
    pub obs: Observability,
    pub live_ops: crate::execution::live_ops::LiveOpsHandle,
    pub risk: crate::risk::RiskHandle,
    pub runtime_config_state: SharedRuntimeConfigState,
    pub capture: MaybeCapture,
    pub shutdown: CancellationToken,
    pub odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
}

impl RuntimeFileRuntime {
    pub fn start(start: RuntimeFileStart) -> Self {
        let RuntimeFileStart {
            obs,
            live_ops,
            risk,
            runtime_config_state,
            capture,
            shutdown,
            odds_rx,
        } = start;
        let pause_runtime_config_state = runtime_config_state.clone();
        let publisher_shutdown = shutdown.clone();
        let publisher_thread = thread::Builder::new()
            .name("runtime-file-publisher".to_string())
            .spawn(move || {
                run_runtime_file_publisher(
                    obs,
                    live_ops,
                    risk,
                    runtime_config_state,
                    publisher_shutdown,
                )
            })
            .expect("spawn runtime file publisher");
        let pause_task = tokio::spawn(pause::run_pause_file_watcher(
            pause_runtime_config_state,
            capture,
            shutdown.clone(),
        ));
        let report_runtime = ReportRuntime::start(odds_rx);
        Self {
            shutdown,
            publisher_thread,
            pause_task,
            report_runtime,
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        self.shutdown.cancel();
        self.publisher_thread.thread().unpark();
        let mut failures = Vec::new();
        if let Err(error) = self.pause_task.await {
            failures.push(format!("join pause file watcher: {error}"));
        }
        match self.publisher_thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("final runtime file publication: {error:#}")),
            Err(_) => failures.push("runtime file publisher panicked".to_string()),
        }
        if let Err(error) = self.report_runtime.shutdown() {
            failures.push(format!("{error:#}"));
        }
        anyhow::ensure!(
            failures.is_empty(),
            "runtime file shutdown failed: {}",
            failures.join("; ")
        );
        Ok(())
    }
}

impl ReportRuntime {
    fn start(odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let queue = Arc::new((
            Mutex::new(ReportQueue {
                pending: None,
                projection_done: false,
            }),
            Condvar::new(),
        ));
        let writer_queue = queue.clone();
        let writer_thread = thread::Builder::new()
            .name("live-report-writer".to_string())
            .spawn(move || run_live_report_writer(writer_queue))
            .expect("spawn live report writer");
        let projection_stop = stop.clone();
        let projection_queue = queue.clone();
        let projection_thread = thread::Builder::new()
            .name("live-report-projector".to_string())
            .spawn(move || run_live_report_projector(projection_stop, projection_queue, odds_rx))
            .expect("spawn live report projector");
        Self {
            stop,
            queue,
            projection_thread,
            writer_thread,
        }
    }

    fn shutdown(self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        self.projection_thread.thread().unpark();
        let mut failures = Vec::new();
        match self.projection_thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("final live report projection: {error:#}")),
            Err(_) => failures.push("live report projector panicked".to_string()),
        }
        let (state, ready) = &*self.queue;
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        state.projection_done = true;
        ready.notify_all();
        drop(state);
        match self.writer_thread.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("final live report persistence: {error:#}")),
            Err(_) => failures.push("live report writer panicked".to_string()),
        }
        anyhow::ensure!(
            failures.is_empty(),
            "live report shutdown failed: {}",
            failures.join("; ")
        );
        Ok(())
    }
}

fn run_live_report_projector(
    stop: Arc<AtomicBool>,
    queue: Arc<(Mutex<ReportQueue>, Condvar)>,
    odds_rx: Option<watch::Receiver<Arc<PolymarketOddsState>>>,
) -> Result<()> {
    while !stop.load(Ordering::Acquire) {
        thread::park_timeout(Duration::from_millis(FILE_PUBLISH_INTERVAL_MS));
        if stop.load(Ordering::Acquire) {
            break;
        }
        let _ = publish_report_snapshot(&queue, odds_rx.as_ref());
    }
    publish_report_snapshot(&queue, odds_rx.as_ref())
}

fn publish_report_snapshot(
    queue: &Arc<(Mutex<ReportQueue>, Condvar)>,
    odds_rx: Option<&watch::Receiver<Arc<PolymarketOddsState>>>,
) -> Result<()> {
    let odds = odds_rx.map(|receiver| receiver.borrow().clone());
    match crate::trade_session_ledger::snapshot_live_session_report(None, odds.as_deref()) {
        Ok(Some(artifact)) => {
            let (state, ready) = &**queue;
            let mut state = state
                .lock()
                .map_err(|_| anyhow::anyhow!("live report queue lock poisoned"))?;
            state.pending = Some(artifact);
            ready.notify_one();
        }
        Ok(None) => {}
        Err(error) => {
            tracing::error!(
                target: "runtime_files.live_report",
                error = %error,
                "live_report_projection_failed"
            );
            return Err(error);
        }
    }
    Ok(())
}

fn run_live_report_writer(queue: Arc<(Mutex<ReportQueue>, Condvar)>) -> Result<()> {
    let mut latest_result = Ok(());
    loop {
        let artifact = {
            let (state, ready) = &*queue;
            let mut state = match state.lock() {
                Ok(state) => state,
                Err(_) => anyhow::bail!("live report queue lock poisoned"),
            };
            while state.pending.is_none() && !state.projection_done {
                state = match ready.wait(state) {
                    Ok(state) => state,
                    Err(_) => anyhow::bail!("live report queue wait lock poisoned"),
                };
            }
            match state.pending.take() {
                Some(artifact) => artifact,
                None => return latest_result,
            }
        };
        latest_result =
            crate::trade_session_ledger::write_live_session_report_artifact(&artifact).map(|_| ());
        if let Err(error) = &latest_result {
            tracing::error!(
                target: "runtime_files.live_report",
                run_id = %artifact.run_id,
                accounting_generation = artifact.accounting_generation,
                error = %error,
                "live_report_file_write_failed"
            );
        }
    }
}

pub fn write_terminal_health_file(
    status: &str,
    run_id: Option<&str>,
    event_id: Option<&str>,
    event_ts_ms: Option<u64>,
    config_hash: Option<&str>,
) -> Result<()> {
    ensure_runtime_layout()?;
    json_output::write_json_file(
        crate::event_ledger::HEALTH_JSON_PATH,
        &schema::RuntimeHealthFile {
            status: status.to_string(),
            run_id: run_id.map(ToOwned::to_owned),
            latest_event_id: event_id.map(ToOwned::to_owned),
            latest_event_ts_ms: event_ts_ms,
            config_hash: config_hash.map(ToOwned::to_owned),
            updated_at_ms: crate::event_ledger::now_ms(),
            active_path: EVENTS_JSONL_PATH.to_string(),
            settlement: None,
        },
    )
}

fn run_runtime_file_publisher(
    obs: Observability,
    live_ops: crate::execution::live_ops::LiveOpsHandle,
    risk: crate::risk::RiskHandle,
    runtime_config_state: SharedRuntimeConfigState,
    shutdown: CancellationToken,
) -> Result<()> {
    let runtime_state_rx = obs.runtime_state();
    let runtime_config_rx = runtime_config_state.subscribe();
    let publish = || {
        projection::write_runtime_files(
            &obs,
            &live_ops.snapshot(),
            &runtime_state_rx.borrow().clone(),
            &runtime_config_rx.borrow().clone(),
            risk.account_source_ts_ms(),
        )
    };
    while !shutdown.is_cancelled() {
        let _ = publish();
        thread::park_timeout(Duration::from_millis(FILE_PUBLISH_INTERVAL_MS));
    }
    publish()
}

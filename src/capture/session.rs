use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Mutex};

use bbwick_engine::polymarket_ws::{
    PolymarketBookAdmissionCaptureLane, PolymarketWsIngressCaptureLane,
};

use crate::capture::events::*;
use crate::config::{installed_optimizer_symbol_path, VALID_ASSETS};
use crate::event_ledger::{
    append_event, current_run_id, make_event_id, now_ms, CanonicalEventRecord,
    CanonicalEventSource, CanonicalEventType,
};
use crate::strategy_runtime::SymbolId;

mod journals;
mod writer;
use journals::{MarketJournalLane, StrategyConsumptionJournalLane};
use writer::{
    capture_symbol_filter, run_capture_writer, MarketJournalMsg, StrategyConsumptionJournalMsg,
};

const CAPTURE_MSG_CHANNEL_CAPACITY: usize = 8192;
const CAPTURE_PRIORITY_CHANNEL_CAPACITY: usize = 1024;
const POLYMARKET_WS_INGRESS_CHANNEL_CAPACITY: usize = 8192;
const REQUIRED_CAPTURE_CLOSED: u64 = 1_u64 << 63;
const REQUIRED_CAPTURE_LOST: u64 = 1_u64 << 62;
const REQUIRED_CAPTURE_IN_FLIGHT: u64 = REQUIRED_CAPTURE_LOST - 1;
pub const CAPTURED_ORACLE_LEARNING_INITIAL_FILE: &str = "oracle_learning_initial.rmp";
fn make_session_id() -> String {
    let now = chrono::Utc::now();
    let ts = now.format("%Y%m%dT%H%M%SZ");
    let pid = std::process::id();
    format!("{ts}_live_{pid}")
}

fn hash_config(contents: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    contents.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
pub enum CaptureMsg {
    OrderSubmitLatency(OrderSubmitLatencyCapture),
    OrderSubmitReject(OrderSubmitRejectCapture),
    LocalOrderBlock(LocalOrderBlockCapture),
    RiskBlock(RiskBlockCapture),
    PolymarketUserOrder(PolymarketUserOrderCapture),
    PolymarketUserTrade(PolymarketUserTradeCapture),
    ReplaySeed(ReplaySeedCapture),
    ReplayRiskSnapshot(ReplayRiskSnapshotCapture),
    PolymarketDiscoverySnapshot(PolymarketDiscoverySnapshotCapture),
    ControlActuation(ControlActuationCapture),
    CtfInventoryOperationLatency(CtfInventoryOperationLatencyCapture),
}

#[derive(Clone)]
struct CaptureHandle {
    priority_tx: mpsc::Sender<CaptureMsg>,
    priority_attempted: Arc<AtomicU64>,
    priority_full: Arc<AtomicU64>,
    priority_closed: Arc<AtomicU64>,
    required_capture_admission: Arc<AtomicU64>,
    strategy_consumption_attempted: Arc<AtomicU64>,
    strategy_consumption_lost: Arc<AtomicU64>,
    strategy_consumption_terminal_sequence: Arc<AtomicU64>,
    strategy_consumption_journals:
        Arc<HashMap<(SymbolId, StrategyConsumptionKind), StrategyConsumptionJournalLane>>,
    market_journals: Arc<HashMap<String, MarketJournalLane>>,
    market_unrouted_lost: Arc<AtomicU64>,
    polymarket_ws_ingress_capture: PolymarketWsIngressCaptureLane,
    polymarket_book_admission_capture: PolymarketBookAdmissionCaptureLane,
    session_dir: PathBuf,
    retention_cap: Arc<Mutex<usize>>,
    session_start_ms: u64,
    max_duration_ms: u64,
    symbols_allowlist: Vec<String>,
}

#[derive(Clone, Default)]
pub struct MaybeCapture(Option<CaptureHandle>);

impl MaybeCapture {
    pub fn emit(&self, msg: CaptureMsg) {
        if let Some(handle) = &self.0 {
            if now_ms().saturating_sub(handle.session_start_ms) > handle.max_duration_ms {
                return;
            }
            match msg {
                CaptureMsg::ReplayRiskSnapshot(_) => {
                    tracing::error!(target: "capture.session", "replay_risk_snapshot_requires_bounded_emit");
                }
                msg => {
                    let _ = self.emit_priority(handle, msg);
                }
            }
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub fn session_dir(&self) -> Option<&Path> {
        self.0.as_ref().map(|handle| handle.session_dir.as_path())
    }

    pub async fn update_retention(
        &self,
        base_dir: &str,
        max_sessions: usize,
        min_save_duration_ms: u64,
    ) -> Result<()> {
        let Some(handle) = &self.0 else {
            return super::retention::prune_capture_sessions(
                base_dir,
                max_sessions,
                min_save_duration_ms,
            )
            .await;
        };
        let mut cap = handle.retention_cap.lock().await;
        *cap = max_sessions;
        super::retention::prune_capture_sessions(base_dir, *cap, min_save_duration_ms).await
    }

    pub fn write_initial_oracle_learning(&self, bytes: &[u8]) -> Result<()> {
        let Some(handle) = &self.0 else {
            return Ok(());
        };
        crate::runtime_files::write_bytes_atomic(
            &handle
                .session_dir
                .join(CAPTURED_ORACLE_LEARNING_INITIAL_FILE),
            bytes,
        )
        .context("write captured initial oracle learning")
    }

    pub fn polymarket_book_admission_capture(&self) -> Option<PolymarketBookAdmissionCaptureLane> {
        self.0
            .as_ref()
            .map(|handle| handle.polymarket_book_admission_capture.clone())
    }

    pub fn polymarket_ws_ingress_capture(&self) -> Option<PolymarketWsIngressCaptureLane> {
        self.0
            .as_ref()
            .map(|handle| handle.polymarket_ws_ingress_capture.clone())
    }

    fn emit_priority(&self, handle: &CaptureHandle, msg: CaptureMsg) -> Result<()> {
        if !Self::enter_required_capture(handle) {
            anyhow::bail!("capture priority admission closed");
        }
        handle.priority_attempted.fetch_add(1, Ordering::Relaxed);
        let send_result = handle.priority_tx.try_send(msg);
        if let Err(error) = &send_result {
            match error {
                mpsc::error::TrySendError::Full(_) => {
                    handle.priority_full.fetch_add(1, Ordering::Relaxed);
                }
                mpsc::error::TrySendError::Closed(_) => {
                    handle.priority_closed.fetch_add(1, Ordering::Relaxed);
                }
            }
            tracing::warn!(
                target: "capture.session",
                error = %error,
                "priority_capture_emit_dropped"
            );
        }
        self.finish_required_capture(send_result.is_err());
        send_result.context("capture priority event")
    }

    pub fn emit_replay_risk_snapshot(&self, event: ReplayRiskSnapshotCapture) -> Result<bool> {
        let Some(handle) = &self.0 else {
            return Ok(false);
        };
        if event.session_ts_ms < handle.session_start_ms
            || event.session_ts_ms.saturating_sub(handle.session_start_ms) > handle.max_duration_ms
        {
            return Ok(false);
        }
        self.emit_priority(handle, CaptureMsg::ReplayRiskSnapshot(event))
            .context("capture replay risk snapshot")?;
        Ok(true)
    }
}

pub fn captured_initial_oracle_learning_bytes(session_dir: &Path) -> Result<Option<Vec<u8>>> {
    crate::capture::finalized_capture_identity(session_dir)?;
    let manifest_path = session_dir.join("capture_artifact_manifest.json");
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .with_context(|| format!("read finalized capture {}", manifest_path.display()))?,
    )
    .with_context(|| format!("parse finalized capture {}", manifest_path.display()))?;
    let artifacts = manifest
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .context("finalized capture manifest artifacts are missing")?;
    let Some(artifact) = artifacts.iter().find(|artifact| {
        artifact.get("file").and_then(serde_json::Value::as_str)
            == Some(CAPTURED_ORACLE_LEARNING_INITIAL_FILE)
    }) else {
        return Ok(None);
    };
    anyhow::ensure!(
        artifact.get("present").and_then(serde_json::Value::as_bool) == Some(true),
        "capture declares missing initial oracle learning: {}",
        session_dir.display()
    );
    let expected_sha256 = artifact
        .get("sha256")
        .and_then(serde_json::Value::as_str)
        .context("captured initial oracle learning has no content identity")?;
    let path = session_dir.join(CAPTURED_ORACLE_LEARNING_INITIAL_FILE);
    let bytes = fs::read(&path)
        .with_context(|| format!("read captured initial oracle learning {}", path.display()))?;
    anyhow::ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == expected_sha256,
        "captured initial oracle learning content changed: {}",
        path.display()
    );
    Ok(Some(bytes))
}

#[allow(clippy::too_many_arguments)]
pub async fn init_capture_session(
    base_dir: &str,
    max_duration_ms: u64,
    min_save_duration_ms: u64,
    symbols_allowlist: &[String],
    replay_required_underlyings: &[String],
    max_sessions: usize,
    config_toml: &str,
    oracle_buy_and_hold: bool,
    strategy_kind: &str,
    symbols: &[String],
    ohlc_source_root: PathBuf,
    ohlc_symbols: Vec<String>,
    ohlc_timeframes: Vec<String>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(MaybeCapture, tokio::task::JoinHandle<Result<()>>)> {
    if let Err(error) =
        super::retention::prune_capture_sessions(base_dir, max_sessions, min_save_duration_ms).await
    {
        tracing::warn!(
            target: "capture.retention",
            base_dir,
            error = %error,
            "capture retention pruning failed; capture startup continuing"
        );
    }
    let session_id = make_session_id();
    let run_id = current_run_id().context("capture session has no canonical run identity")?;
    let session_dir = PathBuf::from(base_dir).join(&session_id);
    tokio::fs::create_dir_all(base_dir)
        .await
        .with_context(|| format!("failed to create capture base dir {base_dir}"))?;
    tokio::fs::create_dir(&session_dir)
        .await
        .with_context(|| format!("failed to create session dir {}", session_dir.display()))?;

    // Write config snapshot.
    tokio::fs::write(session_dir.join("config_snapshot.toml"), config_toml)
        .await
        .context("failed to write config_snapshot.toml")?;
    let optimizer_config_hash = snapshot_optimizer_configs(&session_dir, oracle_buy_and_hold)
        .await
        .context("failed to write optimizer config snapshots")?;

    // Write initial session_meta.json.
    let config_hash = hash_config(config_toml);
    let meta = SessionMeta {
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        start_ts_ms: now_ms(),
        end_ts_ms: None,
        pid: std::process::id(),
        execution_mode: "live".to_string(),
        strategy_kind: strategy_kind.to_string(),
        config_hash: config_hash.clone(),
        optimizer_config_hash: Some(optimizer_config_hash.clone()),
        final_config_hash: None,
        config_timeline_hash: None,
        config_change_count: Some(0),
        symbols: symbols.to_vec(),
        market_ids: Vec::new(),
        exit_reason: None,
    };
    let meta_json = serde_json::to_string_pretty(&meta).context("serialize session meta")?;
    tokio::fs::write(session_dir.join("session_meta.json"), &meta_json)
        .await
        .context("failed to write session_meta.json")?;
    let _ = append_event(&CanonicalEventRecord {
        event_id: make_event_id(
            CanonicalEventType::Actuation,
            meta.start_ts_ms,
            session_id.clone(),
        ),
        run_id: run_id.clone(),
        ts_ms: meta.start_ts_ms,
        event_type: CanonicalEventType::Actuation,
        source: CanonicalEventSource::System,
        subject: "capture".to_string(),
        refs: json!({
            "session_id": session_id,
            "config_hash": config_hash,
            "base_dir": base_dir,
        }),
        metrics: json!({
            "symbol_count": symbols.len(),
            "max_duration_ms": max_duration_ms,
            "min_save_duration_ms": min_save_duration_ms,
        }),
        context: json!({
            "action": "capture_enabled",
            "strategy_kind": strategy_kind,
            "symbols": symbols,
        }),
    });

    let (priority_tx, priority_rx) = mpsc::channel::<CaptureMsg>(CAPTURE_PRIORITY_CHANNEL_CAPACITY);
    let priority_attempted = Arc::new(AtomicU64::new(0));
    let priority_full = Arc::new(AtomicU64::new(0));
    let priority_closed = Arc::new(AtomicU64::new(0));
    let required_capture_admission = Arc::new(AtomicU64::new(0));
    let strategy_consumption_attempted = Arc::new(AtomicU64::new(0));
    let strategy_consumption_lost = Arc::new(AtomicU64::new(0));
    let strategy_consumption_terminal_sequence = Arc::new(AtomicU64::new(0));
    let (strategy_consumption_journals, strategy_consumption_receivers) =
        journals::make_strategy_consumption_journals(symbols);
    let (polymarket_ws_ingress_capture, polymarket_ws_ingress_rx) =
        PolymarketWsIngressCaptureLane::bounded(POLYMARKET_WS_INGRESS_CHANNEL_CAPACITY);
    let (polymarket_book_admission_capture, polymarket_book_admission_rx) =
        PolymarketBookAdmissionCaptureLane::bounded(POLYMARKET_WS_INGRESS_CHANNEL_CAPACITY);
    let start_ms = meta.start_ts_ms;
    let symbols_allowlist = capture_symbol_filter(symbols_allowlist);
    let (market_journals, market_receivers) = journals::make_market_journals(&symbols_allowlist);
    let market_unrouted_lost = Arc::new(AtomicU64::new(0));
    let retention_cap = Arc::new(Mutex::new(max_sessions));
    let handle = CaptureHandle {
        priority_tx,
        priority_attempted: Arc::clone(&priority_attempted),
        priority_full: Arc::clone(&priority_full),
        priority_closed: Arc::clone(&priority_closed),
        required_capture_admission: Arc::clone(&required_capture_admission),
        strategy_consumption_attempted: Arc::clone(&strategy_consumption_attempted),
        strategy_consumption_lost: Arc::clone(&strategy_consumption_lost),
        strategy_consumption_terminal_sequence: Arc::clone(&strategy_consumption_terminal_sequence),
        strategy_consumption_journals: Arc::clone(&strategy_consumption_journals),
        market_journals: Arc::clone(&market_journals),
        market_unrouted_lost: Arc::clone(&market_unrouted_lost),
        polymarket_ws_ingress_capture: polymarket_ws_ingress_capture.clone(),
        polymarket_book_admission_capture: polymarket_book_admission_capture.clone(),
        session_dir: session_dir.clone(),
        retention_cap: Arc::clone(&retention_cap),
        session_start_ms: start_ms,
        max_duration_ms,
        symbols_allowlist: symbols_allowlist.clone(),
    };
    let ohlc_shutdown = shutdown.child_token();
    let ohlc_task = tokio::spawn(run_capture_ohlc_watcher(
        ohlc_source_root,
        session_dir.join("bbwick_ohlc"),
        ohlc_symbols,
        ohlc_timeframes,
        max_duration_ms,
        ohlc_shutdown.clone(),
    ));

    let task = tokio::spawn(run_capture_writer(
        priority_rx,
        polymarket_ws_ingress_rx,
        polymarket_book_admission_rx,
        CaptureWriterConfig {
            session_dir,
            session_id,
            run_id,
            start_ms,
            strategy_kind: strategy_kind.to_string(),
            config_hash,
            optimizer_config_hash,
            symbols: symbols.to_vec(),
            replay_required_underlyings: capture_symbol_filter(replay_required_underlyings),
            max_duration_ms,
            min_save_duration_ms,
            shutdown,
            ohlc_shutdown,
            ohlc_task: Some(ohlc_task),
            retention_cap,
            required_capture_admission,
            priority_attempted,
            priority_full,
            priority_closed,
            strategy_consumption_attempted,
            strategy_consumption_lost,
            strategy_consumption_terminal_sequence,
            strategy_consumption_journals,
            strategy_consumption_receivers,
            strategy_consumption_threads: Vec::new(),
            market_journals,
            market_receivers,
            market_threads: Vec::new(),
            journal_entries: Vec::new(),
            polymarket_ws_ingress_capture,
            polymarket_book_admission_capture,
        },
    ));

    Ok((MaybeCapture(Some(handle)), task))
}

async fn run_capture_ohlc_watcher(
    source_root: PathBuf,
    dest_root: PathBuf,
    symbols: Vec<String>,
    timeframes: Vec<String>,
    max_duration_ms: u64,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let mut copy_result = Ok(());
    for symbol in &symbols {
        for timeframe in &timeframes {
            let result = copy_ohlc_file(
                ohlc_path(&source_root, symbol, timeframe),
                ohlc_path(&dest_root, symbol, timeframe),
            )
            .await;
            copy_result = copy_result.and(result);
        }
    }

    let watch_root =
        tokio::task::spawn_blocking(move || fs::canonicalize(&source_root).unwrap_or(source_root))
            .await
            .context("join capture OHLC watch root lookup")?;
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel::<notify::Result<Event>>();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
        let _ = watch_tx.send(res);
    })
    .context("initialize capture OHLC watcher")?;
    watcher
        .watch(&watch_root, RecursiveMode::Recursive)
        .with_context(|| format!("watch capture OHLC source {}", watch_root.display()))?;
    let max_duration_sleep = tokio::time::sleep(Duration::from_millis(max_duration_ms));
    tokio::pin!(max_duration_sleep);
    let mut watcher = Some(watcher);
    loop {
        tokio::select! {
            _ = shutdown.cancelled(), if watcher.is_some() => drop(watcher.take()),
            _ = &mut max_duration_sleep, if watcher.is_some() => drop(watcher.take()),
            maybe_event = watch_rx.recv() => {
                let Some(event) = maybe_event else { break };
                let Ok(event) = event else { continue };
                for path in event.paths {
                    if !path.ends_with("ohlc.arrow") {
                        continue;
                    }
                    let Ok(relative) = path.strip_prefix(&watch_root) else {
                        continue;
                    };
                    let destination = dest_root.join(relative);
                    let result = copy_ohlc_file(path, destination).await;
                    copy_result = copy_result.and(result);
                }
            }
        }
    }
    copy_result
}

fn ohlc_path(root: &Path, symbol: &str, timeframe: &str) -> PathBuf {
    root.join(canonical_ohlc_component(symbol))
        .join(timeframe.trim().to_ascii_lowercase())
        .join("ohlc.arrow")
}

fn canonical_ohlc_component(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

async fn copy_ohlc_file(src: PathBuf, dst: PathBuf) -> Result<()> {
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let parent = dst
            .parent()
            .context("capture OHLC destination has no parent")?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create capture OHLC directory {}", parent.display()))?;
        fs::copy(&src, &dst)
            .with_context(|| format!("copy capture OHLC {} -> {}", src.display(), dst.display()))?;
        Ok(())
    })
    .await
    .context("join capture OHLC copy")
    .and_then(|result| result);
    if let Err(error) = &result {
        tracing::warn!(
            target: "capture.writer",
            error = %error,
            "capture_ohlc_copy_failed"
        );
    }
    result
}

async fn snapshot_optimizer_configs(
    session_dir: &Path,
    oracle_buy_and_hold: bool,
) -> Result<String> {
    let snapshot_dir = session_dir.join("optimizer_snapshot");
    tokio::fs::create_dir_all(&snapshot_dir)
        .await
        .with_context(|| format!("create {}", snapshot_dir.display()))?;
    let mut snapshot_body = String::new();
    for (file_name, source) in optimizer_snapshot_files(oracle_buy_and_hold) {
        anyhow::ensure!(
            source.is_file(),
            "optimizer config snapshot source missing: {}",
            source.display()
        );
        let target = snapshot_dir.join(&file_name);
        let content = tokio::fs::read_to_string(&source)
            .await
            .with_context(|| format!("read optimizer config snapshot {}", source.display()))?;
        tokio::fs::write(&target, &content).await.with_context(|| {
            format!(
                "write optimizer config snapshot {} -> {}",
                source.display(),
                target.display()
            )
        })?;
        snapshot_body.push_str(&file_name);
        snapshot_body.push('\n');
        snapshot_body.push_str(&content);
        snapshot_body.push('\n');
    }
    Ok(hash_config(&snapshot_body))
}

fn optimizer_snapshot_files(oracle_buy_and_hold: bool) -> Vec<(String, PathBuf)> {
    VALID_ASSETS
        .iter()
        .map(|symbol| {
            (
                format!("{symbol}.optimization.yml"),
                installed_optimizer_symbol_path(Path::new("."), symbol, oracle_buy_and_hold),
            )
        })
        .collect()
}

pub(super) struct CaptureWriterConfig {
    pub(super) session_dir: PathBuf,
    pub(super) session_id: String,
    pub(super) run_id: String,
    pub(super) start_ms: u64,
    pub(super) strategy_kind: String,
    pub(super) config_hash: String,
    pub(super) optimizer_config_hash: String,
    pub(super) symbols: Vec<String>,
    pub(super) replay_required_underlyings: Vec<String>,
    pub(super) max_duration_ms: u64,
    pub(super) min_save_duration_ms: u64,
    shutdown: tokio_util::sync::CancellationToken,
    ohlc_shutdown: tokio_util::sync::CancellationToken,
    ohlc_task: Option<tokio::task::JoinHandle<Result<()>>>,
    pub(super) retention_cap: Arc<Mutex<usize>>,
    pub(super) required_capture_admission: Arc<AtomicU64>,
    pub(super) priority_attempted: Arc<AtomicU64>,
    pub(super) priority_full: Arc<AtomicU64>,
    pub(super) priority_closed: Arc<AtomicU64>,
    pub(super) strategy_consumption_attempted: Arc<AtomicU64>,
    pub(super) strategy_consumption_lost: Arc<AtomicU64>,
    pub(super) strategy_consumption_terminal_sequence: Arc<AtomicU64>,
    strategy_consumption_journals:
        Arc<HashMap<(SymbolId, StrategyConsumptionKind), StrategyConsumptionJournalLane>>,
    strategy_consumption_receivers: Vec<(
        SymbolId,
        StrategyConsumptionKind,
        mpsc::Receiver<StrategyConsumptionJournalMsg>,
    )>,
    strategy_consumption_threads: Vec<(
        SymbolId,
        StrategyConsumptionKind,
        std::thread::JoinHandle<writer::StrategyConsumptionJournalResult>,
    )>,
    market_journals: Arc<HashMap<String, MarketJournalLane>>,
    market_receivers: Vec<(String, mpsc::Receiver<MarketJournalMsg>)>,
    market_threads: Vec<(String, std::thread::JoinHandle<writer::MarketJournalResult>)>,
    pub(super) journal_entries: Vec<crate::capture::finalization::CaptureJournalManifestEntry>,
    pub(super) polymarket_ws_ingress_capture: PolymarketWsIngressCaptureLane,
    pub(super) polymarket_book_admission_capture: PolymarketBookAdmissionCaptureLane,
}

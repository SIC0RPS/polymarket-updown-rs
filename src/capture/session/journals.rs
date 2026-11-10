use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::{Context, Result};
use bbwick_engine::provider_ws::{ProviderCapture, ProviderObservation};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::capture::arrow::StrategyConsumptionMergedArrowReader;
use crate::capture::events::{
    PolymarketMarketWsIn, StrategyConsumptionKind, StrategyConsumptionRecord,
};
use crate::capture::finalization::{CaptureJournalFile, CaptureJournalManifestEntry};
use crate::capture::polymarket_resolution::PolymarketResolutionCapture;
use crate::capture::scope::{capture_symbol_allowed, capture_underlying_from_text};
use crate::strategy_runtime::SymbolId;

use super::writer::{
    run_strategy_consumption_journal, strategy_consumption_journal_path, MarketJournalMsg,
    StrategyConsumptionJournalMsg, StrategyConsumptionJournalResult,
};
use super::{
    CaptureHandle, CaptureWriterConfig, MaybeCapture, CAPTURE_MSG_CHANNEL_CAPACITY,
    REQUIRED_CAPTURE_CLOSED, REQUIRED_CAPTURE_IN_FLIGHT, REQUIRED_CAPTURE_LOST,
};

/// Original receipt evidence; these rows never establish strategy consumption.
#[derive(Serialize)]
pub(super) struct BinanceProviderRecord {
    pub kind: &'static str,
    pub recv_wall_ms: u64,
    pub received_mono_ns: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invalid_utf8_payload: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl ProviderCapture for MaybeCapture {
    fn observe(&self, symbol: Option<&str>, observation: ProviderObservation<'_>) {
        let Some(handle) = self.0.as_ref() else {
            return;
        };
        if !self.polymarket_resolution_in_window(observation.recv_wall_ms)
            || handle.required_capture_admission.load(Ordering::Acquire)
                & REQUIRED_CAPTURE_CLOSED != 0
        {
            return;
        }
        let (payload, invalid_utf8_payload) = match std::str::from_utf8(observation.payload) {
            Ok(text) => (Some(text.to_owned()), None),
            Err(_) => (None, Some(observation.payload.to_vec())),
        };
        let record = Arc::new(BinanceProviderRecord {
            kind: observation.kind,
            recv_wall_ms: observation.recv_wall_ms,
            received_mono_ns: observation.received_mono_ns,
            payload,
            invalid_utf8_payload,
            detail: observation.detail.map(str::to_owned),
        });
        if let Some(underlying) = symbol.and_then(capture_underlying_from_text) {
            self.try_send_market_journal(
                handle,
                &underlying.to_ascii_uppercase(),
                MarketJournalMsg::Provider(record),
            );
        } else {
            // Connection-wide or unroutable evidence is shared, not a per-symbol trade claim.
            for underlying in ["BTC", "ETH", "SOL", "XRP"] {
                self.try_send_market_journal(
                    handle,
                    underlying,
                    MarketJournalMsg::Provider(record.clone()),
                );
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct StrategyConsumptionJournalLane {
    pub(super) tx: mpsc::Sender<StrategyConsumptionJournalMsg>,
    pub(super) attempted: Arc<AtomicU64>,
    pub(super) full: Arc<AtomicU64>,
    pub(super) closed: Arc<AtomicU64>,
    pub(super) lost: Arc<AtomicU64>,
}

#[derive(Clone)]
pub(super) struct MarketJournalLane {
    pub(super) tx: mpsc::Sender<MarketJournalMsg>,
    pub(super) attempted: Arc<AtomicU64>,
    pub(super) full: Arc<AtomicU64>,
    pub(super) closed: Arc<AtomicU64>,
    pub(super) lost: Arc<AtomicU64>,
}

pub(super) fn make_strategy_consumption_journals(
    _symbols: &[String],
) -> (
    Arc<HashMap<(SymbolId, StrategyConsumptionKind), StrategyConsumptionJournalLane>>,
    Vec<(
        SymbolId,
        StrategyConsumptionKind,
        mpsc::Receiver<StrategyConsumptionJournalMsg>,
    )>,
) {
    let mut lanes = HashMap::new();
    let mut receivers = Vec::new();
    for actor in [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp] {
        for kind in [
            StrategyConsumptionKind::Runtime,
            StrategyConsumptionKind::Chainlink,
        ] {
            let (tx, rx) = mpsc::channel(CAPTURE_MSG_CHANNEL_CAPACITY);
            lanes.insert(
                (actor, kind),
                StrategyConsumptionJournalLane {
                    tx,
                    attempted: Arc::new(AtomicU64::new(0)),
                    full: Arc::new(AtomicU64::new(0)),
                    closed: Arc::new(AtomicU64::new(0)),
                    lost: Arc::new(AtomicU64::new(0)),
                },
            );
            receivers.push((actor, kind, rx));
        }
    }
    (Arc::new(lanes), receivers)
}

pub(super) fn make_market_journals(
    _symbols_allowlist: &[String],
) -> (
    Arc<HashMap<String, MarketJournalLane>>,
    Vec<(String, mpsc::Receiver<MarketJournalMsg>)>,
) {
    let mut lanes = HashMap::new();
    let mut receivers = Vec::new();
    for underlying in ["BTC", "ETH", "SOL", "XRP"] {
        let (tx, rx) = mpsc::channel(CAPTURE_MSG_CHANNEL_CAPACITY);
        lanes.insert(
            underlying.to_string(),
            MarketJournalLane {
                tx,
                attempted: Arc::new(AtomicU64::new(0)),
                full: Arc::new(AtomicU64::new(0)),
                closed: Arc::new(AtomicU64::new(0)),
                lost: Arc::new(AtomicU64::new(0)),
            },
        );
        receivers.push((underlying.to_string(), rx));
    }
    (Arc::new(lanes), receivers)
}

pub(super) fn hash_journal_file(path: &Path) -> std::io::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let len = reader.read(&mut buffer)?;
        if len == 0 {
            break;
        }
        hasher.update(&buffer[..len]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

impl MaybeCapture {
    pub fn binance_provider_capture(&self) -> Option<Arc<dyn ProviderCapture>> {
        self.0
            .as_ref()
            .map(|_| Arc::new(self.clone()) as Arc<dyn ProviderCapture>)
    }

    pub(super) fn enter_required_capture(handle: &CaptureHandle) -> bool {
        let mut admission = handle.required_capture_admission.load(Ordering::Acquire);
        loop {
            if admission & REQUIRED_CAPTURE_CLOSED != 0 {
                return false;
            }
            if admission & REQUIRED_CAPTURE_IN_FLIGHT == REQUIRED_CAPTURE_IN_FLIGHT {
                tracing::error!(target: "capture.session", "required_capture_admission_overflow");
                handle
                    .required_capture_admission
                    .fetch_or(REQUIRED_CAPTURE_LOST, Ordering::AcqRel);
                return false;
            }
            match handle.required_capture_admission.compare_exchange_weak(
                admission,
                admission + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => admission = observed,
            }
        }
    }

    pub fn reserve_strategy_consumption(
        &self,
        actor: SymbolId,
        kind: StrategyConsumptionKind,
        sequence: u64,
        consumed_at_ms: u64,
    ) -> Option<StrategyConsumptionPermit<'_>> {
        let handle = self.0.as_ref()?;
        if consumed_at_ms < handle.session_start_ms
            || consumed_at_ms.saturating_sub(handle.session_start_ms) > handle.max_duration_ms
        {
            return None;
        }
        if !Self::enter_required_capture(handle) {
            return None;
        }
        handle
            .strategy_consumption_attempted
            .fetch_add(1, Ordering::Relaxed);
        handle
            .strategy_consumption_terminal_sequence
            .fetch_max(sequence, Ordering::AcqRel);
        let Some(lane) = handle.strategy_consumption_journals.get(&(actor, kind)) else {
            handle
                .strategy_consumption_lost
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                target: "capture.session",
                actor = actor.as_str(),
                kind = kind.as_str(),
                sequence,
                "strategy_consumption_journal_missing"
            );
            self.finish_required_capture(true);
            return None;
        };
        lane.attempted.fetch_add(1, Ordering::Relaxed);
        match lane.tx.try_reserve() {
            Ok(permit) => Some(StrategyConsumptionPermit::new(permit)),
            Err(error) => {
                match &error {
                    mpsc::error::TrySendError::Full(_) => {
                        lane.full.fetch_add(1, Ordering::Relaxed);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        lane.closed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                lane.lost.fetch_add(1, Ordering::Relaxed);
                handle
                    .strategy_consumption_lost
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    target: "capture.session",
                    actor = actor.as_str(),
                    kind = kind.as_str(),
                    sequence,
                    error = %error,
                    "strategy_consumption_journal_admission_lost"
                );
                self.finish_required_capture(true);
                None
            }
        }
    }

    fn try_send_market_journal(
        &self,
        handle: &CaptureHandle,
        underlying: &str,
        msg: MarketJournalMsg,
    ) {
        if !Self::enter_required_capture(handle) {
            return;
        }
        let Some(lane) = handle.market_journals.get(underlying) else {
            handle.market_unrouted_lost.fetch_add(1, Ordering::Relaxed);
            tracing::error!(target: "capture.session", underlying, "market_journal_missing");
            self.finish_required_capture(true);
            return;
        };
        lane.attempted.fetch_add(1, Ordering::Relaxed);
        let lost = match lane.tx.try_send(msg) {
            Ok(()) => false,
            Err(error) => {
                match &error {
                    mpsc::error::TrySendError::Full(_) => {
                        lane.full.fetch_add(1, Ordering::Relaxed);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        lane.closed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                lane.lost.fetch_add(1, Ordering::Relaxed);
                tracing::error!(target: "capture.session", underlying, error = %error, "market_journal_admission_lost");
                true
            }
        };
        self.finish_required_capture(lost);
    }

    pub fn emit_engine_state(&self, snapshot: PolymarketMarketWsIn) {
        let Some(handle) = self.0.as_ref() else {
            return;
        };
        if snapshot.session_ts_ms < handle.session_start_ms
            || snapshot
                .session_ts_ms
                .saturating_sub(handle.session_start_ms)
                > handle.max_duration_ms
        {
            return;
        }
        for underlying in ["BTC", "ETH", "SOL", "XRP"] {
            self.try_send_market_journal(
                handle,
                underlying,
                MarketJournalMsg::Snapshot(snapshot.clone()),
            );
        }
    }

    pub fn capture_committed_resolution(&self, resolution: PolymarketResolutionCapture) {
        let Some(handle) = self.0.as_ref() else {
            return;
        };
        if !self.polymarket_resolution_in_window(resolution.observed_at_ms)
            || !capture_symbol_allowed(&handle.symbols_allowlist, &resolution.underlying)
        {
            return;
        }
        let underlying = resolution.underlying.trim().to_ascii_uppercase();
        self.try_send_market_journal(
            handle,
            &underlying,
            MarketJournalMsg::Resolution(resolution),
        );
    }

    pub fn polymarket_resolution_in_window(&self, observed_at_ms: u64) -> bool {
        self.0.as_ref().is_some_and(|handle| {
            observed_at_ms >= handle.session_start_ms
                && observed_at_ms.saturating_sub(handle.session_start_ms) <= handle.max_duration_ms
        })
    }

    pub fn mark_required_capture_lost(&self) {
        if let Some(handle) = self.0.as_ref() {
            handle
                .required_capture_admission
                .fetch_or(REQUIRED_CAPTURE_LOST, Ordering::AcqRel);
        }
    }

    pub fn finish_required_capture(&self, required_lost: bool) {
        let Some(handle) = self.0.as_ref() else {
            return;
        };
        if required_lost {
            self.mark_required_capture_lost();
        }
        handle
            .required_capture_admission
            .fetch_sub(1, Ordering::Release);
    }
}

pub struct StrategyConsumptionPermit<'a>(mpsc::Permit<'a, StrategyConsumptionJournalMsg>);

impl<'a> StrategyConsumptionPermit<'a> {
    pub(super) fn new(permit: mpsc::Permit<'a, StrategyConsumptionJournalMsg>) -> Self {
        Self(permit)
    }

    pub fn send(self, record: StrategyConsumptionRecord) {
        self.0.send(StrategyConsumptionJournalMsg::Record(record));
    }
}

pub(super) fn start_strategy_consumption_journals(cfg: &mut CaptureWriterConfig) -> Result<()> {
    for (actor, kind, rx) in std::mem::take(&mut cfg.strategy_consumption_receivers) {
        let session_dir = cfg.session_dir.clone();
        let thread = std::thread::Builder::new()
            .name(format!(
                "capture-strategy-{}-{}",
                actor.as_str(),
                kind.as_str()
            ))
            .spawn(move || run_strategy_consumption_journal(rx, &session_dir, actor, kind))
            .with_context(|| {
                format!(
                    "spawn strategy journal writer for {}/{}",
                    actor.as_str(),
                    kind.as_str()
                )
            })?;
        cfg.strategy_consumption_threads.push((actor, kind, thread));
    }
    Ok(())
}

pub(super) async fn finish_strategy_consumption_journals(
    cfg: &mut CaptureWriterConfig,
) -> Result<bool> {
    if cfg.strategy_consumption_threads.is_empty()
        && cfg
            .journal_entries
            .iter()
            .any(|entry| entry.lane == "strategy_consumption")
    {
        return Ok(validate_strategy_consumption_union(cfg));
    }

    let threads = std::mem::take(&mut cfg.strategy_consumption_threads);
    let mut finish_errors = HashMap::new();
    for (actor, kind, _) in &threads {
        let Some(lane) = cfg.strategy_consumption_journals.get(&(*actor, *kind)) else {
            continue;
        };
        if let Err(error) = lane.tx.send(StrategyConsumptionJournalMsg::Finish).await {
            finish_errors.insert((*actor, *kind), format!("send journal finish: {error}"));
        }
    }

    let joined: HashMap<
        (SymbolId, StrategyConsumptionKind),
        Option<StrategyConsumptionJournalResult>,
    > = tokio::task::spawn_blocking(move || {
        threads
            .into_iter()
            .map(|(actor, kind, thread)| ((actor, kind), thread.join().ok()))
            .collect()
    })
    .await
    .context("join strategy journal writer threads")?;

    let mut lanes: Vec<_> = cfg.strategy_consumption_journals.keys().copied().collect();
    lanes.sort_by_key(|(actor, kind)| (actor.as_str(), kind.as_str()));
    for (actor, kind) in lanes {
        let lane = &cfg.strategy_consumption_journals[&(actor, kind)];
        let attempted = lane.attempted.load(Ordering::Acquire);
        let full = lane.full.load(Ordering::Acquire);
        let closed = lane.closed.load(Ordering::Acquire);
        let accepted = attempted.saturating_sub(full.saturating_add(closed));
        let result = joined.get(&(actor, kind)).and_then(Option::as_ref);
        let relative_path = format!(
            "strategy_consumption/{}/{}.arrow",
            kind.as_str(),
            actor.as_str()
        );
        let expected_file = strategy_consumption_journal_path(&cfg.session_dir, actor, kind);
        let mut errors = Vec::new();
        if let Some(error) = finish_errors.remove(&(actor, kind)) {
            errors.push(error);
        }
        match result {
            Some(result) => {
                if result.actor != actor {
                    errors.push(format!(
                        "strategy journal writer actor mismatch: expected {}, reported {}",
                        actor.as_str(),
                        result.actor.as_str()
                    ));
                }
                if result.kind != kind {
                    errors.push(format!(
                        "strategy journal writer kind mismatch: expected {}, reported {}",
                        kind.as_str(),
                        result.kind.as_str()
                    ));
                }
                if result.file != expected_file {
                    errors.push(format!(
                        "strategy journal writer file mismatch: expected {}, reported {}",
                        expected_file.display(),
                        result.file.display()
                    ));
                }
                if let Some(error) = &result.writer_error {
                    errors.push(error.clone());
                }
            }
            None => errors.push("strategy journal writer did not finish".to_string()),
        }
        let writer_error = (!errors.is_empty()).then(|| errors.join("; "));
        cfg.journal_entries.push(CaptureJournalManifestEntry {
            lane: "strategy_consumption".to_string(),
            provider_evidence_version: None,
            causal_identity: format!("{}/{}", actor.as_str(), kind.as_str()),
            attempted,
            accepted,
            full,
            closed,
            written: result.map_or(0, |result| result.written),
            first_sequence: result.and_then(|result| result.first_sequence),
            last_sequence: result.and_then(|result| result.last_sequence),
            writer_error,
            files: vec![CaptureJournalFile {
                path: relative_path,
                written: result.map_or(0, |result| result.written),
                sha256: result.and_then(|result| result.sha256.clone()),
            }],
        });
    }
    Ok(validate_strategy_consumption_union(cfg))
}

fn validate_strategy_consumption_union(cfg: &mut CaptureWriterConfig) -> bool {
    let terminal = cfg
        .strategy_consumption_terminal_sequence
        .load(Ordering::Acquire);
    let mut sources = Vec::new();
    let mut valid = true;
    for entry in cfg
        .journal_entries
        .iter_mut()
        .filter(|entry| entry.lane == "strategy_consumption")
    {
        let admission_matches = entry
            .accepted
            .checked_add(entry.full)
            .and_then(|count| count.checked_add(entry.closed))
            == Some(entry.attempted);
        if entry.writer_error.is_some()
            || entry.full != 0
            || entry.closed != 0
            || !admission_matches
            || entry.accepted != entry.written
        {
            valid = false;
        }
        let Some((actor, kind)) = parse_strategy_identity(&entry.causal_identity) else {
            append_journal_error(
                &mut entry.writer_error,
                "strategy journal identity changed".to_string(),
            );
            valid = false;
            continue;
        };
        let Some(file) = entry.files.first() else {
            append_journal_error(
                &mut entry.writer_error,
                "strategy journal file missing".to_string(),
            );
            valid = false;
            continue;
        };
        let path = cfg.session_dir.join(&file.path);
        let expected_path = format!(
            "strategy_consumption/{}/{}.arrow",
            kind.as_str(),
            actor.as_str()
        );
        if entry.files.len() != 1 || file.path != expected_path || file.written != entry.written {
            append_journal_error(
                &mut entry.writer_error,
                "strategy journal file descriptor changed".to_string(),
            );
            valid = false;
            continue;
        }
        match hash_journal_file(&path) {
            Ok(hash) if file.sha256.as_deref() == Some(hash.as_str()) => {}
            Ok(_) => {
                append_journal_error(
                    &mut entry.writer_error,
                    "strategy journal hash changed".to_string(),
                );
                valid = false;
            }
            Err(error) => {
                append_journal_error(
                    &mut entry.writer_error,
                    format!("hash strategy journal: {error}"),
                );
                valid = false;
            }
        }
        sources.push((path, Some(actor), Some(kind)));
    }

    if sources.len() != 8 {
        valid = false;
    }
    let mut observed: HashMap<
        (SymbolId, StrategyConsumptionKind),
        (u64, Option<u64>, Option<u64>),
    > = HashMap::new();
    let mut merged_rows = 0_u64;
    let mut next_sequence = 1_u64;
    let mut union_error = None;
    match StrategyConsumptionMergedArrowReader::open(&sources) {
        Ok(mut reader) => loop {
            match reader.next_event() {
                Ok(Some((record, _))) => {
                    if record.consumption_sequence != next_sequence {
                        valid = false;
                        union_error.get_or_insert_with(|| {
                            format!(
                                "strategy journal union sequence gap: expected {next_sequence}, observed {}",
                                record.consumption_sequence
                            )
                        });
                    }
                    next_sequence = record.consumption_sequence.saturating_add(1);
                    merged_rows = merged_rows.saturating_add(1);
                    let state = observed
                        .entry((record.actor_symbol, record.input.kind()))
                        .or_insert((0, None, None));
                    state.0 = state.0.saturating_add(1);
                    state.1.get_or_insert(record.consumption_sequence);
                    state.2 = Some(record.consumption_sequence);
                }
                Ok(None) => break,
                Err(error) => {
                    valid = false;
                    union_error = Some(format!("read strategy journal union: {error:#}"));
                    break;
                }
            }
        },
        Err(error) => {
            valid = false;
            union_error = Some(format!("open strategy journal union: {error:#}"));
        }
    }

    for entry in cfg
        .journal_entries
        .iter_mut()
        .filter(|entry| entry.lane == "strategy_consumption")
    {
        let Some(identity) = parse_strategy_identity(&entry.causal_identity) else {
            continue;
        };
        let observed = observed.get(&identity).copied().unwrap_or((0, None, None));
        if observed.0 != entry.written {
            append_journal_error(
                &mut entry.writer_error,
                format!(
                    "strategy journal row count changed: expected {}, observed {}",
                    entry.written, observed.0
                ),
            );
            valid = false;
        }
        if observed.1 != entry.first_sequence || observed.2 != entry.last_sequence {
            append_journal_error(
                &mut entry.writer_error,
                "strategy journal sequence bounds changed".to_string(),
            );
            valid = false;
        }
    }
    if merged_rows != terminal || next_sequence != terminal.saturating_add(1) {
        valid = false;
        union_error.get_or_insert_with(|| {
            format!(
                "strategy journal union coverage changed: terminal {terminal}, rows {merged_rows}"
            )
        });
    }
    if let Some(error) = union_error {
        if let Some(entry) = cfg
            .journal_entries
            .iter_mut()
            .find(|entry| entry.lane == "strategy_consumption")
        {
            append_journal_error(&mut entry.writer_error, error);
        }
    }
    valid
}

fn parse_strategy_identity(identity: &str) -> Option<(SymbolId, StrategyConsumptionKind)> {
    let (actor, kind) = identity.split_once('/')?;
    let actor = SymbolId::parse(actor).ok()?;
    let kind = match kind {
        "runtime" => StrategyConsumptionKind::Runtime,
        "chainlink" => StrategyConsumptionKind::Chainlink,
        _ => return None,
    };
    Some((actor, kind))
}

fn append_journal_error(target: &mut Option<String>, error: String) {
    match target {
        Some(existing) => {
            existing.push_str("; ");
            existing.push_str(&error);
        }
        None => *target = Some(error),
    }
}

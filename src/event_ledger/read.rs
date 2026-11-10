use super::*;

use super::rotation::system_time_ms;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventLedgerPathSignature {
    path: PathBuf,
    len_bytes: u64,
    modified_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct HistoryLedgerCache {
    archive_paths_initialized: bool,
    archive_dir_modified_ms: Option<u64>,
    archive_paths: Vec<PathBuf>,
    signatures: Vec<EventLedgerPathSignature>,
    rows: Vec<CanonicalEventRecord>,
}

#[derive(Debug, Clone)]
pub struct HistoryState {
    pub(crate) machine_history: MachineHistoryStatus,
    pub(crate) rows: Arc<Vec<CanonicalEventRecord>>,
}

impl Default for HistoryState {
    fn default() -> Self {
        Self {
            machine_history: MachineHistoryStatus {
                run_id: None,
                active_config_hash: None,
                latest_event_id: None,
                latest_event_ts_ms: None,
                archive_count: 0,
            },
            rows: Arc::new(Vec::new()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryRuntime {
    inner: Arc<HistoryRuntimeInner>,
}

#[derive(Debug)]
struct HistoryRuntimeInner {
    cache: Mutex<HistoryLedgerCache>,
    state_tx: watch::Sender<HistoryState>,
    bootstrapped: AtomicBool,
}

static HISTORY_RUNTIME: OnceLock<HistoryRuntime> = OnceLock::new();

impl HistoryRuntime {
    fn new() -> Self {
        let cache = HistoryLedgerCache::default();
        let seed = HistoryState::default();
        let (state_tx, _) = watch::channel(seed);
        Self {
            inner: Arc::new(HistoryRuntimeInner {
                cache: Mutex::new(cache),
                state_tx,
                bootstrapped: AtomicBool::new(false),
            }),
        }
    }

    pub fn snapshot(&self) -> HistoryState {
        self.ensure_bootstrapped();
        self.inner.state_tx.borrow().clone()
    }

    fn publish_snapshot(&self, next: HistoryState) -> HistoryState {
        let current = self.inner.state_tx.borrow().clone();
        if !history_state_equivalent(&current, &next) {
            let _ = self.inner.state_tx.send_replace(next.clone());
        }
        next
    }

    fn refresh_once(&self) -> HistoryState {
        let mut cache = self
            .inner
            .cache
            .lock()
            .expect("history cache lock poisoned");
        let next = refresh_history_snapshot(&mut cache);
        self.publish_snapshot(next)
    }

    fn ensure_bootstrapped(&self) {
        if self.inner.bootstrapped.load(Ordering::Acquire) {
            return;
        }
        if self
            .inner
            .bootstrapped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let _ = self.refresh_once();
    }

    fn record_append(&self, active_path: &Path, record: &CanonicalEventRecord, rotated: bool) {
        if !self.inner.bootstrapped.load(Ordering::Acquire) {
            self.ensure_bootstrapped();
            return;
        }

        let mut cache = self
            .inner
            .cache
            .lock()
            .expect("history cache lock poisoned");
        if rotated {
            refresh_archive_paths(&mut cache);
            let paths = event_read_paths(&cache);
            cache.rows = load_events_uncached(&paths);
            cache.signatures = event_read_signatures(&paths);
        } else {
            if !cache.archive_paths_initialized {
                refresh_archive_paths(&mut cache);
            }
            cache.rows.push(record.clone());
            refresh_active_signature(&mut cache, active_path);
        }
        let next = build_history_state(&cache, RunIdProvider::current());
        self.publish_snapshot(next);
    }
}

fn history_runtime() -> &'static HistoryRuntime {
    HISTORY_RUNTIME.get_or_init(HistoryRuntime::new)
}

pub fn history_state_snapshot() -> HistoryState {
    history_runtime().snapshot()
}

pub(crate) fn record_appended_event(
    record: &CanonicalEventRecord,
    active_path: &Path,
    rotated: bool,
) {
    history_runtime().record_append(active_path, record, rotated);
}

pub fn load_events() -> Vec<CanonicalEventRecord> {
    history_state_snapshot().rows.as_ref().clone()
}

pub fn latest_event() -> Option<CanonicalEventRecord> {
    history_state_snapshot().rows.last().cloned()
}

pub fn latest_config_hash_after() -> Option<String> {
    let event = history_state_snapshot()
        .rows
        .iter()
        .rev()
        .find(|record| record.event_type == CanonicalEventType::ConfigChange)
        .cloned()?;
    event
        .refs
        .get("config_hash_after")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn refresh_history_snapshot(cache: &mut HistoryLedgerCache) -> HistoryState {
    refresh_event_ledger_cache(cache);
    build_history_state(cache, RunIdProvider::current())
}

fn refresh_event_ledger_cache(cache: &mut HistoryLedgerCache) {
    refresh_archive_paths(cache);
    let paths = event_read_paths(cache);
    let signatures = event_read_signatures(&paths);
    if cache.signatures == signatures {
        return;
    }
    if let Some(appended_rows) = incremental_active_ledger_rows(&cache.signatures, &signatures) {
        cache.rows.extend(appended_rows);
        cache.signatures = signatures;
        return;
    }
    cache.rows = load_events_uncached(&paths);
    cache.signatures = signatures;
}

fn refresh_archive_paths(cache: &mut HistoryLedgerCache) {
    let archive_dir_modified_ms = file_modified_ms(Path::new(EVENTS_ARCHIVE_DIR));
    if cache.archive_paths_initialized && cache.archive_dir_modified_ms == archive_dir_modified_ms {
        return;
    }
    cache.archive_paths = scan_archive_paths();
    cache.archive_dir_modified_ms = archive_dir_modified_ms;
    cache.archive_paths_initialized = true;
}

fn event_read_paths(cache: &HistoryLedgerCache) -> Vec<PathBuf> {
    let mut paths = cache.archive_paths.clone();
    paths.push(PathBuf::from(EVENTS_JSONL_PATH));
    paths
}

fn load_events_uncached(paths: &[PathBuf]) -> Vec<CanonicalEventRecord> {
    let mut rows = Vec::new();
    for path in paths {
        let file_rows = if path.extension() == Some(OsStr::new("gz")) {
            load_gzip_jsonl_events(path)
        } else {
            load_jsonl_events(path)
        };
        rows.extend(file_rows);
    }
    rows
}

fn event_read_signatures(paths: &[PathBuf]) -> Vec<EventLedgerPathSignature> {
    paths.iter().map(|path| path_signature(path)).collect()
}

fn refresh_active_signature(cache: &mut HistoryLedgerCache, active_path: &Path) {
    let active_signature = path_signature(active_path);
    let expected_signatures = cache.archive_paths.len().saturating_add(1);
    if cache.signatures.len() != expected_signatures {
        cache.signatures = event_read_signatures(&event_read_paths(cache));
        return;
    }
    if let Some(last) = cache.signatures.last_mut() {
        *last = active_signature;
    } else {
        cache.signatures = vec![active_signature];
    }
}

fn path_signature(path: &Path) -> EventLedgerPathSignature {
    let metadata = fs::metadata(path).ok();
    EventLedgerPathSignature {
        path: path.to_path_buf(),
        len_bytes: metadata.as_ref().map(|meta| meta.len()).unwrap_or(0),
        modified_ms: metadata
            .as_ref()
            .and_then(|meta| meta.modified().ok())
            .and_then(system_time_ms),
    }
}

fn incremental_active_ledger_rows(
    previous: &[EventLedgerPathSignature],
    next: &[EventLedgerPathSignature],
) -> Option<Vec<CanonicalEventRecord>> {
    let active_path = Path::new(EVENTS_JSONL_PATH);
    if previous.len() != next.len() || previous.is_empty() {
        return None;
    }
    let previous_active = previous.last()?;
    let next_active = next.last()?;
    if previous_active.path != PathBuf::from(EVENTS_JSONL_PATH)
        || next_active.path != PathBuf::from(EVENTS_JSONL_PATH)
    {
        return None;
    }
    if previous[..previous.len().saturating_sub(1)] != next[..next.len().saturating_sub(1)] {
        return None;
    }
    if next_active.len_bytes < previous_active.len_bytes {
        return None;
    }
    if next_active.len_bytes == previous_active.len_bytes {
        return Some(Vec::new());
    }
    Some(load_jsonl_events_from_offset(
        active_path,
        previous_active.len_bytes,
    ))
}

fn scan_archive_paths() -> Vec<PathBuf> {
    let mut paths = match fs::read_dir(EVENTS_ARCHIVE_DIR) {
        Ok(entries) => entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension() == Some(OsStr::new("gz")))
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths
}

fn current_active_config_hash_from_rows(rows: &[CanonicalEventRecord]) -> Option<String> {
    rows.iter().rev().find_map(|event| match event.event_type {
        CanonicalEventType::ConfigChange => event
            .refs
            .get("config_hash_after")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        CanonicalEventType::RuntimeStart => event
            .refs
            .get("config_hash")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                event
                    .context
                    .get("config_hash")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            }),
        _ => None,
    })
}

fn build_history_state(cache: &HistoryLedgerCache, current_run_id: Option<String>) -> HistoryState {
    let rows = Arc::new(cache.rows.clone());
    let latest = rows.last();
    let machine_history = MachineHistoryStatus {
        run_id: current_run_id.clone(),
        active_config_hash: current_active_config_hash_from_rows(rows.as_ref()),
        latest_event_id: latest.map(|event| event.event_id.clone()),
        latest_event_ts_ms: latest.map(|event| event.ts_ms),
        archive_count: cache.archive_paths.len(),
    };
    HistoryState {
        machine_history,
        rows,
    }
}

fn machine_history_status_equivalent(
    left: &MachineHistoryStatus,
    right: &MachineHistoryStatus,
) -> bool {
    left.run_id == right.run_id
        && left.active_config_hash == right.active_config_hash
        && left.latest_event_id == right.latest_event_id
        && left.latest_event_ts_ms == right.latest_event_ts_ms
        && left.archive_count == right.archive_count
}

fn history_state_equivalent(left: &HistoryState, right: &HistoryState) -> bool {
    machine_history_status_equivalent(&left.machine_history, &right.machine_history)
        && left.rows.len() == right.rows.len()
}

pub(crate) fn load_jsonl_events(path: &Path) -> Vec<CanonicalEventRecord> {
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    reader
        .lines()
        .map_while(std::result::Result::ok)
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_slice::<CanonicalEventRecord>(line.as_bytes()).ok())
        .filter_map(|record| validate_event(&record).ok().map(|_| record))
        .collect()
}

pub(crate) fn load_jsonl_events_from_offset(path: &Path, offset: u64) -> Vec<CanonicalEventRecord> {
    use std::io::{Seek, SeekFrom};

    let Ok(mut file) = fs::File::open(path) else {
        return Vec::new();
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return Vec::new();
    }
    let reader = BufReader::new(file);
    reader
        .lines()
        .map_while(std::result::Result::ok)
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_slice::<CanonicalEventRecord>(line.as_bytes()).ok())
        .filter_map(|record| validate_event(&record).ok().map(|_| record))
        .collect()
}

pub(crate) fn load_gzip_jsonl_events(path: &Path) -> Vec<CanonicalEventRecord> {
    let Ok(output) = Command::new("gzip").arg("-cd").arg(path).output() else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_slice::<CanonicalEventRecord>(line.as_bytes()).ok())
        .filter_map(|record| validate_event(&record).ok().map(|_| record))
        .collect()
}

pub(crate) fn file_modified_ms(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .ok()
        .and_then(|meta| meta.modified().ok())
        .and_then(system_time_ms)
}

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use polymarket_hft::capture::scope::required_bbwick_symbols_for_capture;
use polymarket_hft::config::AppConfig;
use polymarket_hft::execution::supervisor::OracleRuntimeStatus;
use polymarket_hft::observability::action::ActionEvent;

const BBWICK_RUNTIME_STATUS_EMIT_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BbwickOraclePhase {
    WaitingForOracle,
    Ready,
    Degraded,
}

impl BbwickOraclePhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForOracle => "waiting_for_oracle",
            Self::Ready => "ready",
            Self::Degraded => "degraded",
        }
    }
}

pub(crate) struct BbwickOracleRuntimeState {
    pub(crate) phase: BbwickOraclePhase,
    pub(crate) connected: bool,
    pub(crate) required_symbols: Vec<String>,
    pub(crate) ready_symbols: Vec<String>,
    pub(crate) canonical_symbol_cache: HashMap<String, Option<String>>,
    last_tick_ts_ms_by_symbol: HashMap<String, u64>,
    last_tick_ingest_mono_ns_by_symbol: HashMap<String, u64>,
    status_symbols: Vec<String>,
    pub(crate) last_error: Option<String>,
    last_status_emit_ms: u64,
    last_status_emit_sig: Option<u64>,
}

impl BbwickOracleRuntimeState {
    pub(crate) fn new(cfg: &AppConfig) -> Self {
        Self {
            phase: BbwickOraclePhase::WaitingForOracle,
            connected: false,
            required_symbols: required_bbwick_symbols_for_capture(cfg),
            ready_symbols: Vec::new(),
            canonical_symbol_cache: HashMap::new(),
            last_tick_ts_ms_by_symbol: HashMap::new(),
            last_tick_ingest_mono_ns_by_symbol: HashMap::new(),
            status_symbols: Vec::new(),
            last_error: None,
            last_status_emit_ms: 0,
            last_status_emit_sig: None,
        }
    }

    pub(crate) fn supervisor_status(&self) -> OracleRuntimeStatus {
        OracleRuntimeStatus {
            phase: self.phase.as_str().to_string(),
            connected: self.connected,
            last_error: self.last_error.clone(),
        }
    }

    pub(crate) fn refresh_readiness(
        &mut self,
        max_oracle_staleness_ms: u64,
        now_ingest_mono_ns: u64,
    ) -> bool {
        let mut ready_symbols: Vec<String> = self
            .required_symbols
            .iter()
            .filter(|symbol| {
                self.last_tick_ingest_mono_ns_by_symbol
                    .get(symbol.as_str())
                    .copied()
                    .is_some_and(|tick_ns| {
                        mono_age_ms(now_ingest_mono_ns, tick_ns) <= max_oracle_staleness_ms
                    })
            })
            .cloned()
            .collect();
        ready_symbols.sort();
        ready_symbols.dedup();

        let next_phase = if !self.connected {
            if self.last_error.is_some() {
                BbwickOraclePhase::Degraded
            } else {
                BbwickOraclePhase::WaitingForOracle
            }
        } else if self.required_symbols.is_empty()
            || ready_symbols.len() == self.required_symbols.len()
        {
            BbwickOraclePhase::Ready
        } else if ready_symbols.is_empty() {
            BbwickOraclePhase::WaitingForOracle
        } else {
            BbwickOraclePhase::Degraded
        };

        let changed = self.phase != next_phase || self.ready_symbols != ready_symbols;
        self.phase = next_phase;
        self.ready_symbols = ready_symbols;
        changed
    }

    pub(crate) fn mark_connected(&mut self, connected: bool) -> bool {
        if self.connected == connected {
            return false;
        }
        self.connected = connected;
        true
    }

    pub(crate) fn note_tick(&mut self, symbol: &str, ts_ms: u64, ingest_mono_ns: u64) {
        if let Some(current) = self.last_tick_ts_ms_by_symbol.get_mut(symbol) {
            *current = ts_ms;
        } else {
            self.last_tick_ts_ms_by_symbol
                .insert(symbol.to_string(), ts_ms);
        }
        if let Some(current) = self.last_tick_ingest_mono_ns_by_symbol.get_mut(symbol) {
            *current = ingest_mono_ns;
        } else {
            self.last_tick_ingest_mono_ns_by_symbol
                .insert(symbol.to_string(), ingest_mono_ns);
        }
        self.remember_status_symbol(symbol);
        self.last_error = None;
    }

    pub(crate) fn note_runtime_error(&mut self, _now_ms: u64, err: impl Into<String>) {
        self.last_error = Some(err.into());
        self.phase = BbwickOraclePhase::Degraded;
    }

    pub(crate) fn reset_oracle_epoch_state(&mut self) {
        self.ready_symbols.clear();
        self.last_tick_ts_ms_by_symbol.clear();
        self.last_tick_ingest_mono_ns_by_symbol.clear();
        self.status_symbols = self.required_symbols.clone();
        self.status_symbols.sort();
        self.status_symbols.dedup();
    }

    fn remember_status_symbol(&mut self, symbol: &str) {
        if self
            .status_symbols
            .binary_search_by(|current| current.as_str().cmp(symbol))
            .is_err()
        {
            self.status_symbols.insert(
                self.status_symbols
                    .binary_search_by(|current| current.as_str().cmp(symbol))
                    .unwrap_err(),
                symbol.to_string(),
            );
        }
    }

    fn symbol_last_tick_rows(&self) -> Vec<(String, u64)> {
        self.status_symbols
            .iter()
            .filter_map(|symbol| {
                self.last_tick_ts_ms_by_symbol
                    .get(symbol.as_str())
                    .map(|ts_ms| (symbol.clone(), *ts_ms))
            })
            .collect()
    }

    fn symbol_last_tick_age_rows(&self, now_ingest_mono_ns: u64) -> Vec<(String, u64)> {
        self.status_symbols
            .iter()
            .filter_map(|symbol| {
                self.last_tick_ingest_mono_ns_by_symbol
                    .get(symbol.as_str())
                    .map(|tick_ns| (symbol.clone(), mono_age_ms(now_ingest_mono_ns, *tick_ns)))
            })
            .collect()
    }

    fn status_emit_sig(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.phase.as_str().hash(&mut hasher);
        self.connected.hash(&mut hasher);
        self.required_symbols.hash(&mut hasher);
        self.ready_symbols.hash(&mut hasher);
        self.last_error.hash(&mut hasher);
        for symbol in &self.status_symbols {
            symbol.hash(&mut hasher);
            self.last_tick_ts_ms_by_symbol
                .get(symbol.as_str())
                .copied()
                .unwrap_or_default()
                .hash(&mut hasher);
            self.last_tick_ingest_mono_ns_by_symbol
                .get(symbol.as_str())
                .copied()
                .unwrap_or_default()
                .hash(&mut hasher);
        }
        hasher.finish()
    }

    fn status_event(&self, ts_ms: u64, now_ingest_mono_ns: u64) -> ActionEvent {
        ActionEvent::OracleRuntimeStatus {
            ts_ms,
            phase: self.phase.as_str().to_string(),
            connected: self.connected,
            required_symbols: self.required_symbols.clone(),
            ready_symbols: self.ready_symbols.clone(),
            symbol_last_tick_ts_ms: self.symbol_last_tick_rows(),
            symbol_last_tick_age_ms: self.symbol_last_tick_age_rows(now_ingest_mono_ns),
            last_error: self.last_error.clone(),
        }
    }

    pub(crate) fn emit_status(
        &mut self,
        obs: &polymarket_hft::observability::Observability,
        now_ms: u64,
        now_ingest_mono_ns: u64,
        force: bool,
    ) {
        if !force
            && now_ms.saturating_sub(self.last_status_emit_ms)
                < BBWICK_RUNTIME_STATUS_EMIT_INTERVAL_MS
        {
            return;
        }
        let status_sig = self.status_emit_sig();
        if force && self.last_status_emit_sig == Some(status_sig) {
            return;
        }
        self.last_status_emit_ms = now_ms;
        self.last_status_emit_sig = Some(status_sig);
        obs.emit_ui(self.status_event(now_ms, now_ingest_mono_ns));
    }
}

pub(crate) fn mono_age_ms(now_ns: u64, then_ns: u64) -> u64 {
    now_ns.saturating_sub(then_ns).saturating_add(999_999) / 1_000_000
}

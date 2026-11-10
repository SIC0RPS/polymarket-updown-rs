mod trade_flow;

pub(crate) use trade_flow::AggregateTrade;
pub use trade_flow::{OracleTradeFlowCoverage, OracleTradeFlowSnapshot};

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use canonical_ohlc_arrow::{read_tail, validate_bars, OhlcBar, OhlcFileTransaction};
use canonical_ohlc_resync::{
    build_resync_http_client, ensure_window, fetch_binance_klines_range,
    fetch_binance_server_time_ms, latest_closed_open_ts, parse_interval_ms,
    validate_continuity, validate_range_bars, EnsureConfig, EnsureStatus, ResyncClient,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::ids::MarketDataKeyRegistry;
use crate::oracle_state_updates::now_ms;
use crate::wire::StepFrame;

const HISTORY_REST_TOP_UP_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OracleCandleWindow {
    pub symbol: String,
    pub timeframe: String,
    pub revision: u64,
    pub received_mono_ns: u64,
    pub published_mono_ns: u64,
    pub bars: Vec<OhlcBar>,
}

impl OracleCandleWindow {
    pub fn validate(&self, max_bars: usize) -> Result<()> {
        anyhow::ensure!(!self.symbol.is_empty(), "candle state has no symbol");
        anyhow::ensure!(
            self.bars.len() <= max_bars,
            "candle state exceeds configured history limit"
        );
        anyhow::ensure!(
            self.bars.is_empty() == (self.revision == 0),
            "invalid candle state identity"
        );
        let interval_ms = history_interval_ms(&self.timeframe)?;
        validate_history_bars(&self.bars, interval_ms)
    }
}

#[derive(Debug, Clone)]
pub struct OracleHistoryView {
    registry: Arc<MarketDataKeyRegistry>,
    windows: Vec<Arc<OracleCandleWindow>>,
}

impl OracleHistoryView {
    pub fn window(&self, symbol: &str, timeframe: &str) -> Option<&OracleCandleWindow> {
        let symbol_id = self.registry.symbol_id(symbol)?;
        let timeframe_id = self.registry.timeframe_id(timeframe)?;
        self.windows
            .get(self.registry.step_slot(symbol_id, timeframe_id))
            .map(Arc::as_ref)
    }

    pub fn window_arcs_for_symbol(
        &self,
        symbol: &str,
    ) -> Option<&[Arc<OracleCandleWindow>]> {
        let symbol_id = self.registry.symbol_id(symbol)?;
        let start = symbol_id.index() * self.registry.timeframe_count();
        Some(&self.windows[start..start + self.registry.timeframe_count()])
    }

    pub fn windows_for_symbol(
        &self,
        symbol: &str,
    ) -> impl Iterator<Item = &OracleCandleWindow> {
        self.window_arcs_for_symbol(symbol)
            .unwrap_or(&[])
            .iter()
            .map(Arc::as_ref)
    }
}

#[derive(Debug)]
pub struct OracleHistory {
    registry: Arc<MarketDataKeyRegistry>,
    data_root: PathBuf,
    max_bars: usize,
    monotonic_origin: Instant,
    state: RwLock<Arc<OracleHistoryView>>,
    flow: trade_flow::FlowHistory,
    persistence_failed: AtomicBool,
}

impl OracleHistory {
    pub fn new(
        data_root: PathBuf,
        mut active_symbols: Vec<String>,
        mut step_timeframes: Vec<String>,
        max_bars: usize,
        monotonic_origin: Instant,
    ) -> Result<Self> {
        for symbol in &mut active_symbols {
            *symbol = symbol.trim().to_ascii_uppercase();
        }
        active_symbols.sort();
        active_symbols.dedup();
        anyhow::ensure!(
            !active_symbols.is_empty() && active_symbols.iter().all(|symbol| !symbol.is_empty()),
            "history requires nonempty symbols"
        );
        for timeframe in &mut step_timeframes {
            *timeframe = timeframe.trim().to_ascii_lowercase();
            history_interval_ms(timeframe)?;
        }
        step_timeframes.sort();
        step_timeframes.dedup();
        anyhow::ensure!(!step_timeframes.is_empty(), "history requires timeframes");
        let registry = Arc::new(MarketDataKeyRegistry::new(
            active_symbols,
            step_timeframes,
        )?);
        let mut windows = Vec::with_capacity(registry.step_slot_count());
        for symbol in registry.symbols() {
            for timeframe in registry.timeframes() {
                windows.push(Arc::new(OracleCandleWindow {
                    symbol: symbol.to_string(),
                    timeframe: timeframe.to_string(),
                    revision: 0,
                    received_mono_ns: 0,
                    published_mono_ns: 0,
                    bars: Vec::new(),
                }));
            }
        }
        let state = RwLock::new(Arc::new(OracleHistoryView {
            registry: registry.clone(),
            windows,
        }));
        Ok(Self {
            flow: trade_flow::FlowHistory::new(registry.symbol_count()),
            registry,
            data_root,
            max_bars: max_bars.max(1),
            monotonic_origin,
            state,
            persistence_failed: AtomicBool::new(false),
        })
    }

    pub fn load_retained(
        data_root: PathBuf,
        active_symbols: Vec<String>,
        step_timeframes: Vec<String>,
        max_bars: usize,
        monotonic_origin: Instant,
    ) -> Result<Self> {
        let history = Self::new(
            data_root,
            active_symbols,
            step_timeframes,
            max_bars,
            monotonic_origin,
        )?;
        for symbol in history.registry.symbols() {
            for timeframe in history.registry.timeframes() {
                let path = history_arrow_path(&history.data_root, symbol, timeframe);
                if path.exists() {
                    let bars = read_tail(&path, history.max_bars)
                        .with_context(|| format!("load retained OHLC {}", path.display()))?;
                    history.admit_bars(symbol, timeframe, &bars, 0)?;
                }
            }
        }
        Ok(history)
    }

    pub fn key_registry(&self) -> Arc<MarketDataKeyRegistry> {
        self.registry.clone()
    }

    pub fn monotonic_origin(&self) -> Instant {
        self.monotonic_origin
    }

    pub fn current_mono_ns(&self) -> u64 {
        self.monotonic_origin
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64
    }

    pub fn persistence_failed(&self) -> bool {
        self.persistence_failed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_persistence_failed(&self) {
        self.persistence_failed.store(true, Ordering::Release);
    }

    pub fn admit_step(&self, step: &StepFrame, received_mono_ns: u64) -> Result<bool> {
        let bar = &step.bar;
        self.admit_bars(
            &step.symbol,
            &step.timeframe,
            &[OhlcBar {
                ts_ms: i64::try_from(bar.ts_ms).context("candle timestamp exceeds i64")?,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
            }],
            received_mono_ns,
        )
    }

    pub fn admit_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        bars: &[OhlcBar],
        received_mono_ns: u64,
    ) -> Result<bool> {
        let symbol_id = self.registry.symbol_id(symbol)
            .ok_or_else(|| anyhow::anyhow!("unknown OHLC symbol {symbol}"))?;
        let timeframe_id = self.registry.timeframe_id(timeframe)
            .ok_or_else(|| anyhow::anyhow!("unknown OHLC timeframe {timeframe}"))?;
        let interval_ms = history_interval_ms(timeframe)?;
        validate_history_bars(bars, interval_ms)?;
        if bars.is_empty() {
            return Ok(false);
        }
        let bars = &bars[bars.len().saturating_sub(self.max_bars)..];
        let slot = self.registry.step_slot(symbol_id, timeframe_id);
        let mut state = self.state.write()
            .map_err(|_| anyhow::anyhow!("OHLC memory lock poisoned"))?;
        let current = &state.windows[slot];
        if let (Some(first), Some(last)) = (current.bars.first(), current.bars.last()) {
            if bars[0].ts_ms >= first.ts_ms && bars[bars.len() - 1].ts_ms <= last.ts_ms {
                return Ok(false);
            }
        }
        let mut merged = Vec::with_capacity(current.bars.len() + bars.len());
        let (mut old, mut incoming) = (0, 0);
        while old < current.bars.len() && incoming < bars.len() {
            let current_bar = current.bars[old];
            let incoming_bar = bars[incoming];
            if current_bar.ts_ms <= incoming_bar.ts_ms {
                merged.push(current_bar);
                old += 1;
                incoming += usize::from(current_bar.ts_ms == incoming_bar.ts_ms);
            } else {
                merged.push(incoming_bar);
                incoming += 1;
            }
        }
        merged.extend_from_slice(&current.bars[old..]);
        merged.extend_from_slice(&bars[incoming..]);
        if merged.len() > self.max_bars {
            merged.drain(..merged.len() - self.max_bars);
        }
        validate_continuity(&merged, interval_ms)?;
        if merged == current.bars {
            return Ok(false);
        }
        let revision = current.revision.checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("OHLC memory revision exhausted"))?;
        let window = Arc::new(OracleCandleWindow {
            symbol: current.symbol.clone(),
            timeframe: current.timeframe.clone(),
            revision,
            received_mono_ns,
            published_mono_ns: self.current_mono_ns(),
            bars: merged,
        });
        let mut next = (**state).clone();
        next.windows[slot] = window;
        *state = Arc::new(next);
        Ok(true)
    }

    pub fn validate_consumed_state<T: AsRef<str>>(
        previous: Option<&[Arc<OracleCandleWindow>]>,
        state: &[Arc<OracleCandleWindow>],
        symbol: &str,
        canonical_timeframes: impl IntoIterator<Item = T>,
        max_bars: usize,
        consumed_mono_ns: u64,
    ) -> Result<()> {
        anyhow::ensure!(consumed_mono_ns > 0, "missing candle consumption clock");
        anyhow::ensure!(
            previous.is_none_or(|prior| prior.len() == state.len()),
            "consumed candle key set changed"
        );
        let mut count = 0;
        for timeframe in canonical_timeframes {
            let window = state.get(count).context("missing consumed candle key")?;
            anyhow::ensure!(
                window.symbol == symbol && window.timeframe == timeframe.as_ref()
                    && (count == 0 || state[count - 1].timeframe < window.timeframe),
                "invalid consumed candle key or ordering"
            );
            anyhow::ensure!(
                window.received_mono_ns <= window.published_mono_ns
                    && window.published_mono_ns <= consumed_mono_ns,
                "invalid consumed candle clocks"
            );
            let prior = previous.map(|prior| &prior[count]);
            count += 1;
            if prior.is_some_and(|prior| Arc::ptr_eq(prior, window)) {
                continue;
            }
            window.validate(max_bars)?;
            if let Some(prior) = prior {
                anyhow::ensure!(
                    window.symbol == prior.symbol && window.timeframe == prior.timeframe,
                    "previous consumed candle key differs"
                );
                anyhow::ensure!(
                    window.revision >= prior.revision
                        && window.published_mono_ns >= prior.published_mono_ns,
                    "consumed candle identity regressed"
                );
                if window.revision == prior.revision {
                    anyhow::ensure!(
                        window.as_ref() == prior.as_ref(),
                        "equal candle revision has different contents"
                    );
                }
                if let Some(last) = prior.bars.last() {
                    anyhow::ensure!(
                        window.bars.last().is_some_and(|bar| bar.ts_ms >= last.ts_ms),
                        "consumed candle tail regressed"
                    );
                }
            }
        }
        anyhow::ensure!(
            count > 0 && count == state.len(),
            "consumed candle key set differs from configured keys"
        );
        Ok(())
    }

    pub fn resolve_consumed_state<T: AsRef<str>>(
        previous: Option<&Arc<[Arc<OracleCandleWindow>]>>,
        changed: Option<&Arc<[Arc<OracleCandleWindow>]>>,
        symbol: &str,
        canonical_timeframes: impl IntoIterator<Item = T>,
        max_bars: usize,
        previous_consumed_mono_ns: u64,
        consumed_mono_ns: u64,
    ) -> Result<Arc<[Arc<OracleCandleWindow>]>> {
        anyhow::ensure!(
            consumed_mono_ns >= previous_consumed_mono_ns,
            "candle consumption clock regressed"
        );
        let state = changed.or(previous).context("missing initial consumed candle state")?;
        Self::validate_consumed_state(
            previous.map(|state| state.as_ref()),
            state,
            symbol,
            canonical_timeframes,
            max_bars,
            consumed_mono_ns,
        )?;
        if let Some(previous) = previous {
            if previous.iter().zip(state.iter()).all(|(a, b)| a.revision == b.revision) {
                return Ok(Arc::clone(previous));
            }
            return Ok(previous.iter().zip(state.iter()).map(|(prior, next)| {
                Arc::clone(if prior.revision == next.revision { prior } else { next })
            }).collect());
        }
        Ok(Arc::clone(state))
    }

    pub fn apply_consumed_state(
        &self,
        symbol: &str,
        windows: &[Arc<OracleCandleWindow>],
        consumed_mono_ns: u64,
    ) -> Result<()> {
        let mut state = self.state.write()
            .map_err(|_| anyhow::anyhow!("OHLC memory lock poisoned"))?;
        let prior = state.window_arcs_for_symbol(symbol)
            .context("unknown consumed candle symbol")?;
        Self::validate_consumed_state(
            Some(prior),
            windows,
            symbol,
            self.registry.timeframes(),
            self.max_bars,
            consumed_mono_ns,
        )?;
        if prior.iter().zip(windows).all(|(prior, next)| Arc::ptr_eq(prior, next)) {
            return Ok(());
        }
        let symbol_id = self.registry.symbol_id(symbol)
            .context("unknown consumed candle symbol")?;
        let start = symbol_id.index() * self.registry.timeframe_count();
        let mut next = (**state).clone();
        for (slot, window) in windows.iter().enumerate() {
            next.windows[start + slot] = Arc::clone(window);
        }
        *state = Arc::new(next);
        Ok(())
    }

    pub fn persist(&self, symbol: &str, timeframe: &str) -> Result<()> {
        let result = (|| {
            anyhow::ensure!(
                self.registry.symbol_id(symbol).is_some()
                    && self.registry.timeframe_id(timeframe).is_some(),
                "unknown OHLC persistence key {symbol}:{timeframe}"
            );
            let path = history_arrow_path(&self.data_root, symbol, timeframe);
            let mut transaction = OhlcFileTransaction::open(&path)
                .context("open OHLC persistence transaction")?;
            // Pin after the file lock so an older writer cannot replace a newer window.
            let view = self.pin()?;
            let window = view.window(symbol, timeframe)
                .ok_or_else(|| anyhow::anyhow!("unknown OHLC persistence key {symbol}:{timeframe}"))?;
            if !window.bars.is_empty() {
                transaction.write(&window.bars).context("persist OHLC memory window")?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.mark_persistence_failed();
        }
        result
    }
}

fn history_interval_ms(timeframe: &str) -> Result<i64> {
    parse_interval_ms(timeframe)
        .filter(|interval_ms| *interval_ms > 0)
        .ok_or_else(|| anyhow::anyhow!("unsupported bbwick history timeframe {timeframe}"))
}

fn validate_history_bars(bars: &[OhlcBar], interval_ms: i64) -> Result<()> {
    if bars.is_empty() {
        return Ok(());
    }
    validate_bars(bars)?;
    validate_continuity(bars, interval_ms)?;
    anyhow::ensure!(
        bars.iter().all(|bar| bar.ts_ms >= 0 && bar.ts_ms % interval_ms == 0),
        "OHLC candle timestamp is not aligned to its timeframe"
    );
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct HistoryRuntimeConfig {
    pub data_root: PathBuf,
    pub max_bars: usize,
    pub rest_base_url: String,
    pub active_symbols: Vec<String>,
    pub step_timeframes: Vec<String>,
}

pub(crate) fn spawn_history_runtime(
    config: HistoryRuntimeConfig,
    history: Arc<OracleHistory>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_history_runtime(config, history, shutdown).await {
            tracing::error!(
                target: "engine.oracle",
                error = %err,
                "bbwick_history_runtime_exited"
            );
        }
    })
}

async fn run_history_runtime(
    config: HistoryRuntimeConfig,
    history: Arc<OracleHistory>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let client = build_resync_http_client();
    let config = &config;
    let history = &history;
    let client = &client;
    let mut pending = FuturesUnordered::new();
    let mut active = HashSet::new();
    let mut stopping = *shutdown.borrow();
    let mut rest_top_up = tokio::time::interval_at(
        tokio::time::Instant::now() + HISTORY_REST_TOP_UP_INTERVAL,
        HISTORY_REST_TOP_UP_INTERVAL,
    );
    rest_top_up.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if stopping && pending.is_empty() {
            return Ok(());
        }
        tokio::select! {
            result = shutdown.changed(), if !stopping => {
                stopping = result.is_err() || *shutdown.borrow();
            }
            _ = rest_top_up.tick(), if !stopping => {
                for symbol in history.registry.symbols() {
                    for timeframe in history.registry.timeframes() {
                        let key = (symbol, timeframe);
                        if active.insert(key) {
                            pending.push(async move {
                                (key, repair_history_window(config, history, client, key.0, key.1).await)
                            });
                        }
                    }
                }
            }
            Some((key, result)) = pending.next(), if !pending.is_empty() => {
                active.remove(&key);
                if let Err(err) = result {
                    tracing::warn!(
                        target: "engine.oracle",
                        symbol = key.0,
                        timeframe = key.1,
                        error = %err,
                        "bbwick_history_periodic_top_up_failed"
                    );
                }
            }
        }
    }
}

async fn repair_history_window(
    config: &HistoryRuntimeConfig,
    history: &Arc<OracleHistory>,
    client: &ResyncClient,
    symbol: &str,
    timeframe: &str,
) -> Result<()> {
    let interval_ms = history_interval_ms(timeframe)?;
    let server_time_ms = fetch_binance_server_time_ms(client, &config.rest_base_url).await?;
    let latest_closed = latest_closed_open_ts(server_time_ms, interval_ms);
    if latest_closed <= 0 {
        return Ok(());
    }
    let intervals = i64::try_from(history.max_bars.saturating_sub(1))
        .context("OHLC history limit exceeds i64")?;
    let earliest = latest_closed.saturating_sub(interval_ms.saturating_mul(intervals)).max(0);
    let start = {
        let view = history.pin()?;
        let window = view.window(symbol, timeframe)
            .ok_or_else(|| anyhow::anyhow!("unknown OHLC repair key {symbol}:{timeframe}"))?;
        match window.bars.last() {
            Some(last) if window.bars.len() >= history.max_bars => {
                last.ts_ms.saturating_add(interval_ms).max(earliest)
            }
            _ => earliest,
        }
    };
    if start <= latest_closed {
        let bars = fetch_binance_klines_range(
            client,
            &config.rest_base_url,
            symbol,
            timeframe,
            start,
            latest_closed,
            interval_ms,
            server_time_ms,
        ).await?;
        let expected_count = latest_closed
            .checked_sub(start)
            .and_then(|span| span.checked_div(interval_ms))
            .and_then(|count| count.checked_add(1))
            .context("OHLC repair range count overflow")?;
        let expected_count = usize::try_from(expected_count)
            .context("OHLC repair range count exceeds usize")?;
        validate_range_bars(&bars, start, latest_closed, interval_ms, expected_count)?;
        history.admit_bars(symbol, timeframe, &bars, history.current_mono_ns())?;
    }
    let writer = history.clone();
    let symbol = symbol.to_string();
    let timeframe = timeframe.to_string();
    match tokio::task::spawn_blocking(move || writer.persist(&symbol, &timeframe)).await {
        Ok(result) => result,
        Err(err) => {
            history.mark_persistence_failed();
            Err(anyhow::anyhow!("OHLC repair persistence task failed: {err}"))
        }
    }
}

pub(crate) async fn ensure_history_windows(
    config: &HistoryRuntimeConfig,
) -> Result<Arc<OracleHistory>> {
    let client = build_resync_http_client();
    let client = &client;
    let mut pending = FuturesUnordered::new();
    for symbol in &config.active_symbols {
        for timeframe in &config.step_timeframes {
            pending.push(async move {
                (
                    symbol,
                    timeframe,
                    ensure_history_window(config, client, symbol, timeframe).await,
                )
            });
        }
    }
    let mut issues = Vec::new();
    while let Some((symbol, timeframe, result)) = pending.next().await {
        if let Err(err) = result {
            issues.push(format!("{symbol} {timeframe} {err}"));
            tracing::warn!(
                target: "engine.oracle",
                error = %err,
                symbol,
                timeframe,
                "bbwick_history_ensure_window_failed"
            );
        }
    }
    anyhow::ensure!(
        issues.is_empty(),
        "bbwick history startup failed: {}",
        issues.join("; ")
    );
    let config = config.clone();
    tokio::task::spawn_blocking(move || {
        OracleHistory::load_retained(
            config.data_root,
            config.active_symbols,
            config.step_timeframes,
            config.max_bars,
            Instant::now(),
        ).map(Arc::new)
    }).await.context("join initial OHLC memory load")?
}

async fn ensure_history_window(
    config: &HistoryRuntimeConfig,
    client: &ResyncClient,
    symbol: &str,
    timeframe: &str,
) -> Result<()> {
    let interval_ms = history_interval_ms(timeframe)?;
    let latest_closed_ts_ms = latest_closed_open_ts(now_ms() as i64, interval_ms);
    if latest_closed_ts_ms <= 0 {
        return Ok(());
    }
    let arrow_path = history_arrow_path(&config.data_root, symbol, timeframe);
    let outcome = ensure_window(
        client,
        EnsureConfig {
            arrow_path: &arrow_path,
            symbol,
            timeframe,
            interval_ms,
            rest_base_url: &config.rest_base_url,
            window: Some(config.max_bars),
            latest_closed_ts_ms: Some(latest_closed_ts_ms),
            fetch_bars: Some(config.max_bars),
            required_bars: Some(config.max_bars),
            max_gap_repairs: 3,
        },
    ).await?;
    match outcome.status {
        EnsureStatus::Ready => trim_arrow_to_limit(&arrow_path, config.max_bars).await,
        EnsureStatus::InsufficientHistory { available, required } => {
            tracing::warn!(
                target: "engine.oracle",
                symbol,
                timeframe,
                available,
                required,
                "bbwick_history_startup_insufficient_history"
            );
            anyhow::bail!("insufficient_history available={available} required={required}")
        }
        EnsureStatus::PausedForResync { reason, next_retry_at_ms, error_kind } => {
            tracing::warn!(
                target: "engine.oracle",
                symbol,
                timeframe,
                reason = %reason,
                next_retry_at_ms,
                ?error_kind,
                "bbwick_history_startup_paused_for_resync"
            );
            anyhow::bail!(
                "paused_for_resync reason={reason} next_retry_at_ms={next_retry_at_ms} error_kind={error_kind:?}"
            )
        }
    }
}

pub(crate) fn history_arrow_path(data_root: &Path, symbol: &str, timeframe: &str) -> PathBuf {
    data_root
        .join(canonical_symbol_dir(symbol))
        .join(timeframe.trim().to_ascii_lowercase())
        .join("ohlc.arrow")
}

fn canonical_symbol_dir(symbol: &str) -> String {
    symbol
        .trim()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase()
}

async fn trim_arrow_to_limit(path: &Path, max_bars: usize) -> Result<()> {
    if max_bars == 0 {
        return Ok(());
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut transaction = OhlcFileTransaction::open(&path)
            .context("open OHLC startup trim transaction")?;
        let bars = transaction.read_tail(max_bars).context("read OHLC startup tail")?;
        if !bars.is_empty() {
            transaction.write(&bars).context("trim OHLC startup history")?;
        }
        Ok::<_, anyhow::Error>(())
    }).await.context("join OHLC startup trim")?
}

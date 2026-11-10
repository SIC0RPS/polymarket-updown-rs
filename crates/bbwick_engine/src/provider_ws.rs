mod aggregate_trade;

use aggregate_trade::{parse_agg_trade_event, AggTradeEvent};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use canonical_ohlc_arrow::OhlcBar;
use futures_util::StreamExt;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::ids::{IdHeartbeatFrame, IdStepFrame, IdTopOfBookIngressFrame, MarketDataKeyRegistry};
use crate::ingest_event::IngestEvent;
use crate::live_lanes::{OracleTapeLane, ProviderQuoteLane};
use crate::market_data::SourceTsKind;
use crate::oracle_history::OracleHistory;
use crate::wire::BarPayload;

static PROVIDER_WS_JITTER_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ProviderWsRuntimeConfig {
    pub ws_url: String,
    pub key_registry: Arc<MarketDataKeyRegistry>,
    pub history: Arc<OracleHistory>,
    pub subscribe_trades: bool,
    pub subscribe_top_of_book: bool,
    pub subscribe_klines: bool,
    pub capture: Option<Arc<dyn ProviderCapture>>,
}

/// Receipt observations only; journal order is local arrival order, not strategy consumption.
#[derive(Debug, Clone, Copy)]
pub struct ProviderObservation<'a> {
    pub kind: &'static str,
    pub recv_wall_ms: u64,
    pub received_mono_ns: u64,
    pub payload: &'a [u8],
    pub detail: Option<&'a str>,
}

pub trait ProviderCapture: Send + Sync {
    fn observe(&self, symbol: Option<&str>, observation: ProviderObservation<'_>);
}

impl std::fmt::Debug for dyn ProviderCapture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProviderCapture")
    }
}

impl ProviderWsRuntimeConfig {
    fn observe_boundary(
        &self,
        wall_clock: &mut ProviderWallClock,
        kind: &'static str,
        detail: Option<&str>,
    ) {
        let received_at = Instant::now();
        let received_mono_ns = received_at
            .saturating_duration_since(self.history.monotonic_origin())
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        if matches!(
            kind,
            "connected" | "disconnected" | "read_failed" | "runtime_failed"
        ) {
            if let Err(error) = self.history.set_trade_flow_connection(
                kind == "connected" && self.subscribe_trades,
                received_mono_ns,
            ) {
                tracing::error!(%error, "bbwick_trade_flow_connection_failed");
            }
        }
        let Some(capture) = &self.capture else {
            return;
        };
        capture.observe(
            None,
            ProviderObservation {
                kind,
                recv_wall_ms: wall_clock.now_ms_from(received_at),
                received_mono_ns,
                payload: &[],
                detail,
            },
        );
    }
}

const WALL_CLOCK_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
struct ProviderWallClock {
    anchor_wall_ms: u64,
    anchor_mono: Instant,
    last_refresh_mono: Instant,
}

impl ProviderWallClock {
    fn new() -> Self {
        let anchor_mono = Instant::now();
        Self {
            anchor_wall_ms: system_now_ms(),
            anchor_mono,
            last_refresh_mono: anchor_mono,
        }
    }

    fn now_ms_from(&mut self, now_mono: Instant) -> u64 {
        if now_mono.duration_since(self.last_refresh_mono) >= WALL_CLOCK_REFRESH_INTERVAL {
            self.anchor_wall_ms = system_now_ms();
            self.anchor_mono = now_mono;
            self.last_refresh_mono = now_mono;
            return self.anchor_wall_ms;
        }
        self.anchor_wall_ms
            .saturating_add(now_mono.duration_since(self.anchor_mono).as_millis() as u64)
    }

    fn refresh(&mut self) {
        let now_mono = Instant::now();
        self.anchor_wall_ms = system_now_ms();
        self.anchor_mono = now_mono;
        self.last_refresh_mono = now_mono;
    }
}

#[derive(Debug, Deserialize)]
struct KlineEvent<'a> {
    #[serde(borrow, rename = "s")]
    symbol: &'a str,
    #[serde(borrow, rename = "k")]
    kline: KlinePayload<'a>,
}

#[derive(Debug, Deserialize)]
struct KlinePayload<'a> {
    #[serde(rename = "x")]
    closed: bool,
    #[serde(borrow, rename = "i")]
    interval: &'a str,
    #[serde(rename = "t")]
    ts_ms: u64,
    #[serde(rename = "o", deserialize_with = "deserialize_numeric_f64")]
    open: f64,
    #[serde(rename = "h", deserialize_with = "deserialize_numeric_f64")]
    high: f64,
    #[serde(rename = "l", deserialize_with = "deserialize_numeric_f64")]
    low: f64,
    #[serde(rename = "c", deserialize_with = "deserialize_numeric_f64")]
    close: f64,
    #[serde(rename = "v", deserialize_with = "deserialize_numeric_f64")]
    volume: f64,
}

#[derive(Debug, Deserialize)]
struct PartialDepthEvent<'a> {
    #[serde(rename = "lastUpdateId")]
    _last_update_id: u64,
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
#[serde(bound(deserialize = "'de: 'a"))]
enum ProviderEvent<'a> {
    #[serde(rename = "aggTrade")]
    AggTrade(AggTradeEvent<'a>),
    #[serde(rename = "kline")]
    Kline(KlineEvent<'a>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[serde(bound(deserialize = "'de: 'a"))]
enum ProviderMessage<'a> {
    Wrapped {
        #[serde(borrow)]
        data: ProviderEvent<'a>,
    },
    WrappedDepth {
        #[serde(borrow)]
        stream: &'a str,
        #[serde(borrow)]
        data: PartialDepthEvent<'a>,
    },
    Event(ProviderEvent<'a>),
    Ack(ProviderAck),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderAck {
    result: serde_json::Value,
    id: serde_json::Value,
}

fn positive_finite(num: f64) -> Option<f64> {
    (num.is_finite() && num > 0.0).then_some(num)
}

fn deserialize_numeric_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    struct NumericFieldVisitor;

    impl<'de> Visitor<'de> for NumericFieldVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a finite JSON number or numeric string")
        }

        fn visit_f64<E>(self, value: f64) -> std::result::Result<f64, E>
        where
            E: de::Error,
        {
            if value.is_finite() {
                Ok(value)
            } else {
                Err(E::custom("non-finite numeric value"))
            }
        }

        fn visit_i64<E>(self, value: i64) -> std::result::Result<f64, E>
        where
            E: de::Error,
        {
            Ok(value as f64)
        }

        fn visit_u64<E>(self, value: u64) -> std::result::Result<f64, E>
        where
            E: de::Error,
        {
            Ok(value as f64)
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<f64, E>
        where
            E: de::Error,
        {
            value.parse::<f64>().map_err(E::custom)
        }

        fn visit_borrowed_str<E>(self, value: &'de str) -> std::result::Result<f64, E>
        where
            E: de::Error,
        {
            self.visit_str(value)
        }
    }

    deserializer.deserialize_any(NumericFieldVisitor)
}

pub(crate) fn spawn_provider_ws_runtime(
    config: ProviderWsRuntimeConfig,
    quote_lane: ProviderQuoteLane,
    tape_lane: OracleTapeLane,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = run_provider_ws_loop(&config, quote_lane, tape_lane).await {
            config.observe_boundary(
                &mut ProviderWallClock::new(),
                "runtime_failed",
                Some(&err.to_string()),
            );
            tracing::error!(
                target: "engine.provider_ws",
                error = %err,
                "bbwick_provider_ws_runtime_exited"
            );
        }
    })
}

async fn run_provider_ws_loop(
    config: &ProviderWsRuntimeConfig,
    quote_lane: ProviderQuoteLane,
    tape_lane: OracleTapeLane,
) -> Result<()> {
    let ws_url = build_combined_ws_url(
        &config.ws_url,
        &config.key_registry,
        config.subscribe_trades,
        config.subscribe_top_of_book,
        config.subscribe_klines,
    )?;
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(10);
    let mut tick_seq = vec![0u32; config.key_registry.symbol_count()];
    let mut step_seq = vec![0u32; config.key_registry.step_slot_count()];
    let mut wall_clock = ProviderWallClock::new();
    let ledger_epoch = wall_clock.now_ms_from(Instant::now());
    config.observe_boundary(&mut wall_clock, "runtime_started", Some(&ws_url));

    loop {
        config.observe_boundary(&mut wall_clock, "connecting", Some(&ws_url));
        tracing::info!(
            ws_url = %ws_url,
            symbols = config.key_registry.symbol_count(),
            "bbwick provider ws connecting"
        );
        match connect_async(&ws_url).await {
            Ok((ws_stream, _)) => {
                wall_clock.refresh();
                config.observe_boundary(&mut wall_clock, "connected", None);
                tracing::info!(ws_url = %ws_url, "bbwick provider ws connected");
                backoff = Duration::from_millis(500);
                let (_, mut read) = ws_stream.split();
                while let Some(message) = read.next().await {
                    let message = match message {
                        Ok(message) => message,
                        Err(err) => {
                            config.observe_boundary(
                                &mut wall_clock,
                                "read_failed",
                                Some(&err.to_string()),
                            );
                            tracing::warn!(
                                target: "engine.provider_ws",
                                error = %err,
                                "bbwick_provider_ws_recv_failed"
                            );
                            break;
                        }
                    };
                    let received_at = Instant::now();
                    let recv_wall_ms = wall_clock.now_ms_from(received_at);
                    let received_mono_ns = received_at
                        .saturating_duration_since(config.history.monotonic_origin())
                        .as_nanos()
                        .min(u128::from(u64::MAX))
                        as u64;
                    match message {
                        Message::Text(text) => {
                            process_ws_payload(
                                text.as_bytes(),
                                &config.key_registry,
                                &config.history,
                                config.capture.as_deref(),
                                &quote_lane,
                                &tape_lane,
                                &mut tick_seq,
                                &mut step_seq,
                                recv_wall_ms,
                                received_mono_ns,
                                ledger_epoch,
                            )
                            .await?;
                        }
                        Message::Binary(bytes) => {
                            process_ws_payload(
                                bytes.as_ref(),
                                &config.key_registry,
                                &config.history,
                                config.capture.as_deref(),
                                &quote_lane,
                                &tape_lane,
                                &mut tick_seq,
                                &mut step_seq,
                                recv_wall_ms,
                                received_mono_ns,
                                ledger_epoch,
                            )
                            .await?;
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                        Message::Close(frame) => {
                            if let Some(capture) = &config.capture {
                                capture.observe(
                                    None,
                                    ProviderObservation {
                                        kind: "close_frame",
                                        recv_wall_ms,
                                        received_mono_ns,
                                        payload: &[],
                                        detail: Some(&format!("{frame:?}")),
                                    },
                                );
                            }
                            break;
                        }
                    }
                }
                config.observe_boundary(&mut wall_clock, "disconnected", None);
            }
            Err(err) => {
                config.observe_boundary(&mut wall_clock, "connect_failed", Some(&err.to_string()));
                tracing::warn!(
                    target: "engine.provider_ws",
                    error = %err,
                    "bbwick_provider_ws_connect_failed"
                );
            }
        }
        tokio::time::sleep(full_jitter_duration(backoff)).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

fn full_jitter_duration(cap: Duration) -> Duration {
    let cap_ms = cap.as_millis() as u64;
    if cap_ms == 0 {
        return Duration::ZERO;
    }
    let counter = PROVIDER_WS_JITTER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(counter);
    Duration::from_millis(
        (nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)) % cap_ms.saturating_add(1),
    )
}

fn build_combined_ws_url(
    ws_url: &str,
    key_registry: &MarketDataKeyRegistry,
    subscribe_trades: bool,
    subscribe_top_of_book: bool,
    subscribe_klines: bool,
) -> Result<String> {
    let step_timeframes = key_registry.timeframes().collect::<Vec<_>>();
    let mut streams = Vec::with_capacity(
        key_registry.symbol_count()
            * (usize::from(subscribe_trades)
                + usize::from(subscribe_top_of_book)
                + if subscribe_klines {
                    step_timeframes.len()
                } else {
                    0
                }),
    );
    for symbol in key_registry.symbols() {
        let stream_symbol = symbol.to_ascii_lowercase();
        if subscribe_trades {
            streams.push(format!("{stream_symbol}@aggTrade"));
        }
        if subscribe_top_of_book {
            streams.push(format!("{stream_symbol}@depth5@100ms"));
        }
        if subscribe_klines {
            for timeframe in &step_timeframes {
                streams.push(format!("{stream_symbol}@kline_{timeframe}"));
            }
        }
    }
    if streams.is_empty() {
        anyhow::bail!("bbwick provider stream list is empty");
    }
    let stream_list = streams.join("/");
    let ws_url = ws_url.trim();
    if ws_url.is_empty() {
        anyhow::bail!("bbwick provider ws_url is empty");
    }
    if ws_url.contains("/stream") {
        let separator = if ws_url.contains('?') { '&' } else { '?' };
        return Ok(format!("{ws_url}{separator}streams={stream_list}"));
    }
    if let Some(prefix) = ws_url.strip_suffix("/ws") {
        return Ok(format!("{prefix}/stream?streams={stream_list}"));
    }
    Ok(format!("{ws_url}/stream?streams={stream_list}"))
}

async fn process_ws_payload(
    payload: &[u8],
    key_registry: &MarketDataKeyRegistry,
    history: &OracleHistory,
    capture: Option<&dyn ProviderCapture>,
    quote_lane: &ProviderQuoteLane,
    tape_lane: &OracleTapeLane,
    tick_seq: &mut [u32],
    step_seq: &mut [u32],
    recv_wall_ms: u64,
    received_mono_ns: u64,
    ledger_epoch: u64,
) -> Result<()> {
    let observation = ProviderObservation {
        kind: "invalid_message",
        recv_wall_ms,
        received_mono_ns,
        payload,
        detail: None,
    };
    let message = match serde_json::from_slice::<ProviderMessage<'_>>(payload) {
        Ok(message) => message,
        Err(error) => {
            history.mark_trade_flow_malformed(received_mono_ns)?;
            if let Some(capture) = capture {
                capture.observe(
                    None,
                    ProviderObservation {
                        detail: Some(&error.to_string()),
                        ..observation
                    },
                );
            }
            return Ok(());
        }
    };
    match message {
        ProviderMessage::Wrapped { data } | ProviderMessage::Event(data) => {
            if let (Some(capture), ProviderEvent::AggTrade(event)) = (capture, &data) {
                capture.observe(
                    Some(event.symbol),
                    ProviderObservation {
                        kind: "aggregate_trade",
                        ..observation
                    },
                );
            }
            process_provider_event(
                data,
                key_registry,
                history,
                tape_lane,
                tick_seq,
                step_seq,
                recv_wall_ms,
                received_mono_ns,
                ledger_epoch,
            )
            .await?;
        }
        ProviderMessage::WrappedDepth { stream, data } => {
            if let Some(frame) = parse_depth5_top(stream, data, key_registry, recv_wall_ms) {
                quote_lane.publish_top_of_book(frame)?;
            }
        }
        ProviderMessage::Ack(ack) => {
            let valid_id = ack.id.is_null()
                || ack.id.as_i64().is_some()
                || ack.id.as_str().is_some_and(|id| {
                    id.len() <= 36 && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
                });
            if !ack.result.is_null() || !valid_id {
                if let Some(capture) = capture {
                    capture.observe(None, observation);
                }
            }
        }
    };
    Ok(())
}

async fn process_provider_event(
    event: ProviderEvent<'_>,
    key_registry: &MarketDataKeyRegistry,
    history: &OracleHistory,
    tape_lane: &OracleTapeLane,
    tick_seq: &mut [u32],
    step_seq: &mut [u32],
    recv_wall_ms: u64,
    received_mono_ns: u64,
    ledger_epoch: u64,
) -> Result<()> {
    match event {
        ProviderEvent::AggTrade(event) => {
            history.admit_aggregate_trade(
                event.symbol.trim(),
                event.flow_trade(),
                received_mono_ns,
            )?;
            if let Some(frame) = parse_agg_trade_event(&event, key_registry, recv_wall_ms) {
                let seq = &mut tick_seq[frame.symbol_id.index()];
                *seq = seq.wrapping_add(1);
                tape_lane.try_send(IngestEvent::TickById(frame), "tick")?;
                tape_lane.try_send(
                    IngestEvent::HeartbeatById(IdHeartbeatFrame {
                        symbol_id: frame.symbol_id,
                        ts_ms: frame.ts_ms,
                        seq: *seq,
                    }),
                    "heartbeat",
                )?;
            }
        }
        ProviderEvent::Kline(event) => {
            if let Some((symbol_id, timeframe_id, bar)) = parse_kline_event(event, key_registry) {
                let admitted = i64::try_from(bar.ts_ms)
                    .map_err(anyhow::Error::from)
                    .and_then(|ts_ms| {
                        history.admit_bars(
                            key_registry.symbol(symbol_id),
                            key_registry.timeframe(timeframe_id),
                            &[OhlcBar {
                                ts_ms,
                                open: bar.open,
                                high: bar.high,
                                low: bar.low,
                                close: bar.close,
                                volume: bar.volume,
                            }],
                            received_mono_ns,
                        )
                    });
                if let Err(err) = admitted {
                    tracing::error!(
                        target: "engine.oracle",
                        error = %err,
                        symbol = key_registry.symbol(symbol_id),
                        timeframe = key_registry.timeframe(timeframe_id),
                        ts_ms = bar.ts_ms,
                        "bbwick_history_step_rejected"
                    );
                    return Ok(());
                }
                let seq = &mut step_seq[key_registry.step_slot(symbol_id, timeframe_id)];
                *seq = seq.wrapping_add(1);
                tape_lane.try_send(
                    IngestEvent::StepById(IdStepFrame {
                        symbol_id,
                        timeframe_id,
                        received_mono_ns,
                        bar,
                        overlays: Vec::new(),
                        trades: Vec::new(),
                        seq: *seq,
                        ledger_epoch,
                    }),
                    "step",
                )?;
            }
        }
    }
    Ok(())
}

fn parse_depth5_top(
    stream: &str,
    event: PartialDepthEvent<'_>,
    key_registry: &MarketDataKeyRegistry,
    recv_wall_ms: u64,
) -> Option<IdTopOfBookIngressFrame> {
    let (stream_symbol, suffix) = stream.split_once('@')?;
    if suffix != "depth5@100ms" {
        return None;
    }
    let configured_symbol = key_registry
        .symbols()
        .find(|symbol| symbol.eq_ignore_ascii_case(stream_symbol))?;
    let symbol_id = key_registry.symbol_id(configured_symbol)?;
    let (bid_px, bid_sz, bid_total) = depth_side_summary(&event.bids, true)?;
    let (ask_px, ask_sz, ask_total) = depth_side_summary(&event.asks, false)?;
    if ask_px < bid_px {
        return None;
    }
    let total = bid_total + ask_total;
    let queue_imbalance_l5 = (total.is_finite() && total > 0.0)
        .then_some(((bid_total - ask_total) / total).clamp(-1.0, 1.0))?;
    Some(IdTopOfBookIngressFrame {
        symbol_id,
        source_ts_ms: recv_wall_ms,
        source_ts_kind: SourceTsKind::EngineIngestWallclock,
        bid_px,
        ask_px,
        bid_sz,
        ask_sz,
        queue_imbalance_l5: Some(queue_imbalance_l5),
    })
}

fn depth_side_summary(levels: &[[&str; 2]], descending: bool) -> Option<(f64, f64, f64)> {
    if levels.len() != 5 {
        return None;
    }
    let mut total = 0.0;
    let mut first = None;
    let mut previous_price = None;
    for (index, level) in levels.iter().enumerate() {
        let price = positive_finite(level[0].parse::<f64>().ok()?)?;
        let quantity = positive_finite(level[1].parse::<f64>().ok()?)?;
        if let Some(previous) = previous_price {
            let ordered = if descending {
                price < previous
            } else {
                price > previous
            };
            if !ordered {
                return None;
            }
        }
        if index == 0 {
            first = Some((price, quantity));
        }
        previous_price = Some(price);
        total += quantity;
    }
    let (first_price, first_quantity) = first?;
    (total.is_finite() && total > 0.0).then_some((first_price, first_quantity, total))
}

fn parse_kline_event(
    payload: KlineEvent<'_>,
    key_registry: &MarketDataKeyRegistry,
) -> Option<(crate::ids::SymbolId, crate::ids::TimeframeId, BarPayload)> {
    let symbol_id = key_registry.symbol_id(payload.symbol.trim())?;
    if !payload.kline.closed {
        return None;
    }
    let timeframe_id = key_registry.timeframe_id(payload.kline.interval.trim())?;
    let ts_ms = payload.kline.ts_ms;
    let open = positive_finite(payload.kline.open)?;
    let high = positive_finite(payload.kline.high)?;
    let low = positive_finite(payload.kline.low)?;
    let close = positive_finite(payload.kline.close)?;
    let volume = positive_finite(payload.kline.volume)?;
    Some((
        symbol_id,
        timeframe_id,
        BarPayload {
            ts_ms,
            open,
            high,
            low,
            close,
            volume,
        },
    ))
}

fn system_now_ms() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    now.as_millis() as u64
}

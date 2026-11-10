use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use crate::capture::scope::REQUIRED_CHAINLINK_PRICE_SYMBOLS;
use crate::oracle_resolution::{
    validate_chainlink_resolution_feed, ChainlinkResolutionPrice, CHAINLINK_TWAP_PRICE_PRECISION,
    CHAINLINK_TWAP_SIXTY_TOPIC, CHAINLINK_TWAP_THIRTY_TOPIC,
};

const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const RTDS_UPDATE_TYPE: &str = "update";
const PING_INTERVAL: Duration = Duration::from_secs(5);
const VALID_EVENT_TIMEOUT: Duration = Duration::from_secs(15);
const CHANNEL_CAPACITY: usize = 4096;
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub struct RtdsChainlinkRuntime {
    pub receiver: mpsc::Receiver<ChainlinkResolutionPrice>,
    pub task: JoinHandle<()>,
}

pub fn spawn_rtds_chainlink_runtime(shutdown: CancellationToken) -> RtdsChainlinkRuntime {
    let (tx, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let task = tokio::spawn(run_rtds_chainlink(tx, shutdown));
    RtdsChainlinkRuntime { receiver, task }
}

#[derive(Deserialize)]
struct RtdsEnvelope {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    timestamp: u64,
    payload: RtdsPricePayload,
}

#[derive(Deserialize)]
struct RtdsPricePayload {
    symbol: String,
    timestamp: u64,
    full_accuracy_value: String,
    window_s: u64,
}

async fn run_rtds_chainlink(
    tx: mpsc::Sender<ChainlinkResolutionPrice>,
    shutdown: CancellationToken,
) {
    let mut sequence = 0_u64;
    let mut reconnect_delay = Duration::from_secs(1);
    while !shutdown.is_cancelled() && !tx.is_closed() {
        match connect_async(RTDS_URL).await {
            Ok((socket, _)) => {
                reconnect_delay = Duration::from_secs(1);
                if let Err(err) = run_connection(socket, &tx, &shutdown, &mut sequence).await {
                    tracing::warn!(target: "polymarket.rtds_chainlink", error = %err, "rtds_chainlink_connection_ended");
                }
            }
            Err(err) => {
                tracing::warn!(target: "polymarket.rtds_chainlink", error = %err, "rtds_chainlink_connect_failed");
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(reconnect_delay) => {}
        }
        reconnect_delay = reconnect_delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
    }
}

async fn run_connection<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    tx: &mpsc::Sender<ChainlinkResolutionPrice>,
    shutdown: &CancellationToken,
    sequence: &mut u64,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut write, mut read) = socket.split();
    let subscriptions = [
        json!({
            "topic": CHAINLINK_TWAP_THIRTY_TOPIC,
            "type": RTDS_UPDATE_TYPE,
        }),
        json!({
            "topic": CHAINLINK_TWAP_SIXTY_TOPIC,
            "type": RTDS_UPDATE_TYPE,
        }),
    ];
    write
        .send(Message::Text(
            json!({ "action": "subscribe", "subscriptions": subscriptions })
                .to_string()
                .into(),
        ))
        .await?;

    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    let valid_event_deadline = tokio::time::sleep(VALID_EVENT_TIMEOUT);
    tokio::pin!(valid_event_deadline);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = ping.tick() => write.send(Message::Text("PING".into())).await?,
            _ = &mut valid_event_deadline => anyhow::bail!(
                "RTDS valid Chainlink update timeout after {}s",
                VALID_EVENT_TIMEOUT.as_secs()
            ),
            message = read.next() => {
                let Some(message) = message else {
                    anyhow::bail!("RTDS socket closed");
                };
                match message? {
                    Message::Text(text) => {
                        let recv_ts_ms = unix_now_ms();
                        let Some(event) = parse_price_message(&text, recv_ts_ms, sequence) else {
                            continue;
                        };
                        valid_event_deadline
                            .as_mut()
                            .reset(tokio::time::Instant::now() + VALID_EVENT_TIMEOUT);
                        if tx.send(event).await.is_err() {
                            return Ok(());
                        }
                    }
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => anyhow::bail!("RTDS close frame: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn parse_price_message(
    text: &str,
    recv_ts_ms: u64,
    sequence: &mut u64,
) -> Option<ChainlinkResolutionPrice> {
    let envelope: RtdsEnvelope = serde_json::from_str(text).ok()?;
    let symbol = envelope.payload.symbol.trim().to_ascii_lowercase();
    if envelope.payload.full_accuracy_value.is_empty()
        || !envelope
            .payload
            .full_accuracy_value
            .bytes()
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let value_e18 = envelope.payload.full_accuracy_value.parse::<i128>().ok()?;
    if envelope.message_type != RTDS_UPDATE_TYPE
        || !REQUIRED_CHAINLINK_PRICE_SYMBOLS.contains(&symbol.as_str())
        || envelope.timestamp == 0
        || envelope.payload.timestamp == 0
        || validate_chainlink_resolution_feed(&symbol, &envelope.topic, envelope.payload.window_s)
            .is_err()
        || value_e18 <= 0
        || value_e18 >= 10_i128.pow(u32::from(CHAINLINK_TWAP_PRICE_PRECISION))
    {
        return None;
    }
    *sequence = sequence.saturating_add(1);
    Some(ChainlinkResolutionPrice {
        symbol,
        topic: envelope.topic,
        window_s: envelope.payload.window_s,
        payload_ts_ms: envelope.payload.timestamp,
        observed_ts_ms: recv_ts_ms,
        value_e18,
        capture_sequence: *sequence,
    })
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

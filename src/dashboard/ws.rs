use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures_util::{SinkExt as _, StreamExt as _};

use super::model::{capture_operator_snapshot, OperatorStreamEvent};
use super::DashboardState;

pub(super) async fn upgrade(
    ws: WebSocketUpgrade,
    State(state): State<DashboardState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream(socket, state))
}

async fn stream(socket: WebSocket, state: DashboardState) {
    let (mut sender, mut receiver) = socket.split();
    let refresh_period = std::time::Duration::from_secs(1);
    let mut refresh =
        tokio::time::interval_at(tokio::time::Instant::now() + refresh_period, refresh_period);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    if send_snapshot(&mut sender, &state, true).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            _ = refresh.tick() => {
                if send_snapshot(&mut sender, &state, false).await.is_err() {
                    break;
                }
            }
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }
    let _ = sender.close().await;
}

async fn send_snapshot(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    state: &DashboardState,
    include_chart_history: bool,
) -> anyhow::Result<()> {
    let event = OperatorStreamEvent {
        kind: "snapshot",
        snapshot: capture_operator_snapshot(state, include_chart_history).await?,
    };
    sender
        .send(Message::Text(serde_json::to_string(&event)?.into()))
        .await?;
    Ok(())
}

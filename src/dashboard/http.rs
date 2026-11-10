use axum::extract::State;
use axum::http::{header, Method, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::cors::{Any, CorsLayer};

use super::commands::{self, OperatorCommand, OperatorCommandAck};
use super::{ws, DashboardState};

pub(super) fn build_router(state: DashboardState) -> Router {
    Router::new()
        .route("/api/price-series", get(super::chart::price_series))
        .route("/api/equity-history", get(super::chart::equity_history))
        .route("/api/command", post(command))
        .route("/ws/dashboard", get(ws::upgrade))
        .layer(
            CorsLayer::new()
                .allow_origin([
                    "http://127.0.0.1:5188"
                        .parse::<header::HeaderValue>()
                        .expect("valid localhost dashboard origin"),
                    "http://localhost:5188"
                        .parse::<header::HeaderValue>()
                        .expect("valid localhost dashboard origin"),
                ])
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any),
        )
        .with_state(state)
}

async fn command(
    State(state): State<DashboardState>,
    Json(command): Json<OperatorCommand>,
) -> Result<Json<OperatorCommandAck>, (StatusCode, String)> {
    commands::forward(&state, command)
        .await
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("{error:#}")))
}

use std::sync::Arc;

use anyhow::Result;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use prometheus::{core::Collector, Encoder, TextEncoder};
use tokio::sync::RwLock;
use ton_indexer::Engine;

use crate::metrics::BlockMetrics;

pub async fn run(metrics: Arc<RwLock<BlockMetrics>>, engine: Arc<Engine>) -> Result<()> {
    let app = Router::new()
        .route("/", get(get_metrics))
        .route("/sendboc", post(send_boc))
        .with_state(Arc::new(AppState { metrics, engine }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(())
}

struct AppState {
    metrics: Arc<RwLock<BlockMetrics>>,
    engine: Arc<Engine>,
}

async fn get_metrics(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, AppError> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mut metrics = state.metrics.read().await.collect();
    metrics.append(&mut prometheus::gather());
    encoder.encode(&metrics, &mut buffer)?;
    let response = String::from_utf8(buffer)?;
    Ok(response)
}

async fn send_boc(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> Result<impl IntoResponse, AppError> {
    state.engine.broadcast_external_message(0, &body)?;
    state.engine.broadcast_external_message(-1, &body)?;
    Ok("ok")
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response<Body> {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
use std::sync::Arc;

use anyhow::Result;
use axum::{
    body::Body,
    extract::State,
    http::{Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{core::Collector, Encoder, TextEncoder};
use tokio::sync::RwLock;

use crate::metrics::BlockMetrics;

pub async fn run(metrics: Arc<RwLock<BlockMetrics>>) -> Result<()> {
    let app = Router::new()
        .route("/", get(get_metrics))
        .with_state(Arc::new(AppState { metrics }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(())
}

struct AppState {
    metrics: Arc<RwLock<BlockMetrics>>,
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
//! 本地演示用的模型网关。回答由程序生成，练习转发、SSE 和故障回放时无需真实模型。
//! 用 X-Request-ID: mock-outage-all 模拟上游故障，检查缓存命中和未命中。
use std::{env, sync::Arc};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};

fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({"error": {"code": code}}))).into_response()
}

async fn complete(State(auth): State<Arc<str>>, headers: HeaderMap, body: Bytes) -> Response {
    if headers.get(header::AUTHORIZATION).map(|v| v.as_bytes()) != Some(auth.as_bytes()) {
        return error(StatusCode::UNAUTHORIZED, "invalid_api_key");
    }
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Some(model) = payload["model"].as_str().filter(|model| !model.is_empty()) else {
        return error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if model == "mock-model-not-found" {
        return error(StatusCode::SERVICE_UNAVAILABLE, "model_not_found");
    }
    let mode = headers.get("x-request-id").and_then(|v| v.to_str().ok());
    if mode == Some("mock-unknown-400") {
        return error(StatusCode::BAD_REQUEST, "mock_vendor_error");
    }
    if mode == Some("mock-rate-limit") {
        let mut response = error(StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded");
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("2"));
        return response;
    }
    if matches!(mode, Some("mock-outage-all" | "mock-outage-primary")) {
        return error(StatusCode::SERVICE_UNAVAILABLE, "mock_outage");
    }

    let answer = format!("Mock response from {model}.");
    if payload["stream"] == true {
        // SSE 以空行分隔事件。示例按照“回答片段 → 正常结束 → [DONE]”发出三个事件。
        let content = json!({"model":model,"choices":[{"index":0,"delta":{"role":"assistant","content":answer},"finish_reason":null}]});
        let stop = json!({"model":model,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
        let events = format!("data: {content}\n\ndata: {stop}\n\ndata: [DONE]\n\n");
        return ([(header::CONTENT_TYPE, "text/event-stream")], events).into_response();
    }
    Json(json!({"model":model,"choices":[{"index":0,"message":{"role":"assistant","content":answer},"finish_reason":"stop"}]})).into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = env::var("MOCK_UPSTREAM_API_KEY").context("请设置 MOCK_UPSTREAM_API_KEY")?;
    ensure!(!token.is_empty(), "MOCK_UPSTREAM_API_KEY 不能为空");
    let state: Arc<str> = format!("Bearer {token}").into();
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/chat/completions", post(complete))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(state);
    let bind = env::var("MOCK_BIND").unwrap_or_else(|_| "127.0.0.1:8091".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    eprintln!("模拟网关已监听 {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

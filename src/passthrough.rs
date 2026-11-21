//! 通用 HTTP 转发：把 HTTP 请求发给固定上游。这里完全不需要识别 JSON 或模型名称。

use std::{collections::HashSet, sync::Arc};

use axum::{
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use http_body_util::Full;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};

use crate::config::ProxyConfig;

type Upstream = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

pub(crate) struct Proxy {
    config: ProxyConfig,
    client: Upstream,
    slots: Arc<Semaphore>,
}

impl Proxy {
    pub fn new(config: ProxyConfig, slots: Arc<Semaphore>) -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        // 每个请求最多向上游发起一次；POST 可能已经产生副作用。
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(config.max_inflight)
            .retry_canceled_requests(false)
            .build(connector);
        Self {
            config,
            client,
            slots,
        }
    }
}

/// Host 和分帧信息由下一段连接重建。Connection 头还可以点名额外字段，
/// 必须连同这些字段一起去掉。HeaderMap 的 append 可以保留多个 Set-Cookie。
fn end_to_end(headers: &HeaderMap, preserve_length: bool) -> HeaderMap {
    let mut excluded: HashSet<HeaderName> = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
    ]
    .into_iter()
    .map(HeaderName::from_static)
    .collect();
    if !preserve_length || headers.contains_key(header::TRANSFER_ENCODING) {
        excluded.insert(header::CONTENT_LENGTH);
    }
    for value in headers.get_all(header::CONNECTION) {
        for item in value.as_bytes().split(|b| *b == b',') {
            if let Ok(name) = HeaderName::from_bytes(item.trim_ascii()) {
                excluded.insert(name);
            }
        }
    }
    let mut result = HeaderMap::new();
    for (key, value) in headers {
        if !excluded.contains(key) {
            result.append(key.clone(), value.clone());
        }
    }
    result
}

fn error(status: StatusCode, code: &str) -> Response {
    let mut res = (
        status,
        axum::Json(serde_json::json!({"error":{"type":"relay_error", "code":code}})),
    )
        .into_response();
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if status == StatusCode::TOO_MANY_REQUESTS {
        res.headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
    }
    res
}

fn upstream_target(origin: &Uri, method: &Method, target: &Uri) -> Option<Uri> {
    if target.scheme().is_some() || target.authority().is_some() {
        return None;
    }
    let path = target.path_and_query()?;
    if !(path.as_str().starts_with('/') || method == Method::OPTIONS && path.as_str() == "*") {
        return None;
    }
    // 只替换路径部分，保留字节拼写。即使路径以 // 开头，目标主机仍来自配置。
    let mut parts = origin.clone().into_parts();
    parts.path_and_query = Some(path.clone());
    Uri::from_parts(parts).ok()
}

pub(crate) async fn forward(proxy: &Proxy, request: Request) -> Response {
    let Ok(slot) = proxy.slots.clone().try_acquire_owned() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "relay_capacity");
    };
    let (head, body) = request.into_parts();
    if matches!(head.method, Method::CONNECT | Method::TRACE) {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method_not_supported");
    }
    let Some(uri) = upstream_target(&proxy.config.origin, &head.method, &head.uri) else {
        return error(StatusCode::BAD_REQUEST, "invalid_request_target");
    };
    let deadline = Instant::now() + proxy.config.total_timeout;
    let operation = async {
        let bytes = match to_bytes(body, proxy.config.max_body).await {
            Ok(bytes) => bytes,
            Err(e) => {
                use std::error::Error;
                if e.source()
                    .is_some_and(|cause| cause.is::<http_body_util::LengthLimitError>())
                {
                    return error(StatusCode::PAYLOAD_TOO_LARGE, "relay_body_limit");
                }
                return error(StatusCode::BAD_REQUEST, "invalid_request_body");
            }
        };
        let is_head = head.method == Method::HEAD;
        // URI 可以含调用方查询串。分别记录 host、path，就能追踪路由而不写出查询参数。
        tracing::info!(stage = "passthrough", event = "attempt", method = %head.method,
            host = uri.host(), path = uri.path(), "relay_stage");
        let mut outgoing = hyper::Request::new(Full::new(bytes));
        *outgoing.method_mut() = head.method;
        *outgoing.uri_mut() = uri;
        *outgoing.headers_mut() = end_to_end(&head.headers, false);
        let response = match proxy.client.request(outgoing).await {
            Ok(response) => response,
            Err(failure) => {
                let reason = if failure.is_connect() {
                    "connection_error"
                } else {
                    "transport_error"
                };
                tracing::info!(
                    stage = "passthrough",
                    event = "failed",
                    reason,
                    "relay_stage"
                );
                return error(StatusCode::BAD_GATEWAY, "relay_upstream_unavailable");
            }
        };
        let (head, incoming) = response.into_parts();
        tracing::info!(
            stage = "passthrough",
            event = "response",
            upstream_status = head.status.as_u16(),
            "relay_stage"
        );
        let preserve_length = head.status == StatusCode::NOT_MODIFIED
            || (is_head
                && !head.status.is_informational()
                && head.status != StatusCode::NO_CONTENT);
        let mut response = Response::new(Body::new(incoming));
        *response.status_mut() = head.status;
        *response.headers_mut() = end_to_end(&head.headers, preserve_length);
        crate::delivery::bounded(response, slot, deadline)
    };
    timeout_at(deadline, operation).await.unwrap_or_else(|_| {
        tracing::info!(
            stage = "passthrough",
            event = "failed",
            reason = "timeout",
            "relay_stage"
        );
        error(StatusCode::BAD_GATEWAY, "relay_upstream_unavailable")
    })
}

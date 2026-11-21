//! 历史模式的 HTTP 入口。先读 handle，再读 conversation，最后看 upstream::attempt。
//! 每个请求只优先尝试一次主模型，主模型失败后才查询历史。

mod json;
mod protocol;
mod sse;
mod sulphur;
mod upstream;

use crate::{
    config::RelayConfig,
    delivery,
    history::{History, Identity, canonicalize},
};
use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::Response,
};
use futures_util::StreamExt;
use reqwest::Url;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, timeout, timeout_at};
use upstream::UpstreamOutcome;
use uuid::Uuid;

pub struct Relay {
    c: RelayConfig,
    upstream: Url,
    models_url: Option<Url>,
    history: Arc<History>,
    client: reqwest::Client,
    counters: Mutex<HashMap<String, u64>>,
}

impl Relay {
    pub fn new(config: RelayConfig, history: Arc<History>, client: reqwest::Client) -> Self {
        let upstream = Url::parse(&config.upstream_url).expect("URL checked at startup");
        let models_url = crate::config::models_url(&upstream);
        Self {
            upstream,
            models_url,
            counters: Mutex::default(),
            c: config,
            history,
            client,
        }
    }
    pub(crate) fn handles(&self, path: &str) -> bool {
        path == self.c.request_path || path == "/v1/models" && self.models_url.is_some()
    }
    /// 只在更新计数的这几条指令中持锁；锁的保护范围不跨越任何 await。
    fn count(&self, name: &str) {
        let mut counters = self.counters.lock().unwrap_or_else(|e| e.into_inner());
        let count = counters.entry(name.to_owned()).or_default();
        *count += 1;
        tracing::debug!(metric = name, count = *count, "relay_metric");
    }
}

fn reply(code: u16, body: Body, kind: HeaderValue, source: Option<&'static str>) -> Response {
    let mut r = Response::new(body);
    *r.status_mut() = StatusCode::from_u16(code).expect("upstream returned a valid HTTP status");
    r.headers_mut().insert("content-type", kind);
    r.headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    if let Some(source) = source {
        r.headers_mut()
            .insert("x-failover-source", HeaderValue::from_static(source));
    }
    r
}
fn error(code: u16, reason: &str, retry: bool, after: usize) -> Response {
    let body =
        json!({"error":{"type":"failover_error","code":reason,"message":reason,"retryable":retry}});
    let mut response = reply(
        code,
        Body::from(body.to_string()),
        HeaderValue::from_static("application/json"),
        None,
    );
    if retry {
        response.headers_mut().insert(
            "retry-after",
            HeaderValue::from_str(&after.to_string()).unwrap(),
        );
    }
    response
}

fn append_query(mut url: Url, query: &str) -> Url {
    if !query.is_empty() {
        let merged = match url.query().filter(|s| !s.is_empty()) {
            Some(existing) => format!("{existing}&{query}"),
            None => query.to_owned(),
        };
        url.set_query(Some(&merged));
    }
    url
}

fn filtered(headers: &HeaderMap, allowed: &[&'static str], repeat: bool) -> HeaderMap {
    let mut selected = HeaderMap::new();
    for &key in allowed {
        for value in headers.get_all(key) {
            if repeat {
                selected.append(key, value.clone());
            } else {
                selected.insert(key, value.clone());
            }
        }
    }
    selected
}

async fn models(relay: &Relay, url: Url, headers: &HeaderMap) -> Response {
    tracing::info!(
        stage = "models",
        event = "attempt",
        host = url.host_str(),
        path = url.path(),
        "relay_stage"
    );
    let request = relay.client.get(url).headers(filtered(
        headers,
        &["authorization", "accept", "x-request-id"],
        true,
    ));
    let mut upstream_status = None;
    let result = timeout(
        relay.c.upstream_timeout.min(Duration::from_secs(10)),
        async {
            let upstream = request
                .send()
                .await
                .map_err(|_| protocol::Fault::Transport)?;
            let status = upstream.status();
            upstream_status = Some(status.as_u16());
            let mut headers = filtered(
                upstream.headers(),
                &["content-type", "retry-after", "www-authenticate"],
                true,
            );
            headers.insert("cache-control", HeaderValue::from_static("no-store"));
            let body = upstream::read_bounded(upstream, relay.c.max_response).await?;
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            *response.headers_mut() = headers;
            Ok(response)
        },
    )
    .await
    .unwrap_or(Err(protocol::Fault::Timeout));
    match result {
        Ok(response) => {
            tracing::info!(
                stage = "models",
                event = "response",
                upstream_status,
                "relay_stage"
            );
            response
        }
        Err(fault) => {
            tracing::info!(
                stage = "models",
                event = "failed",
                reason = fault.reason(),
                upstream_status,
                "relay_stage"
            );
            error(503, "model_list_unavailable", true, relay.c.retry_after)
        }
    }
}

pub(crate) fn decoded_path(path: &str) -> String {
    // ASGI 的 scope.path 已经解码；沿用原语义可让旧请求继续得到相同的上下文摘要。
    let mut bytes = Vec::with_capacity(path.len());
    let mut iter = path.as_bytes().iter().copied().peekable();
    while let Some(b) = iter.next() {
        if b == b'%' {
            let mut next = iter.clone();
            if let (Some(a), Some(z)) = (next.next(), next.next())
                && let (Some(hi), Some(lo)) = ((a as char).to_digit(16), (z as char).to_digit(16))
            {
                bytes.push((hi * 16 + lo) as u8);
                iter = next;
                continue;
            }
        }
        bytes.push(b);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

pub(crate) async fn handle(relay: Arc<Relay>, request: Request, path: String) -> Response {
    let (parts, body) = request.into_parts();
    let query = parts.uri.query().unwrap_or("");
    let c = &relay.c;
    let upstream = append_query(relay.upstream.clone(), query);
    let list_models = path == "/v1/models" && path != c.request_path && relay.models_url.is_some();
    let method = if list_models {
        Method::GET
    } else {
        Method::POST
    };
    if parts.method != method {
        return error(405, "method_not_allowed", false, c.retry_after);
    }
    let started = Instant::now();
    // POST 是聊天请求的方法，SSE 由请求体的 stream 选择。
    // 只有声明 Sulphur 标识的客户端才采用插件预算和提示词兜底。
    let mut agents = parts.headers.get_all("user-agent").iter();
    let sulphur = agents.next().is_some_and(|s| {
        s.as_bytes()
            .starts_with(b"ComfyUI-Sulphur-Prompt-Enhancer/")
    }) && agents.next().is_none();
    let initial = if sulphur {
        c.sulphur_nonstream_total_timeout
            .unwrap_or(c.total_timeout)
            .min(c.total_timeout)
    } else {
        c.total_timeout
    };
    let initial_deadline = started + initial - Duration::from_millis(10);
    if list_models {
        let result = timeout_at(
            initial_deadline,
            models(
                &relay,
                append_query(relay.models_url.clone().unwrap(), query),
                &parts.headers,
            ),
        )
        .await
        .unwrap_or_else(|_| error(503, "request_deadline", true, c.retry_after));
        return delivery::bounded(result, initial_deadline);
    }

    let upload = async {
        let mut raw = Vec::new();
        let mut chunks = body.into_data_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| error(400, "invalid_request", false, c.retry_after))?;
            if chunk.len() > c.max_body - raw.len() {
                return Err(error(413, "request_too_large", false, c.retry_after));
            }
            raw.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(raw))
    };
    let raw = match timeout_at(initial_deadline, upload).await {
        Ok(Ok(raw)) => raw,
        Ok(Err(r)) => return r,
        Err(_) => return error(503, "request_deadline", true, c.retry_after),
    };
    let Some(request) = protocol::strict_json(&raw).filter(|v| {
        v["model"].is_string()
            && v["messages"].as_array().is_some_and(|m| !m.is_empty())
            && v.get("stream").is_none_or(Value::is_boolean)
    }) else {
        return error(400, "invalid_request", false, c.retry_after);
    };
    let streaming = request["stream"].as_bool().unwrap_or(false);
    if parts
        .extensions
        .get::<crate::content_log::ShowContent>()
        .is_some()
    {
        crate::content_log::image_prompt(&request, &raw);
    }
    let (total, primary) = c.timeouts(streaming, sulphur);
    let deadline = started + total;
    let response = timeout_at(
        deadline - Duration::from_millis(10),
        relay.conversation(
            RequestInfo {
                path,
                query,
                headers: parts.headers,
                raw,
                request,
                upstream,
                streaming,
                sulphur,
                deadline,
            },
            primary,
        ),
    )
    .await
    .unwrap_or_else(|_| error(503, "request_deadline", true, c.retry_after));
    delivery::bounded(response, deadline)
}

struct RequestInfo<'a> {
    path: String,
    query: &'a str,
    headers: HeaderMap,
    raw: Bytes,
    request: Value,
    upstream: Url,
    streaming: bool,
    sulphur: bool,
    deadline: Instant,
}

/// Arc 让返回的流也能持有这些数据。HTTP 处理函数退出时，流可能还在等待下一块模型输出。
struct Turn {
    relay: Arc<Relay>,
    span: tracing::Span,
    request: Value,
    identity: Option<Identity>,
    streaming: bool,
    reject: bool,
    deadline: Instant,
}

impl Turn {
    /// 正文成功交付后才入队。后台写入稍后执行，日志只记录队列是否接收了这份回答。
    async fn enqueue_answer(&self, identity: &Identity, body: Vec<u8>) {
        let queued = self.relay.history.enqueue_wait(identity, body).await;
        tracing::info!(parent: &self.span, queued, streaming = self.streaming, "relay_cache");
    }
}

impl Relay {
    async fn conversation(self: &Arc<Self>, info: RequestInfo<'_>, primary: Duration) -> Response {
        let RequestInfo {
            path,
            query,
            headers,
            raw,
            request,
            upstream,
            streaming,
            sulphur,
            deadline,
        } = info;
        let c = &self.c;
        let forwarded = filtered(&headers, &["content-type", "accept", "x-request-id"], false);
        let mut auth = headers.get_all("authorization").iter();
        let credential = auth
            .next()
            .map(HeaderValue::as_bytes)
            .filter(|bytes| !bytes.is_empty() && auth.next().is_none());
        let format = protocol::replay_format(&request);
        let one = request.get("n").is_none_or(|n| n.as_i64() == Some(1));
        let reject = c.reject_empty_json
            && one
            && (format == Some("json_object")
                || format == Some("text") && request.get("response_format").is_none());
        let allow_prompt = sulphur && !streaming && one && format == Some("text");
        // 没有唯一凭据的请求无法安全查历史，直接跳过正文规范化和图片摘要的计算。
        let identity = credential.filter(|_| format.is_some()).and_then(|token| {
            let digest = context_digest(&path, query, &forwarded, token);
            canonicalize("default", &format!("{}:{digest}", c.policy), &request)
        });
        let mut gateway_headers = forwarded;
        gateway_headers
            .entry("content-type")
            .or_insert(HeaderValue::from_static("application/json"));
        for h in headers.get_all("authorization") {
            gateway_headers.append("authorization", h.clone());
        }
        // 无凭据或 Authorization 重复时，仍可调用模型；历史读写只允许单一、非空凭据。
        let turn = Arc::new(Turn {
            relay: self.clone(),
            span: tracing::Span::current(),
            request,
            identity,
            streaming,
            reject,
            deadline,
        });
        let failure = match turn.attempt(upstream, raw, gateway_headers, primary).await {
            UpstreamOutcome::Forward(response) => return response,
            UpstreamOutcome::CheckHistory(response) => response,
        };
        let primary_status = failure.status().as_u16();
        self.count("fallback_entered");
        tracing::info!(
            outcome = "entered",
            primary_retry_status = primary_status,
            "relay_fallback"
        );
        let mut outcome = "ineligible";
        if let Some(id) = turn.identity.as_ref() {
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .saturating_sub(Duration::from_millis(20));
            if !remaining.is_zero() {
                let validate =
                    |body: &[u8]| protocol::prepare_history(body, &turn.request, streaming, reject);
                // 上游已结束，剩余时间可以用来排队等向量回放。仍给发送响应留出余量。
                match timeout(remaining, self.history.lookup(Some(id), validate)).await {
                    Ok((Some(body), reason)) => {
                        self.count(&format!("fallback_{reason}"));
                        tracing::info!(outcome = "hit", reason, "relay_history");
                        return reply(
                            200,
                            Body::from(body),
                            HeaderValue::from_static(if streaming {
                                "text/event-stream"
                            } else {
                                "application/json"
                            }),
                            Some("history"),
                        );
                    }
                    Ok((None, reason)) => outcome = reason,
                    Err(_) => outcome = "unavailable",
                }
            }
        }
        self.count(&format!("lookup_{outcome}"));
        tracing::info!(outcome = "miss", reason = outcome, "relay_history");
        if allow_prompt && let Some(prompt) = sulphur::prompt(&turn.request) {
            self.count("fallback_prompt_echo");
            tracing::info!(outcome = "prompt", "relay_fallback");
            let created = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let body = json!({"id":format!("chatcmpl-{}",Uuid::new_v4().simple()),"object":"chat.completion","created":created,"model":turn.request["model"],
                "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":prompt}}]});
            return reply(
                200,
                Body::from(body.to_string()),
                HeaderValue::from_static("application/json"),
                Some("prompt"),
            );
        }
        tracing::info!(
            outcome = "exhausted",
            reason = outcome,
            response_status = primary_status,
            "relay_fallback"
        );
        failure
    }
}

/// Python 的 json.dumps 默认带空格并转义非 ASCII 字符。摘要的输入必须逐字节一致，
/// 否则移植前写入的 Redis 和 MySQL 记录就无法命中。
fn context_digest(path: &str, query: &str, headers: &HeaderMap, credential: &[u8]) -> String {
    let quoted =
        |s: &str| json::escape_non_ascii(serde_json::to_string(s).expect("JSON 字符串可序列化"));
    let mut business = BTreeMap::new();
    for (key, value) in headers {
        if key != "x-request-id" {
            let latin1: String = value.as_bytes().iter().map(|b| char::from(*b)).collect();
            business.insert(key.as_str(), latin1);
        }
    }
    let pairs: Vec<_> = business
        .iter()
        .map(|(k, v)| format!("{}: {}", quoted(k), quoted(v)))
        .collect();
    let mut parts = vec![
        quoted(path),
        quoted(&hex::encode(query.as_bytes())),
        format!("{{{}}}", pairs.join(", ")),
    ];
    parts.push(format!(
        "[\"client-auth-v1\", \"{:x}\"]",
        Sha256::digest(credential)
    ));
    format!(
        "{:x}",
        Sha256::digest(format!("[{}]", parts.join(", ")).as_bytes())
    )
}

#[cfg(test)]
mod tests;

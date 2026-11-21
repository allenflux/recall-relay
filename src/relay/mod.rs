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
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout, timeout_at},
};
use upstream::Source;
use uuid::Uuid;

pub struct Relay {
    c: RelayConfig,
    upstream: Url,
    models_url: Option<Url>,
    history: Arc<History>,
    client: reqwest::Client,
    slots: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
    buffered: Arc<AtomicUsize>,
    counters: Mutex<HashMap<String, u64>>,
}

impl Relay {
    pub fn with_slots(
        config: RelayConfig,
        history: Arc<History>,
        client: reqwest::Client,
        slots: Arc<Semaphore>,
    ) -> Self {
        let upstream = Url::parse(&config.upstream_url).expect("URL checked at startup");
        let models_url = crate::config::models_url(&upstream);
        Self {
            upstream,
            models_url,
            slots,
            waiting: Arc::new(Semaphore::new(config.max_pending)),
            cpu: Arc::new(Semaphore::new(config.cpu_max_inflight)),
            buffered: Arc::new(AtomicUsize::new(0)),
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
    async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        if let Ok(slot) = self.slots.clone().try_acquire_owned() {
            return Some(slot);
        }
        // 排队时还没有读取请求体，等待者最多占用一个队列名额。离开作用域自动归还。
        let _pending = self.waiting.clone().try_acquire_owned().ok()?;
        timeout(self.c.queue_timeout, self.slots.clone().acquire_owned())
            .await
            .ok()?
            .ok()
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

/// 只要请求还活着，它申请过的正文空间就由这个对象负责归还。超时、return、客户端断开
/// 都会触发 Drop，无需在每条错误路径手动减计数。
struct Budget {
    buffered: Arc<AtomicUsize>,
    bytes: usize,
    _slot: OwnedSemaphorePermit,
}
impl Budget {
    fn charge(&mut self, size: usize, limit: usize) -> bool {
        if self
            .buffered
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(size).filter(|sum| *sum <= limit)
            })
            .is_err()
        {
            return false;
        }
        self.bytes += size;
        true
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        self.buffered.fetch_sub(self.bytes, Ordering::Relaxed);
    }
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
    let request = relay.client.get(url).headers(filtered(
        headers,
        &["authorization", "accept", "x-request-id"],
        true,
    ));
    let result = timeout(
        relay.c.upstream_timeout.min(Duration::from_secs(10)),
        async {
            let upstream = request.send().await.ok()?;
            let status = upstream.status();
            let mut headers = filtered(
                upstream.headers(),
                &["content-type", "retry-after", "www-authenticate"],
                true,
            );
            headers.insert("cache-control", HeaderValue::from_static("no-store"));
            let body = upstream::read_bounded(upstream, relay.c.max_response)
                .await
                .ok()?;
            let mut response = Response::new(Body::from(body));
            *response.status_mut() = status;
            *response.headers_mut() = headers;
            Some(response)
        },
    )
    .await;
    result
        .ok()
        .flatten()
        .unwrap_or_else(|| error(503, "model_list_unavailable", true, relay.c.retry_after))
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
    let Some(slot) = relay.acquire().await else {
        relay.count("local_rejected");
        return error(429, "local_capacity", true, c.retry_after);
    };
    let mut budget = Budget {
        buffered: relay.buffered.clone(),
        bytes: 0,
        _slot: slot,
    };
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
        return delivery::bounded(result, budget, initial_deadline);
    }

    let upload = async {
        let mut raw = Vec::new();
        let mut chunks = body.into_data_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|_| error(400, "invalid_request", false, c.retry_after))?;
            if chunk.len() > c.max_body - raw.len() {
                return Err(error(413, "request_too_large", false, c.retry_after));
            }
            if !budget.charge(chunk.len(), c.max_buffered_body) {
                relay.count("local_body_rejected");
                return Err(error(429, "local_body_capacity", true, c.retry_after));
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
    let (total, primary, alternate) = c.timeouts(streaming, sulphur);
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
            alternate,
        ),
    )
    .await
    .unwrap_or_else(|_| error(503, "request_deadline", true, c.retry_after));
    delivery::bounded(response, budget, deadline)
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
    request: Value,
    identity: Option<Identity>,
    streaming: bool,
    reject: bool,
    deadline: Instant,
    request_id: String,
}

impl Relay {
    async fn conversation(
        self: &Arc<Self>,
        info: RequestInfo<'_>,
        primary: Duration,
        alternate: Duration,
    ) -> Response {
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
        let digest = context_digest(&path, query, &forwarded, credential);
        let format = protocol::replay_format(&request);
        let one = request.get("n").is_none_or(|n| n.as_i64() == Some(1));
        let reject = c.reject_empty_json
            && one
            && (format == Some("json_object")
                || format == Some("text") && request.get("response_format").is_none());
        let allow_prompt = sulphur && !streaming && one && format == Some("text");
        let identity = format
            .and_then(|_| canonicalize("default", &format!("{}:{digest}", c.policy), &request));
        let mut cpu_headers = forwarded;
        cpu_headers
            .entry("content-type")
            .or_insert(HeaderValue::from_static("application/json"));
        let mut gateway_headers = cpu_headers.clone();
        for h in headers.get_all("authorization") {
            gateway_headers.append("authorization", h.clone());
        }
        let request_id = headers
            .get_all("x-relay-request-id")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|v| {
                v.len() == 32
                    && v.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            .unwrap_or("-")
            .to_owned();

        let mut stages = Vec::new();
        if identity.is_some() {
            let images = request["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|m| {
                    m["content"]
                        .as_array()
                        .is_some_and(|parts| parts.iter().any(|p| p["type"] == "image_url"))
                });
            if !c.fallback_model.is_empty()
                && request["model"] != c.fallback_model
                && (!images || c.fallback_model_multimodal)
            {
                stages.push((
                    Source::Model,
                    upstream.clone(),
                    &c.fallback_model,
                    alternate,
                ));
            }
            if !c.cpu_fallback_url.is_empty() && (!images || c.cpu_fallback_multimodal) {
                stages.push((
                    Source::Cpu,
                    Url::parse(&c.cpu_fallback_url).expect("URL checked at startup"),
                    &c.cpu_fallback_model,
                    c.cpu_fallback_timeout,
                ));
            }
        }
        let mut reserve: Duration = stages.iter().map(|s| s.3).sum();
        // 无凭据或 Authorization 重复时，仍可调用模型；历史读写只允许单一、非空凭据。
        let turn = Arc::new(Turn {
            relay: self.clone(),
            request,
            identity: identity.filter(|_| credential.is_some()),
            streaming,
            reject,
            deadline,
            request_id,
        });
        let primary_status = match turn
            .attempt(
                upstream,
                raw,
                gateway_headers.clone(),
                Source::Primary,
                primary,
                c.fallback_budget + reserve,
            )
            .await
        {
            Ok(response) => return response,
            Err(status) => status,
        };
        self.count("fallback_entered");
        let mut outcome = "ineligible";
        if let Some(id) = turn.identity.as_ref() {
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .saturating_sub(reserve + Duration::from_millis(20));
            if !remaining.is_zero() {
                let validate =
                    |body: &[u8]| protocol::prepare_history(body, &turn.request, streaming, reject);
                match timeout(
                    c.fallback_budget.min(remaining),
                    self.history.lookup(Some(id), validate),
                )
                .await
                {
                    Ok((Some(body), reason)) => {
                        self.count(&format!("fallback_{reason}"));
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
        let had_stages = !stages.is_empty();
        for (source, url, model, duration) in stages {
            reserve = reserve.saturating_sub(duration);
            let mut body = turn.request.clone();
            body["model"] = json!(model);
            let mut cpu_slot = None;
            let stage_headers = if source == Source::Cpu {
                let Ok(slot) = self.cpu.clone().try_acquire_owned() else {
                    self.count("cpu_rejected");
                    continue;
                };
                cpu_slot = Some(slot);
                if let Some(map) = body.as_object_mut() {
                    map.remove("chat_template_kwargs");
                }
                cpu_headers.clone()
            } else {
                gateway_headers.clone()
            };
            if let Ok(mut response) = turn
                .attempt(
                    url,
                    Bytes::from(body.to_string()),
                    stage_headers,
                    source,
                    duration,
                    reserve,
                )
                .await
            {
                if let Some(slot) = cpu_slot {
                    response.extensions_mut().insert(Arc::new(slot));
                }
                return response;
            }
        }
        if allow_prompt && let Some(prompt) = sulphur::prompt(&turn.request) {
            self.count("fallback_prompt_echo");
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
        error(
            primary_status,
            if outcome == "unavailable" && !had_stages {
                "history_unavailable"
            } else {
                "retryable_upstream_failure"
            },
            true,
            c.retry_after,
        )
    }
}

/// Python 的 json.dumps 默认带空格并转义非 ASCII 字符。摘要的输入必须逐字节一致，
/// 否则移植前写入的 Redis 和 MySQL 记录就无法命中。
fn context_digest(
    path: &str,
    query: &str,
    headers: &HeaderMap,
    credential: Option<&[u8]>,
) -> String {
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
    if let Some(credential) = credential {
        parts.push(format!(
            "[\"client-auth-v1\", \"{:x}\"]",
            Sha256::digest(credential)
        ));
    }
    format!(
        "{:x}",
        Sha256::digest(format!("[{}]", parts.join(", ")).as_bytes())
    )
}

#[cfg(test)]
mod tests;

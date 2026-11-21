//! 每个聊天请求只调用一次固定上游。正常响应直接转发；可回放的故障保留原响应，
//! 调用方查不到历史时，仍能把上游的错误状态、正文和重试时间交给客户端。

use super::{
    Turn, error,
    protocol::{self, Fault},
    reply, sse,
};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, HeaderValue},
    response::Response,
};
use futures_util::TryStreamExt;
use reqwest::Url;
use std::{io, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

pub(super) enum UpstreamOutcome {
    Forward(Response),
    CheckHistory(Response),
}
pub(super) async fn read_bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Bytes, Fault> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Fault::Transport)? {
        if chunk.len() > limit - body.len() {
            return Err(Fault::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

impl Turn {
    pub(super) async fn attempt(
        self: &Arc<Self>,
        url: Url,
        raw: Bytes,
        headers: HeaderMap,
        allowance: Duration,
    ) -> UpstreamOutcome {
        let available = self
            .deadline
            .saturating_duration_since(Instant::now())
            .saturating_sub(Duration::from_millis(20));
        let mut remaining = available.saturating_sub(self.relay.c.fallback_budget);
        // 上传较慢时，仍让主模型在剩余的总预算内尝试一次。
        if remaining.is_zero() {
            remaining = available;
        }
        if remaining.is_zero() {
            tracing::info!(
                stage = "primary",
                event = "skipped",
                reason = "deadline",
                "relay_stage"
            );
            return UpstreamOutcome::CheckHistory(error(
                503,
                "request_deadline",
                true,
                self.relay.c.retry_after,
            ));
        }
        self.relay.count("upstream_calls");
        let started = Instant::now();
        let cutoff = started + allowance.min(remaining);
        let model = self.request["model"].as_str();
        // 上游 URL 可能携带固定查询参数，日志只记录主机、路径及有限长度的普通模型标识。
        let model = model.filter(|name| {
            name.len() <= 160
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_./:".contains(&b))
        });
        tracing::info!(
            stage = "primary",
            event = "attempt",
            model,
            host = url.host_str(),
            path = url.path(),
            streaming = self.streaming,
            budget_ms = allowance.min(remaining).as_millis() as u64,
            "relay_stage"
        );
        let mut status = None;
        let mut terminal = None;
        let mut failure_status = 503;
        let mut upstream_error = None;
        let mut reason = "http_error";
        let result = timeout_at(cutoff, async {
            let mut upstream = self
                .relay
                .client
                .post(url)
                .headers(headers)
                .body(raw)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        Fault::Timeout
                    } else {
                        if error.is_connect() {
                            reason = "connection_error";
                        }
                        Fault::Transport
                    }
                })?;
            let code = upstream.status().as_u16();
            status = Some(code);
            let ty = upstream.headers().get("content-type").cloned();
            let media = ty
                .as_ref()
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            let is_json = media == "application/json" || media.ends_with("+json");
            let is_sse = media == "text/event-stream";
            let ty = ty.unwrap_or_else(|| {
                HeaderValue::from_static(if self.streaming && code == 200 {
                    "text/event-stream"
                } else {
                    "application/json"
                })
            });
            let retry = upstream.headers().get("retry-after").cloned();
            let challenge = upstream.headers().get("www-authenticate").cloned();
            let finish = |body: Body| {
                let mut response = reply(code, body, ty.clone(), None);
                // 厂商也可能用 HTTP 200 包装错误，这两个上游提示头跟随原响应。
                if let Some(value) = &retry {
                    response.headers_mut().insert("retry-after", value.clone());
                }
                if let Some(value) = &challenge {
                    response
                        .headers_mut()
                        .insert("www-authenticate", value.clone());
                }
                response
            };
            let limit = self.relay.c.max_response;

            if code != 200 {
                // 这些状态已经明确表示端点、鉴权或策略问题，即使读取正文失败也直接返回。
                if code < 400 || matches!(code, 401 | 403 | 404 | 407 | 451) {
                    terminal = Some(code);
                } else {
                    failure_status = code;
                }
                let error_limit = if matches!(code, 408 | 429) || code >= 500 {
                    limit.min(64 * 1024)
                } else {
                    limit
                };
                let body = read_bounded(upstream, error_limit).await?;
                if terminal.is_some() || protocol::terminal_error(&body) {
                    reason = "terminal_upstream_response";
                    return Ok(Some(finish(Body::from(body))));
                }
                // 未知错误和已知能力限制都查缓存，分类仅用于记录故障原因。
                if code == 400
                    && let Some((kind, _)) = protocol::capability_failure(&body)
                {
                    reason = kind;
                }
                upstream_error = Some(finish(Body::from(body)));
                return Ok(None);
            }
            let replayable_format = protocol::replay_format(&self.request).is_some();
            if !self.streaming || (is_json && replayable_format) {
                let body = read_bounded(upstream, limit).await?;
                let capture = if !self.streaming && self.identity.is_some() && is_json {
                    protocol::prepare_history(&body, &self.request, false, self.reject)
                } else {
                    None
                };
                if capture.is_none() && replayable_format {
                    let validation = if self.streaming {
                        protocol::strict_json(&body)
                            .as_ref()
                            .map_or(Ok(()), protocol::embedded_error)
                    } else {
                        protocol::validate_live(&body, self.reject)
                    };
                    if let Err(fault) = validation {
                        self.relay.count("ingest_invalid");
                        if matches!(fault, Fault::Temporary(_) | Fault::UpstreamError) {
                            // 某些网关通过 HTTP 200 返回 JSON 错误。缓存未命中时原样交给客户端。
                            upstream_error = Some(finish(Body::from(body)));
                        }
                        return Err(fault);
                    }
                }
                if self.streaming {
                    return Ok(Some(finish(Body::from(body))));
                }
                let turn = self.clone();
                // 生成器先交付正文，再入队。客户端提前断开会丢弃生成器。
                let output = async_stream::stream! {
                    yield Ok::<_, io::Error>(body);
                    if let (Some(identity), Some(capture)) = (&turn.identity, capture) {
                        turn.enqueue_answer(identity, capture);
                    } else {
                        turn.relay.count("ingest_invalid");
                    }
                };
                return Ok(Some(finish(Body::from_stream(output))));
            }
            let (first, opening_error) =
                sse::first_chunks(&mut upstream, is_sse, limit, self.reject).await?;
            let limits = crate::delivery::Limits {
                wall: if is_sse {
                    Instant::now() + self.relay.c.stream_timeout
                } else {
                    cutoff
                },
                idle: is_sse.then_some(allowance),
            };
            let output =
                self.streaming_body(upstream, first, limits, is_sse && opening_error.is_none());
            let mut response = finish(output);
            response.extensions_mut().insert(limits);
            if let Some(fault) = opening_error {
                // 保留首个事件和其余流。若查到历史，丢弃这个响应也会关闭对原上游的读取。
                upstream_error = Some(response);
                return Err(fault);
            }
            Ok(Some(response))
        })
        .await
        .unwrap_or(Err(Fault::Timeout));
        match result {
            Ok(Some(response)) => {
                // 流式请求在此只选定响应，剩余字节仍受流读取超时和完整性检查约束。
                let reason = if response.status().is_success() {
                    "accepted"
                } else {
                    reason
                };
                tracing::info!(
                    stage = "primary",
                    event = "response",
                    reason,
                    upstream_status = status,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "relay_stage"
                );
                return UpstreamOutcome::Forward(response);
            }
            Ok(None) => {}
            Err(fault) => {
                if !(fault == Fault::Transport && reason == "connection_error") {
                    reason = fault.reason();
                }
                if matches!(fault, Fault::Temporary(_)) {
                    failure_status = fault.status();
                }
            }
        }
        tracing::info!(
            stage = "primary",
            event = "failed",
            reason,
            upstream_status = status,
            retry_status = terminal.is_none().then_some(failure_status),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "relay_stage"
        );
        let retry_after = self.relay.c.retry_after;
        match terminal {
            Some(code @ (401 | 403)) => UpstreamOutcome::Forward(error(
                code,
                "upstream_authentication_failed",
                false,
                retry_after,
            )),
            Some(code) => UpstreamOutcome::Forward(error(
                if code >= 400 { code } else { 503 },
                "upstream_error_response_interrupted",
                code < 400,
                retry_after,
            )),
            None => UpstreamOutcome::CheckHistory(upstream_error.unwrap_or_else(|| {
                error(
                    failure_status,
                    "retryable_upstream_failure",
                    true,
                    retry_after,
                )
            })),
        }
    }

    fn streaming_body(
        self: &Arc<Self>,
        mut upstream: reqwest::Response,
        first: Vec<Bytes>,
        limits: crate::delivery::Limits,
        check_completion: bool,
    ) -> Body {
        let turn = self.clone();
        let can_store = self.identity.is_some() && check_completion;
        let next_cutoff = move || {
            limits
                .idle
                .map_or(limits.wall, |idle| limits.wall.min(Instant::now() + idle))
        };
        let mut cutoff = next_cutoff();
        let stream = async_stream::try_stream! {
            let mut tracker = sse::Completion::default();
            let mut capture = can_store.then(Vec::new);
            let mut initial = first.into_iter();
            loop {
                let next = if let Some(first) = initial.next() {
                    Some(first)
                } else {
                    timeout_at(cutoff, upstream.chunk())
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "upstream stream timed out"))?
                        .map_err(|_| io::Error::other("upstream stream interrupted"))?
                };
                let Some(chunk) = next else { break; };
                if chunk.is_empty() {
                    continue;
                }
                cutoff = next_cutoff();
                if check_completion {
                    tracker.feed(&chunk, turn.relay.c.max_response);
                }
                if let Some(body) = capture.as_mut() {
                    if body.len() + chunk.len() <= turn.relay.c.max_ingest {
                        body.extend_from_slice(&chunk);
                    } else {
                        // 超出缓存上限后仍照常转发，不再积累历史副本。
                        capture = None;
                    }
                }
                // yield 会把控制权交回 HTTP 层；下游准备好之前不会拉取下一块上游数据。
                yield chunk;
            }
            if check_completion && tracker.incomplete() {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete upstream OpenAI SSE"))?;
            }
            if let (Some(identity), Some(captured)) = (&turn.identity, capture)
                && let Some(body) = protocol::prepare_history(&captured, &turn.request, true, turn.reject) {
                turn.enqueue_answer(identity, body);
            }
        };
        // 明确错误类型，使生成器内的 ? 使用标准 I/O 错误终止流。
        fn typed<S: futures_util::Stream<Item = Result<Bytes, io::Error>>>(stream: S) -> S {
            stream
        }
        // HTTP 响应体稍后会在独立任务中读取。保存此请求的 span，确保半途断流也能关联到同一请求。
        let span = self.span.clone();
        Body::from_stream(typed(stream).inspect_err(move |error| {
            let reason = match error.kind() {
                io::ErrorKind::TimedOut => "timeout",
                io::ErrorKind::UnexpectedEof => "incomplete_sse",
                _ => "transport_error",
            };
            tracing::info!(parent: &span, stage = "primary", event = "stream_failed", reason, "relay_stage");
        }))
    }
}

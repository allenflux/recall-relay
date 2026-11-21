//! 一次调用只访问一个预先配置的地址。返回 Ok 表示已选定交给客户端的响应；
//! Err(status) 表示尚未提交任何响应，可以由调用方进入下一个降级阶段。

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
use reqwest::Url;
use std::{io, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    Primary,
    Model,
    Cpu,
}
impl Source {
    pub fn name(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Model => "model",
            Self::Cpu => "cpu",
        }
    }
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
        source: Source,
        allowance: Duration,
        reserve: Duration,
    ) -> Result<Response, u16> {
        let available = self
            .deadline
            .saturating_duration_since(Instant::now())
            .saturating_sub(Duration::from_millis(20));
        let mut remaining = available.saturating_sub(reserve);
        // 上传较慢时，仍让主模型在剩余的总预算内尝试一次。
        if source == Source::Primary && remaining.is_zero() {
            remaining = available;
        }
        if remaining.is_zero() {
            return Err(503);
        }
        self.relay.count(match source {
            Source::Primary => "upstream_calls",
            Source::Model => "model_calls",
            Source::Cpu => "cpu_calls",
        });
        let started = Instant::now();
        let cutoff = started + allowance.min(remaining);
        let mut status = None;
        let mut terminal = None;
        let mut failure_status = 503;
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
                .map_err(|_| Fault::Transport)?;
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
            let finish = |body: Body| {
                reply(
                    code,
                    body,
                    ty.clone(),
                    (source != Source::Primary && code == 200).then_some(source.name()),
                )
            };
            let limit = self.relay.c.max_response;

            if code == 408 || code == 429 || code >= 500 {
                if code == 429 {
                    failure_status = 429;
                }
                let body = read_bounded(upstream, limit.min(64 * 1024)).await?;
                if protocol::model_not_found(&body) {
                    self.relay.count("terminal_model_not_found");
                    return Ok(Some(finish(Body::from(body))));
                }
                return Ok(None);
            }
            if code != 200 {
                // 已收到授权失败等业务状态。读取错误正文中断也不能转成历史回放。
                terminal = Some(code);
                let body = read_bounded(upstream, limit).await?;
                if code == 400
                    && source != Source::Model
                    && let Some((kind, s)) = protocol::capability_failure(&body)
                    && (source == Source::Primary || kind == "context_limit")
                {
                    terminal = None;
                    failure_status = s;
                    reason = kind;
                    if source == Source::Cpu {
                        self.relay.count("cpu_context_exceeded");
                    }
                    return Ok(None);
                }
                return Ok(Some(finish(Body::from(body))));
            }
            let replayable_format = protocol::replay_format(&self.request).is_some();
            if self.streaming && is_json && replayable_format {
                let body = read_bounded(upstream, limit).await?;
                if let Some(v) = protocol::strict_json(&body) {
                    protocol::embedded_error(&v)?;
                }
                return Ok(Some(finish(Body::from(body))));
            }
            let can_store = source == Source::Primary && self.identity.is_some();
            if !self.streaming {
                let body = read_bounded(upstream, limit).await?;
                let capture = if can_store && is_json {
                    protocol::prepare_history(&body, &self.request, false, self.reject)
                } else {
                    None
                };
                if capture.is_none() && replayable_format {
                    protocol::validate_live(&body, self.reject).inspect_err(|_| {
                        if source == Source::Primary {
                            self.relay.count("ingest_invalid");
                        }
                    })?;
                }
                let turn = self.clone();
                // 生成器先交付正文，再入队。客户端提前断开会丢弃生成器。
                let output = async_stream::stream! {
                    yield Ok::<_, io::Error>(body);
                    if let (Some(identity), Some(capture)) = (&turn.identity, capture) {
                        turn.relay.history.enqueue(identity, capture);
                    } else if source == Source::Primary {
                        turn.relay.count("ingest_invalid");
                    }
                };
                return Ok(Some(finish(Body::from_stream(output))));
            }
            let first = sse::first_chunks(&mut upstream, is_sse, limit, self.reject).await?;
            let output = self.streaming_body(upstream, first, is_sse, can_store, allowance, cutoff);
            let mut response = finish(output);
            response.extensions_mut().insert(crate::delivery::Limits {
                wall: if is_sse {
                    Instant::now() + self.relay.c.stream_timeout
                } else {
                    cutoff
                },
                idle: is_sse.then_some(allowance),
            });
            Ok(Some(response))
        })
        .await
        .unwrap_or(Err(Fault::Timeout));
        match result {
            Ok(Some(response)) => return Ok(response),
            Ok(None) => {}
            Err(fault) => {
                reason = fault.reason();
                if matches!(fault, Fault::Temporary(_)) {
                    failure_status = fault.status();
                }
            }
        }
        eprintln!(
            "relay_stage request_id={} stage={} reason={} http={} elapsed_s={:.3}",
            self.request_id,
            source.name(),
            reason,
            status.map(|c| c.to_string()).as_deref().unwrap_or("-"),
            started.elapsed().as_secs_f64()
        );
        let retry_after = self.relay.c.retry_after;
        match terminal {
            Some(code @ (401 | 403)) => Ok(error(
                code,
                "upstream_authentication_failed",
                false,
                retry_after,
            )),
            Some(_) => Ok(error(
                503,
                "upstream_error_response_interrupted",
                true,
                retry_after,
            )),
            None => Err(failure_status),
        }
    }

    fn streaming_body(
        self: &Arc<Self>,
        mut upstream: reqwest::Response,
        first: Vec<Bytes>,
        is_sse: bool,
        can_store: bool,
        idle: Duration,
        initial_cutoff: Instant,
    ) -> Body {
        let turn = self.clone();
        let c = &turn.relay.c;
        let wall = if is_sse {
            Instant::now() + c.stream_timeout
        } else {
            initial_cutoff
        };
        let mut cutoff = if is_sse {
            wall.min(Instant::now() + idle)
        } else {
            wall
        };
        let stream = async_stream::try_stream! {
            let mut tracker = sse::Completion::default();
            let mut capture = (can_store && is_sse).then(Vec::new);
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
                if is_sse {
                    tracker.feed(&chunk, turn.relay.c.max_response);
                    cutoff = wall.min(Instant::now() + idle);
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
            if is_sse && tracker.incomplete() {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete upstream OpenAI SSE"))?;
            }
            if let (Some(identity), Some(captured)) = (&turn.identity, capture)
                && let Some(body) = protocol::prepare_history(&captured, &turn.request, true, turn.reject) {
                turn.relay.history.enqueue(identity, body);
            }
        };
        // 明确错误类型，使生成器内的 ? 使用标准 I/O 错误终止流。
        fn typed<S: futures_util::Stream<Item = Result<Bytes, io::Error>>>(stream: S) -> S {
            stream
        }
        Body::from_stream(typed(stream))
    }
}

//! 在入口观察原始 HTTP 正文。每个方向最多预览 2 KiB，后续字节只计数。
//! 不单独读流：下游读到一帧，这里才看到一帧，然后将同一帧交还给下游。

use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::{HeaderMap, header},
    middleware::Next,
    response::Response,
};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tracing::{Instrument, Span, info, info_span};
use uuid::Uuid;

const LIMIT: usize = 2048;

#[derive(Clone, Copy)]
pub(crate) struct ShowContent;

/// 处理器已经解析了聊天 JSON，可以直接找到实际提问。长图片在请求开头时，
/// 即使前 2 KiB 都是图片，也能看到最后一条用户消息的文字部分。
pub(crate) fn image_prompt(request: &serde_json::Value, raw: &[u8]) {
    if raw.len() <= LIMIT || !raw[..LIMIT].windows(8).any(|s| s == b";base64,") {
        return;
    }
    let Some(message) = request["messages"]
        .as_array()
        .and_then(|messages| messages.iter().rev().find(|m| m["role"] == "user"))
    else {
        return;
    };
    let content = &message["content"];
    let text = content.as_str().or_else(|| {
        content
            .as_array()?
            .iter()
            .find_map(|part| part["text"].as_str().filter(|_| part["type"] == "text"))
    });
    if let Some(text) = text {
        let end = text.floor_char_boundary(256);
        info!(prompt = ?&text[..end], omitted_bytes = text.len() - end, "图片请求的提问");
    }
}

pub(crate) async fn observe(request: Request, next: Next, show_body: bool) -> Response {
    let span = info_span!("http", request_id = %Uuid::new_v4().simple());
    async move {
        let started = Instant::now();
        info!(method = %request.method(), path = request.uri().path(), "收到请求");
        let (mut head, body) = request.into_parts();
        if show_body {
            head.extensions.insert(ShowContent);
        }
        let body = observe_body(body, &head.headers, "请求", started, show_body);
        let response = next.run(Request::from_parts(head, body)).await;
        let (head, body) = response.into_parts();
        let fallback = head
            .headers
            .get("x-failover-source")
            .and_then(|s| s.to_str().ok())
            .filter(|s| matches!(*s, "history" | "prompt"));
        info!(
            status = head.status.as_u16(),
            fallback_source = fallback,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "返回响应头"
        );
        let body = observe_body(body, &head.headers, "响应", started, show_body);
        Response::from_parts(head, body)
    }
    .instrument(span)
    .await
}

fn observe_body(
    body: Body,
    headers: &HeaderMap,
    direction: &'static str,
    started: Instant,
    show_body: bool,
) -> Body {
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let textual = media.is_empty()
        || media.starts_with("text/")
        || ["json", "xml", "javascript", "x-www-form-urlencoded"]
            .iter()
            .any(|s| media.ends_with(s));
    let encoded = headers
        .get(header::CONTENT_ENCODING)
        .is_some_and(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"));
    let mut observed = ObservedBody {
        body,
        direction,
        started,
        span: Span::current(),
        bytes: 0,
        previewed: 0,
        pending: Vec::new(),
        finished: false,
        show_body,
        textual: textual && !encoded,
        sse: media == "text/event-stream",
    };
    if observed.body.is_end_stream() {
        observed.finish("完整");
    }
    Body::new(observed)
}

struct ObservedBody {
    body: Body,
    direction: &'static str,
    started: Instant,
    span: Span,
    bytes: usize,
    previewed: usize,
    pending: Vec<u8>,
    finished: bool,
    show_body: bool,
    textual: bool,
    sse: bool,
}

impl ObservedBody {
    fn record(&mut self, chunk: &[u8]) {
        self.bytes = self.bytes.saturating_add(chunk.len());
        if !self.show_body || !self.textual || self.previewed == LIMIT {
            return;
        }
        let count = chunk.len().min(LIMIT - self.previewed);
        self.pending.extend_from_slice(&chunk[..count]);
        self.previewed += count;
        // SSE 按已经到齐的行输出，避免等整个回答结束才看到第一个事件。
        if self.sse
            && let Some(last) = self.pending.iter().rposition(|&b| b == b'\n')
        {
            self.flush(last + 1);
        }
        if self.previewed == LIMIT {
            self.flush(self.pending.len());
        }
    }

    fn flush(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let data = &self.pending[..len];
        // 预览上限可能恰好截在一个中文字中间。只显示完整字符，并将控制字符转义，
        // 防止正文里的换行或终端转义符冒充下一条日志。
        let text = match std::str::from_utf8(data) {
            Ok(s) => s.into(),
            Err(e) if e.error_len().is_none() => String::from_utf8_lossy(&data[..e.valid_up_to()]),
            Err(_) => String::from_utf8_lossy(data),
        };
        let mut line = String::new();
        for c in text.chars() {
            if c.is_control() {
                line.extend(c.escape_debug());
            } else {
                line.push(c);
            }
        }
        // 图片正文仍按原字节传输。日志里只保留 data URI 的类型和短占位提示。
        let mut offset = 0;
        while let Some(pos) = line[offset..].find(";base64,") {
            let start = offset + pos + ";base64,".len();
            let end = start
                + line.as_bytes()[start..]
                    .iter()
                    .take_while(|b| b.is_ascii_alphanumeric() || b"+/=".contains(b))
                    .count();
            if end - start > 128 {
                line.replace_range(start..end, "…");
                offset = start + "…".len();
            } else {
                offset = end;
            }
        }
        self.span
            .in_scope(|| info!(direction = self.direction, body = %line, "正文预览"));
        self.pending.drain(..len);
    }

    fn finish(&mut self, result: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.flush(self.pending.len());
        let omitted = self.bytes.saturating_sub(self.previewed);
        self.span.in_scope(|| {
            info!(
                direction = self.direction,
                result,
                bytes = self.bytes,
                omitted_bytes = omitted,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "正文结束"
            )
        });
    }
}

impl HttpBody for ObservedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.record(data);
                }
                if self.body.is_end_stream() {
                    self.finish("完整");
                }
            }
            Poll::Ready(Some(Err(_))) => self.finish("读取失败"),
            Poll::Ready(None) => self.finish("完整"),
            Poll::Pending => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.finish("提前结束");
    }
}

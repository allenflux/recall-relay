//! SSE 是按空行划分的事件流。一次网络读取可能只有半行，也可能包含多个事件。
//! 在首个业务事件前可以换上游，首个业务事件发给客户端后，只能继续转发或结束连接。

use super::protocol::{self, Fault};
use axum::body::Bytes;
use std::ops::Range;

/// 找到两个换行之间的空行，同时支持 LF 与 CRLF。
fn separator(bytes: &[u8], start: usize) -> Option<Range<usize>> {
    for i in start..bytes.len() {
        if bytes[i] != b'\n' {
            continue;
        }
        let end = match bytes.get(i + 1..) {
            Some([b'\n', ..]) => i + 2,
            Some([b'\r', b'\n', ..]) => i + 3,
            _ => continue,
        };
        let begin = if i > 0 && bytes[i - 1] == b'\r' {
            i - 1
        } else {
            i
        };
        return Some(begin..end);
    }
    None
}

fn lines(event: &[u8]) -> impl Iterator<Item = &[u8]> {
    event
        .split(|b| *b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
}
fn data(event: &[u8]) -> Option<Vec<u8>> {
    let fields: Vec<_> = lines(event)
        .filter_map(|line| line.strip_prefix(b"data:").map(<[u8]>::trim_ascii_start))
        .collect();
    if fields.is_empty() {
        None
    } else {
        Some(fields.join(&b'\n'))
    }
}

enum Opening {
    Candidate(String, bool, bool),
    Forward,
}
fn opening(event: &[u8], reject: bool) -> Result<Opening, Fault> {
    if lines(event).any(|line| matches!(line, b"event: ping" | b"event: heartbeat")) {
        return Ok(Opening::Candidate(String::new(), false, false));
    }
    let raw = data(event).unwrap_or_default();
    if raw.is_empty() || raw == b"[DONE]" {
        return Ok(Opening::Candidate(String::new(), false, raw == b"[DONE]"));
    }
    let Some(v) = protocol::strict_json(&raw) else {
        return Ok(Opening::Forward);
    };
    if v.get("error").is_some() || v.get("refusal").is_some() {
        protocol::embedded_error(&v)?;
        return Ok(Opening::Forward);
    }
    let Some(choices) = v["choices"].as_array() else {
        return Ok(Opening::Forward);
    };
    if choices.is_empty() && v.get("usage").is_some() {
        return Ok(Opening::Candidate(String::new(), false, false));
    }
    let [c] = choices.as_slice() else {
        return Ok(Opening::Forward);
    };
    if !protocol::index_zero(c)
        || !(c["finish_reason"].is_null() || c["finish_reason"] == "stop")
        || !c["refusal"].is_null()
        || c.get("error").is_some()
        || !protocol::plain_fields(&c["delta"], reject)
    {
        return Ok(Opening::Forward);
    }
    let content = &c["delta"]["content"];
    if !content.is_null() && !content.is_string() {
        return Ok(Opening::Forward);
    }
    Ok(Opening::Candidate(
        content.as_str().unwrap_or("").to_owned(),
        c["finish_reason"] == "stop",
        false,
    ))
}

#[derive(Default)]
struct Prelude {
    bytes: Vec<u8>,
    start: usize,
    scan: usize,
    candidate: String,
    finished: bool,
    done: bool,
}
impl Prelude {
    /// 保存已经检查过的偏移，慢速逐字节上传一个长事件时也只线性扫描。
    fn feed(&mut self, bytes: &[u8], reject: bool) -> Result<bool, Fault> {
        self.bytes.extend_from_slice(bytes);
        while let Some(sep) = separator(&self.bytes, self.scan) {
            match opening(&self.bytes[self.start..sep.start], reject)? {
                Opening::Forward => return Ok(true),
                Opening::Candidate(content, stopped, done) => {
                    self.finished |= stopped;
                    self.done |= done;
                    if reject {
                        self.candidate
                            .extend(content.chars().filter(|c| !c.is_whitespace()));
                    } else {
                        self.candidate = content.trim().to_owned();
                    }
                    let still_ambiguous = self.candidate.is_empty()
                        || reject && protocol::could_be_empty(&self.candidate);
                    if !still_ambiguous {
                        return Ok(true);
                    }
                    if self.finished && self.done {
                        if self.candidate.is_empty() || protocol::empty_marker(&self.candidate) {
                            return Err(Fault::Empty);
                        }
                        return Ok(true);
                    }
                }
            }
            self.start = sep.end;
            self.scan = self.start;
        }
        self.scan = self.start.max(self.bytes.len().saturating_sub(3));
        Ok(false)
    }
}

/// 在创建下游响应头之前读取首段。chunk 超出预读上限的剩余部分仍原样转发。
pub(super) async fn first_chunks(
    response: &mut reqwest::Response,
    is_sse: bool,
    limit: usize,
    reject: bool,
) -> Result<Vec<Bytes>, Fault> {
    let mut prelude = Prelude::default();
    while let Some(chunk) = response.chunk().await.map_err(|_| Fault::Transport)? {
        if chunk.is_empty() {
            continue;
        }
        if !is_sse {
            return Ok(vec![chunk]);
        }
        let take = chunk.len().min(limit - prelude.bytes.len());
        if prelude.feed(&chunk[..take], reject)? {
            let mut output = vec![Bytes::from(prelude.bytes)];
            if take < chunk.len() {
                output.push(chunk.slice(take..));
            }
            return Ok(output);
        }
        if prelude.bytes.len() >= limit {
            return Err(Fault::Protocol);
        }
    }
    Err(Fault::Protocol)
}

#[derive(Default)]
pub(super) struct Completion {
    buffer: Vec<u8>,
    scan: usize,
    known: bool,
    finished: bool,
    done: bool,
    invalid: bool,
    opaque: bool,
}
impl Completion {
    pub fn feed(&mut self, mut chunk: &[u8], limit: usize) {
        while !chunk.is_empty() && !self.opaque {
            let room = limit - self.buffer.len();
            if room == 0 {
                self.opaque = true;
                self.buffer.clear();
                return;
            }
            let take = chunk.len().min(room);
            self.buffer.extend_from_slice(&chunk[..take]);
            chunk = &chunk[take..];
            let mut start = 0;
            while let Some(sep) = separator(&self.buffer, self.scan) {
                if let Some(data) = data(&self.buffer[start..sep.start]) {
                    self.event(&data);
                }
                start = sep.end;
                self.scan = start;
            }
            self.buffer.drain(..start);
            self.scan = self.buffer.len().saturating_sub(3);
        }
    }
    fn event(&mut self, data: &[u8]) {
        if data == b"[DONE]" {
            self.done = true;
            return;
        }
        let Some(v) = protocol::strict_json(data) else {
            self.invalid |= self.known;
            return;
        };
        let Some(choices) = v["choices"].as_array() else {
            return;
        };
        if choices.is_empty() || !choices.iter().all(|c| c["delta"].is_object()) {
            return;
        }
        self.invalid |= self.done;
        self.known = true;
        self.finished |= choices
            .iter()
            .all(|c| c["finish_reason"].as_str().is_some_and(|s| !s.is_empty()));
    }
    pub fn incomplete(&self) -> bool {
        self.known
            && (self.invalid
                || self.opaque
                || !self.finished
                || !self.done
                || !self.buffer.trim_ascii().is_empty())
    }
}

pub(super) fn history_text(raw: &[u8]) -> Option<(String, Vec<u8>)> {
    std::str::from_utf8(raw).ok()?;
    let mut body = Vec::with_capacity(raw.len());
    let mut text = String::new();
    let (mut done, mut stopped) = (false, false);
    let mut start = 0;
    while let Some(sep) = separator(raw, start) {
        let event = &raw[start..sep.start];
        let mut replacement = None;
        if let Some(data) = data(event) {
            if done {
                return None;
            }
            if data == b"[DONE]" {
                done = true;
            } else {
                let mut v = protocol::strict_json(&data)?;
                if v.get("error").is_some() || v.get("refusal").is_some() {
                    return None;
                }
                let choices = v["choices"].as_array()?;
                if !(stopped && choices.is_empty() && v.get("usage").is_some()) {
                    let [c] = choices.as_slice() else {
                        return None;
                    };
                    let d = c["delta"].as_object()?;
                    if stopped
                        || !protocol::index_zero(c)
                        || d.keys().any(|k| !matches!(k.as_str(), "role" | "content"))
                        || d.get("role").is_some_and(|r| r != "assistant")
                    {
                        return None;
                    }
                    if let Some(content) = d.get("content").filter(|v| !v.is_null()) {
                        text.push_str(content.as_str()?);
                    }
                    if !c["finish_reason"].is_null() {
                        if c["finish_reason"] != "stop" {
                            return None;
                        }
                        stopped = true;
                    }
                }
                replacement = protocol::remove_echo(&mut v);
            }
        }
        if let Some(clean) = replacement {
            let newline: &[u8] = if raw[sep.clone()].starts_with(b"\r\n") {
                b"\r\n"
            } else {
                b"\n"
            };
            for line in lines(event).filter(|s| !s.starts_with(b"data:")) {
                body.extend(line);
                body.extend(newline);
            }
            body.extend(b"data: ");
            body.extend(clean);
        } else {
            body.extend(event);
        }
        body.extend(&raw[sep.clone()]);
        start = sep.end;
    }
    if !done || !stopped || !raw[start..].trim_ascii().is_empty() {
        return None;
    }
    body.extend(&raw[start..]);
    Some((text, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_first_answer_still_allows_fallback() {
        let start = b": heartbeat\r\n\r\n";
        let stop = b"data: {\"choices\":[{\"delta\":{\"content\":\"{}\"},\"finish_reason\":\"stop\"}]}\r\n\r\n";
        let done = b"data: [DONE]\n\n";
        let mut prelude = Prelude::default();
        for chunk in [start.as_slice(), stop.as_slice()] {
            for byte in chunk.chunks(1) {
                assert_eq!(prelude.feed(byte, true), Ok(false));
            }
        }
        assert_eq!(prelude.feed(done, true), Err(Fault::Empty));
    }

    #[test]
    fn text_flushes_immediately_and_only_explicit_errors_can_fallback() {
        let mut prelude = Prelude::default();
        let good = b"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n";
        assert_eq!(prelude.feed(good, true), Ok(true));
        let transient = b"data: {\"error\":{\"type\":\"overloaded\"}}\n\n";
        assert_eq!(
            Prelude::default().feed(transient, false),
            Err(Fault::Temporary(503))
        );
        let denial = b"data: {\"error\":{\"code\":\"invalid_api_key\"}}\n\n";
        assert_eq!(Prelude::default().feed(denial, false), Ok(true));
    }
}

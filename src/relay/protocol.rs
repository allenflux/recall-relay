//! 两类校验各有用途：实时回答可以包含工具调用或拒答，历史记录只接收完整的单条文本回答。

pub(super) use super::json::strict_json;
use regex::Regex;
use serde_json::Value;
use std::{collections::HashSet, sync::LazyLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    Protocol,
    Empty,
    Temporary(u16),
    UpstreamError,
    Timeout,
    Transport,
    TooLarge,
}
impl Fault {
    pub fn status(self) -> u16 {
        if let Self::Temporary(s) = self {
            s
        } else {
            503
        }
    }
    pub fn reason(self) -> &'static str {
        match self {
            Self::Protocol => "protocol_error",
            Self::Empty => "empty_response",
            Self::Temporary(_) => "embedded_upstream_error",
            Self::UpstreamError => "unknown_upstream_error",
            Self::Timeout => "timeout",
            Self::Transport => "transport_error",
            Self::TooLarge => "invalid_response",
        }
    }
}

pub(super) fn replay_format(request: &Value) -> Option<&str> {
    if request.get("tools").is_some() || request.get("functions").is_some() {
        return None;
    }
    match request.get("response_format") {
        None => Some("text"),
        Some(Value::Object(map)) => match map.get("type") {
            None => Some("text"),
            Some(kind) => kind
                .as_str()
                .filter(|v| matches!(*v, "text" | "json_object")),
        },
        _ => None,
    }
}

static EMPTY_MARKERS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut markers = Vec::new();
    for label in ["", "json:", "preview:"] {
        for value in ["{}", "[]", "null", "[{}]"] {
            markers.extend([
                format!("{label}{value}"),
                format!("{label}```{value}```"),
                format!("{label}```json{value}```"),
            ]);
        }
    }
    markers
});
pub(super) fn could_be_empty(compact: &str) -> bool {
    let lower = compact.to_ascii_lowercase();
    EMPTY_MARKERS
        .iter()
        .any(|marker| marker.starts_with(&lower))
}
pub(super) fn empty_marker(compact: &str) -> bool {
    // 标签和代码围栏的大小写可变，JSON 字面量 null 仍须小写。
    let lower = compact.to_ascii_lowercase();
    for prefix in [
        "preview:```json",
        "json:```json",
        "preview:",
        "json:",
        "```json",
        "",
    ] {
        if lower.starts_with(prefix) {
            let normalized = format!("{prefix}{}", &compact[prefix.len()..]);
            return EMPTY_MARKERS.contains(&normalized);
        }
    }
    false
}
pub(super) fn empty_content(content: &str, reject_empty_json: bool) -> bool {
    content.trim().is_empty()
        || reject_empty_json && empty_marker(&content.split_whitespace().collect::<String>())
}

pub(super) fn plain_fields(fields: &Value, allow_empty_reasoning: bool) -> bool {
    fields.as_object().is_some_and(|fields| {
        fields.get("role").is_none_or(|role| role == "assistant")
            && fields.iter().all(|(k, v)| {
                matches!(k.as_str(), "role" | "content")
                    || v.is_null()
                    || allow_empty_reasoning && k == "reasoning_content" && v == ""
            })
    })
}
pub(super) fn index_zero(choice: &Value) -> bool {
    choice.get("index").is_none_or(|i| i.as_i64() == Some(0))
}
pub(super) fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        Value::String(s) => !s.is_empty(),
    }
}
pub(super) fn validate_live(raw: &[u8], reject: bool) -> Result<(), Fault> {
    let v = strict_json(raw)
        .filter(Value::is_object)
        .ok_or(Fault::Protocol)?;
    if has_error(&v) || embedded_terminal(&v) {
        return embedded_error(&v);
    }
    let choices = v["choices"]
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or(Fault::Protocol)?;
    if !choices
        .iter()
        .all(|c| c["finish_reason"].is_string() && c["message"].is_object())
    {
        return Err(Fault::Protocol);
    }
    let c = &choices[0];
    let m = &c["message"];
    if choices.len() == 1
        && index_zero(c)
        && c["finish_reason"] == "stop"
        && c["refusal"].is_null()
        && c.get("error").is_none()
        && plain_fields(m, reject)
        && (m["content"].is_null()
            || m["content"]
                .as_str()
                .is_some_and(|s| empty_content(s, reject)))
    {
        return Err(Fault::Empty);
    }
    Ok(())
}

// 某些网关用 HTTP 400/200 包装鉴权或内容政策拒绝。这里仅检查错误字段和明确的
// 拒绝标记，不会因为正常回答里出现了 “error” 或 “permission” 就去回放。
fn denial_text(text: &str) -> bool {
    let label = text.split(':').next().unwrap_or("").trim();
    if label.len() > 64 {
        return false;
    }
    matches!(
        label.to_ascii_lowercase().replace([' ', '-'], "_").as_str(),
        "model_not_found"
            | "unauthorized"
            | "forbidden"
            | "invalid_api_key"
            | "invalid_api_key_provided"
            | "invalid_token"
            | "permission_denied"
            | "permission_error"
            | "insufficient_permissions"
            | "access_denied"
            | "authentication_error"
            | "authentication_failed"
            | "authentication_required"
            | "content_filter"
            | "content_policy_violation"
            | "refusal"
            | "401"
            | "403"
            | "404"
            | "407"
            | "451"
    )
}

fn terminal_fields(value: &Value) -> bool {
    if let Some(s) = value.as_str() {
        return denial_text(s);
    }
    ErrorTags::parse(value).terminal() || value["message"].as_str().is_some_and(denial_text)
}

pub(super) fn embedded_terminal(v: &Value) -> bool {
    truthy(&v["refusal"]) || terminal_fields(v) || v.get("error").is_some_and(terminal_fields)
}

pub(super) fn terminal_error(raw: &[u8]) -> bool {
    strict_json(raw).map_or_else(
        || std::str::from_utf8(raw).is_ok_and(denial_text),
        |value| embedded_terminal(&value),
    )
}

pub(super) fn has_error(v: &Value) -> bool {
    v["type"] == "error"
        || v.get("error")
            .is_some_and(|err| err.is_object() || truthy(err))
}

struct ErrorTags {
    codes: HashSet<String>,
    statuses: Vec<i64>,
}
impl ErrorTags {
    fn parse(e: &Value) -> Self {
        let codes = ["code", "type", "error_type"]
            .iter()
            .filter_map(|k| e[k].as_str().map(|s| s.trim().to_ascii_lowercase()))
            .collect();
        let statuses = ["code", "status", "status_code"]
            .iter()
            .filter_map(|k| {
                e[k].as_i64().or_else(|| {
                    let s = e[k].as_str()?;
                    (s.len() == 3 && s.bytes().all(|c| c.is_ascii_digit()))
                        .then(|| s.parse().ok())
                        .flatten()
                })
            })
            .collect();
        Self { codes, statuses }
    }
    fn has(&self, tags: &[&str]) -> bool {
        tags.iter().any(|tag| self.codes.contains(*tag))
    }
    fn terminal(&self) -> bool {
        self.codes.iter().any(|s| denial_text(s))
            || self
                .statuses
                .iter()
                .any(|s| matches!(s, 401 | 403 | 404 | 407 | 451))
    }
    fn temporary(&self) -> Option<u16> {
        if self.terminal() {
            return None;
        }
        if self.statuses.contains(&429)
            || self.has(&[
                "rate_limit",
                "rate_limit_exceeded",
                "rate_limit_error",
                "too_many_requests",
            ])
        {
            return Some(429);
        }
        if self
            .statuses
            .iter()
            .any(|s| *s == 408 || (500..=599).contains(s))
            || self.has(&[
                "server_error",
                "internal_server_error",
                "service_unavailable",
                "model_unavailable",
                "temporarily_unavailable",
                "provider_unavailable",
                "overloaded",
                "overloaded_error",
                "model_overloaded",
                "gateway_timeout",
                "request_timeout",
                "upstream_timeout",
                "timeout",
                "connection_error",
                "api_connection_error",
            ])
        {
            return Some(503);
        }
        None
    }
}

pub(super) fn embedded_error(v: &Value) -> Result<(), Fault> {
    if !has_error(v) || embedded_terminal(v) {
        return Ok(());
    }
    let error = v.get("error").unwrap_or(v);
    Err(match ErrorTags::parse(error).temporary() {
        Some(status) => Fault::Temporary(status),
        None => Fault::UpstreamError,
    })
}

pub(super) fn capability_failure(raw: &[u8]) -> Option<(&'static str, u16)> {
    static IMAGE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"^at most \d+ image(?:\(s\)|s)? (?:may be provided|are supported|can be provided)\b",
        )
        .unwrap()
    });
    static CONTEXT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^request \(\d+ tokens\) exceeds the available context size\b").unwrap()
    });
    let value = strict_json(raw)
        .unwrap_or_else(|| Value::String(String::from_utf8_lossy(raw).into_owned()));
    let err = value.get("error").unwrap_or(&value);
    if err.is_object() {
        let tags = ErrorTags::parse(err);
        if tags.terminal() {
            return None;
        }
        if tags.has(&[
            "exceed_context_size_error",
            "context_length_exceeded",
            "context_window_exceeded",
        ]) {
            return Some(("context_limit", 503));
        }
        if tags.has(&[
            "model_does_not_support_image",
            "model_does_not_support_images",
            "unsupported_modality",
            "image_input_not_supported",
        ]) {
            return Some(("modality_limit", 503));
        }
        if let Some(status) = tags.temporary() {
            return Some(("embedded_upstream_error", status));
        }
    }
    let message = err
        .as_str()
        .or_else(|| err["message"].as_str())?
        .trim()
        .to_lowercase();
    if IMAGE.is_match(&message) {
        return Some(("modality_limit", 503));
    }
    if message.starts_with("this model's maximum context length is ") || CONTEXT.is_match(&message)
    {
        return Some(("context_limit", 503));
    }
    None
}

/// 清理的是进入历史库的副本。实时响应仍使用上游原始字节。
pub(super) fn remove_echo(v: &mut Value) -> Option<Vec<u8>> {
    let map = v.as_object_mut()?;
    let prompt = map.shift_remove("prompt_text");
    let ids = map.shift_remove("prompt_token_ids");
    if prompt.is_some() || ids.is_some() {
        Some(super::json::escape_non_ascii(serde_json::to_string(map).ok()?).into_bytes())
    } else {
        None
    }
}

pub(super) fn prepare_history(
    raw: &[u8],
    request: &Value,
    stream: bool,
    reject: bool,
) -> Option<Vec<u8>> {
    let format = replay_format(request)?;
    let (content, body) = if stream {
        super::sse::history_text(raw)?
    } else {
        let mut v = strict_json(raw)?;
        if has_error(&v)
            || v.get("error").is_some()
            || embedded_terminal(&v)
            || v.get("refusal").is_some()
        {
            return None;
        }
        let [choice] = v["choices"].as_array()?.as_slice() else {
            return None;
        };
        let message = &choice["message"];
        if !index_zero(choice)
            || choice["finish_reason"] != "stop"
            || message["role"] != "assistant"
            || choice.get("error").is_some()
            || truthy(&choice["refusal"])
            || truthy(&message["refusal"])
            || message
                .as_object()?
                .keys()
                .any(|k| !matches!(k.as_str(), "role" | "content" | "refusal"))
        {
            return None;
        }
        (
            message["content"].as_str()?.to_owned(),
            remove_echo(&mut v).unwrap_or_else(|| raw.to_vec()),
        )
    };
    if empty_content(&content, reject)
        || (format == "json_object"
            && !strict_json(content.as_bytes()).is_some_and(|v| v.is_object()))
    {
        return None;
    }
    Some(body)
}

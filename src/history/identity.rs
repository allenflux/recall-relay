//! 请求指纹决定哪一份历史回答可以复用。同一张图、同一组参数也要区分租户和策略版本。

use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const FIELDS: &[&str] = &[
    "model",
    "messages",
    "stream",
    "stream_options",
    "response_format",
    "metadata",
    "temperature",
    "top_p",
    "top_k",
    "presence_penalty",
    "frequency_penalty",
    "seed",
    "n",
    "max_tokens",
    "max_completion_tokens",
    "stop",
    "user",
    "reasoning_effort",
    "verbosity",
    "logprobs",
    "top_logprobs",
    "logit_bias",
    "service_tier",
    "chat_template_kwargs",
];
const MAX_IMAGE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    /// 完整请求的摘要，用于 Redis 和 MySQL 的精确查询。
    pub exact: String,
    /// 保留相同的图片和生成参数，只把最后一个用户问题替换成 null 后计算的摘要。
    pub scope: Option<String>,
    /// 单图请求才会留下可参与候选匹配的原始问题。
    pub prompt: Option<String>,
    /// 规则与向量匹配要求问题自成一条消息，避免把依赖上文的问题混在一起。
    pub intent_eligible: bool,
}

/// serde_json 选择能精确还原二进制浮点数的最短十进制表示，也正确处理舍入中点。
/// Python 从 1e-5 起改用科学记数法，并给一位数的指数补前导零。
fn python_float(value: f64) -> String {
    let digits = serde_json::Number::from_f64(value)
        .expect("已检查为有限值")
        .to_string();
    if let Some((coefficient, exponent)) = digits.split_once('e') {
        let power: i32 = exponent.parse().expect("指数由序列化器生成");
        return format!("{coefficient}e{power:+03}");
    }
    let unsigned = digits.trim_start_matches('-');
    if let Some(fraction) = unsigned.strip_prefix("0.")
        && fraction.starts_with("0000")
    {
        let significant = fraction.trim_start_matches('0');
        if !significant.is_empty() {
            let power = -(fraction.len() as i32 - significant.len() as i32 + 1);
            let sign = if value.is_sign_negative() { "-" } else { "" };
            let (head, rest) = significant.split_at(1);
            let point = if rest.is_empty() { "" } else { "." };
            return format!("{sign}{head}{point}{rest}e{power:+03}");
        }
    }
    digits
}

/// Python 和 Rust 序列化同一个浮点数时，指数的写法可能不同。
/// 原系统已将这些字节用作哈希输入，因此这里保留 Python 的数字、排序和转义习惯。
fn write_json(value: &Value, sorted: bool, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            if sorted {
                keys.sort_unstable();
            }
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("字符串可以序列化"));
                out.push(':');
                write_json(&map[key], sorted, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, sorted, out);
            }
            out.push(']');
        }
        Value::Number(number) if number.is_f64() => {
            out.push_str(&python_float(number.as_f64().expect("已检查浮点类型")));
        }
        Value::Number(number) if number.to_string() == "-0" => out.push('0'),
        other => out.push_str(&other.to_string()),
    }
}

pub(super) fn digest(value: &Value) -> String {
    let mut bytes = String::new();
    write_json(value, true, &mut bytes);
    hex::encode(Sha256::digest(bytes.as_bytes()))
}

fn allowed_fields(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.keys().all(|k| allowed.contains(&k.as_str())))
}

#[derive(Default)]
struct Images {
    count: usize,
    bytes: usize,
}

impl Images {
    fn replace_url(&mut self, image: &mut Value) -> Option<()> {
        if !allowed_fields(image, &["url", "detail"]) || self.count >= 8 {
            return None;
        }
        let (header, payload) = image.get("url")?.as_str()?.split_once(',')?;
        if payload.len() > 4 * MAX_IMAGE.div_ceil(3) {
            return None;
        }
        let raw = STANDARD.decode(payload).ok()?;
        if raw.len() > MAX_IMAGE || self.bytes + raw.len() > 2 * MAX_IMAGE {
            return None;
        }
        let recognized = match header {
            "data:image/png;base64" => raw.starts_with(b"\x89PNG\r\n\x1a\n"),
            "data:image/jpeg;base64" => raw.starts_with(b"\xff\xd8\xff"),
            "data:image/webp;base64" => raw.starts_with(b"RIFF") && raw.get(8..12) == Some(b"WEBP"),
            _ => false,
        };
        if !recognized || STANDARD.encode(&raw) != payload {
            return None;
        }
        self.count += 1;
        self.bytes += raw.len();
        image["url"] = json!([header, hex::encode(Sha256::digest(raw))]);
        Some(())
    }
}

/// `&Value` 借用调用方的请求。只修改本地副本，发往上游的字节仍来自原始请求。
/// 返回 `None` 表示无法安全建立身份，例如请求引用了可能变化的远程图片。
pub fn canonicalize(tenant: &str, policy: &str, request: &Value) -> Option<Identity> {
    if tenant.is_empty()
        || policy.is_empty()
        || !allowed_fields(request, FIELDS)
        || request.get("model")?.as_str()?.is_empty()
    {
        return None;
    }
    // arbitrary_precision 保留大整数原文。形如 1e999 的数字则会被 Python
    // 解析为无穷大；它不能出现在旧版缓存键的 JSON 里。
    fn finite(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.values().all(finite),
            Value::Array(items) => items.iter().all(finite),
            Value::Number(n) => !n.to_string().contains(['.', 'e', 'E']) || n.as_f64().is_some(),
            _ => true,
        }
    }
    if !finite(request) {
        return None;
    }
    if let Some(top_k) = request.get("top_k") {
        let number = top_k.as_number()?.to_string();
        if !number.bytes().all(|c| c.is_ascii_digit()) && number != "-1" && number != "-0" {
            return None;
        }
    }
    if let Some(options) = request.get("chat_template_kwargs")
        && options != &json!({"enable_thinking": false})
    {
        return None;
    }

    let mut normalized = request.clone();
    if let Some(format) = normalized.get_mut("response_format") {
        // 有的模型按 JSON Schema 中属性的声明顺序输出，故这里单独保留原始键序。
        let mut schema = String::new();
        write_json(format, false, &mut schema);
        *format = Value::String(schema);
    }
    let messages = normalized.get_mut("messages")?.as_array_mut()?;
    let length = messages.len();
    if length == 0 {
        return None;
    }
    let mut images = Images::default();
    let mut text_path = None;
    for (i, msg) in messages.iter_mut().enumerate() {
        if !allowed_fields(msg, &["role", "content", "name"]) {
            return None;
        }
        let role = msg.get("role")?.as_str()?;
        if !matches!(role, "user" | "system" | "developer" | "assistant") {
            return None;
        }
        let final_user = i + 1 == length && role == "user";
        let content = msg.get_mut("content")?;
        if content.is_string() {
            if final_user {
                text_path = Some(format!("/messages/{i}/content"));
            }
            continue;
        }
        let parts = content.as_array_mut()?;
        if parts.is_empty() {
            return None;
        }
        let mut text_indices = Vec::new();
        for (j, part) in parts.iter_mut().enumerate() {
            match part.get("type")?.as_str()? {
                "text"
                    if allowed_fields(part, &["type", "text"]) && part.get("text")?.is_string() =>
                {
                    text_indices.push(j);
                }
                "image_url" if allowed_fields(part, &["type", "image_url"]) => {
                    images.replace_url(part.get_mut("image_url")?)?;
                }
                _ => return None,
            }
        }
        if final_user && let [j] = text_indices.as_slice() {
            text_path = Some(format!("/messages/{i}/content/{j}/text"));
        }
    }
    // 把副本移入数组，后面直接在同一数组中把问题置空，无须再复制整份对话。
    let mut fingerprint = Value::Array(vec![
        "exact-v1".into(),
        tenant.into(),
        policy.into(),
        normalized,
    ]);
    let mut id = Identity {
        exact: digest(&fingerprint),
        scope: None,
        prompt: None,
        intent_eligible: false,
    };
    if images.count > 0
        && let Some(path) = text_path
    {
        let field = fingerprint[3].pointer_mut(&path)?;
        let prompt = field.as_str()?.to_owned();
        if !prompt.trim().is_empty() && prompt.chars().count() <= 2048 {
            *field = Value::Null;
            fingerprint[0] = json!("near-v1");
            id.scope = Some(digest(&fingerprint));
            // 多张图片的旧记录依然能精确命中，但省略提问去查近似答案会引入指代歧义。
            if images.count == 1 {
                id.prompt = Some(prompt);
                id.intent_eligible = length == 1;
            }
        }
    }
    Some(id)
}

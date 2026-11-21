//! 拆开 JSON 容器才能发现重名字段。字符串转义和数字语法交给 serde_json；
//! 数字保留原始精度，避免两个相邻的大整数在转换成 f64 后得到同一条历史记录。

use serde_json::{Deserializer, Map, Number, Value};

/// 保持 Python json.dumps 的 ASCII 转义约定；补充平面字符用一对 UTF-16 代理项表示。
pub(super) fn escape_non_ascii(serialized: String) -> String {
    use std::fmt::Write;
    if serialized.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
        return serialized;
    }
    let mut out = String::with_capacity(serialized.len());
    for ch in serialized.chars() {
        if (' '..='~').contains(&ch) {
            out.push(ch);
        } else {
            for unit in ch.encode_utf16(&mut [0; 2]) {
                write!(out, "\\u{unit:04x}").expect("可以写入 String");
            }
        }
    }
    out
}

pub(super) fn strict_json(raw: &[u8]) -> Option<Value> {
    let mut reader = Reader { remaining: raw };
    let value = reader.value(0)?;
    reader.whitespace();
    reader.remaining.is_empty().then_some(value)
}

struct Reader<'a> {
    remaining: &'a [u8],
}
impl Reader<'_> {
    // JSON 语法只接受这四种空白字符。
    fn whitespace(&mut self) {
        let first = self
            .remaining
            .iter()
            .position(|c| !matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
            .unwrap_or(self.remaining.len());
        self.remaining = &self.remaining[first..];
    }
    fn take(&mut self, prefix: &[u8]) -> bool {
        if let Some(rest) = self.remaining.strip_prefix(prefix) {
            self.remaining = rest;
            true
        } else {
            false
        }
    }
    fn string(&mut self) -> Option<String> {
        let mut token = Deserializer::from_slice(self.remaining).into_iter::<String>();
        let value = token.next()?.ok()?;
        self.remaining = &self.remaining[token.byte_offset()..];
        Some(value)
    }
    fn number(&mut self) -> Option<Number> {
        let end = self
            .remaining
            .iter()
            .position(|c| !matches!(c, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9'))
            .unwrap_or(self.remaining.len());
        let token = &self.remaining[..end];
        // 先看到数字开头再调用 Number 解析，普通 JSON 对象不可能被误当成数字。
        let number: Number = serde_json::from_slice(token).ok()?;
        if token.iter().any(|c| matches!(c, b'.' | b'e' | b'E'))
            && !number.as_f64().is_some_and(f64::is_finite)
        {
            return None;
        }
        self.remaining = &self.remaining[end..];
        Some(number)
    }
    fn value(&mut self, depth: usize) -> Option<Value> {
        if depth > 128 {
            return None;
        }
        self.whitespace();
        match self.remaining.first()? {
            b'"' => Some(Value::String(self.string()?)),
            b'-' | b'0'..=b'9' => Some(Value::Number(self.number()?)),
            b'n' if self.take(b"null") => Some(Value::Null),
            b't' if self.take(b"true") => Some(Value::Bool(true)),
            b'f' if self.take(b"false") => Some(Value::Bool(false)),
            b'[' => {
                self.take(b"[");
                self.whitespace();
                let mut items = Vec::new();
                if !self.take(b"]") {
                    loop {
                        items.push(self.value(depth + 1)?);
                        self.whitespace();
                        if self.take(b"]") {
                            break;
                        }
                        if !self.take(b",") {
                            return None;
                        }
                    }
                }
                Some(Value::Array(items))
            }
            b'{' => {
                self.take(b"{");
                self.whitespace();
                let mut fields = Map::new();
                if !self.take(b"}") {
                    loop {
                        self.whitespace();
                        let key = self.string()?;
                        self.whitespace();
                        if fields.contains_key(&key) || !self.take(b":") {
                            return None;
                        }
                        fields.insert(key, self.value(depth + 1)?);
                        self.whitespace();
                        if self.take(b"}") {
                            break;
                        }
                        if !self.take(b",") {
                            return None;
                        }
                    }
                }
                Some(Value::Object(fields))
            }
            _ => None,
        }
    }
}

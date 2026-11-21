//! 只有完整匹配已知句式，才允许把不同提问视作同一个意图。
//! 否定、附加限制或相邻句子均会使匹配失败。

use super::identity::digest;
use regex::Regex;
use serde_json::json;
use std::sync::LazyLock;

static FORMS: LazyLock<Vec<(&str, Regex)>> = LazyLock::new(|| {
    let en_image = r"(?:(?:this|the) )?(?:image|picture|photo|screenshot)";
    let zh_image = r"(?:这张|这幅|该)?(?:图片|图像|照片|截图)";
    let code = r"([45４５][0-9０-９]{2})";
    [
        ("en:caption", format!(r"(?i-u:^(?:please )?(?:describe|give (?:me )?a description of) {en_image}[.!?]?$)")),
        ("en:ocr", format!(r"(?i-u:^(?:please )?(?:read|extract|transcribe) (?:the )?text (?:in|from) {en_image}[.!?]?$)")),
        ("en:http-error", format!(r"^(?i-u:(?:please )?explain (?:the )?http (?:error(?: code)?|status code)) {code}[.!?]?$")),
        ("zh:caption", format!(r"^(?:请(?:你)?)?描述{zh_image}(?:的内容)?[。！？.!?]?$")),
        ("zh:ocr", format!(r"^(?:请(?:你)?)?(?:读出|识别|提取){zh_image}(?:中|里|上)的文字[。！？.!?]?$")),
        ("zh:http-error", format!(r"^(?:请(?:你)?)?(?:解释|说明) *(?i-u:HTTP) *(?:(?:错误(?:码)?|状态码) *{code}|{code} *错误)[。！？.!?]?$")),
    ].into_iter().map(|(name, form)| (name, Regex::new(&form).expect("固定句式必须有效"))).collect()
});

pub(super) fn intent_marker(scope: &str, prompt: &str) -> Option<String> {
    let text = prompt
        .trim()
        .split(' ')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    for (intent, regex) in FORMS.iter() {
        if let Some(captures) = regex.captures(&text) {
            let code = captures.iter().skip(1).flatten().next();
            let signature = code.map_or_else(
                || intent.to_string(),
                |v| format!("{intent}:{}", v.as_str()),
            );
            return Some(format!("intent-v1:{}", digest(&json!([scope, signature]))));
        }
    }
    None
}

/// 给旧语料验证器的前置筛选。数字、否定、顺序变化会改变问句含义，即使验证器宽松也不能放行。
pub(super) fn guard(query: &str, stored: &str) -> bool {
    use std::collections::HashSet;
    use unicode_casefold::UnicodeCaseFold;
    static PROTECTED: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(concat!(
            r"[+\-−＋－]?\d+(?:[.,:/-]\d+)*[%％]?|",
            r"\b(?:not|no|none|without|never|except|exclude|unless|only|before|after|first|",
            r"second|third|last|cannot|reverse|ascending|descending|one|two|three|four|five|",
            r"six|seven|eight|nine|ten|\w+n['’]t)\b|",
            r"[不未无無莫没沒除禁仅僅先后後第逆零一二两兩三四五六七八九十百千万萬亿億]"
        ))
        .expect("固定保护词正则有效")
    });
    static WORD: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"[㐀-鿿]|[[\p{L}\p{N}]--[㐀-鿿]]+").expect("固定分词正则有效")
    });
    let a: String = query.case_fold().collect();
    let b: String = stored.case_fold().collect();
    if !PROTECTED
        .find_iter(&a)
        .map(|m| m.as_str())
        .eq(PROTECTED.find_iter(&b).map(|m| m.as_str()))
    {
        return false;
    }
    let aa: Vec<_> = WORD.find_iter(&a).map(|m| m.as_str()).collect();
    let bb: Vec<_> = WORD.find_iter(&b).map(|m| m.as_str()).collect();
    let set_a: HashSet<_> = aa.iter().copied().collect();
    let set_b: HashSet<_> = bb.iter().copied().collect();
    !set_a.is_disjoint(&set_b)
        && aa
            .into_iter()
            .filter(|w| set_b.contains(w))
            .eq(bb.into_iter().filter(|w| set_a.contains(w)))
}

//! Sulphur 插件专用的最后一步：保留用户自己的提示词，过滤插件附加的探针元数据。

use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;

const PROBE: &str = "You analyze a reference image and pick two metadata values for a downstream video-generation prompt.";
const METADATA: &str = "PRE-DECIDED METADATA (use these verbatim in the LANGUAGE: and STYLE: header lines, and in every [DIALOGUE] / [SETTING] block):\n";
const MEDIA_HEADER: &str = "H3 reference media. Use these exact <Picture N>, <Video N>, <Audio N> labels in the prompt.\nVideo preview frames share one Video label; they are not separate Picture references.\n";
static LABEL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^(<(?:Picture|Video|Audio) [1-9][0-9]*>) = ").unwrap());

pub(super) fn prompt(request: &Value) -> Option<String> {
    let [system, user] = request["messages"].as_array()?.as_slice() else {
        return None;
    };
    if system["role"] != "system"
        || user["role"] != "user"
        || system["content"].as_str()?.starts_with(PROBE)
    {
        return None;
    }
    let mut media = None;
    let mut content = if let Some(parts) = user["content"].as_array() {
        let mut texts = Vec::new();
        let mut has_image = false;
        for part in parts {
            match part["type"].as_str()? {
                "text" => texts.push(part["text"].as_str()?),
                "image_url" => {
                    has_image |= part["image_url"]["url"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty())
                }
                _ => return None,
            }
        }
        match texts.as_slice() {
            [text] => {
                if has_image {
                    media = Some("Use the provided reference image as visual guidance.".to_owned());
                }
                *text
            }
            [manifest, .., last]
                if manifest.starts_with(MEDIA_HEADER)
                    && parts.first()?["type"] == "text"
                    && parts.last()?["type"] == "text" =>
            {
                let manifest = manifest.split("\nSource descriptions:\n").next()?;
                let labels: Vec<_> = LABEL
                    .captures_iter(manifest)
                    .map(|c| c.get(1).unwrap().as_str())
                    .collect();
                if labels.is_empty() {
                    return None;
                }
                media = Some(format!(
                    "Use the provided media references: {}.",
                    labels.join(", ")
                ));
                *last
            }
            _ => return None,
        }
    } else {
        user["content"].as_str()?
    };
    if let Some((head, prompt)) = content
        .strip_prefix(METADATA)
        .and_then(|s| s.split_once("\n\n"))
    {
        let lines: Vec<_> = head.split('\n').collect();
        if (1..=2).contains(&lines.len())
            && lines
                .iter()
                .all(|s| s.starts_with("  LANGUAGE: ") || s.starts_with("  STYLE: "))
        {
            content = prompt;
        }
    }
    if content.trim().is_empty() {
        media
    } else {
        Some(content.to_owned())
    }
}

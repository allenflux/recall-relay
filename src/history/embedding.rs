//! 仅把有界文字发送给本机 Ollama。图片内容留在请求指纹里，不参与向量上传。

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{net::IpAddr, time::Duration};
use tokio::sync::Semaphore;

pub(super) const DIMENSIONS: usize = 384;
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(8);

pub(super) struct Embedder {
    client: reqwest::Client,
    url: reqwest::Url,
    pub model: String,
    prefix: &'static str,
    keep_alive: &'static str,
    permits: Semaphore,
}

impl Embedder {
    pub fn new(url: &str, model: String, cpu_enabled: bool) -> Result<Self> {
        let url = reqwest::Url::parse(url)?;
        let host = url.host_str().unwrap_or("").trim_matches(['[', ']']);
        ensure!(
            url.scheme() == "http"
                && host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/api/embed"
                && url.query().is_none()
                && url.fragment().is_none(),
            "向量接口必须是回环 IP 的 HTTP /api/embed"
        );
        ensure!(
            !model.is_empty()
                && model.chars().count() <= 256
                && !model.chars().any(char::is_whitespace)
                && !["-cloud", ":cloud"]
                    .iter()
                    .any(|s| model.to_lowercase().ends_with(s)),
            "向量模型名称无效"
        );
        let name = model
            .rsplit('/')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("");
        let prefix = if name.eq_ignore_ascii_case("embeddinggemma") {
            "task: sentence similarity | query: "
        } else {
            ""
        };
        let client = reqwest::Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        Ok(Self {
            client,
            model,
            url,
            prefix,
            keep_alive: if cpu_enabled { "10m" } else { "-1m" },
            permits: Semaphore::new(1),
        })
    }

    pub async fn embed(&self, prompt: &str, budget: Duration) -> Option<Vec<f64>> {
        if prompt.trim().is_empty() || prompt.chars().count() > 512 {
            return None;
        }
        // 持有到函数返回；正常返回、出错和任务取消都会自动释放许可。
        let _permit = self.permits.try_acquire().ok()?;
        tokio::time::timeout(budget, async {
            let payload = json!({
                "model": self.model,
                "input": format!("{}{prompt}", self.prefix),
                "truncate": false,
                "dimensions": DIMENSIONS,
                "options": {"num_gpu": 0},
                "keep_alive": self.keep_alive,
            });
            let mut response = self
                .client
                .post(self.url.clone())
                .header("accept-encoding", "identity")
                .json(&payload)
                .send()
                .await
                .ok()?;
            let encoded = response
                .headers()
                .get("content-encoding")
                .is_some_and(|v| v != "identity");
            if response.status() != 200 || encoded {
                return None;
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.ok()? {
                if bytes.len() + chunk.len() > DIMENSIONS * 32 + 2048 {
                    return None;
                }
                bytes.extend_from_slice(&chunk);
            }
            let value: Value = serde_json::from_slice(&bytes).ok()?;
            let [vector] = value.get("embeddings")?.as_array()?.as_slice() else {
                return None;
            };
            let values = vector
                .as_array()?
                .iter()
                .map(Value::as_f64)
                .collect::<Option<Vec<_>>>()?;
            normalize(values)
        })
        .await
        .ok()
        .flatten()
    }
}

pub(super) fn normalize(mut values: Vec<f64>) -> Option<Vec<f64>> {
    if values.len() != DIMENSIONS {
        return None;
    }
    // hypot 在中途平方非常大或非常小的分量时仍能稳定计算长度。
    let length = values.iter().fold(0.0_f64, |length, x| length.hypot(*x));
    if !length.is_finite() || length == 0.0 {
        return None;
    }
    for x in &mut values {
        *x /= length;
    }
    Some(values)
}

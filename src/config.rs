//! 在启动时把环境变量转换成有类型的配置。请求处理函数拿到的超时已经是
//! `Duration`，因此不用在每次请求时解析字符串，也不会把秒误当成毫秒。

use std::{env, net::IpAddr, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use url::Url;

pub(crate) fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("请设置环境变量 {name}"))
}

pub(crate) fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn number<T: FromStr>(name: &str, default: T) -> Result<T> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} 数值无效")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(_) => bail!("{name} 不是有效的文本"),
    }
}

fn optional_time(name: &str) -> Result<Option<Duration>> {
    let raw = setting(name, "");
    if raw.is_empty() {
        return Ok(None);
    }
    let seconds = raw
        .parse::<f64>()
        .with_context(|| format!("{name} 应为秒数"))?;
    let duration =
        Duration::try_from_secs_f64(seconds).with_context(|| format!("{name} 超出范围"))?;
    Ok(Some(duration))
}

fn time(name: &str, default_seconds: u64) -> Result<Duration> {
    Ok(optional_time(name)?.unwrap_or(Duration::from_secs(default_seconds)))
}

pub(crate) fn flag(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Ok(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Err(env::VarError::NotPresent) => Ok(default),
        _ => bail!("{name} 只接受 true 或 false"),
    }
}

/// 地址完全由部署者指定。解析失败时只报告变量名，避免把 URL 中误填的密码打进日志。
fn http_url(raw: &str, name: &str) -> Result<Url> {
    let url = Url::parse(raw).with_context(|| format!("{name} 地址无效"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none(),
        "{name} 应为无用户凭据的 HTTP(S) 地址"
    );
    Ok(url)
}

pub(crate) fn models_url(upstream: &Url) -> Option<Url> {
    let prefix = upstream.path().strip_suffix("/chat/completions")?;
    let mut models = upstream.clone();
    models.set_path(&format!("{prefix}/models"));
    Some(models)
}

fn origin(raw: &str, name: &str) -> Result<Url> {
    let url = http_url(raw, name)?;
    // 检查原字符串，防止 URL 解析器把 /a/.. 规范化成 / 后误接受路径。
    let authority = raw
        .split_once("://")
        .map(|(_, rest)| rest.trim_end_matches('/'));
    ensure!(
        authority.is_some_and(|s| !s.is_empty() && !s.contains(['/', '?', '#', '@', '\\']))
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && !raw.ends_with("//"),
        "{name} 只能包含协议、主机与端口"
    );
    Ok(url)
}

#[derive(Clone)]
pub struct ProxyConfig {
    pub origin: axum::http::Uri,
    pub max_body: usize,
    pub max_inflight: usize,
    pub total_timeout: Duration,
}

impl ProxyConfig {
    fn from_env(upstream: &str, ingress: &str) -> Result<Self> {
        Self::new(
            upstream,
            ingress,
            number("MAX_BODY_BYTES", 10 * 1024 * 1024)?,
            number("MAX_INFLIGHT", 16)?,
            time("TOTAL_TIMEOUT_SECONDS", 60)?,
        )
    }

    pub fn new(
        upstream: &str,
        ingress: &str,
        max_body: usize,
        max_inflight: usize,
        total_timeout: Duration,
    ) -> Result<Self> {
        let upstream = origin(upstream, "UPSTREAM_ORIGIN")?;
        let ingress = origin(ingress, "INGRESS_ORIGIN")?;
        ensure!(
            upstream.origin() != ingress.origin(),
            "上游和中继入口相同，会形成循环"
        );
        ensure!(
            max_body > 0 && (1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&max_inflight),
            "请求体和并发上限无效"
        );
        ensure!(
            (Duration::from_millis(50)..=Duration::from_secs(60)).contains(&total_timeout),
            "TOTAL_TIMEOUT_SECONDS 应在 0.05 到 60 之间"
        );
        Ok(Self {
            origin: upstream.as_str().parse()?,
            max_body,
            max_inflight,
            total_timeout,
        })
    }
}

/// 入口只绑定一个地址。聊天端点可以有自己的上游路径，其余请求沿用原始路径。
pub(crate) struct Routes {
    pub proxy: ProxyConfig,
    pub chat_url: Url,
    pub chat_path: String,
    pub ingress: String,
}

impl Routes {
    pub fn from_env(listener: std::net::SocketAddr) -> Result<Self> {
        let ingress = setting("INGRESS_ORIGIN", &format!("http://{listener}"));
        let chat_path = setting("REQUEST_PATH", "/v1/chat/completions");
        let path: axum::http::Uri = chat_path.parse().context("REQUEST_PATH 应为 HTTP 路径")?;
        ensure!(
            chat_path.starts_with('/')
                && path.path() == chat_path
                && path.scheme().is_none()
                && path.authority().is_none()
                && chat_path != "/healthz",
            "REQUEST_PATH 应是独立的请求路径，/healthz 已用于健康检查"
        );

        let configured_url = setting("UPSTREAM_URL", "");
        let configured_origin = setting("UPSTREAM_ORIGIN", "");
        let chat_url = if configured_url.is_empty() {
            ensure!(
                !configured_origin.is_empty(),
                "请设置 UPSTREAM_URL，例如 http://127.0.0.1:8091/v1/chat/completions"
            );
            let mut url = origin(&configured_origin, "UPSTREAM_ORIGIN")?;
            url.set_path(&chat_path);
            url
        } else {
            let url = http_url(&configured_url, "UPSTREAM_URL")?;
            ensure!(url.fragment().is_none(), "UPSTREAM_URL 不接受片段标识");
            if !configured_origin.is_empty() {
                ensure!(
                    origin(&configured_origin, "UPSTREAM_ORIGIN")?.origin() == url.origin(),
                    "UPSTREAM_URL 和 UPSTREAM_ORIGIN 应指向同一个上游"
                );
            }
            url
        };
        // 监听所有网卡时，127.0.0.1 仍然能连回自己。即便部署者提供了外部入口域名，
        // 也要单独比较实际监听地址，避免常见的本机循环转发。
        let upstream_ip = chat_url
            .host_str()
            .and_then(|s| s.trim_matches(['[', ']']).parse::<IpAddr>().ok());
        let is_listener = upstream_ip.is_some_and(|ip| {
            ip == listener.ip() || ip.is_loopback() && listener.ip().is_unspecified()
        }) || chat_url.host_str() == Some("localhost")
            && (listener.ip().is_loopback() || listener.ip().is_unspecified());
        ensure!(
            !(is_listener
                && chat_url.scheme() == "http"
                && chat_url.port_or_known_default() == Some(listener.port())),
            "上游指向当前进程的监听端口，会形成循环"
        );
        let proxy = ProxyConfig::from_env(&chat_url.origin().ascii_serialization(), &ingress)?;
        Ok(Self {
            proxy,
            chat_url,
            chat_path,
            ingress,
        })
    }
}

/// 聊天请求先走主上游。故障时，在总时限里预留历史查询与写回响应的时间。
/// Option<Duration> 对应 Python 中可为 None 的配置；没有覆盖值时使用通用预算。
#[derive(Clone)]
pub struct RelayConfig {
    pub upstream_url: String,
    pub policy: String,
    pub request_path: String,
    pub total_timeout: Duration,
    pub upstream_timeout: Duration,
    pub fallback_budget: Duration,
    pub max_body: usize,
    pub max_response: usize,
    pub max_ingest: usize,
    pub max_inflight: usize,
    pub retry_after: usize,
    pub stream_timeout: Duration,
    pub max_pending: usize,
    pub queue_timeout: Duration,
    pub max_buffered_body: usize,
    pub nonstream_total_timeout: Option<Duration>,
    pub nonstream_upstream_timeout: Option<Duration>,
    pub reject_empty_json: bool,
    pub sse_startup_timeout: Option<Duration>,
    pub sse_upstream_timeout: Option<Duration>,
    pub sulphur_nonstream_total_timeout: Option<Duration>,
    pub sulphur_nonstream_upstream_timeout: Option<Duration>,
}

impl RelayConfig {
    pub(crate) fn from_env(routes: &Routes, fallback_budget: Duration) -> Result<Self> {
        let config = Self {
            upstream_url: routes.chat_url.to_string(),
            policy: setting("POLICY_EPOCH", "v1"),
            request_path: routes.chat_path.clone(),
            total_timeout: time("TOTAL_TIMEOUT_SECONDS", 60)?,
            upstream_timeout: time("UPSTREAM_TIMEOUT_SECONDS", 55)?,
            fallback_budget,
            max_body: number("MAX_BODY_BYTES", 10 * 1024 * 1024)?,
            max_response: 2 * 1024 * 1024,
            max_ingest: 256 * 1024,
            retry_after: 2,
            max_inflight: number("MAX_INFLIGHT", 16)?,
            max_pending: number("MAX_PENDING", 0)?,
            queue_timeout: time("QUEUE_TIMEOUT_SECONDS", 15)?,
            max_buffered_body: number("MAX_BUFFERED_BODY_BYTES", 64 * 1024 * 1024)?,
            stream_timeout: time("STREAM_TIMEOUT_SECONDS", 600)?,
            nonstream_total_timeout: optional_time("NONSTREAM_TOTAL_TIMEOUT_SECONDS")?,
            nonstream_upstream_timeout: optional_time("NONSTREAM_UPSTREAM_TIMEOUT_SECONDS")?,
            reject_empty_json: flag("REJECT_EMPTY_JSON_COMPLETIONS", false)?,
            sse_startup_timeout: optional_time("SSE_STARTUP_TIMEOUT_SECONDS")?,
            sse_upstream_timeout: optional_time("SSE_UPSTREAM_TIMEOUT_SECONDS")?,
            sulphur_nonstream_total_timeout: optional_time(
                "SULPHUR_NONSTREAM_TOTAL_TIMEOUT_SECONDS",
            )?,
            sulphur_nonstream_upstream_timeout: optional_time(
                "SULPHUR_NONSTREAM_UPSTREAM_TIMEOUT_SECONDS",
            )?,
        };
        config.validate(&routes.ingress)?;
        Ok(config)
    }

    pub fn timeouts(&self, streaming: bool, sulphur: bool) -> (Duration, Duration) {
        if streaming {
            return (
                self.sse_startup_timeout.unwrap_or(self.total_timeout),
                self.sse_upstream_timeout.unwrap_or(self.upstream_timeout),
            );
        }
        let total = self.nonstream_total_timeout.unwrap_or(self.total_timeout);
        let primary = self
            .nonstream_upstream_timeout
            .unwrap_or(self.upstream_timeout);
        if sulphur {
            (
                self.sulphur_nonstream_total_timeout.unwrap_or(total),
                self.sulphur_nonstream_upstream_timeout.unwrap_or(primary),
            )
        } else {
            (total, primary)
        }
    }

    fn validate(&self, ingress: &str) -> Result<()> {
        let upstream = http_url(&self.upstream_url, "UPSTREAM_URL")?;
        let ingress = origin(ingress, "INGRESS_ORIGIN")?;
        ensure!(
            upstream.origin() != ingress.origin(),
            "上游和中继入口相同，会形成循环"
        );
        ensure!(
            !self.policy.is_empty() && self.request_path.starts_with('/'),
            "策略或请求路径无效"
        );
        let max_permits = tokio::sync::Semaphore::MAX_PERMITS;
        ensure!(
            (1..=max_permits).contains(&self.max_inflight)
                && self.max_pending <= max_permits
                && self.max_body > 0
                && self.max_buffered_body >= self.max_body,
            "并发数或请求体上限无效"
        );
        let minute = Duration::from_secs(60);
        let minimum = Duration::from_millis(50);
        let ceiling = Duration::from_secs(6000);
        ensure!(
            (minimum..=minute).contains(&self.total_timeout)
                && (minimum..=ceiling).contains(&self.stream_timeout)
                && !self.queue_timeout.is_zero()
                && self.queue_timeout <= minute,
            "总预算、流式或排队超时无效"
        );
        for (streaming, sulphur) in [(true, false), (false, false), (false, true)] {
            let custom_sulphur = sulphur
                && (self.sulphur_nonstream_total_timeout.is_some()
                    || self.sulphur_nonstream_upstream_timeout.is_some());
            let limit = if custom_sulphur && self.max_pending > 0 {
                minute - self.queue_timeout
            } else if custom_sulphur {
                minute
            } else {
                ceiling
            };
            let (total, primary) = self.timeouts(streaming, sulphur);
            ensure!(
                total >= minimum
                    && total <= limit
                    && !primary.is_zero()
                    && !self.fallback_budget.is_zero()
                    && primary.saturating_add(self.fallback_budget) < total,
                "需要在总时限内为上游、历史回放与响应预留时间"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_validates_origin_and_deadline() {
        let build = |upstream: &str, ingress: &str| {
            ProxyConfig::new(upstream, ingress, 10, 1, Duration::from_secs(1))
        };
        assert!(build("https://example.org", "https://EXAMPLE.org:443").is_err());
        for invalid in [
            "http://a/x",
            "http://a/x/..",
            "http://user@a",
            "http://a?x=1",
            "ftp://a",
        ] {
            assert!(build(invalid, "http://127.0.0.1").is_err());
        }
        assert!(build("https://example.org/", "http://127.0.0.1").is_ok());
    }
}

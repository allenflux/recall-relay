//! RecallRelay 的统一 HTTP 入口。从 `dispatch` 可以看清一个请求的完整去向。
//! 独立运行时由 main 绑定端口；已有 Axum 服务也可以使用 `Application::router`。

mod archive;
mod config;
mod content_log;
mod delivery;
pub mod history;
mod model_file;
mod passthrough;
mod relay;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, Uri, uri::PathAndQuery},
    response::{IntoResponse, Response},
};
use tokio::sync::Semaphore;
use url::{Position, Url};

use config::{RelayConfig, Routes, setting};
use history::History;
use passthrough::Proxy;
use relay::Relay;

pub struct Application {
    state: Arc<Service>,
    history: Option<Arc<History>>,
    log_content: bool,
}

struct Service {
    proxy: Proxy,
    chat: Option<Arc<Relay>>,
    // 上游的聊天地址可能有额外的路径前缀或固定查询串；只为需要改写的路径保存映射。
    rewrites: Vec<(String, PathAndQuery)>,
}

impl Application {
    /// 在 Tokio 运行时内调用。listener 是本进程实际绑定的地址，用来检查上游回环。
    pub fn from_env(listener: SocketAddr) -> Result<Self> {
        let history = if setting("REDIS_URL", "").is_empty() {
            None
        } else {
            Some(Arc::new(History::from_env()?))
        };
        Self::with_history(listener, history)
    }

    /// 嵌入已有服务时，可传入使用自定义审核回调的历史库；HTTP 仍只注册一个路由入口。
    pub fn with_history(listener: SocketAddr, history: Option<Arc<History>>) -> Result<Self> {
        let routes = Routes::from_env(listener)?;
        let slots = Arc::new(Semaphore::new(routes.proxy.max_inflight));
        let chat = match &history {
            Some(store) => {
                let config = RelayConfig::from_env(&routes, store.fallback_budget())?;
                let client = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    .pool_max_idle_per_host(config.max_inflight)
                    .build()?;
                // 两类请求共用一个信号量，MAX_INFLIGHT 限制整个服务的活跃请求总数。
                Some(Arc::new(Relay::with_slots(
                    config,
                    Arc::clone(store),
                    client,
                    Arc::clone(&slots),
                )))
            }
            None => None,
        };
        let mut rewrites = Vec::new();
        let mut map = |path: &str, url: &Url| -> Result<()> {
            let upstream = &url[Position::BeforePath..];
            if upstream != path {
                rewrites.push((
                    path.to_owned(),
                    upstream.parse().context("上游路径无法用于 HTTP 请求")?,
                ));
            }
            Ok(())
        };
        map(&routes.chat_path, &routes.chat_url)?;
        if routes.chat_path != "/v1/models"
            && let Some(models) = config::models_url(&routes.chat_url)
        {
            map("/v1/models", &models)?;
        }
        let state = Arc::new(Service {
            proxy: Proxy::new(routes.proxy, slots),
            chat,
            rewrites,
        });
        Ok(Self {
            state,
            history,
            log_content: config::flag("RELAY_LOG_CONTENT", true)?,
        })
    }

    pub fn router(&self) -> Router {
        let show_body = self.log_content;
        Router::new()
            .fallback(dispatch)
            .with_state(Arc::clone(&self.state))
            .layer(axum::middleware::from_fn(move |req, next| {
                content_log::observe(req, next, show_body)
            }))
    }

    /// 启用内置向量回放时，首次读取、校验和预热模型都在接收流量之前完成。
    pub async fn ready(&self) -> Result<()> {
        if let Some(history) = &self.history {
            history.ready().await?;
        }
        Ok(())
    }

    pub async fn close(&self) {
        if let Some(history) = &self.history {
            history.close().await;
        }
    }
}

async fn dispatch(State(service): State<Arc<Service>>, mut request: Request) -> Response {
    // 外部地址不能成为请求目标。交给通用处理函数返回与普通透传一致的错误。
    if request.uri().scheme().is_some() || request.uri().authority().is_some() {
        return passthrough::forward(&service.proxy, request).await;
    }
    let path = relay::decoded_path(request.uri().path());
    if path == "/healthz" {
        return Response::new(Body::from("ok"));
    }
    if let Some(chat) = &service.chat
        && chat.handles(&path)
    {
        return relay::handle(Arc::clone(chat), request, path).await;
    }
    if let Some((_, target)) = service.rewrites.iter().find(|(src, _)| src == &path) {
        // 只拼接已通过 HTTP 解析的路径和查询段，主机始终由固定配置决定。
        let merged = match request.uri().query().filter(|q| !q.is_empty()) {
            None => target.clone(),
            Some(query) => {
                let join = match target.query() {
                    Some("") => "",
                    Some(_) => "&",
                    None => "?",
                };
                let Ok(path) = format!("{target}{join}{query}").parse() else {
                    return StatusCode::URI_TOO_LONG.into_response();
                };
                path
            }
        };
        *request.uri_mut() = Uri::builder()
            .path_and_query(merged)
            .build()
            .expect("有效的相对请求路径");
    }
    passthrough::forward(&service.proxy, request).await
}

//! 程序只有一个 HTTP 监听端口。路由如何处理每个请求，可接着读 lib.rs 的 dispatch。

use std::net::SocketAddr;

use anyhow::{Context, Result};
use recall_relay::Application;
use tokio::{net::TcpListener, signal};

#[tokio::main]
async fn main() -> Result<()> {
    // Tokio 调度网络 I/O。一个请求等待上游响应时，其他请求可以继续运行。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "recall_relay=info".into()),
        )
        .with_target(false)
        .with_ansi(false)
        .init();
    let bind: SocketAddr = std::env::var("RELAY_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8000".to_owned())
        .parse()
        .context("RELAY_BIND 需要形如 127.0.0.1:8000 的监听地址")?;
    let listener = TcpListener::bind(bind)
        .await
        .context("无法绑定 RELAY_BIND")?;
    let addr = listener.local_addr()?;
    let app = Application::from_env(addr)?;
    app.ready().await?;

    eprintln!("RecallRelay 已启动，监听 {addr}");
    let result = axum::serve(listener, app.router())
        .with_graceful_shutdown(shutdown_signal())
        .await;
    app.close().await;
    result.context("HTTP 服务异常退出")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        // Compose 和 Kubernetes 一般发 SIGTERM，开发终端的 Ctrl+C 则发 SIGINT。
        let mut term = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("无法注册 SIGTERM 监听");
        tokio::select! { _ = signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}

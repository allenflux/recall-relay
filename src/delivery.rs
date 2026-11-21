//! 独立任务把上游响应逐块送给客户端。即使下游 TCP 缓冲区写满、HTTP 层停止读取正文，
//! 计时器仍能在超时后主动关闭上游响应。

use axum::{
    body::{Body, Bytes},
    response::Response,
};
use futures_util::StreamExt;
use std::{io, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub wall: Instant,
    pub idle: Option<Duration>,
}

pub(crate) fn bounded(response: Response, default_deadline: Instant) -> Response {
    let (mut head, body) = response.into_parts();
    let limits = head.extensions.remove::<Limits>().unwrap_or(Limits {
        wall: default_deadline,
        idle: None,
    });
    let (sender, mut receiver) = mpsc::channel::<(Bytes, oneshot::Sender<()>)>(1);
    let (finished, outcome) = oneshot::channel();
    tokio::spawn(async move {
        let pump = async {
            let mut chunks = body.into_data_stream();
            while let Some(chunk) = chunks.next().await {
                let bytes = chunk.map_err(io::Error::other)?;
                let (ack, received) = oneshot::channel();
                let send = async {
                    sender
                        .send((bytes, ack))
                        .await
                        .map_err(|_| io::Error::other("downstream disconnected"))?;
                    // HTTP 层索取下一块时，表示上一块已交付。收到确认后才继续拉上游与写历史。
                    received.await.map_err(io::Error::other)
                };
                if let Some(idle) = limits.idle {
                    timeout(idle, send).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "downstream stalled")
                    })??;
                } else {
                    send.await?;
                }
            }
            Ok::<(), io::Error>(())
        };
        let result = tokio::select! {
            _ = sender.closed() => Ok(()),
            result = timeout_at(limits.wall,pump) => result.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut,"response timed out"))),
        };
        let _ = finished.send(result);
    });
    let stream = async_stream::stream! {
        while let Some((bytes, ack)) = receiver.recv().await {
            yield Ok::<_,io::Error>(bytes);
            let _ = ack.send(());
        }
        if let Err(err) = outcome.await.unwrap_or_else(|e| Err(io::Error::other(e))) {yield Err(err);}
    };
    Response::from_parts(head, Body::from_stream(stream))
}

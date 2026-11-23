//! 独立任务把上游响应逐块送给客户端。即使下游 TCP 缓冲区写满、HTTP 层停止读取正文，
//! 计时器仍能在超时后主动关闭上游响应。

use axum::{
    body::{Body, Bytes},
    response::Response,
};
use futures_util::StreamExt;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub wall: Instant,
    pub idle: Option<Duration>,
}

/// 两步确认最后一块：上游先解析出结束事件，HTTP 层随后实际取走包含它的数据块。
/// 仅仅从上游读到了 [DONE]，但客户端此前已经断开时，不能替它保存回答。
#[derive(Default)]
pub(crate) struct SseCacheTail {
    candidate: AtomicBool,
    delivered: AtomicBool,
}

impl SseCacheTail {
    pub fn set_candidate(&self, value: bool) {
        self.candidate.store(value, Ordering::Relaxed);
    }

    fn chunk_delivered(&self) {
        if self.candidate.load(Ordering::Relaxed) {
            self.delivered.store(true, Ordering::Relaxed);
        }
    }

    fn should_finish(&self) -> bool {
        self.delivered.load(Ordering::Relaxed) && self.candidate.load(Ordering::Relaxed)
    }
}

pub(crate) fn bounded(response: Response, default_deadline: Instant) -> Response {
    let (mut head, body) = response.into_parts();
    let limits = head.extensions.remove::<Limits>().unwrap_or(Limits {
        wall: default_deadline,
        idle: None,
    });
    let tail = head.extensions.remove::<Arc<SseCacheTail>>();
    let downstream_tail = tail.clone();
    let (sender, mut receiver) = mpsc::channel::<(Bytes, oneshot::Sender<()>)>(1);
    let (finished, outcome) = oneshot::channel();
    tokio::spawn(async move {
        let pump = async {
            let mut chunks = body.into_data_stream();
            loop {
                let next = tokio::select! {
                    biased;
                    _ = sender.closed() => break,
                    next = chunks.next() => next,
                };
                let Some(chunk) = next else {
                    return Ok(());
                };
                let bytes = chunk.map_err(io::Error::other)?;
                let (ack, received) = oneshot::channel();
                let send = async {
                    if sender.send((bytes, ack)).await.is_err() {
                        return false;
                    }
                    // HTTP 层索取下一块时，表示上一块已交付。收到确认后才继续拉上游与写历史。
                    received.await.is_ok()
                };
                let connected = if let Some(idle) = limits.idle {
                    timeout(idle, send).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "downstream stalled")
                    })?
                } else {
                    send.await
                };
                if !connected {
                    break;
                }
            }
            // 有些客户端读到 [DONE] 就关闭。继续驱动原来的流，等真实 EOF 后由聊天处理器
            // 校验整份回答并入队。尾部最多再等一个空闲时限，也受原流的总期限约束。
            if let Some(tail) = tail.filter(|state| state.should_finish()) {
                let deadline = limits
                    .idle
                    .map_or(limits.wall, |idle| limits.wall.min(Instant::now() + idle));
                timeout_at(deadline, async {
                    while tail.should_finish() {
                        let Some(chunk) = chunks.next().await else {
                            break;
                        };
                        chunk.map_err(io::Error::other)?;
                    }
                    Ok::<(), io::Error>(())
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "SSE cache tail timed out")
                })??;
            }
            Ok::<(), io::Error>(())
        };
        let result = timeout_at(limits.wall, pump).await.unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response timed out",
            ))
        });
        let _ = finished.send(result);
    });
    let stream = async_stream::stream! {
        while let Some((bytes, ack)) = receiver.recv().await {
            if let Some(tail) = &downstream_tail {
                tail.chunk_delivered();
            }
            yield Ok::<_,io::Error>(bytes);
            let _ = ack.send(());
        }
        if let Err(err) = outcome.await.unwrap_or_else(|e| Err(io::Error::other(e))) {yield Err(err);}
    };
    Response::from_parts(head, Body::from_stream(stream))
}

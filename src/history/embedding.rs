//! 一个常驻线程独占 llama.cpp 的模型和上下文。Tokio 只收发消息和等待结果，
//! 因此即使 CPU 正忙于推理，也能及时处理其他 HTTP 请求和超时。

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};

use super::native::Engine;

pub(super) const DIMENSIONS: usize = 384;
pub(super) const WRITE_TIMEOUT: Duration = Duration::from_secs(8);
const PREFIX: &str = "task: sentence similarity | query: ";

pub(super) struct Embedder {
    /// 向量索引包含模型标签，避免不同模型的输出混在一起比较。
    pub model: String,
    sender: Mutex<Option<mpsc::Sender<Job>>>,
    slot: Arc<Semaphore>,
    stopped: Arc<AtomicBool>,
    ready: watch::Receiver<Option<Result<(), String>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
}

struct Job {
    input: String,
    until: Instant,
    cancelled: Arc<AtomicBool>,
    answer: oneshot::Sender<Option<Vec<f64>>>,
    // 把唯一许可跟随任务送给线程。即便调用者已经超时，也要等 C 代码真正退出才接下一单。
    slot: OwnedSemaphorePermit,
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Embedder {
    pub(super) fn new(model: String) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Job>(1);
        let (status, ready) = watch::channel(None);
        let stopped = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&stopped);
        let tag = model.clone();
        let thread = thread::Builder::new()
            .name("embedding-cpu".into())
            .spawn(move || {
                // 权重校验、映射和第一次计算都可能耗时。只在这个线程内构造、使用和析构 C 指针。
                let setup = || -> Result<Engine> {
                    let file = crate::model_file::embedding_path(&tag)?;
                    let mut engine = Engine::load(file.path(), &stop)?;
                    let warmup = AtomicBool::new(false);
                    engine
                        .infer(&format!("{PREFIX}warmup"), &stop, &warmup, None)
                        .context("EmbeddingGemma 预热失败")?;
                    Ok(engine)
                };
                let mut engine = match setup() {
                    Ok(engine) => engine,
                    Err(e) => {
                        tracing::error!(error = %format!("{e:#}"), "本机向量模型启动失败");
                        status.send_replace(Some(Err(format!("{e:#}"))));
                        return;
                    }
                };
                status.send_replace(Some(Ok(())));
                while let Some(job) = receiver.blocking_recv() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if job.answer.is_closed() || Instant::now() >= job.until {
                        continue;
                    }
                    let answer = engine.infer(&job.input, &stop, &job.cancelled, Some(job.until));
                    // 先归还 CPU 许可再唤醒调用方；收到结果后立刻开始下一次推理也不会误判为繁忙。
                    drop(job.slot);
                    let _ = job.answer.send(answer);
                }
            })
            .context("创建向量推理线程失败")?;
        Ok(Self {
            model,
            sender: Mutex::new(Some(sender)),
            ready,
            thread: Mutex::new(Some(thread)),
            stopped,
            slot: Arc::new(Semaphore::new(1)),
        })
    }

    pub(super) async fn ready(&self) -> Result<()> {
        let mut status = self.ready.clone();
        loop {
            if let Some(outcome) = status.borrow_and_update().clone() {
                return outcome.map_err(anyhow::Error::msg);
            }
            status.changed().await.context("向量推理线程在预热时退出")?;
        }
    }

    pub(super) async fn embed(
        &self,
        prompt: &str,
        budget: Duration,
        wait_for_cpu: bool,
    ) -> Option<Vec<f64>> {
        if prompt.trim().is_empty() || prompt.chars().count() > 512 {
            return None;
        }
        // 在线回放按到达推理槽的顺序等待。取消等待会自动退出队列，不会留下待算的任务。
        // 后台建索引和 shadow 只尝试拿空闲的槽，不与等待回放的请求争抢。
        let slot = self.slot.clone();
        let permit = if wait_for_cpu {
            let queued_at = Instant::now();
            let permit = slot.acquire_owned().await.ok()?;
            tracing::debug!(
                wait_ms = queued_at.elapsed().as_millis(),
                "向量回放取得推理槽"
            );
            permit
        } else {
            slot.try_acquire_owned().ok()?
        };
        // 拿到槽才开始计算本次推理的超时。HTTP 请求的总期限同时覆盖排队和计算。
        let until = Instant::now().checked_add(budget)?;
        let (answer, reply) = oneshot::channel();
        let cancel = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let job = Job {
            input: format!("{PREFIX}{prompt}"),
            until,
            answer,
            cancelled: cancel.0.clone(),
            slot: permit,
        };
        self.sender
            .lock()
            .expect("向量发送端锁未损坏")
            .as_ref()?
            .try_send(job)
            .ok()?;
        // 外层请求取消时，本 future 也会被销毁，CancelOnDrop 同样会通知 CPU 线程。
        tokio::time::timeout_at(until.into(), reply)
            .await
            .ok()?
            .ok()?
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.slot.close();
        self.sender.lock().expect("向量发送端锁未损坏").take();
    }

    pub(super) async fn close(&self) {
        self.stop();
        let handle = self.thread.lock().expect("向量线程锁未损坏").take();
        if let Some(handle) = handle {
            let _ = tokio::task::spawn_blocking(move || handle.join()).await;
        }
    }
}
impl Drop for Embedder {
    fn drop(&mut self) {
        self.stop();
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

#[cfg(test)]
#[path = "../../tests/support/embedding_compat.rs"]
mod compatibility;

#[cfg(test)]
mod tests {
    use super::*;

    // 用测试端接收任务，手动决定何时归还 CPU，排队顺序就不依赖模型速度。
    fn manual_worker() -> (Embedder, mpsc::Receiver<Job>) {
        let (sender, receiver) = mpsc::channel(1);
        let (_, ready) = watch::channel(Some(Ok(())));
        let native = Embedder {
            model: String::new(),
            sender: Mutex::new(Some(sender)),
            slot: Arc::new(Semaphore::new(1)),
            stopped: Arc::new(AtomicBool::new(false)),
            ready,
            thread: Mutex::new(None),
        };
        (native, receiver)
    }

    #[tokio::test]
    async fn replay_waits_in_order_and_keeps_its_inference_budget() {
        let (native, mut jobs) = manual_worker();
        let occupied = native.slot.clone().try_acquire_owned().unwrap();
        let budget = Duration::from_millis(20);
        let mut first = Box::pin(native.embed("第一条", budget, true));
        let mut cancelled = Box::pin(native.embed("将取消", budget, true));
        let mut last = Box::pin(native.embed("最后一条", budget, true));
        assert!(futures_util::poll!(&mut first).is_pending());
        assert!(futures_util::poll!(&mut cancelled).is_pending());
        assert!(futures_util::poll!(&mut last).is_pending());
        drop(cancelled);

        // 让等待时间超过单次推理预算。释放许可后，后台写入和后到的回放都不能插队。
        tokio::time::sleep(budget + Duration::from_millis(5)).await;
        let released = Instant::now();
        drop(occupied);
        let mut background = Box::pin(native.embed("后台索引", budget, false));
        assert_eq!(
            futures_util::poll!(&mut background),
            std::task::Poll::Ready(None)
        );
        assert!(futures_util::poll!(&mut last).is_pending());
        assert!(futures_util::poll!(&mut first).is_pending());
        let job = jobs.try_recv().unwrap();
        assert_eq!(job.input, format!("{PREFIX}第一条"));
        assert!(job.until >= released + budget);
        drop(job.slot);
        job.answer.send(Some(vec![1.0])).unwrap();
        assert_eq!(first.await, Some(vec![1.0]));

        assert!(futures_util::poll!(&mut last).is_pending());
        let job = jobs.try_recv().unwrap();
        assert_eq!(job.input, format!("{PREFIX}最后一条"));
        drop(job.slot);
        job.answer.send(Some(vec![2.0])).unwrap();
        assert_eq!(last.await, Some(vec![2.0]));
    }

    #[tokio::test]
    async fn shutdown_wakes_replay_waiting_for_a_busy_cpu() {
        let (native, _jobs) = manual_worker();
        let _occupied = native.slot.clone().try_acquire_owned().unwrap();
        let mut waiting = Box::pin(native.embed("等待停机", Duration::from_secs(1), true));
        assert!(futures_util::poll!(&mut waiting).is_pending());
        native.stop();
        assert_eq!(
            futures_util::poll!(&mut waiting),
            std::task::Poll::Ready(None)
        );
    }

    #[tokio::test]
    #[ignore = "需要本机官方 EmbeddingGemma GGUF"]
    async fn cancelled_inference_releases_the_cpu_and_shutdown_joins_the_worker() -> Result<()> {
        let native = Embedder::new("embeddinggemma:300m-qat-q8_0".to_owned())?;
        native.ready().await?;
        let budget = Duration::from_secs(5);

        // 手动 poll 回放请求，确保它先拿到许可。后台索引此时应立刻跳过计算。
        let long_prompt = "猫".repeat(512);
        let mut first = Box::pin(native.embed(&long_prompt, budget, true));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(native.embed("second", budget, false).await.is_none());

        // 模拟下游断开。下一条回放等线程退出上次推理后，仍能复用同一上下文。
        drop(first);
        let next = tokio::time::timeout(budget, native.embed("next", budget, true)).await?;
        assert!(next.is_some());
        native.close().await;
        assert!(native.embed("closed", budget, true).await.is_none());
        Ok(())
    }
}

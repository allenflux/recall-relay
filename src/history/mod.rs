//! 主模型成功时，把可回放的完整回答交给有界写队列。上游故障时再查 Redis、
//! MySQL 和近似索引。缓存写入拥堵只影响回答收尾，请求到达后仍然直接访问上游。

mod embedding;
mod hot;
mod identity;
mod native;
mod semantic;
mod vector;

use crate::{
    archive::Archive,
    config::{required, setting},
};
use anyhow::{Context, Result, ensure};
use embedding::Embedder;
use hot::{Hot, IO, Index, MAX_RESPONSE, Write, member_exact, read_record};
pub use identity::{Identity, canonicalize};
use semantic::intent_marker;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use vector::{Job, Vectors};

/// 嵌入应用可注册异步验证器，按顺序接收本次问题、已批准的问题和候选回答。
/// BoxFuture 擦除了各个 async 函数产生的具体类型，借用只在本次验证期间有效。
pub type ReplayVerifier = Arc<
    dyn for<'a> Fn(&'a str, &'a str, &'a [u8]) -> futures_util::future::BoxFuture<'a, bool>
        + Send
        + Sync,
>;

type Lookup = (Option<Vec<u8>>, &'static str);
const QUEUE: usize = 16;
const HOT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Shadow,
    Replay,
}

impl Mode {
    fn setting(key: &str, default: &str) -> Result<Self> {
        match setting(key, default).as_str() {
            "off" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "replay" => Ok(Self::Replay),
            _ => anyhow::bail!("{key} 只能是 off、shadow 或 replay"),
        }
    }
}

struct Storage {
    hot: Hot,
    archive: Option<Archive>,
    near_mode: Mode,
    verifier: Option<ReplayVerifier>,
    vectors: Option<Vectors>,
}

impl Storage {
    async fn archive_answer(&self, entry: &Write) -> bool {
        // 同步入口可能已提交给 MySQL。回填来自已有记录，只需要写 Redis。
        if entry.archived || entry.backfill.is_some() {
            return entry.archived;
        }
        let Some(db) = &self.archive else {
            return false;
        };
        db.enqueue_wait(
            &entry.identity.exact,
            entry.identity.scope.as_deref(),
            &entry.body,
        )
        .await
    }

    async fn write_hot(&self, entry: &Write, retry_hot_at: &mut Instant) -> bool {
        let saved = matches!(timeout(IO, self.hot.put(entry)).await, Ok(Ok(())));
        let now = Instant::now();
        *retry_hot_at = if saved { now } else { now + HOT_RETRY_INTERVAL };
        saved
    }

    async fn write(
        &self,
        entry: Write,
        jobs: Option<&mpsc::Sender<Job>>,
        retry_hot_at: &mut Instant,
    ) {
        // 正常时同时写热缓存并提交长期存储。后台按队列顺序处理下一条，
        // 同一请求键的连续更新也会按提交顺序覆盖。
        let (saved, archived) = if Instant::now() >= *retry_hot_at {
            tokio::join!(
                self.write_hot(&entry, retry_hot_at),
                self.archive_answer(&entry)
            )
        } else {
            // Redis 刚刚写失败。在下一次探测前，已有 MySQL 原件的回填可以跳过；
            // 新回答只有被健康的 MySQL 接收后才跳过热写，避免每条都卡一次 Redis 超时。
            if entry.backfill.is_some() {
                return;
            }
            let archived = self.archive_answer(&entry).await;
            let backed_up =
                archived && self.archive.as_ref().is_some_and(Archive::writes_available);
            let saved = !backed_up && self.write_hot(&entry, retry_hot_at).await;
            (saved, archived)
        };
        if entry.backfill.is_some() {
            return;
        }
        if !saved && !archived {
            tracing::warn!("回答未能写入 Redis 或提交给 MySQL");
            return;
        }
        if let Some(tx) = jobs
            && let Some(job) = Job::new(entry.identity, archived)
        {
            let _ = tx.try_send(job);
        }
    }
}

struct Workers {
    stop: oneshot::Sender<()>,
    writer: JoinHandle<()>,
    vector_writer: Option<JoinHandle<()>>,
}

pub struct History {
    storage: Arc<Storage>,
    sender: mpsc::Sender<Write>,
    vector_sender: Option<mpsc::Sender<Job>>,
    workers: Mutex<Option<Workers>>,
}

impl History {
    /// 只在启动阶段解析配置。Redis 和 MySQL 延迟建连，历史库宕机不阻塞主模型启动。
    pub fn from_env() -> Result<Self> {
        Self::configured(None)
    }

    /// 仅供有可信审核流程的嵌入应用使用。设置了意图匹配或向量匹配时会拒绝启动。
    pub fn from_env_with_verifier(verifier: ReplayVerifier) -> Result<Self> {
        Self::configured(Some(verifier))
    }

    fn configured(verifier: Option<ReplayVerifier>) -> Result<Self> {
        let prefix = setting("REDIS_PREFIX", "recall-relay:v1");
        let hot = Hot::new(&required("REDIS_URL")?, &prefix)?;
        let near_mode = Mode::setting(
            "HISTORY_NEAR_MODE",
            if verifier.is_some() { "off" } else { "replay" },
        )?;
        let embedding_mode = Mode::setting("HISTORY_EMBEDDING_MODE", "off")?;
        ensure!(
            verifier.is_none() || (near_mode == Mode::Off && embedding_mode == Mode::Off),
            "自定义验证器与规则、向量近似模式不能同时启用"
        );
        let vectors = if near_mode != Mode::Off && embedding_mode != Mode::Off {
            let threshold: f64 = setting("HISTORY_EMBEDDING_THRESHOLD", ".90")
                .parse()
                .context("向量阈值应为浮点数")?;
            let seconds: f64 = setting("HISTORY_EMBEDDING_TIMEOUT_SECONDS", ".65")
                .parse()
                .context("向量超时应为秒数")?;
            ensure!(
                (0.8..=1.0).contains(&threshold) && (0.01..=3.0).contains(&seconds),
                "向量阈值应在 0.80–1 之间，超时应在 0.01–3 秒之间"
            );
            let last = setting("HISTORY_EMBEDDING_LAST_RESORT_THRESHOLD", "");
            let last_resort = if last.is_empty() {
                None
            } else {
                let floor: f64 = last.parse().context("最终向量阈值应为浮点数")?;
                ensure!(
                    (0.8..threshold).contains(&floor),
                    "最终向量阈值应至少为 0.80 并低于常规阈值"
                );
                Some(floor)
            };
            let model = setting("HISTORY_EMBEDDING_MODEL", "embeddinggemma:300m-qat-q8_0");
            let embedder = Embedder::new(model)?;
            Some(Vectors {
                mode: embedding_mode,
                embedder,
                threshold,
                last_resort,
                timeout: Duration::from_secs_f64(seconds),
            })
        } else {
            None
        };
        let archive = Archive::from_env(&prefix)?;
        Ok(Self::start(Storage {
            hot,
            archive,
            near_mode,
            verifier,
            vectors,
        }))
    }

    fn start(storage: Storage) -> Self {
        let storage = Arc::new(storage);
        let (sender, mut receiver) = mpsc::channel::<Write>(QUEUE);
        let (stop, mut stopped) = oneshot::channel();
        let (vector_sender, vector_writer) = if storage.vectors.is_some() {
            let (tx, mut rx) = mpsc::channel(QUEUE);
            let storage = storage.clone();
            let task = tokio::spawn(async move {
                let vectors = storage.vectors.as_ref().expect("已启用向量模式");
                while let Some(job) = rx.recv().await {
                    let _ = vectors
                        .write(&storage.hot, storage.archive.as_ref(), job)
                        .await;
                }
            });
            (Some(tx), Some(task))
        } else {
            (None, None)
        };
        let store = storage.clone();
        let jobs = vector_sender.clone();
        let writer = tokio::spawn(async move {
            let mut stopping = false;
            let mut retry_hot_at = Instant::now();
            loop {
                tokio::select! {
                    _ = &mut stopped, if !stopping => {
                        // 停止接收新任务，但继续处理队列中已有的记录。
                        stopping = true;
                        receiver.close();
                    }
                    item = receiver.recv() => {
                        let Some(entry) = item else {break};
                        store.write(entry, jobs.as_ref(), &mut retry_hot_at).await;
                    }
                }
            }
        });
        Self {
            storage,
            sender,
            vector_sender,
            workers: Mutex::new(Some(Workers {
                stop,
                writer,
                vector_writer,
            })),
        }
    }

    /// 为上游失败后的历史读取和一次推理预留时间。排队可以利用上游提前结束后剩下的时间。
    pub fn fallback_budget(&self) -> Duration {
        let archive_budget = self.storage.archive.as_ref().map(Archive::read_timeout);
        let Some(vectors) = &self.storage.vectors else {
            return Duration::from_millis(250) + archive_budget.unwrap_or_default();
        };
        let mut budget = vectors.timeout
            + archive_budget.map_or(Duration::from_millis(400), |d| {
                Duration::from_millis(300) + 3 * d
            });
        if vectors.last_resort.is_some() {
            budget += (IO / 2).max(archive_budget.unwrap_or_default());
        }
        budget
    }

    /// 供同步调用方尝试入队；如果马上不能接收，调用方可根据返回值稍后重试。
    pub fn enqueue(&self, id: &Identity, body: Vec<u8>) -> bool {
        self.queue_answer(id, body, false)
    }

    /// 只有调用者已审核的语料才走这个入口。旧索引按原问题做二次校验，因而需要保存该问题。
    pub fn enqueue_verified(&self, id: &Identity, body: Vec<u8>) -> bool {
        self.queue_answer(id, body, true)
    }

    /// HTTP 请求使用这个入口。队列已满时由当前回答等待空位，不额外创建后台等待任务。
    pub async fn enqueue_wait(&self, id: &Identity, body: Vec<u8>) -> bool {
        let Some(entry) = self.answer_entry(id, body, false) else {
            return false;
        };
        self.sender.send(entry).await.is_ok()
    }

    fn answer_entry(&self, id: &Identity, body: Vec<u8>, verified: bool) -> Option<Write> {
        if body.is_empty() || body.len() > MAX_RESPONSE || self.sender.is_closed() {
            return None;
        }
        let index = match id.scope.as_deref().zip(id.prompt.as_deref()) {
            Some((scope, prompt))
                if self.storage.near_mode != Mode::Off
                    && self.storage.vectors.is_none()
                    && id.intent_eligible =>
            {
                intent_marker(scope, prompt).map_or(Index::Exact, Index::Intent)
            }
            Some((_, prompt)) if verified && self.storage.verifier.is_some() => {
                Index::Verified(prompt.to_owned())
            }
            _ => Index::Exact,
        };
        Some(Write {
            identity: id.clone(),
            body,
            index,
            archived: false,
            backfill: None,
        })
    }

    fn queue_answer(&self, id: &Identity, body: Vec<u8>, verified: bool) -> bool {
        let Some(mut entry) = self.answer_entry(id, body, verified) else {
            return false;
        };
        let archived = self
            .storage
            .archive
            .as_ref()
            .is_some_and(|db| db.enqueue(&id.exact, id.scope.as_deref(), &entry.body));
        entry.archived = archived;
        if let Err(error) = self.sender.try_send(entry) {
            if archived
                && let Some(tx) = &self.vector_sender
                && let Some(job) = Job::new(error.into_inner().identity, true)
            {
                let _ = tx.try_send(job);
            }
            return archived;
        }
        true
    }

    fn backfill(&self, exact: String, scope: Option<String>, body: &[u8], vector: Vec<u8>) {
        if body.is_empty() || body.len() > MAX_RESPONSE {
            return;
        }
        let id = Identity {
            exact,
            scope,
            prompt: None,
            intent_eligible: false,
        };
        let _ = self.sender.try_send(Write {
            identity: id,
            body: body.to_vec(),
            index: Index::Exact,
            archived: false,
            backfill: Some(vector),
        });
    }

    /// validate 由 HTTP 层提供。即使键命中，也要核实记录是一条可安全回放的完整回答。
    /// 向量回放可能等待空闲的推理槽。调用方可用自己的请求期限取消这次查找。
    pub async fn lookup(
        &self,
        id: Option<&Identity>,
        validate: impl Fn(&[u8]) -> Option<Vec<u8>>,
    ) -> Lookup {
        let Some(id) = id else {
            return (None, "ineligible");
        };
        let archive = self.storage.archive.as_ref();
        let archive_time = archive.map(Archive::read_timeout).unwrap_or_default();
        let (body, outcome) = timeout(IO + archive_time, self.lookup_records(id, &validate))
            .await
            .unwrap_or((None, "unavailable"));
        if body.is_some() {
            return (body, outcome);
        }
        let Some(vectors) = &self.storage.vectors else {
            return (None, outcome);
        };
        // 各次存储读取和实际推理都有独立超时。只有能把向量结果发给客户端时才排队。
        let replay = self.storage.near_mode == Mode::Replay && vectors.mode == Mode::Replay;
        let Some(answer) = vectors.lookup(&self.storage.hot, archive, id, replay).await else {
            return (None, outcome);
        };
        if !replay {
            return (None, answer.shadow_outcome);
        }
        let Some(body) = validate(&answer.body) else {
            return (None, "validation_reject");
        };
        if let Some((exact, bytes)) = answer.backfill {
            self.backfill(exact, id.scope.clone(), &body, bytes);
        }
        (Some(body), answer.outcome)
    }

    async fn lookup_records(
        &self,
        id: &Identity,
        validate: &impl Fn(&[u8]) -> Option<Vec<u8>>,
    ) -> Lookup {
        let store = &self.storage;
        let scope = id.scope.as_deref();
        let budget = if store.archive.is_some() { IO / 2 } else { IO };
        let hot_outcome = match timeout(budget, store.hot.get(&id.exact)).await {
            Ok(Ok(Some(raw))) => {
                if let Some(body) =
                    read_record(&raw, &id.exact, scope).and_then(|r| validate(&r.body))
                {
                    return (Some(body), "exact");
                }
                "validation_reject"
            }
            Ok(Ok(None)) => "miss",
            _ => "unavailable",
        };
        let mut outcome = hot_outcome;
        if let Some(db) = &store.archive {
            match timeout(db.read_timeout(), db.get(&id.exact, scope)).await {
                Ok(Ok(Some(raw))) if !raw.is_empty() && raw.len() <= MAX_RESPONSE => {
                    if let Some(body) = validate(&raw) {
                        self.backfill(id.exact.clone(), id.scope.clone(), &body, Vec::new());
                        return (Some(body), "mysql_exact");
                    }
                    if outcome == "miss" {
                        outcome = "validation_reject";
                    }
                }
                Err(_) | Ok(Err(_)) => outcome = "unavailable",
                _ => {}
            }
        }
        let Some((scope, prompt)) = scope.zip(id.prompt.as_deref()) else {
            return (None, outcome);
        };
        if hot_outcome != "miss" || store.vectors.is_some() {
            return (None, outcome);
        }
        let marker = if store.verifier.is_some() {
            None
        } else if store.near_mode != Mode::Off && id.intent_eligible {
            let Some(marker) = intent_marker(scope, prompt) else {
                return (None, outcome);
            };
            Some(marker)
        } else {
            return (None, outcome);
        };
        match self.lookup_near(scope, prompt, marker.as_deref()).await {
            Ok((Some(raw), source)) => match validate(&raw) {
                Some(body) => (Some(body), source),
                None => (None, "validation_reject"),
            },
            Ok((_, near)) if near == "near_shadow" || outcome == "miss" => (None, near),
            Ok(_) => (None, outcome),
            Err(_) => (None, "unavailable"),
        }
    }

    async fn lookup_near(&self, scope: &str, prompt: &str, marker: Option<&str>) -> Result<Lookup> {
        let hot = &self.storage.hot;
        let prefix = if marker.is_some() {
            format!("intent-v1:{scope}:")
        } else {
            format!("{scope}:")
        };
        let members = hot
            .range(&format!("[{prefix}"), &format!("[{prefix}g"), 5)
            .await?;
        // 五条说明至少有一条超出检查预算；不能证明“唯一”就放弃近似回放。
        if members.len() > 4 {
            return Ok((None, "validation_reject"));
        }
        let mut found = None;
        let mut outcome = "miss";
        for member in &members {
            let Some(exact) = member_exact(member, &prefix) else {
                return Ok((None, "validation_reject"));
            };
            let Some(raw) = hot.get(exact).await? else {
                continue;
            };
            let Some(record) = read_record(&raw, exact, Some(scope)) else {
                return Ok((None, "validation_reject"));
            };
            let approved = match (
                marker,
                record.marker.as_deref(),
                self.storage.verifier.as_ref(),
            ) {
                (Some(expected), Some(stored), _) => expected == stored,
                (None, Some(stored), Some(verify)) if semantic::guard(prompt, stored) => {
                    verify(prompt, stored, &record.body).await
                }
                _ => false,
            };
            if !approved {
                outcome = "validation_reject";
                continue;
            }
            if found.is_some() {
                return Ok((None, "validation_reject"));
            }
            found = Some((exact, raw, record.body));
        }
        if let Some((exact, raw, body)) = found {
            // 查询索引和读取正文之间可能发生过期或替换，发回给用户前再检查一次。
            if hot.get(exact).await?.as_ref() != Some(&raw) {
                return Ok((None, "validation_reject"));
            }
            if self.storage.near_mode == Mode::Shadow {
                return Ok((None, "near_shadow"));
            }
            return Ok((Some(body), "near"));
        }
        Ok((None, outcome))
    }

    /// 启用进程内向量时，等待模型校验、加载和首次计算完成后再对外提供服务。
    pub async fn ready(&self) -> Result<()> {
        if let Some(vectors) = &self.storage.vectors {
            vectors.embedder.ready().await?;
        }
        Ok(())
    }

    pub async fn close(&self) {
        if let Some(Workers {
            stop,
            writer,
            vector_writer,
        }) = self.workers.lock().await.take()
        {
            let _ = stop.send(());
            // 已经向 HTTP 调用方确认入队的回答优先排空，然后再让 MySQL 提交剩余批次。
            let _ = writer.await;
            if let Some(task) = vector_writer {
                task.abort();
                let _ = task.await;
            }
            if let Some(vectors) = &self.storage.vectors {
                vectors.embedder.close().await;
            }
            if let Some(db) = &self.storage.archive {
                db.close().await;
            }
        }
    }
}

#[cfg(test)]
mod tests;

//! Redis 热记录。每次读取都核验记录头，避免使用损坏或不属于当前请求的正文。

use anyhow::{Context, Result};
use redis::{Cmd, FromRedisValue, aio::MultiplexedConnection};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

use super::Identity;

pub(super) const IO: Duration = Duration::from_millis(200);
pub(super) const MAX_RESPONSE: usize = 256 * 1024;
const MAX_HEADER: usize = 13_000;

pub(super) struct Hot {
    client: redis::Client,
    // Arc 让连接可在锁外使用，慢查询不会占着互斥锁阻塞所有读者。
    conn: Mutex<Option<Arc<MultiplexedConnection>>>,
    pub prefix: String,
}

pub(super) struct Record {
    pub body: Vec<u8>,
    pub marker: Option<String>,
}

/// 每种索引只携带它需要的标记，避免把普通回答误存进可信语料索引。
pub(super) enum Index {
    Exact,
    Verified(String),
    Intent(String),
}

pub(super) struct Write {
    pub identity: Identity,
    pub body: Vec<u8>,
    pub index: Index,
    pub archived: bool,
    // None 是主模型的新结果；Some(空字节) 也是回填，只是没有对应向量。
    pub backfill: Option<Vec<u8>>,
}

impl Hot {
    pub fn new(url: &str, prefix: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|_| anyhow::anyhow!("REDIS_URL 格式无效"))?;
        anyhow::ensure!(!prefix.is_empty(), "REDIS_PREFIX 不能为空");
        Ok(Self {
            client,
            conn: Mutex::new(None),
            prefix: format!("{prefix}:"),
        })
    }

    pub async fn query<T: FromRedisValue>(&self, cmd: &Cmd) -> Result<T> {
        let connection = {
            let mut slot = self.conn.lock().await;
            if slot.is_none() {
                let config = redis::AsyncConnectionConfig::new()
                    .set_connection_timeout(Some(IO))
                    .set_response_timeout(Some(IO));
                let new = self
                    .client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await?;
                *slot = Some(Arc::new(new));
            }
            Arc::clone(slot.as_ref().expect("连接已经建立"))
        };
        let mut multiplex = (*connection).clone();
        let result = cmd.query_async(&mut multiplex).await;
        if result
            .as_ref()
            .is_err_and(|e| e.is_io_error() || e.is_connection_dropped())
        {
            let mut slot = self.conn.lock().await;
            if slot
                .as_ref()
                .is_some_and(|existing| Arc::ptr_eq(existing, &connection))
            {
                *slot = None;
            }
        }
        Ok(result?)
    }

    pub async fn get(&self, exact: &str) -> Result<Option<Vec<u8>>> {
        self.query(redis::cmd("GET").arg(format!("{}record:{exact}", self.prefix)))
            .await
    }

    pub async fn range(&self, low: &str, high: &str, limit: usize) -> Result<Vec<Vec<u8>>> {
        self.query(
            redis::cmd("ZRANGEBYLEX")
                .arg(format!("{}verified", self.prefix))
                .arg(low)
                .arg(high)
                .arg("LIMIT")
                .arg(0)
                .arg(limit),
        )
        .await
    }

    pub async fn put(&self, entry: &Write) -> Result<()> {
        let id = &entry.identity;
        let (kind, marker) = match &entry.index {
            Index::Exact => (0, None),
            Index::Verified(prompt) => (1, Some(prompt)),
            Index::Intent(tag) => (2, Some(tag)),
        };
        let mut record = serde_json::to_vec(&json!([id.exact, id.scope, marker]))?;
        record.push(b'\n');
        record.extend_from_slice(&entry.body);
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(include_str!("put.lua"))
            .arg(2)
            .arg(format!("{}entries", self.prefix))
            .arg(format!("{}verified", self.prefix))
            .arg(format!("{}record:", self.prefix))
            .arg(&id.exact)
            .arg(id.scope.as_deref().unwrap_or(""))
            .arg(86_400_000u64)
            .arg(1024)
            .arg(record)
            .arg(kind)
            .arg("intent-v1:");
        if let Some(vector) = &entry.backfill {
            cmd.arg(vector);
        }
        self.query::<i64>(&cmd).await?;
        Ok(())
    }
}

pub(super) fn read_record(raw: &[u8], exact: &str, scope: Option<&str>) -> Option<Record> {
    if raw.len() > MAX_RESPONSE + 1 + MAX_HEADER {
        return None;
    }
    let split = raw.iter().take(MAX_HEADER + 1).position(|&b| b == b'\n')?;
    let body = &raw[split + 1..];
    if body.is_empty() || body.len() > MAX_RESPONSE {
        return None;
    }
    let (key, stored_scope, marker): (String, Option<String>, Option<String>) =
        serde_json::from_slice(&raw[..split]).ok()?;
    if key != exact
        || stored_scope.as_deref() != scope
        || marker
            .as_ref()
            .is_some_and(|m| m.is_empty() || m.chars().count() > 2048)
    {
        return None;
    }
    Some(Record {
        body: body.to_vec(),
        marker,
    })
}

pub(super) fn member_exact<'a>(member: &'a [u8], prefix: &str) -> Option<&'a str> {
    let key = member.strip_prefix(prefix.as_bytes())?;
    (key.len() == 64
        && key
            .iter()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(v)))
    .then(|| std::str::from_utf8(key).expect("已验证为十六进制 ASCII"))
}

pub(super) async fn hot_body(
    hot: &Hot,
    exact: &str,
    scope: Option<&str>,
) -> Result<Option<Record>> {
    hot.get(exact)
        .await?
        .map(|raw| read_record(&raw, exact, scope).context("历史记录头无效"))
        .transpose()
}

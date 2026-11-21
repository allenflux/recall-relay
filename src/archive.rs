//! MySQL 长期历史。在线请求只尝试把成功结果放入队列，后台任务负责落库。
//! 故障回放时按键读库，使用独立的短超时，避免数据库拖住当前请求。

use std::{
    env,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use sqlx::{
    MySqlPool,
    mysql::{MySqlConnectOptions, MySqlDatabaseError, MySqlPoolOptions},
};
use tokio::{
    sync::{OnceCell, mpsc},
    task::JoinHandle,
    time::timeout,
};

const MAX_RESPONSE: usize = 256 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const INDEX_NAME: &str = "recall_relay_scope_vectors";
const INDEX_COLUMNS: [&str; 4] = [
    "namespace_hash",
    "scope_hash",
    "vector_marker",
    "request_hash",
];

const CREATE_TABLE: &str = r#"
    CREATE TABLE IF NOT EXISTS recall_relay_history (
        namespace_hash BINARY(32) NOT NULL,
        request_hash BINARY(32) NOT NULL,
        scope_hash BINARY(32) NULL,
        response MEDIUMBLOB NOT NULL,
        vector_marker BINARY(32) NULL,
        embedding VARBINARY(4096) NULL,
        PRIMARY KEY (namespace_hash, request_hash),
        KEY recall_relay_scope_vectors (namespace_hash, scope_hash, vector_marker, request_hash)
    ) ENGINE=InnoDB"#;
const UPSERT: &str = r#"
    INSERT INTO recall_relay_history (namespace_hash, request_hash, scope_hash, response)
    VALUES (?, UNHEX(?), UNHEX(?), ?) AS incoming
    ON DUPLICATE KEY UPDATE scope_hash = incoming.scope_hash, response = incoming.response"#;

enum Write {
    Answer {
        exact: String,
        scope: Option<String>,
        body: Vec<u8>,
    },
    Vector {
        exact: String,
        scope: String,
        marker: Vec<u8>,
        data: Vec<u8>,
    },
}

struct Database {
    pool: MySqlPool,
    namespace: [u8; 32],
    read_timeout: Duration,
    // 多个请求可能同时发现表尚未建立。OnceCell 让它们共用一次成功的初始化。
    // 初始化出错或超时时，下次请求仍可重试。
    table: OnceCell<()>,
    vector_schema: OnceCell<()>,
    read_failures: AtomicU64,
    write_failures: AtomicU64,
}

struct Writer {
    sender: Mutex<Option<mpsc::Sender<Write>>>,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    dropped: AtomicU64,
}

/// 克隆 Archive 只会增加引用计数。所有请求共享同一连接池和一个有界写队列。
#[derive(Clone)]
pub struct Archive {
    db: Arc<Database>,
    writer: Arc<Writer>,
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn valid_vector(data: &[u8]) -> bool {
    // 每个分量是网络字节序的 f32，因此长度必须是 4 的倍数。
    !data.is_empty() && data.len() <= 4096 && data.len().is_multiple_of(4)
}

fn scoped_marker(scope: &str, marker: &[u8]) -> Option<[u8; 32]> {
    if !valid_hash(scope) || marker.len() != 32 {
        return None;
    }
    // 对原始 32 字节作用域做哈希，和 Python 的 bytes.fromhex(scope) 一致。
    let mut digest = Sha256::new();
    digest.update(hex::decode(scope).ok()?);
    digest.update(marker);
    Some(digest.finalize().into())
}

fn note(counter: &AtomicU64, event: &str) {
    // 计数器仅用于统计，Relaxed 不需要让其他线程同步它们持有的数据。
    let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
    if n == 1 || n.is_multiple_of(100) {
        eprintln!("{event} count={n}");
    }
}

fn db_errno(error: &sqlx::Error) -> Option<u16> {
    Some(
        error
            .as_database_error()?
            .try_downcast_ref::<MySqlDatabaseError>()?
            .number(),
    )
}

impl Archive {
    /// 未配置 MYSQL_HOST 时，返回 Ok(None)。调用此函数时 Tokio 运行时须已启动。
    pub fn from_env(namespace: &str) -> Result<Option<Self>> {
        let host = env::var("MYSQL_HOST").unwrap_or_default();
        if host.is_empty() {
            return Ok(None);
        }
        let required = |key: &str| -> Result<String> {
            let value = env::var(key).with_context(|| format!("缺少环境变量 {key}"))?;
            ensure!(!value.is_empty(), "{key} 不能为空");
            Ok(value)
        };
        let port: u16 = env::var("MYSQL_PORT")
            .unwrap_or_else(|_| "3306".into())
            .parse()
            .context("MYSQL_PORT 需要有效端口")?;
        ensure!(port > 0, "MYSQL_PORT 必须大于 0");
        let seconds: f64 = env::var("MYSQL_HISTORY_READ_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "0.25".into())
            .parse()
            .context("MySQL 读取超时需要秒数")?;
        ensure!(
            (0.01..=0.5).contains(&seconds),
            "MySQL 读取超时须在 0.01 至 0.5 秒之间"
        );
        ensure!(!namespace.is_empty(), "MySQL 历史命名空间不能为空");
        // 分开传入账号和密码，允许密码包含 @、/ 等 URL 的保留字符。
        let options = MySqlConnectOptions::new()
            .host(&host)
            .port(port)
            .username(&required("MYSQL_USER")?)
            .password(&required("MYSQL_PASSWORD")?)
            .database(&required("MYSQL_DATABASE")?)
            .charset("utf8mb4");
        Ok(Some(Self::start(
            options,
            namespace,
            Duration::from_secs_f64(seconds),
        )))
    }

    fn start(options: MySqlConnectOptions, namespace: &str, read_timeout: Duration) -> Self {
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            .max_lifetime(Duration::from_secs(300))
            .connect_lazy_with(options);
        let db = Arc::new(Database {
            pool,
            namespace: Sha256::digest(namespace.as_bytes()).into(),
            read_timeout,
            table: OnceCell::new(),
            vector_schema: OnceCell::new(),
            read_failures: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
        });
        let (sender, receiver) = mpsc::channel(64);
        let task = tokio::spawn(Arc::clone(&db).run_writer(receiver));
        Self {
            db,
            writer: Arc::new(Writer {
                sender: Mutex::new(Some(sender)),
                task: tokio::sync::Mutex::new(Some(task)),
                dropped: AtomicU64::new(0),
            }),
        }
    }

    pub fn read_timeout(&self) -> Duration {
        self.db.read_timeout
    }

    fn offer(&self, item: Option<Write>) -> bool {
        let sender = self.writer.sender.lock().expect("队列锁未被破坏");
        if let (Some(item), Some(sender)) = (item, sender.as_ref())
            && sender.try_send(item).is_ok()
        {
            return true;
        }
        note(&self.writer.dropped, "mysql_history_drop");
        false
    }

    pub fn enqueue(&self, exact: &str, scope: Option<&str>, body: &[u8]) -> bool {
        let valid = valid_hash(exact)
            && scope.is_none_or(valid_hash)
            && (1..=MAX_RESPONSE).contains(&body.len());
        // &str、&[u8] 是临时借用。请求结束后后台任务仍要写入，所以在入队时复制并移交所有权。
        self.offer(valid.then(|| Write::Answer {
            exact: exact.to_owned(),
            scope: scope.map(str::to_owned),
            body: body.to_vec(),
        }))
    }

    pub fn enqueue_vector(&self, exact: &str, scope: &str, marker: &[u8], data: &[u8]) -> bool {
        let valid =
            valid_hash(exact) && valid_hash(scope) && marker.len() == 32 && valid_vector(data);
        self.offer(valid.then(|| Write::Vector {
            exact: exact.to_owned(),
            scope: scope.to_owned(),
            marker: marker.to_vec(),
            data: data.to_vec(),
        }))
    }

    pub async fn get(&self, exact: &str, scope: Option<&str>) -> Result<Option<Vec<u8>>> {
        if !valid_hash(exact) || !scope.is_none_or(valid_hash) {
            return Ok(None);
        }
        // <=> 是 MySQL 的 NULL 安全等号。纯文本请求的 scope 可能为 NULL。
        self.read_with_budget(async {
            self.db.ready().await?;
            Ok(sqlx::query_scalar(
                r#"
                SELECT response FROM recall_relay_history
                WHERE namespace_hash = ? AND request_hash = UNHEX(?) AND scope_hash <=> UNHEX(?)
                  AND OCTET_LENGTH(response) BETWEEN 1 AND ?"#,
            )
            .bind(self.db.namespace.as_slice())
            .bind(exact)
            .bind(scope)
            .bind(MAX_RESPONSE as u32)
            .fetch_optional(&self.db.pool)
            .await?)
        })
        .await
    }

    /// 从随机起点向后取索引中的候选，数量不足时再从开头补齐。此处只读取向量，
    /// 通过相似度筛选后才调用 get 读取可能很大的回答正文。
    pub async fn vector_candidates(
        &self,
        scope: &str,
        marker: &[u8],
        limit: usize,
        pivot: Option<&str>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let Some(marker) = scoped_marker(scope, marker) else {
            return Ok(vec![]);
        };
        if !(1..=64).contains(&limit) || !pivot.is_none_or(valid_hash) {
            return Ok(vec![]);
        }
        self.read_with_budget(async {
            self.db.ready().await?;
            let fetch = async |clause: &str, pivot: Option<&str>, count: usize| {
                let sql = format!(
                    "SELECT request_hash, embedding FROM recall_relay_history
                    FORCE INDEX (recall_relay_scope_vectors)
                    WHERE namespace_hash = ? AND scope_hash = UNHEX(?) AND vector_marker = ?
                    {clause} ORDER BY request_hash LIMIT ?"
                );
                let mut query = sqlx::query_as::<_, (Vec<u8>, Option<Vec<u8>>)>(&sql)
                    .bind(self.db.namespace.as_slice())
                    .bind(scope)
                    .bind(marker.as_slice());
                if let Some(key) = pivot {
                    query = query.bind(key);
                }
                query.bind(count as u32).fetch_all(&self.db.pool).await
            };
            let rows = async {
                if pivot.is_none() {
                    return fetch("", None, limit).await;
                }
                let mut rows = fetch("AND request_hash >= UNHEX(?)", pivot, limit).await?;
                if rows.len() < limit {
                    rows.extend(
                        fetch("AND request_hash < UNHEX(?)", pivot, limit - rows.len()).await?,
                    );
                }
                Ok(rows)
            }
            .await;
            let rows = match rows {
                Ok(rows) => rows,
                // Python 旧表也可以精确查询。第一次写向量时才会补齐缺少的列与索引。
                Err(e) if matches!(db_errno(&e), Some(1054 | 1176)) => return Ok(vec![]),
                Err(e) => return Err(e.into()),
            };
            Ok(rows
                .into_iter()
                .filter_map(|(hash, data)| {
                    let data = data?;
                    valid_vector(&data).then(|| (hex::encode(hash), data))
                })
                .collect())
        })
        .await
    }

    async fn read_with_budget<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        // 超时覆盖等连接、初始化和 SQL 整个操作。返回 Err 让上层区分“未命中”和“不可用”。
        let outcome = match timeout(self.read_timeout(), operation).await {
            Ok(outcome) => outcome,
            Err(e) => Err(e.into()),
        };
        if outcome.is_err() {
            note(&self.db.read_failures, "mysql_history_read_failed");
        }
        outcome
    }

    /// 先关闭发送端，后台任务会依序处理已有记录。超过一次写超时后终止剩余工作。
    pub async fn close(&self) {
        self.writer.sender.lock().expect("队列锁未被破坏").take();
        let mut task_guard = self.writer.task.lock().await;
        if let Some(mut task) = task_guard.take()
            && timeout(WRITE_TIMEOUT, &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
        self.db.pool.close().await;
    }
}

impl Database {
    async fn ready(&self) -> Result<()> {
        self.table
            .get_or_try_init(|| async {
                sqlx::query(CREATE_TABLE).execute(&self.pool).await?;
                Ok::<(), sqlx::Error>(())
            })
            .await?;
        Ok(())
    }

    async fn add_schema_item(&self, ddl: &str, duplicate_code: u16) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        // 下列 SET 仅用于当前迁移。归还连接前关闭它，避免影响普通查询的锁超时。
        conn.close_on_drop();
        sqlx::query("SET SESSION lock_wait_timeout = 1")
            .execute(&mut *conn)
            .await?;
        match sqlx::query(ddl).execute(&mut *conn).await {
            Ok(_) => Ok(()),
            // 多个 Relay 实例都能写入长期历史，另一实例可能刚刚补完这列或索引。
            Err(e) if db_errno(&e) == Some(duplicate_code) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn vector_index_columns(&self) -> Result<Vec<String>> {
        let sql = "SELECT COLUMN_NAME FROM information_schema.STATISTICS
                   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'recall_relay_history'
                     AND INDEX_NAME = ? ORDER BY SEQ_IN_INDEX";
        Ok(sqlx::query_scalar(sql)
            .bind(INDEX_NAME)
            .fetch_all(&self.pool)
            .await?)
    }

    async fn ensure_vector_schema(&self) -> Result<()> {
        self.vector_schema
            .get_or_try_init(|| self.migrate_vector_schema())
            .await?;
        Ok(())
    }

    async fn migrate_vector_schema(&self) -> Result<()> {
        let sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'recall_relay_history'
                     AND COLUMN_NAME IN ('vector_marker', 'embedding')";
        let columns: Vec<String> = sqlx::query_scalar(sql).fetch_all(&self.pool).await?;
        let migration = [
            (
                "vector_marker",
                "ALTER TABLE recall_relay_history ADD COLUMN vector_marker BINARY(32) NULL, ALGORITHM=INSTANT",
            ),
            (
                "embedding",
                "ALTER TABLE recall_relay_history ADD COLUMN embedding VARBINARY(4096) NULL, ALGORITHM=INSTANT",
            ),
        ];
        for (column, ddl) in migration {
            if !columns.iter().any(|name| name == column) {
                self.add_schema_item(ddl, 1060).await?;
            }
        }

        // 相同名称的索引也可能包含不同的列。确认列顺序后才能按命名空间、作用域和标记定位向量。
        let mut index = self.vector_index_columns().await?;
        if index.is_empty() {
            let ddl = "ALTER TABLE recall_relay_history ADD INDEX recall_relay_scope_vectors
                       (namespace_hash, scope_hash, vector_marker, request_hash), ALGORITHM=INPLACE, LOCK=NONE";
            self.add_schema_item(ddl, 1061).await?;
            index = self.vector_index_columns().await?;
        }
        if index != INDEX_COLUMNS {
            bail!("MySQL 历史表的向量索引列或顺序不匹配");
        }
        Ok(())
    }

    async fn store(&self, item: Write) -> Result<()> {
        self.ready().await?;
        match item {
            Write::Answer { exact, scope, body } => {
                sqlx::query(UPSERT)
                    .bind(self.namespace.as_slice())
                    .bind(exact)
                    .bind(scope)
                    .bind(body)
                    .execute(&self.pool)
                    .await?;
            }
            Write::Vector {
                exact,
                scope,
                marker,
                data,
            } => {
                let marker = scoped_marker(&scope, &marker).context("向量作用域无效")?;
                self.ensure_vector_schema().await?;
                // UPDATE 只给已有的成功记录补向量，防止创建没有回答的索引条目。
                sqlx::query(
                    "UPDATE recall_relay_history SET vector_marker = ?, embedding = ?
                    WHERE namespace_hash = ? AND request_hash = UNHEX(?) AND scope_hash = UNHEX(?)",
                )
                .bind(marker.as_slice())
                .bind(data)
                .bind(self.namespace.as_slice())
                .bind(exact)
                .bind(scope)
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn run_writer(self: Arc<Self>, mut receiver: mpsc::Receiver<Write>) {
        if !matches!(timeout(WRITE_TIMEOUT, self.ready()).await, Ok(Ok(()))) {
            eprintln!("mysql_history_initialize_failed");
        }
        while let Some(item) = receiver.recv().await {
            if !matches!(timeout(WRITE_TIMEOUT, self.store(item)).await, Ok(Ok(()))) {
                note(&self.write_failures, "mysql_history_write_failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_and_vectors_match_python_format() {
        let scope = "ab".repeat(32);
        let marker: Vec<u8> = (0u8..32).collect();
        assert_eq!(
            hex::encode(scoped_marker(&scope, &marker).unwrap()),
            "b10cda1a64486b07978a11487725ee4861232b3ce87c114a78abbbb2304ea224"
        );
        assert!(scoped_marker(&scope.to_uppercase(), &marker).is_none());
        assert!(scoped_marker(&scope, &marker[..31]).is_none());
        assert!(!valid_vector(&[1, 2]));
        assert!(valid_vector(&[0; 4096]));
        assert!(!valid_vector(&[0; 4100]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_full_queue_returns_immediately() {
        // 单线程测试中，当前任务第一次 await 前后台写入器不会被调度，队列长度可以精确复现。
        let archive = Archive::start(
            MySqlConnectOptions::new().host("127.0.0.1").port(0),
            "queue-test",
            Duration::from_millis(75),
        );
        let key = "a".repeat(64);
        assert!(!archive.enqueue("无效的请求键", None, b"answer"));
        assert!(!archive.enqueue(&key, None, &[]));
        assert!(!archive.enqueue_vector(&key, &key, &[8; 32], &[0; 3]));
        for _ in 0..64 {
            assert!(archive.enqueue(&key, None, b"answer"));
        }
        assert!(!archive.enqueue(&key, None, b"answer"));
        // 本例只验证排队，直接终止后台任务即可，无需连接数据库。
        archive.writer.task.lock().await.take().unwrap().abort();
        archive.close().await;
        assert!(!archive.enqueue(&key, None, b"answer"));
    }

    #[tokio::test]
    #[ignore = "需要隔离的 MySQL 8.4 测试库，参数使用 TEST_MYSQL_* 环境变量"]
    async fn mysql_shares_responses_and_scans_vectors_across_pools() -> Result<()> {
        let options = MySqlConnectOptions::new()
            .host(&env::var("TEST_MYSQL_HOST")?)
            .port(env::var("TEST_MYSQL_PORT")?.parse()?)
            .username(&env::var("TEST_MYSQL_USER")?)
            .password(&env::var("TEST_MYSQL_PASSWORD")?)
            .database(&env::var("TEST_MYSQL_DATABASE")?);
        let namespace = format!("rust-test-{}", uuid::Uuid::new_v4());
        let start =
            |namespace: &str| Archive::start(options.clone(), namespace, Duration::from_secs(3));
        let writer = start(&namespace);
        let reader = start(&namespace);
        let other = start(&format!("{namespace}-other"));
        let scope = "b".repeat(64);
        let first = "1".repeat(64);
        let second = "d".repeat(64);
        let marker = [1; 32];
        let vector = [0x3f, 0x80, 0, 0]; // 网络字节序的 1.0f32。
        for exact in [&first, &second] {
            assert!(writer.enqueue(exact, Some(&scope), &[0, 255]));
            assert!(writer.enqueue_vector(exact, &scope, &marker, &vector));
        }
        writer.close().await; // 返回时，队列中的模拟数据已经提交给 MySQL。
        assert_eq!(reader.get(&first, Some(&scope)).await?, Some(vec![0, 255]));
        assert_eq!(reader.get(&first, None).await?, None);
        assert_eq!(other.get(&first, Some(&scope)).await?, None);
        assert_eq!(
            reader
                .vector_candidates(&scope, &marker, 2, Some(&second))
                .await?,
            vec![
                (second.clone(), vector.to_vec()),
                (first.clone(), vector.to_vec())
            ]
        );
        assert!(reader.enqueue(&first, None, b"replacement"));
        reader.close().await;
        let reopened = start(&namespace);
        assert_eq!(
            reopened.get(&first, None).await?,
            Some(b"replacement".to_vec())
        );
        assert_eq!(reopened.get(&first, Some(&scope)).await?, None);
        // 作用域变化后，之前保存的向量不再参与旧范围的检索。
        assert_eq!(
            reopened.vector_candidates(&scope, &marker, 2, None).await?,
            vec![(second, vector.to_vec())]
        );
        reopened.close().await;
        other.close().await;
        Ok(())
    }
}

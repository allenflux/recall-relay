use super::*;
use serde_json::{Value, json};

fn image_request(prompt: &str) -> Value {
    json!({
        "model":"fixture","metadata":{"α":"茶","z":[1e-5,1e16,0.0001,-0.0]},
        "messages":[{"role":"user","content":[{"type":"text","text":prompt},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgpB"}}]}]
    })
}

#[test]
fn python_cache_fingerprints_and_intents_match() {
    // 两个摘要和意图标记来自原 Python 项目。包括 Unicode、浮点数和内联图片。
    let id = canonicalize("租户", "版本", &image_request("Describe this image.")).unwrap();
    let scope = id.scope.as_deref().unwrap();
    assert_eq!(
        id.exact,
        "c0853c4ffa26d68a7e76dfcc8f3fa60b68e02b092302d2b42d4b1a462335110b"
    );
    assert_eq!(
        scope,
        "d01f3a3c19aa47a15dfb35e1ce078e4835fd5af5d5ecc59309bc95a97e8b9a50"
    );
    assert_eq!(
        intent_marker(scope, "Please give me a description of this picture.").unwrap(),
        "intent-v1:3a16eed3c5747d7a1a98d0df6d1861930084134f1d4d2a0feec459dd77c0cd19"
    );
    assert_eq!(
        intent_marker(scope, "请解释 HTTP ４０１ 错误。"),
        intent_marker(scope, "说明 HTTP 状态码 ４０１。")
    );
    assert!(intent_marker(scope, "请解释 HTTP ４０１ 错误。").is_some());
    assert!(intent_marker(scope, "Describe this image briefly.").is_none());
}

#[test]
fn schema_order_scope_and_image_eligibility() {
    let mut a = image_request("请描述这张照片");
    a["response_format"] =
        serde_json::from_str(r#"{"schema":{"properties":{"a":1,"b":2}}}"#).unwrap();
    let mut b = a.clone();
    b["response_format"] =
        serde_json::from_str(r#"{"schema":{"properties":{"b":2,"a":1}}}"#).unwrap();
    let identity = |r: &Value| canonicalize("t", "p", r).unwrap();
    assert_ne!(identity(&a).exact, identity(&b).exact);
    let image = a["messages"][0]["content"][1].clone();
    a["messages"][0]["content"]
        .as_array_mut()
        .unwrap()
        .push(image);
    assert!(identity(&a).scope.is_some());
    assert!(identity(&a).prompt.is_none());
    a["messages"][0]["content"][1]["image_url"]["url"] = json!("https://example.com/changed.jpg");
    assert!(canonicalize("t", "p", &a).is_none());
}

#[test]
fn vector_constraints_and_invalid_records_are_rejected() {
    for prompt in [
        "Describe only the object",
        "What color is the cat?",
        "请用两句话描述",
        "请描述第３张图",
        "请写出左侧的文字",
        "描述\n下一步",
    ] {
        assert!(vector::group(prompt).is_none(), "{prompt}");
    }
    assert_eq!(vector::group("What is shown?").as_deref(), Some("0:0"));
    assert_eq!(vector::group("提取图片上的文字").as_deref(), Some("1:1"));
    let record = b"[\"exact\",null,null]\nhello\nworld";
    assert_eq!(
        read_record(record, "exact", None).unwrap().body,
        b"hello\nworld"
    );
    assert!(read_record(record, "another", None).is_none());
    assert!(read_record(record, "exact", Some("scope")).is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn local_redis_exact_near_shadow_and_backfill() -> Result<()> {
    use std::{
        fs,
        io::ErrorKind,
        process::{Child, Command, Stdio},
    };

    // 用独立 Unix socket 和临时目录隔离测试。Drop 在测试提前返回时也回收子进程。
    struct RedisFixture {
        child: Child,
        dir: std::path::PathBuf,
    }
    impl Drop for RedisFixture {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
    let program = std::env::var("TEST_REDIS_BIN").unwrap_or_else(|_| "redis-server".into());
    let dir = std::env::temp_dir().join(format!(
        "rr-{}",
        uuid::Uuid::new_v4().simple().to_string().get(..12).unwrap()
    ));
    fs::create_dir(&dir)?;
    let socket = dir.join("redis.sock");
    let child = Command::new(&program)
        .arg("--port")
        .arg("0")
        .arg("--unixsocket")
        .arg(&socket)
        .args(["--save", "", "--appendonly", "no"])
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    let child = match child {
        Ok(c) => c,
        Err(e)
            if e.kind() == ErrorKind::NotFound && std::env::var_os("TEST_REDIS_BIN").is_none() =>
        {
            fs::remove_dir(&dir)?;
            eprintln!("跳过 Redis 集成用例，可设置 TEST_REDIS_BIN 指定 redis-server");
            return Ok(());
        }
        Err(e) => {
            let _ = fs::remove_dir(&dir);
            return Err(e.into());
        }
    };
    let mut fixture = RedisFixture { child, dir };
    let url = format!("redis+unix://{}", socket.display());
    let storage = |mode| -> Result<Storage> {
        Ok(Storage {
            hot: Hot::new(&url, "fixture")?,
            archive: None,
            near_mode: mode,
            verifier: None,
            vectors: None,
        })
    };
    let history = History::start(storage(Mode::Replay)?);
    let ping = redis::cmd("PING");
    timeout(Duration::from_secs(3), async {
        loop {
            if history.storage.hot.query::<String>(&ping).await.is_ok() {
                return Ok(());
            }
            ensure!(fixture.child.try_wait()?.is_none(), "测试用 Redis 启动失败");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let original = canonicalize("t", "p", &image_request("Describe this image.")).unwrap();
    let paraphrase =
        canonicalize("t", "p", &image_request("Please describe this picture.")).unwrap();
    let answer = |v: &[u8]| Some(v.to_vec());
    assert!(history.enqueue(&original, b"answer".to_vec()));
    timeout(Duration::from_secs(2), async {
        while history.lookup(Some(&original), answer).await.1 != "exact" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(
        history.lookup(Some(&paraphrase), answer).await,
        (Some(b"answer".to_vec()), "near")
    );
    let shadow = History::start(storage(Mode::Shadow)?);
    assert_eq!(
        shadow.lookup(Some(&paraphrase), answer).await,
        (None, "near_shadow")
    );
    let old = Write {
        identity: original.clone(),
        body: b"old".to_vec(),
        index: Index::Exact,
        backfill: Some(vec![]),
        archived: false,
    };
    history.storage.hot.put(&old).await?;
    assert_eq!(
        history.lookup(Some(&original), answer).await,
        (Some(b"answer".to_vec()), "exact")
    );
    let mut verified_store = storage(Mode::Off)?;
    verified_store.verifier = Some(Arc::new(|_, _, body| {
        Box::pin(async move { body == b"reviewed" })
    }));
    let verified = History::start(verified_store);
    let identity = |p: &str| canonicalize("test", "verified", &image_request(p)).unwrap();
    let training = identity("Explain 401 error");
    let alternate = identity("Please explain 401 error.");
    assert!(verified.enqueue_verified(&training, b"reviewed".to_vec()));
    timeout(Duration::from_secs(2), async {
        while verified.lookup(Some(&training), answer).await.1 != "exact" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert_eq!(
        verified.lookup(Some(&alternate), answer).await,
        (Some(b"reviewed".to_vec()), "near")
    );
    assert_eq!(
        verified
            .lookup(Some(&identity("Please explain 429 error.")), answer)
            .await,
        (None, "validation_reject")
    );
    verified.close().await;
    verify_vector_replay(&url).await?;
    history.close().await;
    shadow.close().await;
    Ok(())
}

/// 用只返回固定向量的 HTTP 服务验证 Ollama 协议和 Redis 向量格式，全程使用本机数据。
#[cfg(unix)]
async fn verify_vector_replay(redis_url: &str) -> Result<()> {
    use axum::{Json, Router, routing::post};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/api/embed", listener.local_addr()?);
    let endpoint = Router::new().route(
        "/api/embed",
        post(|Json(payload): Json<Value>| async move {
            assert_eq!(payload["dimensions"], embedding::DIMENSIONS);
            assert_eq!(payload["options"]["num_gpu"], 0);
            assert!(
                payload["input"]
                    .as_str()
                    .unwrap()
                    .starts_with("task: sentence similarity | query: ")
            );
            let mut vector = vec![0.0; embedding::DIMENSIONS];
            vector[0] = 1.0;
            Json(json!({"embeddings":[vector]}))
        }),
    );
    let mock = tokio::spawn(async move {
        axum::serve(listener, endpoint).await.unwrap();
    });
    let vectors = Vectors {
        mode: Mode::Replay,
        embedder: Embedder::new(&url, "embeddinggemma:test".to_owned(), true)?,
        threshold: 0.90,
        last_resort: Some(0.85),
        timeout: Duration::from_millis(650),
    };
    let history = History::start(Storage {
        hot: Hot::new(redis_url, "vector-fixture")?,
        archive: None,
        near_mode: Mode::Replay,
        vectors: Some(vectors),
        verifier: None,
    });
    let identity = |p: &str| canonicalize("t", "v", &image_request(p)).unwrap();
    let original = identity("What is in this image?");
    let alternate = identity("Please describe the image.");
    assert!(history.enqueue(&original, b"answer".to_vec()));
    let key = format!("vector-fixture:record:{}:vector", original.exact);
    timeout(Duration::from_secs(3), async {
        while history
            .storage
            .hot
            .query::<Option<Vec<u8>>>(redis::cmd("GET").arg(&key))
            .await
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let answer = |b: &[u8]| Some(b.to_vec());
    assert_eq!(
        history.lookup(Some(&alternate), answer).await,
        (Some(b"answer".to_vec()), "vector_near")
    );
    assert_eq!(
        history
            .lookup(Some(&identity("Read only the letters.")), answer)
            .await,
        (None, "miss")
    );
    history.close().await;
    mock.abort();
    let _ = mock.await;
    Ok(())
}

#[test]
fn verified_guard_keeps_numbers_negation_and_order() {
    use super::semantic::guard;
    assert!(guard("Straße and 猫", "please describe STRASSE and 猫"));
    assert!(!guard("Describe 一猫", "Describe 二猫"));
    assert!(!guard("read A before B", "read B before A"));
    assert!(!guard("explain 401", "explain ４０１"));
}

#[test]
fn integer_precision_and_float_ties_preserve_python_keys() {
    let mut a = image_request("Describe the image.");
    let mut b = a.clone();
    a["top_k"] = serde_json::from_str("123456789012345678901234567890").unwrap();
    b["top_k"] = serde_json::from_str("123456789012345678901234567891").unwrap();
    let id = |r: &Value| canonicalize("t", "v", r).unwrap().exact;
    assert_ne!(id(&a), id(&b));
    a["top_k"] = json!(1.0);
    assert!(canonicalize("t", "v", &a).is_none());
    // 恰好在两种最短十进制写法中点时，Python 和 JSON 按偶数舍入。
    let value = serde_json::from_str("-1714646921285519.25").unwrap();
    assert_eq!(
        super::identity::digest(&value),
        hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
            b"-1714646921285519.2"
        ))
    );
}

//! 这些用例通过 TCP 调用实际的 recall-relay 可执行文件，覆盖启动配置到下游响应。
//! 所有上游都是临时的本机服务。子进程由 ChildGuard 管理，断言失败时也会被回收。

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_relay(settings: &[(&str, &str)]) -> (ChildGuard, SocketAddr) {
    start_relay_with_stdout(settings, Stdio::null())
}

fn start_relay_with_stdout(settings: &[(&str, &str)], stdout: Stdio) -> (ChildGuard, SocketAddr) {
    let child = Command::new(env!("CARGO_BIN_EXE_recall-relay"))
        .env_clear()
        .env("RELAY_BIND", "127.0.0.1:0")
        .env("INGRESS_ORIGIN", "http://127.0.0.1:8000")
        .envs(settings.iter().copied())
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut guard = ChildGuard(child);
    let mut lines = BufReader::new(guard.0.stderr.take().unwrap()).lines();
    // llama.cpp 探测本机 CPU / GPU 时可以先写诊断日志，端口只取应用自己的就绪行。
    let mut last = String::new();
    let addr = loop {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("服务启动失败，最后日志：{last}"))
            .unwrap();
        if let Some(address) = line.strip_prefix("RecallRelay 已启动，监听 ") {
            break address.parse().expect("监听地址");
        }
        last = line;
    };
    std::thread::spawn(move || for _ in lines {});
    (guard, addr)
}

async fn serve(router: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

async fn reflect(req: Request) -> Response {
    let target = req.uri().to_string();
    if target == "/slow" {
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"first\n"));
            tokio::time::sleep(Duration::from_secs(10)).await;
            yield Ok(Bytes::from_static(b"last\n"));
        };
        return Response::new(Body::from_stream(stream));
    }
    if target == "/head" {
        return ([(header::CONTENT_LENGTH, "1234")], "").into_response();
    }
    if target == "/packed" {
        // 故意用不可解压的字节：纯透传模式必须原样传输，不能替客户端解压。
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [
                (header::CONTENT_ENCODING, "gzip"),
                (header::RETRY_AFTER, "19"),
            ],
            Bytes::from_static(b"\x1f\x8b\x01\0"),
        )
            .into_response();
    }
    let (head, body) = req.into_parts();
    let bytes = to_bytes(body, 1024).await.unwrap();
    let mut response = axum::Json(json!({
        "method": head.method.as_str(), "target": target,
        "auth": head.headers.get("authorization").and_then(|v| v.to_str().ok()),
        "saw_hop": head.headers.contains_key("x-private-hop"),
        "saw_cookie": head.headers.contains_key("cookie"), "body": hex::encode(bytes)
    }))
    .into_response();
    response
        .headers_mut()
        .append(header::SET_COOKIE, HeaderValue::from_static("a=1"));
    response
        .headers_mut()
        .append(header::SET_COOKIE, HeaderValue::from_static("b=2"));
    response
}

async fn raw_request(relay: SocketAddr, request: &[u8]) -> String {
    // 原始请求行保留 /a/../ 和 OPTIONS *，避免测试用的客户端先做 URL 规范化。
    let mut tcp = TcpStream::connect(relay).await.unwrap();
    tcp.write_all(request).await.unwrap();
    let mut result = String::new();
    tcp.read_to_string(&mut result).await.unwrap();
    result
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

#[tokio::test]
async fn passthrough_preserves_wire_target_headers_bytes_and_limits() {
    let upstream = serve(Router::new().fallback(reflect)).await;
    let (_relay, addr) = start_relay(&[
        ("UPSTREAM_ORIGIN", &format!("http://{upstream}")),
        ("MAX_BODY_BYTES", "12"),
    ]);
    let raw = raw_request(addr, b"PATCH //else.invalid/a/../b%2f?x=a%2Fb&x=c+d HTTP/1.1\r\nHost: relay\r\nConnection: close, x-private-hop\r\nx-private-hop: remove\r\nAuthorization: Bearer test\r\nContent-Length: 3\r\n\r\na\0b").await;
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
    for expected in [
        "//else.invalid/a/../b%2f?x=a%2Fb&x=c+d",
        "Bearer test",
        "610062",
        "\"saw_hop\":false",
        "set-cookie: a=1",
        "set-cookie: b=2",
    ] {
        assert!(raw.contains(expected), "{raw}");
    }
    let star = raw_request(
        addr,
        b"OPTIONS * HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(star.contains("\"target\":\"*\""), "{star}");
    let invalid = raw_request(addr, b"GET http://other.invalid/path HTTP/1.1\r\nHost: other.invalid\r\nConnection: close\r\n\r\n").await;
    assert!(invalid.starts_with("HTTP/1.1 400"), "{invalid}");
    let client = client();
    let url = format!("http://{addr}");
    let packed = client.get(format!("{url}/packed")).send().await.unwrap();
    assert_eq!(packed.status(), 429);
    assert_eq!(packed.headers()[header::RETRY_AFTER], "19");
    assert_eq!(&packed.bytes().await.unwrap()[..], b"\x1f\x8b\x01\0");
    let head = client.head(format!("{url}/head")).send().await.unwrap();
    assert_eq!(head.headers()[header::CONTENT_LENGTH], "1234");
    let send = client.post(&url).body("x".repeat(13)).send().await.unwrap();
    assert_eq!(send.status(), 413);
    assert_eq!(
        client
            .request(reqwest::Method::TRACE, &url)
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
}

#[tokio::test]
async fn open_sse_streams_do_not_limit_chat_or_passthrough() {
    const STREAMS: usize = 12;
    const FIRST: &str =
        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n";
    const LAST: &str =
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    let (release, gate) = tokio::sync::watch::channel(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let upstream = serve(Router::new().fallback({
        let calls = calls.clone();
        move |req: Request| {
            let mut gate = gate.clone();
            calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if req
                    .headers()
                    .get("x-request-id")
                    .is_some_and(|v| v == "limited")
                {
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        [(header::RETRY_AFTER, "9")],
                        "gateway limit",
                    )
                        .into_response();
                }
                let body = async_stream::stream! {
                    yield Ok::<_,std::io::Error>(Bytes::from_static(FIRST.as_bytes()));
                    gate.wait_for(|&ready| ready).await.unwrap();
                    yield Ok(Bytes::from_static(LAST.as_bytes()));
                };
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(body),
                )
                    .into_response()
            }
        }
    }))
    .await;
    let (_relay, addr) = start_relay(&[
        (
            "UPSTREAM_URL",
            &format!("http://{upstream}/v1/chat/completions"),
        ),
        ("REDIS_URL", "redis://127.0.0.1:1"),
        // 老部署保留的配置也不能重新启用本地并发限制或总正文限额。
        ("MAX_INFLIGHT", "4"),
        ("MAX_PENDING", "0"),
        ("MAX_BODY_BYTES", "512"),
        ("MAX_BUFFERED_BODY_BYTES", "1024"),
    ]);
    let client = client();
    let url = format!("http://{addr}/v1/chat/completions");
    let payload =
        json!({"model":"m","messages":[{"role":"user","content":"x".repeat(200)}],"stream":true});
    let mut held = Vec::new();
    // 上游一直不发送结束事件。四条聊天 SSE 都在传输时，再交替发起聊天和普通请求。
    for i in 0..STREAMS {
        let request = if i < 4 || i % 2 == 0 {
            client.post(&url).json(&payload)
        } else {
            client.get(format!("http://{addr}/stream/{i}"))
        };
        let mut response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.chunk().await.unwrap().unwrap(), FIRST);
        held.push(response);
    }
    assert_eq!(calls.load(Ordering::Relaxed), STREAMS);

    // 所有流仍打开时，上游主动限流的下一条请求应收到上游自己的错误信息。
    let limited = client
        .post(&url)
        .header("x-request-id", "limited")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(limited.headers()[header::RETRY_AFTER], "9");
    assert_eq!(limited.text().await.unwrap(), "gateway limit");
    assert_eq!(calls.load(Ordering::Relaxed), STREAMS + 1);
    release.send(true).unwrap();
    for response in held {
        assert_eq!(response.text().await.unwrap(), LAST);
    }
}

#[derive(Default)]
struct Model {
    offline: AtomicBool,
    calls: AtomicUsize,
}

async fn complete(State(model): State<Arc<Model>>, req: Request) -> Response {
    model.calls.fetch_add(1, Ordering::Relaxed);
    let request: Value =
        serde_json::from_slice(&to_bytes(req.into_body(), 8192).await.unwrap()).unwrap();
    if request["model"] == "primary" && model.offline.load(Ordering::Relaxed) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    axum::Json(json!({
        "object":"chat.completion", "model":request["model"],
        "choices":[{"index":0, "finish_reason":"stop","message":{"role":"assistant","content":"测试回答"}}]
    })).into_response()
}

#[tokio::test]
async fn history_returns_the_upstream_failure_when_storage_is_unavailable() {
    let model = Arc::new(Model::default());
    let upstream = serve(
        Router::new()
            .route("/vendor/v1/chat/completions", axum::routing::post(complete))
            .fallback(reflect)
            .with_state(model.clone()),
    )
    .await;
    let (_relay, addr) = start_relay(&[
        (
            "UPSTREAM_URL",
            &format!("http://{upstream}/vendor/v1/chat/completions?api=one"),
        ),
        ("POLICY_EPOCH", "test"),
        ("REDIS_URL", "redis://127.0.0.1:1"),
        ("MAX_INFLIGHT", "1"),
    ]);
    let url = format!("http://{addr}/v1/chat/completions");
    let client = client();
    let request = json!({"model":"primary", "messages":[{"role":"user","content":"你好"}]});
    let send = || client.post(&url).bearer_auth("test").json(&request).send();
    let live = send().await.unwrap();
    assert_eq!(live.status(), 200);
    assert!(live.headers().get("x-failover-source").is_none());
    assert_eq!(live.json::<Value>().await.unwrap()["model"], "primary");
    model.offline.store(true, Ordering::Relaxed);
    let failure = send().await.unwrap();
    assert_eq!(failure.status(), 503);
    assert!(failure.headers().get("x-failover-source").is_none());
    assert!(failure.bytes().await.unwrap().is_empty());
    assert_eq!(model.calls.load(Ordering::Relaxed), 2);

    // 在同一进程、同一端口检查通用转发、健康检查与模型列表。
    let simple = client
        .patch(format!("http://{addr}/custom?x=1&x=2"))
        .body("plain")
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(simple["target"], "/custom?x=1&x=2");
    assert_eq!(simple["body"], "706c61696e");
    let list = client
        .get(format!("http://{addr}/v1/models?tag=a%2Fb"))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(list["target"], "/vendor/v1/models?api=one&tag=a%2Fb");
    let mut slow = client
        .get(format!("http://{addr}/slow"))
        .send()
        .await
        .unwrap();
    assert_eq!(slow.chunk().await.unwrap().unwrap(), "first\n");
    assert_eq!(send().await.unwrap().status(), 503);
    assert_eq!(
        model.calls.load(Ordering::Relaxed),
        3,
        "已有普通流时仍立即请求聊天上游"
    );
    let health = client
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.text().await.unwrap(), "ok");
    drop(slow);
}

#[tokio::test]
async fn completed_json_and_sse_log_the_cache_queue_with_request_id() {
    let upstream = serve(Router::new().fallback(|axum::Json(req): axum::Json<Value>| async move {
        if req["stream"] == true {
            ([(header::CONTENT_TYPE, "text/event-stream")],
             "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n").into_response()
        } else {
            axum::Json(json!({"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]})).into_response()
        }
    })).await;
    let log = tempfile::NamedTempFile::new().unwrap();
    let (_relay, addr) = start_relay_with_stdout(
        &[
            (
                "UPSTREAM_URL",
                &format!("http://{upstream}/v1/chat/completions"),
            ),
            ("REDIS_URL", "redis://127.0.0.1:1"),
            ("RELAY_LOG_CONTENT", "false"),
        ],
        log.reopen().unwrap().into(),
    );
    for streaming in [false, true] {
        let response = client().post(format!("http://{addr}/v1/chat/completions"))
            .bearer_auth("test")
            .json(&json!({"model":"m","messages":[{"role":"user","content":"query"}],"stream":streaming}))
            .send().await.unwrap();
        assert_eq!(response.status(), 200);
        let _body = response.bytes().await.unwrap();
    }
    let logs = std::fs::read_to_string(log.path()).unwrap();
    for streaming in [false, true] {
        let line = logs
            .lines()
            .find(|line| {
                line.contains("relay_cache") && line.contains(&format!("streaming={streaming}"))
            })
            .unwrap();
        assert!(line.contains("queued=true"));
        let (_, id) = line.split_once("request_id=").unwrap();
        let id = id.split('}').next().unwrap();
        assert!(logs.lines().any(|l| l.contains(id)
            && l.contains("relay_stage")
            && l.contains("event=\"attempt\"")));
    }
}

/// 老部署可能还保留备用模型配置。命不中缓存时，保留主模型的限流响应，原请求只发一次。
#[tokio::test]
async fn legacy_model_settings_cannot_trigger_a_second_upstream_call() {
    let gateway_calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::clone(&gateway_calls);
    let gateway = serve(Router::new().fallback(move |req: Request| {
        let calls = Arc::clone(&calls);
        async move {
            let raw = to_bytes(req.into_body(), 8192).await.unwrap();
            let payload: Value = serde_json::from_slice(&raw).unwrap();
            calls
                .lock()
                .unwrap()
                .push(payload["model"].as_str().unwrap().to_owned());
            (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "23")],
                "model overloaded",
            )
        }
    }))
    .await;
    let legacy_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&legacy_calls);
    let legacy = serve(Router::new().fallback(move || {
        calls.fetch_add(1, Ordering::Relaxed);
        async { StatusCode::SERVICE_UNAVAILABLE }
    }))
    .await;
    let (_relay, addr) = start_relay(&[
        (
            "UPSTREAM_URL",
            &format!("http://{gateway}/v1/chat/completions"),
        ),
        ("REDIS_URL", "redis://127.0.0.1:1"),
        ("FALLBACK_MODEL", "gateway-backup"),
        ("CPU_FALLBACK_ENABLED", "true"),
        (
            "CPU_FALLBACK_URL",
            &format!("http://{legacy}/v1/chat/completions"),
        ),
        ("CPU_FALLBACK_MODEL", "unused-cpu"),
    ]);
    let request = json!({"model":"primary","messages":[{"role":"user","content":"你好"}]});
    let reply = client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .bearer_auth("test")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 429);
    assert!(reply.headers().get("x-failover-source").is_none());
    assert_eq!(reply.headers()[header::RETRY_AFTER], "23");
    assert_eq!(reply.text().await.unwrap(), "model overloaded");
    assert_eq!(*gateway_calls.lock().unwrap(), ["primary"]);
    assert_eq!(legacy_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn sulphur_fallback_requires_plugin_header_and_nonstream_request() {
    let upstream =
        serve(Router::new().fallback(|| async { StatusCode::SERVICE_UNAVAILABLE })).await;
    let (_relay, addr) = start_relay(&[
        (
            "UPSTREAM_URL",
            &format!("http://{upstream}/v1/chat/completions"),
        ),
        ("REDIS_URL", "redis://127.0.0.1:1"),
    ]);
    let url = format!("http://{addr}/v1/chat/completions");
    let client = client();
    let mut payload = json!({"model":"primary", "messages":[
        {"role":"system","content":"Enhance this prompt."},{"role":"user","content":"海边的灯塔"}]});
    let plugin = "ComfyUI-Sulphur-Prompt-Enhancer/1.0";
    // 相同的 URL、方法和内容。只有带插件标识的非流式请求才回显用户提示词。
    let ordinary = client.post(&url).json(&payload).send().await.unwrap();
    assert_eq!(ordinary.status(), 503);
    let echo = client
        .post(&url)
        .header("user-agent", plugin)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(echo.status(), 200);
    assert_eq!(echo.headers()["x-failover-source"], "prompt");
    assert_eq!(
        echo.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "海边的灯塔"
    );
    payload["stream"] = true.into();
    let sse = client
        .post(&url)
        .header("user-agent", plugin)
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(sse.status(), 503);
    assert!(sse.headers().get("x-failover-source").is_none());
}

/// 将原版 Redis 记录装进隔离库，分别模拟可回放错误和必须直接返回的鉴权错误。
/// 两个问题标点不同，没有新问题的精确记录；只有原生向量命中旧记录才能返回这段回答。
#[cfg(unix)]
#[tokio::test]
#[ignore = "需要本机官方 GGUF 和 redis-server"]
async fn upstream_errors_use_only_eligible_history() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    use base64::{Engine, engine::general_purpose::STANDARD};
    use std::env;

    let fixture: Value = serde_json::from_str(include_str!("fixtures/embeddinggemma-ollama.json"))?;
    let legacy = &fixture["legacy_record"];
    let dir = tempfile::Builder::new().prefix("rr-legacy-").tempdir()?;
    let socket = dir.path().join("redis.sock");
    let redis_bin = env::var("TEST_REDIS_BIN").unwrap_or_else(|_| "redis-server".into());
    let redis = Command::new(redis_bin)
        .args(["--port", "0", "--unixsocket"])
        .arg(&socket)
        .args(["--save", "", "--appendonly", "no"])
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("无法启动测试 Redis，可设置 TEST_REDIS_BIN")?;
    let mut redis_process = ChildGuard(redis);
    let redis_url = format!("redis+unix://{}", socket.display());
    let database = redis::Client::open(redis_url.as_str())?;
    let mut db = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(conn) = database.get_multiplexed_async_connection().await {
                return anyhow::Ok(conn);
            }
            ensure!(redis_process.0.try_wait()?.is_none(), "隔离 Redis 启动失败");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let exact = legacy["exact"].as_str().unwrap();
    let scope = legacy["scope"].as_str().unwrap();
    let key = format!("gguf-test:record:{exact}");
    let record = STANDARD.decode(legacy["record_base64"].as_str().unwrap())?;
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["label"] == legacy["vector_case"])
        .unwrap();
    let mut blob = hex::decode(legacy["marker_hex"].as_str().unwrap())?;
    blob.extend(STANDARD.decode(case["vector_f32_be"].as_str().unwrap())?);
    redis::pipe()
        .atomic()
        .set_ex(&key, &record, 180)
        .ignore()
        .set_ex(format!("{key}:vector"), blob, 180)
        .ignore()
        .zadd(
            "gguf-test:verified",
            format!("vector-v1:{scope}:{exact}"),
            0,
        )
        .ignore()
        .query_async::<()>(&mut db)
        .await?;

    const UNKNOWN: &str =
        r#"{"error":{"code":"new_vendor_error","detail":"unrecognized upstream failure"}}"#;
    const DENIED: &str = r#"{"error":{"code":"invalid_api_key","message":"access denied"}}"#;
    const SSE_OK: &str = "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"saved SSE\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    // 每个案例在正式入口发两次，用不同凭据区分能匹配该历史和必须返回原错误的请求。
    const CASES: &[(&str, u16, &str, bool)] = &[
        ("unavailable", 503, UNKNOWN, true),
        ("rate-limit", 429, UNKNOWN, true),
        ("bad-request", 400, UNKNOWN, true),
        ("unprocessable", 422, UNKNOWN, true),
        ("json-error", 200, UNKNOWN, true),
        ("forbidden", 403, DENIED, false),
        ("masked-auth", 400, DENIED, false),
        ("auth-envelope", 200, DENIED, false),
    ];
    const TAIL: &str = "data: [DONE]\n\n";
    let finish_miss = Arc::new(tokio::sync::Notify::new());
    let tail_gate = finish_miss.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let upstream = serve(
        Router::new().fallback(move |headers: axum::http::HeaderMap| {
            counter.fetch_add(1, Ordering::Relaxed);
            let tail_gate = tail_gate.clone();
            async move {
                let mode = headers["x-request-id"].to_str().unwrap();
                if mode == "live-sse" {
                    return ([(header::CONTENT_TYPE, "text/event-stream")], SSE_OK).into_response();
                }
                if matches!(mode, "failed-sse" | "denied-sse") {
                    let data = if mode == "denied-sse" { DENIED } else { UNKNOWN };
                    let keep_open = headers[header::AUTHORIZATION] == "Bearer different-tenant";
                    let body = async_stream::stream! {
                        yield Ok::<_,std::io::Error>(Bytes::from(format!("event: error\ndata: {data}\n\n")));
                        if keep_open { tail_gate.notified().await; }
                        yield Ok(Bytes::from_static(TAIL.as_bytes()));
                    };
                    return ([(header::CONTENT_TYPE,"text/event-stream")],Body::from_stream(body)).into_response();
                }
                let (_, code, body, _) = CASES.iter().find(|(name, ..)| *name == mode).unwrap();
                (
                    StatusCode::from_u16(*code).unwrap(),
                    [(header::CONTENT_TYPE, "application/json")],
                    *body,
                )
                    .into_response()
            }
        }),
    )
    .await;
    let upstream_url = format!("http://{upstream}/v1/chat/completions");
    let explicit = env::var("HISTORY_EMBEDDING_MODEL_PATH").unwrap_or_default();
    let models = env::var("HISTORY_EMBEDDING_MODEL_DIR")
        .or_else(|_| env::var("OLLAMA_MODELS"))
        .unwrap_or_default();
    let mut settings = vec![
        ("UPSTREAM_URL", upstream_url.as_str()),
        ("REDIS_URL", redis_url.as_str()),
        ("REDIS_PREFIX", "gguf-test"),
        ("POLICY_EPOCH", legacy["policy"].as_str().unwrap()),
        ("HISTORY_EMBEDDING_MODE", "replay"),
        ("HISTORY_EMBEDDING_TIMEOUT_SECONDS", "3"),
    ];
    if !explicit.is_empty() {
        settings.push(("HISTORY_EMBEDDING_MODEL_PATH", &explicit));
    }
    if !models.is_empty() {
        settings.push(("HISTORY_EMBEDDING_MODEL_DIR", &models));
    }
    let (_relay, addr) = start_relay(&settings);
    let client = client();
    let url = format!("http://{addr}/v1/chat/completions");
    let split = record
        .iter()
        .position(|&b| b == b'\n')
        .context("历史记录缺少分隔符")?;
    let send = |mode: &str, token: &str, request: &Value| {
        client
            .post(&url)
            .bearer_auth(token)
            .header("x-request-id", mode)
            .header(header::ACCEPT, "application/json")
            .json(request)
            .send()
    };
    for &(mode, status, original, can_replay) in CASES {
        for token in ["compat-fixture", "different-tenant"] {
            let response = send(mode, token, &legacy["request"]).await?;
            if can_replay && token == "compat-fixture" {
                ensure!(response.status() == 200, "{mode} 应回放成功");
                ensure!(
                    response
                        .headers()
                        .get("x-failover-source")
                        .is_some_and(|v| v == "history"),
                    "{mode} 未命中旧向量"
                );
                ensure!(
                    response.bytes().await?.as_ref() == &record[split + 1..],
                    "{mode} 的回放正文有变化"
                );
            } else {
                ensure!(
                    response.status().as_u16() == status,
                    "{mode} 应保留上游状态"
                );
                ensure!(
                    !response.headers().contains_key("x-failover-source"),
                    "{mode} 意外回放"
                );
                ensure!(
                    response.text().await? == original,
                    "{mode} 应保留上游错误正文"
                );
            }
        }
    }
    ensure!(
        calls.load(Ordering::Relaxed) == CASES.len() * 2,
        "每次应先访问上游一次"
    );

    // SSE 的首个事件就是新供应商的错误时，也要在提交下游响应头之前尝试回放。
    let stream =
        json!({"model":"sse","stream":true,"messages":[{"role":"user","content":"hello"}]});
    let live = send("live-sse", "sse-tenant", &stream).await?;
    ensure!(live.status() == 200 && !live.headers().contains_key("x-failover-source"));
    ensure!(live.text().await? == SSE_OK);
    // 正常回答异步写入。只在尚未写入时重试，超时会使测试失败。
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let reply = send("failed-sse", "sse-tenant", &stream).await?;
            if reply
                .headers()
                .get("x-failover-source")
                .is_some_and(|v| v == "history")
            {
                ensure!(reply.status() == 200, "SSE 首事件错误时应回放");
                ensure!(reply.text().await?.contains("saved SSE"));
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    // 换租户后应原样收到首个错误和后续事件。分开发送还能检查转发过程中是否丢帧。
    let mut miss = send("failed-sse", "different-tenant", &stream).await?;
    ensure!(miss.status() == 200 && !miss.headers().contains_key("x-failover-source"));
    ensure!(miss.headers()[header::CONTENT_TYPE] == "text/event-stream");
    ensure!(
        miss.chunk().await?.context("缺少 SSE 错误首帧")?
            == format!("event: error\ndata: {UNKNOWN}\n\n")
    );
    finish_miss.notify_one();
    ensure!(miss.text().await? == TAIL);
    let sensitive = send("denied-sse", "sse-tenant", &stream).await?;
    ensure!(sensitive.status() == 200 && !sensitive.headers().contains_key("x-failover-source"));
    ensure!(sensitive.text().await? == format!("event: error\ndata: {DENIED}\n\n{TAIL}"));
    Ok(())
}

#[tokio::test]
async fn logs_preview_both_directions_and_stream_before_eof() {
    let finish = Arc::new(tokio::sync::Notify::new());
    let upstream = serve(Router::new().fallback({
        let finish = finish.clone();
        move || {
            let finish = finish.clone();
            async move {
                let stream = async_stream::stream! {
                    yield Ok::<_, std::io::Error>(Bytes::from_static("data: 你好\n\n".as_bytes()));
                    finish.notified().await;
                    yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
                };
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from_stream(stream),
                )
            }
        }
    }))
    .await;
    let log = tempfile::NamedTempFile::new().unwrap();
    let (_relay, addr) = start_relay_with_stdout(
        &[("UPSTREAM_ORIGIN", &format!("http://{upstream}"))],
        log.reopen().unwrap().into(),
    );
    let text = format!("用户提问{}超出预览的尾部", "中".repeat(750));
    let mut answer = client()
        .post(format!("http://{addr}/chat?token=hidden-query"))
        .bearer_auth("hidden-authorization")
        .body(text.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(answer.chunk().await.unwrap().unwrap(), "data: 你好\n\n");
    let first = std::fs::read_to_string(log.path()).unwrap();
    for expected in [
        "用户提问",
        "data: 你好",
        "stage=\"passthrough\"",
        "request_id=",
        "omitted_bytes=",
    ] {
        assert!(first.contains(expected), "缺少日志字段 {expected}: {first}");
    }
    for secret in [
        "超出预览的尾部",
        "hidden-query",
        "hidden-authorization",
        "[DONE]",
    ] {
        assert!(!first.contains(secret), "意外的日志字段 {secret}");
    }
    let ids: std::collections::HashSet<_> = first
        .lines()
        .map(|line| {
            line.split_once("request_id=")
                .unwrap()
                .1
                .split('}')
                .next()
                .unwrap()
        })
        .collect();
    assert_eq!(ids.len(), 1);
    finish.notify_one();
    assert_eq!(answer.text().await.unwrap(), "data: [DONE]\n\n");
    let complete = std::fs::read_to_string(log.path()).unwrap();
    assert!(complete.contains("[DONE]"));
}

#[tokio::test]
async fn disabled_body_logging_still_explains_fallback() {
    let model = Arc::new(Model::default());
    model.offline.store(true, Ordering::Relaxed);
    let upstream = serve(Router::new().fallback(complete).with_state(model)).await;
    let log = tempfile::NamedTempFile::new().unwrap();
    let (_relay, addr) = start_relay_with_stdout(
        &[
            (
                "UPSTREAM_URL",
                &format!("http://{upstream}/v1/chat/completions"),
            ),
            ("POLICY_EPOCH", "test"),
            ("REDIS_URL", "redis://127.0.0.1:1"),
            ("RELAY_LOG_CONTENT", "false"),
        ],
        log.reopen().unwrap().into(),
    );
    let response = client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&json!({"model":"primary","messages":[{"role":"user","content":"私有正文"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let _ = response.bytes().await.unwrap();
    let logs = std::fs::read_to_string(log.path()).unwrap();
    for event in ["upstream_status=503", "relay_fallback", "relay_history"] {
        assert!(logs.contains(event), "缺少 {event}: {logs}");
    }
    for body in ["私有正文", "测试回答", "正文预览"] {
        assert!(!logs.contains(body));
    }
}

#[tokio::test]
async fn long_inline_image_leaves_the_user_prompt_visible_in_logs() {
    let upstream = serve(
        Router::new()
            .fallback(complete)
            .with_state(Arc::new(Model::default())),
    )
    .await;
    let log = tempfile::NamedTempFile::new().unwrap();
    let (_relay, addr) = start_relay_with_stdout(
        &[
            (
                "UPSTREAM_URL",
                &format!("http://{upstream}/v1/chat/completions"),
            ),
            ("POLICY_EPOCH", "test"),
            ("REDIS_URL", "redis://127.0.0.1:1"),
        ],
        log.reopen().unwrap().into(),
    );
    let image = "A".repeat(3000);
    let request = json!({"model":"primary","messages":[{"role":"user","content":[
        {"type":"image_url","image_url":{"url":format!("data:image/png;base64,{image}")}},
        {"type":"text","text":"请描述画面"}
    ]}]});
    let response = client()
        .post(format!("http://{addr}/v1/chat/completions"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["model"], "primary");
    let logs = std::fs::read_to_string(log.path()).unwrap();
    assert!(logs.contains("请描述画面"), "{logs}");
    assert!(!logs.contains(&"A".repeat(200)));
}

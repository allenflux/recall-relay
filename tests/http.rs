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
    time::{Duration, Instant},
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
    let child = Command::new(env!("CARGO_BIN_EXE_recall-relay"))
        .env_clear()
        .env("RELAY_BIND", "127.0.0.1:0")
        .env("INGRESS_ORIGIN", "http://127.0.0.1:8000")
        .envs(settings.iter().copied())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut guard = ChildGuard(child);
    let mut lines = BufReader::new(guard.0.stderr.take().unwrap()).lines();
    let first = lines.next().expect("服务在启动前退出").unwrap();
    let addr = first
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap_or_else(|_| panic!("服务未监听：{first}"));
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
async fn open_stream_holds_its_slot_and_disconnect_releases_it() {
    let upstream = serve(Router::new().fallback(reflect)).await;
    let (_relay, addr) = start_relay(&[
        ("UPSTREAM_ORIGIN", &format!("http://{upstream}")),
        ("MAX_INFLIGHT", "1"),
    ]);
    let client = client();
    let url = format!("http://{addr}");
    let mut stream = client.get(format!("{url}/slow")).send().await.unwrap();
    assert_eq!(stream.chunk().await.unwrap().unwrap(), "first\n");
    let busy = client.get(&url).send().await.unwrap();
    assert_eq!(busy.status(), 429);
    assert_eq!(busy.headers()[header::RETRY_AFTER], "2");
    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.get(&url).send().await.unwrap().status() != 200 {
        assert!(Instant::now() < deadline, "下游断开后应释放并发名额");
        tokio::time::sleep(Duration::from_millis(10)).await;
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
async fn history_tries_primary_then_gateway_when_storage_is_unavailable() {
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
        ("FALLBACK_MODEL", "backup"),
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
    let backup = send().await.unwrap();
    assert_eq!(backup.status(), 200);
    assert_eq!(backup.headers()["x-failover-source"], "model");
    assert_eq!(backup.json::<Value>().await.unwrap()["model"], "backup");
    assert_eq!(model.calls.load(Ordering::Relaxed), 3);

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
    assert_eq!(
        send().await.unwrap().status(),
        429,
        "普通请求和聊天请求共用并发额度"
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

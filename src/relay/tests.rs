//! 这些测试直接固定容易意外改变的协议行为：缓存隔离、SSE 的提交边界和错误分类。
use super::*;
use axum::body::to_bytes;
use protocol::{prepare_history, strict_json};

#[test]
fn duplicate_keys_and_non_finite_numbers_are_rejected_at_any_depth() {
    for bad in [
        r#"{"a":[{"x":1,"\u0078":2}]}"#,
        "[NaN]",
        "[2,]",
        "null false",
        "1e888",
    ] {
        assert!(strict_json(bad.as_bytes()).is_none(), "{bad}");
    }
    let n = "184467440737095516170000000001";
    assert_eq!(strict_json(n.as_bytes()).unwrap().to_string(), n);
    let object = br#"{"$serde_json::private::Number":"123"}"#;
    assert!(strict_json(object).unwrap().is_object());
}
#[test]
fn credential_context_matches_the_python_digest() {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    headers.insert("accept", HeaderValue::from_bytes(&[0xe9]).unwrap());
    headers.insert("x-request-id", HeaderValue::from_static("ignored"));
    assert_eq!(
        context_digest("/v1/中", "a=1&a=2", &headers, b"Bearer example"),
        "0a371ddfb397f2b5d4c2312669efecf9164a2d23a3ca947769b5428373306e00"
    );
}
#[test]
fn terminal_failures_never_become_history() {
    assert!(
        protocol::capability_failure(
            br#"{"error":{"code":"context_length_exceeded","status":401}}"#
        )
        .is_none()
    );
    assert_eq!(
        protocol::capability_failure(b"At most 0 image(s) may be provided in one prompt."),
        Some(("modality_limit", 503))
    );
    assert!(protocol::terminal_error(
        br#"{"error":{"type":"MODEL_NOT_FOUND"}}"#
    ));
    assert!(protocol::validate_live(br#"{"error":{"type":"authentication_error"}}"#, true).is_ok());
    assert_eq!(
        protocol::validate_live(br#"{"error":{"type":"rate_limit_error"}}"#, true),
        Err(protocol::Fault::Temporary(429))
    );
}
#[test]
fn unknown_error_envelopes_never_enter_history() {
    let request = json!({"model":"m"});
    let tagged = br#"{"type":"error","error":{"code":"vendor_specific"},"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"bad"}}]}"#;
    assert_eq!(
        protocol::validate_live(tagged, false),
        Err(protocol::Fault::UpstreamError)
    );
    assert!(prepare_history(tagged, &request, false, false).is_none());

    let stream = b"event: error\ndata: {\"choices\":[{\"delta\":{\"content\":\"bad\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    assert!(prepare_history(stream, &request, true, false).is_none());
}
#[test]
fn empty_marker_respects_json_case_and_terminal_finish() {
    assert!(protocol::empty_content(" PrEvIeW: ```JSON {} ``` ", true));
    assert!(!protocol::empty_content("JSON: NULL", true));
    assert!(!protocol::empty_content("{}", false));
    let raw =
        br#"{"choices":[{"finish_reason":"length","message":{"role":"assistant","content":""}}]}"#;
    assert!(protocol::validate_live(raw, true).is_ok());
    assert!(prepare_history(raw, &json!({}), false, true).is_none());
}
#[test]
fn openai_sse_must_finish_and_end_before_ingest() {
    let chunk = b"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}],\"prompt_text\":\"private\"}\r\n\r\n";
    let done = b"data: [DONE]\r\n\r\n";
    let mut tracker = sse::Completion::default();
    for b in chunk.chunks(1) {
        tracker.feed(b, 1024);
    }
    assert!(tracker.incomplete());
    assert!(prepare_history(chunk, &json!({}), true, false).is_none());
    for b in done.chunks(1) {
        tracker.feed(b, 1024);
    }
    assert!(!tracker.incomplete());
    let raw = [chunk.as_slice(), done.as_slice()].concat();
    let clean = prepare_history(&raw, &json!({}), true, false).unwrap();
    assert!(!String::from_utf8(clean).unwrap().contains("private"));
    assert!(String::from_utf8(raw).unwrap().contains("private"));
}
#[test]
fn sulphur_returns_user_text_or_numbered_references() {
    let r = json!({"messages":[{"role":"system","content":"enhance"},{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}},{"type":"text","text":"  keep spaces  "}]}]});
    assert_eq!(sulphur::prompt(&r).as_deref(), Some("  keep spaces  "));
    let mut r = r;
    r["messages"][1]["content"][1]["text"] = json!("");
    assert_eq!(
        sulphur::prompt(&r).as_deref(),
        Some("Use the provided reference image as visual guidance.")
    );
}
#[tokio::test]
async fn unpolled_downstream_drops_the_upstream_response_at_deadline() {
    let (guard, dropped) = tokio::sync::oneshot::channel::<()>();
    // 这个发送端跟随模拟上游响应。只有响应被释放，接收端才会收到关闭通知。
    let upstream = async_stream::stream! {
        let _guard = guard;
        yield Ok::<_, std::io::Error>(Bytes::from_static(b"buffered"));
    };
    let response = delivery::bounded(
        Response::new(Body::from_stream(upstream)),
        Instant::now() + Duration::from_millis(30),
    );
    timeout(Duration::from_secs(1), dropped)
        .await
        .expect("下游没有读取正文时，超时也应释放上游响应")
        .expect_err("上游响应只在析构时关闭通知通道");
    assert!(to_bytes(response.into_body(), 1024).await.is_err());
}

#[tokio::test]
async fn reading_done_from_upstream_does_not_confirm_delivery() {
    let state = Arc::new(delivery::SseCacheTail::default());
    let upstream_state = state.clone();
    let (parsed, ready) = tokio::sync::oneshot::channel();
    let (resumed, drained) = tokio::sync::oneshot::channel();
    let upstream = async_stream::stream! {
        upstream_state.set_candidate(true);
        let _ = parsed.send(());
        yield Ok::<_, std::io::Error>(Bytes::from_static(b"data: [DONE]\n\n"));
        let _ = resumed.send(());
    };
    let mut response = Response::new(Body::from_stream(upstream));
    response.extensions_mut().insert(state);
    let response = delivery::bounded(response, Instant::now() + Duration::from_secs(10));

    // 上游已解析终帧，下游连这一帧都没取就断开。此时须释放上游，不能进入排尾阶段。
    ready.await.unwrap();
    drop(response);
    assert!(
        timeout(Duration::from_secs(1), drained)
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn delivered_done_with_a_stalled_tail_releases_the_upstream() {
    let state = Arc::new(delivery::SseCacheTail::default());
    let upstream_state = state.clone();
    let (resumed, draining) = tokio::sync::oneshot::channel();
    let (guard, dropped) = tokio::sync::oneshot::channel::<()>();
    let upstream = async_stream::stream! {
        let _guard = guard;
        upstream_state.set_candidate(true);
        yield Ok::<_, std::io::Error>(Bytes::from_static(b"data: [DONE]\n\n"));
        let _ = resumed.send(());
        std::future::pending::<()>().await;
    };
    let mut response = Response::new(Body::from_stream(upstream));
    let wall = Instant::now() + Duration::from_secs(10);
    response.extensions_mut().insert(delivery::Limits {
        wall,
        idle: Some(Duration::from_millis(50)),
    });
    response.extensions_mut().insert(state);
    let mut chunks = delivery::bounded(response, wall)
        .into_body()
        .into_data_stream();

    // 客户端取走终帧后直接关闭。上游没有发 HTTP EOF，排尾应在短时限内自行结束。
    assert_eq!(
        chunks.next().await.unwrap().unwrap().as_ref(),
        b"data: [DONE]\n\n"
    );
    drop(chunks);
    timeout(Duration::from_secs(1), draining)
        .await
        .unwrap()
        .unwrap();
    assert!(
        timeout(Duration::from_secs(1), dropped)
            .await
            .unwrap()
            .is_err()
    );
}

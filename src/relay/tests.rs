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
        context_digest("/v1/中", "a=1&a=2", &headers, Some(b"Bearer example")),
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
    assert!(protocol::model_not_found(
        br#"{"error":{"type":"MODEL_NOT_FOUND"}}"#
    ));
    assert!(protocol::validate_live(br#"{"error":{"type":"authentication_error"}}"#, true).is_ok());
    assert_eq!(
        protocol::validate_live(br#"{"error":{"type":"rate_limit_error"}}"#, true),
        Err(protocol::Fault::Temporary(429))
    );
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
async fn unpolled_downstream_releases_capacity_at_deadline() {
    let slots = Arc::new(Semaphore::new(1));
    let permit = slots.clone().acquire_owned().await.unwrap();
    let response = delivery::bounded(
        Response::new(Body::from("buffered")),
        permit,
        Instant::now() + Duration::from_millis(30),
    );
    assert!(slots.clone().try_acquire_owned().is_err());
    let _permit = timeout(Duration::from_secs(1), slots.acquire())
        .await
        .unwrap()
        .unwrap();
    assert!(to_bytes(response.into_body(), 1024).await.is_err());
}

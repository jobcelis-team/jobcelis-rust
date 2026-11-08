//! Retry behaviour of `JobcelisClient` against a small fake HTTP server.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

use crate::{JobcelisClient, JobcelisError};

/// What the fake server does with a request.
#[derive(Clone)]
enum Reply {
    /// Respond with this status, extra headers and body.
    Status(u16, Vec<(&'static str, &'static str)>, &'static str),
    /// Close the connection without responding (a network error).
    Drop,
}

fn status(code: u16) -> Reply {
    Reply::Status(code, vec![], r#"{"error":"status"}"#)
}

fn ok(body: &'static str) -> Reply {
    Reply::Status(200, vec![], body)
}

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Recorded {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

/// Serves the replies in order (the last one repeats) and records requests.
struct FakeServer {
    url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

impl FakeServer {
    fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let Some(request) = read_request(&stream) else { continue };
                let n = {
                    let mut all = recorded.lock().unwrap();
                    all.push(request);
                    all.len() - 1
                };
                let reply = replies[n.min(replies.len() - 1)].clone();
                respond(stream, reply);
            }
        });
        Self { url, requests }
    }

    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    fn client(&self) -> JobcelisClient {
        JobcelisClient::with_base_url("test_key", &self.url).with_retry_base_delay(Duration::from_millis(1))
    }
}

fn read_request(stream: &TcpStream) -> Option<Recorded> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length = 0;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let (k, v) = (k.trim().to_string(), v.trim().to_string());
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }

    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).ok()?;
    Some(Recorded { method, path, headers, body: String::from_utf8(body).ok()? })
}

fn respond(mut stream: TcpStream, reply: Reply) {
    match reply {
        Reply::Drop => drop(stream),
        Reply::Status(code, headers, body) => {
            let mut response = format!(
                "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
                body.len()
            );
            for (k, v) in headers {
                response.push_str(&format!("{k}: {v}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(body);
            let _ = stream.write_all(response.as_bytes());
        }
    }
}

fn api_status(result: Result<Value, JobcelisError>) -> u16 {
    match result {
        Err(JobcelisError::Api { status, .. }) => status,
        other => panic!("expected an API error, got {other:?}"),
    }
}

#[tokio::test]
async fn get_retries_503_until_it_succeeds() {
    let server = FakeServer::start(vec![status(503), status(503), ok(r#"{"id":"evt_1"}"#)]);
    let event = server.client().get_event("evt_1").await.unwrap();
    assert_eq!(event["id"], "evt_1");
    assert_eq!(server.count(), 3);
    assert!(server.requests().iter().all(|r| r.method == "GET" && r.path == "/api/v1/events/evt_1"));
}

#[tokio::test]
async fn retries_429_after_retry_after() {
    let server = FakeServer::start(vec![
        Reply::Status(429, vec![("retry-after", "0")], r#"{"error":"slow down"}"#),
        ok(r#"{"status":"ok"}"#),
    ]);
    let started = std::time::Instant::now();
    let health = server.client().health().await.unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(server.count(), 2);
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn send_event_keeps_the_same_idempotency_key_on_every_attempt() {
    let server = FakeServer::start(vec![status(502), Reply::Drop, ok(r#"{"event_id":"evt_1"}"#)]);
    let result = server.client().send_event("order.created", json!({"id": 1})).await.unwrap();
    assert_eq!(result["event_id"], "evt_1");

    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    let key = requests[0].header("x-idempotency-key").unwrap().to_string();
    assert_eq!(key.len(), 36);
    assert_eq!(&key[14..15], "4");
    assert!(requests.iter().all(|r| r.header("x-idempotency-key") == Some(key.as_str())));
}

#[tokio::test]
async fn send_event_keeps_a_caller_supplied_idempotency_key() {
    let server = FakeServer::start(vec![status(503), ok(r#"{"event_id":"evt_1"}"#)]);
    server
        .client()
        .send_event_with_idempotency_key("order.created", json!({}), "my-key")
        .await
        .unwrap();
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|r| r.header("x-idempotency-key") == Some("my-key")));
}

#[tokio::test]
async fn send_events_adds_missing_idempotency_keys() {
    let server = FakeServer::start(vec![status(503), ok(r#"{"accepted":2}"#)]);
    server
        .client()
        .send_events(vec![
            json!({"topic": "a", "payload": {}}),
            json!({"topic": "b", "payload": {}, "idempotency_key": "existing"}),
        ])
        .await
        .unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    let first = requests[0].json();
    let generated = first["events"][0]["idempotency_key"].as_str().unwrap().to_string();
    assert_eq!(generated.len(), 36);
    assert_eq!(first["events"][1]["idempotency_key"], "existing");
    assert_eq!(requests[1].json(), first, "retries send the same body");
}

#[tokio::test]
async fn post_without_idempotency_key_is_not_retried_on_network_error() {
    let server = FakeServer::start(vec![Reply::Drop, ok(r#"{"id":"wh_1"}"#)]);
    let result = server.client().create_webhook("https://example.com/hook", None).await;
    assert!(matches!(result, Err(JobcelisError::Request(_))), "{result:?}");
    assert_eq!(server.count(), 1);
}

#[tokio::test]
async fn post_without_idempotency_key_is_retried_on_503() {
    let server = FakeServer::start(vec![status(503), ok(r#"{"id":"wh_1"}"#)]);
    let webhook = server.client().create_webhook("https://example.com/hook", None).await.unwrap();
    assert_eq!(webhook["id"], "wh_1");
    assert_eq!(server.count(), 2);
}

#[tokio::test]
async fn get_is_retried_on_network_error() {
    let server = FakeServer::start(vec![Reply::Drop, ok(r#"{"id":"evt_1"}"#)]);
    let event = server.client().get_event("evt_1").await.unwrap();
    assert_eq!(event["id"], "evt_1");
    assert_eq!(server.count(), 2);
}

#[tokio::test]
async fn zero_max_retries_makes_one_request() {
    let server = FakeServer::start(vec![status(503)]);
    let result = server.client().with_max_retries(0).get_event("evt_1").await;
    assert_eq!(api_status(result), 503);
    assert_eq!(server.count(), 1);
}

#[tokio::test]
async fn gives_up_after_max_retries_with_the_last_error() {
    let server = FakeServer::start(vec![status(503)]);
    let result = server.client().with_max_retries(2).get_event("evt_1").await;
    assert_eq!(api_status(result), 503);
    assert_eq!(server.count(), 3);
}

#[tokio::test]
async fn non_retryable_statuses_are_not_retried() {
    for code in [400, 404, 500] {
        let server = FakeServer::start(vec![status(code), ok("{}")]);
        let result = server.client().get_event("evt_1").await;
        assert_eq!(api_status(result), code);
        assert_eq!(server.count(), 1, "status {code}");
    }
}

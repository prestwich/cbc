#![allow(dead_code)]

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::Response;
use cbc::{Config, app};
use serde_json::Value;
use std::net::SocketAddr;
use tower::ServiceExt;

pub const IP: &str = "10.0.0.1:1234";

pub fn open() -> Config {
    Config {
        probation_secs: 0,
        post_interval_secs: 0.001,
        post_burst: 1000.0,
        topic_interval_secs: 0.001,
        topic_burst: 10_000.0,
        global_posts_per_sec: 100_000.0,
        registrations_per_hour: 10_000.0,
        max_wait_secs: 2,
        timeout_secs: 5,
        ..Config::default()
    }
}

pub fn build(config: Config) -> Router {
    app(config)
}

pub fn creds(name: &str, pass: &str) -> String {
    serde_urlencoded::to_string([("name", name), ("pass", pass)]).unwrap()
}

pub fn post_url(topic: &str, content: &str, name: &str, pass: &str) -> String {
    let q = serde_urlencoded::to_string([
        ("topic", topic),
        ("content", content),
        ("name", name),
        ("pass", pass),
    ])
    .unwrap();
    format!("/?{q}")
}

pub fn wait_url(topic: &str, after: u64, wait: u64, name: &str, pass: &str) -> String {
    format!(
        "/?topic={topic}&after={after}&wait={wait}&{}",
        creds(name, pass)
    )
}

pub async fn get_from(router: &Router, uri: &str, ip: &str, forwarded: Option<&str>) -> Response {
    let mut request = Request::get(uri).body(Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo(ip.parse::<SocketAddr>().unwrap()));
    if let Some(chain) = forwarded {
        request
            .headers_mut()
            .insert("x-forwarded-for", HeaderValue::from_str(chain).unwrap());
    }
    router.clone().oneshot(request).await.unwrap()
}

pub async fn get(router: &Router, uri: &str) -> Response {
    get_from(router, uri, IP, None).await
}

pub async fn json(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, value)
}

pub async fn fetch(router: &Router, uri: &str) -> (StatusCode, Value) {
    json(get(router, uri).await).await
}

pub async fn register(router: &Router, name: &str, pass: &str) -> (StatusCode, Value) {
    fetch(router, &format!("/register?{}", creds(name, pass))).await
}

pub async fn ready(router: &Router, name: &str, pass: &str) {
    let (status, body) = register(router, name, pass).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

pub async fn post(
    router: &Router,
    topic: &str,
    content: &str,
    name: &str,
    pass: &str,
) -> (StatusCode, Value) {
    fetch(router, &post_url(topic, content, name, pass)).await
}

pub async fn posted(router: &Router, topic: &str, content: &str, name: &str, pass: &str) -> u64 {
    let (status, body) = post(router, topic, content, name, pass).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["seq"].as_u64().unwrap()
}

pub async fn items(router: &Router, uri: &str) -> Vec<Value> {
    let (status, body) = fetch(router, uri).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["items"].as_array().unwrap().clone()
}

pub fn contents(items: &[Value]) -> Vec<&str> {
    items
        .iter()
        .map(|i| i["content"].as_str().unwrap())
        .collect()
}

pub fn seqs(items: &[Value]) -> Vec<Option<u64>> {
    items.iter().map(|i| i["seq"].as_u64()).collect()
}

pub fn error(body: &Value) -> &str {
    body["error"].as_str().unwrap_or("")
}

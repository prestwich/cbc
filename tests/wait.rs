mod common;

use axum::http::StatusCode;
use cbc::Config;
use common::*;
use std::time::{Duration, Instant};

#[tokio::test]
async fn wait_returns_immediately_when_behind() {
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    posted(&router, "t", "one", "alice", "passphrase").await;
    posted(&router, "t", "two", "alice", "passphrase").await;
    let started = Instant::now();
    let (status, body) = fetch(&router, &wait_url("t", 1, 2, "alice", "passphrase")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(started.elapsed() < Duration::from_millis(500));
    assert_eq!(contents(body["items"].as_array().unwrap()), ["two"]);
    assert_eq!(
        body["next"],
        format!("/?topic=t&after=2&wait=2&{}", creds("alice", "passphrase"))
    );
}

#[tokio::test]
async fn wait_blocks_until_a_post_arrives() {
    let router = build(open());
    ready(&router, "reader", "passphrase").await;
    ready(&router, "writer", "passphrase").await;
    let writer = router.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        posted(&writer, "t", "late", "writer", "passphrase").await;
    });
    let started = Instant::now();
    let (status, body) = fetch(&router, &wait_url("t", 0, 2, "reader", "passphrase")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        started.elapsed() >= Duration::from_millis(250)
            && started.elapsed() < Duration::from_secs(2)
    );
    assert_eq!(contents(body["items"].as_array().unwrap()), ["late"]);
    assert_eq!(seqs(body["items"].as_array().unwrap()), [Some(1)]);
    assert!(body["next"].as_str().unwrap().contains("after=1"));
}

#[tokio::test]
async fn wait_times_out_empty() {
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    let started = Instant::now();
    let (status, body) = fetch(&router, &wait_url("t", 0, 1, "alice", "passphrase")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(body["items"].as_array().unwrap().is_empty());
    assert!(body["next"].as_str().unwrap().contains("after=0"));
}

#[tokio::test]
async fn wait_is_capped_to_max_wait() {
    let router = build(Config {
        max_wait_secs: 1,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    let started = Instant::now();
    fetch(&router, &wait_url("t", 0, 60, "alice", "passphrase")).await;
    assert!(started.elapsed() < Duration::from_millis(1800));
}

#[tokio::test]
async fn waiter_sees_ephemeral_with_null_seq() {
    let router = build(open());
    ready(&router, "reader", "passphrase").await;
    ready(&router, "writer", "passphrase").await;
    let writer = router.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        fetch(
            &writer,
            &format!(
                "{}&ephemeral=true",
                post_url("t", "flash", "writer", "passphrase")
            ),
        )
        .await;
    });
    let (_, body) = fetch(&router, &wait_url("t", 0, 2, "reader", "passphrase")).await;
    assert_eq!(contents(body["items"].as_array().unwrap()), ["flash"]);
    assert_eq!(seqs(body["items"].as_array().unwrap()), [None]);
    assert!(body["next"].as_str().unwrap().contains("after=0"));
    assert!(items(&router, "/?topic=t").await.is_empty());
}

#[tokio::test]
async fn waiters_capped_per_identity() {
    let router = build(Config {
        max_waiters: 1,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    ready(&router, "bob", "passphrase").await;
    let first = router.clone();
    let held =
        tokio::spawn(
            async move { fetch(&first, &wait_url("t", 0, 1, "alice", "passphrase")).await },
        );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, body) = fetch(&router, &wait_url("t", 0, 1, "alice", "passphrase")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("1 waits"));
    let (status, _) = fetch(&router, &wait_url("u", 0, 1, "bob", "passphrase")).await;
    assert_eq!(status, StatusCode::OK, "other identity unaffected");
    assert_eq!(held.await.unwrap().0, StatusCode::OK);
    let (status, _) = fetch(&router, &wait_url("t", 0, 1, "alice", "passphrase")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "slot released after the first wait finished"
    );
}

#[tokio::test]
async fn wait_requires_credentials_and_counts_topic_creation() {
    let router = build(Config {
        probation_secs: 3600,
        probation_interval_secs: 0.001,
        probation_topics: 1,
        ..open()
    });
    let (status, _) = fetch(&router, "/?topic=t&after=0&wait=1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    ready(&router, "alice", "passphrase").await;
    let (status, _) = fetch(&router, &wait_url("t", 0, 1, "alice", "wrong-pass")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = fetch(&router, &wait_url("first", 0, 1, "alice", "passphrase")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = fetch(&router, &wait_url("second", 0, 1, "alice", "passphrase")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("topics"));
}

#[tokio::test]
async fn wait_applies_min_age_and_n_to_live_messages() {
    let router = build(Config { max_n: 5, ..open() });
    ready(&router, "reader", "passphrase").await;
    ready(&router, "newborn", "passphrase").await;
    let writer = router.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        for i in 0..3 {
            posted(&writer, "t", &format!("m{i}"), "newborn", "passphrase").await;
        }
    });
    let filtered = format!("{}&min_age=60", wait_url("t", 0, 2, "reader", "passphrase"));
    let (status, body) = fetch(&router, &filtered).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["items"].as_array().unwrap().is_empty(), "{body}");
    let (_, capped) = fetch(
        &router,
        &format!("{}&n=2", wait_url("t", 0, 1, "reader", "passphrase")),
    )
    .await;
    assert_eq!(capped["items"].as_array().unwrap().len(), 2);
    assert!(capped["next"].as_str().unwrap().contains("after=2"));
}

mod common;

use axum::http::StatusCode;
use cbc::Config;
use common::*;

#[tokio::test]
async fn post_requires_credentials() {
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    let (status, body) = fetch(&router, "/?topic=t&content=hi").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(error(&body).contains("register"));
    let (status, _) = post(&router, "t", "hi", "alice", "wrong-pass").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = post(&router, "t", "hi", "nobody", "passphrase").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(items(&router, "/?topic=t").await.is_empty());
}

#[tokio::test]
async fn post_then_read() {
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    let (status, body) = post(&router, "t", "hello there", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["seq"], 1);
    assert_eq!(body["duplicate"], false);
    assert_eq!(body["next"]["read"], "/?topic=t&after=1");
    assert!(
        body["next"]["wait"]
            .as_str()
            .unwrap()
            .starts_with("/?topic=t&after=1&wait=2&name=alice&pass=passphrase")
    );
    let items = items(&router, "/?topic=t").await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["content"], "hello there");
    assert_eq!(items[0]["sender"], "alice");
    assert_eq!(items[0]["seq"], 1);
    assert!(items[0]["ts"].is_u64() && items[0]["sender_since"].is_u64());
}

#[tokio::test]
async fn ordering_paging_and_next() {
    let router = build(Config {
        default_n: 2,
        max_n: 3,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    for c in ["a", "b", "c", "d", "e"] {
        posted(&router, "t", c, "alice", "passphrase").await;
    }
    assert_eq!(contents(&items(&router, "/?topic=t").await), ["e", "d"]);
    assert_eq!(contents(&items(&router, "/?topic=t&n=1").await), ["e"]);
    assert_eq!(
        contents(&items(&router, "/?topic=t&n=99").await),
        ["e", "d", "c"]
    );
    let (_, page) = fetch(&router, "/?topic=t&after=3").await;
    assert_eq!(contents(page["items"].as_array().unwrap()), ["d", "e"]);
    assert_eq!(page["gap"], false);
    assert_eq!(page["next"], "/?topic=t&after=5");
    let (_, gap) = fetch(&router, "/?topic=t&after=0&n=10").await;
    assert_eq!(gap["gap"], true);
    assert_eq!(contents(gap["items"].as_array().unwrap()), ["c", "d", "e"]);
    let (_, capped) = fetch(&router, "/?topic=t&after=0").await;
    assert_eq!(contents(capped["items"].as_array().unwrap()), ["c", "d"]);
    assert_eq!(capped["next"], "/?topic=t&after=4");
    let (_, empty) = fetch(&router, "/?topic=t&after=5").await;
    assert!(empty["items"].as_array().unwrap().is_empty());
    assert_eq!(empty["next"], "/?topic=t&after=5");
}

#[tokio::test]
async fn duplicate_post_is_free_and_idempotent() {
    let router = build(Config {
        post_burst: 1.0,
        post_interval_secs: 3600.0,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    assert_eq!(posted(&router, "t", "same", "alice", "passphrase").await, 1);
    let (status, body) = post(&router, "t", "same", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["duplicate"], true);
    assert_eq!(body["next"]["read"], "/?topic=t&after=1");
    assert_eq!(items(&router, "/?topic=t").await.len(), 1);
    let (status, body) = post(&router, "t", "different", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("identity"));
}

#[tokio::test]
async fn probation_limits() {
    let router = build(Config {
        probation_secs: 3600,
        probation_interval_secs: 300.0,
        probation_topics: 1,
        ..open()
    });
    ready(&router, "newbie", "passphrase").await;
    ready(&router, "other", "passphrase").await;
    assert_eq!(
        posted(&router, "mine", "first", "newbie", "passphrase").await,
        1
    );
    let (status, body) = post(&router, "mine", "second", "newbie", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("identity"));
    assert!((1..=300).contains(&body["retry_after"].as_u64().unwrap()));
    let router = build(Config {
        probation_secs: 3600,
        probation_interval_secs: 0.001,
        probation_topics: 1,
        ..open()
    });
    ready(&router, "newbie", "passphrase").await;
    ready(&router, "other", "passphrase").await;
    posted(&router, "mine", "x", "newbie", "passphrase").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (status, body) = post(&router, "second-topic", "x", "newbie", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("at most 1 topics"), "{body}");
    posted(&router, "theirs", "x", "other", "passphrase").await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    posted(
        &router,
        "theirs",
        "joining an existing topic is fine",
        "newbie",
        "passphrase",
    )
    .await;
}

#[tokio::test]
async fn established_burst_then_limit() {
    let router = build(Config {
        post_burst: 3.0,
        post_interval_secs: 3600.0,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    for i in 0..3 {
        posted(&router, "t", &format!("m{i}"), "alice", "passphrase").await;
    }
    let (status, body) = post(&router, "t", "m3", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(error(&body).contains("identity"));
}

#[tokio::test]
async fn per_topic_and_global_limits() {
    let router = build(Config {
        topic_burst: 2.0,
        topic_interval_secs: 3600.0,
        ..open()
    });
    ready(&router, "a", "passphrase").await;
    ready(&router, "b", "passphrase").await;
    ready(&router, "c", "passphrase").await;
    posted(&router, "hot", "1", "a", "passphrase").await;
    posted(&router, "hot", "2", "b", "passphrase").await;
    let (status, body) = post(&router, "hot", "3", "c", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("topic rate"));
    posted(&router, "cool", "elsewhere", "c", "passphrase").await;

    let router = build(Config {
        global_posts_per_sec: 1.0,
        ..open()
    });
    ready(&router, "a", "passphrase").await;
    ready(&router, "b", "passphrase").await;
    posted(&router, "t", "1", "a", "passphrase").await;
    let (status, body) = post(&router, "u", "2", "b", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(
        error(&body).contains("global") || error(&body).contains("identity"),
        "{body}"
    );
}

#[tokio::test]
async fn content_rules_and_ephemeral() {
    let router = build(Config {
        max_content_bytes: 8,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    let (status, body) = post(&router, "t", "", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = post(&router, "t", "123456789", "alice", "passphrase").await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(error(&body).contains("8 bytes"));
    posted(&router, "t", "12345678", "alice", "passphrase").await;
    let (status, body) = fetch(
        &router,
        &format!(
            "{}&ephemeral=true",
            post_url("t", "flash", "alice", "passphrase")
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["seq"].is_null());
    assert_eq!(contents(&items(&router, "/?topic=t").await), ["12345678"]);
    let unicode = "héllo\n\"wörld\" & #tag";
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    posted(&router, "t", unicode, "alice", "passphrase").await;
    assert_eq!(contents(&items(&router, "/?topic=t").await), [unicode]);
}

#[tokio::test]
async fn min_age_hides_young_senders() {
    let router = build(open());
    ready(&router, "fresh", "passphrase").await;
    posted(&router, "t", "from a newborn", "fresh", "passphrase").await;
    assert_eq!(items(&router, "/?topic=t&min_age=0").await.len(), 1);
    assert!(items(&router, "/?topic=t&min_age=60").await.is_empty());
    assert!(
        items(&router, "/?topic=t&after=0&min_age=60")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn many_identities_one_topic() {
    let router = build(open());
    let mut handles = Vec::new();
    for i in 0..40 {
        let router = router.clone();
        handles.push(tokio::spawn(async move {
            let name = format!("agent{i}");
            ready(&router, &name, "passphrase").await;
            posted(&router, "t", &format!("m{i}"), &name, "passphrase").await
        }));
    }
    let mut seqs: Vec<u64> = Vec::new();
    for handle in handles {
        seqs.push(handle.await.unwrap());
    }
    seqs.sort();
    assert_eq!(seqs, (1..=40).collect::<Vec<_>>());
    assert_eq!(items(&router, "/?topic=t&n=100").await.len(), 40);
}

#[tokio::test]
async fn reading_an_unknown_topic_is_empty_and_creates_nothing() {
    let router = build(open());
    let (status, body) = fetch(&router, "/?topic=ghost").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["items"].as_array().unwrap().is_empty());
    assert_eq!(body["gap"], false);
    assert_eq!(body["next"], "/?topic=ghost&after=0");
    let (_, paged) = fetch(&router, "/?topic=ghost&after=7").await;
    assert_eq!(paged["next"], "/?topic=ghost&after=7");
    ready(&router, "alice", "passphrase").await;
    let router = build(Config {
        probation_secs: 3600,
        probation_topics: 1,
        ..open()
    });
    ready(&router, "alice", "passphrase").await;
    fetch(&router, "/?topic=ghost").await;
    posted(
        &router,
        "real",
        "reading ghost did not use my one topic",
        "alice",
        "passphrase",
    )
    .await;
}

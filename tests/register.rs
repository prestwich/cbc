mod common;

use axum::http::StatusCode;
use cbc::Config;
use common::*;

#[tokio::test]
async fn register_creates_then_recognizes() {
    let router = build(Config {
        probation_secs: 3600,
        ..open()
    });
    let (status, body) = register(&router, "scout-7", "blue-lantern-42").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["existing"], false);
    assert_eq!(body["tier"], "probation");
    assert_eq!(body["limits"]["topics"], 1);
    assert!(body["since"].as_u64().unwrap().abs_diff(cbc::now()) <= 1);
    let post = body["how"]["post"].as_str().unwrap();
    assert!(
        post.contains("name=scout-7")
            && post.contains("pass=blue-lantern-42")
            && post.contains("topic=TOPIC"),
        "{post}"
    );
    assert!(body["how"]["wait"].as_str().unwrap().contains("wait=2"));

    let (status, again) = register(&router, "scout-7", "blue-lantern-42").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["existing"], true);
    assert_eq!(again["since"], body["since"]);
}

#[tokio::test]
async fn register_rejects_bad_input() {
    let router = build(open());
    ready(&router, "taken", "correct-pass").await;
    let cases = [
        (
            "/register?name=taken&pass=wrong-pass",
            StatusCode::CONFLICT,
            "taken",
        ),
        ("/register?name=x", StatusCode::BAD_REQUEST, "required"),
        (
            "/register?pass=longenough",
            StatusCode::BAD_REQUEST,
            "required",
        ),
        (
            "/register?name=has%20space&pass=longenough",
            StatusCode::BAD_REQUEST,
            "1 to 32",
        ),
        (
            "/register?name=&pass=longenough",
            StatusCode::BAD_REQUEST,
            "1 to 32",
        ),
        (
            &format!("/register?name={}&pass=longenough", "a".repeat(33)),
            StatusCode::BAD_REQUEST,
            "1 to 32",
        ),
        (
            "/register?name=ok&pass=short",
            StatusCode::BAD_REQUEST,
            "8 to 128",
        ),
    ];
    for (uri, status, needle) in cases {
        let (got, body) = fetch(&router, uri).await;
        assert_eq!(got, status, "{uri} {body}");
        assert!(error(&body).contains(needle), "{uri} {body}");
        assert_eq!(body["ok"], false);
        assert_eq!(body["help"], "/");
        assert!(body["server_time"].is_u64());
    }
}

#[tokio::test]
async fn registrations_throttled_per_ip() {
    let router = build(Config {
        registrations_per_hour: 2.0,
        ..open()
    });
    ready(&router, "one", "passphrase").await;
    ready(&router, "two", "passphrase").await;
    let (status, body) = register(&router, "three", "passphrase").await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(error(&body).contains("registration"));
    assert!(body["retry_after"].as_u64().unwrap() > 0);
    let (status, _) = register(&router, "one", "passphrase").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "existing identity is not a registration"
    );
    let (status, _) = json(
        get_from(
            &router,
            &format!("/register?{}", creds("three", "passphrase")),
            "10.0.0.2:1",
            None,
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "other address has its own budget");
}

#[tokio::test]
async fn forwarded_for_only_when_trusted() {
    let uri = |n: u8| format!("/register?{}", creds(&format!("agent{n}"), "passphrase"));
    let untrusting = build(Config {
        registrations_per_hour: 1.0,
        ..open()
    });
    assert_eq!(
        get_from(&untrusting, &uri(1), IP, Some("1.1.1.1"))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        get_from(&untrusting, &uri(2), IP, Some("2.2.2.2"))
            .await
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let trusting = build(Config {
        registrations_per_hour: 1.0,
        trust_forwarded_for: true,
        ..open()
    });
    assert_eq!(
        get_from(&trusting, &uri(1), IP, Some("1.1.1.1, 9.9.9.9"))
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        get_from(&trusting, &uri(2), IP, Some("2.2.2.2, 9.9.9.9"))
            .await
            .status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        get_from(&trusting, &uri(3), IP, Some("2.2.2.2, 8.8.8.8"))
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn register_reports_topics_created() {
    let router = build(open());
    ready(&router, "alice", "passphrase").await;
    posted(&router, "first", "x", "alice", "passphrase").await;
    posted(&router, "second", "y", "alice", "passphrase").await;
    posted(&router, "first", "z", "alice", "passphrase").await;
    let (_, body) = register(&router, "alice", "passphrase").await;
    assert_eq!(body["limits"]["topics_created"], 2);
    assert_eq!(body["existing"], true);
}

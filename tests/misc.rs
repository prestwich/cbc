mod common;

use axum::http::{StatusCode, Uri};
use cbc::Config;
use common::*;

#[tokio::test]
async fn help_on_bare_root() {
    let router = build(Config {
        max_content_bytes: 777,
        probation_topics: 3,
        ..open()
    });
    for uri in ["/", "/?"] {
        let response = get(&router, uri).await;
        assert_eq!(response.status(), StatusCode::OK);
        let text = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let text = String::from_utf8(text.to_vec()).unwrap();
        assert!(text.contains("REGISTER") && text.contains("WAIT") && text.contains("ENCODING"));
        assert!(text.contains("1 to 777 bytes"), "{text}");
        assert!(text.contains("may create 3 topic"), "{text}");
        let placeholder = |rest: &str| {
            rest.split('}').next().is_some_and(|k| {
                !k.is_empty() && k.chars().all(|c| c.is_ascii_lowercase() || c == '_')
            })
        };
        assert!(
            !text.split('{').skip(1).any(placeholder),
            "unreplaced placeholder in {text}"
        );
    }
}

#[tokio::test]
async fn soft_errors_are_200() {
    let router = build(open());
    let (status, body) = fetch(&router, "/?topic=&soft=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], false);
    assert!(error(&body).contains("topic"));
    let (status, body) = fetch(&router, "/register?name=x&soft=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], false);
    let (status, _) = fetch(&router, &format!("/?topic={}&soft=1", "x".repeat(9000))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = fetch(&router, "/?topic=&soft=0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn transport_rules() {
    let router = build(open());
    let (status, body) = fetch(&router, "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(get(&router, "/nope").await.status(), StatusCode::NOT_FOUND);
    let (status, body) = fetch(&router, &format!("/?topic={}", "x".repeat(9000))).await;
    assert_eq!(status, StatusCode::URI_TOO_LONG);
    assert!(error(&body).contains("8192"));
    let (status, body) = fetch(&router, "/?topic=t&n=lots").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(error(&body).contains("n"));
    for uri in [
        "/",
        "/healthz",
        "/nope",
        "/?topic=t",
        "/?topic=",
        "/register?name=a&pass=passphrase",
    ] {
        assert_eq!(
            get(&router, uri).await.headers()["cache-control"],
            "no-store",
            "{uri}"
        );
    }
}

#[test]
fn redaction_hides_pass_only() {
    let uri: Uri = "/?topic=t&content=hi&name=alice&pass=s3cret&wait=5"
        .parse()
        .unwrap();
    assert_eq!(
        cbc::redact(&uri),
        "/?topic=t&content=hi&name=alice&pass=[redacted]&wait=5"
    );
    let bare: Uri = "/healthz".parse().unwrap();
    assert_eq!(cbc::redact(&bare), "/healthz");
    let register: Uri = "/register?pass=first&name=x".parse().unwrap();
    assert_eq!(cbc::redact(&register), "/register?pass=[redacted]&name=x");
}

#[test]
fn config_validation() {
    assert_eq!(Config::default().validate(), Ok(()));
    let cases = [
        (
            Config {
                max_n: 0,
                ..Config::default()
            },
            "max_n",
        ),
        (
            Config {
                max_memory_bytes: 10,
                ..Config::default()
            },
            "max_memory_bytes",
        ),
        (
            Config {
                max_wait_secs: 30,
                timeout_secs: 30,
                ..Config::default()
            },
            "max_wait_secs",
        ),
        (
            Config {
                probation_topics: 0,
                ..Config::default()
            },
            "probation_topics",
        ),
        (
            Config {
                max_waiters: 0,
                ..Config::default()
            },
            "max_waiters",
        ),
        (
            Config {
                registrations_per_hour: 0.0,
                ..Config::default()
            },
            "registrations_per_hour",
        ),
        (
            Config {
                post_interval_secs: -1.0,
                ..Config::default()
            },
            "post_interval_secs",
        ),
        (
            Config {
                global_posts_per_sec: f64::NAN,
                ..Config::default()
            },
            "global_posts_per_sec",
        ),
    ];
    for (config, needle) in cases {
        let error = config.validate().unwrap_err();
        assert!(error.contains(needle), "{error}");
    }
    let config = Config {
        max_memory_bytes: 1 << 30,
        max_identities: 1 << 18,
        max_content_bytes: 1024,
        max_n: 100,
        ..Config::default()
    };
    assert_eq!(
        config.max_topics(),
        ((1u64 << 30) - (1 << 18) * 512) / (1152 * 100)
    );
}

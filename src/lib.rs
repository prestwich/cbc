use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use moka::sync::Cache;
use opentelemetry::metrics::{Counter, UpDownCounter};
use opentelemetry::{KeyValue, global};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Semaphore, broadcast};
use tower::ServiceBuilder;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, Span, info_span, instrument, warn};

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub bind: String,
    pub default_n: usize,
    pub max_n: usize,
    pub max_content_bytes: usize,
    pub max_query_bytes: usize,
    pub max_memory_bytes: u64,
    pub max_concurrency: usize,
    pub timeout_secs: u64,
    pub max_identities: u64,
    pub identity_ttl_secs: u64,
    pub registrations_per_hour: f64,
    pub trust_forwarded_for: bool,
    pub probation_secs: u64,
    pub probation_interval_secs: f64,
    pub probation_topics: usize,
    pub post_interval_secs: f64,
    pub post_burst: f64,
    pub max_topics_per_identity: usize,
    pub topic_interval_secs: f64,
    pub topic_burst: f64,
    pub global_posts_per_sec: f64,
    pub dedupe_secs: u64,
    pub max_wait_secs: u64,
    pub max_waiters: usize,
    pub otlp_endpoint: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:3000".into(),
            default_n: 10,
            max_n: 100,
            max_content_bytes: 1024,
            max_query_bytes: 8192,
            max_memory_bytes: 1 << 30,
            max_concurrency: 1024,
            timeout_secs: 30,
            max_identities: 1 << 18,
            identity_ttl_secs: 30 * 86_400,
            registrations_per_hour: 3.0,
            trust_forwarded_for: false,
            probation_secs: 3600,
            probation_interval_secs: 300.0,
            probation_topics: 1,
            post_interval_secs: 10.0,
            post_burst: 5.0,
            max_topics_per_identity: 20,
            topic_interval_secs: 2.0,
            topic_burst: 10.0,
            global_posts_per_sec: 50.0,
            dedupe_secs: 300,
            max_wait_secs: 25,
            max_waiters: 4,
            otlp_endpoint: None,
        }
    }
}

const IDENTITY_BYTES: u64 = 512;

impl Config {
    pub fn from_env() -> Result<Self, envy::Error> {
        envy::prefixed("CBC_").from_env()
    }

    pub fn max_topics(&self) -> u64 {
        let per_topic = ((self.max_content_bytes + 128) * self.max_n) as u64;
        let budget = self
            .max_memory_bytes
            .saturating_sub(self.max_identities * IDENTITY_BYTES);
        budget.checked_div(per_topic).unwrap_or(0)
    }

    pub fn validate(&self) -> Result<(), String> {
        let positive = |v: f64| v > 0.0 && v.is_finite();
        let checks = [
            (self.max_n >= 1, "max_n must be at least 1"),
            (
                self.max_topics() >= 1,
                "max_memory_bytes too small for identities plus one topic",
            ),
            (
                self.max_wait_secs < self.timeout_secs,
                "max_wait_secs must be below timeout_secs",
            ),
            (
                self.probation_topics >= 1,
                "probation_topics must be at least 1 or nobody can create a topic",
            ),
            (self.max_waiters >= 1, "max_waiters must be at least 1"),
            (
                positive(self.registrations_per_hour),
                "registrations_per_hour must be positive",
            ),
            (
                positive(self.probation_interval_secs),
                "probation_interval_secs must be positive",
            ),
            (
                positive(self.post_interval_secs),
                "post_interval_secs must be positive",
            ),
            (positive(self.post_burst), "post_burst must be positive"),
            (
                positive(self.topic_interval_secs),
                "topic_interval_secs must be positive",
            ),
            (positive(self.topic_burst), "topic_burst must be positive"),
            (
                positive(self.global_posts_per_sec),
                "global_posts_per_sec must be positive",
            ),
        ];
        checks
            .iter()
            .find(|(ok, _)| !ok)
            .map_or(Ok(()), |(_, msg)| Err(msg.to_string()))
    }
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn full(burst: f64) -> Mutex<Self> {
        Mutex::new(Self {
            tokens: burst,
            last: Instant::now(),
        })
    }

    fn take(&mut self, per_sec: f64, burst: f64) -> Result<(), u64> {
        let now = Instant::now();
        let refill = now.duration_since(self.last).as_secs_f64() * per_sec;
        self.tokens = (self.tokens + refill).min(burst);
        self.last = now;
        match self.tokens >= 1.0 {
            true => {
                self.tokens -= 1.0;
                Ok(())
            }
            false => Err(((1.0 - self.tokens) / per_sec).ceil() as u64),
        }
    }
}

#[derive(Debug)]
pub enum Reject {
    Bad(String),
    ContentTooLong(usize),
    QueryTooLong(usize),
    Credentials,
    NameTaken,
    Limited(&'static str, u64),
    Topics(usize),
    Waiters(usize),
}

impl Reject {
    fn reason(&self) -> &'static str {
        match self {
            Reject::Bad(_) => "bad_request",
            Reject::ContentTooLong(_) => "content_too_long",
            Reject::QueryTooLong(_) => "query_too_long",
            Reject::Credentials => "credentials",
            Reject::NameTaken => "name_taken",
            Reject::Limited(scope, _) => scope,
            Reject::Topics(_) => "topic_limit",
            Reject::Waiters(_) => "waiter_limit",
        }
    }

    fn response(&self, soft: bool) -> Response {
        let (status, error) = match self {
            Reject::Bad(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Reject::ContentTooLong(max) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("content exceeds {max} bytes"),
            ),
            Reject::QueryTooLong(max) => (
                StatusCode::URI_TOO_LONG,
                format!("query exceeds {max} bytes"),
            ),
            Reject::Credentials => (
                StatusCode::FORBIDDEN,
                "unknown name or wrong pass; register first".into(),
            ),
            Reject::NameTaken => (
                StatusCode::CONFLICT,
                "name is taken; choose another or supply its pass".into(),
            ),
            Reject::Limited(scope, secs) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("{scope} rate limit; retry in {secs}s"),
            ),
            Reject::Topics(max) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("identity may create at most {max} topics at its tier"),
            ),
            Reject::Waiters(max) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("identity already has {max} waits open"),
            ),
        };
        let mut body = json!({ "ok": false, "error": error, "server_time": now(), "help": "/" });
        if let Reject::Limited(_, secs) = self {
            body["retry_after"] = (*secs).into();
        }
        let status = if soft { StatusCode::OK } else { status };
        (status, Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct Message {
    seq: Option<u64>,
    ts: u64,
    sender: Arc<str>,
    sender_since: u64,
    content: String,
}

struct Topic {
    seq: AtomicU64,
    ring: RwLock<VecDeque<Arc<Message>>>,
    tx: broadcast::Sender<Arc<Message>>,
    bucket: Mutex<Bucket>,
}

pub struct Identity {
    name: Arc<str>,
    since: u64,
    salt: [u8; 16],
    hash: [u8; 32],
    topics: AtomicUsize,
    bucket: Mutex<Bucket>,
    waiters: Arc<Semaphore>,
}

fn hash(salt: &[u8; 16], pass: &str) -> [u8; 32] {
    Sha256::new()
        .chain_update(salt)
        .chain_update(pass)
        .finalize()
        .into()
}

#[derive(Clone)]
struct Metrics {
    rejections: Counter<u64>,
    messages: Counter<u64>,
    registrations: Counter<u64>,
    waiters: UpDownCounter<i64>,
}

#[derive(Clone)]
pub struct App {
    config: Arc<Config>,
    topics: Cache<String, Arc<Topic>>,
    identities: Cache<String, Arc<Identity>>,
    ips: Cache<IpAddr, Arc<Mutex<Bucket>>>,
    recent: Cache<[u8; 32], ()>,
    active: Cache<Arc<str>, ()>,
    global: Arc<Mutex<Bucket>>,
    metrics: Metrics,
}

struct Tier {
    name: &'static str,
    interval: f64,
    burst: f64,
    topics: usize,
}

impl App {
    fn tier(&self, identity: &Identity) -> Tier {
        let c = &self.config;
        match now().saturating_sub(identity.since) < c.probation_secs {
            true => Tier {
                name: "probation",
                interval: c.probation_interval_secs,
                burst: 1.0,
                topics: c.probation_topics,
            },
            false => Tier {
                name: "established",
                interval: c.post_interval_secs,
                burst: c.post_burst,
                topics: c.max_topics_per_identity,
            },
        }
    }

    fn authenticate(
        &self,
        name: Option<&str>,
        pass: Option<&str>,
    ) -> Result<Arc<Identity>, Reject> {
        let (Some(name), Some(pass)) = (name, pass) else {
            return Err(Reject::Bad(
                "name and pass are required; GET /register?name=NAME&pass=PASS first".into(),
            ));
        };
        let identity = self.identities.get(name).ok_or(Reject::Credentials)?;
        match hash(&identity.salt, pass) == identity.hash {
            true => Ok(identity),
            false => Err(Reject::Credentials),
        }
    }

    fn topic_for(&self, identity: &Identity, name: String) -> Result<Arc<Topic>, Reject> {
        if let Some(topic) = self.topics.get(&name) {
            return Ok(topic);
        }
        let allowed = self.tier(identity).topics;
        if identity
            .topics
            .fetch_update(Relaxed, Relaxed, |n| (n < allowed).then_some(n + 1))
            .is_err()
        {
            return Err(Reject::Topics(allowed));
        }
        Ok(self.topics.get_with(name, || {
            Arc::new(Topic {
                seq: AtomicU64::new(0),
                ring: Default::default(),
                tx: broadcast::channel(self.config.max_n).0,
                bucket: Bucket::full(self.config.topic_burst),
            })
        }))
    }

    fn track(&self, soft: bool, result: Result<Response, Reject>) -> Response {
        result.unwrap_or_else(|reject| {
            warn!(reason = reject.reason(), detail = ?reject, "rejected");
            self.metrics
                .rejections
                .add(1, &[KeyValue::new("reason", reject.reason())]);
            reject.response(soft)
        })
    }
}

#[derive(Deserialize)]
struct Params {
    topic: Option<String>,
    content: Option<String>,
    n: Option<usize>,
    after: Option<u64>,
    min_age: Option<u64>,
    wait: Option<u64>,
    name: Option<String>,
    pass: Option<String>,
    #[serde(default)]
    ephemeral: bool,
}

fn parse(uri: &Uri) -> Result<Params, Reject> {
    let Query(params) =
        Query::<Params>::try_from_uri(uri).map_err(|e| Reject::Bad(e.body_text()))?;
    Ok(params)
}

fn soft(uri: &Uri) -> bool {
    uri.query()
        .is_some_and(|q| q.split('&').any(|kv| kv == "soft=true" || kv == "soft=1"))
}

fn ok(mut body: Value) -> Response {
    body["ok"] = true.into();
    body["server_time"] = now().into();
    Json(body).into_response()
}

fn query(pairs: &[(&str, String)]) -> String {
    serde_urlencoded::to_string(pairs).unwrap()
}

fn valid_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn client_ip(config: &Config, headers: &HeaderMap, addr: SocketAddr) -> IpAddr {
    let forwarded = || {
        headers
            .get("x-forwarded-for")?
            .to_str()
            .ok()?
            .rsplit(',')
            .next()?
            .trim()
            .parse()
            .ok()
    };
    match config.trust_forwarded_for {
        true => forwarded().unwrap_or(addr.ip()),
        false => addr.ip(),
    }
}

fn help(config: &Config) -> Response {
    let values = [
        ("{default_n}", config.default_n.to_string()),
        ("{max_n}", config.max_n.to_string()),
        ("{max_content_bytes}", config.max_content_bytes.to_string()),
        ("{max_wait_secs}", config.max_wait_secs.to_string()),
        ("{max_waiters}", config.max_waiters.to_string()),
        (
            "{registrations_per_hour}",
            config.registrations_per_hour.to_string(),
        ),
        ("{probation_secs}", config.probation_secs.to_string()),
        (
            "{probation_interval_secs}",
            config.probation_interval_secs.to_string(),
        ),
        ("{probation_topics}", config.probation_topics.to_string()),
        (
            "{post_interval_secs}",
            config.post_interval_secs.to_string(),
        ),
        ("{post_burst}", config.post_burst.to_string()),
        (
            "{max_topics_per_identity}",
            config.max_topics_per_identity.to_string(),
        ),
        ("{dedupe_secs}", config.dedupe_secs.to_string()),
    ];
    let text = include_str!("help.txt");
    values
        .iter()
        .fold(text.to_string(), |t, (k, v)| t.replace(k, v))
        .into_response()
}

#[instrument(skip_all, fields(name = params.name.as_deref().unwrap_or(""), ip = %ip))]
fn register(app: &App, ip: IpAddr, params: Params) -> Result<Response, Reject> {
    let (Some(name), Some(pass)) = (params.name, params.pass) else {
        return Err(Reject::Bad("name and pass are required".into()));
    };
    if !valid_name(&name) {
        return Err(Reject::Bad(
            "name must be 1 to 32 characters from a-z A-Z 0-9 _ -".into(),
        ));
    }
    if !(8..=128).contains(&pass.len()) {
        return Err(Reject::Bad("pass must be 8 to 128 bytes".into()));
    }
    let (identity, existed) = match app.identities.get(&name) {
        Some(identity) if hash(&identity.salt, &pass) == identity.hash => (identity, true),
        Some(_) => return Err(Reject::NameTaken),
        None => {
            let bucket = app.ips.get_with(ip, || {
                Arc::new(Bucket::full(app.config.registrations_per_hour))
            });
            let per_sec = app.config.registrations_per_hour / 3600.0;
            bucket
                .lock()
                .take(per_sec, app.config.registrations_per_hour)
                .map_err(|s| Reject::Limited("registration", s))?;
            let salt: [u8; 16] = rand::random();
            let fresh = Arc::new(Identity {
                name: name.as_str().into(),
                since: now(),
                hash: hash(&salt, &pass),
                salt,
                topics: AtomicUsize::new(0),
                bucket: Bucket::full(app.config.post_burst.max(1.0)),
                waiters: Arc::new(Semaphore::new(app.config.max_waiters)),
            });
            let stored = app.identities.get_with(name.clone(), || fresh.clone());
            if !Arc::ptr_eq(&stored, &fresh) {
                return Err(Reject::NameTaken);
            }
            (stored, false)
        }
    };
    let tier = app.tier(&identity);
    let creds = query(&[("name", name.clone()), ("pass", pass)]);
    app.metrics
        .registrations
        .add(1, &[KeyValue::new("existing", existed)]);
    Ok(ok(json!({
        "name": name,
        "existing": existed,
        "since": identity.since,
        "tier": tier.name,
        "limits": {
            "post_interval_secs": tier.interval,
            "post_burst": tier.burst,
            "topics": tier.topics,
            "topics_created": identity.topics.load(Relaxed),
        },
        "how": {
            "post": format!("/?topic=TOPIC&content=CONTENT&{creds}"),
            "post_ephemeral": format!("/?topic=TOPIC&content=CONTENT&ephemeral=true&{creds}"),
            "read": "/?topic=TOPIC",
            "wait": format!("/?topic=TOPIC&after=SEQ&wait={}&{creds}", app.config.max_wait_secs),
            "help": "/",
        },
    })))
}

fn limit(params: &Params, config: &Config) -> usize {
    params.n.unwrap_or(config.default_n).min(config.max_n)
}

fn young(message: &Message, min_age: Option<u64>) -> bool {
    min_age.is_some_and(|age| now().saturating_sub(message.sender_since) < age)
}

fn items(topic: &Topic, params: &Params, config: &Config) -> (Vec<Arc<Message>>, bool) {
    let n = limit(params, config);
    let ring = topic.ring.read();
    let too_young = |m: &Arc<Message>| young(m, params.min_age);
    match params.after {
        None => (
            ring.iter()
                .rev()
                .filter(|m| !too_young(m))
                .take(n)
                .cloned()
                .collect(),
            false,
        ),
        Some(after) => {
            let oldest = ring.front().and_then(|m| m.seq).unwrap_or(1);
            let gap = after + 1 < oldest;
            let items = ring
                .iter()
                .filter(|m| m.seq.is_some_and(|s| s > after) && !too_young(m))
                .take(n)
                .cloned()
                .collect();
            (items, gap)
        }
    }
}

fn page(topic_name: &str, items: Vec<Arc<Message>>, gap: bool, params: &Params) -> Response {
    let last = items
        .iter()
        .filter_map(|m| m.seq)
        .max()
        .or(params.after)
        .unwrap_or(0);
    let mut pairs = vec![
        ("topic", topic_name.to_string()),
        ("after", last.to_string()),
    ];
    if let Some(wait) = params.wait {
        pairs.push(("wait", wait.to_string()));
        pairs.push(("name", params.name.clone().unwrap_or_default()));
        pairs.push(("pass", params.pass.clone().unwrap_or_default()));
    }
    ok(
        json!({ "topic": topic_name, "items": items, "gap": gap, "next": format!("/?{}", query(&pairs)) }),
    )
}

#[instrument(skip_all, fields(topic = %topic_name))]
fn read(app: &App, topic_name: &str, params: &Params) -> Result<Response, Reject> {
    let (items, gap) = match app.topics.get(topic_name) {
        Some(topic) => items(&topic, params, &app.config),
        None => (Vec::new(), false),
    };
    Ok(page(topic_name, items, gap, params))
}

#[instrument(skip_all, fields(topic = %topic_name, name = params.name.as_deref().unwrap_or("")))]
async fn wait(app: &App, topic_name: &str, params: &Params, secs: u64) -> Result<Response, Reject> {
    let identity = app.authenticate(params.name.as_deref(), params.pass.as_deref())?;
    let secs = secs.min(app.config.max_wait_secs);
    let _permit = identity
        .waiters
        .clone()
        .try_acquire_owned()
        .map_err(|_| Reject::Waiters(app.config.max_waiters))?;
    let topic = app.topic_for(&identity, topic_name.to_string())?;
    let mut rx = topic.tx.subscribe();
    let (mut found, gap) = items(&topic, params, &app.config);
    if found.is_empty() {
        app.metrics.waiters.add(1, &[]);
        if let Ok(Ok(first)) = tokio::time::timeout(Duration::from_secs(secs), rx.recv()).await {
            found.push(first);
            while let Ok(more) = rx.try_recv() {
                found.push(more);
            }
            found.retain(|m| !young(m, params.min_age));
            found.truncate(limit(params, &app.config));
        }
        app.metrics.waiters.add(-1, &[]);
    }
    Ok(page(topic_name, found, gap, params))
}

#[instrument(skip_all, fields(topic = %topic_name, name = params.name.as_deref().unwrap_or(""), ephemeral = params.ephemeral))]
fn post(app: &App, topic_name: &str, params: Params, content: String) -> Result<Response, Reject> {
    let config = &app.config;
    let identity = app.authenticate(params.name.as_deref(), params.pass.as_deref())?;
    if content.is_empty() {
        return Err(Reject::Bad("content is required".into()));
    }
    if content.len() > config.max_content_bytes {
        return Err(Reject::ContentTooLong(config.max_content_bytes));
    }
    let creds = query(&[
        ("name", identity.name.to_string()),
        ("pass", params.pass.clone().unwrap_or_default()),
    ]);
    let next = |seq: u64| {
        let t = query(&[
            ("topic", topic_name.to_string()),
            ("after", seq.to_string()),
        ]);
        json!({ "read": format!("/?{t}"), "wait": format!("/?{t}&wait={}&{creds}", config.max_wait_secs) })
    };
    let key: [u8; 32] = Sha256::new()
        .chain_update(identity.name.as_bytes())
        .chain_update([0])
        .chain_update(topic_name)
        .chain_update([0])
        .chain_update(&content)
        .finalize()
        .into();
    if app.recent.contains_key(&key) {
        let seq = app
            .topics
            .get(topic_name)
            .map_or(0, |t| t.seq.load(Relaxed));
        return Ok(ok(json!({ "duplicate": true, "next": next(seq) })));
    }
    let tier = app.tier(&identity);
    let share = config.global_posts_per_sec / app.active.entry_count().max(1) as f64;
    let per_sec = (1.0 / tier.interval).min(share);
    identity
        .bucket
        .lock()
        .take(per_sec, tier.burst)
        .map_err(|s| Reject::Limited("identity", s))?;
    app.global
        .lock()
        .take(config.global_posts_per_sec, config.global_posts_per_sec)
        .map_err(|s| Reject::Limited("global", s))?;
    let topic = app.topic_for(&identity, topic_name.to_string())?;
    topic
        .bucket
        .lock()
        .take(1.0 / config.topic_interval_secs, config.topic_burst)
        .map_err(|s| Reject::Limited("topic", s))?;
    app.recent.insert(key, ());
    app.active.insert(identity.name.clone(), ());
    let mut ring = topic.ring.write();
    let seq = (!params.ephemeral).then(|| topic.seq.fetch_add(1, Relaxed) + 1);
    let message = Arc::new(Message {
        seq,
        ts: now(),
        sender: identity.name.clone(),
        sender_since: identity.since,
        content,
    });
    if seq.is_some() {
        ring.push_back(message.clone());
        if ring.len() > config.max_n {
            ring.pop_front();
        }
    }
    let _ = topic.tx.send(message);
    drop(ring);
    app.metrics
        .messages
        .add(1, &[KeyValue::new("ephemeral", params.ephemeral)]);
    let latest = topic.seq.load(Relaxed);
    Ok(ok(
        json!({ "seq": seq, "duplicate": false, "next": next(latest) }),
    ))
}

async fn root_route(State(app): State<App>, uri: Uri) -> Response {
    if uri.query().is_none_or(str::is_empty) {
        return help(&app.config);
    }
    let soft = soft(&uri);
    let result = async {
        let params = parse(&uri)?;
        let topic = params
            .topic
            .clone()
            .filter(|t| !t.is_empty())
            .ok_or(Reject::Bad("topic is required".into()))?;
        match (params.content.clone(), params.wait) {
            (Some(content), _) => post(&app, &topic, params, content),
            (None, Some(secs)) => wait(&app, &topic, &params, secs).await,
            (None, None) => read(&app, &topic, &params),
        }
    }
    .await;
    app.track(soft, result)
}

async fn register_route(
    State(app): State<App>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let ip = client_ip(&app.config, &headers, addr);
    let result = parse(&uri).and_then(|params| register(&app, ip, params));
    app.track(soft(&uri), result)
}

async fn health() -> Response {
    Json(json!({ "status": "ok" })).into_response()
}

async fn limit_query(State(app): State<App>, req: Request, next: Next) -> Response {
    let max = app.config.max_query_bytes;
    if req.uri().query().is_some_and(|q| q.len() > max) {
        return app.track(soft(req.uri()), Err(Reject::QueryTooLong(max)));
    }
    next.run(req).await
}

pub fn redact(uri: &Uri) -> String {
    let Some(query) = uri.query() else {
        return uri.path().to_string();
    };
    let cleaned: Vec<&str> = query
        .split('&')
        .map(|kv| {
            if kv.starts_with("pass=") {
                "pass=[redacted]"
            } else {
                kv
            }
        })
        .collect();
    format!("{}?{}", uri.path(), cleaned.join("&"))
}

fn span(req: &Request) -> Span {
    info_span!("request", method = %req.method(), uri = %redact(req.uri()))
}

type Gauge = Box<dyn Fn() -> u64 + Send + Sync>;

pub fn app(config: Config) -> Router {
    let meter = global::meter("cbc");
    let seconds = Duration::from_secs;
    let app = App {
        topics: Cache::new(config.max_topics()),
        identities: Cache::builder()
            .max_capacity(config.max_identities)
            .time_to_idle(seconds(config.identity_ttl_secs))
            .build(),
        ips: Cache::builder()
            .max_capacity(config.max_identities)
            .time_to_idle(seconds(3600))
            .build(),
        recent: Cache::builder()
            .max_capacity(config.max_identities)
            .time_to_live(seconds(config.dedupe_secs))
            .build(),
        active: Cache::builder()
            .max_capacity(config.max_identities)
            .time_to_live(seconds(60))
            .build(),
        global: Arc::new(Bucket::full(config.global_posts_per_sec)),
        metrics: Metrics {
            rejections: meter.u64_counter("cbc.rejections").build(),
            messages: meter.u64_counter("cbc.messages").build(),
            registrations: meter.u64_counter("cbc.registrations").build(),
            waiters: meter.i64_up_down_counter("cbc.waiters").build(),
        },
        config: Arc::new(config),
    };
    let gauges: [(&str, Gauge); 2] = [
        (
            "cbc.topics",
            Box::new({
                let c = app.topics.clone();
                move || c.entry_count()
            }),
        ),
        (
            "cbc.identities",
            Box::new({
                let c = app.identities.clone();
                move || c.entry_count()
            }),
        ),
    ];
    for (name, count) in gauges {
        meter
            .u64_observable_gauge(name)
            .with_callback(move |o| o.observe(count(), &[]))
            .build();
    }
    let trace = TraceLayer::new_for_http()
        .make_span_with(span)
        .on_request(DefaultOnRequest::new().level(Level::INFO))
        .on_response(DefaultOnResponse::new().level(Level::INFO));
    let no_store = SetResponseHeaderLayer::overriding(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    let timeout = TimeoutLayer::with_status_code(
        StatusCode::REQUEST_TIMEOUT,
        seconds(app.config.timeout_secs),
    );
    Router::new()
        .route("/", get(root_route))
        .route("/register", get(register_route))
        .route("/healthz", get(health))
        .layer(
            ServiceBuilder::new()
                .layer(trace)
                .layer(no_store)
                .layer(GlobalConcurrencyLimitLayer::new(app.config.max_concurrency))
                .layer(timeout)
                .layer(middleware::from_fn_with_state(app.clone(), limit_query)),
        )
        .with_state(app)
}

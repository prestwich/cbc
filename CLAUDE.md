# cbc

In-memory topic message board for agents whose only tool is HTTP GET. Identities
self-register with a chosen name and passphrase. Abuse is bounded by age-based
quotas, per-topic and global rate limits, and reader-side filtering, not by any
client-side computation. No persistence. Everything is lost on restart.

## Layout

- `src/lib.rs` — config, token bucket, rejection type, identity and topic state, quota logic, handlers, `app(config) -> Router`.
- `src/main.rs` — loads and validates config, sets up tracing and OTLP export, binds with connect-info, serves.
- `src/help.txt` — help page served on a bare `GET /`. `{placeholders}` are replaced from config at request time. Written for a language model to read.
- `tests/common/mod.rs` — shared helpers: `build`, `open` (a permissive config), `register`/`ready`, `post`/`posted`, `wait_url`, `get_from` with a fake peer address and optional `X-Forwarded-For`.
- `tests/register.rs`, `tests/post.rs`, `tests/wait.rs`, `tests/misc.rs` — integration tests via `tower::ServiceExt::oneshot`. No network, no server process.
- `Dockerfile` — two-stage build to a distroless image. Verified: builds, runs as uid 65532, passes the same curl checks as the host binary.
- `README.md` — user-facing docs. Keep in sync with `src/help.txt` and the config defaults.
- `LICENSE-MIT`, `LICENSE-APACHE` — dual licensed, MIT OR Apache-2.0. Cargo.toml carries publish metadata, but the crate is not published: the name `cbc` is taken on crates.io and the owner chose to keep it.

## Build and check

```sh
cargo fmt
cargo clippy --all-targets      # must be warning-free
cargo test
cargo build --release
```

Run: `./target/release/cbc`. Configure with `CBC_*` env vars (README has the table).

For a quick manual session, run with `CBC_PROBATION_SECS=0` so a fresh identity is
established immediately, then use curl as in the README. Strip ANSI codes before
grepping logs. `python3 -m http.server 4318` works as a stand-in OTLP collector; set
`OTEL_METRIC_EXPORT_INTERVAL=2000` to see metrics quickly.

## How requests flow

- `GET /` with no query → help. `GET /register` → `register`. Everything else on `/`
  dispatches on params: `content` present → `post`; else `wait` present → `wait`; else `read`.
- `post` check order: authenticate, content non-empty and within size, dedupe key hit →
  free duplicate response, identity bucket (rate is the tier rate capped by the global fair
  share), global bucket, topic lookup-or-create (creation counts against the tier's topic
  allowance), topic bucket, then store and broadcast. Buckets consumed before a later
  check fails stay consumed; that is accepted.
- `wait` authenticates, takes a waiter permit, looks up or creates the topic, subscribes to
  the broadcast channel *before* scanning the ring, returns immediately if anything is
  newer than `after`, otherwise awaits the first broadcast up to `wait` seconds, drains
  whatever else is already queued, then applies `min_age` and `n` to that batch.
- `read` is unauthenticated and never creates a topic.
- `App::track` turns a `Reject` into the JSON error, logs it at warn, and counts it. `soft=1`
  in the query makes the status 200.
- Tier is derived from identity age only: probation below `probation_secs`, established after.

## Decisions already made

These were chosen deliberately by the owner. Do not change them without asking.

- Code stays minimal and uncommented. Prefer fewer lines over abstraction.
- Everything is GET with query params, including writes and credentials, because the
  target client can do nothing else. `Cache-Control: no-store` on every response.
- No operator, no invite codes, no client-side proof of work or signatures. Identities are
  free by design; the defenses are quotas, per-topic and global limits, and `min_age`.
- Chosen name and passphrase rather than server-issued tokens, so an agent that remembers
  its own words can recover its identity. Passphrases are salted SHA-256, not a slow KDF;
  the stakes are low and verification runs on every request.
- Re-registering with the same pair returns the same identity. This is the login path.
- Duplicate posts within `dedupe_secs` succeed with `duplicate: true` and cost nothing,
  because agents retry and a 409 would loop them.
- Long-polling with per-topic sequence numbers replaced SSE. Ephemeral messages reach
  waiters with `seq: null` and are never stored.
- Waits require credentials so the waiter cap can be per identity. Reads do not.
- No graceful shutdown. No SSE. No CI. Vesting is by age only, no post-count component.
  The optional registration delay challenge from the design discussion was omitted, not
  shipped disabled.
- Config is passed into `app(config)` and lives in state, so tests build routers with
  different settings. `Config::validate` runs before bind and exits 2 on failure.
- Eviction is moka (TinyLFU). Rings hold `Arc<Message>` behind `parking_lot::RwLock`.
- Tests exist despite the minimal-code rule. Tests use `open()` to disable limits they are
  not testing, and set exact limits when they are. Probation has a burst of one, so tests
  that post twice on probation must sleep a few milliseconds between posts.

## Gotchas

- `pass` is redacted from the request span by a custom `make_span_with`, and from nothing
  else. Never log a URI or `Params` directly. `redact` is public and tested.
- Buckets start full at `post_burst`, then clamp to the tier's burst on first use, so a
  probation identity has exactly one token. Starting them at 1.0 broke every burst test.
- Fair share divides `global_posts_per_sec` by moka `entry_count` of the `active` cache,
  which is approximate and can lag, so per-identity rates may be briefly lower than ideal.
- `topic_for` increments the identity's topic counter before creating the topic and does
  not roll back if a later check fails. Waiting on a new topic counts as creating it.
- An identity evicted from the cache loses its counters and buckets; re-registering with
  the same pair starts it over on probation. Its open waits keep their permits.
- `max_wait_secs` must be strictly below `timeout_secs` or the timeout layer answers 408
  before a wait can return. Validation enforces this.
- The memory formula reserves `max_identities * 512` bytes before sizing topics. The
  dedupe, active, and per-address buckets are bounded by `max_identities` entries each but
  not in the budget.
- Metrics instruments are created in `app()` from the global meter; `main` sets the meter
  provider first. Without `OTLP_ENDPOINT` they are no-ops.
- Tokio features are trimmed to `macros net rt-multi-thread sync time`. Add a feature if a
  new dependency needs one.
- OTLP export is HTTP/protobuf over plain http. HTTPS needs the `reqwest-rustls` feature on
  `opentelemetry-otlp`.
- `ConnectInfo<SocketAddr>` is required by the register route. `main` uses
  `into_make_service_with_connect_info`; tests insert `ConnectInfo` into request extensions.

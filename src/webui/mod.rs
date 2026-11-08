//! Web management UI and its `/webui/api/*` endpoints (SPEC §10).
//!
//! The UI is a single hand-written page served at `/` with two embedded assets,
//! `/assets/style.css` and `/assets/app.js`. There is no frontend build step:
//! the bytes live in this crate (`include_str!`) and are served straight from
//! the binary.
//!
//! # Routes
//!
//! | Route | Behaviour |
//! |---|---|
//! | `GET /` | the single page |
//! | `GET /assets/style.css`, `GET /assets/app.js` | embedded assets |
//! | `GET /webui/api/status` | live tuner/session snapshot for the Status tab |
//! | `GET/PUT /webui/api/tuners` | `tuners.yml` (SPEC §3.2) |
//! | `GET/PUT /webui/api/channels` | `channels.yml` (SPEC §3.3) |
//! | `GET/PUT /webui/api/pipelines` | `pipelines.yml` (SPEC §3.4) |
//! | `GET /webui/api/server-config` | `server.yml`, read-only (SPEC §3.1) |
//! | `POST /webui/api/restart` | re-validate, stop sessions, self-reexec |
//! | `POST /webui/api/services/scan` | manual service re-scan (`text/plain`, SPEC §8.2) |
//!
//! The JSON CRUD API stays under `/webui/api/*`; only the HTML entry point is at
//! `/`. `/api/*` is untouched: [`app`] merges the existing API router with this
//! one, so the API keeps its own routes and its 404 fallback.
//!
//! The per-file locks and the atomic write live in the `store` submodule; the
//! channel scan takes the same `channels.yml` lock, and re-reads the file inside
//! it, so a scan and a browser save cannot silently overwrite each other.
//!
//! # Validation and writes
//!
//! A `PUT` never trusts the browser: the candidate document is written into a
//! scratch directory next to copies of the other three files and loaded with
//! [`Config::load`], so exactly the startup rules (SPEC §3.6) apply — including
//! the cross-file ones (`pipelines.types` must name an existing pipeline, the
//! type-name regex, duplicate detection, `deny_unknown_fields`). Invalid input
//! is a 400 with the issue list and nothing is written.
//!
//! The write itself is `.bak` copy → `<file>.tmp` → `fsync` → `rename` (see
//! the `store` submodule), serialised by one mutex per file.
//!
//! # Optimistic concurrency
//!
//! `GET` returns an `ETag` header (FNV-1a of the file content). A `PUT` may send
//! it back as `If-Match`; a mismatch is `409 {"code":409,…}` and no write. A
//! `PUT` without `If-Match` is accepted, which keeps `curl` usable.
//!
//! # Restart
//!
//! `POST /webui/api/restart` re-loads and validates the whole configuration,
//! then replaces the process image with `execve` on Unix (after
//! [`SessionManager::shutdown`](crate::session::SessionManager::shutdown)), so
//! the listening socket is released exactly when the new image binds it. A
//! successful restart therefore drops the HTTP connection without a body; the
//! page treats that as success and polls until the server answers again. A
//! broken configuration is a 400 and nothing is stopped or replaced. If the
//! `exec` itself still fails, the manager is resumed
//! ([`SessionManager::resume`](crate::session::SessionManager::resume)) so the
//! running process keeps serving (a later request just spawns a fresh tree)
//! instead of leaving every stream at 503 until someone restarts it by hand; the
//! failure is reported as a 500.
//!
//! # Host allowlist (DNS rebinding)
//!
//! Comparing `Origin` with `Host` alone does not stop DNS rebinding: an
//! attacker's page can make `Host` be its own name, in which case the two match
//! and the same-origin check passes. Every request — `GET` and mutation, `/`,
//! `/assets/*`, `/webui/api/*` and, through [`app`], `/api/*` — therefore also
//! has to carry a `Host` whose *host part* is
//!
//! * `localhost`, or
//! * an IP literal inside `server.allowed_cidrs`, or
//! * one of `server.allowed_hosts` (exact, ASCII-case-insensitive).
//!
//! A present but disallowed `Host` is `403 {"code":403,…}`. An **absent or
//! empty** `Host` (and no URI authority) is allowed through: browsers cannot
//! issue a request without an authority (HTTP/1.1 requires `Host`, HTTP/2 uses
//! `:authority`), so skipping the guard for that case does not weaken
//! DNS-rebinding protection — only non-browser clients such as
//! BonDriver_Mirakurun send no `Host`. The rebinding check is per request, not
//! per method, so reads are covered too. A deployment behind a reverse proxy, or
//! reached by a host name, must list that name in `allowed_hosts`: the
//! peer-address guard cannot see the name the browser used.
//!
//! # CSRF hardening
//!
//! The UI has no authentication; access is limited by `server.allowed_cidrs`
//! and the `Host` allowlist above. To stop a page on another origin from
//! driving it, every mutating request (`PUT /webui/api/*`,
//! `POST /webui/api/restart`) must carry `Content-Type: application/json`, and a
//! request that sends an `Origin` header must have it match the request `Host`;
//! anything else is a `403 {"code":403,…}` and nothing is written or executed.

// Shared with the channel scan so both take the same per-file lock (SPEC §10).
pub(crate) mod store;

/// Test hook: the async per-file save lock, so an integration test can hold a
/// file while a second request is in flight (the `store` module is crate
/// private).
#[doc(hidden)]
pub use store::lock_async as store_lock_async;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::api::{with_common_layers, AppState};
use crate::config::{host_is_allowed, ChannelConfig, Config, ConfigError, ServerConfig};
use crate::session::{SessionStatus, TunerStatus};

use store::{CHANNELS_FILE, PIPELINES_FILE, SERVER_FILE, TUNERS_FILE};

/// `text/html; charset=utf-8`, the entry point's content type.
const HTML: &str = "text/html; charset=utf-8";
/// The stylesheet's content type.
const CSS: &str = "text/css; charset=utf-8";
/// The script's content type.
const JS: &str = "text/javascript; charset=utf-8";

/// The single page (SPEC §10: no frontend build step).
const INDEX_HTML: &str = include_str!("assets/index.html");
/// The page's stylesheet.
const APP_CSS: &str = include_str!("assets/style.css");
/// The page's behaviour.
const APP_JS: &str = include_str!("assets/app.js");

/// Runs the self-reexec. Hidden behind a hook so tests can observe it without
/// replacing the test process (SPEC §10).
pub type ExecHook = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// Per-process state of the webui that does not belong to [`AppState`]: the
/// restart hook. The per-file save mutexes live in the `store` submodule,
/// process-wide, so two routers cannot race on the same file either.
#[derive(Clone)]
pub struct Webui {
    exec: ExecHook,
}

impl Webui {
    /// Production state: real self-reexec.
    pub fn new() -> Self {
        Self {
            exec: Arc::new(reexec),
        }
    }

    /// Replaces the restart hook (tests).
    pub fn with_exec(hook: impl Fn() -> Result<(), String> + Send + Sync + 'static) -> Self {
        Self {
            exec: Arc::new(hook),
        }
    }
}

impl Default for Webui {
    fn default() -> Self {
        Self::new()
    }
}

/// The whole application: the mirakurun-compatible API plus the management UI
/// (SPEC §9, §10). `/api/*` keeps working exactly as before; `/` and
/// `/webui/api/*` are added.
///
/// The `Host` allowlist is applied once more around the merged router: the API
/// router is built in [`crate::api`] and cannot carry the guard itself, but
/// DNS-rebinding protection has to cover `/api/*` too (the guard is per
/// request, not per method). Applying it twice to the webui routes is harmless
/// and idempotent.
pub fn app(state: AppState) -> Router {
    let api = crate::api::router(state.clone());
    let webui = router_with(state.clone(), Webui::new());
    api.merge(webui)
        .layer(axum::middleware::from_fn_with_state(state, host_guard))
}

/// The management UI router with an explicit [`Webui`] state (the restart hook).
pub fn router_with(state: AppState, webui: Webui) -> Router {
    let guard_state = state.clone();
    let routes = Router::new()
        .route("/", get(index))
        .route("/assets/style.css", get(style_css))
        .route("/assets/app.js", get(app_js))
        .route("/webui/api/status", get(status))
        .route("/webui/api/tuners", get(get_tuners).put(put_tuners))
        .route("/webui/api/channels", get(get_channels).put(put_channels))
        .route(
            "/webui/api/pipelines",
            get(get_pipelines).put(put_pipelines),
        )
        .route("/webui/api/server-config", get(server_config))
        .route("/webui/api/services/scan", post(rescan_services))
        .route("/webui/api/restart", post(restart))
        // Mutating requests are guarded against cross-site use (the UI has no
        // authentication; `allowed_cidrs` and the Host allowlist are the only
        // other boundaries).
        .route_layer(axum::middleware::from_fn(csrf_guard))
        .layer(Extension(webui));
    with_common_layers(&state, routes).with_state(state).layer(
        axum::middleware::from_fn_with_state(guard_state, host_guard),
    )
}

// ---------------------------------------------------------------------------
// Host allowlist (DNS rebinding)
// ---------------------------------------------------------------------------

/// Rejects a request whose `Host` is present but not `localhost`, not an IP
/// inside `allowed_cidrs` and not listed in `allowed_hosts` (SPEC §3.1).
///
/// This runs for every method and every path (the static page and assets
/// included): a DNS-rebinding page reaches this server with its own name in
/// `Host`, so it is rejected even though its `Origin` matches. Since the guard
/// also wraps the merged router in [`app`], `/api/*` is covered as well. A
/// deployment reached by a name (or behind a reverse proxy that forwards the
/// browser's `Host`) must add it to `allowed_hosts`.
///
/// An **absent or empty** `Host` is accepted instead of rejected. Browsers
/// cannot send a request without an authority (HTTP/1.1 mandates `Host`,
/// HTTP/2 uses `:authority`), so this cannot let a rebinding page through; it
/// exists because native clients such as BonDriver_Mirakurun omit `Host`
/// entirely and would otherwise get a 403 on every request.
///
/// The allowlist comes from the configuration the process started with (there
/// is no `PUT` for `server.yml`, so a hand edit needs the documented restart
/// anyway); the read-only `server-config` view below does show the current
/// file.
async fn host_guard(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // HTTP/1.1 carries the authority in `Host`; HTTP/2 uses `:authority`, which
    // hyper leaves in the request URI instead of synthesising a `Host` header.
    let header_host = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    let uri_host = request
        .uri()
        .authority()
        .map(|authority| authority.as_str());
    let host = header_host.or(uri_host).unwrap_or_default();
    // Only non-browser clients (BonDriver_Mirakurun and the like) omit the
    // authority; a browser always sends `Host` (HTTP/1.1) or `:authority`
    // (HTTP/2), so letting this through does not weaken rebinding protection.
    if host.trim().is_empty() {
        tracing::debug!(
            path = %request.uri().path(),
            "request has no Host; allowing a non-browser client"
        );
        return next.run(request).await;
    }
    if !host_is_allowed(
        host,
        &state.config.server.allowed_hosts,
        &state.config.server.allowed_cidrs,
    ) {
        tracing::warn!(
            host,
            path = %request.uri().path(),
            "request Host is not in allowed_hosts / allowed_cidrs"
        );
        return error_response(
            StatusCode::FORBIDDEN,
            "Host is not allowed (see server.yml allowed_hosts)",
            Vec::new(),
        );
    }
    next.run(request).await
}

// ---------------------------------------------------------------------------
// CSRF guard
// ---------------------------------------------------------------------------

/// Guard for every mutating `/webui/api/*` request.
///
/// `GET`/`HEAD` pass through. A mutation must:
/// * carry `Content-Type: application/json` (the only content type the UI
///   sends; a cross-site HTML form cannot set it without a CORS preflight, which
///   the server never approves), and
/// * when it sends `Origin`, match the request `Host` (same-origin only).
///
/// Everything else is a 403 with the standard JSON error body.
async fn csrf_guard(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = request.method();
    if matches!(
        *method,
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    ) {
        return next.run(request).await;
    }
    let headers = request.headers();
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if !json {
        return error_response(
            StatusCode::FORBIDDEN,
            "Content-Type: application/json is required",
            Vec::new(),
        );
    }
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin.to_str().unwrap_or_default();
        // HTTP/2 carries the authority in the URI, not a `Host` header (the
        // same fallback `host_guard` uses); comparing against an empty host
        // would reject every legitimate same-origin HTTP/2 mutation.
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .or_else(|| request.uri().authority().map(|a| a.as_str()))
            .unwrap_or_default();
        // `host_guard` accepts a request with no authority, but an `Origin`
        // without any `Host` is anomalous: a browser-originated mutation always
        // carries one. Keep rejecting it rather than treating it as same-origin.
        // Host names are ASCII-case-insensitive, so `LOCALHOST:40772` and
        // `localhost:40772` are the same origin.
        if !origin_host(origin).is_some_and(|origin_host| origin_host.eq_ignore_ascii_case(host)) {
            return error_response(
                StatusCode::FORBIDDEN,
                "Cross-origin request rejected",
                Vec::new(),
            );
        }
    }
    next.run(request).await
}

/// The `host[:port]` part of an `Origin` header value, or `None` when it is not
/// a well-formed `scheme://host[:port]` origin (or is the opaque `null`).
fn origin_host(origin: &str) -> Option<&str> {
    let rest = origin.split_once("://")?.1;
    let host = rest.split('/').next().unwrap_or_default();
    if host.is_empty() {
        None
    } else {
        Some(host)
    }
}

// ---------------------------------------------------------------------------
// Static assets
// ---------------------------------------------------------------------------

async fn index() -> Response {
    static_response(HTML, INDEX_HTML)
}

async fn style_css() -> Response {
    static_response(CSS, APP_CSS)
}

async fn app_js() -> Response {
    static_response(JS, APP_JS)
}

fn static_response(content_type: &'static str, body: &'static str) -> Response {
    let mut response = Response::new(axum::body::Body::from(body));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    // The page is a single file with an inline refresh loop; a stale cached
    // page would keep talking to an old API shape after an upgrade.
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Live state for the Status tab: tuners with their users, sessions, counts.
///
/// The counts are computed per request: `services` from the live
/// [`ServiceTable`](crate::services::ServiceTable) and `channels` from the
/// current `channels.yml` on disk (falling back to the startup snapshot when the
/// file cannot be read or parsed, so a hand edit is reflected without a stale
/// zero).
async fn status(State(state): State<AppState>) -> Json<Value> {
    let tuners = state.manager.tuners_snapshot();
    let sessions = state.manager.sessions_snapshot();
    let users: usize = tuners.iter().map(|tuner| tuner.users.len()).sum();
    let dir = state.config_dir.clone();
    let channels = tokio::task::spawn_blocking(move || channels_on_disk(&dir))
        .await
        .ok()
        .flatten()
        .unwrap_or(state.config.channels.len());
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "pid": std::process::id(),
        "tuners": tuners.iter().map(tuner_json).collect::<Vec<_>>(),
        "sessions": sessions.iter().map(session_json).collect::<Vec<_>>(),
        "counts": {
            "tuners": tuners.len(),
            "sessions": sessions.len(),
            "users": users,
            "services": state.manager.services().len(),
            "channels": channels,
        },
    }))
}

fn tuner_json(tuner: &TunerStatus) -> Value {
    json!({
        "index": tuner.index,
        "name": tuner.name,
        "types": tuner.types,
        "isAvailable": tuner.is_available,
        "isFree": tuner.is_free,
        "isUsing": tuner.is_using,
        "channel": tuner
            .channel
            .as_ref()
            .map(|(type_name, channel)| json!({ "type": type_name, "channel": channel }))
            .unwrap_or(Value::Null),
        "users": tuner
            .users
            .iter()
            .map(|user| {
                json!({
                    "id": user.id,
                    "priority": user.priority,
                    "provider": user.provider,
                    "agent": user.agent,
                    "startedAt": epoch_millis(user.started_at),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn session_json(session: &SessionStatus) -> Value {
    json!({
        "id": session.id,
        "tuner": session.tuner,
        "type": session.type_name,
        "channel": session.channel,
        "phase": format!("{:?}", session.phase),
        "users": session.users,
        "pids": session.pids,
        "startedAt": epoch_millis(session.started_at),
    })
}

fn epoch_millis(time: std::time::SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Document shapes
//
// These mirror the YAML of SPEC §3.2–§3.4. They are serialised back to YAML
// verbatim, so the key names (`type`, `pipelines`, `types`, `pipeline`) have to
// stay identical to the config schema.
// ---------------------------------------------------------------------------

/// One `tuners.yml` entry (SPEC §3.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TunerDoc {
    name: String,
    types: Vec<String>,
    command: String,
    #[serde(default)]
    disabled: bool,
}

/// One `channels.yml` entry (SPEC §3.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelDoc {
    name: String,
    #[serde(rename = "type", alias = "channel_type")]
    channel_type: String,
    channel: String,
    /// Extra arguments substituted for `{extra_args}` (SPEC §3.3). The
    /// canonical `extra-args` spelling and the `extra_args` alias are both
    /// accepted; an empty value is omitted from the YAML like the scan does.
    #[serde(
        default,
        rename = "extra-args",
        alias = "extra_args",
        skip_serializing_if = "String::is_empty"
    )]
    extra_args: String,
    #[serde(default)]
    disabled: bool,
    /// Optional tuner routes (SPEC §3.3, §6.4). Empty is omitted so the UI does
    /// not add a `routes: []` to a channel that has none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    routes: Vec<ChannelRouteDoc>,
}

/// One entry of a channel's `routes` (SPEC §3.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelRouteDoc {
    tuner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    channel: Option<String>,
    /// Route-local `extra-args`; `extra_args` is accepted as an alias.
    #[serde(
        default,
        rename = "extra-args",
        alias = "extra_args",
        skip_serializing_if = "Option::is_none"
    )]
    extra_args: Option<String>,
}

/// `pipelines.yml` (SPEC §3.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PipelinesDoc {
    #[serde(default)]
    pipelines: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    types: BTreeMap<String, BindingDoc>,
}

/// A `pipelines.types` value. The UI accepts the bare pipeline name as a
/// convenience, but the object form is parsed strictly: `deny_unknown_fields`
/// stops a typo such as `{"pipelne": …}` from being silently dropped (the
/// startup schema is equally strict).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum BindingDoc {
    Name(String),
    Object(BindingObject),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingObject {
    pipeline: String,
}

impl BindingDoc {
    fn pipeline(&self) -> &str {
        match self {
            Self::Name(name) => name,
            Self::Object(BindingObject { pipeline }) => pipeline,
        }
    }
}

impl Serialize for BindingDoc {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut document = serializer.serialize_struct("PipelineBinding", 1)?;
        document.serialize_field("pipeline", self.pipeline())?;
        document.end()
    }
}

// ---------------------------------------------------------------------------
// GET documents
// ---------------------------------------------------------------------------

async fn get_tuners(State(state): State<AppState>) -> Response {
    let (text, etag) = match read_document_async(&state.config_dir, TUNERS_FILE).await {
        Ok(document) => document,
        Err(message) => return internal_error(message),
    };
    match serde_yaml_ng::from_str::<Vec<TunerDoc>>(&text) {
        Ok(tuners) => document_response(json!({ "tuners": tuners }), &etag),
        Err(error) => internal_error(format!("failed to parse {TUNERS_FILE}: {error}")),
    }
}

async fn get_channels(State(state): State<AppState>) -> Response {
    let (text, etag) = match read_document_async(&state.config_dir, CHANNELS_FILE).await {
        Ok(document) => document,
        Err(message) => return internal_error(message),
    };
    match serde_yaml_ng::from_str::<Vec<ChannelDoc>>(&text) {
        Ok(channels) => document_response(json!({ "channels": channels }), &etag),
        Err(error) => internal_error(format!("failed to parse {CHANNELS_FILE}: {error}")),
    }
}

async fn get_pipelines(State(state): State<AppState>) -> Response {
    let (text, etag) = match read_document_async(&state.config_dir, PIPELINES_FILE).await {
        Ok(document) => document,
        Err(message) => return internal_error(message),
    };
    match serde_yaml_ng::from_str::<PipelinesDoc>(&text) {
        Ok(document) => document_response(pipelines_json(&document), &etag),
        Err(error) => internal_error(format!("failed to parse {PIPELINES_FILE}: {error}")),
    }
}

/// The JSON form of `pipelines.yml`: bindings are always objects, so the UI has
/// one shape to render.
fn pipelines_json(document: &PipelinesDoc) -> Value {
    let types: BTreeMap<&str, Value> = document
        .types
        .iter()
        .map(|(type_name, binding)| {
            (
                type_name.as_str(),
                json!({
                    "pipeline": binding.pipeline(),
                }),
            )
        })
        .collect();
    json!({ "pipelines": document.pipelines, "types": types })
}

/// Reads `file` from the config directory, returning the text and its ETag.
///
/// Blocking; called from a `spawn_blocking` task by the handlers.
fn read_document(dir: &Path, file: &str) -> Result<(String, String), String> {
    let path = dir.join(file);
    let text = std::fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let etag = etag(&text);
    Ok((text, etag))
}

/// [`read_document`] on the blocking pool: even a small read can stall on a
/// network or USB filesystem.
async fn read_document_async(dir: &Path, file: &str) -> Result<(String, String), String> {
    let dir = dir.to_path_buf();
    let file = file.to_string();
    tokio::task::spawn_blocking(move || read_document(&dir, &file))
        .await
        .map_err(|error| format!("read task failed: {error}"))?
}

fn document_response(body: Value, etag: &str) -> Response {
    let mut response = Json(body).into_response();
    if let Ok(value) = HeaderValue::from_str(etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

/// FNV-1a: a short, stable content tag. It only has to detect "someone else
/// saved while this form was open", not resist an attacker.
fn etag(content: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in content.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("\"{hash:016x}\"")
}

// ---------------------------------------------------------------------------
// PUT documents
// ---------------------------------------------------------------------------

async fn put_tuners(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let tuners = match parse_list::<TunerDoc>(&body, "tuners") {
        Ok(tuners) => tuners,
        Err(reason) => return bad_request(reason),
    };
    let yaml = match to_yaml(&tuners) {
        Ok(yaml) => yaml,
        Err(reason) => return internal_error(reason),
    };
    save_document(state, headers, TUNERS_FILE, yaml).await
}

async fn put_channels(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let channels = match parse_list::<ChannelDoc>(&body, "channels") {
        Ok(channels) => channels,
        Err(reason) => return bad_request(reason),
    };
    let yaml = match to_yaml(&channels) {
        Ok(yaml) => yaml,
        Err(reason) => return internal_error(reason),
    };
    save_document(state, headers, CHANNELS_FILE, yaml).await
}

async fn put_pipelines(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let document = match parse_pipelines(&body) {
        Ok(document) => document,
        Err(reason) => return bad_request(reason),
    };
    let yaml = match to_yaml(&document) {
        Ok(yaml) => yaml,
        Err(reason) => return internal_error(reason),
    };
    save_document(state, headers, PIPELINES_FILE, yaml).await
}

/// A list document: either the bare JSON array or `{"<key>": [...]}`.
fn parse_list<T: for<'de> Deserialize<'de>>(body: &Bytes, key: &str) -> Result<Vec<T>, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|error| format!("invalid JSON body: {error}"))?;
    let list = match value {
        Value::Object(mut map) => {
            if map.len() != 1 {
                return Err(format!("expected a JSON array or {{\"{key}\": [...]}}"));
            }
            map.remove(key)
                .ok_or_else(|| format!("missing the {key:?} field"))?
        }
        other => other,
    };
    serde_json::from_value(list).map_err(|error| format!("invalid {key} document: {error}"))
}

/// The `pipelines.yml` document, sent as `{"pipelines": …, "types": …}`.
fn parse_pipelines(body: &Bytes) -> Result<PipelinesDoc, String> {
    serde_json::from_slice(body).map_err(|error| format!("invalid pipelines document: {error}"))
}

fn to_yaml<T: Serialize>(value: &T) -> Result<String, String> {
    serde_yaml_ng::to_string(value).map_err(|error| format!("failed to build YAML: {error}"))
}

/// The `.bak` + `.tmp` + `rename` save path, serialised by the per-file mutex.
///
/// The mutex is acquired asynchronously, then the guard is moved into a
/// `spawn_blocking` closure that re-checks `If-Match` and writes the file (see
/// the `store` module docs): the `fsync` cannot stall a runtime worker and no
/// guard is held across an `await` by one.
async fn save_document(
    state: AppState,
    headers: HeaderMap,
    file: &'static str,
    yaml: String,
) -> Response {
    let if_match = match headers.get(header::IF_MATCH) {
        None => None,
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.trim().to_string()),
            Err(_) => return bad_request("If-Match is not valid UTF-8"),
        },
    };
    let dir = state.config_dir.clone();
    let bytes = yaml.len();
    let tag = etag(&yaml);
    let guard = store::lock_async(file).await;
    let outcome = tokio::task::spawn_blocking(move || {
        let _guard = guard;
        if let Some(expected) = if_match {
            if expected != "*" {
                // A read failure here is a server-side problem (unreadable
                // file), not a malformed request: it must be a 500, not the
                // 400 a client error would be.
                let (_, current) = read_document(&dir, file).map_err(SaveFailure::Read)?;
                if current != expected {
                    return Err(SaveFailure::Conflict);
                }
            }
        }
        store::save(&dir, file, &yaml).map_err(SaveFailure::Save)
    })
    .await;

    match outcome {
        Ok(Ok(())) => {
            tracing::info!(file, bytes, "configuration saved");
            document_response(json!({ "status": "saved", "file": file }), &tag)
        }
        Ok(Err(SaveFailure::Read(reason))) => internal_error(reason),
        Ok(Err(SaveFailure::Conflict)) => {
            error_response(StatusCode::CONFLICT, "Configuration Changed", Vec::new())
        }
        Ok(Err(SaveFailure::Save(store::SaveError::Invalid(issues)))) => {
            tracing::warn!(file, issues = ?issues, "configuration rejected");
            error_response(StatusCode::BAD_REQUEST, "Invalid Configuration", issues)
        }
        Ok(Err(SaveFailure::Save(store::SaveError::Internal(message)))) => {
            tracing::error!(file, %message, "configuration write failed");
            internal_error(format!("failed to write {file}: {message}"))
        }
        Err(error) => internal_error(format!("save task failed: {error}")),
    }
}

/// Why the blocking save body failed.
enum SaveFailure {
    /// The `If-Match` header could not be checked (file read failed).
    Read(String),
    /// The file changed since the client read it.
    Conflict,
    /// Validation or the write itself failed.
    Save(store::SaveError),
}

// ---------------------------------------------------------------------------
// Server config (read-only)
// ---------------------------------------------------------------------------

/// `server.yml` as read-only JSON (SPEC §3.1, §10). There is deliberately no
/// `PUT`: a wrong `allowed_cidrs` would lock the operator out of the webui.
///
/// The file is re-read on every request (the webui's view is not a startup
/// snapshot): a hand edit shows up without a restart. When the file is missing
/// or unreadable the in-memory value the server started with is returned and
/// `source` says `"memory"` (with `error` describing why) instead of pretending
/// the file was read.
async fn server_config(State(state): State<AppState>) -> Response {
    let dir = state.config_dir.clone();
    let client_dir = dir.clone();
    let loaded = tokio::task::spawn_blocking(move || load_server_config(&dir)).await;
    let (server, source, error) = match loaded {
        Ok(Ok(server)) => (server, "file", None),
        Ok(Err(error)) => {
            // The full error (with the absolute path) goes to the server log
            // only; the client gets the same message with the config
            // directory stripped, so the response does not leak the host's
            // filesystem layout.
            tracing::warn!(%error, "server-config fell back to the in-memory value");
            let client_error = strip_config_dir(&error, &client_dir);
            (state.config.server.clone(), "memory", Some(client_error))
        }
        Err(error) => {
            let error = format!("failed to read server.yml: {error}");
            tracing::warn!(%error, "server-config fell back to the in-memory value");
            (state.config.server.clone(), "memory", Some(error))
        }
    };
    let mut body = json!({
        "file": SERVER_FILE,
        "addr": server.addr,
        "allowed_cidrs": server.allowed_cidrs,
        "allowed_hosts": server.allowed_hosts,
        "idle_timeout_secs": server.idle_timeout_secs,
        "subscriber_buffer_chunks": server.subscriber_buffer_chunks,
        "chunk_size_bytes": server.chunk_size_bytes,
        "read_only": true,
        // Extra, non-breaking fields: where the values came from.
        "source": source,
    });
    if let Some(error) = error {
        body["error"] = json!(error);
    }
    let mut response = Json(body).into_response();
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
    response
}

/// Removes the absolute config-directory path from an error message that is
/// about to be returned to a client, keeping only the file name
/// (`/etc/unituner/server.yml` → `server.yml`). The unmodified error is what
/// the server logs.
fn strip_config_dir(error: &str, dir: &Path) -> String {
    error.replace(&dir.display().to_string(), SERVER_FILE)
}

/// Reads and parses `server.yml` from `dir` (blocking; call from a blocking
/// task).
fn load_server_config(dir: &Path) -> Result<ServerConfig, String> {
    let path = dir.join(SERVER_FILE);
    let text = std::fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    serde_yaml_ng::from_str(&text)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

/// The number of entries in the current `channels.yml` (blocking; call from a
/// blocking task). `None` when the file cannot be read or parsed, so the caller
/// can fall back to the startup snapshot.
///
/// The Status tab polls this endpoint, and re-parsing (and fully validating)
/// the whole document on every poll is pure waste when the file has not
/// changed: the parsed count is cached behind a content hash (the same
/// FNV-1a [`etag`] the save path uses) and only recomputed after a hand edit
/// or a webui save changes the content. Reading the small file on every poll
/// is cheap; the YAML parse is the part being skipped.
///
/// A content hash replaces the previous `(mtime, size)` stamp, which could
/// serve a stale count after a same-size rewrite inside one coarse mtime
/// tick (FAT/exFAT, a stale NFS attribute cache, `cp -p`). The hash is
/// recomputed from the bytes just read, so a rewrite is always seen. The
/// count is still a display value on the Status tab only; the file itself is
/// re-read by every code path that writes or validates configuration.
fn channels_on_disk(dir: &Path) -> Option<usize> {
    use std::sync::Mutex;

    static CACHE: Mutex<Option<(std::path::PathBuf, String, usize)>> = Mutex::new(None);

    let path = dir.join(CHANNELS_FILE);
    let text = std::fs::read_to_string(&path).ok()?;
    let stamp = etag(&text);

    let cached = CACHE.lock().ok().and_then(|guard| guard.clone());
    if let Some((cached_path, cached_stamp, count)) = cached {
        if cached_path == path && cached_stamp == stamp {
            return Some(count);
        }
    }
    let count = serde_yaml_ng::from_str::<Vec<ChannelConfig>>(&text)
        .ok()
        .map(|channels| channels.len())?;
    if let Ok(mut guard) = CACHE.lock() {
        *guard = Some((path, stamp, count));
    }
    Some(count)
}

// ---------------------------------------------------------------------------
// Service re-scan (SPEC §8.2)
// ---------------------------------------------------------------------------

/// `POST /webui/api/services/scan`: one manual service re-scan.
///
/// Distinct from `PUT /api/config/channels/scan`, which discovers and rewrites
/// `channels.yml`. This endpoint never writes the configuration; it re-reads
/// every enabled channel once, drops records of channels that are no longer
/// enabled, and rewrites `services.json`. Progress is the same `text/plain`
/// stream the discovery scan uses, and the shared [`crate::scan::ScanLock`]
/// makes a concurrent request a 409 `Already Scanning`.
async fn rescan_services(State(state): State<AppState>) -> Response {
    let scan_lease = match state.scan_lock.acquire() {
        Some(lease) => lease,
        None => return error_response(StatusCode::CONFLICT, "Already Scanning", Vec::new()),
    };

    let (tx, rx) = mpsc::channel::<String>(64);
    let manager = state.manager.clone();
    let config = Arc::clone(&state.config);
    let config_dir = state.config_dir.clone();
    let shutdown = state.shutdown_watch();
    tokio::spawn(async move {
        tracing::info!("service re-scan started");
        let result = crate::scan::rescan_services(
            &manager,
            &config,
            &config_dir,
            tx.clone(),
            Some(shutdown),
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "service re-scan failed");
            let _ = tx.send(format!("error: {error}\n")).await;
        }
        // Releasing the lease is what makes the next scan possible.
        drop(scan_lease);
    });

    let stream = ReceiverStream::new(rx).map(|line| Ok::<Bytes, Infallible>(Bytes::from(line)));
    let mut response = Response::new(Body::from_stream(stream));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

/// Re-validates the whole configuration from disk and self-reexecs (SPEC §10).
///
/// A broken configuration is a 400 and no process is replaced, so a bad save
/// cannot put the server into a restart loop. The executable is checked before
/// the sessions are stopped, because a failed exec after the shutdown would
/// leave a running server whose streams all 503.
///
/// If `exec` still fails, the manager is resumed
/// ([`SessionManager::resume`](crate::session::SessionManager::resume)) so the
/// process keeps serving — the next request spawns a fresh tree instead of every
/// stream answering 503 until a manual restart. The failure is reported as a 500.
async fn restart(State(state): State<AppState>, Extension(webui): Extension<Webui>) -> Response {
    let dir = state.config_dir.clone();
    let loaded = tokio::task::spawn_blocking(move || Config::load(&dir)).await;
    match loaded {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(%error, "restart refused: configuration is invalid");
            return config_error(response_issues(&error));
        }
        Err(error) => return internal_error(format!("config load task failed: {error}")),
    }
    if let Err(message) = preflight_exec() {
        tracing::error!(%message, "restart refused");
        return internal_error(message);
    }
    // A running scan holds the channels.yml lock and is mid read-merge-write;
    // exec'ing underneath it would kill it mid-save. *Take* the same lease the
    // scan endpoint uses (an `is_running()` check would be a TOCTOU: a scan
    // could start between the check and the exec) and hold it across the
    // shutdown and the exec. A start-up scan does not take this lease, so it is
    // covered only by the fact that it stops writing before the exec tears the
    // process down.
    let _scan_lease = match state.scan_lock.acquire() {
        Some(lease) => lease,
        None => {
            tracing::warn!("restart refused: channel scan in progress");
            return error_response(StatusCode::CONFLICT, "Already Scanning", Vec::new());
        }
    };
    // Also serialise against a webui `channels.yml` save that is mid
    // validate-and-write (it holds the store lock around its `fsync`).
    let _channels_guard = store::lock_async(store::CHANNELS_FILE).await;

    tracing::info!("restart requested: stopping all tuner sessions");
    state.manager.shutdown().await;
    tracing::info!("all tuner sessions stopped; re-executing");

    match (webui.exec)() {
        Ok(()) => Json(json!({ "status": "restarted" })).into_response(),
        Err(message) => {
            // The process image was not replaced: undo the shutdown so this
            // process still serves streams instead of 503ing for ever.
            state.manager.resume();
            tracing::error!(%message, "restart failed; sessions resumed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, message, Vec::new())
        }
    }
}

/// Checks that a new image can plausibly be started, so a doomed restart is
/// rejected before the sessions are torn down.
fn preflight_exec() -> Result<(), String> {
    let exe = std::env::current_exe()
        .map_err(|error| format!("failed to locate the current executable: {error}"))?;
    if !exe.is_file() {
        return Err(format!("{} is not a file", exe.display()));
    }
    Ok(())
}

/// The production restart hook: replace this process with a fresh one that
/// re-reads the configuration from disk (SPEC §10).
///
/// On Unix this is `execve` through [`std::os::unix::process::CommandExt::exec`],
/// so the listening socket is released exactly when the new image starts — a
/// `spawn` + `exit` pair would race the child's `bind` against the parent's
/// socket teardown. Stdio is inherited, so the log stream stays attached to the
/// terminal or the systemd journal.
fn reexec() -> Result<(), String> {
    let exe = std::env::current_exe()
        .map_err(|error| format!("failed to locate the current executable: {error}"))?;
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    tracing::info!(exe = %exe.display(), ?args, "re-executing");

    let mut command = std::process::Command::new(&exe);
    command.args(&args);

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Only returns on failure.
        let error = command.exec();
        Err(format!("failed to exec {}: {error}", exe.display()))
    }

    #[cfg(not(unix))]
    {
        let status = command
            .status()
            .map_err(|error| format!("failed to exec {}: {error}", exe.display()))?;
        std::process::exit(status.code().unwrap_or(1));
    }
}

// ---------------------------------------------------------------------------
// Error helpers (the SPEC §9 `{code,reason,errors:[]}` shape)
// ---------------------------------------------------------------------------

fn response_issues(error: &ConfigError) -> Vec<String> {
    match error {
        ConfigError::Invalid(issues) => issues.iter().map(|issue| issue.to_string()).collect(),
        other => vec![other.to_string()],
    }
}

/// A configuration problem found when re-reading the files at restart time.
fn config_error(issues: Vec<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, "Invalid Configuration", issues)
}

fn bad_request(reason: impl Into<String>) -> Response {
    error_response(StatusCode::BAD_REQUEST, reason, Vec::new())
}

fn internal_error(reason: impl Into<String>) -> Response {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, reason, Vec::new())
}

fn error_response(status: StatusCode, reason: impl Into<String>, errors: Vec<String>) -> Response {
    let body = json!({
        "code": status.as_u16(),
        "reason": reason.into(),
        "errors": errors,
    });
    (status, Json(body)).into_response()
}

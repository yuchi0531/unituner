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
//! # CSRF hardening
//!
//! The UI has no authentication; access is limited by `server.allowed_cidrs`.
//! To stop a page on another origin from driving it, every mutating request
//! (`PUT /webui/api/*`, `POST /webui/api/restart`) must carry
//! `Content-Type: application/json`, and a request that sends an `Origin`
//! header must have it match the request `Host`; anything else is a
//! `403 {"code":403,…}` and nothing is written or executed.

// Shared with the channel scan so both take the same per-file lock (SPEC §10).
pub(crate) mod store;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::{with_common_layers, AppState};
use crate::config::{Config, ConfigError};
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
pub fn app(state: AppState) -> Router {
    crate::api::router(state.clone()).merge(router_with(state, Webui::new()))
}

/// The management UI router with an explicit [`Webui`] state (the restart hook).
pub fn router_with(state: AppState, webui: Webui) -> Router {
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
        .route("/webui/api/restart", post(restart))
        // Mutating requests are guarded against cross-site use (the UI has no
        // authentication; `allowed_cidrs` is the only other boundary).
        .route_layer(axum::middleware::from_fn(csrf_guard))
        .layer(Extension(webui));
    with_common_layers(&state, routes).with_state(state)
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
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if origin_host(origin) != Some(host) {
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
async fn status(State(state): State<AppState>) -> Json<Value> {
    let tuners = state.manager.tuners_snapshot();
    let sessions = state.manager.sessions_snapshot();
    let users: usize = tuners.iter().map(|tuner| tuner.users.len()).sum();
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
            "channels": state.config.channels.len(),
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
    #[serde(default)]
    disabled: bool,
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

/// A `pipelines.types` value: `{ pipeline: name }` in YAML, but the UI may send
/// the bare pipeline name as a convenience. It is always written as the object
/// form, because that is the schema `config.rs` parses.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum BindingDoc {
    Name(String),
    Object { pipeline: String },
}

impl BindingDoc {
    fn pipeline(&self) -> &str {
        match self {
            Self::Name(name) => name,
            Self::Object { pipeline } => pipeline,
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
    let (text, etag) = match read_document(&state, TUNERS_FILE) {
        Ok(document) => document,
        Err(message) => return internal_error(message),
    };
    match serde_yaml_ng::from_str::<Vec<TunerDoc>>(&text) {
        Ok(tuners) => document_response(json!({ "tuners": tuners }), &etag),
        Err(error) => internal_error(format!("failed to parse {TUNERS_FILE}: {error}")),
    }
}

async fn get_channels(State(state): State<AppState>) -> Response {
    let (text, etag) = match read_document(&state, CHANNELS_FILE) {
        Ok(document) => document,
        Err(message) => return internal_error(message),
    };
    match serde_yaml_ng::from_str::<Vec<ChannelDoc>>(&text) {
        Ok(channels) => document_response(json!({ "channels": channels }), &etag),
        Err(error) => internal_error(format!("failed to parse {CHANNELS_FILE}: {error}")),
    }
}

async fn get_pipelines(State(state): State<AppState>) -> Response {
    let (text, etag) = match read_document(&state, PIPELINES_FILE) {
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
                json!({ "pipeline": binding.pipeline() }),
            )
        })
        .collect();
    json!({ "pipelines": document.pipelines, "types": types })
}

/// Reads `file` from the config directory, returning the text and its ETag.
fn read_document(state: &AppState, file: &str) -> Result<(String, String), String> {
    let path = state.config_dir.join(file);
    let text = std::fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    let etag = etag(&text);
    Ok((text, etag))
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
    save_document(&state, &headers, TUNERS_FILE, &yaml)
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
    save_document(&state, &headers, CHANNELS_FILE, &yaml)
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
    save_document(&state, &headers, PIPELINES_FILE, &yaml)
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

/// The `.bak` + `.tmp` + `rename` save path, guarded by the per-file mutex.
fn save_document(
    state: &AppState,
    headers: &HeaderMap,
    file: &'static str,
    yaml: &str,
) -> Response {
    let _guard = store::lock(file);
    if let Err(rejection) = check_if_match(state, headers, file) {
        return match rejection {
            IfMatchError::BadRequest(reason) => bad_request(reason),
            IfMatchError::Conflict => {
                error_response(StatusCode::CONFLICT, "Configuration Changed", Vec::new())
            }
        };
    }
    match store::save(&state.config_dir, file, yaml) {
        Ok(()) => {
            tracing::info!(file, bytes = yaml.len(), "configuration saved");
            let etag = etag(yaml);
            document_response(json!({ "status": "saved", "file": file }), &etag)
        }
        Err(store::SaveError::Invalid(issues)) => {
            tracing::warn!(file, issues = ?issues, "configuration rejected");
            error_response(StatusCode::BAD_REQUEST, "Invalid Configuration", issues)
        }
        Err(store::SaveError::Internal(message)) => {
            tracing::error!(file, %message, "configuration write failed");
            internal_error(format!("failed to write {file}: {message}"))
        }
    }
}

/// Why an `If-Match` precondition failed.
enum IfMatchError {
    /// The header is not a valid string.
    BadRequest(String),
    /// The file changed since the client read it.
    Conflict,
}

/// Optional optimistic concurrency: a stale `If-Match` is a 409 (SPEC §10).
fn check_if_match(state: &AppState, headers: &HeaderMap, file: &str) -> Result<(), IfMatchError> {
    let Some(expected) = headers.get(header::IF_MATCH) else {
        return Ok(());
    };
    let expected = expected
        .to_str()
        .map_err(|_| IfMatchError::BadRequest("If-Match is not valid UTF-8".to_string()))?;
    let expected = expected.trim();
    if expected == "*" {
        return Ok(());
    }
    let (_, current) = read_document(state, file).map_err(IfMatchError::BadRequest)?;
    if current == expected {
        Ok(())
    } else {
        Err(IfMatchError::Conflict)
    }
}

// ---------------------------------------------------------------------------
// Server config (read-only)
// ---------------------------------------------------------------------------

/// `server.yml` as read-only JSON (SPEC §3.1, §10). There is deliberately no
/// `PUT`: a wrong `allowed_cidrs` would lock the operator out of the webui.
async fn server_config(State(state): State<AppState>) -> Response {
    let server = &state.config.server;
    let mut response = Json(json!({
        "file": SERVER_FILE,
        "addr": server.addr,
        "allowed_cidrs": server.allowed_cidrs,
        "idle_timeout_secs": server.idle_timeout_secs,
        "subscriber_buffer_chunks": server.subscriber_buffer_chunks,
        "chunk_size_bytes": server.chunk_size_bytes,
        "read_only": true,
    }))
    .into_response();
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
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
    if let Err(error) = Config::load(&state.config_dir) {
        tracing::warn!(%error, "restart refused: configuration is invalid");
        return config_error(response_issues(&error));
    }
    if let Err(message) = preflight_exec() {
        tracing::error!(%message, "restart refused");
        return internal_error(message);
    }

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

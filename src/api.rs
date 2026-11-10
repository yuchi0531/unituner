//! axum routes for the mirakurun-compatible API subset (SPEC §9).
//!
//! # Endpoints
//!
//! | Route | Behaviour |
//! |---|---|
//! | `GET /api/version` | `{"current":…,"latest":…}`, both the unituner version |
//! | `GET /api/status` | minimal mirakurun-shaped status (no EPG) |
//! | `GET /api/channels` | every enabled configured channel, services embedded |
//! | `GET /api/channels/{type}` | the same, filtered by channel type |
//! | `GET /api/channels/{type}/{channel}` | one enabled channel |
//! | `GET /api/channels/{type}/{channel}/stream` | post-pipeline TS, unfiltered |
//! | `GET /api/channels/{type}/{channel}/services/{id}/stream` | one service |
//! | `GET /api/services` / `{id}` | the service table |
//! | `GET /api/services/{id}/stream` | one service, `{id}` = item id or bare sid |
//! | `GET /api/tuners` / `{index}` | tuner state and users |
//! | `GET /api/programs` | always `[]` (no EPG, SPEC §1) |
//! | `GET /api/programs/{id}` | 404 |
//! | `GET /api/iptv/playlist` / `HEAD` | mirakurun-compatible M3U (no EPG) |
//! | `PUT /api/config/channels/scan` | channel discovery (SPEC §8.1) |
//! | `GET /api/config/channels/scan` | 404 (`async` is not implemented) |
//!
//! # Stream plumbing
//!
//! Every stream route acquires a [`Subscription`] from
//! [`SessionManager`](crate::session::SessionManager) and turns its receiver
//! into an HTTP body. `decode=0` asks the session layer to bypass the whole
//! pipeline (SPEC §4.1); a service stream wraps the subscription in a
//! per-subscriber [`ServiceFilter`] (SPEC §4, §5) so subscribers never
//! interfere.
//!
//! Every tuner-backed stream route resolves `(type, channel)` against
//! `channels.yml` before touching a tuner, so the configured list is the
//! allowlist and the URL's `{channel}` is not an arbitrary tuner selector. An
//! unknown pair and a `disabled: true` entry are both a **404** `Channel Not
//! Found`: a disabled channel is treated as *not present* rather than
//! forbidden, because the URL is not a stream endpoint at all (and that keeps
//! the response shape identical to a typo). The channel list/detail routes use
//! the same rule: a disabled entry is omitted from `/api/channels[/{type}]` and
//! is a 404 from `/api/channels/{type}/{channel}`.
//! `/api/services/{id}/stream` checks the channel the service table resolves to
//! the same way.
//!
//! The validation order is fixed for `GET` and `HEAD`:
//!
//! 1. path syntax ([`validate_path`]): 400;
//! 2. channel allowlist (`channels.yml`, enabled): 404;
//! 3. service resolution: 404;
//! 4. header parse (priority): 400;
//! 5. pinned tuner existence / type support: 400;
//! 6. tuner scheduling and resource errors: 409/429/503.
//!
//! `/api/services/{id}/stream` has no `(type, channel)` in its path, so it must
//! resolve the service first to learn one; step 3 runs before step 2 there (an
//! unknown service and a service bound to an unknown/disabled channel are both
//! a 404 anyway).
//!
//! `HEAD` is served by explicit handlers that return the stream headers and an
//! empty body without acquiring a session, so a probe never tunes a device. A
//! `HEAD` runs steps 1–5 (via the same `validate_*` functions as its `GET`), so
//! both answer the same 400/404. Steps 6 need a session and are not simulated:
//! `HEAD` reports only statically decidable errors, so a request that `GET`
//! would refuse with 503 (all tuners busy) or 409 (decode-mode conflict) can
//! still answer 200, but a static error is never a 200.
//!
//! A stream request whose `decode` value differs from the mode the channel is
//! already streaming with is a **409** `{"code":409,…}`: one process tree per
//! channel (SPEC §4, §6.2), so it can neither join nor preempt that session.
//!
//! A request that would exceed a SPEC §7 subscriber / session cap is a **429**
//! `{"code":429,"reason":"Too Many Requests"}`. The cap is enforced in
//! [`SessionManager`](crate::session::SessionManager) under the registry lock,
//! using the socket peer ([`ConnectInfo`]) for the per-IP bucket; a header is
//! never trusted for that.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use axum::body::Body;
use axum::extract::{ConnectInfo, Path as UrlPath, Query, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use bytes::Bytes;
use ipnet::IpNet;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::config::{
    normalize_host, validate_channel_value, validate_type_name, ChannelConfig, Config,
};
use crate::scan::{self, ScanLock, ScanQuery};
use crate::services::{ServiceRecord, ServiceTable};
use crate::session::{
    AcquireError, SessionManager, SessionTap, Subscription, TunerStatus, TunerUser,
};
use crate::tsfilter::ServiceFilter;

/// `unituner/<version>`, sent as the `Server` header on every response (SPEC §2).
const SERVER_ID: &str = concat!("unituner/", env!("CARGO_PKG_VERSION"));

/// The unituner version reported by `/api/version` and `/api/status`.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// `X-Mirakurun-Tuner-User-ID`, the mirakurun wire identifier (SPEC §2).
const TUNER_USER_ID: &str = "x-mirakurun-tuner-user-id";

/// `VIDEO/MP2T` as sent by mirakurun; the case is preserved verbatim.
const TS_CONTENT_TYPE: &str = "video/MP2T";

/// mirakurun's IPTV playlist content type (SPEC §9).
const IPTV_CONTENT_TYPE: &str = "application/x-mpegURL; charset=utf-8";

/// Shared state of the HTTP layer.
#[derive(Clone)]
pub struct AppState {
    /// Sessions, tuner state and the service table.
    pub manager: SessionManager,
    /// The configuration the server was started with.
    pub config: Arc<Config>,
    /// Directory `channels.yml` is written to by the scan API.
    pub config_dir: PathBuf,
    /// Guards against concurrent scans (SPEC §8.1).
    pub scan_lock: ScanLock,
    /// Fires when the process is shutting down; a running scan observes it so
    /// shutdown is not held for `timeout × channels`.
    shutdown: watch::Sender<bool>,
    allowed: Arc<Vec<IpNet>>,
}

impl AppState {
    /// Builds the HTTP state for `config`, using `services` as the initial
    /// service table (empty until a scan fills it).
    pub fn new(config: Arc<Config>, services: ServiceTable, config_dir: PathBuf) -> Self {
        let allowed = config
            .server
            .allowed_cidrs
            .iter()
            .filter_map(|cidr| cidr.parse::<IpNet>().ok())
            .collect();
        let (shutdown, _) = watch::channel(false);
        Self {
            manager: SessionManager::new(Arc::clone(&config), services),
            config,
            config_dir,
            scan_lock: ScanLock::default(),
            shutdown,
            allowed: Arc::new(allowed),
        }
    }

    /// A receiver for the process-wide shutdown signal.
    pub fn shutdown_watch(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Tells a running scan to stop before the caller tears the manager down.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Whether `ip` is inside `server.allowed_cidrs` (SPEC §3.1). An empty list
    /// denies everything, which is the safe reading of "no allowed network".
    ///
    /// The original peer address is matched first, then the IPv4 forms of it:
    /// an IPv4-mapped IPv6 peer (`::ffff:127.0.0.1`, which is what a dual-stack
    /// listener reports) is matched as its IPv4 address so the RFC1918 defaults
    /// keep working, and the IPv6 loopback `::1` is also matched as
    /// `127.0.0.1` so a v6 bind does not 403 a local probe when the list only
    /// carries the IPv4 loopback. Matching the original address first means an
    /// explicit `::1/128` (or `::ffff:0:0/96`) entry works too. Deny-by-default
    /// is unchanged: no candidate form inside any CIDR is still a 403.
    fn allows(&self, ip: IpAddr) -> bool {
        // At most two spellings of one address; a stack array avoids a heap
        // allocation on every request for what is a pure check.
        let mut candidates = [ip, ip, ip];
        let mut count = 1usize;
        match ip {
            IpAddr::V6(v6) => {
                if v6.is_loopback() {
                    candidates[count] = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
                    count += 1;
                }
                if let Some(v4) = v6.to_ipv4_mapped() {
                    candidates[count] = IpAddr::V4(v4);
                    count += 1;
                }
            }
            IpAddr::V4(v4) => {
                // A v4 peer may arrive from a v4-mapped listener, so accept an
                // explicit v4-mapped CIDR as well.
                candidates[count] = IpAddr::V6(v4.to_ipv6_mapped());
                count += 1;
            }
        }
        candidates[..count]
            .iter()
            .any(|candidate| self.allowed.iter().any(|net| net.contains(candidate)))
    }
}

/// Builds the API router. The caller serves it with
/// `into_make_service_with_connect_info::<SocketAddr>()` so the peer address is
/// available for `allowed_cidrs` and `X-Mirakurun-Tuner-User-ID`.
pub fn router(state: AppState) -> Router {
    let routes = api_routes().fallback(not_found);
    with_common_layers(&state, routes).with_state(state)
}

/// Applies the layers every unituner route needs, so another router (the webui
/// one, SPEC §3.1 "webui も同じ") can share them:
///
/// * the `allowed_cidrs` peer guard (403 JSON);
/// * the `Server: unituner/<version>` header and the debug request log.
pub fn with_common_layers(state: &AppState, router: Router<AppState>) -> Router<AppState> {
    router
        .layer(middleware::from_fn_with_state(state.clone(), network_guard))
        .layer(middleware::from_fn(server_identity))
}

/// The API routes, before the shared middleware is applied.
fn api_routes() -> Router<AppState> {
    Router::new()
        .route("/api/version", get(version))
        .route("/api/status", get(status))
        .route("/api/channels", get(channels))
        .route("/api/channels/{channel_type}", get(channels_by_type))
        .route(
            "/api/channels/{channel_type}/{channel}",
            get(channel_detail),
        )
        .route(
            "/api/channels/{channel_type}/{channel}/stream",
            get(channel_stream).head(stream_head),
        )
        .route(
            "/api/channels/{channel_type}/{channel}/services/{service}/stream",
            get(channel_service_stream).head(channel_service_head),
        )
        .route("/api/services", get(services_list))
        .route("/api/services/{service}", get(service_detail))
        .route(
            "/api/services/{service}/stream",
            get(service_stream).head(service_head),
        )
        .route("/api/tuners", get(tuners))
        .route("/api/tuners/{index}", get(tuner))
        .route("/api/programs", get(programs))
        .route("/api/programs/{program}", get(program_not_found))
        .route(
            "/api/iptv/playlist",
            get(iptv_playlist).head(iptv_playlist_head),
        )
        // `async` is not implemented (SPEC §8.1): a GET must be a 404, not a
        // 405, because mirakurun clients poll this path.
        .route(
            "/api/config/channels/scan",
            put(scan_channels).get(scan_not_found),
        )
}

// ---------------------------------------------------------------------------
// Metadata endpoints
// ---------------------------------------------------------------------------

async fn version() -> Json<Value> {
    Json(json!({ "current": VERSION, "latest": VERSION }))
}

async fn status(State(state): State<AppState>) -> Json<Value> {
    let tuners = state.manager.tuners_snapshot();
    let sessions = state.manager.sessions_snapshot();
    let tuners_json: Vec<Value> = tuners.iter().map(tuner_json).collect();
    Json(json!({
        "version": VERSION,
        "process": {
            "pid": std::process::id(),
            "arch": std::env::consts::ARCH,
            "platform": std::env::consts::OS,
            "versions": { "unituner": VERSION },
        },
        // EPG is out of scope (SPEC §1); the fields stay for shape
        // compatibility and are always empty.
        "epg": {
            "lastScanned": "",
            "lastScanning": "",
            "isScanning": false,
            "errorCount": 0,
        },
        "streamCount": {
            "total": sessions.len(),
            "tv": sessions.len(),
            "radio": 0,
            "encoding": 0,
        },
        "streamInfos": [],
        "tunerCount": tuners.len(),
        "tuners": tuners_json,
        "time": "",
        "epgReady": true,
        "scanReady": true,
    }))
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

/// Every **enabled** channel from `channels.yml` (SPEC §9). `disabled: true`
/// entries stay in the configuration (and in the webui round-trip) but are
/// omitted here: a disabled channel is not a resource the API serves, and the
/// detail/stream routes answer 404 for it.
async fn channels(State(state): State<AppState>) -> Json<Value> {
    // One table snapshot for the whole response: the previous shape called
    // `ServiceTable::list()` once per configured channel, i.e. N full
    // clone+sort passes over the table for N channels.
    let records = state.manager.services().list();
    Json(Value::Array(
        state
            .config
            .channels
            .iter()
            .filter(|channel| is_enabled(channel))
            .map(|channel| channel_json(&records, channel))
            .collect(),
    ))
}

async fn channels_by_type(
    State(state): State<AppState>,
    UrlPath(channel_type): UrlPath<String>,
) -> Response {
    if let Err(reason) = validate_type_name(&channel_type) {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }
    let records = state.manager.services().list();
    let list: Vec<Value> = state
        .config
        .channels
        .iter()
        .filter(|channel| is_enabled(channel) && channel.channel_type == channel_type)
        .map(|channel| channel_json(&records, channel))
        .collect();
    Json(Value::Array(list)).into_response()
}

async fn channel_detail(
    State(state): State<AppState>,
    UrlPath((channel_type, channel)): UrlPath<(String, String)>,
) -> Response {
    if let Err(reason) = validate_path(&channel_type, &channel) {
        return error_response(StatusCode::BAD_REQUEST, reason);
    }
    match require_channel(&state.config, &channel_type, &channel) {
        // Only this channel's records are cloned/sorted, not the whole table.
        Ok(entry) => Json(channel_json(
            &state
                .manager
                .services()
                .list_by_channel(&channel_type, &channel),
            entry,
        ))
        .into_response(),
        Err(response) => response,
    }
}

/// The channel's JSON, with its services embedded as a short list (SPEC §9).
/// `records` is one already-sorted snapshot; filtering it in place keeps the
/// per-channel cost at a scan with no clone or sort.
fn channel_json(records: &[ServiceRecord], channel: &ChannelConfig) -> Value {
    let embedded: Vec<Value> = records
        .iter()
        .filter(|record| {
            record
                .channel
                .as_ref()
                .is_some_and(|(t, c)| t == &channel.channel_type && c == &channel.channel)
        })
        .map(|record| {
            json!({
                "id": record.item_id().0,
                "serviceId": record.service_id,
                "networkId": record.network_id,
                "name": record.name,
                "type": record.service_type,
            })
        })
        .collect();
    json!({
        "type": channel.channel_type,
        "channel": channel.channel,
        "name": channel.name,
        "services": embedded,
    })
}

/// A `channels.yml` entry is API-visible and tuneable only when enabled. Single
/// authority for the list, detail and stream routes: a disabled entry is
/// omitted from the lists and treated as absent (404) everywhere else.
fn is_enabled(channel: &ChannelConfig) -> bool {
    !channel.disabled
}

/// The configured entry for `(channel_type, channel)`.
fn find_channel<'a>(
    config: &'a Config,
    channel_type: &str,
    channel: &str,
) -> Option<&'a ChannelConfig> {
    config
        .channels
        .iter()
        .find(|entry| entry.channel_type == channel_type && entry.channel == channel)
}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/// The service table as mirakurun clients consume it.
///
/// The same `(nid, sid)` can be received on two physical channels, which would
/// otherwise repeat one `id` in the list. Clients key on `id`, so each id is
/// emitted once: the first record in the stable table order (network, tsid,
/// service id, then channel) wins and later duplicates are skipped. That is
/// also the record `/api/services/{id}` returns, so list and detail agree.
async fn services_list(State(state): State<AppState>) -> Json<Value> {
    let services = state.manager.services();
    let mut seen = HashSet::new();
    Json(Value::Array(
        services
            .list()
            .iter()
            .filter(|record| seen.insert(record.item_id()))
            .map(service_json)
            .collect(),
    ))
}

async fn service_detail(
    State(state): State<AppState>,
    UrlPath(service): UrlPath<String>,
) -> Response {
    match state.manager.services().resolve(&service) {
        Ok(record) => Json(service_json(&record)).into_response(),
        Err(error) => {
            tracing::debug!(%error, id = %service, "service not found");
            error_response(StatusCode::NOT_FOUND, "Service Not Found")
        }
    }
}

/// The full mirakurun-shaped service item (SPEC §9).
fn service_json(record: &ServiceRecord) -> Value {
    json!({
        "id": record.item_id().0,
        "serviceId": record.service_id,
        "networkId": record.network_id,
        "tsid": record.tsid,
        "name": record.name,
        "type": record.service_type,
        "logoId": 0,
        "hasLogoData": record.has_logo_data,
        // mirakurun always carries the field; 0 means "unknown".
        "remoteControlKeyId": record.remote_control_key_id.unwrap_or(0),
        "channel": record
            .channel
            .as_ref()
            .map(|(channel_type, channel)| json!({ "type": channel_type, "channel": channel }))
            .unwrap_or(Value::Null),
    })
}

// ---------------------------------------------------------------------------
// Tuners
// ---------------------------------------------------------------------------

async fn tuners(State(state): State<AppState>) -> Json<Value> {
    Json(Value::Array(
        state
            .manager
            .tuners_snapshot()
            .iter()
            .map(tuner_json)
            .collect(),
    ))
}

async fn tuner(State(state): State<AppState>, UrlPath(index): UrlPath<String>) -> Response {
    let Ok(index) = index.parse::<usize>() else {
        return error_response(StatusCode::NOT_FOUND, "Tuner Not Found");
    };
    match state
        .manager
        .tuners_snapshot()
        .into_iter()
        .find(|tuner| tuner.index == index)
    {
        Some(tuner) => Json(tuner_json(&tuner)).into_response(),
        None => error_response(StatusCode::NOT_FOUND, "Tuner Not Found"),
    }
}

fn tuner_json(tuner: &TunerStatus) -> Value {
    json!({
        "index": tuner.index,
        "name": tuner.name,
        "types": tuner.types,
        "isAvailable": tuner.is_available,
        "isFree": tuner.is_free,
        "isUsing": tuner.is_using,
        "isFault": tuner.is_fault,
        "users": tuner
            .users
            .iter()
            .map(|user| {
                json!({
                    "date": user
                        .started_at
                        .duration_since(UNIX_EPOCH)
                        .map(|elapsed| elapsed.as_millis() as u64)
                        .unwrap_or(0),
                    "id": user.id,
                    "priority": user.priority,
                    "provider": user.provider,
                    "agent": user.agent,
                })
            })
            .collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// Programs (not implemented)
// ---------------------------------------------------------------------------

async fn programs() -> Json<Value> {
    Json(Value::Array(Vec::new()))
}

async fn program_not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "Program Not Found")
}

// ---------------------------------------------------------------------------
// IPTV playlist (no EPG)
// ---------------------------------------------------------------------------

/// mirakurun's IPTV playlist, minus `url-tvg` because unituner serves no EPG
/// (SPEC §9). Only service types `0x01` (TV) and `0xAD` (BS4K/180) are listed.
async fn iptv_playlist(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let api_root = format!("http://{}/api", playlist_authority(&state, &headers));
    let records = state.manager.services().list();
    playlist_response(Body::from(iptv_playlist_body(
        &api_root,
        &records,
        &state.config,
    )))
}

/// `HEAD /api/iptv/playlist`: the GET response headers, without a body.
async fn iptv_playlist_head() -> Response {
    playlist_response(Body::empty())
}

fn playlist_response(body: Body) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(IPTV_CONTENT_TYPE),
    );
    response
}

/// The request's authority, or the configured listen address when the client
/// omitted `Host` (native clients do that; SPEC §3.1).
///
/// The raw `Host` is never trusted verbatim: it is normalized with the same
/// [`normalize_host`] the webui host guard uses, and an invalid value falls
/// back to the configured listen address.
fn playlist_authority(state: &AppState, headers: &HeaderMap) -> String {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(normalize_authority)
        .unwrap_or_else(|| fallback_authority(&state.config.server.addr))
}

/// A `Host` header as a URL authority, or `None` when it is not a valid host.
///
/// Validates with [`normalize_host`] (same normalization as the webui host
/// guard), then re-attaches the port and IPv6 brackets `normalize_host`
/// strips, so `EXAMPLE.LAN:40772` -> `example.lan:40772` and
/// `[::1]:40772` -> `[::1]:40772`.
fn normalize_authority(value: &str) -> Option<String> {
    let host = normalize_host(value)?;
    let trimmed = value.trim();
    let port = if let Some(rest) = trimmed.strip_prefix('[') {
        rest.split_once(']')?.1.strip_prefix(':')
    } else if trimmed.matches(':').count() == 1 {
        trimmed.split_once(':').map(|(_, port)| port)
    } else {
        None
    };
    let authority = if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    };
    Some(match port {
        Some(port) => format!("{authority}:{port}"),
        None => authority,
    })
}

/// `server.addr` as `<host>:<port>`, with a wildcard bind pointing at loopback.
fn fallback_authority(addr: &str) -> String {
    match bind_address(addr) {
        Ok(bind) if bind.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), bind.port()).to_string()
        }
        Ok(bind) => bind.to_string(),
        // Config validation rejects this, but never return an empty host.
        Err(_) => "127.0.0.1:40772".to_string(),
    }
}

fn iptv_playlist_body(api_root: &str, records: &[ServiceRecord], config: &Config) -> String {
    // Only tuneable services are listed: service types `0x01`/`0xAD`, bound to
    // a channel, and that channel is configured and enabled — the same check
    // `validate_service_stream`/`require_channel` apply to stream URLs, so the
    // playlist never emits a dead link.
    let eligible: Vec<&ServiceRecord> = records
        .iter()
        .filter(|record| {
            matches!(record.service_type, 1 | 173)
                && record
                    .channel
                    .as_ref()
                    .is_some_and(|(channel_type, channel)| {
                        find_channel(config, channel_type, channel).is_some_and(is_enabled)
                    })
        })
        .collect();

    // Dedup by item id with the same winner as `ServiceTable::get`: the first
    // record in `order_key` order (network, tsid, service id, then channel
    // lexical, so BS beats GR). `list()` is already in that order, but pick
    // the minimum explicitly so the result does not depend on input order.
    let mut winners: HashMap<crate::services::ServiceItemId, &ServiceRecord> = HashMap::new();
    for record in eligible {
        let entry = winners.entry(record.item_id()).or_insert(record);
        if service_order_key(record) < service_order_key(entry) {
            *entry = record;
        }
    }

    // Only the winners are sorted for display, by the channel-oriented order.
    let mut services: Vec<&ServiceRecord> = winners.into_values().collect();
    services.sort_by(|left, right| playlist_order(left, right));

    let mut body = String::from("#EXTM3U\n");
    for record in services {
        let item_id = record.item_id();
        let channel_type = record
            .channel
            .as_ref()
            .map(|(channel_type, _)| channel_type.as_str())
            .unwrap_or("");
        let name = record.name.replace(['\r', '\n'], " ");
        body.push_str("#KODIPROP:mimetype=video/mp2t\n");
        body.push_str(&format!(
            "#EXTINF:-1 tvg-id=\"{item_id}\" group-title=\"{channel_type}\",{name}\n"
        ));
        body.push_str(&format!("{api_root}/services/{item_id}/stream\n"));
    }
    body
}

/// The dedup key mirrored from `ServiceRecord::order_key` (services.rs):
/// network, transport stream, service id, then channel (`None` first).
fn service_order_key(record: &ServiceRecord) -> (u16, u16, u16, Option<(&str, &str)>) {
    (
        record.network_id,
        record.tsid,
        record.service_id,
        record
            .channel
            .as_ref()
            .map(|(channel_type, channel)| (channel_type.as_str(), channel.as_str())),
    )
}

/// Channel-oriented order: GR, BS, CS, SKY, then any other type alphabetically;
/// each group by natural channel value, then service id.
fn playlist_order(left: &ServiceRecord, right: &ServiceRecord) -> Ordering {
    playlist_type_rank(left)
        .cmp(&playlist_type_rank(right))
        .then_with(|| playlist_other_type(left).cmp(&playlist_other_type(right)))
        .then_with(|| {
            cmp_channel(
                left.channel.as_ref().map(|(_, channel)| channel.as_str()),
                right.channel.as_ref().map(|(_, channel)| channel.as_str()),
            )
        })
        .then_with(|| left.service_id.cmp(&right.service_id))
        .then_with(|| left.item_id().0.cmp(&right.item_id().0))
}

fn playlist_type_rank(record: &ServiceRecord) -> u8 {
    match record
        .channel
        .as_ref()
        .map(|(channel_type, _)| channel_type.as_str())
    {
        Some("GR") => 0,
        Some("BS") => 1,
        Some("CS") => 2,
        Some("SKY") => 3,
        _ => 4,
    }
}

/// The channel type for the alphabetical tie-break among non-GR/BS/CS/SKY
/// types; `None` for the four ranked types (and for an unbound service).
fn playlist_other_type(record: &ServiceRecord) -> Option<&str> {
    match record
        .channel
        .as_ref()
        .map(|(channel_type, _)| channel_type.as_str())
    {
        None | Some("GR" | "BS" | "CS" | "SKY") => None,
        Some(other) => Some(other),
    }
}

/// Natural channel order: numeric when both sides are integers, else lexical.
fn cmp_channel(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match (left.parse::<u64>(), right.parse::<u64>()) {
            (Ok(left_number), Ok(right_number)) => {
                left_number.cmp(&right_number).then_with(|| left.cmp(right))
            }
            _ => left.cmp(right),
        },
        // An unbound service has no channel to sort by; keep it after bound ones.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// Request metadata taken from the headers (SPEC §6.1, §6.3).
struct StreamRequest {
    priority: i32,
    pin: Option<String>,
    provider: Option<String>,
    agent: Option<String>,
    user_id: String,
    /// Socket peer the request came from, used by the per-IP subscriber cap
    /// (SPEC §7). `X-Forwarded-For` is deliberately never consulted.
    peer: IpAddr,
}

impl StreamRequest {
    fn new(headers: &HeaderMap, peer: SocketAddr) -> Result<Self, String> {
        let priority = match last_header(headers, "x-mirakurun-priority") {
            Some(raw) => raw
                .parse::<i32>()
                // The header name is deliberately not echoed: SPEC §2 keeps
                // "Mirakurun" out of response bodies.
                .map_err(|_| {
                    format!("invalid priority header value {raw:?}: expected an integer")
                })?,
            None => 0,
        };
        // `X-Mirakc-Tuner` wins over its `X-Mirakurun-Tuner` alias when both
        // carry a value; within one name the last non-empty value wins
        // (SPEC §6.3).
        let pin = last_header(headers, "x-mirakc-tuner")
            .or_else(|| last_header(headers, "x-mirakurun-tuner"));
        Ok(Self {
            priority,
            pin,
            provider: last_header(headers, "x-mirakurun-tuner-user-provider"),
            agent: last_header(headers, "x-mirakurun-tuner-user-agent"),
            user_id: peer.to_string(),
            peer: peer.ip(),
        })
    }

    fn user(&self) -> TunerUser {
        let mut user = TunerUser::new(self.user_id.clone(), self.priority);
        if let Some(provider) = &self.provider {
            user = user.with_provider(provider.clone());
        }
        if let Some(agent) = &self.agent {
            user = user.with_agent(agent.clone());
        }
        user.with_peer(self.peer)
    }
}

/// The last non-empty value of a possibly repeated header, trimmed.
fn last_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get_all(name).iter().rev().find_map(|value| {
        value
            .to_str()
            .ok()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

async fn channel_stream(
    State(state): State<AppState>,
    UrlPath((channel_type, channel)): UrlPath<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let request = match validate_channel_stream(&state, &channel_type, &channel, &headers, peer) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let decode = is_decode_zero(&params);
    let subscription = state
        .manager
        .acquire_full(
            &channel_type,
            &channel,
            request.user(),
            request.pin.clone(),
            SessionTap::Raw,
            decode,
        )
        .await;
    match subscription {
        Ok(subscription) => stream_response(subscription, None, &request.user_id),
        Err(error) => acquire_failure(&error),
    }
}

async fn channel_service_stream(
    State(state): State<AppState>,
    UrlPath((channel_type, channel, service)): UrlPath<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (record, request) = match validate_channel_service_stream(
        &state,
        &channel_type,
        &channel,
        &service,
        &headers,
        peer,
    ) {
        Ok(validated) => validated,
        Err(response) => return response,
    };
    stream_service(&state, &record, &channel_type, &channel, request, &params).await
}

async fn service_stream(
    State(state): State<AppState>,
    UrlPath(service): UrlPath<String>,
    Query(params): Query<HashMap<String, String>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (record, channel_type, channel, request) =
        match validate_service_stream(&state, &service, &headers, peer) {
            Ok(validated) => validated,
            Err(response) => return response,
        };
    stream_service(&state, &record, &channel_type, &channel, request, &params).await
}

/// Shared acquisition of the two service-stream routes.
async fn stream_service(
    state: &AppState,
    record: &ServiceRecord,
    channel_type: &str,
    channel: &str,
    request: StreamRequest,
    params: &HashMap<String, String>,
) -> Response {
    let decode = is_decode_zero(params);
    let subscription = state
        .manager
        .acquire_full(
            channel_type,
            channel,
            request.user(),
            request.pin.clone(),
            SessionTap::Filtered {
                service_id: Some(record.service_id),
            },
            decode,
        )
        .await;
    match subscription {
        Ok(subscription) => stream_response(
            subscription,
            Some(ServiceFilter::new(record.service_id)),
            &request.user_id,
        ),
        Err(error) => acquire_failure(&error),
    }
}

/// HEAD for the channel stream: the same headers, an empty body, and no
/// session is acquired (a probe must not tune a device). It runs the same
/// [`validate_channel_stream`] as the GET, so both answer the same 400/404.
async fn stream_head(
    State(state): State<AppState>,
    UrlPath((channel_type, channel)): UrlPath<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    match validate_channel_stream(&state, &channel_type, &channel, &headers, peer) {
        Ok(_) => empty_stream_head(peer),
        Err(response) => response,
    }
}

/// HEAD for `/api/services/{id}/stream`: resolves the service and its channel
/// first, so an unknown id or an unconfigured channel is a 404 like the GET.
async fn service_head(
    State(state): State<AppState>,
    UrlPath(service): UrlPath<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    match validate_service_stream(&state, &service, &headers, peer) {
        Ok(_) => empty_stream_head(peer),
        Err(response) => response,
    }
}

/// HEAD for `/api/channels/{type}/{channel}/services/{id}/stream`: resolves the
/// service on that channel (like the GET) and validates the path, the channel
/// allowlist and the headers, so the same 400/404 statuses come back without
/// acquiring a session.
async fn channel_service_head(
    State(state): State<AppState>,
    UrlPath((channel_type, channel, service)): UrlPath<(String, String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    match validate_channel_service_stream(&state, &channel_type, &channel, &service, &headers, peer)
    {
        Ok(_) => empty_stream_head(peer),
        Err(response) => response,
    }
}

/// The statically decidable validation `GET` and `HEAD` share, in order:
/// path syntax (400), channel allowlist (404), header parse (400), pinned tuner
/// (400). Resource errors (409/503) need a session and are left to the GET.
#[allow(clippy::result_large_err)]
fn validate_channel_stream(
    state: &AppState,
    channel_type: &str,
    channel: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Result<StreamRequest, Response> {
    validate_path(channel_type, channel)
        .map_err(|reason| error_response(StatusCode::BAD_REQUEST, reason))?;
    require_channel(&state.config, channel_type, channel)?;
    finish_stream_request(state, headers, peer, channel_type)
}

#[allow(clippy::result_large_err)]
fn validate_channel_service_stream(
    state: &AppState,
    channel_type: &str,
    channel: &str,
    service: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Result<(ServiceRecord, StreamRequest), Response> {
    validate_path(channel_type, channel)
        .map_err(|reason| error_response(StatusCode::BAD_REQUEST, reason))?;
    require_channel(&state.config, channel_type, channel)?;
    // The channel is in the path, so it disambiguates a sid present on two
    // channels: resolve it channel-scoped, not through the global lookup.
    let record = state
        .manager
        .services()
        .resolve_raw_on_channel(channel_type, channel, service)
        .map_err(|error| {
            tracing::debug!(%error, id = %service, "service not found");
            error_response(StatusCode::NOT_FOUND, "Service Not Found")
        })?;
    let request = finish_stream_request(state, headers, peer, channel_type)?;
    Ok((record, request))
}

#[allow(clippy::result_large_err)]
fn validate_service_stream(
    state: &AppState,
    service: &str,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> Result<(ServiceRecord, String, String, StreamRequest), Response> {
    let record = state.manager.services().resolve(service).map_err(|error| {
        tracing::debug!(%error, id = %service, "service not found");
        error_response(StatusCode::NOT_FOUND, "Service Not Found")
    })?;
    let Some((channel_type, channel)) = record.channel.clone() else {
        // Without a channel binding there is no way to tune to this service.
        return Err(error_response(StatusCode::NOT_FOUND, "Service Not Found"));
    };
    validate_path(&channel_type, &channel)
        .map_err(|reason| error_response(StatusCode::BAD_REQUEST, reason))?;
    // A service whose channel is no longer in `channels.yml` (or is disabled)
    // is not tuneable, so it is as absent as an unknown service.
    require_channel(&state.config, &channel_type, &channel)?;
    let request = finish_stream_request(state, headers, peer, &channel_type)?;
    Ok((record, channel_type, channel, request))
}

/// The configured, **enabled** `(type, channel)`, or the 404 every stream route
/// answers for an unknown one. `disabled: true` is treated as absent: the
/// channel is not a streamable resource, and a 404 keeps a disabled entry
/// indistinguishable from a typo.
#[allow(clippy::result_large_err)]
fn require_channel<'a>(
    config: &'a Config,
    channel_type: &str,
    channel: &str,
) -> Result<&'a ChannelConfig, Response> {
    match find_channel(config, channel_type, channel) {
        Some(entry) if is_enabled(entry) => Ok(entry),
        _ => Err(error_response(StatusCode::NOT_FOUND, "Channel Not Found")),
    }
}

/// Parses the priority header (400) and validates the pin against `type_name`
/// (400), the two values `HEAD` needs but cannot defer to the session layer.
#[allow(clippy::result_large_err)]
fn finish_stream_request(
    state: &AppState,
    headers: &HeaderMap,
    peer: SocketAddr,
    type_name: &str,
) -> Result<StreamRequest, Response> {
    let request = StreamRequest::new(headers, peer)
        .map_err(|reason| error_response(StatusCode::BAD_REQUEST, reason))?;
    if let Some(pin) = &request.pin {
        if let Err(error) = state.manager.validate_pin(pin, type_name) {
            return Err(acquire_failure(&error));
        }
    }
    Ok(request)
}

fn empty_stream_head(peer: SocketAddr) -> Response {
    let mut response = Response::new(Body::empty());
    fill_stream_headers(response.headers_mut(), &peer.to_string());
    response
}

/// The 200 response of a stream route, with the SPEC §9 headers.
fn stream_response(
    subscription: Subscription,
    filter: Option<ServiceFilter>,
    user_id: &str,
) -> Response {
    let body = subscription_body(subscription, filter);
    let mut response = Response::new(body);
    fill_stream_headers(response.headers_mut(), user_id);
    response
}

fn fill_stream_headers(headers: &mut HeaderMap, user_id: &str) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(TS_CONTENT_TYPE),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::HeaderName::from_static(TUNER_USER_ID),
        HeaderValue::from_str(user_id).unwrap_or_else(|_| HeaderValue::from_static("unknown")),
    );
}

/// Turns a subscription into a body, optionally through a per-subscriber
/// [`ServiceFilter`] (SPEC §4, §5).
fn subscription_body(subscription: Subscription, filter: Option<ServiceFilter>) -> Body {
    let Subscription { rx, .. } = subscription;
    match filter {
        None => {
            let stream = ReceiverStream::new(rx).map(Ok::<Bytes, Infallible>);
            Body::from_stream(stream)
        }
        Some(mut filter) => {
            // One bounded hop so a slow client applies backpressure to its own
            // subscriber only (SPEC §7). Dropping the body drops `out_rx`,
            // which ends this task and releases the subscription.
            let (tx, out_rx) = mpsc::channel::<Result<Bytes, Infallible>>(4);
            tokio::spawn(async move {
                let mut rx = rx;
                while let Some(chunk) = rx.recv().await {
                    let filtered = filter.push(&chunk);
                    if filtered.is_empty() {
                        continue;
                    }
                    if tx.send(Ok(Bytes::from(filtered))).await.is_err() {
                        break;
                    }
                }
                tracing::debug!(
                    service_id = filter.service_id(),
                    packets_in = filter.packets_in,
                    packets_out = filter.packets_out,
                    "service stream ended"
                );
            });
            Body::from_stream(ReceiverStream::new(out_rx))
        }
    }
}

/// `decode=0` bypasses the whole pipeline (SPEC §4.1).
fn is_decode_zero(params: &HashMap<String, String>) -> bool {
    params
        .get("decode")
        .is_some_and(|value| value.trim() == "0")
}

/// Maps an [`AcquireError`] to the SPEC §6 response.
fn acquire_failure(error: &AcquireError) -> Response {
    let status = match error {
        AcquireError::UnknownTuner(_) | AcquireError::UnsupportedType(_) => StatusCode::BAD_REQUEST,
        // One process tree per channel: a request that wants the other decode
        // mode cannot be satisfied without cutting the existing stream, so it
        // is refused as a conflict rather than preempting (which would let two
        // clients ping-pong the tuner).
        AcquireError::DecodeModeConflict => StatusCode::CONFLICT,
        // SPEC §7 caps: a client that is over the limit is rate-limited (429),
        // not told the tuner is broken (503). The existing subscribers keep
        // streaming.
        AcquireError::TooManySubscribers
        | AcquireError::TooManySubscribersForIp
        | AcquireError::TooManySessions => StatusCode::TOO_MANY_REQUESTS,
        AcquireError::NoTuner
        | AcquireError::Unavailable
        | AcquireError::Busy
        | AcquireError::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
    };
    if status == StatusCode::SERVICE_UNAVAILABLE {
        tracing::warn!(%error, "stream request rejected");
        error_response(status, "Tuner Resource Unavailable")
    } else if status == StatusCode::CONFLICT {
        tracing::debug!(%error, "stream request rejected");
        error_response(
            status,
            format!("{error}: the channel is already streaming with the other decode mode"),
        )
    } else if status == StatusCode::TOO_MANY_REQUESTS {
        tracing::debug!(%error, "stream request rejected");
        error_response(status, "Too Many Requests")
    } else {
        tracing::debug!(%error, "stream request rejected");
        error_response(status, error.to_string())
    }
}

/// Validates a `{type}`/`{channel}` pair taken from the URL (SPEC §3.5).
fn validate_path(channel_type: &str, channel: &str) -> Result<(), String> {
    validate_type_name(channel_type)?;
    validate_channel_value(channel)
}

// ---------------------------------------------------------------------------
// Channel scan (SPEC §8.1)
// ---------------------------------------------------------------------------

async fn scan_channels(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let mut query = match ScanQuery::from_params(&params) {
        Ok(query) => query,
        Err(reason) => return error_response(StatusCode::BAD_REQUEST, reason),
    };
    // The discovery scan obeys the same per-channel budget as the startup scan
    // and the manual re-scan (SPEC §8). `from_params` only knows the default.
    query.timeout = state.config.server.scan_channel_timeout();
    // The scan reads and rewrites channels.yml, so a corrupt file must fail the
    // request before the response starts streaming: once the body is a stream
    // the status is already 200 and an error can only be a progress line.
    // The check is filesystem I/O, so it runs on the blocking pool rather than
    // parking an async worker (a slow USB/NFS config dir included).
    let config_dir = state.config_dir.clone();
    match tokio::task::spawn_blocking(move || check_scan_config(&config_dir)).await {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => {
            tracing::warn!(%reason, "channel scan rejected");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, reason);
        }
        Err(error) => {
            tracing::warn!(%error, "channel scan rejected");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("config check task failed: {error}"),
            );
        }
    }
    let scan_lease = match state.scan_lock.acquire() {
        Some(lease) => lease,
        None => return error_response(StatusCode::CONFLICT, "Already Scanning"),
    };

    let (tx, rx) = mpsc::channel::<String>(64);
    let manager = state.manager.clone();
    let config = Arc::clone(&state.config);
    let config_dir = state.config_dir.clone();
    let shutdown = state.shutdown_watch();
    tokio::spawn(async move {
        tracing::info!(
            type_name = %query.type_name,
            dry_run = query.dry_run,
            "channel scan started"
        );
        let result = scan::run_scan(
            &manager,
            &config,
            &config_dir,
            &query,
            tx.clone(),
            Some(shutdown),
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "channel scan failed");
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

/// Checks the on-disk `channels.yml` the scan is about to read and rewrite.
/// Blocking filesystem I/O; called from a `spawn_blocking` task.
///
/// A missing or blank file is the normal "nothing configured yet" case (the
/// scan creates it). Anything unreadable or unparseable is an error the caller
/// must surface *before* streaming, so it becomes a real 500 instead of a
/// `error:` progress line inside a 200 body.
fn check_scan_config(config_dir: &Path) -> Result<(), String> {
    let path = config_dir.join("channels.yml");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(());
    }
    serde_yaml_ng::from_str::<Vec<scan::ChannelEntry>>(&text)
        .map(|_| ())
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

async fn scan_not_found() -> Response {
    // SPEC §8.1: `async` scan polling is not implemented.
    error_response(StatusCode::NOT_FOUND, "Not Found")
}

// ---------------------------------------------------------------------------
// Middleware and error helpers
// ---------------------------------------------------------------------------

/// Sets `Server: unituner/<version>` on every response and logs the request
/// line at debug level (SPEC §2, §9).
async fn server_identity(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::SERVER, HeaderValue::from_static(SERVER_ID));
    tracing::debug!(%method, %path, status = response.status().as_u16(), "request");
    response
}

/// Rejects peers outside `server.allowed_cidrs` with a 403 JSON body
/// (SPEC §3.1). Requests without peer information (in-process tests) pass.
async fn network_guard(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0);
    if let Some(ip) = peer.map(|peer| peer.ip()) {
        if !state.allows(ip) {
            tracing::warn!(%ip, path = %request.uri().path(), "peer outside allowed_cidrs");
            return error_response(StatusCode::FORBIDDEN, "Forbidden");
        }
    }
    next.run(request).await
}

async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "Not Found")
}

/// The SPEC §9 error body: `{"code":…,"reason":…,"errors":[]}`.
fn error_response(status: StatusCode, reason: impl Into<String>) -> Response {
    let body = json!({
        "code": status.as_u16(),
        "reason": reason.into(),
        "errors": [],
    });
    (status, Json(body)).into_response()
}

/// Used by `main` to translate `server.addr` (`:40772`) into a bind address.
pub fn bind_address(addr: &str) -> Result<SocketAddr, String> {
    let trimmed = addr.trim();
    let candidate = if trimmed.starts_with(':') {
        format!("0.0.0.0{trimmed}")
    } else {
        trimmed.to_string()
    };
    candidate
        .parse::<SocketAddr>()
        .map_err(|_| format!("{addr:?} is not a valid socket address"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PipelinesConfig, ServerConfig};

    fn channel(channel_type: &str, channel: &str, disabled: bool) -> ChannelConfig {
        ChannelConfig {
            name: format!("{channel_type} {channel}"),
            channel_type: channel_type.to_string(),
            channel: channel.to_string(),
            extra_args: String::new(),
            disabled,
            routes: Vec::new(),
        }
    }

    fn config(channels: Vec<ChannelConfig>) -> Config {
        Config {
            server: ServerConfig::default(),
            tuners: Vec::new(),
            channels,
            pipelines: PipelinesConfig {
                pipelines: HashMap::new(),
                types: HashMap::new(),
            },
        }
    }

    fn record(
        sid: u16,
        service_type: u8,
        name: &str,
        channel: Option<(&str, &str)>,
    ) -> ServiceRecord {
        let mut record = ServiceRecord::new(0x1111, 0x2222, sid, service_type, name);
        record.channel = channel.map(|(t, c)| (t.to_string(), c.to_string()));
        record
    }

    #[test]
    fn playlist_filters_unbound_unconfigured_and_disabled_channels() {
        let cfg = config(vec![
            channel("GR", "27", false),
            channel("BS", "BS01", true),
        ]);
        let records = vec![
            record(1, 1, "Enabled", Some(("GR", "27"))),
            record(2, 1, "Disabled", Some(("BS", "BS01"))),
            record(3, 1, "Unconfigured", Some(("GR", "99"))),
            record(4, 1, "Unbound", None),
            record(5, 2, "Radio", Some(("GR", "27"))),
        ];
        let body = iptv_playlist_body("http://h/api", &records, &cfg);
        assert!(body.contains("Enabled"), "{body}");
        for absent in ["Disabled", "Unconfigured", "Unbound", "Radio"] {
            assert!(!body.contains(absent), "{body}");
        }
    }

    #[test]
    fn playlist_dedup_winner_matches_service_table_get_order() {
        // Same item id on GR and BS, same nid/tsid/sid: order_key's channel
        // lexical order puts BS before GR, so BS must win even though
        // playlist_order would rank GR first.
        let cfg = config(vec![
            channel("GR", "27", false),
            channel("BS", "BS01", false),
        ]);
        let records = vec![
            record(1, 1, "GR copy", Some(("GR", "27"))),
            record(1, 1, "BS copy", Some(("BS", "BS01"))),
        ];
        let body = iptv_playlist_body("http://h/api", &records, &cfg);
        assert!(body.contains("BS copy"), "{body}");
        assert!(body.contains("group-title=\"BS\""), "{body}");
        assert!(!body.contains("GR copy"), "{body}");
        assert_eq!(body.matches("#EXTINF").count(), 1, "{body}");
    }

    #[test]
    fn playlist_winners_are_sorted_by_playlist_order_after_dedup() {
        let cfg = config(vec![
            channel("GR", "27", false),
            channel("BS", "BS01", false),
        ]);
        let records = vec![
            record(2, 1, "BS svc", Some(("BS", "BS01"))),
            record(1, 1, "GR svc", Some(("GR", "27"))),
        ];
        let body = iptv_playlist_body("http://h/api", &records, &cfg);
        let gr = body.find("GR svc").expect("GR entry");
        let bs = body.find("BS svc").expect("BS entry");
        assert!(gr < bs, "{body}");
    }

    #[test]
    fn normalize_authority_lowercases_and_keeps_port_and_brackets() {
        assert_eq!(
            normalize_authority("EXAMPLE.LAN:40772").as_deref(),
            Some("example.lan:40772")
        );
        assert_eq!(
            normalize_authority("[::1]:40772").as_deref(),
            Some("[::1]:40772")
        );
        assert_eq!(normalize_authority("::1").as_deref(), Some("[::1]"));
        assert_eq!(
            normalize_authority("localhost.").as_deref(),
            Some("localhost")
        );
        for invalid in ["a b", "host/path", "host:bad", "", "http://x"] {
            assert_eq!(normalize_authority(invalid), None, "{invalid:?}");
        }
    }
}

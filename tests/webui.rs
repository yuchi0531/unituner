//! Webui tests (SPEC §10): embedded assets at `/`, document CRUD with startup
//! validation, atomic writes with a `.bak`, the read-only server view, the
//! restart hook and the interaction with the existing `/api/*` routes.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use unituner::api::AppState;
use unituner::config::Config;
use unituner::services::ServiceTable;
use unituner::webui::{self, Webui};

const SERVER: &str = r#"
addr: ":0"
allowed_cidrs:
  - 127.0.0.0/8
idle_timeout_secs: 7
subscriber_buffer_chunks: 64
chunk_size_bytes: 4096
"#;

const TUNERS: &str = r#"
- name: t0
  types: [GR, BS]
  command: "recpt1 {channel} -"
"#;

const CHANNELS: &str = r#"
- name: NHK General
  type: GR
  channel: "27"
- name: NHK BS
  type: BS
  channel: "BS13_1"
"#;

const PIPELINES: &str = r#"
pipelines:
  pass:
    - "cat"
  cas:
    - "arib-b25-stream-test"
    - "ts-convert"
types:
  GR: { pipeline: pass }
  BS: { pipeline: cas }
"#;

const PEER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const OUTSIDE: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)), 0);

struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        Self::with(CHANNELS)
    }

    fn with(channels: &str) -> Self {
        Self::with_server(SERVER, channels)
    }

    fn with_server(server: &str, channels: &str) -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", server),
            ("tuners.yml", TUNERS),
            ("channels.yml", channels),
            ("pipelines.yml", PIPELINES),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        Self {
            path: dir.path().to_path_buf(),
            _dir: dir,
        }
    }

    fn state(&self) -> AppState {
        AppState::new(
            Arc::new(Config::load(&self.path).expect("config loads")),
            ServiceTable::new(),
            self.path.clone(),
        )
    }

    fn read(&self, name: &str) -> String {
        std::fs::read_to_string(self.path.join(name)).expect("read config file")
    }

    fn exists(&self, name: &str) -> bool {
        self.path.join(name).exists()
    }
}

/// A restart hook that records how often it ran, so a test can assert that a
/// rejected restart never re-executed the process.
#[derive(Clone, Default)]
struct ExecCounter(Arc<AtomicUsize>);

impl ExecCounter {
    fn hook(&self) -> Webui {
        let counter = self.0.clone();
        Webui::with_exec(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// A request with the host a loopback client would send, so the Host allowlist
/// accepts it. Tests that exercise the allowlist build their own request.
fn request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "127.0.0.1:40772")
        .body(Body::empty())
        .expect("request")
}

fn json_request(method: Method, uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("host", "127.0.0.1:40772")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn send(
    state: &AppState,
    webui: Webui,
    peer: Option<SocketAddr>,
    request: Request<Body>,
) -> axum::response::Response {
    let router = webui::router_with(state.clone(), webui);
    let router = match peer {
        Some(peer) => router.layer(axum::Extension(ConnectInfo(peer))),
        None => router,
    };
    router.oneshot(request).await.expect("infallible")
}

async fn send_body(
    state: &AppState,
    webui: Webui,
    request: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let response = send(state, webui, None, request).await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body read")
        .to_bytes()
        .to_vec();
    (status, headers, body)
}

fn json(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).unwrap_or_else(|error| {
        panic!(
            "valid JSON body: {error}: {}",
            String::from_utf8_lossy(body)
        )
    })
}

fn text(body: &[u8]) -> String {
    String::from_utf8(body.to_vec()).expect("utf-8 body")
}

async fn get(state: &AppState, uri: &str) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    send_body(state, Webui::default(), request(Method::GET, uri)).await
}

async fn put(
    state: &AppState,
    webui: Webui,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, Vec<u8>) {
    let (status, _, bytes) = send_body(state, webui, json_request(Method::PUT, uri, body)).await;
    (status, bytes)
}

// ---------------------------------------------------------------------------
// Static assets and the API merge
// ---------------------------------------------------------------------------

#[tokio::test]
async fn index_is_served_at_the_root_as_html() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, headers, body) = get(&state, "/").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(
        headers.get("server").and_then(|v| v.to_str().ok()),
        Some(concat!("unituner/", env!("CARGO_PKG_VERSION")))
    );
    let html = text(&body);
    // Known markers: the tab bar and the status containers the JS fills.
    assert!(html.contains("id=\"tabs\""), "{html}");
    assert!(html.contains("id=\"status-tuners\""), "{html}");
    assert!(html.contains("data-tab=\"channels\""), "{html}");
    assert!(html.contains("/assets/app.js"), "{html}");
    assert!(
        html.contains("allowed_cidrs"),
        "the footer states the auth model"
    );
    assert!(
        !html.contains("Mirakurun"),
        "SPEC §2 keeps the name out of bodies"
    );
}

#[tokio::test]
async fn embedded_assets_have_their_content_types() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let (status, headers, body) = get(&state, "/assets/app.js").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/javascript; charset=utf-8")
    );
    let script = text(&body);
    assert!(script.contains("/webui/api/status"), "{script}");
    assert!(script.contains("/webui/api/restart"));
    assert!(script.contains("/webui/api/' + name"), "{script}");
    // Per-type channel sub-tabs: the bar is rebuilt from the in-memory model
    // and the table body is filtered by the selected type.
    assert!(script.contains("renderChannelTypeTabs"), "{script}");
    assert!(script.contains("channel-subtabs"), "{script}");
    assert!(script.contains("channelType"), "{script}");
    assert!(script.contains("aria-selected"), "{script}");

    let (status, headers, body) = get(&state, "/assets/style.css").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("text/css; charset=utf-8")
    );
    let css = text(&body);
    assert!(css.contains("@media"), "responsive CSS present");
    assert!(
        css.contains(".subtabs"),
        "the sub-tab bar reuses the tab styling"
    );
}

#[tokio::test]
async fn the_index_has_a_channel_subtab_container() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, _, body) = get(&state, "/").await;

    assert_eq!(status, StatusCode::OK);
    let html = text(&body);
    // Rendered by app.js from the model, never hardcoded in the document.
    assert!(html.contains("id=\"channel-subtabs\""), "{html}");
    assert!(html.contains("role=\"tablist\""), "{html}");
}

#[tokio::test]
async fn the_merged_app_keeps_the_mirakurun_api_working() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let response = webui::app(state.clone())
        .oneshot(request(Method::GET, "/api/version"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value = json(&body);
    assert_eq!(value["current"], env!("CARGO_PKG_VERSION"));

    // `/` and the API coexist, and an unknown path is still a JSON 404.
    let response = webui::app(state)
        .oneshot(request(Method::GET, "/webui/api/does-not-exist"))
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_cidr_guard_covers_the_ui() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let response = send(
        &state,
        Webui::default(),
        Some(OUTSIDE),
        request(Method::GET, "/"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = send(
        &state,
        Webui::default(),
        Some(PEER),
        request(Method::GET, "/webui/api/status"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_reports_tuners_sessions_and_counts() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, _, body) = get(&state, "/webui/api/status").await;

    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(value["counts"]["tuners"], 1);
    assert_eq!(value["counts"]["sessions"], 0);
    assert_eq!(value["counts"]["channels"], 2);
    assert_eq!(value["counts"]["services"], 0);

    let tuner = &value["tuners"][0];
    assert_eq!(tuner["name"], "t0");
    assert_eq!(tuner["types"][0], "GR");
    assert_eq!(tuner["isFree"], true);
    assert_eq!(tuner["isUsing"], false);
    assert_eq!(tuner["isAvailable"], true);
    assert!(tuner["users"].as_array().expect("users array").is_empty());
    assert!(tuner["channel"].is_null());
    assert!(value["sessions"].as_array().expect("sessions").is_empty());
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_channels_returns_the_parsed_file_and_an_etag() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, headers, body) = get(&state, "/webui/api/channels").await;

    assert_eq!(status, StatusCode::OK);
    assert!(headers.contains_key("etag"), "GET returns an ETag");
    let value = json(&body);
    let channels = value["channels"].as_array().expect("channels");
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0]["name"], "NHK General");
    assert_eq!(channels[0]["type"], "GR");
    assert_eq!(channels[0]["channel"], "27");
    assert_eq!(channels[0]["disabled"], false);
}

#[tokio::test]
async fn put_channels_writes_the_yaml_and_keeps_a_backup() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({
            "channels": [
                { "name": "NHK General", "type": "GR", "channel": "27", "disabled": false },
                { "name": "TOKYO MX", "type": "GR", "channel": "16", "disabled": true },
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{}", text(&body));
    assert_eq!(json(&body)["status"], "saved");

    let after = fixture.read("channels.yml");
    assert!(after.contains("TOKYO MX"), "{after}");
    assert!(after.contains("disabled: true"), "{after}");
    assert!(
        !after.contains("NHK BS"),
        "the document is replaced: {after}"
    );
    // The YAML shape is the SPEC §3.3 one.
    assert!(after.contains("type: GR"), "{after}");
    assert!(!after.contains("channel_type"), "{after}");

    assert_eq!(fixture.read("channels.yml.bak"), before);
    assert!(!fixture.exists("channels.yml.tmp"));

    // The result still loads through the startup path.
    Config::load(&fixture.path).expect("saved config is valid");
}

#[tokio::test]
async fn put_channels_with_an_invalid_body_is_400_and_writes_nothing() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({
            "channels": [
                { "name": "Bad", "type": "GR!", "channel": "27" }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    let value = json(&body);
    assert_eq!(value["code"], 400);
    let issues = value["errors"].as_array().expect("errors list");
    assert!(
        issues.iter().any(|issue| issue
            .as_str()
            .is_some_and(|text| text.contains("invalid type name"))),
        "{value}"
    );

    assert_eq!(fixture.read("channels.yml"), before);
    assert!(!fixture.exists("channels.yml.bak"), "no backup on failure");
    assert!(!fixture.exists("channels.yml.tmp"));
}

#[tokio::test]
async fn duplicate_channel_pair_is_rejected_without_writing() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({
            "channels": [
                { "name": "A", "type": "GR", "channel": "27" },
                { "name": "B", "type": "GR", "channel": "27" },
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert!(text(&body).contains("duplicate (type, channel) pair"));
    assert_eq!(fixture.read("channels.yml"), before);
}

#[tokio::test]
async fn a_stale_if_match_is_a_conflict() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (_, headers, _) = get(&state, "/webui/api/channels").await;
    let etag = headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .expect("etag")
        .to_string();

    // Someone else saves in between.
    std::fs::write(
        fixture.path.join("channels.yml"),
        "- name: Other\n  type: GR\n  channel: \"99\"\n",
    )
    .expect("external write");

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .header("content-type", "application/json")
        .header("if-match", etag)
        .body(Body::from(
            serde_json::json!({ "channels": [{ "name": "Mine", "type": "GR", "channel": "1" }] })
                .to_string(),
        ))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;

    assert_eq!(status, StatusCode::CONFLICT, "{}", text(&body));
    assert!(fixture.read("channels.yml").contains("Other"));
}

// ---------------------------------------------------------------------------
// Tuners
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_tuners_round_trips_through_the_startup_validator() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/tuners",
        serde_json::json!({
            "tuners": [
                { "name": "t0", "types": ["GR", "BS"], "command": "recpt1 {channel} -", "disabled": false },
                { "name": "t1", "types": ["CS"], "command": "recpt1 --device /dev/x {channel} -", "disabled": true },
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    let yaml = fixture.read("tuners.yml");
    assert!(yaml.contains("name: t1"), "{yaml}");
    assert!(yaml.contains("- GR"), "{yaml}");
    assert!(yaml.contains("disabled: true"), "{yaml}");

    let (status, _, body) = get(&state, "/webui/api/tuners").await;
    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(value["tuners"][1]["name"], "t1");
    assert_eq!(value["tuners"][1]["types"][0], "CS");
    assert_eq!(value["tuners"][1]["disabled"], true);
}

#[tokio::test]
async fn tuners_with_an_unknown_field_are_rejected() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("tuners.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/tuners",
        serde_json::json!({ "tuners": [{ "name": "t0", "types": ["GR"], "cmd": "recpt1" }] }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert_eq!(fixture.read("tuners.yml"), before);
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_pipelines_rejects_a_binding_to_a_missing_pipeline() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("pipelines.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({
            "pipelines": { "pass": ["cat"] },
            "types": { "GR": { "pipeline": "cas" } }
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert!(text(&body).contains("references unknown pipeline"));
    assert_eq!(fixture.read("pipelines.yml"), before);
}

#[tokio::test]
async fn put_pipelines_writes_the_spec_shape_and_reads_it_back() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({
            "pipelines": { "pass": ["cat"], "cas": ["arib-b25-stream-test", "ts-convert"] },
            "types": { "GR": { "pipeline": "pass" }, "BS": { "pipeline": "cas" } }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    let yaml = fixture.read("pipelines.yml");
    assert!(yaml.contains("pipelines:"), "{yaml}");
    assert!(yaml.contains("- arib-b25-stream-test"), "{yaml}");
    assert!(yaml.contains("types:"), "{yaml}");
    assert!(yaml.contains("pipeline: cas"), "{yaml}");

    let (status, _, body) = get(&state, "/webui/api/pipelines").await;
    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(
        value["pipelines"]["cas"].as_array().expect("stages").len(),
        2
    );
    assert_eq!(value["types"]["BS"]["pipeline"], "cas");
}

#[tokio::test]
async fn put_pipelines_accepts_an_empty_document_as_raw_ts() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // Raw-TS operation: no pipelines at all is a valid configuration.
    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({ "pipelines": {}, "types": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    let yaml = fixture.read("pipelines.yml");
    assert!(yaml.contains("pipelines: {}"), "{yaml}");
    Config::load(&fixture.path).expect("raw-TS config loads");
}

#[tokio::test]
async fn put_pipelines_still_rejects_an_empty_command_list() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("pipelines.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({ "pipelines": { "pass": [] }, "types": {} }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert!(text(&body).contains("at least one command"));
    assert_eq!(fixture.read("pipelines.yml"), before);
}

#[tokio::test]
async fn put_pipelines_rejects_an_unknown_binding_field() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("pipelines.yml");

    // A typo in the binding object used to be silently dropped by the untagged
    // enum and then rejected as "unknown pipeline" (or worse, ignored when
    // untagged fell through to a bare name). It is now a 400 at parse time.
    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({
            "pipelines": { "pass": ["cat"] },
            "types": { "GR": { "pipelne": "pass" } }
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert!(
        text(&body).contains("invalid pipelines document"),
        "{}",
        text(&body)
    );
    assert_eq!(fixture.read("pipelines.yml"), before);
}

#[tokio::test]
async fn put_pipelines_still_accepts_the_bare_pipeline_name() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/pipelines",
        serde_json::json!({
            "pipelines": { "pass": ["cat"] },
            "types": { "GR": "pass" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));
    assert!(fixture.read("pipelines.yml").contains("pipeline: pass"));
}

#[tokio::test]
async fn status_counts_follow_the_files_and_the_live_table() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // The live service table is counted per request.
    state
        .manager
        .services()
        .upsert(unituner::services::ServiceRecord::new(
            0x7FE0, 0x7FE0, 0x0408, 0x01, "NHK",
        ));

    // A hand edit of channels.yml is visible without a restart.
    std::fs::write(
        fixture.path.join("channels.yml"),
        "- name: Only\n  type: GR\n  channel: \"27\"\n",
    )
    .expect("write channels");

    let (status, _, body) = get(&state, "/webui/api/status").await;
    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(value["counts"]["services"], 1);
    assert_eq!(value["counts"]["channels"], 1);

    // An unreadable file falls back to the startup snapshot instead of 0.
    std::fs::write(fixture.path.join("channels.yml"), "not: a list\n").expect("break file");
    let (status, _, body) = get(&state, "/webui/api/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["counts"]["channels"], 2);
}

// ---------------------------------------------------------------------------
// Server config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_config_is_read_only_json() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("server.yml");

    let (status, headers, body) = get(&state, "/webui/api/server-config").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get("allow").and_then(|value| value.to_str().ok()),
        Some("GET, HEAD")
    );
    let value = json(&body);
    assert_eq!(value["addr"], ":0");
    assert_eq!(value["allowed_cidrs"][0], "127.0.0.0/8");
    assert_eq!(value["allowed_hosts"].as_array().expect("array").len(), 0);
    assert_eq!(value["idle_timeout_secs"], 7);
    assert_eq!(value["subscriber_buffer_chunks"], 64);
    assert_eq!(value["chunk_size_bytes"], 4096);
    assert_eq!(value["read_only"], true);
    assert_eq!(value["source"], "file");
    assert!(value.get("error").is_none(), "{value}");

    // No mutation endpoint: PUT is a method-not-allowed error, never a write.
    let (status, _) = put(
        &state,
        Webui::default(),
        "/webui/api/server-config",
        serde_json::json!({ "addr": ":1" }),
    )
    .await;
    assert!(
        status == StatusCode::METHOD_NOT_ALLOWED || status == StatusCode::NOT_FOUND,
        "unexpected status: {status}"
    );
    assert_eq!(fixture.read("server.yml"), before);
}

#[tokio::test]
async fn server_config_reflects_a_hand_edit_and_flags_a_fallback() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // Hand-edited after startup: the response must show the file, not the boot
    // snapshot.
    std::fs::write(
        fixture.path.join("server.yml"),
        "addr: \":0\"\nallowed_cidrs: [127.0.0.0/8]\nallowed_hosts: [tv.example.lan]\nidle_timeout_secs: 9\n",
    )
    .expect("edit server.yml");

    let (status, _, body) = get(&state, "/webui/api/server-config").await;
    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(value["idle_timeout_secs"], 9);
    assert_eq!(value["allowed_hosts"][0], "tv.example.lan");
    assert_eq!(value["source"], "file");

    // A broken file falls back to the in-memory value and says so.
    std::fs::write(fixture.path.join("server.yml"), "addr: [unterminated\n").expect("break");
    let (status, _, body) = get(&state, "/webui/api/server-config").await;
    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    assert_eq!(value["source"], "memory");
    assert_eq!(value["idle_timeout_secs"], 7);
    assert!(value["error"].is_string(), "{value}");
}

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_with_a_broken_config_is_400_and_does_not_exec() {
    let fixture = Fixture::new();
    let counter = ExecCounter::default();
    let state = fixture.state();

    // A document that parses but fails validation.
    std::fs::write(
        fixture.path.join("channels.yml"),
        "- name: Bad\n  type: \"GR!\"\n  channel: \"27\"\n",
    )
    .expect("break the config");

    let (status, _, body) = send_body(
        &state,
        counter.hook(),
        json_request(Method::POST, "/webui/api/restart", serde_json::json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    assert!(text(&body).contains("invalid type name"));
    assert_eq!(counter.count(), 0, "must not re-exec on a broken config");
}

#[tokio::test]
async fn restart_with_a_valid_config_runs_the_exec_hook_once() {
    let fixture = Fixture::new();
    let counter = ExecCounter::default();
    let state = fixture.state();

    let (status, _, body) = send_body(
        &state,
        counter.hook(),
        json_request(Method::POST, "/webui/api/restart", serde_json::json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{}", text(&body));
    assert_eq!(json(&body)["status"], "restarted");
    assert_eq!(counter.count(), 1);
}

#[tokio::test]
async fn restart_requires_post() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, _, _) = get(&state, "/webui/api/restart").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn an_exec_failure_is_reported_as_500() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let webui = Webui::with_exec(|| Err("boom".to_string()));

    let (status, _, body) = send_body(
        &state,
        webui,
        json_request(Method::POST, "/webui/api/restart", serde_json::json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{}", text(&body));
    let value = json(&body);
    assert_eq!(value["reason"], "boom");
    assert!(value["errors"].as_array().expect("errors").is_empty());
}

// ---------------------------------------------------------------------------
// CSRF hardening
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mutating_requests_require_a_json_content_type() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .header("content-type", "text/plain")
        .body(Body::from(r#"{"channels":[]}"#))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(json(&body)["code"], 403);

    // No content type at all is rejected too (a cross-site form cannot set one).
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .body(Body::from(r#"{"channels":[]}"#))
        .expect("request");
    let (status, _, _) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    assert_eq!(fixture.read("channels.yml"), before, "nothing was written");

    // A JSON request to the same path still works.
    let (status, _) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({ "channels": [] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn a_foreign_origin_is_rejected_for_mutations() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // Same origin: the Origin host matches the Host header.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .header("origin", "http://127.0.0.1:40772")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"channels":[]}"#))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    // Another origin: rejected before anything is parsed or written.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .header("origin", "http://evil.example")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"channels":[]}"#))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(json(&body)["code"], 403);

    // The opaque `null` origin is rejected as well.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("host", "127.0.0.1:40772")
        .header("origin", "null")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"channels":[]}"#))
        .expect("request");
    let (status, _, _) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Reads are unaffected.
    let (status, _, _) = get(&state, "/webui/api/channels").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_restart_endpoint_is_guarded_too() {
    let fixture = Fixture::new();
    let counter = ExecCounter::default();
    let state = fixture.state();

    let request = Request::builder()
        .method(Method::POST)
        .uri("/webui/api/restart")
        .header("host", "127.0.0.1:40772")
        .body(Body::empty())
        .expect("request");
    let (status, _, body) = send_body(&state, counter.hook(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(counter.count(), 0, "a guarded restart must not exec");
}

// ---------------------------------------------------------------------------
// Host allowlist (DNS rebinding)
// ---------------------------------------------------------------------------

async fn get_with_host(state: &AppState, uri: &str, host: &str) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header("host", host)
        .body(Body::empty())
        .expect("request");
    let (status, _, body) = send_body(state, Webui::default(), request).await;
    (status, body)
}

#[tokio::test]
async fn the_host_allowlist_accepts_loopback_and_private_literals() {
    // The default `allowed_cidrs` (RFC1918 + loopback), so 192.168.x.x is in.
    let fixture = Fixture::with_server("addr: \":0\"\n", CHANNELS);
    let state = fixture.state();

    for host in [
        "localhost",
        "localhost:40772",
        "127.0.0.1:40772",
        "192.168.1.5:40772",
        "[::1]:40772",
    ] {
        let (status, body) = get_with_host(&state, "/", host).await;
        assert_eq!(status, StatusCode::OK, "Host: {host}: {}", text(&body));
    }
}

#[tokio::test]
async fn a_foreign_host_is_403_for_reads_and_writes() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    // Rebinding: the Origin matches the Host, but the Host is not allowlisted.
    let (status, body) = get_with_host(&state, "/", "evil.example").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(json(&body)["code"], 403);

    for uri in [
        "/",
        "/assets/app.js",
        "/assets/style.css",
        "/webui/api/status",
        "/webui/api/channels",
        "/api/status",
        "/api/channels",
    ] {
        let (status, _) = get_with_host(&state, uri, "evil.example").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri} must be rejected");
    }

    // Mutations with a matching Origin are rejected before anything is written.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/pipelines")
        .header("host", "evil.example")
        .header("origin", "http://evil.example")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "pipelines": {}, "types": {} }).to_string(),
        ))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(json(&body)["code"], 403);
    assert_eq!(fixture.read("channels.yml"), before);
    assert_eq!(fixture.read("pipelines.yml"), PIPELINES);
}

#[tokio::test]
async fn the_host_allowlist_covers_the_merged_api_router() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let response = webui::app(state.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/version")
                .header("host", "evil.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = webui::app(state)
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/version")
                .header("host", "127.0.0.1:40772")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn allowed_hosts_admits_a_name_that_is_not_localhost() {
    let server = r#"
addr: ":0"
allowed_cidrs:
  - 127.0.0.0/8
allowed_hosts:
  - tv.example.lan
"#;
    let fixture = Fixture::with_server(server, CHANNELS);
    let state = fixture.state();

    let (status, body) = get_with_host(&state, "/", "tv.example.lan:40772").await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    // Still case-insensitive, but nothing else sneaks in.
    let (status, _) = get_with_host(&state, "/", "TV.EXAMPLE.LAN:40772").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = get_with_host(&state, "/", "other.example.lan").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_missing_host_header_is_rejected() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // HTTP/1.1 requires Host; refusing an empty one is the safe default.
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .expect("request");
    let (status, _, _) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Concurrent saves
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_concurrent_puts_to_the_same_file_both_succeed() {
    let fixture = Fixture::new();
    let state = fixture.state();

    let first = tokio::spawn({
        let state = state.clone();
        async move {
            put(
                &state,
                Webui::default(),
                "/webui/api/channels",
                serde_json::json!({
                    "channels": [{ "name": "First", "type": "GR", "channel": "1" }]
                }),
            )
            .await
        }
    });
    let second = tokio::spawn({
        let state = state.clone();
        async move {
            put(
                &state,
                Webui::default(),
                "/webui/api/channels",
                serde_json::json!({
                    "channels": [{ "name": "Second", "type": "GR", "channel": "2" }]
                }),
            )
            .await
        }
    });

    let (first_status, first_body) = first.await.expect("join");
    let (second_status, second_body) = second.await.expect("join");
    assert_eq!(first_status, StatusCode::OK, "{}", text(&first_body));
    assert_eq!(second_status, StatusCode::OK, "{}", text(&second_body));

    // Both writes happened, serialised: the .bak holds the loser and the file
    // holds the winner, and the result is still a valid configuration.
    let file = fixture.read("channels.yml");
    let bak = fixture.read("channels.yml.bak");
    assert!(
        (file.contains("First") && bak.contains("Second"))
            || (file.contains("Second") && bak.contains("First")),
        "file: {file}\nbak: {bak}"
    );
    Config::load(&fixture.path).expect("saved config is valid");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_to_another_file_is_not_blocked_by_a_slow_save() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // Hold channels.yml's lock, then save tuners.yml: the other file must not
    // wait for it.
    let guard = unituner::webui::store_lock_async("channels.yml").await;
    let tuners = put(
        &state,
        Webui::default(),
        "/webui/api/tuners",
        serde_json::json!({
            "tuners": [{ "name": "t0", "types": ["GR"], "command": "recpt1 {channel} -" }]
        }),
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), tuners).await;
    drop(guard);
    let (status, body) = result.expect("a different file must not be blocked");
    assert_eq!(status, StatusCode::OK, "{}", text(&body));
}

#[tokio::test]
async fn a_failed_exec_resumes_the_manager() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let webui = Webui::with_exec(|| Err("boom".to_string()));

    let (status, _, _) = send_body(
        &state,
        webui,
        json_request(Method::POST, "/webui/api/restart", serde_json::json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // The process was not replaced, so the manager must accept requests again
    // instead of answering 503 `ShuttingDown` for ever. (The fixture's tuner
    // command does not exist, so the spawn itself fails: that is a different,
    // expected error.)
    let error = state
        .manager
        .acquire(
            "GR",
            "27",
            unituner::session::TunerUser::new("b", 0),
            None,
            true,
        )
        .await
        .expect_err("the fixture tuner does not run");
    assert_ne!(
        error,
        unituner::session::AcquireError::ShuttingDown,
        "a failed exec must not leave the manager shut down"
    );
    state.manager.shutdown().await;
}

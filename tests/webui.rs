//! Webui tests (SPEC §10): embedded assets at `/`, document CRUD with startup
//! validation, atomic writes with a `.bak`, the read-only server view, the
//! restart hook and the interaction with the existing `/api/*` routes.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use unituner::api::AppState;
use unituner::config::Config;
use unituner::scan::startup_scan;
use unituner::services::{
    load_service_cache, read_service_cache, service_fingerprint, ServiceRecord, ServiceTable,
};
use unituner::si::crc32_mpeg2;
use unituner::tsframe::TS_PACKET_SIZE;
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

/// A `channels.yml` with the SPEC §3.3 routes / extra-args fields. Two routes
/// are listed `t1` then `t0` on purpose: the webui must keep that order.
const ROUTED_CHANNELS: &str = r#"
- name: NHK General
  type: GR
  channel: "27"
  extra-args: "--lnb 15"
  routes:
    - tuner: t1
      channel: "13"
      extra-args: "--lnb 20"
    - tuner: t0
- name: NHK BS
  type: BS
  channel: "BS13_1"
"#;

/// The default two-type tuner plus a second tuner, so a route can name either.
const TUNERS_TWO: &str = r#"
- name: t0
  types: [GR, BS]
  command: "recpt1 {channel} -"
- name: t1
  types: [GR, BS]
  command: "recpt1 --device /dev/x {channel} {extra_args} -"
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

/// The default fixture with a routed `channels.yml` and a second tuner.
fn routed_fixture() -> Fixture {
    let fixture = Fixture::with(ROUTED_CHANNELS);
    std::fs::write(fixture.path.join("tuners.yml"), TUNERS_TWO).expect("write tuners.yml");
    fixture
}

// ---------------------------------------------------------------------------
// Fake tuner TS fixture (PAT + SDT-actual + NIT, two services)
// ---------------------------------------------------------------------------

const CACHE_NETWORK_ID: u16 = 0x7FE0;
const CACHE_TSID: u16 = 0x7FE0;
const GOOD_CHANNEL: &str = "- name: NHK\n  type: GR\n  channel: \"27\"\n";
const DEAD_CHANNEL: &str = "- name: Broken\n  type: DEAD\n  channel: \"1\"\n";

fn ts_packet(pid: u16, first: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0xFFu8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = ((first as u8) << 6) | (pid >> 8) as u8;
    packet[2] = pid as u8;
    packet[3] = 0x10 | (cc & 0x0F);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}

fn ts_section(table_id: u8, id: u16, rest: &[u8]) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (id >> 8) as u8,
        id as u8,
        0xC1,
        0x00,
        0x00,
    ];
    bytes.extend_from_slice(rest);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

fn pat_section() -> Vec<u8> {
    let mut rest = vec![0x00, 0x00];
    rest.extend_from_slice(&(0xE000u16 | 0x0010).to_be_bytes());
    for (program, pmt_pid) in [(0x0408u16, 0x01F0u16), (0x0409, 0x01F1)] {
        rest.extend_from_slice(&program.to_be_bytes());
        rest.extend_from_slice(&(0xE000 | pmt_pid).to_be_bytes());
    }
    ts_section(0x00, CACHE_TSID, &rest)
}

fn sdt_section() -> Vec<u8> {
    let mut rest = vec![(CACHE_NETWORK_ID >> 8) as u8, CACHE_NETWORK_ID as u8, 0xFF];
    for (service_id, name) in [(0x0408u16, "NHK General"), (0x0409, "NHK E")] {
        let mut encoded = vec![0x1B, 0x28, 0x4A];
        encoded.extend_from_slice(name.as_bytes());
        let mut descriptor = vec![0x01, 0x00, encoded.len() as u8];
        descriptor.extend_from_slice(&encoded);
        rest.extend_from_slice(&service_id.to_be_bytes());
        rest.push(0xFD);
        let loop_length = 2 + descriptor.len();
        rest.extend_from_slice(&[((loop_length >> 8) & 0x0F) as u8, loop_length as u8]);
        rest.push(0x48);
        rest.push(descriptor.len() as u8);
        rest.extend_from_slice(&descriptor);
    }
    ts_section(0x42, CACHE_TSID, &rest)
}

fn nit_section() -> Vec<u8> {
    ts_section(0x40, CACHE_NETWORK_ID, &[0xF0, 0x00, 0xF0, 0x00])
}

fn section_packets(pid: u16, section: &[u8]) -> Vec<u8> {
    let mut packets = Vec::new();
    let mut remaining = section;
    let mut cc = 0u8;
    let mut first = true;
    while first || !remaining.is_empty() {
        let capacity = TS_PACKET_SIZE - 4 - usize::from(first);
        let take = capacity.min(remaining.len());
        let mut payload = Vec::with_capacity(take + 1);
        if first {
            payload.push(0x00);
        }
        payload.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
        packets.extend_from_slice(&ts_packet(pid, first, cc, &payload));
        cc = cc.wrapping_add(1) & 0x0F;
        first = false;
    }
    packets
}

/// A complete multiplex for channels 27/28 (two services, no PMT).
fn cache_multiplex() -> Vec<u8> {
    [
        section_packets(0x0000, &pat_section()),
        section_packets(0x0011, &sdt_section()),
        section_packets(0x0010, &nit_section()),
    ]
    .concat()
}

/// `sh -c "cat <path>; sleep 30"`: streams the fixture then holds the pipe.
fn cache_streaming_command(path: &Path) -> String {
    format!("sh -c \"cat {}; sleep 30\"", path.display())
}

/// A fixture with `channels` and a GR tuner streaming `cache_multiplex()`.
///
/// `extra_tuners` is appended to `tuners.yml` (for a DEAD type, for example).
fn cache_fixture(channels: &str, extra_tuners: &str) -> Fixture {
    let fixture = Fixture::with(channels);
    let ts = fixture.path.join("mux.ts");
    std::fs::write(&ts, cache_multiplex()).expect("write ts");
    let command = cache_streaming_command(&ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: good\n  types: [GR]\n  command: '{command}'\n{extra_tuners}"),
    )
    .expect("write tuners.yml");
    fixture
}

fn cache_record(type_name: &str, channel: &str, sid: u16, name: &str) -> ServiceRecord {
    let mut record = ServiceRecord::new(CACHE_NETWORK_ID, CACHE_TSID, sid, 0x01, name);
    record.channel = Some((type_name.to_owned(), channel.to_owned()));
    record
}

/// Runs the startup scan against the fixture's on-disk configuration.
async fn populate_cache(state: &AppState) {
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(
        state.manager.clone(),
        Arc::clone(&state.config),
        state.config_dir.clone(),
        shutdown_rx,
    ));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");
}

fn scan_request() -> Request<Body> {
    json_request(
        Method::POST,
        "/webui/api/services/scan",
        serde_json::json!({}),
    )
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
    // Theme bootstrap: an inline script in <head> sets [data-theme] before the
    // stylesheet is applied, so the first paint already matches the choice.
    assert!(
        html.contains("document.documentElement.dataset.theme"),
        "{html}"
    );
    assert!(
        html.contains("localStorage.getItem('unituner-theme')"),
        "{html}"
    );
    assert!(html.contains("prefers-color-scheme: light"), "{html}");
    // The toggle itself: an icon button in the app bar.
    assert!(html.contains("id=\"theme-toggle\""), "{html}");
    assert!(html.contains("Switch to light theme"), "{html}");
    // Restart is an app-bar action now: inside <header>, no floating FAB.
    let header_start = html.find("<header").expect("header opens");
    let header_end = html.find("</header>").expect("header closes");
    let restart = html.find("id=\"restart\"").expect("restart button present");
    assert!(
        header_start < restart && restart < header_end,
        "Restart must live inside <header>"
    );
    assert!(html.contains("class=\"appbar-action restart\""), "{html}");
    assert!(
        html.contains("<span class=\"restart-label\">Restart</span>"),
        "{html}"
    );
    assert!(!html.contains("class=\"fab\""), "the extended FAB is gone");
    // It keeps its icon + accessible label.
    assert!(html.contains("aria-label=\"Restart\""), "{html}");
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
    // Per-channel route editing: a toggle reveals a nested editor with a
    // tuner datalist, add/remove/move controls and a client-side tuner check.
    for identifier in [
        "routeEditor",
        "renderTunerDatalist",
        "routesProblem",
        "routes-toggle",
        "tuner-names",
        "'extra-args'",
        "Add route",
        "Move route up",
        "Move route down",
    ] {
        assert!(script.contains(identifier), "missing {identifier}");
    }
    assert!(
        script.contains("setAttribute('list', 'tuner-names')"),
        "the tuner input is a datalist suggestion: {script}"
    );
    // Theme wiring: the toggle persists the choice and keeps its label in sync.
    assert!(script.contains("unituner-theme"), "{script}");
    assert!(script.contains("theme-toggle"), "{script}");
    assert!(script.contains("applyTheme"), "{script}");
    assert!(script.contains("localStorage.setItem"), "{script}");

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
    // Material Design 2 tokens: dark stays the default in `:root`, and the
    // light palette scopes the same names to [data-theme="light"].
    assert!(css.contains("--md-primary: #bb86fc"), "{css}");
    assert!(css.contains("--md-surface: #121212"), "{css}");
    assert!(css.contains("--md-on-primary: #000000"), "{css}");
    assert!(css.contains("--md-error: #cf6679"), "{css}");
    assert!(css.contains("--md-surface-1: #1e1e1e"), "{css}");
    assert!(css.contains("--md-elevation-4"), "app bar elevation token");
    assert!(css.contains(".message"), "the snackbar styles #message");
    assert!(css.contains("prefers-reduced-motion"), "{css}");

    // The light theme ships as a scoped token block with the full MD2 light
    // palette, not as a dead comment.
    assert!(css.contains("[data-theme=\"light\"]"), "{css}");
    assert!(css.contains("--md-primary: #6200ee"), "{css}");
    assert!(css.contains("--md-on-primary: #ffffff"), "{css}");
    assert!(css.contains("--md-surface: #ffffff"), "{css}");
    assert!(css.contains("--md-background: #fafafa"), "{css}");
    assert!(css.contains("--md-divider: rgba(0, 0, 0, 0.12)"), "{css}");
    assert!(css.contains("--md-appbar: #6200ee"), "{css}");
    // Tokens that keep both themes correct: chips, snackbars, inputs, ripple.
    for token in [
        "--md-ripple",
        "--md-chip-ok-bg",
        "--md-chip-ok-fg",
        "--md-chip-warn-bg",
        "--md-chip-warn-fg",
        "--md-chip-bad-bg",
        "--md-chip-bad-fg",
        "--md-snackbar-bg",
        "--md-snackbar-fg",
        "--md-snackbar-error-bg",
        "--md-snackbar-error-fg",
        "--md-scrollbar-thumb",
        "--md-code-bg",
        "--md-input-fill",
        "--md-input-fill-hover",
        "--md-focus-ring",
        "--md-appbar-focus-ring",
        "--md-appbar-fg",
        "--md-appbar-hover",
    ] {
        assert!(css.contains(token), "missing token {token}");
    }
    // The rules reference tokens rather than literal dark-only colors.
    assert!(css.contains("background: var(--md-snackbar-bg)"), "{css}");
    assert!(css.contains("background: var(--md-chip-ok-bg)"), "{css}");
    assert!(
        css.contains("border-bottom: 1px solid var(--md-input-underline)"),
        "{css}"
    );
    assert!(!css.contains("background: #333;"), "snackbar uses a token");
    assert!(
        !css.contains(".fab"),
        "Restart is a header action, not a FAB"
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
    // The Channels table grew the route editor's columns.
    assert!(html.contains("id=\"channels-table\""), "{html}");
    assert!(html.contains("<th>Extra-args</th>"), "{html}");
    assert!(html.contains("<th>Routes</th>"), "{html}");
}

#[tokio::test]
async fn the_index_has_a_rescan_action_and_the_script_streams_it() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, _, body) = get(&state, "/").await;
    assert_eq!(status, StatusCode::OK);
    let html = text(&body);
    assert!(html.contains("id=\"rescan\""), "{html}");
    assert!(html.contains("aria-label=\"Re-scan services\""), "{html}");
    assert!(html.contains("rescan-label"), "{html}");

    let (status, _, body) = send_body(
        &state,
        Webui::default(),
        request(Method::GET, "/assets/app.js"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let script = text(&body);
    assert!(
        script.contains("/webui/api/services/scan"),
        "the button calls the re-scan endpoint"
    );
    assert!(
        script.contains("getReader"),
        "the progress is consumed as a stream"
    );
    assert!(
        script.contains("textContent +="),
        "server text is appended without innerHTML"
    );
}

#[tokio::test]
async fn the_services_table_shows_the_physical_channel() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let (status, _, body) = get(&state, "/").await;
    assert_eq!(status, StatusCode::OK);

    // Scope the header assertions to the services table: the channels editor
    // keeps its own <th>Channel</th>.
    let html = text(&body);
    let table = html
        .find("id=\"services-table\"")
        .map(|start| &html[start..])
        .expect("services table present");
    let table = &table[..table.find("</table>").expect("table closes")];
    assert!(table.contains("<th>Type</th>"), "{table}");
    assert!(table.contains("<th>Phys CH</th>"), "{table}");
    assert!(!table.contains("<th>Channel</th>"), "{table}");

    let (status, _, body) = get(&state, "/assets/app.js").await;
    assert_eq!(status, StatusCode::OK);
    let script = text(&body);
    assert!(
        script.contains("physicalChannel"),
        "the services table renders a dedicated physical channel"
    );
    assert!(
        script.contains("service.channel.channel"),
        "the cell reads the channel number, not the combined label"
    );
    assert!(
        script.contains("service.channel.type"),
        "the Type column shows the channel type"
    );

    let (status, _, body) = get(&state, "/assets/style.css").await;
    assert_eq!(status, StatusCode::OK);
    let css = text(&body);
    assert!(
        !css.contains("#services-table th:nth-child(n + 5)"),
        "the services table no longer hides columns 5+"
    );
    assert!(
        css.contains("#services-table th:nth-child(2)"),
        "SID is hidden on phones"
    );
    assert!(
        css.contains("#services-table th:nth-child(3)"),
        "NID is hidden on phones"
    );
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
async fn get_channels_returns_routes_and_extra_args() {
    let fixture = routed_fixture();
    let state = fixture.state();
    let (status, _, body) = get(&state, "/webui/api/channels").await;

    assert_eq!(status, StatusCode::OK);
    let value = json(&body);
    let channels = value["channels"].as_array().expect("channels");
    assert_eq!(channels.len(), 2);
    let routed = &channels[0];
    assert_eq!(routed["extra-args"], "--lnb 15");
    let routes = routed["routes"].as_array().expect("routes");
    assert_eq!(routes.len(), 2, "both routes are returned, in order");
    assert_eq!(routes[0]["tuner"], "t1");
    assert_eq!(routes[0]["channel"], "13");
    assert_eq!(routes[0]["extra-args"], "--lnb 20");
    assert_eq!(routes[1]["tuner"], "t0");
    assert!(routes[1]["channel"].is_null(), "{value}");
    // A channel without the fields simply omits them.
    assert!(channels[1].get("routes").is_none(), "{value}");
    assert!(channels[1].get("extra-args").is_none(), "{value}");
}

#[tokio::test]
async fn a_routed_channel_round_trips_through_put_and_get() {
    let fixture = routed_fixture();
    let state = fixture.state();

    // GET the file, PUT it back: the routed fields and their order survive and
    // the second write is byte-identical (the YAML is stable, not just equal
    // after a re-quote).
    let (_, _, body) = get(&state, "/webui/api/channels").await;
    let document = json(&body);
    let (status, put_body) = put(&state, Webui::default(), "/webui/api/channels", document).await;
    assert_eq!(status, StatusCode::OK, "{}", text(&put_body));
    let first = fixture.read("channels.yml");

    let (_, _, body) = get(&state, "/webui/api/channels").await;
    let (status, put_body) =
        put(&state, Webui::default(), "/webui/api/channels", json(&body)).await;
    assert_eq!(status, StatusCode::OK, "{}", text(&put_body));
    let second = fixture.read("channels.yml");
    assert_eq!(first, second, "a GET/PUT round trip must be byte-stable");

    // The routes are written verbatim, in the author's order.
    let written: Vec<unituner::scan::ChannelEntry> =
        serde_yaml_ng::from_str(&first).expect("the written channels.yml parses");
    let routed = &written[0];
    assert_eq!(routed.extra_args, "--lnb 15");
    assert_eq!(routed.routes.len(), 2);
    assert_eq!(routed.routes[0].tuner, "t1");
    assert_eq!(routed.routes[0].channel.as_deref(), Some("13"));
    assert_eq!(routed.routes[0].extra_args.as_deref(), Some("--lnb 20"));
    assert_eq!(routed.routes[1].tuner, "t0");
    assert!(routed.routes[1].channel.is_none());

    // The written file still passes the startup validation.
    Config::load(&fixture.path).expect("the written config is valid");
}

#[tokio::test]
async fn an_unknown_route_tuner_is_rejected_with_a_useful_message() {
    let fixture = routed_fixture();
    let state = fixture.state();
    let before = fixture.read("channels.yml");

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({
            "channels": [
                {
                    "name": "NHK General",
                    "type": "GR",
                    "channel": "27",
                    "routes": [{ "tuner": "ghost" }]
                }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{}", text(&body));
    let value = json(&body);
    let errors = value["errors"].as_array().expect("errors list");
    assert!(
        errors.iter().any(|issue| issue
            .as_str()
            .is_some_and(|text| text.contains("references unknown tuner"))),
        "{value}"
    );
    assert_eq!(fixture.read("channels.yml"), before, "nothing was written");
}

/// Accepts the `extra_args` alias on channels and routes, exactly like the
/// startup schema (SPEC §3.3).
#[tokio::test]
async fn put_channels_accepts_the_extra_args_alias() {
    let fixture = routed_fixture();
    let state = fixture.state();

    let (status, body) = put(
        &state,
        Webui::default(),
        "/webui/api/channels",
        serde_json::json!({
            "channels": [
                {
                    "name": "NHK General",
                    "type": "GR",
                    "channel": "27",
                    "extra_args": "--lnb 15",
                    "routes": [{ "tuner": "t0", "extra_args": "--lnb 20" }]
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));

    let written: Vec<unituner::scan::ChannelEntry> =
        serde_yaml_ng::from_str(&fixture.read("channels.yml")).expect("parse written yaml");
    assert_eq!(written[0].extra_args, "--lnb 15");
    assert_eq!(written[0].routes[0].extra_args.as_deref(), Some("--lnb 20"));
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

    // A missing Host is accepted, but a present non-allowlisted one is not.
    let (status, body) = get_with_host(&state, "/", "other.example.lan").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));

    for uri in [
        "/",
        "/assets/app.js",
        "/assets/style.css",
        "/webui/api/status",
        "/webui/api/channels",
        "/api/version",
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
async fn a_missing_host_header_is_accepted() {
    // Native clients such as BonDriver_Mirakurun send no `Host` at all, so the
    // guard must not turn their reads or streams into a 403. The streaming
    // request goes through the merged router (the guard wraps `/api/*` there).
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let state = fixture.state();

    for uri in ["/", "/api/version"] {
        let request = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let (status, _, body) = send_app_body(&state, request).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {}", text(&body));
    }

    // A real tuner fixture: a 200 with bytes proves the session actually
    // streamed and was not just an empty success.
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/channels/GR/27/stream")
        .body(Body::empty())
        .expect("request");
    let response = webui::app(state)
        .layer(axum::Extension(ConnectInfo(PEER)))
        .oneshot(request)
        .await
        .expect("infallible");
    assert_eq!(response.status(), StatusCode::OK);
    let first = read_first_chunk(response.into_body()).await;
    assert!(!first.is_empty(), "the stream must carry TS bytes");
    assert_eq!(first[0], 0x47, "the first byte must be a TS sync byte");
}

/// Sends one request through the merged [`webui::app`] router (which applies the
/// Host guard to `/api/*` as well) and reads the whole body.
async fn send_app_body(
    state: &AppState,
    request: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let response = webui::app(state.clone())
        .oneshot(request)
        .await
        .expect("infallible");
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

/// Reads the first non-empty data frame of a possibly endless body.
async fn read_first_chunk(body: Body) -> Vec<u8> {
    let mut body = body;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(20), body.frame())
            .await
            .expect("first chunk timed out")
            .expect("body ended before any data")
            .expect("frame");
        if let Ok(data) = frame.into_data() {
            if !data.is_empty() {
                return data.to_vec();
            }
        }
    }
}

#[tokio::test]
async fn a_mutation_without_host_still_needs_json_and_passes_the_host_guard() {
    let fixture = Fixture::new();
    let state = fixture.state();

    // No Host and no JSON content type: the CSRF guard rejects it, as before.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .body(Body::empty())
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(
        json(&body)["reason"],
        "Content-Type: application/json is required"
    );

    // JSON content type and no Origin: the Host guard accepts the absent Host
    // (native clients, not browsers) and the CSRF guard only compares Origin
    // when it is present, so the write is accepted.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "channels": [{ "name": "No Host", "type": "GR", "channel": "1" }]
            })
            .to_string(),
        ))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::OK, "{}", text(&body));
    assert!(fixture.read("channels.yml").contains("No Host"));

    // An Origin without a Host is still rejected: a browser mutation always
    // carries an authority, so this combination is anomalous.
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/webui/api/channels")
        .header("origin", "http://127.0.0.1:40772")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "channels": [{ "name": "Origin Only", "type": "GR", "channel": "2" }]
            })
            .to_string(),
        ))
        .expect("request");
    let (status, _, body) = send_body(&state, Webui::default(), request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{}", text(&body));
    assert_eq!(json(&body)["reason"], "Cross-origin request rejected");
    assert!(!fixture.read("channels.yml").contains("Origin Only"));
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

// ---------------------------------------------------------------------------
// Persistent service cache + manual Re-scan (SPEC §8.2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_restart_serves_the_cached_services_without_a_scan() {
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let state = fixture.state();
    populate_cache(&state).await;
    state.manager.shutdown().await;

    // Every tuner command would leave a marker if spawned; the cache matches the
    // unchanged channels.yml, so the restart must not tune anything.
    let marker = fixture.path.join("spawned");
    let command = format!("sh -c \"touch {}; exec /nonexistent/x\"", marker.display());
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: good\n  types: [GR]\n  command: '{command}'\n"),
    )
    .expect("rewrite tuners.yml");

    let config = Arc::new(Config::load(&fixture.path).expect("config loads"));
    let table = load_service_cache(&fixture.path);
    let restarted = AppState::new(config, table, fixture.path.clone());

    // The production startup path: it sees the valid cache and returns.
    populate_cache(&restarted).await;
    assert!(!marker.exists(), "a valid cache must not spawn a tuner");

    let response = webui::app(restarted.clone())
        .oneshot(request(Method::GET, "/api/services"))
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
    let names: Vec<&str> = value
        .as_array()
        .expect("services")
        .iter()
        .map(|service| service["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["NHK General", "NHK E"], "{value}");
    assert_eq!(value[0]["channel"]["type"], "GR");
    assert_eq!(value[0]["channel"]["channel"], "27");
}

#[tokio::test]
async fn the_manual_rescan_streams_and_rewrites_the_cache() {
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let state = fixture.state();
    populate_cache(&state).await;

    let before = read_service_cache(&fixture.path)
        .expect("read")
        .expect("present");
    let channels_before = fixture.read("channels.yml");
    // Millisecond precision in generated_at: leave at least one tick between
    // the two writes.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let (status, headers, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain")
    );
    let log = text(&body);
    assert!(log.contains("service re-scan"), "{log}");
    assert!(log.contains("channel: \"27\""), "{log}");
    assert!(log.contains("**RE-SCAN COMPLETE**"), "{log}");

    let after = read_service_cache(&fixture.path)
        .expect("read")
        .expect("present");
    assert_ne!(after.generated_at, before.generated_at, "{after:?}");
    assert_eq!(
        after.fingerprint,
        service_fingerprint(&state.config.channels),
        "the re-scan keeps the current config's fingerprint"
    );
    assert_eq!(after.services.len(), 2);
    assert_eq!(state.manager.services().list().len(), 2);
    // The endpoint is strictly about services: the channel file is byte-identical.
    assert_eq!(fixture.read("channels.yml"), channels_before);

    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_waits_for_a_delayed_tuner_and_rewrites_the_cache() {
    // The tuner only produces its first byte after 3 s: the re-scan must wait
    // for it instead of instantly calling the channel "no signal", and the
    // result must reach both the live table and `services.json`.
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let ts = fixture.path.join("mux.ts");
    let command = format!("sh -c \"sleep 3; cat {}; sleep 30\"", ts.display());
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: good\n  types: [GR]\n  command: '{command}'\n"),
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let started = std::time::Instant::now();
    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK);
    let log = text(&body);
    assert!(
        log.contains("2 services found"),
        "the delayed SI was missed: {log}"
    );
    assert!(log.contains("**RE-SCAN COMPLETE**"), "{log}");
    assert!(
        elapsed >= Duration::from_millis(2500),
        "the re-scan did not wait for the delayed tuner: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(8), "{elapsed:?}");
    assert_eq!(state.manager.services().list().len(), 2);

    let first = read_service_cache(&fixture.path)
        .expect("read")
        .expect("cache");
    assert_eq!(first.services.len(), 2);

    // A second re-scan rewrites the cache with a new generated_at. Sleep past
    // the millisecond resolution so the timestamps differ.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(text(&body).contains("**RE-SCAN COMPLETE**"));
    let second = read_service_cache(&fixture.path)
        .expect("read")
        .expect("cache");
    assert_ne!(
        second.generated_at, first.generated_at,
        "a successful re-scan must refresh generated_at"
    );
    assert_eq!(second.services.len(), 2);
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_with_a_missing_tuner_writes_no_empty_cache() {
    let fixture = Fixture::with(GOOD_CHANNEL);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        "- name: good\n  types: [GR]\n  command: '/nonexistent/unituner-rescan-tuner'\n",
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let started = std::time::Instant::now();
    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK);
    let log = text(&body);
    assert!(
        log.contains("tuner failed to start"),
        "a missing command must be a tuner failure: {log}"
    );
    assert!(
        !log.contains("no signal\n"),
        "a missing command must not be reported as no signal: {log}"
    );
    assert!(
        log.contains("did not write"),
        "an all-start-failure pass must not persist an empty cache: {log}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "a spawn failure must not wait out the budget: {elapsed:?}"
    );
    assert!(
        !fixture.path.join("services.json").exists(),
        "no services.json may be written when no tuner could start"
    );
    assert!(state.manager.services().is_empty());
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_with_every_tuner_broken_adds_one_aggregate_hint() {
    // Three channels, all using a command that cannot start. The per-channel
    // lines show the expanded argv, and one explicit line after them says the
    // whole pass failed to start a tuner.
    let channels = "- name: A\n  type: GR\n  channel: \"26\"\n\
                    - name: B\n  type: GR\n  channel: \"27\"\n\
                    - name: C\n  type: GR\n  channel: \"28\"\n";
    let fixture = Fixture::with(channels);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        "- name: broken\n  types: [GR]\n  command: '/nonexistent/unituner-rescan-tuner {channel}'\n",
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::OK);
    let log = text(&body);
    assert_eq!(
        log.matches("all 3 channels failed to start their tuner")
            .count(),
        1,
        "exactly one aggregate hint: {log}"
    );
    assert!(
        log.contains("/nonexistent/unituner-rescan-tuner 26"),
        "the spawn error must show the expanded argv: {log}"
    );
    assert!(
        !log.contains("{channel}"),
        "the unsubstituted template must not leak: {log}"
    );
    assert!(
        log.contains("0 no signal, 0 busy, 3 tuner failed"),
        "the summary must break the outcomes down: {log}"
    );
    assert!(
        !fixture.path.join("services.json").exists(),
        "an all-start-failure pass must not persist an empty cache"
    );
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_with_one_broken_channel_has_no_aggregate_hint() {
    // Channel 27 streams the multiplex; 26 uses a command that cannot start.
    // Only the summary's outcome counts may mention the failure.
    let channels = "- name: Good\n  type: GR\n  channel: \"27\"\n\
                    - name: Broken\n  type: GR\n  channel: \"26\"\n";
    let fixture = Fixture::with(channels);
    let ts = fixture.path.join("mux.ts");
    std::fs::write(&ts, cache_multiplex()).expect("write ts");
    let command = format!(
        "sh -c \"case {{channel}} in 26) exec /nonexistent/x;; *) cat {}; sleep 30;; esac\"",
        ts.display()
    );
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: mixed\n  types: [GR]\n  command: '{command}'\n"),
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::OK);
    let log = text(&body);
    assert!(
        !log.contains("channels failed to start their tuner"),
        "a partial failure must not claim every channel failed: {log}"
    );
    assert!(
        log.contains("2 services found"),
        "the good channel must still be scanned: {log}"
    );
    assert!(
        log.contains("1 channels scanned, 2 found"),
        "the summary keeps the scanned/found counts: {log}"
    );
    assert!(
        log.contains("0 no signal, 0 busy, 1 tuner failed"),
        "the summary must count the one failed tuner: {log}"
    );
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_reports_a_tuner_that_exits_before_output() {
    let fixture = Fixture::with(GOOD_CHANNEL);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        "- name: good\n  types: [GR]\n  command: 'sh -c \"sleep 1; exit 0\"'\n",
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let started = std::time::Instant::now();
    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK);
    let log = text(&body);
    assert!(
        log.contains("tuner failed to start"),
        "an early exit must be a tuner failure: {log}"
    );
    assert!(
        log.contains("before any data"),
        "the reason must say the process exited before output: {log}"
    );
    assert!(
        !log.contains("no signal\n"),
        "an early exit must not be reported as no signal: {log}"
    );
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(5),
        "the exit must be noticed without waiting out the budget: {elapsed:?}"
    );
    assert!(
        !fixture.path.join("services.json").exists(),
        "no services.json may be written when no tuner could start"
    );
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_concurrent_manual_rescan_is_409() {
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let state = fixture.state();

    // The first request holds the shared scan lock for its whole pass.
    let first = send(&state, Webui::default(), None, scan_request()).await;
    assert_eq!(first.status(), StatusCode::OK);

    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let value = json(&body);
    assert_eq!(value["code"], 409);
    assert_eq!(value["reason"], "Already Scanning");

    // Dropping the body disconnects the first scan; wait until its tuner is
    // released so the test does not leak a process into the TempDir cleanup.
    drop(first);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !state.manager.sessions_snapshot().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the cancelled re-scan never released its tuner"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_disconnected_manual_rescan_leaves_the_cache_untouched() {
    let fixture = cache_fixture(GOOD_CHANNEL, "");
    let state = fixture.state();
    populate_cache(&state).await;
    state.manager.shutdown().await;

    // A slow tuner gives the client time to disconnect mid-pass.
    let marker = fixture.path.join("spawned");
    let ts = fixture.path.join("mux.ts");
    let command = format!(
        "sh -c \"touch {}; sleep 1; cat {}; sleep 30\"",
        marker.display(),
        ts.display()
    );
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: good\n  types: [GR]\n  command: '{command}'\n"),
    )
    .expect("rewrite tuners.yml");

    let config = Arc::new(Config::load(&fixture.path).expect("config loads"));
    let table = load_service_cache(&fixture.path);
    let state = AppState::new(config, table, fixture.path.clone());

    let cache_before = std::fs::read(fixture.path.join("services.json")).expect("cache");
    let response = send(&state, Webui::default(), None, scan_request()).await;
    assert_eq!(response.status(), StatusCode::OK);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the re-scan never reached the tuner"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(response);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !state.manager.sessions_snapshot().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the disconnected re-scan never released its tuner"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cache_after = std::fs::read(fixture.path.join("services.json")).expect("cache");
    assert_eq!(
        cache_after, cache_before,
        "a disconnected client must leave services.json untouched"
    );
    state.manager.shutdown().await;
}

#[tokio::test]
async fn a_manual_rescan_drops_removed_channels_and_keeps_busy_ones() {
    let fixture = cache_fixture(
        &format!("{GOOD_CHANNEL}{DEAD_CHANNEL}"),
        "- name: dead\n  types: [DEAD]\n  command: 'sh -c \"exit 1\"'\n",
    );
    let state = fixture.state();
    let table = state.manager.services();
    // A stale service on a scanned channel, a service of a channel that no
    // longer exists, and a previous record of a no-signal channel.
    table.insert(cache_record("GR", "27", 0x0999, "stale on 27"));
    table.insert(cache_record("GR", "28", 0x0901, "removed channel"));
    table.insert(cache_record("DEAD", "1", 0x0900, "kept no-signal"));

    let (status, _, body) = send_body(&state, Webui::default(), scan_request()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(text(&body).contains("**RE-SCAN COMPLETE**"));

    let records = table.list();
    assert_eq!(records.len(), 3, "{records:?}");
    assert!(
        records.iter().any(|record| record.name == "NHK General"),
        "the scanned channel was refreshed"
    );
    assert!(
        !records.iter().any(|record| record.name == "stale on 27"),
        "a service the channel no longer carries is dropped"
    );
    assert!(
        !records
            .iter()
            .any(|record| record.name == "removed channel"),
        "a channel removed from channels.yml is dropped"
    );
    assert!(
        records.iter().any(|record| record.name == "kept no-signal"),
        "a no-signal channel keeps its previous records"
    );

    let cache = read_service_cache(&fixture.path)
        .expect("read")
        .expect("present");
    assert!(
        !cache
            .services
            .iter()
            .any(|record| record.name == "removed channel"),
        "the cache follows the retain step"
    );
    assert!(cache
        .services
        .iter()
        .any(|record| record.name == "kept no-signal"));

    state.manager.shutdown().await;
}

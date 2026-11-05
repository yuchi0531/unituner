//! HTTP layer tests (SPEC §9): metadata endpoints, TS streaming, HEAD, error
//! mapping, priority takeover, pinning and the channel scan API.
//!
//! Every test drives a real [`unituner::api::router`] through `tower::oneshot`
//! with a fake tuner command (`sh -c "cat <fixture>"`) and a `cat` pipeline, so
//! the process tree, the session registry and the HTTP body are all exercised
//! together.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use unituner::api::{router, AppState};
use unituner::config::Config;
use unituner::services::{ServiceRecord, ServiceTable};
use unituner::si::crc32_mpeg2;
use unituner::tsfilter::ServiceFilter;
use unituner::tsframe::TS_PACKET_SIZE;

const TUNER_USER_ID: &str = "x-mirakurun-tuner-user-id";
const PEER: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

// ---------------------------------------------------------------------------
// TS fixture builders
// ---------------------------------------------------------------------------

fn packet(pid: u16, payload_unit_start: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = vec![0xFFu8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = ((payload_unit_start as u8) << 6) | (pid >> 8) as u8;
    packet[2] = pid as u8;
    packet[3] = 0x10 | (cc & 0x0F);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}

fn section(table_id: u8, id: u16, version: u8, rest: &[u8]) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (id >> 8) as u8,
        id as u8,
        0xC1 | ((version & 0x1F) << 1),
        0x00,
        0x00,
    ];
    bytes.extend_from_slice(rest);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

fn pat_section(tsid: u16, programs: &[(u16, u16)]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&[0x00, 0x00]);
    rest.extend_from_slice(&(0xE000u16 | 0x0010).to_be_bytes());
    for &(program, pmt_pid) in programs {
        rest.extend_from_slice(&program.to_be_bytes());
        rest.extend_from_slice(&(0xE000 | pmt_pid).to_be_bytes());
    }
    section(0x00, tsid, 0, &rest)
}

fn pmt_section(program_number: u16, pcr_pid: u16, es_pids: &[u16]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&(0xE000 | pcr_pid).to_be_bytes());
    rest.extend_from_slice(&[0xF0, 0x00]);
    for &pid in es_pids {
        rest.extend_from_slice(&[0x1B, (0xE0 | (pid >> 8)) as u8, pid as u8, 0xF0, 0x00]);
    }
    section(0x02, program_number, 0, &rest)
}

/// SDT with one service per entry, using a plain ASCII name so the test does
/// not depend on ARIB text decoding.
fn sdt_section(original_network_id: u16, tsid: u16, services: &[(u16, u8, &str)]) -> Vec<u8> {
    let mut loop_rest = Vec::new();
    for &(service_id, service_type, name) in services {
        // service_type, empty provider name, then the service name. The ARIB
        // text decoder needs an explicit G0 designation before ASCII bytes.
        let mut encoded = vec![0x1B, 0x28, 0x4A];
        encoded.extend_from_slice(name.as_bytes());
        let mut descriptor = vec![service_type, 0x00, encoded.len() as u8];
        descriptor.extend_from_slice(&encoded);
        loop_rest.extend_from_slice(&service_id.to_be_bytes());
        loop_rest.push(0xFD);
        let loop_length = 2 + descriptor.len();
        loop_rest.extend_from_slice(&[((loop_length >> 8) & 0x0F) as u8, loop_length as u8]);
        loop_rest.push(0x48);
        loop_rest.push(descriptor.len() as u8);
        loop_rest.extend_from_slice(&descriptor);
    }
    // The section header already carries TSID; the body is the original
    // network id, one reserved byte and then the services directly (an SDT has
    // no descriptor-loop length field).
    let mut rest = vec![
        (original_network_id >> 8) as u8,
        original_network_id as u8,
        0xFF,
    ];
    rest.extend_from_slice(&loop_rest);
    section(0x42, tsid, 0, &rest)
}

fn nit_section(network_id: u16) -> Vec<u8> {
    let mut rest = vec![0xF0, 0x00];
    rest.extend_from_slice(&[0xF0, 0x00]);
    section(0x40, network_id, 0, &rest)
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
        packets.extend_from_slice(&packet(pid, first, cc, &payload));
        cc = cc.wrapping_add(1) & 0x0F;
        first = false;
    }
    packets
}

fn es_packet(pid: u16, marker: u8) -> Vec<u8> {
    let mut payload = vec![marker; TS_PACKET_SIZE - 4];
    payload[0] = 0x00;
    payload[1] = 0x00;
    payload[2] = 0x01;
    packet(pid, false, 0, &payload)
}

/// A three-programme multiplex used for the service-stream tests.
const TARGET_SID: u16 = 0x0401;
const NETWORK_ID: u16 = 0x7FE0;
const TSID: u16 = 0x7FE0;

fn service_multiplex() -> Vec<u8> {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(
        0x0000,
        &pat_section(
            TSID,
            &[(0x0400, 0x0100), (TARGET_SID, 0x0101), (0x0402, 0x0102)],
        ),
    ));
    stream.extend_from_slice(&section_packets(
        0x0100,
        &pmt_section(0x0400, 0x01A0, &[0x0120]),
    ));
    stream.extend_from_slice(&section_packets(
        0x0101,
        &pmt_section(TARGET_SID, 0x01A1, &[0x0121, 0x0122]),
    ));
    stream.extend_from_slice(&section_packets(
        0x0102,
        &pmt_section(0x0402, 0x01A2, &[0x0123]),
    ));
    for (pid, marker) in [
        (0x0120u16, 0xB0u8),
        (0x0121, 0xB1),
        (0x0122, 0xB2),
        (0x0123, 0xB3),
        (0x01A1, 0xB4),
    ] {
        stream.extend_from_slice(&es_packet(pid, marker));
    }
    stream.extend_from_slice(&packet(0x1FFF, false, 0, &[0xFF; TS_PACKET_SIZE - 4]));
    stream
}

/// A scan fixture: PAT + PMT + SDT + NIT for two services on one multiplex.
fn scan_multiplex() -> Vec<u8> {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(
        0x0000,
        &pat_section(TSID, &[(0x0408, 0x01F0), (0x0409, 0x01F1)]),
    ));
    stream.extend_from_slice(&section_packets(
        0x01F0,
        &pmt_section(0x0408, 0x1F1, &[0x1F2]),
    ));
    stream.extend_from_slice(&section_packets(
        0x01F1,
        &pmt_section(0x0409, 0x1F3, &[0x1F4]),
    ));
    stream.extend_from_slice(&section_packets(
        0x0011,
        &sdt_section(
            NETWORK_ID,
            TSID,
            &[(0x0408, 0x01, "NHK General"), (0x0409, 0x01, "NHK E")],
        ),
    ));
    stream.extend_from_slice(&section_packets(0x0010, &nit_section(NETWORK_ID)));
    stream.extend_from_slice(&es_packet(0x1F2, 0xC0));
    stream
}

// ---------------------------------------------------------------------------
// Config fixture
// ---------------------------------------------------------------------------

const SERVER: &str = r#"
addr: ":0"
allowed_cidrs:
  - 127.0.0.0/8
idle_timeout_secs: 1
"#;

struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl Fixture {
    fn new(tuners: &str, channels: &str, pipelines: &str) -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", SERVER.to_string()),
            ("tuners.yml", tuners.to_string()),
            ("channels.yml", channels.to_string()),
            ("pipelines.yml", pipelines.to_string()),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        Self {
            path: dir.path().to_path_buf(),
            _dir: dir,
        }
    }

    fn write_ts(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.path.join(name);
        std::fs::write(&path, bytes).expect("write fixture ts");
        path
    }

    fn config(&self) -> Config {
        Config::load(&self.path).expect("config loads")
    }

    fn state(&self) -> AppState {
        self.state_with(ServiceTable::new())
    }

    fn state_with(&self, services: ServiceTable) -> AppState {
        AppState::new(
            std::sync::Arc::new(self.config()),
            services,
            self.path.clone(),
        )
    }

    fn channels_yml(&self) -> String {
        std::fs::read_to_string(self.path.join("channels.yml")).expect("read channels.yml")
    }
}

fn tuners(command: &str) -> String {
    format!("- name: t0\n  types: [GR]\n  command: '{command}'\n")
}

fn channels(entries: &[(&str, &str)]) -> String {
    entries
        .iter()
        .map(|(name, channel)| format!("- name: {name}\n  type: GR\n  channel: \"{channel}\"\n"))
        .collect()
}

const PIPELINE_CAT: &str = "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n";

/// `sh -c "cat <path>; sleep 30"`: streams the fixture and keeps the pipe open,
/// so the session stays alive like a real tuner.
fn streaming_command(path: &Path) -> String {
    format!("sh -c \"cat {}; sleep 30\"", path.display())
}

fn packet_bytes(seed: u8) -> Vec<u8> {
    let mut packet = vec![seed; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

fn request(method: Method, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn request_with_headers(method: Method, uri: &str, headers: &[(&str, &str)]) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(Body::empty()).expect("request")
}

/// Sends one request through the router with the peer address `peer`.
async fn send(
    state: &AppState,
    peer: SocketAddr,
    request: Request<Body>,
) -> axum::response::Response {
    router(state.clone())
        .layer(axum::Extension(ConnectInfo(peer)))
        .oneshot(request)
        .await
        .expect("infallible")
}

/// Sends a request and reads the whole body.
async fn send_body(
    state: &AppState,
    peer: SocketAddr,
    request: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
    let response = send(state, peer, request).await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = read_all(response.into_body()).await;
    (status, headers, body)
}

async fn read_all(body: Body) -> Vec<u8> {
    let collected = tokio::time::timeout(Duration::from_secs(20), body.collect())
        .await
        .expect("body did not finish")
        .expect("body read");
    collected.to_bytes().to_vec()
}

/// Reads the first data frame of a possibly endless body.
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

fn json(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).expect("valid JSON body")
}

fn header_str<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}: {headers:?}"))
        .to_str()
        .expect("utf-8 header")
}

fn assert_server_identity(headers: &axum::http::HeaderMap) {
    assert_eq!(
        header_str(headers, "server"),
        concat!("unituner/", env!("CARGO_PKG_VERSION"))
    );
}

// ---------------------------------------------------------------------------
// Metadata endpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn version_reports_the_unituner_version_and_identity() {
    let fixture = Fixture::new(&tuners("true"), &channels(&[]), PIPELINE_CAT);
    let state = fixture.state();
    let (status, headers, body) =
        send_body(&state, PEER, request(Method::GET, "/api/version")).await;

    assert_eq!(status, StatusCode::OK);
    assert_server_identity(&headers);
    let value = json(&body);
    assert_eq!(value["current"], env!("CARGO_PKG_VERSION"));
    assert_eq!(value["latest"], value["current"]);
    // SPEC §2: the body never claims to be Mirakurun.
    assert!(!String::from_utf8_lossy(&body).contains("Mirakurun"));
}

#[tokio::test]
async fn status_and_metadata_endpoints_are_mirakurun_shaped() {
    let fixture = Fixture::new(
        &tuners("true"),
        &channels(&[("NHK", "27"), ("E-TV", "26")]),
        PIPELINE_CAT,
    );
    let state = fixture.state();

    let (status, headers, body) =
        send_body(&state, PEER, request(Method::GET, "/api/status")).await;
    assert_eq!(status, StatusCode::OK);
    assert_server_identity(&headers);
    let value = json(&body);
    assert_eq!(value["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(value["tunerCount"], 1);
    assert_eq!(value["epg"]["errorCount"], 0);
    assert!(!String::from_utf8_lossy(&body).contains("Mirakurun"));

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/channels")).await;
    assert_eq!(status, StatusCode::OK);
    let channels = json(&body);
    assert_eq!(channels.as_array().expect("array").len(), 2);
    assert_eq!(channels[0]["type"], "GR");
    assert_eq!(channels[0]["channel"], "27");
    assert_eq!(channels[0]["services"], serde_json::json!([]));

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/channels/GR")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body).as_array().expect("array").len(), 2);

    let (status, _, body) =
        send_body(&state, PEER, request(Method::GET, "/api/channels/GR/27")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["name"], "NHK");

    let (status, _, _) = send_body(&state, PEER, request(Method::GET, "/api/channels/BS/1")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/tuners")).await;
    assert_eq!(status, StatusCode::OK);
    let tuners = json(&body);
    assert_eq!(tuners[0]["index"], 0);
    assert_eq!(tuners[0]["name"], "t0");
    assert_eq!(tuners[0]["isAvailable"], true);
    assert_eq!(tuners[0]["isFree"], true);
    assert_eq!(tuners[0]["isFault"], false);
    assert_eq!(tuners[0]["users"], serde_json::json!([]));

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/tuners/0")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body)["name"], "t0");

    let (status, _, _) = send_body(&state, PEER, request(Method::GET, "/api/tuners/7")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/programs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json(&body), serde_json::json!([]));

    let (status, _, body) =
        send_body(&state, PEER, request(Method::GET, "/api/programs/123")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let error = json(&body);
    assert_eq!(error["code"], 404);
    assert_eq!(error["errors"], serde_json::json!([]));
}

#[tokio::test]
async fn services_report_the_spec_fields() {
    let fixture = Fixture::new(&tuners("true"), &channels(&[("NHK", "27")]), PIPELINE_CAT);
    let services = ServiceTable::new();
    let mut record = ServiceRecord::new(NETWORK_ID, TSID, TARGET_SID, 0x01, "NHK General");
    record.channel = Some(("GR".to_string(), "27".to_string()));
    record.remote_control_key_id = Some(3);
    services.upsert(record);
    let state = fixture.state_with(services);

    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/services")).await;
    assert_eq!(status, StatusCode::OK);
    let list = json(&body);
    let service = &list[0];
    assert_eq!(
        service["id"],
        NETWORK_ID as u64 * 100_000 + TARGET_SID as u64
    );
    assert_eq!(service["serviceId"], TARGET_SID);
    assert_eq!(service["networkId"], NETWORK_ID);
    assert_eq!(service["tsid"], TSID);
    assert_eq!(service["name"], "NHK General");
    assert_eq!(service["type"], 0x01);
    assert_eq!(service["logoId"], 0);
    assert_eq!(service["hasLogoData"], false);
    assert_eq!(service["remoteControlKeyId"], 3);
    assert_eq!(service["channel"]["type"], "GR");
    assert_eq!(service["channel"]["channel"], "27");

    // A service item is also embedded in its channel.
    let (_, _, body) = send_body(&state, PEER, request(Method::GET, "/api/channels/GR/27")).await;
    let embedded = &json(&body)["services"][0];
    assert_eq!(embedded["serviceId"], TARGET_SID);
    assert_eq!(embedded["type"], 0x01);

    // Item id and bare sid both resolve, an unknown one is a 404.
    for id in [
        format!("{}", NETWORK_ID as u64 * 100_000 + TARGET_SID as u64),
        TARGET_SID.to_string(),
    ] {
        let (status, _, body) = send_body(
            &state,
            PEER,
            request(Method::GET, &format!("/api/services/{id}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "id {id}");
        assert_eq!(json(&body)["serviceId"], TARGET_SID);
    }
    let (status, _, _) = send_body(&state, PEER, request(Method::GET, "/api/services/9999")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unknown_routes_and_scan_polling_are_404() {
    let fixture = Fixture::new(&tuners("true"), &channels(&[]), PIPELINE_CAT);
    let state = fixture.state();

    for uri in ["/api/unknown", "/api/config/channels/scan", "/rpc"] {
        let (status, headers, body) = send_body(&state, PEER, request(Method::GET, uri)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_server_identity(&headers);
        assert_eq!(json(&body)["code"], 404);
    }
}

#[tokio::test]
async fn peers_outside_allowed_cidrs_are_forbidden() {
    let fixture = Fixture::new(&tuners("true"), &channels(&[]), PIPELINE_CAT);
    let state = fixture.state();
    let outside = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 5)),
        1,
    );

    let (status, headers, body) =
        send_body(&state, outside, request(Method::GET, "/api/version")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_server_identity(&headers);
    assert_eq!(json(&body)["code"], 403);
}

// ---------------------------------------------------------------------------
// Channel stream
// ---------------------------------------------------------------------------

fn stream_fixture() -> (Fixture, Vec<u8>) {
    let fixture = Fixture::new(&tuners("true"), &channels(&[("NHK", "27")]), PIPELINE_CAT);
    let ts = [packet_bytes(0x11), packet_bytes(0x22), packet_bytes(0x33)].concat();
    let path = fixture.write_ts("stream.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&path)),
    )
    .expect("rewrite tuners.yml");
    (fixture, ts)
}

#[tokio::test]
async fn the_channel_stream_returns_the_post_pipeline_bytes() {
    let (fixture, ts) = stream_fixture();
    let state = fixture.state();

    let response = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_server_identity(headers);
    assert_eq!(header_str(headers, "content-type"), "video/MP2T");
    assert_eq!(header_str(headers, "cache-control"), "no-store");
    assert_eq!(header_str(headers, TUNER_USER_ID), PEER.to_string());

    let first = read_first_chunk(response.into_body()).await;
    assert_eq!(first, ts);
}

#[tokio::test]
async fn head_returns_the_stream_headers_without_a_body() {
    let (fixture, _) = stream_fixture();
    let state = fixture.state();

    let response = send(
        &state,
        PEER,
        request(Method::HEAD, "/api/channels/GR/27/stream"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_server_identity(response.headers());
    assert_eq!(header_str(response.headers(), "content-type"), "video/MP2T");
    assert_eq!(
        header_str(response.headers(), TUNER_USER_ID),
        PEER.to_string()
    );
    assert!(read_all(response.into_body()).await.is_empty());

    // The probe never tuned a device.
    let tuners = send_body(&state, PEER, request(Method::GET, "/api/tuners"))
        .await
        .2;
    assert_eq!(json(&tuners)[0]["isUsing"], false);
}

#[tokio::test]
async fn head_of_a_service_stream_has_the_stream_headers() {
    let (fixture, _, services) = service_fixture();
    let state = fixture.state_with(services);

    for uri in [
        format!("/api/services/{TARGET_SID}/stream"),
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
    ] {
        let response = send(&state, PEER, request(Method::HEAD, &uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_server_identity(response.headers());
        assert_eq!(header_str(response.headers(), "content-type"), "video/MP2T");
        assert_eq!(
            header_str(response.headers(), TUNER_USER_ID),
            PEER.to_string()
        );
        assert!(read_all(response.into_body()).await.is_empty(), "{uri}");
    }
    let tuners = send_body(&state, PEER, request(Method::GET, "/api/tuners"))
        .await
        .2;
    assert_eq!(json(&tuners)[0]["isUsing"], false);
}

#[tokio::test]
async fn head_of_an_unknown_service_is_404() {
    let (fixture, _, services) = service_fixture();
    let state = fixture.state_with(services);

    for uri in [
        "/api/services/9999/stream",
        "/api/channels/GR/27/services/9999/stream",
    ] {
        let response = send(&state, PEER, request(Method::HEAD, uri)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        assert_server_identity(response.headers());
    }
}

#[tokio::test]
async fn decode_zero_bypasses_the_whole_pipeline() {
    let ts = [packet_bytes(0x44), packet_bytes(0x55), packet_bytes(0x66)].concat();
    // The pipeline appends a marker *packet*, so "pipeline ran" and "pipeline
    // bypassed" are distinguishable even though both stream 188-byte packets.
    // The marker must be a whole packet: a subscriber's byte stream is only ever
    // whole TS packets, so the pump discards an incomplete trailing packet.
    let pipelines = "pipelines:\n  pass:\n    - \"sh -c 'cat; printf %0188d 0 | tr 0 P'\"\ntypes:\n  GR: { pipeline: pass }\n";
    let fixture = Fixture::new(&tuners("true"), &channels(&[("NHK", "27")]), pipelines);
    let path = fixture.write_ts("stream.ts", &ts);
    // A finite tuner command: the pipeline marker then arrives at EOF, which
    // lets both bodies be read to completion.
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&format!("cat {}", path.display())),
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let response = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream"),
    )
    .await;
    // The pipeline stage appends its marker packet, so the body is the fixture
    // plus one 188-byte packet of `P`.
    let with_pipeline = read_all(response.into_body()).await;
    let marker = vec![b'P'; TS_PACKET_SIZE];
    assert_eq!(with_pipeline, [ts.clone(), marker].concat());

    // `decode=0` must never spawn the pipeline stages: the bytes come straight
    // from the (normalized) tuner output.
    let response = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream?decode=0"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let raw = read_first_chunk(response.into_body()).await;
    assert_eq!(raw, ts);
}

// ---------------------------------------------------------------------------
// Service stream
// ---------------------------------------------------------------------------

fn service_fixture() -> (Fixture, Vec<u8>, ServiceTable) {
    let fixture = Fixture::new(&tuners("true"), &channels(&[("NHK", "27")]), PIPELINE_CAT);
    let ts = service_multiplex();
    let path = fixture.write_ts("multiplex.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&path)),
    )
    .expect("rewrite tuners.yml");

    let services = ServiceTable::new();
    for (sid, name) in [
        (TARGET_SID, "NHK General"),
        (0x0400, "Other A"),
        (0x0402, "Other B"),
    ] {
        let mut record = ServiceRecord::new(NETWORK_ID, TSID, sid, 0x01, name);
        record.channel = Some(("GR".to_string(), "27".to_string()));
        services.upsert(record);
    }
    (fixture, ts, services)
}

#[tokio::test]
async fn the_service_stream_is_the_tsfilter_output() {
    let (fixture, ts, services) = service_fixture();
    let state = fixture.state_with(services);
    let expected = ServiceFilter::new(TARGET_SID).push(&ts);
    assert!(!expected.is_empty());

    for uri in [
        format!("/api/services/{TARGET_SID}/stream"),
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
        format!(
            "/api/services/{}/stream",
            NETWORK_ID as u64 * 100_000 + TARGET_SID as u64
        ),
    ] {
        let response = send(&state, PEER, request(Method::GET, &uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert_eq!(header_str(response.headers(), "content-type"), "video/MP2T");
        let bytes = read_first_chunk(response.into_body()).await;
        assert_eq!(bytes, expected, "{uri}");
    }
}

#[tokio::test]
async fn a_decode_zero_service_stream_still_filters() {
    // `decode=0` bypasses the pipeline but keeps the built-in TSFilter
    // (SPEC §4.1), so the bytes are the filter output over the raw tuner TS.
    let ts = service_multiplex();
    let pipelines = "pipelines:\n  pass:\n    - \"sh -c 'cat; printf PIPE'\"\ntypes:\n  GR: { pipeline: pass }\n";
    let fixture = Fixture::new(&tuners("true"), &channels(&[("NHK", "27")]), pipelines);
    let path = fixture.write_ts("multiplex.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&format!("cat {}", path.display())),
    )
    .expect("rewrite tuners.yml");
    let services = ServiceTable::new();
    let mut record = ServiceRecord::new(NETWORK_ID, TSID, TARGET_SID, 0x01, "NHK General");
    record.channel = Some(("GR".to_string(), "27".to_string()));
    services.upsert(record);
    let state = fixture.state_with(services);

    let expected = ServiceFilter::new(TARGET_SID).push(&ts);
    let response = send(
        &state,
        PEER,
        request(
            Method::GET,
            &format!("/api/services/{TARGET_SID}/stream?decode=0"),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = read_all(response.into_body()).await;
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn service_resolution_comes_before_tuner_validation() {
    let (fixture, _, services) = service_fixture();
    let state = fixture.state_with(services);

    // A bogus service id and a bogus pinned tuner: 404 wins (SPEC §6.3).
    let (status, _, body) = send_body(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/27/services/9999/stream",
            &[("x-mirakc-tuner", "nope")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json(&body)["code"], 404);

    // A real service with a bogus pinned tuner: 400.
    let (status, _, _) = send_body(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            &format!("/api/services/{TARGET_SID}/stream"),
            &[("x-mirakc-tuner", "nope")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Busy / takeover / pinning
// ---------------------------------------------------------------------------

/// Two channels with stable per-channel fixtures.
fn two_channel_fixture() -> (Fixture, Vec<u8>, Vec<u8>) {
    let fixture = Fixture::new(
        &tuners("true"),
        &channels(&[("A", "27"), ("B", "28")]),
        PIPELINE_CAT,
    );
    let ts27 = [packet_bytes(0x27), packet_bytes(0xA7)].concat();
    let ts28 = [packet_bytes(0x28), packet_bytes(0xB8)].concat();
    fixture.write_ts("27.ts", &ts27);
    fixture.write_ts("28.ts", &ts28);
    // One tuner whose command streams the fixture of the requested channel.
    let command = format!(
        "sh -c \"cat {0}/{{channel}}.ts; sleep 30\"",
        fixture.path.display()
    );
    std::fs::write(fixture.path.join("tuners.yml"), tuners(&command)).expect("rewrite tuners.yml");
    (fixture, ts27, ts28)
}

#[tokio::test]
async fn a_second_channel_is_503_when_the_only_tuner_is_busy() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    let held = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream"),
    )
    .await;
    assert_eq!(held.status(), StatusCode::OK);
    // Keep the body alive: dropping it would close the subscription and free
    // the tuner before the second request arrives.
    let _held_body = held.into_body();

    let (status, headers, body) = send_body(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/28/stream"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_server_identity(&headers);
    let error = json(&body);
    assert_eq!(error["code"], 503);
    assert_eq!(error["reason"], "Tuner Resource Unavailable");

    // The users of the busy tuner are reported by `/api/tuners`.
    let (_, _, body) = send_body(&state, PEER, request(Method::GET, "/api/tuners")).await;
    let tuner = &json(&body)[0];
    assert_eq!(tuner["isUsing"], true);
    assert_eq!(tuner["users"][0]["id"], PEER.to_string());
    assert_eq!(tuner["users"][0]["priority"], 0);
    drop(body);
}

#[tokio::test]
async fn a_higher_priority_takes_over_the_tuner() {
    let (fixture, _, ts28) = two_channel_fixture();
    let state = fixture.state();

    let held = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream"),
    )
    .await;
    assert_eq!(held.status(), StatusCode::OK);

    let response = send(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/28/stream",
            &[("x-mirakurun-priority", "10")],
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let served = read_first_chunk(response.into_body()).await;
    assert_eq!(served, ts28, "the takeover must serve the new channel");

    // The victim's session was replaced by the new channel.
    let (_, _, body) = send_body(&state, PEER, request(Method::GET, "/api/tuners")).await;
    let tuner = &json(&body)[0];
    assert_eq!(tuner["users"][0]["priority"], 10);
    drop(body);
}

#[tokio::test]
async fn pinning_an_unknown_tuner_is_400_on_every_stream_route() {
    let (fixture, _, services) = service_fixture();
    let state = fixture.state_with(services);

    for uri in [
        "/api/channels/GR/27/stream".to_string(),
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
        format!("/api/services/{TARGET_SID}/stream"),
    ] {
        let (status, _, body) = send_body(
            &state,
            PEER,
            request_with_headers(Method::GET, &uri, &[("x-mirakc-tuner", "nope")]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(json(&body)["code"], 400);
        let (status, _, _) = send_body(
            &state,
            PEER,
            request_with_headers(Method::GET, &uri, &[("x-mirakurun-tuner", "nope")]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "alias {uri}");
    }
}

#[tokio::test]
async fn a_pinned_busy_tuner_without_priority_is_503() {
    let (fixture, ts27, _) = two_channel_fixture();
    let state = fixture.state();

    // Pinning with the correct name starts the session.
    let response = send(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/27/stream",
            &[("x-mirakc-tuner", "t0")],
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(read_first_chunk(response.into_body()).await, ts27);

    // Pinned on the same (busy) tuner, equal priority: no takeover, no
    // fallback, so 503.
    let (status, _, body) = send_body(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/28/stream",
            &[("x-mirakurun-tuner", "t0")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json(&body)["reason"], "Tuner Resource Unavailable");
}

#[tokio::test]
async fn the_last_priority_header_wins() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    // priority 10 first, then 0: the last value wins, so no takeover happens
    // and the second channel gets 503 (SPEC §6.1, single value assumed).
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/channels/GR/27/stream")
        .header("x-mirakurun-priority", "10")
        .header("x-mirakurun-priority", "0")
        .body(Body::empty())
        .expect("request");
    let response = send(&state, PEER, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    // Keep the stream open while the second request is made.
    let _held_body = response.into_body();

    let (status, _, _) = send_body(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/28/stream",
            &[("x-mirakurun-priority", "0")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // `/api/tuners` shows the priority that was actually applied.
    let (_, _, body) = send_body(
        &state,
        PEER,
        axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/tuners")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(json(&body)[0]["users"][0]["priority"], 0);
}

#[tokio::test]
async fn x_mirakc_tuner_takes_precedence_over_its_alias() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    // `X-Mirakc-Tuner: t0` (valid) must win over the bogus alias, so the
    // request starts instead of failing with 400 (SPEC §6.3).
    let request = Request::builder()
        .method(Method::GET)
        .uri("/api/channels/GR/27/stream")
        .header("x-mirakurun-tuner", "nope")
        .header("x-mirakc-tuner", "t0")
        .body(Body::empty())
        .expect("request");
    let response = send(&state, PEER, request).await;
    assert_eq!(response.status(), StatusCode::OK);
    let _held_body = response.into_body();
}

#[tokio::test]
async fn an_empty_pin_header_is_ignored() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    // A whitespace-only pin is ignored, so the request tunes the only tuner
    // instead of failing with 400. The stream itself never ends, so only the
    // response head is inspected.
    let response = send(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/27/stream",
            &[("x-mirakc-tuner", "   ")],
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let _held_body = response.into_body();
}

#[tokio::test]
async fn an_invalid_channel_value_is_400() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    let (status, _, body) = send_body(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/2%3B7/stream"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json(&body)["code"], 400);
}

#[tokio::test]
async fn head_matches_get_validation_for_the_stream_routes() {
    let (fixture, _, services) = service_fixture();
    let state = fixture.state_with(services);

    let routes = [
        "/api/channels/GR/27/stream".to_string(),
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
        format!("/api/services/{TARGET_SID}/stream"),
    ];
    for uri in &routes {
        // An unknown pin is a 400 on GET *and* HEAD.
        for method in [Method::GET, Method::HEAD] {
            let response = send(
                &state,
                PEER,
                request_with_headers(method.clone(), uri, &[("x-mirakc-tuner", "nope")]),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{method} {uri} with an unknown pin"
            );
        }

        // A malformed priority is a 400 on GET *and* HEAD.
        for method in [Method::GET, Method::HEAD] {
            let response = send(
                &state,
                PEER,
                request_with_headers(method.clone(), uri, &[("x-mirakurun-priority", "high")]),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{method} {uri} with a malformed priority"
            );
        }

        // A valid pin still succeeds on both (and HEAD never tunes).
        for method in [Method::GET, Method::HEAD] {
            let response = send(
                &state,
                PEER,
                request_with_headers(method.clone(), uri, &[("x-mirakc-tuner", "t0")]),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
            if method == Method::GET {
                // Drain the head so the session is released.
                let _ = read_first_chunk(response.into_body()).await;
            } else {
                assert!(read_all(response.into_body()).await.is_empty(), "{uri}");
            }
        }
    }
}

#[tokio::test]
async fn a_malformed_priority_header_is_400() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    let (status, _, _) = send_body(
        &state,
        PEER,
        request_with_headers(
            Method::GET,
            "/api/channels/GR/27/stream",
            &[("x-mirakurun-priority", "high")],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Channel scan (SPEC §8.1)
// ---------------------------------------------------------------------------

/// A scan fixture whose single tuner command streams `scan.ts`.
fn scan_fixture() -> (Fixture, Vec<u8>) {
    let fixture = Fixture::new(&tuners("true"), "[]\n", PIPELINE_CAT);
    let ts = scan_multiplex();
    let path = fixture.write_ts("scan.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&path)),
    )
    .expect("rewrite tuners.yml");
    (fixture, ts)
}

const SCAN_RANGE: &str = "type=GR&minCh=24&maxCh=24";

#[tokio::test]
async fn dry_run_reports_progress_and_writes_nothing() {
    let (fixture, _) = scan_fixture();
    let before = fixture.channels_yml();
    let state = fixture.state();

    let (status, headers, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            &format!("/api/config/channels/scan?{SCAN_RANGE}&dryRun=true"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_server_identity(&headers);
    assert!(
        header_str(&headers, "content-type").starts_with("text/plain"),
        "scan progress is text/plain"
    );
    let text = String::from_utf8(body).expect("utf-8 progress");
    assert!(text.starts_with("channel scanning..."), "{text}");
    assert!(
        text.contains("channel scanning... (type: \"GR\")"),
        "{text}"
    );
    assert!(text.contains("2 services found"), "{text}");
    assert!(text.contains("dry run"), "{text}");
    assert!(
        text.contains("**RESTART REQUIRED** to apply changes."),
        "{text}"
    );
    assert_eq!(fixture.channels_yml(), before, "dryRun must not write");
}

#[tokio::test]
async fn a_scan_appends_entries_and_keeps_other_types() {
    let (fixture, _) = scan_fixture();
    std::fs::write(
        fixture.path.join("channels.yml"),
        "- name: Existing BS\n  type: BS\n  channel: \"BS01_0\"\n",
    )
    .expect("seed channels.yml");
    let ts = scan_multiplex();
    let path = fixture.write_ts("scan.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&path)),
    )
    .expect("rewrite tuners.yml");
    let state = fixture.state();

    let (status, _, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            &format!("/api/config/channels/scan?{SCAN_RANGE}"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf-8 progress");
    assert!(text.contains("**RESTART REQUIRED**"), "{text}");

    let written = fixture.channels_yml();
    assert!(written.contains("type: BS"), "{written}");
    assert!(written.contains("type: GR"), "{written}");
    assert_eq!(written.matches("channel: '24'").count(), 1, "{written}");
    assert!(written.contains("NHK General"), "{written}");

    // The same channels.yml still passes the startup validation.
    let config = fixture.config();
    assert_eq!(config.channels.len(), 2);

    // SPEC §13: the scan result is visible through `/api/services`, and the
    // found services are bound to the scanned channel.
    let (status, _, body) = send_body(&state, PEER, request(Method::GET, "/api/services")).await;
    assert_eq!(status, StatusCode::OK);
    let found = json(&body);
    let found = found.as_array().expect("array");
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(found[0]["name"], "NHK General");
    assert_eq!(found[0]["channel"]["type"], "GR");
    assert_eq!(found[0]["channel"]["channel"], "24");
}

#[tokio::test]
async fn a_running_scan_makes_the_next_one_a_409() {
    let (fixture, _) = scan_fixture();
    let state = fixture.state();
    // Hold the scan lock the way a running scan does for its whole duration.
    let lease = state.scan_lock.acquire().expect("scan lock");

    let (status, headers, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            &format!("/api/config/channels/scan?{SCAN_RANGE}"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_server_identity(&headers);
    let error = json(&body);
    assert_eq!(error["code"], 409);
    assert_eq!(error["reason"], "Already Scanning");

    // Releasing the lock admits the next scan again.
    drop(lease);
    assert!(!state.scan_lock.is_running());
}

#[tokio::test]
async fn an_invalid_scan_query_is_400() {
    let (fixture, _) = scan_fixture();
    let state = fixture.state();

    for query in ["", "type=GR&minCh=x", "type=GR&scanMode=Bogus"] {
        let (status, _, body) = send_body(
            &state,
            PEER,
            request(Method::PUT, &format!("/api/config/channels/scan?{query}")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "query {query:?}");
        assert_eq!(json(&body)["code"], 400);
    }
}

#[tokio::test]
async fn a_huge_scan_range_is_400_without_allocating() {
    let (fixture, _) = scan_fixture();
    let state = fixture.state();
    let before = fixture.channels_yml();

    let (status, _, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            "/api/config/channels/scan?type=GR&minCh=0&maxCh=4294967295",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = json(&body);
    assert_eq!(error["code"], 400);
    assert!(
        error["reason"]
            .as_str()
            .expect("reason")
            .contains("at most"),
        "{error}"
    );
    assert_eq!(fixture.channels_yml(), before, "must not write");
    // The lock was never taken, so the next scan is not a 409.
    assert!(!state.scan_lock.is_running());
}

#[tokio::test]
async fn a_scan_range_exactly_at_the_cap_is_allowed() {
    let (fixture, _) = scan_fixture();
    let state = fixture.state();

    // 512 candidates is the cap. No tuner supports the `XX` type, so each
    // channel is skipped as "tuner busy" without spawning anything.
    let (status, _, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            "/api/config/channels/scan?type=XX&minCh=0&maxCh=511&dryRun=true",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf-8 progress");
    assert!(text.contains("dry run"), "{text}");
    assert!(
        text.matches("skipped (tuner busy)").count() == 512,
        "{text}"
    );
}

// ---------------------------------------------------------------------------
// Channel-scoped service resolution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_sid_on_two_channels_resolves_on_the_channel_path() {
    let fixture = Fixture::new(
        &tuners("true"),
        &channels(&[("A", "27"), ("B", "28")]),
        PIPELINE_CAT,
    );
    let ts = service_multiplex();
    let path = fixture.write_ts("multiplex.ts", &ts);
    // Two tuners so both channels can stream at once: the second request must
    // not be a 503 on a single-tuner pool.
    let command = streaming_command(&path);
    let second = format!("- name: t1\n  types: [GR]\n  command: '{command}'\n");
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("{}{}", tuners(&command), second),
    )
    .expect("rewrite tuners.yml");

    let services = ServiceTable::new();
    for channel in ["27", "28"] {
        let mut record = ServiceRecord::new(NETWORK_ID, TSID, TARGET_SID, 0x01, "Shared");
        record.channel = Some(("GR".to_string(), channel.to_string()));
        services.upsert(record);
    }
    let state = fixture.state_with(services);

    // The global lookup is ambiguous (404), the channel path is not.
    let (status, _, _) = send_body(
        &state,
        PEER,
        request(Method::GET, &format!("/api/services/{TARGET_SID}/stream")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for uri in [
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
        format!("/api/channels/GR/28/services/{TARGET_SID}/stream"),
    ] {
        let response = send(&state, PEER, request(Method::GET, &uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        // Consume the head so the body is not dropped mid-stream.
        let bytes = read_first_chunk(response.into_body()).await;
        assert!(!bytes.is_empty(), "{uri}");
    }

    // HEAD follows the same resolution and the same validation.
    for uri in [
        format!("/api/channels/GR/27/services/{TARGET_SID}/stream"),
        format!("/api/channels/GR/28/services/{TARGET_SID}/stream"),
    ] {
        let response = send(&state, PEER, request(Method::HEAD, &uri)).await;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        assert!(read_all(response.into_body()).await.is_empty(), "{uri}");
    }
    let response = send(
        &state,
        PEER,
        request(
            Method::HEAD,
            &format!("/api/channels/GR/99/services/{TARGET_SID}/stream"),
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // A malformed channel is validated like the GET: resolution is
    // channel-scoped, so no service can be bound to an invalid channel and both
    // methods answer the same way.
    let mut statuses = Vec::new();
    for method in [Method::GET, Method::HEAD] {
        let response = send(
            &state,
            PEER,
            request(
                method.clone(),
                &format!("/api/channels/GR/2%3B7/services/{TARGET_SID}/stream"),
            ),
        )
        .await;
        statuses.push(response.status());
    }
    assert_eq!(statuses[0], statuses[1], "HEAD must match GET");
    assert_eq!(statuses[0], StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn the_ipv6_loopback_is_allowed_by_the_ipv4_loopback_entry() {
    let fixture = Fixture::new(&tuners("true"), &channels(&[]), PIPELINE_CAT);
    let state = fixture.state();
    let loopback = SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST), 1);

    let (status, _, _) = send_body(&state, loopback, request(Method::GET, "/api/version")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a v6 loopback peer must not be forbidden by 127.0.0.0/8"
    );
}

#[tokio::test]
async fn a_decode_zero_request_for_a_piped_channel_is_409() {
    let (fixture, _, _) = two_channel_fixture();
    let state = fixture.state();

    // Start the normal (piped) stream and keep its body alive.
    let held = send(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream"),
    )
    .await;
    assert_eq!(held.status(), StatusCode::OK);
    let _held_body = held.into_body();

    let (status, _, body) = send_body(
        &state,
        PEER,
        request(Method::GET, "/api/channels/GR/27/stream?decode=0"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let error = json(&body);
    assert_eq!(error["code"], 409);
    assert!(
        error["reason"]
            .as_str()
            .expect("reason")
            .contains("different decode mode"),
        "{error}"
    );

    // The piped session is untouched: still one session, one user.
    let (_, _, tuners) = send_body(&state, PEER, request(Method::GET, "/api/tuners")).await;
    let tuners = json(&tuners);
    assert_eq!(tuners[0]["users"].as_array().expect("users").len(), 1);
}

// ---------------------------------------------------------------------------
// Scan cancellation and Service-mode refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_disconnected_scan_writes_nothing() {
    let fixture = Fixture::new(&tuners("true"), "[]\n", PIPELINE_CAT);
    let ts = scan_multiplex();
    let path = fixture.write_ts("scan.ts", &ts);
    // Slow enough that the client is gone before the last channel finishes.
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&format!(
            "sh -c \"sleep 0.3; cat {}; sleep 30\"",
            path.display()
        )),
    )
    .expect("rewrite tuners.yml");
    let before = fixture.channels_yml();
    let state = fixture.state();

    let response = send(
        &state,
        PEER,
        request(
            Method::PUT,
            "/api/config/channels/scan?type=GR&minCh=24&maxCh=29",
        ),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    // Read one frame, then hang up.
    let mut body = response.into_body();
    let _ = tokio::time::timeout(Duration::from_secs(5), body.frame()).await;
    drop(body);

    // The scan must notice and leave channels.yml alone.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        fixture.channels_yml(),
        before,
        "a cancelled scan must not write"
    );
}

#[tokio::test]
async fn a_service_mode_refresh_replaces_every_per_service_entry() {
    let fixture = Fixture::new(&tuners("true"), "[]\n", PIPELINE_CAT);
    let ts = scan_multiplex();
    let path = fixture.write_ts("scan.ts", &ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!(
            "- name: bs\n  types: [BS]\n  command: '{}\n'\n",
            streaming_command(&path)
        ),
    )
    .expect("rewrite tuners.yml");
    // Stale per-service entries of the same multiplex.
    std::fs::write(
        fixture.path.join("channels.yml"),
        "- name: Stale One\n  type: BS\n  channel: \"BS13_0\"\n\
         - name: Stale Two\n  type: BS\n  channel: \"BS13_1\"\n\
         - name: Stale Gone\n  type: BS\n  channel: \"BS13_2\"\n",
    )
    .expect("seed channels.yml");
    let state = fixture.state();

    let (status, _, body) = send_body(
        &state,
        PEER,
        request(
            Method::PUT,
            "/api/config/channels/scan?type=BS&minCh=13&maxCh=13&refresh=true",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).expect("utf-8 progress");
    assert!(text.contains("**RESTART REQUIRED**"), "{text}");

    let written = fixture.channels_yml();
    assert!(written.contains("NHK General"), "{written}");
    assert!(written.contains("NHK E"), "{written}");
    assert!(!written.contains("Stale One"), "{written}");
    assert!(!written.contains("Stale Two"), "{written}");
    assert!(
        !written.contains("Stale Gone"),
        "a disappeared service must be dropped on refresh: {written}"
    );
    // Both produced entries are present as per-service channels.
    assert_eq!(written.matches("channel: BS13_").count(), 2, "{written}");
}

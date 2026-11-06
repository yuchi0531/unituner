//! Scan-layer regressions that need the whole stack: the service table, the
//! `channels.yml` write and the progress stream.
//!
//! Covered here:
//!
//! * a refresh scan drops the services that disappeared from a multiplex but
//!   keeps the ones a no-signal channel failed to read;
//! * channel-less records are pruned only after a completed scan;
//! * a shutdown or a client disconnect leaves both the file and the table
//!   untouched (and no `.tmp` behind);
//! * a `disabled: true` entry is rescanned, stays disabled and says so;
//! * a user-defined type works with an explicit range.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use unituner::config::Config;
use unituner::scan::{run_scan, ScanQuery};
use unituner::services::{ServiceRecord, ServiceTable};
use unituner::session::SessionManager;
use unituner::si::crc32_mpeg2;
use unituner::tsframe::TS_PACKET_SIZE;

const NETWORK_ID: u16 = 0x7FE0;
const TSID: u16 = 0x7FE0;

// ---------------------------------------------------------------------------
// TS fixture builders (PAT + SDT + NIT)
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

fn section(table_id: u8, id: u16, rest: &[u8]) -> Vec<u8> {
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

fn pat_section(tsid: u16, programs: &[(u16, u16)]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&[0x00, 0x00]);
    rest.extend_from_slice(&(0xE000u16 | 0x0010).to_be_bytes());
    for &(program, pmt_pid) in programs {
        rest.extend_from_slice(&program.to_be_bytes());
        rest.extend_from_slice(&(0xE000 | pmt_pid).to_be_bytes());
    }
    section(0x00, tsid, &rest)
}

fn sdt_section(original_network_id: u16, tsid: u16, services: &[(u16, u8, &str)]) -> Vec<u8> {
    let mut loop_rest = Vec::new();
    for &(service_id, service_type, name) in services {
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
    let mut rest = vec![
        (original_network_id >> 8) as u8,
        original_network_id as u8,
        0xFF,
    ];
    rest.extend_from_slice(&loop_rest);
    section(0x42, tsid, &rest)
}

fn nit_section(network_id: u16) -> Vec<u8> {
    let mut rest = vec![0xF0, 0x00];
    rest.extend_from_slice(&[0xF0, 0x00]);
    section(0x40, network_id, &rest)
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
        cc = (cc + 1) & 0x0F;
        first = false;
    }
    packets
}

/// PAT + SDT-actual + NIT for `services`.
fn multiplex(services: &[(u16, u8, &str)]) -> Vec<u8> {
    let programs: Vec<(u16, u16)> = services
        .iter()
        .enumerate()
        .map(|(index, &(service_id, _, _))| (service_id, 0x01F0 + index as u16))
        .collect();
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(0x0000, &pat_section(TSID, &programs)));
    stream.extend_from_slice(&section_packets(
        0x0011,
        &sdt_section(NETWORK_ID, TSID, services),
    ));
    stream.extend_from_slice(&section_packets(0x0010, &nit_section(NETWORK_ID)));
    stream
}

// ---------------------------------------------------------------------------
// Config fixture
// ---------------------------------------------------------------------------

const SERVER: &str = "addr: \":0\"\nidle_timeout_secs: 1\n";

const PIPELINES: &str = "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n";

const CHANNEL_24: &str = "- name: NHK\n  type: GR\n  channel: \"24\"\n";

const PIPELINE_CAT: &str = PIPELINES;

/// A fixture directory with one tuner type and a `channels.yml` body.
struct Fixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl Fixture {
    fn new(tuners: &str, channels: &str) -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", SERVER.to_string()),
            ("tuners.yml", tuners.to_string()),
            ("channels.yml", channels.to_string()),
            ("pipelines.yml", PIPELINE_CAT.to_string()),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let path = dir.path().to_path_buf();
        Self { _dir: dir, path }
    }

    /// Writes `body` as a stream the tuner command prints.
    fn tuner(&self, channel_type: &str, command: &str) {
        std::fs::write(
            self.path.join("tuners.yml"),
            format!("- name: t0\n  types: [{channel_type}]\n  command: '{command}'\n"),
        )
        .expect("rewrite tuners.yml");
    }

    fn stream(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.path.join(name);
        std::fs::write(&path, bytes).expect("write stream");
        path
    }

    fn manager(&self) -> SessionManager {
        SessionManager::new(
            Arc::new(Config::load(&self.path).expect("config loads")),
            ServiceTable::new(),
        )
    }

    fn config(&self) -> Arc<Config> {
        Arc::new(Config::load(&self.path).expect("config loads"))
    }

    fn channels_yml(&self) -> String {
        std::fs::read_to_string(self.path.join("channels.yml")).unwrap_or_default()
    }

    /// Every `*.tmp` file left in the config directory.
    fn tmp_files(&self) -> Vec<PathBuf> {
        std::fs::read_dir(&self.path)
            .expect("read config dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "tmp"))
            .collect()
    }
}

fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn record(service_id: u16, name: &str, channel: Option<(&str, &str)>) -> ServiceRecord {
    let mut record = ServiceRecord::new(NETWORK_ID, TSID, service_id, 0x01, name);
    record.channel =
        channel.map(|(channel_type, channel)| (channel_type.to_string(), channel.to_string()));
    record
}

/// A query for `type` over exactly the given channel numbers.
fn query(pairs: &[(&str, &str)]) -> ScanQuery {
    let mut query = ScanQuery::from_params(&params(pairs)).expect("query");
    // Keep the tests fast: a stream that ends is enough for the scan.
    query.timeout = Duration::from_secs(5);
    query
}

/// Runs the scan, draining the progress stream, and returns the progress text.
async fn run(
    manager: &SessionManager,
    config: &Arc<Config>,
    fixture: &Fixture,
    query: &ScanQuery,
) -> String {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(64);
    let drain = tokio::spawn(async move {
        let mut text = String::new();
        while let Some(line) = rx.recv().await {
            text.push_str(&line);
        }
        text
    });
    run_scan(manager, config, &fixture.path, query, tx, None)
        .await
        .expect("scan");
    drain.await.expect("drain progress")
}

// ---------------------------------------------------------------------------
// C4: the service table follows a refresh scan
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_refresh_scan_drops_a_service_that_disappeared() {
    let fixture = Fixture::new("", CHANNEL_24);
    // The multiplex used to carry 0x0408 (NHK) and 0x0409 (E); only NHK is on
    // air now.
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner("GR", &format!("sh -c \"cat {}; sleep 30\"", ts.display()));
    let manager = fixture.manager();
    manager
        .services()
        .insert(record(0x0408, "NHK General", Some(("GR", "24"))));
    manager
        .services()
        .insert(record(0x0409, "NHK E", Some(("GR", "24"))));

    let query = query(&[
        ("type", "GR"),
        ("minCh", "24"),
        ("maxCh", "24"),
        ("refresh", "true"),
    ]);
    run(&manager, &fixture.config(), &fixture, &query).await;

    let services = manager.services().list();
    assert_eq!(
        services.len(),
        1,
        "the service that left the multiplex must be dropped: {services:?}"
    );
    assert_eq!(services[0].service_id, 0x0408);
    assert_eq!(services[0].name, "NHK General");
    manager.shutdown().await;
}

#[tokio::test]
async fn a_no_signal_channel_keeps_its_previous_services() {
    let fixture = Fixture::new("", CHANNEL_24);
    // The tuner prints nothing: the scan must not treat "no SI" as "no
    // services any more".
    fixture.tuner("GR", "sh -c \"sleep 30\"");
    let manager = fixture.manager();
    manager
        .services()
        .insert(record(0x0408, "NHK General", Some(("GR", "24"))));

    let mut query = query(&[
        ("type", "GR"),
        ("minCh", "24"),
        ("maxCh", "24"),
        ("refresh", "true"),
    ]);
    query.timeout = Duration::from_millis(400);
    let progress = run(&manager, &fixture.config(), &fixture, &query).await;
    assert!(progress.contains("no signal"), "{progress}");

    let services = manager.services().list();
    assert_eq!(services.len(), 1, "a failed scan must not wipe the table");
    assert_eq!(services[0].service_id, 0x0408);
    assert_eq!(fixture.channels_yml(), CHANNEL_24);
    manager.shutdown().await;
}

#[tokio::test]
async fn unbound_records_are_pruned_after_a_successful_scan() {
    let fixture = Fixture::new("", "");
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner("GR", &format!("sh -c \"cat {}; sleep 30\"", ts.display()));
    let manager = fixture.manager();
    // A record no channel ever claimed (an SDT-other or a stale NIT).
    manager
        .services()
        .insert(record(0x7001, "Never bound", None));

    let query = query(&[("type", "GR"), ("minCh", "24"), ("maxCh", "24")]);
    run(&manager, &fixture.config(), &fixture, &query).await;

    let services = manager.services().list();
    assert_eq!(services.len(), 1, "{services:?}");
    assert_eq!(services[0].service_id, 0x0408);
    assert_eq!(
        services[0].channel,
        Some(("GR".to_string(), "24".to_string()))
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn a_dry_run_keeps_the_table_untouched() {
    let fixture = Fixture::new("", "");
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner("GR", &format!("sh -c \"cat {}; sleep 30\"", ts.display()));
    let manager = fixture.manager();
    manager
        .services()
        .insert(record(0x7001, "Never bound", None));

    let query = query(&[
        ("type", "GR"),
        ("minCh", "24"),
        ("maxCh", "24"),
        ("dryRun", "true"),
    ]);
    run(&manager, &fixture.config(), &fixture, &query).await;

    let services = manager.services().list();
    assert_eq!(services.len(), 1, "dryRun must not touch the table");
    assert_eq!(services[0].service_id, 0x7001);
    assert!(fixture.channels_yml().is_empty());
    manager.shutdown().await;
}

// ---------------------------------------------------------------------------
// C13 / ordering: cancellation leaves neither a `.tmp` nor a table update
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_shutdown_stops_the_scan_without_leaving_a_tmp_file() {
    let fixture = Fixture::new("", CHANNEL_24);
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    // The tuner only starts streaming after the shutdown has been signalled.
    fixture.tuner(
        "GR",
        &format!("sh -c \"sleep 0.5; cat {}; sleep 30\"", ts.display()),
    );
    let manager = fixture.manager();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let scan = tokio::spawn({
        let manager = manager.clone();
        let config = fixture.config();
        let dir = fixture.path.clone();
        let query = query(&[("type", "GR"), ("minCh", "24"), ("maxCh", "30")]);
        async move { run_scan(&manager, &config, &dir, &query, tx, Some(shutdown_rx)).await }
    });

    // Wait until the scan actually started, then signal shutdown the way the
    // restart path should.
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the scan must start")
        .expect("progress line");
    assert!(first.contains("channel scanning"), "{first}");
    let started = Instant::now();
    shutdown_tx.send(true).expect("signal shutdown");

    tokio::time::timeout(Duration::from_secs(3), scan)
        .await
        .expect("the scan must observe the shutdown promptly")
        .expect("scan task")
        .expect("scan result");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the scan took {:?} to honour the shutdown",
        started.elapsed()
    );

    assert!(
        fixture.tmp_files().is_empty(),
        "a cancelled scan must not leave a .tmp behind: {:?}",
        fixture.tmp_files()
    );
    assert_eq!(fixture.channels_yml(), CHANNEL_24, "nothing was written");
    assert!(
        manager.services().list().is_empty(),
        "a cancelled scan must not update the table"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn a_shutdown_interrupts_the_si_read_loop_promptly() {
    // The tuner streams (so the session is Ready and the scan is inside its SI
    // read loop) but carries no SI, so the only way out before the budget is
    // the shutdown watch.
    let fixture = Fixture::new("", "");
    let mut packet = vec![0x11u8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    let ts = fixture.stream(
        "silent.ts",
        &[packet.clone(), packet.clone(), packet].concat(),
    );
    fixture.tuner("GR", &format!("sh -c \"cat {}; sleep 30\"", ts.display()));
    let manager = fixture.manager();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (tx, _rx) = tokio::sync::mpsc::channel::<String>(64);
    let scan = tokio::spawn({
        let manager = manager.clone();
        let config = fixture.config();
        let dir = fixture.path.clone();
        // A long budget: hitting it would fail the promptness assertion.
        let mut query = query(&[("type", "GR"), ("minCh", "24"), ("maxCh", "24")]);
        query.timeout = Duration::from_secs(20);
        async move { run_scan(&manager, &config, &dir, &query, tx, Some(shutdown_rx)).await }
    });

    // Wait until the scan is actually reading the stream.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = manager.sessions_snapshot().iter().any(|session| {
            session.channel == "24" && session.phase == unituner::session::SessionPhase::Ready
        });
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scan never started reading: {:?}",
            manager.sessions_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let before = fixture.channels_yml();
    let started = Instant::now();
    shutdown_tx.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(3), scan)
        .await
        .expect("the read loop must observe the shutdown")
        .expect("scan task")
        .expect("scan result");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "the scan took {:?} to stop reading",
        started.elapsed()
    );
    assert_eq!(
        fixture.channels_yml(),
        before,
        "a shutdown mid-read must not write"
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn a_client_disconnect_leaves_the_file_and_the_table_untouched() {
    let fixture = Fixture::new("", CHANNEL_24);
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    // Slow enough that the receiver is gone before the channel is read.
    fixture.tuner(
        "GR",
        &format!("sh -c \"sleep 0.4; cat {}; sleep 30\"", ts.display()),
    );
    let manager = fixture.manager();

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let scan = tokio::spawn({
        let manager = manager.clone();
        let config = fixture.config();
        let dir = fixture.path.clone();
        let query = query(&[("type", "GR"), ("minCh", "24"), ("maxCh", "24")]);
        async move { run_scan(&manager, &config, &dir, &query, tx, None).await }
    });

    // The HTTP client disconnects after the first frame.
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(rx);

    let result = tokio::time::timeout(Duration::from_secs(10), scan)
        .await
        .expect("the scan must notice the disconnect")
        .expect("scan task");
    result.expect("scan result");

    assert_eq!(
        fixture.channels_yml(),
        CHANNEL_24,
        "a disconnected scan must not write"
    );
    assert!(
        manager.services().list().is_empty(),
        "a disconnected scan must not update the table: {:?}",
        manager.services().list()
    );
    assert!(fixture.tmp_files().is_empty(), "no .tmp may be left behind");
    manager.shutdown().await;
}

// ---------------------------------------------------------------------------
// D: disabled entries and user-defined types
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_disabled_entry_is_rescanned_and_stays_disabled() {
    let fixture = Fixture::new(
        "",
        "- name: NHK\n  type: GR\n  channel: \"24\"\n  disabled: true\n",
    );
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner("GR", &format!("sh -c \"cat {}; sleep 30\"", ts.display()));
    let manager = fixture.manager();

    let query = query(&[
        ("type", "GR"),
        ("minCh", "24"),
        ("maxCh", "24"),
        ("refresh", "true"),
    ]);
    let progress = run(&manager, &fixture.config(), &fixture, &query).await;
    assert!(
        progress.contains("(disabled: kept disabled)"),
        "the progress must say the entry stayed disabled: {progress}"
    );
    assert!(
        !progress.contains("skipped (already configured)"),
        "a disabled entry is a re-scan target: {progress}"
    );

    // The channel was really scanned, so the table is filled ...
    let services = manager.services().list();
    assert_eq!(services.len(), 1, "{services:?}");
    assert_eq!(
        services[0].channel,
        Some(("GR".to_string(), "24".to_string()))
    );
    // ... but the entry is still disabled.
    assert!(fixture.channels_yml().contains("disabled: true"));
    manager.shutdown().await;
}

#[tokio::test]
async fn a_user_defined_type_scans_with_an_explicit_range() {
    let fixture = Fixture::new("", "");
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner(
        "CABLE_CATV",
        &format!("sh -c \"cat {}; sleep 30\"", ts.display()),
    );
    let manager = fixture.manager();

    let query = query(&[("type", "CABLE_CATV"), ("minCh", "17"), ("maxCh", "17")]);
    let progress = run(&manager, &fixture.config(), &fixture, &query).await;
    assert!(progress.contains("channel: \"17\""), "{progress}");

    let services = manager.services().list();
    assert_eq!(services.len(), 1, "{services:?}");
    assert_eq!(
        services[0].channel,
        Some(("CABLE_CATV".to_string(), "17".to_string())),
        "the conservative channel-name default is the bare number"
    );
    assert!(fixture.channels_yml().contains("type: CABLE_CATV"));
    assert!(fixture.channels_yml().contains("channel: '17'"));
    manager.shutdown().await;
}

#[tokio::test]
async fn a_user_defined_type_without_a_range_is_rejected_before_scanning() {
    let fixture = Fixture::new("", "");
    let ts = fixture.stream("mux.ts", &multiplex(&[(0x0408, 0x01, "NHK General")]));
    fixture.tuner(
        "CABLE_CATV",
        &format!("sh -c \"cat {}; sleep 30\"", ts.display()),
    );

    for pairs in [
        vec![("type", "CABLE_CATV")],
        vec![("type", "CABLE_CATV"), ("minCh", "1")],
        vec![("type", "CABLE_CATV"), ("maxCh", "9")],
    ] {
        let error = ScanQuery::from_params(&params(&pairs)).expect_err("range required");
        assert!(
            error.contains("minCh") && error.contains("CABLE_CATV"),
            "{error}"
        );
    }
    // Nothing was scanned, so nothing was written.
    assert_eq!(fixture.channels_yml(), "");
}

// ---------------------------------------------------------------------------
// D: subchannel expansion in the progress stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_subchannel_scan_tunes_the_cross_product_and_says_so() {
    let fixture = Fixture::new("", "");
    let ts = fixture.stream("silent.ts", &{
        let mut packet = vec![0x11u8; TS_PACKET_SIZE];
        packet[0] = 0x47;
        [packet.clone(), packet.clone(), packet].concat()
    });
    // One BS tuner, per-subchannel marker: the scan is the only way to tell
    // which pairs were tuned.
    let marker_dir = fixture.path.join("tuned");
    std::fs::create_dir_all(&marker_dir).expect("marker dir");
    fixture.tuner(
        "BS",
        &format!(
            "sh -c \"echo {{channel}} >> {}; cat {}; sleep 30\"",
            marker_dir.join("log").display(),
            ts.display()
        ),
    );
    let manager = fixture.manager();

    let mut query = query(&[
        ("type", "BS"),
        ("minCh", "13"),
        ("maxCh", "14"),
        ("minSubCh", "0"),
        ("maxSubCh", "1"),
        ("dryRun", "true"),
    ]);
    query.timeout = Duration::from_millis(300);
    let progress = run(&manager, &fixture.config(), &fixture, &query).await;

    // Channel outer, subchannel inner, in that documented order.
    assert!(
        progress.contains("channel: \"BS13_0\" (ch 13, subch 0)"),
        "{progress}"
    );
    assert!(
        progress.contains("channel: \"BS13_1\" (ch 13, subch 1)"),
        "{progress}"
    );
    assert!(
        progress.contains("channel: \"BS14_0\" (ch 14, subch 0)"),
        "{progress}"
    );
    let positions: Vec<usize> = ["BS13_0", "BS13_1", "BS14_0", "BS14_1"]
        .iter()
        .map(|name| {
            progress
                .find(name)
                .unwrap_or_else(|| panic!("{name}: {progress}"))
        })
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "expected channel-outer order: {positions:?}"
    );

    // The tuner saw the channel names, in the same order.
    let tuned = std::fs::read_to_string(marker_dir.join("log")).unwrap_or_default();
    let tuned: Vec<&str> = tuned.lines().collect();
    assert_eq!(
        tuned,
        ["BS13_0", "BS13_1", "BS14_0", "BS14_1"],
        "the cross product must be tuned in order"
    );
    assert!(fixture.channels_yml().is_empty(), "dryRun writes nothing");
    manager.shutdown().await;
}

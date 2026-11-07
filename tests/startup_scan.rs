//! Startup service scan (SPEC §8): the background task fills the service
//! table for every channel whose tuner works, leaves it alone for a channel
//! whose tuner fails, and stops promptly on shutdown.
//!
//! Every test drives [`unituner::scan::startup_scan`] with a fake tuner command
//! and a temporary config directory, so the real process tree, the session
//! manager and the shared table are all exercised together.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use unituner::config::Config;
use unituner::scan::startup_scan;
use unituner::services::ServiceTable;
use unituner::session::{SessionManager, SessionPhase};
use unituner::si::crc32_mpeg2;
use unituner::tsframe::TS_PACKET_SIZE;

const NETWORK_ID: u16 = 0x7FE0;
const TSID: u16 = 0x7FE0;

// ---------------------------------------------------------------------------
// TS fixture builders (PAT + SDT + NIT for two services)
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
    section_with(table_id, id, 0xC1, 0x00, 0x00, rest)
}

/// A section with explicit `version_and_current`, `section_number` and
/// `last_section_number`.
fn section_with(
    table_id: u8,
    id: u16,
    version_and_current: u8,
    section_number: u8,
    last_section_number: u8,
    rest: &[u8],
) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (id >> 8) as u8,
        id as u8,
        version_and_current,
        section_number,
        last_section_number,
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
    sdt_section_with(0x42, original_network_id, tsid, 0, 0, services)
}

/// An SDT section with an explicit table id and section numbers.
fn sdt_section_with(
    table_id: u8,
    original_network_id: u16,
    tsid: u16,
    section_number: u8,
    last_section_number: u8,
    services: &[(u16, u8, &str)],
) -> Vec<u8> {
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
    section_with(
        table_id,
        tsid,
        0xC1,
        section_number,
        last_section_number,
        &rest,
    )
}

fn nit_section(network_id: u16) -> Vec<u8> {
    let mut rest = vec![0xF0, 0x00];
    rest.extend_from_slice(&[0xF0, 0x00]);
    section(0x40, network_id, &rest)
}

fn section_packets(pid: u16, section: &[u8]) -> Vec<u8> {
    section_packets_with_cc(pid, section, 0)
}

/// Like [`section_packets`] but starting at `cc`: two sections on the same PID
/// need different continuity counters, or the assembler sees the second one as
/// a retransmission of the first.
fn section_packets_with_cc(pid: u16, section: &[u8], start_cc: u8) -> Vec<u8> {
    let mut packets = Vec::new();
    let mut remaining = section;
    let mut cc = start_cc;
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

/// Builds a TS stream from `(pid, section)` pairs, keeping one continuity
/// counter per PID so consecutive sections on the same PID are not mistaken for
/// retransmissions.
fn stream_of(sections: &[(u16, Vec<u8>)]) -> Vec<u8> {
    let mut counters: std::collections::HashMap<u16, u8> = std::collections::HashMap::new();
    let mut stream = Vec::new();
    for (pid, section) in sections {
        let cc = counters.entry(*pid).or_insert(0);
        let packets = section_packets_with_cc(*pid, section, *cc);
        // Each emitted packet consumed one continuity counter value.
        let count = packets.len() / TS_PACKET_SIZE;
        *cc = (*cc + count as u8) & 0x0F;
        stream.extend_from_slice(&packets);
    }
    stream
}

/// Two services on one multiplex: PAT, SDT and NIT.
fn multiplex() -> Vec<u8> {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(
        0x0000,
        &pat_section(TSID, &[(0x0408, 0x01F0), (0x0409, 0x01F1)]),
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
    stream
}

/// PMT for `program_number` with one H.264 elementary stream per `es_pids`
/// entry. The PMT PID is the one the PAT announced, so the scan's parser sees
/// the table where the multiplex says it is.
fn pmt_section(program_number: u16, pcr_pid: u16, es_pids: &[u16]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&(0xE000 | pcr_pid).to_be_bytes());
    rest.extend_from_slice(&[0xF0, 0x00]); // program_info_length 0
    for &pid in es_pids {
        rest.extend_from_slice(&[0x1B, (0xE0 | (pid >> 8)) as u8, pid as u8, 0xF0, 0x00]);
    }
    section(0x02, program_number, &rest)
}

/// The core tables of one multiplex for `services` (`(service_id, pmt_pid,
/// name)`), split into "the PAT alone" and "everything else".
///
/// A fake tuner that hands the PAT over first and only streams the rest later
/// stays inside the per-channel budget (and out of the SI grace) while the
/// other channel is being observed, which is what makes the overlap of a
/// parallel pass measurable from the timestamps each tuner command writes.
fn core_tables(services: &[(u16, u16, &str)], pmt: bool, nit: bool) -> (Vec<u8>, Vec<u8>) {
    let programs: Vec<(u16, u16)> = services
        .iter()
        .map(|&(service_id, pmt_pid, _)| (service_id, pmt_pid))
        .collect();
    let names: Vec<(u16, u8, &str)> = services
        .iter()
        .map(|&(service_id, _, name)| (service_id, 0x01, name))
        .collect();
    let pat = stream_of(&[(0x0000, pat_section(TSID, &programs))]);
    let mut rest: Vec<(u16, Vec<u8>)> = Vec::new();
    if pmt {
        for (index, &(service_id, pmt_pid, _)) in services.iter().enumerate() {
            rest.push((
                pmt_pid,
                pmt_section(service_id, pmt_pid, &[0x0100 + index as u16]),
            ));
        }
    }
    rest.push((0x0011, sdt_section(NETWORK_ID, TSID, &names)));
    if nit {
        rest.push((0x0010, nit_section(NETWORK_ID)));
    }
    (pat, stream_of(&rest))
}

/// [`core_tables`] concatenated into one stream.
fn core_multiplex(services: &[(u16, u16, &str)], pmt: bool, nit: bool) -> Vec<u8> {
    let (pat, rest) = core_tables(services, pmt, nit);
    [pat, rest].concat()
}

/// Channel 27's two services (`multiplex()` without a PMT).
const SERVICES_27: &[(u16, u16, &str)] =
    &[(0x0408, 0x01F0, "NHK General"), (0x0409, 0x01F1, "NHK E")];
/// A second multiplex for channel 28, with different service ids so both
/// channels' results are distinguishable in the table.
const SERVICES_28: &[(u16, u16, &str)] = &[(0x0410, 0x01F2, "BS1"), (0x0411, 0x01F3, "BS2")];

/// Reads a one-line epoch-nanosecond timestamp written by a fake tuner.
fn read_nanos(path: &Path) -> u128 {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("{} was never written: {error}", path.display()))
        .trim()
        .parse()
        .unwrap_or_else(|error| panic!("{} is not epoch nanos: {error}", path.display()))
}

// ---------------------------------------------------------------------------
// Config fixture
// ---------------------------------------------------------------------------

const SERVER: &str = "addr: \":0\"\nidle_timeout_secs: 1\n";

const PIPELINES: &str = "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n";

const GOOD_CHANNEL: &str = "- name: NHK\n  type: GR\n  channel: \"27\"\n";
const DEAD_CHANNEL: &str = "- name: Broken\n  type: DEAD\n  channel: \"1\"\n";
const SECOND_CHANNEL: &str = "- name: Second\n  type: GR\n  channel: \"28\"\n";

/// `sh -c "cat <path>; sleep 30"`: streams the fixture, then keeps the pipe
/// open like a real tuner.
fn streaming_command(path: &Path) -> String {
    format!("sh -c \"cat {}; sleep 30\"", path.display())
}

/// A `GR` tuner running `gr` plus a `DEAD` tuner running `dead`.
fn tuners(gr: &str, dead: &str) -> String {
    format!(
        "- name: good\n  types: [GR]\n  command: '{gr}'\n\
         - name: dead\n  types: [DEAD]\n  command: '{dead}'\n"
    )
}

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
            ("pipelines.yml", PIPELINES.to_string()),
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

    fn manager(&self) -> SessionManager {
        let config = Config::load(&self.path).expect("config loads");
        SessionManager::new(Arc::new(config), ServiceTable::new())
    }

    fn config(&self) -> Arc<Config> {
        Arc::new(Config::load(&self.path).expect("config loads"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn startup_scan_populates_the_table_for_a_working_channel() {
    // 27 streams a real multiplex; the DEAD channel's tuner exits at once.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let ts = fixture.write_ts("mux.ts", &multiplex());
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&ts), "sh -c \"exit 1\""),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(
        fixture.path.join("channels.yml"),
        format!("{GOOD_CHANNEL}{DEAD_CHANNEL}"),
    )
    .expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));

    // One pass: 27 succeeds and the failing channel is not retried.
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");

    let services = manager.services().list();
    assert_eq!(services.len(), 2, "{services:?}");
    assert_eq!(services[0].name, "NHK General");
    assert_eq!(services[0].network_id, NETWORK_ID);
    assert_eq!(services[0].tsid, TSID);
    assert_eq!(services[0].service_id, 0x0408);
    assert_eq!(services[0].service_type, 0x01);
    assert_eq!(services[0].pmt_pid, Some(0x01F0));
    assert_eq!(
        services[0].channel,
        Some(("GR".to_string(), "27".to_string()))
    );
    assert_eq!(services[1].name, "NHK E");
    assert_eq!(services[1].pmt_pid, Some(0x01F1));

    // The scan session is released before the task ends.
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_leaves_the_table_empty_when_the_tuner_fails() {
    // The only channel's tuner exits before producing any byte.
    let fixture = Fixture::new(&tuners("true", "sh -c \"exit 1\""), DEAD_CHANNEL);

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));

    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");

    assert!(manager.services().list().is_empty());
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_stops_on_shutdown() {
    // The tuner produces no byte at all, so the scan is mid-tune when the
    // shutdown signal arrives.
    let fixture = Fixture::new(
        "- name: good\n  types: [GR]\n  command: 'sh -c \"sleep 30\"'\n",
        GOOD_CHANNEL,
    );

    let manager = fixture.manager();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));

    // Let the scan reach the acquire, then shut down the way `serve` does.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    let _ = shutdown_tx.send(true);
    manager.shutdown().await;
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the startup scan stopped promptly")
        .expect("startup scan task");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the startup scan took {:?} to stop",
        started.elapsed()
    );
    assert!(manager.sessions_snapshot().is_empty());
}

#[tokio::test]
async fn startup_scan_releases_the_tuner_for_the_next_channel() {
    // The first channel streams a real multiplex and the second also has a
    // working tuner: the scan's own session must be released in between, so the
    // second channel is never reported as busy.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let ts = fixture.write_ts("mux.ts", &multiplex());
    let command = streaming_command(&ts);
    // Two independent GR tuners, so a lingering scan session on the first would
    // make the second channel busy.
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!(
            "- name: good\n  types: [GR]\n  command: '{command}'\n\
             - name: second\n  types: [GR]\n  command: '{command}'\n"
        ),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(
        fixture.path.join("channels.yml"),
        format!("{GOOD_CHANNEL}{SECOND_CHANNEL}"),
    )
    .expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));

    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");

    let channels: Vec<String> = manager
        .services()
        .list()
        .iter()
        .filter_map(|record| record.channel.as_ref().map(|(_, channel)| channel.clone()))
        .collect();
    assert!(
        channels.contains(&"27".to_string()) && channels.contains(&"28".to_string()),
        "both channels must be scanned: {channels:?}"
    );
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_ignores_an_sdt_other_and_still_finds_the_real_services() {
    // The stream starts with the SDT-other (0x46) of another transponder, then
    // carries the real SDT-actual (0x42) of this multiplex. Only the latter may
    // reach the service table.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let stream = stream_of(&[
        (
            0x0000,
            pat_section(TSID, &[(0x0408, 0x01F0), (0x0409, 0x01F1)]),
        ),
        (
            0x0011,
            sdt_section_with(
                0x46,
                NETWORK_ID,
                0x1111,
                0,
                0,
                &[(0x7001, 0x01, "Wrong Transponder")],
            ),
        ),
        (
            0x0011,
            sdt_section(
                NETWORK_ID,
                TSID,
                &[(0x0408, 0x01, "NHK General"), (0x0409, 0x01, "NHK E")],
            ),
        ),
        (0x0010, nit_section(NETWORK_ID)),
    ]);
    let ts = fixture.write_ts("mux.ts", &stream);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&ts), "true"),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(fixture.path.join("channels.yml"), GOOD_CHANNEL).expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");

    let services = manager.services().list();
    let names: Vec<&str> = services.iter().map(|record| record.name.as_str()).collect();
    assert_eq!(names, ["NHK General", "NHK E"], "{services:?}");
    assert!(
        !services
            .iter()
            .any(|record| record.name == "Wrong Transponder"),
        "the SDT-other must not contribute services: {services:?}"
    );
    assert!(services.iter().all(|record| record.tsid == TSID));
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_collects_every_section_of_a_multi_section_sdt() {
    // A 3-section SDT-actual. Sections 2 and 0 arrive before section 1, so the
    // scan must not stop at the first SDT section it sees.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let stream = stream_of(&[
        (
            0x0000,
            pat_section(
                TSID,
                &[
                    (0x0408, 0x01F0),
                    (0x0409, 0x01F1),
                    (0x040A, 0x01F2),
                    (0x040B, 0x01F3),
                ],
            ),
        ),
        (
            0x0011,
            sdt_section_with(
                0x42,
                NETWORK_ID,
                TSID,
                2,
                2,
                &[(0x040A, 0x01, "Service Two")],
            ),
        ),
        (
            0x0011,
            sdt_section_with(
                0x42,
                NETWORK_ID,
                TSID,
                0,
                2,
                &[(0x0408, 0x01, "Service Zero")],
            ),
        ),
        (
            0x0011,
            sdt_section_with(
                0x42,
                NETWORK_ID,
                TSID,
                1,
                2,
                &[(0x0409, 0x01, "Service One")],
            ),
        ),
        (0x0010, nit_section(NETWORK_ID)),
    ]);
    let ts = fixture.write_ts("mux.ts", &stream);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&ts), "true"),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(fixture.path.join("channels.yml"), GOOD_CHANNEL).expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");

    let mut ids: Vec<u16> = manager
        .services()
        .list()
        .iter()
        .map(|record| record.service_id)
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [0x0408, 0x0409, 0x040A],
        "every section of the SDT must contribute: {:?}",
        manager.services().list()
    );
    manager.shutdown().await;
}

#[tokio::test]
async fn a_scan_api_reports_a_user_takeover_as_preempted() {
    // A viewer takes the tuner while the scan is reading the channel. The
    // channel must be reported as preempted (the startup scan retries it with
    // the busy backoff), never as "no signal", which would write the channel
    // off for the lifetime of the process.
    use unituner::scan::{run_scan, ScanQuery};

    let fixture = Fixture::new(&tuners("true", "true"), "");
    // A stream without SI: the scan keeps reading until its budget, which is
    // the window a viewer takeover has to land in.
    let mut packet = vec![0x11u8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    let ts = fixture.write_ts(
        "silent.ts",
        &[packet.clone(), packet.clone(), packet].concat(),
    );
    let command = streaming_command(&ts);
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!("- name: good\n  types: [GR]\n  command: '{command}'\n"),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(fixture.path.join("channels.yml"), GOOD_CHANNEL).expect("write channels.yml");

    let manager = fixture.manager();
    let config = fixture.config();
    let mut query = ScanQuery::from_params(&std::collections::HashMap::from([
        ("type".to_string(), "GR".to_string()),
        ("minCh".to_string(), "27".to_string()),
        ("maxCh".to_string(), "27".to_string()),
        ("scanMode".to_string(), "Channel".to_string()),
        // The channel is already configured: scan it again, without writing.
        ("refresh".to_string(), "true".to_string()),
        ("dryRun".to_string(), "true".to_string()),
    ]))
    .expect("query");
    query.timeout = Duration::from_secs(20);

    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let scan = tokio::spawn({
        let manager = manager.clone();
        let config = Arc::clone(&config);
        let dir = fixture.path.clone();
        async move { run_scan(&manager, &config, &dir, &query, tx, None).await }
    });

    // Take the tuner over once the scan is streaming on it (a takeover cannot
    // replace a reservation that is still starting).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let streaming = manager.sessions_snapshot().iter().any(|session| {
            session.type_name == "GR"
                && session.channel == "27"
                && session.phase == unituner::session::SessionPhase::Ready
        });
        if streaming {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scan never acquired the tuner: {:?}",
            manager.sessions_snapshot()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A different channel: a same-channel request would just join the scan.
    let viewer = manager
        .acquire(
            "GR",
            "28",
            unituner::session::TunerUser::new("viewer", 0),
            None,
            true,
        )
        .await
        .expect("the viewer must win over the scan");

    let result = tokio::time::timeout(Duration::from_secs(15), scan)
        .await
        .expect("the scan must finish")
        .expect("scan task");
    result.expect("the scan itself must not fail");

    let mut progress = String::new();
    while let Ok(line) = rx.try_recv() {
        progress.push_str(&line);
    }
    assert!(
        progress.contains("preempted"),
        "the scan must report the preemption instead of no signal: {progress:?}"
    );

    drop(viewer);
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_tunes_two_channels_on_two_tuners_at_once() {
    // Two GR tuners and two channels. Each fake tuner writes the epoch
    // nanoseconds of its own start, then streams only the PAT, waits 1 s,
    // stamps a second marker and streams the rest of the multiplex (SDT + NIT,
    // but no PMT, so the channel ends on the 700 ms SI grace).
    //
    // A sequential pass cannot start the second tuner before the first
    // channel's grace expires, so `start_28 < sdt_27` can only hold when both
    // channels are tuned concurrently. The session snapshot is sampled as well:
    // two `GR` sessions must be `Ready` at the same instant.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    for (channel, services) in [("27", SERVICES_27), ("28", SERVICES_28)] {
        let (pat, rest) = core_tables(services, false, true);
        fixture.write_ts(&format!("{channel}-pat.ts"), &pat);
        fixture.write_ts(&format!("{channel}-rest.ts"), &rest);
    }

    let dir = fixture.path.display().to_string();
    // `{channel}` is substituted by the session layer before argv splitting, so
    // one command template serves both channels. argv[0] is `sh` (executed
    // directly, no implicit shell): the double-quoted argument is one token.
    let command = format!(
        "sh -c \"date +%s%N > {dir}/scan_{{channel}}.start; cat {dir}/{{channel}}-pat.ts; \
         sleep 1.0; date +%s%N > {dir}/scan_{{channel}}.sdt; cat {dir}/{{channel}}-rest.ts; sleep 30\""
    );
    std::fs::write(
        fixture.path.join("tuners.yml"),
        format!(
            "- name: tuner-a\n  types: [GR]\n  command: '{command}'\n\
             - name: tuner-b\n  types: [GR]\n  command: '{command}'\n"
        ),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(
        fixture.path.join("channels.yml"),
        format!("{GOOD_CHANNEL}{SECOND_CHANNEL}"),
    )
    .expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Sample the session table while the pass runs and remember whether both
    // channels were `Ready` at the same instant.
    let overlapping = Arc::new(AtomicBool::new(false));
    let poller = {
        let manager = manager.clone();
        let overlapping = Arc::clone(&overlapping);
        tokio::spawn(async move {
            loop {
                let snapshot = manager.sessions_snapshot();
                let ready = |channel: &str| {
                    snapshot.iter().any(|session| {
                        session.type_name == "GR"
                            && session.channel == channel
                            && session.phase == SessionPhase::Ready
                    })
                };
                if ready("27") && ready("28") {
                    overlapping.store(true, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };

    let started = Instant::now();
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");
    let elapsed = started.elapsed();
    poller.abort();

    let start_27 = read_nanos(&fixture.path.join("scan_27.start"));
    let start_28 = read_nanos(&fixture.path.join("scan_28.start"));
    let sdt_27 = read_nanos(&fixture.path.join("scan_27.sdt"));
    let sdt_28 = read_nanos(&fixture.path.join("scan_28.sdt"));
    assert!(
        start_27 < sdt_28 && start_28 < sdt_27,
        "the two tuners must run concurrently: 27 {start_27}..{sdt_27}, 28 {start_28}..{sdt_28}"
    );
    assert!(
        overlapping.load(Ordering::SeqCst),
        "two GR sessions must be Ready at the same instant"
    );
    // One channel costs ~1 s of stream + the 700 ms grace; a sequential pass
    // would cost twice that.
    assert!(
        elapsed < Duration::from_secs(3),
        "the parallel pass took {elapsed:?}"
    );

    let mut found: Vec<(String, String)> = manager
        .services()
        .list()
        .iter()
        .map(|record| {
            let channel = record
                .channel
                .as_ref()
                .map(|(_, channel)| channel.clone())
                .unwrap_or_default();
            (channel, record.name.clone())
        })
        .collect();
    found.sort();
    assert_eq!(
        found,
        [
            ("27".to_string(), "NHK E".to_string()),
            ("27".to_string(), "NHK General".to_string()),
            ("28".to_string(), "BS1".to_string()),
            ("28".to_string(), "BS2".to_string()),
        ],
        "both channels' services must be in the table"
    );
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_exits_once_the_core_tables_are_complete() {
    // PAT + PMT + SDT-actual + NIT-actual in one burst, then the tuner keeps
    // the stream open (`sleep 30`): the scan must return as soon as the core
    // tables are in, well before CHANNEL_TIMEOUT (25 s) and without waiting for
    // the stream to end.
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let ts = fixture.write_ts("mux.ts", &core_multiplex(SERVICES_27, true, true));
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&ts), "true"),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(fixture.path.join("channels.yml"), GOOD_CHANNEL).expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let started = Instant::now();
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "the scan waited {elapsed:?} for a stream that never ends"
    );

    let services = manager.services().list();
    assert_eq!(services.len(), 2, "{services:?}");
    assert_eq!(services[0].name, "NHK General");
    assert_eq!(services[0].network_id, NETWORK_ID); // NIT-actual
    assert_eq!(services[0].tsid, TSID);
    assert_eq!(services[0].pmt_pid, Some(0x01F0)); // PAT
    assert_eq!(services[1].name, "NHK E");
    assert_eq!(services[1].pmt_pid, Some(0x01F1));
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

#[tokio::test]
async fn startup_scan_falls_back_to_the_sdt_network_id_after_the_grace() {
    // PAT + PMT + SDT-actual, but no NIT-actual, and the stream stays open. The
    // scan must still finish on SI_GRACE_AFTER_SDT (700 ms) + slack instead of
    // the channel budget, and the records must carry the SDT's
    // original_network_id as their network id (the documented fallback).
    let fixture = Fixture::new(&tuners("true", "true"), "");
    let (pat, rest) = core_tables(SERVICES_27, true, false);
    let ts = fixture.write_ts("mux.ts", &[pat, rest].concat());
    std::fs::write(
        fixture.path.join("tuners.yml"),
        tuners(&streaming_command(&ts), "true"),
    )
    .expect("rewrite tuners.yml");
    std::fs::write(fixture.path.join("channels.yml"), GOOD_CHANNEL).expect("write channels.yml");

    let manager = fixture.manager();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let started = Instant::now();
    let task = tokio::spawn(startup_scan(manager.clone(), fixture.config(), shutdown_rx));
    tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("startup scan finished")
        .expect("startup scan task");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(4),
        "the scan waited {elapsed:?} after a complete SDT without a NIT"
    );
    // The grace really was waited out (a channel that exited earlier would have
    // had every core table, which this stream never provides).
    assert!(
        elapsed >= Duration::from_millis(500),
        "the SDT grace was not applied: {elapsed:?}"
    );

    let services = manager.services().list();
    assert_eq!(services.len(), 2, "{services:?}");
    assert_eq!(services[0].name, "NHK General");
    assert_eq!(
        services[0].network_id, NETWORK_ID,
        "the SDT's original_network_id is the documented fallback"
    );
    assert_eq!(services[0].tsid, TSID);
    assert_eq!(services[0].pmt_pid, Some(0x01F0));
    assert_eq!(services[1].name, "NHK E");
    assert!(manager.sessions_snapshot().is_empty());
    manager.shutdown().await;
}

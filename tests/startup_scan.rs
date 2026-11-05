//! Startup service scan (SPEC §8): the background task fills the service
//! table for every channel whose tuner works, leaves it alone for a channel
//! whose tuner fails, and stops promptly on shutdown.
//!
//! Every test drives [`unituner::scan::startup_scan`] with a fake tuner command
//! and a temporary config directory, so the real process tree, the session
//! manager and the shared table are all exercised together.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use unituner::config::Config;
use unituner::scan::startup_scan;
use unituner::services::ServiceTable;
use unituner::session::SessionManager;
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
        cc = cc.wrapping_add(1) & 0x0F;
        first = false;
    }
    packets
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

// ---------------------------------------------------------------------------
// Config fixture
// ---------------------------------------------------------------------------

const SERVER: &str = "addr: \":0\"\nidle_timeout_secs: 1\n";

const PIPELINES: &str = "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n";

const GOOD_CHANNEL: &str = "- name: NHK\n  type: GR\n  channel: \"27\"\n";
const DEAD_CHANNEL: &str = "- name: Broken\n  type: DEAD\n  channel: \"1\"\n";

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

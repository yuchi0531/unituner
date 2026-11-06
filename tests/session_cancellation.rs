//! Cancellation safety of [`SessionManager::acquire`] (SPEC §6.2, §8.1).
//!
//! A request can disappear at any moment: an HTTP client aborts, a scan's
//! per-channel budget expires, a `select!` drops the future. None of those may
//! leave a `Starting` reservation that keeps the tuner occupied forever, and
//! none may leave a live process tree behind.
//!
//! These tests drive the public session API only, exactly like the HTTP layer,
//! and use a tuner that never produces output so the readiness gate stays
//! closed for the whole cancellation window.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use unituner::config::Config;
use unituner::services::ServiceTable;
use unituner::session::{SessionManager, SessionPhase, TunerUser};

fn process_exists(pid: u32) -> bool {
    // SAFETY: signal 0 only checks for existence and permission.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

struct Fixture {
    _dir: tempfile::TempDir,
    manager: SessionManager,
}

impl Fixture {
    /// A manager whose single `GR` tuner runs `command`.
    fn new(command: &str) -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            (
                "server.yml",
                "addr: \":0\"\nidle_timeout_secs: 5\nchunk_size_bytes: 188\nsubscriber_buffer_chunks: 8\n"
                    .to_string(),
            ),
            (
                "tuners.yml",
                format!("- name: t0\n  types: [GR]\n  command: '{command}'\n"),
            ),
            (
                "channels.yml",
                "- name: a\n  type: GR\n  channel: \"27\"\n- name: b\n  type: GR\n  channel: \"28\"\n"
                    .to_string(),
            ),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n".to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Config::load(dir.path()).expect("config loads");
        let manager = SessionManager::new(Arc::new(config), ServiceTable::new());
        Self { _dir: dir, manager }
    }

    fn pids(&self) -> Vec<u32> {
        self.manager
            .sessions_snapshot()
            .iter()
            .flat_map(|session| session.pids.clone())
            .collect()
    }

    async fn wait_for_pids(&self) -> Vec<u32> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let pids = self.pids();
            if !pids.is_empty() {
                return pids;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the session never spawned a process tree"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_cleanup(&self, pids: &[u32]) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let clean = self.manager.sessions_snapshot().is_empty()
                && pids.iter().all(|pid| !process_exists(*pid));
            if clean {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "cleanup did not finish: {:?}",
                self.manager.sessions_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn user(id: &str) -> TunerUser {
    TunerUser::new(id, 0)
}

/// A packet-aligned fixture: three 188-byte TS packets.
fn mux(dir: &Path) -> std::path::PathBuf {
    let mut packet = vec![0x11u8; 188];
    packet[0] = 0x47;
    let path = dir.join("mux.bin");
    std::fs::write(&path, [packet.clone(), packet.clone(), packet].concat()).expect("write mux");
    path
}

#[tokio::test]
async fn a_dropped_acquire_leaves_no_reservation_and_no_tree() {
    // The tuner produces nothing until the marker exists.
    let dir = tempfile::TempDir::new().expect("tempdir");
    let marker = dir.path().join("go");
    let mux = mux(dir.path());
    let command = format!(
        "sh -c \"while test ! -f {}; do sleep 1; done; cat {}; sleep 30\"",
        marker.display(),
        mux.display()
    );
    let fixture = Fixture::new(&command);

    // Drop the acquire future mid-readiness, like a client abort or a scan
    // budget expiring.
    let acquire = tokio::spawn({
        let manager = fixture.manager.clone();
        async move {
            tokio::time::timeout(
                Duration::from_millis(400),
                manager.acquire("GR", "27", user("cancelled"), None, true),
            )
            .await
        }
    });
    let pids = fixture.wait_for_pids().await;
    assert!(
        acquire.await.expect("acquire task").is_err(),
        "the acquire must have been cancelled"
    );

    // (a) no `Starting` reservation remains, (b) the tree is stopped.
    assert!(
        fixture
            .manager
            .sessions_snapshot()
            .iter()
            .all(|session| session.phase != SessionPhase::Starting),
        "a Starting reservation leaked: {:?}",
        fixture.manager.sessions_snapshot()
    );
    fixture.wait_for_cleanup(&pids).await;
    assert!(
        !fixture.manager.tuners_snapshot()[0].is_using,
        "/api/tuners must not report the cancelled tuner as used"
    );

    // (c) the same tuner is usable again.
    std::fs::write(&marker, b"go").expect("marker");
    let mut subscription = fixture
        .manager
        .acquire("GR", "27", user("later"), None, true)
        .await
        .expect("the tuner must be usable after the cancellation");
    let chunk = tokio::time::timeout(Duration::from_secs(5), subscription.rx.recv())
        .await
        .expect("the new session must stream")
        .expect("a chunk");
    assert_eq!(chunk.len() % 188, 0);

    fixture.manager.shutdown().await;
}

#[tokio::test]
async fn a_cancelled_acquire_does_not_keep_a_second_channel_out() {
    // The first channel's tuner is silent, the second's streams. Once the first
    // request is cancelled, the tuner must be free for the second one instead of
    // answering 503 forever.
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mux = mux(dir.path());
    let command = format!(
        "sh -c \"case {{channel}} in 27) sleep 30;; *) cat {}; sleep 30;; esac\"",
        mux.display()
    );
    let fixture = Fixture::new(&command);

    let acquire = tokio::spawn({
        let manager = fixture.manager.clone();
        async move {
            tokio::time::timeout(
                Duration::from_millis(300),
                manager.acquire("GR", "27", user("cancelled"), None, true),
            )
            .await
        }
    });
    let pids = fixture.wait_for_pids().await;
    assert!(acquire.await.expect("acquire task").is_err());
    fixture.wait_for_cleanup(&pids).await;

    let mut subscription = fixture
        .manager
        .acquire("GR", "28", user("viewer"), None, true)
        .await
        .expect("the other channel must be requestable");
    let chunk = tokio::time::timeout(Duration::from_secs(5), subscription.rx.recv())
        .await
        .expect("the other channel must stream")
        .expect("a chunk");
    assert_eq!(chunk.len() % 188, 0);
    drop(subscription);

    fixture.manager.shutdown().await;
}

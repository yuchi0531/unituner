//! End-to-end checks of the CLI: config loading, exit codes, the server bind
//! line and graceful shutdown.

use std::fs;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

const SERVER: &str = "addr: \":0\"\n";
const TUNERS: &str = r#"
- name: pt3-gr0
  types: [GR]
  command: "recpt1 --device /dev/pt3video0 {channel} -"
"#;
const CHANNELS: &str = r#"
- name: NHK General
  type: GR
  channel: "27"
"#;
const PIPELINES: &str = r#"
pipelines:
  soft:
    - "softcas-dec /etc/softcas/keys.json"
types:
  GR: { pipeline: soft }
"#;

fn config_dir(channels: &str) -> TempDir {
    let dir = TempDir::new().expect("tempdir");
    for (name, body) in [
        ("server.yml", SERVER),
        ("tuners.yml", TUNERS),
        ("channels.yml", channels),
        ("pipelines.yml", PIPELINES),
    ] {
        fs::write(dir.path().join(name), body).expect("write fixture");
    }
    dir
}

fn run(config_dir: &TempDir) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_unituner"))
        .arg("--config-dir")
        .arg(config_dir.path())
        .env("UNITUNER_LOG", "info")
        .output()
        .expect("run unituner")
}

/// Starts the server, waits for its bind line, then stops it with `signal`.
///
/// The server runs until told to stop (SPEC §9), so a plain `output()` would
/// never return.
fn run_until_listening(config_dir: &TempDir, signal: i32) -> (String, std::process::ExitStatus) {
    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_unituner"))
        .arg("--config-dir")
        .arg(config_dir.path())
        .env("UNITUNER_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn unituner");

    let stderr = child.stderr.take().expect("stderr");
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let reader = std::thread::spawn(move || {
        let mut collected = String::new();
        let mut announced = false;
        for line in BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            collected.push_str(&line);
            collected.push('\n');
            if !announced && collected.contains("listening") {
                announced = true;
                let _ = tx.send(());
            }
        }
        collected
    });

    let deadline = Instant::now() + Duration::from_secs(20);
    match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(()) => {}
        Err(error) => panic!("the server did not listen in time: {error}"),
    }

    // SAFETY: `child.id()` is this child's pid; a signal is the documented way
    // to ask it to shut down.
    let sent = unsafe { libc::kill(child.id() as libc::pid_t, signal) };
    assert_eq!(sent, 0, "failed to signal the child");

    let status = child.wait().expect("wait for unituner");
    let collected = reader.join().expect("stderr reader");
    (collected, status)
}

#[test]
fn valid_config_starts_the_server_and_shuts_down_gracefully() {
    let dir = config_dir(CHANNELS);
    let (stderr, status) = run_until_listening(&dir, libc::SIGTERM);

    assert!(status.success(), "stderr: {stderr}");
    assert!(stderr.contains("configuration loaded"), "stderr: {stderr}");
    assert!(stderr.contains("listening"), "stderr: {stderr}");
    assert!(stderr.contains("SIGTERM received"), "stderr: {stderr}");
    assert!(
        stderr.contains("all tuner sessions stopped"),
        "stderr: {stderr}"
    );
}

#[test]
fn sigint_also_shuts_down_gracefully() {
    let dir = config_dir(CHANNELS);
    let (stderr, status) = run_until_listening(&dir, libc::SIGINT);
    assert!(status.success(), "stderr: {stderr}");
    assert!(stderr.contains("SIGINT received"), "stderr: {stderr}");
}

#[test]
fn invalid_config_exits_nonzero_with_the_offending_file_and_item() {
    let dir = config_dir(
        r#"
- name: NHK General
  type: "GR!"
  channel: "27"
"#,
    );
    let output = run(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("channels.yml"), "stderr: {stderr}");
    assert!(stderr.contains("invalid type name"), "stderr: {stderr}");
    assert!(!stderr.contains("configuration loaded"));
}

#[test]
fn invalid_config_is_reported_even_with_logging_disabled() {
    let dir = config_dir(
        r#"
- name: NHK General
  type: "GR!"
  channel: "27"
"#,
    );
    let output = Command::new(env!("CARGO_BIN_EXE_unituner"))
        .arg("--config-dir")
        .arg(dir.path())
        .env("UNITUNER_LOG", "off")
        .output()
        .expect("run unituner");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("channels.yml"), "stderr: {stderr}");
}

#[test]
fn missing_config_dir_exits_nonzero() {
    let dir = TempDir::new().expect("tempdir");
    let output = run(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("server.yml"), "stderr: {stderr}");
}

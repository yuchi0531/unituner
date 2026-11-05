//! End-to-end checks of the phase 1 CLI: config loading, exit codes and the
//! summary output.

use std::fs;
use std::process::Command;

use tempfile::TempDir;

const SERVER: &str = "addr: \":40772\"\n";
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

#[test]
fn valid_config_exits_zero_and_reports_phase_1() {
    let dir = config_dir(CHANNELS);
    let output = run(&dir);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("phase 1: config loaded"),
        "stdout: {stdout}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("configuration loaded"), "stderr: {stderr}");
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
    assert!(!stderr.contains("phase 1: config loaded"));
}

#[test]
fn missing_config_dir_exits_nonzero() {
    let dir = TempDir::new().expect("tempdir");
    let output = run(&dir);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("server.yml"), "stderr: {stderr}");
}

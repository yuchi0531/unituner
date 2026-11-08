//! Configuration loading and validation (SPEC §3).
//!
//! Four YAML files are read from the config directory:
//! `server.yml`, `tuners.yml`, `channels.yml` and `pipelines.yml`.
//! Every document is parsed with `deny_unknown_fields`, so typos such as
//! `cmd:` instead of `command:` are rejected instead of silently ignored.
//! Validation is fail-fast at startup: any issue is reported with the file and
//! the offending item, and the process exits non-zero.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use ipnet::IpNet;
use regex::Regex;
use serde::Deserialize;

/// Default listen address. `:40772` binds every interface, matching the
/// mirakurun default port (SPEC §3.1 leaves the choice to the implementation).
pub const DEFAULT_ADDR: &str = ":40772";

/// Default allowed networks: RFC1918 plus loopback (SPEC §3.1).
pub const DEFAULT_ALLOWED_CIDRS: [&str; 4] = [
    "10.0.0.0/8",
    "127.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
];

/// Default seconds a session with no subscribers is kept alive (SPEC §7).
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 5;

/// Default per-subscriber buffer, in chunks (SPEC §7).
pub const DEFAULT_SUBSCRIBER_BUFFER_CHUNKS: usize = 1024;

/// Default chunk size handed to subscribers (SPEC §7).
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 65_536;

/// Default maximum subscribers attached to one session (SPEC §7).
pub const DEFAULT_MAX_SUBSCRIBERS_PER_SESSION: usize = 32;

/// Default maximum subscribers from one client IP across all sessions
/// (SPEC §7).
pub const DEFAULT_MAX_SUBSCRIBERS_PER_IP: usize = 16;

/// Default maximum number of concurrent sessions (SPEC §7).
pub const DEFAULT_MAX_SESSIONS: usize = 16;

/// Default total subscriber buffer budget, in bytes (SPEC §7).
///
/// One default subscriber queue costs `65536 × 1024 = 64 MiB` worst case, so
/// 2 GiB fits 32 of them — the same as `max_subscribers_per_session`, which
/// therefore normally binds before this budget does. The budget is a
/// worst-case reservation, not typical usage: a real queue only holds what the
/// client is actually being fed. It still bounds process-wide growth. Raise it
/// further (or lower `subscriber_buffer_chunks`) on memory-constrained hosts.
pub const DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Default startup probe window for a routed channel, in milliseconds
/// (SPEC §3.1, §6.4).
///
/// While a route's tuner is being started, the session waits this long for the
/// first data. A process that exits before any data is a start failure and the
/// next route is tried; a timeout with the process still alive is **not** a
/// failure — a tuner that has no data yet (e.g. no signal) is treated as
/// successfully started, matching mirakc-BS4K.
pub const DEFAULT_TUNER_STARTUP_PROBE_MS: u64 = 1000;

/// Upper bound for `server.tuner_startup_probe_ms` (SPEC §3.1, §6.4).
///
/// The probe is waited once per route; it is the **route fallback window**, not
/// the readiness gate. 10 s is a fixed sanity bound so a typo such as
/// `tuner_startup_probe_ms: 100000` cannot hold a single route for minutes.
pub const MAX_TUNER_STARTUP_PROBE_MS: u64 = 10_000;

/// Default time [`SessionManager::acquire`] waits for the first pipeline output
/// before treating a spawn as failed (SPEC §6.2, §8).
pub const DEFAULT_TUNER_READINESS_TIMEOUT_SECS: u64 = 30;

/// Default per-channel scan budget covering acquire and SI reading (SPEC §8).
pub const DEFAULT_SCAN_CHANNEL_TIMEOUT_SECS: u64 = 60;

/// Upper bound for the two timeout fields above.
pub const MAX_SCAN_TIMEOUT_SECS: u64 = 600;

/// Default `PATH` handed to every tuner/pipeline child (SPEC §3.1, §3.5).
///
/// The child environment is explicit: it never inherits `PATH` from the server
/// process, where a service-manager environment may list directories the
/// service user cannot stat (e.g. `/root/.cargo/bin`).
pub const DEFAULT_CHILD_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Default `HOME` handed to every child (SPEC §3.1, §3.5).
pub const DEFAULT_CHILD_HOME: &str = "/var/lib/unituner";

/// Default `TMPDIR` handed to every child (SPEC §3.1, §3.5).
pub const DEFAULT_CHILD_TMPDIR: &str = "/tmp";

/// Default working directory for every child (SPEC §3.1, §3.5).
///
/// `""` means "inherit the server's working directory".
pub const DEFAULT_CHILD_CWD: &str = "/var/lib/unituner";

/// `server.yml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_addr")]
    pub addr: String,
    #[serde(default = "default_allowed_cidrs")]
    pub allowed_cidrs: Vec<String>,
    /// Additional `Host` header values the webui accepts, besides any host in
    /// `allowed_cidrs` and `localhost` (SPEC §3.1).
    ///
    /// The webui rejects a request whose `Host` is neither an IP inside
    /// `allowed_cidrs` nor `localhost` nor one of these names, which is what
    /// stops a DNS-rebinding page from being treated as same-origin. A real
    /// reverse proxy or a mDNS name therefore has to be listed here. Entries
    /// are host names (no port, no scheme); matching ignores ASCII case.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    #[serde(default = "default_subscriber_buffer_chunks")]
    pub subscriber_buffer_chunks: usize,
    #[serde(default = "default_chunk_size_bytes")]
    pub chunk_size_bytes: usize,
    /// Maximum subscribers attached to one session; a further request is
    /// refused with HTTP 429 (SPEC §7).
    #[serde(default = "default_max_subscribers_per_session")]
    pub max_subscribers_per_session: usize,
    /// Maximum subscribers from one client IP across every session; a further
    /// request from that IP is refused with HTTP 429 (SPEC §7).
    ///
    /// The peer address of the TCP connection is used; `X-Forwarded-For` is
    /// deliberately ignored, because a client can forge it and a reverse proxy
    /// would otherwise let every client share one bucket.
    #[serde(default = "default_max_subscribers_per_ip")]
    pub max_subscribers_per_ip: usize,
    /// Maximum number of concurrent session objects, including `Starting` and
    /// `Stopping` ones; a request that would exceed it is refused with HTTP 429
    /// (SPEC §7).
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    /// Total budget for all subscriber queues, in bytes (SPEC §7).
    ///
    /// A subscriber costs up to `chunk_size_bytes × subscriber_buffer_chunks`
    /// (64 KiB × 1024 ≈ 64 MiB with the defaults), so
    /// `max_subscribers_per_session: 32` alone would allow ≈2 GiB per session.
    /// This budget caps the sum across every session: a request that would
    /// exceed it is refused with HTTP 429. The default 2 GiB fits 32
    /// default-sized queues, so the per-session cap normally binds first; the
    /// budget is a worst-case reservation and still prevents unbounded growth.
    #[serde(default = "default_max_subscriber_buffer_bytes")]
    pub max_subscriber_buffer_bytes: usize,
    /// Startup probe window for a routed channel's tuner, in milliseconds
    /// (SPEC §3.1, §6.4).
    ///
    /// Only `routes` fallback uses it: a tuner that exits before producing its
    /// first data is a start failure and the next route is tried, while a
    /// timeout with the process still alive counts as started. Channels without
    /// `routes` keep the existing readiness gate and are unaffected.
    #[serde(default = "default_tuner_startup_probe_ms")]
    pub tuner_startup_probe_ms: u64,
    /// Time given to a tuner/pipeline to produce its first byte before the
    /// legacy readiness gate treats the spawn as failed (SPEC §3.1, §8).
    ///
    /// This is **not** the routed startup probe (`tuner_startup_probe_ms`):
    /// that window only decides whether a route falls back, while this gate
    /// decides whether the request as a whole fails. 30 s tolerates a tuner
    /// that needs several seconds to lock before its first packet.
    #[serde(default = "default_tuner_readiness_timeout_secs")]
    pub tuner_readiness_timeout_secs: u64,
    /// Per-channel budget for every scan (startup, manual re-scan and the
    /// discovery API), covering acquire *and* SI reading (SPEC §8).
    ///
    /// Must be strictly greater than `tuner_readiness_timeout_secs`; the scan
    /// also caps its acquire at `readiness + 5 s` so SI still gets the rest of
    /// the budget.
    #[serde(default = "default_scan_channel_timeout_secs")]
    pub scan_channel_timeout_secs: u64,
    /// `PATH` for every tuner/pipeline child (SPEC §3.5). Never empty.
    #[serde(default = "default_child_path")]
    pub child_path: String,
    /// `HOME` for every tuner/pipeline child (SPEC §3.5). Never empty.
    #[serde(default = "default_child_home")]
    pub child_home: String,
    /// `TMPDIR` for every tuner/pipeline child (SPEC §3.5). Never empty.
    #[serde(default = "default_child_tmpdir")]
    pub child_tmpdir: String,
    /// Working directory for every tuner/pipeline child (SPEC §3.5).
    ///
    /// Empty means "inherit the server's working directory". If a non-empty
    /// value does not exist, startup only warns; each spawn then omits
    /// `current_dir` so the server still works with a stale default.
    #[serde(default = "default_child_cwd")]
    pub child_cwd: String,
    /// Extra environment variables for every child (SPEC §3.5). Merged after
    /// `PATH` / `HOME` / `TMPDIR`, so an entry here overrides them.
    #[serde(default)]
    pub child_env: HashMap<String, String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            allowed_cidrs: default_allowed_cidrs(),
            allowed_hosts: Vec::new(),
            idle_timeout_secs: default_idle_timeout_secs(),
            subscriber_buffer_chunks: default_subscriber_buffer_chunks(),
            chunk_size_bytes: default_chunk_size_bytes(),
            max_subscribers_per_session: default_max_subscribers_per_session(),
            max_subscribers_per_ip: default_max_subscribers_per_ip(),
            max_sessions: default_max_sessions(),
            max_subscriber_buffer_bytes: default_max_subscriber_buffer_bytes(),
            tuner_startup_probe_ms: default_tuner_startup_probe_ms(),
            tuner_readiness_timeout_secs: default_tuner_readiness_timeout_secs(),
            scan_channel_timeout_secs: default_scan_channel_timeout_secs(),
            child_path: default_child_path(),
            child_home: default_child_home(),
            child_tmpdir: default_child_tmpdir(),
            child_cwd: default_child_cwd(),
            child_env: HashMap::new(),
        }
    }
}

impl ServerConfig {
    /// The configured readiness gate (SPEC §6.2, §8).
    pub fn tuner_readiness_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.tuner_readiness_timeout_secs)
    }

    /// The configured per-channel scan budget (SPEC §8).
    pub fn scan_channel_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.scan_channel_timeout_secs)
    }
}

fn default_addr() -> String {
    DEFAULT_ADDR.to_string()
}

fn default_allowed_cidrs() -> Vec<String> {
    DEFAULT_ALLOWED_CIDRS
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

fn default_idle_timeout_secs() -> u64 {
    DEFAULT_IDLE_TIMEOUT_SECS
}

fn default_subscriber_buffer_chunks() -> usize {
    DEFAULT_SUBSCRIBER_BUFFER_CHUNKS
}

fn default_chunk_size_bytes() -> usize {
    DEFAULT_CHUNK_SIZE_BYTES
}

fn default_max_subscribers_per_session() -> usize {
    DEFAULT_MAX_SUBSCRIBERS_PER_SESSION
}

fn default_max_subscribers_per_ip() -> usize {
    DEFAULT_MAX_SUBSCRIBERS_PER_IP
}

fn default_max_sessions() -> usize {
    DEFAULT_MAX_SESSIONS
}

fn default_max_subscriber_buffer_bytes() -> usize {
    DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES
}

fn default_tuner_startup_probe_ms() -> u64 {
    DEFAULT_TUNER_STARTUP_PROBE_MS
}

fn default_tuner_readiness_timeout_secs() -> u64 {
    DEFAULT_TUNER_READINESS_TIMEOUT_SECS
}

fn default_scan_channel_timeout_secs() -> u64 {
    DEFAULT_SCAN_CHANNEL_TIMEOUT_SECS
}

fn default_child_path() -> String {
    DEFAULT_CHILD_PATH.to_string()
}

fn default_child_home() -> String {
    DEFAULT_CHILD_HOME.to_string()
}

fn default_child_tmpdir() -> String {
    DEFAULT_CHILD_TMPDIR.to_string()
}

fn default_child_cwd() -> String {
    DEFAULT_CHILD_CWD.to_string()
}

/// One entry of `tuners.yml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TunerConfig {
    pub name: String,
    pub types: Vec<String>,
    pub command: String,
    #[serde(default)]
    pub disabled: bool,
}

/// One entry of `channels.yml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: String,
    pub channel: String,
    /// Extra arguments substituted for `{extra_args}` in the tuner command
    /// (SPEC §3.3). Empty by default; a route may override it for itself.
    ///
    /// A standalone argv fragment: quotes, backslashes and newlines are refused
    /// by validation, so it must never be placed inside a shell string. Both the
    /// `extra-args` spelling (canonical) and `extra_args` are accepted.
    #[serde(default, rename = "extra-args", alias = "extra_args")]
    pub extra_args: String,
    #[serde(default)]
    pub disabled: bool,
    /// Optional list of tuner routes (SPEC §3.3, §6.4). Empty (the default)
    /// keeps the type-independent selection behavior of SPEC §6.2; a non-empty
    /// list restricts the channel to exactly the named tuners, in order.
    #[serde(default)]
    pub routes: Vec<ChannelRouteConfig>,
}

/// One entry of a channel's `routes`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRouteConfig {
    /// `name` of a tuner in `tuners.yml`.
    pub tuner: String,
    /// Channel value used for this route only, overriding the channel's
    /// `channel` in the tuner command and in session identity.
    #[serde(default)]
    pub channel: Option<String>,
    /// Extra arguments used for this route only, overriding the channel's
    /// `extra-args`. A standalone argv fragment (quotes, backslashes and
    /// newlines are refused); never place it inside a shell string. Both the
    /// `extra-args` spelling and `extra_args` are accepted.
    #[serde(default, rename = "extra-args", alias = "extra_args")]
    pub extra_args: Option<String>,
}

/// `pipelines.yml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelinesConfig {
    /// Pipeline name -> argv stages. An empty map is valid: every type then
    /// streams raw TS (SPEC §3.4).
    #[serde(default)]
    pub pipelines: HashMap<String, Vec<String>>,
    /// Channel type -> pipeline binding. Types without a binding are delivered
    /// as raw TS (SPEC §3.4).
    #[serde(default)]
    pub types: HashMap<String, PipelineBinding>,
}

/// A `pipelines.types` value.
///
/// The bound pipeline receives the tuner's bytes as-is and must emit MPEG-TS;
/// unituner normalizes the pipeline output (204 → 188) before broadcasting
/// (SPEC §3.4, §4).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineBinding {
    /// The pipeline this type feeds.
    pub pipeline: String,
}

/// The whole configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub tuners: Vec<TunerConfig>,
    pub channels: Vec<ChannelConfig>,
    pub pipelines: PipelinesConfig,
}

/// A single validation problem, pointing at the file and the offending item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub file: &'static str,
    pub item: String,
    pub message: String,
}

impl Issue {
    fn new(file: &'static str, item: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            file,
            item: item.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}: {}", self.file, self.item, self.message)
    }
}

/// A configuration loading failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("invalid configuration:\n{}", format_issues(.0))]
    Invalid(Vec<Issue>),
}

fn format_issues(issues: &[Issue]) -> String {
    issues
        .iter()
        .map(|issue| format!("  - {issue}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_yaml_ng::from_str(&text).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

impl Config {
    /// Reads and validates the four YAML files in `dir`.
    pub fn load(dir: &Path) -> Result<Self, ConfigError> {
        let config = Self {
            server: parse_file(&dir.join("server.yml"))?,
            tuners: parse_file(&dir.join("tuners.yml"))?,
            channels: parse_file(&dir.join("channels.yml"))?,
            pipelines: parse_file(&dir.join("pipelines.yml"))?,
        };
        config.validate()?;
        Ok(config)
    }

    /// Runs all SPEC §3.6 validation rules, returning every issue found.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let issues = validate(self);
        if issues.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(issues))
        }
    }

    /// Warns once at startup when `server.child_cwd` is non-empty but does not
    /// exist.
    ///
    /// A missing working directory is **not** a startup error: the default
    /// `/var/lib/unituner` may legitimately not exist yet, and each spawn then
    /// omits `current_dir` so existing deployments keep working. The spawn-time
    /// warning explains the per-process omission.
    pub fn warn_if_child_cwd_missing(&self) {
        let cwd = self.server.child_cwd.as_str();
        if !cwd.is_empty() && !Path::new(cwd).is_dir() {
            tracing::warn!(
                child_cwd = %cwd,
                "server.child_cwd does not exist; child processes will inherit the server's working directory"
            );
        }
    }
}

/// Collects every validation issue in `config`.
pub fn validate(config: &Config) -> Vec<Issue> {
    let mut issues = Vec::new();
    validate_server(&config.server, &mut issues);
    validate_tuners(&config.tuners, &mut issues);
    validate_channels(&config.channels, &config.tuners, &mut issues);
    validate_pipelines(&config.pipelines, &mut issues);
    issues
}

fn validate_server(server: &ServerConfig, issues: &mut Vec<Issue>) {
    if let Err(message) = validate_addr(&server.addr) {
        issues.push(Issue::new("server.yml", "addr", message));
    }
    for (index, cidr) in server.allowed_cidrs.iter().enumerate() {
        if let Err(error) = cidr.parse::<ipnet::IpNet>() {
            issues.push(Issue::new(
                "server.yml",
                format!("allowed_cidrs[{index}]"),
                format!("{cidr:?} is not a valid CIDR: {error}"),
            ));
        }
    }
    for (index, host) in server.allowed_hosts.iter().enumerate() {
        if let Err(message) = validate_allowed_host(host) {
            issues.push(Issue::new(
                "server.yml",
                format!("allowed_hosts[{index}]"),
                message,
            ));
        }
    }
    if server.subscriber_buffer_chunks == 0 {
        issues.push(Issue::new(
            "server.yml",
            "subscriber_buffer_chunks",
            "must be greater than 0",
        ));
    }
    if server.chunk_size_bytes == 0 {
        issues.push(Issue::new(
            "server.yml",
            "chunk_size_bytes",
            "must be greater than 0",
        ));
    }
    for (field, value) in [
        (
            "max_subscribers_per_session",
            server.max_subscribers_per_session,
        ),
        ("max_subscribers_per_ip", server.max_subscribers_per_ip),
        ("max_sessions", server.max_sessions),
        (
            "max_subscriber_buffer_bytes",
            server.max_subscriber_buffer_bytes,
        ),
    ] {
        if value == 0 {
            issues.push(Issue::new("server.yml", field, "must be greater than 0"));
        }
    }
    if server.tuner_startup_probe_ms == 0 {
        issues.push(Issue::new(
            "server.yml",
            "tuner_startup_probe_ms",
            "must be greater than 0",
        ));
    } else if server.tuner_startup_probe_ms > MAX_TUNER_STARTUP_PROBE_MS {
        issues.push(Issue::new(
            "server.yml",
            "tuner_startup_probe_ms",
            format!("must be at most {MAX_TUNER_STARTUP_PROBE_MS} ms"),
        ));
    }
    for (field, value) in [
        (
            "tuner_readiness_timeout_secs",
            server.tuner_readiness_timeout_secs,
        ),
        (
            "scan_channel_timeout_secs",
            server.scan_channel_timeout_secs,
        ),
    ] {
        if value == 0 {
            issues.push(Issue::new("server.yml", field, "must be greater than 0"));
        } else if value > MAX_SCAN_TIMEOUT_SECS {
            issues.push(Issue::new(
                "server.yml",
                field,
                format!("must be at most {MAX_SCAN_TIMEOUT_SECS}"),
            ));
        }
    }
    if server.scan_channel_timeout_secs > 0
        && server.scan_channel_timeout_secs <= server.tuner_readiness_timeout_secs
    {
        issues.push(Issue::new(
            "server.yml",
            "scan_channel_timeout_secs",
            format!(
                "must be strictly greater than tuner_readiness_timeout_secs ({} <= {})",
                server.scan_channel_timeout_secs, server.tuner_readiness_timeout_secs
            ),
        ));
    }
    // `child_cwd` alone may be empty (inherit the server's working directory).
    for (field, value, allow_empty) in [
        ("child_path", server.child_path.as_str(), false),
        ("child_home", server.child_home.as_str(), false),
        ("child_tmpdir", server.child_tmpdir.as_str(), false),
        ("child_cwd", server.child_cwd.as_str(), true),
    ] {
        if let Err(message) = validate_child_value(value, allow_empty) {
            issues.push(Issue::new("server.yml", field, message));
        }
    }
    for (name, value) in &server.child_env {
        if name.is_empty() || name.contains('\0') || name.contains('\n') || name.contains('\r') {
            issues.push(Issue::new(
                "server.yml",
                format!("child_env[{name:?}]"),
                "environment variable name must be non-empty and contain no NUL or newline",
            ));
        }
        if value.contains('\0') {
            issues.push(Issue::new(
                "server.yml",
                format!("child_env[{name:?}]"),
                "environment variable value must not contain NUL",
            ));
        }
    }
}

/// Validates a child-process path-like setting.
///
/// `None` for `value` means "this field may be empty"; `Some` requires a
/// non-empty value. NUL and newlines are never accepted in an environment value
/// or a path.
fn validate_child_value(value: &str, allow_empty: bool) -> Result<(), String> {
    if value.is_empty() {
        return if allow_empty {
            Ok(())
        } else {
            Err("must not be empty".to_string())
        };
    }
    if value.contains('\0') || value.contains('\n') || value.contains('\r') {
        return Err("must not contain NUL or newlines".to_string());
    }
    Ok(())
}

/// A `server.yml` `allowed_hosts` entry must be a bare host name or IP literal:
/// no scheme, no port, no brackets, no path (SPEC §3.1).
fn validate_allowed_host(host: &str) -> Result<(), String> {
    if host.trim().is_empty() {
        return Err("must not be empty".to_string());
    }
    match normalize_host(host) {
        // `normalize_host` strips a port, brackets and a trailing dot, so
        // comparing with the ASCII-lowercased entry itself rejects
        // `example.com:40772`, `http://example.com` and `[::1]` while still
        // allowing an uppercase host name.
        Some(normalized) if normalized == host.to_ascii_lowercase() => Ok(()),
        Some(_) => Err(format!(
            "{host:?} must be a bare host name or IP literal (no scheme, port or brackets)"
        )),
        None => Err(format!("{host:?} is not a valid host name or IP literal")),
    }
}

/// The host part of a `Host` / `Origin` authority, lowercased.
///
/// Accepts `host`, `host:port`, `[::1]` and `[::1]:port`; a bare IPv6 literal
/// (which is not valid in a `Host` header, but tolerated) is accepted too. A
/// trailing dot is dropped, so `localhost.` normalises to `localhost`. Returns
/// `None` for an empty host or anything containing a character a host cannot
/// have (whitespace, `/`, `@`, `\`, …).
pub fn normalize_host(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let host = if let Some(rest) = value.strip_prefix('[') {
        let (inside, after) = rest.split_once(']')?;
        if !(after.is_empty() || after.strip_prefix(':').is_some_and(is_port)) {
            return None;
        }
        inside.parse::<Ipv6Addr>().ok()?.to_string()
    } else if value.matches(':').count() > 1 {
        // A bare IPv6 literal (`::1`): no port can be attached without brackets.
        value.parse::<Ipv6Addr>().ok()?.to_string()
    } else {
        let (host, port) = match value.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (value, None),
        };
        if port.is_some_and(|port| !is_port(port)) {
            return None;
        }
        host.to_string()
    };
    let host = host.strip_suffix('.').unwrap_or(&host);
    if host.is_empty() || !host.chars().all(is_host_char) {
        return None;
    }
    Some(host.to_ascii_lowercase())
}

fn is_port(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_digit())
}

fn is_host_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':')
}

/// Whether a request `Host` header may be served (SPEC §3.1, SPEC §10).
///
/// `host` is a raw `Host` header value (`host[:port]`, `[::1]:port`). It is
/// accepted when its host part is
///
/// * listed in `allowed_hosts` (exact, ASCII-case-insensitive), or
/// * `localhost`, or
/// * an IP literal inside one of `allowed_cidrs` (with the same IPv4/IPv6
///   equivalences as the peer guard: `::1` matches the IPv4 loopback list and a
///   v4-mapped peer matches its IPv4 form).
///
/// Anything else — including a missing or malformed `Host` — is rejected, which
/// is what stops a DNS-rebinding page (whose `Host` is its own name) from being
/// treated as same-origin with this server.
pub fn host_is_allowed(host: &str, allowed_hosts: &[String], allowed_cidrs: &[String]) -> bool {
    let Some(host) = normalize_host(host) else {
        return false;
    };
    if host == "localhost" {
        return true;
    }
    if allowed_hosts
        .iter()
        .any(|allowed| allowed.trim().to_ascii_lowercase() == host)
    {
        return true;
    }
    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };
    allowed_cidrs.iter().any(|cidr| {
        cidr.parse::<IpNet>().is_ok_and(|net| {
            ip_candidates(ip)
                .iter()
                .any(|candidate| net.contains(candidate))
        })
    })
}

/// The address forms an IP is matched in: its own, plus the IPv4/IPv6 spellings
/// an operator may have written for it.
fn ip_candidates(ip: IpAddr) -> Vec<IpAddr> {
    let mut candidates = vec![ip];
    match ip {
        IpAddr::V6(v6) => {
            if v6.is_loopback() {
                candidates.push(IpAddr::V4(Ipv4Addr::LOCALHOST));
            }
            if let Some(v4) = v6.to_ipv4_mapped() {
                candidates.push(IpAddr::V4(v4));
            }
        }
        IpAddr::V4(v4) => candidates.push(IpAddr::V6(v4.to_ipv6_mapped())),
    }
    candidates
}

fn validate_addr(addr: &str) -> Result<(), String> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return Err("must not be empty".to_string());
    }
    // `:40772` means "every interface" the same way Go's net.Listen does.
    let candidate = if trimmed.starts_with(':') {
        format!("0.0.0.0{trimmed}")
    } else {
        trimmed.to_string()
    };
    candidate
        .parse::<std::net::SocketAddr>()
        .map(|_| ())
        .map_err(|_| format!("{addr:?} is not a valid socket address (expected host:port)"))
}

fn validate_tuners(tuners: &[TunerConfig], issues: &mut Vec<Issue>) {
    let mut seen_names = HashSet::new();
    for (index, tuner) in tuners.iter().enumerate() {
        let label = format!("[{index}] ({:?})", tuner.name);
        if tuner.name.trim().is_empty() {
            issues.push(Issue::new(
                "tuners.yml",
                label.clone(),
                "name must not be empty",
            ));
        } else if !seen_names.insert(tuner.name.as_str()) {
            issues.push(Issue::new(
                "tuners.yml",
                label.clone(),
                "duplicate tuner name",
            ));
        }
        if tuner.types.is_empty() {
            issues.push(Issue::new(
                "tuners.yml",
                label.clone(),
                "types must contain at least one type name",
            ));
        }
        for (type_index, type_name) in tuner.types.iter().enumerate() {
            if let Err(message) = validate_type_name(type_name) {
                issues.push(Issue::new(
                    "tuners.yml",
                    format!("{label}.types[{type_index}]"),
                    message,
                ));
            }
        }
        if let Err(message) = validate_command(&tuner.command) {
            issues.push(Issue::new(
                "tuners.yml",
                format!("{label}.command"),
                message,
            ));
        }
    }
}

fn validate_channels(channels: &[ChannelConfig], tuners: &[TunerConfig], issues: &mut Vec<Issue>) {
    let tuner_names: HashSet<&str> = tuners.iter().map(|tuner| tuner.name.as_str()).collect();
    let mut seen_names = HashSet::new();
    let mut seen = HashSet::new();
    for (index, channel) in channels.iter().enumerate() {
        let label = format!("[{index}] ({:?})", channel.name);
        if channel.name.trim().is_empty() {
            issues.push(Issue::new(
                "channels.yml",
                label.clone(),
                "name must not be empty",
            ));
        } else if !seen_names.insert(channel.name.as_str()) {
            // SPEC §3.3: channel names are unique as well as (type, channel).
            issues.push(Issue::new(
                "channels.yml",
                label.clone(),
                "duplicate channel name",
            ));
        }
        if let Err(message) = validate_type_name(&channel.channel_type) {
            issues.push(Issue::new("channels.yml", format!("{label}.type"), message));
        }
        if let Err(message) = validate_channel_value(&channel.channel) {
            issues.push(Issue::new(
                "channels.yml",
                format!("{label}.channel"),
                message,
            ));
        }
        if let Err(message) = validate_extra_args(&channel.extra_args) {
            issues.push(Issue::new(
                "channels.yml",
                format!("{label}.extra-args"),
                message,
            ));
        }
        if !seen.insert((channel.channel_type.as_str(), channel.channel.as_str())) {
            issues.push(Issue::new(
                "channels.yml",
                label.clone(),
                format!(
                    "duplicate (type, channel) pair ({:?}, {:?})",
                    channel.channel_type, channel.channel
                ),
            ));
        }
        validate_channel_routes(channel, &label, &tuner_names, issues);
    }
}

/// Validates one channel's `routes` (SPEC §3.6): every route names a configured
/// tuner, no tuner is listed twice for one channel, and a per-route `channel`
/// passes the same value check as a channel's own.
fn validate_channel_routes(
    channel: &ChannelConfig,
    label: &str,
    tuner_names: &HashSet<&str>,
    issues: &mut Vec<Issue>,
) {
    let mut seen_tuners = HashSet::new();
    // Effective channel -> (route index, effective extra-args). Two routes that
    // resolve to the same effective channel share one session (SPEC §6.4), so a
    // different `extra-args` could never take effect and is a config mistake.
    let mut effective: HashMap<&str, (usize, &str)> = HashMap::new();
    for (route_index, route) in channel.routes.iter().enumerate() {
        let route_label = format!("{label}.routes[{route_index}]");
        if route.tuner.trim().is_empty() {
            issues.push(Issue::new(
                "channels.yml",
                format!("{route_label}.tuner"),
                "tuner must not be empty",
            ));
        } else if !tuner_names.contains(route.tuner.as_str()) {
            issues.push(Issue::new(
                "channels.yml",
                format!("{route_label}.tuner"),
                format!(
                    "references unknown tuner {:?} (channel {:?})",
                    route.tuner, channel.name
                ),
            ));
        } else if !seen_tuners.insert(route.tuner.as_str()) {
            issues.push(Issue::new(
                "channels.yml",
                format!("{route_label}.tuner"),
                format!("duplicate tuner {:?} in routes", route.tuner),
            ));
        }
        if let Some(route_channel) = &route.channel {
            if let Err(message) = validate_channel_value(route_channel) {
                issues.push(Issue::new(
                    "channels.yml",
                    format!("{route_label}.channel"),
                    message,
                ));
            }
        }
        if let Some(extra_args) = &route.extra_args {
            if let Err(message) = validate_extra_args(extra_args) {
                issues.push(Issue::new(
                    "channels.yml",
                    format!("{route_label}.extra-args"),
                    message,
                ));
            }
        }
        let effective_channel = route.channel.as_deref().unwrap_or(channel.channel.as_str());
        let effective_extra = route
            .extra_args
            .as_deref()
            .unwrap_or(channel.extra_args.as_str());
        if let Some((first_index, first_extra)) =
            effective.insert(effective_channel, (route_index, effective_extra))
        {
            if first_extra != effective_extra {
                issues.push(Issue::new(
                    "channels.yml",
                    format!("{route_label}.extra-args"),
                    format!(
                        "route resolves to the same effective channel {effective_channel:?} as \
                         routes[{first_index}] but declares different extra-args; effective-channel \
                         reuse ignores route order, so these extra-args would be silently ignored"
                    ),
                ));
            }
        }
    }
}

fn validate_pipelines(pipelines: &PipelinesConfig, issues: &mut Vec<Issue>) {
    // An empty `pipelines` map is valid: raw TS (no descrambling/transcoding) is
    // a legitimate deployment, and every type without a binding streams the
    // unfiltered multiplex (SPEC §3.4). A pipeline that *is* declared must have
    // at least one non-empty command, which the loop below checks.
    for (name, commands) in sorted_entries(&pipelines.pipelines) {
        let label = format!("pipelines.{name}");
        if commands.is_empty() {
            issues.push(Issue::new(
                "pipelines.yml",
                label.clone(),
                "must contain at least one command",
            ));
        }
        for (index, command) in commands.iter().enumerate() {
            if let Err(message) = validate_command(command) {
                issues.push(Issue::new(
                    "pipelines.yml",
                    format!("{label}[{index}]"),
                    message,
                ));
            }
        }
    }

    for (type_name, binding) in sorted_entries(&pipelines.types) {
        if let Err(message) = validate_type_name(type_name) {
            issues.push(Issue::new(
                "pipelines.yml",
                format!("types.{type_name}"),
                message,
            ));
        }
        if !pipelines.pipelines.contains_key(&binding.pipeline) {
            issues.push(Issue::new(
                "pipelines.yml",
                format!("types.{type_name}.pipeline"),
                format!("references unknown pipeline {:?}", binding.pipeline),
            ));
        }
    }
}

fn sorted_entries<V>(map: &HashMap<String, V>) -> Vec<(&String, &V)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by_key(|(key, _)| key.as_str());
    entries
}

fn validate_command(command: &str) -> Result<(), String> {
    if command.trim().is_empty() {
        Err("command must not be empty".to_string())
    } else {
        Ok(())
    }
}

/// Channel type names are user-defined, half-width ASCII identifiers:
/// `^[A-Za-z0-9][A-Za-z0-9_-]*$` (SPEC §3.3, §3.6).
///
/// The same function validates `tuners.types`, `pipelines.types` keys and
/// `channels.type`, and is the one the API must use for `{type}` values taken
/// from request paths such as `/api/channels/{type}/{channel}/stream`
/// (SPEC §3.5).
pub fn validate_type_name(name: &str) -> Result<(), String> {
    if type_name_regex().is_match(name) {
        Ok(())
    } else {
        Err(format!(
            "invalid type name {name:?}: must match ^[A-Za-z0-9][A-Za-z0-9_-]*$"
        ))
    }
}

fn type_name_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_-]*$").expect("valid regex"))
}

/// Validates an `extra-args` value (SPEC §3.3).
///
/// `extra-args` is substituted into the tuner command as a standalone argv
/// fragment. unituner never passes it through a shell, but an operator could
/// still write `{extra_args}` inside `sh -c "…"` in `tuners.yml`; a value
/// containing a quote, a backslash or a newline could then break out of that
/// string. Those four characters are refused, so the "one argv fragment, never
/// shell text" invariant is explicit. Everything else (notably spaces between
/// arguments) is allowed.
pub fn validate_extra_args(value: &str) -> Result<(), String> {
    match value
        .chars()
        .find(|c| matches!(c, '\'' | '"' | '\\' | '\n' | '\r'))
    {
        None => Ok(()),
        Some(c) => Err(format!(
            "invalid extra-args {value:?}: character {c:?} is not allowed \
             (extra-args is a standalone argv fragment and must never be placed \
             inside a shell string)"
        )),
    }
}

/// Validates a `{channel}` value, as found in `channels.yml` or in the URL path
/// `/api/channels/{type}/{channel}/stream`.
///
/// The same function is used for config values and URL-derived values
/// (SPEC §3.5).
///
/// The value must be non-empty and may only contain:
///
/// * ASCII alphanumerics (`A-Z`, `a-z`, `0-9`)
/// * `.` `_` `+` `-` `:` `@` `,`
///
/// Every other character is rejected, notably whitespace, quotes (`'` `"`),
/// backslash, `/`, and shell metacharacters (`$ ; | & ` * ? ( ) < > [ ] { } !
/// # ~ = % ^`). Commands are spawned after argv splitting with no shell
/// involved, but the whitelist keeps the "one channel value is one argv token"
/// invariant explicit (SPEC §3.5). `/` is refused so that a channel value
/// always fits in a single URL path segment of
/// `/api/channels/{type}/{channel}/stream`.
pub fn validate_channel_value(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("channel value must not be empty".to_string());
    }
    match value.chars().find(|c| {
        !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '+' | '-' | ':' | '@' | ','))
    }) {
        None => Ok(()),
        Some(c) => Err(format!(
            "invalid channel value {value:?}: character {c:?} is not allowed \
             (allowed: A-Z a-z 0-9 . _ + - : @ ,)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;
    use tempfile::TempDir;

    const SERVER: &str = r#"
addr: ":40772"
allowed_cidrs:
  - 10.0.0.0/8
  - 127.0.0.0/8
"#;

    const TUNERS: &str = r#"
- name: pt3-gr0
  types: [GR]
  command: "recpt1 --device /dev/pt3video0 {channel} -"
- name: pt3-bs0
  types: [BS, CABLE_CATV]
  command: "recpt1 --device /dev/pt3video1 {channel} -"
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
  soft:
    - "softcas-dec /etc/softcas/keys.json"
  soft4k:
    - "softcas-bs /etc/softcas/bs.json"
    - "arib-common-convert"
types:
  GR: { pipeline: soft }
  BS: { pipeline: soft4k }
"#;

    struct Fixture {
        _dir: TempDir,
        path: PathBuf,
    }

    impl Fixture {
        fn new(server: &str, tuners: &str, channels: &str, pipelines: &str) -> Self {
            let dir = TempDir::new().expect("tempdir");
            for (name, body) in [
                ("server.yml", server),
                ("tuners.yml", tuners),
                ("channels.yml", channels),
                ("pipelines.yml", pipelines),
            ] {
                fs::write(dir.path().join(name), body).expect("write fixture");
            }
            Self {
                path: dir.path().to_path_buf(),
                _dir: dir,
            }
        }

        fn valid() -> Self {
            Self::new(SERVER, TUNERS, CHANNELS, PIPELINES)
        }

        fn load(&self) -> Result<Config, ConfigError> {
            Config::load(&self.path)
        }

        /// Loads and expects a validation failure, returning the messages.
        fn issues(&self) -> Vec<String> {
            match self.load() {
                Ok(_) => panic!("expected validation to fail"),
                Err(ConfigError::Invalid(issues)) => {
                    issues.iter().map(|issue| issue.to_string()).collect()
                }
                Err(other) => panic!("unexpected error: {other}"),
            }
        }
    }

    fn assert_issue_contains(issues: &[String], needle: &str) {
        assert!(
            issues.iter().any(|issue| issue.contains(needle)),
            "no issue containing {needle:?} in {issues:#?}"
        );
    }

    #[test]
    fn valid_config_loads() {
        let config = Fixture::valid().load().expect("valid config");
        assert_eq!(config.server.addr, ":40772");
        assert_eq!(config.server.idle_timeout_secs, 5);
        assert_eq!(config.server.subscriber_buffer_chunks, 1024);
        assert_eq!(config.server.chunk_size_bytes, 65_536);
        assert_eq!(config.server.allowed_cidrs.len(), 2);
        assert_eq!(config.tuners.len(), 2);
        assert_eq!(config.channels.len(), 2);
        assert_eq!(config.pipelines.pipelines.len(), 2);
        assert_eq!(config.pipelines.types.len(), 2);
    }

    #[test]
    fn server_defaults_apply_when_fields_are_omitted() {
        let fixture = Fixture::new("{}", TUNERS, CHANNELS, PIPELINES);
        let config = fixture.load().expect("valid config");
        assert_eq!(config.server.addr, DEFAULT_ADDR);
        assert_eq!(config.server.allowed_cidrs, DEFAULT_ALLOWED_CIDRS.to_vec());
        assert_eq!(config.server.idle_timeout_secs, DEFAULT_IDLE_TIMEOUT_SECS);
        assert_eq!(
            config.server.subscriber_buffer_chunks,
            DEFAULT_SUBSCRIBER_BUFFER_CHUNKS
        );
        assert_eq!(config.server.chunk_size_bytes, DEFAULT_CHUNK_SIZE_BYTES);
        assert_eq!(
            config.server.max_subscribers_per_session,
            DEFAULT_MAX_SUBSCRIBERS_PER_SESSION
        );
        assert_eq!(
            config.server.max_subscribers_per_ip,
            DEFAULT_MAX_SUBSCRIBERS_PER_IP
        );
        assert_eq!(config.server.max_sessions, DEFAULT_MAX_SESSIONS);
        assert_eq!(
            config.server.max_subscriber_buffer_bytes,
            DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES
        );
    }

    #[test]
    fn default_subscriber_buffer_budget_is_2_gib() {
        // 2 GiB = 32 default-sized queues (64 MiB each), matching
        // max_subscribers_per_session.
        assert_eq!(DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES, 2 * 1024 * 1024 * 1024);
        assert_eq!(
            DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES
                / (DEFAULT_CHUNK_SIZE_BYTES * DEFAULT_SUBSCRIBER_BUFFER_CHUNKS),
            32
        );
    }

    #[test]
    fn zero_caps_are_rejected() {
        // Every cap is "greater than 0"; 0 is not an "unlimited" spelling.
        for field in [
            "max_subscribers_per_session",
            "max_subscribers_per_ip",
            "max_sessions",
            "max_subscriber_buffer_bytes",
        ] {
            let server = format!("{SERVER}\n{field}: 0\n");
            let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
            assert_issue_contains(&issues, field);
            assert_issue_contains(&issues, "must be greater than 0");
        }
    }

    #[test]
    fn disabled_defaults_to_false() {
        let config = Fixture::valid().load().expect("valid config");
        assert!(config.tuners.iter().all(|tuner| !tuner.disabled));
        assert!(config.channels.iter().all(|channel| !channel.disabled));
    }

    #[test]
    fn duplicate_tuner_name_is_rejected() {
        let tuners = r#"
- name: pt3-gr0
  types: [GR]
  command: "recpt1 {channel} -"
- name: pt3-gr0
  types: [BS]
  command: "recpt1 {channel} -"
"#;
        let issues = Fixture::new(SERVER, tuners, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "duplicate tuner name");
    }

    #[test]
    fn duplicate_type_channel_pair_is_rejected() {
        let channels = r#"
- name: NHK General
  type: GR
  channel: "27"
- name: NHK General Copy
  type: GR
  channel: "27"
"#;
        let issues = Fixture::new(SERVER, TUNERS, channels, PIPELINES).issues();
        assert_issue_contains(&issues, "duplicate (type, channel) pair");
    }

    #[test]
    fn duplicate_channel_name_is_rejected() {
        let channels = r#"
- name: NHK General
  type: GR
  channel: "27"
- name: NHK General
  type: BS
  channel: "BS13_1"
"#;
        let issues = Fixture::new(SERVER, TUNERS, channels, PIPELINES).issues();
        assert_issue_contains(&issues, "duplicate channel name");
    }

    #[test]
    fn same_channel_number_on_different_types_is_allowed() {
        let channels = r#"
- name: NHK General
  type: GR
  channel: "27"
- name: BS 27
  type: BS
  channel: "27"
"#;
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("channels are distinct per type");
    }

    #[test]
    fn bad_type_names_are_rejected() {
        for bad in ["GR!", "gr space", "-GR", "", "gr.space"] {
            let channels = format!("- name: test\n  type: \"{bad}\"\n  channel: \"27\"\n");
            let issues = Fixture::new(SERVER, TUNERS, &channels, PIPELINES).issues();
            assert_issue_contains(&issues, "invalid type name");
        }
    }

    #[test]
    fn type_regex_applies_to_tuner_types() {
        let tuners = r#"
- name: pt3-gr0
  types: ["GR!"]
  command: "recpt1 {channel} -"
"#;
        let issues = Fixture::new(SERVER, tuners, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "tuners.yml: [0] (\"pt3-gr0\").types[0]");
        assert_issue_contains(&issues, "invalid type name");
    }

    #[test]
    fn type_regex_applies_to_pipeline_type_keys() {
        let pipelines = r#"
pipelines:
  soft:
    - "softcas-dec"
types:
  "GR!": { pipeline: soft }
"#;
        let issues = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).issues();
        assert_issue_contains(&issues, "invalid type name");
        assert_issue_contains(&issues, "pipelines.yml: types.GR!");
    }

    #[test]
    fn type_referencing_missing_pipeline_is_rejected() {
        let pipelines = r#"
pipelines:
  soft:
    - "softcas-dec"
types:
  GR: { pipeline: does-not-exist }
"#;
        let issues = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).issues();
        assert_issue_contains(&issues, "references unknown pipeline");
    }

    #[test]
    fn empty_pipeline_command_is_rejected() {
        let pipelines = r#"
pipelines:
  soft:
    - "   "
types: {}
"#;
        let issues = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).issues();
        assert_issue_contains(&issues, "command must not be empty");
    }

    #[test]
    fn empty_pipeline_list_is_rejected() {
        let pipelines = r#"
pipelines:
  soft: []
types: {}
"#;
        let issues = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).issues();
        assert_issue_contains(&issues, "at least one command");
    }

    #[test]
    fn a_passthrough_pipeline_is_accepted() {
        // One `passthrough` stage satisfies the "at least one command" rule;
        // at runtime it spawns nothing and delivers the raw TS (SPEC §3.4).
        let pipelines = r#"
pipelines:
  none:
    - "passthrough"
types:
  GR: { pipeline: none }
"#;
        let config = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines)
            .load()
            .expect("passthrough pipeline is valid");
        assert_eq!(
            config.pipelines.pipelines["none"],
            vec!["passthrough".to_string()]
        );
    }

    #[test]
    fn pipeline_binding_has_only_the_pipeline_field() {
        let pipelines = r#"
pipelines:
  mmt:
    - "dantto4k"
types:
  BS4K: { pipeline: mmt }
"#;
        let config = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines)
            .load()
            .expect("a binding needs only the pipeline name");
        assert_eq!(config.pipelines.types["BS4K"].pipeline, "mmt");
    }

    #[test]
    fn an_input_field_is_rejected_as_unknown() {
        // The input mode was removed: the pipeline always receives the tuner
        // bytes raw and unituner only normalizes its TS output (SPEC §3.4).
        for value in ["ts", "tlv", "mpegts", ""] {
            let pipelines = format!(
                "pipelines:\n  mmt:\n    - \"dantto4k\"\ntypes:\n  BS4K: {{ pipeline: mmt, input: \"{value}\" }}\n"
            );
            match Fixture::new(SERVER, TUNERS, CHANNELS, &pipelines).load() {
                Err(ConfigError::Parse { path, source }) => {
                    assert!(path.ends_with("pipelines.yml"));
                    assert!(
                        source.to_string().contains("unknown field"),
                        "unexpected parse error for {value:?}: {source}"
                    );
                }
                other => panic!("input {value:?} must be rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_misspelled_pipeline_binding_field_is_rejected() {
        // `deny_unknown_fields` catches the typo instead of silently ignoring it.
        let pipelines = r#"
pipelines:
  mmt:
    - "dantto4k"
types:
  BS4K: { pipeline: mmt, inputs: tlv }
"#;
        match Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("pipelines.yml"));
                assert!(source.to_string().contains("unknown field"));
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn a_binding_without_a_pipeline_is_rejected() {
        let pipelines = r#"
pipelines:
  mmt:
    - "dantto4k"
types:
  BS4K: { }
"#;
        match Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("pipelines.yml"));
                assert!(
                    source.to_string().contains("missing field"),
                    "unexpected parse error: {source}"
                );
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn empty_pipelines_map_is_accepted_as_raw_ts() {
        // No pipelines at all: every type streams raw TS (SPEC §3.4).
        let pipelines = r#"
pipelines: {}
types: {}
"#;
        let config = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines)
            .load()
            .expect("raw-TS operation needs no pipeline");
        assert!(config.pipelines.pipelines.is_empty());
        assert!(config.pipelines.types.is_empty());
    }

    #[test]
    fn omitted_pipelines_and_types_maps_are_accepted() {
        let config = Fixture::new(SERVER, TUNERS, CHANNELS, "{}\n")
            .load()
            .expect("both maps default to empty");
        assert!(config.pipelines.pipelines.is_empty());
        assert!(config.pipelines.types.is_empty());
    }

    #[test]
    fn empty_server_pipelines_section_is_accepted() {
        let pipelines = r#"
pipelines: {}
types:
  GR: { pipeline: soft }
"#;
        // A binding to a pipeline that does not exist is still rejected.
        let issues = Fixture::new(SERVER, TUNERS, CHANNELS, pipelines).issues();
        assert_issue_contains(&issues, "references unknown pipeline");
    }

    #[test]
    fn unknown_field_is_rejected() {
        let tuners = r#"
- name: pt3-gr0
  types: [GR]
  cmd: "recpt1 {channel} -"
"#;
        match Fixture::new(SERVER, tuners, CHANNELS, PIPELINES).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("tuners.yml"));
                assert!(
                    source.to_string().contains("unknown field"),
                    "unexpected parse error: {source}"
                );
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_field_in_server_is_rejected() {
        let server = "addr: \":40772\"\nallowed_cidr:\n  - 10.0.0.0/8\n";
        match Fixture::new(server, TUNERS, CHANNELS, PIPELINES).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("server.yml"));
                assert!(source.to_string().contains("unknown field"));
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[derive(serde::Serialize)]
    struct ChannelFixture<'a> {
        name: &'a str,
        #[serde(rename = "type")]
        channel_type: &'a str,
        channel: &'a str,
        disabled: bool,
    }

    fn channels_yaml(channel: &str) -> String {
        serde_yaml_ng::to_string(&vec![ChannelFixture {
            name: "test",
            channel_type: "GR",
            channel,
            disabled: false,
        }])
        .expect("yaml fixture")
    }

    #[test]
    fn channel_value_with_shell_metacharacters_is_rejected() {
        for bad in [
            "27 28",
            "27;rm -rf /",
            "$(id)",
            "`id`",
            "a|b",
            "a&b",
            "a\"b",
            "a'b",
            "a\\b",
            "a>b",
            "a*b",
            "C17\n",
            "",
        ] {
            let issues = Fixture::new(SERVER, TUNERS, &channels_yaml(bad), PIPELINES).issues();
            assert_issue_contains(&issues, ".channel: ");
            assert_issue_contains(&issues, "channel value");
        }
    }

    #[test]
    fn channel_value_allows_expected_charset() {
        for good in [
            "27", "BS13_1", "C17", "13.1", "a+b", "a:b", "a@b", "a,b", "a-b",
        ] {
            assert!(
                validate_channel_value(good).is_ok(),
                "{good:?} should be allowed"
            );
        }
        let channels = r#"
- name: C17
  type: GR
  channel: "C17"
"#;
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("C17 is a valid channel value");
    }

    #[test]
    fn empty_tuner_types_is_rejected() {
        let tuners = r#"
- name: pt3-gr0
  types: []
  command: "recpt1 {channel} -"
"#;
        let issues = Fixture::new(SERVER, tuners, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "at least one type name");
    }

    #[test]
    fn empty_command_is_rejected() {
        let tuners = r#"
- name: pt3-gr0
  types: [GR]
  command: "  "
"#;
        let issues = Fixture::new(SERVER, tuners, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "command must not be empty");
    }

    #[test]
    fn invalid_cidr_is_rejected() {
        let server = "addr: \":40772\"\nallowed_cidrs: [\"10.0.0.0/99\"]\n";
        let issues = Fixture::new(server, TUNERS, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "not a valid CIDR");
    }

    #[test]
    fn invalid_addr_is_rejected() {
        let server = "addr: \"not-an-address\"\n";
        let issues = Fixture::new(server, TUNERS, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "not a valid socket address");
    }

    #[test]
    fn missing_file_is_reported_with_its_path() {
        let fixture = Fixture::valid();
        fs::remove_file(fixture.path.join("channels.yml")).expect("remove");
        match fixture.load() {
            Err(ConfigError::Read { path, .. }) => {
                assert!(path.ends_with("channels.yml"));
            }
            other => panic!("expected a read error, got {other:?}"),
        }
    }

    #[test]
    fn type_name_validation() {
        for ok in ["GR", "BS", "CABLE_CATV", "gr", "a", "0", "A-b_9"] {
            assert!(validate_type_name(ok).is_ok(), "{ok:?} should be valid");
        }
        for bad in ["", "GR!", "gr space", "-GR", "_GR", "gr.space", "ぐら"] {
            assert!(
                validate_type_name(bad).is_err(),
                "{bad:?} should be invalid"
            );
        }
    }

    #[test]
    fn channel_value_validation() {
        for ok in [
            "27", "BS13_1", "C17", "13.1", "a-b", "a_b", "a+b", "a:b", "a@b",
        ] {
            assert!(validate_channel_value(ok).is_ok(), "{ok:?} should be valid");
        }
        for bad in [
            "", " ", "a b", "a;b", "$x", "`x`", "a\\b", "a'b", "a\"b", "{x}", "[x]", "a/b", "a\nb",
        ] {
            assert!(
                validate_channel_value(bad).is_err(),
                "{bad:?} should be invalid"
            );
        }
    }

    #[test]
    fn allowed_hosts_defaults_to_empty_and_is_parsed_when_present() {
        let config = Fixture::valid().load().expect("valid config");
        assert!(config.server.allowed_hosts.is_empty());

        let server = "addr: \":40772\"\nallowed_hosts:\n  - tv.example.lan\n  - living-room\n";
        let config = Fixture::new(server, TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("allowed_hosts is accepted");
        assert_eq!(
            config.server.allowed_hosts,
            vec!["tv.example.lan".to_string(), "living-room".to_string()]
        );
    }

    #[test]
    fn bad_allowed_hosts_entries_are_rejected() {
        for bad in [
            "\"\"",
            "\"   \"",
            "tv.example.lan:40772",
            "\"http://tv.example.lan\"",
            "\"[::1]\"",
            "\"tv example\"",
            "\"tv/example\"",
        ] {
            let server = format!("addr: \":40772\"\nallowed_hosts: [{bad}]\n");
            let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
            assert_issue_contains(&issues, "allowed_hosts[0]");
        }
    }

    #[test]
    fn host_allowlist_accepts_loopback_private_and_listed_names() {
        let cidrs: Vec<String> = DEFAULT_ALLOWED_CIDRS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let listed = vec!["tv.example.lan".to_string()];

        for ok in [
            "localhost",
            "localhost:40772",
            "LOCALHOST:40772",
            "127.0.0.1:40772",
            "192.168.1.5:40772",
            "10.1.2.3",
            "[::1]:40772",
            "[::1]",
            "::1",
            "tv.example.lan",
            "TV.Example.LAN:40772",
            "localhost.",
        ] {
            assert!(
                host_is_allowed(ok, &listed, &cidrs),
                "{ok:?} should be allowed"
            );
        }

        for bad in [
            "evil.example",
            "evil.example:40772",
            "203.0.113.9:40772",
            "[2001:db8::1]:40772",
            "",
            "   ",
            "localhost:not-a-port",
        ] {
            assert!(
                !host_is_allowed(bad, &listed, &cidrs),
                "{bad:?} should be rejected"
            );
        }

        // Without `allowed_hosts` a name is rejected, which is the default.
        assert!(!host_is_allowed("tv.example.lan", &[], &cidrs));
    }

    #[test]
    fn allowed_hosts_adds_names_and_private_ips_outside_the_cidrs() {
        let listed = vec!["tv.example.lan".to_string(), "203.0.113.9".to_string()];
        let cidrs = vec!["127.0.0.0/8".to_string()];
        assert!(host_is_allowed("tv.example.lan", &listed, &cidrs));
        assert!(host_is_allowed("203.0.113.9:40772", &listed, &cidrs));
        assert!(!host_is_allowed("192.168.1.5", &listed, &cidrs));
    }

    #[test]
    fn normalize_host_strips_ports_brackets_and_case() {
        assert_eq!(
            normalize_host("LocalHost:40772").as_deref(),
            Some("localhost")
        );
        assert_eq!(normalize_host("[::1]:40772").as_deref(), Some("::1"));
        assert_eq!(normalize_host("[::1]").as_deref(), Some("::1"));
        assert_eq!(
            normalize_host("192.168.1.5").as_deref(),
            Some("192.168.1.5")
        );
        assert_eq!(normalize_host("host:bad"), None);
        assert_eq!(normalize_host("[::1]:bad"), None);
        assert_eq!(normalize_host(""), None);
        assert_eq!(normalize_host("a b"), None);
        assert_eq!(normalize_host("host/path"), None);
    }

    #[test]
    fn issues_display_file_and_item() {
        let issues = validate(&Config {
            server: ServerConfig::default(),
            tuners: vec![TunerConfig {
                name: "t".into(),
                types: vec!["GR!".into()],
                command: "cmd".into(),
                disabled: false,
            }],
            channels: Vec::new(),
            pipelines: PipelinesConfig {
                pipelines: HashMap::from([("soft".to_string(), vec!["cmd".to_string()])]),
                types: HashMap::new(),
            },
        });
        assert_eq!(issues.len(), 1);
        let text = issues[0].to_string();
        assert!(
            text.starts_with("tuners.yml: [0] (\"t\").types[0]: "),
            "{text}"
        );
    }

    // -----------------------------------------------------------------------
    // extra-args / routes (SPEC §3.3, §6.4)
    // -----------------------------------------------------------------------

    /// The route channels fixture: `CHANNELS` plus a routed channel that uses
    /// the two tuners from `TUNERS` (GR + BS/CABLE_CATV).
    fn routes_fixture(channels: &str) -> Fixture {
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
    }

    #[test]
    fn extra_args_defaults_to_empty_and_is_parsed_on_the_channel() {
        let config = Fixture::valid().load().expect("valid config");
        assert!(config
            .channels
            .iter()
            .all(|channel| channel.extra_args.is_empty()));
        assert!(config
            .channels
            .iter()
            .all(|channel| channel.routes.is_empty()));

        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  extra-args: "--lnb 15"
"#;
        // The tuner set does not matter for parsing; use the default one.
        let config = Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("extra-args is accepted");
        assert_eq!(config.channels[0].extra_args, "--lnb 15");
    }

    #[test]
    fn extra_args_accepts_the_underscore_alias() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  extra_args: "--lnb 15"
"#;
        let config = Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("extra_args alias is accepted");
        assert_eq!(config.channels[0].extra_args, "--lnb 15");
    }

    #[test]
    fn routes_parse_with_optional_channel_and_extra_args() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: pt3-gr0
      channel: "13"
      extra-args: "--lnb 15"
    - tuner: pt3-bs0
"#;
        let config = routes_fixture(channels)
            .load()
            .expect("routes are accepted");
        let routes = &config.channels[0].routes;
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].tuner, "pt3-gr0");
        assert_eq!(routes[0].channel.as_deref(), Some("13"));
        assert_eq!(routes[0].extra_args.as_deref(), Some("--lnb 15"));
        assert_eq!(routes[1].tuner, "pt3-bs0");
        assert!(routes[1].channel.is_none());
        assert!(routes[1].extra_args.is_none());
    }

    #[test]
    fn routes_accept_the_underscore_extra_args_alias() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: pt3-gr0
      extra_args: "--lnb 15"
"#;
        let config = routes_fixture(channels)
            .load()
            .expect("extra_args route alias is accepted");
        assert_eq!(
            config.channels[0].routes[0].extra_args.as_deref(),
            Some("--lnb 15")
        );
    }

    #[test]
    fn a_route_naming_an_unknown_tuner_is_rejected() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: nope
"#;
        let issues = routes_fixture(channels).issues();
        assert_issue_contains(&issues, ".routes[0].tuner");
        assert_issue_contains(&issues, "references unknown tuner");
        assert_issue_contains(&issues, "\"nope\"");
    }

    #[test]
    fn a_duplicate_tuner_in_routes_is_rejected() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: pt3-gr0
    - tuner: pt3-gr0
"#;
        let issues = routes_fixture(channels).issues();
        assert_issue_contains(&issues, "duplicate tuner");
        assert_issue_contains(&issues, "pt3-gr0");
    }

    #[test]
    fn a_bad_route_channel_value_is_rejected() {
        // A space makes it two argv tokens, which the channel charset forbids.
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: pt3-gr0
      channel: "13 14"
"#;
        let issues = routes_fixture(channels).issues();
        assert_issue_contains(&issues, ".routes[0].channel");
        assert_issue_contains(&issues, "channel value");
    }

    #[test]
    fn unknown_fields_in_a_route_are_rejected() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tunerr: pt3-gr0
"#;
        match routes_fixture(channels).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("channels.yml"));
                assert!(
                    source.to_string().contains("unknown field"),
                    "unexpected parse error: {source}"
                );
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_fields_on_a_channel_are_still_rejected_with_routes_support() {
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  extra_argz: "--lnb 15"
"#;
        match routes_fixture(channels).load() {
            Err(ConfigError::Parse { path, source }) => {
                assert!(path.ends_with("channels.yml"));
                assert!(source.to_string().contains("unknown field"));
            }
            other => panic!("expected a parse error, got {other:?}"),
        }
    }

    #[test]
    fn tuner_startup_probe_defaults_to_1000_and_zero_is_rejected() {
        let config = Fixture::valid().load().expect("valid config");
        assert_eq!(config.server.tuner_startup_probe_ms, 1000);

        let server = format!("{SERVER}\ntuner_startup_probe_ms: 0\n");
        let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "tuner_startup_probe_ms");
        assert_issue_contains(&issues, "must be greater than 0");

        let server = format!("{SERVER}\ntuner_startup_probe_ms: 250\n");
        let config = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("a positive probe window is accepted");
        assert_eq!(config.server.tuner_startup_probe_ms, 250);
    }

    #[test]
    fn server_defaults_include_the_probe_window() {
        let fixture = Fixture::new("{}", TUNERS, CHANNELS, PIPELINES);
        let config = fixture.load().expect("valid config");
        assert_eq!(
            config.server.tuner_startup_probe_ms,
            DEFAULT_TUNER_STARTUP_PROBE_MS
        );
    }

    #[test]
    fn server_defaults_cover_child_env_and_timeouts() {
        let config = Fixture::new("{}", TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("valid config");
        assert_eq!(config.server.child_path, DEFAULT_CHILD_PATH);
        assert_eq!(config.server.child_home, DEFAULT_CHILD_HOME);
        assert_eq!(config.server.child_tmpdir, DEFAULT_CHILD_TMPDIR);
        assert_eq!(config.server.child_cwd, DEFAULT_CHILD_CWD);
        assert!(config.server.child_env.is_empty());
        assert_eq!(
            config.server.tuner_readiness_timeout(),
            Duration::from_secs(DEFAULT_TUNER_READINESS_TIMEOUT_SECS)
        );
        assert_eq!(
            config.server.scan_channel_timeout(),
            Duration::from_secs(DEFAULT_SCAN_CHANNEL_TIMEOUT_SECS)
        );
    }

    #[test]
    fn child_env_is_parsed_and_an_empty_cwd_is_allowed() {
        let server =
            format!("{SERVER}\nchild_cwd: \"\"\nchild_env:\n  PATH: /custom/bin\n  FOO: bar\n");
        let config = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("valid config");
        assert!(config.server.child_cwd.is_empty());
        assert_eq!(config.server.child_env["PATH"], "/custom/bin");
        assert_eq!(config.server.child_env["FOO"], "bar");
    }

    #[test]
    fn an_empty_child_path_home_or_tmpdir_is_rejected() {
        for field in ["child_path", "child_home", "child_tmpdir"] {
            let server = format!("{SERVER}\n{field}: \"\"\n");
            let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
            assert_issue_contains(&issues, field);
            assert_issue_contains(&issues, "must not be empty");
        }
        // `child_cwd` is the one field allowed to be empty.
        Fixture::new(
            &format!("{SERVER}\nchild_cwd: \"\"\n"),
            TUNERS,
            CHANNELS,
            PIPELINES,
        )
        .load()
        .expect("an empty child_cwd inherits the server's working directory");
    }

    #[test]
    fn child_paths_with_nul_or_newlines_are_rejected() {
        for field in ["child_path", "child_home", "child_tmpdir", "child_cwd"] {
            for raw in ["x\0y", "x\ny", "x\ry"] {
                let escaped = raw
                    .replace('\0', "\\0")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r");
                let server = format!("{SERVER}\n{field}: \"{escaped}\"\n");
                let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
                assert_issue_contains(&issues, field);
                assert_issue_contains(&issues, "NUL or newlines");
            }
        }
    }

    #[test]
    fn timeout_defaults_are_30_and_60_and_bounded() {
        assert_eq!(DEFAULT_TUNER_READINESS_TIMEOUT_SECS, 30);
        assert_eq!(DEFAULT_SCAN_CHANNEL_TIMEOUT_SECS, 60);
        for field in ["tuner_readiness_timeout_secs", "scan_channel_timeout_secs"] {
            let server = format!("{SERVER}\n{field}: 0\n");
            let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
            assert_issue_contains(&issues, field);
            assert_issue_contains(&issues, "must be greater than 0");

            let server = format!("{SERVER}\n{field}: {}\n", MAX_SCAN_TIMEOUT_SECS + 1);
            let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
            assert_issue_contains(&issues, field);
            assert_issue_contains(&issues, "must be at most");
        }
    }

    #[test]
    fn the_scan_budget_must_exceed_the_readiness_gate() {
        let server =
            format!("{SERVER}\ntuner_readiness_timeout_secs: 30\nscan_channel_timeout_secs: 30\n");
        let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "scan_channel_timeout_secs");
        assert_issue_contains(&issues, "strictly greater");
        assert_issue_contains(&issues, "30 <= 30");

        let server =
            format!("{SERVER}\ntuner_readiness_timeout_secs: 30\nscan_channel_timeout_secs: 31\n");
        Fixture::new(&server, TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("one second above the readiness gate is valid");
    }

    #[test]
    fn a_probe_window_above_the_upper_bound_is_rejected() {
        let server = format!(
            "{SERVER}\ntuner_startup_probe_ms: {}\n",
            MAX_TUNER_STARTUP_PROBE_MS + 1
        );
        let issues = Fixture::new(&server, TUNERS, CHANNELS, PIPELINES).issues();
        assert_issue_contains(&issues, "tuner_startup_probe_ms");
        assert_issue_contains(&issues, "must be at most");

        // The bound itself is accepted.
        let server = format!("{SERVER}\ntuner_startup_probe_ms: {MAX_TUNER_STARTUP_PROBE_MS}\n");
        Fixture::new(&server, TUNERS, CHANNELS, PIPELINES)
            .load()
            .expect("the upper bound is accepted");
    }

    #[test]
    fn extra_args_may_not_contain_shell_breaking_characters() {
        // Each raw value contains one refused character; it is YAML-escaped so
        // it reaches the parser as that character (not as YAML syntax).
        for raw in ["x'y", "x\"y", "x\\y", "x\ny"] {
            let yaml_value = raw
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            // Channel-level.
            let channels = format!(
                "- name: AT-X\n  type: CS\n  channel: AT-X\n  extra-args: \"{yaml_value}\"\n"
            );
            let issues = routes_fixture(&channels).issues();
            assert_issue_contains(&issues, ".extra-args");
            assert_issue_contains(&issues, "invalid extra-args");

            // Route-level.
            let channels = format!(
                "- name: AT-X\n  type: CS\n  channel: AT-X\n  routes:\n    - tuner: pt3-gr0\n      extra-args: \"{yaml_value}\"\n"
            );
            let issues = routes_fixture(&channels).issues();
            assert_issue_contains(&issues, ".routes[0].extra-args");
            assert_issue_contains(&issues, "invalid extra-args");
        }

        // Spaces between arguments are fine: it stays one argv fragment.
        let channels = "- name: AT-X\n  type: CS\n  channel: AT-X\n  extra-args: \"--lnb 15 -x\"\n";
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("spaces are allowed in extra-args");
    }

    #[test]
    fn two_routes_with_the_same_effective_channel_must_agree_on_extra_args() {
        // Both routes resolve to effective channel 13 (route 1 inherits the
        // channel's own): the second declares a different extra-args, which
        // could never take effect.
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  routes:
    - tuner: pt3-gr0
      extra-args: "--lnb 15"
    - tuner: pt3-bs0
      extra-args: "--lnb 20"
"#;
        let issues = routes_fixture(channels).issues();
        assert_issue_contains(&issues, "same effective channel");
        assert_issue_contains(&issues, "silently ignored");

        // The same extra-args (or an inherited one) is not a mistake.
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  extra-args: "--lnb 15"
  routes:
    - tuner: pt3-gr0
    - tuner: pt3-bs0
      extra-args: "--lnb 15"
"#;
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("matching extra-args on a shared effective channel is accepted");

        // Different effective channels may have different extra-args.
        let channels = r#"
- name: AT-X
  type: CS
  channel: AT-X
  extra-args: "--lnb 15"
  routes:
    - tuner: pt3-gr0
    - tuner: pt3-bs0
      channel: "99"
      extra-args: "--lnb 20"
"#;
        Fixture::new(SERVER, TUNERS, channels, PIPELINES)
            .load()
            .expect("distinct effective channels are independent");
    }
}

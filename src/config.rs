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
/// One default subscriber queue costs `65536 × 1024 = 64 MiB`, so 256 MiB fits
/// four of them at once. The per-session cap bounds a single session; this
/// budget bounds the whole process, so 32 × 64 MiB can never happen. Raise it
/// (or lower `subscriber_buffer_chunks`) to serve more concurrent viewers.
pub const DEFAULT_MAX_SUBSCRIBER_BUFFER_BYTES: usize = 256 * 1024 * 1024;

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
    /// exceed it is refused with HTTP 429. With the default 256 MiB, at most
    /// four default-sized subscriber queues fit at once.
    #[serde(default = "default_max_subscriber_buffer_bytes")]
    pub max_subscriber_buffer_bytes: usize,
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
        }
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
    #[serde(default)]
    pub disabled: bool,
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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineBinding {
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
}

/// Collects every validation issue in `config`.
pub fn validate(config: &Config) -> Vec<Issue> {
    let mut issues = Vec::new();
    validate_server(&config.server, &mut issues);
    validate_tuners(&config.tuners, &mut issues);
    validate_channels(&config.channels, &mut issues);
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

fn validate_channels(channels: &[ChannelConfig], issues: &mut Vec<Issue>) {
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
        if !seen.insert((channel.channel_type.as_str(), channel.channel.as_str())) {
            issues.push(Issue::new(
                "channels.yml",
                label,
                format!(
                    "duplicate (type, channel) pair ({:?}, {:?})",
                    channel.channel_type, channel.channel
                ),
            ));
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
}

//! Channel discovery and the `PUT /api/config/channels/scan` implementation
//! (SPEC §8.1).
//!
//! # Flow
//!
//! For every candidate channel number in `[minCh, maxCh]` (minus `skipCh`) the
//! scanner tunes through [`SessionManager`] as a normal (but pipeline-running)
//! session at [`SCAN_PRIORITY`] and reads the **unfiltered** broadcast, exactly
//! like any other subscriber (SPEC §8: scan is a dedicated unfiltered
//! subscriber, never a filtered one). SI sections are parsed with
//! [`SiParser`]; SDT service names and NIT remote-control key ids fill the
//! [`ServiceTable`] and the generated `channels.yml` entries.
//!
//! The candidate list is capped at [`MAX_SCAN_CHANNELS`] channels per request
//! (including the subchannel expansion and the `skipCh` list). The range is
//! generated lazily, so progress starts with the first channel instead of after
//! materialising a multi-billion-entry vector. A request above the cap is a 400
//! from [`ScanQuery::from_params`].
//!
//! # Per-type defaults
//!
//! The mirakurun defaults only apply to the literal built-in type names:
//!
//! | type | `minCh` / `maxCh` | `scanMode` | `setDisabledOnAdd` | name format |
//! |------|-------------------|------------|--------------------|-------------|
//! | `GR` | 13 / 62 | `Channel` | `false` | `{ch}` |
//! | `BS` | 1 / 24 | `Service` | `true` | `BS{ch00}_{subch}` |
//! | `CS` | 1 / 24 | `Service` | `true` | `CS{ch00}` |
//!
//! Any other type name (`BS4K`, `CABLE_CATV`, ...) is user-defined: it has no
//! conventional channel numbering, so guessing a range would tune channels the
//! caller never asked for. Such a scan must pass **both** `minCh` and `maxCh`
//! (otherwise [`ScanQuery::from_params`] returns the 400 message) and gets the
//! conservative defaults `scanMode=Channel`, `setDisabledOnAdd=false` and
//! `channelNameFormat="{ch}"` (the bare number). The configuration schema is
//! unchanged: no per-type table is added to `channels.yml`.
//!
//! # Subchannels
//!
//! `useSubCh=true` (or the `BS` type) expands every channel number into the
//! requested subchannels. `minSubCh` / `maxSubCh` are comma-separated lists of
//! ranges: the pair at index `i` means `min[i]..=max[i]`, a shorter list pads
//! with its last value, and the union is tuned in ascending order. The cross
//! product is walked channel outer, subchannel inner, and each progress line
//! names the subchannel it is tuning. Values are ARIB BS subchannels, so
//! `0..=3`; anything else is a 400.
//!
//! A channel that produces no signal, times out, or whose tuner is busy is
//! skipped and the scan continues (SPEC §8.1). Each per-channel session is
//! released before the next channel is tuned.
//!
//! Outcomes are reported truthfully (SPEC §8): a channel whose type has no
//! configured tuner is `skipped (no tuner for type <type>)`, a tuner command
//! that cannot start or exits before its first data is `tuner failed to start:
//! <short reason>`, and only a channel whose session was actually acquired and
//! produced no usable SI is `no signal`. The startup scan retries only `Busy`
//! and `Preempted`; `NoTuner` and `TunerFailed` would only spin.
//!
//! A channel the scan loses to a **user request** (any non-negative priority
//! beats [`SCAN_PRIORITY`]) is reported as preempted, never as "no signal": the
//! user keeps the tuner and the startup scan retries the channel with the same
//! backoff as a busy one, so its services still appear later.
//!
//! # SI completeness
//!
//! Only the **SDT-actual** (`0x42`) describes the multiplex being received, so
//! `ChannelSink` ignores SDT-other (`0x46`, another transponder) entirely. A
//! multi-section SDT is accumulated until every `section_number` up to
//! `last_section_number` of one table version has arrived, and only then
//! replaces the channel's service list; a table that never completes still
//! contributes the sections that did arrive when the channel budget expires.
//!
//! # Cancellation
//!
//! A scan stops when its progress receiver goes away (the HTTP client
//! disconnected) and when the server-wide shutdown watch fires: both are
//! observed between channels, while waiting for the tuner to become ready, and
//! while waiting for SI, and neither writes `channels.yml` nor touches the
//! service table. The acquire itself may also be cut off by the per-channel
//! budget: the session layer is cancellation-safe, so a cut-off acquire (by the
//! budget *or* by a shutdown) stops its own process tree rather than leaking the
//! tuner. [`scan_channel`] then waits for that teardown before the next channel,
//! so one abandoned channel cannot report the next one as "busy".
//!
//! # Startup scan
//!
//! The same per-channel helper ([`scan_channel`]) also drives the SPEC §8
//! startup service scan ([`startup_scan`]): every enabled `channels.yml` entry
//! is tuned once, the found services are upserted into the shared
//! [`ServiceTable`], and channels that were busy or preempted by a user are
//! retried in the background with exponential backoff. A channel with no tuner
//! or a tuner that failed to start is not retried (that would only spin), and
//! an all-start-failure pass does not persist an empty cache. The task stops on
//! a shutdown signal and never preempts a user ([`SCAN_PRIORITY`]).
//!
//! # Writing `channels.yml`
//!
//! Only appended entries are written: entries of other types (and of the same
//! type outside the scanned channel range) are preserved at the YAML level.
//! With `refresh=true` an entry of the scanned type is replaced only when the
//! scan actually read SI on that channel; a no-signal or busy channel keeps its
//! existing entry, so a failed refresh can never delete a working channel. In
//! `Service` mode every produced per-service entry is replaced, not just the
//! first one. The merged list is validated with the same rules as startup
//! (`Config::validate`) before it is written through a `.tmp` file plus rename,
//! so a failed write can never truncate the live configuration. The write takes
//! the same per-file lock as the webui save and re-reads the file inside it, so
//! a concurrent webui save is never silently lost. When the merged list is
//! semantically identical to the file's current content (compared as parsed
//! entries, never as bytes) nothing is written at all: re-serialising would only
//! change YAML quoting (`"24"` → `'24'`) and therefore the file hash, which makes
//! an unchanged configuration look edited.
//!
//! `dryRun=true` skips both the write and the service-table update: it only
//! prints what a real scan would do. A non-dry-run scan commits in two steps:
//! the atomic `channels.yml` write first, then the shared [`ServiceTable`]
//! update (upsert, `retain_channel` for the entries that produced services, and
//! one final `prune_unbound`). A cancelled scan (a disconnected client or a
//! shutdown) therefore leaves both the file and the table untouched, and a scan
//! that finds nothing new still updates the table without rewriting the file.
//!
//! A completed scan prints `**RESTART REQUIRED** to apply changes.`: the
//! running server keeps its in-memory configuration (SPEC §8.1 / §10).

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::config::{Config, ConfigError};
use crate::services::{
    read_service_cache, service_fingerprint, write_service_cache, ServiceRecord, ServiceTable,
    SERVICE_CACHE_FILE, SERVICE_CACHE_VERSION,
};
use crate::session::{SessionManager, SessionTap, TunerUser, SCAN_PRIORITY};
use crate::si::{SdtInfo, SiEvent, SiParser, TableCompleteness};
use crate::webui::store;

/// Default per-channel scan budget (SPEC §8: "既定 timeout 10s/ch").
///
/// This budget covers **both** the acquire (including the session readiness
/// wait, [`crate::session::READINESS_TIMEOUT`] = 10 s) and reading SI, so it
/// must stay strictly greater than the readiness timeout: otherwise the scan's
/// own `timeout_at` would always fire first and cut the acquire off from
/// outside. 25 s leaves the readiness gate its 10 s plus up to 5 s of waiting
/// for a concurrent spawn, with the rest for SI.
///
/// The session layer is cancellation-safe (see [`crate::session`]), so even a
/// cut-off acquire now stops its tree instead of leaking the tuner; the larger
/// budget simply avoids the cancellation on a merely slow tuner.
pub const CHANNEL_TIMEOUT: Duration = Duration::from_secs(25);

/// Compile-time guard for the budget relationship documented on
/// [`CHANNEL_TIMEOUT`]: a channel budget at or below the readiness timeout would
/// make the scan cancel every slow spawn.
const _: () = assert!(CHANNEL_TIMEOUT.as_secs() > crate::session::READINESS_TIMEOUT.as_secs());

/// Hard cap on the channels one scan request may touch.
///
/// The range is user supplied; without a cap `minCh=0&maxCh=4294967295` builds
/// a four-billion-element list (remote OOM/hang). 512 covers a full BS/CS
/// sweep with subchannels and is small enough to stay a handful of bytes of
/// state per candidate.
pub const MAX_SCAN_CHANNELS: usize = 512;

/// Highest ARIB BS subchannel number (SPEC §8.1 `minSubCh` / `maxSubCh`).
///
/// A BS multiplex is split into subchannels `0..=3`; anything above that is not
/// a subchannel but a typo (or a non-BS input), so the scan rejects it instead
/// of tuning a channel that cannot exist.
pub const MAX_SUBCHANNEL: u32 = 3;

/// First retry delay for a startup-scan channel whose tuner was busy
/// (SPEC §8: "相手がbusyならskip＋次回retry").
pub const STARTUP_RETRY_INITIAL: Duration = Duration::from_secs(30);

/// Upper bound of the startup-scan retry backoff.
pub const STARTUP_RETRY_MAX: Duration = Duration::from_secs(300);

/// Service types kept in `channels.yml` and the service table (SPEC §8.1):
/// digital TV, digital audio, temporary video, engineering, promotion, 4K and
/// data-only.
pub const SCAN_SERVICE_TYPES: [u8; 7] = [0x01, 0x02, 0xA1, 0xA4, 0xA5, 0xAD, 0xC0];

/// What a scan writes per tuned channel (SPEC §8.1 `scanMode`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanMode {
    /// One `channels.yml` entry per channel.
    Channel,
    /// One `channels.yml` entry per service found on the channel.
    Service,
}

/// The query parameters of `PUT /api/config/channels/scan` (SPEC §8.1).
#[derive(Clone, Debug)]
pub struct ScanQuery {
    pub type_name: String,
    pub min_channel: u32,
    pub max_channel: u32,
    /// Numbers skipped via `skipCh` (comma separated).
    pub skip_channels: Vec<u32>,
    pub refresh: bool,
    pub dry_run: bool,
    pub mode: ScanMode,
    pub set_disabled_on_add: bool,
    /// `channelNameFormat`; `None` means the per-type default.
    pub channel_name_format: Option<String>,
    pub use_sub_channel: bool,
    /// The subchannel numbers to tune for every channel number, in the order
    /// they were requested. One entry (`0`) unless `useSubCh`/`BS` expands.
    pub sub_channels: Vec<u32>,
    pub timeout: Duration,
}

impl ScanQuery {
    /// Parses the query string, applying the SPEC §8.1 defaults per type.
    ///
    /// `type` is required. `async` (if present) is ignored: the endpoint is
    /// always synchronous.
    pub fn from_params(params: &std::collections::HashMap<String, String>) -> Result<Self, String> {
        let type_name = params
            .get("type")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "type is required".to_string())?;
        crate::config::validate_type_name(&type_name)?;

        let satellite = matches!(type_name.as_str(), "BS" | "CS");
        let builtin = satellite || type_name == "GR";
        // The BS/CS/GR defaults are only meaningful for those literal type
        // names: a user-defined type (`BS4K`, `CABLE_CATV`, ...) has no
        // conventional channel numbering, so guessing a range would tune
        // unrelated channels. Requiring both bounds keeps the config schema
        // stable (SPEC §8.1 accepts any type string) without inventing a
        // per-type default table.
        let min_channel = parse_u32(params, "minCh")?;
        let max_channel = parse_u32(params, "maxCh")?;
        if !builtin && (min_channel.is_none() || max_channel.is_none()) {
            return Err(format!(
                "type {type_name:?} is not a built-in type (GR/BS/CS): \
                 minCh and maxCh are required"
            ));
        }
        let min_channel = min_channel.unwrap_or(if satellite { 1 } else { 13 });
        let max_channel = max_channel.unwrap_or(if satellite { 24 } else { 62 });
        if min_channel > max_channel {
            return Err(format!(
                "minCh ({min_channel}) must not be greater than maxCh ({max_channel})"
            ));
        }

        let refresh = parse_bool(params, "refresh")?.unwrap_or(false);
        let dry_run = parse_bool(params, "dryRun")?.unwrap_or(false);
        let mode = match params.get("scanMode").map(|value| value.trim()) {
            // mirakurun's defaults: BS/CS scan per service, everything else
            // (GR and user-defined types) per channel.
            None | Some("") => {
                if satellite {
                    ScanMode::Service
                } else {
                    ScanMode::Channel
                }
            }
            Some(value) if value.eq_ignore_ascii_case("Channel") => ScanMode::Channel,
            Some(value) if value.eq_ignore_ascii_case("Service") => ScanMode::Service,
            Some(other) => {
                return Err(format!(
                    "invalid scanMode {other:?}: expected Service or Channel"
                ))
            }
        };
        let set_disabled_on_add = parse_bool(params, "setDisabledOnAdd")?.unwrap_or(satellite);
        let use_sub_channel = parse_bool(params, "useSubCh")?.unwrap_or(false);
        let channel_name_format = params
            .get("channelNameFormat")
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let skip_channels = parse_list(params.get("skipCh"))?;
        if skip_channels.len() > MAX_SCAN_CHANNELS {
            return Err(format!(
                "skipCh lists {} channels; at most {MAX_SCAN_CHANNELS} are allowed",
                skip_channels.len()
            ));
        }
        let min_sub_channels = parse_list(params.get("minSubCh"))?;
        let max_sub_channels = parse_list(params.get("maxSubCh"))?;
        // mirakurun ignores the subchannel parameters for a type that is not
        // expanded; validating them anyway would reject a request whose result
        // does not depend on them.
        let sub_channels = if use_sub_channel || type_name == "BS" {
            sub_channels_from(&min_sub_channels, &max_sub_channels)?
        } else {
            vec![0]
        };

        let query = Self {
            type_name,
            min_channel,
            max_channel,
            skip_channels,
            refresh,
            dry_run,
            mode,
            set_disabled_on_add,
            channel_name_format,
            use_sub_channel,
            sub_channels,
            timeout: CHANNEL_TIMEOUT,
        };
        // A remote request must not build an unbounded candidate list.
        let count = query.candidate_count();
        if count > MAX_SCAN_CHANNELS as u64 {
            return Err(format!(
                "channel range covers {count} channels; at most {MAX_SCAN_CHANNELS} are allowed"
            ));
        }
        Ok(query)
    }

    /// Whether the scan tunes one candidate per subchannel.
    ///
    /// Only `BS` expands by default (SPEC §8.1); any other type expands only
    /// with `useSubCh=true`, exactly like mirakurun.
    fn expands_subchannels(&self) -> bool {
        self.use_sub_channel || self.type_name == "BS"
    }

    /// How many candidates [`candidates`] yields, without building them.
    ///
    /// Used to reject an oversized request before any work is done; the scan
    /// itself walks the range lazily.
    pub fn candidate_count(&self) -> u64 {
        let numbers = u64::from(self.max_channel) - u64::from(self.min_channel) + 1;
        let subs = self.subchannel_count();
        numbers.saturating_mul(subs)
    }

    /// Number of subchannels expanded per channel number.
    fn subchannel_count(&self) -> u64 {
        if self.expands_subchannels() {
            self.sub_channels.len() as u64
        } else {
            1
        }
    }

    /// The channel name for `number`/`sub`, per `channelNameFormat` or the
    /// per-type default (SPEC §8.1).
    fn channel_name(&self, number: u32, sub: u32) -> String {
        match &self.channel_name_format {
            Some(format) => format
                .replace("{ch}", &number.to_string())
                .replace("{ch00}", &format!("{number:02}"))
                .replace("{subch}", &sub.to_string()),
            // Literal type names get the mirakurun-style defaults; any other
            // type name is user-defined and simply uses the number (SPEC §8.1).
            None => match self.type_name.as_str() {
                "BS" => format!("BS{number:02}_{sub}"),
                "CS" => format!("CS{number:02}"),
                _ => number.to_string(),
            },
        }
    }
}

/// Expands the `minSubCh` / `maxSubCh` lists into the subchannels to tune.
///
/// Both parameters are comma separated (SPEC §8.1). The pair at index `i`
/// describes the range `min[i]..=max[i]`; a list shorter than the other pads
/// with its last value, and a missing bound falls back to `0` (min) or to the
/// paired minimum (max). The subchannels to tune are the union of those ranges
/// in ascending order, so `minSubCh=0&maxSubCh=1` tunes `0, 1` and
/// `minSubCh=0,2&maxSubCh=1,3` tunes `0, 1, 2, 3`.
///
/// Every value must be an ARIB BS subchannel ([`MAX_SUBCHANNEL`]): anything
/// above is a typo or a non-BS input, not a channel that can exist. Both lists
/// are capped at the number of subchannels for the same reason.
fn sub_channels_from(min: &[u32], max: &[u32]) -> Result<Vec<u32>, String> {
    for (name, values) in [("minSubCh", min), ("maxSubCh", max)] {
        if values.len() > MAX_SUBCHANNEL as usize + 1 {
            return Err(format!(
                "{name} lists {} subchannels; at most {} exist",
                values.len(),
                MAX_SUBCHANNEL + 1
            ));
        }
        if let Some(&value) = values.iter().find(|&&value| value > MAX_SUBCHANNEL) {
            return Err(format!(
                "invalid {name} {value}: a BS subchannel is 0..={MAX_SUBCHANNEL}"
            ));
        }
    }

    let pairs = min.len().max(max.len());
    let mut sub_channels: Vec<u32> = Vec::new();
    for index in 0..pairs {
        let low = min.get(index).or_else(|| min.last()).copied().unwrap_or(0);
        let high = max
            .get(index)
            .or_else(|| max.last())
            .copied()
            .unwrap_or(low);
        if low > high {
            return Err(format!(
                "minSubCh ({low}) must not be greater than maxSubCh ({high})"
            ));
        }
        sub_channels.extend(low..=high);
    }
    sub_channels.sort_unstable();
    sub_channels.dedup();
    if sub_channels.is_empty() {
        sub_channels.push(0);
    }
    Ok(sub_channels)
}

fn parse_u32(
    params: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<Option<u32>, String> {
    match params.get(name).map(|value| value.trim()) {
        None | Some("") => Ok(None),
        Some(raw) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|_| format!("invalid {name} {raw:?}: expected a non-negative integer")),
    }
}

fn parse_bool(
    params: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, String> {
    match params.get(name).map(|value| value.trim()) {
        None | Some("") => Ok(None),
        Some(raw) if raw == "true" || raw == "1" => Ok(Some(true)),
        Some(raw) if raw == "false" || raw == "0" => Ok(Some(false)),
        Some(raw) => Err(format!("invalid {name} {raw:?}: expected true or false")),
    }
}

/// Parses a comma-separated integer list (`skipCh`, `minSubCh`, `maxSubCh`).
fn parse_list(raw: Option<&String>) -> Result<Vec<u32>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<u32>()
                .map_err(|_| format!("invalid channel list item {item:?}: expected an integer"))
        })
        .collect()
}

/// Guards against concurrent scans (SPEC §8.1: 409 `Already Scanning`).
///
/// Cloning shares one flag. A [`ScanLease`] clears it on drop, so a panicking
/// scan task cannot leave the endpoint permanently locked.
#[derive(Clone, Debug, Default)]
pub struct ScanLock {
    running: Arc<AtomicBool>,
}

impl ScanLock {
    /// Takes the lock, or returns `None` when a scan is already running.
    pub fn acquire(&self) -> Option<ScanLease> {
        self.running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| ScanLease(self.clone()))
    }

    /// Whether a scan currently holds the lock.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// An acquired [`ScanLock`]; dropping it releases the lock.
#[derive(Debug)]
pub struct ScanLease(ScanLock);

impl Drop for ScanLease {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::SeqCst);
    }
}

/// One generated `channels.yml` entry.
///
/// Carries the optional `extra-args` / `routes` fields of SPEC §3.3 so a scan
/// can read and write a routed configuration without dropping them. `Default`
/// exists so the scan-internal entry builders can name only the fields they
/// set.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: String,
    pub channel: String,
    /// `extra-args` (canonical) / `extra_args` (alias). Empty is omitted from
    /// the written YAML so an appended entry stays clean.
    #[serde(
        default,
        rename = "extra-args",
        alias = "extra_args",
        skip_serializing_if = "String::is_empty"
    )]
    pub extra_args: String,
    #[serde(default)]
    pub disabled: bool,
    /// Tuner routes (SPEC §3.3, §6.4). Empty is omitted from the written YAML.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<ChannelRouteEntry>,
}

/// One entry of a channel's `routes` (SPEC §3.3).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelRouteEntry {
    /// `name` of a tuner in `tuners.yml`.
    pub tuner: String,
    /// Channel override for this route only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Route-local `extra-args`; `extra_args` is accepted as an alias.
    #[serde(
        default,
        rename = "extra-args",
        alias = "extra_args",
        skip_serializing_if = "Option::is_none"
    )]
    pub extra_args: Option<String>,
}

/// A service found on one scanned channel.
#[derive(Clone, Debug)]
pub(crate) struct FoundService {
    service_id: u16,
    network_id: u16,
    tsid: u16,
    service_type: u8,
    name: String,
    remote_control_key_id: Option<u8>,
    pmt_pid: Option<u16>,
}

/// Folds the SI events of one tuned channel into the services found on it.
///
/// The network id comes from the parser ([`SiParser::network_id`], which only
/// accepts a NIT *actual*, table id `0x40`), then from any NIT-actual seen in
/// this channel's stream, and only then from the SDT's
/// `original_network_id`. A NIT *other* (`0x41`) describes a *different*
/// transport stream and never contributes a network id or a remote-control key
/// id (SPEC §8).
///
/// The SDT is handled the same way: only an **SDT-actual** (`0x42`) describes
/// this multiplex, so an SDT-other (`0x46`, another transponder) never
/// contributes services. A multi-section SDT is accumulated until every
/// `section_number` up to `last_section_number` of one version has arrived
/// ([`TableCompleteness`]); the accumulated set replaces [`found`](Self::found)
/// only once, when the table is complete, so a partially received table never
/// half-populates the result.
#[derive(Debug, Default)]
struct ChannelSink {
    /// The parser's `network_id`, i.e. the NIT-actual's network id.
    parser_network_id: Option<u16>,
    /// `network_id` of a NIT *actual* seen on this channel.
    nit_network_id: Option<u16>,
    /// Remote-control key id from the NIT *actual*.
    remote_control_key_id: Option<u8>,
    /// `service_id -> PMT PID` from the PAT.
    pmt_pids: HashMap<u16, u16>,
    /// `transport_stream_id` of the multiplex: from the PAT (authoritative),
    /// else from the first SDT-actual.
    expected_tsid: Option<u16>,
    /// Sections of the SDT-actual being assembled.
    sdt: SdtAccumulator,
    /// Services from the last complete SDT-actual (or, at the end of the
    /// budget, the partially received one).
    found: Vec<FoundService>,
    /// Whether a **complete** SDT-actual for [`expected_tsid`](Self::expected_tsid)
    /// has already been stored.
    complete: bool,
    /// Whether a PAT has been seen (supplies the PMT PIDs of `found`).
    seen_pat: bool,
    /// Whether a PMT has been seen.
    seen_pmt: bool,
    /// Whether a NIT *actual* has been seen (supplies the network id and the
    /// remote-control key id of `found`).
    seen_nit_actual: bool,
}

/// How long a channel may linger after its SDT-actual completes to collect the
/// remaining core tables (PAT / PMT / NIT-actual) before the scan finishes
/// anyway. A channel that already has its SDT therefore leaves after at most
/// this grace instead of burning the whole [`CHANNEL_TIMEOUT`]; a channel
/// whose NIT never arrives still finishes on the SDT's
/// `original_network_id` fallback when the grace expires.
///
/// Sized at more than one repetition of every table, not just "probably
/// enough": measured on a real GR multiplex, NIT-actual repeats every
/// ~0.4-0.8 s (and PAT/PMT/SDT faster), so a 700 ms window could still miss
/// one NIT repetition — and with it the `network_id` and the
/// `remote_control_key_id` of every service on that channel. 1.5 s covers at
/// least one repetition while staying far below the 25 s budget.
const SI_GRACE_AFTER_SDT: Duration = Duration::from_millis(1500);

/// Accumulates SDT-actual sections until one table version is complete.
#[derive(Debug, Default)]
struct SdtAccumulator {
    completeness: TableCompleteness,
    /// `(original_network_id, transport_stream_id, version_number)` of the
    /// table whose sections are being collected.
    current: Option<(u16, u16, u8)>,
    /// `section_number -> services of that section`.
    sections: HashMap<u8, Vec<(u16, u8, String)>>,
    /// The table version whose services are currently in
    /// [`ChannelSink::found`]. A newer version that is still being assembled
    /// must block the early exit, otherwise a half-received new table would be
    /// discarded while the old one is reported as complete.
    completed: Option<(u16, u16, u8)>,
}

impl SdtAccumulator {
    /// Records one SDT-actual section and, once its table version is complete,
    /// returns the services of every section in section order.
    fn push(&mut self, sdt: &SdtInfo) -> Option<Vec<(u16, u8, String)>> {
        let table = (
            sdt.original_network_id,
            sdt.transport_stream_id,
            sdt.version_number,
        );
        if self.current != Some(table) {
            // A new table (or table version): sections of the previous one
            // must not contribute to it.
            self.current = Some(table);
            self.sections.clear();
            self.completeness.reset();
        }
        self.sections
            .insert(sdt.section_number, sdt.services.clone());
        if !self.completeness.observe(sdt) {
            return None;
        }
        self.completed = Some(table);
        Some(self.collect(sdt.last_section_number))
    }

    /// Whether a *newer* table version has started but not completed yet.
    fn pending_new_version(&self) -> bool {
        match self.current {
            Some(current) => self.completed != Some(current) && !self.sections.is_empty(),
            None => false,
        }
    }

    /// Every collected service, in section order, up to `last_section_number`.
    fn collect(&self, last_section_number: u8) -> Vec<(u16, u8, String)> {
        (0..=last_section_number)
            .filter_map(|number| self.sections.get(&number))
            .flat_map(|services| services.iter().cloned())
            .collect()
    }

    /// Every collected service of a table that did not finish within the
    /// channel budget, in section order.
    ///
    /// The highest received section number bounds the walk, so a gap in the
    /// middle still yields the sections around it instead of nothing.
    fn partial(&self) -> Vec<(u16, u8, String)> {
        match self.sections.keys().max().copied() {
            Some(last) => self.collect(last),
            None => Vec::new(),
        }
    }
}

impl ChannelSink {
    /// The best network id known for this channel, if any.
    fn network_id(&self) -> Option<u16> {
        self.parser_network_id.or(self.nit_network_id)
    }

    /// Applies one parsed table.
    fn push(&mut self, event: &SiEvent, parser_network_id: Option<u16>) {
        if let Some(network_id) = parser_network_id {
            self.parser_network_id = Some(network_id);
        }
        match event {
            SiEvent::Nit(nit) => {
                // Only the NIT *actual* describes the multiplex being received.
                if nit.is_actual {
                    self.seen_nit_actual = true;
                    self.nit_network_id = Some(nit.network_id);
                    self.remote_control_key_id =
                        nit.remote_control_key_id.or(self.remote_control_key_id);
                }
            }
            SiEvent::Pat(pat) => {
                self.seen_pat = true;
                for &(service_id, pmt_pid) in &pat.programs {
                    self.pmt_pids.insert(service_id, pmt_pid);
                }
                // The PAT is authoritative for the multiplex's TSID.
                if self.expected_tsid.is_none() {
                    self.expected_tsid = Some(pat.transport_stream_id);
                }
                for service in &mut self.found {
                    if service.tsid == 0 {
                        service.tsid = pat.transport_stream_id;
                    }
                }
            }
            SiEvent::Sdt(sdt) => self.push_sdt(sdt),
            SiEvent::Pmt(_) => {
                self.seen_pmt = true;
            }
        }
    }

    fn push_sdt(&mut self, sdt: &SdtInfo) {
        // Only the SDT *actual* describes this multiplex (SPEC §8, B10). An
        // SDT-other carries another transponder's services and TSID, so it
        // must not contribute anything, not even the expected TSID.
        if !sdt.is_actual {
            return;
        }
        // A stream can be mislabelled: when the PAT already named the
        // multiplex, an SDT-actual for a different TSID is not ours.
        if self
            .expected_tsid
            .is_some_and(|tsid| tsid != sdt.transport_stream_id)
        {
            return;
        }
        self.expected_tsid = Some(sdt.transport_stream_id);

        let Some(services) = self.sdt.push(sdt) else {
            return;
        };
        let network_id = self.network_id().unwrap_or(sdt.original_network_id);
        self.found = services
            .into_iter()
            .map(|(service_id, service_type, name)| FoundService {
                service_id,
                network_id,
                tsid: sdt.transport_stream_id,
                service_type,
                name,
                remote_control_key_id: None,
                pmt_pid: None,
            })
            .collect();
        self.complete = true;
    }

    /// Whether a complete SDT-actual has been stored and no newer table
    /// version is halfway through being received.
    fn is_complete(&self) -> bool {
        self.complete && !self.found.is_empty() && !self.sdt.pending_new_version()
    }

    /// Whether every core table needed for a finished record has arrived:
    /// PAT (PMT PIDs), a PMT, the complete SDT-actual (service names) and a
    /// NIT-actual (network id / remote-control key id). The scan may exit as
    /// soon as this holds instead of waiting out the channel budget.
    fn has_all_core_tables(&self) -> bool {
        self.is_complete() && self.seen_pat && self.seen_pmt && self.seen_nit_actual
    }

    /// Copies the NIT remote-control key id and the PAT PMT PIDs onto every
    /// service found so far.
    ///
    /// When the budget expires before the SDT table is complete, the sections
    /// that did arrive are used instead: a slow table still yields its services
    /// rather than "no signal".
    fn finish(&mut self) {
        if !self.complete {
            let services = self.sdt.partial();
            if !services.is_empty() {
                let network_id = self.network_id().unwrap_or(0);
                let tsid = self.expected_tsid.unwrap_or(0);
                self.found = services
                    .into_iter()
                    .map(|(service_id, service_type, name)| FoundService {
                        service_id,
                        network_id,
                        tsid,
                        service_type,
                        name,
                        remote_control_key_id: None,
                        pmt_pid: None,
                    })
                    .collect();
            }
        }
        for service in &mut self.found {
            service.remote_control_key_id = self.remote_control_key_id;
            service.pmt_pid = self.pmt_pids.get(&service.service_id).copied();
        }
    }
}

/// Runs a scan to completion, streaming progress text to `tx`.
///
/// Every line is sent as-is; the caller turns them into the `text/plain` body.
/// Errors are reported to the caller after the progress text, which is why the
/// signature returns the error instead of folding it into a line.
///
/// The scan stops early — without writing `channels.yml` — when the progress
/// receiver goes away (the client disconnected) or when `shutdown` fires.
pub async fn run_scan(
    manager: &SessionManager,
    config: &Config,
    config_dir: &Path,
    query: &ScanQuery,
    tx: mpsc::Sender<String>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<(), String> {
    let existing = read_channels(config_dir)?;
    let mut lines = Lines::new(tx);
    lines
        .push(format!(
            "channel scanning... (type: {:?})\n\n",
            query.type_name
        ))
        .await;

    let total = query.candidate_count().max(1);
    let mut found_entries: Vec<ChannelEntry> = Vec::new();
    // Table updates collected during the scan and published only once the file
    // write has succeeded (or was known to be unnecessary). A scan cancelled by
    // a disconnect or a shutdown therefore leaves the table untouched, so the
    // API and `channels.yml` never disagree.
    let mut table_updates: Vec<TableUpdate> = Vec::new();
    // Channels the scan actually produced results for. `refresh=true` may only
    // replace an existing entry for one of these: a no-signal or busy channel
    // must never delete a working configuration entry (SPEC §8.1).
    let mut rescanned: HashSet<String> = HashSet::new();
    // Multiplex families of the candidates that produced `Service` entries
    // (`BS13` for `BS13_0`, `BS13_1`, ...), so a refresh also drops entries the
    // scan no longer produces.
    let mut families: HashSet<String> = HashSet::new();
    // Channels whose scan produced at least one service (used to tell "no
    // signal" apart from "tuned but nothing kept").
    let mut scanned_channels = 0usize;

    // With more than one tuner supporting this type, the channels are tuned
    // in parallel (bounded by the tuner count) while the loop below still
    // prints and processes them in candidate order. With a single tuner the
    // scan stays sequential — parallel acquires would only report `Busy` for
    // each other — and so does a scan whose client has already hung up.
    let candidate_list: Vec<Candidate> = candidates(query).collect();
    let mut parallel: Option<ParallelScan> = None;
    if tuner_count_for(config, &query.type_name) > 1 {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(tuner_count_for(
            config,
            &query.type_name,
        )));
        let mut tasks = tokio::task::JoinSet::new();
        let mut results: Vec<Option<tokio::sync::oneshot::Receiver<(ChannelOutcome, bool)>>> =
            (0..candidate_list.len()).map(|_| None).collect();
        let mut skipped = vec![false; candidate_list.len()];
        for (index, candidate) in candidate_list.iter().enumerate() {
            if skip_reason(query, &existing, candidate).is_some() {
                skipped[index] = true;
                continue;
            }
            // The client can hang up (and shutdown can fire) while the pre-pass
            // is still spawning: without this check a detached scan would keep
            // tuning every candidate for the whole scan budget.
            if lines.is_closed() || cancelled(&shutdown) {
                tasks.abort_all();
                tracing::info!("channel scan cancelled");
                return Ok(());
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            results[index] = Some(rx);
            let manager = manager.clone();
            let semaphore = semaphore.clone();
            let type_name = query.type_name.clone();
            let channel = candidate.channel.clone();
            let timeout = query.timeout;
            let mut task_shutdown = shutdown.clone();
            tasks.spawn(async move {
                let acquired = match task_shutdown.as_mut() {
                    Some(rx) => tokio::select! {
                        permit = semaphore.clone().acquire_owned() => permit.ok(),
                        _ = shutdown_fired(rx) => None,
                    },
                    None => semaphore.clone().acquire_owned().await.ok(),
                };
                let Some(_permit) = acquired else {
                    return;
                };
                let report = match scan_channel_cancellable(
                    &manager,
                    &type_name,
                    &channel,
                    timeout,
                    task_shutdown,
                )
                .await
                {
                    Some(outcome) => (outcome, false),
                    // Shutdown cut the channel off before it reported: pass the
                    // "cancelled" flag through so the loop stops too.
                    None => (ChannelOutcome::NoSignal, true),
                };
                let _ = tx.send(report);
            });
        }
        parallel = Some(ParallelScan {
            tasks,
            results,
            skipped,
        });
    }

    for (index, candidate) in candidate_list.into_iter().enumerate() {
        if lines.is_closed() || cancelled(&shutdown) {
            tracing::info!("channel scan cancelled");
            return Ok(());
        }
        let percent = (index as u64 + 1) * 100 / total;
        // The channel string alone hides which subchannel is being tuned when
        // `channelNameFormat` does not contain `{subch}`, so name both whenever
        // the scan expands subchannels.
        let label = if query.expands_subchannels() {
            format!(
                "{:?} (ch {}, subch {})",
                candidate.channel, candidate.number, candidate.sub
            )
        } else {
            format!("{:?}", candidate.channel)
        };
        lines
            .push(format!(
                "channel: {label} ({}/{}) [{}%] ... ",
                index + 1,
                total,
                percent
            ))
            .await;

        if let Some(reason) = skip_reason(query, &existing, &candidate) {
            lines.push(reason.to_string()).await;
            continue;
        }

        let mut outcome = match parallel.as_mut() {
            Some(parallel) => {
                // Disconnect/shutdown is re-checked per channel: the tasks that
                // are still queued or running are aborted, so a detached scan
                // stops tuning when nobody is listening.
                if lines.is_closed() || cancelled(&shutdown) {
                    parallel.abort();
                    tracing::info!("channel scan cancelled");
                    return Ok(());
                }
                match parallel.outcome(index).await {
                    Some((outcome, channel_cancelled)) => {
                        if channel_cancelled {
                            parallel.abort();
                            tracing::info!("channel scan cancelled");
                            return Ok(());
                        }
                        outcome
                    }
                    None => {
                        // Only reachable if the skip predicates disagreed; the
                        // pre-pass and this loop share `skip_reason`.
                        tracing::warn!(index, "parallel scan lost an outcome");
                        ChannelOutcome::Failed
                    }
                }
            }
            None => {
                let outcome = scan_channel_cancellable(
                    manager,
                    &query.type_name,
                    &candidate.channel,
                    query.timeout,
                    shutdown.clone(),
                )
                .await;
                let Some(outcome) = outcome else {
                    tracing::info!("channel scan cancelled");
                    return Ok(());
                };
                outcome
            }
        };

        // A parallel pass can report `Busy` for a channel that was *itself* the
        // reason: a routed channel is pinned to one tuner, and one tuner can
        // serve several types, so two concurrent channels may fight over one
        // device even though the semaphore counted more. Nothing else is
        // running at this point, so retry the channel on its own before
        // reporting it busy (a sequential scan would have done exactly that).
        if matches!(outcome, ChannelOutcome::Busy) {
            if let Some(parallel) = parallel.as_mut() {
                parallel.wait_all().await;
                if let Some(retry) = scan_channel_cancellable(
                    manager,
                    &query.type_name,
                    &candidate.channel,
                    query.timeout,
                    shutdown.clone(),
                )
                .await
                {
                    outcome = retry;
                }
            }
        }
        match outcome {
            ChannelOutcome::Services(services) => {
                let services: Vec<FoundService> = services
                    .into_iter()
                    .filter(|service| SCAN_SERVICE_TYPES.contains(&service.service_type))
                    .collect();
                lines
                    .push(format!("-> {} services found.\n", services.len()))
                    .await;
                if services.is_empty() {
                    continue;
                }
                scanned_channels += 1;
                let entries = entries_for(query, &existing, &candidate, &services);
                // Record every produced channel: in `Service` mode one
                // candidate yields several entries (`BS01_0/1/2`), and a stale
                // sibling must be replaced too. The multiplex family covers
                // entries the scan no longer produces (a service that
                // disappeared); `merge` drops those on refresh.
                for entry in &entries {
                    rescanned.insert(entry.channel.clone());
                }
                rescanned.insert(candidate.channel.clone());
                if query.mode == ScanMode::Service {
                    families.insert(family_of(&candidate.channel).to_string());
                }
                // A disabled entry is a real channel and is scanned, but its
                // config entry stays disabled (SPEC §8.1). Say so instead of
                // implying the file changed.
                let kept_disabled = entries.iter().any(|entry| {
                    existing.iter().any(|seeded| {
                        seeded.channel_type == entry.channel_type
                            && seeded.channel == entry.channel
                            && seeded.disabled
                    })
                });
                let note = if kept_disabled {
                    " (disabled: kept disabled)"
                } else {
                    ""
                };
                lines
                    .push(format!(
                        "Found {} channels for {}{note}\n",
                        entries.len(),
                        candidate.channel
                    ))
                    .await;
                if query.dry_run {
                    found_entries.extend(entries);
                } else {
                    table_updates.push(TableUpdate {
                        type_name: query.type_name.clone(),
                        mode: query.mode,
                        entries,
                        services,
                    });
                }
            }
            other => {
                if let Some(line) = outcome_line(&query.type_name, &other) {
                    lines.push(line).await;
                }
            }
        }
    }

    // The client may have gone away while the last channel was read.
    if lines.is_closed() || cancelled(&shutdown) {
        tracing::info!("channel scan cancelled before saving");
        return Ok(());
    }

    if query.dry_run {
        let entries = merge(&existing, query, &rescanned, &families, found_entries);
        lines
            .push(format!(
                "\ndry run: {} entries would be written, nothing saved.\n",
                entries.len()
            ))
            .await;
    } else {
        let found_entries: Vec<ChannelEntry> = table_updates
            .iter()
            .flat_map(|update| update.entries.iter().cloned())
            .collect();
        let (entries, written) = write_channels(
            config,
            config_dir,
            query,
            &rescanned,
            &families,
            found_entries,
        )?;
        // The file is now consistent (written, or already identical), so the
        // in-memory table may follow. Doing this after the write is what keeps
        // the API from showing services a cancelled scan never saved.
        for update in &table_updates {
            update_service_table(manager.services(), update);
        }
        manager.services().prune_unbound();
        if written {
            lines
                .push(format!(
                    "\nwrote {} entries to {} ({} channels scanned).\n",
                    entries.len(),
                    config_dir.join("channels.yml").display(),
                    scanned_channels
                ))
                .await;
        } else {
            // Semantically identical: rewriting only changes YAML quoting and
            // the file hash, and a hash change would look like a config edit.
            lines
                .push(format!(
                    "\nchannels.yml unchanged: {} entries ({} channels scanned), nothing written.\n",
                    entries.len(),
                    scanned_channels
                ))
                .await;
        }
    }
    lines
        .push("**RESTART REQUIRED** to apply changes.\n".to_string())
        .await;
    Ok(())
}

/// Whether a shutdown watch has fired (or its sender was dropped).
fn cancelled(shutdown: &Option<watch::Receiver<bool>>) -> bool {
    shutdown.as_ref().is_some_and(stopped)
}

/// Sends progress lines; a hung-up receiver stops the scan instead of wedging
/// it.
///
/// `push` waits for the bounded channel, so a client that reads the stream gets
/// every line (including the final `RESTART REQUIRED`); a client that
/// disconnects is noticed by [`is_closed`](Self::is_closed) on the next line.
struct Lines {
    tx: mpsc::Sender<String>,
}

impl Lines {
    fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
    }

    /// Whether the HTTP client behind this progress stream has disconnected.
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    async fn push(&mut self, line: String) {
        if self.tx.send(line).await.is_err() {
            tracing::debug!("scan progress receiver is gone");
        }
    }
}

/// One channel number (and subchannel, for BS) to tune.
#[derive(Clone, Debug)]
struct Candidate {
    number: u32,
    sub: u32,
    channel: String,
}

/// Enumerates the channels to tune lazily, expanding BS subchannels
/// (SPEC §8.1).
///
/// The expansion is the cross product of the channel range and the requested
/// subchannels, in that order: channel outer, subchannel inner. `ScanQuery::
/// from_params` already rejected a range above [`MAX_SCAN_CHANNELS`], so this
/// iterator is bounded; it is still lazy so the first progress line is emitted
/// before any list is built.
fn candidates(query: &ScanQuery) -> impl Iterator<Item = Candidate> + '_ {
    let sub_channels: &[u32] = if query.expands_subchannels() {
        &query.sub_channels
    } else {
        &[0]
    };
    (query.min_channel..=query.max_channel).flat_map(move |number| {
        sub_channels.iter().copied().map(move |sub| Candidate {
            number,
            sub,
            channel: query.channel_name(number, sub),
        })
    })
}

/// Why a candidate is not tuned by this scan, if it is skipped.
///
/// The parallel pre-pass and the printing loop must agree exactly: a
/// disagreement would make the loop read an outcome index that was never
/// spawned. One helper keeps them in step.
fn skip_reason(
    query: &ScanQuery,
    existing: &[ChannelEntry],
    candidate: &Candidate,
) -> Option<&'static str> {
    if query.skip_channels.contains(&candidate.number) {
        return Some("skipped (skipCh)\n");
    }
    // SPEC §8.1: an enabled entry is left alone unless `refresh=true`; a
    // disabled one is a re-scan target.
    if !query.refresh
        && existing.iter().any(|entry| {
            entry.channel_type == query.type_name
                && entry.channel == candidate.channel
                && !entry.disabled
        })
    {
        return Some("skipped (already configured)\n");
    }
    None
}

/// Channels tuned in the background for a parallel scan.
///
/// Results are delivered per index through a oneshot, so the printing loop can
/// stay in candidate order while the tuners work concurrently, and a task that
/// ends without reporting (a panic) is attributable to its index — a
/// `JoinSet::join_next` error carries no index. `skipped` mirrors
/// [`skip_reason`] so the loop never waits for a channel that was not spawned.
struct ParallelScan {
    tasks: tokio::task::JoinSet<()>,
    /// Per candidate index: the channel outcome and whether the channel was
    /// cut short by shutdown.
    results: Vec<Option<tokio::sync::oneshot::Receiver<(ChannelOutcome, bool)>>>,
    skipped: Vec<bool>,
}

impl ParallelScan {
    fn abort(&mut self) {
        self.tasks.abort_all();
    }

    /// Waits for the outcome of `index` (in candidate order): the channel
    /// outcome plus whether shutdown cut that channel short. `None` only when
    /// the channel was skipped.
    async fn outcome(&mut self, index: usize) -> Option<(ChannelOutcome, bool)> {
        if self.skipped.get(index).copied().unwrap_or(true) {
            return None;
        }
        let Some(rx) = self.results[index].take() else {
            // Already consumed: cannot happen, but reporting a failure is
            // safer than panicking inside a scan.
            return Some((ChannelOutcome::Failed, false));
        };
        match rx.await {
            Ok(report) => Some(report),
            // The sender was dropped: the task panicked (or was aborted).
            Err(_) => {
                tracing::warn!(index, "scan task ended without an outcome");
                Some((ChannelOutcome::Failed, false))
            }
        }
    }

    /// Lets every remaining task finish, keeping their results in the oneshots,
    /// so a channel can be retried without another task holding the tuner.
    async fn wait_all(&mut self) {
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Why a channel produced no entries.
#[derive(Debug)]
pub(crate) enum ChannelOutcome {
    /// Every tuner was busy: SPEC §8 skips the channel and retries it later
    /// with the busy backoff.
    Busy,
    /// No configured tuner supports the channel type. This is a configuration
    /// problem, not a busy tuner: it is never retried (retrying would spin
    /// forever), and the operator is told which type lacks a tuner.
    NoTuner,
    /// The tuner command could not be started (spawn failed, or the process
    /// exited before its first data). The channel was never read, so this is
    /// **not** "no signal"; it carries a short diagnostic and is never retried
    /// (a broken command would only spin).
    TunerFailed(String),
    /// A user request took the tuner over during the scan (SPEC §6.1: any
    /// non-negative priority beats [`SCAN_PRIORITY`]). This is **not** "no
    /// signal": the user won, and the channel must be retried with the same
    /// backoff as a busy tuner so its services still appear.
    Preempted,
    /// A session was actually acquired and the read loop saw no usable SI
    /// before the deadline.
    NoSignal,
    /// Worse than "no signal": the scan task ended without reporting (a panic
    /// or an aborted task). Never retried automatically.
    Failed,
    /// SDT services seen on the channel.
    Services(Vec<FoundService>),
}

/// The progress line for an outcome that produced no services.
///
/// Kept in one place so the discovery scan (`run_scan`) and the manual re-scan
/// (`rescan_services`) always say the same thing for the same outcome.
fn outcome_line(type_name: &str, outcome: &ChannelOutcome) -> Option<String> {
    let line = match outcome {
        ChannelOutcome::NoTuner => format!("skipped (no tuner for type {type_name})\n"),
        ChannelOutcome::Busy => "skipped (tuner busy)\n".to_string(),
        ChannelOutcome::TunerFailed(reason) => {
            format!("tuner failed to start: {}\n", short_reason(reason))
        }
        ChannelOutcome::Preempted => "skipped (preempted by a user request)\n".to_string(),
        ChannelOutcome::NoSignal => "no signal\n".to_string(),
        ChannelOutcome::Failed => "error (scan task failed)\n".to_string(),
        ChannelOutcome::Services(_) => return None,
    };
    Some(line)
}

/// Collapses a start-failure diagnostic to one line of at most ~200 chars.
///
/// A tuner command can print arbitrary stderr; the progress stream must stay
/// readable and must never carry a huge multi-line dump.
fn short_reason(reason: &str) -> String {
    const MAX_CHARS: usize = 200;
    let one_line = reason.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.chars().count() <= MAX_CHARS {
        return one_line;
    }
    let mut truncated: String = one_line.chars().take(MAX_CHARS - 3).collect();
    truncated.push_str("...");
    truncated
}

/// [`scan_channel`] with a shutdown watch.
///
/// Cancellation is observed **between reads**, so the normal "release the scan
/// session" path always runs. The session layer is cancellation-safe as well
/// (see [`crate::session`]), so an acquire that is cut off by the per-channel
/// budget cleans up after itself instead of leaking the tuner. `None` means
/// shutdown fired while the channel was being read; the caller stops without
/// writing `channels.yml`.
async fn scan_channel_cancellable(
    manager: &SessionManager,
    type_name: &str,
    channel: &str,
    timeout: Duration,
    shutdown: Option<watch::Receiver<bool>>,
) -> Option<ChannelOutcome> {
    let Some(mut shutdown) = shutdown else {
        return Some(scan_channel(manager, type_name, channel, timeout).await);
    };
    if stopped(&shutdown) {
        return None;
    }
    let (outcome, cancelled) =
        scan_channel_watched(manager, type_name, channel, timeout, Some(&mut shutdown)).await;
    if cancelled || stopped(&shutdown) {
        None
    } else {
        Some(outcome)
    }
}

/// Whether the shutdown watch has fired (or its sender was dropped).
fn stopped(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

/// Resolves as soon as the shutdown watch is set to `true` (or its sender is
/// dropped).
///
/// Used inside a `select!` instead of a bare `changed()` so an already-fired
/// watch is noticed too: `changed()` only resolves on a *new* value, and a scan
/// that starts just after the server began shutting down must not keep tuning.
async fn shutdown_fired(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if stopped(shutdown) {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// Tunes one channel, collects SI for at most `timeout`, and releases the scan
/// session before returning (SPEC §8).
///
/// Shared by the SPEC §8 startup scan and the SPEC §8.1 scan API so both parse
/// SI the same way. The session is acquired through
/// [`SessionManager::acquire_scan_detailed`], which may retire an idle session
/// even at [`SCAN_PRIORITY`] and keeps the start-failure diagnostic; every path
/// releases that session again before returning,
/// so the next channel always finds the tuner.
pub(crate) async fn scan_channel(
    manager: &SessionManager,
    type_name: &str,
    channel: &str,
    timeout: Duration,
) -> ChannelOutcome {
    scan_channel_watched(manager, type_name, channel, timeout, None)
        .await
        .0
}

/// Waits (bounded) until no session for the effective channel(s) of
/// `(type_name, channel)` is still starting or stopping.
///
/// A cancelled acquire stops its reservation in a detached task, which is
/// asynchronous: the tuner stays `Starting`/`Stopping` until the process group
/// is gone. Returning before that would make the *next* channel of the same scan
/// report "busy" because of the channel it just gave up on. A `Ready` session is
/// someone else's (a user joined first), so it is left alone.
async fn wait_for_release(manager: &SessionManager, type_name: &str, channel: &str) {
    // A routed channel's session is keyed by the route's effective channel, not
    // by the configured `channel` (SPEC §6.4): wait for any of them.
    let effective = manager.effective_channels(type_name, channel);
    let deadline = Instant::now() + RELEASE_WAIT;
    loop {
        let occupied = manager.sessions_snapshot().iter().any(|session| {
            session.type_name == type_name
                && effective.iter().any(|name| name == &session.channel)
                && matches!(
                    session.phase,
                    crate::session::SessionPhase::Starting | crate::session::SessionPhase::Stopping
                )
        });
        if !occupied {
            return;
        }
        if Instant::now() >= deadline {
            tracing::warn!(channel, "the cancelled scan session is still stopping");
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Upper bound for [`wait_for_release`]: long enough for one SIGTERM grace
/// period plus the reap, short enough not to wedge a scan.
const RELEASE_WAIT: Duration = Duration::from_secs(5);

/// [`scan_channel`] with an optional shutdown watch, returning whether the read
/// loop was cancelled.
async fn scan_channel_watched(
    manager: &SessionManager,
    type_name: &str,
    channel: &str,
    timeout: Duration,
    mut shutdown: Option<&mut watch::Receiver<bool>>,
) -> (ChannelOutcome, bool) {
    // SPEC §8: scan is a plain subscriber on the shared broadcast. Joining a
    // session a user already holds is therefore correct, and the release path
    // refuses to stop it (`release_session` checks for other users); it is
    // asked for unconditionally, because a snapshot of "sessions that existed
    // before" is racy while several scan tasks run in parallel. A routed
    // channel's session is keyed by the route's effective channel, not by the
    // configured one (SPEC §6.4), and the release uses that too.
    let user = TunerUser::new("scan", SCAN_PRIORITY);
    // One budget per channel covers tuning *and* reading (SPEC §8). The
    // acquire gets at most its own readiness gate plus the concurrent-wait
    // margin, so a slow spawn cannot swallow the whole budget and leave nothing
    // for SI; the read then uses what is left of the channel deadline.
    let started = Instant::now();
    let deadline = started + timeout;
    let acquire_budget = crate::session::READINESS_TIMEOUT + Duration::from_secs(5);
    let acquire_deadline = deadline.min(started + acquire_budget);
    let acquire = tokio::time::timeout_at(
        acquire_deadline,
        manager.acquire_scan_detailed(type_name, channel, user, SessionTap::Raw, false),
    );
    // Shutdown must be honoured within about a second even while the acquire is
    // still waiting for readiness (which can take up to `acquire_budget`). The
    // session layer is cancellation-safe, so dropping the acquire stops the
    // reservation it already took instead of leaking the tuner.
    let acquired = match shutdown.as_deref_mut() {
        Some(rx) => tokio::select! {
            acquired = acquire => Some(acquired),
            _ = shutdown_fired(rx) => None,
        },
        None => Some(acquire.await),
    };
    let Some(acquired) = acquired else {
        tracing::debug!(channel, "scan acquire cancelled by shutdown");
        return (ChannelOutcome::NoSignal, true);
    };

    let mut subscription = match acquired {
        Err(_) => {
            tracing::debug!(channel, "scan acquire timed out");
            // The tuner process was started (otherwise this would have been
            // `Unavailable`) but produced no first data before the scan's own
            // budget; a live-but-silent tuner is genuine no signal, not a start
            // failure. The session layer is cancellation-safe and stops the
            // reservation it had already taken, but that takes a moment
            // (SIGTERM grace, then SIGKILL). Wait for it to be gone, otherwise
            // the tuner still holds `Stopping`/`Starting` and the next channel
            // of the scan would be refused with "busy" because of this one.
            wait_for_release(manager, type_name, channel).await;
            return (ChannelOutcome::NoSignal, false);
        }
        Ok(Err(failure)) => {
            tracing::debug!(channel, ?failure, "scan could not acquire a tuner");
            let outcome = match failure.error {
                // SPEC §8: "相手がbusyならskip＋次回retry". Only a genuinely busy
                // tuner is retried.
                crate::session::AcquireError::Busy
                | crate::session::AcquireError::DecodeModeConflict => ChannelOutcome::Busy,
                // A missing tuner is a configuration problem: retrying it
                // forever would only spin, and calling it "busy" hides the real
                // cause from the operator.
                crate::session::AcquireError::NoTuner => ChannelOutcome::NoTuner,
                // The command failed to start (spawn error, or it exited before
                // its first data). The channel was never read, so this must not
                // be reported as "no signal".
                crate::session::AcquireError::Unavailable => ChannelOutcome::TunerFailed(
                    failure
                        .reason
                        .unwrap_or_else(|| "command failed".to_string()),
                ),
                _ => ChannelOutcome::Failed,
            };
            return (outcome, false);
        }
        Ok(Ok(subscription)) => subscription,
    };

    let mut parser = SiParser::new();
    let mut sink = ChannelSink::default();
    let mut cancelled = false;
    // Remembered before the subscription is dropped: it tells the release path
    // which subscriber was ours, so a client that attached meanwhile keeps the
    // session alive while a stale scan subscriber does not wedge it. The
    // effective channel is the session identity the release must use (SPEC
    // §6.4).
    let subscriber_id = subscription.subscriber_id;
    let subscription_session_id = subscription.session_id;
    let effective_channel = subscription.channel.clone();
    let preempted = subscription.preempted.clone();
    // Once the SDT-actual is complete, only the remaining core tables are
    // worth waiting for, and only for a short grace: the read deadline shrinks
    // to `now + SI_GRACE_AFTER_SDT` so a channel finishes promptly instead of
    // holding its tuner for the rest of the budget, while a PAT/PMT/NIT that
    // is still in flight (repeating well within the grace) is not missed.
    let mut read_deadline = deadline;
    let mut sdt_complete = false;
    // Diagnostics: when each core table first arrives relative to the start of
    // the read loop. Debug-level only; it makes the SI grace measurable on a
    // real tuner without guessing.
    let trace_timing = tracing::enabled!(tracing::Level::DEBUG);
    let read_started = Instant::now();
    let mut first_pat = false;
    let mut first_pmt = false;
    let mut first_nit = false;
    loop {
        let chunk = match shutdown.as_deref_mut() {
            Some(rx) => {
                let watched = tokio::select! {
                    chunk = tokio::time::timeout_at(read_deadline, subscription.rx.recv()) => Some(chunk),
                    _ = shutdown_fired(rx) => None,
                };
                match watched {
                    Some(chunk) => chunk,
                    None => {
                        cancelled = true;
                        break;
                    }
                }
            }
            None => tokio::time::timeout_at(read_deadline, subscription.rx.recv()).await,
        };
        match chunk {
            Ok(Some(chunk)) => {
                for event in parser.push_packets(&chunk) {
                    sink.push(&event, parser.network_id());
                    let elapsed = if trace_timing {
                        read_started.elapsed()
                    } else {
                        Duration::ZERO
                    };
                    if sink.seen_pat && !first_pat {
                        first_pat = true;
                        tracing::debug!(channel, ms = elapsed.as_millis() as u64, "scan: PAT");
                    }
                    if sink.seen_pmt && !first_pmt {
                        first_pmt = true;
                        tracing::debug!(channel, ms = elapsed.as_millis() as u64, "scan: PMT");
                    }
                    if sink.seen_nit_actual && !first_nit {
                        first_nit = true;
                        tracing::debug!(channel, ms = elapsed.as_millis() as u64, "scan: NIT");
                    }
                    if sink.is_complete() && !sdt_complete {
                        sdt_complete = true;
                        read_deadline = read_deadline.min(Instant::now() + SI_GRACE_AFTER_SDT);
                        tracing::debug!(
                            channel,
                            ms = read_started.elapsed().as_millis() as u64,
                            "scan: SDT complete"
                        );
                    }
                    // All core tables arrived (PAT + PMT + complete SDT-actual
                    // + NIT-actual for the network id / remote key): there is
                    // no reason to keep the tuner for the rest of the budget.
                    if sink.has_all_core_tables() {
                        sink.finish();
                        drop(subscription);
                        // Always ask for a release: `release_session` itself
                        // refuses when another user is watching, and the
                        // `preexisting` snapshot is racy between parallel scan
                        // tasks (a peer may have spawned this session after
                        // `before` was taken, which used to leak it until the
                        // idle timeout).
                        manager
                            .release_session(
                                type_name,
                                &effective_channel,
                                subscription_session_id,
                                subscriber_id,
                            )
                            .await;
                        return (ChannelOutcome::Services(sink.found), cancelled);
                    }
                }
            }
            // The stream ended (a finite fake tuner, or a dead pipeline), or
            // the per-channel budget — or, after a complete SDT, the short
            // grace for the remaining core tables — ran out.
            Ok(None) | Err(_) => break,
        }
    }

    tracing::debug!(
        channel,
        stats = ?parser.stats(),
        found = sink.found.len(),
        "scan channel finished without completing a service list"
    );
    // Release the scan session before the next channel is tuned (SPEC §8.1).
    drop(subscription);
    // Always ask for a release: `release_session` itself refuses when another
    // user is watching (see the note at the early-return path above).
    manager
        .release_session(
            type_name,
            &effective_channel,
            subscription_session_id,
            subscriber_id,
        )
        .await;

    // Fold in the partially received SDT first: a channel preempted after a
    // partial table still produced services, which `finish` materialises —
    // checking `preempted` against the not-yet-finished `found` would report
    // those as "preempted" and drop them.
    sink.finish();

    // A user request preempted the scan (its priority beats the scan's
    // `-1000`). That is not "no signal": the channel must be retried with the
    // same backoff as a busy tuner, and the user keeps the tuner. A *partial*
    // SDT is not enough to report services either — `Preempted` is retried,
    // whereas `Services` is final, so returning the partial list here would
    // freeze an incomplete service table in `channels.yml`.
    if preempted.is_preempted() && (!sink.complete || sink.found.is_empty()) {
        return (ChannelOutcome::Preempted, cancelled);
    }

    let outcome = if sink.found.is_empty() {
        ChannelOutcome::NoSignal
    } else {
        ChannelOutcome::Services(sink.found)
    };
    (outcome, cancelled)
}

/// Builds the entries generated for one channel (SPEC §8.1 `scanMode`).
fn entries_for(
    query: &ScanQuery,
    existing: &[ChannelEntry],
    candidate: &Candidate,
    services: &[FoundService],
) -> Vec<ChannelEntry> {
    let channel = candidate.channel.as_str();
    let disabled_of = |channel: &str| {
        existing
            .iter()
            .find(|entry| entry.channel_type == query.type_name && entry.channel == channel)
            .map(|entry| entry.disabled)
            .unwrap_or(query.set_disabled_on_add)
    };

    match query.mode {
        ScanMode::Channel => vec![ChannelEntry {
            name: services
                .first()
                .map(|service| service.name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| channel.to_string()),
            channel_type: query.type_name.clone(),
            channel: channel.to_string(),
            disabled: disabled_of(channel),
            ..Default::default()
        }],
        ScanMode::Service => services
            .iter()
            .enumerate()
            .map(|(index, service)| {
                // In `Service` mode the subchannel is the service's position on
                // the multiplex, so every service of one multiplex gets its own
                // (type, channel) pair even without `{sid}` in the format.
                let sub = if candidate.sub == 0 {
                    index as u32
                } else {
                    candidate.sub
                };
                let channel = match &query.channel_name_format {
                    // `{sid}` is a scan extension; `{ch}`/`{ch00}`/`{subch}`
                    // are resolved here too because the candidate name already
                    // formatted them.
                    Some(format) => format
                        .replace("{ch}", &candidate.number.to_string())
                        .replace("{ch00}", &format!("{:02}", candidate.number))
                        .replace("{subch}", &sub.to_string())
                        .replace("{sid}", &service.service_id.to_string()),
                    // Defaults: mirakurun's `BS{ch00}_{subch}` for the literal
                    // BS type, `<ch>` for CS-like types' first service and
                    // `<ch>_<n>` for the rest.
                    None => {
                        let name = query.channel_name(candidate.number, sub);
                        if query.type_name == "BS" || sub == 0 {
                            name
                        } else {
                            format!("{name}_{sub}")
                        }
                    }
                };
                ChannelEntry {
                    // An empty SDT name is kept empty here: `merge` falls back
                    // to the channel-derived name only in that case, so a real
                    // service name is never discarded.
                    name: service.name.clone(),
                    channel_type: query.type_name.clone(),
                    disabled: disabled_of(&channel),
                    channel,
                    ..Default::default()
                }
            })
            .collect(),
    }
}

/// Merges the freshly found entries into the existing list (SPEC §8.1).
///
/// * entries of other types are untouched;
/// * with `refresh=true`, entries of the scanned type whose channel was
///   produced by the scan are dropped and replaced, and so are entries of the
///   scanned multiplex whose service disappeared (`families`);
/// * without `refresh`, an existing `(type, channel)` always wins;
/// * names stay unique (the startup validation requires it). A collision keeps
///   the original service name and appends ` (2)`, ` (3)`, ...; only an empty
///   service name falls back to the channel-derived name.
fn merge(
    existing: &[ChannelEntry],
    query: &ScanQuery,
    rescanned: &HashSet<String>,
    families: &HashSet<String>,
    found: Vec<ChannelEntry>,
) -> Vec<ChannelEntry> {
    let mut merged: Vec<ChannelEntry> = existing
        .iter()
        .filter(|entry| {
            !(query.refresh
                && entry.channel_type == query.type_name
                && (rescanned.contains(entry.channel.as_str())
                    || families
                        .iter()
                        .any(|family| in_family(family, &entry.channel))))
        })
        .cloned()
        .collect();

    let mut names: HashSet<String> = merged.iter().map(|entry| entry.name.clone()).collect();
    for mut entry in found {
        let occupied = merged.iter().any(|existing| {
            existing.channel_type == entry.channel_type && existing.channel == entry.channel
        });
        if occupied {
            continue;
        }
        // The scan discovers services, not routes. When a refresh replaces an
        // entry, keep the hand-written `routes` / `extra-args` of the channel it
        // replaces instead of inventing (empty) ones.
        if entry.extra_args.is_empty() && entry.routes.is_empty() {
            if let Some(previous) = existing.iter().find(|previous| {
                previous.channel_type == entry.channel_type && previous.channel == entry.channel
            }) {
                entry.extra_args = previous.extra_args.clone();
                entry.routes = previous.routes.clone();
            }
        }
        if entry.name.is_empty() {
            entry.name = entry.channel.clone();
        }
        if !names.insert(entry.name.clone()) {
            // Two services can share a name (a relay, or a duplicated SDT).
            // The service name is real information, so keep it and only add a
            // suffix instead of replacing it with the channel.
            let base = entry.name.clone();
            let mut suffix = 2;
            loop {
                let candidate = format!("{base} ({suffix})");
                if names.insert(candidate.clone()) {
                    entry.name = candidate;
                    break;
                }
                suffix += 1;
            }
        }
        merged.push(entry);
    }
    merged
}

/// The multiplex a candidate belongs to: its channel name with a trailing
/// `_<digits>` subchannel suffix removed.
///
/// `Service` mode names every service of one multiplex after the same base
/// (`BS01_0`, `BS01_1`, ... / `CS02_0`, `CS02_1`, ...), so a refresh can drop
/// the entries of the whole multiplex — including services that disappeared —
/// instead of leaving stale siblings behind.
fn family_of(channel: &str) -> &str {
    match channel.rsplit_once('_') {
        Some((base, sub)) if !sub.is_empty() && sub.bytes().all(|byte| byte.is_ascii_digit()) => {
            base
        }
        _ => channel,
    }
}

/// Whether `channel` belongs to the multiplex named by `family`.
fn in_family(family: &str, channel: &str) -> bool {
    channel == family
        || channel
            .strip_prefix(family)
            .is_some_and(|rest| rest.starts_with('_'))
}

/// The services and entries one scanned channel contributed, published to the
/// shared [`ServiceTable`] only after the `channels.yml` write succeeded.
#[derive(Debug)]
struct TableUpdate {
    type_name: String,
    mode: ScanMode,
    entries: Vec<ChannelEntry>,
    services: Vec<FoundService>,
}

/// Writes the found services into the shared table (SPEC §13: a scan is
/// reflected by `/api/services`), then drops the services the channel no longer
/// carries.
///
/// `retain_channel` runs after the upsert loop, so there is no window in which
/// a service that is still on air answers 404: the fresh records are already in
/// place when the stale ones go. It is only called for a channel that produced
/// at least one service, so a no-signal or busy channel keeps whatever the
/// previous pass stored (a failed scan must never wipe the table).
fn update_service_table(services: &ServiceTable, update: &TableUpdate) {
    let TableUpdate {
        type_name,
        mode,
        entries,
        services: found,
    } = update;
    for (index, service) in found.iter().enumerate() {
        // In `Service` mode `entries_for` emits one entry per service, in the
        // same order; in `Channel` mode every service shares the one entry.
        let channel = match mode {
            ScanMode::Service => entries.get(index),
            ScanMode::Channel => entries.first(),
        }
        .map(|entry| entry.channel.clone());
        let mut record = ServiceRecord::new(
            service.network_id,
            service.tsid,
            service.service_id,
            service.service_type,
            service.name.clone(),
        );
        record.remote_control_key_id = service.remote_control_key_id;
        record.pmt_pid = service.pmt_pid;
        if let Some(channel) = channel {
            record.channel = Some((type_name.clone(), channel));
        }
        services.upsert(record);
    }

    if found.is_empty() {
        // Nothing was read on this channel: never prune on failure.
        return;
    }
    // Group the sids by the `channels.yml` channel they were attributed to, so
    // a format that maps several services onto one channel string (for example
    // `{ch}` in `Service` mode) does not let one entry prune another's record.
    let mut keep_by_channel: HashMap<&str, HashSet<u16>> = HashMap::new();
    for (index, service) in found.iter().enumerate() {
        let entry = match mode {
            ScanMode::Service => entries.get(index),
            ScanMode::Channel => entries.first(),
        };
        if let Some(entry) = entry {
            keep_by_channel
                .entry(entry.channel.as_str())
                .or_default()
                .insert(service.service_id);
        }
    }
    for (channel, keep) in keep_by_channel {
        services.retain_channel(type_name, channel, &keep);
    }
}

/// Reads `channels.yml`, treating a missing file as an empty list.
fn read_channels(config_dir: &Path) -> Result<Vec<ChannelEntry>, String> {
    let path = config_dir.join("channels.yml");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    };
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_yaml_ng::from_str(&text)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))
}

// ---------------------------------------------------------------------------
// Startup service scan (SPEC §8)
// ---------------------------------------------------------------------------

/// One `channels.yml` entry the startup scan tunes.
#[derive(Clone, Debug)]
struct StartupChannel {
    type_name: String,
    channel: String,
}

impl StartupChannel {
    fn of(entry: &crate::config::ChannelConfig) -> Self {
        Self {
            type_name: entry.channel_type.clone(),
            channel: entry.channel.clone(),
        }
    }
}

/// The enabled `channels.yml` entries, in configuration order.
fn startup_channels(config: &Config) -> Vec<StartupChannel> {
    config
        .channels
        .iter()
        .filter(|entry| !entry.disabled)
        .map(StartupChannel::of)
        .collect()
}

/// Number of enabled tuners that support `type_name`.
///
/// This is the scan concurrency for that type: with one tuner (e.g. a single
/// PX-S1UD) channels are necessarily scanned sequentially and the semaphore
/// of one permit preserves that; with two or more tuners up to that many
/// channels are scanned at once. A routed channel may in fact be pinned to a
/// single tuner, so the count can overestimate for it — the extra contender
/// then just reports `Busy` and is retried on the next pass, as before.
fn tuner_count_for(config: &Config, type_name: &str) -> usize {
    config
        .tuners
        .iter()
        .filter(|tuner| {
            !tuner.disabled && tuner.types.iter().any(|supported| supported == type_name)
        })
        .count()
}

/// How long the startup scan waits before its next retry pass.
///
/// `pass` is the number of completed passes after the first one: 0 → 30 s,
/// 1 → 60 s, then 120 s, 240 s and finally [`STARTUP_RETRY_MAX`] forever.
fn retry_delay(pass: usize) -> Duration {
    let factor = 1u32 << pass.min(4);
    STARTUP_RETRY_INITIAL
        .saturating_mul(factor)
        .min(STARTUP_RETRY_MAX)
}

/// Summary of one startup-scan pass, logged when it ends (SPEC §8).
#[derive(Debug, Default, PartialEq, Eq)]
struct PassSummary {
    /// Channels the pass attempted.
    attempted: usize,
    /// Channels that produced a service list.
    ok: usize,
    /// Channels skipped because no tuner was usable.
    skipped: usize,
    /// Services upserted into the table.
    found: usize,
    /// Channels whose tuner never started (no tuner, spawn failure, or a scan
    /// task panic). These are not retried, but they must not make an empty
    /// cache look authoritative.
    hard_failures: usize,
}

/// Scans every enabled `channels.yml` entry once and retries the skipped ones
/// with exponential backoff until `shutdown` fires (SPEC §8, §8.2).
///
/// * A valid `services.json` (matching schema version + fingerprint) makes the
///   whole scan a no-op: the cached services are restored and no tuner is
///   touched. There is no expiry; only a fingerprint change or a manual
///   re-scan replaces it.
/// * Each channel runs through [`scan_channel`] at [`SCAN_PRIORITY`], so a user
///   request always wins and a busy tuner is skipped rather than preempted.
/// * The per-channel budget is [`CHANNEL_TIMEOUT`], covering acquire + SI read.
/// * Channels that failed for lack of a tuner ([`ChannelOutcome::Busy`], i.e.
///   busy or no supporting tuner) or because a user request preempted the scan
///   ([`ChannelOutcome::Preempted`]) are retried; a no-signal channel is not.
/// * Entries are only ever upserted, never cleared, so a failed channel keeps
///   whatever the previous pass (or an HTTP scan) put in the table.
/// * The cache is written after the first pass that either found a service or
///   had nothing left to retry. Later retry passes rewrite it only when the
///   table actually changed. A pass interrupted by shutdown or a dropped
///   progress receiver never writes. Once written, the cache is authoritative
///   until the fingerprint changes or the WebUI Re-scan runs (busy channels are
///   not retried across a restart).
///
/// The task owns no processes: [`scan_channel`] releases each session before
/// returning, and the shutdown signal is observed before every channel.
pub async fn startup_scan(
    manager: SessionManager,
    config: Arc<Config>,
    config_dir: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) {
    let fingerprint = service_fingerprint(&config.channels);
    let table = manager.services();

    match read_service_cache(&config_dir) {
        Ok(Some(cache))
            if cache.version == SERVICE_CACHE_VERSION && cache.fingerprint == fingerprint =>
        {
            // Restoring here keeps the function self-contained: a caller that
            // did not preload the cache (a test, or a future entry point) still
            // serves the cached services.
            table.restore(cache.services);
            tracing::info!("startup scan skipped: service cache is valid");
            return;
        }
        Ok(Some(cache)) if cache.version != SERVICE_CACHE_VERSION => {
            tracing::warn!(
                version = cache.version,
                expected = SERVICE_CACHE_VERSION,
                "startup scan: unsupported services.json version, scanning"
            );
        }
        Ok(Some(_)) => {
            tracing::info!("startup scan: services.json fingerprint changed, scanning");
        }
        Ok(None) => tracing::info!("startup scan: no service cache, scanning"),
        Err(error) => {
            tracing::warn!(%error, "startup scan: cannot read services.json, scanning")
        }
    }

    let mut pending = startup_channels(&config);
    if pending.is_empty() {
        tracing::info!("startup scan: no enabled channels");
        // An empty cache is a valid answer: remembering that a config has no
        // scannable channel avoids probing again on every boot.
        retain_enabled_channels(table, &config);
        if let Err(error) = write_service_cache(&config_dir, &fingerprint, table) {
            tracing::warn!(%error, "failed to write services.json");
        }
        return;
    }
    tracing::info!(
        channels = pending.len(),
        "startup scan started (priority {SCAN_PRIORITY})"
    );

    let mut pass = 0usize;
    let mut cache_written = false;
    loop {
        if *shutdown.borrow() {
            tracing::info!(pass, "startup scan stopped");
            return;
        }
        let before = table.snapshot();
        let (summary, retry) = scan_pass(&manager, &config, &pending, &mut shutdown).await;
        tracing::info!(
            pass,
            attempted = summary.attempted,
            ok = summary.ok,
            skipped = summary.skipped,
            found = summary.found,
            "startup scan pass finished"
        );

        // A pass interrupted by shutdown must not sleep (nor write the cache):
        // return right away so the task never outlives the server.
        if *shutdown.borrow() {
            tracing::info!(pass, "startup scan stopped");
            return;
        }

        // First write: only when the pass really produced something, or when
        // there is nothing left to retry (an all-no-signal config legitimately
        // has an empty service list). A first pass that was entirely busy keeps
        // retrying instead of persisting an empty cache that a later boot would
        // accept as valid.
        let should_write = if cache_written {
            summary.found > 0 && before != table.snapshot()
        } else {
            // "Nothing left to retry" is only a legitimate empty answer when no
            // tuner failed to start: a config whose tuner is missing must be
            // re-probed on the next boot, not remembered as empty forever.
            summary.found > 0 || (retry.is_empty() && summary.hard_failures == 0)
        };
        if should_write {
            // Drop records of channels that are no longer enabled. Busy or
            // no-signal channels are still enabled, so their records survive.
            retain_enabled_channels(table, &config);
            match write_service_cache(&config_dir, &fingerprint, table) {
                Ok(()) => {
                    cache_written = true;
                    tracing::info!(
                        services = table.len(),
                        "service cache written to {}",
                        config_dir.join(SERVICE_CACHE_FILE).display()
                    );
                }
                Err(error) => {
                    // A warning only: the table stays valid in memory and the
                    // previous file is untouched.
                    tracing::warn!(%error, "failed to write services.json");
                }
            }
        }

        if retry.is_empty() {
            tracing::info!("startup scan complete");
            return;
        }

        pending = retry;
        pass += 1;
        let delay = retry_delay(pass - 1);
        tracing::debug!(
            retry = pending.len(),
            delay_secs = delay.as_secs(),
            "startup scan backing off"
        );
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                // A closed watch (the sender was dropped) is a shutdown too.
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!(pass, "startup scan stopped");
                    return;
                }
            }
        }
    }
}

/// Keeps only records of enabled `channels.yml` entries.
fn retain_enabled_channels(table: &ServiceTable, config: &Config) -> usize {
    let keep: HashSet<(String, String)> = config
        .channels
        .iter()
        .filter(|channel| !channel.disabled)
        .map(|channel| (channel.channel_type.clone(), channel.channel.clone()))
        .collect();
    table.retain_bound_channels(&keep)
}

/// Re-scans the enabled `channels.yml` entries exactly once (SPEC §8.2).
///
/// This is the manual WebUI re-scan: it uses the in-memory configuration (no
/// `channels.yml` is read or written), streams one progress line per channel
/// and, after the whole pass, drops records for channels that are no longer
/// enabled. A channel whose tuner is busy or has no signal keeps its previous
/// records. The cache is written only after a completed pass; a client
/// disconnect or shutdown returns before the retain step and leaves
/// `services.json` untouched (the in-memory table may keep the upserts that
/// already happened).
pub async fn rescan_services(
    manager: &SessionManager,
    config: &Config,
    config_dir: &Path,
    tx: mpsc::Sender<String>,
    shutdown: Option<watch::Receiver<bool>>,
) -> Result<(), String> {
    let mut lines = Lines::new(tx);
    let channels = startup_channels(config);
    let total = channels.len().max(1);
    lines
        .push(format!(
            "service re-scan... ({} channels)\n\n",
            channels.len()
        ))
        .await;

    let table = manager.services();
    let mut scanned = 0usize;
    let mut found_total = 0usize;
    // Channels whose tuner never started (no tuner / spawn failure). An
    // all-hard-failure first pass must not persist an empty `services.json`
    // that a later boot would trust as a valid "no services" answer.
    let mut hard_failures = 0usize;
    // Channel-level refreshed sid sets, applied only after the whole pass
    // completes: a disconnect must not run any retain step (SPEC §8.2).
    let mut refreshed: Vec<(String, String, HashSet<u16>)> = Vec::new();
    for (index, channel) in channels.iter().enumerate() {
        if lines.is_closed() || cancelled(&shutdown) {
            tracing::info!("service re-scan cancelled");
            return Ok(());
        }
        let percent = (index as u64 + 1) * 100 / total as u64;
        lines
            .push(format!(
                "channel: {:?} ({}/{}) [{}%] ... ",
                channel.channel,
                index + 1,
                total,
                percent
            ))
            .await;

        let outcome = scan_channel_cancellable(
            manager,
            &channel.type_name,
            &channel.channel,
            CHANNEL_TIMEOUT,
            shutdown.clone(),
        )
        .await;
        let Some(outcome) = outcome else {
            tracing::info!("service re-scan cancelled");
            return Ok(());
        };

        match outcome {
            ChannelOutcome::Services(services) => {
                let services: Vec<FoundService> = services
                    .into_iter()
                    .filter(|service| SCAN_SERVICE_TYPES.contains(&service.service_type))
                    .collect();
                lines
                    .push(format!("-> {} services found.\n", services.len()))
                    .await;
                if services.is_empty() {
                    continue;
                }
                scanned += 1;
                found_total += services.len();
                upsert_startup_services(table, channel, &services);
                // Same refresh rule as the discovery scan: a channel that did
                // produce SI drops the services it no longer carries.
                // Collected here and applied after the pass, so a mid-pass
                // disconnect keeps the previous records untouched.
                let keep: HashSet<u16> =
                    services.iter().map(|service| service.service_id).collect();
                refreshed.push((channel.type_name.clone(), channel.channel.clone(), keep));
            }
            other => {
                if matches!(
                    other,
                    ChannelOutcome::NoTuner
                        | ChannelOutcome::TunerFailed(_)
                        | ChannelOutcome::Failed
                ) {
                    hard_failures += 1;
                }
                if let Some(line) = outcome_line(&channel.type_name, &other) {
                    lines.push(line).await;
                }
            }
        }
    }

    // A disconnected client or a shutdown must not touch the cache (and must
    // not apply the channel-level retain step).
    if lines.is_closed() || cancelled(&shutdown) {
        tracing::info!("service re-scan cancelled before saving");
        return Ok(());
    }

    let mut removed = 0usize;
    for (channel_type, channel, keep) in refreshed {
        removed += table.retain_channel(&channel_type, &channel, &keep);
    }
    removed += retain_enabled_channels(table, config);
    let fingerprint = service_fingerprint(&config.channels);
    // A first scan in which every channel's tuner failed to start has nothing
    // to remember: writing an empty cache would mark the config as "scanned"
    // and suppress the next boot's retry. Existing records are never dropped by
    // this guard (the retain above already ran, and a non-empty table is
    // written normally).
    let cache_note = if scanned == 0 && table.is_empty() && hard_failures > 0 {
        format!(
            "did not write {} (no tuner could be started)",
            config_dir.join(SERVICE_CACHE_FILE).display()
        )
    } else {
        match write_service_cache(config_dir, &fingerprint, table) {
            Ok(()) => format!("wrote {}", config_dir.join(SERVICE_CACHE_FILE).display()),
            Err(error) => {
                tracing::warn!(%error, "failed to write services.json");
                format!("warning: failed to write services.json: {error}")
            }
        }
    };
    lines
        .push(format!(
            "\n**RE-SCAN COMPLETE**: {} services ({} channels scanned, {found_total} found, \
             {removed} stale records dropped; {cache_note}).\n",
            table.len(),
            scanned
        ))
        .await;
    Ok(())
}

/// Whether a startup-scan pass retries a channel after `outcome`.
///
/// Only `Busy` (the tuner is in use) and `Preempted` (a user took it over) are
/// transient: the next pass retries them with the same exponential backoff.
/// A channel that legitimately produced no SI (`NoSignal`) is not retried, and
/// neither are the configuration failures `NoTuner` and `TunerFailed`: retrying
/// them would spin forever without the tuner changing state.
fn retries(outcome: &ChannelOutcome) -> bool {
    matches!(outcome, ChannelOutcome::Busy | ChannelOutcome::Preempted)
}

/// Scans `pending`, returning the pass summary and the channels that should
/// be retried (only those whose tuner was unavailable).
///
/// Channels are scanned **in parallel**, up to the number of enabled tuners
/// supporting each channel's type (a per-type semaphore): with one tuner the
/// pass is sequential exactly as before, with more tuners the pass takes
/// roughly `channels / tuners` channel budgets. The first pass still attempts
/// every channel once — a busy/preempted channel does not wait or block the
/// others, it is collected into the retry queue for the next backoff pass.
/// Results are folded in configuration order, so the retry queue and the
/// summary are deterministic regardless of completion order.
async fn scan_pass(
    manager: &SessionManager,
    config: &Config,
    pending: &[StartupChannel],
    shutdown: &mut watch::Receiver<bool>,
) -> (PassSummary, Vec<StartupChannel>) {
    // One semaphore per type, sized by that type's tuner count.
    let mut permits: HashMap<String, Arc<tokio::sync::Semaphore>> = HashMap::new();
    for channel in pending {
        permits.entry(channel.type_name.clone()).or_insert_with(|| {
            Arc::new(tokio::sync::Semaphore::new(
                tuner_count_for(config, &channel.type_name).max(1),
            ))
        });
    }

    let mut tasks = tokio::task::JoinSet::new();
    for (index, channel) in pending.iter().enumerate() {
        let manager = manager.clone();
        let channel = channel.clone();
        let semaphore = permits[&channel.type_name].clone();
        let mut shutdown = shutdown.clone();
        tasks.spawn(async move {
            // Wait for this type's tuner slot, honouring shutdown promptly
            // instead of queueing (and then tuning) after it fired.
            let acquired = tokio::select! {
                permit = semaphore.clone().acquire_owned() => permit.ok(),
                _ = shutdown_fired(&mut shutdown) => None,
            };
            let Some(_permit) = acquired else {
                return (index, None);
            };
            // Cancelled between reads (never by dropping an in-flight
            // `acquire`, which could leave a reservation behind): shutdown
            // must not wait for a channel's whole budget, let alone
            // `timeout × channels`.
            let outcome = scan_channel_cancellable(
                &manager,
                &channel.type_name,
                &channel.channel,
                CHANNEL_TIMEOUT,
                Some(shutdown.clone()),
            )
            .await;
            (index, outcome)
        });
    }

    let mut outcomes: Vec<Option<ChannelOutcome>> = (0..pending.len()).map(|_| None).collect();
    while let Some(joined) = tasks.join_next().await {
        if let Ok((index, outcome)) = joined {
            outcomes[index] = outcome;
        }
    }

    let mut summary = PassSummary::default();
    let mut retry = Vec::new();
    for (channel, outcome) in pending.iter().zip(outcomes) {
        let Some(outcome) = outcome else {
            continue;
        };
        summary.attempted += 1;
        let should_retry = retries(&outcome);
        let preempted = matches!(&outcome, ChannelOutcome::Preempted);
        match outcome {
            ChannelOutcome::Busy | ChannelOutcome::Preempted => {
                summary.skipped += 1;
                tracing::debug!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    preempted,
                    "startup scan skipped the channel: no tuner available"
                );
            }
            ChannelOutcome::NoTuner => {
                summary.skipped += 1;
                summary.hard_failures += 1;
                tracing::warn!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    "startup scan skipped the channel: no tuner for this type"
                );
            }
            ChannelOutcome::TunerFailed(reason) => {
                summary.skipped += 1;
                summary.hard_failures += 1;
                tracing::warn!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    reason = %short_reason(&reason),
                    "startup scan skipped the channel: tuner failed to start"
                );
            }
            ChannelOutcome::NoSignal => {
                summary.skipped += 1;
                tracing::debug!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    "startup scan skipped the channel: no signal"
                );
            }
            ChannelOutcome::Failed => {
                summary.skipped += 1;
                summary.hard_failures += 1;
                tracing::warn!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    "startup scan task failed without an outcome"
                );
            }
            ChannelOutcome::Services(services) => {
                summary.ok += 1;
                summary.found += upsert_startup_services(manager.services(), channel, &services);
            }
        }
        if should_retry {
            retry.push(channel.clone());
        }
    }
    (summary, retry)
}

/// Upserts the services of one channel, returning how many records were
/// written.
fn upsert_startup_services(
    table: &ServiceTable,
    channel: &StartupChannel,
    services: &[FoundService],
) -> usize {
    for service in services {
        let mut record = ServiceRecord::new(
            service.network_id,
            service.tsid,
            service.service_id,
            service.service_type,
            service.name.clone(),
        );
        record.remote_control_key_id = service.remote_control_key_id;
        record.pmt_pid = service.pmt_pid;
        record.channel = Some((channel.type_name.clone(), channel.channel.clone()));
        table.upsert(record);
    }
    services.len()
}

/// Validates the merged channel list with the startup rules and writes it
/// atomically (SPEC §3.6, §8.1), returning the merged entries and whether the
/// file was written.
///
/// The write takes the same per-file lock as a webui save and re-reads
/// `channels.yml` **inside** it: a save that landed while the scan was tuning
/// must not be silently overwritten with the copy the scan read at its start.
/// The re-read is merged exactly like the initial one, so the scan's own result
/// still replaces what it produced and everything else (including the webui's
/// change) survives.
///
/// When the merged list is *semantically* identical to the file's current
/// content, nothing is written: re-serialising would only change YAML quoting
/// (`"24"` → `'24'`), which changes the file hash and makes an unchanged
/// configuration look edited. The comparison is on parsed
/// [`ChannelEntry`] values, never on bytes.
fn write_channels(
    config: &Config,
    config_dir: &Path,
    query: &ScanQuery,
    rescanned: &HashSet<String>,
    families: &HashSet<String>,
    found: Vec<ChannelEntry>,
) -> Result<(Vec<ChannelEntry>, bool), String> {
    let _guard = store::lock(store::CHANNELS_FILE);
    let existing = read_channels(config_dir)?;
    let entries = merge(&existing, query, rescanned, families, found);
    if entries == existing {
        return Ok((entries, false));
    }

    let mut candidate = config.clone();
    candidate.channels = entries
        .iter()
        .map(|entry| crate::config::ChannelConfig {
            name: entry.name.clone(),
            channel_type: entry.channel_type.clone(),
            channel: entry.channel.clone(),
            extra_args: entry.extra_args.clone(),
            disabled: entry.disabled,
            routes: entry
                .routes
                .iter()
                .map(|route| crate::config::ChannelRouteConfig {
                    tuner: route.tuner.clone(),
                    channel: route.channel.clone(),
                    extra_args: route.extra_args.clone(),
                })
                .collect(),
        })
        .collect();
    candidate.validate().map_err(|error| match error {
        ConfigError::Invalid(issues) => format!(
            "refusing to write an invalid channels.yml: {}",
            issues
                .iter()
                .map(|issue| issue.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ),
        other => other.to_string(),
    })?;

    let yaml = serde_yaml_ng::to_string(&entries)
        .map_err(|error| format!("failed to serialize channels.yml: {error}"))?;

    let path = config_dir.join("channels.yml");
    let tmp = temp_path(config_dir);
    write_atomically(&path, &tmp, yaml.as_bytes())?;
    Ok((entries, true))
}

/// The temporary sibling used for the atomic rename.
fn temp_path(config_dir: &Path) -> PathBuf {
    let pid = std::process::id();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    config_dir.join(format!("channels.yml.{pid}.{stamp}.tmp"))
}

/// Writes `bytes` to `path` through `tmp`: fsync, then rename (SPEC §10's
/// write flow, reused by the scan).
fn write_atomically(path: &Path, tmp: &Path, bytes: &[u8]) -> Result<(), String> {
    let result = (|| {
        let mut file = std::fs::File::create(tmp)
            .map_err(|error| format!("failed to create {}: {error}", tmp.display()))?;
        file.write_all(bytes)
            .map_err(|error| format!("failed to write {}: {error}", tmp.display()))?;
        file.sync_all()
            .map_err(|error| format!("failed to sync {}: {error}", tmp.display()))?;
        std::fs::rename(tmp, path)
            .map_err(|error| format!("failed to replace {}: {error}", path.display()))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn type_is_required() {
        assert!(ScanQuery::from_params(&params(&[])).is_err());
        assert!(ScanQuery::from_params(&params(&[("type", "")])).is_err());
    }

    #[test]
    fn gr_defaults_match_the_spec() {
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        assert_eq!(query.min_channel, 13);
        assert_eq!(query.max_channel, 62);
        assert_eq!(query.mode, ScanMode::Channel);
        assert!(!query.set_disabled_on_add);
        assert_eq!(query.channel_name(27, 0), "27");
    }

    #[test]
    fn bs_defaults_use_service_mode_and_disabled_entries() {
        let query = ScanQuery::from_params(&params(&[("type", "BS")])).expect("query");
        assert_eq!(query.mode, ScanMode::Service);
        assert!(query.set_disabled_on_add);
        assert_eq!(query.channel_name(13, 1), "BS13_1");
    }

    #[test]
    fn cs_defaults_use_the_cs_prefix() {
        let query = ScanQuery::from_params(&params(&[("type", "CS")])).expect("query");
        assert_eq!(query.channel_name(2, 0), "CS02");
    }

    #[test]
    fn a_custom_format_replaces_every_placeholder() {
        let query = ScanQuery::from_params(&params(&[
            ("type", "GR"),
            ("channelNameFormat", "{ch00}-{ch}-{subch}"),
        ]))
        .expect("query");
        assert_eq!(query.channel_name(7, 3), "07-7-3");
    }

    #[test]
    fn skip_and_subchannel_lists_are_comma_separated() {
        let query = ScanQuery::from_params(&params(&[
            ("type", "BS"),
            ("skipCh", "1, 2,3"),
            ("minSubCh", "1"),
            ("maxSubCh", "2"),
        ]))
        .expect("query");
        assert_eq!(query.skip_channels, vec![1, 2, 3]);
        assert_eq!(query.sub_channels, vec![1, 2]);
    }

    #[test]
    fn subchannel_lists_pair_up_and_merge_into_one_ascending_set() {
        // `minSubCh=0,2` / `maxSubCh=1,3`: two ranges, tuned channel outer and
        // subchannel inner.
        let query = ScanQuery::from_params(&params(&[
            ("type", "BS"),
            ("minCh", "13"),
            ("maxCh", "13"),
            ("minSubCh", "0,2"),
            ("maxSubCh", "1,3"),
        ]))
        .expect("query");
        assert_eq!(query.sub_channels, vec![0, 1, 2, 3]);
        let names: Vec<String> = candidates(&query)
            .map(|candidate| candidate.channel)
            .collect();
        assert_eq!(names, ["BS13_0", "BS13_1", "BS13_2", "BS13_3"]);
        assert_eq!(query.candidate_count(), 4);

        // A shorter list is padded from its last value: `minSubCh=1&maxSubCh=1,2`.
        let query = ScanQuery::from_params(&params(&[
            ("type", "BS"),
            ("minCh", "13"),
            ("maxCh", "13"),
            ("minSubCh", "1"),
            ("maxSubCh", "1,2"),
        ]))
        .expect("query");
        assert_eq!(query.sub_channels, vec![1, 2]);
    }

    #[test]
    fn an_out_of_range_or_inverted_subchannel_list_is_rejected() {
        for (min, max) in [("0", "4"), ("4", "4"), ("2", "1")] {
            let error = ScanQuery::from_params(&params(&[
                ("type", "BS"),
                ("minCh", "13"),
                ("maxCh", "13"),
                ("minSubCh", min),
                ("maxSubCh", max),
            ]))
            .expect_err("invalid subchannel range");
            assert!(
                error.contains("SubCh") || error.contains("subchannel"),
                "{error}"
            );
        }
    }

    #[test]
    fn subchannel_parameters_are_ignored_when_the_type_does_not_expand() {
        // mirakurun: the subchannel parameters only apply to a BS subchannel
        // scan. A GR request that happens to carry them is not rejected and
        // tunes one candidate per channel number.
        let query = ScanQuery::from_params(&params(&[
            ("type", "GR"),
            ("minCh", "24"),
            ("maxCh", "25"),
            ("minSubCh", "0"),
            ("maxSubCh", "9"),
        ]))
        .expect("query");
        assert_eq!(query.sub_channels, vec![0]);
        let names: Vec<String> = candidates(&query)
            .map(|candidate| candidate.channel)
            .collect();
        assert_eq!(names, ["24", "25"]);
    }

    #[test]
    fn a_custom_type_requires_an_explicit_range() {
        // The BS/CS/GR defaults do not apply to a user-defined type: guessing
        // would tune channels the caller never asked for.
        let error = ScanQuery::from_params(&params(&[("type", "CABLE_CATV")]))
            .expect_err("no default range");
        assert!(error.contains("minCh"), "{error}");
        assert!(error.contains("CABLE_CATV"), "{error}");
        assert!(
            ScanQuery::from_params(&params(&[("type", "CABLE_CATV"), ("minCh", "1")])).is_err(),
            "both bounds are required"
        );

        let query = ScanQuery::from_params(&params(&[
            ("type", "CABLE_CATV"),
            ("minCh", "17"),
            ("maxCh", "18"),
        ]))
        .expect("query");
        assert_eq!(query.min_channel, 17);
        assert_eq!(query.max_channel, 18);
        // Conservative defaults for anything that is not BS/CS.
        assert_eq!(query.mode, ScanMode::Channel);
        assert!(!query.set_disabled_on_add);
        assert_eq!(query.channel_name(17, 0), "17");
        let names: Vec<String> = candidates(&query)
            .map(|candidate| candidate.channel)
            .collect();
        assert_eq!(names, ["17", "18"]);
    }

    #[test]
    fn the_builtin_types_still_default_their_ranges() {
        for (type_name, min, max) in [("GR", 13, 62), ("BS", 1, 24), ("CS", 1, 24)] {
            let query = ScanQuery::from_params(&params(&[("type", type_name)])).expect("query");
            assert_eq!(query.min_channel, min, "{type_name}");
            assert_eq!(query.max_channel, max, "{type_name}");
        }
    }

    #[test]
    fn invalid_values_are_rejected() {
        assert!(ScanQuery::from_params(&params(&[("type", "GR"), ("minCh", "x")])).is_err());
        assert!(ScanQuery::from_params(&params(&[("type", "GR"), ("refresh", "maybe")])).is_err());
        assert!(ScanQuery::from_params(&params(&[("type", "GR"), ("scanMode", "Bogus")])).is_err());
        assert!(ScanQuery::from_params(&params(&[
            ("type", "GR"),
            ("minCh", "30"),
            ("maxCh", "20")
        ]))
        .is_err());
    }

    #[test]
    fn candidates_expand_the_requested_range() {
        let query =
            ScanQuery::from_params(&params(&[("type", "GR"), ("minCh", "24"), ("maxCh", "26")]))
                .expect("query");
        let names: Vec<String> = candidates(&query)
            .map(|candidate| candidate.channel)
            .collect();
        assert_eq!(names, ["24", "25", "26"]);
        assert_eq!(query.candidate_count(), 3);
    }

    #[test]
    fn a_huge_range_is_rejected_before_anything_is_built() {
        let error = ScanQuery::from_params(&params(&[
            ("type", "GR"),
            ("minCh", "0"),
            ("maxCh", "4294967295"),
        ]))
        .expect_err("unbounded range");
        assert!(error.contains("at most 512"), "{error}");

        // A BS sweep with subchannels is counted with the expansion.
        let error = ScanQuery::from_params(&params(&[
            ("type", "BS"),
            ("minCh", "1"),
            ("maxCh", "400"),
            ("minSubCh", "0"),
            ("maxSubCh", "1"),
        ]))
        .expect_err("800 candidates");
        assert!(error.contains("at most 512"), "{error}");
    }

    #[test]
    fn a_range_exactly_at_the_cap_is_allowed() {
        let query =
            ScanQuery::from_params(&params(&[("type", "GR"), ("minCh", "0"), ("maxCh", "511")]))
                .expect("exactly at the cap");
        assert_eq!(query.candidate_count(), MAX_SCAN_CHANNELS as u64);
        assert_eq!(candidates(&query).count(), MAX_SCAN_CHANNELS);

        let error =
            ScanQuery::from_params(&params(&[("type", "GR"), ("minCh", "0"), ("maxCh", "512")]))
                .expect_err("one above the cap");
        assert!(error.contains("513"), "{error}");
    }

    #[test]
    fn an_oversized_skip_list_is_rejected() {
        let list = (0..MAX_SCAN_CHANNELS + 1)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let error = ScanQuery::from_params(&params(&[
            ("type", "GR"),
            ("minCh", "13"),
            ("maxCh", "13"),
            ("skipCh", &list),
        ]))
        .expect_err("skipCh is capped too");
        assert!(error.contains("skipCh"), "{error}");
    }

    #[test]
    fn a_service_refresh_drops_every_sibling_of_the_multiplex() {
        let existing = vec![
            ChannelEntry {
                name: "Old One".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_0".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "Old Two".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_1".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "Gone".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_2".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "Other".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS02_0".to_string(),
                disabled: false,
                ..Default::default()
            },
        ];
        let query =
            ScanQuery::from_params(&params(&[("type", "BS"), ("refresh", "true")])).expect("query");
        // The scan produced BS01_0 and BS01_1; BS01_2 disappeared.
        let found = vec![
            ChannelEntry {
                name: "New One".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_0".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "New Two".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_1".to_string(),
                disabled: false,
                ..Default::default()
            },
        ];
        let rescanned: HashSet<String> = ["BS01_0".to_string(), "BS01_1".to_string()]
            .into_iter()
            .collect();
        let families: HashSet<String> = ["BS01".to_string()].into_iter().collect();
        let merged = merge(&existing, &query, &rescanned, &families, found);
        let mut channels: Vec<&str> = merged.iter().map(|entry| entry.channel.as_str()).collect();
        channels.sort_unstable();
        assert_eq!(channels, ["BS01_0", "BS01_1", "BS02_0"]);
        assert!(!merged.iter().any(|entry| entry.name == "Gone"));
    }

    #[test]
    fn merge_appends_without_touching_other_types() {
        let existing = vec![
            ChannelEntry {
                name: "NHK".to_string(),
                channel_type: "GR".to_string(),
                channel: "27".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "BS1".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_0".to_string(),
                disabled: true,
                ..Default::default()
            },
        ];
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let rescanned: HashSet<String> = ["27".to_string()].into_iter().collect();
        let found = vec![ChannelEntry {
            name: "New".to_string(),
            channel_type: "GR".to_string(),
            channel: "28".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let merged = merge(&existing, &query, &rescanned, &HashSet::new(), found);
        assert_eq!(merged.len(), 3);
        // The already configured channel is kept, the BS entry is untouched.
        assert!(merged.iter().any(|entry| entry.channel == "27"));
        assert!(merged.iter().any(|entry| entry.channel_type == "BS"));
        assert!(merged.iter().any(|entry| entry.channel == "28"));
    }

    #[test]
    fn refresh_replaces_only_the_scanned_channels_of_the_type() {
        let existing = vec![
            ChannelEntry {
                name: "Old".to_string(),
                channel_type: "GR".to_string(),
                channel: "27".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "Keep".to_string(),
                channel_type: "GR".to_string(),
                channel: "99".to_string(),
                disabled: false,
                ..Default::default()
            },
        ];
        let query =
            ScanQuery::from_params(&params(&[("type", "GR"), ("refresh", "true")])).expect("query");
        let rescanned: HashSet<String> = ["27".to_string()].into_iter().collect();
        let found = vec![ChannelEntry {
            name: "Fresh".to_string(),
            channel_type: "GR".to_string(),
            channel: "27".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let merged = merge(&existing, &query, &rescanned, &HashSet::new(), found);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|entry| entry.name == "Fresh"));
        assert!(merged.iter().any(|entry| entry.name == "Keep"));
        assert!(!merged.iter().any(|entry| entry.name == "Old"));
    }

    #[test]
    fn refresh_keeps_channels_the_scan_could_not_read() {
        let existing = vec![ChannelEntry {
            name: "NHK".to_string(),
            channel_type: "GR".to_string(),
            channel: "27".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let query =
            ScanQuery::from_params(&params(&[("type", "GR"), ("refresh", "true")])).expect("query");
        // The scan saw no signal on 27, so nothing is rescanned.
        let merged = merge(
            &existing,
            &query,
            &HashSet::new(),
            &HashSet::new(),
            Vec::new(),
        );
        assert_eq!(merged, existing);
    }

    #[test]
    fn colliding_names_keep_the_service_name_and_get_a_suffix() {
        let existing = vec![ChannelEntry {
            name: "Same".to_string(),
            channel_type: "GR".to_string(),
            channel: "27".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let found = vec![
            ChannelEntry {
                name: "Same".to_string(),
                channel_type: "GR".to_string(),
                channel: "28".to_string(),
                disabled: false,
                ..Default::default()
            },
            ChannelEntry {
                name: "Same".to_string(),
                channel_type: "GR".to_string(),
                channel: "29".to_string(),
                disabled: false,
                ..Default::default()
            },
            // An empty SDT name is the only case that falls back to the channel.
            ChannelEntry {
                name: String::new(),
                channel_type: "GR".to_string(),
                channel: "30".to_string(),
                disabled: false,
                ..Default::default()
            },
        ];
        let merged = merge(&existing, &query, &HashSet::new(), &HashSet::new(), found);
        assert_eq!(merged[1].name, "Same (2)", "the service name survives");
        assert_eq!(merged[2].name, "Same (3)");
        assert_eq!(
            merged[3].name, "30",
            "an empty name falls back to the channel"
        );
    }

    #[test]
    fn service_mode_emits_one_entry_per_service() {
        let query = ScanQuery::from_params(&params(&[
            ("type", "BS"),
            ("scanMode", "Service"),
            ("channelNameFormat", "BS{ch00}_{sid}"),
        ]))
        .expect("query");
        let services = vec![
            FoundService {
                service_id: 101,
                network_id: 1,
                tsid: 1,
                service_type: 0x01,
                name: "One".to_string(),
                remote_control_key_id: None,
                pmt_pid: None,
            },
            FoundService {
                service_id: 102,
                network_id: 1,
                tsid: 1,
                service_type: 0x01,
                name: "Two".to_string(),
                remote_control_key_id: None,
                pmt_pid: None,
            },
        ];
        let candidate = Candidate {
            number: 1,
            sub: 0,
            channel: "BS01_0".to_string(),
        };
        let entries = entries_for(&query, &[], &candidate, &services);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].channel, "BS01_101");
        assert_eq!(entries[1].channel, "BS01_102");
    }

    #[test]
    fn update_service_table_keeps_each_entries_sids_and_drops_the_stale_ones() {
        let table = ServiceTable::new();
        for (sid, channel) in [
            (101u16, "BS01_1"),
            (102, "BS01_1"),
            (103, "BS01_1"),
            (201, "BS13_1"),
        ] {
            let mut record = ServiceRecord::new(1, 1, sid, 0x01, "old");
            record.channel = Some(("BS".to_string(), channel.to_string()));
            table.insert(record);
        }

        let entry = |channel: &str, sid: u16| ChannelEntry {
            name: format!("Service {sid}"),
            channel_type: "BS".to_string(),
            channel: channel.to_string(),
            disabled: false,
            ..Default::default()
        };
        let service = |sid: u16| FoundService {
            service_id: sid,
            network_id: 1,
            tsid: 1,
            service_type: 0x01,
            name: format!("Service {sid}"),
            remote_control_key_id: None,
            pmt_pid: None,
        };
        // `useSubCh` collapses every service of one multiplex onto the same
        // channel: 103 disappeared, 101/102 are still on air.
        update_service_table(
            &table,
            &TableUpdate {
                type_name: "BS".to_string(),
                mode: ScanMode::Service,
                entries: vec![entry("BS01_1", 101), entry("BS01_1", 102)],
                services: vec![service(101), service(102)],
            },
        );

        let mut remaining: Vec<(u16, String)> = table
            .list()
            .into_iter()
            .map(|record| (record.service_id, record.channel.expect("bound").1))
            .collect();
        remaining.sort_unstable();
        assert_eq!(
            remaining,
            [
                (101, "BS01_1".to_string()),
                (102, "BS01_1".to_string()),
                (201, "BS13_1".to_string()),
            ],
            "only the disappeared sid of this channel goes"
        );

        // A channel that produced nothing must never be pruned.
        update_service_table(
            &table,
            &TableUpdate {
                type_name: "BS".to_string(),
                mode: ScanMode::Service,
                entries: Vec::new(),
                services: Vec::new(),
            },
        );
        assert_eq!(table.len(), 3, "a failed channel keeps its services");
    }

    #[test]
    fn the_scan_lock_admits_one_scanner() {
        let lock = ScanLock::default();
        let lease = lock.acquire().expect("first scan");
        assert!(lock.is_running());
        assert!(lock.acquire().is_none());
        drop(lease);
        assert!(!lock.is_running());
        assert!(lock.acquire().is_some());
    }

    #[test]
    fn a_concurrent_save_is_not_lost_by_the_write() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", "addr: \":0\"\n".to_string()),
            (
                "tuners.yml",
                "- name: t0\n  types: [GR]\n  command: 'true'\n".to_string(),
            ),
            (
                "channels.yml",
                "- name: NHK\n  type: GR\n  channel: \"27\"\n".to_string(),
            ),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n"
                    .to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Config::load(dir.path()).expect("config loads");
        // Simulate a webui save that landed while the scan was tuning: the file
        // on disk now holds a channel the scan never saw.
        std::fs::write(
            dir.path().join("channels.yml"),
            "- name: NHK\n  type: GR\n  channel: \"27\"\n- name: Saved\n  type: GR\n  channel: \"40\"\n",
        )
        .expect("simulate webui save");

        let query =
            ScanQuery::from_params(&params(&[("type", "GR"), ("refresh", "true")])).expect("query");
        let rescanned: HashSet<String> = ["28".to_string()].into_iter().collect();
        let found = vec![ChannelEntry {
            name: "New".to_string(),
            channel_type: "GR".to_string(),
            channel: "28".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let (written, did_write) = write_channels(
            &config,
            dir.path(),
            &query,
            &rescanned,
            &HashSet::new(),
            found,
        )
        .expect("write");
        assert!(did_write, "a new entry must be written");
        let channels: HashSet<String> = written.iter().map(|entry| entry.channel.clone()).collect();
        assert!(channels.contains("27"), "the scan's own entry is kept");
        assert!(channels.contains("28"), "the new entry is appended");
        assert!(
            channels.contains("40"),
            "the concurrent webui save must survive: {channels:?}"
        );
    }

    #[test]
    fn an_unchanged_channel_list_is_not_rewritten() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", "addr: \":0\"\n".to_string()),
            (
                "tuners.yml",
                "- name: t0\n  types: [GR]\n  command: 'true'\n".to_string(),
            ),
            // A channel the scan will find again, with the quoting a user wrote.
            (
                "channels.yml",
                "- name: NHK\n  type: GR\n  channel: \"24\"\n  disabled: false\n".to_string(),
            ),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n"
                    .to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Config::load(dir.path()).expect("config loads");
        let path = dir.path().join("channels.yml");
        let before = std::fs::read_to_string(&path).expect("read back");
        let before_meta = std::fs::metadata(&path).expect("metadata");

        // The scan finds exactly the entry already in the file.
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let found = vec![ChannelEntry {
            name: "NHK".to_string(),
            channel_type: "GR".to_string(),
            channel: "24".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let (entries, written) = write_channels(
            &config,
            dir.path(),
            &query,
            &HashSet::new(),
            &HashSet::new(),
            found,
        )
        .expect("write");

        assert!(
            !written,
            "a semantically unchanged list must not be written"
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(
            std::fs::read_to_string(&path).expect("read back"),
            before,
            "the file must stay byte-identical (quoting included)"
        );
        let after_meta = std::fs::metadata(&path).expect("metadata");
        assert_eq!(
            before_meta.modified().ok(),
            after_meta.modified().ok(),
            "the file must not be rewritten at all"
        );
    }

    #[test]
    fn a_changed_channel_list_is_still_written() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            ("server.yml", "addr: \":0\"\n".to_string()),
            (
                "tuners.yml",
                "- name: t0\n  types: [GR]\n  command: 'true'\n".to_string(),
            ),
            (
                "channels.yml",
                "- name: NHK\n  type: GR\n  channel: \"24\"\n  disabled: false\n".to_string(),
            ),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes:\n  GR: { pipeline: pass }\n"
                    .to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Config::load(dir.path()).expect("config loads");
        let path = dir.path().join("channels.yml");
        let before = std::fs::read_to_string(&path).expect("read back");

        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let found = vec![ChannelEntry {
            name: "New".to_string(),
            channel_type: "GR".to_string(),
            channel: "25".to_string(),
            disabled: false,
            ..Default::default()
        }];
        let (entries, written) = write_channels(
            &config,
            dir.path(),
            &query,
            &HashSet::new(),
            &HashSet::new(),
            found,
        )
        .expect("write");

        assert!(written, "an added entry must be written");
        assert_eq!(entries.len(), 2);
        let after = std::fs::read_to_string(&path).expect("read back");
        assert_ne!(after, before);
        assert!(after.contains("channel: '25'"), "{after}");
    }

    fn nit(network_id: u16, is_actual: bool, key_id: Option<u8>) -> SiEvent {
        SiEvent::Nit(crate::si::NitInfo {
            network_id,
            remote_control_key_id: key_id,
            is_actual,
            transport_stream_ids: Vec::new(),
            version_number: 0,
            current_next: true,
            section_number: 0,
            last_section_number: 0,
        })
    }

    fn sdt(original_network_id: u16, tsid: u16, service_id: u16) -> SiEvent {
        sdt_section(original_network_id, tsid, service_id, true, 0, 0)
    }

    #[allow(clippy::too_many_arguments)]
    fn sdt_section(
        original_network_id: u16,
        tsid: u16,
        service_id: u16,
        is_actual: bool,
        section_number: u8,
        last_section_number: u8,
    ) -> SiEvent {
        SiEvent::Sdt(crate::si::SdtInfo {
            original_network_id,
            transport_stream_id: tsid,
            services: vec![(service_id, 0x01, format!("Service {service_id:04X}"))],
            is_actual,
            version_number: 0,
            current_next: true,
            section_number,
            last_section_number,
        })
    }

    #[test]
    fn an_sdt_other_never_contributes_services() {
        let mut sink = ChannelSink::default();
        // The SDT-other of another transponder arrives first; its services and
        // TSID must be ignored, and the real SDT-actual must still be adopted.
        sink.push(&sdt_section(3, 0x1111, 0x0001, false, 0, 0), None);
        assert!(
            sink.found.is_empty(),
            "the SDT-other must not contribute services"
        );
        assert!(!sink.is_complete());

        sink.push(&sdt_section(3, 0x7FE0, 0x0408, true, 0, 0), None);
        sink.finish();
        assert!(sink.is_complete());
        assert_eq!(sink.found.len(), 1);
        assert_eq!(sink.found[0].service_id, 0x0408);
        assert_eq!(sink.found[0].tsid, 0x7FE0);
    }

    #[test]
    fn a_multi_section_sdt_is_accumulated_until_it_is_complete() {
        let mut sink = ChannelSink::default();
        // Three sections, delivered out of order.
        sink.push(&sdt_section(3, 0x7FE0, 0x0409, true, 1, 2), None);
        assert!(!sink.is_complete(), "1 of 2 alone is not complete");
        assert!(
            sink.found.is_empty(),
            "a partial table must not half-populate the result"
        );
        sink.push(&sdt_section(3, 0x7FE0, 0x040B, true, 2, 2), None);
        assert!(!sink.is_complete());
        sink.push(&sdt_section(3, 0x7FE0, 0x0408, true, 0, 2), None);

        assert!(sink.is_complete());
        sink.finish();
        let ids: Vec<u16> = sink
            .found
            .iter()
            .map(|service| service.service_id)
            .collect();
        assert_eq!(
            ids,
            [0x0408, 0x0409, 0x040B],
            "section order, no duplicates"
        );
    }

    #[test]
    fn a_partial_sdt_still_yields_its_services_at_the_end_of_the_budget() {
        let mut sink = ChannelSink::default();
        sink.push(&sdt_section(3, 0x7FE0, 0x0408, true, 0, 2), None);
        // Section 1 never arrives; the budget expires.
        assert!(!sink.is_complete());
        sink.finish();
        let ids: Vec<u16> = sink
            .found
            .iter()
            .map(|service| service.service_id)
            .collect();
        assert_eq!(ids, [0x0408], "the received sections are not thrown away");
    }

    #[test]
    fn a_nit_other_never_changes_the_derived_network_id() {
        let mut sink = ChannelSink::default();
        // NIT-actual of this multiplex: network 1, remote key 7.
        sink.push(&nit(1, true, Some(7)), None);
        // A NIT-other describes another transport stream: neither its network
        // id nor its remote-control key may be adopted.
        sink.push(&nit(2, false, Some(9)), None);
        sink.push(&sdt(3, 0x7FE0, 0x0408), None);
        sink.finish();

        assert_eq!(sink.found.len(), 1);
        assert_eq!(sink.found[0].network_id, 1);
        assert_eq!(sink.found[0].remote_control_key_id, Some(7));
        assert_eq!(sink.found[0].tsid, 0x7FE0);
    }

    #[test]
    fn a_lone_nit_other_leaves_the_sdt_network_id_in_place() {
        let mut sink = ChannelSink::default();
        sink.push(&nit(2, false, Some(9)), None);
        sink.push(&sdt(3, 0x7FE0, 0x0408), None);
        sink.finish();

        assert_eq!(sink.found[0].network_id, 3);
        assert_eq!(sink.found[0].remote_control_key_id, None);
    }

    #[test]
    fn the_parser_network_id_wins_over_the_sdt_and_the_sink() {
        let mut sink = ChannelSink::default();
        sink.push(&nit(1, true, None), None);
        // The parser applies the TSID-matching rule; its answer is preferred.
        sink.push(&sdt(3, 0x7FE0, 0x0408), Some(42));
        sink.finish();
        assert_eq!(sink.found[0].network_id, 42);
    }

    #[test]
    fn core_tables_require_pat_pmt_and_nit_actual_besides_the_sdt() {
        let mut sink = ChannelSink::default();
        // SDT complete alone is not enough to finish early.
        sink.push(&sdt(3, 0x7FE0, 0x0408), None);
        assert!(sink.is_complete());
        assert!(!sink.has_all_core_tables());
        sink.push(
            &SiEvent::Pat(crate::si::PatInfo {
                transport_stream_id: 0x7FE0,
                network_id: None,
                network_pid: None,
                programs: vec![(0x0408, 0x0100)],
                version_number: 0,
                current_next: true,
                section_number: 0,
                last_section_number: 0,
            }),
            None,
        );
        assert!(!sink.has_all_core_tables(), "PMT still missing");
        sink.push(
            &SiEvent::Pmt(crate::si::PmtInfo {
                program_number: 0x0408,
                pcr_pid: 0x0101,
                version_number: 0,
                current_next: true,
                section_number: 0,
                last_section_number: 0,
                streams: Vec::new(),
            }),
            None,
        );
        assert!(!sink.has_all_core_tables(), "NIT-actual still missing");
        sink.push(&nit(1, true, Some(7)), None);
        assert!(sink.has_all_core_tables());
    }

    #[test]
    fn the_startup_retry_backoff_doubles_and_caps() {
        assert_eq!(retry_delay(0), Duration::from_secs(30));
        assert_eq!(retry_delay(1), Duration::from_secs(60));
        assert_eq!(retry_delay(2), Duration::from_secs(120));
        assert_eq!(retry_delay(3), Duration::from_secs(240));
        assert_eq!(retry_delay(4), STARTUP_RETRY_MAX);
        assert_eq!(retry_delay(50), STARTUP_RETRY_MAX);
    }

    #[test]
    fn a_preempted_channel_is_retried_like_a_busy_one() {
        assert!(retries(&ChannelOutcome::Busy));
        assert!(retries(&ChannelOutcome::Preempted));
        assert!(!retries(&ChannelOutcome::NoSignal));
        // A missing tuner or a broken command never changes state by waiting:
        // retrying them would spin forever.
        assert!(!retries(&ChannelOutcome::NoTuner));
        assert!(!retries(&ChannelOutcome::TunerFailed(
            "command failed".to_string()
        )));
    }

    #[test]
    fn every_outcome_has_its_own_progress_line() {
        // The five non-service outcomes must be distinguishable at a glance:
        // a broken tuner must never look like no signal, and an absent tuner
        // must never look busy.
        assert_eq!(
            outcome_line("XX", &ChannelOutcome::NoTuner).as_deref(),
            Some("skipped (no tuner for type XX)\n")
        );
        assert_eq!(
            outcome_line("GR", &ChannelOutcome::Busy).as_deref(),
            Some("skipped (tuner busy)\n")
        );
        assert_eq!(
            outcome_line("GR", &ChannelOutcome::Preempted).as_deref(),
            Some("skipped (preempted by a user request)\n")
        );
        assert_eq!(
            outcome_line(
                "GR",
                &ChannelOutcome::TunerFailed("stream ended before any data".to_string())
            )
            .as_deref(),
            Some("tuner failed to start: stream ended before any data\n")
        );
        assert_eq!(
            outcome_line("GR", &ChannelOutcome::NoSignal).as_deref(),
            Some("no signal\n")
        );
        assert_eq!(
            outcome_line("GR", &ChannelOutcome::Failed).as_deref(),
            Some("error (scan task failed)\n")
        );
        // A service outcome is rendered by the caller's "N services found".
        assert!(outcome_line("GR", &ChannelOutcome::Services(Vec::new())).is_none());
    }

    #[test]
    fn a_start_failure_reason_is_a_single_short_line() {
        let long = format!("error\n\n{}", "x".repeat(500));
        let short = short_reason(&long);
        assert!(!short.contains('\n'), "{short}");
        assert_eq!(short.chars().count(), 200, "{short}");
        assert!(short.ends_with("..."), "{short}");
        assert_eq!(short_reason("line one\nline two"), "line one line two");
    }

    #[test]
    fn the_channel_budget_outlasts_the_readiness_gate() {
        assert!(
            CHANNEL_TIMEOUT > crate::session::READINESS_TIMEOUT,
            "a channel budget at or below the readiness timeout would cancel every slow spawn"
        );
    }

    /// A scan fixture: one `GR` tuner whose command depends on the channel.
    fn scan_tuner(
        commands: &str,
    ) -> (
        tempfile::TempDir,
        Arc<Config>,
        crate::session::SessionManager,
    ) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        for (name, body) in [
            (
                "server.yml",
                "addr: \":0\"\nidle_timeout_secs: 5\n".to_string(),
            ),
            (
                "tuners.yml",
                format!("- name: t0\n  types: [GR]\n  command: '{commands}'\n"),
            ),
            ("channels.yml", String::new()),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n".to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Arc::new(Config::load(dir.path()).expect("config loads"));
        let manager = crate::session::SessionManager::new(
            Arc::clone(&config),
            crate::services::ServiceTable::new(),
        );
        (dir, config, manager)
    }

    #[tokio::test]
    async fn a_silent_channel_does_not_block_the_next_one() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mux = dir.path().join("mux.bin");
        let mut packet = vec![0x11u8; crate::tsframe::TS_PACKET_SIZE];
        packet[0] = 0x47;
        std::fs::write(&mux, [packet.clone(), packet.clone(), packet].concat()).expect("write mux");
        // Channel 27 produces no byte at all; 28 streams immediately.
        let commands = format!(
            "sh -c \"case {{channel}} in 27) sleep 30;; *) cat {}; sleep 30;; esac\"",
            mux.display()
        );
        let (_dir, _config, manager) = scan_tuner(&commands);

        let started = tokio::time::Instant::now();
        let silent = scan_channel(&manager, "GR", "27", Duration::from_secs(1)).await;
        assert!(
            matches!(silent, ChannelOutcome::NoSignal),
            "a silent channel is no signal: {silent:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the channel budget was not honoured: {:?}",
            started.elapsed()
        );

        // The silent channel must not occupy the tuner: the next one is scanned.
        let next = tokio::time::timeout(
            Duration::from_secs(5),
            scan_channel(&manager, "GR", "28", Duration::from_secs(2)),
        )
        .await
        .expect("the next channel must not be blocked by the silent one");
        assert!(
            !matches!(next, ChannelOutcome::Busy | ChannelOutcome::Preempted),
            "the next channel lost the tuner: {next:?} sessions={:?}",
            manager.sessions_snapshot()
        );
        assert!(
            manager.sessions_snapshot().is_empty(),
            "the scan must release its session: {:?}",
            manager.sessions_snapshot()
        );

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_user_takeover_during_a_scan_is_reported_as_preempted() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mux = dir.path().join("mux.bin");
        let mut packet = vec![0x11u8; crate::tsframe::TS_PACKET_SIZE];
        packet[0] = 0x47;
        // Three packets: the normalizer confirms the packet size from more than
        // one sync byte before it emits anything.
        std::fs::write(&mux, [packet.clone(), packet.clone(), packet].concat()).expect("write mux");
        // Both channels stream, but the fixture carries no SI, so the scan
        // stays on 27 without ever completing a service list.
        let commands = format!("sh -c \"cat {}; sleep 30\"", mux.display());
        let (_dir, _config, manager) = scan_tuner(&commands);

        let scan = tokio::spawn({
            let manager = manager.clone();
            async move { scan_channel(&manager, "GR", "27", Duration::from_secs(10)).await }
        });

        // Wait until the scan session is streaming, then let a viewer take it
        // over (any non-negative priority beats `SCAN_PRIORITY`).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let ready = manager.sessions_snapshot().iter().any(|session| {
                session.channel == "27" && session.phase == crate::session::SessionPhase::Ready
            });
            if ready {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the scan never started streaming: {:?}",
                manager.sessions_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let viewer = manager
            .acquire("GR", "28", TunerUser::new("viewer", 0), None, true)
            .await
            .expect("the viewer must win over the scan");
        assert_eq!(viewer.channel, "28");

        let outcome = tokio::time::timeout(Duration::from_secs(10), scan)
            .await
            .expect("the scan must notice the takeover")
            .expect("scan task");
        assert!(
            matches!(outcome, ChannelOutcome::Preempted),
            "a user takeover must not be reported as no signal: {outcome:?}"
        );

        drop(viewer);
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_tuner_that_exits_before_output_is_tuner_failed_not_no_signal() {
        // The command starts but exits before its first TS byte: that is a
        // start failure, not "no signal" (the channel was never read).
        let (_dir, _config, manager) = scan_tuner("sh -c \"sleep 1; exit 0\"");
        let started = tokio::time::Instant::now();
        let outcome = scan_channel(&manager, "GR", "27", Duration::from_secs(5)).await;
        match outcome {
            ChannelOutcome::TunerFailed(reason) => {
                assert!(
                    reason.contains("ended before any data") || reason.contains("before any data"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("an early exit must be a tuner failure, got {other:?}"),
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(700),
            "the exit happened before the readiness gate: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(4),
            "the scan waited for the whole budget instead of noticing the exit: {elapsed:?}"
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn garbage_without_si_is_no_signal_after_the_budget() {
        // The tuner delivers data continuously, but every packet is a null
        // packet: a session was acquired and the read loop simply saw no
        // usable SI, which is the only case that should say "no signal".
        let dir = tempfile::TempDir::new().expect("tempdir");
        let garbage = dir.path().join("garbage.ts");
        let mut packet = vec![0xFFu8; crate::tsframe::TS_PACKET_SIZE];
        packet[0] = 0x47;
        packet[1] = 0x1F; // PID 0x1FFF: null packet, ignored by the parser.
        packet[2] = 0xFF;
        packet[3] = 0x10;
        packet[4] = 0x00;
        std::fs::write(&garbage, [packet.clone(), packet.clone(), packet].concat())
            .expect("write garbage");
        let command = format!("sh -c \"cat {}; sleep 30\"", garbage.display());
        let (_config_dir, _config, manager) = scan_tuner(&command);

        let started = tokio::time::Instant::now();
        let outcome = scan_channel(&manager, "GR", "27", Duration::from_secs(1)).await;
        assert!(
            matches!(outcome, ChannelOutcome::NoSignal),
            "garbage must be no signal, got {outcome:?}"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "the read loop gave up before its budget: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "the read loop ignored its budget: {elapsed:?}"
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn a_scan_releases_the_routed_channels_effective_session_only() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mux = dir.path().join("mux.bin");
        let mut packet = vec![0x11u8; crate::tsframe::TS_PACKET_SIZE];
        packet[0] = 0x47;
        std::fs::write(&mux, [packet.clone(), packet.clone(), packet].concat()).expect("write mux");
        let command = format!("sh -c \"cat {}; sleep 30\"", mux.display());
        for (name, body) in [
            (
                "server.yml",
                "addr: \":0\"\nidle_timeout_secs: 30\n".to_string(),
            ),
            (
                "tuners.yml",
                format!(
                    "- name: t0\n  types: [GR]\n  command: '{command}'\n\
                     - name: t1\n  types: [GR]\n  command: '{command}'\n"
                ),
            ),
            (
                "channels.yml",
                "- name: AT-X\n  type: GR\n  channel: \"AT-X\"\n  routes:\n    - tuner: t0\n      channel: \"13\"\n"
                    .to_string(),
            ),
            (
                "pipelines.yml",
                "pipelines:\n  pass:\n    - \"cat\"\ntypes: {}\n".to_string(),
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write config");
        }
        let config = Arc::new(Config::load(dir.path()).expect("config loads"));
        let manager =
            SessionManager::new(Arc::clone(&config), crate::services::ServiceTable::new());

        // An unrelated session using the *logical* channel name, created by a
        // pinned request on the other tuner (pinning ignores routes). It is left
        // idle so nothing but the release path could stop it.
        let pinned = manager
            .acquire(
                "GR",
                "AT-X",
                TunerUser::new("viewer", 0),
                Some("t1".to_string()),
                true,
            )
            .await
            .expect("pinned acquire");
        assert_eq!(pinned.channel, "AT-X");
        drop(pinned);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while manager
            .sessions_snapshot()
            .iter()
            .any(|session| session.channel == "AT-X" && session.users != 0)
        {
            assert!(std::time::Instant::now() < deadline, "session never idle");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // The scan tunes the routed channel; its session is keyed by the route's
        // effective channel "13", not by "AT-X".
        let outcome = scan_channel(&manager, "GR", "AT-X", Duration::from_millis(200)).await;
        assert!(matches!(outcome, ChannelOutcome::NoSignal), "{outcome:?}");

        let sessions = manager.sessions_snapshot();
        assert!(
            sessions.iter().all(|session| session.channel != "13"),
            "the routed scan session was not released: {sessions:?}"
        );
        assert!(
            sessions
                .iter()
                .any(|session| session.channel == "AT-X" && session.tuner == "t1"),
            "the unrelated logical-channel session was disturbed: {sessions:?}"
        );

        manager.shutdown().await;
    }
}

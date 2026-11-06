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
//! A channel that produces no signal, times out, or whose tuner is busy is
//! skipped and the scan continues (SPEC §8.1). Each per-channel session is
//! released before the next channel is tuned.
//!
//! # Cancellation
//!
//! A scan stops when its progress receiver goes away (the HTTP client
//! disconnected) and when the server-wide shutdown watch fires: both are
//! observed between channels and while waiting for SI, and neither writes
//! `channels.yml`.
//!
//! # Startup scan
//!
//! The same per-channel helper ([`scan_channel`]) also drives the SPEC §8
//! startup service scan ([`startup_scan`]): every enabled `channels.yml` entry
//! is tuned once, the found services are upserted into the shared
//! [`ServiceTable`], and channels whose tuner was busy or absent are retried in
//! the background with exponential backoff. The task stops on a shutdown
//! signal and never preempts a user ([`SCAN_PRIORITY`]).
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
//! prints what a real scan would do.
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
use crate::services::{ServiceRecord, ServiceTable};
use crate::session::{SessionManager, SessionTap, TunerUser, SCAN_PRIORITY};
use crate::si::{SiEvent, SiParser};
use crate::webui::store;

/// Default per-channel scan budget (SPEC §8: "既定 timeout 10s/ch").
pub const CHANNEL_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard cap on the channels one scan request may touch.
///
/// The range is user supplied; without a cap `minCh=0&maxCh=4294967295` builds
/// a four-billion-element list (remote OOM/hang). 512 covers a full BS/CS
/// sweep with subchannels and is small enough to stay a handful of bytes of
/// state per candidate.
pub const MAX_SCAN_CHANNELS: usize = 512;

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
    pub min_sub_channels: Vec<u32>,
    pub max_sub_channels: Vec<u32>,
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
        let min_channel = parse_u32(params, "minCh")?.unwrap_or(if satellite { 1 } else { 13 });
        let max_channel = parse_u32(params, "maxCh")?.unwrap_or(if satellite { 24 } else { 62 });
        if min_channel > max_channel {
            return Err(format!(
                "minCh ({min_channel}) must not be greater than maxCh ({max_channel})"
            ));
        }

        let refresh = parse_bool(params, "refresh")?.unwrap_or(false);
        let dry_run = parse_bool(params, "dryRun")?.unwrap_or(false);
        let mode = match params.get("scanMode").map(|value| value.trim()) {
            None | Some("") => {
                if type_name == "GR" || !satellite {
                    ScanMode::Channel
                } else {
                    ScanMode::Service
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
            min_sub_channels,
            max_sub_channels,
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
        if self.use_sub_channel || self.type_name == "BS" {
            let min = self.min_sub_channels.first().copied().unwrap_or(0).min(1);
            let max = self.max_sub_channels.last().copied().unwrap_or(0).max(min);
            u64::from(max - min) + 1
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChannelEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: String,
    pub channel: String,
    #[serde(default)]
    pub disabled: bool,
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
    /// Services in SDT order, one entry per service id.
    found: Vec<FoundService>,
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
                    self.nit_network_id = Some(nit.network_id);
                    self.remote_control_key_id =
                        nit.remote_control_key_id.or(self.remote_control_key_id);
                }
            }
            SiEvent::Pat(pat) => {
                for &(service_id, pmt_pid) in &pat.programs {
                    self.pmt_pids.insert(service_id, pmt_pid);
                }
                for service in &mut self.found {
                    if service.tsid == 0 {
                        service.tsid = pat.transport_stream_id;
                    }
                }
            }
            SiEvent::Sdt(sdt) => {
                let network_id = self.network_id().unwrap_or(sdt.original_network_id);
                for (service_id, service_type, name) in &sdt.services {
                    if self
                        .found
                        .iter()
                        .any(|service| service.service_id == *service_id)
                    {
                        continue;
                    }
                    self.found.push(FoundService {
                        service_id: *service_id,
                        network_id,
                        tsid: sdt.transport_stream_id,
                        service_type: *service_type,
                        name: name.clone(),
                        remote_control_key_id: None,
                        pmt_pid: None,
                    });
                }
            }
            SiEvent::Pmt(_) => {}
        }
    }

    /// Copies the NIT remote-control key id and the PAT PMT PIDs onto every
    /// service found so far.
    fn finish(&mut self) {
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

    for (index, candidate) in candidates(query).enumerate() {
        if lines.is_closed() || cancelled(&shutdown) {
            tracing::info!("channel scan cancelled");
            return Ok(());
        }
        let percent = (index as u64 + 1) * 100 / total;
        lines
            .push(format!(
                "channel: {:?} ({}/{}) [{}%] ... ",
                candidate.channel,
                index + 1,
                total,
                percent
            ))
            .await;

        if query.skip_channels.contains(&candidate.number) {
            lines.push("skipped (skipCh)\n".to_string()).await;
            continue;
        }
        // SPEC §8.1: an enabled entry is left alone unless `refresh=true`; a
        // disabled one is a re-scan target.
        if !query.refresh {
            if let Some(entry) = existing.iter().find(|entry| {
                entry.channel_type == query.type_name && entry.channel == candidate.channel
            }) {
                if !entry.disabled {
                    lines
                        .push("skipped (already configured)\n".to_string())
                        .await;
                    continue;
                }
            }
        }

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
        match outcome {
            ChannelOutcome::Busy => lines.push("skipped (tuner busy)\n".to_string()).await,
            ChannelOutcome::NoSignal => lines.push("no signal\n".to_string()).await,
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
                lines
                    .push(format!(
                        "Found {} channels for {}\n",
                        entries.len(),
                        candidate.channel
                    ))
                    .await;
                if !query.dry_run {
                    update_service_table(
                        manager.services(),
                        &query.type_name,
                        query.mode,
                        &entries,
                        &services,
                    );
                }
                found_entries.extend(entries);
            }
        }
    }

    // The client may have gone away while the last channel was read.
    if lines.is_closed() {
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
        let (entries, written) = write_channels(
            config,
            config_dir,
            query,
            &rescanned,
            &families,
            found_entries,
        )?;
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
    shutdown
        .as_ref()
        .is_some_and(|rx| *rx.borrow() || rx.has_changed().is_err())
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
/// `ScanQuery::from_params` already rejected a range above
/// [`MAX_SCAN_CHANNELS`], so this iterator is bounded; it is still lazy so the
/// first progress line is emitted before any list is built.
fn candidates(query: &ScanQuery) -> impl Iterator<Item = Candidate> + '_ {
    let (sub_min, sub_max) = if query.use_sub_channel || query.type_name == "BS" {
        let min = query.min_sub_channels.first().copied().unwrap_or(0).min(1);
        let max = query.max_sub_channels.last().copied().unwrap_or(0).max(min);
        (min, max)
    } else {
        (0, 0)
    };
    (query.min_channel..=query.max_channel).flat_map(move |number| {
        (sub_min..=sub_max).map(move |sub| Candidate {
            number,
            sub,
            channel: query.channel_name(number, sub),
        })
    })
}

/// Why a channel produced no entries.
#[derive(Debug)]
pub(crate) enum ChannelOutcome {
    /// Every tuner was busy, or no configured tuner supports the type:
    /// SPEC §8 skips the channel and retries it later.
    Busy,
    /// No output, or output without SI, within the per-channel budget (also
    /// what a tuner that failed to start is reported as).
    NoSignal,
    /// SDT services seen on the channel.
    Services(Vec<FoundService>),
}

/// [`scan_channel`] with a shutdown watch.
///
/// The cancellation is observed **between reads**, never by dropping a future
/// that is still acquiring: an interrupted `acquire` can leave a reservation
/// behind, while a read-loop break still runs the normal "release the scan
/// session" path. `None` means shutdown fired while the channel was being read;
/// the caller stops without writing `channels.yml`.
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

/// Tunes one channel, collects SI for at most `timeout`, and releases the scan
/// session before returning (SPEC §8).
///
/// Shared by the SPEC §8 startup scan and the SPEC §8.1 scan API so both parse
/// SI the same way. The session is acquired through
/// [`SessionManager::acquire_scan`], which may retire an idle session even at
/// [`SCAN_PRIORITY`]; every path releases that session again before returning,
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
    // session a user already holds is therefore correct, and it must not be
    // released afterwards.
    let preexisting = manager
        .sessions_snapshot()
        .iter()
        .any(|session| session.type_name == type_name && session.channel == channel);

    let user = TunerUser::new("scan", SCAN_PRIORITY);
    // One budget per channel covers tuning *and* reading, so a wedged frontend
    // cannot stretch a scan past `timeout` per channel (SPEC §8: 10s/ch).
    let deadline = Instant::now() + timeout;
    let acquired = tokio::time::timeout_at(
        deadline,
        manager.acquire_scan(type_name, channel, user, SessionTap::Raw, false),
    )
    .await;

    let mut subscription = match acquired {
        Err(_) => {
            tracing::debug!(channel, "scan acquire timed out");
            return (ChannelOutcome::NoSignal, false);
        }
        Ok(Err(error)) => {
            tracing::debug!(channel, %error, "scan could not acquire a tuner");
            return (
                match error {
                    // SPEC §8: "相手がbusyならskip＋次回retry". A busy tuner and a
                    // channel type with no tuner at all are both remembered for
                    // the next pass. A tuner whose command failed to start is
                    // `Unavailable`: retrying it forever would only spin, so it
                    // is reported like no signal.
                    crate::session::AcquireError::Busy
                    | crate::session::AcquireError::NoTuner
                    | crate::session::AcquireError::DecodeModeConflict => ChannelOutcome::Busy,
                    _ => ChannelOutcome::NoSignal,
                },
                false,
            );
        }
        Ok(Ok(subscription)) => subscription,
    };

    let mut parser = SiParser::new();
    let mut sink = ChannelSink::default();
    let mut cancelled = false;
    // Remembered before the subscription is dropped: it tells the release path
    // which subscriber was ours, so a client that attached meanwhile keeps the
    // session alive while a stale scan subscriber does not wedge it.
    let subscriber_id = subscription.subscriber_id;
    loop {
        let chunk = match shutdown.as_deref_mut() {
            Some(rx) => {
                let watched = tokio::select! {
                    chunk = tokio::time::timeout_at(deadline, subscription.rx.recv()) => Some(chunk),
                    _ = rx.changed() => None,
                };
                match watched {
                    Some(chunk) => chunk,
                    None => {
                        cancelled = true;
                        break;
                    }
                }
            }
            None => tokio::time::timeout_at(deadline, subscription.rx.recv()).await,
        };
        match chunk {
            Ok(Some(chunk)) => {
                for event in parser.push_packets(&chunk) {
                    sink.push(&event, parser.network_id());
                    // SDT supplies the names; once it has arrived there is no
                    // reason to keep the tuner for the rest of the budget. A
                    // PAT seen before it also supplied the PMT PIDs; if it did
                    // not, the record simply has no PMT PID yet.
                    if matches!(event, SiEvent::Sdt(_)) && !sink.found.is_empty() {
                        sink.finish();
                        drop(subscription);
                        if !preexisting {
                            manager
                                .release_session(type_name, channel, subscriber_id)
                                .await;
                        }
                        return (ChannelOutcome::Services(sink.found), cancelled);
                    }
                }
            }
            // The stream ended (a finite fake tuner, or a dead pipeline), or
            // the per-channel budget ran out.
            Ok(None) | Err(_) => break,
        }
    }

    tracing::debug!(
        channel,
        stats = ?parser.stats(),
        found = sink.found.len(),
        "scan channel finished without completing a service list"
    );
    sink.finish();

    // Release the scan session before the next channel is tuned (SPEC §8.1).
    drop(subscription);
    if !preexisting {
        manager
            .release_session(type_name, channel, subscriber_id)
            .await;
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
                    name: if service.name.is_empty() {
                        channel.clone()
                    } else {
                        service.name.clone()
                    },
                    channel_type: query.type_name.clone(),
                    disabled: disabled_of(&channel),
                    channel,
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
/// * names stay unique (the startup validation requires it), so a colliding
///   name gets the channel appended.
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
        if !names.insert(entry.name.clone()) {
            entry.name = format!("{} ({})", entry.name, entry.channel);
            // Two services can share a name on the same multiplex; keep the
            // generated name unique by falling back to the channel + sid.
            let mut suffix = 1;
            while !names.insert(entry.name.clone()) {
                entry.name = format!("{} ({}-{suffix})", entry.channel, suffix);
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

/// Writes the found services into the shared table (SPEC §13: a scan is
/// reflected by `/api/services`).
fn update_service_table(
    services: &ServiceTable,
    type_name: &str,
    mode: ScanMode,
    entries: &[ChannelEntry],
    found: &[FoundService],
) {
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
            record.channel = Some((type_name.to_string(), channel));
        }
        services.upsert(record);
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
}

/// Scans every enabled `channels.yml` entry once and retries the skipped ones
/// with exponential backoff until `shutdown` fires (SPEC §8).
///
/// * Each channel runs through [`scan_channel`] at [`SCAN_PRIORITY`], so a user
///   request always wins and a busy tuner is skipped rather than preempted.
/// * The per-channel budget is [`CHANNEL_TIMEOUT`], covering acquire + SI read.
/// * Channels that failed for lack of a tuner ([`ChannelOutcome::Busy`], i.e.
///   busy or no supporting tuner) are retried; a no-signal channel is not.
/// * Entries are only ever upserted, never cleared, so a failed channel keeps
///   whatever the previous pass (or an HTTP scan) put in the table.
///
/// The task owns no processes: [`scan_channel`] releases each session before
/// returning, and the shutdown signal is observed before every channel.
pub async fn startup_scan(
    manager: SessionManager,
    config: Arc<Config>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut pending = startup_channels(&config);
    if pending.is_empty() {
        tracing::info!("startup scan: no enabled channels");
        return;
    }
    tracing::info!(
        channels = pending.len(),
        "startup scan started (priority {SCAN_PRIORITY})"
    );

    let mut pass = 0usize;
    loop {
        if *shutdown.borrow() {
            tracing::info!(pass, "startup scan stopped");
            return;
        }
        let (summary, retry) = scan_pass(&manager, &pending, &mut shutdown).await;
        tracing::info!(
            pass,
            attempted = summary.attempted,
            ok = summary.ok,
            skipped = summary.skipped,
            found = summary.found,
            "startup scan pass finished"
        );

        // A pass interrupted by shutdown must not sleep: return right away so
        // the task never outlives the server.
        if *shutdown.borrow() {
            tracing::info!(pass, "startup scan stopped");
            return;
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

/// Scans `pending` sequentially, returning the pass summary and the channels
/// that should be retried (only those whose tuner was unavailable).
async fn scan_pass(
    manager: &SessionManager,
    pending: &[StartupChannel],
    shutdown: &mut watch::Receiver<bool>,
) -> (PassSummary, Vec<StartupChannel>) {
    let mut summary = PassSummary::default();
    let mut retry = Vec::new();
    for channel in pending {
        if *shutdown.borrow() {
            break;
        }
        summary.attempted += 1;
        // Cancelled between reads (never by dropping an in-flight `acquire`,
        // which could leave a reservation behind): shutdown must not wait for a
        // channel's whole budget, let alone `timeout × channels`.
        let Some(outcome) = scan_channel_cancellable(
            manager,
            &channel.type_name,
            &channel.channel,
            CHANNEL_TIMEOUT,
            Some(shutdown.clone()),
        )
        .await
        else {
            summary.attempted -= 1;
            break;
        };
        match outcome {
            ChannelOutcome::Busy => {
                summary.skipped += 1;
                retry.push(channel.clone());
                tracing::debug!(
                    type_name = %channel.type_name,
                    channel = %channel.channel,
                    "startup scan skipped the channel: no tuner available"
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
            ChannelOutcome::Services(services) => {
                summary.ok += 1;
                summary.found += upsert_startup_services(manager.services(), channel, &services);
            }
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
            disabled: entry.disabled,
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
        assert_eq!(query.min_sub_channels, vec![1]);
        assert_eq!(query.max_sub_channels, vec![2]);
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
            },
            ChannelEntry {
                name: "Old Two".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_1".to_string(),
                disabled: false,
            },
            ChannelEntry {
                name: "Gone".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_2".to_string(),
                disabled: false,
            },
            ChannelEntry {
                name: "Other".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS02_0".to_string(),
                disabled: false,
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
            },
            ChannelEntry {
                name: "New Two".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_1".to_string(),
                disabled: false,
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
            },
            ChannelEntry {
                name: "BS1".to_string(),
                channel_type: "BS".to_string(),
                channel: "BS01_0".to_string(),
                disabled: true,
            },
        ];
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let rescanned: HashSet<String> = ["27".to_string()].into_iter().collect();
        let found = vec![ChannelEntry {
            name: "New".to_string(),
            channel_type: "GR".to_string(),
            channel: "28".to_string(),
            disabled: false,
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
            },
            ChannelEntry {
                name: "Keep".to_string(),
                channel_type: "GR".to_string(),
                channel: "99".to_string(),
                disabled: false,
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
    fn colliding_names_stay_unique() {
        let existing = vec![ChannelEntry {
            name: "Same".to_string(),
            channel_type: "GR".to_string(),
            channel: "27".to_string(),
            disabled: false,
        }];
        let query = ScanQuery::from_params(&params(&[("type", "GR")])).expect("query");
        let found = vec![ChannelEntry {
            name: "Same".to_string(),
            channel_type: "GR".to_string(),
            channel: "28".to_string(),
            disabled: false,
        }];
        let merged = merge(&existing, &query, &HashSet::new(), &HashSet::new(), found);
        assert_eq!(merged[1].name, "Same (28)");
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
        SiEvent::Sdt(crate::si::SdtInfo {
            original_network_id,
            transport_stream_id: tsid,
            services: vec![(service_id, 0x01, "Service".to_string())],
            is_actual: true,
            version_number: 0,
            current_next: true,
            section_number: 0,
            last_section_number: 0,
        })
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
    fn the_startup_retry_backoff_doubles_and_caps() {
        assert_eq!(retry_delay(0), Duration::from_secs(30));
        assert_eq!(retry_delay(1), Duration::from_secs(60));
        assert_eq!(retry_delay(2), Duration::from_secs(120));
        assert_eq!(retry_delay(3), Duration::from_secs(240));
        assert_eq!(retry_delay(4), STARTUP_RETRY_MAX);
        assert_eq!(retry_delay(50), STARTUP_RETRY_MAX);
    }
}

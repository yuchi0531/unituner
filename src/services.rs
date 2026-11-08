//! In-memory service table (SPEC §5, §8).
//!
//! A scan fills a [`ServiceTable`] from PAT/PMT/SDT/NIT sections; the HTTP API
//! and the built-in TSFilter look services up from it, either by
//! [`ServiceItemId`] (`nid * 100000 + sid`) or by a bare service id.
//!
//! # Item id
//!
//! SPEC §5 defines the item id as `nid * 1e5 + sid`. Service ids are 16-bit and
//! network ids are 16-bit, so the sum never overflows `u64` and stays injective
//! as long as `sid < 100000` — always true for a 16-bit service id.
//!
//! # Storage key
//!
//! The same `(nid, sid)` can be received on two physical channels at once: a
//! relay carries the same service, and the same multiplex can be scanned on a
//! `GR` and a `CABLE_CATV` input. Records are therefore keyed by
//! `(item_id, channel_type, channel)`, not by the item id alone. The item id
//! stays the value the API exposes as `id`; the channel is context.
//!
//! Records without a channel binding (a scan that has not yet attached one) use
//! `None` for both channel fields and are the fallback for channel-less
//! lookups.
//!
//! # Field updates
//!
//! SI tables arrive in any order, and a PAT carries no network id, an NIT no
//! service list, an SDT no PMT PID. [`upsert`](ServiceTable::upsert) therefore
//! *merges*: fields the incoming record leaves unset (`None`, or an empty name)
//! keep the value already stored, so SDT-before-NIT and NIT-before-PAT both end
//! up complete. [`insert`](ServiceTable::insert) is the explicit full replace,
//! and [`modify`](ServiceTable::modify) plus the per-field setters update one
//! attribute under a single write lock.
//!
//! # Lookup by bare service id
//!
//! SPEC §5: an exact [`ServiceItemId`] match always wins. A bare sid resolves
//! only when exactly one record carries it; with matches on two channels (or in
//! two networks) the result is [`SidLookupError::Ambiguous`] (which the API
//! maps to 404). Scanning is channel-scoped, so the scan path should use
//! [`resolve_on_channel`](ServiceTable::resolve_on_channel) instead.
//!
//! # Persistent cache (`services.json`, SPEC §8.2)
//!
//! The table can be snapshotted to `config_dir/services.json` and restored at
//! startup, so the API serves the last scan's services without tuning anything.
//! The file carries the [`SERVICE_CACHE_VERSION`], a fingerprint of the enabled
//! `channels.yml` entries (see [`service_fingerprint`]) and an RFC3339 UTC
//! `generated_at` stamp.
//!
//! The cache has **no expiry**: it stays valid until the fingerprint changes
//! (a channel was added, removed, disabled or had its routes / extra-args
//! edited) or the operator presses the WebUI Re-scan button. A broadcast
//! reorganisation that only changes service names / ids therefore keeps the old
//! cache until the manual re-scan.
//!
//! Writes mirror the other configuration writes: `services.json.tmp` →
//! `fsync` → `rename`, with mode `0600` on Unix (an existing mode is preserved).
//! A failed write is a warning only; the in-memory table stays authoritative.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::config::ChannelConfig;

/// The file name of the persistent service cache (SPEC §8.2).
pub const SERVICE_CACHE_FILE: &str = "services.json";

/// Schema version written into `services.json`.
pub const SERVICE_CACHE_VERSION: u32 = 1;

/// Prefix of the canonical string hashed by [`service_fingerprint`].
///
/// Bumping this (or changing the canonical encoding) deliberately invalidates
/// every existing cache.
const FINGERPRINT_SCHEMA: &str = "unituner-services-v1";

/// Identifier of a service: `network_id * 100_000 + service_id`.
///
/// `ServiceItemId` is the value the API exposes as `id`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ServiceItemId(pub u64);

impl ServiceItemId {
    /// Builds an item id from a network id and a service id (SPEC §5).
    ///
    /// ```
    /// # use unituner::services::ServiceItemId;
    /// assert_eq!(ServiceItemId::new(0x7FE0, 0x0408).0, 0x7FE0 * 100_000 + 0x0408);
    /// ```
    pub fn new(network_id: u16, service_id: u16) -> Self {
        Self(u64::from(network_id) * 100_000 + u64::from(service_id))
    }

    /// The raw numeric value.
    pub fn value(self) -> u64 {
        self.0
    }

    /// The `(network_id, service_id)` pair this id encodes.
    ///
    /// The inverse of [`new`](Self::new) for every value it produced.
    pub fn parts(self) -> (u16, u16) {
        let network_id = self.0 / 100_000;
        let service_id = self.0 % 100_000;
        (network_id as u16, service_id as u16)
    }
}

impl std::fmt::Display for ServiceItemId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl From<ServiceItemId> for u64 {
    fn from(id: ServiceItemId) -> Self {
        id.0
    }
}

/// Why a service id could not be resolved to one service.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SidLookupError {
    /// No service in the table has this service id.
    #[error("service id {0} not found")]
    NotFound(u16),
    /// More than one service carries this service id: another network or
    /// another channel (SPEC §5).
    #[error("service id {0} is ambiguous")]
    Ambiguous(u16),
    /// The id text is not a decimal number, is negative, or is too large to be
    /// either an item id or a service id.
    ///
    /// Kept distinct from [`NotFound`](Self::NotFound) so the API can answer
    /// 404 while logging what the client actually sent instead of a misleading
    /// `service id 0 not found`.
    #[error("malformed service id {0:?}")]
    Malformed(String),
}

/// One service known to the server.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceRecord {
    /// Network id the service belongs to.
    pub network_id: u16,
    /// `transport_stream_id` of the multiplex the service was found in.
    pub tsid: u16,
    /// Service id within that multiplex.
    pub service_id: u16,
    /// Service type from the SDT service descriptor (SPEC §8.1 lists the
    /// values: `0x01`, `0x02`, `0xA1`, `0xA4`, `0xA5`, `0xAD`, `0xC0`).
    pub service_type: u8,
    /// ARIB-decoded service name.
    pub name: String,
    /// Remote-control key id from the NIT TS information descriptor, if known.
    #[serde(default)]
    pub remote_control_key_id: Option<u8>,
    /// `(type, channel)` this service was found on, once the scan knows it.
    #[serde(default)]
    pub channel: Option<(String, String)>,
    /// PID of the service's PMT, once a PAT has been seen.
    #[serde(default)]
    pub pmt_pid: Option<u16>,
    /// Whether logo (CDT) data is available. Always `false`: SPEC §1 excludes
    /// logo handling, and §9 reports `hasLogoData: false`.
    #[serde(default)]
    pub has_logo_data: bool,
}

impl ServiceRecord {
    /// Builds a record with no channel binding, no PMT PID and no logo.
    pub fn new(
        network_id: u16,
        tsid: u16,
        service_id: u16,
        service_type: u8,
        name: impl Into<String>,
    ) -> Self {
        Self {
            network_id,
            tsid,
            service_id,
            service_type,
            name: name.into(),
            remote_control_key_id: None,
            channel: None,
            pmt_pid: None,
            has_logo_data: false,
        }
    }

    /// The item id of this record.
    pub fn item_id(&self) -> ServiceItemId {
        ServiceItemId::new(self.network_id, self.service_id)
    }

    /// Merges `newer` into `self`, keeping every field `newer` leaves unset.
    ///
    /// A field is unset when it is `None`, an empty name, or zero `tsid` /
    /// `service_type` (0 is not a valid TSID nor an ARIB service type, so both
    /// mean "not known yet"). Network id and service id identify the record and
    /// are always taken from `newer`.
    fn merge(&mut self, newer: &ServiceRecord) {
        self.network_id = newer.network_id;
        self.service_id = newer.service_id;
        if newer.tsid != 0 {
            self.tsid = newer.tsid;
        }
        if newer.service_type != 0 {
            self.service_type = newer.service_type;
        }
        if !newer.name.is_empty() {
            self.name.clone_from(&newer.name);
        }
        if newer.remote_control_key_id.is_some() {
            self.remote_control_key_id = newer.remote_control_key_id;
        }
        if newer.pmt_pid.is_some() {
            self.pmt_pid = newer.pmt_pid;
        }
        if newer.channel.is_some() {
            self.channel.clone_from(&newer.channel);
        }
        self.has_logo_data = newer.has_logo_data;
    }

    /// Key used for the stable list order: network, then transport stream,
    /// then service id, then channel. Channels sort after unbound records.
    fn order_key(&self) -> (u16, u16, u16, Option<(&str, &str)>) {
        (
            self.network_id,
            self.tsid,
            self.service_id,
            self.channel
                .as_ref()
                .map(|(channel_type, channel)| (channel_type.as_str(), channel.as_str())),
        )
    }
}

/// Storage key: item id plus the physical channel the record was received on.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ServiceKey {
    item_id: ServiceItemId,
    channel_type: Option<String>,
    channel: Option<String>,
}

impl ServiceKey {
    fn of(record: &ServiceRecord) -> Self {
        let (channel_type, channel) = match &record.channel {
            Some((channel_type, channel)) => (Some(channel_type.clone()), Some(channel.clone())),
            None => (None, None),
        };
        Self {
            item_id: record.item_id(),
            channel_type,
            channel,
        }
    }

    fn for_channel(item_id: ServiceItemId, channel_type: &str, channel: &str) -> Self {
        Self {
            item_id,
            channel_type: Some(channel_type.to_owned()),
            channel: Some(channel.to_owned()),
        }
    }

    fn is_on(&self, channel_type: &str, channel: &str) -> bool {
        self.channel_type.as_deref() == Some(channel_type)
            && self.channel.as_deref() == Some(channel)
    }
}

/// The services known to the server.
///
/// Cloning shares the same table; every method is safe to call concurrently.
#[derive(Clone, Debug, Default)]
pub struct ServiceTable {
    records: Arc<RwLock<HashMap<ServiceKey, ServiceRecord>>>,
}

impl ServiceTable {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or *merges* the record for its item id and channel.
    ///
    /// Fields the incoming record leaves unset (`None` remote-control key /
    /// PMT PID / channel, empty name, zero type or TSID) keep the value already
    /// stored. That makes the order SDT, NIT, PAT irrelevant: no update wipes
    /// a field another table supplied. Use [`insert`](Self::insert) to replace
    /// a record wholesale.
    ///
    /// When the incoming record is bound to a channel and only an *unbound*
    /// record for the same item id exists, that record is merged and moved onto
    /// the channel rather than left behind as a second, channel-less copy.
    pub fn upsert(&self, record: ServiceRecord) {
        let key = ServiceKey::of(&record);
        let mut records = self.write();
        if let Some(stored) = records.get_mut(&key) {
            stored.merge(&record);
            return;
        }
        if record.channel.is_some() {
            // Binding a previously unbound record: move it onto the channel
            // rather than keeping a channel-less twin beside it.
            let unbound = ServiceKey {
                item_id: record.item_id(),
                channel_type: None,
                channel: None,
            };
            if let Some(mut stored) = records.remove(&unbound) {
                stored.merge(&record);
                stored.channel.clone_from(&record.channel);
                records.insert(key, stored);
                return;
            }
        }
        records.insert(key, record);
    }

    /// Inserts or replaces the record for its item id and channel, dropping
    /// every field of the previous record.
    pub fn insert(&self, record: ServiceRecord) {
        let key = ServiceKey::of(&record);
        self.write().insert(key, record);
    }

    /// Applies `f` to the record with `item_id`, under one write lock.
    ///
    /// With several channels carrying the same item id the first record in
    /// stable list order is used; [`modify_all`](Self::modify_all) visits every
    /// one of them. Returns whether a record was found.
    pub fn modify<F: FnOnce(&mut ServiceRecord)>(&self, item_id: ServiceItemId, f: F) -> bool {
        let mut records = self.write();
        // First record in list order (order_key, same as get()/list()),
        // found in one pass without allocating/sorting a key Vec.
        let best_key = records
            .iter()
            .filter(|(_, record)| record.item_id() == item_id)
            .min_by(|(_, l), (_, r)| l.order_key().cmp(&r.order_key()))
            .map(|(key, _)| key.clone());
        let Some(key) = best_key else {
            return false;
        };
        match records.get_mut(&key) {
            Some(record) => {
                f(record);
                true
            }
            None => false,
        }
    }

    /// Applies `f` to every record with `item_id`, under one write lock.
    ///
    /// Returns whether at least one record was found.
    pub fn modify_all<F: FnMut(&mut ServiceRecord)>(
        &self,
        item_id: ServiceItemId,
        mut f: F,
    ) -> bool {
        let mut records = self.write();
        let mut found = false;
        for record in records.values_mut() {
            if record.item_id() == item_id {
                f(record);
                found = true;
            }
        }
        found
    }

    /// Applies `f` to the record with `item_id` on `(channel_type, channel)`.
    ///
    /// Returns whether the record exists.
    pub fn modify_on_channel<F: FnOnce(&mut ServiceRecord)>(
        &self,
        item_id: ServiceItemId,
        channel_type: &str,
        channel: &str,
        f: F,
    ) -> bool {
        let key = ServiceKey::for_channel(item_id, channel_type, channel);
        let mut records = self.write();
        match records.get_mut(&key) {
            Some(record) => {
                f(record);
                true
            }
            None => false,
        }
    }

    /// Sets the remote-control key id of every record with `item_id`.
    pub fn set_remote_control_key_id(&self, item_id: ServiceItemId, key_id: u8) -> bool {
        self.modify_all(item_id, |record| {
            record.remote_control_key_id = Some(key_id);
        })
    }

    /// Sets the transport stream id of every record with `item_id`.
    pub fn set_tsid(&self, item_id: ServiceItemId, tsid: u16) -> bool {
        self.modify_all(item_id, |record| record.tsid = tsid)
    }

    /// Sets the service type of every record with `item_id`.
    pub fn set_service_type(&self, item_id: ServiceItemId, service_type: u8) -> bool {
        self.modify_all(item_id, |record| record.service_type = service_type)
    }

    /// Sets the name of every record with `item_id`.
    pub fn set_name(&self, item_id: ServiceItemId, name: impl Into<String>) -> bool {
        let name = name.into();
        self.modify_all(item_id, |record| record.name.clone_from(&name))
    }

    /// Sets the `(type, channel)` binding of a known service.
    ///
    /// Binding an unbound record moves its storage key; if a record already
    /// exists for the target channel, the two are merged. Does nothing when the
    /// item id is unknown; returns whether it was set.
    pub fn set_channel(&self, item_id: ServiceItemId, channel_type: &str, channel: &str) -> bool {
        let mut records = self.write();
        let target = ServiceKey::for_channel(item_id, channel_type, channel);
        let mut sources: Vec<ServiceKey> = records
            .keys()
            .filter(|key| key.item_id == item_id && **key != target)
            .cloned()
            .collect();
        if sources.is_empty() {
            let Some(record) = records.get_mut(&target) else {
                return false;
            };
            record.channel = Some((channel_type.to_owned(), channel.to_owned()));
            return true;
        }
        sources.sort();
        for source in sources {
            let Some(mut record) = records.remove(&source) else {
                continue;
            };
            record.channel = Some((channel_type.to_owned(), channel.to_owned()));
            match records.get_mut(&target) {
                Some(existing) => existing.merge(&record),
                None => {
                    records.insert(target.clone(), record);
                }
            }
        }
        true
    }

    /// Sets the PMT PID of a known service. Returns whether it was set.
    pub fn set_pmt_pid(&self, item_id: ServiceItemId, pmt_pid: u16) -> bool {
        self.modify_all(item_id, |record| record.pmt_pid = Some(pmt_pid))
    }

    /// Every record, ordered by network, transport stream, service id and
    /// channel.
    ///
    /// The order is stable regardless of insertion order, so API responses do
    /// not shuffle between calls.
    pub fn list(&self) -> Vec<ServiceRecord> {
        let mut records: Vec<ServiceRecord> = self.read().values().cloned().collect();
        records.sort_by(|left, right| left.order_key().cmp(&right.order_key()));
        records
    }

    /// A stable snapshot of the table, used to persist it (`services.json`).
    ///
    /// Same order as [`list`](Self::list), so two snapshots of an unchanged
    /// table compare equal and the cache writer can tell whether a retry pass
    /// actually changed anything.
    pub fn snapshot(&self) -> Vec<ServiceRecord> {
        self.list()
    }

    /// Replaces the whole table with `records` (restoring `services.json`).
    ///
    /// Records are keyed exactly as [`upsert`](Self::upsert) keys them. A
    /// duplicate key keeps the last record, which can only happen with a
    /// hand-edited cache.
    pub fn restore(&self, records: Vec<ServiceRecord>) {
        let mut stored = self.write();
        stored.clear();
        for record in records {
            stored.insert(ServiceKey::of(&record), record);
        }
    }

    /// Every record received on `(channel_type, channel)`, in list order.
    pub fn list_by_channel(&self, channel_type: &str, channel: &str) -> Vec<ServiceRecord> {
        let mut records: Vec<ServiceRecord> = self
            .read()
            .iter()
            .filter(|(key, _)| key.is_on(channel_type, channel))
            .map(|(_, record)| record.clone())
            .collect();
        records.sort_by(|left, right| left.order_key().cmp(&right.order_key()));
        records
    }

    /// Look a service up by item id.
    ///
    /// When several channels carry the same item id the first in list order is
    /// returned, so the result is deterministic; use
    /// [`get_on_channel`](Self::get_on_channel) or
    /// [`get_all`](Self::get_all) when that matters.
    pub fn get(&self, item_id: ServiceItemId) -> Option<ServiceRecord> {
        // One read lock, one pass, clone only the winner (previously
        // get_all cloned+sorted every match). Winner is first in list order.
        self.read()
            .values()
            .filter(|record| record.item_id() == item_id)
            .min_by(|l, r| l.order_key().cmp(&r.order_key()))
            .cloned()
    }

    /// Every record carrying `item_id`, across channels, in list order.
    pub fn get_all(&self, item_id: ServiceItemId) -> Vec<ServiceRecord> {
        let mut records: Vec<ServiceRecord> = self
            .read()
            .iter()
            .filter(|(key, _)| key.item_id == item_id)
            .map(|(_, record)| record.clone())
            .collect();
        records.sort_by(|left, right| left.order_key().cmp(&right.order_key()));
        records
    }

    /// The record for `item_id` on one physical channel.
    pub fn get_on_channel(
        &self,
        channel_type: &str,
        channel: &str,
        item_id: ServiceItemId,
    ) -> Option<ServiceRecord> {
        self.read()
            .get(&ServiceKey::for_channel(item_id, channel_type, channel))
            .cloned()
    }

    /// Looks a service up by a bare service id (SPEC §5).
    ///
    /// Returns the only match, [`SidLookupError::NotFound`] when there is none,
    /// and [`SidLookupError::Ambiguous`] when several records carry the same
    /// service id — whether they differ by network or by channel. Callers that
    /// have a [`ServiceItemId`] should use [`get`](Self::get); callers with a
    /// channel should use
    /// [`resolve_on_channel`](Self::resolve_on_channel).
    pub fn get_by_sid(&self, sid: u16) -> Result<ServiceRecord, SidLookupError> {
        let records = self.read();
        let mut matches = records.values().filter(|record| record.service_id == sid);
        match (matches.next(), matches.next()) {
            (None, _) => Err(SidLookupError::NotFound(sid)),
            (Some(record), None) => Ok(record.clone()),
            (Some(_), Some(_)) => Err(SidLookupError::Ambiguous(sid)),
        }
    }

    /// Resolves a `{id}` path segment, preferring an exact item id (SPEC §5).
    ///
    /// `raw` is the decimal text from the URL. It is first read as a
    /// [`ServiceItemId`]; if no such service exists it is read as a bare
    /// service id. Text that is not a decimal number, is negative, or exceeds
    /// both a `u64` and a `u16` yields [`SidLookupError::Malformed`].
    pub fn resolve(&self, raw: &str) -> Result<ServiceRecord, SidLookupError> {
        let trimmed = raw.trim();
        if let Ok(value) = trimmed.parse::<u64>() {
            if let Some(record) = self.get(ServiceItemId(value)) {
                return Ok(record);
            }
        }
        match trimmed.parse::<u16>() {
            Ok(sid) => self.get_by_sid(sid),
            Err(_) => Err(SidLookupError::Malformed(raw.to_owned())),
        }
    }

    /// Resolves a bare service id on one physical channel.
    ///
    /// This is the unambiguous lookup for a scan or a channel-scoped stream:
    /// the channel is part of the key, so a service carried on two channels no
    /// longer becomes [`SidLookupError::Ambiguous`]. Ambiguity remains possible
    /// when one channel carries the same sid in two networks.
    pub fn resolve_on_channel(
        &self,
        channel_type: &str,
        channel: &str,
        sid: u16,
    ) -> Result<ServiceRecord, SidLookupError> {
        let records = self.read();
        let mut matches = records
            .iter()
            .filter(|(key, record)| key.is_on(channel_type, channel) && record.service_id == sid)
            .map(|(_, record)| record);
        match (matches.next(), matches.next()) {
            (None, _) => Err(SidLookupError::NotFound(sid)),
            (Some(record), None) => Ok(record.clone()),
            (Some(_), Some(_)) => Err(SidLookupError::Ambiguous(sid)),
        }
    }

    /// Resolves a `{id}` path segment on one physical channel, accepting both
    /// an item id and a bare service id.
    pub fn resolve_raw_on_channel(
        &self,
        channel_type: &str,
        channel: &str,
        raw: &str,
    ) -> Result<ServiceRecord, SidLookupError> {
        let trimmed = raw.trim();
        if let Ok(value) = trimmed.parse::<u64>() {
            if let Some(record) = self.get_on_channel(channel_type, channel, ServiceItemId(value)) {
                return Ok(record);
            }
        }
        match trimmed.parse::<u16>() {
            Ok(sid) => self.resolve_on_channel(channel_type, channel, sid),
            Err(_) => Err(SidLookupError::Malformed(raw.to_owned())),
        }
    }

    /// Number of services in the table.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the table holds no service.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Removes every record received on `(channel_type, channel)`.
    ///
    /// This is the refresh half of a re-scan: drop a channel's stale services,
    /// then repopulate them as the new SI arrives. Returns how many records
    /// were removed.
    ///
    /// Prefer [`retain_channel`](Self::retain_channel) when the new SI has
    /// already been merged in: it does the same thing without dropping the
    /// services the new scan just found.
    pub fn remove_channel(&self, channel_type: &str, channel: &str) -> usize {
        let mut records = self.write();
        let before = records.len();
        records.retain(|key, _| !key.is_on(channel_type, channel));
        before - records.len()
    }

    /// Drops the records for `(channel_type, channel)` whose service id is not
    /// in `keep`.
    ///
    /// This is what a refresh scan must call: after the new SI has been written
    /// with [`upsert`](Self::upsert), the services that disappeared from the
    /// multiplex are exactly those still stored on that channel whose sid is
    /// not among the sids the scan just found. Unlike
    /// [`remove_channel`](Self::remove_channel) it keeps the fresh records, so
    /// there is no window where `/api/services/{id}/stream` answers 404 for a
    /// service that is still on air.
    ///
    /// **Scan layer call site** (the scan owner must add it, `scan.rs`):
    ///
    /// ```text
    /// // in update_service_table(), after the upsert loop:
    /// let keep: HashSet<u16> = found.iter().map(|s| s.service_id).collect();
    /// if mode == ScanMode::Channel {
    ///     services.retain_channel(type_name, &entries[0].channel, &keep);
    /// } else {
    ///     for entry in entries {
    ///         services.retain_channel(type_name, &entry.channel, &keep);
    ///     }
    /// }
    /// ```
    ///
    /// (Only call it when the scan produced at least one service for the
    /// channel: a no-signal result must not wipe the table.) Returns how many
    /// records were removed.
    pub fn retain_channel(&self, channel_type: &str, channel: &str, keep: &HashSet<u16>) -> usize {
        let mut records = self.write();
        let before = records.len();
        records.retain(|key, record| {
            !key.is_on(channel_type, channel) || keep.contains(&record.service_id)
        });
        before - records.len()
    }

    /// Removes records that are not bound to any channel (`channel == None`).
    ///
    /// Records are created unbound while the SI is being parsed and are moved
    /// onto a channel by [`upsert`](Self::upsert) /
    /// [`set_channel`](Self::set_channel). A record whose service never appears
    /// on a scanned channel (SDT-other, a stale NIT, a channel that failed
    /// mid-scan) would otherwise stay in the table for ever and keep showing up
    /// in `/api/services`. The scan layer should call this once after a
    /// completed, non-dry-run scan; the API keeps serving channel-less records
    /// in the meantime. Returns how many records were removed.
    pub fn prune_unbound(&self) -> usize {
        let mut records = self.write();
        let before = records.len();
        records.retain(|key, _| key.channel_type.is_some());
        before - records.len()
    }

    /// Removes every service.
    pub fn clear(&self) {
        self.write().clear();
    }

    /// Keeps only records bound to one of `channels` (`(type, channel)`).
    ///
    /// This is the channel-level retain step of a full re-scan: a channel that
    /// was removed from `channels.yml` or set `disabled: true` loses its
    /// records, while an enabled channel whose tuner was busy or had no signal
    /// keeps the records the previous scan stored (it is still in `keep`).
    /// Unbound records (no channel binding) are dropped as well. Returns how
    /// many records were removed.
    pub fn retain_bound_channels(&self, keep: &HashSet<(String, String)>) -> usize {
        let mut records = self.write();
        let before = records.len();
        records.retain(|_, record| match &record.channel {
            Some((channel_type, channel)) => {
                keep.contains(&(channel_type.clone(), channel.clone()))
            }
            None => false,
        });
        before - records.len()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<ServiceKey, ServiceRecord>> {
        self.records
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<ServiceKey, ServiceRecord>> {
        self.records
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The persistent service table (`services.json`, SPEC §8.2).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ServiceCache {
    /// Must be [`SERVICE_CACHE_VERSION`]; an unknown version is ignored.
    pub version: u32,
    /// Fingerprint of the enabled `channels.yml` entries.
    pub fingerprint: String,
    /// RFC3339 UTC time the snapshot was written.
    pub generated_at: String,
    /// The services, in [`ServiceTable::snapshot`] order.
    pub services: Vec<ServiceRecord>,
}

/// Path of `services.json` inside `config_dir`.
pub fn service_cache_path(config_dir: &Path) -> PathBuf {
    config_dir.join(SERVICE_CACHE_FILE)
}

/// Reads and parses `services.json`.
///
/// `Ok(None)` means the file does not exist (the normal first boot); an
/// unreadable or malformed file is an `Err` for the caller to warn about and
/// treat as "no cache". The schema version and fingerprint are checked by the
/// caller because only it knows which failure should trigger a scan.
pub fn read_service_cache(config_dir: &Path) -> Result<Option<ServiceCache>, String> {
    let path = service_cache_path(config_dir);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("failed to read {}: {error}", path.display())),
    };
    let cache = serde_json::from_str(&text)
        .map_err(|error| format!("failed to parse {}: {error}", path.display()))?;
    Ok(Some(cache))
}

/// Loads the cache into a [`ServiceTable`] for startup.
///
/// A missing or corrupt cache only warns (a missing file is silent at info
/// level) and yields an empty table; startup must never be aborted by a cache.
pub fn load_service_cache(config_dir: &Path) -> ServiceTable {
    let table = ServiceTable::new();
    match read_service_cache(config_dir) {
        Ok(Some(cache)) if cache.version == SERVICE_CACHE_VERSION => {
            tracing::info!(
                services = cache.services.len(),
                generated_at = %cache.generated_at,
                "restored services.json"
            );
            table.restore(cache.services);
        }
        Ok(Some(cache)) => tracing::warn!(
            version = cache.version,
            expected = SERVICE_CACHE_VERSION,
            "ignoring services.json with an unsupported version"
        ),
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "ignoring an unreadable services.json"),
    }
    table
}

/// FNV-1a 64 over the canonical encoding of the enabled channels.
///
/// `DefaultHasher` / `RandomState` are explicitly not used: their output is not
/// guaranteed stable across Rust versions, and the fingerprint is persisted.
///
/// The canonical input is (in order):
///
/// * the schema constant [`FINGERPRINT_SCHEMA`],
/// * every **enabled** entry (disabled entries are excluded), sorted by
///   `(type, channel)`, contributing `type`, `channel`, `extra-args`, the route
///   count and then each route's `tuner`, optional `channel` and optional
///   `extra-args`.
///
/// Every string is length-prefixed, so a field containing the encoding's
/// separators cannot collide with another field. `tuners.yml` is deliberately
/// not part of the fingerprint: a channel's services belong to the broadcast,
/// not to the tuner definition that receives it.
pub fn service_fingerprint(channels: &[ChannelConfig]) -> String {
    let mut canonical = Vec::new();
    push_fingerprint_field(&mut canonical, FINGERPRINT_SCHEMA);

    let mut enabled: Vec<&ChannelConfig> = channels
        .iter()
        .filter(|channel| !channel.disabled)
        .collect();
    enabled.sort_by(|left, right| {
        (left.channel_type.as_str(), left.channel.as_str())
            .cmp(&(right.channel_type.as_str(), right.channel.as_str()))
    });

    for channel in enabled {
        push_fingerprint_field(&mut canonical, &channel.channel_type);
        push_fingerprint_field(&mut canonical, &channel.channel);
        push_fingerprint_field(&mut canonical, &channel.extra_args);
        canonical.extend_from_slice(&(channel.routes.len() as u64).to_be_bytes());
        for route in &channel.routes {
            push_fingerprint_field(&mut canonical, &route.tuner);
            push_fingerprint_optional(&mut canonical, route.channel.as_deref());
            push_fingerprint_optional(&mut canonical, route.extra_args.as_deref());
        }
    }

    format!("{:016x}", fnv1a(&canonical))
}

fn push_fingerprint_field(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&(value.len() as u64).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn push_fingerprint_optional(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            push_fingerprint_field(out, value);
        }
        None => out.push(0),
    }
}

/// FNV-1a 64 (the same primitive the webui uses for its ETag).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Serialises one lock for the cache writer. The scan lock only serialises
/// scans; a startup-scan write and a manual re-scan write can still overlap, so
/// they must not share the fixed `services.json.tmp` path unprotected.
static CACHE_WRITE_LOCK: Mutex<()> = Mutex::new(());

/// Writes the current table to `services.json` atomically.
///
/// Called only after a completed scan pass; a failure is a warning for the
/// caller and leaves the previous file untouched. The table itself is never
/// changed by a failed write.
pub fn write_service_cache(
    config_dir: &Path,
    fingerprint: &str,
    table: &ServiceTable,
) -> Result<(), String> {
    let _guard = CACHE_WRITE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let cache = ServiceCache {
        version: SERVICE_CACHE_VERSION,
        fingerprint: fingerprint.to_owned(),
        generated_at: generated_at(),
        services: table.snapshot(),
    };
    let json = serde_json::to_vec(&cache)
        .map_err(|error| format!("failed to serialize {SERVICE_CACHE_FILE}: {error}"))?;
    let path = service_cache_path(config_dir);
    let tmp = config_dir.join(format!("{SERVICE_CACHE_FILE}.tmp"));
    write_cache_atomically(&path, &tmp, &json)
}

/// The atomic replacement: create/truncate `tmp` with `0600`, fsync, rename.
///
/// Mirrors the configuration writes (`store::write_atomic` / the scan's
/// `write_atomically`): an existing `services.json` keeps its mode across the
/// rename, and a failed write removes the temporary file.
fn write_cache_atomically(path: &Path, tmp: &Path, bytes: &[u8]) -> Result<(), String> {
    #[cfg(unix)]
    let original_mode = path.metadata().ok().map(|metadata| {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode()
    });

    let result = (|| -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // A leftover tmp from a crashed write may already exist with another
            // mode; apply the desired mode to the handle (also ignores umask).
            file.set_permissions(std::fs::Permissions::from_mode(
                original_mode.unwrap_or(0o600),
            ))?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(tmp, path)?;
        // Best effort: persist the rename itself.
        if let Some(dir) = path.parent() {
            if let Ok(handle) = std::fs::File::open(dir) {
                let _ = handle.sync_all();
            }
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    result.map_err(|error| format!("failed to write {}: {error}", path.display()))
}

/// RFC3339 UTC with millisecond precision, e.g. `2026-11-09T12:34:56.789Z`.
///
/// `std::time` only gives an epoch offset; the civil-date conversion is the
/// standard days-from-civil algorithm, written out rather than adding a
/// time-formatting dependency for one field.
fn generated_at() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = elapsed.as_secs() as i64;
    let millis = elapsed.subsec_millis();

    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Converts days since 1970-01-01 into a proleptic Gregorian `(year, month,
/// day)`. (Howard Hinnant's `civil_from_days`.)
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(network_id: u16, tsid: u16, service_id: u16, name: &str) -> ServiceRecord {
        ServiceRecord::new(network_id, tsid, service_id, 0x01, name)
    }

    fn on_channel(
        network_id: u16,
        tsid: u16,
        service_id: u16,
        name: &str,
        channel_type: &str,
        channel: &str,
    ) -> ServiceRecord {
        let mut record = record(network_id, tsid, service_id, name);
        record.channel = Some((channel_type.to_owned(), channel.to_owned()));
        record
    }

    #[test]
    fn item_id_encodes_network_and_service() {
        let id = ServiceItemId::new(0x7FE0, 0x0408);
        assert_eq!(id.0, 0x7FE0 * 100_000 + 0x0408);
        assert_eq!(id.parts(), (0x7FE0, 0x0408));
        assert_eq!(id.value(), id.0);
        assert_eq!(u64::from(id), id.0);
        assert_eq!(id.to_string(), id.0.to_string());
    }

    #[test]
    fn item_id_round_trips_across_the_whole_range() {
        for network_id in [0u16, 1, 0x7FE0, u16::MAX] {
            for service_id in [0u16, 1, 0x0408, 0x7FFF, u16::MAX] {
                let id = ServiceItemId::new(network_id, service_id);
                assert_eq!(id.parts(), (network_id, service_id));
            }
        }
    }

    #[test]
    fn upsert_then_list_is_ordered_by_network_tsid_and_service() {
        let table = ServiceTable::new();
        table.upsert(record(2, 20, 30, "third"));
        table.upsert(record(1, 10, 50, "first"));
        table.upsert(record(1, 10, 10, "second"));
        table.upsert(record(1, 5, 99, "zeroth"));

        let names: Vec<String> = table.list().into_iter().map(|r| r.name).collect();
        assert_eq!(names, ["zeroth", "second", "first", "third"]);
        assert_eq!(table.len(), 4);
        assert!(!table.is_empty());
    }

    #[test]
    fn upsert_replaces_the_same_item_id() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 10, "old"));
        table.upsert(record(1, 10, 10, "new"));

        assert_eq!(table.len(), 1);
        let stored = table.get(ServiceItemId::new(1, 10)).expect("record");
        assert_eq!(stored.name, "new");
    }

    #[test]
    fn insert_replaces_but_upsert_merges() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        let mut full = record(1, 10, 10, "full");
        full.pmt_pid = Some(0x1F0);
        full.remote_control_key_id = Some(3);
        table.insert(full.clone());
        assert_eq!(table.get(id).expect("record"), full);

        // An upsert with the same identity but no optional fields keeps them.
        table.upsert(record(1, 10, 10, ""));
        let stored = table.get(id).expect("record");
        assert_eq!(stored.name, "full");
        assert_eq!(stored.pmt_pid, Some(0x1F0));
        assert_eq!(stored.remote_control_key_id, Some(3));

        // `insert` is the explicit clobber.
        table.insert(record(1, 10, 10, "replaced"));
        let stored = table.get(id).expect("record");
        assert_eq!(stored.name, "replaced");
        assert_eq!(stored.pmt_pid, None);
        assert_eq!(stored.remote_control_key_id, None);
    }

    #[test]
    fn sdt_before_nit_keeps_the_remote_control_key_id() {
        // The SDT creates the record; the NIT arrives afterwards and only knows
        // the key id. A whole-record replace would wipe the name and PMT PID.
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        let mut service = record(1, 10, 10, "svc");
        service.pmt_pid = Some(0x1F0);
        table.upsert(service);

        let mut nit = record(1, 10, 10, "");
        nit.service_type = 0;
        nit.remote_control_key_id = Some(5);
        table.upsert(nit);

        let stored = table.get(id).expect("record");
        assert_eq!(stored.name, "svc");
        assert_eq!(stored.pmt_pid, Some(0x1F0));
        assert_eq!(stored.remote_control_key_id, Some(5));
    }

    #[test]
    fn nit_before_pat_keeps_the_key_id_when_the_pat_arrives() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        let mut nit = record(1, 10, 10, "");
        nit.remote_control_key_id = Some(7);
        table.upsert(nit);
        table.set_pmt_pid(id, 0x1F1);
        // A later SDT supplies only the name and type.
        table.upsert(record(1, 10, 10, "named"));

        let stored = table.get(id).expect("record");
        assert_eq!(stored.name, "named");
        assert_eq!(stored.pmt_pid, Some(0x1F1));
        assert_eq!(stored.remote_control_key_id, Some(7));
    }

    #[test]
    fn the_two_channels_do_not_evict_each_other() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(0x7FE0, 0x0408);
        table.insert(on_channel(0x7FE0, 0x7FE0, 0x0408, "GR copy", "GR", "27"));
        table.insert(on_channel(
            0x7FE0,
            0x7FE0,
            0x0408,
            "CATV copy",
            "CABLE_CATV",
            "C17",
        ));

        assert_eq!(table.len(), 2);
        assert_eq!(table.get_all(id).len(), 2);
        assert_eq!(
            table
                .get_on_channel("GR", "27", id)
                .expect("GR record")
                .name,
            "GR copy"
        );
        assert_eq!(
            table
                .get_on_channel("CABLE_CATV", "C17", id)
                .expect("CATV record")
                .name,
            "CATV copy"
        );
        assert_eq!(table.list_by_channel("GR", "27").len(), 1);
        assert_eq!(table.list_by_channel("CABLE_CATV", "C17").len(), 1);
        assert!(table.list_by_channel("BS", "BS01_0").is_empty());
    }

    #[test]
    fn a_sid_on_two_channels_is_ambiguous_globally_but_resolves_on_channel() {
        let table = ServiceTable::new();
        let sid = 0x0408;
        table.insert(on_channel(0x7FE0, 0x7FE0, sid, "GR copy", "GR", "27"));
        table.insert(on_channel(
            0x7FE0,
            0x7FE0,
            sid,
            "CATV copy",
            "CABLE_CATV",
            "C17",
        ));

        assert_eq!(table.get_by_sid(sid), Err(SidLookupError::Ambiguous(sid)));
        assert_eq!(table.resolve("1032"), Err(SidLookupError::Ambiguous(sid)));
        assert_eq!(
            table
                .resolve_on_channel("GR", "27", sid)
                .expect("GR record")
                .name,
            "GR copy"
        );
        assert_eq!(
            table
                .resolve_raw_on_channel("CABLE_CATV", "C17", "1032")
                .expect("CATV record")
                .name,
            "CATV copy"
        );
        assert_eq!(
            table.resolve_on_channel("BS", "BS01_0", sid),
            Err(SidLookupError::NotFound(sid))
        );
    }

    #[test]
    fn remove_channel_only_drops_that_channel() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(0x7FE0, 0x0408);
        table.insert(on_channel(0x7FE0, 0x7FE0, 0x0408, "GR", "GR", "27"));
        table.insert(on_channel(
            0x7FE0,
            0x7FE0,
            0x0408,
            "CATV",
            "CABLE_CATV",
            "C17",
        ));
        table.insert(record(0x7FE0, 0x7FE0, 0x0409, "unbound"));

        assert_eq!(table.remove_channel("GR", "27"), 1);
        assert_eq!(table.len(), 2);
        assert!(table.get_on_channel("GR", "27", id).is_none());
        assert!(table.get_on_channel("CABLE_CATV", "C17", id).is_some());
        assert_eq!(table.remove_channel("GR", "27"), 0);
    }

    #[test]
    fn retain_channel_drops_only_the_services_the_scan_no_longer_found() {
        let table = ServiceTable::new();
        table.insert(on_channel(1, 10, 27, "kept", "GR", "27"));
        table.insert(on_channel(1, 10, 28, "stale", "GR", "27"));
        table.insert(on_channel(1, 10, 28, "other channel", "GR", "28"));
        table.insert(on_channel(1, 10, 29, "CATV copy", "CABLE_CATV", "C17"));

        let keep: HashSet<u16> = HashSet::from([27]);
        assert_eq!(table.retain_channel("GR", "27", &keep), 1);

        assert_eq!(table.len(), 3);
        assert!(table
            .get_on_channel("GR", "27", ServiceItemId::new(1, 27))
            .is_some());
        assert!(table
            .get_on_channel("GR", "27", ServiceItemId::new(1, 28))
            .is_none());
        // Other channels are untouched.
        assert!(table
            .get_on_channel("GR", "28", ServiceItemId::new(1, 28))
            .is_some());
        assert!(table
            .get_on_channel("CABLE_CATV", "C17", ServiceItemId::new(1, 29))
            .is_some());

        // Idempotent: nothing left to drop.
        assert_eq!(table.retain_channel("GR", "27", &keep), 0);
    }

    #[test]
    fn retain_channel_keeps_fresh_records_without_a_drop_window() {
        // The refresh order the scan uses: merge the new SI first, then prune.
        let table = ServiceTable::new();
        table.insert(on_channel(1, 10, 27, "same sid, old name", "GR", "27"));
        table.upsert(on_channel(1, 10, 27, "same sid, new name", "GR", "27"));
        table.upsert(on_channel(1, 10, 30, "brand new", "GR", "27"));

        let keep: HashSet<u16> = HashSet::from([27, 30]);
        assert_eq!(table.retain_channel("GR", "27", &keep), 0);
        assert_eq!(
            table
                .get_on_channel("GR", "27", ServiceItemId::new(1, 27))
                .expect("record")
                .name,
            "same sid, new name"
        );
        assert!(table
            .get_on_channel("GR", "27", ServiceItemId::new(1, 30))
            .is_some());
    }

    #[test]
    fn prune_unbound_removes_only_channel_less_records() {
        let table = ServiceTable::new();
        table.insert(on_channel(1, 10, 27, "bound", "GR", "27"));
        table.insert(record(1, 10, 40, "unbound one"));
        table.insert(record(2, 20, 41, "unbound two"));

        assert_eq!(table.prune_unbound(), 2);
        assert_eq!(table.len(), 1);
        assert!(table
            .get_on_channel("GR", "27", ServiceItemId::new(1, 27))
            .is_some());
        assert_eq!(table.prune_unbound(), 0);
    }

    #[test]
    fn record_defaults_match_the_spec() {
        let stored = record(0x7FE0, 0x7FE0, 0x0408, "ＮＨＫ総合");
        assert_eq!(stored.channel, None);
        assert_eq!(stored.pmt_pid, None);
        assert!(!stored.has_logo_data);
        assert_eq!(stored.remote_control_key_id, None);
        assert_eq!(stored.item_id(), ServiceItemId::new(0x7FE0, 0x0408));
    }

    #[test]
    fn upserting_a_channel_record_moves_an_unbound_one_instead_of_duplicating() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        let mut unbound = record(1, 10, 10, "svc");
        unbound.pmt_pid = Some(0x1F0);
        table.upsert(unbound);
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(id).expect("record").channel, None);

        // The scan learns the channel: the unbound record is moved, not copied.
        table.upsert(on_channel(1, 10, 10, "", "GR", "27"));
        assert_eq!(table.len(), 1);
        let stored = table.get_on_channel("GR", "27", id).expect("record");
        assert_eq!(stored.name, "svc");
        assert_eq!(stored.pmt_pid, Some(0x1F0));
        assert_eq!(stored.channel, Some(("GR".to_owned(), "27".to_owned())));

        // A second channel always gets its own record.
        table.upsert(on_channel(1, 10, 10, "", "CABLE_CATV", "C17"));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn set_channel_updates_a_known_service_only() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        table.upsert(record(1, 10, 10, "svc"));

        assert!(table.set_channel(id, "GR", "27"));
        assert_eq!(
            table.get(id).expect("record").channel,
            Some(("GR".to_owned(), "27".to_owned()))
        );
        assert!(table.get_on_channel("GR", "27", id).is_some());

        assert!(!table.set_channel(ServiceItemId::new(9, 9), "GR", "27"));
    }

    #[test]
    fn set_channel_merges_into_an_existing_channel_record() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        let mut bound = on_channel(1, 10, 10, "bound", "GR", "27");
        bound.remote_control_key_id = Some(3);
        table.insert(bound);

        // An unbound record for the same service, carrying the PMT PID, is
        // bound to the channel that already has a record.
        let mut unbound = record(1, 10, 10, "");
        unbound.service_type = 0;
        unbound.pmt_pid = Some(0x1F0);
        table.upsert(unbound);
        assert_eq!(table.len(), 2);

        assert!(table.set_channel(id, "GR", "27"));
        assert_eq!(table.len(), 1);
        let stored = table.get_on_channel("GR", "27", id).expect("record");
        assert_eq!(stored.name, "bound");
        assert_eq!(stored.remote_control_key_id, Some(3));
        assert_eq!(stored.pmt_pid, Some(0x1F0));
    }

    #[test]
    fn set_pmt_pid_and_field_setters_update_a_known_service_only() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        table.upsert(record(1, 10, 10, "svc"));

        assert!(table.set_pmt_pid(id, 0x1F0));
        assert!(table.set_remote_control_key_id(id, 4));
        assert!(table.set_tsid(id, 0x0102));
        assert!(table.set_service_type(id, 0xA5));
        assert!(table.set_name(id, "renamed"));

        let stored = table.get(id).expect("record");
        assert_eq!(stored.pmt_pid, Some(0x1F0));
        assert_eq!(stored.remote_control_key_id, Some(4));
        assert_eq!(stored.tsid, 0x0102);
        assert_eq!(stored.service_type, 0xA5);
        assert_eq!(stored.name, "renamed");

        assert!(!table.set_pmt_pid(ServiceItemId::new(9, 9), 0x1F0));
        assert!(!table.set_remote_control_key_id(ServiceItemId::new(9, 9), 1));
        assert!(!table.set_tsid(ServiceItemId::new(9, 9), 1));
        assert!(!table.set_service_type(ServiceItemId::new(9, 9), 1));
        assert!(!table.set_name(ServiceItemId::new(9, 9), "x"));
    }

    #[test]
    fn field_setters_reach_every_channel_copy() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        table.insert(on_channel(1, 10, 10, "GR", "GR", "27"));
        table.insert(on_channel(1, 10, 10, "CATV", "CABLE_CATV", "C17"));

        assert!(table.set_remote_control_key_id(id, 3));
        for record in table.get_all(id) {
            assert_eq!(record.remote_control_key_id, Some(3));
        }
    }

    #[test]
    fn modify_runs_under_one_lock_and_reports_missing_records() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        table.upsert(record(1, 10, 10, "svc"));

        assert!(table.modify(id, |record| {
            record.name = "modified".to_owned();
            record.pmt_pid = Some(0x1F2);
        }));
        let stored = table.get(id).expect("record");
        assert_eq!(stored.name, "modified");
        assert_eq!(stored.pmt_pid, Some(0x1F2));

        assert!(!table.modify(ServiceItemId::new(2, 10), |record| {
            record.name = "never".to_owned()
        }));
    }

    #[test]
    fn get_returns_none_for_an_unknown_item_id() {
        let table = ServiceTable::new();
        assert_eq!(table.get(ServiceItemId::new(1, 1)), None);
        assert!(table.is_empty());
    }

    #[test]
    fn an_exact_item_id_lookup_wins_over_an_ambiguous_sid() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 42, "network one"));
        table.upsert(record(2, 20, 42, "network two"));

        // The sid is ambiguous...
        assert_eq!(table.get_by_sid(42), Err(SidLookupError::Ambiguous(42)));
        // ...but the item id resolves one of them exactly.
        let id = ServiceItemId::new(2, 42);
        assert_eq!(table.get(id).expect("record").name, "network two");
        assert_eq!(
            table.resolve(&id.to_string()).expect("record").name,
            "network two"
        );
    }

    #[test]
    fn the_same_sid_in_two_networks_is_ambiguous() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 7, "first"));
        table.upsert(record(2, 20, 7, "second"));
        assert_eq!(table.get_by_sid(7), Err(SidLookupError::Ambiguous(7)));
        assert_eq!(table.resolve("7"), Err(SidLookupError::Ambiguous(7)));
    }

    #[test]
    fn a_unique_sid_resolves() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 7, "only"));
        table.upsert(record(1, 10, 8, "other"));

        assert_eq!(table.get_by_sid(7).expect("record").name, "only");
        assert_eq!(table.resolve("7").expect("record").name, "only");
    }

    #[test]
    fn an_unknown_sid_is_not_found() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 7, "only"));
        assert_eq!(table.get_by_sid(9), Err(SidLookupError::NotFound(9)));
        assert_eq!(table.resolve("9"), Err(SidLookupError::NotFound(9)));
    }

    #[test]
    fn resolve_falls_back_from_item_id_to_sid() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 7, "only"));

        // 7 is not an item id in the table, so it is read as a bare sid.
        assert_eq!(table.resolve("7").expect("record").name, "only");

        // Now insert the service whose item id is exactly 7 (network 0).
        table.upsert(record(0, 10, 7, "network zero"));
        // The exact item-id match wins over the bare-sid match.
        assert_eq!(table.resolve("7").expect("record").name, "network zero");
    }

    #[test]
    fn resolve_rejects_non_numeric_ids_as_malformed() {
        let table = ServiceTable::new();
        assert_eq!(
            table.resolve("abc"),
            Err(SidLookupError::Malformed("abc".to_owned()))
        );
        assert_eq!(
            table.resolve(""),
            Err(SidLookupError::Malformed(String::new()))
        );
        assert_eq!(
            table.resolve("-1"),
            Err(SidLookupError::Malformed("-1".to_owned()))
        );
        // Too large for both a u64 and a u16.
        assert_eq!(
            table.resolve("99999999999999999999999"),
            Err(SidLookupError::Malformed(
                "99999999999999999999999".to_owned()
            ))
        );
    }

    #[test]
    fn resolve_reports_an_out_of_range_id_as_malformed() {
        // 70_000 parses as a u64 (so it is not a bare u16 sid) but no item id
        // in the table matches it, and it cannot be a service id.
        let table = ServiceTable::new();
        assert_eq!(
            table.resolve("70000"),
            Err(SidLookupError::Malformed("70000".to_owned()))
        );
        assert_eq!(
            table.resolve_raw_on_channel("GR", "27", "70000"),
            Err(SidLookupError::Malformed("70000".to_owned()))
        );
    }

    #[test]
    fn clear_empties_the_table() {
        let table = ServiceTable::new();
        table.upsert(record(1, 10, 7, "only"));
        table.clear();
        assert!(table.is_empty());
        assert!(table.list().is_empty());
    }

    #[test]
    fn clones_share_one_table() {
        let table = ServiceTable::new();
        let clone = table.clone();
        clone.upsert(record(1, 10, 7, "shared"));
        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get(ServiceItemId::new(1, 7)).expect("record").name,
            "shared"
        );
    }

    #[test]
    fn snapshot_and_restore_round_trip_the_table() {
        let table = ServiceTable::new();
        table.insert(on_channel(2, 20, 30, "second", "GR", "27"));
        let mut first = on_channel(1, 10, 10, "first", "GR", "27");
        first.pmt_pid = Some(0x1F0);
        first.remote_control_key_id = Some(3);
        table.insert(first);

        let snapshot = table.snapshot();
        assert_eq!(snapshot, table.list(), "snapshot keeps list order");

        let restored = ServiceTable::new();
        restored.restore(snapshot.clone());
        assert_eq!(restored.snapshot(), snapshot);

        // restore replaces the whole table rather than merging into it.
        restored.restore(Vec::new());
        assert!(restored.is_empty());
    }

    #[test]
    fn retain_bound_channels_drops_removed_and_disabled_channels_only() {
        let table = ServiceTable::new();
        table.insert(on_channel(1, 10, 10, "keep", "GR", "27"));
        table.insert(on_channel(1, 10, 11, "removed", "GR", "28"));
        table.insert(record(1, 10, 12, "unbound"));

        let keep: HashSet<(String, String)> = HashSet::from([("GR".to_owned(), "27".to_owned())]);
        assert_eq!(table.retain_bound_channels(&keep), 2);
        assert_eq!(table.len(), 1);
        assert!(table
            .get_on_channel("GR", "27", ServiceItemId::new(1, 10))
            .is_some());
    }

    fn config_channel(
        channel_type: &str,
        channel: &str,
        extra_args: &str,
        disabled: bool,
        routes: Vec<crate::config::ChannelRouteConfig>,
    ) -> crate::config::ChannelConfig {
        crate::config::ChannelConfig {
            name: format!("{channel_type}-{channel}"),
            channel_type: channel_type.to_owned(),
            channel: channel.to_owned(),
            extra_args: extra_args.to_owned(),
            disabled,
            routes,
        }
    }

    fn route(
        tuner: &str,
        channel: Option<&str>,
        extra_args: Option<&str>,
    ) -> crate::config::ChannelRouteConfig {
        crate::config::ChannelRouteConfig {
            tuner: tuner.to_owned(),
            channel: channel.map(str::to_owned),
            extra_args: extra_args.map(str::to_owned),
        }
    }

    #[test]
    fn fingerprint_is_canonical_over_enabled_channels() {
        let a = config_channel("GR", "27", "", false, Vec::new());
        let b = config_channel("BS", "BS13_1", "", false, Vec::new());

        let forward = service_fingerprint(&[a.clone(), b.clone()]);
        let backward = service_fingerprint(&[b, a.clone()]);
        assert_eq!(forward, backward, "the entry order must not matter");

        // Disabled entries are not part of the fingerprint.
        let disabled = config_channel("CS", "CS01", "", true, Vec::new());
        assert_eq!(
            service_fingerprint(&[a.clone(), disabled]),
            service_fingerprint(&[a])
        );
    }

    #[test]
    fn fingerprint_covers_extra_args_and_route_overrides() {
        let base = config_channel("GR", "27", "", false, Vec::new());
        let fingerprinted = service_fingerprint(std::slice::from_ref(&base));

        let extra = config_channel("GR", "27", "--lnb 15", false, Vec::new());
        assert_ne!(service_fingerprint(&[extra]), fingerprinted);

        let routed = config_channel(
            "GR",
            "27",
            "",
            false,
            vec![
                route("t0", Some("13"), Some("--lnb 20")),
                route("t1", None, None),
            ],
        );
        assert_ne!(
            service_fingerprint(std::slice::from_ref(&routed)),
            fingerprinted
        );

        let reordered = config_channel(
            "GR",
            "27",
            "",
            false,
            vec![
                route("t1", None, None),
                route("t0", Some("13"), Some("--lnb 20")),
            ],
        );
        assert_ne!(
            service_fingerprint(&[reordered]),
            service_fingerprint(&[routed]),
            "route order is part of the fingerprint"
        );
    }

    #[test]
    fn fingerprint_is_a_stable_hex_string() {
        let fingerprint = service_fingerprint(&[config_channel("GR", "27", "", false, Vec::new())]);
        assert_eq!(fingerprint.len(), 16);
        assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    fn cache_fixture() -> (tempfile::TempDir, ServiceTable) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let table = ServiceTable::new();
        let mut record = on_channel(1, 10, 7, "cached", "GR", "27");
        record.pmt_pid = Some(0x1F0);
        table.insert(record);
        (dir, table)
    }

    #[test]
    fn service_cache_round_trips_and_keeps_the_schema() {
        let (dir, table) = cache_fixture();
        write_service_cache(dir.path(), "abc123", &table).expect("write cache");

        let path = service_cache_path(dir.path());
        assert!(path.exists());
        assert!(!dir.path().join("services.json.tmp").exists());

        let cache = read_service_cache(dir.path())
            .expect("read cache")
            .expect("present");
        assert_eq!(cache.version, SERVICE_CACHE_VERSION);
        assert_eq!(cache.fingerprint, "abc123");
        assert!(cache.generated_at.ends_with('Z'));
        assert_eq!(cache.services, table.snapshot());

        let restored = load_service_cache(dir.path());
        assert_eq!(restored.snapshot(), table.snapshot());
    }

    #[test]
    fn a_missing_cache_is_none_not_an_error() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        assert_eq!(read_service_cache(dir.path()), Ok(None));
        assert!(load_service_cache(dir.path()).is_empty());
    }

    #[test]
    fn a_corrupt_or_old_cache_is_ignored_at_startup() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join(SERVICE_CACHE_FILE), "{\"version\":")
            .expect("write truncated cache");
        assert!(read_service_cache(dir.path()).is_err());
        assert!(load_service_cache(dir.path()).is_empty());

        std::fs::write(
            dir.path().join(SERVICE_CACHE_FILE),
            "{\"version\":99,\"fingerprint\":\"x\",\"generated_at\":\"\",\"services\":[]}",
        )
        .expect("write old cache");
        assert!(load_service_cache(dir.path()).is_empty());
    }

    #[test]
    fn generated_at_uses_rfc3339_utc() {
        let stamp = generated_at();
        assert!(stamp.ends_with('Z'), "{stamp}");
        assert_eq!(stamp.len(), "2026-11-09T12:34:56.789Z".len(), "{stamp}");
        // 1970-01-01 is day 0 in the civil conversion.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }
}

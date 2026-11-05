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
//! # Lookup by bare service id
//!
//! SPEC §5: an exact [`ServiceItemId`] match always wins. A bare sid resolves
//! only when exactly one network carries it; with matches in two networks the
//! result is [`SidLookupError::Ambiguous`] (which the API maps to 404).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

/// Why a bare service id could not be resolved to one service.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SidLookupError {
    /// No service in the table has this service id.
    #[error("service id {0} not found")]
    NotFound(u16),
    /// More than one network carries this service id (SPEC §5).
    #[error("service id {0} is ambiguous")]
    Ambiguous(u16),
}

/// One service known to the server.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    pub remote_control_key_id: Option<u8>,
    /// `(type, channel)` this service was found on, once the scan knows it.
    pub channel: Option<(String, String)>,
    /// PID of the service's PMT, once a PAT has been seen.
    pub pmt_pid: Option<u16>,
    /// Whether logo (CDT) data is available. Always `false`: SPEC §1 excludes
    /// logo handling, and §9 reports `hasLogoData: false`.
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

    /// Key used for the stable list order: network, then transport stream,
    /// then service id.
    fn order_key(&self) -> (u16, u16, u16) {
        (self.network_id, self.tsid, self.service_id)
    }
}

/// The services known to the server.
///
/// Cloning shares the same table; every method is safe to call concurrently.
#[derive(Clone, Debug, Default)]
pub struct ServiceTable {
    records: Arc<RwLock<HashMap<u64, ServiceRecord>>>,
}

impl ServiceTable {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces the record for its item id.
    ///
    /// Fields the caller does not know (channel, PMT PID, remote-control key)
    /// are taken from the new record as-is; use [`set_channel`](Self::set_channel)
    /// to update a single field without rebuilding the record.
    pub fn upsert(&self, record: ServiceRecord) {
        let key = record.item_id().0;
        self.write().insert(key, record);
    }

    /// Sets the `(type, channel)` binding of a known service.
    ///
    /// Does nothing when the item id is unknown; returns whether it was set.
    pub fn set_channel(&self, item_id: ServiceItemId, channel_type: &str, channel: &str) -> bool {
        let mut records = self.write();
        let Some(record) = records.get_mut(&item_id.0) else {
            return false;
        };
        record.channel = Some((channel_type.to_owned(), channel.to_owned()));
        true
    }

    /// Sets the PMT PID of a known service. Returns whether it was set.
    pub fn set_pmt_pid(&self, item_id: ServiceItemId, pmt_pid: u16) -> bool {
        let mut records = self.write();
        let Some(record) = records.get_mut(&item_id.0) else {
            return false;
        };
        record.pmt_pid = Some(pmt_pid);
        true
    }

    /// Every record, ordered by network, transport stream and service id.
    ///
    /// The order is stable regardless of insertion order, so API responses do
    /// not shuffle between calls.
    pub fn list(&self) -> Vec<ServiceRecord> {
        let mut records: Vec<ServiceRecord> = self.read().values().cloned().collect();
        records.sort_by_key(ServiceRecord::order_key);
        records
    }

    /// Looks a service up by item id.
    pub fn get(&self, item_id: ServiceItemId) -> Option<ServiceRecord> {
        self.read().get(&item_id.0).cloned()
    }

    /// Looks a service up by a bare service id (SPEC §5).
    ///
    /// Returns the only match, [`SidLookupError::NotFound`] when there is none,
    /// and [`SidLookupError::Ambiguous`] when several networks carry the same
    /// service id. Callers that have a [`ServiceItemId`] should use
    /// [`get`](Self::get), which is never ambiguous.
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
    /// service id. A value that is not a decimal number yields
    /// [`SidLookupError::NotFound`].
    pub fn resolve(&self, raw: &str) -> Result<ServiceRecord, SidLookupError> {
        if let Ok(value) = raw.parse::<u64>() {
            if let Some(record) = self.get(ServiceItemId(value)) {
                return Ok(record);
            }
        }
        match raw.parse::<u16>() {
            Ok(sid) => self.get_by_sid(sid),
            Err(_) => Err(SidLookupError::NotFound(0)),
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

    /// Removes every service.
    pub fn clear(&self) {
        self.write().clear();
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<u64, ServiceRecord>> {
        self.records
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<u64, ServiceRecord>> {
        self.records
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(network_id: u16, tsid: u16, service_id: u16, name: &str) -> ServiceRecord {
        ServiceRecord::new(network_id, tsid, service_id, 0x01, name)
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
    fn record_defaults_match_the_spec() {
        let stored = record(0x7FE0, 0x7FE0, 0x0408, "ＮＨＫ総合");
        assert_eq!(stored.channel, None);
        assert_eq!(stored.pmt_pid, None);
        assert!(!stored.has_logo_data);
        assert_eq!(stored.remote_control_key_id, None);
        assert_eq!(stored.item_id(), ServiceItemId::new(0x7FE0, 0x0408));
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

        assert!(!table.set_channel(ServiceItemId::new(9, 9), "GR", "27"));
    }

    #[test]
    fn set_pmt_pid_updates_a_known_service_only() {
        let table = ServiceTable::new();
        let id = ServiceItemId::new(1, 10);
        table.upsert(record(1, 10, 10, "svc"));

        assert!(table.set_pmt_pid(id, 0x1F0));
        assert_eq!(table.get(id).expect("record").pmt_pid, Some(0x1F0));
        assert!(!table.set_pmt_pid(ServiceItemId::new(9, 9), 0x1F0));
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
    fn resolve_rejects_non_numeric_ids() {
        let table = ServiceTable::new();
        assert_eq!(table.resolve("abc"), Err(SidLookupError::NotFound(0)));
        assert_eq!(table.resolve(""), Err(SidLookupError::NotFound(0)));
        assert_eq!(table.resolve("-1"), Err(SidLookupError::NotFound(0)));
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
}

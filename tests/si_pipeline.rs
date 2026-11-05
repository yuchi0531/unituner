//! End-to-end check of the phase 2 data path: a synthetic transport stream is
//! normalized (`tsframe`), assembled and parsed (`si`), and applied to the
//! service table (`services`).
//!
//! The stream is built programmatically, both 188- and 204-framed, with null
//! packets interleaved and the SDT split over several packets.

use unituner::services::{ServiceItemId, ServiceTable};
use unituner::si::{crc32_mpeg2, SiEvent, SiParser};
use unituner::tsframe::{TsNormalizer, TS_PACKET_SIZE, TS_PACKET_SIZE_204};

const PAT_PID: u16 = 0x0000;
const PMT_PID: u16 = 0x01F0;
const SDT_PID: u16 = 0x0011;
const NIT_PID: u16 = 0x0010;
const NULL_PID: u16 = 0x1FFF;

const TSID: u16 = 0x7FE0;
const NETWORK_ID: u16 = 0x7FE0;
const SERVICE_ID: u16 = 0x0408;
const SERVICE_TYPE: u8 = 0x01;
const SERVICE_NAME: &str = "ＮＨＫ総合";
const REMOTE_CONTROL_KEY_ID: u8 = 0x03;

// ---------------------------------------------------------------------------
// Section builders
// ---------------------------------------------------------------------------

/// Wraps `rest` in a long-form section and appends its CRC32.
fn section(table_id: u8, id: u16, rest: &[u8]) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (id >> 8) as u8,
        id as u8,
        0xC1, // version 0, current
        0x00, // section_number
        0x00, // last_section_number
    ];
    bytes.extend_from_slice(rest);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

/// PAT announcing one programme (SERVICE_ID -> PMT_PID) and the NIT PID.
fn pat_section() -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&[0x00, 0x00]); // program_number 0 = network PID
    rest.extend_from_slice(&(0xE000 | NIT_PID).to_be_bytes());
    rest.extend_from_slice(&SERVICE_ID.to_be_bytes());
    rest.extend_from_slice(&(0xE000 | PMT_PID).to_be_bytes());
    section(0x00, TSID, &rest)
}

/// PMT for SERVICE_ID with an H.264 video and an AAC audio elementary stream.
fn pmt_section() -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&0xE1F1u16.to_be_bytes()); // PCR_PID 0x1F1
    rest.extend_from_slice(&[0xF0, 0x00]); // program_info_length 0
    rest.extend_from_slice(&[0x1B, 0xE1, 0xF1, 0xF0, 0x00]); // H.264
    rest.extend_from_slice(&[0x0F, 0xE1, 0xF2, 0xF0, 0x00]); // AAC
    section(0x02, SERVICE_ID, &rest)
}

/// SDT with one service. The provider name is padded so the section needs more
/// than one packet.
fn sdt_section() -> Vec<u8> {
    let mut descriptor = vec![
        SERVICE_TYPE,
        160, // service_provider_name_length (padded, keeps the section large)
    ];
    descriptor.extend(std::iter::repeat_n(b'P', 160));
    // ARIB STD-B24: designate JIS X 0208-1997 into G0, then the code points.
    let name: [u8; 13] = [
        0x1B, 0x24, 0x42, // ESC $ B, Kanji into G0
        0x23, 0x4E, // Ｎ
        0x23, 0x48, // Ｈ
        0x23, 0x4B, // Ｋ
        0x41, 0x6D, // 総
        0x39, 0x67, // 合
    ];
    descriptor.push(name.len() as u8);
    descriptor.extend_from_slice(&name);

    let mut rest = vec![
        (NETWORK_ID >> 8) as u8,
        NETWORK_ID as u8, // original_network_id
        0xFF,             // reserved
        (SERVICE_ID >> 8) as u8,
        SERVICE_ID as u8, // service_id
        0xFD,             // EIT flags
    ];
    let loop_length = 2 + descriptor.len();
    rest.extend_from_slice(&[0x80 | ((loop_length >> 8) as u8 & 0x0F), loop_length as u8]);
    rest.extend_from_slice(&[0x48, descriptor.len() as u8]);
    rest.extend_from_slice(&descriptor);
    section(0x42, TSID, &rest)
}

/// NIT carrying a TS information descriptor (tag 0xCD) with the
/// remote-control key id.
fn nit_section() -> Vec<u8> {
    // The descriptor body: key id, ts_name length/transmission count, then one
    // transmission type carrying the service.
    let mut body = vec![
        REMOTE_CONTROL_KEY_ID,
        0x01, // length_of_ts_name 0, transmission_type_count 1
        b'G',
        b'R', // ts_name
        0x01, // transmission_type_info
        0x01, // num_of_service
    ];
    body.extend_from_slice(&SERVICE_ID.to_be_bytes());
    let mut descriptor = vec![0xCD, body.len() as u8];
    descriptor.extend_from_slice(&body);

    let mut rest = vec![0xF0, descriptor.len() as u8];
    rest.extend_from_slice(&descriptor);
    rest.extend_from_slice(&[0xF0, 0x00]); // transport_stream_loop_length 0
    section(0x40, NETWORK_ID, &rest)
}

// ---------------------------------------------------------------------------
// Packet builders
// ---------------------------------------------------------------------------

/// Builds a 188-byte packet with the given PID, PUSI flag, CC and payload.
fn packet(pid: u16, payload_unit_start: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
    assert!(
        payload.len() <= TS_PACKET_SIZE - 4,
        "payload must fit a packet"
    );
    let mut packet = vec![0xFFu8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = ((payload_unit_start as u8) << 6) | (pid >> 8) as u8;
    packet[2] = pid as u8;
    packet[3] = 0x10 | (cc & 0x0F);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}

/// Fragments a section over as many 188-byte packets as it needs.
fn section_packets(pid: u16, section: &[u8], first_cc: u8) -> Vec<u8> {
    let mut packets = Vec::new();
    let mut remaining = section;
    let mut cc = first_cc;
    let mut first = true;
    while first || !remaining.is_empty() {
        let capacity = TS_PACKET_SIZE - 4 - usize::from(first);
        let take = capacity.min(remaining.len());
        let mut payload = Vec::with_capacity(take + 1);
        if first {
            payload.push(0x00); // pointer_field
        }
        payload.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
        packets.extend_from_slice(&packet(pid, first, cc, &payload));
        cc = cc.wrapping_add(1) & 0x0F;
        first = false;
    }
    packets
}

/// A null packet, exercising the "skip PID 0x1FFF" path.
fn null_packet(cc: u8) -> Vec<u8> {
    packet(NULL_PID, false, cc, &[0xFF; TS_PACKET_SIZE - 4])
}

/// The complete synthetic multiplex as 188-byte packets.
fn transport_stream() -> Vec<u8> {
    let mut stream = Vec::new();
    stream.extend_from_slice(&null_packet(0));
    stream.extend_from_slice(&section_packets(PAT_PID, &pat_section(), 0));
    stream.extend_from_slice(&null_packet(1));
    stream.extend_from_slice(&section_packets(NIT_PID, &nit_section(), 0));
    stream.extend_from_slice(&section_packets(PMT_PID, &pmt_section(), 0));
    stream.extend_from_slice(&null_packet(2));
    stream.extend_from_slice(&section_packets(SDT_PID, &sdt_section(), 0));
    stream.extend_from_slice(&null_packet(3));
    stream
}

/// Re-frames 188-byte packets as 204-byte packets with a parity tail.
fn to_204(stream: &[u8]) -> Vec<u8> {
    stream
        .chunks_exact(TS_PACKET_SIZE)
        .flat_map(|packet| {
            let mut framed = packet.to_vec();
            framed.extend_from_slice(&[0xA5; 16]);
            framed
        })
        .collect()
}

// ---------------------------------------------------------------------------
// SI -> service table
// ---------------------------------------------------------------------------

/// Minimal scan sink for the test: folds SI events into a [`ServiceTable`].
///
/// This mirrors what the phase 7 scanner will do. Two orderings matter and are
/// handled here: a PAT carries no network id (only a network PID), so services
/// are keyed by the SDT's `original_network_id`; and the NIT's remote-control
/// key id may arrive before or after the SDT it applies to.
#[derive(Default)]
struct ScanSink {
    table: ServiceTable,
    channel: Option<(String, String)>,
    /// PMT PID per service id, learned from the PAT.
    pmt_pids: std::collections::HashMap<u16, u16>,
    /// Remote-control key id per network id, learned from the NIT.
    key_ids: std::collections::HashMap<u16, u8>,
}

impl ScanSink {
    fn new(channel: (&str, &str)) -> Self {
        Self {
            channel: Some((channel.0.to_owned(), channel.1.to_owned())),
            ..Self::default()
        }
    }

    fn apply(&mut self, event: &SiEvent) {
        match event {
            SiEvent::Pat(pat) => {
                for (service_id, pmt_pid) in &pat.programs {
                    self.pmt_pids.insert(*service_id, *pmt_pid);
                }
            }
            SiEvent::Pmt(_) => {}
            SiEvent::Nit(nit) => {
                if let Some(key_id) = nit.remote_control_key_id {
                    self.key_ids.insert(nit.network_id, key_id);
                }
                for record in self.table.list() {
                    let Some(key_id) = self.key_ids.get(&record.network_id).copied() else {
                        continue;
                    };
                    let mut updated = record;
                    updated.remote_control_key_id = Some(key_id);
                    self.table.upsert(updated);
                }
            }
            SiEvent::Sdt(sdt) => {
                for (service_id, service_type, name) in &sdt.services {
                    let mut record = unituner::services::ServiceRecord::new(
                        sdt.original_network_id,
                        sdt.transport_stream_id,
                        *service_id,
                        *service_type,
                        name.clone(),
                    );
                    record.channel.clone_from(&self.channel);
                    record.pmt_pid = self.pmt_pids.get(service_id).copied();
                    record.remote_control_key_id =
                        self.key_ids.get(&sdt.original_network_id).copied();
                    self.table.upsert(record);
                }
            }
        }
    }
}

/// Runs the whole path and returns the resulting table.
fn scan(framed: &[u8], chunk: usize, channel: (&str, &str)) -> (ServiceTable, u64, TsNormalizer) {
    let mut normalizer = TsNormalizer::new();
    let mut parser = SiParser::new();
    let mut sink = ScanSink::new(channel);
    let mut events = 0u64;

    for piece in framed.chunks(chunk) {
        let packets = normalizer.push(piece);
        for event in parser.push_packets(&packets) {
            events += 1;
            sink.apply(&event);
        }
    }
    let packets = normalizer.finish();
    for event in parser.push_packets(&packets) {
        events += 1;
        sink.apply(&event);
    }
    (sink.table, events, normalizer)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn a_188_framed_stream_fills_the_service_table() {
    let stream = transport_stream();
    let (table, events, normalizer) = scan(&stream, 4096, ("GR", "27"));

    assert_eq!(
        normalizer.framing(),
        Some(unituner::tsframe::Framing::Ts188)
    );
    assert_eq!(normalizer.bytes_discarded, 0);
    assert_eq!(normalizer.sync_errors, 0);
    assert!(events >= 4, "expected PAT, NIT, PMT and SDT, got {events}");

    let record = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("the service must be in the table");

    assert_eq!(record.network_id, NETWORK_ID);
    assert_eq!(record.tsid, TSID);
    assert_eq!(record.service_id, SERVICE_ID);
    assert_eq!(record.service_type, SERVICE_TYPE);
    assert_eq!(record.name, SERVICE_NAME);
    assert_eq!(record.remote_control_key_id, Some(REMOTE_CONTROL_KEY_ID));
    assert_eq!(record.channel, Some(("GR".to_owned(), "27".to_owned())));
    assert_eq!(record.pmt_pid, Some(PMT_PID));
    assert!(!record.has_logo_data);

    assert_eq!(table.len(), 1);
    assert_eq!(table.list(), vec![record.clone()]);

    // The bare-sid lookup resolves too, because only one network carries it.
    assert_eq!(table.get_by_sid(SERVICE_ID).expect("unique sid"), record);
    assert_eq!(
        table
            .resolve(&ServiceItemId::new(NETWORK_ID, SERVICE_ID).to_string())
            .expect("item id"),
        record
    );
}

#[test]
fn a_204_framed_stream_yields_the_same_table() {
    let stream = transport_stream();
    let framed = to_204(&stream);
    assert_eq!(framed.len() % TS_PACKET_SIZE_204, 0);

    let (table, events, normalizer) = scan(&framed, 8192, ("GR", "27"));

    assert_eq!(
        normalizer.framing(),
        Some(unituner::tsframe::Framing::Ts204)
    );
    assert_eq!(normalizer.bytes_discarded, 0);
    assert!(events >= 4);

    let record = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("the service must be in the table");
    assert_eq!(record.name, SERVICE_NAME);
    assert_eq!(record.service_type, SERVICE_TYPE);
    assert_eq!(record.remote_control_key_id, Some(REMOTE_CONTROL_KEY_ID));
    assert_eq!(record.pmt_pid, Some(PMT_PID));
}

#[test]
fn byte_at_a_time_feeding_and_leading_junk_do_not_change_the_result() {
    let stream = transport_stream();

    let mut noisy = vec![0x00, 0x13, 0x37, 0x47, 0x99]; // leading junk, incl. a false sync
    noisy.extend_from_slice(&stream);

    let (baseline, _, _) = scan(&stream, 65_536, ("GR", "27"));
    let (chunked, _, normalizer) = scan(&noisy, 1, ("GR", "27"));

    assert_eq!(chunked.list(), baseline.list());
    assert_eq!(normalizer.bytes_discarded, 5);
    assert_eq!(normalizer.sync_errors, 0);
}

#[test]
fn a_partial_sdt_is_recovered_when_the_stream_ends_cleanly() {
    let stream = transport_stream();
    let (table, _, _) = scan(&stream, 3, ("GR", "27"));
    let record = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("the service must be in the table");
    assert_eq!(record.name, SERVICE_NAME);
}

#[test]
fn the_sdt_section_really_spans_several_packets() {
    let section = sdt_section();
    assert!(
        section.len() > TS_PACKET_SIZE - 4,
        "the SDT fixture must not fit one packet (len {})",
        section.len()
    );
    let packets = section_packets(SDT_PID, &section, 0);
    assert!(packets.len() >= 2 * TS_PACKET_SIZE);

    // Parsing the reassembled section yields exactly the expected service.
    let mut parser = SiParser::new();
    let events = parser.push_packets(&packets);
    assert_eq!(events.len(), 1);
    match &events[0] {
        SiEvent::Sdt(sdt) => {
            assert_eq!(sdt.original_network_id, NETWORK_ID);
            assert_eq!(sdt.transport_stream_id, TSID);
            assert_eq!(
                sdt.services,
                vec![(SERVICE_ID, SERVICE_TYPE, SERVICE_NAME.to_owned())]
            );
        }
        other => panic!("expected an SDT, got {other:?}"),
    }
}

#[test]
fn a_corrupted_sdt_packet_is_skipped_without_polluting_the_table() {
    let stream = transport_stream();
    let mut damaged = stream.clone();
    // Break the sync byte of the SDT's first packet, found by PID rather than
    // by a hard-coded offset.
    let sdt_offset = stream
        .chunks_exact(TS_PACKET_SIZE)
        .position(|packet| packet[1] & 0x1F == (SDT_PID >> 8) as u8 && packet[2] == SDT_PID as u8)
        .expect("the fixture contains SDT packets")
        * TS_PACKET_SIZE;
    damaged[sdt_offset] = 0x00;

    let (table, events, normalizer) = scan(&damaged, 1024, ("GR", "27"));

    // The PAT, NIT and PMT still arrive...
    assert!(events >= 3, "expected the other tables, got {events}");
    // ...the 204/188 framing still holds, the damaged packet was resynced...
    assert!(normalizer.sync_errors >= 1);
    // ...and no half-decoded service leaks into the table.
    assert!(table.is_empty(), "table: {:?}", table.list());
}

#[test]
fn pat_and_pmt_agree_on_the_programme_and_pids() {
    let mut parser = SiParser::new();
    let events = parser.push_packets(&section_packets(PAT_PID, &pat_section(), 0));
    let pat = match &events[0] {
        SiEvent::Pat(pat) => pat,
        other => panic!("expected a PAT, got {other:?}"),
    };
    assert_eq!(pat.transport_stream_id, TSID);
    assert_eq!(pat.programs, vec![(SERVICE_ID, PMT_PID)]);

    let events = parser.push_packets(&section_packets(PMT_PID, &pmt_section(), 0));
    let pmt = match &events[0] {
        SiEvent::Pmt(pmt) => pmt,
        other => panic!("expected a PMT, got {other:?}"),
    };
    assert_eq!(pmt.program_number, SERVICE_ID);
    assert_eq!(pmt.pcr_pid, 0x1F1);
    assert_eq!(pmt.streams, vec![(0x1B, 0x1F1), (0x0F, 0x1F2)]);
}

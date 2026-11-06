//! End-to-end check of the phase 2 data path: a synthetic transport stream is
//! normalized (`tsframe`), assembled and parsed (`si`), and applied to the
//! service table (`services`).
//!
//! The stream is built programmatically, both 188- and 204-framed, with null
//! packets interleaved and the SDT split over several packets.

use unituner::services::{ServiceItemId, ServiceRecord, ServiceTable};
use unituner::si::{crc32_mpeg2, SiEvent, SiParser, TableCompleteness};
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
/// This mirrors what the phase 7 scanner will do and uses only the table's own
/// API — no shadow maps. `ServiceTable::upsert` merges, so SDT-before-NIT and
/// NIT-before-PAT both come out complete; `set_remote_control_key_id` and
/// `set_pmt_pid` update one field under a single write lock.
///
/// A batch of events is applied in a fixed order (SDT first, then NIT, then
/// PAT) so that the record-creating table runs before the tables that only
/// enrich it, regardless of the order the sections physically arrived in. A
/// real scanner re-applies every few hundred milliseconds anyway; the ordering
/// just makes a single pass sufficient here.
#[derive(Default)]
struct ScanSink {
    table: ServiceTable,
    channel: Option<(String, String)>,
}

impl ScanSink {
    fn new(channel: (&str, &str)) -> Self {
        Self {
            channel: Some((channel.0.to_owned(), channel.1.to_owned())),
            ..Self::default()
        }
    }

    fn channel_ref(&self) -> Option<(&str, &str)> {
        self.channel
            .as_ref()
            .map(|(channel_type, channel)| (channel_type.as_str(), channel.as_str()))
    }

    fn apply_all(&mut self, events: &[SiEvent]) {
        // 1. SDT creates/updates the records and binds them to the channel.
        for event in events {
            let SiEvent::Sdt(sdt) = event else {
                continue;
            };
            for (service_id, service_type, name) in &sdt.services {
                let mut record = ServiceRecord::new(
                    sdt.original_network_id,
                    sdt.transport_stream_id,
                    *service_id,
                    *service_type,
                    name.clone(),
                );
                record.channel.clone_from(&self.channel);
                self.table.upsert(record);
            }
        }

        // 2. NIT supplies the remote-control key id for its whole network.
        for event in events {
            let SiEvent::Nit(nit) = event else {
                continue;
            };
            let Some(key_id) = nit.remote_control_key_id else {
                continue;
            };
            for record in self.table.list() {
                if record.network_id == nit.network_id {
                    self.table
                        .set_remote_control_key_id(record.item_id(), key_id);
                }
            }
        }

        // 3. PAT supplies each programme's PMT PID; the channel makes the
        //    lookup unambiguous.
        let Some((channel_type, channel)) = self.channel_ref() else {
            return;
        };
        for event in events {
            let SiEvent::Pat(pat) = event else {
                continue;
            };
            for (service_id, pmt_pid) in &pat.programs {
                if let Ok(record) =
                    self.table
                        .resolve_on_channel(channel_type, channel, *service_id)
                {
                    self.table.set_pmt_pid(record.item_id(), *pmt_pid);
                }
            }
        }
    }
}

/// Runs the whole path and returns the resulting table.
///
/// Events are collected across the whole stream and applied in one pass at the
/// end: SI tables may arrive in any order relative to each other, and a PAT
/// cannot set a PMT PID before the SDT has created the record it belongs to.
/// This is what a periodic re-scan does too (apply the accumulated state, not
/// each section in isolation).
fn scan(framed: &[u8], chunk: usize, channel: (&str, &str)) -> (ServiceTable, u64, TsNormalizer) {
    let mut normalizer = TsNormalizer::new();
    let mut parser = SiParser::new();
    let mut sink = ScanSink::new(channel);
    let mut events = Vec::new();

    for piece in framed.chunks(chunk) {
        let packets = normalizer.push(piece);
        events.extend(parser.push_packets(&packets));
    }
    let packets = normalizer.finish();
    events.extend(parser.push_packets(&packets));

    let count = events.len() as u64;
    sink.apply_all(&events);
    (sink.table, count, normalizer)
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
    assert_eq!(pat.network_pid, Some(NIT_PID));
    assert_eq!(pat.version_number, 0);
    assert!(pat.current_next);
    assert_eq!(pat.section_number, 0);
    assert_eq!(pat.last_section_number, 0);

    let events = parser.push_packets(&section_packets(PMT_PID, &pmt_section(), 0));
    let pmt = match &events[0] {
        SiEvent::Pmt(pmt) => pmt,
        other => panic!("expected a PMT, got {other:?}"),
    };
    assert_eq!(pmt.program_number, SERVICE_ID);
    assert_eq!(pmt.pcr_pid, 0x1F1);
    assert_eq!(pmt.streams, vec![(0x1B, 0x1F1), (0x0F, 0x1F2)]);
}

#[test]
fn a_multi_section_sdt_is_tracked_to_completion() {
    // Two SDT sections of the same table version: section 0 carries the
    // service, section 1 carries another one. Completeness must only be
    // reported once both have arrived, in any order.
    let mut first_rest = Vec::new();
    first_rest.extend_from_slice(&[(NETWORK_ID >> 8) as u8, NETWORK_ID as u8, 0xFF]);
    first_rest.extend_from_slice(&sdt_service_loop(SERVICE_ID, "one"));

    let mut second_rest = Vec::new();
    second_rest.extend_from_slice(&[(NETWORK_ID >> 8) as u8, NETWORK_ID as u8, 0xFF]);
    second_rest.extend_from_slice(&sdt_service_loop(0x0409, "two"));

    let first = sdt_section_with(0x42, 0, 0, 1, &first_rest);
    let second = sdt_section_with(0x42, 0, 1, 1, &second_rest);

    let mut parser = SiParser::new();
    let mut tracker = TableCompleteness::new();

    let events = parser.push_packets(&section_packets(SDT_PID, &second, 0));
    let SiEvent::Sdt(info) = &events[0] else {
        panic!("expected an SDT");
    };
    assert!(
        !tracker.observe(info),
        "section 1 of 1 alone is not complete"
    );

    let events = parser.push_packets(&section_packets(SDT_PID, &first, 2));
    let SiEvent::Sdt(info) = &events[0] else {
        panic!("expected an SDT");
    };
    assert!(
        tracker.observe(info),
        "sections 0 and 1 of 1 complete the table"
    );
}

#[test]
fn sdt_other_is_parsed_but_marked_as_not_actual() {
    let mut rest = Vec::new();
    rest.extend_from_slice(&[(NETWORK_ID >> 8) as u8, NETWORK_ID as u8, 0xFF]);
    rest.extend_from_slice(&sdt_service_loop(SERVICE_ID, "other"));
    let other = sdt_section_with(0x46, 0, 0, 0, &rest);

    let mut parser = SiParser::new();
    let events = parser.push_packets(&section_packets(SDT_PID, &other, 0));
    let SiEvent::Sdt(info) = &events[0] else {
        panic!("expected an SDT");
    };
    assert!(
        !info.is_actual,
        "table id 0x46 is the SDT-other and must be distinguishable"
    );
    // The parser still reports it; only the consumer decides to ignore it.
    assert_eq!(info.services.len(), 1);
}

/// One-service SDT service loop (service_descriptor with a plain ASCII name).
fn sdt_service_loop(service_id: u16, name: &str) -> Vec<u8> {
    let mut descriptor = vec![SERVICE_TYPE, 0x00]; // provider name length 0
    let encoded = format!("\u{1B}(J{name}").into_bytes();
    descriptor.push(encoded.len() as u8);
    descriptor.extend_from_slice(&encoded);

    let mut services = vec![(service_id >> 8) as u8, service_id as u8, 0xFD];
    let loop_length = 2 + descriptor.len();
    services.extend_from_slice(&[0x80 | ((loop_length >> 8) as u8 & 0x0F), loop_length as u8]);
    services.extend_from_slice(&[0x48, descriptor.len() as u8]);
    services.extend_from_slice(&descriptor);
    services
}

/// SDT section with explicit version and section numbering.
fn sdt_section_with(
    table_id: u8,
    version: u8,
    section_number: u8,
    last_section_number: u8,
    rest: &[u8],
) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (TSID >> 8) as u8,
        TSID as u8,
        0xC0 | ((version & 0x1F) << 1) | 1,
        section_number,
        last_section_number,
    ];
    bytes.extend_from_slice(rest);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

#[test]
fn a_204_stream_whose_parity_byte_is_0x47_is_not_read_as_188() {
    // The first packet's parity tail starts with 0x47, i.e. a false sync at the
    // 188 stride. A naive detector decides 188 on the tie and every following
    // packet is misaligned; the fixed one waits for the 204 lead.
    let stream = transport_stream();
    let mut framed: Vec<u8> = stream
        .chunks_exact(TS_PACKET_SIZE)
        .enumerate()
        .flat_map(|(index, packet)| {
            let mut framed = packet.to_vec();
            framed.extend_from_slice(&[0xA5; 16]);
            // Only the very first tail is 0x47: later ones must not extend the
            // false 188 chain past the detection window.
            if index == 0 {
                framed[TS_PACKET_SIZE] = 0x47;
            }
            framed
        })
        .collect();
    assert_eq!(framed[TS_PACKET_SIZE], 0x47);
    // Feeding in 188-byte chunks makes the tie appear after ~2 packets, which
    // is exactly the state the bug froze.
    let (table, _, normalizer) = scan(&framed, TS_PACKET_SIZE, ("GR", "27"));
    assert_eq!(
        normalizer.framing(),
        Some(unituner::tsframe::Framing::Ts204),
        "the 0x47 parity byte must not lock in 188 framing"
    );
    assert_eq!(normalizer.bytes_discarded, 0);
    assert_eq!(normalizer.sync_errors, 0);
    let record = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("the service must be in the table");
    assert_eq!(record.name, SERVICE_NAME);
    assert_eq!(record.pmt_pid, Some(PMT_PID));
    framed.clear();
}

#[test]
fn the_same_service_on_two_channels_coexists_and_resolves_per_channel() {
    let stream = transport_stream();
    let (gr, _, _) = scan(&stream, 4096, ("GR", "27"));
    // The second channel gets its own table, as a per-channel scan would.
    let (catv, _, _) = scan(&stream, 4096, ("CABLE_CATV", "C17"));

    // Merge both into one table, the way a whole-server scan would.
    let table = ServiceTable::new();
    for record in gr.list().into_iter().chain(catv.list()) {
        table.upsert(record);
    }

    let id = ServiceItemId::new(NETWORK_ID, SERVICE_ID);
    assert_eq!(table.len(), 2, "both channels must be kept");
    assert_eq!(table.get_all(id).len(), 2);
    assert_eq!(
        table
            .get_on_channel("GR", "27", id)
            .expect("GR record")
            .channel,
        Some(("GR".to_owned(), "27".to_owned()))
    );
    assert_eq!(
        table
            .get_on_channel("CABLE_CATV", "C17", id)
            .expect("CATV record")
            .channel,
        Some(("CABLE_CATV".to_owned(), "C17".to_owned()))
    );
    assert_eq!(table.list_by_channel("GR", "27").len(), 1);
    assert_eq!(table.list_by_channel("CABLE_CATV", "C17").len(), 1);

    // A channel-less lookup cannot pick one, but a channel-scoped one can.
    assert!(matches!(
        table.get_by_sid(SERVICE_ID),
        Err(unituner::services::SidLookupError::Ambiguous(_))
    ));
    assert_eq!(
        table
            .resolve_on_channel("GR", "27", SERVICE_ID)
            .expect("resolves on the channel")
            .channel,
        Some(("GR".to_owned(), "27".to_owned()))
    );
    assert_eq!(
        table
            .resolve_raw_on_channel("CABLE_CATV", "C17", &id.to_string())
            .expect("resolves by item id on the channel")
            .channel,
        Some(("CABLE_CATV".to_owned(), "C17".to_owned()))
    );

    // Refreshing one channel leaves the other alone.
    assert_eq!(table.remove_channel("GR", "27"), 1);
    assert_eq!(table.len(), 1);
    assert!(table.get_on_channel("GR", "27", id).is_none());
    assert!(table.get_on_channel("CABLE_CATV", "C17", id).is_some());
}

#[test]
fn sdt_and_nit_arriving_in_either_order_both_keep_the_key_id() {
    // NIT first, then SDT: the SDT's record must not lose the key id.
    let table = ServiceTable::new();
    let mut nit = ServiceRecord::new(NETWORK_ID, TSID, SERVICE_ID, 0, "");
    nit.remote_control_key_id = Some(REMOTE_CONTROL_KEY_ID);
    nit.channel = Some(("GR".to_owned(), "27".to_owned()));
    table.upsert(nit);

    let mut sdt = ServiceRecord::new(NETWORK_ID, TSID, SERVICE_ID, SERVICE_TYPE, SERVICE_NAME);
    sdt.channel = Some(("GR".to_owned(), "27".to_owned()));
    table.upsert(sdt);

    let stored = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("record");
    assert_eq!(stored.name, SERVICE_NAME);
    assert_eq!(stored.service_type, SERVICE_TYPE);
    assert_eq!(stored.remote_control_key_id, Some(REMOTE_CONTROL_KEY_ID));

    // And SDT first, NIT second: the NIT's key id lands on the existing record.
    table.clear();
    let mut sdt = ServiceRecord::new(NETWORK_ID, TSID, SERVICE_ID, SERVICE_TYPE, SERVICE_NAME);
    sdt.channel = Some(("GR".to_owned(), "27".to_owned()));
    table.upsert(sdt);
    assert!(table.set_remote_control_key_id(
        ServiceItemId::new(NETWORK_ID, SERVICE_ID),
        REMOTE_CONTROL_KEY_ID
    ));

    let stored = table
        .get(ServiceItemId::new(NETWORK_ID, SERVICE_ID))
        .expect("record");
    assert_eq!(stored.name, SERVICE_NAME);
    assert_eq!(stored.remote_control_key_id, Some(REMOTE_CONTROL_KEY_ID));
}

//! Tests for the phase 5 per-service TS filter (SPEC §5).
//!
//! Every transport stream here is built programmatically: a PAT with three
//! programmes, one PMT per programme, elementary-stream packets for each, and
//! null packets interleaved.

use unituner::si::{crc32_mpeg2, parse_pat};
use unituner::tsfilter::{rewrite_pat_for_service, ServiceFilter};
use unituner::tsframe::TS_PACKET_SIZE;

const PAT_PID: u16 = 0x0000;
const NIT_PID: u16 = 0x0010;
const SDT_PID: u16 = 0x0011;
const NULL_PID: u16 = 0x1FFF;
const TSID: u16 = 0x7FE0;

// ---------------------------------------------------------------------------
// Section builders
// ---------------------------------------------------------------------------

/// Long-form section with a CRC32, parameterised by table id, table id
/// extension and version.
fn section(table_id: u8, id: u16, version: u8, rest: &[u8]) -> Vec<u8> {
    let section_length = 5 + rest.len() + 4;
    let mut bytes = vec![
        table_id,
        0xB0 | ((section_length >> 8) as u8 & 0x0F),
        section_length as u8,
        (id >> 8) as u8,
        id as u8,
        0xC1 | ((version & 0x1F) << 1),
        0x00,
        0x00,
    ];
    bytes.extend_from_slice(rest);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    bytes
}

/// PAT with the network-PID entry and the given programmes.
fn pat_section(programs: &[(u16, u16)]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&[0x00, 0x00]);
    rest.extend_from_slice(&(0xE000 | NIT_PID).to_be_bytes());
    for &(program, pmt_pid) in programs {
        rest.extend_from_slice(&program.to_be_bytes());
        rest.extend_from_slice(&(0xE000 | pmt_pid).to_be_bytes());
    }
    section(0x00, TSID, 0, &rest)
}

/// PMT for `program_number` with one PCR PID and the given ES PIDs.
fn pmt_section(program_number: u16, version: u8, pcr_pid: u16, es_pids: &[u16]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&(0xE000 | pcr_pid).to_be_bytes());
    rest.extend_from_slice(&[0xF0, 0x00]); // program_info_length 0
    for &pid in es_pids {
        rest.extend_from_slice(&[0x1B, (0xE0 | (pid >> 8)) as u8, pid as u8, 0xF0, 0x00]);
    }
    section(0x02, program_number, version, &rest)
}

// ---------------------------------------------------------------------------
// Packet builders
// ---------------------------------------------------------------------------

fn packet(pid: u16, payload_unit_start: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() <= TS_PACKET_SIZE - 4);
    let mut packet = vec![0xFFu8; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = ((payload_unit_start as u8) << 6) | (pid >> 8) as u8;
    packet[2] = pid as u8;
    packet[3] = 0x10 | (cc & 0x0F);
    packet[4..4 + payload.len()].copy_from_slice(payload);
    packet
}

/// Fragments a section over as many packets as it needs.
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
            payload.push(0x00);
        }
        payload.extend_from_slice(&remaining[..take]);
        remaining = &remaining[take..];
        packets.extend_from_slice(&packet(pid, first, cc, &payload));
        cc = cc.wrapping_add(1) & 0x0F;
        first = false;
    }
    packets
}

/// An elementary-stream packet with a recognisable body.
fn es_packet(pid: u16, cc: u8, marker: u8) -> Vec<u8> {
    let mut payload = vec![marker; TS_PACKET_SIZE - 4];
    payload[0] = 0x00;
    payload[1] = 0x00;
    payload[2] = 0x01;
    packet(pid, false, cc, &payload)
}

fn null_packet(cc: u8) -> Vec<u8> {
    packet(NULL_PID, false, cc, &[0xFF; TS_PACKET_SIZE - 4])
}

/// Every PID in a run of packets, in order.
fn pids(stream: &[u8]) -> Vec<u16> {
    stream
        .chunks_exact(TS_PACKET_SIZE)
        .map(|p| (u16::from(p[1] & 0x1F) << 8) | u16::from(p[2]))
        .collect()
}

// ---------------------------------------------------------------------------
// The three-programme multiplex
// ---------------------------------------------------------------------------

const TARGET: u16 = 0x0401;
const OTHER_A: u16 = 0x0400;
const OTHER_B: u16 = 0x0402;
const TARGET_PMT: u16 = 0x0101;
const TARGET_PCR: u16 = 0x01A1;
const TARGET_ES: [u16; 2] = [0x0121, 0x0122];
const OTHER_A_PMT: u16 = 0x0100;
const OTHER_A_PCR: u16 = 0x01A0;
const OTHER_A_ES: u16 = 0x0120;
const OTHER_B_PMT: u16 = 0x0102;
const OTHER_B_PCR: u16 = 0x01A2;
const OTHER_B_ES: u16 = 0x0123;

fn multiplex(pat: &[u8]) -> Vec<u8> {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, pat, 0));
    stream.extend_from_slice(&section_packets(
        OTHER_A_PMT,
        &pmt_section(OTHER_A, 0, OTHER_A_PCR, &[OTHER_A_ES]),
        0,
    ));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &TARGET_ES),
        0,
    ));
    stream.extend_from_slice(&section_packets(
        OTHER_B_PMT,
        &pmt_section(OTHER_B, 0, OTHER_B_PCR, &[OTHER_B_ES]),
        0,
    ));
    stream.extend_from_slice(&null_packet(0));
    for (pid, marker) in [
        (OTHER_A_ES, 0xA0),
        (TARGET_ES[0], 0xA1),
        (TARGET_ES[1], 0xA2),
        (OTHER_B_ES, 0xA3),
        (TARGET_PCR, 0xA4),
        (OTHER_A_PCR, 0xA5),
    ] {
        stream.extend_from_slice(&es_packet(pid, 0, marker));
    }
    // A NIT and SDT packet must survive untouched (fixed PIDs).
    stream.extend_from_slice(&section_packets(
        NIT_PID,
        &section(0x40, 0x7FE0, 0, &[0xF0, 0x00]),
        0,
    ));
    stream.extend_from_slice(&section_packets(SDT_PID, &section(0x42, TSID, 0, &[]), 0));
    stream.extend_from_slice(&null_packet(1));
    stream
}

fn three_program_pat() -> Vec<u8> {
    pat_section(&[
        (OTHER_A, OTHER_A_PMT),
        (TARGET, TARGET_PMT),
        (OTHER_B, OTHER_B_PMT),
    ])
}

// ---------------------------------------------------------------------------
// 1. End-to-end PID filtering
// ---------------------------------------------------------------------------

#[test]
fn passes_only_the_target_programme_and_fixed_pids() {
    let stream = multiplex(&three_program_pat());
    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);

    let out_pids = pids(&out);
    let allowed = [
        PAT_PID,
        NIT_PID,
        SDT_PID,
        TARGET_PMT,
        TARGET_PCR,
        TARGET_ES[0],
        TARGET_ES[1],
    ];
    for pid in &out_pids {
        assert!(allowed.contains(pid), "unexpected PID {pid:#06x} leaked");
    }
    for pid in [
        OTHER_A_PMT,
        OTHER_A_PCR,
        OTHER_A_ES,
        OTHER_B_PMT,
        OTHER_B_PCR,
        OTHER_B_ES,
        NULL_PID,
    ] {
        assert!(!out_pids.contains(&pid), "PID {pid:#06x} should be dropped");
    }

    // One PAT (rewritten in place of the original) and the target PMT.
    assert_eq!(out_pids.iter().filter(|&&pid| pid == PAT_PID).count(), 1);
    assert_eq!(out_pids.iter().filter(|&&pid| pid == TARGET_PMT).count(), 1);
    // Both ES PIDs and the PCR PID made it through.
    assert_eq!(
        out_pids.iter().filter(|&&pid| pid == TARGET_ES[0]).count(),
        1
    );
    assert_eq!(
        out_pids.iter().filter(|&&pid| pid == TARGET_ES[1]).count(),
        1
    );
    assert_eq!(out_pids.iter().filter(|&&pid| pid == TARGET_PCR).count(), 1);

    assert_eq!(filter.packets_in, (stream.len() / TS_PACKET_SIZE) as u64);
    assert_eq!(filter.packets_out, (out.len() / TS_PACKET_SIZE) as u64);
    assert_eq!(filter.pat_rewrites, 1);
    assert!(filter.pmt_updates >= 1);
    assert_eq!(filter.pmt_pid(), Some(TARGET_PMT));
    assert_eq!(filter.missing_service_sections, 0);
}

// ---------------------------------------------------------------------------
// 2. Rewritten PAT contents
// ---------------------------------------------------------------------------

#[test]
fn rewritten_pat_has_one_programme_and_a_valid_crc() {
    let original = three_program_pat();
    let rewritten = rewrite_pat_for_service(&original, TARGET).expect("service is in the PAT");

    let parsed = parse_pat(&rewritten).expect("rewritten PAT must parse");
    assert_eq!(parsed.transport_stream_id, TSID);
    assert_eq!(parsed.programs, vec![(TARGET, TARGET_PMT)]);

    assert_eq!(rewritten[5], original[5], "version/current_next preserved");
    assert_eq!(rewritten[6], 0, "section_number");
    assert_eq!(rewritten[7], 0, "last_section_number");
    assert_eq!(&rewritten[3..5], &original[3..5], "tsid preserved");

    let body = &rewritten[..rewritten.len() - 4];
    let crc = u32::from_be_bytes(rewritten[rewritten.len() - 4..].try_into().unwrap());
    assert_eq!(crc, crc32_mpeg2(body));
    // The full-section CRC over section+CRC is zero.
    assert_eq!(crc32_mpeg2(&rewritten), 0);
}

#[test]
fn rewritten_pat_from_the_filter_parses_downstream() {
    let stream = multiplex(&three_program_pat());
    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);

    let pat_packet = out
        .chunks_exact(TS_PACKET_SIZE)
        .find(|p| (u16::from(p[1] & 0x1F) << 8) | u16::from(p[2]) == PAT_PID)
        .expect("a PAT was forwarded");
    // Pointer field at offset 4, section starts at offset 5.
    let pointer = usize::from(pat_packet[4]);
    let start = 5 + pointer;
    let section_length =
        3 + ((usize::from(pat_packet[start + 1] & 0x0F) << 8) | usize::from(pat_packet[start + 2]));
    let parsed = parse_pat(&pat_packet[start..start + section_length]).expect("parses");
    assert_eq!(parsed.programs, vec![(TARGET, TARGET_PMT)]);
}

// ---------------------------------------------------------------------------
// 3. TEI packets
// ---------------------------------------------------------------------------

#[test]
fn drops_packets_with_the_transport_error_indicator() {
    let mut broken = es_packet(TARGET_ES[0], 0, 0xEE);
    broken[1] |= 0x80; // transport_error_indicator

    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &TARGET_ES),
        0,
    ));
    stream.extend_from_slice(&broken);
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 1, 0x01));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);

    assert_eq!(filter.packets_dropped_tei, 1);
    let forwarded = out
        .chunks_exact(TS_PACKET_SIZE)
        .filter(|p| ((u16::from(p[1] & 0x1F) << 8) | u16::from(p[2])) == TARGET_ES[0])
        .count();
    assert_eq!(forwarded, 1, "only the healthy ES packet survives");
}

#[test]
fn a_tei_pat_is_not_rewritten() {
    let mut broken = section_packets(PAT_PID, &three_program_pat(), 0);
    broken[1] |= 0x80;

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&broken);
    assert!(out.is_empty());
    assert_eq!(filter.pat_rewrites, 0);
    assert_eq!(filter.packets_dropped_tei, 1);
}

// ---------------------------------------------------------------------------
// 4. PMT version changes
// ---------------------------------------------------------------------------

#[test]
fn pmt_version_change_rebuilds_the_pid_set() {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &[TARGET_ES[0]]),
        0,
    ));
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 0, 0x10));
    stream.extend_from_slice(&es_packet(TARGET_ES[1], 0, 0x11)); // not in the PMT yet
                                                                 // Version 1: TARGET_ES[0] removed, TARGET_ES[1] added.
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 1, TARGET_PCR, &[TARGET_ES[1]]),
        1,
    ));
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 1, 0x12));
    stream.extend_from_slice(&es_packet(TARGET_ES[1], 1, 0x13));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    let out_pids = pids(&out);

    assert_eq!(
        out_pids.iter().filter(|&&pid| pid == TARGET_ES[0]).count(),
        1,
        "the removed ES PID stops being forwarded"
    );
    assert_eq!(
        out_pids.iter().filter(|&&pid| pid == TARGET_ES[1]).count(),
        1,
        "the added ES PID is forwarded only after the new PMT"
    );
    assert_eq!(filter.pmt_updates, 2);
}

#[test]
fn a_pmt_for_another_programme_is_ignored() {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    // A PMT on the target PID that describes a different programme: it passes
    // through (the PID is allowed) but must not define ES PIDs.
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(OTHER_B, 0, OTHER_B_PCR, &[OTHER_B_ES]),
        0,
    ));
    stream.extend_from_slice(&es_packet(OTHER_B_ES, 0, 0x20));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    let out_pids = pids(&out);

    assert!(out_pids.contains(&TARGET_PMT));
    assert!(!out_pids.contains(&OTHER_B_ES));
    assert!(!out_pids.contains(&OTHER_B_PCR));
    assert_eq!(filter.pmt_updates, 0);
}

// ---------------------------------------------------------------------------
// 5. Before the first PMT
// ---------------------------------------------------------------------------

#[test]
fn before_the_first_pmt_only_fixed_pids_and_the_pmt_pid_pass() {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        NIT_PID,
        &section(0x40, TSID, 0, &[0xF0, 0x00]),
        0,
    ));
    stream.extend_from_slice(&es_packet(OTHER_A_ES, 0, 0x30));
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 0, 0x31));
    stream.extend_from_slice(&null_packet(0));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    let out_pids = pids(&out);

    assert!(out_pids.contains(&PAT_PID));
    assert!(out_pids.contains(&NIT_PID));
    assert!(!out_pids.contains(&OTHER_A_ES));
    assert!(
        !out_pids.contains(&TARGET_ES[0]),
        "even the target ES is unknown before its PMT"
    );
    assert!(!out_pids.contains(&NULL_PID));
    assert_eq!(filter.packets_dropped_pre_pmt, 2);
    assert_eq!(filter.packets_dropped_null, 1);

    // Once the PMT arrives, the target ES flows.
    let after = filter.push(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &TARGET_ES),
        0,
    ));
    assert!(pids(&after).contains(&TARGET_PMT));
    let later = filter.push(&es_packet(TARGET_ES[0], 1, 0x32));
    assert_eq!(pids(&later), vec![TARGET_ES[0]]);
}

#[test]
fn pmt_after_pat_only_needs_its_own_arrival() {
    // PMT PID must be forwarded before the PMT is parsed, otherwise the
    // receiver could never obtain it.
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &TARGET_ES),
        0,
    ));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    assert_eq!(pids(&out), vec![PAT_PID, TARGET_PMT]);
    assert_eq!(filter.packets_dropped_pre_pmt, 0);
}

// ---------------------------------------------------------------------------
// 6. Chunk boundaries
// ---------------------------------------------------------------------------

/// Deterministic LCG so the chunking test needs no dependency.
fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state >> 33
}

#[test]
fn chunk_boundaries_do_not_change_the_output() {
    let stream = multiplex(&three_program_pat());

    let mut whole = ServiceFilter::new(TARGET);
    let expected = whole.push(&stream);

    // One byte at a time.
    let mut bytewise = ServiceFilter::new(TARGET);
    let mut actual = Vec::new();
    for byte in &stream {
        actual.extend(bytewise.push(std::slice::from_ref(byte)));
    }
    assert_eq!(actual, expected);
    assert_eq!(bytewise.packets_in, whole.packets_in);
    assert_eq!(bytewise.packets_out, whole.packets_out);
    assert_eq!(bytewise.pat_rewrites, whole.pat_rewrites);
    assert_eq!(
        bytewise.packets_dropped_unknown_pid,
        whole.packets_dropped_unknown_pid
    );

    // Random chunk sizes.
    let mut random = ServiceFilter::new(TARGET);
    let mut actual = Vec::new();
    let mut state = 0x1234_5678_9ABC_DEF0u64;
    let mut offset = 0usize;
    while offset < stream.len() {
        let len = (lcg(&mut state) as usize % 700).max(1);
        let end = (offset + len).min(stream.len());
        actual.extend(random.push(&stream[offset..end]));
        offset = end;
    }
    assert_eq!(actual, expected);
}

#[test]
fn a_two_packet_pat_is_rewritten_to_a_single_packet() {
    // 45 programmes push the PAT over one packet (3 + 5 + 4 + 45*4 + 4 = 196).
    let programs: Vec<(u16, u16)> = (0..45)
        .map(|index| (0x0500 + index as u16, 0x0200 + index as u16))
        .collect();
    let (target, target_pmt) = programs[22];
    let pat = pat_section(&programs);

    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &pat, 0));
    // The PMT of the target, so its ES PIDs become known.
    stream.extend_from_slice(&section_packets(
        target_pmt,
        &pmt_section(target, 0, 0x0300, &[0x0301]),
        0,
    ));
    stream.extend_from_slice(&es_packet(0x0301, 0, 0x40));

    let mut filter = ServiceFilter::new(target);
    let out = filter.push(&stream);
    let out_pids = pids(&out);

    assert_eq!(out_pids.iter().filter(|&&pid| pid == PAT_PID).count(), 1);
    assert_eq!(filter.pat_rewrites, 1);
    assert_eq!(parse_pat_of(&out), vec![(target, target_pmt)]);
    assert!(out_pids.contains(&0x0301));

    // Feed the same stream one packet at a time; the result must match.
    let mut packets = ServiceFilter::new(target);
    let mut chunked = Vec::new();
    for packet in stream.chunks_exact(TS_PACKET_SIZE) {
        chunked.extend(packets.push(packet));
    }
    assert_eq!(chunked, out);
}

/// Parses the forwarded PAT out of a filtered stream.
fn parse_pat_of(stream: &[u8]) -> Vec<(u16, u16)> {
    for packet in stream.chunks_exact(TS_PACKET_SIZE) {
        let pid = (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2]);
        if pid != PAT_PID {
            continue;
        }
        let start = 5 + usize::from(packet[4]);
        let length =
            3 + ((usize::from(packet[start + 1] & 0x0F) << 8) | usize::from(packet[start + 2]));
        return parse_pat(&packet[start..start + length])
            .expect("pat")
            .programs;
    }
    panic!("no PAT in the filtered stream");
}

// ---------------------------------------------------------------------------
// 7. Unknown service
// ---------------------------------------------------------------------------

#[test]
fn a_service_absent_from_the_pat_is_reported_and_emits_no_pat() {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&es_packet(OTHER_A_ES, 0, 0x50));
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 0, 0x51));

    let mut filter = ServiceFilter::new(0x0999);
    let out = filter.push(&stream);

    // Documented behaviour: no PAT at all (neither the original, which would
    // leak other programmes, nor an empty one).
    assert!(pids(&out).is_empty());
    assert_eq!(filter.pat_rewrites, 0);
    assert_eq!(filter.missing_service_sections, 1);
    assert_eq!(filter.pmt_pid(), None);
    // Both groups' ES are unknown because no PMT was ever learned.
    assert_eq!(filter.packets_dropped_pre_pmt, 2);
    assert_eq!(filter.packets_out, 0);

    // A later PAT still without the service keeps the same behaviour.
    let more = filter.push(&section_packets(PAT_PID, &three_program_pat(), 1));
    assert!(more.is_empty());
    assert_eq!(filter.missing_service_sections, 2);
}

#[test]
fn a_pat_without_the_service_does_not_discard_learned_pmts() {
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &TARGET_ES),
        0,
    ));
    // A PAT from another multiplex (no TARGET) must not erase the PMT state.
    let other_pat = pat_section(&[(OTHER_A, OTHER_A_PMT)]);
    stream.extend_from_slice(&section_packets(PAT_PID, &other_pat, 1));
    stream.extend_from_slice(&es_packet(TARGET_ES[0], 0, 0x60));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    assert_eq!(filter.missing_service_sections, 1);
    assert_eq!(
        pids(&out)
            .iter()
            .filter(|&&pid| pid == TARGET_ES[0])
            .count(),
        1
    );
}

// ---------------------------------------------------------------------------
// Robustness: continuity errors and odd ordering
// ---------------------------------------------------------------------------

#[test]
fn a_corrupt_pat_continuation_does_not_panic() {
    let programs: Vec<(u16, u16)> = (0..45)
        .map(|index| (0x0500 + index as u16, 0x0200 + index as u16))
        .collect();
    let (target, target_pmt) = programs[22];
    let mut packets: Vec<Vec<u8>> = section_packets(PAT_PID, &pat_section(&programs), 0)
        .chunks_exact(TS_PACKET_SIZE)
        .map(|p| p.to_vec())
        .collect();
    // Break the continuity counter of the second PAT packet.
    packets[1][3] = 0x10 | ((packets[0][3] + 3) & 0x0F);

    let mut filter = ServiceFilter::new(target);
    let out = filter.push(&packets.concat());
    assert!(out.is_empty());
    assert_eq!(filter.dropped_pat_sections, 1);

    // A fresh, intact PAT recovers.
    let out = filter.push(&section_packets(PAT_PID, &pat_section(&programs), 2));
    assert_eq!(pids(&out), vec![PAT_PID]);
    assert_eq!(parse_pat_of(&out), vec![(target, target_pmt)]);
}

#[test]
fn duplicate_pat_packets_are_ignored() {
    let pat = section_packets(PAT_PID, &three_program_pat(), 0);
    let mut stream = pat.clone();
    stream.extend_from_slice(&pat); // same CC: a retransmission

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    assert_eq!(pids(&out), vec![PAT_PID]);
    assert_eq!(filter.pat_rewrites, 1);
}

#[test]
fn a_pmt_split_over_packets_is_assembled() {
    // Many ES PIDs stretch the PMT over two packets.
    let es: Vec<u16> = (0..40).map(|index| 0x0400 + index).collect();
    let mut stream = Vec::new();
    stream.extend_from_slice(&section_packets(PAT_PID, &three_program_pat(), 0));
    stream.extend_from_slice(&section_packets(
        TARGET_PMT,
        &pmt_section(TARGET, 0, TARGET_PCR, &es),
        0,
    ));
    stream.extend_from_slice(&es_packet(es[39], 0, 0x70));

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&stream);
    let out_pids = pids(&out);
    assert_eq!(
        out_pids.iter().filter(|&&pid| pid == TARGET_PMT).count(),
        2,
        "both PMT packets are forwarded"
    );
    assert!(
        out_pids.contains(&es[39]),
        "the second-packet ES PID is known"
    );
}

#[test]
fn a_pat_section_ending_before_a_pointer_field_is_completed() {
    // 45 programmes make the PAT 196 bytes, so it spans two packets. The
    // second packet starts with the 13-byte tail, then a following section
    // begins: the pointer field says where.
    let programs: Vec<(u16, u16)> = (0..45)
        .map(|index| (0x0500 + index as u16, 0x0200 + index as u16))
        .collect();
    let (target, target_pmt) = programs[22];
    let big = pat_section(&programs);
    assert!(big.len() > TS_PACKET_SIZE - 5);
    let (head, tail) = big.split_at(TS_PACKET_SIZE - 5);

    let mut stream = Vec::new();
    let mut first = vec![0x00];
    first.extend_from_slice(head);
    stream.extend_from_slice(&packet(PAT_PID, true, 0, &first));

    // Packet 2: pointer = tail length, tail bytes, then the target PMT.
    let pmt_packets = section_packets(target_pmt, &pmt_section(target, 0, 0x0300, &[0x0301]), 1);
    let mut second = vec![tail.len() as u8];
    second.extend_from_slice(tail);
    // Fill the rest of the packet with 0xFF stuffing so the PMT stays in the
    // next packet.
    second.resize(TS_PACKET_SIZE - 4, 0xFF);
    let mut second_packet = packet(PAT_PID, true, 1, &second);
    second_packet[4] = tail.len() as u8;
    stream.extend_from_slice(&second_packet);
    stream.extend_from_slice(&pmt_packets);

    let mut filter = ServiceFilter::new(target);
    let out = filter.push(&stream);
    assert_eq!(filter.pat_rewrites, 1);
    assert_eq!(parse_pat_of(&out), vec![(target, target_pmt)]);
    assert!(pids(&out).contains(&target_pmt));
}

#[test]
fn a_pat_spanning_packets_with_stuffing_is_completed() {
    let programs: Vec<(u16, u16)> = (0..45)
        .map(|index| (0x0500 + index as u16, 0x0200 + index as u16))
        .collect();
    let (target, target_pmt) = programs[22];
    let big = pat_section(&programs);
    let (head, tail) = big.split_at(TS_PACKET_SIZE - 5);

    let mut first = vec![0x00];
    first.extend_from_slice(head);
    let mut stream = packet(PAT_PID, true, 0, &first);

    let mut second = vec![0x00];
    second.extend_from_slice(tail);
    stream.extend_from_slice(&packet(PAT_PID, false, 1, &second));

    let mut filter = ServiceFilter::new(target);
    let out = filter.push(&stream);
    assert_eq!(filter.pat_rewrites, 1);
    assert_eq!(parse_pat_of(&out), vec![(target, target_pmt)]);
}

#[test]
fn malformed_input_is_counted_not_forwarded() {
    let mut broken = es_packet(TARGET_ES[0], 0, 0x80);
    broken[0] = 0x00; // no sync byte

    let mut filter = ServiceFilter::new(TARGET);
    let out = filter.push(&broken);
    assert!(out.is_empty());
    assert_eq!(filter.packets_dropped_malformed, 1);
}

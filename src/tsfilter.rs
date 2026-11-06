//! Built-in TSFilter for service streams (SPEC §5).
//!
//! A service stream must carry exactly one programme. This module is the
//! per-subscriber filter that turns the shared, post-pipeline multiplex into
//! that single-programme transport stream. One [`ServiceFilter`] instance is
//! created per service-stream subscriber, so two subscribers asking for
//! different services never interfere.
//!
//! # What passes
//!
//! * PID `0x0000` (PAT), rewritten to a single programme (see below).
//! * PID `0x0001` (CAT), `0x0010` (NIT) and `0x0011` (SDT/BAT), byte-identical.
//! * The target programme's PMT, whose PID is learned from the PAT.
//! * That PMT's `PCR_PID` and every elementary-stream PID it lists. The PID
//!   set is rebuilt whenever the PMT's `version_number` or its contents change.
//!
//! Everything else is dropped. Null packets (`0x1FFF`) and packets with the
//! transport error indicator set are dropped too.
//!
//! # PAT rewriting
//!
//! The rewritten PAT keeps `transport_stream_id`, the whole version /
//! `current_next_indicator` byte, and forces `section_number = 0`,
//! `last_section_number = 0`. It contains the target programme and, when the
//! original PAT had one, the `program_number == 0` network-PID entry (that
//! entry is not a programme; it identifies the NIT PID and must survive for
//! downstream SI to stay complete). The CRC32 (MPEG-2) is recomputed with
//! [`crate::si::crc32_mpeg2`].
//!
//! The rewritten PAT is a *new logical section stream*: several source packets
//! collapse into one output packet, so the source continuity counter cannot be
//! copied (a receiver would report a continuity error, e.g. CC 0 then 2). The
//! emitted CCs therefore come from a local counter for PID `0x0000`, starting
//! at 0 and incrementing for every emitted packet.
//!
//! [`rewrite_pat_for_service`] is the pure helper behind this, usable and
//! testable on a bare section.
//!
//! # Applicability
//!
//! Only tables with `current_next_indicator == 1` are applied: a next-version
//! PMT must not swap the PCR/ES PIDs before the table is in force (video would
//! stall during the switch), and a next-version PAT must not move the PMT PID
//! early. Such sections are counted in
//! [`ServiceFilter::ignored_next_version_pmt_sections`] and
//! [`ServiceFilter::ignored_next_version_pat_sections`] and never change the
//! forwarded PID set.
//!
//! # Decisions
//!
//! * **No target programme in the PAT**: nothing is emitted for that PAT
//!   (forwarding the original would leak other programmes, and an empty PAT
//!   would make downstream drop the service anyway). The event is counted in
//!   [`ServiceFilter::missing_service_sections`]. Previously learned PMT/ES
//!   knowledge is kept, so a transiently incomplete PAT does not tear down an
//!   otherwise healthy stream.
//! * **Before the first target PMT**: only the fixed PIDs and the target PMT
//!   PID (once the PAT has named it) pass. Other programmes' ES packets are
//!   dropped and counted in [`ServiceFilter::packets_dropped_pre_pmt`]. The PMT
//!   PID itself must pass so the receiver can obtain the PMT at all. The same
//!   rule applies again if a later PAT moves the service to another PMT PID.
//! * **PAT spanning several packets**: the packets are held until the section
//!   completes, then one packet carrying the (short) rewritten section is
//!   emitted in place of the group. This never delays the common single-packet
//!   PAT, which is rewritten and emitted immediately. A multi-section PAT is
//!   treated section by section: only the section that actually contains the
//!   target programme is forwarded.
//! * A PAT section that never completes (continuity gap, or a new section
//!   starting first) is dropped and counted in
//!   [`ServiceFilter::dropped_pat_sections`].
//!
//! The module is synchronous and makes no allocations beyond the buffers it
//! returns; it is intended to run inside the session's per-subscriber forward
//! path once the pipeline has produced aligned 188-byte packets.

use crate::si::{crc32_mpeg2, parse_pmt, SectionAssembler, PAT_PID};

/// Size of a normalized TS packet.
const TS_PACKET_SIZE: usize = 188;

/// The sync byte every TS packet starts with.
const SYNC_BYTE: u8 = 0x47;

/// PID of null packets, which carry nothing.
const NULL_PID: u16 = 0x1FFF;

/// Conditional Access Table PID, forwarded unchanged.
const CAT_PID: u16 = 0x0001;
/// Network Information Table PID, forwarded unchanged.
const NIT_PID: u16 = 0x0010;
/// Service Description Table / Bouquet Association Table PID.
const SDT_PID: u16 = 0x0011;

/// PIDs forwarded regardless of the PAT/PMT contents.
const FIXED_PIDS: [u16; 4] = [PAT_PID, CAT_PID, NIT_PID, SDT_PID];

/// Largest legal `section_length` plus the 3-byte section header.
const MAX_SECTION_LEN: usize = 4096;

/// Table id of the Program Association Table.
const TABLE_ID_PAT: u8 = 0x00;

/// Fields of a 188-byte TS packet this module needs.
#[derive(Clone, Copy, Debug)]
struct PacketHeader {
    pid: u16,
    payload_unit_start: bool,
    transport_error: bool,
    continuity_counter: u8,
    payload_offset: Option<usize>,
}

/// Parses the header of a 188-byte packet, or returns `None` when the packet is
/// malformed (wrong length or no sync byte).
fn parse_header(packet: &[u8]) -> Option<PacketHeader> {
    if packet.len() < TS_PACKET_SIZE || packet[0] != SYNC_BYTE {
        return None;
    }
    let pid = (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2]);
    let payload_offset = match (packet[3] >> 4) & 0x03 {
        0x01 => Some(4),
        0x03 => {
            let offset = 5 + usize::from(packet[4]);
            (offset < TS_PACKET_SIZE).then_some(offset)
        }
        // 0x00 is reserved and 0x02 is "adaptation field only": no payload.
        _ => None,
    };
    Some(PacketHeader {
        pid,
        payload_unit_start: packet[1] & 0x40 != 0,
        transport_error: packet[1] & 0x80 != 0,
        continuity_counter: packet[3] & 0x0F,
        payload_offset,
    })
}

/// Returns the programme-loop bytes of a complete PAT section, or `None` when
/// the section is not a well-formed PAT.
///
/// The input CRC is not checked: the assembler only ever hands over complete
/// sections, and a rewrite replaces the checksum anyway.
fn pat_body(section: &[u8]) -> Option<&[u8]> {
    if section.len() < 12 || section[0] != TABLE_ID_PAT || section[1] & 0x80 == 0 {
        return None;
    }
    let total = 3 + ((usize::from(section[1] & 0x0F) << 8) | usize::from(section[2]));
    if total != section.len() || total > MAX_SECTION_LEN {
        return None;
    }
    let body = &section[8..total - 4];
    if body.len() % 4 != 0 {
        return None;
    }
    Some(body)
}

/// Finds the PMT PID of `service_id` in a complete PAT section.
fn find_program_pid(section: &[u8], service_id: u16) -> Option<u16> {
    if service_id == 0 {
        // Programme number zero is the network PID, never a service.
        return None;
    }
    let body = pat_body(section)?;
    body.chunks_exact(4).find_map(|entry| {
        let program_number = u16::from_be_bytes([entry[0], entry[1]]);
        (program_number == service_id)
            .then(|| (u16::from(entry[2] & 0x1F) << 8) | u16::from(entry[3]))
    })
}

/// Rewrites a PAT section so that it announces only `service_id`.
///
/// The result preserves `table_id`, `transport_stream_id`, the version /
/// `current_next_indicator` byte ([`section[5]`]) and the `program_number == 0`
/// network-PID entry, and forces `section_number = 0` and
/// `last_section_number = 0`. The trailing CRC32 is recomputed over the new
/// section.
///
/// Returns `None` when the input is not a complete PAT section or does not
/// contain `service_id`.
///
/// ```
/// use unituner::si::crc32_mpeg2;
/// use unituner::tsfilter::rewrite_pat_for_service;
///
/// // A one-programme PAT: tsid 0x7FE0, version 0, programme 0x0408 -> PID 0x1F0.
/// let section = [
///     0x00, 0xB0, 0x0D, 0x7F, 0xE0, 0xC1, 0x00, 0x00, 0x04, 0x08, 0xE1, 0xF0,
///     0x00, 0x00, 0x00, 0x00,
/// ];
/// // The sample has a zero CRC; the rewrite computes the real one.
/// let rewritten = rewrite_pat_for_service(&section, 0x0408).unwrap();
/// assert_eq!(crc32_mpeg2(&rewritten[..rewritten.len() - 4]),
///            u32::from_be_bytes(rewritten[rewritten.len() - 4..].try_into().unwrap()));
/// assert!(rewrite_pat_for_service(&section, 0x0409).is_none());
/// ```
pub fn rewrite_pat_for_service(section_bytes: &[u8], service_id: u16) -> Option<Vec<u8>> {
    if service_id == 0 {
        return None;
    }
    let body = pat_body(section_bytes)?;

    let mut entries: Vec<[u8; 4]> = Vec::new();
    let mut found = false;
    for entry in body.chunks_exact(4) {
        let program_number = u16::from_be_bytes([entry[0], entry[1]]);
        if program_number == 0 || program_number == service_id {
            entries.push([entry[0], entry[1], entry[2], entry[3]]);
            found |= program_number == service_id;
        }
    }
    if !found {
        return None;
    }

    // 5 bytes: transport_stream_id (2), version/current_next (1),
    // section_number (1), last_section_number (1).
    let section_length = 5 + entries.len() * 4 + 4;
    let mut out = Vec::with_capacity(3 + section_length);
    out.push(TABLE_ID_PAT);
    out.push(0xB0 | ((section_length >> 8) as u8 & 0x0F));
    out.push(section_length as u8);
    out.extend_from_slice(&section_bytes[3..5]); // transport_stream_id
    out.push(section_bytes[5]); // reserved bits, version, current_next
    out.push(0x00); // section_number
    out.push(0x00); // last_section_number
    for entry in &entries {
        out.extend_from_slice(entry);
    }
    let crc = crc32_mpeg2(&out);
    out.extend_from_slice(&crc.to_be_bytes());
    Some(out)
}

/// Reassembly state of the PAT PID.
#[derive(Debug, Default)]
struct PatState {
    /// Section bytes collected so far.
    partial: Vec<u8>,
    /// Section length once the header has arrived.
    expected: Option<usize>,
    /// The packet that started the section, kept as the template for the
    /// rewritten output. `None` when no section is in progress.
    template: Option<Vec<u8>>,
    /// Payload offset of [`template`](Self::template).
    template_offset: usize,
    /// Continuity counter of the last PAT packet with a payload.
    last_cc: Option<u8>,
    /// Continuity counter of the last PAT packet this filter emitted. The
    /// rewritten PAT is a new logical section stream, so its CCs increment
    /// locally instead of copying the (possibly skipped) source counters.
    out_cc: Option<u8>,
}

impl PatState {
    /// Forgets the section in progress but keeps the continuity counters.
    fn abandon(&mut self) {
        self.partial.clear();
        self.expected = None;
        self.template = None;
        self.template_offset = 0;
    }

    /// Whether a section is currently being assembled.
    fn in_progress(&self) -> bool {
        self.template.is_some()
    }
}

/// Per-subscriber service filter (SPEC §5).
///
/// Feed the post-pipeline packets with [`push`](Self::push); only the packets
/// belonging to the configured service (plus the fixed SI PIDs) come back.
#[derive(Debug)]
pub struct ServiceFilter {
    service_id: u16,
    /// Target programme's PMT PID, learned from the PAT.
    pmt_pid: Option<u16>,
    /// Target programme's `PCR_PID`, learned from the PMT.
    pcr_pid: Option<u16>,
    /// Elementary-stream PIDs of the target programme.
    es_pids: Vec<u16>,
    /// `version_number` of the PMT currently applied.
    pmt_version: Option<u8>,
    /// Whether a PMT for the target programme has been seen.
    pmt_seen: bool,
    pat: PatState,
    pmt_assembler: SectionAssembler,
    /// Bytes of a trailing partial packet, kept for the next push.
    buf: Vec<u8>,

    /// Packets offered to the filter.
    pub packets_in: u64,
    /// Packets forwarded.
    ///
    /// A multi-packet PAT collapses into a single rewritten packet, so this
    /// can be lower than `packets_in` by more than the number of dropped
    /// packets.
    pub packets_out: u64,
    /// Packets dropped because their PID is neither fixed, the PMT, nor part
    /// of the target programme (counted only after the first PMT).
    pub packets_dropped_unknown_pid: u64,
    /// Packets dropped before the target programme's first PMT was seen.
    pub packets_dropped_pre_pmt: u64,
    /// Null packets (`0x1FFF`) dropped.
    pub packets_dropped_null: u64,
    /// Packets dropped because the transport error indicator was set.
    pub packets_dropped_tei: u64,
    /// Packets dropped because they were malformed (no sync byte).
    pub packets_dropped_malformed: u64,
    /// PAT sections rewritten and emitted.
    pub pat_rewrites: u64,
    /// Times the allowed PMT/PCR/ES PID set was (re)built.
    pub pmt_updates: u64,
    /// PAT sections that did not contain the configured service.
    pub missing_service_sections: u64,
    /// PAT sections abandoned before they completed.
    pub dropped_pat_sections: u64,
    /// PAT sections ignored because `current_next_indicator` was 0 (a
    /// next-version table: it must not move the PMT PID before it is in force).
    pub ignored_next_version_pat_sections: u64,
    /// PMT sections ignored because `current_next_indicator` was 0. Applying
    /// one early would swap the PCR/ES PID set before the version is in force.
    pub ignored_next_version_pmt_sections: u64,
}

impl ServiceFilter {
    /// Creates a filter for the service whose ISDB service id is `service_id`.
    pub fn new(service_id: u16) -> Self {
        Self {
            service_id,
            pmt_pid: None,
            pcr_pid: None,
            es_pids: Vec::new(),
            pmt_version: None,
            pmt_seen: false,
            pat: PatState::default(),
            pmt_assembler: SectionAssembler::new(),
            buf: Vec::new(),
            packets_in: 0,
            packets_out: 0,
            packets_dropped_unknown_pid: 0,
            packets_dropped_pre_pmt: 0,
            packets_dropped_null: 0,
            packets_dropped_tei: 0,
            packets_dropped_malformed: 0,
            pat_rewrites: 0,
            pmt_updates: 0,
            missing_service_sections: 0,
            dropped_pat_sections: 0,
            ignored_next_version_pat_sections: 0,
            ignored_next_version_pmt_sections: 0,
        }
    }

    /// The service id this filter was created for.
    pub fn service_id(&self) -> u16 {
        self.service_id
    }

    /// The PMT PID currently associated with the service, if the PAT named it.
    pub fn pmt_pid(&self) -> Option<u16> {
        self.pmt_pid
    }

    /// `version_number` of the PMT currently applied, if one was applied.
    ///
    /// A next-version PMT (`current_next_indicator == 0`) never changes this.
    pub fn pmt_version(&self) -> Option<u8> {
        self.pmt_version
    }

    /// Feeds a run of 188-byte packets and returns the packets to forward.
    ///
    /// `packets` should hold a whole number of packets. Trailing partial bytes
    /// (fewer than 188) are buffered and completed by a later call, so chunk
    /// boundaries never change the output.
    pub fn push(&mut self, packets: &[u8]) -> Vec<u8> {
        let mut buf = std::mem::take(&mut self.buf);
        buf.extend_from_slice(packets);
        let complete = buf.len() - buf.len() % TS_PACKET_SIZE;

        let mut out = Vec::new();
        for packet in buf[..complete].chunks_exact(TS_PACKET_SIZE) {
            self.process(packet, &mut out);
        }
        buf.drain(..complete);
        self.buf = buf;
        out
    }

    /// Handles one 188-byte packet.
    fn process(&mut self, packet: &[u8], out: &mut Vec<u8>) {
        self.packets_in += 1;
        let Some(header) = parse_header(packet) else {
            self.packets_dropped_malformed += 1;
            return;
        };
        if header.transport_error {
            self.packets_dropped_tei += 1;
            return;
        }
        if header.pid == NULL_PID {
            self.packets_dropped_null += 1;
            return;
        }
        if header.pid == PAT_PID {
            self.handle_pat(packet, &header, out);
            return;
        }
        if Some(header.pid) == self.pmt_pid {
            if header.payload_offset.is_some() {
                for section in self.pmt_assembler.push_packet(packet) {
                    self.apply_pmt_section(&section.bytes);
                }
            }
            out.extend_from_slice(packet);
            self.packets_out += 1;
            return;
        }
        if self.is_allowed(header.pid) {
            out.extend_from_slice(packet);
            self.packets_out += 1;
        } else if self.pmt_seen {
            self.packets_dropped_unknown_pid += 1;
        } else {
            self.packets_dropped_pre_pmt += 1;
        }
    }

    /// Whether `pid` should be forwarded according to the current knowledge.
    fn is_allowed(&self, pid: u16) -> bool {
        FIXED_PIDS.contains(&pid)
            || Some(pid) == self.pmt_pid
            || Some(pid) == self.pcr_pid
            || self.es_pids.contains(&pid)
    }

    /// Buffers PAT packets and emits the rewritten PAT once its section is
    /// complete.
    fn handle_pat(&mut self, packet: &[u8], header: &PacketHeader, out: &mut Vec<u8>) {
        let Some(offset) = header.payload_offset else {
            // Adaptation field only: no section bytes, nothing to do.
            return;
        };
        let payload = &packet[offset..];
        if payload.is_empty() {
            return;
        }

        if let Some(last) = self.pat.last_cc {
            if header.continuity_counter == last {
                // A retransmitted packet carries nothing new.
                return;
            }
            if self.pat.in_progress() && header.continuity_counter != last.wrapping_add(1) & 0x0F {
                self.pat.abandon();
                self.dropped_pat_sections += 1;
            }
        }
        self.pat.last_cc = Some(header.continuity_counter);

        if header.payload_unit_start {
            let pointer = usize::from(payload[0]);
            let start = 1 + pointer;
            if start > payload.len() {
                // The pointer field points past the packet: unusable.
                if self.pat.in_progress() {
                    self.pat.abandon();
                    self.dropped_pat_sections += 1;
                }
                return;
            }

            // Bytes before the pointer finish the section still in progress.
            if self.pat.in_progress() && pointer > 0 {
                self.continue_pat_section(&payload[1..start], out);
            }
            if self.pat.in_progress() {
                // A new section starts here, so this one is abandoned.
                self.pat.abandon();
                self.dropped_pat_sections += 1;
            }

            // A PAT may carry several sections, and each may span packets.
            self.walk_pat_sections(&payload[start..], packet, offset, out);
        } else if self.pat.in_progress() {
            let used = self.continue_pat_section(payload, out);
            if used < payload.len() {
                // The section in progress completed inside this packet and a
                // second section starts right after it: handle it as if the
                // packet had begun with a pointer field pointing here.
                self.walk_pat_sections(&payload[used..], packet, offset, out);
            }
        }
    }

    /// Feeds `bytes` to the PAT section in progress and emits it if it has now
    /// fully arrived.
    ///
    /// Returns the number of bytes consumed: fewer than `bytes.len()` only when
    /// the section completed and the rest of `bytes` belongs to a following
    /// section (a continuation packet may carry several sections).
    fn continue_pat_section(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize {
        let mut used = 0;
        if self.pat.expected.is_none() {
            // The section header itself was split across packets.
            let take = (3 - self.pat.partial.len()).min(bytes.len());
            self.pat.partial.extend_from_slice(&bytes[..take]);
            used += take;
            if self.pat.partial.len() < 3 {
                return used;
            }
            let total = 3
                + ((usize::from(self.pat.partial[1] & 0x0F) << 8)
                    | usize::from(self.pat.partial[2]));
            if !(12..=MAX_SECTION_LEN).contains(&total) {
                // A corrupt section length: drop it and do not try to read the
                // rest of the packet as another section (its boundaries are
                // unknown now).
                self.pat.abandon();
                self.dropped_pat_sections += 1;
                return bytes.len();
            }
            self.pat.expected = Some(total);
        }

        let expected = self.pat.expected.unwrap_or(0);
        let need = expected.saturating_sub(self.pat.partial.len());
        let take = need.min(bytes.len() - used);
        self.pat
            .partial
            .extend_from_slice(&bytes[used..used + take]);
        used += take;
        if self.pat.partial.len() < expected {
            return used;
        }

        let section = self.pat.partial[..expected].to_vec();
        let template = self.pat.template.take();
        let template_offset = self.pat.template_offset;
        self.pat.abandon();
        if let Some(template) = template {
            self.emit_pat(&section, &template, template_offset, out);
        }
        used
    }

    /// Walks a run of complete or partial PAT sections, emitting the complete
    /// ones and buffering the last, incomplete one.
    fn walk_pat_sections(
        &mut self,
        rest: &[u8],
        packet: &[u8],
        payload_offset: usize,
        out: &mut Vec<u8>,
    ) {
        let mut rest = rest;
        while !rest.is_empty() && rest[0] != 0xFF {
            if rest.len() < 3 {
                // The header is split across packets: buffer what there is and
                // let the continuation path compute the length.
                self.pat.template = Some(packet.to_vec());
                self.pat.template_offset = payload_offset;
                self.pat.partial.extend_from_slice(rest);
                return;
            }
            let total = 3 + ((usize::from(rest[1] & 0x0F) << 8) | usize::from(rest[2]));
            if !(12..=MAX_SECTION_LEN).contains(&total) {
                return;
            }
            if rest.len() >= total {
                self.emit_pat(&rest[..total], packet, payload_offset, out);
                rest = &rest[total..];
            } else {
                self.pat.expected = Some(total);
                self.pat.template = Some(packet.to_vec());
                self.pat.template_offset = payload_offset;
                self.pat.partial.extend_from_slice(rest);
                return;
            }
        }
    }

    /// Rewrites `section` and emits it in place of `template`.
    ///
    /// The packet's header is preserved except for `payload_unit_start_indicator`
    /// and the continuity counter, which come from a local sequence for PID
    /// `0x0000`: the rewritten PAT is a new logical section stream, so its first
    /// packet is CC 0 and every following one increments by one. Copying the
    /// source counters would emit jumps (0, 2, 4, …) whenever a multi-packet PAT
    /// collapses into one packet, which strict demuxers report as continuity
    /// errors. PUSI is set because every emitted packet starts a section, even
    /// when the template was a continuation packet. The payload is replaced by a
    /// zero pointer field, the rewritten section, and `0xFF` stuffing.
    ///
    /// A section with `current_next_indicator == 0` is a next-version table: it
    /// is counted and not emitted, so a version switch cannot move the service
    /// to another PMT PID before the new PAT is in force.
    fn emit_pat(
        &mut self,
        section: &[u8],
        template: &[u8],
        payload_offset: usize,
        out: &mut Vec<u8>,
    ) {
        if section.get(5).is_some_and(|byte| byte & 0x01 == 0) {
            self.ignored_next_version_pat_sections += 1;
            return;
        }
        let Some(rewritten) = rewrite_pat_for_service(section, self.service_id) else {
            self.missing_service_sections += 1;
            return;
        };
        let capacity = TS_PACKET_SIZE.saturating_sub(payload_offset + 1);
        if rewritten.len() > capacity {
            // Should be impossible for a single programme; never emit a torn
            // section.
            self.dropped_pat_sections += 1;
            return;
        }
        let mut packet = template.to_vec();
        let cc = match self.pat.out_cc {
            Some(cc) => cc.wrapping_add(1) & 0x0F,
            None => 0,
        };
        self.pat.out_cc = Some(cc);
        packet[3] = (packet[3] & 0xF0) | cc;
        // Every emitted packet begins a section, even when the template was a
        // continuation packet of the original (PUSI clear).
        packet[1] |= 0x40;
        packet[payload_offset] = 0x00;
        packet[payload_offset + 1..payload_offset + 1 + rewritten.len()]
            .copy_from_slice(&rewritten);
        packet[payload_offset + 1 + rewritten.len()..].fill(0xFF);
        out.extend_from_slice(&packet);
        self.packets_out += 1;
        self.pat_rewrites += 1;

        if let Some(pmt_pid) = find_program_pid(section, self.service_id) {
            self.set_pmt_pid(pmt_pid);
        }
    }

    /// Records a PMT PID learned from the PAT, resetting PMT-derived state when
    /// it changes.
    fn set_pmt_pid(&mut self, pmt_pid: u16) {
        if self.pmt_pid == Some(pmt_pid) {
            return;
        }
        self.pmt_pid = Some(pmt_pid);
        self.pmt_assembler.reset();
        self.pcr_pid = None;
        self.es_pids.clear();
        self.pmt_version = None;
        self.pmt_seen = false;
    }

    /// Applies a complete PMT section when it describes the target programme
    /// and is the version currently in force.
    fn apply_pmt_section(&mut self, section: &[u8]) {
        let Ok(pmt) = parse_pmt(section) else {
            return;
        };
        if pmt.program_number != self.service_id {
            return;
        }
        if !pmt.current_next {
            // A next-version PMT: applying it now would swap the PCR/ES PIDs
            // before the version is in force and stall the video.
            self.ignored_next_version_pmt_sections += 1;
            return;
        }

        let version = Some(pmt.version_number);
        let mut streams: Vec<u16> = pmt
            .streams
            .iter()
            .map(|&(_, pid)| pid)
            .filter(|&pid| pid != NULL_PID)
            .collect();
        streams.sort_unstable();
        streams.dedup();

        let changed = !self.pmt_seen
            || self.pmt_version != version
            || self.pcr_pid != Some(pmt.pcr_pid)
            || self.es_pids != streams;
        if changed {
            self.pmt_seen = true;
            self.pmt_version = version;
            self.pcr_pid = Some(pmt.pcr_pid);
            self.es_pids = streams;
            self.pmt_updates += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal PAT section with the given `(program_number, pmt_pid)` entries.
    fn pat_section(programs: &[(u16, u16)]) -> Vec<u8> {
        let mut rest = Vec::new();
        rest.extend_from_slice(&[0x00, 0x00]); // network PID entry
        rest.extend_from_slice(&(0xE000 | NIT_PID).to_be_bytes());
        for &(program, pmt_pid) in programs {
            rest.extend_from_slice(&program.to_be_bytes());
            rest.extend_from_slice(&(0xE000 | pmt_pid).to_be_bytes());
        }
        let section_length = 5 + rest.len() + 4;
        let mut section = vec![
            0x00,
            0xB0 | ((section_length >> 8) as u8 & 0x0F),
            section_length as u8,
            0x7F,
            0xE0,
            0xC1,
            0x00,
            0x00,
        ];
        section.extend_from_slice(&rest);
        let crc = crc32_mpeg2(&section);
        section.extend_from_slice(&crc.to_be_bytes());
        section
    }

    #[test]
    fn rewrite_keeps_the_target_programme_and_the_network_entry() {
        let section = pat_section(&[(0x0400, 0x0100), (0x0401, 0x0101)]);
        let rewritten = rewrite_pat_for_service(&section, 0x0401).unwrap();

        assert_eq!(&rewritten[3..5], &section[3..5]); // tsid
        assert_eq!(rewritten[5], section[5]); // version / current_next
        assert_eq!(rewritten[6], 0); // section_number
        assert_eq!(rewritten[7], 0); // last_section_number
                                     // Network entry plus one programme.
        assert_eq!(rewritten.len(), 3 + 5 + 2 * 4 + 4);
        // CRC covers the section minus its trailing 4 bytes.
        let crc = u32::from_be_bytes(rewritten[rewritten.len() - 4..].try_into().unwrap());
        assert_eq!(crc, crc32_mpeg2(&rewritten[..rewritten.len() - 4]));
    }

    #[test]
    fn rewrite_returns_none_when_the_service_is_absent() {
        let section = pat_section(&[(0x0400, 0x0100)]);
        assert!(rewrite_pat_for_service(&section, 0x0409).is_none());
        assert!(rewrite_pat_for_service(&section, 0).is_none());
        assert!(rewrite_pat_for_service(&[], 0x0400).is_none());
    }
}

//! PSI/SI section assembly and parsing (SPEC §8).
//!
//! The tuner output is a stream of 188-byte TS packets; PSI/SI tables arrive
//! as *sections* spread over one or more packets of a PID, delimited by
//! `payload_unit_start_indicator` and a `pointer_field`, and protected by an
//! MPEG-2 CRC32.
//!
//! This module does that assembly itself ([`SectionAssembler`]) and then hands
//! the complete section bytes to the `arib` crate's STD-B10 parser
//! (`arib::ts::table::Table::read`). The `arib` crate expects a bare section,
//! so pointer-field handling must happen here first.
//!
//! Only the four tables SPEC §8 needs are turned into typed values: PAT, PMT,
//! SDT and NIT. [`parse_section`] rejects any other table id, and
//! [`SiParser`] ignores such sections without treating them as errors.
//!
//! # CRC32
//!
//! [`crc32_mpeg2`] is public because SPEC §5 requires the same checksum when
//! rewriting a PAT for a service stream (phase 5).

use std::collections::HashMap;

use arib::ts::descriptor::Descriptor;
use arib::ts::table::{ProgramAssociation, Table};

/// PID of the Program Association Table.
pub const PAT_PID: u16 = 0x0000;
/// Conventional PID of the Network Information Table. The authoritative source
/// is the PAT's program-number-zero entry; this constant is only the usual
/// value and is convenient for tests and default subscriptions.
pub const NIT_PID: u16 = 0x0010;
/// PID of the Service Description Table.
pub const SDT_PID: u16 = 0x0011;

/// The sync byte every TS packet starts with.
const SYNC_BYTE: u8 = 0x47;
/// Size of one normalized TS packet.
const TS_PACKET_SIZE: usize = 188;
/// Largest legal `section_length` plus the 3-byte section header.
const MAX_SECTION_LEN: usize = 4096;

/// Table id of the Program Association Table.
const TABLE_ID_PAT: u8 = 0x00;
/// Table id of a Program Map Table.
const TABLE_ID_PMT: u8 = 0x02;
/// Table id of a NIT (actual or other).
const TABLE_ID_NIT_ACTUAL: u8 = 0x40;
const TABLE_ID_NIT_OTHER: u8 = 0x41;
/// Table id of an SDT (actual or other).
const TABLE_ID_SDT_ACTUAL: u8 = 0x42;
const TABLE_ID_SDT_OTHER: u8 = 0x46;
/// Descriptor tag of the TS information descriptor (ARIB STD-B10), which
/// carries `remote_control_key_id`. The `arib` crate keeps it as
/// `Descriptor::Unknown`.
const DESCRIPTOR_TAG_TS_INFORMATION: u8 = 0xCD;

/// Computes the CRC32 used by MPEG-2 systems and ARIB STD-B10 sections.
///
/// Polynomial `0x04C11DB7`, initial value `0xFFFFFFFF`, most significant bit
/// first, no input/output reflection and no final XOR. A section whose last
/// four bytes hold this value over the whole section has a CRC of zero, which
/// is how [`SectionAssembler`] consumers verify it.
///
/// ```
/// assert_eq!(unituner::si::crc32_mpeg2(b"123456789"), 0x0376_E6E7);
/// ```
pub fn crc32_mpeg2(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in bytes {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Decodes ARIB STD-B24 8-bit text into a `String`.
///
/// Wraps `arib::text::decode` so callers never have to care about the
/// dependency. Invalid glyphs decode to the Unicode replacement character and
/// undecodable escapes are skipped; the surrounding whitespace and NUL padding
/// that service names often carry is trimmed.
///
/// A panic inside the dependency is caught and turned into an empty string, so
/// a corrupt service name can never take down a scan.
pub fn decode_arib_text(bytes: &[u8]) -> String {
    let decoded = std::panic::catch_unwind(|| arib::text::decode(bytes)).unwrap_or_default();
    decoded
        .trim_matches(|c: char| c == '\0' || c.is_whitespace())
        .to_owned()
}

/// Programme information read from a PAT section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatInfo {
    /// `transport_stream_id` of the multiplex.
    pub transport_stream_id: u16,
    /// Network id, when the caller could supply one.
    ///
    /// A PAT itself carries only a network *PID* (the program-number-zero
    /// entry), never a network id, so [`parse_pat`] leaves this `None`.
    /// [`SiParser`] fills it in from the most recently parsed NIT, which is
    /// why the type is optional.
    pub network_id: Option<u16>,
    /// `(program_number, PMT PID)` for every real programme. The
    /// network-PID entry (`program_number == 0`) is not listed.
    pub programs: Vec<(u16, u16)>,
}

/// Stream information read from a PMT section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PmtInfo {
    /// Programme this PMT describes, equal to the service id on ISDB.
    pub program_number: u16,
    /// PID carrying the programme clock reference.
    pub pcr_pid: u16,
    /// `(stream_type, elementary_pid)` for every elementary stream.
    pub streams: Vec<(u8, u16)>,
}

/// Service information read from an SDT section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SdtInfo {
    /// Network the multiplex originally belongs to.
    pub original_network_id: u16,
    /// `transport_stream_id` of the multiplex.
    pub transport_stream_id: u16,
    /// `(service_id, service_type, name)` for every service in the section.
    /// The name is already ARIB-decoded.
    pub services: Vec<(u16, u8, String)>,
}

/// Network information read from a NIT section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NitInfo {
    /// `network_id` of the network.
    pub network_id: u16,
    /// Remote-control key ("channel number on the remote") for the transport
    /// stream, from the TS information descriptor (tag `0xCD`).
    pub remote_control_key_id: Option<u8>,
}

/// A parsed SI table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SiEvent {
    /// A Program Association Table section.
    Pat(PatInfo),
    /// A Program Map Table section.
    Pmt(PmtInfo),
    /// A Service Description Table section.
    Sdt(SdtInfo),
    /// A Network Information Table section.
    Nit(NitInfo),
}

/// Why a section could not be parsed.
#[derive(Debug, thiserror::Error)]
pub enum SiError {
    /// The section is shorter than its header claims, or longer.
    #[error("invalid section length")]
    BadLength,
    /// The CRC32 does not match the section contents.
    #[error("bad section CRC32")]
    BadCrc,
    /// The section holds a table id this parser does not handle.
    #[error("unexpected table id {0:#04x}")]
    UnexpectedTable(u8),
    /// The `arib` crate rejected the section.
    #[error("ARIB parse error: {0}")]
    Parse(String),
}

/// Validates a complete section: header length consistency and CRC32.
fn validate_section(section: &[u8]) -> Result<(), SiError> {
    if section.len() < 3 {
        return Err(SiError::BadLength);
    }
    let total = 3 + (usize::from(section[1] & 0x0F) << 8 | usize::from(section[2]));
    if section.len() != total || total > MAX_SECTION_LEN {
        return Err(SiError::BadLength);
    }
    // Only sections with the syntax indicator carry a CRC (ISO/IEC 13818-1).
    if section[1] & 0x80 != 0 && crc32_mpeg2(section) != 0 {
        return Err(SiError::BadCrc);
    }
    Ok(())
}

/// Parses a section with the `arib` crate after validating it.
fn read_table(section: &[u8]) -> Result<Table, SiError> {
    validate_section(section)?;
    let mut bytes = section.to_vec().into();
    Table::read(&mut bytes).map_err(|error| SiError::Parse(error.to_string()))
}

/// Parses a complete PAT section.
pub fn parse_pat(section: &[u8]) -> Result<PatInfo, SiError> {
    expect_table_id(section, &[TABLE_ID_PAT])?;
    let Table::Pat(pat) = read_table(section)? else {
        return Err(SiError::Parse("not a PAT".to_owned()));
    };
    let programs = pat
        .programs
        .into_iter()
        .filter_map(|association| match association {
            ProgramAssociation::ProgramMap {
                program_number,
                program_map_pid,
            } if program_number != 0 => Some((program_number, program_map_pid)),
            _ => None,
        })
        .collect();
    Ok(PatInfo {
        transport_stream_id: pat.transport_stream_id,
        network_id: None,
        programs,
    })
}

/// Parses a complete PMT section.
pub fn parse_pmt(section: &[u8]) -> Result<PmtInfo, SiError> {
    expect_table_id(section, &[TABLE_ID_PMT])?;
    let Table::Pmt(pmt) = read_table(section)? else {
        return Err(SiError::Parse("not a PMT".to_owned()));
    };
    let streams = pmt
        .streams
        .into_iter()
        .filter(|stream| stream.elementary_pid != 0x1FFF)
        .map(|stream| (stream.stream_type, stream.elementary_pid))
        .collect();
    Ok(PmtInfo {
        program_number: pmt.program_number,
        pcr_pid: pmt.pcr_pid,
        streams,
    })
}

/// Parses a complete SDT section.
pub fn parse_sdt(section: &[u8]) -> Result<SdtInfo, SiError> {
    expect_table_id(section, &[TABLE_ID_SDT_ACTUAL, TABLE_ID_SDT_OTHER])?;
    let Table::Sdt(sdt) = read_table(section)? else {
        return Err(SiError::Parse("not an SDT".to_owned()));
    };
    let services = sdt
        .services
        .into_iter()
        .map(|service| {
            let mut service_type = 0u8;
            let mut name = String::new();
            for descriptor in &service.descriptors {
                if let Descriptor::Service(descriptor) = descriptor {
                    service_type = descriptor.service_type;
                    name = decode_arib_text(&descriptor.service_name);
                    break;
                }
            }
            (service.service_id, service_type, name)
        })
        .collect();
    Ok(SdtInfo {
        original_network_id: sdt.original_network_id,
        transport_stream_id: sdt.transport_stream_id,
        services,
    })
}

/// Parses a complete NIT section.
///
/// `remote_control_key_id` comes from the **TS information descriptor**, ARIB
/// STD-B10 tag `0xCD`, whose first byte after the tag/length is the key id.
/// (The task brief suggested tag `0xFA`; that is the terrestrial delivery
/// system descriptor, which carries no key id, so `0xCD` is used instead.)
/// The `arib` crate has no typed variant for tag `0xCD` and hands it back as
/// `Descriptor::Unknown`, so the byte is read out of the raw body here.
///
/// The descriptor is looked for among the network descriptors first and then
/// among the transport-stream descriptors; the first one found wins.
pub fn parse_nit(section: &[u8]) -> Result<NitInfo, SiError> {
    expect_table_id(section, &[TABLE_ID_NIT_ACTUAL, TABLE_ID_NIT_OTHER])?;
    let Table::Nit(nit) = read_table(section)? else {
        return Err(SiError::Parse("not a NIT".to_owned()));
    };
    let remote_control_key_id = nit
        .descriptors
        .iter()
        .chain(nit.transport_streams.iter().flat_map(|ts| &ts.descriptors))
        .find_map(remote_control_key_id_of);
    Ok(NitInfo {
        network_id: nit.network_id,
        remote_control_key_id,
    })
}

/// Fails with [`SiError::UnexpectedTable`] unless the table id is one of `ids`.
fn expect_table_id(section: &[u8], ids: &[u8]) -> Result<(), SiError> {
    let table_id = section.first().copied().ok_or(SiError::BadLength)?;
    if ids.contains(&table_id) {
        Ok(())
    } else {
        Err(SiError::UnexpectedTable(table_id))
    }
}

/// Reads `remote_control_key_id` out of a TS information descriptor.
fn remote_control_key_id_of(descriptor: &Descriptor) -> Option<u8> {
    match descriptor {
        Descriptor::Unknown(DESCRIPTOR_TAG_TS_INFORMATION, body) => body.first().copied(),
        _ => None,
    }
}

/// Parses any section the four supported table ids cover.
pub fn parse_section(section: &[u8]) -> Result<SiEvent, SiError> {
    match section.first().copied() {
        Some(TABLE_ID_PAT) => parse_pat(section).map(SiEvent::Pat),
        Some(TABLE_ID_PMT) => parse_pmt(section).map(SiEvent::Pmt),
        Some(TABLE_ID_SDT_ACTUAL | TABLE_ID_SDT_OTHER) => parse_sdt(section).map(SiEvent::Sdt),
        Some(TABLE_ID_NIT_ACTUAL | TABLE_ID_NIT_OTHER) => parse_nit(section).map(SiEvent::Nit),
        Some(other) => Err(SiError::UnexpectedTable(other)),
        None => Err(SiError::BadLength),
    }
}

/// A complete section, with the PID it was assembled from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssembledSection {
    /// PID the section arrived on.
    pub pid: u16,
    /// The section bytes: header, body and trailing CRC32.
    pub bytes: Vec<u8>,
}

/// Reassembly state of one PID.
#[derive(Debug, Default)]
struct PidState {
    /// Bytes of the section being assembled.
    buf: Vec<u8>,
    /// Total length of that section, once its header has arrived.
    expected: Option<usize>,
    /// Continuity counter of the last payload-bearing packet.
    last_cc: Option<u8>,
    /// Partial sections abandoned on this PID.
    dropped: u64,
}

impl PidState {
    /// Discards the section in progress, counting it as dropped.
    fn drop_partial(&mut self) {
        if !self.buf.is_empty() {
            self.buf.clear();
            self.expected = None;
            self.dropped += 1;
        }
    }

    /// Appends payload bytes and moves every completed section into `out`.
    fn extend_and_collect(&mut self, payload: &[u8], out: &mut Vec<Vec<u8>>) {
        self.buf.extend_from_slice(payload);
        loop {
            if self.buf.is_empty() {
                self.expected = None;
                return;
            }
            // Stuffing after the last section of a packet.
            if self.buf[0] == 0xFF {
                self.buf.clear();
                self.expected = None;
                return;
            }
            if self.expected.is_none() {
                if self.buf.len() < 3 {
                    return;
                }
                let total = 3 + (usize::from(self.buf[1] & 0x0F) << 8 | usize::from(self.buf[2]));
                if total > MAX_SECTION_LEN {
                    self.drop_partial();
                    return;
                }
                self.expected = Some(total);
            }
            let Some(total) = self.expected else {
                return;
            };
            if self.buf.len() < total {
                return;
            }
            out.push(self.buf[..total].to_vec());
            self.buf.drain(..total);
            self.expected = None;
        }
    }
}

/// The fields of a 188-byte TS packet this module needs.
#[derive(Clone, Copy, Debug)]
struct PacketHeader {
    pid: u16,
    payload_unit_start: bool,
    continuity_counter: u8,
    payload_offset: Option<usize>,
    transport_error: bool,
}

/// Parses the header of a 188-byte packet, or returns `None` without a sync byte.
fn parse_header(packet: &[u8]) -> Option<PacketHeader> {
    if packet.len() < TS_PACKET_SIZE || packet[0] != SYNC_BYTE {
        return None;
    }
    let pid = (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2]);
    let adaptation_field_control = (packet[3] >> 4) & 0x03;
    let payload_offset = match adaptation_field_control {
        0x01 => Some(4),
        0x03 => {
            let offset = 5 + usize::from(packet[4]);
            (offset < TS_PACKET_SIZE).then_some(offset)
        }
        _ => None,
    };
    Some(PacketHeader {
        pid,
        payload_unit_start: packet[1] & 0x40 != 0,
        continuity_counter: packet[3] & 0x0F,
        payload_offset,
        transport_error: packet[1] & 0x80 != 0,
    })
}

/// Turns TS packets into complete PSI/SI sections.
///
/// Per PID it tracks the section being assembled, the expected length from the
/// section header, and the continuity counter. A duplicate packet (same
/// counter) is ignored; a gap drops the partial section and increments
/// [`dropped_sections`](Self::dropped_sections).
///
/// Packets with the transport error indicator set, without a payload, or
/// without a sync byte are skipped.
#[derive(Debug, Default)]
pub struct SectionAssembler {
    pids: HashMap<u16, PidState>,
}

impl SectionAssembler {
    /// Creates an assembler with no state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one 188-byte packet and returns every section it completed.
    pub fn push_packet(&mut self, packet: &[u8]) -> Vec<AssembledSection> {
        let Some(header) = parse_header(packet) else {
            return Vec::new();
        };
        if header.transport_error {
            return Vec::new();
        }
        let Some(offset) = header.payload_offset else {
            return Vec::new();
        };
        let payload = &packet[offset..];
        if payload.is_empty() {
            return Vec::new();
        }

        let state = self.pids.entry(header.pid).or_default();
        if let Some(last) = state.last_cc {
            if header.continuity_counter == last {
                // A retransmitted packet carries nothing new.
                return Vec::new();
            }
            if header.continuity_counter != last.wrapping_add(1) & 0x0F {
                state.drop_partial();
            }
        }
        state.last_cc = Some(header.continuity_counter);

        let mut sections: Vec<Vec<u8>> = Vec::new();
        if header.payload_unit_start {
            let pointer = usize::from(payload[0]);
            let start = 1 + pointer;
            if start > payload.len() {
                // The pointer field points past the packet: unusable.
                state.drop_partial();
                return Vec::new();
            }
            if !state.buf.is_empty() {
                // A new section starts here, so whatever was being assembled
                // is abandoned. Bytes before the pointer belong to it and may
                // be enough to finish it.
                if pointer > 0 {
                    state.extend_and_collect(&payload[1..start], &mut sections);
                }
                state.drop_partial();
            }
            if start < payload.len() {
                state.extend_and_collect(&payload[start..], &mut sections);
            }
        } else if !state.buf.is_empty() {
            // A continuation only makes sense once a section has started.
            state.extend_and_collect(payload, &mut sections);
        }

        sections
            .into_iter()
            .map(|bytes| AssembledSection {
                pid: header.pid,
                bytes,
            })
            .collect()
    }

    /// Appends a run of 188-byte packets (for example the output of
    /// [`crate::tsframe::TsNormalizer`]) and returns every completed section.
    ///
    /// A trailing partial packet is ignored.
    pub fn push_packets(&mut self, packets: &[u8]) -> Vec<AssembledSection> {
        packets
            .chunks_exact(TS_PACKET_SIZE)
            .flat_map(|packet| self.push_packet(packet))
            .collect()
    }

    /// Number of partial sections dropped on continuity or pointer errors.
    pub fn dropped_sections(&self) -> u64 {
        self.pids.values().map(|state| state.dropped).sum()
    }

    /// Forgets every partially assembled section and the continuity state.
    pub fn reset(&mut self) {
        self.pids.clear();
    }
}

/// Statistics of a [`SiParser`] run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SiStats {
    /// Packets offered to the parser.
    pub packets: u64,
    /// Complete sections emitted by the assembler.
    pub sections: u64,
    /// Sections that failed validation or parsing.
    pub invalid_sections: u64,
    /// Sections ignored because their table id is not one of the four handled.
    pub ignored_sections: u64,
}

/// Assembles sections out of TS packets and parses the four tables of SPEC §8.
///
/// ```
/// use unituner::si::SiParser;
///
/// let mut parser = SiParser::new();
/// // A 188-byte run of null packets carries no SI, so nothing is emitted.
/// let mut null_packet = [0xFFu8; 188];
/// null_packet[0] = 0x47;
/// null_packet[1] = 0x1F;
/// null_packet[2] = 0xFF;
/// assert!(parser.push_packets(&null_packet).is_empty());
/// ```
#[derive(Debug, Default)]
pub struct SiParser {
    assembler: SectionAssembler,
    stats: SiStats,
    network_id: Option<u16>,
}

impl SiParser {
    /// Creates a parser with no state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses one 188-byte packet and returns the tables it completed.
    pub fn push_packet(&mut self, packet: &[u8]) -> Vec<SiEvent> {
        self.stats.packets += 1;
        let assembled = self.assembler.push_packet(packet);
        self.parse_assembled(assembled)
    }

    /// Parses a run of 188-byte packets and returns the tables it completed.
    pub fn push_packets(&mut self, packets: &[u8]) -> Vec<SiEvent> {
        self.stats.packets += (packets.len() / TS_PACKET_SIZE) as u64;
        let assembled = self.assembler.push_packets(packets);
        self.parse_assembled(assembled)
    }

    /// Counters of this parser.
    pub fn stats(&self) -> SiStats {
        self.stats
    }

    /// The `network_id` of the most recently parsed NIT, if any.
    pub fn network_id(&self) -> Option<u16> {
        self.network_id
    }

    /// Number of partial sections dropped by the assembler.
    pub fn dropped_sections(&self) -> u64 {
        self.assembler.dropped_sections()
    }

    /// Forgets all parse state, keeping the statistics.
    pub fn reset(&mut self) {
        self.assembler.reset();
        self.network_id = None;
    }

    fn parse_assembled(&mut self, assembled: Vec<AssembledSection>) -> Vec<SiEvent> {
        let mut events = Vec::with_capacity(assembled.len());
        for section in assembled {
            self.stats.sections += 1;
            if !matches!(
                section.bytes.first().copied(),
                Some(
                    TABLE_ID_PAT
                        | TABLE_ID_PMT
                        | TABLE_ID_NIT_ACTUAL
                        | TABLE_ID_NIT_OTHER
                        | TABLE_ID_SDT_ACTUAL
                        | TABLE_ID_SDT_OTHER
                )
            ) {
                self.stats.ignored_sections += 1;
                continue;
            }
            match parse_section(&section.bytes) {
                Ok(SiEvent::Pat(mut pat)) => {
                    pat.network_id = self.network_id;
                    events.push(SiEvent::Pat(pat));
                }
                Ok(SiEvent::Nit(nit)) => {
                    self.network_id = Some(nit.network_id);
                    events.push(SiEvent::Nit(nit));
                }
                Ok(event) => events.push(event),
                Err(_) => self.stats.invalid_sections += 1,
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Appends a CRC32 over `body` (a section without its trailing CRC).
    fn with_crc(mut body: Vec<u8>) -> Vec<u8> {
        let crc = crc32_mpeg2(&body);
        body.extend_from_slice(&crc.to_be_bytes());
        body
    }

    /// Builds a complete long-form section: table id, the `id` field (a
    /// transport_stream_id, network_id or program_number), version 0/current,
    /// section 0 of 0, `rest`, and the CRC32.
    fn section(table_id: u8, id: u16, rest: Vec<u8>) -> Vec<u8> {
        let section_length = 5 + rest.len() + 4;
        let mut bytes = vec![
            table_id,
            0xB0 | ((section_length >> 8) as u8 & 0x0F),
            section_length as u8,
            (id >> 8) as u8,
            id as u8,
            0xC1,
            0x00,
            0x00,
        ];
        bytes.extend_from_slice(&rest);
        with_crc(bytes)
    }

    /// PAT: tsid 0x7FE0, NIT PID 0x0010, programme 0x0408 -> PMT PID 0x01F0.
    fn pat_section() -> Vec<u8> {
        section(
            0x00,
            0x7FE0,
            vec![
                0x00, 0x00, 0xE0, 0x10, // program 0 -> NIT PID 0x010
                0x04, 0x08, 0xE1, 0xF0, // program 0x0408 -> PMT PID 0x1F0
            ],
        )
    }

    /// PMT: program 0x0408, PCR PID 0x1F1, H.264 + AAC.
    fn pmt_section() -> Vec<u8> {
        section(
            0x02,
            0x0408,
            vec![
                0xE1, 0xF1, // PCR_PID 0x1F1
                0xF0, 0x00, // program_info_length 0
                0x1B, 0xE1, 0xF1, 0xF0, 0x00, // H.264 on 0x1F1
                0x0F, 0xE1, 0xF2, 0xF0, 0x00, // AAC on 0x1F2
            ],
        )
    }

    /// SDT: tsid 0x7FE0, original_network_id 0x7FE0, two services. The first,
    /// 0x0408, has the ARIB-encoded name "ＮＨＫ総合"; the second, 0x0409, has
    /// a long ASCII name so the section outgrows one packet.
    fn sdt_section() -> Vec<u8> {
        let mut services = vec![
            0x04, 0x08, // service_id
            0xFD, // EIT flags
        ];
        // Service descriptor for 0x0408: provider "unituner", Japanese name.
        let mut descriptor = vec![
            0x01, // service_type: digital TV
            0x08, b'u', b'n', b'i', b't', b'u', b'n', b'e', b'r', // provider
            0x0D, // service name length: 13 bytes
        ];
        descriptor.extend_from_slice(&[
            0x1B, 0x24, 0x42, // designate JIS X 0208-1997 into G0
            0x23, 0x4E, // Ｎ
            0x23, 0x48, // Ｈ
            0x23, 0x4B, // Ｋ
            0x41, 0x6D, // 総
            0x39, 0x67, // 合
        ]);
        let loop_length = 2 + descriptor.len();
        services.extend_from_slice(&[0x80 | ((loop_length >> 8) as u8 & 0x0F), loop_length as u8]);
        services.extend_from_slice(&[0x48, descriptor.len() as u8]);
        services.extend_from_slice(&descriptor);

        // Second service, long enough that the section needs two packets.
        let mut long_name = vec![0x1B, 0x28, 0x4A]; // G0 alphanumeric
        long_name.extend(std::iter::repeat_n(b'A', 130));
        let mut long_descriptor = vec![
            0xA5, // service_type: promotion
            0x08,
            b'u',
            b'n',
            b'i',
            b't',
            b'u',
            b'n',
            b'e',
            b'r', // provider
            long_name.len() as u8,
        ];
        long_descriptor.extend_from_slice(&long_name);
        let loop_length = 2 + long_descriptor.len();
        services.extend_from_slice(&[0x04, 0x09, 0xFD]);
        services.extend_from_slice(&[0x80 | ((loop_length >> 8) as u8 & 0x0F), loop_length as u8]);
        services.extend_from_slice(&[0x48, long_descriptor.len() as u8]);
        services.extend_from_slice(&long_descriptor);

        let mut rest = vec![
            0x7F, 0xE0, // original_network_id
            0xFF, // reserved
        ];
        rest.extend_from_slice(&services);
        section(0x42, 0x7FE0, rest)
    }

    /// NIT: network_id 0x7FE0 with a TS information descriptor (0xCD).
    fn nit_section() -> Vec<u8> {
        let ts_information = vec![
            0xCD, 0x08, // TS information descriptor, length 8
            0x05, // remote_control_key_id
            0x09, // length_of_ts_name 2, transmission_type_count 1
            b'G', b'R', // ts_name
            0x01, // transmission_type_info
            0x01, // num_of_service
            0x04, 0x08, // service_id
        ];
        let mut rest = vec![0xF0, ts_information.len() as u8];
        rest.extend_from_slice(&ts_information);
        rest.extend_from_slice(&[0xF0, 0x00]); // transport_stream_loop_length 0
        section(0x40, 0x7FE0, rest)
    }

    /// Builds a NIT with no network descriptor at all.
    fn empty_nit_section() -> Vec<u8> {
        section(0x40, 0x7FE0, vec![0xF0, 0x00, 0xF0, 0x00])
    }

    /// Builds a 188-byte packet with the given PID, PUSI flag, CC and payload.
    fn packet(pid: u16, payload_unit_start: bool, cc: u8, payload: &[u8]) -> Vec<u8> {
        assert!(
            payload.len() <= TS_PACKET_SIZE - 4,
            "payload must fit a packet"
        );
        let mut packet = vec![0xFFu8; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        packet[1] = ((payload_unit_start as u8) << 6) | (pid >> 8) as u8;
        packet[2] = pid as u8;
        packet[3] = 0x10 | (cc & 0x0F);
        packet[4..4 + payload.len()].copy_from_slice(payload);
        packet
    }

    /// Fragments a section into as many packets as it needs, the first with a
    /// zero pointer field and PUSI set.
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

    /// Wraps a section in the packets needed to carry it.
    fn section_packet(pid: u16, section: &[u8]) -> Vec<u8> {
        section_packets(pid, section, 0)
    }

    #[test]
    fn crc32_matches_the_mpeg2_check_vector() {
        assert_eq!(crc32_mpeg2(b"123456789"), 0x0376_E6E7);
        assert_eq!(crc32_mpeg2(&[]), 0xFFFF_FFFF);
    }

    #[test]
    fn section_with_a_matching_crc_sums_to_zero() {
        let section = pat_section();
        assert_eq!(crc32_mpeg2(&section), 0);
    }

    #[test]
    fn parses_a_pat() {
        let info = parse_pat(&pat_section()).expect("PAT must parse");
        assert_eq!(info.transport_stream_id, 0x7FE0);
        assert_eq!(info.network_id, None);
        assert_eq!(info.programs, vec![(0x0408, 0x01F0)]);
    }

    #[test]
    fn parses_a_pmt() {
        let info = parse_pmt(&pmt_section()).expect("PMT must parse");
        assert_eq!(info.program_number, 0x0408);
        assert_eq!(info.pcr_pid, 0x1F1);
        assert_eq!(info.streams, vec![(0x1B, 0x1F1), (0x0F, 0x1F2)]);
    }

    #[test]
    fn parses_an_sdt_and_decodes_the_japanese_name() {
        let info = parse_sdt(&sdt_section()).expect("SDT must parse");
        assert_eq!(info.original_network_id, 0x7FE0);
        assert_eq!(info.transport_stream_id, 0x7FE0);
        assert_eq!(info.services.len(), 2);

        let (service_id, service_type, name) = &info.services[0];
        assert_eq!(*service_id, 0x0408);
        assert_eq!(*service_type, 0x01);
        assert_eq!(name, "ＮＨＫ総合");

        let (service_id, service_type, name) = &info.services[1];
        assert_eq!(*service_id, 0x0409);
        assert_eq!(*service_type, 0xA5);
        assert_eq!(name, &"A".repeat(130));
    }

    #[test]
    fn parses_a_nit_with_the_remote_control_key_id() {
        let info = parse_nit(&nit_section()).expect("NIT must parse");
        assert_eq!(info.network_id, 0x7FE0);
        assert_eq!(info.remote_control_key_id, Some(0x05));
    }

    #[test]
    fn nit_without_a_ts_information_descriptor_has_no_key_id() {
        let info = parse_nit(&empty_nit_section()).expect("NIT must parse");
        assert_eq!(info.network_id, 0x7FE0);
        assert_eq!(info.remote_control_key_id, None);
    }

    #[test]
    fn rejects_a_section_with_a_bad_crc() {
        let mut section = pat_section();
        let last = section.len() - 1;
        section[last] ^= 0xFF;
        assert!(matches!(parse_pat(&section), Err(SiError::BadCrc)));
    }

    #[test]
    fn rejects_a_truncated_or_over_long_section() {
        let section = pat_section();
        assert!(matches!(
            parse_pat(&section[..section.len() - 1]),
            Err(SiError::BadLength)
        ));
        let mut longer = section.clone();
        longer.push(0x00);
        assert!(matches!(parse_pat(&longer), Err(SiError::BadLength)));
        assert!(matches!(parse_pat(&[]), Err(SiError::BadLength)));
    }

    #[test]
    fn rejects_a_wrong_table_id() {
        let mut section = pat_section();
        section[0] = 0x42; // claims to be an SDT
                           // Recompute so the only failure is the table id.
        let len = section.len();
        let crc = crc32_mpeg2(&section[..len - 4]);
        section[len - 4..].copy_from_slice(&crc.to_be_bytes());
        assert!(matches!(
            parse_pat(&section),
            Err(SiError::UnexpectedTable(0x42))
        ));
        assert!(matches!(
            parse_pmt(&section),
            Err(SiError::UnexpectedTable(0x42))
        ));
        assert!(matches!(
            parse_nit(&section),
            Err(SiError::UnexpectedTable(0x42))
        ));
    }

    #[test]
    fn assembles_a_single_packet_section() {
        let section = pat_section();
        let mut assembler = SectionAssembler::new();
        let sections = assembler.push_packet(&section_packet(PAT_PID, &section));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].pid, PAT_PID);
        assert_eq!(sections[0].bytes, section);
        assert_eq!(assembler.dropped_sections(), 0);
    }

    #[test]
    fn assembles_a_multi_packet_section() {
        let section = sdt_section();
        assert!(
            section.len() > TS_PACKET_SIZE - 4,
            "the test section must not fit one packet"
        );

        let mut assembler = SectionAssembler::new();
        let mut sections = Vec::new();
        for packet in section_packets(SDT_PID, &section, 0).chunks_exact(TS_PACKET_SIZE) {
            sections.extend(assembler.push_packet(packet));
        }

        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].bytes, section);
        assert_eq!(assembler.dropped_sections(), 0);
    }

    #[test]
    fn multiple_sections_in_one_packet_are_all_emitted() {
        let first = pat_section();
        let second = pat_section();
        let mut payload = vec![0x00];
        payload.extend_from_slice(&first);
        payload.extend_from_slice(&second);

        let mut assembler = SectionAssembler::new();
        let sections = assembler.push_packet(&packet(PAT_PID, true, 0, &payload));
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].bytes, first);
        assert_eq!(sections[1].bytes, second);
    }

    #[test]
    fn a_non_zero_pointer_field_completes_the_previous_section() {
        let section = sdt_section();
        let next = pat_section();
        // Packet 1 carries the first 183 section bytes; packet 2 starts with a
        // pointer field pointing at the 6-byte tail, then begins a new section.
        let split = TS_PACKET_SIZE - 5;
        let tail = &section[split..];
        assert!(tail.len() < 100);

        let mut first_payload = vec![0x00];
        first_payload.extend_from_slice(&section[..split]);

        let mut second_payload = vec![tail.len() as u8];
        second_payload.extend_from_slice(tail);
        second_payload.extend_from_slice(&next);

        let mut assembler = SectionAssembler::new();
        assert!(assembler
            .push_packet(&packet(SDT_PID, true, 0, &first_payload))
            .is_empty());
        let sections = assembler.push_packet(&packet(SDT_PID, true, 1, &second_payload));

        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].bytes, section);
        assert_eq!(sections[1].bytes, next);
        assert_eq!(assembler.dropped_sections(), 0);
    }

    #[test]
    fn a_continuity_gap_drops_the_partial_section() {
        let section = sdt_section();
        let mut fragmented = section_packets(SDT_PID, &section, 4);
        assert!(fragmented.len() >= 2 * TS_PACKET_SIZE);

        let mut assembler = SectionAssembler::new();
        // Feed the first packet at CC 4, then jump the rest to CC 7.
        let first = fragmented.drain(..TS_PACKET_SIZE).collect::<Vec<u8>>();
        assert!(assembler.push_packet(&first).is_empty());
        let mut sections = Vec::new();
        for (index, packet) in fragmented.chunks_exact(TS_PACKET_SIZE).enumerate() {
            let mut packet = packet.to_vec();
            packet[3] = 0x10 | ((7 + index as u8) & 0x0F);
            sections.extend(assembler.push_packet(&packet));
        }

        assert!(sections.is_empty());
        assert_eq!(assembler.dropped_sections(), 1);
    }

    #[test]
    fn a_new_section_start_drops_an_unfinished_one() {
        let section = sdt_section();
        let first_packet = section_packets(SDT_PID, &section, 0)[..TS_PACKET_SIZE].to_vec();

        let mut assembler = SectionAssembler::new();
        assert!(assembler.push_packet(&first_packet).is_empty());
        assert_eq!(assembler.dropped_sections(), 0);

        // A fresh PAT section starts while the SDT is still incomplete.
        let sections = assembler.push_packet(&section_packet(PAT_PID, &pat_section()));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].bytes, pat_section());
        // The PAT is a different PID, so the SDT state is untouched there.
        assert_eq!(assembler.dropped_sections(), 0);

        // A second SDT start on the same PID drops the unfinished one.
        let mut restarted = first_packet.clone();
        restarted[3] = 0x10 | 1; // next continuity counter
        assert!(assembler.push_packet(&restarted).is_empty());
        assert_eq!(assembler.dropped_sections(), 1);
    }

    #[test]
    fn a_duplicate_packet_is_ignored() {
        let section = sdt_section();
        let first_packet = section_packets(SDT_PID, &section, 0)[..TS_PACKET_SIZE].to_vec();

        let mut assembler = SectionAssembler::new();
        assert!(assembler.push_packet(&first_packet).is_empty());
        // The same counter again is a retransmission, not a gap.
        assert!(assembler.push_packet(&first_packet).is_empty());
        assert_eq!(assembler.dropped_sections(), 0);
    }

    #[test]
    fn a_continuation_without_a_start_is_ignored() {
        let section = pat_section();
        let mut assembler = SectionAssembler::new();
        let sections = assembler.push_packet(&packet(PAT_PID, false, 0, &section));
        assert!(sections.is_empty());
        assert_eq!(assembler.dropped_sections(), 0);
    }

    #[test]
    fn a_pointer_field_past_the_payload_drops_the_partial_section() {
        let section = sdt_section();
        let first_packet = section_packets(SDT_PID, &section, 0)[..TS_PACKET_SIZE].to_vec();

        let mut assembler = SectionAssembler::new();
        assert!(assembler.push_packet(&first_packet).is_empty());

        // PUSI claims 200 bytes of pointer data but the packet has 184.
        let mut payload = vec![200u8];
        payload.resize(10, 0xFF);
        let sections = assembler.push_packet(&packet(SDT_PID, true, 1, &payload));
        assert!(sections.is_empty());
        assert_eq!(assembler.dropped_sections(), 1);
    }

    #[test]
    fn packets_without_sync_or_payload_are_skipped() {
        let mut assembler = SectionAssembler::new();
        assert!(assembler.push_packet(&[0x00; TS_PACKET_SIZE]).is_empty());
        assert!(assembler.push_packet(&[]).is_empty());

        // Adaptation field only: no payload.
        let mut adaptation_only = vec![0xFFu8; TS_PACKET_SIZE];
        adaptation_only[0] = SYNC_BYTE;
        adaptation_only[3] = 0x20;
        adaptation_only[4] = 183;
        assert!(assembler.push_packet(&adaptation_only).is_empty());

        // Transport error indicator set.
        let mut errored = section_packet(PAT_PID, &pat_section());
        errored[1] |= 0x80;
        assert!(assembler.push_packet(&errored).is_empty());
    }

    #[test]
    fn the_parser_turns_packets_into_tables() {
        let packets = section_packet(SDT_PID, &sdt_section());
        let mut parser = SiParser::new();
        let events = parser.push_packets(&packets);
        assert_eq!(events.len(), 1);
        match &events[0] {
            SiEvent::Sdt(info) => assert_eq!(info.services[0].2, "ＮＨＫ総合"),
            other => panic!("expected an SDT, got {other:?}"),
        }
        assert_eq!(
            parser.stats().packets,
            (packets.len() / TS_PACKET_SIZE) as u64
        );
        assert_eq!(parser.stats().sections, 1);
        assert_eq!(parser.stats().invalid_sections, 0);
    }

    #[test]
    fn the_parser_reports_the_nit_network_id_on_later_pats() {
        let mut parser = SiParser::new();
        parser.push_packets(&section_packet(NIT_PID, &nit_section()));
        let events = parser.push_packets(&section_packet(PAT_PID, &pat_section()));

        assert_eq!(parser.network_id(), Some(0x7FE0));
        match &events[0] {
            SiEvent::Pat(info) => assert_eq!(info.network_id, Some(0x7FE0)),
            other => panic!("expected a PAT, got {other:?}"),
        }
    }

    #[test]
    fn the_parser_ignores_unknown_tables_and_counts_bad_sections() {
        let mut parser = SiParser::new();

        // A table id nobody handles, with a valid CRC.
        let unknown = section(0x70, 0x0001, Vec::new());
        let events = parser.push_packets(&section_packet(0x0014, &unknown));
        assert!(events.is_empty());
        assert_eq!(parser.stats().ignored_sections, 1);

        let mut broken = pat_section();
        let last = broken.len() - 1;
        broken[last] ^= 0xFF;
        let events = parser.push_packets(&section_packet(PAT_PID, &broken));
        assert!(events.is_empty());
        assert_eq!(parser.stats().invalid_sections, 1);
    }

    #[test]
    fn the_decoder_never_panics_on_arbitrary_bytes() {
        assert_eq!(decode_arib_text(&[]), "");
        assert_eq!(decode_arib_text(&[0x1B]), "");
        assert_eq!(decode_arib_text(&[0x1B, 0x24, 0x42, 0x21]), ""); // lead without trail
        assert_eq!(decode_arib_text(&[0x00, 0x00]), ""); // NUL padding
        assert_eq!(
            decode_arib_text(&[0x1B, 0x28, 0x4A, b'N', b'H', b'K']),
            "NHK"
        );
        assert_eq!(
            decode_arib_text(&[0x1B, 0x24, 0x42, 0x23, 0x4E, 0x23, 0x48, 0x23, 0x4B]),
            "ＮＨＫ"
        );
    }

    #[test]
    fn assembling_packets_in_one_slice_matches_one_at_a_time() {
        let packets = [
            section_packet(PAT_PID, &pat_section()),
            section_packet(SDT_PID, &sdt_section()),
            packet(PAT_PID, false, 1, &[0xFF; 184]),
        ]
        .concat();
        assert_eq!(packets.len() % TS_PACKET_SIZE, 0);

        let mut one_by_one = SectionAssembler::new();
        let mut expected = Vec::new();
        for packet in packets.chunks_exact(TS_PACKET_SIZE) {
            expected.extend(one_by_one.push_packet(packet));
        }

        let mut at_once = SectionAssembler::new();
        let actual = at_once.push_packets(&packets);
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
    }
}

#[cfg(test)]
mod arib_probe {
    use arib::ts::descriptor::Descriptor;
    use arib::ts::table::Table;
    use bytes::Bytes;

    fn read_table(section: &[u8]) -> Table {
        Table::read(&mut Bytes::copy_from_slice(section)).expect("section must parse")
    }

    /// Behavioral probe of the `arib` crate (kept as an active test because it
    /// passes; it is the Phase 1 evidence that the crate's STD-B10/STD-B24 APIs
    /// work for our use case).
    #[test]
    fn parses_pat_pmt_sdt_with_arib() {
        // PAT: transport_stream_id 1, program 1 -> PMT PID 0x010.
        let pat = read_table(&[
            0x00, 0xB0, 0x0D, // table id, syntax+length
            0x00, 0x01, // transport_stream_id
            0xC1, 0x00, 0x00, // version 0, current, section 0 of 0
            0x00, 0x01, 0xE0, 0x10, // program 1 -> PMT PID 0x010
            0xDE, 0xAD, 0xBE, 0xEF, // CRC (not verified by arib)
        ]);
        let Table::Pat(pat) = pat else {
            panic!("expected a PAT");
        };
        assert_eq!(pat.transport_stream_id, 1);
        assert_eq!(pat.programs.len(), 1);

        // PMT: program 1, PCR PID 0x100, one H.264 video and one AAC audio ES.
        let pmt = read_table(&[
            0x02, 0xB0, 0x17, // table id, syntax+length
            0x00, 0x01, // program_number
            0xC1, 0x00, 0x00, // version 0, current, section 0 of 0
            0xE1, 0x00, // PCR_PID 0x100
            0xF0, 0x00, // program_info_length 0
            0x1B, 0xE1, 0x01, 0xF0, 0x00, // stream_type 0x1B, PID 0x101
            0x0F, 0xE1, 0x02, 0xF0, 0x00, // stream_type 0x0F, PID 0x102
            0xDE, 0xAD, 0xBE, 0xEF, // CRC (not verified by arib)
        ]);
        let Table::Pmt(pmt) = pmt else {
            panic!("expected a PMT");
        };
        assert_eq!(pmt.program_number, 1);
        assert_eq!(pmt.pcr_pid, 0x100);
        assert_eq!(pmt.streams.len(), 2);
        assert_eq!(pmt.streams[0].elementary_pid, 0x101);
        assert_eq!(pmt.streams[1].elementary_pid, 0x102);

        // SDT actual, with the service descriptor of ARIB STD-B10's example.
        let sdt = read_table(&[
            0x42, 0xB0, 0x1C, // table id, syntax+length
            0x7F, 0xE0, // transport_stream_id
            0xC3, 0x00, 0x00, // version 1, current, section 0 of 0
            0x7F, 0xE0, // original_network_id
            0xFF, // reserved
            0x04, 0x08, 0xFD, // service_id 0x0408, EIT flags
            0x80, 0x0B, // running status 4, 11 descriptor bytes
            0x48, 0x09, // service descriptor
            0x01, // service_type: digital TV
            0x03, b'N', b'H', b'K', // provider name
            0x03, b'N', b'H', b'K', // service name
            0xDE, 0xAD, 0xBE, 0xEF, // CRC (not verified by arib)
        ]);
        let Table::Sdt(sdt) = sdt else {
            panic!("expected an SDT");
        };
        assert_eq!(sdt.original_network_id, 0x7FE0);
        assert_eq!(sdt.services.len(), 1);
        let service = &sdt.services[0];
        assert_eq!(service.service_id, 0x0408);

        let mut name = Vec::new();
        let mut service_type = 0;
        for descriptor in &service.descriptors {
            if let Descriptor::Service(service) = descriptor {
                service_type = service.service_type;
                name.clone_from(&service.service_name);
            }
        }
        assert_eq!(service_type, 0x01);
        assert_eq!(name, b"NHK");

        // STD-B24 text decoding: designate JIS X 0201 alphanumeric into G1,
        // lock-shift to it, then "NHK". Real ISDB service names use the Kanji
        // sets, but the decoder entry point is the same.
        assert_eq!(
            arib::text::decode(&[0x1B, 0x29, 0x4A, 0x0E, b'N', b'H', b'K']),
            "NHK"
        );
    }
}

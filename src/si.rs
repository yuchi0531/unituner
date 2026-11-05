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
//! # Filters and counters
//!
//! [`SiParser`] only assembles PIDs `0x0000` (PAT), `0x0010` (NIT), `0x0011`
//! (SDT) and the PMT PIDs a PAT named, and only the six supported table ids.
//! A PES payload or an EIT section therefore never even starts a parse, which
//! keeps [`SiStats::invalid_sections`] a real corruption metric instead of a
//! count of "every section I did not want". Those packets are counted in
//! [`SiStats::non_si_packets`]; [`SectionAssembler::new`] and
//! [`SiParser::with_full_assembly`] keep the unfiltered behaviour for tests.
//!
//! # Applicability
//!
//! Sections with `current_next_indicator == 0` describe a table that is not
//! yet in force. They are parsed (so their version and section numbers are
//! available) but not returned: a scan that applied them would fill the
//! service table with next-version entries. They are counted in
//! [`SiStats::ignored_sections`], not as corruption.
//!
//! # CRC32
//!
//! [`crc32_mpeg2`] is public because SPEC §5 requires the same checksum when
//! rewriting a PAT for a service stream (phase 5).

use std::collections::{HashMap, HashSet};

use arib::text::{Decoder, DrcsMapping};
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
/// Wraps the `arib` crate's [`Decoder`] so callers never have to care about the
/// dependency. DRCS glyphs (which have no Unicode code point of their own) are
/// decoded with the decoder's [`DrcsMapping::Replacement`] mapping, i.e. as
/// U+FFFD, and any private-use code point that still comes out is replaced with
/// U+FFFD too; clients of the API cannot render a private-use glyph, and the
/// default mapping would otherwise invent U+EC00.. code points per stream.
/// Undecodable escapes are skipped; the surrounding whitespace and NUL padding
/// that service names often carry is trimmed.
///
/// A panic inside the dependency is caught and turned into an empty string, so
/// a corrupt service name can never take down a scan. The panic hook is muted
/// for the duration of the call: catching a panic from a piece of untrusted
/// input is routine here, and the default hook would print a backtrace to
/// stderr for every corrupt name.
pub fn decode_arib_text(bytes: &[u8]) -> String {
    let decoded = catch_without_hook(|| {
        let mut decoder = Decoder::with_drcs_mapping(DrcsMapping::Replacement);
        decoder.decode(bytes)
    })
    .unwrap_or_default();
    decoded
        .chars()
        .map(|c| {
            if ('\u{E000}'..='\u{F8FF}').contains(&c) {
                '\u{FFFD}'
            } else {
                c
            }
        })
        .collect::<String>()
        .trim_matches(|c: char| c == '\0' || c.is_whitespace())
        .to_owned()
}

/// Serialises the panic-hook swap below: the hook is process-global.
static PANIC_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The type of the process-global panic hook, as `std::panic` stores it.
type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

/// Runs `f`, catching a panic and returning `None` instead, without letting the
/// default panic hook print anything.
///
/// The hook is process-global, so the swap is serialised with a mutex and the
/// previous hook is restored before returning.
fn catch_without_hook<T>(f: impl FnOnce() -> T) -> Option<T> {
    /// Restores the previous panic hook when dropped.
    struct HookGuard(Option<PanicHook>);
    impl Drop for HookGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.0.take() {
                std::panic::set_hook(previous);
            }
        }
    }

    let _serialised = PANIC_HOOK_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let guard = HookGuard(Some(std::panic::take_hook()));
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).ok();
    drop(guard);
    result
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
    /// [`SiParser`] fills it in from the most recently parsed NIT-actual
    /// section, and only when that NIT describes this PAT's transport stream
    /// (its transport-stream loop names `transport_stream_id`); see
    /// [`SiParser::parse_assembled`]. A NIT-actual for another multiplex is
    /// never stamped onto a PAT, so the field is `None` rather than wrong.
    pub network_id: Option<u16>,
    /// PID of the NIT, from the PAT's program-number-zero entry.
    ///
    /// This is the authoritative NIT PID; [`NIT_PID`] is only the usual value.
    /// `None` when the section has no network-PID entry.
    pub network_pid: Option<u16>,
    /// `(program_number, PMT PID)` for every real programme. The
    /// network-PID entry (`program_number == 0`) is not listed; see
    /// [`network_pid`](Self::network_pid).
    pub programs: Vec<(u16, u16)>,
    /// Table `version_number` (0-31).
    pub version_number: u8,
    /// `current_next_indicator`: `false` means the table is not yet applicable.
    pub current_next: bool,
    /// `section_number` of this section.
    pub section_number: u8,
    /// `last_section_number` of the table this section belongs to.
    pub last_section_number: u8,
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
    /// Table `version_number` (0-31).
    pub version_number: u8,
    /// `current_next_indicator`: `false` means the table is not yet applicable.
    pub current_next: bool,
    /// `section_number` of this section.
    pub section_number: u8,
    /// `last_section_number` of the table this section belongs to.
    pub last_section_number: u8,
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
    /// Table `version_number` (0-31).
    pub version_number: u8,
    /// `current_next_indicator`: `false` means the table is not yet applicable.
    pub current_next: bool,
    /// `section_number` of this section.
    pub section_number: u8,
    /// `last_section_number` of the table this section belongs to.
    pub last_section_number: u8,
}

/// Network information read from a NIT section.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NitInfo {
    /// `network_id` of the network.
    pub network_id: u16,
    /// Remote-control key ("channel number on the remote") for the transport
    /// stream, from the TS information descriptor (tag `0xCD`).
    pub remote_control_key_id: Option<u8>,
    /// Whether this is the NIT *actual* (`0x40`, describes the multiplex being
    /// received) rather than the NIT *other* (`0x41`, describes another
    /// transport stream).
    ///
    /// Only NIT-actual may set [`SiParser::network_id`]; NIT-other is still
    /// parsed and reported but never mutates the session's network id.
    pub is_actual: bool,
    /// `transport_stream_id`s listed in this NIT's transport-stream loop.
    ///
    /// A NIT-actual's loop names the multiplex it describes; [`SiParser`] uses
    /// it to decide whether a PAT belongs to the same transport stream before
    /// stamping a network id onto it.
    pub transport_stream_ids: Vec<u16>,
    /// Table `version_number` (0-31).
    pub version_number: u8,
    /// `current_next_indicator`: `false` means the table is not yet applicable.
    pub current_next: bool,
    /// `section_number` of this section.
    pub section_number: u8,
    /// `last_section_number` of the table this section belongs to.
    pub last_section_number: u8,
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
    let mut network_pid = None;
    let mut programs = Vec::new();
    for association in pat.programs {
        match association {
            ProgramAssociation::Network {
                network_pid: pid, ..
            } => network_pid = Some(pid),
            ProgramAssociation::ProgramMap {
                program_number,
                program_map_pid,
            } => programs.push((program_number, program_map_pid)),
        }
    }
    Ok(PatInfo {
        transport_stream_id: pat.transport_stream_id,
        network_id: None,
        network_pid,
        programs,
        version_number: pat.version_number,
        current_next: pat.current_next_indicator,
        section_number: pat.section_number,
        last_section_number: pat.last_section_number,
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
        version_number: pmt.version_number,
        current_next: pmt.current_next_indicator,
        section_number: pmt.section_number,
        last_section_number: pmt.last_section_number,
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
        version_number: sdt.version_number,
        current_next: sdt.current_next_indicator,
        section_number: sdt.section_number,
        last_section_number: sdt.last_section_number,
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
///
/// The table id distinguishes NIT-actual (`0x40`) from NIT-other (`0x41`);
/// [`NitInfo::is_actual`] records which one this is.
pub fn parse_nit(section: &[u8]) -> Result<NitInfo, SiError> {
    expect_table_id(section, &[TABLE_ID_NIT_ACTUAL, TABLE_ID_NIT_OTHER])?;
    let table_id = section[0];
    let Table::Nit(nit) = read_table(section)? else {
        return Err(SiError::Parse("not a NIT".to_owned()));
    };
    let remote_control_key_id = nit
        .descriptors
        .iter()
        .chain(nit.transport_streams.iter().flat_map(|ts| &ts.descriptors))
        .find_map(remote_control_key_id_of);
    let transport_stream_ids = nit
        .transport_streams
        .iter()
        .map(|ts| ts.transport_stream_id)
        .collect();
    Ok(NitInfo {
        network_id: nit.network_id,
        remote_control_key_id,
        is_actual: table_id == TABLE_ID_NIT_ACTUAL,
        transport_stream_ids,
        version_number: nit.version_number,
        current_next: nit.current_next_indicator,
        section_number: nit.section_number,
        last_section_number: nit.last_section_number,
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
    /// Bytes of an unwanted section still to skip before assembly resumes.
    skip: usize,
    /// Sections skipped because their table id was not in the allowlist.
    skipped: u64,
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
        self.skip = 0;
    }

    /// Appends payload bytes and moves every completed section into `out`.
    ///
    /// When `tables` is set, a section whose table id is not in it is skipped
    /// byte-exactly as soon as its header has arrived, so its body is never
    /// buffered or copied.
    fn extend_and_collect(
        &mut self,
        payload: &[u8],
        tables: Option<&HashSet<u8>>,
        out: &mut Vec<Vec<u8>>,
    ) {
        // Finish skipping an unwanted section left over from an earlier packet.
        let skipped = self.skip.min(payload.len());
        self.skip -= skipped;
        let payload = &payload[skipped..];
        if payload.is_empty() {
            return;
        }
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
            if let Some(tables) = tables {
                if !tables.contains(&self.buf[0]) {
                    if self.buf.len() < 3 {
                        return;
                    }
                    let total =
                        3 + (usize::from(self.buf[1] & 0x0F) << 8 | usize::from(self.buf[2]));
                    if total > MAX_SECTION_LEN {
                        self.drop_partial();
                        return;
                    }
                    if self.buf.len() < total {
                        // The section spans packets: remember how many of its
                        // bytes still have to be skipped, without keeping any
                        // of them.
                        self.skip = total - self.buf.len();
                        self.buf.clear();
                        self.expected = None;
                        self.skipped += 1;
                        return;
                    }
                    self.buf.drain(..total);
                    self.expected = None;
                    self.skipped += 1;
                    continue;
                }
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

/// Runs `f` for every whole 188-byte packet in `data`, carrying a packet cut by
/// a call boundary in `leftover` to the next call.
///
/// `leftover` holds fewer than [`TS_PACKET_SIZE`] bytes: the tail of the packet
/// the previous `data` ended in. Both [`SectionAssembler::push_packets`] and
/// [`SiParser::push_packets`] use this so a chunk that is not a multiple of 188
/// bytes loses nothing.
fn for_each_packet(data: &[u8], leftover: &mut Vec<u8>, mut f: impl FnMut(&[u8])) {
    let mut offset = 0;
    if !leftover.is_empty() {
        let take = (TS_PACKET_SIZE - leftover.len()).min(data.len());
        leftover.extend_from_slice(&data[..take]);
        offset = take;
        if leftover.len() < TS_PACKET_SIZE {
            return;
        }
        let mut packet = std::mem::take(leftover);
        f(&packet);
        packet.clear();
        *leftover = packet;
    }
    let whole = (data.len() - offset) / TS_PACKET_SIZE * TS_PACKET_SIZE;
    for packet in data[offset..offset + whole].chunks_exact(TS_PACKET_SIZE) {
        f(packet);
    }
    leftover.extend_from_slice(&data[offset + whole..]);
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
///
/// # Filters
///
/// A bare assembler assembles every PID and every table. [`with_interest`] and
/// [`with_tables`] narrow that down:
///
/// * an interest set skips packets on other PIDs before any state or buffer is
///   touched; they are counted in [`non_si_packets`](Self::non_si_packets).
/// * a table allowlist is checked as soon as a section's table id is known, so
///   a section of an unwanted table is never copied.
///
/// [`SiParser`] sets both, so PES, EIT and other non-PSI payloads cost nothing
/// and cannot pollute the parser's error counters. Full assembly stays
/// available with [`SectionAssembler::new`] and
/// [`SiParser::with_full_assembly`].
///
/// [`with_interest`]: Self::with_interest
/// [`with_tables`]: Self::with_tables
#[derive(Debug, Default)]
pub struct SectionAssembler {
    pids: HashMap<u16, PidState>,
    /// PIDs to assemble, or `None` for every PID.
    interest: Option<HashSet<u16>>,
    /// Table ids to assemble, or `None` for every table.
    tables: Option<HashSet<u8>>,
    /// Packets skipped because their PID was not in [`interest`](Self::interest).
    non_si_packets: u64,
    /// Bytes of a packet cut in half by a [`push_packets`](Self::push_packets)
    /// call boundary. A tuner read is not a multiple of 188 bytes, so without
    /// this the tail of every non-aligned chunk would be discarded.
    leftover: Vec<u8>,
}

impl SectionAssembler {
    /// Creates an assembler with no state and no filters: every PID and every
    /// table is assembled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an assembler that only assembles the given PIDs.
    ///
    /// Packets on other PIDs are counted in
    /// [`non_si_packets`](Self::non_si_packets) and otherwise ignored.
    /// [`add_interest`](Self::add_interest) extends the set later (a PAT names
    /// its PMT PIDs only after it has been parsed).
    pub fn with_interest<I: IntoIterator<Item = u16>>(pids: I) -> Self {
        Self {
            interest: Some(pids.into_iter().collect()),
            ..Self::default()
        }
    }

    /// Restricts assembly to the given table ids.
    ///
    /// A section of another table is dropped as soon as its table id byte is
    /// seen, before its body is copied.
    pub fn with_tables<I: IntoIterator<Item = u8>>(mut self, tables: I) -> Self {
        self.tables = Some(tables.into_iter().collect());
        self
    }

    /// Adds one PID to the interest set. Does nothing without a filter.
    pub fn add_interest(&mut self, pid: u16) {
        if let Some(interest) = &mut self.interest {
            interest.insert(pid);
        }
    }

    /// Whether `pid` is assembled.
    pub fn is_interested(&self, pid: u16) -> bool {
        self.interest
            .as_ref()
            .is_none_or(|interest| interest.contains(&pid))
    }

    /// Packets skipped because their PID was not in the interest set.
    pub fn non_si_packets(&self) -> u64 {
        self.non_si_packets
    }

    /// Appends one 188-byte packet and returns every section it completed.
    pub fn push_packet(&mut self, packet: &[u8]) -> Vec<AssembledSection> {
        let Some(header) = parse_header(packet) else {
            return Vec::new();
        };
        if header.transport_error {
            return Vec::new();
        }
        if !self.is_interested(header.pid) {
            self.non_si_packets += 1;
            return Vec::new();
        }
        let Some(offset) = header.payload_offset else {
            return Vec::new();
        };
        let payload = &packet[offset..];
        if payload.is_empty() {
            return Vec::new();
        }

        let tables = self.tables.as_ref();
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
            // A new section starts here, so any pending skip is obsolete.
            state.skip = 0;
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
                    state.extend_and_collect(&payload[1..start], tables, &mut sections);
                }
                state.drop_partial();
            }
            if start < payload.len() {
                state.extend_and_collect(&payload[start..], tables, &mut sections);
            }
        } else if !state.buf.is_empty() || state.skip > 0 {
            // A continuation only makes sense once a section has started.
            state.extend_and_collect(payload, tables, &mut sections);
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
    /// The input is treated as a byte stream, not as whole packets: bytes of a
    /// packet that is cut by the end of `packets` are kept and completed by the
    /// next call, exactly like [`crate::tsframe::TsNormalizer`] buffers its
    /// input. A tuner read is not a multiple of 188 bytes (`chunk_size_bytes`
    /// defaults to 65536, and `65536 % 188 == 112`), so cutting at the call
    /// boundary instead would silently drop the tail of every chunk and with it
    /// most SI sections.
    ///
    /// [`push_packet`](Self::push_packet) takes whole packets and does not
    /// participate in this buffering; do not mix the two on one assembler.
    pub fn push_packets(&mut self, packets: &[u8]) -> Vec<AssembledSection> {
        let mut sections = Vec::new();
        let mut leftover = std::mem::take(&mut self.leftover);
        for_each_packet(packets, &mut leftover, |packet| {
            sections.extend(self.push_packet(packet));
        });
        self.leftover = leftover;
        sections
    }

    /// Number of partial sections dropped on continuity or pointer errors.
    pub fn dropped_sections(&self) -> u64 {
        self.pids.values().map(|state| state.dropped).sum()
    }

    /// Sections skipped because their table id was outside the allowlist.
    pub fn ignored_sections(&self) -> u64 {
        self.pids.values().map(|state| state.skipped).sum()
    }

    /// Forgets every partially assembled section and the continuity state.
    ///
    /// The counters stay: [`non_si_packets`](Self::non_si_packets) is cumulative
    /// over the assembler's lifetime, while
    /// [`dropped_sections`](Self::dropped_sections) and
    /// [`ignored_sections`](Self::ignored_sections) describe the current state
    /// and restart at zero.
    pub fn reset(&mut self) {
        self.pids.clear();
        self.leftover.clear();
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
    ///
    /// With the default interest filter this only counts real SI corruption:
    /// packets of other PIDs and sections of other tables never reach the
    /// validator. See [`non_si_packets`](Self::non_si_packets).
    pub invalid_sections: u64,
    /// Packet-level ignorance: packets skipped because their PID is neither
    /// PAT, NIT, SDT nor a PMT PID learned from the PAT (PES, EIT, null, ...).
    ///
    /// Before this split every one of those PUSI packets started a bogus
    /// section and inflated `invalid_sections`, which made the metric useless.
    pub non_si_packets: u64,
    /// Sections skipped because their table id is not one of the six handled
    /// (PAT, PMT, SDT actual/other, NIT actual/other).
    pub ignored_sections: u64,
}

/// The PIDs [`SiParser`] assembles by default; PMT PIDs are added as PATs
/// arrive.
const DEFAULT_INTEREST: [u16; 3] = [PAT_PID, NIT_PID, SDT_PID];

/// The table ids [`SiParser`] parses.
const SUPPORTED_TABLES: [u8; 6] = [
    TABLE_ID_PAT,
    TABLE_ID_PMT,
    TABLE_ID_NIT_ACTUAL,
    TABLE_ID_NIT_OTHER,
    TABLE_ID_SDT_ACTUAL,
    TABLE_ID_SDT_OTHER,
];

/// Assembles sections out of TS packets and parses the four tables of SPEC §8.
///
/// By default only PIDs `0x0000` (PAT), `0x0010` (NIT), `0x0011` (SDT) and the
/// PMT PIDs named by the PAT are assembled, and only the six table ids above
/// are handed to the parser; everything else is counted in
/// [`SiStats::non_si_packets`] or [`SiStats::ignored_sections`]. This keeps
/// `invalid_sections` a real corruption metric on a healthy mux.
///
/// Sections with `current_next_indicator == 0` are *not* reported: they
/// describe a table that is not yet applicable, and a scan that applied them
/// would fill the service table with next-version entries. They are dropped
/// silently (they are not corruption).
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
/// assert_eq!(parser.stats().non_si_packets, 1);
/// ```
#[derive(Debug)]
pub struct SiParser {
    assembler: SectionAssembler,
    stats: SiStats,
    network_id: Option<u16>,
    /// `transport_stream_id`s of the multiplex the `network_id` came from,
    /// learned from the NIT-actual's transport-stream loop.
    network_tsids: HashSet<u16>,
    /// Bytes of a packet cut in half by a [`push_packets`](Self::push_packets)
    /// call boundary; see [`SectionAssembler::push_packets`]. Kept here as well
    /// because this type parses packet by packet (a PAT must name its PMT PIDs
    /// before the PMTs in the same read are assembled).
    leftover: Vec<u8>,
}

impl Default for SiParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SiParser {
    /// Creates a parser with the default interest and table filters.
    pub fn new() -> Self {
        let assembler =
            SectionAssembler::with_interest(DEFAULT_INTEREST).with_tables(SUPPORTED_TABLES);
        Self {
            assembler,
            stats: SiStats::default(),
            network_id: None,
            network_tsids: HashSet::new(),
            leftover: Vec::new(),
        }
    }

    /// Creates a parser that assembles every PID and every table.
    ///
    /// Only useful for tests and diagnostics: on real input this copies every
    /// PES and EIT section before discarding it.
    pub fn with_full_assembly() -> Self {
        Self {
            assembler: SectionAssembler::new(),
            ..Self::new()
        }
    }

    /// Parses one 188-byte packet and returns the tables it completed.
    pub fn push_packet(&mut self, packet: &[u8]) -> Vec<SiEvent> {
        self.stats.packets += 1;
        let assembled = self.assembler.push_packet(packet);
        self.parse_assembled(assembled)
    }

    /// Parses a run of 188-byte packets and returns the tables it completed.
    ///
    /// Packets are parsed one at a time rather than assembled as one batch:
    /// a PAT in the run names its PMT PIDs, and those PMTs usually follow it
    /// within the same read from the tuner. Batching the assembly would see the
    /// PMT before the PAT had been parsed and skip it until the next PAT.
    ///
    /// The run is a byte stream, not a whole number of packets: bytes of the
    /// packet that `packets` cuts in half are kept for the next call, so a chunk
    /// whose length is not a multiple of 188 loses nothing.
    pub fn push_packets(&mut self, packets: &[u8]) -> Vec<SiEvent> {
        let mut events = Vec::new();
        let mut leftover = std::mem::take(&mut self.leftover);
        for_each_packet(packets, &mut leftover, |packet| {
            events.extend(self.push_packet(packet));
        });
        self.leftover = leftover;
        events
    }

    /// Counters of this parser.
    pub fn stats(&self) -> SiStats {
        SiStats {
            non_si_packets: self.assembler.non_si_packets(),
            ignored_sections: self.stats.ignored_sections + self.assembler.ignored_sections(),
            ..self.stats
        }
    }

    /// The `network_id` learned from the most recent NIT *actual*, if any.
    ///
    /// A NIT *other* (table id `0x41`) describes a different transport stream
    /// and never changes this value; see [`NitInfo::is_actual`].
    pub fn network_id(&self) -> Option<u16> {
        self.network_id
    }

    /// Number of partial sections dropped by the assembler.
    pub fn dropped_sections(&self) -> u64 {
        self.assembler.dropped_sections()
    }

    /// Forgets all parse state.
    ///
    /// Packet, section and corruption counters (`packets`, `sections`,
    /// `invalid_sections`, parse-level `ignored_sections`) are kept; the
    /// assembler's own counters restart with its state, so
    /// [`SiStats::non_si_packets`] and the assembler share of
    /// `ignored_sections` go back to zero.
    pub fn reset(&mut self) {
        self.assembler.reset();
        self.leftover.clear();
        self.network_id = None;
        self.network_tsids.clear();
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
                Ok(event) if !is_current(&event) => {
                    // A next-version table: parsed but not yet applicable.
                    self.stats.ignored_sections += 1;
                }
                Ok(SiEvent::Pat(mut pat)) => {
                    pat.network_id = self.network_id_for(pat.transport_stream_id);
                    for &(_, pmt_pid) in &pat.programs {
                        // A PAT names its PMT PIDs; assemble them from now on.
                        self.assembler.add_interest(pmt_pid);
                    }
                    if let Some(network_pid) = pat.network_pid {
                        // ...and its NIT PID, which is only conventionally
                        // 0x0010.
                        self.assembler.add_interest(network_pid);
                    }
                    events.push(SiEvent::Pat(pat));
                }
                Ok(SiEvent::Nit(nit)) => {
                    if nit.is_actual {
                        self.network_id = Some(nit.network_id);
                        self.network_tsids = nit.transport_stream_ids.iter().copied().collect();
                    }
                    events.push(SiEvent::Nit(nit));
                }
                Ok(event) => events.push(event),
                Err(_) => self.stats.invalid_sections += 1,
            }
        }
        events
    }

    /// The network id to stamp onto a PAT of `transport_stream_id`.
    ///
    /// A PAT carries no network id. The only trustworthy source is a NIT
    /// *actual* (a NIT *other* never sets `self.network_id`, so a stray
    /// NIT-other in the stream cannot leak another network's id onto this PAT).
    ///
    /// It is stamped only when the NIT-actual's transport-stream loop names this
    /// very transport stream, i.e. the PAT's TSID is the NIT's TSID. Otherwise
    /// the result is `None`: a missing network id is recoverable (the next NIT
    /// retries), a wrong one is not.
    fn network_id_for(&self, transport_stream_id: u16) -> Option<u16> {
        let network_id = self.network_id?;
        self.network_tsids
            .contains(&transport_stream_id)
            .then_some(network_id)
    }
}

/// Whether a parsed table is applicable now (`current_next_indicator == 1`).
fn is_current(event: &SiEvent) -> bool {
    match event {
        SiEvent::Pat(info) => info.current_next,
        SiEvent::Pmt(info) => info.current_next,
        SiEvent::Sdt(info) => info.current_next,
        SiEvent::Nit(info) => info.current_next,
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
        section_header(table_id, id, 0xC1, 0x00, 0x00, rest)
    }

    /// Builds a section with explicit `version_and_current`, `section_number`
    /// and `last_section_number`.
    fn section_header(
        table_id: u8,
        id: u16,
        version_and_current: u8,
        section_number: u8,
        last_section_number: u8,
        rest: Vec<u8>,
    ) -> Vec<u8> {
        let section_length = 5 + rest.len() + 4;
        let mut bytes = vec![
            table_id,
            0xB0 | ((section_length >> 8) as u8 & 0x0F),
            section_length as u8,
            (id >> 8) as u8,
            id as u8,
            version_and_current,
            section_number,
            last_section_number,
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

    /// NIT: network_id 0x7FE0 with a TS information descriptor (0xCD) and a
    /// transport-stream loop naming TSID 0x7FE0 (the PAT's TSID).
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
        rest.extend_from_slice(&[0xF0, 0x06]); // transport_stream_loop_length 6
        rest.extend_from_slice(&[0x7F, 0xE0, 0x7F, 0xE0, 0xF0, 0x00]);
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
        assert_eq!(info.network_pid, Some(NIT_PID));
        assert_eq!(info.programs, vec![(0x0408, 0x01F0)]);
        assert_eq!(info.version_number, 0);
        assert!(info.current_next);
        assert_eq!(info.section_number, 0);
        assert_eq!(info.last_section_number, 0);
    }

    #[test]
    fn a_pat_without_a_network_entry_has_no_network_pid() {
        let section = section(0x00, 0x7FE0, vec![0x04, 0x08, 0xE1, 0xF0]);
        let info = parse_pat(&section).expect("PAT must parse");
        assert_eq!(info.network_pid, None);
        assert_eq!(info.programs, vec![(0x0408, 0x01F0)]);
    }

    #[test]
    fn a_pat_keeps_its_table_version_and_section_numbers() {
        let section = section_header(
            0x00,
            0x7FE0,
            0xC0 | (7 << 1) | 1,
            3,
            9,
            vec![0x04, 0x08, 0xE1, 0xF0],
        );
        let info = parse_pat(&section).expect("PAT must parse");
        assert_eq!(info.version_number, 7);
        assert!(info.current_next);
        assert_eq!(info.section_number, 3);
        assert_eq!(info.last_section_number, 9);
    }

    #[test]
    fn parses_a_pmt() {
        let info = parse_pmt(&pmt_section()).expect("PMT must parse");
        assert_eq!(info.program_number, 0x0408);
        assert_eq!(info.pcr_pid, 0x1F1);
        assert_eq!(info.streams, vec![(0x1B, 0x1F1), (0x0F, 0x1F2)]);
        assert_eq!(info.version_number, 0);
        assert!(info.current_next);
        assert_eq!(info.section_number, 0);
        assert_eq!(info.last_section_number, 0);
    }

    #[test]
    fn a_pmt_reports_a_next_version_section() {
        let section = section_header(
            0x02,
            0x0408,
            0xC0 | (2 << 1),
            1,
            4,
            vec![
                0xE1, 0xF1, // PCR_PID 0x1F1
                0xF0, 0x00, // program_info_length 0
            ],
        );
        let info = parse_pmt(&section).expect("PMT must parse");
        assert_eq!(info.version_number, 2);
        assert!(!info.current_next);
        assert_eq!(info.section_number, 1);
        assert_eq!(info.last_section_number, 4);
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

        assert_eq!(info.version_number, 0);
        assert!(info.current_next);
        assert_eq!(info.section_number, 0);
        assert_eq!(info.last_section_number, 0);
    }

    #[test]
    fn parses_a_nit_with_the_remote_control_key_id() {
        let info = parse_nit(&nit_section()).expect("NIT must parse");
        assert_eq!(info.network_id, 0x7FE0);
        assert_eq!(info.remote_control_key_id, Some(0x05));
        assert!(info.is_actual);
        assert_eq!(info.transport_stream_ids, vec![0x7FE0]);
        assert_eq!(info.version_number, 0);
        assert!(info.current_next);
    }

    #[test]
    fn nit_without_a_ts_information_descriptor_has_no_key_id() {
        let info = parse_nit(&empty_nit_section()).expect("NIT must parse");
        assert_eq!(info.network_id, 0x7FE0);
        assert_eq!(info.remote_control_key_id, None);
    }

    #[test]
    fn nit_other_is_reported_but_does_not_become_the_session_network_id() {
        // A NIT-other for a different multiplex (network 0x0001, TSID 0x0002).
        let other = section(0x41, 0x0001, vec![0xF0, 0x00, 0xF0, 0x00]);
        let info = parse_nit(&other).expect("NIT other must parse");
        assert!(!info.is_actual);

        let mut parser = SiParser::new();
        let events = parser.push_packets(&section_packet(NIT_PID, &other));
        assert_eq!(events.len(), 1, "NIT-other is still reported");
        assert_eq!(
            parser.network_id(),
            None,
            "NIT-other must not be remembered"
        );

        // A NIT-actual later sets the id. (Different continuity counter: the
        // assembler would treat the same one twice as a retransmission.)
        parser.push_packets(&section_packets(NIT_PID, &nit_section(), 1));
        assert_eq!(parser.network_id(), Some(0x7FE0));
    }

    #[test]
    fn a_pat_is_not_stamped_with_the_network_id_of_another_transport_stream() {
        // NIT-actual for network 0x7FE0 whose TS loop names TSID 0x7FE1 only.
        let mut rest = Vec::new();
        rest.extend_from_slice(&[0xF0, 0x00]); // network descriptors
        rest.extend_from_slice(&[0xF0, 0x06]); // TS loop length 6
        rest.extend_from_slice(&[0x7F, 0xE1, 0x7F, 0xE0, 0xF0, 0x00]);
        let nit = section(0x40, 0x7FE0, rest);

        let mut parser = SiParser::new();
        let events = parser.push_packets(&section_packet(NIT_PID, &nit));
        let SiEvent::Nit(info) = &events[0] else {
            panic!("expected a NIT");
        };
        assert_eq!(info.transport_stream_ids, vec![0x7FE1]);

        // The PAT is for TSID 0x7FE0, which the NIT does not describe.
        let events = parser.push_packets(&section_packet(PAT_PID, &pat_section()));
        match &events[0] {
            SiEvent::Pat(info) => assert_eq!(info.network_id, None),
            other => panic!("expected a PAT, got {other:?}"),
        }
    }

    #[test]
    fn a_pat_is_stamped_when_the_nit_actual_names_its_transport_stream() {
        // NIT-actual for network 0x7FE0 describing TSID 0x7FE0.
        let mut rest = Vec::new();
        rest.extend_from_slice(&[0xF0, 0x00]);
        rest.extend_from_slice(&[0xF0, 0x06]); // TS loop length 6
        rest.extend_from_slice(&[0x7F, 0xE0, 0x7F, 0xE0, 0xF0, 0x00]);
        let nit = section(0x40, 0x7FE0, rest);

        let mut parser = SiParser::new();
        parser.push_packets(&section_packet(NIT_PID, &nit));
        let events = parser.push_packets(&section_packet(PAT_PID, &pat_section()));
        match &events[0] {
            SiEvent::Pat(info) => assert_eq!(info.network_id, Some(0x7FE0)),
            other => panic!("expected a PAT, got {other:?}"),
        }
    }

    #[test]
    fn sections_with_current_next_false_are_not_reported() {
        let next_version = section_header(
            0x00,
            0x7FE0,
            0xC0, // version 0, current_next = 0
            0,
            0,
            vec![0x04, 0x08, 0xE1, 0xF0],
        );

        let mut parser = SiParser::new();
        let events = parser.push_packets(&section_packet(PAT_PID, &next_version));
        assert!(events.is_empty(), "next-version PAT must not be reported");
        assert_eq!(parser.stats().ignored_sections, 1);
        assert_eq!(parser.stats().invalid_sections, 0);

        // The current version of the same table is reported. A new continuity
        // counter, or the assembler would take it for a retransmission.
        let events = parser.push_packets(&section_packets(PAT_PID, &pat_section(), 1));
        assert_eq!(events.len(), 1);
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

        // A table id nobody handles, with a valid CRC, on a PID we assemble.
        let unknown = section(0x70, 0x0001, Vec::new());
        let events = parser.push_packets(&section_packet(SDT_PID, &unknown));
        assert!(events.is_empty());
        assert_eq!(parser.stats().ignored_sections, 1);
        assert_eq!(parser.stats().invalid_sections, 0);

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
    fn drcs_glyphs_decode_to_the_replacement_character() {
        // ESC ) 20 41: designate DRCS-1 into G1, shift out, then one glyph.
        // The default mapping would invent a private-use U+EC00 here.
        let decoded = decode_arib_text(&[0x1B, 0x29, 0x20, 0x41, 0x0E, 0x21]);
        assert_eq!(decoded, "\u{FFFD}");
        assert!(
            !decoded
                .chars()
                .any(|c| ('\u{E000}'..='\u{F8FF}').contains(&c)),
            "no private-use code point may survive: {decoded:?}"
        );

        // A DRCS glyph inside otherwise ordinary text keeps the text.
        assert_eq!(
            decode_arib_text(&[
                0x1B, 0x28, 0x4A, b'A', // designates JIS X 0201 alphanumeric
                0x1B, 0x29, 0x20, 0x41, // designates DRCS-1 into G1
                0x0E, 0x21, // shift out, one DRCS glyph
            ]),
            "A\u{FFFD}"
        );
    }

    #[test]
    fn malformed_text_decodes_without_panicking_or_printing_to_stderr() {
        // C1 control sequences, truncated escapes and random high bytes: the
        // decoder must swallow all of it (the panic hook is muted inside
        // `decode_arib_text`, so a corrupt name produces no stderr noise).
        let mut seed = 0x1234_5678u32;
        for len in 0..64 {
            for _ in 0..20 {
                let bytes: Vec<u8> = (0..len)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 17;
                        seed ^= seed << 5;
                        seed as u8
                    })
                    .collect();
                let decoded = decode_arib_text(&bytes);
                // Whatever comes out must be valid, printable-ish text.
                assert!(
                    !decoded.contains('\u{0}'),
                    "NUL must be trimmed from {bytes:02X?}"
                );
            }
        }

        // A truncated escape and a lead byte without its trail decode to ""
        // rather than panicking, and a complete escape still decodes.
        assert_eq!(decode_arib_text(&[0x1B, 0x24, 0x42, 0x21]), "");
        assert_eq!(decode_arib_text(&[0x1B]), "");
        assert_eq!(decode_arib_text(&[0x1B, 0x24, 0x42, 0x22, 0x2F]), "＇");
    }

    #[test]
    fn pes_packets_do_not_inflate_invalid_sections() {
        let mut parser = SiParser::new();

        // A PES payload on a video PID: PUSI set, so a naive assembler would
        // start a "section" from the PES header and fail to parse it.
        let mut payload = vec![0x00, 0x00, 0x01, 0xE0];
        payload.extend_from_slice(&[0x00, 0x00, 0x80, 0x80, 0x05]);
        payload.extend(std::iter::repeat_n(0xAA, 100));
        let events = parser.push_packets(&packet(0x0111, true, 0, &payload));
        assert!(events.is_empty());
        assert_eq!(parser.stats().invalid_sections, 0);
        assert_eq!(parser.stats().non_si_packets, 1);

        // A healthy PAT still parses and does not count as an error.
        let events = parser.push_packets(&section_packet(PAT_PID, &pat_section()));
        assert_eq!(events.len(), 1);
        assert_eq!(parser.stats().invalid_sections, 0);
    }

    #[test]
    fn eit_sections_on_their_pid_are_not_assembled_at_all() {
        // An EIT (table id 0x4E) on PID 0x0012: valid CRC, but nobody asked
        // for it. It must not be copied, reported, or counted as corruption.
        let eit = section(0x4E, 0x0408, vec![0x00; 40]);

        let mut assembler = SectionAssembler::with_interest([PAT_PID, NIT_PID, SDT_PID]);
        let sections = assembler.push_packet(&section_packet(0x0012, &eit));
        assert!(sections.is_empty());
        assert_eq!(assembler.non_si_packets(), 1);
        assert_eq!(assembler.ignored_sections(), 0);

        let mut parser = SiParser::new();
        let events = parser.push_packets(&section_packet(0x0012, &eit));
        assert!(events.is_empty());
        assert_eq!(parser.stats().sections, 0);
        assert_eq!(parser.stats().invalid_sections, 0);
        assert_eq!(parser.stats().non_si_packets, 1);
    }

    #[test]
    fn a_table_allowlist_skips_sections_without_copying_them() {
        // The EIT is on an allowed PID but its table is not allowed: it is
        // skipped at the table-id byte, counted as ignored but never handed
        // out and never counted as invalid.
        let eit = section(0x4E, 0x0408, vec![0x00; 40]);

        let mut assembler = SectionAssembler::with_interest([SDT_PID]).with_tables([0x42]);
        let sections = assembler.push_packet(&section_packet(SDT_PID, &eit));
        assert!(sections.is_empty());
        assert_eq!(assembler.ignored_sections(), 1);

        // A multi-packet unwanted section is skipped too, without buffering it.
        let long = section(0x4E, 0x0408, vec![0x11; 400]);
        assert!(long.len() > TS_PACKET_SIZE * 2);
        let mut assembler = SectionAssembler::with_interest([SDT_PID]).with_tables([0x42]);
        let mut sections = Vec::new();
        for packet in section_packets(SDT_PID, &long, 0).chunks_exact(TS_PACKET_SIZE) {
            sections.extend(assembler.push_packet(packet));
        }
        assert!(sections.is_empty());
        assert_eq!(assembler.ignored_sections(), 1);

        // The wanted table on the same PID still comes through.
        let wanted = sdt_section();
        let sections = assembler.push_packets(&section_packets(SDT_PID, &wanted, 5));
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].bytes, wanted);
    }

    #[test]
    fn the_default_interest_filter_still_parses_pat_pmt_sdt_and_nit() {
        let mut parser = SiParser::new();
        let mut events = Vec::new();
        events.extend(parser.push_packets(&section_packet(PAT_PID, &pat_section())));
        // The PMT PID is only known once the PAT named it.
        assert!(parser.assembler.is_interested(0x01F0));
        events.extend(parser.push_packets(&section_packet(0x01F0, &pmt_section())));
        events.extend(parser.push_packets(&section_packet(SDT_PID, &sdt_section())));
        events.extend(parser.push_packets(&section_packet(NIT_PID, &nit_section())));

        assert_eq!(events.len(), 4);
        assert!(matches!(events[0], SiEvent::Pat(_)));
        assert!(matches!(events[1], SiEvent::Pmt(_)));
        assert!(matches!(events[2], SiEvent::Sdt(_)));
        assert!(matches!(events[3], SiEvent::Nit(_)));
        assert_eq!(parser.stats().invalid_sections, 0);
        assert_eq!(parser.stats().non_si_packets, 0);
    }

    #[test]
    fn full_assembly_still_assembles_every_pid_for_tests() {
        let eit = section(0x4E, 0x0408, vec![0x00; 10]);
        let mut parser = SiParser::with_full_assembly();
        let events = parser.push_packets(&section_packet(0x0012, &eit));
        assert!(events.is_empty());
        // The section was assembled and then ignored by table id.
        assert_eq!(parser.stats().sections, 1);
        assert_eq!(parser.stats().ignored_sections, 1);
        assert_eq!(parser.stats().non_si_packets, 0);
    }

    #[test]
    fn the_parser_learns_a_non_standard_nit_pid_from_the_pat() {
        // PAT with program 0 -> NIT PID 0x0020 instead of the usual 0x0010.
        let pat = section(
            0x00,
            0x7FE0,
            vec![
                0x00, 0x00, 0xE0, 0x20, // program 0 -> NIT PID 0x020
                0x04, 0x08, 0xE1, 0xF0, // program 0x0408 -> PMT PID 0x1F0
            ],
        );

        let mut parser = SiParser::new();
        parser.push_packets(&section_packet(PAT_PID, &pat));
        assert!(parser.assembler.is_interested(0x0020));

        // A NIT on that PID now parses.
        let events = parser.push_packets(&section_packets(0x0020, &nit_section(), 0));
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SiEvent::Nit(_)));
        assert_eq!(parser.network_id(), Some(0x7FE0));
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

    /// A full multiplex (PAT, PMT, SDT, NIT) repeated a few times, so the
    /// chunk-boundary behaviour is checked on sections that both fit one packet
    /// and span several.
    fn multiplex() -> Vec<u8> {
        let mut stream = Vec::new();
        for round in 0..3u8 {
            stream.extend(section_packets(PAT_PID, &pat_section(), round * 3));
            stream.extend(section_packets(NIT_PID, &nit_section(), round * 3));
            stream.extend(section_packets(SDT_PID, &sdt_section(), round * 3));
            // The PMT PID is only known after the PAT, which is why it follows.
            stream.extend(section_packets(0x01F0, &pmt_section(), round * 3));
            stream.extend(packet(0x1FFF, false, round, &[0xFF; 184]));
        }
        stream
    }

    /// Runs `stream` through a fresh parser in `chunk`-byte reads.
    fn parse_in_chunks(stream: &[u8], chunk: usize) -> (Vec<SiEvent>, SiStats) {
        let mut parser = SiParser::new();
        let mut events = Vec::new();
        for piece in stream.chunks(chunk) {
            events.extend(parser.push_packets(piece));
        }
        (events, parser.stats())
    }

    #[test]
    fn chunk_boundaries_do_not_change_the_parse_result() {
        // A tuner read is not a multiple of 188 bytes: 65536 % 188 == 112, and a
        // pipe read has no alignment at all. Every one of these chunk sizes must
        // yield exactly the same sections, in the same order.
        let stream = multiplex();
        assert_eq!(stream.len() % TS_PACKET_SIZE, 0);

        let (baseline_events, baseline_stats) = parse_in_chunks(&stream, TS_PACKET_SIZE);
        assert!(
            baseline_events.len() >= 12,
            "the fixture must produce several sections, got {}",
            baseline_events.len()
        );
        assert_eq!(baseline_events.len() as u64, baseline_stats.sections);
        assert_eq!(baseline_stats.invalid_sections, 0);

        for chunk in [1usize, 3, 188, 4096, 32_768, 65_536, 65_612] {
            let (events, stats) = parse_in_chunks(&stream, chunk);
            assert_eq!(
                events, baseline_events,
                "chunk size {chunk} produced different SI events"
            );
            assert_eq!(
                stats.sections, baseline_stats.sections,
                "chunk size {chunk} produced a different section count"
            );
            assert_eq!(stats.invalid_sections, 0, "chunk size {chunk}");
            assert_eq!(stats.packets, baseline_stats.packets, "chunk size {chunk}");
        }
    }

    #[test]
    fn a_packet_cut_by_a_chunk_boundary_is_carried_over() {
        let packets = section_packets(PAT_PID, &pat_section(), 0);
        assert_eq!(packets.len(), TS_PACKET_SIZE);
        // Feed one byte short of the packet, then the rest.
        let mut assembler = SectionAssembler::new();
        assert!(assembler
            .push_packets(&packets[..TS_PACKET_SIZE - 1])
            .is_empty());
        let sections = assembler.push_packets(&packets[TS_PACKET_SIZE - 1..]);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].bytes, pat_section());

        // The same through the parser, with the split inside the section body.
        let mut parser = SiParser::new();
        let split = 17;
        assert!(parser.push_packets(&packets[..split]).is_empty());
        let events = parser.push_packets(&packets[split..]);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SiEvent::Pat(_)));
    }

    #[test]
    fn reset_drops_a_carried_over_packet() {
        let packets = section_packets(PAT_PID, &pat_section(), 0);
        let mut parser = SiParser::new();
        assert!(parser.push_packets(&packets[..100]).is_empty());
        parser.reset();
        // After a reset the tail of the old packet must not be glued to new
        // input, so the input alone never forms a section.
        let events = parser.push_packets(&packets[100..]);
        assert!(events.is_empty());
        assert_eq!(parser.stats().sections, 0);
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

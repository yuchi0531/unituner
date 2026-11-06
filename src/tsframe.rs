//! Incremental MPEG-TS packet framing (SPEC §4).
//!
//! Tuner output arrives as a byte stream in arbitrary chunks and may be
//! 188-byte packets or 204-byte packets (188 bytes plus the 16-byte
//! Reed-Solomon parity tail). This module turns either into a clean run of
//! 188-byte packets, discarding leading junk and resynchronising after a lost
//! sync byte. It is synchronous and allocates nothing per packet beyond the
//! output buffer it is asked to fill.
//!
//! # Supported framings
//!
//! * 188: passed through byte-for-byte.
//! * 204: the trailing 16-byte parity tail of every packet is dropped.
//! * 192 (a 4-byte timestamp prefix before each 188-byte packet) is **not
//!   supported**: SPEC §4 only requires 188 and 204. It is still *detected*
//!   and reported as [`Framing::Ts192Unsupported`]; from then on every byte is
//!   discarded and counted, so a 192 stream can never be emitted as garbage
//!   misaligned packets.
//!
//! # Detection
//!
//! At the start of the stream the packet size is decided from sync votes
//! (following the softcas-rs approach): the number of consecutive `0x47`
//! bytes at the 188, 204 and 192 strides is counted, up to eight votes each.
//! A decision needs two votes (two aligned packets) *and* a strict lead over
//! the other candidates, or a forced decision at [`TsNormalizer::finish`].
//!
//! A 188-versus-204 tie is not decided on the spot. It is a normal state early
//! in a 204 stream: a byte at offset 188 is that packet's Reed-Solomon parity
//! tail, and parity may well be `0x47`, which makes the 188 stride score one
//! vote too. Voting therefore keeps accumulating until one candidate leads;
//! only at end of stream ([`TsNormalizer::finish`]), when the vote window is
//! exhausted, is the 188-first tie-break used. A single 204-byte packet is
//! indistinguishable from a 188-byte one by sync votes and is read as 188 at
//! EOF (correct bytes, wrong framing), so a caller that flushes mid-stream
//! should use [`TsNormalizer::finish_and_reset`] rather than leaving that
//! framing locked in.
//!
//! # Resynchronisation
//!
//! Once the framing is known, a lost sync byte is recovered by searching for a
//! *confirmed* packet start: a `0x47` at some position **and** another `0x47`
//! one packet size after it. A single `0x47` is not enough: about half of all
//! TS packets carry `0x47` somewhere in their payload, so accepting the first
//! one would lock the stream onto a false boundary and keep it there. When the
//! confirming byte is not buffered yet, the normalizer waits for more input
//! instead of committing. Only at [`TsNormalizer::finish`], where no more input
//! can arrive, does the last unconfirmed candidate win.

/// Size of a normalized TS packet.
pub const TS_PACKET_SIZE: usize = 188;

/// Size of a TS packet with the 16-byte Reed-Solomon parity tail.
pub const TS_PACKET_SIZE_204: usize = 204;

/// Size of a TS packet with a 4-byte prefix. Unsupported; only detected.
const TS_PACKET_SIZE_192: usize = 192;

/// The sync byte every TS packet starts with.
pub const SYNC_BYTE: u8 = 0x47;

/// Maximum number of sync votes counted per candidate framing.
const MAX_SYNC_VOTES: usize = 8;

/// A decision needs two votes (two aligned sync bytes) from one candidate.
///
/// A lone `0x47` cannot distinguish 188 from 204 (both score one), so fewer
/// than two votes leave the framing undecided while more input is expected.
const MIN_DECIDING_VOTES: usize = 2;

/// After this much undecided input the leading `0x47` is taken as junk and
/// skipped, one byte at a time. Two 204-byte packets plus one byte: past this
/// point a genuine packet start must have produced a second sync byte.
const DETECT_GIVE_UP_BYTES: usize = 2 * TS_PACKET_SIZE_204 + 1;

/// The framing a stream was found to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    /// 188-byte packets.
    Ts188,
    /// 204-byte packets (188 bytes plus a 16-byte parity tail).
    Ts204,
    /// 192-byte packets (4-byte prefix plus 188 bytes). Detected, not
    /// supported: no packets are emitted.
    Ts192Unsupported,
}

impl Framing {
    /// Length of one packet on the wire, or `None` for an unsupported framing.
    pub fn packet_len(self) -> Option<usize> {
        match self {
            Self::Ts188 => Some(TS_PACKET_SIZE),
            Self::Ts204 => Some(TS_PACKET_SIZE_204),
            Self::Ts192Unsupported => None,
        }
    }
}

/// Counts consecutive sync bytes at `stride` from `offset`, up to
/// [`MAX_SYNC_VOTES`]. Returns zero when the byte at `offset` is not `0x47`.
fn count_syncs(buf: &[u8], stride: usize, offset: usize) -> usize {
    if buf.len() <= offset || buf[offset] != SYNC_BYTE {
        return 0;
    }
    let mut votes = 1;
    let mut pos = offset + stride;
    while pos < buf.len() && votes < MAX_SYNC_VOTES {
        if buf[pos] != SYNC_BYTE {
            break;
        }
        votes += 1;
        pos += stride;
    }
    votes
}

/// Turns a tuner byte stream into 188-byte packets.
///
/// Feed bytes with [`push`](Self::push) and call [`finish`](Self::finish) at
/// end of stream. All methods are synchronous.
///
/// ```
/// use unituner::tsframe::{TsNormalizer, TS_PACKET_SIZE};
///
/// let mut packet = [0xFFu8; TS_PACKET_SIZE];
/// packet[0] = 0x47;
/// // 204-byte framing: two packets so the size is detected, then the 16-byte
/// // tails are removed and 188-byte packets come back.
/// let mut framed = Vec::new();
/// for _ in 0..2 {
///     framed.extend_from_slice(&packet);
///     framed.extend_from_slice(&[0u8; 16]);
/// }
///
/// let mut normalizer = TsNormalizer::new();
/// let mut out = normalizer.push(&framed);
/// out.extend(normalizer.finish());
/// assert_eq!(out, [packet, packet].concat());
/// ```
#[derive(Debug, Default)]
pub struct TsNormalizer {
    buf: Vec<u8>,
    framing: Option<Framing>,
    /// Complete 188-byte packets emitted so far.
    pub packets_out: u64,
    /// Times the byte at a packet boundary was not `0x47` and the stream had
    /// to be resynchronised. Leading junk before the first sync byte is *not*
    /// counted here; it is counted in [`bytes_discarded`](Self::bytes_discarded).
    pub sync_errors: u64,
    /// Bytes dropped: leading junk, resynchronisation skips, unsupported-192
    /// input and an incomplete packet left at [`finish`](Self::finish).
    pub bytes_discarded: u64,
    /// Whether the sync error at the current resynchronisation point has
    /// already been counted. A resync may span several `push` calls (the
    /// confirming sync byte has not arrived yet), and the error must be counted
    /// once, not once per call.
    sync_error_pending: bool,
}

/// The outcome of searching for a confirmed packet start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Resync {
    /// A packet start confirmed by the next packet's sync byte at this offset.
    At(usize),
    /// A `0x47` was seen but the byte one packet later is not buffered yet, so
    /// the candidate cannot be confirmed. Wait for more input.
    NeedMore,
    /// No position in the buffer can start a packet.
    NotFound,
}

impl TsNormalizer {
    /// Creates a normalizer with no framing decided yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The framing decided for this stream, if any.
    pub fn framing(&self) -> Option<Framing> {
        self.framing
    }

    /// Bytes currently buffered awaiting more input.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// Feeds bytes and returns the complete 188-byte packets that became
    /// available, back to back. Returns an empty vector when more input is
    /// needed to decide the framing or to complete a packet.
    ///
    /// Once an unsupported framing (192) is detected, every byte is discarded
    /// and counted, so the internal buffer never grows without bound.
    pub fn push(&mut self, data: &[u8]) -> Vec<u8> {
        self.buf.extend_from_slice(data);
        if self.framing.is_none() {
            self.detect(false);
        }
        let mut out = Vec::new();
        self.drain_packets(&mut out, false);
        out
    }

    /// Flushes the stream at end of input: decides the framing from whatever
    /// is buffered, returns the remaining complete packets and discards an
    /// incomplete tail.
    ///
    /// This is an **EOF-only** operation. The detected framing is kept
    /// afterwards (see [`finish_and_reset`](Self::finish_and_reset)) and an
    /// undecided stream is forced to a framing here — a single 204-byte packet,
    /// for example, is indistinguishable from a 188-byte one and is read as
    /// 188. A caller flushing mid-stream must therefore use
    /// [`finish_and_reset`](Self::finish_and_reset), or it would lock that
    /// guess in permanently.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        if self.framing.is_none() {
            self.detect(true);
        }
        self.drain_packets(&mut out, true);
        if !self.buf.is_empty() {
            self.bytes_discarded += self.buf.len() as u64;
            self.buf.clear();
        }
        // End of stream terminates any resynchronisation: a later `push` on
        // this normalizer is a new stream segment, so its first bad sync byte
        // is a new error.
        self.sync_error_pending = false;
        out
    }

    /// Like [`finish`](Self::finish), but forgets the framing afterwards, so
    /// the next [`push`](Self::push) detects it again from fresh bytes.
    ///
    /// Use this when flushing a stream that may resume (a reconnect, a
    /// re-tuning tuner) and the framing of the resumed stream is not known to
    /// be the same. Counters are preserved: only the framing guess and the
    /// buffered bytes are dropped.
    pub fn finish_and_reset(&mut self) -> Vec<u8> {
        let out = self.finish();
        self.framing = None;
        self.buf.clear();
        out
    }

    /// Forgets all state, including the detected framing and the counters.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Decides the framing from the buffered bytes.
    ///
    /// Leaves the framing undecided when more data is needed (`force == false`)
    /// and the votes do not yet point clearly at one candidate. Leading junk is
    /// dropped and counted on every path.
    fn detect(&mut self, force: bool) {
        let mut start = 0usize;
        loop {
            let Some(rel) = self.buf[start..].iter().position(|&b| b == SYNC_BYTE) else {
                // No sync byte left: nothing here can start a packet.
                self.bytes_discarded += self.buf.len() as u64;
                self.buf.clear();
                return;
            };
            start += rel;

            let window = &self.buf[start..];
            let votes_188 = count_syncs(window, TS_PACKET_SIZE, 0);
            let votes_204 = count_syncs(window, TS_PACKET_SIZE_204, 0);
            let votes_192 = count_syncs(window, TS_PACKET_SIZE_192, 0);
            let votes = votes_188.max(votes_204).max(votes_192);

            if !force {
                if votes < MIN_DECIDING_VOTES {
                    if window.len() < DETECT_GIVE_UP_BYTES {
                        // Too little data to tell 188 from 204 yet.
                        self.discard_prefix(start);
                        return;
                    }
                    // Two full 204-byte packets and no confirming sync byte:
                    // this 0x47 is junk, not a packet start. Step over it.
                    start += 1;
                    continue;
                }

                // A 188/204 tie is *not* a decision. In a 204 stream the byte
                // at offset 188 is Reed-Solomon parity and is `0x47` about once
                // in 256 packets, so a short 204 stream can score as many 188
                // votes as 204 votes, and deciding there would be permanent.
                //
                // Keep accumulating until exactly one candidate leads. Give up
                // waiting once the whole vote window has been seen (a tie that
                // survives eight aligned packets is not going to break) or once
                // a whole window's worth of bytes is buffered (the chains can
                // both stop early, after two votes each); `finish` breaks any
                // remaining tie at EOF.
                let leaders = u8::from(votes_188 == votes)
                    + u8::from(votes_204 == votes)
                    + u8::from(votes_192 == votes);
                let window_exhausted =
                    votes >= MAX_SYNC_VOTES || window.len() >= MAX_SYNC_VOTES * TS_PACKET_SIZE_204;
                if leaders > 1 && !window_exhausted {
                    self.discard_prefix(start);
                    return;
                }
            }

            // Tie-break order is 188, then 204, then 192.
            let framing = if votes_192 > votes_188 && votes_192 > votes_204 {
                Framing::Ts192Unsupported
            } else if votes_204 > votes_188 {
                Framing::Ts204
            } else {
                Framing::Ts188
            };
            self.discard_prefix(start);
            self.framing = Some(framing);
            return;
        }
    }

    /// Drops and counts `len` bytes from the front of the buffer.
    fn discard_prefix(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        self.bytes_discarded += len as u64;
        self.buf.drain(..len);
    }

    /// Searches for a *confirmed* packet start at or after `from`.
    ///
    /// A candidate `0x47` is only confirmed when the byte one packet later is
    /// also `0x47`; a lone `0x47` in a payload would otherwise lock the stream
    /// onto a false boundary permanently. When the confirming byte is past the
    /// end of the buffer, the caller must wait for more input — unless `eof`
    /// says no more can arrive, in which case the first candidate with a whole
    /// packet buffered is taken.
    fn find_resync(&self, from: usize, packet_len: usize, eof: bool) -> Resync {
        let mut candidate = from;
        while let Some(rel) = self.buf[candidate..].iter().position(|&b| b == SYNC_BYTE) {
            candidate += rel;
            if candidate + packet_len < self.buf.len() {
                if self.buf[candidate + packet_len] == SYNC_BYTE {
                    return Resync::At(candidate);
                }
                // A false sync inside a payload: try the next candidate.
                candidate += 1;
                continue;
            }
            if eof {
                // No more input can confirm or refute this candidate.
                return if self.buf.len() - candidate >= packet_len {
                    Resync::At(candidate)
                } else {
                    Resync::NotFound
                };
            }
            // The confirming byte is not buffered yet: wait for more data.
            return Resync::NeedMore;
        }
        Resync::NotFound
    }

    /// Emits complete packets from the buffer at the detected framing.
    ///
    /// The buffer is compacted once at the end, so a large chunk costs one
    /// memmove rather than one per packet. `eof` allows the last, unconfirmed
    /// resynchronisation candidate to be used; see [`find_resync`](Self::find_resync).
    fn drain_packets(&mut self, out: &mut Vec<u8>, eof: bool) {
        let Some(framing) = self.framing else {
            return;
        };
        let Some(packet_len) = framing.packet_len() else {
            // Unsupported 192 framing: never emit misaligned bytes.
            self.bytes_discarded += self.buf.len() as u64;
            self.buf.clear();
            return;
        };

        let mut pos = 0usize;
        // One physical sync loss may span several `push` calls (the confirming
        // byte has not arrived yet), so the error is counted once.
        let mut resyncing = self.sync_error_pending;
        while pos < self.buf.len() {
            // While resyncing, even a byte that *is* 0x47 is only a candidate:
            // it must be confirmed by the next packet's sync byte. Skipping
            // that check is exactly how a 0x47 in a payload becomes a
            // permanent misalignment.
            if resyncing || self.buf[pos] != SYNC_BYTE {
                if !resyncing {
                    self.sync_errors += 1;
                    resyncing = true;
                }
                match self.find_resync(pos, packet_len, eof) {
                    Resync::At(next) => {
                        self.bytes_discarded += (next - pos) as u64;
                        pos = next;
                        resyncing = false;
                    }
                    Resync::NeedMore => break,
                    Resync::NotFound => {
                        // No position in the buffer can start a confirmed
                        // packet: all of it is junk. The sync loss is not over
                        // yet (the stream has not resumed), so keep the event
                        // open to avoid counting the rest of this same loss as
                        // further errors.
                        self.bytes_discarded += (self.buf.len() - pos) as u64;
                        pos = self.buf.len();
                    }
                }
                continue;
            }
            if self.buf.len() - pos < packet_len {
                break;
            }
            out.extend_from_slice(&self.buf[pos..pos + TS_PACKET_SIZE]);
            self.packets_out += 1;
            pos += packet_len;
        }
        self.sync_error_pending = resyncing;
        self.buf.drain(..pos);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(fill: u8) -> Vec<u8> {
        let mut packet = vec![fill; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        packet
    }

    fn with_parity(packet: &[u8]) -> Vec<u8> {
        let mut framed = packet.to_vec();
        framed.extend_from_slice(&[0x5A; 16]);
        framed
    }

    /// A packet filled with `fill` whose payload also carries a `0x47` at each
    /// given absolute packet offset (payload starts at byte 4).
    fn packet_with_syncs(fill: u8, sync_offsets: &[usize]) -> Vec<u8> {
        let mut packet = packet(fill);
        for &offset in sync_offsets {
            assert!((4..TS_PACKET_SIZE).contains(&offset));
            packet[offset] = SYNC_BYTE;
        }
        packet
    }

    fn stream(packets: &[Vec<u8>]) -> Vec<u8> {
        packets.concat()
    }

    #[test]
    fn passes_188_packets_through_byte_identically() {
        let input = stream(&[packet(0x00), packet(0x11), packet(0x22), packet(0x33)]);
        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, input);
        assert_eq!(normalizer.packets_out, 4);
        assert_eq!(normalizer.sync_errors, 0);
        assert_eq!(normalizer.bytes_discarded, 0);
        assert_eq!(normalizer.framing(), Some(Framing::Ts188));
    }

    #[test]
    fn strips_the_204_parity_tail() {
        let packets = [packet(0x01), packet(0x02), packet(0x03)];
        let input: Vec<u8> = packets
            .iter()
            .map(|p| with_parity(p))
            .collect::<Vec<_>>()
            .concat();

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&packets));
        assert_eq!(normalizer.packets_out, 3);
        assert_eq!(normalizer.bytes_discarded, 0);
        assert_eq!(normalizer.framing(), Some(Framing::Ts204));
    }

    #[test]
    fn skips_leading_garbage() {
        let packets = [packet(0x01), packet(0x02), packet(0x03)];
        let mut input = vec![0xAA, 0xBB, 0xCC, 0x00];
        input.extend_from_slice(&stream(&packets));

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&packets));
        assert_eq!(normalizer.bytes_discarded, 4);
        assert_eq!(normalizer.sync_errors, 0);
    }

    #[test]
    fn leading_garbage_with_a_false_sync_byte_is_skipped() {
        let packets = [packet(0x01), packet(0x02), packet(0x03)];
        // 0x47 inside the junk is a false start: no third sync follows it.
        let mut input = vec![0x47, 0x00, 0x00];
        input.extend_from_slice(&stream(&packets));

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&packets));
        assert_eq!(normalizer.bytes_discarded, 3);
    }

    #[test]
    fn resynchronises_after_a_corrupted_sync_byte() {
        let packets = [packet(0x01), packet(0x02), packet(0x03)];
        let mut input = stream(&packets);
        input[2 * TS_PACKET_SIZE] = 0x00; // break the sync byte of packet 3

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&packets[..2]));
        assert_eq!(normalizer.packets_out, 2);
        assert_eq!(normalizer.sync_errors, 1);
        assert_eq!(normalizer.bytes_discarded, TS_PACKET_SIZE as u64);
    }

    #[test]
    fn resynchronises_to_the_true_boundary_when_payloads_contain_0x47() {
        // Every payload carries a 0x47, so a naive "first 0x47 wins" resync
        // would lock onto one of them. Only the true packet boundary is
        // confirmed by the next packet's sync byte.
        let packets = [
            packet_with_syncs(0x10, &[60, 150]),
            packet_with_syncs(0x11, &[40]),
            packet_with_syncs(0x12, &[100]),
            packet_with_syncs(0x13, &[120]),
            packet_with_syncs(0x14, &[80]),
        ];
        let mut input = stream(&packets);
        input[2 * TS_PACKET_SIZE] = 0x00; // break the sync byte of packet 3

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        let expected = stream(&[
            packets[0].clone(),
            packets[1].clone(),
            packets[3].clone(),
            packets[4].clone(),
        ]);
        assert_eq!(out, expected);
        assert_eq!(normalizer.packets_out, 4);
        assert_eq!(normalizer.sync_errors, 1);
        assert_eq!(normalizer.bytes_discarded, TS_PACKET_SIZE as u64);
    }

    #[test]
    fn a_false_0x47_in_a_payload_is_not_adopted() {
        // Packet 3's payload contains a lone 0x47 (offset 50). One packet later
        // that offset holds ordinary payload, so the candidate is rejected and
        // the true boundary of packet 4 is used instead. (The broken packet is
        // late enough that the framing is still detected from clean packets.)
        let packets = [
            packet_with_syncs(0x21, &[]),
            packet_with_syncs(0x22, &[]),
            packet_with_syncs(0x23, &[]),
            packet_with_syncs(0x24, &[50]),
            packet_with_syncs(0x25, &[]),
            packet_with_syncs(0x26, &[]),
        ];
        let mut input = stream(&packets);
        input[3 * TS_PACKET_SIZE] = 0x00; // break the sync byte of packet 4

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        let expected = stream(&[
            packets[0].clone(),
            packets[1].clone(),
            packets[2].clone(),
            packets[4].clone(),
            packets[5].clone(),
        ]);
        assert_eq!(out, expected);
        assert_eq!(normalizer.sync_errors, 1);
        assert_eq!(normalizer.bytes_discarded, TS_PACKET_SIZE as u64);
    }

    #[test]
    fn one_byte_chunks_still_resynchronise_past_payload_sync_bytes() {
        let packets = [
            packet_with_syncs(0x31, &[60, 150]),
            packet_with_syncs(0x32, &[40]),
            packet_with_syncs(0x33, &[100]),
            packet_with_syncs(0x34, &[120]),
            packet_with_syncs(0x35, &[80]),
            packet_with_syncs(0x36, &[]),
        ];
        let mut input = stream(&packets);
        input[3 * TS_PACKET_SIZE] = 0x00;

        let mut whole = TsNormalizer::new();
        let mut expected = whole.push(&input);
        expected.extend(whole.finish());

        let mut chunked = TsNormalizer::new();
        let mut actual = Vec::new();
        for byte in &input {
            actual.extend(chunked.push(std::slice::from_ref(byte)));
        }
        actual.extend(chunked.finish());

        assert_eq!(actual, expected);
        assert_eq!(chunked.packets_out, 5);
        assert_eq!(chunked.sync_errors, 1);
        assert_eq!(chunked.bytes_discarded, TS_PACKET_SIZE as u64);
        // The output must be whole, packet-aligned source packets.
        for (index, packet) in actual.chunks_exact(TS_PACKET_SIZE).enumerate() {
            assert_eq!(packet[0], SYNC_BYTE, "packet {index} lost alignment");
        }
    }

    #[test]
    fn resynchronises_within_a_204_stream() {
        let packets = [packet(0x01), packet(0x02), packet(0x03), packet(0x04)];
        let mut input: Vec<u8> = packets
            .iter()
            .map(|p| with_parity(p))
            .collect::<Vec<_>>()
            .concat();
        input[2 * TS_PACKET_SIZE_204] = 0xFF; // break the sync byte of packet 3

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        // Packets 1 and 2 survive; packet 3 is lost; packet 4 is reached by
        // the resync and survives.
        assert_eq!(
            out,
            stream(&[packets[0].clone(), packets[1].clone(), packets[3].clone()])
        );
        assert_eq!(normalizer.sync_errors, 1);
        assert_eq!(normalizer.bytes_discarded, TS_PACKET_SIZE_204 as u64);
    }

    #[test]
    fn one_byte_chunks_match_a_single_big_chunk() {
        let packets = [packet(0x01), packet(0x02), packet(0x03), packet(0x04)];
        let input = stream(&packets);

        let mut whole = TsNormalizer::new();
        let mut expected = whole.push(&input);
        expected.extend(whole.finish());

        let mut chunked = TsNormalizer::new();
        let mut actual = Vec::new();
        for byte in &input {
            actual.extend(chunked.push(std::slice::from_ref(byte)));
        }
        actual.extend(chunked.finish());

        assert_eq!(actual, expected);
        assert_eq!(actual, input);
        assert_eq!(chunked.packets_out, whole.packets_out);
        assert_eq!(chunked.bytes_discarded, whole.bytes_discarded);
    }

    #[test]
    fn one_byte_chunks_match_for_204_framing() {
        let packets = [packet(0x01), packet(0x02), packet(0x03), packet(0x04)];
        let input: Vec<u8> = packets
            .iter()
            .map(|p| with_parity(p))
            .collect::<Vec<_>>()
            .concat();

        let mut whole = TsNormalizer::new();
        let mut expected = whole.push(&input);
        expected.extend(whole.finish());

        let mut chunked = TsNormalizer::new();
        let mut actual = Vec::new();
        for byte in &input {
            actual.extend(chunked.push(std::slice::from_ref(byte)));
        }
        actual.extend(chunked.finish());

        assert_eq!(actual, expected);
        assert_eq!(actual, stream(&packets));
    }

    #[test]
    fn detects_192_as_unsupported_and_emits_nothing() {
        let mut input = Vec::new();
        for _ in 0..4 {
            input.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            input.extend_from_slice(&packet(0x01));
        }

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert!(out.is_empty());
        assert_eq!(normalizer.framing(), Some(Framing::Ts192Unsupported));
        assert_eq!(normalizer.packets_out, 0);
    }

    #[test]
    fn an_unsupported_192_stream_does_not_grow_the_buffer() {
        let mut input = Vec::new();
        for _ in 0..4 {
            input.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            input.extend_from_slice(&packet(0x01));
        }

        let mut normalizer = TsNormalizer::new();
        let mut discarded = 0u64;
        for _ in 0..8 {
            assert!(normalizer.push(&input).is_empty());
            discarded += input.len() as u64;
            assert_eq!(normalizer.buffered(), 0);
        }
        assert_eq!(normalizer.bytes_discarded, discarded);
        assert_eq!(normalizer.packets_out, 0);
    }

    #[test]
    fn incomplete_trailing_packet_is_discarded() {
        let packets = [packet(0x01), packet(0x02)];
        let mut input = stream(&packets);
        input.truncate(input.len() - 10);

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&input);
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&packets[..1]));
        assert_eq!(normalizer.packets_out, 1);
        assert_eq!(normalizer.bytes_discarded, (TS_PACKET_SIZE - 10) as u64);
    }

    #[test]
    fn garbage_only_input_is_fully_discarded() {
        let mut normalizer = TsNormalizer::new();
        let out = normalizer.push(&[0x00, 0x11, 0x22]);
        assert!(out.is_empty());

        let out = normalizer.finish();
        assert!(out.is_empty());
        assert_eq!(normalizer.packets_out, 0);
        assert_eq!(normalizer.bytes_discarded, 3);
    }

    #[test]
    fn finish_is_a_flush_and_the_normalizer_stays_usable() {
        let first = packet(0x01);
        let second = packet(0x02);

        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&first);
        out.extend(normalizer.finish());
        out.extend(normalizer.push(&second));
        out.extend(normalizer.finish());

        assert_eq!(out, stream(&[first, second]));
        assert_eq!(normalizer.packets_out, 2);
    }

    #[test]
    fn a_204_tie_broken_by_a_parity_byte_is_still_detected_as_204() {
        // A real 204 stream whose first packet's parity tail starts with 0x47:
        // the 188 stride then scores a second "sync" and a naive implementation
        // decides 188 permanently. Feeding the stream in 188-byte chunks makes
        // the tie appear exactly when about two packets are buffered.
        let packets = [packet(0x01), packet(0x02), packet(0x03), packet(0x04)];
        let mut input: Vec<u8> = packets
            .iter()
            .map(|p| {
                let mut framed = p.clone();
                framed.extend_from_slice(&[0x47, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5]);
                framed.extend_from_slice(&[0xA5; 8]);
                framed
            })
            .collect::<Vec<Vec<u8>>>()
            .concat();
        // Sanity: the byte at the 188 stride of the first packet is 0x47.
        assert_eq!(input[TS_PACKET_SIZE], SYNC_BYTE);
        // Keep the later tails from accidentally extending the false 188 chain.
        for index in 2..packets.len() {
            let offset = index * TS_PACKET_SIZE_204 + TS_PACKET_SIZE;
            input[offset] = 0xA5;
        }

        let mut normalizer = TsNormalizer::new();
        let mut out = Vec::new();
        for chunk in input.chunks(TS_PACKET_SIZE) {
            out.extend(normalizer.push(chunk));
        }
        out.extend(normalizer.finish());

        assert_eq!(normalizer.framing(), Some(Framing::Ts204));
        assert_eq!(out, stream(&packets));
        assert_eq!(normalizer.packets_out, packets.len() as u64);
        assert_eq!(normalizer.bytes_discarded, 0);
        assert_eq!(normalizer.sync_errors, 0);
    }

    #[test]
    fn a_single_204_packet_plus_finish_reads_as_188_but_finish_and_reset_recovers() {
        let only = packet(0x01);
        let framed = with_parity(&only);

        // EOF right after one packet: indistinguishable by votes, so the
        // 188-first tie-break applies. The 188-byte payload still comes out.
        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&framed);
        out.extend(normalizer.finish());
        assert_eq!(normalizer.framing(), Some(Framing::Ts188));
        assert_eq!(out, only);
        // The 16-byte tail cannot complete a 188-byte packet and is dropped.
        assert_eq!(normalizer.bytes_discarded, 16);

        // A mid-stream flush that resets the framing recovers 204 as soon as a
        // second packet proves it.
        let mut normalizer = TsNormalizer::new();
        let mut out = normalizer.push(&framed);
        out.extend(normalizer.finish_and_reset());
        assert_eq!(normalizer.framing(), None);
        let mut second = Vec::new();
        for _ in 0..2 {
            second.extend_from_slice(&with_parity(&only));
        }
        out.extend(normalizer.push(&second));
        out.extend(normalizer.finish());
        assert_eq!(normalizer.framing(), Some(Framing::Ts204));
        assert_eq!(out, [only.clone(), only.clone(), only].concat());
    }

    #[test]
    fn reset_clears_framing_and_counters() {
        let mut normalizer = TsNormalizer::new();
        normalizer.push(&stream(&[packet(0x01), packet(0x02), packet(0x03)]));
        normalizer.reset();

        assert_eq!(normalizer.framing(), None);
        assert_eq!(normalizer.packets_out, 0);
        assert_eq!(normalizer.buffered(), 0);
    }
}
